use core::{future::Future, pin::Pin};
use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use dashmap::DashMap;
use fancy_regex::Regex;
use futures::TryFutureExt;
use http_body_util::Full;
use hyper::{body::Incoming, client, service::Service, Request, Response};
use mirrord_protocol::{tcp::HttpResponse, ConnectionId, Port, RequestId};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{error::SendError, Sender},
        oneshot,
        oneshot::error::RecvError,
    },
};
use tracing::trace;

use super::{error::HttpTrafficError, UnmatchedHttpResponse, UnmatchedSender};
use crate::{
    steal::{HandlerHttpRequest, MatchedHttpRequest},
    util::ClientId,
};

pub(super) const DUMMY_RESPONSE_MATCHED: &str = "Matched!";
pub(super) const DUMMY_RESPONSE_UNMATCHED: &str = "Unmatched!";

#[derive(Debug)]
pub(super) struct HyperHandler {
    pub(super) filters: Arc<DashMap<ClientId, Regex>>,
    pub(super) matched_tx: Sender<HandlerHttpRequest>,
    pub(super) unmatched_tx: UnmatchedSender,
    pub(crate) connection_id: ConnectionId,
    pub(crate) port: Port,
    pub(crate) original_destination: SocketAddr,
    pub(crate) request_id: RequestId,
}

/// Sends a [`MatchedHttpRequest`] through `tx` to be handled by the stealer -> layer.
#[tracing::instrument(level = "debug", skip(tx))]
async fn matched_request(
    request: HandlerHttpRequest,
    tx: Sender<HandlerHttpRequest>,
) -> Result<(), HttpTrafficError> {
    tx.send(request).map_err(HttpTrafficError::from).await
}

/// Handles the case when no filter matches a header in the request.
///
/// 1. Creates a [`hyper::client::conn::http1::Connection`] to the `original_destination`;
/// 2. Sends the [`Request`] to it, and awaits a [`Response`];
/// 3. Sends the [`HttpResponse`] to the stealer, via the [`UnmatchedSender`] channel.
#[tracing::instrument(level = "debug", skip(tx))]
async fn unmatched_request(
    request: Request<Incoming>,
    tx: UnmatchedSender,
    original_destination: SocketAddr,
    connection_id: ConnectionId,
    request_id: RequestId,
) -> Result<(), SendError<Result<UnmatchedHttpResponse, HttpTrafficError>>> {
    // TODO(alex): We need a "retry" mechanism here for the client handling part, when the server
    // closes a connection, the client could still be wanting to send a request, so we need to
    // re-connect and send.
    let response = TcpStream::connect(original_destination)
        .map_err(From::from)
        .and_then(|target_stream| client::conn::http1::handshake(target_stream).map_err(From::from))
        .and_then(|(mut request_sender, connection)| {
            let tx = tx.clone();

            tokio::spawn(async move {
                if let Err(fail) = connection.await {
                    let _ = tx.send(Err(fail.into())).await.inspect_err(|fail| {
                        trace!("Sending an `UnmatchedHttpResponse` failed due to {fail:#?}!")
                    });
                }
            });

            request_sender.send_request(request).map_err(From::from)
        })
        .and_then(|intercepted_response| {
            HttpResponse::from_hyper_response(
                intercepted_response,
                original_destination.port(),
                connection_id,
                request_id,
            )
            .map_err(From::from)
        })
        .await
        .map(UnmatchedHttpResponse);

    tx.send(response)
        .await
        .inspect_err(|fail| trace!("Sending an `UnmatchedHttpResponse` failed due to {fail:#?}!"))
}

impl Service<Request<Incoming>> for HyperHandler {
    type Response = Response<String>;

    type Error = HttpTrafficError;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    // TODO(alex) [mid] 2022-12-13: Do we care at all about what is sent from here as a response to
    // our client duplex stream?
    #[tracing::instrument(level = "debug", skip(self))]
    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        if let Some(client_id) = request
            .headers()
            .iter()
            .map(|(header_name, header_value)| {
                header_value
                    .to_str()
                    .map(|header_value| format!("{}={}", header_name, header_value))
            })
            .find_map(|header| {
                self.filters.iter().find_map(|filter| {
                    // TODO(alex) [low] 2022-12-23: Remove the `header` unwrap.
                    if filter.is_match(header.as_ref().unwrap()).unwrap() {
                        Some(*filter.key())
                    } else {
                        None
                    }
                })
            })
        {
            self.request_id += 1;
            let req = MatchedHttpRequest {
                port: self.port,
                connection_id: self.connection_id,
                client_id,
                request_id: self.request_id,
                request,
            };

            let (response_tx, response_rx) = oneshot::channel();
            let handler_request = HandlerHttpRequest {
                request: req,
                response_tx,
            };

            let request_tx = self.matched_tx.clone();
            async move {
                // TODO: NO UNWRAP!
                matched_request(handler_request, request_tx).await.unwrap();
            };

            // TODO: NO UNWRAP!
            let response = response_rx.blocking_recv().unwrap();

            Box::pin(response)
        } else {
            self.request_id += 1;

            let tx = self.unmatched_tx.clone();
            let original_destination = self.original_destination;
            let connection_id = self.connection_id;
            let request_id = self.request_id;

            let response = async move {
                if let Err(fail) =
                    unmatched_request(request, tx, original_destination, connection_id, request_id)
                        .await
                {
                    fail.0?;
                };

                Ok(Response::new(DUMMY_RESPONSE_UNMATCHED.to_string()))
            };

            Box::pin(response)
        }
    }
}
