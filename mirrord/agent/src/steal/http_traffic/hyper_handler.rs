use core::{future::Future, pin::Pin};
use std::{net::SocketAddr, sync::Arc};

use dashmap::DashMap;
use fancy_regex::Regex;
use futures::{FutureExt, TryFutureExt};
use hyper::{body::Incoming, client, service::Service, Request, Response};
use mirrord_protocol::{tcp::HttpResponse, ConnectionId, Port, RequestId};
use tokio::{net::TcpStream, sync::mpsc::Sender};

use super::{error::HttpTrafficError, UnmatchedHttpResponse};
use crate::{error::AgentError, steal::MatchedHttpRequest, util::ClientId};

pub(super) const DUMMY_RESPONSE_MATCHED: &str = "Matched!";
pub(super) const DUMMY_RESPONSE_UNMATCHED: &str = "Unmatched!";

pub(crate) type UnmatchedSender = Sender<Result<UnmatchedHttpResponse, HttpTrafficError>>;

#[derive(Debug)]
pub(super) struct HyperHandler {
    pub(super) filters: Arc<DashMap<ClientId, Regex>>,
    pub(super) matched_tx: Sender<MatchedHttpRequest>,
    // pub(super) unmatched_tx: Sender<UnmatchedHttpResponse>,
    // pub(super) matched_tx: Sender<Result<MatchedHttpRequest, HttpTrafficError>>,
    pub(super) unmatched_tx: UnmatchedSender,
    pub(crate) connection_id: ConnectionId,
    pub(crate) port: Port,
    pub(crate) original_destination: SocketAddr,
    pub(crate) request_id: RequestId,
}

// #[tracing::instrument(level = "debug", skip(tx))]
fn unmatched_request(
    request: Request<Incoming>,
    tx: UnmatchedSender,
    original_destination: SocketAddr,
    connection_id: ConnectionId,
    request_id: RequestId,
) {
    tokio::spawn(async move {
        let response = TcpStream::connect(original_destination)
            .map_err(From::from)
            .and_then(|target_stream| {
                client::conn::http1::handshake(target_stream).map_err(From::from)
            })
            .and_then(|(mut request_sender, connection)| {
                tokio::spawn(async move {
                    if let Err(fail) = connection.await {
                        // TODO(alex) [low] 2022-12-23: Send the error in the channel.
                        eprintln!("Error in connection: {}", fail);
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
            .map(|response| UnmatchedHttpResponse(response));

        tx.send(response).map_err(AgentError::from).await
    });
}

impl Service<Request<Incoming>> for HyperHandler {
    type Response = Response<String>;

    type Error = HttpTrafficError;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    // TODO(alex) [mid] 2022-12-13: Do we care at all about what is sent from here as a response to
    // our client duplex stream?
    // #[tracing::instrument(level = "debug", skip(self))]
    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        // TODO(alex) [mid] 2022-12-20: The `Incoming` to `Bytes` conversion should be done here
        // for both cases, as that's what we care about.
        if let Some(client_id) = request
            .headers()
            .iter()
            .map(|(header_name, header_value)| {
                format!("{}={}", header_name, header_value.to_str().unwrap())
            })
            .find_map(|header| {
                self.filters.iter().find_map(|filter| {
                    if filter.is_match(&header).unwrap() {
                        Some(filter.key().clone())
                    } else {
                        None
                    }
                })
            })
        {
            let request = MatchedHttpRequest {
                port: self.port,
                connection_id: self.connection_id,
                client_id,
                request_id: self.request_id,
                request,
            };

            let matched_tx = self.matched_tx.clone();

            // Creates a task to send the matched request (cannot use `await` in the `call`
            // function, so we have to do this).
            tokio::spawn(async move {
                matched_tx
                    .send(request)
                    .map_err(HttpTrafficError::from)
                    .await
            });

            self.request_id += 1;

            let response = async { Ok(Response::new(DUMMY_RESPONSE_MATCHED.to_string())) };
            Box::pin(response)
        } else {
            unmatched_request(
                request,
                self.unmatched_tx.clone(),
                self.original_destination,
                self.connection_id,
                self.request_id,
            );
            self.request_id += 1;

            let response = async { Ok(Response::new(DUMMY_RESPONSE_UNMATCHED.to_string())) };
            Box::pin(response)
        }
    }
}
