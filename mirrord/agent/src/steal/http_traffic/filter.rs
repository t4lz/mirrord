use core::{
    future::poll_fn,
    ops::{Deref, DerefMut},
    task::Poll,
};
use std::{net::SocketAddr, sync::Arc};

use dashmap::DashMap;
use fancy_regex::Regex;
use hyper::server::conn::http1;
use mirrord_protocol::ConnectionId;
use tokio::{
    io::{copy_bidirectional, duplex, AsyncReadExt, DuplexStream},
    net::TcpStream,
    sync::mpsc::Sender,
    task::JoinHandle,
};
use tracing::error;

use super::{
    error::HttpTrafficError, hyper_handler::HyperHandler, HttpVersion, PassthroughRequest,
};
use crate::{
    steal::{http_traffic::error, StealerHttpRequest},
    util::ClientId,
};

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0";

#[derive(Debug)]
pub(super) struct HttpFilterBuilder {
    http_version: HttpVersion,
    stolen_connection: StolenConnection,
    client_filters: Arc<DashMap<ClientId, Regex>>,
    captured_tx: Sender<StealerHttpRequest>,
    passthrough_tx: Sender<PassthroughRequest>,
}

/// Used by the stealer handler to:
///
/// 1. Read the requests from hyper's channels through [`captured_rx`], and [`passthrough_rx`];
/// 2. Send the raw bytes we got from the remote connection to hyper through [`interceptor_stream`];
pub(crate) struct HttpFilter {
    pub(super) hyper_task: JoinHandle<Result<(), HttpTrafficError>>,
    /// The original [`TcpStream`] that is connected to us, this is where we receive the requests
    /// from.
    pub(crate) original_stream: TcpStream,

    /// A stream that we use to communicate with the hyper task.
    ///
    /// Don't ever [`DuplexStream::read`] anything from it, as the hyper task only responds with
    /// garbage (treat the `read` side as `/dev/null`).
    ///
    /// We use [`DuplexStream::write`] to write the bytes we have `read` from [`original_stream`]
    /// to the hyper task, acting as a "client".
    pub(crate) interceptor_stream: DuplexStream,
}

/// Wrapper around the stealer's [`TcpStream`] that is being used to steal the data, that would
/// originally go to [`StolenStream::original_address`].
#[derive(Debug)]
pub(crate) struct StolenConnection {
    tcp_stream: TcpStream,

    /// Address that this stream was originally supposed to connect to.
    original_address: SocketAddr,

    connection_id: ConnectionId,
}

impl StolenConnection {
    #[tracing::instrument(level = "debug")]
    pub(crate) fn new(
        tcp_stream: TcpStream,
        original_address: SocketAddr,
        connection_id: ConnectionId,
    ) -> Self {
        Self {
            tcp_stream,
            original_address,
            connection_id,
        }
    }
}

impl Deref for StolenConnection {
    type Target = TcpStream;

    fn deref(&self) -> &Self::Target {
        &self.tcp_stream
    }
}

impl DerefMut for StolenConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tcp_stream
    }
}

impl HttpFilterBuilder {
    /// Does not consume bytes from the stream.
    ///
    /// Checks if the first available bytes in a stream could be of an http request.
    ///
    /// This is a best effort classification, not a guarantee that the stream is HTTP.
    #[tracing::instrument(level = "debug")]
    pub(super) async fn new(
        mut stolen_connection: StolenConnection,
        filters: Arc<DashMap<ClientId, Regex>>,
        captured_tx: Sender<StealerHttpRequest>,
        passthrough_tx: Sender<PassthroughRequest>,
    ) -> Result<Self, HttpTrafficError> {
        let mut buffer = [0u8; 64];
        // TODO(alex) [mid] 2022-12-09: Maybe we do need `poll_peek` here, otherwise just `peek`
        // might return 0 bytes peeked.
        //
        // ADD(alex) [mid] 2022-12-19: Just hit this issue while running tests, we return error due
        // to peeking 0 bytes.
        //
        // But wait, we're taking the not-http-stream as well, as the http-stream, so here we could
        // read. This requires changing a bunch of stuff though, probably easier to find a fix for
        // `peek`.

        match stolen_connection
            .peek(&mut buffer)
            .await
            .map_err(From::from)
            .and_then(|peeked_amount| {
                if peeked_amount == 0 {
                    Err(HttpTrafficError::Empty)
                } else {
                    Ok(HttpVersion::new(
                        &buffer[..peeked_amount],
                        &H2_PREFACE[..peeked_amount.min(H2_PREFACE.len())],
                    ))
                }
            }) {
            Ok(http_version) => Ok(Self {
                http_version,
                client_filters: filters,
                stolen_connection,
                captured_tx,
                passthrough_tx,
            }),
            // TODO(alex) [high] 2022-12-09: This whole filter is a passthrough case.
            //
            // ADD(alex) [high] 2022-12-16: Here we can use `orig_dst` or even get the original
            // destination from the stealer, then we can spawn a task that just pairs a stream that
            // reads from each side and writes to the other side.
            //
            // This being the handling mechanism for not-http passthrough.
            //
            // ADD(alex) [high] 2022-12-16: Should this even be an error? I think we could move this
            // into the `HttpVersion` enum, and have it be `NotHttp` variant, thus unifying the
            // creation of tasks there.
            Err(fail) => {
                error!("Something went wrong in http filter {fail:#?}");
                Err(fail)
            }
        }
    }

    /// Creates the hyper task, and returns an [`HttpFilter`] that contains the channels we use to
    /// pass the requests to the layer.
    #[tracing::instrument(level = "debug")]
    pub(super) fn start(self) -> Result<Option<HttpFilter>, HttpTrafficError> {
        let Self {
            http_version,
            client_filters,
            captured_tx,
            passthrough_tx,
            stolen_connection:
                StolenConnection {
                    mut tcp_stream,
                    original_address,
                    connection_id,
                },
        } = self;

        // Incoming tcp stream definitely has an address.
        let port = tcp_stream.local_addr().unwrap().port();

        match http_version {
            HttpVersion::V1 => {
                let (hyper_stream, interceptor_stream) = duplex(15000);

                let hyper_task = tokio::task::spawn(async move {
                    http1::Builder::new()
                        .serve_connection(
                            hyper_stream,
                            HyperHandler {
                                filters: client_filters,
                                captured_tx,
                                passthrough_tx,
                                connection_id,
                                port,
                            },
                        )
                        .await
                        .map_err(From::from)
                });

                Ok(Some(HttpFilter {
                    hyper_task,
                    original_stream: tcp_stream,
                    interceptor_stream,
                }))
            }
            // TODO(alex): hyper handling of HTTP/2 requires a bit more work, as it takes an
            // "executor" (just `tokio::spawn` in the `Builder::new` function is good enough), and
            // some more effort to chase some missing implementations.
            HttpVersion::V2 | HttpVersion::NotHttp => {
                println!("foo");
                let passhtrough_task = tokio::task::spawn(async move {
                    let mut interceptor_to_original = TcpStream::connect(original_address).await?;

                    copy_bidirectional(&mut tcp_stream, &mut interceptor_to_original).await?;
                    Ok::<_, error::HttpTrafficError>(())
                });

                Ok(None)
            }
        }
    }
}
