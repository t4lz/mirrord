#![feature(result_option_inspect)]
// #![warn(missing_docs)]
// #![warn(rustdoc::missing_crate_level_docs)]

use std::collections::HashMap;

use fancy_regex::Regex;
use hyper::{body, server::conn::http1, service::service_fn, Request, Response};
use thiserror::Error;
use tokio::{
    io::{duplex, AsyncReadExt, AsyncWriteExt, DuplexStream, ReadBuf},
    net::TcpStream,
    sync::mpsc::{channel, Receiver, Sender},
};
use tracing::debug;

// TODO(alex) [high] 2022-12-06: Serve 1 hyper per connection? If we don't do 1 to 1, then there is
// no easy way of knowing to whom we want to respond, or if these bytes are part of read A or read
// B.

// TODO(alex) [high] 2022-12-06: Which of these 2 do we want?
// - `A` leaves handling the pairing of connection/client id to the agent;
// - `B` makes this crate aware of such associations;
//
// I would go for `A`, and have the agent deal with ports, connections, and stuff like that.
//
// This means that the agent will be creating many filters.
//
// ADD(alex) [high] 2022-12-06: Flow is: agent creates a `Filter` with
// `new() -> Receiver<PassOrCaptured>`, this means that `Filter` holds a `Sender<PassOrCaptured>`.
//
// We use this `Sender` to send the response we get from the duplex channel we create for hyper,
// doing so keeps the whole filtering inside of this crate, and the agent will just keep reading
// from the `Receiver` channel.
//
// The agent-filter message is a bit more involved, we're going to use some sort of `Command`-like
// enum, so we can send back errors, maybe even a "done" message.
//
// We hold the `Client` side of the `DuplexStream` here, and have a public method `filter_message`
// that acts as a `stream.send()` wrapper, so the agent doesn't need to hold anything more than
// the `Filter` itself, and the `Receiver`.
//
// ADD(alex) [high] 2022-12-07: We probably need to hold a map of client/filter(regex) here?
// Not in the `Filter` itself, I think this should be done in the agent? Or in some higher
// abstraction?

struct FilterA {
    filters: HashMap<ClientId, Regex>,
    response_tx: Sender<String>, // this is where we send data back to the agent.
}

struct FilterB {
    regexes: HashMap<u64, Regex>,
    // we need an association of connection_id to response_tx (meh).
}

struct TrafficFilter {}

// TODO(alex) [low] 2022-12-08: We need to unify some of these types in a common crate, something
// like a `types` crate.
type ClientId = u64;
type ConnectionId = u64;

// TODO(alex) [high] 2022-12-07: This is created by the stealer (during its creation phase).
pub struct EnterpriseTrafficManager {
    clients: HashMap<ClientId, Regex>,
    filters: HashMap<u64, Vec<TrafficFilter>>,
}

impl Default for EnterpriseTrafficManager {
    fn default() -> Self {
        Self {
            clients: Default::default(),
            filters: Default::default(),
        }
    }
}

impl EnterpriseTrafficManager {
    // TODO(alex) [high] 2022-12-07: We don't have connections yet, just a filter for this client,
    // so no hyper involved here.
    //
    // ADD(alex) [high] 2022-12-08: Don't think we even need this, just pass the filter when there
    // is a new connection. Yeah, it could be done there, but then it gets messy if the client wants
    // to add a new filter, or change an existing one?
    //
    // It's less about adding/changing filters, and more about we keep this here instead of keeping
    // the client/filter map in the agent.
    pub fn new_client(&mut self, client_id: ClientId, filter: Regex) {
        self.clients.insert(client_id, filter);
    }

    // TODO(alex) [high] 2022-12-08: agent got a new connection in `listener.accept`, so now we
    // create the hyper connection and filter on it.
    //
    // We must return a channel that will be used by the stealer to send us the actual, final
    // response it gets from the layer, as we're taking the stream.
    //
    // The `Receiver` part of this channel does a `blocking_recv` in the hyper handler.
    pub async fn new_connection<ResponseFromStolenLayer>(
        &mut self,
        client_id: ClientId,
        connection_id: ConnectionId,
        // TODO(alex) [high] 2022-12-08: We need to return this `tcp_stream` for our error cases,
        // otherwise the agent would not steal from it?
        //
        // Or should something different happen when we detect this traffic as "not-HTTP"? Do we
        // fallback to regular "ports stealer"?
        //
        // If we can just error out and drop the connection, then we can just take the stream, and
        // drop it on error.
        //
        // Otherwise we need to send the stream back with the error (or take a reference, check
        // HTTP, then take the stream, blargh).
        tcp_stream: TcpStream,
    ) -> Sender<ResponseFromStolenLayer> {
        let filter = self.clients.get(&client_id).unwrap().clone();

        HttpFilterBuilder::new(tcp_stream).await.unwrap();

        todo!()
    }
}

#[derive(Error, Debug)]
pub enum HttpError {
    #[error("Failed parsing HTTP with 0 bytes!")]
    Empty,

    #[error("Failed parsing HTTP smaller than minimal!")]
    TooSmall,

    #[error("Failed as the buffer does not contain a valid HTTP request!")]
    NotHttp,

    #[error("Failed with IO `{0}`!")]
    IO(#[from] std::io::Error),

    #[error("Failed with Parse `{0}`!")]
    Parse(#[from] httparse::Error),

    #[error("Failed with Hyper `{0}`!")]
    Hyper(#[from] hyper::Error),
}

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0";

struct HttpFilterBuilder {
    tcp_stream: TcpStream,
}
struct HttpFilter {}

impl HttpFilterBuilder {
    /// Does not consume bytes from the stream.
    ///
    /// Checks if the first available bytes in a stream could be of an http request.
    ///
    /// This is a best effort classification, not a guarantee that the stream is HTTP.
    async fn new(tcp_stream: TcpStream) -> Result<Self, HttpError> {
        let mut buffer = [0u8; 64];
        let peeked_amount = tcp_stream.peek(&mut buffer).await?;

        if peeked_amount == 0 {
            Err(HttpError::Empty)
        } else {
            check_http(&buffer[1..peeked_amount], &H2_PREFACE[1..peeked_amount])?;

            Ok(Self { tcp_stream })
        }
    }

    // TODO(alex) [high] 2022-12-08: Creates the hyper task, should return the channels we need to
    // communicate with the hyper task.
    async fn start(self) -> Result<HttpFilter, HttpError> {
        todo!()
    }
}

/// Checks if `buffer` contains a valid HTTP/1.x request, or if it could be an HTTP/2 request by
/// comparing it with a slice of [`H2_PREFACE`].
fn check_http(buffer: &[u8], h2_preface: &[u8]) -> Result<(), HttpError> {
    let mut empty_headers = [httparse::EMPTY_HEADER; 0];
    if buffer == h2_preface
        || matches!(
            httparse::Request::new(&mut empty_headers).parse(buffer),
            Ok(_) | Err(httparse::Error::TooManyHeaders)
        )
    {
        Ok(())
    } else {
        Err(HttpError::NotHttp)
    }
}
