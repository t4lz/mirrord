// #![warn(missing_docs)]
// #![warn(rustdoc::missing_crate_level_docs)]

use std::{net::SocketAddr, sync::Arc};

use dashmap::DashMap;
use fancy_regex::Regex;
use mirrord_protocol::ConnectionId;
use tokio::{net::TcpStream, sync::mpsc::Sender};

use self::{
    error::HttpTrafficError,
    filter::{HttpFilterBuilder, MINIMAL_HEADER_SIZE},
    reversible_stream::ReversibleStream,
};
use crate::{steal::HandlerHttpRequest, util::ClientId};

pub(crate) mod error;
pub(super) mod filter;
mod hyper_handler;
pub(super) mod reversible_stream;

pub(super) type DefaultReversibleStream = ReversibleStream<MINIMAL_HEADER_SIZE>;

/// Identifies a message as being HTTP or not.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum HttpVersion {
    #[default]
    V1,
    V2,

    /// Handled as a special passthrough case, where the captured stream just forwards messages to
    /// their original destination (and vice-versa).
    NotHttp,
}

impl HttpVersion {
    /// Checks if `buffer` contains a valid HTTP/1.x request, or if it could be an HTTP/2 request by
    /// comparing it with a slice of [`H2_PREFACE`].
    #[tracing::instrument(level = "trace")]
    fn new(buffer: &[u8], h2_preface: &[u8]) -> Self {
        let mut empty_headers = [httparse::EMPTY_HEADER; 0];

        if buffer == h2_preface {
            Self::V2
        } else if matches!(
            httparse::Request::new(&mut empty_headers).parse(buffer),
            Ok(_) | Err(httparse::Error::TooManyHeaders)
        ) {
            Self::V1
        } else {
            Self::NotHttp
        }
    }
}

/// Created for every new port we want to filter HTTP traffic on.
#[derive(Debug)]
pub(super) struct HttpFilterManager {
    _port: u16,
    client_filters: Arc<DashMap<ClientId, Regex>>,

    /// We clone this to pass them down to the hyper tasks.
    matched_tx: Sender<HandlerHttpRequest>,
}

impl HttpFilterManager {
    /// Creates a new [`HttpFilterManager`] per port.
    ///
    /// You can't create just an empty [`HttpFilterManager`], as we don't steal traffic on ports
    /// that no client has registered interest in.
    pub(super) fn new(
        port: u16,
        client_id: ClientId,
        filter: Regex,
        matched_tx: Sender<HandlerHttpRequest>,
    ) -> Self {
        let client_filters = Arc::new(DashMap::with_capacity(128));
        client_filters.insert(client_id, filter);

        Self {
            _port: port,
            client_filters,
            matched_tx,
        }
    }

    // TODO(alex): Is adding a filter like this enough for it to be added to the hyper task? Do we
    // have a possible deadlock here? Tune in next week for the conclusion!
    //
    /// Inserts a new client (layer) and its filter.
    ///
    /// [`HttpFilterManager::client_filters`] are shared between hyper tasks, so adding a new one
    /// here will impact the tasks as well.
    pub(super) fn new_client(&mut self, client_id: ClientId, filter: Regex) -> Option<Regex> {
        self.client_filters.insert(client_id, filter)
    }

    /// Removes a client (layer) from [`HttpFilterManager::client_filters`].
    ///
    /// [`HttpFilterManager::client_filters`] are shared between hyper tasks, so removing a client
    /// here will impact the tasks as well.
    pub(super) fn remove_client(&mut self, client_id: &ClientId) -> Option<(ClientId, Regex)> {
        self.client_filters.remove(client_id)
    }

    pub(super) fn contains_client(&self, client_id: &ClientId) -> bool {
        self.client_filters.contains_key(client_id)
    }

    // If it matches the filter, we send this request via a channel to the layer. And on the
    // Manager, we wait for a message from the layer to send on the writer side of the actual
    // TcpStream.
    //
    /// Starts a new hyper task if the `connection` contains a _valid-ish_ HTTP request.
    ///
    /// The [`TcpStream`] itself os not what we feed hyper, instead we create a [`DuplexStream`],
    /// where one half (_server_) is where hyper does its magic, while the other half
    /// (_interceptor_) sends the bytes we get from the remote connection.
    ///
    /// The _interceptor_ stream is fed the bytes we're reading from the _original_ [`TcpStream`],
    /// and sends them to the _server_ stream.
    ///
    /// This mechanism is required to avoid having hyper send back [`Response`]s to the remote
    /// connection.
    pub(super) async fn new_connection(
        &self,
        original_stream: TcpStream,
        original_address: SocketAddr,
        connection_id: ConnectionId,
        connection_close_sender: Sender<ConnectionId>,
    ) -> Result<(), HttpTrafficError> {
        HttpFilterBuilder::new(
            original_stream,
            original_address,
            connection_id,
            self.client_filters.clone(),
            self.matched_tx.clone(),
            connection_close_sender,
        )
        .await?
        .start()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.client_filters.is_empty()
    }
}
