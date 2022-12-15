use std::{fmt, net::IpAddr};

use bincode::{Decode, Encode};
use hyper::{HeaderMap, Method, StatusCode, Uri, Version};
use serde::{Deserialize, Serialize};

use crate::{ConnectionId, Port, RemoteResult};

#[derive(Encode, Decode, Debug, PartialEq, Eq, Clone)]
pub struct NewTcpConnection {
    pub connection_id: ConnectionId,
    pub address: IpAddr,
    pub destination_port: Port,
    pub source_port: Port,
}

#[derive(Encode, Decode, PartialEq, Eq, Clone)]
pub struct TcpData {
    pub connection_id: ConnectionId,
    pub bytes: Vec<u8>,
}

impl fmt::Debug for TcpData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpData")
            .field("connection_id", &self.connection_id)
            .field("bytes (length)", &self.bytes.len())
            .finish()
    }
}

#[derive(Encode, Decode, Debug, PartialEq, Eq, Clone)]
pub struct TcpClose {
    pub connection_id: ConnectionId,
}

/// Messages related to Tcp handler from client.
#[derive(Encode, Decode, Debug, PartialEq, Eq, Clone)]
pub enum LayerTcp {
    PortSubscribe(Port),
    ConnectionUnsubscribe(ConnectionId),
    PortUnsubscribe(Port),
}

/// Messages related to Tcp handler from server.
#[derive(Encode, Decode, Debug, PartialEq, Eq, Clone)]
pub enum DaemonTcp {
    NewConnection(NewTcpConnection),
    Data(TcpData),
    Close(TcpClose),
    /// Used to notify the subscription occured, needed for e2e tests to remove sleeps and
    /// flakiness.
    SubscribeResult(RemoteResult<Port>),
    HttpRequest(HttpRequest),
}

/// Describes the stealing subscription to a port:
#[derive(Encode, Decode, Debug, PartialEq, Eq, Clone)]
pub enum PortSteal {
    /// Steal all traffic to this port.
    Steal(Port),
    /// Steal HTTP traffic matching a given filter.
    HttpFilterSteal(Port, String),
}

/// Messages related to Steal Tcp handler from client.
#[derive(Encode, Decode, Debug, PartialEq, Eq, Clone)]
pub enum LayerTcpSteal {
    PortSubscribe(PortSteal),
    ConnectionUnsubscribe(ConnectionId),
    PortUnsubscribe(Port),
    Data(TcpData),
    HttpResponse(HttpResponse),
}

/// (De-)Serializable HTTP request.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct InternalHttpRequest {
    #[serde(with = "http_serde::method")]
    pub method: Method,

    #[serde(with = "http_serde::uri")]
    pub uri: Uri,

    #[serde(with = "http_serde::header_map")]
    pub headers: HeaderMap,

    #[serde(with = "http_serde::version")]
    pub version: Version,

    pub body: Vec<u8>,
    // TODO: What about `extensions`? There is no `http_serde` method for it but it is in `Parts`.
}

#[derive(Encode, Decode, Debug, PartialEq, Eq, Clone)]
pub struct HttpRequest {
    #[bincode(with_serde)]
    pub request: InternalHttpRequest,
    pub connection_id: ConnectionId,
    /// Unlike TcpData, HttpRequest includes the port, so that the connection can be created
    /// "lazily", with the first filtered request.
    pub port: Port,
}

/// (De-)Serializable HTTP response.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct InternalHttpResponse {
    #[serde(with = "http_serde::status_code")]
    status: StatusCode,

    #[serde(with = "http_serde::version")]
    version: Version,

    #[serde(with = "http_serde::header_map")]
    headers: HeaderMap,

    body: Vec<u8>,
}

#[derive(Encode, Decode, Debug, PartialEq, Eq, Clone)]
pub struct HttpResponse {
    /// This is used to make sure the response is sent in its turn, after responses to all earlier
    /// requests were already sent.
    pub request_id: u64,
    pub connection_id: ConnectionId,
    pub port: Port,
    #[bincode(with_serde)]
    pub request: InternalHttpRequest,
}
