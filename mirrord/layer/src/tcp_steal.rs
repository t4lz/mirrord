use std::collections::{HashMap, HashSet};

use anyhow::Result;
use async_trait::async_trait;
use mirrord_protocol::{tcp::{HttpRequest, LayerTcpSteal, NewTcpConnection, PortSteal::Steal, TcpClose, TcpData}, ClientMessage, ConnectionId, Port};
use streammap_ext::StreamMap;
use tokio::{
    io::{AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::mpsc::Sender,
};
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use tracing::{error, trace};
use hyper::{Body, Client};
use hyper::client::conn::{handshake, SendRequest};

use crate::{
    error::LayerError,
    tcp::{Listen, TcpHandler},
};

#[derive(Default)]
pub struct TcpStealHandler {
    ports: HashSet<Listen>,
    write_streams: HashMap<ConnectionId, WriteHalf<TcpStream>>,
    read_streams: StreamMap<ConnectionId, ReaderStream<ReadHalf<TcpStream>>>,
    request_senders: HashMap<ConnectionId, SendRequest<Body>> // TODO: is Vec<u8> right?
}

#[async_trait]
impl TcpHandler for TcpStealHandler {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_new_connection(
        &mut self,
        tcp_connection: NewTcpConnection,
    ) -> Result<(), LayerError> {
        let stream = self.create_local_stream(&tcp_connection).await?;

        let (read_half, write_half) = tokio::io::split(stream);
        self.write_streams
            .insert(tcp_connection.connection_id, write_half);
        self.read_streams
            .insert(tcp_connection.connection_id, ReaderStream::new(read_half));

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(data = data.connection_id))]
    async fn handle_new_data(&mut self, data: TcpData) -> Result<(), LayerError> {
        // TODO: "remove -> op -> insert" pattern here, maybe we could improve the overlying
        // abstraction to use something that has mutable access.
        let mut connection = self
            .write_streams
            .remove(&data.connection_id)
            .ok_or(LayerError::NoConnectionId(data.connection_id))?;

        trace!(
            "handle_new_data -> writing {:#?} bytes to id {:#?}",
            data.bytes.len(),
            data.connection_id
        );
        // TODO: Due to the above, if we fail here this connection is leaked (-agent won't be told
        // that we just removed it).
        connection.write_all(&data.bytes[..]).await?;

        self.write_streams.insert(data.connection_id, connection);

        Ok(())
    }

    /// An http request was stolen by the http filter. Pass it to the local application.
    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_http_request(&mut self, request: HttpRequest) -> Result<(), LayerError> {
        let sender = match self.request_senders.get_mut(&request.connection_id) {
            Some(sender) => sender,
            None => self.create_http_connection(request.port, request.connection_id),
        };
        tokio::spawn( async move {
            let res = sender.send_request(request.request.into_hyper_request()).await?;
        });
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn handle_close(&mut self, close: TcpClose) -> Result<(), LayerError> {
        let TcpClose { connection_id } = close;

        // Dropping the connection -> Sender drops -> Receiver disconnects -> tcp_tunnel ends
        let _ = self.read_streams.remove(&connection_id);
        let _ = self.write_streams.remove(&connection_id);

        Ok(())
    }

    fn ports(&self) -> &HashSet<Listen> {
        &self.ports
    }

    fn ports_mut(&mut self) -> &mut HashSet<Listen> {
        &mut self.ports
    }

    #[tracing::instrument(level = "trace", skip(self, tx))]
    async fn handle_listen(
        &mut self,
        listen: Listen,
        tx: &Sender<ClientMessage>,
    ) -> Result<(), LayerError> {
        let port = listen.requested_port;

        self.ports_mut()
            .insert(listen)
            .then_some(())
            .ok_or(LayerError::ListenAlreadyExists)?;

        tx.send(ClientMessage::TcpSteal(LayerTcpSteal::PortSubscribe(
            Steal(port),
        )))
        .await
        .map_err(From::from)
    }
}

impl TcpStealHandler {
    pub async fn next(&mut self) -> Option<ClientMessage> {
        let (connection_id, value) = self.read_streams.next().await?;
        match value {
            Some(Ok(bytes)) => Some(ClientMessage::TcpSteal(LayerTcpSteal::Data(TcpData {
                connection_id,
                bytes: bytes.to_vec(),
            }))),
            Some(Err(err)) => {
                error!("connection id {connection_id:?} read error: {err:?}");
                None
            }
            None => Some(ClientMessage::TcpSteal(
                LayerTcpSteal::ConnectionUnsubscribe(connection_id),
            )),
        }
    }

    async fn create_http_connection(&mut self, port: Port, connection_id: ConnectionId) -> &mut SendRequest<Body> {
        let target_stream = TcpStream::connect(format!("localhost:{}", port)).await?;

        let (mut sender, connection) = handshake(target_stream).await?;

        // spawn a task to poll the connection and drive the HTTP state
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Error in http connection {} on port {}: {}", connection_id, port, e);
            }
        });

        // TODO: can those two lines be done in one step?
        self.request_senders.insert(connection_id, sender);
        self.request_senders.get_mut(&connection_id).unwrap()
    }

}
