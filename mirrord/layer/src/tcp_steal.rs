use std::collections::{HashMap, HashSet};

use anyhow::Result;
use async_trait::async_trait;
use mirrord_config::network::NetworkConfig;
use mirrord_http::HttpHeaderSelect;
use mirrord_protocol::{
    tcp::{LayerTcpSteal, NewTcpConnection, TcpClose, TcpData},
    ClientMessage, ConnectionId,
};
use streammap_ext::StreamMap;
use tokio::{
    io::{AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::mpsc::Sender,
};
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use tracing::{error, trace};

use crate::{
    error::LayerError,
    tcp::{Listen, TcpHandler},
};

#[derive(Default)]
pub struct TcpStealHandler {
    ports: HashSet<Listen>,
    write_streams: HashMap<ConnectionId, WriteHalf<TcpStream>>,
    read_streams: StreamMap<ConnectionId, ReaderStream<ReadHalf<TcpStream>>>,

    /// Only 1 filter can be active for this `layer` as, currently, there is no way for the user to
    /// specify filters for different ports.
    http_header_select: Option<HttpHeaderSelect>,
}

impl TcpStealHandler {
    pub(super) fn new(config: NetworkConfig) -> Self {
        Self {
            http_header_select: config.into(),
            ..Default::default()
        }
    }
}

#[async_trait]
impl TcpHandler for TcpStealHandler {
    #[tracing::instrument(level = "debug", skip(self))]
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

    #[tracing::instrument(level = "debug", skip(self), fields(data = data.connection_id))]
    async fn handle_new_data(&mut self, data: TcpData) -> Result<(), LayerError> {
        // TODO: "remove -> op -> insert" pattern here, maybe we could improve the overlying
        // abstraction to use something that has mutable access.
        let mut connection = self
            .write_streams
            .remove(&data.connection_id)
            .ok_or(LayerError::NoConnectionId(data.connection_id))?;

        // TODO: Due to the above, if we fail here this connection is leaked (-agent won't be told
        // that we just removed it).
        connection.write_all(&data.bytes[..]).await?;

        self.write_streams.insert(data.connection_id, connection);

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
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

    #[tracing::instrument(level = "debug", skip(self, tx))]
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

        let layer_tcp_steal = self
            .http_header_select
            .map_or(LayerTcpSteal::PortSubscribe(port), |http_header_select| {
                LayerTcpSteal::FilterTraffic(port, self.http_header_select.into())
            });

        Ok(tx.send(ClientMessage::TcpSteal(layer_tcp_steal)).await?)
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
}
