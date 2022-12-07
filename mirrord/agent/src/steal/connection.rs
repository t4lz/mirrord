use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
};
use std::collections::HashMap;
use std::path::PathBuf;

use bytes::Bytes;
use futures::SinkExt;
use mirrord_protocol::{tcp::{NewTcpConnection, TcpClose}, ResponseError::PortAlreadyStolen, Port, ConnectionId};
use iptables::IPTables;
use streammap_ext::StreamMap;
use tokio::{io::{AsyncWriteExt, ReadHalf, WriteHalf}, net::TcpStream, select};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};
use mirrord_protocol::tcp::{DaemonTcp, TcpData};

use crate::AgentError::AgentInvariantViolated;
use crate::{AgentError, ClientCommand, ClientId, IndexAllocator};
use crate::runtime::set_namespace;
use crate::util::Subscriptions;
use crate::error::Result;
use crate::steal::api::StealerCommand;
use crate::steal::ip_tables::SafeIpTables;

/// Created once per agent during initialization.
///
/// Runs as a separate thread while the agent lives.
///
/// - (agent -> stealer) communication is handled by [`command_rx`];
/// - (stealer -> agent) communication is handled by [`client_senders`], and the [`Sender`] channels
///   come inside [`StealerCommand`]s through  [`command_rx`];
pub(crate) struct TcpConnectionStealer {
    port_subscriptions: Subscriptions<Port, ClientId>,

    /// Communication between (agent -> stealer) task.
    ///
    /// The agent controls the stealer task through [`TcpStealerAPI::command_tx`].
    command_rx: Receiver<ClientCommand<StealerCommand>>,

    /// Connected clients (layer instances) and the channels which the stealer task uses to send
    /// back messages (stealer -> agent -> layer).
    clients: HashMap<ClientId, Sender<DaemonTcp>>,
    index_allocator: IndexAllocator<ConnectionId>,

    /// Intercepts the connections, instead of letting them go through their normal pathways, this
    /// is used to steal the traffic.
    stealer: TcpListener,

    /// Set of rules the agent uses to steal traffic from through the
    /// [`TcpConnectionStealer::stealer`] listener.
    /// None when there are no subscribers.
    iptables: Option<SafeIpTables<IPTables>>,

    /// Used to send data back to the original remote connection.
    write_streams: HashMap<ConnectionId, WriteHalf<TcpStream>>,

    /// Used to read data from the remote connections.
    read_streams: StreamMap<ConnectionId, ReaderStream<ReadHalf<TcpStream>>>,

    /// Associates a `ConnectionId` with a `ClientID`, so we can send the data we read from
    /// [`TcpConnectionStealer::read_streams`] to the appropriate client (layer).
    client_connections: HashMap<ConnectionId, ClientId>,
}

impl TcpConnectionStealer {
    /// Initializes a new [`TcpConnectionStealer`] fields, but doesn't start the actual working
    /// task (call [`TcpConnectionStealer::start`] to do so).
    #[tracing::instrument(level = "trace")]
    pub(crate) async fn new(
        command_rx: Receiver<ClientCommand<StealerCommand>>,
        pid: Option<u64>,
    ) -> Result<Self, AgentError> {
        if let Some(pid) = pid {
            let namespace = PathBuf::from("/proc")
                .join(PathBuf::from(pid.to_string()))
                .join(PathBuf::from("ns/net"));

            set_namespace(namespace).unwrap();
        }

        Ok(Self {
            port_subscriptions: Subscriptions::new(),
            command_rx,
            clients: HashMap::with_capacity(8),
            index_allocator: IndexAllocator::new(),
            stealer: TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).await?,
            iptables: None, // Initialize on first subscription.
            write_streams: HashMap::with_capacity(8),
            read_streams: StreamMap::with_capacity(8),
            client_connections: HashMap::with_capacity(8),
        })
    }

    /// Get a result with a reference to the iptables.
    /// Should only be called while there are subscribers (otherwise self.iptables is None).
    fn iptables(&self) -> Result<&SafeIpTables<IPTables>> {
        debug_assert!(self.iptables.is_some()); // is_some as long as there are subs
        self.iptables.as_ref().ok_or(AgentInvariantViolated)
    }

    /// Runs the tcp traffic stealer loop.
    ///
    /// The loop deals with 3 different paths:
    ///
    /// 1. Receiving [`StealerCommand`]s and calling [`TcpConnectionStealer::handle_command`];
    ///
    /// 2. Accepting remote connections through the [`TcpConnectionStealer::stealer`]
    /// [`TcpListener`]. We steal traffic from the created streams.
    ///
    /// 3. Handling the cancellation of the whole stealer thread.
    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn start(
        mut self,
        cancellation_token: CancellationToken,
    ) -> Result<(), AgentError> {
        loop {
            select! {
                command = self.command_rx.recv() => {
                    if let Some(command) = command {
                        self.handle_command(command).await?;
                    } else { break; }
                },
                // Accepts a connection that we're going to be stealing traffic from.
                accept = self.stealer.accept() => {
                    match accept {
                        Ok(accept) => {
                            self.incoming_connection(accept).await?;
                        }
                        Err(fail) => {
                            error!("Something went wrong while accepting a connection {:#?}", fail);
                            break;
                        }
                    }
                }
                Some((connection_id, incoming_data)) = self.read_streams.next() => {
                    // TODO: Should we spawn a task to forward the data?
                    if let Err(fail) = self.forward_incoming_tcp_data(connection_id, incoming_data).await {
                        error!("Failed reading incoming tcp data with {fail:#?}!");
                    }
                }
                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Forwards data from a remote stream to the client with `connection_id`.
    #[tracing::instrument(level = "trace", skip(self))]
    async fn forward_incoming_tcp_data(
        &mut self,
        connection_id: ConnectionId,
        incoming_data: Option<Result<Bytes, io::Error>>,
    ) -> Result<(), AgentError> {
        // Create a message to send to the client, or propagate an error.
        let daemon_tcp_message = incoming_data
            .map(|incoming_data_result| match incoming_data_result {
                Ok(bytes) => Ok(DaemonTcp::Data(TcpData {
                    connection_id,
                    bytes: bytes.to_vec(),
                })),
                Err(fail) => {
                    error!("connection id {connection_id:?} read error: {fail:?}");
                    Err(AgentError::IO(fail))
                }
            })
            .unwrap_or(Ok(DaemonTcp::Close(TcpClose { connection_id })))?;

        if let Some(daemon_tx) = self
            .client_connections
            .get(&connection_id)
            .and_then(|client_id| self.clients.get(client_id))
        {
            Ok(daemon_tx.send(daemon_tcp_message).await?)
        } else {
            // Either connection_id or client_id does not exist. This would be a bug.
            error!(
                "Internal error: An invariant is not being held between connection_id and client_id for stealer!"
            );
            debug_assert!(false);
            Ok(())
        }
    }

    /// Handles a new remote connection that was accepted on the [`TcpConnectionStealer::stealer`]
    /// listener.
    ///
    /// We separate the stream created by accepting the connection into [`ReadHalf`] and
    /// [`WriteHalf`] to handle reading and sending separately.
    ///
    /// Also creates an association between `connection_id` and `client_id` to be used by
    /// [`forward_incoming_tcp_data`].
    #[tracing::instrument(level = "trace", skip(self))]
    async fn incoming_connection(
        &mut self,
        (stream, address): (TcpStream, SocketAddr),
    ) -> Result<(), AgentError> {
        let real_address = super::orig_dst::orig_dst_addr(&stream)?;

        // Get the first client that is subscribed to this port and give it the new connection.
        if let Some(client_id) = self
            .port_subscriptions
            .get_topic_subscribers(real_address.port())
            .first()
            .cloned()
        {
            let connection_id = self.index_allocator.next_index().unwrap();

            let (read_half, write_half) = tokio::io::split(stream);
            self.write_streams.insert(connection_id, write_half);
            self.read_streams
                .insert(connection_id, ReaderStream::new(read_half));

            self.client_connections.insert(connection_id, client_id);

            let new_connection = DaemonTcp::NewConnection(NewTcpConnection {
                connection_id,
                destination_port: real_address.port(),
                source_port: address.port(),
                address: address.ip(),
            });

            // Send new connection to subscribed layer.
            debug_assert!(self.clients.get(&client_id).is_some());
            match self.clients.get(&client_id) {
                Some(daemon_tx) => Ok(daemon_tx.send(new_connection).await?),
                None => {
                    // Should not happen.
                    error!("Internal error: subscriptions of closed client still present.");
                    Ok(self.close_client(client_id)?)
                }
            }
        } else {
            Err(AgentError::UnexpectedConnection(real_address.port()))
        }
    }

    /// Registers a new layer instance that has the `steal` feature enabled.
    #[tracing::instrument(level = "trace", skip(self, sender))]
    fn new_client(&mut self, client_id: ClientId, sender: Sender<DaemonTcp>) {
        self.clients.insert(client_id, sender);
    }

    /// Helper function to handle [`Command::PortSubscribe`] messages.
    ///
    /// Inserts `port` into [`TcpConnectionStealer::iptables`] rules, and subscribes the layer with
    /// `client_id` to steal traffic from it.
    #[tracing::instrument(level = "trace", skip(self))]
    async fn port_subscribe(&mut self, client_id: ClientId, port: Port) -> Result<(), AgentError> {
        let res =
            if let Some(client_id) = self.port_subscriptions.get_topic_subscribers(port).first() {
                error!("Port {port:?} is already being stolen by client {client_id:?}!");
                Err(PortAlreadyStolen(port))
            } else {
                if self.port_subscriptions.is_empty() {
                    // Is this the first client?
                    // Initialize IP table only when a client is subscribed.
                    self.iptables = Some(SafeIpTables::new(iptables::new(false).unwrap())?);
                }
                self.port_subscriptions.subscribe(client_id, port);
                self.iptables
                    .add_redirect(port, self.stealer.local_addr()?.port())?;
                Ok(port)
            };
        self.send_message_to_single_client(&client_id, DaemonTcp::SubscribeResult(res))
            .await
    }

    /// Helper function to handle [`Command::PortUnsubscribe`] messages.
    ///
    /// Removes `port` from [`TcpConnectionStealer::iptables`] rules, and unsubscribes the layer
    /// with `client_id`.
    #[tracing::instrument(level = "trace", skip(self))]
    fn port_unsubscribe(&mut self, client_id: ClientId, port: Port) -> Result<(), AgentError> {
        self.port_subscriptions
            .get_client_topics(client_id)
            .iter()
            .find_map(|subscribed_port| {
                (*subscribed_port == port).then(|| {
                    self.iptables()?
                        .remove_redirect(port, self.stealer.local_addr()?.port())
                })
            })
            .transpose()?;

        self.port_subscriptions.unsubscribe(client_id, port);
        if self.port_subscriptions.is_empty() {
            // Was this the last client?
            self.iptables = None; // The Drop impl of iptables cleans up.
        }

        Ok(())
    }

    /// Removes the client with `client_id` from our list of clients (layers), and also removes
    /// their redirection rules from [`TcpConnectionStealer::iptables`].
    #[tracing::instrument(level = "trace", skip(self))]
    fn close_client(&mut self, client_id: ClientId) -> Result<(), AgentError> {
        let stealer_port = self.stealer.local_addr()?.port();
        let ports = self.port_subscriptions.get_client_topics(client_id);
        debug_assert!(self.iptables.is_some()); // is_some as long as there are subs
        ports
            .into_iter()
            .try_for_each(|port| self.iptables()?.remove_redirect(port, stealer_port))?;

        self.port_subscriptions.remove_client(client_id);

        self.clients.remove(&client_id);
        Ok(())
    }

    /// Sends a [`DaemonTcp`] message back to the client with `client_id`.
    #[tracing::instrument(level = "trace", skip(self))]
    async fn send_message_to_single_client(
        &mut self,
        client_id: &ClientId,
        message: DaemonTcp,
    ) -> Result<(), AgentError> {
        if let Some(sender) = self.clients.get(client_id) {
            sender.send(message).await.map_err(|fail| {
                warn!(
                    "Failed to send message to client {} with {:#?}!",
                    client_id, fail
                );
                let _ = self.close_client(*client_id);
                fail
            })?;
        }

        Ok(())
    }

    /// Write the data received from local app via layer to the stream with end client.
    async fn forward_data(&mut self, tcp_data: TcpData) -> std::result::Result<(), AgentError> {
        if let Some(stream) = self.write_streams.get_mut(&tcp_data.connection_id) {
            stream.write_all(&tcp_data.bytes[..]).await?;
            Ok(())
        } else {
            warn!(
                "Trying to send data to closed connection {:?}",
                tcp_data.connection_id
            );
            Ok(())
        }
    }

    /// Removes the ([`ReadHalf`], [`WriteHalf`]) pair of streams, disconnecting the remote
    /// connection.
    #[tracing::instrument(level = "trace", skip(self))]
    fn connection_unsubscribe(&mut self, connection_id: ConnectionId) {
        self.write_streams.remove(&connection_id);
        self.read_streams.remove(&connection_id);
    }

    /// Handles [`Command`]s that were received by [`TcpConnectionStealer::command_rx`].
    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_command(&mut self, command: ClientCommand<StealerCommand>) -> Result<(), AgentError> {
        let ClientCommand { client_id, command } = command;

        match command {
            StealerCommand::NewClient(daemon_tx) => self.new_client(client_id, daemon_tx),
            StealerCommand::ConnectionUnsubscribe(connection_id) => {
                self.connection_unsubscribe(connection_id)
            }
            StealerCommand::PortSubscribe(port) => self.port_subscribe(client_id, port).await?,
            StealerCommand::PortUnsubscribe(port) => self.port_unsubscribe(client_id, port)?,
            StealerCommand::ClientClose => self.close_client(client_id)?,
            StealerCommand::ResponseData(tcp_data) => self.forward_data(tcp_data).await?,
        }

        Ok(())
    }
}
