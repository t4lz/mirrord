use tokio::sync::mpsc::{Receiver, Sender};
use mirrord_protocol::{ConnectionId, Port};
use mirrord_protocol::tcp::DaemonTcp;
use crate::{AgentError, ClientId};
use crate::AgentError::SendStealerCommand;
use crate::error::Result;

/// Association between a client (identified by the `client_id`) and a [`Command`].
///
/// The (agent -> worker) channel uses this, instead of naked [`Command`]s when communicating.
#[derive(Debug)]
pub struct ClientCommand<C> {
    /// Identifies which layer instance is sending the [`Command`].
    client_id: ClientId,

    /// The command message sent from (layer -> agent) to be handled by the stealer worker.
    command: C,
}

/// Bridges the communication between the agent and the [`TcpConnectionStealer`] task.
/// There is an API instance for each connected layer ("client"). All API instances send commands
/// On the same stealer command channel, where the layer-independent stealer listens to them.
#[derive(Debug)]
pub(crate) struct AgentTcpWorkerApi<C: TcpCommand> {
    /// Identifies which layer instance is associated with this API.
    client_id: ClientId,

    /// Channel that allows the agent to communicate with the stealer task.
    ///
    /// The agent controls the stealer task through this.
    command_tx: Sender<ClientCommand<C>>,

    /// Channel that receives [`DaemonTcp`] messages from the stealer worker thread.
    ///
    /// This is where we get the messages that should be passed back to agent or layer.
    daemon_rx: Receiver<DaemonTcp>,
}

pub(crate) trait TcpCommand {
    fn get_new_client_command(message_sender: Sender<DaemonTcp>) -> Self;
    fn get_port_subscribe_command(port: Port) -> Self;
    fn get_port_unsubscribe_command(port: Port) -> Self;
    fn get_connection_unsubscribe_command(connection: ConnectionId) -> Self;
    fn get_client_close_command() -> Self;
}


impl<C: TcpCommand> AgentTcpWorkerApi<C> {
    /// Initializes a [`TcpStealerApi`] and sends a message to [`TcpConnectionStealer`] signaling
    /// that we have a new client.
    #[tracing::instrument(level = "trace")]
    pub(crate) async fn new(
        client_id: ClientId,
        command_tx: Sender<ClientCommand<C>>,
        (daemon_tx, daemon_rx): (Sender<DaemonTcp>, Receiver<DaemonTcp>),
    ) -> Result<Self, AgentError> {
        let mut worker_api_instance = Self {
            client_id,
            command_tx,
            daemon_rx,
        };
        worker_api_instance.new_client(daemon_tx).await?;
        Ok(worker_api_instance)
    }

    /// Send `command` to stealer, with the client id of the client that is using this API instance.
    pub(crate) async fn send_command(&self, command: C) -> Result<()> {
        self.command_tx
            .send(ClientCommand {
                client_id: self.client_id,
                command,
            })
            .await
            .map_err(|err| SendStealerCommand)
    }

    /// Send `command` synchronously to stealer with `try_send`, with the client id of the client
    /// that is using this API instance.
    fn try_send_command(&self, command: C) -> Result<()> {
        self.command_tx
            .try_send(ClientCommand {
                client_id: self.client_id,
                command
            })
            .map_err(From::from)
    }

    /// Helper function that passes the [`DaemonTcp`] messages we generated in the
    /// [`TcpConnectionStealer`] task, back to the agent.
    ///
    /// Called in the [`ClientConnectionHandler`].
    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn recv(&mut self) -> Option<DaemonTcp>{
        self.daemon_rx.recv().await
    }

    /// Handles the conversion of [`LayerTcpSteal::PortSubscribe`], that is passed from the
    /// agent, to an internal stealer command [`Command::PortSubscribe`].
    ///
    /// The actual handling of this message is done in [`TcpConnectionStealer`].
    pub(crate) async fn new_client(&mut self, message_sender: Sender<DaemonTcp>) -> Result<(), AgentError> {
        self.send_command(C::get_new_client_command(message_sender)).await
    }

    /// Handles the conversion of [`LayerTcpSteal::PortSubscribe`], that is passed from the
    /// agent, to an internal stealer command [`Command::PortSubscribe`].
    ///
    /// The actual handling of this message is done in [`TcpConnectionStealer`].
    pub(crate) async fn port_subscribe(&mut self, port: Port) -> Result<(), AgentError> {
        self.send_command(C::get_port_subscribe_command(port)).await
    }

    /// Handles the conversion of [`LayerTcpSteal::PortUnsubscribe`], that is passed from the
    /// agent, to an internal stealer command [`Command::PortUnsubscribe`].
    ///
    /// The actual handling of this message is done in [`TcpConnectionStealer`].
    pub(crate) async fn port_unsubscribe(&mut self, port: Port) -> Result<(), AgentError> {
        self.send_command(C::get_port_unsubscribe_command(port)).await
    }

    /// Handles the conversion of [`LayerTcpSteal::ConnectionUnsubscribe`], that is passed from the
    /// agent, to an internal stealer command [`Command::ConnectionUnsubscribe`].
    ///
    /// The actual handling of this message is done in [`TcpConnectionStealer`].
    pub(crate) async fn connection_unsubscribe(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<(), AgentError> {
        self.send_command(C::get_connection_unsubscribe_command(connection_id)).await
    }

    /// Handles the conversion of [`LayerTcpSteal::ClientClose`], that is passed from the
    /// agent, to an internal stealer command [`Command::ClientClose`].
    ///
    /// The actual handling of this message is done in [`TcpConnectionStealer`].
    ///
    /// Called by the [`Drop`] implementation of [`TcpStealerApi`].
    pub(crate) fn close_client(&mut self) -> Result<(), AgentError> {
        self.try_send_command(C::get_client_close_command())
    }
}

/// Drop for only TCP (stealer and sniffer) APIs.
impl<C: TcpCommand> Drop for AgentTcpWorkerApi<C> {
    fn drop(&mut self) {
        self.close_client()
            .expect("Failed while dropping TcpStealerApi!")
    }
}
