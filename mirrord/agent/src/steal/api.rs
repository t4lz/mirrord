use mirrord_protocol::tcp::{DaemonTcp, TcpData};
use tokio::sync::mpsc::{Receiver, Sender};
use mirrord_protocol::{ConnectionId, Port};

use crate::{
    error::{AgentError, Result},
    util::ClientId,
};
use crate::tcp_api::{AgentTcpWorkerApi, TcpCommand};

/// Commands from the agent that are passed down to the stealer worker, through [`TcpStealerApi`].
///
/// These are the operations that the agent receives from the layer to make the _steal_ feature
/// work.
#[derive(Debug)]
pub(crate) enum StealerCommand {
    /// Contains the channel that's used by the stealer worker to respond back to the agent
    /// (stealer -> agent -> layer).
    NewClient(Sender<DaemonTcp>),

    /// A layer wants to subscribe to this [`Port`].
    ///
    /// The agent starts stealing traffic on this [`Port`].
    PortSubscribe(Port),

    /// A layer wants to unsubscribe from this [`Port`].
    ///
    /// The agent stops stealing traffic from this [`Port`].
    PortUnsubscribe(Port),

    /// Part of the [`Drop`] implementation of [`TcpStealerApi`].
    ///
    /// Closes a layer connection, and unsubscribe its ports.
    ClientClose,

    /// A connection here is a pair of ([`ReadHalf`], [`WriteHalf`]) streams that are used to
    /// capture a remote connection (the connection we're stealing data from).
    ConnectionUnsubscribe(ConnectionId),

    /// There is new data in the direction going from the local process to the end-user (Going
    /// via the layer and the agent  local-process -> layer --> agent --> end-user).
    ///
    /// Agent forwards this data to the other side of original connection.
    ResponseData(TcpData),
}

impl TcpCommand for StealerCommand {
    fn get_new_client_command(message_sender: Sender<DaemonTcp>) -> Self {
        StealerCommand::NewClient(message_sender)
    }

    fn get_port_subscribe_command(port: Port) -> Self {
        StealerCommand::PortSubscribe(port)
    }

    fn get_port_unsubscribe_command(port: Port) -> Self {
        StealerCommand::PortUnsubscribe(port)
    }

    fn get_connection_unsubscribe_command(connection: ConnectionId) -> Self {
        StealerCommand::ConnectionUnsubscribe(connection)
    }

    fn get_client_close_command() -> Self {
        StealerCommand::ClientClose
    }
}

pub(crate) type TcpStealerApi = AgentTcpWorkerApi<StealerCommand>;

/// Add Stealer specific functions to AgentTcpWorkerApi
impl TcpStealerApi {
    /// Handles the conversion of [`LayerTcpSteal::TcpData`], that is passed from the
    /// agent, to an internal stealer command [`Command::ResponseData`].
    ///
    /// The actual handling of this message is done in [`TcpConnectionStealer`].
    pub(crate) async fn client_data(&mut self, tcp_data: TcpData) -> Result<(), AgentError> {
        self.send_command(StealerCommand::ResponseData(tcp_data)).await
    }
}