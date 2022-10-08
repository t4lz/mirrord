use std::{collections::HashMap, path::PathBuf, process::Stdio, time::Duration};

use actix_codec::Framed;
use futures::{stream::StreamExt, SinkExt};
use mirrord_protocol::{
    tcp::{DaemonTcp, LayerTcp, NewTcpConnection, TcpClose, TcpData},
    ClientMessage, DaemonCodec, DaemonMessage,
};
use rstest::{fixture, rstest};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{ChildStdout, Command},
};

struct LayerConnection {
    codec: Framed<TcpStream, DaemonCodec>,
    num_connections: u64,
}

impl LayerConnection {
    /// Accept a connection from the libraries and verify the first message it is supposed to send
    /// to the agent - GetEnvVarsRequest. Send back a response.
    /// Return the codec of the accepted stream.
    async fn accept_library_connection(listener: &TcpListener) -> Framed<TcpStream, DaemonCodec> {
        let (stream, _) = listener.accept().await.unwrap();
        println!("Got connection from library.");
        let mut codec = Framed::new(stream, DaemonCodec::new());
        let msg = codec.next().await.unwrap().unwrap();
        println!("Got first message from library.");
        if let ClientMessage::GetEnvVarsRequest(request) = msg {
            assert!(request.env_vars_filter.is_empty());
            assert_eq!(request.env_vars_select.len(), 1);
            assert!(request.env_vars_select.contains("*"));
        } else {
            panic!("unexpected request {:?}", msg)
        }
        codec
            .send(DaemonMessage::GetEnvVarsResponse(Ok(HashMap::new())))
            .await
            .unwrap();
        codec
    }

    /// Accept the library's connection and verify initial ENV message and PortSubscribe message
    /// caused by the listen hook.
    /// Handle flask's 2 process behaviour.
    async fn get_initialized_connection(listener: &TcpListener) -> LayerConnection {
        let mut codec = Self::accept_library_connection(listener).await;
        let msg = match codec.next().await {
            Some(option) => option.unwrap(),
            None => {
                // Python runs in 2 processes, only one of which is the application. The library is
                // loaded into both so the first connection will not contain the application and
                // so will not send any of the messages that are generated by the hooks that are
                // triggered by the app.
                // So accept the next connection which will be the one by the library that was
                // loaded to the python process that actually runs the application.
                codec = Self::accept_library_connection(&listener).await;
                codec.next().await.unwrap().unwrap()
            }
        };
        assert_eq!(msg, ClientMessage::Tcp(LayerTcp::PortSubscribe(80)));
        LayerConnection {
            codec,
            num_connections: 0,
        }
    }

    /// Send the layer a message telling it the target got a new incoming connection.
    /// There is no such actual connection, because there is no target, but the layer should start
    /// a mirror connection with the application.
    /// Return the id of the new connection.
    async fn send_new_connection(&mut self) -> u64 {
        let new_connection_id = self.num_connections;
        self.codec
            .send(DaemonMessage::Tcp(DaemonTcp::NewConnection(
                NewTcpConnection {
                    connection_id: new_connection_id,
                    address: "127.0.0.1".parse().unwrap(),
                    destination_port: "80".parse().unwrap(),
                    source_port: "31415".parse().unwrap(),
                },
            )))
            .await
            .unwrap();
        self.num_connections += 1;
        new_connection_id
    }

    async fn send_tcp_data(&mut self, message_data: &str, connection_id: u64) {
        self.codec
            .send(DaemonMessage::Tcp(DaemonTcp::Data(TcpData {
                connection_id,
                bytes: Vec::from(message_data),
            })))
            .await
            .unwrap();
    }

    /// Send the layer a message telling it the target got a new incoming connection.
    /// There is no such actual connection, because there is no target, but the layer should start
    /// a mirror connection with the application.
    /// Return the id of the new connection.
    async fn send_close(&mut self, connection_id: u64) {
        self.codec
            .send(DaemonMessage::Tcp(DaemonTcp::Close(TcpClose {
                connection_id,
            })))
            .await
            .unwrap();
    }

    /// Tell the layer there is a new incoming connection, then send data "from that connection".
    async fn send_connection_then_data(&mut self, message_data: &str) {
        let new_connection_id = self.send_new_connection().await;
        self.send_tcp_data(message_data, new_connection_id).await;
        self.send_close(new_connection_id).await;
    }
}

#[derive(Debug)]
enum Application {
    // TODO: add more applications.
    PythonFlaskHTTP,
    PythonFastApiHTTP,
    NodeHTTP,
    Go19HTTP,
}

impl Application {
    /// Run python with shell resolving to find the actual executable.
    ///
    /// This is to help tests that run python with mirrord work locally on systems with pyenv.
    /// If we run `python3` on a system with pyenv the first executed is not python but bash. On mac
    /// that prevents the layer from loading because of SIP.
    async fn get_python3_executable() -> String {
        let mut python = Command::new("python3")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let child_stdin = python.stdin.as_mut().unwrap();
        child_stdin
            .write_all(b"import sys\nprint(sys.executable)")
            .await
            .unwrap();
        let output = python.wait_with_output().await.unwrap();
        String::from(String::from_utf8_lossy(&output.stdout).trim())
    }

    async fn get_executable(&self) -> String {
        match self {
            Application::PythonFlaskHTTP => Self::get_python3_executable().await,
            Application::PythonFastApiHTTP => String::from("uvicorn"),
            Application::NodeHTTP => String::from("node"),
            Application::Go19HTTP => String::from("tests/apps/app_go/19"),
        }
    }

    fn get_args(&self) -> Vec<String> {
        let mut app_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        app_path.push("tests/apps/");
        match self {
            Application::PythonFlaskHTTP => {
                app_path.push("app_flask.py");
                println!("using flask server from {:?}", app_path);
                vec![String::from("-u"), app_path.to_string_lossy().to_string()]
            }
            Application::PythonFastApiHTTP => vec![
                String::from("--port=80"),
                String::from("--host=0.0.0.0"),
                String::from("--app-dir=tests/apps/"),
                String::from("app_fastapi:app"),
            ],
            Application::NodeHTTP => {
                app_path.push("app_node.js");
                vec![app_path.to_string_lossy().to_string()]
            }
            Application::Go19HTTP => vec![],
        }
    }
}

/// Return the path to the existing layer lib, or build it first and return the path, according to
/// whether the environment variable MIRRORD_TEST_USE_EXISTING_LIB is set.
/// When testing locally the lib should be rebuilt on each run so that when developers make changes
/// they don't have to also manually build the lib before running the tests.
/// Building is slow on the CI though, so the CI can set the env var and use an artifact of an
/// earlier job on the same run (there are no code changes in between).
#[fixture]
#[once]
fn dylib_path() -> PathBuf {
    match std::env::var("MIRRORD_TEST_USE_EXISTING_LIB") {
        Ok(path) => {
            let dylib_path = PathBuf::from(path);
            println!("Using existing layer lib from: {:?}", dylib_path);
            assert!(dylib_path.exists());
            dylib_path
        }
        Err(_) => {
            let dylib_path = test_cdylib::build_current_project();
            println!("Built library at {:?}", dylib_path);
            dylib_path
        }
    }
}

/// Start a web server injected with the layer, simulate the agent, verify expected messages from
/// the layer, send tcp messages and verify in the server output that the application received them.
/// Tests the layer's communication with the agent, the bind hook, and the forwarding of mirrored
/// traffic to the application.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[timeout(Duration::from_secs(20))]
async fn test_mirroring_with_http(
    #[values(
        Application::PythonFlaskHTTP,
        Application::PythonFastApiHTTP,
        Application::NodeHTTP,
        Application::Go19HTTP
    )]
    application: Application, // TODO: add more apps.
    dylib_path: &PathBuf,
) {
    let mut env = HashMap::new();
    // get_executable must be called before the setting env, because in order to determine the
    // Python executable it starts a new process, and we don't want the lib to be loaded into that.
    let executable = application.get_executable().await; // Own it.
    println!("Using executable: {}", &executable);
    env.insert("RUST_LOG", "warn,mirrord=debug");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    println!("Listening for messages from the layer on {addr}");
    env.insert("MIRRORD_IMPERSONATED_TARGET", "mock-target"); // Just pass some value.
    env.insert("MIRRORD_CONNECT_TCP", &addr);
    env.insert("MIRRORD_REMOTE_DNS", "false");
    env.insert("MIRRORD_FILE_OPS", "false");
    env.insert("DYLD_INSERT_LIBRARIES", dylib_path.to_str().unwrap());
    env.insert("LD_PRELOAD", dylib_path.to_str().unwrap());
    let server = Command::new(executable)
        .args(application.get_args())
        .envs(env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    println!("Started application.");

    // Accept the connection from the layer and verify initial messages.
    let mut layer_connection = LayerConnection::get_initialized_connection(&listener).await;
    println!("Application subscribed to port, sending tcp messages.");

    layer_connection
        .send_connection_then_data("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await;
    layer_connection
        .send_connection_then_data("POST / HTTP/1.1\r\nHost: localhost\r\n\r\npost-data")
        .await;
    layer_connection
        .send_connection_then_data("PUT / HTTP/1.1\r\nHost: localhost\r\n\r\nput-data")
        .await;
    layer_connection
        .send_connection_then_data("DELETE / HTTP/1.1\r\nHost: localhost\r\n\r\ndelete-data")
        .await;

    let output = server.wait_with_output().await.unwrap();
    let stdout_str = String::from_utf8_lossy(&output.stdout).to_string();
    println!("{stdout_str}");
    assert!(stdout_str.contains("GET: Request completed"));
    assert!(stdout_str.contains("POST: Request completed"));
    assert!(stdout_str.contains("PUT: Request completed"));
    assert!(stdout_str.contains("DELETE: Request completed"));
    assert!(!&stdout_str.to_lowercase().contains("error"));
    let stderr_str = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(!&stderr_str.to_lowercase().contains("error"));
}
