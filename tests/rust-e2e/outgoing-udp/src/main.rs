/// The code in this test was copied and adapted from the quinn examples on the quinn repo:
/// https://github.com/quinn-rs/quinn/tree/main/quinn/examples
use std::{env, error::Error, net::SocketAddr, sync::Arc};

use quinn::{Endpoint, Incoming, ServerConfig};
use futures_util::stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    let port = &args[1];

    println!(">> test_outgoing_quic");
    let addr = format!("127.0.0.1:{port}");
    let addr = addr.parse().unwrap();
    run_server(addr).await;
    Ok(())
}

/// Runs a QUIC server bound to given address.
async fn run_server(addr: SocketAddr) {
    let (mut incoming, _server_cert) = make_server_endpoint(addr).unwrap();
    // accept a single connection
    println!("Listening on {:#?}.", addr);
    let incoming_conn = incoming.next().await.unwrap();
    println!("Incoming {:#?}", incoming_conn);
    let new_conn = incoming_conn.await.unwrap();
    println!(
        "[server] connection accepted: addr={}",
        new_conn.connection.remote_address()
    );
}

/// Constructs a QUIC endpoint configured to listen for incoming connections on a certain address
/// and port.
///
/// ## Returns
///
/// - a stream of incoming QUIC connections
/// - server certificate serialized into DER format
#[allow(unused)]
pub fn make_server_endpoint(bind_addr: SocketAddr) -> Result<(Incoming, Vec<u8>), Box<dyn Error>> {
    let (server_config, server_cert) = configure_server()?;
    let (_endpoint, incoming) = Endpoint::server(server_config, bind_addr)?;
    Ok((incoming, server_cert))
}

/// Returns default server configuration along with its certificate.
#[allow(clippy::field_reassign_with_default)] // https://github.com/rust-lang/rust-clippy/issues/6527
fn configure_server() -> Result<(ServerConfig, Vec<u8>), Box<dyn Error>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = cert.serialize_private_key_der();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];

    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key)?;
    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .max_concurrent_uni_streams(0_u8.into());

    Ok((server_config, cert_der))
}

#[allow(unused)]
pub const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];