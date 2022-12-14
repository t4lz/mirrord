use core::{future::Future, pin::Pin};
use std::sync::Arc;

use dashmap::DashMap;
use fancy_regex::Regex;
use futures::TryFutureExt;
use hyper::{body::Incoming, service::Service, Request, Response};
use tokio::sync::mpsc::Sender;

use super::{error::HttpTrafficError, CapturedRequest, PassthroughRequest};
use crate::util::ClientId;

pub(super) struct HyperHandler {
    pub(super) filters: Arc<DashMap<ClientId, Regex>>,
    pub(super) captured_tx: Sender<CapturedRequest>,
    pub(super) passthrough_tx: Sender<PassthroughRequest>,
}

// TODO(alex) [low] 2022-12-13: Come back to these docs to create a link to where this is in the
// agent.
/// Creates a task to send a message (either [`CapturedRequest`] or [`PassthroughRequest`]) to the
/// receiving end that lives in the stealer.
///
/// As the [`hyper::service::Service`] trait doesn't support `async fn` for the [`Service::call`]
/// method, we use this helper function that allows us to send a `value: T` via a `Sender<T>`
/// without the need to call `await`.
fn spawn_send<T>(value: T, tx: Sender<T>)
where
    T: Send + 'static,
    HttpTrafficError: From<tokio::sync::mpsc::error::SendError<T>>,
{
    tokio::spawn(async move {
        println!(
            "Yay we sent the request {:#?}",
            tx.send(value).map_err(HttpTrafficError::from).await
        );
    });
}

impl Service<Request<Incoming>> for HyperHandler {
    type Response = Response<String>;

    type Error = HttpTrafficError;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        println!("hyper request \n{:#?}", request);

        // TODO(alex) [mid] 2022-12-13: Do we care at all about what is sent from here as a
        // response to our client duplex stream?
        let response = async { Ok(Response::new("async is love".to_string())) };

        if let Some(client_id) = request
            .headers()
            .iter()
            .map(|(header_name, header_value)| {
                format!("{}={}", header_name, header_value.to_str().unwrap())
            })
            .find_map(|header| {
                self.filters.iter().find_map(|filter| {
                    if filter.is_match(&header).unwrap() {
                        Some(filter.key().clone())
                    } else {
                        None
                    }
                })
            })
        {
            println!("We have a captured request {:#?}", request);
            spawn_send(
                CapturedRequest { client_id, request },
                self.captured_tx.clone(),
            );

            Box::pin(response)
        } else {
            println!("This is a passthrough {:#?}", request);
            spawn_send(PassthroughRequest(request), self.passthrough_tx.clone());

            Box::pin(response)
        }
    }
}
