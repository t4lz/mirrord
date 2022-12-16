use thiserror::Error;

use super::{CapturedRequest, PassthroughRequest};
use crate::util::ClientId;

/// Errors specific to the HTTP traffic feature.
#[derive(Error, Debug)]
pub(crate) enum HttpTrafficError {
    #[error("Failed parsing HTTP with 0 bytes!")]
    Empty,

    #[error("Failed client not found `{0}`!")]
    ClientNotFound(ClientId),

    #[error("Failed parsing HTTP smaller than minimal!")]
    TooSmall,

    #[error("Failed as the buffer does not contain a valid HTTP request!")]
    NotHttp,

    #[error("Failed with IO `{0}`!")]
    IO(#[from] std::io::Error),

    #[error("Failed with Parse `{0}`!")]
    Parse(#[from] httparse::Error),

    #[error("Failed with Hyper `{0}`!")]
    Hyper(#[from] hyper::Error),

    #[error("Failed with Captured `{0}`!")]
    CapturedSender(#[from] tokio::sync::mpsc::error::SendError<CapturedRequest>),

    #[error("Failed with Passthrough `{0}`!")]
    PassthroughSender(#[from] tokio::sync::mpsc::error::SendError<PassthroughRequest>),
}
