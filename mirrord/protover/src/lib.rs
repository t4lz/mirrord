use actix_codec::{Framed, FramedParts};
use futures::{stream::StreamExt, SinkExt};
use mirrord_protocol::{VersionCodec, VersionSupportAnnouncement};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Error)]
pub enum ProtoVerError {
    #[error("Supported version ranges are disjoint. This side supports versions {} to {} inclusive, peer supports {} to {} inclusive.", local_range.min, local_range.max, peer_range.min, peer_range.max)]
    NoCommonVersion {
        local_range: VersionSupportAnnouncement,
        peer_range: VersionSupportAnnouncement,
    },

    #[error("Sending version negotiation message failed: {0}")]
    SendFailed(#[from] std::io::Error),

    #[error("Receiving version negotiation message failed: {0}")]
    ReceiveFailed(std::io::Error),
}

pub(crate) type Result<T, E = ProtoVerError> = std::result::Result<T, E>;

/// Take the raw tcp stream, determine the highest common version with the other side, return
/// framed stream with given codec.
pub async fn determine_version<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    stream: S,
) -> Result<(Framed<S, VersionCodec>, mirrord_protocol::codec::Version)> {
    let mut stream = Framed::new(stream, VersionCodec::default());
    let local_range = VersionSupportAnnouncement { min: 0, max: 2 }; // TODO: real version
    stream.send(local_range).await?;

    // TODO: timeout!
    let version = match stream.next().await {
        None => todo!(),
        Some(Ok(peer_range)) => local_range.latest_common(&peer_range).ok_or_else(|| {
            ProtoVerError::NoCommonVersion {
                local_range,
                peer_range,
            }
        })?,
        Some(Err(err)) => Err(ProtoVerError::ReceiveFailed(err))?,
    };

    Ok((stream, version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
