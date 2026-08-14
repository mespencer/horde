//! The blob transfer spine: `horde.v1.BlobService` over [`crate::blob`].
//!
//! Thin on purpose, like the other spines: the store owns verification and
//! layout, and this module owns only the translation between gRPC streams
//! and the store's chunked reads and writes. It serves on the main mTLS
//! listener behind the same enrolled-principal door as both other spines —
//! blobs move for Workers and Clients alike, so the service is
//! principal-agnostic rather than a copy on each spine.
//!
//! A server started without a state directory has no store; its blob verbs
//! answer `Unimplemented` honestly, the same choice the control plane makes
//! for enrollment verbs on an unsecured plane.

use std::sync::Arc;

use horde_proto::v1::blob_service_server::BlobService;
use horde_proto::v1::{
    BlobChunk, DownloadBlobRequest, DownloadBlobResponse, UploadBlobRequest, UploadBlobResponse,
    upload_blob_request,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::instrument;

use crate::blob::{BlobError, BlobStore, BlobWrite};
use horde_proto::blob::BlobDigest;

/// How a store refusal reaches the wire.
///
/// The code is the caller's retry policy: `InvalidArgument` means re-shape
/// the request, `NotFound` means the blob is not there to fetch,
/// `DataLoss` means the *store's* copy is bad and no retry will help.
impl From<BlobError> for Status {
    fn from(error: BlobError) -> Self {
        let message = error.to_string();
        match &error {
            BlobError::MalformedDigest { .. }
            | BlobError::DeclarationMismatch { .. }
            | BlobError::TooLarge { .. } => Status::invalid_argument(message),
            BlobError::NotFound { .. } => Status::not_found(message),
            BlobError::Corrupt { .. } => Status::data_loss(message),
            BlobError::Io { .. } => Status::internal(message),
        }
    }
}

/// Implements `horde.v1.BlobService`.
#[derive(Debug, Clone)]
pub struct Blobs {
    /// `None` on a server running without a state directory, where the
    /// verbs honestly answer `Unimplemented`.
    store: Option<Arc<BlobStore>>,
}

impl Blobs {
    #[must_use]
    pub fn new(store: Option<Arc<BlobStore>>) -> Self {
        Self { store }
    }

    fn store(&self) -> Result<&Arc<BlobStore>, Status> {
        self.store.as_ref().ok_or_else(|| {
            Status::unimplemented(
                "this server was started without a blob store (no state directory)",
            )
        })
    }
}

/// The digest a request named, parsed or refused with the request's words.
fn parsed_digest(address: &str) -> Result<BlobDigest, Status> {
    BlobDigest::parse(address).ok_or_else(|| {
        Status::from(BlobError::MalformedDigest {
            given: address.to_owned(),
        })
    })
}

/// Drains an upload's chunk frames into an open store write.
///
/// A second open frame, or an empty frame, is a protocol violation and is
/// refused rather than skipped — a client confused about framing should
/// hear so, not have its confusion papered over.
async fn drain_chunks(
    stream: &mut Streaming<UploadBlobRequest>,
    write: &mut BlobWrite,
) -> Result<(), Status> {
    while let Some(frame) = stream.message().await? {
        match frame.body {
            Some(upload_blob_request::Body::Chunk(chunk)) => write.chunk(&chunk.data).await?,
            Some(upload_blob_request::Body::Open(_)) => {
                return Err(Status::invalid_argument(
                    "an upload opens exactly once; a second open frame arrived mid-stream",
                ));
            }
            None => {
                return Err(Status::invalid_argument(
                    "an upload frame must carry an open or a chunk",
                ));
            }
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl BlobService for Blobs {
    #[instrument(skip_all)]
    async fn upload_blob(
        &self,
        request: Request<Streaming<UploadBlobRequest>>,
    ) -> Result<Response<UploadBlobResponse>, Status> {
        let store = Arc::clone(self.store()?);
        let mut stream = request.into_inner();

        let Some(UploadBlobRequest {
            body: Some(upload_blob_request::Body::Open(open)),
        }) = stream.message().await?
        else {
            return Err(Status::invalid_argument(
                "an upload's first frame must be the open frame declaring digest and size",
            ));
        };
        let digest = parsed_digest(&open.blob_digest)?;

        let mut write = store.begin(&digest, open.size_bytes).await?;
        drain_chunks(&mut stream, &mut write).await?;
        let stored = write.finish().await?;

        Ok(Response::new(UploadBlobResponse {
            blob_digest: stored.digest.to_string(),
            size_bytes: stored.size_bytes,
        }))
    }

    type DownloadBlobStream = ReceiverStream<Result<DownloadBlobResponse, Status>>;

    #[instrument(skip_all)]
    async fn download_blob(
        &self,
        request: Request<DownloadBlobRequest>,
    ) -> Result<Response<Self::DownloadBlobStream>, Status> {
        let store = Arc::clone(self.store()?);
        let digest = parsed_digest(&request.into_inner().blob_digest)?;

        // Opened before the stream exists, so "no such blob" is the call's
        // own refusal rather than a broken stream the caller must interpret.
        let mut read = store.read(&digest).await?;

        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        tokio::spawn(async move {
            loop {
                match read.chunk().await {
                    Ok(Some(chunk)) => {
                        let frame = DownloadBlobResponse {
                            chunk: Some(BlobChunk { data: chunk }),
                        };
                        if sender.send(Ok(frame)).await.is_err() {
                            return; // The caller hung up; nothing owed.
                        }
                    }
                    // The verified end of the blob.
                    Ok(None) => return,
                    // Including Corrupt: the stream must *fail*, because a
                    // clean end here would hand the caller bytes that do
                    // not match the address it asked for.
                    Err(error) => {
                        let _ = sender.send(Err(Status::from(error))).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

#[cfg(test)]
mod tests {
    use super::{Blobs, parsed_digest};
    use crate::blob::BlobError;
    use tonic::Code;

    /// The full mapping, both directions of each choice visible: every
    /// refusal keeps its meaning on the wire, because the code *is* the
    /// caller's retry policy.
    #[test]
    fn every_store_refusal_keeps_its_meaning_as_a_status_code() {
        for (error, code) in [
            (
                BlobError::MalformedDigest {
                    given: "not-a-digest".to_owned(),
                },
                Code::InvalidArgument,
            ),
            (
                BlobError::NotFound {
                    digest: "sha256:00".to_owned(),
                },
                Code::NotFound,
            ),
            (
                BlobError::Corrupt {
                    expected: "sha256:aa".to_owned(),
                    observed: "sha256:bb".to_owned(),
                    observed_size: 7,
                },
                Code::DataLoss,
            ),
            (
                BlobError::DeclarationMismatch {
                    declared: "sha256:aa at 4 bytes".to_owned(),
                    arrived: "hash to sha256:bb and count 5".to_owned(),
                },
                Code::InvalidArgument,
            ),
            (
                BlobError::TooLarge {
                    limit: 8,
                    observed: 9,
                },
                Code::InvalidArgument,
            ),
            (
                BlobError::Io {
                    context: "testing",
                    source: std::io::Error::other("disk fell off"),
                },
                Code::Internal,
            ),
        ] {
            let rendered = error.to_string();
            let status = tonic::Status::from(error);
            assert_eq!(status.code(), code, "{rendered}");
            assert_eq!(status.message(), rendered, "the words must survive");
        }
    }

    #[test]
    fn a_storeless_server_answers_unimplemented_rather_than_pretending() {
        let refused = Blobs::new(None).store().expect_err("must refuse");
        assert_eq!(refused.code(), Code::Unimplemented);
        assert!(refused.message().contains("blob store"));
    }

    #[test]
    fn a_malformed_address_is_refused_naming_what_was_given() {
        let refused = parsed_digest("sha256:tooshort").expect_err("must refuse");
        assert_eq!(refused.code(), Code::InvalidArgument);
        assert!(refused.message().contains("sha256:tooshort"));
    }
}
