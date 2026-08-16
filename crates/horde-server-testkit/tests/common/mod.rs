//! What both testkit suites share: the blob round-trip observation.
//!
//! Each test binary compiles its own copy of this module; the allow is the
//! conventional shape for an integration-test `common` module.
#![allow(dead_code, unreachable_pub)]

use horde_proto::v1::blob_service_client::BlobServiceClient;

type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Uploads `content` through `blobs`, then holds the store to its promises
/// the way a wiring mutant cannot fake: the blob is a real file under
/// `blob_root` (so the store really is rooted where the caller said), and
/// the verified download hands the same bytes back.
pub async fn assert_blob_lands_under_root(
    blobs: &mut BlobServiceClient<tonic::transport::Channel>,
    blob_root: &std::path::Path,
    content: &[u8],
) -> Setup<()> {
    let stored = horde_proto::blob::upload_blob(blobs, content).await?;
    let digest = horde_proto::blob::BlobDigest::parse(&stored.blob_digest)
        .ok_or("the answer must be a well-formed address")?;

    let (fan, rest) = digest.hex().split_at(2);
    if !blob_root.join("sha256").join(fan).join(rest).is_file() {
        return Err("the blob must land under the directory the caller rooted the store at".into());
    }
    let fetched = horde_proto::blob::download_blob(blobs, &digest).await?;
    if fetched != content {
        return Err("the verified download must hand back the uploaded bytes".into());
    }
    Ok(())
}
