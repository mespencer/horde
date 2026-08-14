//! The attachment seam against a real served Blob Store: stashing chooses
//! the carrier, fetching verifies, materializing writes files — with the
//! bytes genuinely crossing a server, because the seam's whole job is the
//! half that cannot be faked in-process.

use horde_proto::blob::{BlobDigest, INLINE_THRESHOLD_BYTES};
use horde_server_testkit::secure::within;
use horde_server_testkit::{Setup, serve_with_blob_store};
use horde_worker::AttachmentBroker;

async fn broker_against_store(
    blob_root: &std::path::Path,
) -> Setup<(AttachmentBroker, tonic::transport::Channel)> {
    let endpoint = serve_with_blob_store(blob_root).await?;
    let channel = tonic::transport::Endpoint::try_from(endpoint)?
        .connect()
        .await?;
    Ok((AttachmentBroker::new(Some(channel.clone())), channel))
}

/// The auto-detect seam, over the wire: content past the threshold is
/// uploaded and travels as a reference, and fetching it brings the same
/// bytes back through the store, verified.
#[tokio::test]
async fn large_content_is_stashed_to_the_store_and_fetched_back_verified() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let (broker, _channel) = broker_against_store(blob_root.path()).await?;

    let content = vec![9u8; usize::try_from(INLINE_THRESHOLD_BYTES)? + 1];
    let attachment = within(
        "the stash",
        broker.stash("profile.trace", "application/octet-stream", content.clone()),
    )
    .await??;

    assert!(
        attachment.inline.is_empty(),
        "past the threshold, the event carries a reference, never the bytes"
    );
    assert_eq!(attachment.blob_digest, BlobDigest::of(&content).to_string());
    assert_eq!(attachment.size_bytes, content.len() as u64);

    // The reference resolves: the store really holds the bytes.
    let fetched = within("the fetch", broker.fetch(&attachment)).await??;
    assert_eq!(fetched, content);
    Ok(())
}

/// Materialization handles both carriers in one payload, and re-delivery
/// overwrites idempotently rather than erroring.
#[tokio::test]
async fn a_mixed_payload_materializes_both_carriers_as_files() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let (broker, _channel) = broker_against_store(blob_root.path()).await?;

    let small = b"four lines of log".to_vec();
    let large = vec![5u8; usize::try_from(INLINE_THRESHOLD_BYTES)? + 100];
    let attachments = vec![
        within(
            "the small stash",
            broker.stash("small.log", "", small.clone()),
        )
        .await??,
        within(
            "the large stash",
            broker.stash("big.bin", "", large.clone()),
        )
        .await??,
    ];

    let checkout = tempfile::tempdir()?;
    let written = within(
        "the materialization",
        broker.materialize(&attachments, checkout.path()),
    )
    .await??;
    assert_eq!(written.len(), 2);
    assert_eq!(std::fs::read(&written[0])?, small);
    assert_eq!(std::fs::read(&written[1])?, large);
    for path in &written {
        assert!(
            path.starts_with(checkout.path().join(horde_worker::blob::ATTACHMENTS_DIR)),
            "attachments land under the attachments dir, outside every repo: {}",
            path.display()
        );
    }

    // Redelivered: same files, same content, no error.
    let again = within(
        "the redelivery",
        broker.materialize(&attachments, checkout.path()),
    )
    .await??;
    assert_eq!(again, written);
    Ok(())
}

/// A reference whose blob the store does not hold fails the fetch loudly —
/// the message named work the receiver cannot see, and inventing bytes or
/// writing an empty file would both be silent drops.
#[tokio::test]
async fn a_dangling_reference_fails_the_fetch() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let (broker, _channel) = broker_against_store(blob_root.path()).await?;

    let dangling = horde_proto::v1::Attachment {
        name: "gone.bin".to_owned(),
        blob_digest: BlobDigest::of(b"never uploaded").to_string(),
        size_bytes: 14,
        media_type: String::new(),
        inline: Vec::new(),
    };
    let error = within("the refusal", broker.fetch(&dangling))
        .await?
        .expect_err("a dangling reference must refuse");
    assert!(
        error.to_string().contains("no blob"),
        "the refusal says the store lacks it: {error}"
    );
    Ok(())
}
