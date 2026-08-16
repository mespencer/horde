//! The blob verbs, driven through `cli::run` against a real served store.
//!
//! Asserted by effect, not by `Ok`: a stash is proven by the store handing
//! the bytes back, a fetch by the verified file on disk — the same
//! discipline `verbs.rs` learned from mutation testing.

mod common;

use common::{Setup, invoke};
use horde_proto::blob::BlobDigest;
use horde_server_testkit::serve_with_blob_store;

#[tokio::test]
async fn stash_then_fetch_round_trips_a_file_through_the_store() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let endpoint = serve_with_blob_store(blob_root.path()).await?;

    let work = tempfile::tempdir()?;
    let input = work.path().join("build.log");
    let content = b"error[E0308]: mismatched types, forty screens of it".to_vec();
    std::fs::write(&input, &content)?;

    invoke(&endpoint, &["blob", "stash", &input.to_string_lossy()]).await?;

    // The stash's effect: the store holds the bytes under their address.
    let address = BlobDigest::of(&content).to_string();
    let out = work.path().join("fetched.log");
    invoke(
        &endpoint,
        &["blob", "fetch", &address, "--out", &out.to_string_lossy()],
    )
    .await?;
    assert_eq!(
        std::fs::read(&out)?,
        content,
        "the fetched file is the stashed bytes, verified against the address"
    );
    Ok(())
}

#[tokio::test]
async fn a_malformed_address_is_a_local_complaint_before_any_dial() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let endpoint = serve_with_blob_store(blob_root.path()).await?;
    let mut client = horde_cli::Client::connect(&endpoint).await?;

    let error = client
        .fetch_blob("sha256:nope")
        .await
        .expect_err("a malformed address must be refused locally");
    assert!(
        matches!(&error, horde_cli::ClientError::BlobAddress { given } if given == "sha256:nope"),
        "got {error}"
    );
    Ok(())
}

/// A server without a store says so in the vocabulary the CLI already
/// distinguishes: `Unimplemented` is a roadmap fact, not a fault.
#[tokio::test]
async fn a_storeless_server_reads_as_unimplemented() -> Setup<()> {
    let endpoint = horde_server_testkit::serve().await?;
    let mut client = horde_cli::Client::connect(&endpoint).await?;

    let error = client
        .stash_blob(b"anything")
        .await
        .expect_err("no store, no stash");
    assert!(error.is_unimplemented(), "got {error}");
    Ok(())
}
