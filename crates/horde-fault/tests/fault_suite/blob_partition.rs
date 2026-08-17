//! Scenario (e): a partition lands in the middle of a blob transfer.
//!
//! The claim under attack is wave 6's: "integrity verified on upload and on
//! every read" (DESIGN.md §8). The injector's byte budget severs the stream
//! a known number of bytes in — deterministically inside the multi-chunk
//! transfer — and the recovery asserted is that *no torn blob exists
//! anywhere*: the upload fails loudly, the store neither serves nor keeps a
//! prefix, the interrupted download refuses to hand back short bytes, and a
//! retry over the healed link moves the same content intact.

use std::sync::Arc;

use horde_proto::blob::{BlobDigest, download_blob, upload_blob};
use horde_proto::v1::blob_service_client::BlobServiceClient;
use horde_server::EventLog;
use horde_server::blob::BlobStore;
use horde_server::service::ServerCore;
use horde_testkit::FaultProxy;

use crate::support;

/// Four transfer chunks and one open frame: enough that a low byte budget
/// is guaranteed to sever mid-stream, never after the last frame.
const BLOB_LEN: usize = 4 * 1024 * 1024;

/// Where the store keeps one digest's bytes: `sha256/<aa>/<62 hex>` — the
/// layout `horde-server`'s own suite asserts against.
fn stored_path(root: &std::path::Path, digest: &BlobDigest) -> std::path::PathBuf {
    let (fan, rest) = digest.hex().split_at(2);
    root.join("sha256").join(fan).join(rest)
}

async fn blob_client(endpoint: &str) -> BlobServiceClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::try_from(endpoint.to_owned())
        .expect("the endpoint parses")
        .connect()
        .await
        .expect("the channel connects");
    BlobServiceClient::new(channel)
}

#[tokio::test]
async fn a_partition_mid_transfer_leaves_no_torn_blob_on_either_side() {
    let blob_root = tempfile::tempdir().expect("a tempdir");
    let store = BlobStore::open(blob_root.path()).expect("the store opens");
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"))
        .with_blob_store(Arc::new(store));
    let upstream = horde_server_testkit::serve_core(core)
        .await
        .expect("the core serves");
    let proxy = FaultProxy::front(&upstream).await.expect("the proxy binds");

    let content = support::patterned_bytes(BLOB_LEN);
    let digest = BlobDigest::of(&content);

    // — Upload, severed mid-stream by byte budget. —
    proxy.cut_after(1_500_000); // Inside chunk two of four.
    let mut client = blob_client(&proxy.endpoint()).await;
    let torn = support::within(
        "the severed upload surfaces its failure",
        upload_blob(&mut client, &content),
    )
    .await;
    assert!(
        torn.is_err(),
        "an upload whose stream died must fail, not report success: {torn:?}"
    );

    // The store accepted nothing: no file where the digest would live, and
    // a fetch over the healed link says NOT_FOUND rather than serving a
    // prefix that happens to exist somewhere.
    proxy.heal();
    assert!(
        !stored_path(blob_root.path(), &digest).exists(),
        "a torn upload must leave no file at the digest's address"
    );
    let mut client = blob_client(&proxy.endpoint()).await;
    let absent = support::within(
        "the store answers about the torn digest",
        download_blob(&mut client, &digest),
    )
    .await;
    assert_eq!(
        absent.err().map(|status| status.code()),
        Some(tonic::Code::NotFound),
        "a blob whose upload tore must not exist to readers"
    );

    // — Retry over the healed link: the same bytes, intact this time. —
    let stored = support::within("the retried upload", upload_blob(&mut client, &content))
        .await
        .expect("the healed link carries the whole blob");
    assert_eq!(stored.blob_digest, digest.to_string());
    assert!(
        stored_path(blob_root.path(), &digest).is_file(),
        "the verified blob lands at its address"
    );

    // — Download, severed mid-stream. The receiver must refuse short bytes. —
    proxy.cut_after(1_500_000);
    let mut client = blob_client(&proxy.endpoint()).await;
    let short = support::within(
        "the severed download surfaces its failure",
        download_blob(&mut client, &digest),
    )
    .await;
    assert!(
        short.is_err(),
        "a download whose stream died must refuse what arrived: {short:?}"
    );

    // — And the recovery: the healed link hands back the exact bytes. —
    proxy.heal();
    let mut client = blob_client(&proxy.endpoint()).await;
    let recovered = support::within("the healed download", download_blob(&mut client, &digest))
        .await
        .expect("the healed link serves the blob");
    assert_eq!(
        recovered, content,
        "what survives the partition is the content, exactly"
    );
}
