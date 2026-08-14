//! The blob path, end to end: transfer through the real `BlobService`, the
//! log's refusal to carry bulk, and the authenticated channel (DESIGN §8).
//!
//! Everything here runs against a served server — the testkit's loopback
//! harness, or the secured wave-5 shape for the mTLS suite — because the
//! properties under test (streaming framing, status codes crossing the
//! wire, the interceptor door) only exist across a real transport. The
//! store's own behavior is unit-tested where it lives.

use horde_proto::blob::{BlobDigest, INLINE_THRESHOLD_BYTES, download_blob, upload_blob};
use horde_proto::v1::blob_service_client::BlobServiceClient;
use horde_proto::v1::control_plane_service_client::ControlPlaneServiceClient;
use horde_proto::v1::{
    AgentId, AgentResult, Assignment, Attachment, DownloadBlobRequest, Event, Payload,
    ResultReported, SendMessageRequest, UploadBlobOpen, UploadBlobRequest, event::Kind,
    send_message_request, upload_blob_request,
};
use horde_server_testkit::secure::{serve_secured_with_blob_store, within};
use horde_server_testkit::{Setup, journal_event, seeded, serve, serve_with_blob_store};

/// An attachment carrying `content` inline, addressed honestly.
fn inline_attachment(name: &str, content: &[u8]) -> Attachment {
    Attachment {
        name: name.to_owned(),
        blob_digest: BlobDigest::of(content).to_string(),
        size_bytes: content.len() as u64,
        media_type: "text/plain".to_owned(),
        inline: content.to_vec(),
    }
}

/// A `SendMessage` assignment to `agent_id` carrying one attachment.
fn assignment_carrying(agent_id: &str, attachment: Attachment) -> SendMessageRequest {
    SendMessageRequest {
        to_agent_id: Some(AgentId::new(agent_id)),
        body: Some(send_message_request::Body::Assignment(Assignment {
            assignment_id: None,
            text: "see the attachment".to_owned(),
            payload: Some(Payload {
                git_refs: Vec::new(),
                attachments: vec![attachment],
            }),
        })),
        delivery: 0,
        idempotency_key: String::new(),
    }
}

#[tokio::test]
async fn a_blob_round_trips_through_the_service_and_reuploads_idempotently() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let endpoint = serve_with_blob_store(blob_root.path()).await?;
    let mut client = BlobServiceClient::connect(endpoint).await?;

    // Big enough to cross several chunks, odd-sized so the tail chunk is
    // partial.
    let content = vec![0xA7u8; 2 * 1024 * 1024 + 333];
    let stored = within("the upload", upload_blob(&mut client, &content)).await??;
    assert_eq!(stored.blob_digest, BlobDigest::of(&content).to_string());
    assert_eq!(stored.size_bytes, content.len() as u64);

    let digest = BlobDigest::parse(&stored.blob_digest).ok_or("the answer parses")?;
    let fetched = within("the download", download_blob(&mut client, &digest)).await??;
    assert_eq!(fetched, content, "what went in must come out, verified");

    // Content addressing makes the second upload a no-op with the same
    // answer, not a conflict.
    let again = within("the re-upload", upload_blob(&mut client, &content)).await??;
    assert_eq!(again, stored);
    Ok(())
}

#[tokio::test]
async fn downloading_an_absent_blob_is_not_found() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let endpoint = serve_with_blob_store(blob_root.path()).await?;
    let mut client = BlobServiceClient::connect(endpoint).await?;

    let absent = BlobDigest::of(b"never uploaded");
    let status = within("the refusal", download_blob(&mut client, &absent))
        .await?
        .expect_err("an absent blob must refuse, not hang or invent");
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains(&absent.to_string()));
    Ok(())
}

#[tokio::test]
async fn a_malformed_address_is_refused_before_anything_streams() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let endpoint = serve_with_blob_store(blob_root.path()).await?;
    let mut client = BlobServiceClient::connect(endpoint).await?;

    let status = within(
        "the refusal",
        client.download_blob(DownloadBlobRequest {
            blob_digest: "sha256:../../escape".to_owned(),
        }),
    )
    .await?
    .expect_err("a malformed address must be refused at the door");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains("sha256:../../escape"));
    Ok(())
}

/// The integrity refusal on upload: declared digest and arrived bytes
/// disagree, so nothing may be stored under either name.
#[tokio::test]
async fn an_upload_lying_about_its_digest_is_refused_and_stores_nothing() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let endpoint = serve_with_blob_store(blob_root.path()).await?;
    let mut client = BlobServiceClient::connect(endpoint).await?;

    let declared = BlobDigest::of(b"what was promised");
    let frames = vec![
        UploadBlobRequest {
            body: Some(upload_blob_request::Body::Open(UploadBlobOpen {
                blob_digest: declared.to_string(),
                size_bytes: 15,
            })),
        },
        UploadBlobRequest {
            body: Some(upload_blob_request::Body::Chunk(
                horde_proto::v1::BlobChunk {
                    data: b"what arrived!!!".to_vec(),
                },
            )),
        },
    ];
    let status = within(
        "the refusal",
        client.upload_blob(tokio_stream::iter(frames)),
    )
    .await?
    .expect_err("a lying upload must be refused");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains(&declared.to_string()),
        "the refusal names the declaration: {status}"
    );

    let status = within("the absence", download_blob(&mut client, &declared))
        .await?
        .expect_err("nothing may be stored under the declared digest");
    assert_eq!(status.code(), tonic::Code::NotFound);
    Ok(())
}

/// The integrity refusal on read, across the wire: bytes tampered with on
/// the server's disk must fail the downloader with `DATA_LOSS`, never
/// arrive looking whole.
#[tokio::test]
async fn a_corrupted_stored_blob_fails_the_download_as_data_loss() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let endpoint = serve_with_blob_store(blob_root.path()).await?;
    let mut client = BlobServiceClient::connect(endpoint).await?;

    let content = b"trusted content".to_vec();
    let stored = within("the upload", upload_blob(&mut client, &content)).await??;
    let digest = BlobDigest::parse(&stored.blob_digest).ok_or("the answer parses")?;

    // Corrupt the stored file behind the server's back.
    let (fan, rest) = digest.hex().split_at(2);
    let path = blob_root.path().join("sha256").join(fan).join(rest);
    std::fs::write(&path, b"tampered content")?;

    let status = within("the refusal", download_blob(&mut client, &digest))
        .await?
        .expect_err("a corrupt blob must fail the reader");
    assert_eq!(status.code(), tonic::Code::DataLoss);
    assert!(
        status.message().contains(&digest.to_string()),
        "the refusal names the address that broke: {status}"
    );
    Ok(())
}

#[tokio::test]
async fn a_server_without_a_store_answers_unimplemented() -> Setup<()> {
    let endpoint = serve().await?;
    let mut client = BlobServiceClient::connect(endpoint).await?;

    let status = within("the refusal", upload_blob(&mut client, b"anything"))
        .await?
        .expect_err("no store means an honest Unimplemented");
    assert_eq!(status.code(), tonic::Code::Unimplemented);
    Ok(())
}

/// The log gate at the client chokepoint: an event smuggling more than the
/// inline threshold is refused with the remedy, and an event at the
/// threshold lands and replays with its content intact.
#[tokio::test]
async fn the_control_plane_refuses_bulk_and_carries_the_threshold_exactly() -> Setup<()> {
    let endpoint = serve().await?;
    let (mut session_client, session_id, root_agent) = seeded(&endpoint, "bulk-gate").await?;
    let mut control = ControlPlaneServiceClient::connect(endpoint.clone()).await?;

    // One byte over: refused, naming the attachment and the rule.
    let over = vec![1u8; usize::try_from(INLINE_THRESHOLD_BYTES)? + 1];
    let status = within(
        "the refusal",
        control.send_message(assignment_carrying(
            &root_agent,
            inline_attachment("huge.log", &over),
        )),
    )
    .await?
    .expect_err("bulk must not enter the log");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    for needed in ["huge.log", "never bulk", "blob store"] {
        assert!(
            status.message().contains(needed),
            "missing {needed:?}: {status}"
        );
    }

    // At the threshold: accepted, and the replay carries the bytes back.
    let at_limit = vec![2u8; usize::try_from(INLINE_THRESHOLD_BYTES)?];
    let sent = within(
        "the accepted send",
        control.send_message(assignment_carrying(
            &root_agent,
            inline_attachment("small.log", &at_limit),
        )),
    )
    .await??
    .into_inner();
    assert!(sent.sequence > 0);

    let replayed = within("the replay", async {
        let mut found = None;
        session_client
            .sync(
                &horde_cli::client::SyncOptions::new(session_id.clone()),
                |envelope| {
                    if let Some(Kind::AssignmentSent(sent)) = envelope
                        .event
                        .as_ref()
                        .and_then(|event| event.kind.as_ref())
                        && let Some(payload) =
                            sent.assignment.as_ref().and_then(|a| a.payload.as_ref())
                        && let Some(attachment) = payload.attachments.first()
                    {
                        found = Some(attachment.clone());
                    }
                },
                |_| {},
            )
            .await
            .map(|_| found)
    })
    .await??
    .ok_or("the attachment must replay")?;
    assert_eq!(replayed.name, "small.log");
    assert_eq!(replayed.inline, at_limit, "inline content survives the log");
    assert_eq!(
        replayed.blob_digest,
        BlobDigest::of(&at_limit).to_string(),
        "the digest rides along so any reader can verify"
    );
    Ok(())
}

/// The log gate at the worker chokepoint: the journal must not carry bulk
/// into the log any more than a client verb may.
#[tokio::test]
async fn the_worker_journal_refuses_bulk_and_accepts_references() -> Setup<()> {
    let endpoint = serve().await?;
    let (_client, session_id, _root) = seeded(&endpoint, "journal-gate").await?;

    let result_with = |attachment: Attachment, key: &str| Event {
        agent_id: None,
        occurred_at: None,
        idempotency_key: key.to_owned(),
        kind: Some(Kind::ResultReported(ResultReported {
            result: Some(AgentResult {
                assignment_id: None,
                status: 1,
                summary: "done".to_owned(),
                payload: Some(Payload {
                    git_refs: Vec::new(),
                    attachments: vec![attachment],
                }),
            }),
        })),
    };

    // Bulk: the server refuses the batch — the stream closes instead of
    // acking, which `journal_event` reports as an error.
    let over = vec![3u8; usize::try_from(INLINE_THRESHOLD_BYTES)? + 1];
    let refused = within(
        "the refusal",
        journal_event(
            &endpoint,
            &session_id,
            result_with(inline_attachment("core-dump.bin", &over), "bulk-1"),
        ),
    )
    .await?;
    assert!(refused.is_err(), "a journaled bulk event must be refused");

    // A reference to the same content is exactly what the log wants.
    let reference = Attachment {
        inline: Vec::new(),
        ..inline_attachment("core-dump.bin", &over)
    };
    within(
        "the accepted journal",
        journal_event(&endpoint, &session_id, result_with(reference, "ref-1")),
    )
    .await??;
    Ok(())
}

/// The wave-5 promise applied to blobs: transfer works for an enrolled
/// principal over mTLS, and stops at the very next call once that
/// principal is revoked — the enrolled door guards this spine too.
#[tokio::test]
async fn blobs_move_over_the_authenticated_channel_and_stop_at_revocation() -> Setup<()> {
    let blob_root = tempfile::tempdir()?;
    let harness = serve_secured_with_blob_store(blob_root.path()).await?;
    let identity = harness
        .enroll(horde_identity::PrincipalRole::Worker, "blob-mover")
        .await?;

    let channel = harness.main_endpoint_as(&identity)?.connect().await?;
    let mut client = BlobServiceClient::new(channel);

    let content = b"a screenshot crossing machines".to_vec();
    let stored = within("the mTLS upload", upload_blob(&mut client, &content)).await??;
    let digest = BlobDigest::parse(&stored.blob_digest).ok_or("the answer parses")?;
    let fetched = within("the mTLS download", download_blob(&mut client, &digest)).await??;
    assert_eq!(fetched, content);

    // Revoked: the same open channel refuses the next blob call.
    let principal =
        horde_identity::Principal::new(horde_identity::PrincipalRole::Worker, "blob-mover")?;
    harness
        .authority
        .revoke(
            &principal,
            "test revocation",
            horde_server::service::enrollment::wall_clock_ms(),
        )
        .await?;

    let status = within("the refusal", download_blob(&mut client, &digest))
        .await?
        .expect_err("a revoked principal must not move blobs");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    Ok(())
}
