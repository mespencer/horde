//! The testkit against the server it stands up.
//!
//! Not a copy of the consumers' suites: cargo-mutants runs a mutated package's
//! own tests, so a helper whose only exercise lives in the crates consuming it
//! is a helper whose mutants all survive. Each test here observes an effect
//! through the server — a session fetched back by the id the helper returned,
//! an event replayed after its push was acked — rather than trusting a
//! returned value, because a returned value is exactly what a mutant fakes.

use horde_cli::client::SyncOptions;
use horde_proto::v1::event::Kind;
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::{AgentId, AgentOutputChannel, AgentOutputChunk, Event, push_events_response};
use horde_server_testkit::{journal_event, journal_event_at, open_journal, seeded, serve};

/// One event a worker might journal: an output chunk carrying `text`.
fn chunk(agent_id: &str, text: &str) -> Event {
    Event {
        agent_id: Some(AgentId::new(agent_id)),
        occurred_at: None,
        idempotency_key: format!("{agent_id}-{text}"),
        kind: Some(Kind::AgentOutputChunk(AgentOutputChunk {
            channel: AgentOutputChannel::Assistant as i32,
            text: text.to_owned(),
            turn_id: "turn-1".to_owned(),
            chunk_index: 0,
        })),
    }
}

/// Replays the session and returns the text of every output chunk, in order.
async fn chunk_texts(
    client: &mut horde_cli::Client,
    session_id: &str,
) -> Result<Vec<String>, horde_cli::ClientError> {
    let mut texts = Vec::new();
    client
        .sync(
            &SyncOptions::new(session_id),
            |envelope| {
                if let Some(Kind::AgentOutputChunk(chunk)) = envelope
                    .event
                    .as_ref()
                    .and_then(|event| event.kind.as_ref())
                {
                    texts.push(chunk.text.clone());
                }
            },
            |_| {},
        )
        .await?;
    Ok(texts)
}

/// `seeded` answers with the server's ids, not fabrications: the session id
/// fetches the session back, and the root agent id is the one the log's own
/// spawn event names.
#[tokio::test]
async fn a_seeded_session_is_fetchable_and_names_its_root() {
    let endpoint = serve().await.expect("the harness must serve");
    let (mut client, session_id, root) = seeded(&endpoint, "testkit")
        .await
        .expect("seeding must succeed");

    let summary = client
        .get_session(&session_id)
        .await
        .expect("the returned session id must name a session the server has");
    assert_eq!(
        summary.session_id.as_ref().map(ToString::to_string),
        Some(session_id.clone()),
        "the server must report the same session the seed returned"
    );

    let mut spawned = Vec::new();
    client
        .sync(
            &SyncOptions::new(session_id),
            |envelope| {
                if let Some(event) = &envelope.event
                    && matches!(event.kind, Some(Kind::AgentSpawned(_)))
                    && let Some(id) = &event.agent_id
                {
                    spawned.push(id.to_string());
                }
            },
            |_| {},
        )
        .await
        .expect("replay must succeed");
    assert_eq!(
        spawned,
        vec![root],
        "the root agent id must be the one the spawn event names"
    );
}

/// An acked `journal_event` is on the log: the event replays. A write that is
/// acked but discarded is the worst failure shape there is, and it is exactly
/// what a body replaced with `Ok(())` produces.
#[tokio::test]
async fn a_journaled_event_reaches_the_session_log() {
    let endpoint = serve().await.expect("the harness must serve");
    let (mut client, session_id, root) = seeded(&endpoint, "journal")
        .await
        .expect("seeding must succeed");

    journal_event(&endpoint, &session_id, chunk(&root, "observed"))
        .await
        .expect("the push must be acked");

    let texts = chunk_texts(&mut client, &session_id)
        .await
        .expect("replay must succeed");
    assert_eq!(
        texts,
        vec!["observed".to_owned()],
        "an acked push must be on the log, exactly once"
    );
}

/// Two pushes at consecutive caller-chosen sequences both land, in order —
/// the property `horde-mcp`'s server-backed journal builds on.
#[tokio::test]
async fn consecutive_journal_sequences_both_land_in_order() {
    let endpoint = serve().await.expect("the harness must serve");
    let (mut client, session_id, root) = seeded(&endpoint, "sequences")
        .await
        .expect("seeding must succeed");

    journal_event_at(&endpoint, &session_id, 1, chunk(&root, "first"))
        .await
        .expect("the first push must be acked");
    journal_event_at(&endpoint, &session_id, 2, chunk(&root, "second"))
        .await
        .expect("the second push must be acked");

    let texts = chunk_texts(&mut client, &session_id)
        .await
        .expect("replay must succeed");
    assert_eq!(
        texts,
        vec!["first".to_owned(), "second".to_owned()],
        "each sequence must land once, in journal order"
    );
}

/// `open_journal` hands back the ack stream with the first ack unread, and
/// that ack is the server accepting the announced position — the contract
/// `horde-server`'s resume tests build their assertions on.
#[tokio::test]
async fn open_journal_leaves_the_first_ack_for_the_caller() {
    let endpoint = serve().await.expect("the harness must serve");
    let mut worker = WorkerServiceClient::connect(
        tonic::transport::Endpoint::try_from(endpoint).expect("the endpoint must parse"),
    )
    .await
    .expect("the worker client must connect");

    let (_to_server, mut acks) = open_journal(&mut worker, "worker-a", 0)
        .await
        .expect("opening the journal must succeed");
    let first = acks
        .message()
        .await
        .expect("the ack stream must answer")
        .expect("the server must ack the start rather than closing the stream");
    assert!(
        matches!(first.body, Some(push_events_response::Body::Accepted(_))),
        "a fresh worker announcing position 0 is accepted, and the caller sees it"
    );
}

/// `serve_with_blob_store` really wires the store, rooted where the caller
/// said: bytes uploaded through the endpoint land as a file under the
/// caller's directory, and download hands them back. The on-disk effect is
/// the assertion, because the returned endpoint string is exactly what a
/// mutant of the wiring fakes.
#[tokio::test]
async fn a_blob_store_server_moves_bytes_under_the_callers_root() {
    let blob_root = tempfile::tempdir().expect("a temp dir");
    let endpoint = horde_server_testkit::serve_with_blob_store(blob_root.path())
        .await
        .expect("the blob harness must serve");
    let mut blobs = horde_proto::v1::blob_service_client::BlobServiceClient::connect(
        tonic::transport::Endpoint::try_from(endpoint).expect("the endpoint must parse"),
    )
    .await
    .expect("the blob client must connect");

    let stored = horde_proto::blob::upload_blob(&mut blobs, b"testkit blob")
        .await
        .expect("the upload must be accepted");
    let digest =
        horde_proto::blob::BlobDigest::parse(&stored.blob_digest).expect("the answer parses");

    let (fan, rest) = digest.hex().split_at(2);
    assert!(
        blob_root
            .path()
            .join("sha256")
            .join(fan)
            .join(rest)
            .is_file(),
        "the blob must land under the directory the caller rooted the store at"
    );
    let fetched = horde_proto::blob::download_blob(&mut blobs, &digest)
        .await
        .expect("the download must verify");
    assert_eq!(fetched, b"testkit blob");
}

/// `serve_with_permissions` really runs the chain: a request journaled the way
/// a worker journals one comes back decided by the configured mode, with no
/// human attached anywhere. Observed through the log rather than trusted from
/// the endpoint string, which is exactly what a mutant of the wiring fakes.
#[tokio::test]
async fn a_permission_server_answers_from_the_configured_mode() {
    use horde_server::permission::{PermissionMode, PermissionModes, Rule};

    let modes = PermissionModes::default()
        .with_default(PermissionMode::escalate_everything().allow(Rule::tool("Read")));
    let endpoint = horde_server_testkit::serve_with_permissions(modes, None)
        .await
        .expect("the permission harness must serve");
    let (mut client, session_id, root) = seeded(&endpoint, "chain")
        .await
        .expect("seeding must succeed");

    let request = Event {
        agent_id: Some(AgentId::new(root.clone())),
        occurred_at: None,
        idempotency_key: "req-chain".to_owned(),
        kind: Some(Kind::PermissionRequested(
            horde_proto::v1::PermissionRequested {
                request: Some(horde_proto::v1::PermissionRequest {
                    request_id: Some(horde_proto::v1::PermissionRequestId::new("req-chain")),
                    agent_id: Some(AgentId::new(root)),
                    tool_call: Some(horde_proto::v1::ProposedToolCall {
                        tool_name: "Read".to_owned(),
                        input: None,
                        rendered: "Read".to_owned(),
                    }),
                    ..horde_proto::v1::PermissionRequest::default()
                }),
            },
        )),
    };
    journal_event(&endpoint, &session_id, request)
        .await
        .expect("the request must be acked");

    let mut stream = client
        .follow(&SyncOptions::new(session_id))
        .await
        .expect("the log must be followable");
    let decided = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.next_matching(|envelope| {
            let Some(Kind::PermissionDecided(decided)) = envelope
                .event
                .as_ref()
                .and_then(|event| event.kind.as_ref())
            else {
                return None;
            };
            decided.decision.clone()
        }),
    )
    .await
    .expect("the chain must decide while the suite watches")
    .expect("the stream must not fail")
    .expect("the stream must not end undecided");

    assert_eq!(
        decided.outcome,
        horde_proto::v1::PermissionOutcome::Allow as i32
    );
    assert_eq!(
        decided.decided_by,
        horde_proto::v1::PermissionLayer::DeterministicAllow as i32,
        "the decision must come from the chain the harness was told to run"
    );
}
