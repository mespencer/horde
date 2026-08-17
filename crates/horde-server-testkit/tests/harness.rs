//! The testkit against the server it stands up.
//!
//! Not a copy of the consumers' suites: cargo-mutants runs a mutated package's
//! own tests, so a helper whose only exercise lives in the crates consuming it
//! is a helper whose mutants all survive. Each test here observes an effect
//! through the server — a session fetched back by the id the helper returned,
//! an event replayed after its push was acked — rather than trusting a
//! returned value, because a returned value is exactly what a mutant fakes.

mod common;

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

/// A placed `seed_agent` is indistinguishable, to the command fold, from a
/// placement the scheduler wrote: the seeded pair folds into a
/// `SpawnAgentCommand` for that worker, carrying the first assignment. The
/// wire effect is the assertion — a seed whose kinds, worker or message were
/// wrong would fold into nothing, or into somebody else's command.
#[tokio::test]
async fn a_placed_seed_folds_into_that_workers_spawn_command() {
    use horde_proto::v1::stream_commands_response::Command;

    let core = horde_server::ServerCore::new(
        horde_server::EventLog::in_memory().expect("an in-memory log"),
    );
    let endpoint = horde_server_testkit::serve_core(core.clone())
        .await
        .expect("the core must serve");
    horde_server_testkit::place_agent(&core.log, "s1", "agent-7", "worker-a", "first words")
        .await
        .expect("the seed must append");

    let mut worker = WorkerServiceClient::connect(
        tonic::transport::Endpoint::try_from(endpoint).expect("the endpoint must parse"),
    )
    .await
    .expect("the worker client must connect");
    let mut commands = worker
        .stream_commands(horde_proto::v1::StreamCommandsRequest {
            worker_id: Some(horde_proto::v1::WorkerId::new("worker-a")),
            resume_from_command_sequence: 1,
        })
        .await
        .expect("the stream must open")
        .into_inner();

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), commands.message())
        .await
        .expect("the fold must answer while the suite watches")
        .expect("the stream must not fail")
        .expect("the fold must issue the seeded spawn");
    let Some(Command::SpawnAgent(spawn)) = first.command else {
        panic!("a placed seed must fold into a spawn, got {first:?}");
    };
    assert_eq!(
        spawn.agent_id.as_ref().map(ToString::to_string),
        Some("agent-7".to_owned())
    );
    assert_eq!(
        spawn
            .spec
            .as_ref()
            .and_then(|spec| spec.initial_assignment.as_ref())
            .map(|assignment| assignment.text.clone()),
        Some("first words".to_owned()),
        "the implicit first assignment rides the seeded spec"
    );
}

/// A worker-less seed is an honest queue entry: the placement event says
/// `QUEUED` and names nobody — so no worker's fold may claim it.
#[tokio::test]
async fn a_worker_less_seed_queues_rather_than_placing() {
    use horde_proto::prost::Message;
    use horde_proto::v1::event::Kind as EventKind;

    let log = horde_server::EventLog::in_memory().expect("an in-memory log");
    horde_server_testkit::seed_agent(&log, "s1", "agent-9", None, "later")
        .await
        .expect("the seed must append");

    let stored = log.read_all("s1").await.expect("a readable log");
    let events: Vec<Event> = stored
        .iter()
        .map(|stored| Event::decode(stored.payload.as_slice()).expect("a decodable event"))
        .collect();
    assert_eq!(events.len(), 2, "one spawn, one placement, nothing else");
    assert!(
        matches!(events[0].kind, Some(EventKind::AgentSpawned(_))),
        "the spawn leads: {events:?}"
    );
    let Some(EventKind::PlacementChanged(changed)) = &events[1].kind else {
        panic!("the placement follows, got {events:?}");
    };
    let placement = changed.placement.as_ref().expect("a placement");
    assert_eq!(
        placement.state,
        horde_proto::v1::PlacementState::Queued as i32
    );
    assert!(
        placement.worker_id.is_none(),
        "a queued placement names no worker"
    );
    assert_eq!(
        events[1].idempotency_key, "agent-9:placed",
        "re-seeding the same agent must deduplicate, so the key is the agent's"
    );

    // And the dedupe is real, not just a key in the payload: the log's own
    // idempotency column must carry it, which only a second seed can prove —
    // a seed that dropped the column would append four events here.
    horde_server_testkit::seed_agent(&log, "s1", "agent-9", None, "later")
        .await
        .expect("the re-seed is absorbed");
    assert_eq!(
        log.read_all("s1").await.expect("a readable log").len(),
        2,
        "an at-least-once seed must land exactly once"
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

    common::assert_blob_lands_under_root(&mut blobs, blob_root.path(), b"testkit blob")
        .await
        .expect("the store must keep its promises");
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
