//! Both services over a real tonic connection.
//!
//! In-process rather than over a socket: the transport is a duplex stream, so
//! there is no port to bind (DESIGN.md §3 forbids binding beyond localhost
//! before auth exists) and no flake from a port race. Everything above the
//! socket — codegen, framing, streaming, status mapping — is the real thing.
//!
//! The worker is simulated at the wire level. There is no worker binary yet,
//! and reaching for the FakeAdapter here would test an adapter rather than the
//! server.

use horde_proto::v1::{
    AgentId, AgentOutputChannel, AgentOutputChunk, AgentSpec, CreateSessionRequest, Event,
    GetSessionRequest, JournaledEvent, JournaledEventBatch, ListSessionsRequest, PermissionOutcome,
    PushEventsRequest, RegisterWorkerRequest, SessionId, SpawnAgentRequest, StreamCommandsRequest,
    WorkerCapabilities, WorkerId, control_plane_service_client::ControlPlaneServiceClient,
    event::Kind, push_events_request, push_events_response, stream_commands_response,
    sync_events_response,
};
use tokio::io::DuplexStream;
use tokio_stream::StreamExt;
use tonic::Code;

mod common;

use common::{Harness, Setup, connect_to, core, open_sync, seed};
use horde_server::service::ServerCore;

/// Opens a journal stream and announces the worker's journal position.
///
/// Returns the sender and the ack stream; the caller decides what to make of
/// the first ack, which is the part each test actually cares about.
async fn open_journal(
    harness: &mut Harness,
    worker_id: &str,
    last_local_journal_sequence: u64,
) -> Setup<(
    tokio::sync::mpsc::Sender<PushEventsRequest>,
    tonic::Streaming<horde_proto::v1::PushEventsResponse>,
)> {
    horde_server_testkit::open_journal(&mut harness.worker, worker_id, last_local_journal_sequence)
        .await
}

/// Registers a worker so the scheduler has somewhere to place agents.
async fn register_worker(harness: &mut Harness, worker_id: &str) -> Setup<u64> {
    Ok(harness
        .worker
        .register_worker(RegisterWorkerRequest {
            hello: None,
            worker_id: Some(WorkerId::new(worker_id)),
            capabilities: Some(WorkerCapabilities::default()),
        })
        .await?
        .into_inner()
        .resume_from_journal_sequence)
}

/// Spawns a root agent under `session_id` and returns its id and placement.
async fn spawn_agent(
    harness: &mut Harness,
    session_id: &str,
) -> Setup<(String, horde_proto::v1::Placement)> {
    let spawned = harness
        .control
        .spawn_agent(SpawnAgentRequest {
            session_id: Some(SessionId::new(session_id)),
            parent_agent_id: None,
            spec: Some(AgentSpec::default()),
            idempotency_key: String::new(),
        })
        .await?
        .into_inner();
    let placement = spawned
        .placement
        .ok_or("a spawn always reports placement")?;
    let agent_id = spawned
        .agent_id
        .map(|id| id.to_string())
        .ok_or("a spawned agent has an id")?;
    Ok((agent_id, placement))
}

/// Answers a pending permission, returning whatever the server made of it.
async fn answer_permission(
    harness: &mut Harness,
    request_id: &str,
    reason: &str,
    outcome: PermissionOutcome,
    updated_input: Option<horde_proto::prost_types::Struct>,
) -> Result<horde_proto::v1::AnswerPermissionResponse, tonic::Status> {
    harness
        .control
        .answer_permission(horde_proto::v1::AnswerPermissionRequest {
            request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
            outcome: outcome as i32,
            reason: reason.to_owned(),
            remember_for_session: false,
            updated_input,
        })
        .await
        .map(tonic::Response::into_inner)
}

fn output_event(agent: &str, text: &str, index: u64) -> Event {
    Event {
        agent_id: Some(AgentId::new(agent)),
        occurred_at: None,
        idempotency_key: format!("{agent}-{index}"),
        kind: Some(Kind::AgentOutputChunk(AgentOutputChunk {
            channel: AgentOutputChannel::Assistant as i32,
            text: text.to_owned(),
            turn_id: "turn-1".to_owned(),
            chunk_index: index,
        })),
    }
}

/// The full round trip: a worker registers, a client spawns, the worker is
/// commanded, it journals events back, and two clients syncing from different
/// offsets see identical prefixes.
#[tokio::test]
async fn a_worker_and_a_client_meet_through_the_log() {
    let core = core().expect("an in-memory core");
    let mut harness = connect_to(core.clone())
        .await
        .expect("the harness must connect");

    // 1. The worker registers and is told where to resume from.
    let resume_from = register_worker(&mut harness, "worker-a")
        .await
        .expect("registration must succeed");
    assert_eq!(
        resume_from, 1,
        "a fresh worker starts at the first journal position"
    );

    // 2. A client creates a session and spawns an agent under it. The root
    //    agent is placed at creation — a session's first prompt needs a
    //    worker like any other spawn — so the explicit spawn is the second
    //    placement this server makes.
    let (session_id, root_agent_id) = seed(&mut harness, "ci-fix", "key-1")
        .await
        .expect("a session must be creatable");

    let (agent_id, placement) = spawn_agent(&mut harness, &session_id)
        .await
        .expect("spawn must succeed");
    assert_eq!(
        placement.state,
        horde_proto::v1::PlacementState::Placed as i32,
        "with one worker registered the agent is placed: {}",
        placement.reason
    );
    // 3. The worker receives both spawn commands, root first.
    let mut commands = harness
        .worker
        .stream_commands(StreamCommandsRequest {
            worker_id: Some(WorkerId::new("worker-a")),
            resume_from_command_sequence: 1,
        })
        .await
        .expect("the command stream must open")
        .into_inner();

    let command = commands
        .next()
        .await
        .expect("a command must arrive")
        .expect("the command must not be an error");
    assert_eq!(command.command_sequence, 1);
    match command.command {
        Some(stream_commands_response::Command::SpawnAgent(spawn)) => {
            assert_eq!(
                spawn.agent_id.map(|id| id.to_string()).as_deref(),
                Some(root_agent_id.as_str()),
                "the session's root agent is the first thing this worker runs"
            );
        }
        other => panic!("expected a spawn command, got {other:?}"),
    }

    let command = commands
        .next()
        .await
        .expect("a second command must arrive")
        .expect("the command must not be an error");
    assert_eq!(command.command_sequence, 2);
    match command.command {
        Some(stream_commands_response::Command::SpawnAgent(spawn)) => {
            assert_eq!(
                spawn.agent_id.map(|id| id.to_string()).as_deref(),
                Some(agent_id.as_str())
            );
        }
        other => panic!("expected a spawn command, got {other:?}"),
    }

    // 4. The worker journals events back and is acked.
    let (to_server, mut acks) = open_journal(&mut harness, "worker-a", 0)
        .await
        .expect("the event stream must open");

    match acks
        .next()
        .await
        .expect("a reply to start")
        .expect("start must be accepted")
        .body
    {
        Some(push_events_response::Body::Accepted(accepted)) => {
            assert_eq!(accepted.resume_from_journal_sequence, 1);
        }
        other => panic!("expected Accepted, got {other:?}"),
    }

    to_server
        .send(PushEventsRequest {
            body: Some(push_events_request::Body::Batch(JournaledEventBatch {
                events: (1..=3)
                    .map(|index| JournaledEvent {
                        journal_sequence: index,
                        session_id: Some(SessionId::new(session_id.clone())),
                        event: Some(output_event(&agent_id, &format!("chunk {index}"), index)),
                    })
                    .collect(),
                acked_command_sequence: 1,
            })),
        })
        .await
        .expect("batch must send");

    match acks
        .next()
        .await
        .expect("a reply to the batch")
        .expect("the batch must be accepted")
        .body
    {
        Some(push_events_response::Body::Ack(ack)) => {
            assert_eq!(
                ack.acked_journal_sequence, 3,
                "the ack is in the worker's own sequence space"
            );
        }
        other => panic!("expected an Ack, got {other:?}"),
    }

    // 5. Two clients sync from different offsets and see identical prefixes.
    let from_zero = sync_to_head(&mut harness.control, &session_id, 0)
        .await
        .expect("a full replay must succeed");
    let from_mid = sync_to_head(&mut harness.control, &session_id, 3)
        .await
        .expect("a mid-stream replay must succeed");

    assert!(
        from_zero.len() >= 5,
        "the log holds the spawn, the placement, and three chunks: {from_zero:?}"
    );
    assert_eq!(
        from_zero[2..],
        from_mid[..],
        "a client resuming mid-stream must see exactly the tail of what a full replay contains"
    );
}

/// Replays a session to its head and returns the sequences seen.
async fn sync_to_head(
    control: &mut ControlPlaneServiceClient<tonic::transport::Channel>,
    session_id: &str,
    from_sequence: u64,
) -> Setup<Vec<u64>> {
    let mut stream = open_sync(control, session_id, from_sequence, false).await?;

    let mut sequences = Vec::new();
    while let Some(message) = stream.next().await {
        let message = message?;
        match message.update {
            Some(sync_events_response::Update::Batch(batch)) => {
                sequences.extend(batch.events.iter().map(|event| event.sequence));
            }
            Some(sync_events_response::Update::CaughtUp(_)) => break,
            _ => {}
        }
    }
    Ok(sequences)
}

#[tokio::test]
async fn sessions_are_listed_and_fetched_from_the_log() {
    let core = core().expect("an in-memory core");
    let mut harness = connect_to(core).await.expect("the harness must connect");

    assert!(
        harness
            .control
            .list_sessions(ListSessionsRequest::default())
            .await
            .expect("list must succeed")
            .into_inner()
            .sessions
            .is_empty(),
        "a fresh server has no sessions because the log has no events"
    );

    let (session_id, _) = seed(&mut harness, "first", "key-1")
        .await
        .expect("a session must be creatable");

    let listed = harness
        .control
        .list_sessions(ListSessionsRequest::default())
        .await
        .expect("list must succeed")
        .into_inner();
    assert_eq!(listed.sessions.len(), 1);

    let fetched = harness
        .control
        .get_session(GetSessionRequest {
            session_id: Some(SessionId::new(session_id)),
        })
        .await
        .expect("get must succeed")
        .into_inner()
        .session
        .expect("a summary");
    assert_eq!(fetched.agent_count, 1, "the root agent is in the tree");
    assert_eq!(
        fetched.head_sequence, 2,
        "creation writes the root's spawn and its placement answer"
    );
}

#[tokio::test]
async fn creating_a_session_twice_with_one_key_creates_one_session() {
    let core = core().expect("an in-memory core");
    let mut harness = connect_to(core).await.expect("the harness must connect");

    let request = || CreateSessionRequest {
        title: "retried".to_owned(),
        workspace_definition_id: None,
        root_agent: Some(AgentSpec::default()),
        idempotency_key: "same-key".to_owned(),
    };

    harness
        .control
        .create_session(request())
        .await
        .expect("first create");
    harness
        .control
        .create_session(request())
        .await
        .expect("a retry after a lost response must succeed");

    let listed = harness
        .control
        .list_sessions(ListSessionsRequest::default())
        .await
        .expect("list")
        .into_inner();
    assert_eq!(
        listed.sessions.len(),
        1,
        "a retried create must not produce a second session"
    );
    assert_eq!(
        listed.sessions[0].head_sequence, 2,
        "nor further events beyond the first create's spawn and placement: \
         the retried root is still queued with the same answer"
    );
}

#[tokio::test]
async fn a_spawn_with_no_worker_is_queued_with_a_reason_rather_than_failing() {
    let core = core().expect("an in-memory core");
    let mut harness = connect_to(core).await.expect("the harness must connect");

    let (session_id, _) = seed(&mut harness, "lonely", "key-1")
        .await
        .expect("a session must be creatable");

    let (agent_id, placement) = spawn_agent(&mut harness, &session_id)
        .await
        .expect("spawn must not fail just because nothing can run it");
    assert_eq!(
        placement.state,
        horde_proto::v1::PlacementState::Queued as i32
    );
    assert!(
        placement.reason.contains("no worker"),
        "the spawner must be told why, got {:?}",
        placement.reason
    );
    assert!(
        !agent_id.is_empty(),
        "the agent exists whether or not it is placed"
    );
}

/// An unbuilt RPC must be distinguishable from a broken one.
#[tokio::test]
async fn unbuilt_rpcs_report_unimplemented_rather_than_a_fabricated_success() {
    let core = core().expect("an in-memory core");
    let mut harness = connect_to(core).await.expect("the harness must connect");

    let status = harness
        .control
        .mint_enrollment_code(horde_proto::v1::MintEnrollmentCodeRequest::default())
        .await
        .expect_err("enrollment is not built");
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(status.message().contains("enrollment"));
}

#[tokio::test]
async fn a_worker_that_lost_its_journal_is_refused_over_the_wire() {
    let core = core().expect("an in-memory core");
    let mut harness = connect_to(core.clone())
        .await
        .expect("the harness must connect");

    register_worker(&mut harness, "worker-a")
        .await
        .expect("registration");

    // Store some journal history directly, then have the worker claim it has
    // less than the server does.
    let (session_id, _) = seed(&mut harness, "s", "key-1")
        .await
        .expect("a session must be creatable");

    let (to_server, mut acks) = open_journal(&mut harness, "worker-a", 0)
        .await
        .expect("stream opens");
    acks.next().await.expect("accepted").expect("accepted");

    to_server
        .send(PushEventsRequest {
            body: Some(push_events_request::Body::Batch(JournaledEventBatch {
                events: vec![JournaledEvent {
                    journal_sequence: 5,
                    session_id: Some(SessionId::new(session_id)),
                    event: Some(output_event("agent-1", "hello", 0)),
                }],
                acked_command_sequence: 0,
            })),
        })
        .await
        .expect("batch sends");
    acks.next().await.expect("ack").expect("ack");

    drop(to_server);

    // Reconnect claiming a journal head below what the server stored.
    let (_to_server, mut acks) = open_journal(&mut harness, "worker-a", 2)
        .await
        .expect("stream reopens");

    let status = acks
        .next()
        .await
        .expect("a reply")
        .expect_err("a regressed journal must be refused, not silently renumbered");
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("refusing to renumber"));
}

/// A permission request, journaled by the worker the way a real one arrives.
fn permission_requested(agent: &str, request_id: &str) -> Event {
    Event {
        agent_id: Some(AgentId::new(agent)),
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(Kind::PermissionRequested(
            horde_proto::v1::PermissionRequested {
                request: Some(horde_proto::v1::PermissionRequest {
                    request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
                    agent_id: Some(AgentId::new(agent)),
                    ..horde_proto::v1::PermissionRequest::default()
                }),
            },
        )),
    }
}

/// Journals one `PermissionRequested` per id, as a single worker batch.
async fn journal_permission_requests(
    harness: &mut Harness,
    session_id: &str,
    request_ids: &[&str],
) -> Setup<()> {
    let (to_server, mut acks) = open_journal(harness, "worker-a", 0).await?;
    acks.next().await.ok_or("a start reply")??;

    let mut events = Vec::new();
    for (index, request_id) in request_ids.iter().enumerate() {
        events.push(JournaledEvent {
            journal_sequence: u64::try_from(index)? + 1,
            session_id: Some(SessionId::new(session_id)),
            event: Some(permission_requested("agent-1", request_id)),
        });
    }
    to_server
        .send(PushEventsRequest {
            body: Some(push_events_request::Body::Batch(JournaledEventBatch {
                events,
                acked_command_sequence: 0,
            })),
        })
        .await?;
    acks.next().await.ok_or("a batch reply")??;
    Ok(())
}

/// A connected harness whose session already has one pending permission per
/// id — the arrangement both answer-side tests start from.
async fn harness_with_pending_permissions(request_ids: &[&str]) -> Setup<Harness> {
    let mut harness = connect_to(core()?).await?;
    let (session_id, _) = seed(&mut harness, "perms", "key-1").await?;
    journal_permission_requests(&mut harness, &session_id, request_ids).await?;
    Ok(harness)
}

#[tokio::test]
async fn a_permission_is_answered_by_request_id_alone() {
    // The client answering a permission knows the request id and nothing else —
    // not the session, not the worker. Finding the rest is the server's job,
    // which is what makes a permission answerable from any attached client.
    let mut harness = harness_with_pending_permissions(&["req-1"])
        .await
        .expect("a pending permission must be arrangeable");

    let answered = answer_permission(
        &mut harness,
        "req-1",
        "looks fine",
        PermissionOutcome::Allow,
        None,
    )
    .await
    .expect("a pending permission must be answerable");
    assert!(
        answered.sequence > 0,
        "the decision is an event like any other, so it has a sequence"
    );

    // Answering it again fails: the projection dropped it when the decision
    // landed, so there is nothing left awaiting an answer.
    let twice = answer_permission(&mut harness, "req-1", "", PermissionOutcome::Allow, None)
        .await
        .expect_err("an answered permission is no longer pending");
    assert_eq!(twice.code(), Code::InvalidArgument);
}

/// A narrowing rides an ALLOW and is refused on a DENY — both directions of
/// the guard, because either operator slip (`&&`→`||`, `!=`→`==`) turns one
/// of them into the other.
#[tokio::test]
async fn a_narrowing_is_accepted_with_allow_and_refused_with_deny() {
    let mut harness = harness_with_pending_permissions(&["req-allow", "req-deny"])
        .await
        .expect("two pending permissions must be arrangeable");

    let narrowing = horde_proto::prost_types::Struct::default();

    // ALLOW with replacement arguments is the narrowing round trip working —
    // the human clamped the call rather than merely permitting it.
    let answered = answer_permission(
        &mut harness,
        "req-allow",
        "clamped",
        PermissionOutcome::Allow,
        Some(narrowing.clone()),
    )
    .await
    .expect("a narrowing alongside ALLOW must be accepted");
    assert!(answered.sequence > 0);

    // The same narrowing on a DENY would write replacement arguments every
    // reader is entitled to ignore — refused, not dropped.
    let refused = answer_permission(
        &mut harness,
        "req-deny",
        "",
        PermissionOutcome::Deny,
        Some(narrowing),
    )
    .await
    .expect_err("a narrowing alongside DENY must be refused");
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert!(refused.message().contains("ALLOW"));
}

#[tokio::test]
async fn answering_a_permission_nobody_asked_for_is_refused() {
    let core = core().expect("an in-memory core");
    let mut harness = connect_to(core).await.expect("the harness must connect");
    seed(&mut harness, "perms", "key-1")
        .await
        .expect("a session must be creatable");

    let status = answer_permission(
        &mut harness,
        "never-asked",
        "",
        PermissionOutcome::Allow,
        None,
    )
    .await
    .expect_err("inventing a decision for an unknown request would be worse");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("never-asked"));
}

#[tokio::test]
async fn an_agent_is_found_by_id_without_being_told_its_session() {
    // interrupt_agent takes an agent id and no session id, so the server has to
    // find the session itself. The failure case matters as much: an unknown
    // agent must be refused rather than interrupting something else.
    let core = core().expect("an in-memory core");
    let mut harness = connect_to(core).await.expect("the harness must connect");
    register_worker(&mut harness, "worker-a")
        .await
        .expect("the worker must register");
    let (session_id, _) = seed(&mut harness, "findable", "key-1")
        .await
        .expect("a session must be creatable");
    let interrupted_session = session_id.clone();
    let (agent_id, _placement) = spawn_agent(&mut harness, &session_id)
        .await
        .expect("spawn must succeed");

    let interrupted = harness
        .control
        .interrupt_agent(horde_proto::v1::InterruptAgentRequest {
            agent_id: Some(AgentId::new(agent_id)),
            reason: "changed my mind".to_owned(),
        })
        .await
        .expect("a known agent must be interruptible")
        .into_inner();
    assert!(interrupted.sequence > 0);

    // The reason has to reach the log — it is what an attached client shows to
    // explain why a turn stopped.
    let reasons = interrupt_reasons(&mut harness.control, &interrupted_session)
        .await
        .expect("the session must be readable");
    assert_eq!(reasons, vec!["changed my mind".to_owned()]);

    let status = harness
        .control
        .interrupt_agent(horde_proto::v1::InterruptAgentRequest {
            agent_id: Some(AgentId::new("no-such-agent")),
            reason: String::new(),
        })
        .await
        .expect_err("an unknown agent must not resolve to some other session");
    assert_eq!(status.code(), Code::NotFound);
}

/// The reason on every `TurnInterrupted` recorded for a session.
async fn interrupt_reasons(
    control: &mut ControlPlaneServiceClient<tonic::transport::Channel>,
    session_id: &str,
) -> Setup<Vec<String>> {
    let mut stream = open_sync(control, session_id, 0, false).await?;

    let mut reasons = Vec::new();
    while let Some(message) = stream.next().await {
        if let Some(sync_events_response::Update::Batch(batch)) = message?.update {
            for envelope in batch.events {
                if let Some(Kind::TurnInterrupted(interrupted)) =
                    envelope.event.and_then(|event| event.kind)
                {
                    reasons.push(interrupted.reason);
                }
            }
        }
    }
    Ok(reasons)
}

/// Everything a sync stream delivered, in order, without stopping early.
async fn sync_everything(
    control: &mut ControlPlaneServiceClient<tonic::transport::Channel>,
    session_id: &str,
    follow: bool,
) -> Setup<Vec<String>> {
    let mut stream = open_sync(control, session_id, 0, follow).await?;

    let mut updates = Vec::new();
    while let Some(message) = stream.next().await {
        match message?.update {
            Some(sync_events_response::Update::Batch(batch)) => {
                updates.push(format!("batch:{}", batch.events.len()));
            }
            Some(sync_events_response::Update::CaughtUp(_)) => updates.push("caught-up".to_owned()),
            _ => {}
        }
    }
    Ok(updates)
}

#[tokio::test]
async fn a_sync_that_is_not_following_ends_at_the_caught_up_marker() {
    // Read the stream to its end rather than stopping at caught-up: a client
    // that is not following needs the stream to actually close, or it waits
    // forever for a completion that never comes.
    let core = core().expect("an in-memory core");
    let mut harness = connect_to(core).await.expect("the harness must connect");
    let (session_id, _) = seed(&mut harness, "syncing", "key-1")
        .await
        .expect("a session must be creatable");

    let updates = sync_everything(&mut harness.control, &session_id, false)
        .await
        .expect("a non-following sync must complete on its own");

    assert_eq!(
        updates,
        vec!["batch:2".to_owned(), "caught-up".to_owned()],
        "history — the root's spawn and its queued placement — arrives \
         before the marker, and nothing arrives after it"
    );
}

#[tokio::test]
async fn a_following_sync_delivers_what_is_appended_after_it_caught_up() {
    let core = core().expect("an in-memory core");
    let mut harness = connect_to(core).await.expect("the harness must connect");
    let (session_id, _) = seed(&mut harness, "following", "key-1")
        .await
        .expect("a session must be creatable");

    let mut stream = open_sync(&mut harness.control, &session_id, 0, true)
        .await
        .expect("the sync stream must open");

    // History, then the marker.
    assert!(matches!(
        stream.next().await.expect("history").expect("ok").update,
        Some(sync_events_response::Update::Batch(_))
    ));
    assert!(matches!(
        stream.next().await.expect("marker").expect("ok").update,
        Some(sync_events_response::Update::CaughtUp(_))
    ));

    // Now append something and watch it arrive on the same stream.
    let (to_server, mut acks) = open_journal(&mut harness, "worker-a", 0)
        .await
        .expect("the event stream must open");
    acks.next().await.expect("a reply").expect("accepted");
    for round in 1..=2u64 {
        to_server
            .send(PushEventsRequest {
                body: Some(push_events_request::Body::Batch(JournaledEventBatch {
                    events: vec![JournaledEvent {
                        journal_sequence: round,
                        session_id: Some(SessionId::new(session_id.clone())),
                        event: Some(output_event("agent-1", "later", round)),
                    }],
                    acked_command_sequence: 0,
                })),
            })
            .await
            .expect("the event must journal");

        let followed = stream.next().await.expect("a followed batch").expect("ok");
        match followed.update {
            Some(sync_events_response::Update::Batch(batch)) => {
                assert_eq!(batch.events.len(), 1, "round {round}");
            }
            other => panic!("expected a batch after catching up, got {other:?}"),
        }
    }
}

// -------------------------------------------------------------------------
// Commands across a restart.
//
// The point of folding commands out of the log instead of queueing them in
// memory. A "restart" here is a fresh `ServerCore` over the *same* `EventLog`:
// every in-memory structure the old server had is gone, and the database is
// the only thing that crosses the boundary. That is exactly what a process
// restart leaves behind, without needing a process.
//
// The worker is still simulated at the wire level, per this file's header. The
// end-to-end version — a real `horde-worker` applying these commands, crashing,
// and reconciling what it actually ran against what it acked — is
// `a_real_worker_reconciles_its_applied_commands_after_a_restart` in
// `horde-worker/tests/connection.rs`. It lives there rather than here because
// the dependency points that way only: the worker's tests may build a real
// server, but this crate knows nothing about the worker.
// -------------------------------------------------------------------------

/// A server with `worker-a` registered and one session seeded.
///
/// Returns the core as well as the harness, because a restart test needs the
/// log to build the *next* server over — that log is the only thing a restart
/// is allowed to carry across.
async fn commanded_server(title: &str) -> Setup<(ServerCore, Harness, String)> {
    let core = core()?;
    let mut harness = connect_to(core.clone()).await?;
    register_worker(&mut harness, "worker-a").await?;
    let (session_id, _) = seed(&mut harness, title, "key-1").await?;
    Ok((core, harness, session_id))
}

/// The same server again, over the same log and nothing else.
///
/// Every in-memory structure is discarded; if a command survives this, it
/// survived because the log carried it.
async fn restart(core: &ServerCore) -> Setup<Harness> {
    connect_to(ServerCore::new(core.log.clone())).await
}

/// Opens a command stream and returns the first command on it.
async fn first_command(
    harness: &mut Harness,
    worker_id: &str,
    resume_from: u64,
) -> Setup<horde_proto::v1::StreamCommandsResponse> {
    let mut commands = harness
        .worker
        .stream_commands(StreamCommandsRequest {
            worker_id: Some(WorkerId::new(worker_id)),
            resume_from_command_sequence: resume_from,
        })
        .await?
        .into_inner();

    // Bounded: a command that never arrives must red this test rather than
    // hang the suite.
    tokio::time::timeout(std::time::Duration::from_secs(5), commands.next())
        .await
        .map_err(|_| "the command stream must produce a command rather than hang")?
        .ok_or("the stream ended without a command")?
        .map_err(Into::into)
}

/// A command placed before a restart must still be delivered after one.
///
/// This is the failure the in-memory queue had: the spawn is recorded in the
/// log, the command derived from it lived only in the old process, and the
/// worker connecting to the new process was told there was nothing to do.
#[tokio::test]
async fn a_command_issued_before_a_restart_is_still_delivered_after_it() {
    let (core, mut harness, session_id) = commanded_server("restart").await.expect("a server");
    let (agent_id, placement) = spawn_agent(&mut harness, &session_id).await.expect("spawn");
    assert_eq!(
        placement.worker_id.as_ref().map(ToString::to_string),
        Some("worker-a".to_owned()),
        "the agent must be placed for there to be a command at all"
    );

    // The worker never opened a stream, so nothing was ever delivered.
    drop(harness);
    let mut restarted = restart(&core).await.expect("the restarted server");
    register_worker(&mut restarted, "worker-a")
        .await
        .expect("register");

    // Command 1 is the root agent's spawn; the spawn under test is command 2.
    let command = first_command(&mut restarted, "worker-a", 2)
        .await
        .expect("the command must survive the restart");
    assert_eq!(command.command_sequence, 2);
    match command.command {
        Some(stream_commands_response::Command::SpawnAgent(spawn)) => {
            assert_eq!(
                spawn.agent_id.as_ref().map(ToString::to_string),
                Some(agent_id),
                "and it must still name the agent that was placed"
            );
        }
        other => panic!("expected a spawn command, got {other:?}"),
    }
}

/// A worker that reconnects after a restart must be handed the *same* command
/// under the same sequence and id, or its ack points at something else.
#[tokio::test]
async fn a_restart_renumbers_nothing() {
    let (core, mut harness, session_id) = commanded_server("stable").await.expect("a server");
    spawn_agent(&mut harness, &session_id).await.expect("spawn");

    let before = first_command(&mut harness, "worker-a", 1)
        .await
        .expect("a command before the restart");

    drop(harness);
    let mut restarted = restart(&core).await.expect("the restarted server");
    let after = first_command(&mut restarted, "worker-a", 1)
        .await
        .expect("the same command after the restart");

    assert_eq!(
        before.command_sequence, after.command_sequence,
        "a restart must not renumber a command the worker may have acked"
    );
    assert_eq!(
        before.command_id, after.command_id,
        "the id a worker dedupes on must be stable across a restart"
    );
}

/// Registration answers with what the worker has actually been issued, folded
/// from the log. The in-memory version answered 1 after every restart, which
/// told a worker that had applied everything to start over.
#[tokio::test]
async fn a_restarted_server_still_knows_what_it_issued_a_worker() {
    let (core, mut harness, session_id) = commanded_server("issued").await.expect("a server");
    spawn_agent(&mut harness, &session_id).await.expect("spawn");
    spawn_agent(&mut harness, &session_id).await.expect("spawn");

    drop(harness);
    let mut restarted = restart(&core).await.expect("the restarted server");

    let resume = restarted
        .worker
        .register_worker(RegisterWorkerRequest {
            hello: None,
            worker_id: Some(WorkerId::new("worker-a")),
            capabilities: Some(WorkerCapabilities::default()),
        })
        .await
        .expect("register")
        .into_inner()
        .resume_from_command_sequence;

    assert_eq!(
        resume, 4,
        "three commands were issued — the root's spawn and two more — so \
         the next one is the fourth"
    );
}

/// A worker that already applied command 1 must not be shown it again, and the
/// commands it has not seen must still arrive — the dedupe story the log's own
/// offsets have, applied to commands.
#[tokio::test]
async fn a_worker_resuming_mid_stream_sees_the_tail_and_no_duplicates() {
    let (_core, mut harness, session_id) = commanded_server("resume").await.expect("a server");
    spawn_agent(&mut harness, &session_id).await.expect("spawn");
    spawn_agent(&mut harness, &session_id).await.expect("spawn");

    let resumed = first_command(&mut harness, "worker-a", 2)
        .await
        .expect("a command");
    assert_eq!(
        resumed.command_sequence, 2,
        "resuming at 2 must not redeliver 1"
    );
}

/// Keeps `DuplexStream` referenced so the import cannot rot unnoticed.
const _: fn(DuplexStream) = |_stream| {};
