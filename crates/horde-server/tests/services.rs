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
    GetSessionRequest, JournaledEvent, JournaledEventBatch, ListSessionsRequest,
    ListWorkspaceDefinitionsRequest, PushEventsRequest, PushEventsStart, RegisterWorkerRequest,
    SessionCursor, SessionId, SpawnAgentRequest, StreamCommandsRequest, SyncEventsRequest,
    WorkerCapabilities, WorkerId, control_plane_service_client::ControlPlaneServiceClient,
    control_plane_service_server::ControlPlaneServiceServer, event::Kind, push_events_request,
    push_events_response, stream_commands_response, sync_events_response,
    worker_service_client::WorkerServiceClient, worker_service_server::WorkerServiceServer,
};
use horde_server::EventLog;
use horde_server::service::{ControlPlane, ServerCore, WorkerFacing};
use tokio::io::DuplexStream;
use tokio_stream::StreamExt;
use tonic::Code;
use tonic::transport::{Endpoint, Server, Uri};

/// Both clients, wired to one server over in-process duplex transports.
struct Harness {
    control: ControlPlaneServiceClient<tonic::transport::Channel>,
    worker: WorkerServiceClient<tonic::transport::Channel>,
}

/// Helpers return `Result` rather than unwrapping.
///
/// `allow-expect-in-tests` exempts anything under `#[cfg(test)]`, which an
/// integration test file's free functions are not — so `expect` here would trip
/// the deny-list. Wrapping the helpers in `#[cfg(test)] mod` would also fix it
/// (see docs/QUALITY_GATES.md); threading the error to a `#[test]` is kept
/// because it also reports *which* setup step failed.
type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn connect(core: ServerCore) -> Setup<Harness> {
    Ok(Harness {
        control: ControlPlaneServiceClient::new(channel_to(core.clone(), true).await?),
        worker: WorkerServiceClient::new(channel_to(core, false).await?),
    })
}

/// Stands up one server on a duplex pair and returns a channel to it.
///
/// Two connections rather than one so each client gets its own transport, which
/// is how they behave in production.
async fn channel_to(core: ServerCore, control_plane: bool) -> Setup<tonic::transport::Channel> {
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);

    tokio::spawn(async move {
        let mut builder = Server::builder();
        let serve = async move {
            if control_plane {
                builder
                    .add_service(ControlPlaneServiceServer::new(ControlPlane::new(core)))
                    .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_side)))
                    .await
            } else {
                builder
                    .add_service(WorkerServiceServer::new(WorkerFacing::new(core)))
                    .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_side)))
                    .await
            }
        };
        let _ = serve.await;
    });

    let mut client_side = Some(client_side);
    let channel = Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let stream = client_side.take();
            async move {
                stream
                    .map(hyper_util::rt::TokioIo::new)
                    .ok_or_else(|| std::io::Error::other("the duplex was already taken"))
            }
        }))
        .await?;
    Ok(channel)
}

fn core() -> Setup<ServerCore> {
    Ok(ServerCore::new(EventLog::in_memory()?))
}

/// Creates a session and returns its id.
///
/// Four tests need a session before they can test anything else; this is that
/// preamble, once.
async fn create_session(harness: &mut Harness, title: &str, key: &str) -> Setup<String> {
    let created = harness
        .control
        .create_session(CreateSessionRequest {
            title: title.to_owned(),
            workspace_definition_id: None,
            root_agent: Some(AgentSpec::default()),
            idempotency_key: key.to_owned(),
        })
        .await?
        .into_inner();
    created
        .session
        .and_then(|summary| summary.session_id)
        .map(|id| id.to_string())
        .ok_or_else(|| "a created session must carry an id".into())
}

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
    let (to_server, from_client) = tokio::sync::mpsc::channel(8);
    let acks = harness
        .worker
        .push_events(tokio_stream::wrappers::ReceiverStream::new(from_client))
        .await?
        .into_inner();
    to_server
        .send(PushEventsRequest {
            body: Some(push_events_request::Body::Start(PushEventsStart {
                worker_id: Some(WorkerId::new(worker_id)),
                last_local_journal_sequence,
            })),
        })
        .await?;
    Ok((to_server, acks))
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
) -> Result<horde_proto::v1::AnswerPermissionResponse, tonic::Status> {
    harness
        .control
        .answer_permission(horde_proto::v1::AnswerPermissionRequest {
            request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
            outcome: horde_proto::v1::PermissionOutcome::Allow as i32,
            reason: reason.to_owned(),
            remember_for_session: false,
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
    let mut harness = connect(core.clone())
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

    // 2. A client creates a session and spawns an agent under it.
    let session_id = create_session(&mut harness, "ci-fix", "key-1")
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
    // 3. The worker receives the spawn command.
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
    let mut stream = control
        .sync_events(SyncEventsRequest {
            cursors: vec![SessionCursor {
                session_id: Some(SessionId::new(session_id)),
                from_sequence,
            }],
            all_sessions: false,
            subtree_root: None,
            // Not following: replay to head and complete, the scriptable form.
            follow: false,
            max_batch_events: 0,
        })
        .await?
        .into_inner();

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
    let mut harness = connect(core).await.expect("the harness must connect");

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

    let session_id = create_session(&mut harness, "first", "key-1")
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
    assert_eq!(fetched.head_sequence, 1);
}

#[tokio::test]
async fn creating_a_session_twice_with_one_key_creates_one_session() {
    let core = core().expect("an in-memory core");
    let mut harness = connect(core).await.expect("the harness must connect");

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
        listed.sessions[0].head_sequence, 1,
        "nor a second event in the one it did create"
    );
}

#[tokio::test]
async fn a_spawn_with_no_worker_is_queued_with_a_reason_rather_than_failing() {
    let core = core().expect("an in-memory core");
    let mut harness = connect(core).await.expect("the harness must connect");

    let session_id = create_session(&mut harness, "lonely", "key-1")
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
    let mut harness = connect(core).await.expect("the harness must connect");

    let status = harness
        .control
        .list_workspace_definitions(ListWorkspaceDefinitionsRequest::default())
        .await
        .expect_err("workspaces are not built");
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(status.message().contains("workspace"));
}

#[tokio::test]
async fn a_worker_that_lost_its_journal_is_refused_over_the_wire() {
    let core = core().expect("an in-memory core");
    let mut harness = connect(core.clone())
        .await
        .expect("the harness must connect");

    register_worker(&mut harness, "worker-a")
        .await
        .expect("registration");

    // Store some journal history directly, then have the worker claim it has
    // less than the server does.
    let session_id = create_session(&mut harness, "s", "key-1")
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

#[tokio::test]
async fn a_permission_is_answered_by_request_id_alone() {
    // The client answering a permission knows the request id and nothing else —
    // not the session, not the worker. Finding the rest is the server's job,
    // which is what makes a permission answerable from any attached client.
    let core = core().expect("an in-memory core");
    let mut harness = connect(core).await.expect("the harness must connect");
    let session_id = create_session(&mut harness, "perms", "key-1")
        .await
        .expect("a session must be creatable");

    let (to_server, mut acks) = open_journal(&mut harness, "worker-a", 0)
        .await
        .expect("the event stream must open");
    acks.next().await.expect("a reply").expect("accepted");

    to_server
        .send(PushEventsRequest {
            body: Some(push_events_request::Body::Batch(JournaledEventBatch {
                events: vec![JournaledEvent {
                    journal_sequence: 1,
                    session_id: Some(SessionId::new(session_id.clone())),
                    event: Some(permission_requested("agent-1", "req-1")),
                }],
                acked_command_sequence: 0,
            })),
        })
        .await
        .expect("the request must journal");
    acks.next().await.expect("a reply").expect("accepted");

    let answered = answer_permission(&mut harness, "req-1", "looks fine")
        .await
        .expect("a pending permission must be answerable");
    assert!(
        answered.sequence > 0,
        "the decision is an event like any other, so it has a sequence"
    );

    // Answering it again fails: the projection dropped it when the decision
    // landed, so there is nothing left awaiting an answer.
    let twice = answer_permission(&mut harness, "req-1", "")
        .await
        .expect_err("an answered permission is no longer pending");
    assert_eq!(twice.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn answering_a_permission_nobody_asked_for_is_refused() {
    let core = core().expect("an in-memory core");
    let mut harness = connect(core).await.expect("the harness must connect");
    create_session(&mut harness, "perms", "key-1")
        .await
        .expect("a session must be creatable");

    let status = answer_permission(&mut harness, "never-asked", "")
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
    let mut harness = connect(core).await.expect("the harness must connect");
    register_worker(&mut harness, "worker-a")
        .await
        .expect("the worker must register");
    let session_id = create_session(&mut harness, "findable", "key-1")
        .await
        .expect("a session must be creatable");
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

/// Keeps `DuplexStream` referenced so the import cannot rot unnoticed.
const _: fn(DuplexStream) = |_stream| {};
