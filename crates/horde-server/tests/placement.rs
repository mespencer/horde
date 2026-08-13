//! Placement over the wire: queue drains, worker loss, expiry, and the
//! materialize-before-spawn command order.
//!
//! `scheduler.rs` and `placement.rs` unit-test the decisions; these tests hold
//! the *passes* to their observable promises — what lands in the log, what a
//! worker's command stream carries, and in what order — because a pass that
//! decides correctly and appends nothing is indistinguishable from a working
//! one in any unit test of the decision.

use horde_proto::v1::{
    AgentSpec, CreateWorkspaceDefinitionRequest, PlacementConstraints, PlacementState,
    RegisterWorkerRequest, SessionId, SpawnAgentRequest, StreamCommandsRequest, WorkerCapabilities,
    WorkerId, event::Kind, stream_commands_response, sync_events_response,
};
use tokio_stream::StreamExt;

mod common;

use common::{Harness, Setup, connect, open_sync, seed};

/// One placement fact, as read back from the log.
#[derive(Debug, PartialEq)]
struct Observed {
    agent_id: String,
    state: PlacementState,
    worker_id: Option<String>,
}

/// Follows a session's log until it holds `minimum` placement events, bounded
/// by a read ceiling — every `next()` is one unit of progress, and a log that
/// stops moving fails on the per-read clock with the count in the message.
async fn placements_until(
    harness: &mut Harness,
    session_id: &str,
    minimum: usize,
) -> Setup<Vec<Observed>> {
    let mut stream = open_sync(&mut harness.control, session_id, 0, true).await?;

    let mut observed = Vec::new();
    for _ in 0_u32..256 {
        if observed.len() >= minimum {
            return Ok(observed);
        }
        let message = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await?
            .ok_or("the stream ended early")??;
        let Some(update) = message.update else {
            continue;
        };
        let sync_events_response::Update::Batch(batch) = update else {
            continue;
        };
        for envelope in batch.events {
            let Some(Kind::PlacementChanged(changed)) = envelope.event.and_then(|event| event.kind)
            else {
                continue;
            };
            let Some(placement) = changed.placement else {
                continue;
            };
            observed.push(Observed {
                agent_id: placement
                    .agent_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                state: PlacementState::try_from(placement.state)
                    .unwrap_or(PlacementState::Unspecified),
                worker_id: placement.worker_id.as_ref().map(ToString::to_string),
            });
        }
    }
    Err(format!("only {} placement events arrived", observed.len()).into())
}

async fn register(
    harness: &mut Harness,
    worker: &str,
    capabilities: WorkerCapabilities,
) -> Setup<()> {
    harness
        .worker
        .register_worker(RegisterWorkerRequest {
            hello: None,
            worker_id: Some(WorkerId::new(worker)),
            capabilities: Some(capabilities),
        })
        .await?;
    Ok(())
}

async fn spawn(harness: &mut Harness, session_id: &str, spec: AgentSpec) -> Setup<String> {
    let spawned = harness
        .control
        .spawn_agent(SpawnAgentRequest {
            session_id: Some(SessionId::new(session_id)),
            parent_agent_id: None,
            spec: Some(spec),
            idempotency_key: String::new(),
        })
        .await?
        .into_inner();
    spawned
        .agent_id
        .map(|id| id.to_string())
        .ok_or_else(|| "a spawned agent has an id".into())
}

/// The queue drains when the pool changes: a session created with no worker
/// queues its root, and the worker registering afterwards is what places it.
#[tokio::test]
async fn a_registration_places_what_was_queued_before_it() {
    let mut harness = connect().await.expect("a harness");
    let (session_id, root) = seed(&mut harness, "queued-first", "key-1")
        .await
        .expect("a session");

    let queued = placements_until(&mut harness, &session_id, 1)
        .await
        .expect("the queued answer");
    assert_eq!(
        queued[0],
        Observed {
            agent_id: root.clone(),
            state: PlacementState::Queued,
            worker_id: None,
        },
        "with no worker the root queues, and the spawner hears it"
    );

    register(&mut harness, "late-worker", WorkerCapabilities::default())
        .await
        .expect("register");

    let placed = placements_until(&mut harness, &session_id, 2)
        .await
        .expect("the placement");
    assert_eq!(
        placed[1],
        Observed {
            agent_id: root,
            state: PlacementState::Placed,
            worker_id: Some("late-worker".to_owned()),
        },
        "registration is the moment a queued agent becomes placeable"
    );
}

/// Re-placement on worker loss (DESIGN §6): a worker whose command stream is
/// gone loses its never-started agents back to the queue, the replacement
/// worker gets their spawns, and the lost worker's replayable stream carries
/// the cancels that settle the at-least-once race.
#[tokio::test]
async fn a_lost_workers_unstarted_agents_are_requeued_and_replaced() {
    let mut harness = connect().await.expect("a harness");
    register(&mut harness, "worker-a", WorkerCapabilities::default())
        .await
        .expect("register");
    let (session_id, root) = seed(&mut harness, "loss", "key-1")
        .await
        .expect("a session");

    // The root is placed on worker-a; its stream delivers the spawn, then the
    // worker vanishes without ceremony.
    let mut commands = harness
        .worker
        .stream_commands(StreamCommandsRequest {
            worker_id: Some(WorkerId::new("worker-a")),
            resume_from_command_sequence: 1,
        })
        .await
        .expect("the command stream opens")
        .into_inner();
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), commands.next())
        .await
        .expect("a command arrives")
        .expect("the stream is open")
        .expect("not an error");
    assert!(matches!(
        first.command,
        Some(stream_commands_response::Command::SpawnAgent(_))
    ));
    drop(commands);

    // The server only notices a lazy disconnect when it next tries to send —
    // which this spawn provides: it is placed on worker-a (still registered),
    // the dead stream fails to deliver it, and the loss machinery runs.
    let orphan = spawn(&mut harness, &session_id, AgentSpec::default())
        .await
        .expect("a spawn");

    // Events 1..=4: root queued? no — root placed at creation, orphan placed.
    // After the loss: both are re-queued (neither ever started), and once
    // worker-b registers, both land there.
    let requeued = placements_until(&mut harness, &session_id, 4)
        .await
        .expect("the re-queues");
    let requeued_agents: Vec<&str> = requeued[2..]
        .iter()
        .map(|observed| observed.agent_id.as_str())
        .collect();
    assert!(
        requeued[2..]
            .iter()
            .all(|observed| observed.state == PlacementState::Queued),
        "both never-started agents fall back to the queue: {requeued:?}"
    );
    assert!(
        requeued_agents.contains(&root.as_str()) && requeued_agents.contains(&orphan.as_str()),
        "the lost worker held exactly these two: {requeued_agents:?}"
    );

    register(&mut harness, "worker-b", WorkerCapabilities::default())
        .await
        .expect("register the replacement");
    let replaced = placements_until(&mut harness, &session_id, 6)
        .await
        .expect("the re-placements");
    assert!(
        replaced[4..]
            .iter()
            .all(|observed| observed.state == PlacementState::Placed
                && observed.worker_id.as_deref() == Some("worker-b")),
        "the replacement worker inherits the queue: {replaced:?}"
    );

    // The lost worker's stream, replayed from the start, ends in the cancels
    // that make an at-least-once replay of its spawns harmless.
    let mut replay = harness
        .worker
        .stream_commands(StreamCommandsRequest {
            worker_id: Some(WorkerId::new("worker-a")),
            resume_from_command_sequence: 1,
        })
        .await
        .expect("the replay stream opens")
        .into_inner();
    let mut cancelled = Vec::new();
    for _ in 0_u32..8 {
        let command = tokio::time::timeout(std::time::Duration::from_secs(5), replay.next())
            .await
            .expect("a command arrives")
            .expect("the stream is open")
            .expect("not an error");
        if let Some(stream_commands_response::Command::CancelPlacement(cancel)) = command.command {
            cancelled.push(
                cancel
                    .agent_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            );
        }
        if cancelled.len() == 2 {
            break;
        }
    }
    assert!(
        cancelled.contains(&root) && cancelled.contains(&orphan),
        "each moved agent is cancelled on the worker that lost it: {cancelled:?}"
    );
}

/// Expiry is evaluated when the pool changes: a spawn with a tiny queue
/// timeout that nothing could place expires — with the timeout in the reason —
/// instead of being handed to a worker that arrived too late.
#[tokio::test]
async fn an_overdue_queued_spawn_expires_when_the_pool_changes() {
    let mut harness = connect().await.expect("a harness");
    let (session_id, _root) = seed(&mut harness, "expiry", "key-1")
        .await
        .expect("a session");

    let impatient = AgentSpec {
        placement: Some(PlacementConstraints {
            queue_timeout: Some(prost_types::Duration {
                seconds: 0,
                nanos: 1_000_000, // one millisecond
            }),
            ..PlacementConstraints::default()
        }),
        ..AgentSpec::default()
    };
    let agent_id = spawn(&mut harness, &session_id, impatient)
        .await
        .expect("the spawn queues");

    // Let the millisecond elapse for real before the pool changes.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    register(&mut harness, "too-late", WorkerCapabilities::default())
        .await
        .expect("register");

    // Root queued, agent queued, root placed, agent expired — order among the
    // last two is the pass's iteration order, so find rather than index.
    let observed = placements_until(&mut harness, &session_id, 4)
        .await
        .expect("the expiry");
    let expired = observed
        .iter()
        .find(|observed| observed.agent_id == agent_id && observed.state == PlacementState::Expired)
        .unwrap_or_else(|| panic!("no expiry in {observed:?}"));
    assert_eq!(
        expired.worker_id, None,
        "an expired spawn names no worker, however many are connected now"
    );
}

/// The workspace path end to end: a spawn naming a definition lands on the
/// supporting worker, whose command stream carries the full-definition
/// materialize command *before* the spawn — and a second spawn on the same
/// warm workspace materializes nothing.
#[tokio::test]
async fn a_workspace_spawn_is_materialize_then_spawn_and_warmth_is_reused() {
    let mut harness = connect().await.expect("a harness");
    register(
        &mut harness,
        "ws-worker",
        WorkerCapabilities {
            supports_workspaces: true,
            ..WorkerCapabilities::default()
        },
    )
    .await
    .expect("register");

    let definition = harness
        .control
        .create_workspace_definition(CreateWorkspaceDefinitionRequest {
            name: "atlas".to_owned(),
            repos: vec![horde_proto::v1::WorkspaceRepo {
                name: "atlas".to_owned(),
                remote_url: "https://example.com/atlas.git".to_owned(),
                default_branch: "main".to_owned(),
                layout_path: String::new(),
            }],
            unmanaged_paths: Vec::new(),
        })
        .await
        .expect("create")
        .into_inner()
        .definition
        .expect("a definition");
    let definition_id = definition.workspace_definition_id.clone().expect("an id");

    let (session_id, _root) = seed(&mut harness, "ws", "key-1").await.expect("a session");

    let workspace_spec = AgentSpec {
        workspace_definition_id: Some(definition_id.clone()),
        ..AgentSpec::default()
    };
    spawn(&mut harness, &session_id, workspace_spec.clone())
        .await
        .expect("the workspace spawn");
    spawn(&mut harness, &session_id, workspace_spec)
        .await
        .expect("a second spawn on the same workspace");

    let mut commands = harness
        .worker
        .stream_commands(StreamCommandsRequest {
            worker_id: Some(WorkerId::new("ws-worker")),
            resume_from_command_sequence: 1,
        })
        .await
        .expect("the command stream opens")
        .into_inner();

    let mut kinds = Vec::new();
    for _ in 0_u32..4 {
        let command = tokio::time::timeout(std::time::Duration::from_secs(5), commands.next())
            .await
            .expect("a command arrives")
            .expect("the stream is open")
            .expect("not an error");
        match command.command {
            Some(stream_commands_response::Command::MaterializeWorkspace(materialize)) => {
                let carried = materialize
                    .definition
                    .expect("the full definition rides along");
                assert_eq!(
                    carried.name, "atlas",
                    "a worker must be able to materialize without a round trip"
                );
                kinds.push("materialize");
            }
            Some(stream_commands_response::Command::SpawnAgent(_)) => kinds.push("spawn"),
            other => panic!("unexpected command {other:?}"),
        }
    }

    assert_eq!(
        kinds,
        vec!["spawn", "materialize", "spawn", "spawn"],
        "the root's spawn first; then materialize-before-spawn exactly once — \
         the second workspace spawn colocates onto warmth instead of \
         materializing again"
    );
}
