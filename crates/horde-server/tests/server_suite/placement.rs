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

use crate::common::{Harness, Setup, connect, open_sync, seed};

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
            hello: crate::common::versioned_hello(),
            worker_id: Some(WorkerId::new(worker)),
            capabilities: Some(capabilities),
            last_local_journal_sequence: None,
        })
        .await?;
    Ok(())
}

/// Spawns under `parent` (the root when `None`), returning the agent id and
/// the placement the caller was answered with.
async fn spawn_under(
    harness: &mut Harness,
    session_id: &str,
    parent: Option<&str>,
    spec: AgentSpec,
) -> Setup<(String, horde_proto::v1::Placement)> {
    let spawned = harness
        .control
        .spawn_agent(SpawnAgentRequest {
            session_id: Some(SessionId::new(session_id)),
            parent_agent_id: parent.map(horde_proto::v1::AgentId::new),
            spec: Some(spec),
            idempotency_key: String::new(),
        })
        .await?
        .into_inner();
    let placement = spawned
        .placement
        .ok_or("a spawn always answers placement")?;
    let agent_id = spawned
        .agent_id
        .map(|id| id.to_string())
        .ok_or("a spawned agent has an id")?;
    Ok((agent_id, placement))
}

async fn spawn(harness: &mut Harness, session_id: &str, spec: AgentSpec) -> Setup<String> {
    Ok(spawn_under(harness, session_id, None, spec).await?.0)
}

/// Capacity is measured from the log, per spawn, over the wire: a worker at
/// its declared limit refuses the next spawn with the arithmetic in the
/// reason. This is the load computation's only observable edge — a scheduler
/// handed an empty load map places here and never says the numbers.
#[tokio::test]
async fn a_spawn_against_a_full_worker_queues_with_the_arithmetic() {
    let mut harness = connect().await.expect("a harness");
    register(
        &mut harness,
        "small",
        WorkerCapabilities {
            max_concurrent_agents: 1,
            ..WorkerCapabilities::default()
        },
    )
    .await
    .expect("register");

    // The root agent takes the only slot at creation.
    let (session_id, _root) = seed(&mut harness, "full", "key-1")
        .await
        .expect("a session");

    let (_agent, placement) = spawn_under(&mut harness, &session_id, None, AgentSpec::default())
        .await
        .expect("the spawn is answered");
    assert_eq!(placement.state, PlacementState::Queued as i32);
    assert_eq!(
        placement.reason, "no connected worker matches: small is at capacity (1/1)",
        "the load is counted from the log, and the refusal shows the count"
    );
}

/// One drain pass tracks the load it is creating: two queued agents draining
/// onto a one-slot worker place exactly one of them, and the other waits for
/// the next worker.
#[tokio::test]
async fn a_drain_pass_counts_its_own_placements_against_capacity() {
    let mut harness = connect().await.expect("a harness");
    let (session_id, _root) = seed(&mut harness, "drain", "key-1")
        .await
        .expect("a session");
    spawn(&mut harness, &session_id, AgentSpec::default())
        .await
        .expect("a second queued spawn");

    register(
        &mut harness,
        "small",
        WorkerCapabilities {
            max_concurrent_agents: 1,
            ..WorkerCapabilities::default()
        },
    )
    .await
    .expect("register the one-slot worker");

    // Two queued answers, then exactly one placement.
    let after_small = placements_until(&mut harness, &session_id, 3)
        .await
        .expect("the first drain");
    let on_small = after_small
        .iter()
        .filter(|observed| observed.worker_id.as_deref() == Some("small"))
        .count();
    assert_eq!(
        on_small, 1,
        "one slot, one placement — a pass that does not count its own \
         placements would seat both: {after_small:?}"
    );

    register(&mut harness, "big", WorkerCapabilities::default())
        .await
        .expect("register the unlimited worker");
    let after_big = placements_until(&mut harness, &session_id, 4)
        .await
        .expect("the second drain");
    assert_eq!(
        after_big
            .iter()
            .filter(|observed| observed.worker_id.as_deref() == Some("big"))
            .count(),
        1,
        "the agent the full worker refused lands on the next one: {after_big:?}"
    );
}

/// The parent's worker is preferred when nothing forces otherwise
/// (DESIGN §6) — through the RPC, not just the scheduler unit.
#[tokio::test]
async fn a_child_lands_beside_its_parent_when_nothing_forces_otherwise() {
    let mut harness = connect().await.expect("a harness");
    register(&mut harness, "alpha", WorkerCapabilities::default())
        .await
        .expect("register");
    register(&mut harness, "beta", WorkerCapabilities::default())
        .await
        .expect("register");
    let (session_id, _root) = seed(&mut harness, "sticky", "key-1")
        .await
        .expect("a session");

    // Pin the parent onto the worker the tiebreak would not choose, so the
    // child's preference is distinguishable from alphabetical accident.
    let pinned = AgentSpec {
        placement: Some(PlacementConstraints {
            pinned_worker_id: Some(WorkerId::new("beta")),
            ..PlacementConstraints::default()
        }),
        ..AgentSpec::default()
    };
    let (parent, placement) = spawn_under(&mut harness, &session_id, None, pinned)
        .await
        .expect("the pinned spawn");
    assert_eq!(
        placement
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("beta")
    );

    let (_child, placement) = spawn_under(
        &mut harness,
        &session_id,
        Some(&parent),
        AgentSpec::default(),
    )
    .await
    .expect("the child spawn");
    assert_eq!(
        placement
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("beta"),
        "the child prefers its parent's worker over the alphabetical tiebreak"
    );
}

/// A session whose root names a workspace the catalog does not hold is
/// refused before it exists — the same check every spawn makes.
#[tokio::test]
async fn a_session_rooted_in_an_unknown_workspace_is_refused() {
    let mut harness = connect().await.expect("a harness");

    let refused = harness
        .control
        .create_session(horde_proto::v1::CreateSessionRequest {
            settings: None,
            title: "ghost".to_owned(),
            workspace_definition_id: None,
            root_agent: Some(AgentSpec {
                workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(
                    "ws-ghost",
                )),
                ..AgentSpec::default()
            }),
            idempotency_key: String::new(),
        })
        .await
        .expect_err("an unknown workspace must refuse the session");
    assert_eq!(refused.code(), tonic::Code::NotFound);
    assert!(
        refused.message().contains("ws-ghost"),
        "the refusal names what was asked for: {}",
        refused.message()
    );
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

/// A spawn requiring a provider account queues, with the account named,
/// against a worker that holds none — and places when that same worker
/// registers again advertising it, which is what a worker does after
/// `horde worker login --account` signs the account in.
#[tokio::test]
async fn a_spawn_requiring_an_account_places_once_its_worker_advertises_it() {
    let mut harness = connect().await.expect("a harness");
    register(&mut harness, "acct", WorkerCapabilities::default())
        .await
        .expect("register without the account");
    let (session_id, _root) = seed(&mut harness, "accounts", "key-1")
        .await
        .expect("a session");

    let needs_work = AgentSpec {
        placement: Some(PlacementConstraints {
            required_provider_accounts: vec!["work".to_owned()],
            ..PlacementConstraints::default()
        }),
        ..AgentSpec::default()
    };
    let (agent, placement) = spawn_under(&mut harness, &session_id, None, needs_work)
        .await
        .expect("the spawn is answered");
    assert_eq!(placement.state, PlacementState::Queued as i32);
    assert!(
        placement
            .reason
            .contains("lacks provider accounts [`work`]"),
        "{}",
        placement.reason
    );

    register(
        &mut harness,
        "acct",
        WorkerCapabilities {
            provider_accounts: vec![horde_proto::v1::ProviderAccountCapability {
                provider: "claude".to_owned(),
                account: "work".to_owned(),
                ..horde_proto::v1::ProviderAccountCapability::default()
            }],
            ..WorkerCapabilities::default()
        },
    )
    .await
    .expect("re-register holding the account");
    let observed = placements_until(&mut harness, &session_id, 3)
        .await
        .expect("the drain");
    assert_eq!(
        observed.last(),
        Some(&Observed {
            agent_id: agent,
            state: PlacementState::Placed,
            worker_id: Some("acct".to_owned()),
        }),
        "the advertised account is what placement matched: {observed:?}"
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
            resume_after_log_position: 0,
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

    // The server notices the stream is gone the moment the transport drops
    // it (ADR-0011) — no command has to fail first — so the never-started
    // root falls back to the queue on its own.
    let requeued = placements_until(&mut harness, &session_id, 2)
        .await
        .expect("the re-queue");
    assert_eq!(
        requeued[1],
        Observed {
            agent_id: root.clone(),
            state: PlacementState::Queued,
            worker_id: None,
        },
        "the never-started root falls back to the queue: {requeued:?}"
    );

    // With worker-a gone from placement, a new spawn has nowhere to go.
    let orphan = spawn(&mut harness, &session_id, AgentSpec::default())
        .await
        .expect("a spawn");
    let queued = placements_until(&mut harness, &session_id, 3)
        .await
        .expect("the orphan's answer");
    assert_eq!(queued[2].agent_id, orphan);
    assert_eq!(queued[2].state, PlacementState::Queued);

    register(&mut harness, "worker-b", WorkerCapabilities::default())
        .await
        .expect("register the replacement");
    let replaced = placements_until(&mut harness, &session_id, 5)
        .await
        .expect("the re-placements");
    assert!(
        replaced[3..]
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
            resume_after_log_position: 0,
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
        if !cancelled.is_empty() {
            break;
        }
    }
    assert_eq!(
        cancelled,
        vec![root],
        "the moved agent is cancelled on the worker that lost it"
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
                ..horde_proto::v1::WorkspaceRepo::default()
            }],
            unmanaged_paths: Vec::new(),
            ..horde_proto::v1::CreateWorkspaceDefinitionRequest::default()
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
            resume_after_log_position: 0,
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
