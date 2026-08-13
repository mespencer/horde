//! The placement passes: everything that turns "an agent needs a worker" into
//! placement events in the log.
//!
//! Three passes, all folds over the log followed by appends, per ADR-0001 —
//! there is no queue structure to lose on restart, because the queue *is* the
//! set of agents whose latest placement is not `PLACED`:
//!
//! * [`place_queued`] — runs when the worker pool changes (registration,
//!   capability refresh, disconnection) and at session creation. Places every
//!   agent awaiting placement, expires the ones whose queue timeout has
//!   elapsed, and writes the first `QUEUED` outcome for agents nothing has
//!   answered yet.
//! * [`queue_lost_worker`] — runs when a worker disconnects. Re-queues the
//!   agents that were placed there but had not started running; agents
//!   mid-turn stay placed, because a disconnected worker is a partition, not
//!   a death (ADR-0001), and live agents are never migrated (DESIGN §5).
//! * [`ensure_instance`] — before a placement lands on a worker without a
//!   warm instance of the agent's workspace, records the prewarm request the
//!   command fold turns into a `MaterializeWorkspaceCommand`.
//!
//! Every pass takes [`ServerCore::placement_lock`] so a decision's
//! project-then-append is atomic against concurrent passes; two unserialized
//! passes can both place the same agent, and the fold would command two
//! spawns.
//!
//! # What no pass does
//!
//! Nothing here runs on a timer. Queue-timeout expiry is evaluated whenever a
//! pass runs, which means an expiry is heard when the worker pool next
//! changes rather than at the exact deadline. A clock-driven sweeper would
//! tighten that latency and is deliberately not built yet: a loop bounded
//! only by time is exactly the shape QUALITY_GATES.md warns against, and no
//! caller needs deadline-exact expiry today.

use std::collections::BTreeMap;

use horde_proto::v1::{
    AgentId, AgentLifecycleState, AgentSpec, Event, PlacementState, WorkerId,
    WorkspaceDefinitionId, WorkspaceInstanceReport, WorkspaceInstanceState, event::Kind,
};

use crate::error::ServerError;
use crate::scheduler::{Placement, PlacementRequest};
use crate::service::shared::{ServerCore, append_event};
use crate::state::{AgentState, SessionState};
use crate::workspace::is_catalog_stream;

/// How long a spawn may sit queued when its constraints do not say:
/// one hour. Long enough to bring a worker online by hand, short enough that
/// a forgotten spawn does not haunt the session for days.
pub const DEFAULT_QUEUE_TIMEOUT_MS: i64 = 60 * 60 * 1000;

/// Whether an agent's latest placement still asks for a worker.
///
/// `Unspecified` means no placement event has answered the spawn at all —
/// which is how a session's root agent is born — and `Queued` is an explicit
/// "not yet". Expired and cancelled placements are settled outcomes; folding
/// them back into the queue would resurrect spawns the spawner already heard
/// the end of.
#[must_use]
pub fn awaits_placement(state: PlacementState) -> bool {
    matches!(state, PlacementState::Unspecified | PlacementState::Queued)
}

/// Whether an agent still counts against its worker's declared capacity.
///
/// Everything alive does, including `Idle`: an idle agent's context lives on
/// the worker and revival lands there. Only the two states that end an
/// agent's tenancy release the slot.
#[must_use]
pub fn occupies_capacity(state: AgentLifecycleState) -> bool {
    !matches!(
        state,
        AgentLifecycleState::Archived | AgentLifecycleState::Failed
    )
}

/// Whether a placed agent may be re-queued when its worker disconnects.
///
/// Only an agent that has not begun running: `Queued` and `Starting` mean the
/// spawn command may never have been applied, and the re-queue's cancel
/// command settles the race if the worker returns. Anything further along is
/// mid-conversation — a disconnect is a partition the worker journals
/// through, and live agents are never migrated.
#[must_use]
pub fn survives_only_as_placed(state: AgentLifecycleState) -> bool {
    !matches!(
        state,
        AgentLifecycleState::Queued | AgentLifecycleState::Starting
    )
}

/// The queue timeout a spec asks for, in milliseconds, defaulted.
///
/// A positive sub-millisecond duration rounds up to one millisecond rather
/// than down to "unset": a caller who asked for a tiny timeout asked for a
/// timeout.
#[must_use]
pub fn queue_timeout_ms(spec: Option<&AgentSpec>) -> i64 {
    let requested = spec
        .and_then(|spec| spec.placement.as_ref())
        .and_then(|constraints| constraints.queue_timeout.as_ref());
    let Some(duration) = requested else {
        return DEFAULT_QUEUE_TIMEOUT_MS;
    };

    let millis = duration.seconds.saturating_mul(1_000) + i64::from(duration.nanos) / 1_000_000;
    if millis > 0 {
        millis
    } else if duration.seconds > 0 || duration.nanos > 0 {
        1
    } else {
        DEFAULT_QUEUE_TIMEOUT_MS
    }
}

/// Whether a queued placement's timeout has elapsed.
#[must_use]
pub fn queue_expired(queued_at_unix_ms: i64, timeout_ms: i64, now_unix_ms: i64) -> bool {
    now_unix_ms >= queued_at_unix_ms.saturating_add(timeout_ms)
}

fn now_unix_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(0))
}

/// Projects every session — the catalog stream is not one.
async fn session_states(core: &ServerCore) -> Result<Vec<SessionState>, ServerError> {
    let mut states = Vec::new();
    for session_id in core.log.session_ids().await? {
        if is_catalog_stream(&session_id) {
            continue;
        }
        let events = core.log.read_all(&session_id).await?;
        states.push(SessionState::project(&session_id, &events)?);
    }
    Ok(states)
}

/// Active agents per worker, across every session — the capacity input.
pub async fn worker_load(core: &ServerCore) -> Result<BTreeMap<String, u64>, ServerError> {
    Ok(load_of(&session_states(core).await?))
}

/// The pure half of [`worker_load`], testable without a log.
fn load_of(states: &[SessionState]) -> BTreeMap<String, u64> {
    let mut load = BTreeMap::new();
    for agent in states.iter().flat_map(|state| state.agents.values()) {
        if let Some(worker_id) = agent.worker_id.as_ref()
            && occupies_capacity(agent.lifecycle)
        {
            *load.entry(worker_id.clone()).or_insert(0) += 1;
        }
    }
    load
}

/// The wire form of a scheduler decision, addressed to one agent.
#[must_use]
pub fn wire_placement(agent_id: &str, placement: &Placement) -> horde_proto::v1::Placement {
    match placement {
        Placement::Placed { worker_id } => horde_proto::v1::Placement {
            agent_id: Some(AgentId::new(agent_id)),
            state: PlacementState::Placed as i32,
            worker_id: Some(WorkerId::new(worker_id.clone())),
            reason: String::new(),
        },
        Placement::Queued { reason } => horde_proto::v1::Placement {
            agent_id: Some(AgentId::new(agent_id)),
            state: PlacementState::Queued as i32,
            worker_id: None,
            reason: reason.clone(),
        },
    }
}

/// Appends one placement outcome to its session's log.
pub async fn append_placement(
    core: &ServerCore,
    session_id: &str,
    placement: horde_proto::v1::Placement,
) -> Result<u64, ServerError> {
    let agent_id = placement.agent_id.clone();
    append_event(
        core,
        session_id,
        "",
        Event {
            agent_id,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::PlacementChanged(horde_proto::v1::PlacementChanged {
                placement: Some(placement),
            })),
        },
    )
    .await
}

/// Records the prewarm the command fold turns into a materialize command,
/// unless the worker is already warm — and primes the cache so a second
/// placement in the same pass does not request a second materialization.
///
/// Ordering is the contract here: callers append this *before* the placement
/// event, so the worker's command stream carries materialize-then-spawn.
pub async fn ensure_instance(
    core: &ServerCore,
    session_id: &str,
    workspace_definition_id: Option<&str>,
    worker_id: &str,
) -> Result<(), ServerError> {
    let Some(definition_id) = workspace_definition_id else {
        return Ok(());
    };
    if core.instances.warm_on(worker_id, definition_id) {
        return Ok(());
    }

    append_event(
        core,
        session_id,
        "",
        Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::WorkspacePrewarmRequested(
                horde_proto::v1::WorkspacePrewarmRequested {
                    workspace_definition_id: Some(WorkspaceDefinitionId::new(definition_id)),
                    worker_id: Some(WorkerId::new(worker_id)),
                },
            )),
        },
    )
    .await?;

    core.instances.record(&WorkspaceInstanceReport {
        workspace_definition_id: Some(WorkspaceDefinitionId::new(definition_id)),
        worker_id: Some(WorkerId::new(worker_id)),
        state: WorkspaceInstanceState::Materializing as i32,
        root_path: String::new(),
        repos: Vec::new(),
        detail: "materialization requested".to_owned(),
        observed_at: None,
    });
    Ok(())
}

/// One agent the queue pass decided about, and what it decided.
#[derive(Debug, PartialEq)]
enum Disposition {
    Place(Placement),
    Expire {
        timeout_ms: i64,
    },
    /// Already queued with the same standing answer; appending another queued
    /// event would spam every attached client with no new fact.
    Leave,
}

/// What to do about one agent awaiting placement. Pure, so the whole decision
/// table is testable without a log or a clock.
fn disposition(agent: &AgentState, placement: Placement, now_unix_ms: i64) -> Disposition {
    if let Some(queued_at) = agent.queued_at_unix_ms {
        let timeout_ms = queue_timeout_ms(agent.spec.as_ref());
        if queue_expired(queued_at, timeout_ms, now_unix_ms) {
            return Disposition::Expire { timeout_ms };
        }
    }
    match (&placement, agent.placement_state) {
        // A queued answer for an already-queued agent is no news.
        (Placement::Queued { .. }, PlacementState::Queued) => Disposition::Leave,
        _ => Disposition::Place(placement),
    }
}

/// Places every agent awaiting placement, expiring the overdue.
///
/// Runs under the placement lock. Load is tracked across the pass so two
/// agents placed in one sweep see each other against worker capacity.
pub async fn place_queued(core: &ServerCore) -> Result<(), ServerError> {
    let _serialized = core.placement_lock.lock().await;
    let states = session_states(core).await?;
    let mut load = load_of(&states);
    let now = now_unix_ms();

    for state in &states {
        for agent in state.agents.values() {
            if !awaits_placement(agent.placement_state) {
                continue;
            }

            let spec = agent.spec.clone().unwrap_or_default();
            let constraints = spec.placement.clone().unwrap_or_default();
            let workspace = workspace_of(&spec);
            let parent_worker = parent_worker_of(state, agent);

            let placement = core.scheduler.place(&PlacementRequest {
                constraints: &constraints,
                parent_worker_id: parent_worker.as_deref(),
                workspace_definition_id: workspace,
                load: &load,
            });

            match disposition(agent, placement, now) {
                Disposition::Place(Placement::Placed { worker_id }) => {
                    ensure_instance(core, &state.session_id, workspace, &worker_id).await?;
                    append_placement(
                        core,
                        &state.session_id,
                        wire_placement(
                            &agent.agent_id,
                            &Placement::Placed {
                                worker_id: worker_id.clone(),
                            },
                        ),
                    )
                    .await?;
                    *load.entry(worker_id).or_insert(0) += 1;
                }
                Disposition::Place(queued @ Placement::Queued { .. }) => {
                    append_placement(
                        core,
                        &state.session_id,
                        wire_placement(&agent.agent_id, &queued),
                    )
                    .await?;
                }
                Disposition::Expire { timeout_ms } => {
                    append_placement(
                        core,
                        &state.session_id,
                        horde_proto::v1::Placement {
                            agent_id: Some(AgentId::new(agent.agent_id.clone())),
                            state: PlacementState::Expired as i32,
                            worker_id: None,
                            reason: format!(
                                "queued for longer than the {timeout_ms}ms queue timeout"
                            ),
                        },
                    )
                    .await?;
                }
                Disposition::Leave => {}
            }
        }
    }
    Ok(())
}

/// Re-queues the not-yet-running agents a disconnected worker was holding.
///
/// The queued event's fold-side consequence is a `CancelPlacementCommand` to
/// the lost worker, which settles the at-least-once race if it reconnects:
/// commands are ordered, so it cancels the spawn it may never have applied.
pub async fn queue_lost_worker(core: &ServerCore, worker_id: &str) -> Result<(), ServerError> {
    let _serialized = core.placement_lock.lock().await;
    for state in session_states(core).await? {
        for agent in state.agents.values() {
            let placed_here = agent.placement_state == PlacementState::Placed
                && agent.worker_id.as_deref() == Some(worker_id);
            if !placed_here || survives_only_as_placed(agent.lifecycle) {
                continue;
            }

            append_placement(
                core,
                &state.session_id,
                horde_proto::v1::Placement {
                    agent_id: Some(AgentId::new(agent.agent_id.clone())),
                    state: PlacementState::Queued as i32,
                    worker_id: None,
                    reason: format!("worker `{worker_id}` disconnected before the agent started"),
                },
            )
            .await?;
        }
    }
    Ok(())
}

/// The workspace a spec names, when it names one.
fn workspace_of(spec: &AgentSpec) -> Option<&str> {
    spec.workspace_definition_id
        .as_ref()
        .map(|id| id.value.as_str())
        .filter(|id| !id.is_empty())
}

/// The worker the agent's parent runs on, for stickiness.
fn parent_worker_of(state: &SessionState, agent: &AgentState) -> Option<String> {
    let parent_id = agent.parent_agent_id.as_ref()?;
    state.agents.get(parent_id)?.worker_id.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use horde_proto::v1::PlacementConstraints;

    #[test]
    fn only_unspecified_and_queued_await_placement() {
        assert!(awaits_placement(PlacementState::Unspecified));
        assert!(awaits_placement(PlacementState::Queued));
        assert!(!awaits_placement(PlacementState::Placed));
        assert!(
            !awaits_placement(PlacementState::Expired),
            "an expired spawn is settled; resurrecting it would replay an \
             outcome the spawner already heard"
        );
        assert!(!awaits_placement(PlacementState::Cancelled));
    }

    #[test]
    fn everything_alive_occupies_capacity() {
        for alive in [
            AgentLifecycleState::Queued,
            AgentLifecycleState::Starting,
            AgentLifecycleState::Running,
            AgentLifecycleState::WaitingOnChild,
            AgentLifecycleState::WaitingOnHuman,
            AgentLifecycleState::Idle,
            AgentLifecycleState::Unspecified,
        ] {
            assert!(occupies_capacity(alive), "{alive:?} holds a slot");
        }
        assert!(!occupies_capacity(AgentLifecycleState::Archived));
        assert!(!occupies_capacity(AgentLifecycleState::Failed));
    }

    /// Only agents that never began running are re-queued on worker loss;
    /// everything mid-conversation rides out the partition where it is.
    #[test]
    fn worker_loss_requeues_only_agents_that_never_started() {
        assert!(!survives_only_as_placed(AgentLifecycleState::Queued));
        assert!(!survives_only_as_placed(AgentLifecycleState::Starting));
        assert!(survives_only_as_placed(AgentLifecycleState::Running));
        assert!(survives_only_as_placed(AgentLifecycleState::WaitingOnChild));
        assert!(survives_only_as_placed(AgentLifecycleState::WaitingOnHuman));
        assert!(survives_only_as_placed(AgentLifecycleState::Idle));
        assert!(survives_only_as_placed(AgentLifecycleState::Archived));
        assert!(survives_only_as_placed(AgentLifecycleState::Failed));
        assert!(survives_only_as_placed(AgentLifecycleState::Unspecified));
    }

    fn spec_with_timeout(seconds: i64, nanos: i32) -> AgentSpec {
        AgentSpec {
            placement: Some(PlacementConstraints {
                queue_timeout: Some(prost_types::Duration { seconds, nanos }),
                ..PlacementConstraints::default()
            }),
            ..AgentSpec::default()
        }
    }

    #[test]
    fn the_queue_timeout_defaults_and_rounds_up_rather_than_vanishing() {
        assert_eq!(
            DEFAULT_QUEUE_TIMEOUT_MS, 3_600_000,
            "the default is one hour of milliseconds, as the doc promises"
        );
        assert_eq!(queue_timeout_ms(None), DEFAULT_QUEUE_TIMEOUT_MS);
        assert_eq!(
            queue_timeout_ms(Some(&AgentSpec::default())),
            DEFAULT_QUEUE_TIMEOUT_MS,
            "no constraints means the default"
        );
        assert_eq!(
            queue_timeout_ms(Some(&spec_with_timeout(0, 0))),
            DEFAULT_QUEUE_TIMEOUT_MS,
            "a zero duration is unset, not instant"
        );
        assert_eq!(queue_timeout_ms(Some(&spec_with_timeout(90, 0))), 90_000);
        assert_eq!(
            queue_timeout_ms(Some(&spec_with_timeout(1, 500_000_000))),
            1_500
        );
        assert_eq!(
            queue_timeout_ms(Some(&spec_with_timeout(0, 1))),
            1,
            "a positive sub-millisecond timeout is a timeout, not unset"
        );
        assert_eq!(
            queue_timeout_ms(Some(&spec_with_timeout(1, -1_500_000_000))),
            1,
            "positive seconds keep a mixed-sign duration a timeout — the \
             sign of `seconds`, not its inverse, is what says the caller \
             asked for one"
        );
    }

    #[test]
    fn expiry_is_measured_from_the_queued_moment() {
        assert!(!queue_expired(1_000, 500, 1_499));
        assert!(
            queue_expired(1_000, 500, 1_500),
            "the deadline itself expires"
        );
        assert!(queue_expired(1_000, 500, 2_000));
        assert!(
            queue_expired(i64::MAX, i64::MAX, i64::MAX),
            "saturation cannot wrap into a never-expiring queue"
        );
    }

    fn agent(state: PlacementState, queued_at: Option<i64>) -> AgentState {
        AgentState {
            agent_id: "agent-1".to_owned(),
            parent_agent_id: None,
            lifecycle: AgentLifecycleState::Queued,
            tree_path: Vec::new(),
            agent_type: String::new(),
            worker_id: None,
            spec: Some(spec_with_timeout(0, 1_000_000)),
            placement_state: state,
            queued_at_unix_ms: queued_at,
        }
    }

    #[test]
    fn a_placed_answer_is_always_written() {
        let placed = Placement::Placed {
            worker_id: "w1".to_owned(),
        };
        assert_eq!(
            disposition(
                &agent(PlacementState::Queued, Some(1_000)),
                placed.clone(),
                1_000
            ),
            Disposition::Place(placed)
        );
    }

    #[test]
    fn a_repeated_queued_answer_is_not_re_written() {
        let queued = Placement::Queued {
            reason: "no worker is connected".to_owned(),
        };
        assert_eq!(
            disposition(
                &agent(PlacementState::Queued, Some(1_000)),
                queued.clone(),
                1_000
            ),
            Disposition::Leave,
            "the standing queued event already says this"
        );
        assert_eq!(
            disposition(
                &agent(PlacementState::Unspecified, None),
                queued.clone(),
                1_000
            ),
            Disposition::Place(queued),
            "the first queued answer is news the spawner must hear"
        );
    }

    #[test]
    fn an_overdue_agent_expires_even_if_a_worker_now_matches() {
        let placed = Placement::Placed {
            worker_id: "w1".to_owned(),
        };
        assert_eq!(
            disposition(&agent(PlacementState::Queued, Some(1_000)), placed, 2_000),
            Disposition::Expire { timeout_ms: 1 },
            "the timeout elapsed while nothing matched; placing now would \
             deliver an outcome the spawner may have stopped waiting for"
        );
    }

    #[test]
    fn an_agent_never_yet_queued_cannot_expire() {
        let placed = Placement::Placed {
            worker_id: "w1".to_owned(),
        };
        assert_eq!(
            disposition(
                &agent(PlacementState::Unspecified, None),
                placed.clone(),
                i64::MAX
            ),
            Disposition::Place(placed),
            "expiry is measured from a queued event, and there is none"
        );
    }

    #[test]
    fn load_counts_placed_living_agents_per_worker() {
        let mut state = SessionState {
            session_id: "s1".to_owned(),
            ..SessionState::default()
        };
        let mut with = |id: &str, worker: Option<&str>, lifecycle| {
            state.agents.insert(
                id.to_owned(),
                AgentState {
                    agent_id: id.to_owned(),
                    parent_agent_id: None,
                    lifecycle,
                    tree_path: Vec::new(),
                    agent_type: String::new(),
                    worker_id: worker.map(ToString::to_string),
                    spec: None,
                    placement_state: PlacementState::Placed,
                    queued_at_unix_ms: None,
                },
            );
        };
        with("a", Some("w1"), AgentLifecycleState::Running);
        with("b", Some("w1"), AgentLifecycleState::Idle);
        with("c", Some("w1"), AgentLifecycleState::Archived);
        with("d", Some("w2"), AgentLifecycleState::Queued);
        with("e", None, AgentLifecycleState::Running);

        let load = load_of(&[state]);
        assert_eq!(load.get("w1"), Some(&2), "archived releases its slot");
        assert_eq!(load.get("w2"), Some(&1));
        assert_eq!(load.len(), 2, "an unplaced agent loads no worker");
    }

    #[test]
    fn a_wire_placement_carries_the_worker_or_the_reason_never_both() {
        let placed = wire_placement(
            "agent-1",
            &Placement::Placed {
                worker_id: "w1".to_owned(),
            },
        );
        assert_eq!(placed.state, PlacementState::Placed as i32);
        assert_eq!(
            placed
                .worker_id
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("w1")
        );
        assert_eq!(placed.reason, "");
        assert_eq!(
            placed.agent_id.as_ref().map(ToString::to_string).as_deref(),
            Some("agent-1")
        );

        let queued = wire_placement(
            "agent-1",
            &Placement::Queued {
                reason: "no worker is connected".to_owned(),
            },
        );
        assert_eq!(queued.state, PlacementState::Queued as i32);
        assert_eq!(queued.worker_id, None);
        assert_eq!(queued.reason, "no worker is connected");
    }

    /// The drain pass carries the parent's worker into the scheduler: a
    /// re-placed child prefers where its parent runs even when another worker
    /// is less loaded. The parent fact has to come from the projection —
    /// a pass that loses it falls back to the load tiebreak and lands the
    /// child on the idle worker instead.
    #[tokio::test]
    async fn a_drained_child_prefers_its_parents_worker_over_the_idle_one() {
        use crate::service::shared::{ServerCore, append_event};
        use horde_proto::v1::{AgentId, AgentSpawned, Event, WorkerCapabilities, event::Kind};

        let core = ServerCore::new(crate::EventLog::in_memory().expect("an in-memory log"));
        core.workers
            .register("alpha", WorkerCapabilities::default());
        core.workers.register("beta", WorkerCapabilities::default());

        let spawn = |agent: &str, parent: Option<&str>| Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::AgentSpawned(AgentSpawned {
                parent_agent_id: parent.map(AgentId::new),
                spawned_by: None,
                spec: Some(AgentSpec::default()),
                tree_path: Vec::new(),
            })),
        };

        // The parent already runs on beta; the child has no placement yet.
        append_event(&core, "s1", "", spawn("parent", None))
            .await
            .expect("append the parent");
        append_placement(
            &core,
            "s1",
            wire_placement(
                "parent",
                &Placement::Placed {
                    worker_id: "beta".to_owned(),
                },
            ),
        )
        .await
        .expect("place the parent");
        append_event(&core, "s1", "", spawn("child", Some("parent")))
            .await
            .expect("append the child");

        place_queued(&core).await.expect("the drain pass");

        let events = core.log.read_all("s1").await.expect("a readable log");
        let state = SessionState::project("s1", &events).expect("project");
        assert_eq!(
            state.agents["child"].worker_id.as_deref(),
            Some("beta"),
            "beta holds the parent and one agent; idle alpha would win the \
             load tiebreak if the parent fact were dropped"
        );
    }

    #[test]
    fn a_spec_names_a_workspace_only_when_the_id_is_non_empty() {
        assert_eq!(workspace_of(&AgentSpec::default()), None);
        let named = AgentSpec {
            workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-1")),
            ..AgentSpec::default()
        };
        assert_eq!(workspace_of(&named), Some("ws-1"));
        let blank = AgentSpec {
            workspace_definition_id: Some(WorkspaceDefinitionId::new("")),
            ..AgentSpec::default()
        };
        assert_eq!(workspace_of(&blank), None, "an empty id names nothing");
    }
}
