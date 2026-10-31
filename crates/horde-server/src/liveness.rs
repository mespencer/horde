//! Worker liveness: the worker-lost lease, boot reconciliation, and the
//! journal reset (ADR-0011).
//!
//! A disconnected worker is a partition, not a death (ADR-0001): its agents
//! keep running and it journals through. But "not a death" cannot be forever.
//! Before this module, an agent placed on a worker that never came back — or
//! on any worker at all when the server restarted, since a fresh process
//! holds no registrations — stayed `Placed` until the heat death of the log:
//! no command could reach it and nothing would ever say so.
//!
//! # The lease
//!
//! When a worker's command stream ends ([`start_lease`]), or when the server
//! boots and finds agents placed on workers nobody has registered
//! ([`reconcile_at_boot`]), the worker gets one lease to come back. A worker
//! that re-registers inside it bumps its registration epoch and the lapsed
//! check finds nothing to do. One that does not is declared lost
//! ([`declare_worker_lost`]): agents that never started are re-queued and
//! re-placed — a fresh spawn is exactly what they were waiting for — while
//! agents that had started are marked `LOST` and `FAILED` in the log, because
//! their context lived on that worker and a fresh spawn elsewhere would
//! silently re-run half-finished work. Every one of those transitions is an
//! appended event; the lease itself is a timer, and timers are not facts.
//!
//! # The journal reset
//!
//! A worker whose state directory was lost reports a journal head below what
//! the log already stored, and is refused — renumbering its events under old
//! positions would forge history. [`reset_worker_journal`] is the operator's
//! explicit way through: it journals a `WorkerJournalReset` naming the
//! highest position stored (the *floor*), and from then on the worker's
//! journal positions land in the log above that floor ([`journal_floor`]), its
//! command stream starts over at sequence one (the command fold reads the
//! same event), and the agents it held are declared lost — its fresh state
//! knows nothing of them.

use std::collections::BTreeSet;
use std::time::Duration;

use horde_proto::v1::{
    Actor, AgentId, AgentLifecycleChanged, AgentLifecycleState, Event, PlacementState, WorkerId,
    WorkerJournalReset, event::Kind,
};
use prost::Message;

use crate::error::ServerError;
use crate::log::{EventLog, StoredEvent};
use crate::placement;
use crate::service::shared::{ServerCore, append_server_event};
use crate::state::AgentState;

/// How long a worker may stay away before its agents are declared lost:
/// two minutes. Long enough to ride out a laptop lid, a Wi-Fi handover, or a
/// worker restart; short enough that a dead machine is noticed while someone
/// still cares about its agents.
pub const DEFAULT_WORKER_LEASE: Duration = Duration::from_secs(120);

/// The reserved stream journal resets are recorded in.
pub const WORKER_JOURNAL_STREAM: &str = "horde-worker-journal";

/// Whether a log stream is the worker-journal stream.
#[must_use]
pub fn is_worker_journal_stream(session_id: &str) -> bool {
    session_id == WORKER_JOURNAL_STREAM
}

/// The journal floor the latest reset of `worker_id` set, or zero: what the
/// worker's own journal positions are offset by in the log.
#[must_use]
pub fn floor_in(events: &[StoredEvent], worker_id: &str) -> u64 {
    events
        .iter()
        .filter_map(|stored| Event::decode(stored.payload.as_slice()).ok())
        .filter_map(|event| match event.kind {
            Some(Kind::WorkerJournalReset(reset))
                if reset.worker_id.as_ref().map(ToString::to_string).as_deref()
                    == Some(worker_id) =>
            {
                Some(reset.journal_floor)
            }
            _ => None,
        })
        .last()
        .unwrap_or(0)
}

/// [`floor_in`], read from the log.
///
/// # Errors
///
/// The log failing to read.
pub async fn journal_floor(log: &EventLog, worker_id: &str) -> Result<u64, ServerError> {
    Ok(floor_in(
        &log.read_all(WORKER_JOURNAL_STREAM).await?,
        worker_id,
    ))
}

/// The highest journal position stored for a worker, in the numbering of
/// its *current* journal — the log's watermark less the floor of its latest
/// reset. What registration and `PushEventsStart` compare a worker's report
/// against, and answer its resume point from.
///
/// # Errors
///
/// The log failing to read.
pub async fn own_watermark(log: &EventLog, worker_id: &str) -> Result<u64, ServerError> {
    let stored = log.worker_journal_watermark(worker_id).await?;
    let floor = journal_floor(log, worker_id).await?;
    Ok(stored.saturating_sub(floor))
}

/// What a worker's loss does to one agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Loss {
    /// Not this worker's, or already over: nothing.
    Untouched,
    /// Never started: back to the queue, to be spawned afresh elsewhere.
    Requeue,
    /// Started: its context died with the worker. `fail` is whether its
    /// lifecycle still has to say so.
    Lose { fail: bool },
}

/// The loss table for one agent, pure.
fn loss_of(agent: &AgentState, worker_id: &str) -> Loss {
    let placed_here = agent.placement_state == PlacementState::Placed
        && agent.worker_id.as_deref() == Some(worker_id);
    if !placed_here || agent.lifecycle == AgentLifecycleState::Archived {
        return Loss::Untouched;
    }
    if !placement::survives_only_as_placed(agent.lifecycle) {
        return Loss::Requeue;
    }
    Loss::Lose {
        fail: agent.lifecycle != AgentLifecycleState::Failed,
    }
}

/// Declares a worker lost: re-queues the agents it never started, marks the
/// ones it had started lost and failed, and runs a placement pass. Returns the
/// agents declared lost.
///
/// # Errors
///
/// The log failing to read or append.
pub async fn declare_worker_lost(
    core: &ServerCore,
    worker_id: &str,
    reason: &str,
) -> Result<Vec<String>, ServerError> {
    let mut lost = Vec::new();
    {
        let _serialized = core.placement_lock.lock().await;
        for state in placement::session_states(core).await? {
            for agent in state.agents.values() {
                match loss_of(agent, worker_id) {
                    Loss::Untouched => {}
                    Loss::Requeue => {
                        requeue(core, &state.session_id, &agent.agent_id, reason).await?;
                    }
                    Loss::Lose { fail } => {
                        lose(core, &state.session_id, agent, fail, reason).await?;
                        lost.push(agent.agent_id.clone());
                    }
                }
            }
        }
    }
    tracing::warn!(worker_id, lost = lost.len(), reason, "worker declared lost");
    placement::place_queued(core).await?;
    Ok(lost)
}

/// Returns a never-started agent to the queue.
async fn requeue(
    core: &ServerCore,
    session_id: &str,
    agent_id: &str,
    reason: &str,
) -> Result<(), ServerError> {
    placement::append_placement(
        core,
        session_id,
        horde_proto::v1::Placement {
            agent_id: Some(AgentId::new(agent_id)),
            state: PlacementState::Queued as i32,
            worker_id: None,
            reason: format!("{reason}; the agent had not started, so it is re-placed"),
        },
        None,
    )
    .await?;
    Ok(())
}

/// Marks a started agent lost (and failed, unless it already was).
async fn lose(
    core: &ServerCore,
    session_id: &str,
    agent: &AgentState,
    fail: bool,
    reason: &str,
) -> Result<(), ServerError> {
    let reason = format!("{reason}; the agent's context is presumed lost with it");
    placement::append_placement(
        core,
        session_id,
        horde_proto::v1::Placement {
            agent_id: Some(AgentId::new(agent.agent_id.clone())),
            state: PlacementState::Lost as i32,
            worker_id: None,
            reason: reason.clone(),
        },
        None,
    )
    .await?;
    if fail {
        append_server_event(
            &core.log,
            session_id,
            "",
            Event {
                agent_id: Some(AgentId::new(agent.agent_id.clone())),
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                    previous_state: agent.lifecycle as i32,
                    state: AgentLifecycleState::Failed as i32,
                    reason,
                })),
            },
        )
        .await?;
    }
    Ok(())
}

/// Whether a lease that started at registration `epoch` has lapsed: the
/// worker has not registered again since, and is not connected.
///
/// Both halves disqualify alone — a re-registration bumps the epoch, and a
/// connected worker is alive whatever its epoch says — and inline neither is
/// separately reachable through a live run.
fn lease_lapsed(current_epoch: u64, epoch: u64, connected: bool) -> bool {
    current_epoch == epoch && !connected
}

/// Starts the worker-lost lease for a worker that just went away (or that
/// a fresh server has never seen): if it has not come back when the lease
/// ends, it is declared lost.
pub fn start_lease(core: &ServerCore, worker_id: &str, epoch: u64) {
    let core = core.clone();
    let worker_id = worker_id.to_owned();
    tokio::spawn(async move {
        tokio::time::sleep(core.worker_lease).await;
        let current = core.workers.epoch_of(&worker_id);
        if !lease_lapsed(current, epoch, core.workers.contains(&worker_id)) {
            return;
        }
        let reason = format!(
            "worker `{worker_id}` stayed away longer than the {}s worker lease",
            core.worker_lease.as_secs()
        );
        if let Err(error) = declare_worker_lost(&core, &worker_id, &reason).await {
            tracing::warn!(%error, worker_id, "declaring a lapsed worker lost failed");
        }
    });
}

/// Boot reconciliation: a fresh server holds no registrations, so every
/// worker an agent is placed on is, as far as this process knows, away. Each
/// is treated exactly as a disconnect — its never-started agents re-queued at
/// once, and a lease started for the rest. Returns the workers it found.
///
/// # Errors
///
/// The log failing to read or append.
pub async fn reconcile_at_boot(core: &ServerCore) -> Result<Vec<String>, ServerError> {
    let mut away = BTreeSet::new();
    for state in placement::session_states(core).await? {
        for agent in state.agents.values() {
            if let Some(worker_id) = agent.worker_id.as_ref()
                && loss_of(agent, worker_id) != Loss::Untouched
                && !core.workers.contains(worker_id)
            {
                away.insert(worker_id.clone());
            }
        }
    }
    for worker_id in &away {
        placement::queue_lost_worker(core, worker_id).await?;
        start_lease(core, worker_id, core.workers.epoch_of(worker_id));
    }
    if !away.is_empty() {
        tracing::info!(workers = away.len(), "boot found agents on workers not connected");
        placement::place_queued(core).await?;
    }
    Ok(away.into_iter().collect())
}

/// What one journal reset did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalReset {
    pub journal_floor: u64,
    pub lost_agents: Vec<String>,
    pub sequence: u64,
}

/// Resets a worker's journal (see the module docs).
///
/// # Errors
///
/// [`ServerError::Invalid`] for an unnamed worker; the log failing.
pub async fn reset_worker_journal(
    core: &ServerCore,
    worker_id: &str,
    reason: &str,
    reset_by: Option<Actor>,
) -> Result<JournalReset, ServerError> {
    if worker_id.is_empty() {
        return Err(ServerError::Invalid(
            "a journal reset must name its worker".to_owned(),
        ));
    }
    let journal_floor = core.log.worker_journal_watermark(worker_id).await?;
    let sequence = append_server_event(
        &core.log,
        WORKER_JOURNAL_STREAM,
        "",
        Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::WorkerJournalReset(WorkerJournalReset {
                worker_id: Some(WorkerId::new(worker_id)),
                journal_floor,
                reset_by,
                reason: reason.to_owned(),
            })),
        },
    )
    .await?;
    let lost_agents = declare_worker_lost(
        core,
        worker_id,
        &format!("worker `{worker_id}`'s journal was reset ({reason})"),
    )
    .await?;
    Ok(JournalReset {
        journal_floor,
        lost_agents,
        sequence,
    })
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{AgentLifecycleState, PlacementState};

    use super::{Loss, lease_lapsed, loss_of};
    use crate::state::AgentState;

    fn agent(placement: PlacementState, worker: Option<&str>, lifecycle: AgentLifecycleState) -> AgentState {
        AgentState {
            agent_id: "agent-1".to_owned(),
            parent_agent_id: None,
            lifecycle,
            tree_path: Vec::new(),
            agent_type: String::new(),
            permission_mode: String::new(),
            worker_id: worker.map(ToString::to_string),
            spec: None,
            placement_state: placement,
            queued_at_unix_ms: None,
            rewrites_tool_input: false,
        }
    }

    /// The whole loss table: only this worker's placed agents are touched;
    /// never-started ones re-queue; started ones are lost, failing unless
    /// already failed; archived ones are over and stay over.
    #[test]
    fn a_loss_requeues_the_unstarted_and_loses_the_started() {
        use AgentLifecycleState as L;
        let placed = |lifecycle| agent(PlacementState::Placed, Some("w1"), lifecycle);
        assert_eq!(loss_of(&placed(L::Queued), "w1"), Loss::Requeue);
        assert_eq!(loss_of(&placed(L::Starting), "w1"), Loss::Requeue);
        assert_eq!(loss_of(&placed(L::Running), "w1"), Loss::Lose { fail: true });
        assert_eq!(loss_of(&placed(L::Idle), "w1"), Loss::Lose { fail: true });
        assert_eq!(
            loss_of(&placed(L::WaitingOnHuman), "w1"),
            Loss::Lose { fail: true }
        );
        assert_eq!(
            loss_of(&placed(L::Failed), "w1"),
            Loss::Lose { fail: false },
            "already failed: lost, but no second failure"
        );
        assert_eq!(loss_of(&placed(L::Archived), "w1"), Loss::Untouched);
        assert_eq!(
            loss_of(&placed(L::Running), "w2"),
            Loss::Untouched,
            "a neighbour's agent is not this loss's business"
        );
        assert_eq!(
            loss_of(
                &agent(PlacementState::Queued, Some("w1"), L::Running),
                "w1"
            ),
            Loss::Untouched,
            "a placement that merely mentions the worker is not placed on it"
        );
        assert_eq!(
            loss_of(&agent(PlacementState::Placed, None, L::Running), "w1"),
            Loss::Untouched
        );
    }

    #[test]
    fn a_lease_lapses_only_for_a_worker_neither_back_nor_connected() {
        assert!(lease_lapsed(3, 3, false));
        assert!(!lease_lapsed(4, 3, false), "it registered again");
        assert!(!lease_lapsed(3, 3, true), "it is connected");
        assert!(!lease_lapsed(4, 3, true));
    }

    #[test]
    fn the_latest_reset_of_the_named_worker_sets_the_floor() {
        use horde_proto::v1::{Event, WorkerId, WorkerJournalReset, event::Kind};
        use prost::Message;
        let reset = |worker: &str, floor: u64| crate::log::StoredEvent {
            session_id: super::WORKER_JOURNAL_STREAM.to_owned(),
            sequence: 0,
            recorded_at_unix_ms: 0,
            payload: Event {
                kind: Some(Kind::WorkerJournalReset(WorkerJournalReset {
                    worker_id: Some(WorkerId::new(worker)),
                    journal_floor: floor,
                    ..WorkerJournalReset::default()
                })),
                ..Event::default()
            }
            .encode_to_vec(),
        };
        let events = vec![reset("w1", 10), reset("w2", 99), reset("w1", 25)];
        assert_eq!(super::floor_in(&events, "w1"), 25);
        assert_eq!(super::floor_in(&events, "w2"), 99);
        assert_eq!(super::floor_in(&events, "w3"), 0, "never reset");
    }
}
