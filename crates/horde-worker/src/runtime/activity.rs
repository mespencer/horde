//! What the Runtime tracks about its agents for workspace housekeeping
//! (ADR-0014): which have a turn in flight and since when they are idle,
//! one checkout lock per agent, and where Instance reports go. A leaf both
//! the shared state and the housekeeper lean on.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use horde_proto::v1::WorkspaceInstanceReport;

use crate::workspace::ledger::now_ms;

/// Where the Worker's workspace facts go: `ReportWorkspaceInstances` with the
/// whole set, authoritative.
#[async_trait]
pub trait InstanceReporter: Send + Sync {
    async fn report(&self, reports: Vec<WorkspaceInstanceReport>);
}

/// What the Runtime knows about each agent's activity.
#[derive(Debug, Default)]
pub struct Activity {
    agents: Mutex<BTreeMap<String, AgentActivity>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct AgentActivity {
    pub(super) busy: bool,
    pub(super) last_active_ms: i64,
    /// Soft-reclaimed for idleness since the last turn.
    pub(super) idle_reclaimed: bool,
    /// When `last_active_ms` was last written through to the ledger.
    written_ms: i64,
}

impl Activity {
    /// Records whether the agent has a turn in flight. A turn starting
    /// resets the idle flag: the next idle period may be reclaimed again.
    pub fn set_busy(&self, agent_id: &str, busy: bool) {
        let mut agents = self.agents.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = agents.entry(agent_id.to_owned()).or_default();
        if busy && !entry.busy {
            entry.idle_reclaimed = false;
        }
        if busy || entry.busy {
            entry.last_active_ms = now_ms();
        }
        entry.busy = busy;
    }

    /// Whether the agent has a turn in flight.
    #[must_use]
    pub fn is_busy(&self, agent_id: &str) -> bool {
        self.agents
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(agent_id)
            .is_some_and(|agent| agent.busy)
    }

    /// Forgets an ended agent.
    pub fn forget(&self, agent_id: &str) {
        self.agents
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(agent_id);
    }

    /// `(busy, last active, idle-reclaimed)` for one agent, if seen.
    pub(super) fn snapshot(&self, agent_id: &str) -> Option<AgentActivity> {
        self.agents
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(agent_id)
            .copied()
    }

    pub(super) fn mark_idle_reclaimed(&self, agent_id: &str) {
        if let Some(agent) = self
            .agents
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(agent_id)
        {
            agent.idle_reclaimed = true;
        }
    }

    /// Whether the agent's activity should be written through now (at most
    /// once a minute), marking it written.
    pub(super) fn due_for_write(&self, agent_id: &str) -> Option<i64> {
        let mut agents = self.agents.lock().unwrap_or_else(PoisonError::into_inner);
        let agent = agents.get_mut(agent_id)?;
        let due = agent.last_active_ms > agent.written_ms
            && now_ms().saturating_sub(agent.written_ms) >= 60_000;
        due.then(|| {
            agent.written_ms = now_ms();
            agent.last_active_ms
        })
    }
}

/// Per-agent checkout locks: one async mutex per agent id.
#[derive(Debug, Default)]
pub(super) struct CheckoutLocks {
    locks: Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl CheckoutLocks {
    /// The lock of one agent's checkout.
    pub(super) fn of(&self, agent_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(locks.entry(agent_id.to_owned()).or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A turn starting clears the idle flag; ending keeps the time.
    #[test]
    fn activity_tracks_turns_and_resets_the_idle_flag() {
        let activity = Activity::default();
        assert!(!activity.is_busy("a"));
        activity.set_busy("a", true);
        assert!(activity.is_busy("a"));
        activity.mark_idle_reclaimed("a");
        activity.set_busy("a", false);
        let seen = activity.snapshot("a").expect("seen");
        assert!(!seen.busy && seen.idle_reclaimed && seen.last_active_ms > 0);
        activity.set_busy("a", true);
        assert!(!activity.snapshot("a").expect("seen").idle_reclaimed);
        assert!(activity.due_for_write("a").is_some());
        assert_eq!(activity.due_for_write("a"), None, "at most once a minute");
        activity.forget("a");
        assert!(activity.snapshot("a").is_none());
    }

    /// Only a turn *starting* clears the idle flag: an agent reported idle
    /// twice, or busy twice within one turn, keeps it.
    #[test]
    fn only_a_starting_turn_clears_the_idle_flag() {
        let activity = Activity::default();
        activity.set_busy("a", false);
        activity.mark_idle_reclaimed("a");
        activity.set_busy("a", false);
        assert!(activity.snapshot("a").expect("seen").idle_reclaimed);
    }

    /// An agent never active has nothing to write through, and a write is
    /// not repeated inside its minute however much activity follows it.
    #[test]
    fn activity_is_written_through_only_when_new_and_not_within_a_minute() {
        let activity = Activity::default();
        activity.set_busy("idle", false);
        assert_eq!(
            activity.due_for_write("idle"),
            None,
            "never active: nothing newer than the last write"
        );

        activity.set_busy("a", true);
        assert!(activity.due_for_write("a").is_some());
        std::thread::sleep(std::time::Duration::from_millis(5));
        activity.set_busy("a", false);
        activity.set_busy("a", true);
        assert_eq!(
            activity.due_for_write("a"),
            None,
            "newer activity, but inside the minute"
        );
    }

    #[tokio::test]
    async fn one_agent_one_lock() {
        let locks = CheckoutLocks::default();
        let first = locks.of("a");
        let _held = first.lock().await;
        assert!(
            locks.of("a").try_lock().is_err(),
            "the same agent's lock is held"
        );
        assert!(locks.of("b").try_lock().is_ok(), "another agent's is not");
    }
}
