//! Session and agent state, folded out of the log.
//!
//! There is no stored copy of any of this. Every value here is computed by
//! replaying a session's events, which is what makes "replay = state" true
//! rather than aspirational: there is no second representation that could
//! disagree, so there is nothing to reconcile after a crash and no write path
//! that can forget to update a projection.
//!
//! The fold is deliberately total and deliberately dull. It ignores what it
//! does not understand rather than failing, because a newer worker may append
//! an event kind this build has never seen, and refusing to project a session
//! because of one unfamiliar event would take down the whole view.

use std::collections::BTreeMap;

use horde_proto::v1::{
    AgentLifecycleState, AgentSpec, Event, PlacementState, SessionLifecycleState,
    event::Kind as EventKind,
};
use prost::Message;

use crate::error::ServerError;
use crate::log::StoredEvent;

/// One agent, as the log describes it.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentState {
    pub agent_id: String,
    /// Unset for the session's top-level agent.
    pub parent_agent_id: Option<String>,
    pub lifecycle: AgentLifecycleState,
    /// Ancestry display names, root first.
    pub tree_path: Vec<String>,
    pub agent_type: String,
    /// The worker this agent was placed on, once it has been placed.
    pub worker_id: Option<String>,
    /// What the spawn asked for — constraints, workspace, checkout policy.
    /// Kept whole because re-placing a queued agent asks the same question
    /// the original spawn did, and a projection that dropped the spec would
    /// force the scheduler to guess.
    pub spec: Option<AgentSpec>,
    /// The latest placement outcome. `Unspecified` means no placement event
    /// has been folded yet, which the placement pass treats as awaiting
    /// placement — a spawn is incomplete until some placement answers it.
    pub placement_state: PlacementState,
    /// When the latest `Queued` placement was recorded, for queue-timeout
    /// expiry. Cleared by any other placement outcome.
    pub queued_at_unix_ms: Option<i64>,
}

/// One session's derived state.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionState {
    pub session_id: String,
    pub lifecycle: SessionLifecycleState,
    /// Highest sequence folded so far — the client's resume point.
    pub head_sequence: u64,
    /// The session's top-level agent, once one has been spawned.
    pub root_agent_id: Option<String>,
    /// Agents by id, in insertion order of their spawn events.
    pub agents: BTreeMap<String, AgentState>,
    /// Permission requests still awaiting a decision.
    pub pending_permissions: BTreeMap<String, String>,
}

impl SessionState {
    /// Folds a session's events into its state.
    ///
    /// Pure: same events in, same state out, with no clock and no I/O. That is
    /// what lets a test assert the projection without a database and what makes
    /// "state is a function of the log" checkable rather than merely claimed.
    pub fn project(session_id: &str, events: &[StoredEvent]) -> Result<Self, ServerError> {
        let mut state = Self {
            session_id: session_id.to_owned(),
            lifecycle: SessionLifecycleState::Active,
            ..Self::default()
        };
        for stored in events {
            state.apply(stored)?;
        }
        Ok(state)
    }

    /// Folds one more event in. Advancing the head is unconditional: an event
    /// this build does not model still occupies a position, and a client
    /// resuming from a stale head would replay it forever.
    pub fn apply(&mut self, stored: &StoredEvent) -> Result<(), ServerError> {
        self.head_sequence = self.head_sequence.max(stored.sequence);

        let event = Event::decode(stored.payload.as_slice())?;
        let agent_id = event.agent_id.as_ref().map(ToString::to_string);
        let Some(kind) = event.kind else {
            return Ok(());
        };

        match kind {
            EventKind::AgentSpawned(spawned) => self.agent_spawned(agent_id, spawned),
            EventKind::AgentLifecycleChanged(changed) => {
                self.agent_lifecycle_changed(agent_id, &changed);
            }
            EventKind::PlacementChanged(changed) => {
                self.placement_changed(changed, stored.recorded_at_unix_ms);
            }
            EventKind::SessionLifecycleChanged(changed) => {
                self.session_lifecycle_changed(&changed);
            }
            EventKind::PermissionRequested(requested) => self.permission_requested(requested),
            EventKind::PermissionDecided(decided) => self.permission_decided(decided),
            // Output chunks, messages, results, usage, and workspace progress
            // are journal content rather than tree structure. They are replayed
            // to clients verbatim and do not change the projection.
            _ => {}
        }
        Ok(())
    }

    /// Adds a spawned agent, and adopts the first parentless one as the root.
    fn agent_spawned(&mut self, agent_id: Option<String>, spawned: horde_proto::v1::AgentSpawned) {
        let Some(agent_id) = agent_id else {
            // An agent event with no agent is malformed, but one bad event must
            // not sink the projection of everything else.
            return;
        };
        let parent = spawned.parent_agent_id.as_ref().map(ToString::to_string);
        if parent.is_none() {
            self.root_agent_id.get_or_insert_with(|| agent_id.clone());
        }
        self.agents.insert(
            agent_id.clone(),
            AgentState {
                agent_id,
                parent_agent_id: parent,
                lifecycle: AgentLifecycleState::Queued,
                tree_path: spawned.tree_path,
                agent_type: spawned
                    .spec
                    .as_ref()
                    .map(|spec| spec.agent_type.clone())
                    .unwrap_or_default(),
                worker_id: None,
                spec: spawned.spec,
                placement_state: PlacementState::Unspecified,
                queued_at_unix_ms: None,
            },
        );
    }

    /// Moves a known agent to a lifecycle state this build understands.
    fn agent_lifecycle_changed(
        &mut self,
        agent_id: Option<String>,
        changed: &horde_proto::v1::AgentLifecycleChanged,
    ) {
        if let Some(agent_id) = agent_id
            && let Some(agent) = self.agents.get_mut(&agent_id)
            && let Ok(next) = AgentLifecycleState::try_from(changed.state)
        {
            agent.lifecycle = next;
        }
    }

    /// Records the latest placement outcome: the worker (when placed), the
    /// state, and — for a queued outcome — when it queued, which is what the
    /// queue timeout is measured from.
    fn placement_changed(
        &mut self,
        changed: horde_proto::v1::PlacementChanged,
        recorded_at_unix_ms: i64,
    ) {
        if let Some(placement) = changed.placement
            && let Some(id) = placement.agent_id.as_ref().map(ToString::to_string)
            && let Some(agent) = self.agents.get_mut(&id)
        {
            agent.worker_id = placement.worker_id.as_ref().map(ToString::to_string);
            agent.placement_state =
                PlacementState::try_from(placement.state).unwrap_or(PlacementState::Unspecified);
            agent.queued_at_unix_ms =
                (agent.placement_state == PlacementState::Queued).then_some(recorded_at_unix_ms);
        }
    }

    /// Moves the session to a lifecycle state this build understands.
    fn session_lifecycle_changed(&mut self, changed: &horde_proto::v1::SessionLifecycleChanged) {
        if let Ok(next) = SessionLifecycleState::try_from(changed.state) {
            self.lifecycle = next;
        }
    }

    /// Remembers which agent is waiting on a permission decision.
    fn permission_requested(&mut self, requested: horde_proto::v1::PermissionRequested) {
        if let Some(request) = requested.request
            && let Some(id) = request.request_id.as_ref().map(ToString::to_string)
        {
            let agent = request
                .agent_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default();
            self.pending_permissions.insert(id, agent);
        }
    }

    /// Forgets a permission once its decision has landed.
    fn permission_decided(&mut self, decided: horde_proto::v1::PermissionDecided) {
        if let Some(decision) = decided.decision
            && let Some(id) = decision.request_id.as_ref().map(ToString::to_string)
        {
            self.pending_permissions.remove(&id);
        }
    }

    /// Agents whose parent is `parent`, or the roots when `None`.
    pub fn children_of<'a>(
        &'a self,
        parent: Option<&'a str>,
    ) -> impl Iterator<Item = &'a AgentState> + 'a {
        self.agents
            .values()
            .filter(move |agent| agent.parent_agent_id.as_deref() == parent)
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentId, AgentLifecycleChanged, AgentLifecycleState, AgentSpawned, AgentSpec, Event,
        Placement, PlacementChanged, PlacementState, SessionLifecycleChanged,
        SessionLifecycleState, WorkerId, event::Kind,
    };
    use prost::Message;

    use super::SessionState;
    use crate::log::StoredEvent;

    fn stored(sequence: u64, agent: Option<&str>, kind: Kind) -> StoredEvent {
        let event = Event {
            agent_id: agent.map(AgentId::new),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(kind),
        };
        StoredEvent {
            session_id: "s1".to_owned(),
            sequence,
            recorded_at_unix_ms: 0,
            payload: event.encode_to_vec(),
        }
    }

    fn spawned(parent: Option<&str>, agent_type: &str) -> Kind {
        Kind::AgentSpawned(AgentSpawned {
            parent_agent_id: parent.map(AgentId::new),
            spawned_by: None,
            spec: Some(AgentSpec {
                agent_type: agent_type.to_owned(),
                ..AgentSpec::default()
            }),
            tree_path: Vec::new(),
        })
    }

    #[test]
    fn a_session_with_no_events_is_active_and_empty() {
        let state = SessionState::project("s1", &[]).expect("project");
        assert_eq!(state.head_sequence, 0);
        assert!(state.agents.is_empty());
        assert_eq!(state.lifecycle, SessionLifecycleState::Active);
    }

    #[test]
    fn the_first_parentless_spawn_becomes_the_root_agent() {
        let events = vec![
            stored(1, Some("root"), spawned(None, "orchestrator")),
            stored(2, Some("child"), spawned(Some("root"), "reviewer")),
        ];
        let state = SessionState::project("s1", &events).expect("project");

        assert_eq!(state.root_agent_id.as_deref(), Some("root"));
        assert_eq!(state.agents.len(), 2);
        assert_eq!(state.head_sequence, 2);

        let children: Vec<&str> = state
            .children_of(Some("root"))
            .map(|agent| agent.agent_id.as_str())
            .collect();
        assert_eq!(children, vec!["child"]);
    }

    #[test]
    fn lifecycle_changes_move_the_agent_rather_than_adding_one() {
        let events = vec![
            stored(1, Some("root"), spawned(None, "orchestrator")),
            stored(
                2,
                Some("root"),
                Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                    previous_state: AgentLifecycleState::Queued as i32,
                    state: AgentLifecycleState::Running as i32,
                    ..AgentLifecycleChanged::default()
                }),
            ),
        ];
        let state = SessionState::project("s1", &events).expect("project");

        assert_eq!(state.agents.len(), 1);
        assert_eq!(state.agents["root"].lifecycle, AgentLifecycleState::Running);
    }

    #[test]
    fn placement_records_the_worker_the_agent_landed_on() {
        let events = vec![
            stored(1, Some("root"), spawned(None, "orchestrator")),
            stored(
                2,
                Some("root"),
                Kind::PlacementChanged(PlacementChanged {
                    placement: Some(Placement {
                        agent_id: Some(AgentId::new("root")),
                        state: PlacementState::Placed as i32,
                        worker_id: Some(WorkerId::new("worker-a")),
                        reason: String::new(),
                    }),
                }),
            ),
        ];
        let state = SessionState::project("s1", &events).expect("project");
        assert_eq!(state.agents["root"].worker_id.as_deref(), Some("worker-a"));
    }

    #[test]
    fn archiving_the_session_is_visible_in_the_projection() {
        let events = vec![stored(
            1,
            None,
            Kind::SessionLifecycleChanged(SessionLifecycleChanged {
                previous_state: SessionLifecycleState::Active as i32,
                state: SessionLifecycleState::Archived as i32,
                reason: String::new(),
            }),
        )];
        let state = SessionState::project("s1", &events).expect("project");
        assert_eq!(state.lifecycle, SessionLifecycleState::Archived);
    }

    /// A newer worker may append a kind this build has never seen. Refusing to
    /// project the session would take down the whole view over one event.
    #[test]
    fn an_event_with_no_kind_advances_the_head_without_breaking_the_fold() {
        let unknown = StoredEvent {
            session_id: "s1".to_owned(),
            sequence: 7,
            recorded_at_unix_ms: 0,
            payload: Event::default().encode_to_vec(),
        };
        let state = SessionState::project("s1", &[unknown]).expect("project");

        assert_eq!(
            state.head_sequence, 7,
            "an unmodelled event still occupies a position; a stale head would replay forever"
        );
        assert!(state.agents.is_empty());
    }

    #[test]
    fn projecting_a_prefix_matches_folding_that_prefix_one_at_a_time() {
        let events = vec![
            stored(1, Some("root"), spawned(None, "orchestrator")),
            stored(2, Some("child"), spawned(Some("root"), "reviewer")),
            stored(
                3,
                Some("child"),
                Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                    previous_state: AgentLifecycleState::Queued as i32,
                    state: AgentLifecycleState::Idle as i32,
                    ..AgentLifecycleChanged::default()
                }),
            ),
        ];

        let all_at_once = SessionState::project("s1", &events).expect("project");

        let mut incremental = SessionState::project("s1", &[]).expect("project");
        for event in &events {
            incremental.apply(event).expect("apply");
        }

        assert_eq!(
            all_at_once, incremental,
            "a live projection kept up to date must equal a cold replay"
        );
    }
}
