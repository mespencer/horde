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

use std::collections::{BTreeMap, BTreeSet};

use horde_proto::prost_types::Struct;
use horde_proto::v1::{
    AgentLifecycleState, AgentSpec, Event, PlacementState, ProposedToolCall, SessionLifecycleState,
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
    /// Named permission mode from the spawn spec; empty means the default.
    pub permission_mode: String,
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

/// One permission request still awaiting its decision.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingPermission {
    /// The agent blocked on the answer.
    pub agent_id: String,
    /// The call as proposed, kept so a decision that remembers itself can
    /// record *what* was allowed — the decision event alone names only the
    /// request id.
    pub tool_call: Option<ProposedToolCall>,
}

/// The identity of a proposed call, for remember-for-session matching.
///
/// The arguments ride as their canonical encoding rather than as a `Struct`,
/// because the map needs a total order and prost's `Struct` (a `BTreeMap`
/// underneath) encodes deterministically. Absent and empty input stay
/// distinct, same as everywhere else in the permission chain: "a call with no
/// arguments" and "arguments unknown" are different calls.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RememberedCall {
    pub tool_name: String,
    pub input: Option<Vec<u8>>,
}

impl RememberedCall {
    /// The remembering key for one proposed call.
    #[must_use]
    pub fn of(call: &ProposedToolCall) -> Self {
        Self {
            tool_name: call.tool_name.clone(),
            input: call.input.as_ref().map(Message::encode_to_vec),
        }
    }
}

/// One session's derived state.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionState {
    pub session_id: String,
    /// The session's display name, from the root spawn's first tree-path
    /// element — the only place the log carries it. Empty until a root spawn
    /// names it. This is the same rule the TUI folds client-side, projected
    /// here so `ListSessions` summaries carry the title instead of every
    /// client re-deriving it.
    pub title: String,
    pub lifecycle: SessionLifecycleState,
    /// Highest sequence folded so far — the client's resume point.
    pub head_sequence: u64,
    /// The session's top-level agent, once one has been spawned.
    pub root_agent_id: Option<String>,
    /// Agents by id, in insertion order of their spawn events.
    pub agents: BTreeMap<String, AgentState>,
    /// Permission requests still awaiting a decision.
    pub pending_permissions: BTreeMap<String, PendingPermission>,
    /// Requests already decided. Kept so a later `PermissionRequested` with a
    /// decided id — the chain's enriched re-raise racing a human's answer —
    /// cannot re-open a request forever.
    pub decided_permissions: BTreeSet<String>,
    /// Calls a human allowed with remember-for-session, keyed by the proposed
    /// call and carrying the narrowing to re-apply, if the decision had one.
    /// This is the widened deterministic-allow layer from
    /// `PermissionDecision.remember_for_session`.
    pub remembered_allows: BTreeMap<RememberedCall, Option<Struct>>,
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
        // The root's first tree-path element is the session's display name.
        if names_the_session(parent.as_deref(), &self.title)
            && let Some(name) = spawned.tree_path.first()
        {
            self.title = name.clone();
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
                permission_mode: spawned
                    .spec
                    .as_ref()
                    .map(|spec| spec.permission_mode.clone())
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

    /// Remembers which agent is waiting on a permission decision, and what it
    /// proposed.
    ///
    /// A request whose id was already decided is ignored: the chain re-raises
    /// an escalation with context for the approver, and that re-raise racing a
    /// human's answer must not re-open a request that will never be answered
    /// again.
    fn permission_requested(&mut self, requested: horde_proto::v1::PermissionRequested) {
        if let Some(request) = requested.request
            && let Some(id) = request.request_id.as_ref().map(ToString::to_string)
            && !self.decided_permissions.contains(&id)
        {
            let agent_id = request
                .agent_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default();
            self.pending_permissions.insert(
                id,
                PendingPermission {
                    agent_id,
                    tool_call: request.tool_call,
                },
            );
        }
    }

    /// Settles a permission: the request stops being pending, its id can never
    /// be re-opened, and a remembering allow widens the session's
    /// deterministic-allow layer with the call it approved.
    fn permission_decided(&mut self, decided: horde_proto::v1::PermissionDecided) {
        if let Some(decision) = decided.decision
            && let Some(id) = decision.request_id.as_ref().map(ToString::to_string)
        {
            if let Some(pending) = self.pending_permissions.remove(&id)
                && decision_remembers(&decision)
                && let Some(call) = pending.tool_call
            {
                // The key is the *proposed* call, and the value re-applies the
                // same narrowing: remembering the narrowed arguments as if
                // they had been proposed would auto-allow the original,
                // unnarrowed call — wider than what the human approved.
                self.remembered_allows
                    .insert(RememberedCall::of(&call), decision.updated_input);
            }
            self.decided_permissions.insert(id);
        }
    }

    /// Whether the deterministic-allow layer already covers this call, and
    /// with what narrowing.
    ///
    /// `None` means the call is not remembered. `Some(None)` means it is
    /// remembered as proposed; `Some(Some(_))` carries the narrowing the
    /// original decision applied.
    #[must_use]
    pub fn remembered_narrowing(&self, call: &ProposedToolCall) -> Option<&Option<Struct>> {
        self.remembered_allows.get(&RememberedCall::of(call))
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

/// Whether a spawn event is the one a session may take its display name
/// from: a root spawn, seen while the session is still nameless.
///
/// A named predicate rather than an inline `&&` in the fold: the second
/// parentless spawn racing the first is not reachable through the RPCs, so
/// inline the operator would be untestable (docs/QUALITY_GATES.md on
/// compound conditions). Mirrors the TUI's `takes_title_from` on purpose —
/// two frontends, one naming rule.
fn names_the_session(parent: Option<&str>, title: &str) -> bool {
    parent.is_none() && title.is_empty()
}

/// Whether a decision widens the deterministic-allow layer for the session.
///
/// Both halves matter and are not separately reachable inline: humans answer
/// allow-without-remember constantly, and a deny that asked to be remembered
/// is refusable UI nonsense — remembering a *denial* as an allow would be the
/// silently widened permission this chain exists to prevent, so `&&` versus
/// `||` here is a security property, not a style choice.
fn decision_remembers(decision: &horde_proto::v1::PermissionDecision) -> bool {
    let allowed = decision.outcome == horde_proto::v1::PermissionOutcome::Allow as i32;
    allowed && decision.remember_for_session
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

    fn spawned_named(parent: Option<&str>, tree_path: &[&str]) -> Kind {
        Kind::AgentSpawned(AgentSpawned {
            parent_agent_id: parent.map(AgentId::new),
            spawned_by: None,
            spec: None,
            tree_path: tree_path.iter().map(ToString::to_string).collect(),
        })
    }

    /// The root spawn's first tree-path element names the session; a child's
    /// spawn and a later parentless impostor must not rename it.
    #[test]
    fn the_root_spawn_names_the_session_and_nothing_renames_it() {
        let events = vec![
            stored(1, Some("root"), spawned_named(None, &["ci-fix"])),
            stored(2, Some("child"), spawned_named(Some("root"), &["other"])),
            stored(3, Some("impostor"), spawned_named(None, &["impostor"])),
        ];
        let state = SessionState::project("s1", &events).expect("project");
        assert_eq!(state.title, "ci-fix");
    }

    /// A child arriving first must not claim the name: the naming rule is
    /// "root spawn", not "first spawn".
    #[test]
    fn a_childs_spawn_never_names_the_session() {
        let events = vec![
            stored(1, Some("child"), spawned_named(Some("root"), &["early"])),
            stored(2, Some("root"), spawned_named(None, &["ci-fix"])),
        ];
        let state = SessionState::project("s1", &events).expect("project");
        assert_eq!(state.title, "ci-fix");
    }

    #[test]
    fn a_root_spawn_with_no_tree_path_leaves_the_session_nameless() {
        let events = vec![stored(1, Some("root"), spawned_named(None, &[]))];
        let state = SessionState::project("s1", &events).expect("project");
        assert_eq!(state.title, "");
    }

    /// All four corners of the naming predicate; the mixed rows are the ones
    /// an `||` and an `&&` disagree on, and neither is reachable end-to-end.
    #[test]
    fn only_a_root_spawn_of_a_nameless_session_names_it() {
        assert!(super::names_the_session(None, ""));
        assert!(!super::names_the_session(Some("root"), ""));
        assert!(!super::names_the_session(None, "already named"));
        assert!(!super::names_the_session(Some("root"), "already named"));
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
        assert_eq!(state.agents["root"].placement_state, PlacementState::Placed);
        assert_eq!(
            state.agents["root"].queued_at_unix_ms, None,
            "a placed agent is not waiting, so nothing can expire it"
        );
    }

    /// A queued placement stamps *when* from the event's recorded time —
    /// that stamp is the whole basis of queue-timeout expiry — and any later
    /// outcome clears it.
    #[test]
    fn a_queued_placement_remembers_when_and_a_later_one_forgets() {
        let queued = |sequence: u64, at_ms: i64| StoredEvent {
            recorded_at_unix_ms: at_ms,
            ..stored(
                sequence,
                Some("root"),
                Kind::PlacementChanged(PlacementChanged {
                    placement: Some(Placement {
                        agent_id: Some(AgentId::new("root")),
                        state: PlacementState::Queued as i32,
                        worker_id: None,
                        reason: "no worker is connected".to_owned(),
                    }),
                }),
            )
        };

        let events = vec![
            stored(1, Some("root"), spawned(None, "o")),
            queued(2, 1_234),
        ];
        let state = SessionState::project("s1", &events).expect("project");
        assert_eq!(state.agents["root"].placement_state, PlacementState::Queued);
        assert_eq!(
            state.agents["root"].queued_at_unix_ms,
            Some(1_234),
            "expiry is measured from the queued event's own clock"
        );

        // A later queued event moves the stamp; it does not accumulate.
        let requeued = vec![
            stored(1, Some("root"), spawned(None, "o")),
            queued(2, 1_234),
            queued(3, 9_999),
        ];
        let state = SessionState::project("s1", &requeued).expect("project");
        assert_eq!(state.agents["root"].queued_at_unix_ms, Some(9_999));
    }

    /// The spec survives into the projection whole: re-placing a queued agent
    /// asks the same question the original spawn did, and a projection that
    /// dropped the constraints would make the scheduler guess.
    #[test]
    fn the_spawn_spec_is_retained_for_re_placement() {
        let events = vec![stored(1, Some("root"), spawned(None, "orchestrator"))];
        let state = SessionState::project("s1", &events).expect("project");

        let spec = state.agents["root"].spec.as_ref().expect("the spec");
        assert_eq!(spec.agent_type, "orchestrator");
        assert_eq!(
            state.agents["root"].placement_state,
            PlacementState::Unspecified,
            "no placement event has answered this spawn yet"
        );
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

    #[test]
    fn the_permission_mode_rides_the_projection_from_the_spawn_spec() {
        let with_mode = Kind::AgentSpawned(AgentSpawned {
            parent_agent_id: None,
            spawned_by: None,
            spec: Some(AgentSpec {
                agent_type: "reviewer".to_owned(),
                permission_mode: "cautious".to_owned(),
                ..AgentSpec::default()
            }),
            tree_path: Vec::new(),
        });
        let state =
            SessionState::project("s1", &[stored(1, Some("root"), with_mode)]).expect("project");

        assert_eq!(state.agents["root"].permission_mode, "cautious");
    }
}

#[cfg(test)]
mod permission_fold_tests {
    use horde_proto::prost_types::{Struct, Value, value::Kind as ValueKind};
    use horde_proto::v1::{
        AgentId, Event, PermissionDecided, PermissionDecision, PermissionOutcome,
        PermissionRequest, PermissionRequestId, PermissionRequested, ProposedToolCall, event::Kind,
    };
    use prost::Message;

    use super::{PendingPermission, SessionState, decision_remembers};
    use crate::log::StoredEvent;

    fn stored(sequence: u64, kind: Kind) -> StoredEvent {
        StoredEvent {
            session_id: "s1".to_owned(),
            sequence,
            recorded_at_unix_ms: 0,
            payload: Event {
                agent_id: Some(AgentId::new("agent-1")),
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(kind),
            }
            .encode_to_vec(),
        }
    }

    fn arguments(path: &str) -> Struct {
        let mut input = Struct::default();
        input.fields.insert(
            "file_path".to_owned(),
            Value {
                kind: Some(ValueKind::StringValue(path.to_owned())),
            },
        );
        input
    }

    fn call(path: &str) -> ProposedToolCall {
        ProposedToolCall {
            tool_name: "Write".to_owned(),
            input: Some(arguments(path)),
            rendered: format!("Write {path}"),
        }
    }

    fn requested(request_id: &str, tool_call: Option<ProposedToolCall>) -> Kind {
        Kind::PermissionRequested(PermissionRequested {
            request: Some(PermissionRequest {
                request_id: Some(PermissionRequestId::new(request_id)),
                agent_id: Some(AgentId::new("agent-1")),
                tool_call,
                ..PermissionRequest::default()
            }),
        })
    }

    fn decided(
        request_id: &str,
        outcome: PermissionOutcome,
        remember: bool,
        updated_input: Option<Struct>,
    ) -> Kind {
        Kind::PermissionDecided(PermissionDecided {
            decision: Some(PermissionDecision {
                request_id: Some(PermissionRequestId::new(request_id)),
                outcome: outcome as i32,
                remember_for_session: remember,
                updated_input,
                ..PermissionDecision::default()
            }),
        })
    }

    #[test]
    fn a_pending_permission_holds_the_agent_and_the_proposed_call() {
        let state = SessionState::project(
            "s1",
            &[stored(1, requested("req-1", Some(call("/etc/hosts"))))],
        )
        .expect("project");

        assert_eq!(
            state.pending_permissions.get("req-1"),
            Some(&PendingPermission {
                agent_id: "agent-1".to_owned(),
                tool_call: Some(call("/etc/hosts")),
            })
        );
    }

    #[test]
    fn a_decision_settles_the_request() {
        let state = SessionState::project(
            "s1",
            &[
                stored(1, requested("req-1", Some(call("/etc/hosts")))),
                stored(2, decided("req-1", PermissionOutcome::Deny, false, None)),
            ],
        )
        .expect("project");

        assert!(state.pending_permissions.is_empty());
        assert!(state.decided_permissions.contains("req-1"));
    }

    /// The race this guard exists for: the chain's enriched re-raise lands
    /// *after* a human's answer. Without the decided set, the re-raise
    /// re-opens a request nobody will ever answer again, and it reads as
    /// pending forever.
    #[test]
    fn a_request_re_raised_after_its_decision_stays_settled() {
        let state = SessionState::project(
            "s1",
            &[
                stored(1, requested("req-1", Some(call("/etc/hosts")))),
                stored(2, decided("req-1", PermissionOutcome::Allow, false, None)),
                stored(3, requested("req-1", Some(call("/etc/hosts")))),
            ],
        )
        .expect("project");

        assert!(
            state.pending_permissions.is_empty(),
            "a decided request must never come back as pending"
        );
    }

    /// All four corners. The false rows are each reachable in production —
    /// plain allows are constant, and a remembered denial would widen the
    /// allow layer with something a human refused.
    #[test]
    fn only_an_allow_that_asked_to_be_remembered_remembers() {
        let case = |outcome: PermissionOutcome, remember: bool| {
            decision_remembers(&PermissionDecision {
                outcome: outcome as i32,
                remember_for_session: remember,
                ..PermissionDecision::default()
            })
        };

        assert!(case(PermissionOutcome::Allow, true));
        assert!(!case(PermissionOutcome::Allow, false));
        assert!(
            !case(PermissionOutcome::Deny, true),
            "remembering a denial as an allow is the silently widened permission"
        );
        assert!(!case(PermissionOutcome::Deny, false));
    }

    #[test]
    fn a_remembering_allow_widens_the_session_for_that_exact_call() {
        let state = SessionState::project(
            "s1",
            &[
                stored(1, requested("req-1", Some(call("/tmp/x")))),
                stored(2, decided("req-1", PermissionOutcome::Allow, true, None)),
            ],
        )
        .expect("project");

        assert_eq!(
            state.remembered_narrowing(&call("/tmp/x")),
            Some(&None),
            "the identical call is now allowed as proposed"
        );
        assert_eq!(
            state.remembered_narrowing(&call("/etc/hosts")),
            None,
            "a different argument is a different call and must re-escalate"
        );
    }

    /// Remembering keys on the *proposed* call and re-applies the narrowing.
    /// Keying on the narrowed arguments would auto-allow the original,
    /// unnarrowed proposal later — wider than what the human approved.
    #[test]
    fn a_remembered_narrowing_is_reapplied_not_forgotten() {
        let state = SessionState::project(
            "s1",
            &[
                stored(1, requested("req-1", Some(call("/etc/hosts")))),
                stored(
                    2,
                    decided(
                        "req-1",
                        PermissionOutcome::Allow,
                        true,
                        Some(arguments("/tmp/safe")),
                    ),
                ),
            ],
        )
        .expect("project");

        assert_eq!(
            state.remembered_narrowing(&call("/etc/hosts")),
            Some(&Some(arguments("/tmp/safe"))),
            "the repeat of the proposal must carry the same clamp"
        );
    }

    #[test]
    fn a_denial_never_widens_the_allow_layer_even_when_asked_to() {
        let state = SessionState::project(
            "s1",
            &[
                stored(1, requested("req-1", Some(call("/etc/hosts")))),
                stored(2, decided("req-1", PermissionOutcome::Deny, true, None)),
            ],
        )
        .expect("project");

        assert!(state.remembered_allows.is_empty());
    }

    /// Absent and empty arguments are different calls, the same distinction
    /// `updated_input` draws. A remember of one must not cover the other.
    #[test]
    fn a_call_with_no_arguments_is_not_a_call_with_empty_arguments() {
        let bare = ProposedToolCall {
            tool_name: "Read".to_owned(),
            input: None,
            rendered: String::new(),
        };
        let empty = ProposedToolCall {
            tool_name: "Read".to_owned(),
            input: Some(Struct::default()),
            rendered: String::new(),
        };

        let state = SessionState::project(
            "s1",
            &[
                stored(1, requested("req-1", Some(bare.clone()))),
                stored(2, decided("req-1", PermissionOutcome::Allow, true, None)),
            ],
        )
        .expect("project");

        assert_eq!(state.remembered_narrowing(&bare), Some(&None));
        assert_eq!(state.remembered_narrowing(&empty), None);
    }
}
