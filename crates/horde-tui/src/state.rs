//! One session's view state, folded from the event log.
//!
//! Everything a screen shows is derived from two inputs and nothing else: the
//! session summaries a `ListSessions` answer carries, and the session's own
//! event stream. There is no third channel — the inbox is not a query, it is
//! the set of `PermissionRequested` events no `PermissionDecided` has answered
//! yet, which is exactly what makes it correct under replay and reconnect
//! (ADR-0001: sync-from-offset is the one mechanism).

use std::collections::BTreeMap;

use horde_proto::v1::{
    AgentLifecycleState, AgentOutputChannel, Event, EventEnvelope, PermissionOutcome,
    PlacementState, ResultStatus, SessionLifecycleState, SessionSummary, event::Kind,
};

/// One agent's place in the tree, learned from its `AgentSpawned` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentNode {
    /// Unset for the session's top-level agent.
    pub parent: Option<String>,
    /// Ancestry display names, root first, as the spawn event carried them.
    pub tree_path: Vec<String>,
}

/// An escalation still awaiting a human. The inbox is a list of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingPermission {
    pub request_id: String,
    pub agent_id: String,
    /// Ancestry for the approver: `["ci-fix", "reviewer", "test-runner"]`.
    pub tree_path: Vec<String>,
    /// Human-readable rendering of the proposed call, e.g. `docker rm x`.
    pub rendered: String,
    /// The named permission mode that escalated.
    pub mode: String,
    /// Log position of the request, which is its arrival order.
    pub sequence: u64,
}

/// One rendered line of a session's feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedLine {
    pub sequence: u64,
    /// The agent the event is about; empty for session-scoped events.
    pub agent_id: String,
    /// A short fixed-width tag naming what happened.
    pub label: &'static str,
    pub text: String,
}

/// Everything the client knows about one session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionState {
    pub session_id: String,
    pub title: String,
    /// "active" / "archived" / "unspecified", from summary or lifecycle event.
    pub lifecycle: &'static str,
    /// Highest sequence seen, from either the summary or the stream.
    pub head: u64,
    /// True once the stream reported replay finished; the screen stops saying
    /// "syncing" without guessing.
    pub caught_up: bool,
    pub root_agent_id: Option<String>,
    /// Agents by id, learned from `AgentSpawned` events.
    pub agents: BTreeMap<String, AgentNode>,
    /// Escalations awaiting a human, by request id.
    pub pending: BTreeMap<String, PendingPermission>,
    pub feed: Vec<FeedLine>,
}

impl SessionState {
    #[must_use]
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            lifecycle: "unspecified",
            ..Self::default()
        }
    }

    /// Folds in a `ListSessions` summary.
    ///
    /// The summary's head is a lower bound, never a rewind: the stream may
    /// already be ahead of the snapshot the list answer was built from.
    pub fn absorb_summary(&mut self, summary: &SessionSummary) {
        self.title = summary.title.clone();
        self.lifecycle = session_lifecycle_name(summary.state);
        self.head = self.head.max(summary.head_sequence);
        if let Some(root) = summary.root_agent_id.as_ref() {
            self.root_agent_id = Some(root.to_string());
        }
    }

    /// Folds one event from the stream into the view.
    pub fn apply(&mut self, envelope: &EventEnvelope) {
        self.head = self.head.max(envelope.sequence);
        let Some(event) = envelope.event.as_ref() else {
            // An event with no body is still a position in the log; dropping
            // it would make the feed's sequence column look gapped.
            self.feed.push(FeedLine {
                sequence: envelope.sequence,
                agent_id: String::new(),
                label: "unknown",
                text: String::new(),
            });
            return;
        };

        self.fold(envelope.sequence, event);
        self.feed.push(feed_line(envelope.sequence, event));
    }

    /// The state-changing half of [`Self::apply`]: tree, inbox, lifecycle.
    fn fold(&mut self, sequence: u64, event: &Event) {
        let agent_id = event
            .agent_id
            .as_ref()
            .map_or_else(String::new, ToString::to_string);
        match event.kind.as_ref() {
            Some(Kind::AgentSpawned(spawned)) => {
                let parent = spawned.parent_agent_id.as_ref().map(ToString::to_string);
                if parent.is_none() && self.root_agent_id.is_none() {
                    self.root_agent_id = Some(agent_id.clone());
                }
                self.agents.insert(
                    agent_id,
                    AgentNode {
                        parent,
                        tree_path: spawned.tree_path.clone(),
                    },
                );
            }
            Some(Kind::PermissionRequested(requested)) => {
                if let Some(request) = requested.request.as_ref() {
                    let request_id = request
                        .request_id
                        .as_ref()
                        .map_or_else(String::new, ToString::to_string);
                    self.pending.insert(
                        request_id.clone(),
                        PendingPermission {
                            request_id,
                            agent_id,
                            tree_path: request.tree_path.clone(),
                            rendered: request
                                .tool_call
                                .as_ref()
                                .map_or_else(String::new, |call| call.rendered.clone()),
                            mode: request.permission_mode.clone(),
                            sequence,
                        },
                    );
                }
            }
            Some(Kind::PermissionDecided(decided)) => {
                if let Some(request_id) = decided
                    .decision
                    .as_ref()
                    .and_then(|decision| decision.request_id.as_ref())
                {
                    self.pending.remove(&request_id.to_string());
                }
            }
            Some(Kind::SessionLifecycleChanged(changed)) => {
                self.lifecycle = session_lifecycle_name(changed.state);
            }
            _ => {}
        }
    }

    /// The pending escalations, oldest first — the order an inbox lists them.
    #[must_use]
    pub fn pending_in_order(&self) -> Vec<&PendingPermission> {
        let mut pending: Vec<&PendingPermission> = self.pending.values().collect();
        pending.sort_by_key(|permission| permission.sequence);
        pending
    }

    /// Whether `agent_id` sits in the subtree rooted at `root`.
    ///
    /// Attaching to an agent scopes prompts to that agent's subtree
    /// (DESIGN.md §11); this is the predicate that draws the line. An agent
    /// the tree has no spawn record for is *outside* every subtree — a prompt
    /// must never be silently claimed by a scope that cannot prove it.
    #[must_use]
    pub fn in_subtree(&self, root: &str, agent_id: &str) -> bool {
        let mut current = agent_id.to_owned();
        // Counted ceiling, not a clock: a malformed parent chain could cycle,
        // and each legitimate hop consumes one distinct known agent.
        for _ in 0..=self.agents.len() {
            if current == root {
                return true;
            }
            match self.agents.get(&current).and_then(|node| node.parent.clone()) {
                Some(parent) => current = parent,
                None => return false,
            }
        }
        false
    }
}

/// Renders one event into its feed line. Total: an event kind this build does
/// not model still shows up under its own tag rather than vanishing.
fn feed_line(sequence: u64, event: &Event) -> FeedLine {
    let agent_id = event
        .agent_id
        .as_ref()
        .map_or_else(String::new, ToString::to_string);
    let (label, text) = describe(event.kind.as_ref());
    FeedLine {
        sequence,
        agent_id,
        label,
        text,
    }
}

/// The tag and the one line of text an event is worth in a feed.
///
/// Deliberately shallow, like the CLI's renderer: a feed that dumps whole
/// payloads is unreadable, and anything deeper is a detail pane's job.
fn describe(kind: Option<&Kind>) -> (&'static str, String) {
    match kind {
        Some(Kind::AgentOutputChunk(chunk)) => (channel_name(chunk.channel), chunk.text.clone()),
        Some(Kind::AgentSpawned(spawned)) => ("spawned", spawned.tree_path.join(" > ")),
        Some(Kind::AssignmentSent(sent)) => (
            "assign",
            sent.assignment
                .as_ref()
                .map_or_else(String::new, |assignment| assignment.text.clone()),
        ),
        Some(Kind::NoteSent(sent)) => (
            "note",
            sent.note
                .as_ref()
                .map_or_else(String::new, |note| note.text.clone()),
        ),
        Some(Kind::ResultReported(reported)) => (
            "result",
            reported.result.as_ref().map_or_else(String::new, |result| {
                joined(result_status_name(result.status), &result.summary)
            }),
        ),
        Some(Kind::PermissionRequested(requested)) => (
            "needs you",
            requested
                .request
                .as_ref()
                .and_then(|request| request.tool_call.as_ref())
                .map_or_else(String::new, |call| call.rendered.clone()),
        ),
        Some(Kind::PermissionDecided(decided)) => (
            "decided",
            decided.decision.as_ref().map_or_else(String::new, |decision| {
                joined(permission_outcome_name(decision.outcome), &decision.reason)
            }),
        ),
        Some(Kind::UsageRecorded(_)) => ("usage", String::new()),
        Some(Kind::WorkspaceInstanceChanged(changed)) => (
            "workspace",
            changed
                .report
                .as_ref()
                .map_or_else(String::new, |report| report.detail.clone()),
        ),
        Some(Kind::AgentLifecycleChanged(changed)) => (
            "lifecycle",
            joined(agent_lifecycle_name(changed.state), &changed.reason),
        ),
        Some(Kind::SessionLifecycleChanged(changed)) => (
            "session",
            joined(session_lifecycle_name(changed.state), &changed.reason),
        ),
        Some(Kind::PlacementChanged(changed)) => (
            "placement",
            changed.placement.as_ref().map_or_else(String::new, |placement| {
                joined(placement_state_name(placement.state), &placement.reason)
            }),
        ),
        Some(Kind::TurnInterrupted(interrupted)) => ("interrupt", interrupted.reason.clone()),
        None => ("unknown", String::new()),
    }
}

/// `state` alone, or `state: reason` when there is a reason to give.
fn joined(state: &str, reason: &str) -> String {
    if reason.is_empty() {
        state.to_owned()
    } else {
        format!("{state}: {reason}")
    }
}

fn channel_name(channel: i32) -> &'static str {
    match AgentOutputChannel::try_from(channel) {
        Ok(AgentOutputChannel::Assistant) => "assistant",
        Ok(AgentOutputChannel::Reasoning) => "reasoning",
        Ok(AgentOutputChannel::ToolCall) => "tool call",
        Ok(AgentOutputChannel::ToolResult) => "tool result",
        Ok(AgentOutputChannel::HarnessDiagnostic) => "harness",
        _ => "output",
    }
}

fn result_status_name(status: i32) -> &'static str {
    match ResultStatus::try_from(status) {
        Ok(ResultStatus::Success) => "success",
        Ok(ResultStatus::Partial) => "partial",
        Ok(ResultStatus::Blocked) => "blocked",
        _ => "unspecified",
    }
}

fn permission_outcome_name(outcome: i32) -> &'static str {
    match PermissionOutcome::try_from(outcome) {
        Ok(PermissionOutcome::Allow) => "allowed",
        Ok(PermissionOutcome::Deny) => "denied",
        _ => "unspecified",
    }
}

fn agent_lifecycle_name(state: i32) -> &'static str {
    match AgentLifecycleState::try_from(state) {
        Ok(AgentLifecycleState::Queued) => "queued",
        Ok(AgentLifecycleState::Starting) => "starting",
        Ok(AgentLifecycleState::Running) => "running",
        Ok(AgentLifecycleState::WaitingOnChild) => "waiting on child",
        Ok(AgentLifecycleState::WaitingOnHuman) => "waiting on human",
        Ok(AgentLifecycleState::Idle) => "idle",
        Ok(AgentLifecycleState::Archived) => "archived",
        Ok(AgentLifecycleState::Failed) => "failed",
        _ => "unspecified",
    }
}

fn session_lifecycle_name(state: i32) -> &'static str {
    match SessionLifecycleState::try_from(state) {
        Ok(SessionLifecycleState::Active) => "active",
        Ok(SessionLifecycleState::Archived) => "archived",
        _ => "unspecified",
    }
}

fn placement_state_name(state: i32) -> &'static str {
    match PlacementState::try_from(state) {
        Ok(PlacementState::Placed) => "placed",
        Ok(PlacementState::Queued) => "queued",
        Ok(PlacementState::Expired) => "expired",
        Ok(PlacementState::Cancelled) => "cancelled",
        _ => "unspecified",
    }
}
