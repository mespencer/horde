//! One session's view state, folded from the event log.
//!
//! Everything a screen shows is derived from two inputs and nothing else: the
//! session summaries a `ListSessions` answer carries, and the session's own
//! event stream. There is no third channel — the inbox is not a query, it is
//! the set of `PermissionRequested` events no `PermissionDecided` has answered
//! yet, which is exactly what makes it correct under replay and reconnect
//! (ADR-0001: sync-from-offset is the one mechanism).

use std::collections::{BTreeMap, VecDeque};

use horde_proto::v1::{AgentOutputChannel, Event, EventEnvelope, SessionSummary, event::Kind};

pub use crate::tree::AgentNode;
use crate::tree::{note_facts, requested_model};
pub use crate::words::{SHORT_ID_LEN, agent_lifecycle_name, placement_state_name, short_id};
use crate::words::{
    channel_name, permission_outcome_name, result_status_name, session_lifecycle_name,
};

/// An escalation still awaiting a human. The inbox is a list of these.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingPermission {
    pub request_id: String,
    pub agent_id: String,
    /// The tool the harness wants to run (`Bash`, `AskUserQuestion`).
    pub tool_name: String,
    /// Ancestry for the approver: `["ci-fix", "reviewer", "test-runner"]`.
    pub tree_path: Vec<String>,
    /// Human-readable rendering of the proposed call, e.g. `docker rm x`.
    pub rendered: String,
    /// The named permission mode that escalated.
    pub mode: String,
    /// Log position of the request, which is its arrival order.
    pub sequence: u64,
    /// The questions the request asks, when it is a question (W12c) — only
    /// answers or a denial settle it.
    pub questions: Vec<PendingQuestion>,
}

/// One question a pending request asks, as the inbox shows it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingQuestion {
    /// The key its answer is filed under.
    pub id: String,
    /// A short label for the question, e.g. "Colour"; may be empty.
    pub header: String,
    pub question: String,
    /// The options, in the order offered; answered by 1-based number.
    pub options: Vec<QuestionChoice>,
    /// More than one option may be chosen.
    pub multi_select: bool,
    /// An answer outside the options is acceptable.
    pub allows_free_text: bool,
    /// The answer is a secret: typed text is never echoed.
    pub secret: bool,
}

/// One option a question offers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuestionChoice {
    pub label: String,
    pub description: String,
}

impl PendingPermission {
    /// Whether only answers (or a denial) can settle this request.
    #[must_use]
    pub fn is_question(&self) -> bool {
        !self.questions.is_empty()
    }
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
    /// The most recent [`FEED_CAPACITY`] lines, oldest first. Bounded: a
    /// session attached for a day must not grow the client without limit,
    /// and the log — not the feed — is the record.
    pub feed: VecDeque<FeedLine>,
    /// Each agent's transcript, by agent id — the main view's content.
    pub transcripts: BTreeMap<String, crate::transcript::Transcript>,
    /// The highest sequence the *stream* delivered. Separate from `head`,
    /// which summaries also raise: an event at or below this has been folded
    /// already (a resubscription overlaps its predecessor) and is skipped.
    pub stream_head: u64,
}

/// How many feed lines a session keeps. Far more than any terminal shows or
/// a human scrolls back through; older lines fall off the front.
pub const FEED_CAPACITY: usize = 2_000;

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
    /// already be ahead of the snapshot the list answer was built from. The
    /// title is likewise never blanked: summaries carry it now, but a name
    /// already learned — from an earlier summary or from the root agent's
    /// spawn event — must survive a refresh whose answer predates it.
    pub fn absorb_summary(&mut self, summary: &SessionSummary) {
        if !summary.title.is_empty() {
            self.title = summary.title.clone();
        }
        self.lifecycle = session_lifecycle_name(summary.state);
        self.head = self.head.max(summary.head_sequence);
        if let Some(root) = summary.root_agent_id.as_ref() {
            self.root_agent_id = Some(root.to_string());
        }
    }

    /// Folds one event from the stream into the view.
    pub fn apply(&mut self, envelope: &EventEnvelope) {
        if self.already_folded(envelope.sequence) {
            return;
        }
        self.stream_head = self.stream_head.max(envelope.sequence);
        self.head = self.head.max(envelope.sequence);
        let Some(event) = envelope.event.as_ref() else {
            // An event with no body is still a position in the log; dropping
            // it would make the feed's sequence column look gapped.
            self.push_feed(FeedLine {
                sequence: envelope.sequence,
                agent_id: String::new(),
                label: "unknown",
                text: String::new(),
            });
            return;
        };

        self.fold(envelope.sequence, event);
        note_facts(&mut self.agents, event);
        crate::transcript::transcribe(&mut self.transcripts, &self.agents, event);
        if !is_internal(event) {
            self.push_feed(feed_line(envelope.sequence, event));
        }
    }

    /// Whether the stream already delivered this position. Position zero is
    /// never a real sequence (the log counts from one), so it is never a
    /// duplicate either.
    fn already_folded(&self, sequence: u64) -> bool {
        sequence != 0 && sequence <= self.stream_head
    }

    /// Appends one line, dropping the oldest once the feed is full.
    fn push_feed(&mut self, line: FeedLine) {
        if self.feed.len() >= FEED_CAPACITY {
            self.feed.pop_front();
        }
        self.feed.push_back(line);
    }

    /// What a human calls this session: its title, or a short id while it
    /// has none.
    #[must_use]
    pub fn display_name(&self) -> String {
        if self.title.is_empty() {
            short_id(&self.session_id)
        } else {
            self.title.clone()
        }
    }

    /// What a human calls an agent: the last element of its spawn-time tree
    /// path (its own display name), or a short id for an agent the replay
    /// never saw spawn. Empty for session-scoped events, which have no agent.
    #[must_use]
    pub fn agent_name(&self, agent_id: &str) -> String {
        crate::tree::agent_name(&self.agents, agent_id)
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
                // The root's first tree-path element is the session's display
                // name — the same rule the server projects into summaries,
                // kept here for streams read before any summary arrives.
                if self.takes_title_from(parent.as_deref())
                    && let Some(name) = spawned.tree_path.first()
                {
                    self.title = name.clone();
                }
                let requested = requested_model(spawned.spec.as_ref());
                self.agents.insert(
                    agent_id,
                    AgentNode {
                        parent,
                        tree_path: spawned.tree_path.clone(),
                        harness: requested.0,
                        model: requested.1,
                        ..AgentNode::default()
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
                            tool_name: request
                                .tool_call
                                .as_ref()
                                .map_or_else(String::new, |call| call.tool_name.clone()),
                            tree_path: request.tree_path.clone(),
                            rendered: request
                                .tool_call
                                .as_ref()
                                .map_or_else(String::new, |call| call.rendered.clone()),
                            mode: request.permission_mode.clone(),
                            sequence,
                            questions: request
                                .tool_call
                                .as_ref()
                                .map(|call| call.questions.iter().map(pending_question).collect())
                                .unwrap_or_default(),
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

    /// Whether a spawn event is the one a session may take its display name
    /// from: a root spawn, seen while the session is still nameless.
    ///
    /// A named predicate rather than an inline `&&` in the fold: the truth
    /// table is assertable here and unreachable there
    /// (docs/QUALITY_GATES.md on compound conditions).
    fn takes_title_from(&self, parent: Option<&str>) -> bool {
        parent.is_none() && self.title.is_empty()
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
            match self
                .agents
                .get(&current)
                .and_then(|node| node.parent.clone())
            {
                Some(parent) => current = parent,
                None => return false,
            }
        }
        false
    }
}

/// A wire question as the inbox holds it.
fn pending_question(question: &horde_proto::v1::Question) -> PendingQuestion {
    PendingQuestion {
        id: question.id.clone(),
        header: question.header.clone(),
        question: question.question.clone(),
        options: question
            .options
            .iter()
            .map(|option| QuestionChoice {
                label: option.label.clone(),
                description: option.description.clone(),
            })
            .collect(),
        multi_select: question.multi_select,
        allows_free_text: question.allows_free_text,
        secret: question.secret,
    }
}

/// Whether an event is the harness talking to itself — warnings, retries,
/// rate-limit notices (`HARNESS_DIAGNOSTIC`). Still folded, never fed: a
/// human steering an agent reads what the agent said and what happened to
/// it, and the CLI's `sync` remains the place to read the raw log.
fn is_internal(event: &Event) -> bool {
    matches!(
        event.kind.as_ref(),
        Some(Kind::AgentOutputChunk(chunk))
            if chunk.channel == AgentOutputChannel::HarnessDiagnostic as i32
    )
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
/// payloads is unreadable, and anything deeper is a detail pane's job. Split
/// into the conversation half and the machinery half — one 14-arm match sat
/// exactly on the CRAP boundary, and the two halves are also the two voices
/// a feed actually interleaves: what the agents said, and what happened to
/// them.
fn describe(kind: Option<&Kind>) -> (&'static str, String) {
    kind.map_or(("unknown", String::new()), |kind| {
        conversation_line(kind).unwrap_or_else(|| machinery_line(kind))
    })
}

/// The conversational kinds: output, spawns, messages, results, escalations.
/// `None` for the kinds that belong to [`machinery_line`].
fn conversation_line(kind: &Kind) -> Option<(&'static str, String)> {
    Some(match kind {
        Kind::AgentOutputChunk(chunk) => (channel_name(chunk.channel), chunk.text.clone()),
        Kind::AgentSpawned(spawned) => ("spawned", spawned.tree_path.join(" > ")),
        Kind::AssignmentSent(sent) => (
            "assign",
            sent.assignment
                .as_ref()
                .map_or_else(String::new, |assignment| assignment.text.clone()),
        ),
        Kind::NoteSent(sent) => (
            "note",
            sent.note
                .as_ref()
                .map_or_else(String::new, |note| note.text.clone()),
        ),
        Kind::ResultReported(reported) => (
            "result",
            reported.result.as_ref().map_or_else(String::new, |result| {
                joined(result_status_name(result.status), &result.summary)
            }),
        ),
        Kind::PermissionRequested(requested) => (
            "needs you",
            requested
                .request
                .as_ref()
                .and_then(|request| request.tool_call.as_ref())
                .map_or_else(String::new, |call| call.rendered.clone()),
        ),
        Kind::PermissionDecided(decided) => (
            "decided",
            decided
                .decision
                .as_ref()
                .map_or_else(String::new, |decision| {
                    joined(permission_outcome_name(decision.outcome), &decision.reason)
                }),
        ),
        _ => return None,
    })
}

/// The machinery kinds: usage, workspaces, lifecycles, placement, interrupts.
fn machinery_line(kind: &Kind) -> (&'static str, String) {
    match kind {
        Kind::UsageRecorded(recorded) => ("usage", usage_text(recorded)),
        Kind::WorkspaceInstanceChanged(changed) => (
            "workspace",
            changed
                .report
                .as_ref()
                .map_or_else(String::new, |report| report.detail.clone()),
        ),
        Kind::AgentLifecycleChanged(changed) => (
            "lifecycle",
            joined(agent_lifecycle_name(changed.state), &changed.reason),
        ),
        Kind::SessionLifecycleChanged(changed) => (
            "session",
            joined(session_lifecycle_name(changed.state), &changed.reason),
        ),
        Kind::PlacementChanged(changed) => (
            "placement",
            changed
                .placement
                .as_ref()
                .map_or_else(String::new, |placement| {
                    joined(placement_state_name(placement.state), &placement.reason)
                }),
        ),
        Kind::TurnInterrupted(interrupted) => ("interrupt", interrupted.reason.clone()),
        Kind::ForwardOpened(opened) => ("forward", forward_opened_text(opened)),
        Kind::ForwardClosed(closed) => (
            "forward",
            joined(
                &format!(
                    "closed {}",
                    closed
                        .forward_id
                        .as_ref()
                        .map_or_else(String::new, ToString::to_string)
                ),
                &closed.reason,
            ),
        ),
        // Only reachable for a conversational kind handed here directly; the
        // dispatcher never does, but a total answer beats a panic.
        _ => ("unknown", String::new()),
    }
}

/// One opened forward as feed text: `vite → localhost:5173 (expose)`, the
/// rendering ADR-0006 promises, with the backer named when there is one.
fn forward_opened_text(opened: &horde_proto::v1::ForwardOpened) -> String {
    let Some(spec) = opened.spec.as_ref() else {
        return String::new();
    };
    let direction = forward_direction_name(spec.direction);
    let mut text = format!(
        "{} → localhost:{} ({direction})",
        spec.name, spec.worker_port
    );
    if let Some(backer) = opened.backer_client_id.as_ref() {
        text.push_str(&format!(", backed by {backer}"));
    }
    text
}

/// The wire direction as the word the panel and the feed print — the schema
/// crate's own vocabulary, re-exported so the fold and the views share it.
pub use horde_proto::forward::direction_name as forward_direction_name;

/// One journaled usage record as feed text: the *measured* facts only —
/// token counts as journaled, and a rate-window statement as the provider
/// made it. Derived headroom arithmetic is `horde usage`'s job; a feed line
/// repeats what the log says, never what Horde computes from it.
fn usage_text(recorded: &horde_proto::v1::UsageRecorded) -> String {
    let Some(record) = recorded.record.as_ref() else {
        return String::new();
    };
    let mut parts = Vec::new();
    if let Some(tokens) = record.tokens.as_ref() {
        parts.push(format!(
            "in {} out {}",
            tokens.input_tokens, tokens.output_tokens
        ));
    }
    if let Some(stated) = record.rate_window.as_ref().and_then(window_statement) {
        parts.push(stated);
    }
    if let Some(account) = account_of(record.binding.as_ref()) {
        parts.push(account);
    }
    parts.join("  ")
}

/// A rate window's own statement, in whichever shape the provider reported:
/// counts, a fraction, or bare serving state. `None` when it stated nothing.
fn window_statement(window: &horde_proto::v1::RateWindow) -> Option<String> {
    if let (Some(used), Some(limit)) = (window.used, window.limit) {
        return Some(format!("window {used}/{limit} used"));
    }
    if let Some(fraction) = window.used_fraction {
        return Some(format!("window {:.0}% used", fraction * 100.0));
    }
    window.allowed.map(|allowed| {
        if allowed {
            "window serving".to_owned()
        } else {
            "window not serving".to_owned()
        }
    })
}

/// `(provider/account)` for attribution, when the binding names one.
fn account_of(binding: Option<&horde_proto::v1::ModelBinding>) -> Option<String> {
    let binding = binding?;
    let unattributed = binding.provider.is_empty() && binding.provider_account.is_empty();
    if unattributed {
        return None;
    }
    Some(format!(
        "({}/{})",
        binding.provider, binding.provider_account
    ))
}

/// `state` alone, or `state: reason` when there is a reason to give.
fn joined(state: &str, reason: &str) -> String {
    if reason.is_empty() {
        state.to_owned()
    } else {
        format!("{state}: {reason}")
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentId, AgentLifecycleChanged, AgentLifecycleState, AgentOutputChannel, AgentOutputChunk,
        AgentResult, Assignment, AssignmentSent, EventEnvelope, Note, NoteSent, PermissionDecided,
        PermissionOutcome, PermissionRequested, Placement, PlacementChanged, PlacementState,
        ResultReported, ResultStatus, SessionId, SessionLifecycleChanged, SessionLifecycleState,
        SessionSummary, TurnInterrupted, UsageRecorded, WorkspaceInstanceChanged,
        WorkspaceInstanceReport, event::Kind,
    };

    use super::{AgentNode, FEED_CAPACITY, SessionState, short_id};
    use crate::fixtures::{decided, envelope, requested, spawned};

    fn chunk(channel: AgentOutputChannel, text: &str) -> Kind {
        Kind::AgentOutputChunk(AgentOutputChunk {
            channel: channel as i32,
            text: text.to_owned(),
            ..AgentOutputChunk::default()
        })
    }

    /// The feed keeps the newest [`FEED_CAPACITY`] lines and no more: the
    /// oldest falls off the front, exactly one per line past the cap.
    #[test]
    fn the_feed_is_bounded_and_drops_its_oldest_lines() {
        let mut state = SessionState::new("s1");
        let total = FEED_CAPACITY as u64 + 3;
        for sequence in 1..=total {
            state.apply(&envelope(
                sequence,
                "a1",
                chunk(AgentOutputChannel::Assistant, "x"),
            ));
        }
        assert_eq!(state.feed.len(), FEED_CAPACITY);
        assert_eq!(state.feed.front().map(|line| line.sequence), Some(4));
        assert_eq!(state.feed.back().map(|line| line.sequence), Some(total));
        assert_eq!(
            state.head, total,
            "trimming the feed never rewinds the head"
        );
    }

    /// Harness diagnostics are the harness talking to itself: they advance
    /// the head but never reach the feed. Every other channel still does.
    #[test]
    fn harness_diagnostics_stay_out_of_the_feed() {
        let mut state = SessionState::new("s1");
        state.apply(&envelope(
            1,
            "a1",
            chunk(AgentOutputChannel::HarnessDiagnostic, "retrying"),
        ));
        assert!(state.feed.is_empty(), "a diagnostic is not a feed line");
        assert_eq!(state.head, 1);
        state.apply(&envelope(
            2,
            "a1",
            chunk(AgentOutputChannel::Reasoning, "hmm"),
        ));
        assert_eq!(
            state.feed.len(),
            1,
            "reasoning is the agent, not the harness"
        );
    }

    /// Names, not raw ids: an agent answers to its own tree-path name, a
    /// session to its title; either falls back to a short id prefix when
    /// nothing better is known, and an empty id stays empty.
    #[test]
    fn agents_and_sessions_answer_to_names_with_a_short_id_fallback() {
        let mut state = SessionState::new("0123456789abcdef");
        assert_eq!(state.display_name(), "01234567");
        state.apply(&envelope(1, "agent-root-xyz", spawned(None, &["ci-fix"])));
        state.apply(&envelope(
            2,
            "agent-child-xyz",
            spawned(Some("agent-root-xyz"), &["ci-fix", "reviewer"]),
        ));
        state.apply(&envelope(3, "agent-blank-xyz", spawned(None, &[""])));
        assert_eq!(state.display_name(), "ci-fix");
        assert_eq!(state.agent_name("agent-root-xyz"), "ci-fix");
        assert_eq!(state.agent_name("agent-child-xyz"), "reviewer");
        assert_eq!(state.agent_name("agent-blank-xyz"), "blank-xy");
        assert_eq!(state.agent_name("never-spawned-1"), "never-sp");
        assert_eq!(state.agent_name(""), "");
        assert_eq!(short_id("abc"), "abc", "a short id never pads");
        assert_eq!(short_id("session-18f3a2b4c5d6-0"), "18f3a2b4");
        assert_eq!(short_id("session-key-1-root"), "key-1-ro");
    }

    #[test]
    fn every_event_kind_renders_a_feed_line_with_its_text() {
        let cases: Vec<(Kind, &str, &str)> = vec![
            (
                Kind::AgentOutputChunk(AgentOutputChunk {
                    channel: AgentOutputChannel::Assistant as i32,
                    text: "hello there".to_owned(),
                    turn_id: String::new(),
                    chunk_index: 0,
                }),
                "assistant",
                "hello there",
            ),
            (
                spawned(None, &["ci-fix", "reviewer"]),
                "spawned",
                "ci-fix > reviewer",
            ),
            (
                Kind::AssignmentSent(AssignmentSent {
                    from: None,
                    assignment: Some(Assignment {
                        assignment_id: None,
                        text: "fix the build".to_owned(),
                        payload: None,
                    }),
                    delivery: 0,
                }),
                "assign",
                "fix the build",
            ),
            (
                Kind::NoteSent(NoteSent {
                    from: None,
                    note: Some(Note {
                        text: "fyi".to_owned(),
                        payload: None,
                    }),
                    delivery: 0,
                }),
                "note",
                "fyi",
            ),
            (
                Kind::ResultReported(ResultReported {
                    result: Some(AgentResult {
                        assignment_id: None,
                        status: ResultStatus::Partial as i32,
                        summary: "half done".to_owned(),
                        payload: None,
                    }),
                }),
                "result",
                "partial: half done",
            ),
            (
                requested("req-1", "docker rm x"),
                "needs you",
                "docker rm x",
            ),
            (
                decided("req-1", PermissionOutcome::Deny, "too risky"),
                "decided",
                "denied: too risky",
            ),
            (Kind::UsageRecorded(UsageRecorded::default()), "usage", ""),
            (
                Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged {
                    report: Some(WorkspaceInstanceReport {
                        detail: "disk is gone".to_owned(),
                        ..WorkspaceInstanceReport::default()
                    }),
                    progress: None,
                }),
                "workspace",
                "disk is gone",
            ),
            (
                Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                    previous_state: 0,
                    state: AgentLifecycleState::Idle as i32,
                    reason: "result reported".to_owned(),
                }),
                "lifecycle",
                "idle: result reported",
            ),
            (
                Kind::SessionLifecycleChanged(SessionLifecycleChanged {
                    previous_state: 0,
                    state: SessionLifecycleState::Archived as i32,
                    reason: String::new(),
                }),
                "session",
                "archived",
            ),
            (
                Kind::PlacementChanged(PlacementChanged {
                    adapter_capabilities: Vec::new(),
                    placement: Some(Placement {
                        agent_id: None,
                        state: PlacementState::Queued as i32,
                        worker_id: None,
                        reason: "no worker".to_owned(),
                    }),
                }),
                "placement",
                "queued: no worker",
            ),
            (
                Kind::TurnInterrupted(TurnInterrupted {
                    requested_by: None,
                    turn_id: String::new(),
                    reason: "changed my mind".to_owned(),
                }),
                "interrupt",
                "changed my mind",
            ),
        ];

        for (kind, label, text) in cases {
            let mut state = SessionState::new("s1");
            state.apply(&envelope(3, "agent-1", kind));
            let line = state.feed.back().expect("a feed line per event");
            assert_eq!(line.label, label);
            assert_eq!(line.text, text, "for label {label}");
            assert_eq!(line.sequence, 3);
            assert_eq!(line.agent_id, "agent-1");
        }
    }

    /// A message whose body failed to arrive renders empty rather than
    /// panicking or inventing text.
    #[test]
    fn bodyless_messages_render_as_empty_text() {
        for (kind, label) in [
            (Kind::AssignmentSent(AssignmentSent::default()), "assign"),
            (Kind::NoteSent(NoteSent::default()), "note"),
            (Kind::ResultReported(ResultReported::default()), "result"),
            (
                Kind::PermissionRequested(PermissionRequested::default()),
                "needs you",
            ),
            (
                Kind::PermissionDecided(PermissionDecided::default()),
                "decided",
            ),
            (
                Kind::PlacementChanged(PlacementChanged::default()),
                "placement",
            ),
            (
                Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged::default()),
                "workspace",
            ),
        ] {
            let mut state = SessionState::new("s1");
            state.apply(&envelope(1, "", kind));
            let line = state.feed.back().expect("a feed line");
            assert_eq!(line.label, label);
            assert_eq!(line.text, "");
        }
    }

    /// An event with no body is still a position in the log; hiding it would
    /// make the feed's sequence column look gapped.
    #[test]
    fn an_event_with_no_body_still_takes_its_position() {
        let mut state = SessionState::new("s1");
        state.apply(&EventEnvelope {
            session_id: Some(SessionId::new("s1")),
            sequence: 9,
            recorded_at: None,
            event: None,
        });
        assert_eq!(state.head, 9);
        let line = state.feed.back().expect("a line for the position");
        assert_eq!((line.label, line.sequence), ("unknown", 9));
    }

    #[test]
    fn the_head_never_rewinds() {
        let mut state = SessionState::new("s1");
        state.apply(&envelope(
            7,
            "a",
            Kind::UsageRecorded(UsageRecorded::default()),
        ));
        state.apply(&envelope(
            3,
            "a",
            Kind::UsageRecorded(UsageRecorded::default()),
        ));
        assert_eq!(
            state.head, 7,
            "an out-of-order replay must not rewind the head"
        );
    }

    #[test]
    fn a_summary_fills_in_what_the_stream_has_not_said_yet() {
        let mut state = SessionState::new("s1");
        state.head = 40;
        state.absorb_summary(&SessionSummary {
            session_id: Some(SessionId::new("s1")),
            title: "ci-fix".to_owned(),
            root_agent_id: Some(AgentId::new("root-1")),
            workspace_definition_id: None,
            state: SessionLifecycleState::Active as i32,
            created_at: None,
            head_sequence: 12,
            agent_count: 1,
            last_activity_at: None,
        });
        assert_eq!(state.title, "ci-fix");
        assert_eq!(state.lifecycle, "active");
        assert_eq!(state.root_agent_id.as_deref(), Some("root-1"));
        assert_eq!(
            state.head, 40,
            "a stale summary must not rewind a live head"
        );
    }

    /// A name learned from the root spawn must not be blanked by a refresh
    /// whose summary carries no title — an archived session's, or a summary
    /// built before the spawn landed.
    #[test]
    fn the_root_spawn_names_the_session_and_a_blank_summary_cannot_unname_it() {
        let mut state = SessionState::new("s1");
        state.apply(&envelope(1, "root-1", spawned(None, &["ci-fix"])));
        assert_eq!(state.title, "ci-fix");

        state.absorb_summary(&SessionSummary::default());
        assert_eq!(state.title, "ci-fix", "an empty summary title is no title");

        // A child's spawn, and a second root-shaped spawn, must not rename.
        state.apply(&envelope(2, "child-1", spawned(Some("root-1"), &["other"])));
        state.apply(&envelope(3, "impostor", spawned(None, &["impostor"])));
        assert_eq!(state.title, "ci-fix");

        // A summary that does carry a title wins: it is the server's word.
        state.absorb_summary(&SessionSummary {
            title: "renamed".to_owned(),
            ..SessionSummary::default()
        });
        assert_eq!(state.title, "renamed");
    }

    /// Both halves of the root rule, separately: a child arriving first must
    /// not claim the slot, and a second parentless spawn must not steal it.
    #[test]
    fn the_root_slot_takes_only_the_first_parentless_spawn() {
        let mut state = SessionState::new("s1");
        spawn_into(&mut state, 1, "child-early", Some("root-later"), &[]);
        assert_eq!(
            state.root_agent_id, None,
            "a child seen before any root is not the root"
        );
        spawn_into(&mut state, 2, "root-1", None, &["ci-fix"]);
        assert_eq!(state.root_agent_id.as_deref(), Some("root-1"));
        spawn_into(&mut state, 3, "impostor", None, &["impostor"]);
        assert_eq!(
            state.root_agent_id.as_deref(),
            Some("root-1"),
            "the root slot is not reassigned"
        );
    }

    #[test]
    fn a_root_spawn_with_no_tree_path_leaves_the_session_nameless() {
        let mut state = SessionState::new("s1");
        state.apply(&envelope(1, "root-1", spawned(None, &[])));
        assert_eq!(state.title, "");
    }

    /// Applies one spawn through the production fold.
    fn spawn_into(
        state: &mut SessionState,
        sequence: u64,
        agent: &str,
        parent: Option<&str>,
        tree_path: &[&str],
    ) {
        state.apply(&envelope(sequence, agent, spawned(parent, tree_path)));
    }

    /// A session grown by applying each `(agent, parent)` spawn in order,
    /// the root carrying the "ci-fix" display name.
    fn tree_of(spawns: &[(&str, Option<&str>)]) -> SessionState {
        let mut state = SessionState::new("s1");
        for (index, (agent, parent)) in spawns.iter().enumerate() {
            let path: &[&str] = if parent.is_none() { &["ci-fix"] } else { &[] };
            spawn_into(&mut state, (index + 1) as u64, agent, *parent, path);
        }
        state
    }

    #[test]
    fn the_first_parentless_spawn_is_the_root() {
        let state = tree_of(&[("root-1", None), ("child-1", Some("root-1"))]);
        assert_eq!(state.root_agent_id.as_deref(), Some("root-1"));
        assert_eq!(state.agents.len(), 2);
        assert_eq!(
            state
                .agents
                .get("child-1")
                .and_then(|node| node.parent.as_deref()),
            Some("root-1")
        );
    }

    #[test]
    fn a_request_waits_until_its_decision_arrives() {
        let mut state = SessionState::new("s1");
        state.apply(&envelope(1, "agent-1", requested("req-1", "docker rm x")));
        state.apply(&envelope(2, "agent-1", requested("req-2", "rm -rf /tmp/x")));
        assert_eq!(
            state
                .pending_in_order()
                .iter()
                .map(|permission| permission.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["req-1", "req-2"],
            "oldest first: the inbox answers in arrival order"
        );
        let front = state.pending.get("req-1").expect("req-1 pends");
        assert_eq!(front.rendered, "docker rm x");
        assert_eq!(front.mode, "manual");
        assert_eq!(front.tree_path, vec!["ci-fix", "reviewer"]);
        assert_eq!(front.agent_id, "agent-1");
        assert_eq!(front.sequence, 1);

        state.apply(&envelope(
            3,
            "agent-1",
            decided("req-1", PermissionOutcome::Allow, ""),
        ));
        assert_eq!(
            state
                .pending_in_order()
                .iter()
                .map(|permission| permission.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["req-2"],
            "a decision retires exactly the request it names"
        );
    }

    #[test]
    fn an_archive_event_flips_the_lifecycle() {
        let mut state = SessionState::new("s1");
        state.absorb_summary(&SessionSummary {
            state: SessionLifecycleState::Active as i32,
            ..SessionSummary::default()
        });
        assert_eq!(state.lifecycle, "active");
        state.apply(&envelope(
            5,
            "",
            Kind::SessionLifecycleChanged(SessionLifecycleChanged {
                previous_state: SessionLifecycleState::Active as i32,
                state: SessionLifecycleState::Archived as i32,
                reason: "done".to_owned(),
            }),
        ));
        assert_eq!(state.lifecycle, "archived");
    }

    #[test]
    fn subtree_membership_follows_parent_links_and_nothing_else() {
        let state = tree_of(&[
            ("root-1", None),
            ("child-1", Some("root-1")),
            ("grandchild-1", Some("child-1")),
            ("child-2", Some("root-1")),
        ]);

        assert!(
            state.in_subtree("child-1", "child-1"),
            "a subtree contains its root"
        );
        assert!(state.in_subtree("child-1", "grandchild-1"));
        assert!(state.in_subtree("root-1", "child-2"));
        assert!(
            !state.in_subtree("child-1", "child-2"),
            "a sibling is not a descendant"
        );
        assert!(
            !state.in_subtree("child-1", "root-1"),
            "membership goes down the tree, never up"
        );
        assert!(
            !state.in_subtree("child-1", "agent-nobody-spawned"),
            "an agent with no spawn record must not be claimed by any scope"
        );
    }

    /// A malformed parent chain must terminate the walk rather than hang it —
    /// the counted ceiling, exercised.
    #[test]
    fn a_parent_cycle_cannot_hang_the_membership_walk() {
        let mut state = SessionState::new("s1");
        state.agents.insert(
            "a".to_owned(),
            AgentNode {
                parent: Some("b".to_owned()),
                tree_path: Vec::new(),
                ..AgentNode::default()
            },
        );
        state.agents.insert(
            "b".to_owned(),
            AgentNode {
                parent: Some("a".to_owned()),
                tree_path: Vec::new(),
                ..AgentNode::default()
            },
        );
        assert!(!state.in_subtree("root-1", "a"));
    }

    /// The ceiling is `len + 1`, not `len`: a walk that ends on a root known
    /// only as a parent reference — never spawned in this replay — needs one
    /// comparison per known agent plus the final one against the root itself.
    #[test]
    fn membership_can_end_on_a_root_the_replay_never_spawned() {
        let mut state = SessionState::new("s1");
        for (agent, parent) in [("a1", "r"), ("a2", "a1")] {
            state.agents.insert(
                agent.to_owned(),
                AgentNode {
                    parent: Some(parent.to_owned()),
                    tree_path: Vec::new(),
                    ..AgentNode::default()
                },
            );
        }
        assert!(
            state.in_subtree("r", "a2"),
            "the walk must be allowed one more comparison than there are known agents"
        );
    }

    /// One scaffold for six vocabularies, so a renamed word or a dropped arm
    /// fails on the exact pair that changed.
    fn assert_names(name_of: fn(i32) -> &'static str, cases: &[(i32, &'static str)]) {
        for (value, expected) in cases {
            assert_eq!(name_of(*value), *expected, "for wire value {value}");
        }
    }

    #[test]
    fn every_channel_and_state_name_is_pinned() {
        assert_names(
            super::channel_name,
            &[
                (AgentOutputChannel::Assistant as i32, "assistant"),
                (AgentOutputChannel::Reasoning as i32, "reasoning"),
                (AgentOutputChannel::ToolCall as i32, "tool call"),
                (AgentOutputChannel::ToolResult as i32, "tool result"),
                (AgentOutputChannel::HarnessDiagnostic as i32, "harness"),
                (887, "output"),
            ],
        );
        assert_names(
            super::result_status_name,
            &[
                (ResultStatus::Success as i32, "success"),
                (ResultStatus::Partial as i32, "partial"),
                (ResultStatus::Blocked as i32, "blocked"),
                (887, "unspecified"),
            ],
        );
        assert_names(
            super::permission_outcome_name,
            &[
                (PermissionOutcome::Allow as i32, "allowed"),
                (PermissionOutcome::Deny as i32, "denied"),
                (887, "unspecified"),
            ],
        );
        assert_names(
            super::agent_lifecycle_name,
            &[
                (AgentLifecycleState::Queued as i32, "queued"),
                (AgentLifecycleState::Starting as i32, "starting"),
                (AgentLifecycleState::Running as i32, "running"),
                (
                    AgentLifecycleState::WaitingOnChild as i32,
                    "waiting on child",
                ),
                (
                    AgentLifecycleState::WaitingOnHuman as i32,
                    "waiting on human",
                ),
                (AgentLifecycleState::Idle as i32, "idle"),
                (AgentLifecycleState::Archived as i32, "archived"),
                (AgentLifecycleState::Failed as i32, "failed"),
                (887, "unspecified"),
            ],
        );
        assert_names(
            super::session_lifecycle_name,
            &[
                (SessionLifecycleState::Active as i32, "active"),
                (SessionLifecycleState::Archived as i32, "archived"),
                (887, "unspecified"),
            ],
        );
        assert_names(
            super::placement_state_name,
            &[
                (PlacementState::Placed as i32, "placed"),
                (PlacementState::Queued as i32, "queued"),
                (PlacementState::Expired as i32, "expired"),
                (PlacementState::Cancelled as i32, "cancelled"),
                (PlacementState::Lost as i32, "lost"),
                (887, "unspecified"),
            ],
        );
    }

    #[test]
    fn a_reason_joins_its_state_only_when_there_is_one() {
        assert_eq!(super::joined("idle", ""), "idle");
        assert_eq!(super::joined("idle", "reaped"), "idle: reaped");
    }

    /// The dispatcher never hands the machinery half a conversational kind,
    /// but its total-answer arm is a promise, and promises get tests.
    #[test]
    fn the_machinery_half_answers_unknown_for_a_conversational_kind() {
        let (label, text) = super::machinery_line(&Kind::NoteSent(NoteSent::default()));
        assert_eq!((label, text.as_str()), ("unknown", ""));
    }

    // -- Usage feed lines --------------------------------------------------

    fn recorded(record: horde_proto::v1::UsageRecord) -> horde_proto::v1::UsageRecorded {
        horde_proto::v1::UsageRecorded {
            record: Some(record),
        }
    }

    fn bound(provider: &str, account: &str) -> Option<horde_proto::v1::ModelBinding> {
        Some(horde_proto::v1::ModelBinding {
            tier: String::new(),
            provider: provider.to_owned(),
            model: String::new(),
            provider_account: account.to_owned(),
        })
    }

    /// A turn's usage record shows its measured token counts, attributed to
    /// the account that spent them.
    #[test]
    fn a_usage_record_shows_its_tokens_and_its_account() {
        let text = super::usage_text(&recorded(horde_proto::v1::UsageRecord {
            binding: bound("claude", "personal"),
            tokens: Some(horde_proto::v1::TokenUsage {
                input_tokens: 100,
                output_tokens: 40,
                ..horde_proto::v1::TokenUsage::default()
            }),
            rate_window: None,
            turn_id: String::new(),
        }));
        assert_eq!(text, "in 100 out 40  (claude/personal)");
    }

    /// A headroom-only record repeats the provider's own statement — counts,
    /// fraction, or serving state — never Horde's arithmetic over it.
    #[test]
    fn a_headroom_report_repeats_the_providers_statement() {
        let with_window = |window| {
            super::usage_text(&recorded(horde_proto::v1::UsageRecord {
                binding: bound("claude", "personal"),
                tokens: None,
                rate_window: Some(window),
                turn_id: String::new(),
            }))
        };

        let counted = horde_proto::v1::RateWindow {
            used: Some(75),
            limit: Some(100),
            ..horde_proto::v1::RateWindow::default()
        };
        assert_eq!(
            with_window(counted),
            "window 75/100 used  (claude/personal)"
        );

        let fractional = horde_proto::v1::RateWindow {
            used_fraction: Some(0.6),
            ..horde_proto::v1::RateWindow::default()
        };
        assert_eq!(
            with_window(fractional),
            "window 60% used  (claude/personal)"
        );

        let serving = horde_proto::v1::RateWindow {
            allowed: Some(true),
            ..horde_proto::v1::RateWindow::default()
        };
        assert_eq!(with_window(serving), "window serving  (claude/personal)");

        let refusing = horde_proto::v1::RateWindow {
            allowed: Some(false),
            ..horde_proto::v1::RateWindow::default()
        };
        assert_eq!(
            with_window(refusing),
            "window not serving  (claude/personal)"
        );

        let silent = horde_proto::v1::RateWindow::default();
        assert_eq!(
            with_window(silent),
            "(claude/personal)",
            "a window that stated nothing adds nothing"
        );
    }

    /// The empties stay empty rather than inventing punctuation: a default
    /// event still renders (the feed is total), and an unattributed record
    /// carries no `(/)`.
    #[test]
    fn usage_lines_degrade_without_inventing_content() {
        assert_eq!(
            super::usage_text(&horde_proto::v1::UsageRecorded::default()),
            ""
        );

        let spend_under = |binding: Option<horde_proto::v1::ModelBinding>| {
            super::usage_text(&recorded(horde_proto::v1::UsageRecord {
                binding,
                tokens: Some(horde_proto::v1::TokenUsage {
                    input_tokens: 1,
                    output_tokens: 2,
                    ..horde_proto::v1::TokenUsage::default()
                }),
                rate_window: None,
                turn_id: String::new(),
            }))
        };
        assert_eq!(spend_under(None), "in 1 out 2");
        assert_eq!(
            spend_under(bound("", "")),
            "in 1 out 2",
            "an all-empty binding is unattributed, not `(/)`"
        );
    }

    /// All four corners of the attribution predicate. The mixed rows are the
    /// ones `&&` and `||` disagree on, and neither arises from the adapters
    /// today — a real binding names both halves or neither — so only the
    /// table can hold the operator.
    #[test]
    fn a_half_named_binding_still_attributes() {
        let account_of =
            |provider: &str, account: &str| super::account_of(bound(provider, account).as_ref());
        assert_eq!(
            account_of("claude", "personal").as_deref(),
            Some("(claude/personal)")
        );
        assert_eq!(
            account_of("claude", "").as_deref(),
            Some("(claude/)"),
            "a provider with no account name is still worth attributing"
        );
        assert_eq!(
            account_of("", "personal").as_deref(),
            Some("(/personal)"),
            "an account with no provider is still worth attributing"
        );
        assert_eq!(account_of("", ""), None);
        assert_eq!(super::account_of(None), None);
    }

    /// The forward feed lines (ADR-0006): an open renders the ADR's own
    /// `name → localhost:port` shape with the backer named, and a close
    /// names what closed and why.
    #[test]
    fn forward_events_render_as_the_adr_promises() {
        use horde_proto::v1::{
            ClientId, ForwardClosed, ForwardDirection, ForwardId, ForwardOpened, ForwardSpec,
        };

        let opened = Kind::ForwardOpened(ForwardOpened {
            forward_id: Some(ForwardId::new("fwd-1")),
            spec: Some(ForwardSpec {
                direction: ForwardDirection::Inject as i32,
                name: "db".to_owned(),
                worker_port: 15432,
                client_port: 5432,
            }),
            opened_by: None,
            backer_client_id: Some(ClientId::new("laptop")),
        });
        let (label, text) = super::describe(Some(&opened));
        assert_eq!(label, "forward");
        assert_eq!(text, "db → localhost:15432 (inject), backed by laptop");

        let unbacked = Kind::ForwardOpened(ForwardOpened {
            forward_id: Some(ForwardId::new("fwd-2")),
            spec: Some(ForwardSpec {
                direction: ForwardDirection::Expose as i32,
                name: "vite".to_owned(),
                worker_port: 5173,
                client_port: 0,
            }),
            opened_by: None,
            backer_client_id: None,
        });
        let (_label, text) = super::describe(Some(&unbacked));
        assert_eq!(text, "vite → localhost:5173 (expose)");

        let specless = Kind::ForwardOpened(ForwardOpened::default());
        let (_label, text) = super::describe(Some(&specless));
        assert_eq!(
            text, "",
            "a specless open renders nothing rather than lying"
        );

        let closed = Kind::ForwardClosed(ForwardClosed {
            forward_id: Some(ForwardId::new("fwd-1")),
            closed_by: None,
            reason: "done".to_owned(),
        });
        let (label, text) = super::describe(Some(&closed));
        assert_eq!(label, "forward");
        assert_eq!(text, "closed fwd-1: done");
    }

    #[test]
    fn forward_directions_name_themselves() {
        use horde_proto::v1::ForwardDirection;
        assert_eq!(
            super::forward_direction_name(ForwardDirection::Expose as i32),
            "expose"
        );
        assert_eq!(
            super::forward_direction_name(ForwardDirection::Inject as i32),
            "inject"
        );
        assert_eq!(super::forward_direction_name(99), "unspecified");
    }
}
