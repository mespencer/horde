//! The TUI's state machine, headless by construction.
//!
//! [`App`] consumes [`Update`]s (what the server said) and terminal input, and
//! produces [`Directive`]s (what the runtime should ask the server for). It
//! never touches a socket or a terminal itself: the interactive loop in
//! `horde-cli` is a thin pump around this type, which is what lets every
//! screen and key be exercised without a TTY — ratatui's `TestBackend` for the
//! pixels, plain method calls for the keys.
//!
//! DESIGN.md §11 names the surfaces. The main one is the attached agent's
//! transcript with a compose box, prompts answerable in place and slash
//! commands, in the manner of Claude Code; around it sit the session/agent
//! tree, the global inbox, workspaces, usage, logs and forwards. All of them
//! are folds over the same event streams or plain RPC answers; no screen has
//! private data.

use std::collections::{BTreeMap, BTreeSet};

use horde_proto::v1::{
    AccountHeadroom, EventEnvelope, ForwardStatus, SessionSummary, UsageAggregate,
    WorkerSummary, WorkspaceDefinition, WorkspaceInstanceReport,
};

use crate::compose::Compose;
use crate::prompt::PromptCursor;
use crate::state::{PendingPermission, SessionState};

mod input;

pub use crate::keys::Screen;

/// Something the server told us, fed in by the runtime.
#[derive(Debug, Clone, PartialEq)]
pub enum Update {
    /// A `ListSessions` answer.
    Sessions(Vec<SessionSummary>),
    /// One event from the follow stream. Boxed: an envelope dwarfs the other
    /// variants, and every update would pay its size otherwise.
    Event(Box<EventEnvelope>),
    /// A session's replay finished; everything after is live.
    CaughtUp { session_id: String, head: u64 },
    /// The stream said "still alive, nothing to say" — the server's
    /// staleness signal (DESIGN §11, fix a9).
    Heartbeat,
    /// The runtime's clock, so staleness can be *judged*: a dead stream
    /// produces no updates, and only something outside the stream can notice
    /// that.
    Tick { now: std::time::Instant },
    /// A `ListForwards` answer (ADR-0006).
    Forwards(Vec<ForwardStatus>),
    /// A `ListWorkers` answer: names for worker ids, and the workers view.
    Workers(Vec<WorkerSummary>),
    /// Workspace definitions with their known instances.
    Workspaces(Vec<WorkspaceEntry>),
    /// A usage query's answer.
    Usage(UsageReport),
    /// A session this client just created; the App attaches to it.
    SessionCreated { session_id: String },
    /// The terminal's size, so scrolling can clamp to what fits.
    Resize { width: u16, height: u16 },
    /// The follow stream closed — the server hung up, or refused the
    /// subscription. Empty for a clean end-of-stream.
    StreamEnded { reason: String },
}

/// One workspace definition and what the server knows of its instances.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkspaceEntry {
    pub definition: WorkspaceDefinition,
    /// Empty when the server reports none, or could not be asked.
    pub instances: Vec<WorkspaceInstanceReport>,
}

/// Usage, three ways: per agent and per tree path (scoped to the attached
/// session when there is one), and per account.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UsageReport {
    /// What the agent and tree rows cover, in words.
    pub scope: String,
    pub by_agent: Vec<UsageAggregate>,
    pub by_tree: Vec<UsageAggregate>,
    pub accounts: Vec<AccountHeadroom>,
}

/// How long the stream may say nothing before the TUI calls it stale: three
/// of the server's 15s heartbeat periods.
pub const STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(45);

/// Whether silence since `last` amounts to a stale stream at `now` —
/// strictly past the ceiling.
fn is_stale(last: Option<std::time::Instant>, now: std::time::Instant) -> bool {
    last.is_some_and(|last| now.saturating_duration_since(last) > STALE_AFTER)
}

/// Something the runtime should ask the server for on the App's behalf.
/// Every variant is a `horde <verb>` too (DESIGN.md §11: CLI parity).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Directive {
    RefreshSessions,
    RefreshForwards,
    RefreshWorkers,
    RefreshWorkspaces,
    /// Usage for one session (or every session when `None`).
    RefreshUsage {
        session_id: Option<String>,
    },
    Approve {
        request_id: String,
        /// Widen the allow rules for the rest of the session.
        remember: bool,
    },
    Deny {
        request_id: String,
    },
    /// Answer a question request: question id to chosen labels or text.
    Answer {
        request_id: String,
        answers: BTreeMap<String, Vec<String>>,
    },
    /// Queue a message for the agent's next turn boundary.
    SendAssignment {
        agent_id: String,
        text: String,
    },
    /// Cancel the current turn and deliver at once.
    SendNow {
        agent_id: String,
        text: String,
    },
    SendNote {
        agent_id: String,
        text: String,
    },
    Interrupt {
        agent_id: String,
    },
    /// Create a session; the runtime answers with [`Update::SessionCreated`].
    NewSession {
        title: Option<String>,
    },
    Archive {
        session_id: String,
    },
}

/// What the client is attached to: a session, optionally scoped to one agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attachment {
    pub session_id: String,
    /// `None` is session-root attachment, which sees everything.
    pub agent_id: Option<String>,
}

/// One row of the global inbox: a pending escalation and the session holding
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxEntry {
    pub session_id: String,
    pub title: String,
    pub permission: PendingPermission,
}

/// One row of the session/agent tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeRow {
    Session {
        session_id: String,
    },
    Agent {
        session_id: String,
        agent_id: String,
        depth: usize,
    },
}

/// A question the App is waiting on before it acts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirm {
    Archive { session_id: String },
}

/// The whole client-side view state.
#[derive(Debug, Clone, PartialEq)]
pub struct App {
    sessions: BTreeMap<String, SessionState>,
    screen: Screen,
    /// Where Esc returns to from a list screen.
    previous: Screen,
    attachment: Option<Attachment>,
    /// The scope `horde attach --agent` launched with, restored on re-attach
    /// (fix a14).
    launched_scope: Option<Attachment>,
    compose: Compose,
    /// The request id the human moved focus off (to the compose box); the
    /// next prompt takes focus again.
    compose_focus_for: Option<String>,
    /// The cursor over the front prompt (inline) or the inbox card.
    cursor: Option<PromptCursor>,
    /// The inbox card has focus.
    inbox_card: bool,
    /// Requests answered from here and not yet decided on the log.
    submitted: BTreeSet<String>,
    /// The highlighted row of the slash-command menu.
    menu: usize,
    /// Tool input and output shown in full.
    details: bool,
    /// Transcript rows scrolled back from the tail.
    scroll: usize,
    viewport: (u16, u16),
    selected: BTreeMap<Screen, usize>,
    forwards: Vec<ForwardStatus>,
    workers: Vec<WorkerSummary>,
    workspaces: Option<Vec<WorkspaceEntry>>,
    usage: Option<UsageReport>,
    confirm: Option<Confirm>,
    /// The last refusal or notice, shown until the next one replaces it.
    status: String,
    /// Sessions whose history the stream must carry.
    replayed: BTreeSet<String>,
    /// Replay everything listed (the overview launch, or after the inbox or
    /// tree asked for it).
    replay_all: bool,
    /// The replay set grew; the runtime should resubscribe.
    resubscribe: bool,
    last_stream_activity: Option<std::time::Instant>,
    stream_stale: bool,
    stream_ended: Option<String>,
    quit: bool,
}

impl App {
    /// Starts at the session/agent tree with nothing attached.
    #[must_use]
    pub fn overview() -> Self {
        Self {
            sessions: BTreeMap::new(),
            screen: Screen::Overview,
            previous: Screen::Overview,
            attachment: None,
            launched_scope: None,
            compose: Compose::default(),
            compose_focus_for: None,
            cursor: None,
            inbox_card: false,
            submitted: BTreeSet::new(),
            menu: 0,
            details: false,
            scroll: 0,
            viewport: (80, 24),
            selected: BTreeMap::new(),
            forwards: Vec::new(),
            workers: Vec::new(),
            workspaces: None,
            usage: None,
            confirm: None,
            status: String::new(),
            replayed: BTreeSet::new(),
            replay_all: true,
            resubscribe: false,
            last_stream_activity: None,
            stream_stale: false,
            stream_ended: None,
            quit: false,
        }
    }

    /// Starts attached — `horde attach <session> [--agent <id>]`.
    #[must_use]
    pub fn attached(session_id: impl Into<String>, agent_id: Option<String>) -> Self {
        let session_id = session_id.into();
        let mut app = Self::overview();
        app.replay_all = false;
        app.sessions
            .insert(session_id.clone(), SessionState::new(session_id.clone()));
        app.replayed.insert(session_id.clone());
        let attachment = Attachment {
            session_id,
            agent_id,
        };
        app.attachment = Some(attachment.clone());
        app.launched_scope = Some(attachment);
        app.screen = Screen::Session;
        app.previous = Screen::Session;
        app
    }

    // -- What the runtime and the views read --------------------------------

    #[must_use]
    pub fn should_quit(&self) -> bool {
        self.quit
    }

    #[must_use]
    pub fn screen(&self) -> Screen {
        self.screen
    }

    #[must_use]
    pub fn attachment(&self) -> Option<&Attachment> {
        self.attachment.as_ref()
    }

    #[must_use]
    pub fn status(&self) -> &str {
        &self.status
    }

    /// The compose box's text.
    #[must_use]
    pub fn input(&self) -> String {
        self.compose.text()
    }

    #[must_use]
    pub fn compose(&self) -> &Compose {
        &self.compose
    }

    #[must_use]
    pub fn scroll(&self) -> usize {
        self.scroll
    }

    #[must_use]
    pub fn details(&self) -> bool {
        self.details
    }

    #[must_use]
    pub fn menu(&self) -> usize {
        self.menu
    }

    #[must_use]
    pub fn confirm(&self) -> Option<&Confirm> {
        self.confirm.as_ref()
    }

    #[must_use]
    pub fn inbox_card(&self) -> bool {
        self.inbox_card
    }

    /// Whether this request was answered from here and awaits its decision.
    #[must_use]
    pub fn is_submitted(&self, request_id: &str) -> bool {
        self.submitted.contains(request_id)
    }

    #[must_use]
    pub fn session(&self, session_id: &str) -> Option<&SessionState> {
        self.sessions.get(session_id)
    }

    /// The attached session's state.
    #[must_use]
    pub fn attached_session(&self) -> Option<&SessionState> {
        self.attachment
            .as_ref()
            .and_then(|attachment| self.sessions.get(&attachment.session_id))
    }

    /// What a human calls the session with this id: its title once known,
    /// a short id prefix otherwise.
    #[must_use]
    pub fn session_name(&self, session_id: &str) -> String {
        self.sessions.get(session_id).map_or_else(
            || crate::state::short_id(session_id),
            SessionState::display_name,
        )
    }

    /// Sessions in stable id order.
    #[must_use]
    pub fn sessions_in_order(&self) -> Vec<&SessionState> {
        self.sessions.values().collect()
    }

    /// The selected row on a list screen.
    #[must_use]
    pub fn selected(&self, screen: Screen) -> usize {
        self.selected.get(&screen).copied().unwrap_or(0)
    }

    #[must_use]
    pub fn forwards(&self) -> &[ForwardStatus] {
        &self.forwards
    }

    #[must_use]
    pub fn workers(&self) -> &[WorkerSummary] {
        &self.workers
    }

    /// `None` until the server has been asked.
    #[must_use]
    pub fn workspaces(&self) -> Option<&[WorkspaceEntry]> {
        self.workspaces.as_deref()
    }

    /// `None` until the server has been asked.
    #[must_use]
    pub fn usage(&self) -> Option<&UsageReport> {
        self.usage.as_ref()
    }

    /// A worker's name, or a short id when the worker list does not know it.
    #[must_use]
    pub fn worker_name(&self, worker_id: &str) -> String {
        self.workers
            .iter()
            .find(|worker| {
                worker
                    .worker_id
                    .as_ref()
                    .is_some_and(|id| id.to_string() == worker_id)
            })
            .map(|worker| worker.name.clone())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| crate::state::short_id(worker_id))
    }

    /// A server refusal worth showing; the TUI outlives it.
    pub fn report_error(&mut self, message: impl Into<String>) {
        self.status = message.into();
    }

    /// A directive the server refused: say why, and un-mark a prompt
    /// answered from here so it can be answered again.
    pub fn directive_failed(&mut self, directive: &Directive, message: impl Into<String>) {
        if let Directive::Approve { request_id, .. }
        | Directive::Deny { request_id }
        | Directive::Answer { request_id, .. } = directive
        {
            self.submitted.remove(request_id);
        }
        self.report_error(message);
    }

    /// Whether the runtime should reopen the stream with
    /// [`Self::replay_cursors`]. Reading clears it.
    pub fn take_resubscribe(&mut self) -> bool {
        std::mem::take(&mut self.resubscribe)
    }

    /// The sessions the stream must replay, each from just past what has
    /// already been folded — a reopened stream never re-delivers history.
    #[must_use]
    pub fn replay_cursors(&self) -> Vec<(String, u64)> {
        self.replayed
            .iter()
            .map(|session_id| {
                let folded = self.sessions.get(session_id).map_or(0, |session| session.stream_head);
                (session_id.clone(), folded + 1)
            })
            .collect()
    }

    // -- The agent in view ---------------------------------------------------

    /// The agent whose transcript the session screen shows: the attached
    /// agent, or the session's root.
    #[must_use]
    pub fn viewed_agent(&self) -> Option<String> {
        self.steer_target()
    }

    /// Who a send or interrupt from this view steers: the attached agent, or
    /// the session's top-level agent for session-root attachment.
    #[must_use]
    pub fn steer_target(&self) -> Option<String> {
        let attachment = self.attachment.as_ref()?;
        if let Some(agent_id) = attachment.agent_id.as_ref() {
            return Some(agent_id.clone());
        }
        self.sessions
            .get(&attachment.session_id)
            .and_then(|session| session.root_agent_id.clone())
    }

    /// Whether the agent in view has a turn in flight.
    #[must_use]
    pub fn agent_working(&self) -> bool {
        let Some(agent_id) = self.viewed_agent() else {
            return false;
        };
        self.attached_session()
            .and_then(|session| session.agents.get(&agent_id))
            .is_some_and(|node| node.working)
    }

    // -- Prompts -------------------------------------------------------------

    /// Every escalation awaiting a human, across all sessions, oldest first
    /// within a session.
    #[must_use]
    pub fn inbox_entries(&self) -> Vec<InboxEntry> {
        self.sessions
            .values()
            .flat_map(|session| {
                session
                    .pending_in_order()
                    .into_iter()
                    .map(|permission| InboxEntry {
                        session_id: session.session_id.clone(),
                        title: session.display_name(),
                        permission: permission.clone(),
                    })
            })
            .collect()
    }

    /// How many escalations are waiting anywhere.
    #[must_use]
    pub fn waiting_total(&self) -> usize {
        self.sessions
            .values()
            .map(|session| session.pending.len())
            .sum()
    }

    /// The escalations the current attachment is scoped to, oldest first.
    /// Session-root attachment sees the whole session; attachment to an
    /// agent sees that agent's subtree only (DESIGN.md §11).
    #[must_use]
    pub fn scoped_pending(&self) -> Vec<PendingPermission> {
        let Some(attachment) = self.attachment.as_ref() else {
            return Vec::new();
        };
        let Some(session) = self.sessions.get(&attachment.session_id) else {
            return Vec::new();
        };
        session
            .pending_in_order()
            .into_iter()
            .filter(|permission| in_scope(session, attachment, permission))
            .cloned()
            .collect()
    }

    /// The passive badge: escalations waiting outside the attached scope.
    #[must_use]
    pub fn waiting_elsewhere(&self) -> usize {
        self.waiting_total() - self.scoped_pending().len()
    }

    /// The prompt the session screen offers in place: the oldest in scope.
    #[must_use]
    pub fn front_prompt(&self) -> Option<PendingPermission> {
        self.scoped_pending().into_iter().next()
    }

    /// Whether keys go to the front prompt rather than the compose box: a
    /// prompt is waiting and the human has not moved focus off it.
    #[must_use]
    pub fn prompt_focused(&self) -> bool {
        self.front_prompt()
            .is_some_and(|front| self.compose_focus_for.as_ref() != Some(&front.request_id))
    }

    /// The cursor to draw over `request`: the live one when it is on this
    /// request, a fresh one otherwise.
    #[must_use]
    pub fn cursor_for(&self, request: &PendingPermission) -> PromptCursor {
        self.cursor
            .clone()
            .filter(|cursor| cursor.request_id == request.request_id)
            .unwrap_or_else(|| PromptCursor::new(request))
    }

    // -- The session/agent tree ---------------------------------------------

    /// The tree's rows: each session, then its agents depth-first.
    #[must_use]
    pub fn tree_rows(&self) -> Vec<TreeRow> {
        let mut rows = Vec::new();
        for session in self.sessions.values() {
            rows.push(TreeRow::Session {
                session_id: session.session_id.clone(),
            });
            let roots = session
                .agents
                .iter()
                .filter(|(_, node)| node.parent.is_none())
                .map(|(id, _)| id.clone());
            for root in roots {
                push_subtree(&mut rows, session, &root, 0);
            }
        }
        rows
    }

    // -- Updates ------------------------------------------------------------

    /// Folds in something the server said.
    pub fn apply(&mut self, update: Update) {
        match update {
            Update::Sessions(summaries) => self.absorb_sessions(&summaries),
            Update::Event(envelope) => {
                let session_id = envelope
                    .session_id
                    .as_ref()
                    .map_or_else(String::new, ToString::to_string);
                self.forget_settled(&envelope);
                self.session_entry(session_id).apply(&envelope);
                self.mark_stream_alive();
            }
            Update::CaughtUp { session_id, head } => {
                let session = self.session_entry(session_id);
                session.caught_up = true;
                session.head = session.head.max(head);
                self.mark_stream_alive();
            }
            Update::Heartbeat => self.mark_stream_alive(),
            Update::Tick { now } => self.stream_stale = is_stale(self.last_stream_activity, now),
            other => self.apply_answer(other),
        }
    }

    /// The RPC-answer half of [`Self::apply`].
    fn apply_answer(&mut self, update: Update) {
        match update {
            Update::Forwards(forwards) => {
                self.forwards = forwards;
                self.clamp_selection(Screen::Forwards, self.forwards.len());
            }
            Update::Workers(workers) => self.workers = workers,
            Update::Workspaces(entries) => self.workspaces = Some(entries),
            Update::Usage(report) => self.usage = Some(report),
            Update::SessionCreated { session_id } => {
                self.session_entry(session_id.clone());
                self.attach_to(session_id, None);
            }
            Update::Resize { width, height } => self.viewport = (width, height),
            Update::StreamEnded { reason } => self.stream_ended = Some(reason),
            _ => {}
        }
    }

    fn absorb_sessions(&mut self, summaries: &[SessionSummary]) {
        for summary in summaries {
            let Some(session_id) = summary.session_id.as_ref().map(ToString::to_string) else {
                continue;
            };
            if self.replay_all && self.replayed.insert(session_id.clone()) {
                self.resubscribe = true;
            }
            self.session_entry(session_id).absorb_summary(summary);
        }
    }

    /// A decision on the log settles a request answered from here.
    fn forget_settled(&mut self, envelope: &EventEnvelope) {
        if let Some(horde_proto::v1::event::Kind::PermissionDecided(decided)) =
            envelope.event.as_ref().and_then(|event| event.kind.as_ref())
            && let Some(id) = decided
                .decision
                .as_ref()
                .and_then(|decision| decision.request_id.as_ref())
        {
            self.submitted.remove(&id.to_string());
        }
    }

    /// Why the follow stream ended — `Some("")` for a clean close, `None`
    /// while it is still open.
    #[must_use]
    pub fn stream_ended(&self) -> Option<&str> {
        self.stream_ended.as_deref()
    }

    /// Every stream-borne update is proof of life; RPC answers are not.
    fn mark_stream_alive(&mut self) {
        self.last_stream_activity = Some(std::time::Instant::now());
        self.stream_stale = false;
    }

    #[must_use]
    pub fn stream_stale(&self) -> bool {
        self.stream_stale
    }

    /// The connection in one word, for the status line.
    #[must_use]
    pub fn connection_word(&self) -> &'static str {
        if self.stream_ended.is_some() {
            "ended"
        } else if self.stream_stale {
            "stale"
        } else if self.last_stream_activity.is_none() {
            "connecting"
        } else if self.attached_session().is_some_and(|session| !session.caught_up) {
            "syncing"
        } else {
            "live"
        }
    }

    fn session_entry(&mut self, session_id: String) -> &mut SessionState {
        self.sessions
            .entry(session_id.clone())
            .or_insert_with(|| SessionState::new(session_id))
    }

    fn clamp_selection(&mut self, screen: Screen, len: usize) {
        let selected = self.selected(screen).min(len.saturating_sub(1));
        self.selected.insert(screen, selected);
    }

    // -- Attaching -------------------------------------------------------------

    /// Attaches to a session (scoped to `agent_id` when given), replaying it
    /// if the stream does not carry its history yet.
    fn attach_to(&mut self, session_id: String, agent_id: Option<String>) {
        let agent_id = agent_id.or_else(|| {
            self.launched_scope
                .as_ref()
                .filter(|scope| scope.session_id == session_id)
                .and_then(|scope| scope.agent_id.clone())
        });
        if self.replayed.insert(session_id.clone()) {
            self.resubscribe = true;
        }
        self.attachment = Some(Attachment {
            session_id,
            agent_id,
        });
        self.screen = Screen::Session;
        self.previous = Screen::Session;
        self.scroll = 0;
        self.cursor = None;
        self.compose_focus_for = None;
    }

    /// Every listed session's history, for the screens that look across
    /// sessions (the tree and the inbox).
    fn replay_everything(&mut self) {
        self.replay_all = true;
        let missing: Vec<String> = self
            .sessions
            .keys()
            .filter(|id| !self.replayed.contains(*id))
            .cloned()
            .collect();
        if !missing.is_empty() {
            self.resubscribe = true;
        }
        self.replayed.extend(missing);
    }

    /// How far back the transcript can scroll at the current size.
    fn scroll_ceiling(&self) -> usize {
        let Some(transcript) = self.viewed_agent().and_then(|agent| {
            self.attached_session()
                .and_then(|session| session.transcripts.get(&agent))
        }) else {
            return 0;
        };
        let rows = crate::layout::transcript_lines(transcript, self.details, self.viewport.0).len();
        rows.saturating_sub(usize::from(self.viewport.1 / 2))
    }
}

fn push_subtree(rows: &mut Vec<TreeRow>, session: &SessionState, agent_id: &str, depth: usize) {
    rows.push(TreeRow::Agent {
        session_id: session.session_id.clone(),
        agent_id: agent_id.to_owned(),
        depth,
    });
    // Counted ceiling, as in `in_subtree`: a malformed parent chain could
    // cycle, and no real tree is deeper than it has agents.
    if depth >= session.agents.len() {
        return;
    }
    let children = session
        .agents
        .iter()
        .filter(|(_, node)| node.parent.as_deref() == Some(agent_id))
        .map(|(id, _)| id.clone());
    for child in children {
        push_subtree(rows, session, &child, depth + 1);
    }
}

/// Whether the attachment's scope claims this escalation.
fn in_scope(session: &SessionState, attachment: &Attachment, permission: &PendingPermission) -> bool {
    match attachment.agent_id.as_ref() {
        None => true,
        Some(root) => session.in_subtree(root, &permission.agent_id),
    }
}

#[cfg(test)]
mod tests;
