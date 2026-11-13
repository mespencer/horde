//! The TUI's state machine, headless by construction.
//!
//! [`App`] consumes [`Update`]s (what the server said) and terminal input, and
//! produces [`Directive`]s (what the runtime should ask the server for). It
//! never touches a socket or a terminal itself: the interactive loop in
//! `horde-cli` is a thin pump around this type, which is what lets every
//! screen and key be exercised without a TTY — ratatui's `TestBackend` for the
//! pixels, plain method calls for the keys.
//!
//! DESIGN.md §11 names the surfaces: the multi-session overview, per-session
//! attach (optionally scoped to one agent's subtree), and the global inbox of
//! escalations awaiting a human. All three are folds over the same event
//! streams; no screen has private data.

use std::collections::BTreeMap;

use horde_proto::v1::{EventEnvelope, SessionSummary};
use ratatui::crossterm::event::{
    Event as TermEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};

use crate::state::{PendingPermission, SessionState};

/// Something the server told us, fed in by the runtime's stream pumps.
#[derive(Debug, Clone, PartialEq)]
pub enum Update {
    /// A `ListSessions` answer.
    Sessions(Vec<SessionSummary>),
    /// One event from a session's follow stream. Boxed: an envelope dwarfs
    /// the other variants, and every update would pay its size otherwise.
    Event(Box<EventEnvelope>),
    /// A session's replay finished; everything after is live.
    CaughtUp { session_id: String, head: u64 },
    /// The stream said "still alive, nothing to say" — the server's
    /// staleness signal (DESIGN §11, fix a9). Carries nothing to render, and
    /// exists precisely so quiet and dead stop looking identical.
    Heartbeat,
    /// The runtime's clock, so staleness can be *judged*: a dead stream
    /// produces no updates, and only something outside the stream can notice
    /// that. Carries the time so the judgment is a pure function a test can
    /// drive.
    Tick { now: std::time::Instant },
    /// A `ListForwards` answer (ADR-0006): the live intents plus the
    /// realization overlay — reachability and backing — that only the
    /// server's connection registry can know. An RPC answer like
    /// [`Update::Sessions`], not a fold: the log deliberately carries no
    /// listener state, so a fold could never say "unbacked".
    Forwards(Vec<horde_proto::v1::ForwardStatus>),
    /// The follow stream closed — the server hung up, or refused the
    /// subscription. Carries why, when the runtime knows; empty for a clean
    /// end-of-stream. Nothing more will arrive, and saying so beats a feed
    /// that silently freezes until the stale badge eventually guesses.
    StreamEnded { reason: String },
}

/// How long the stream may say nothing — no events, no heartbeats — before
/// the TUI calls it stale. Three of the server's 15s heartbeat periods: one
/// missed beat is jitter, three is a dead stream.
pub const STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(45);

/// Whether silence since `last` amounts to a stale stream at `now`.
///
/// Strictly past the ceiling: silence of *exactly* [`STALE_AFTER`] is the
/// third heartbeat arriving on the buzzer, not a dead stream. A pure
/// function of both instants because the boundary is the whole point, and a
/// judgment buried in `apply` (where `last` is stamped by a live clock)
/// cannot have its boundary exercised.
fn is_stale(last: Option<std::time::Instant>, now: std::time::Instant) -> bool {
    last.is_some_and(|last| now.saturating_duration_since(last) > STALE_AFTER)
}

/// Something the runtime should ask the server for on the App's behalf.
///
/// Screen navigation never appears here — it is the App's own state. Only the
/// verbs that cross the wire do, which keeps the interactive loop a dispatch
/// table over [`crate::App::on_input`] with no decisions of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Directive {
    RefreshSessions,
    /// Ask the server for the live forwards and their realization overlay.
    RefreshForwards,
    Approve {
        request_id: String,
    },
    Deny {
        request_id: String,
    },
    /// Answer a question request (W12c): question id to chosen labels.
    Answer {
        request_id: String,
        answers: std::collections::BTreeMap<String, Vec<String>>,
    },
    SendAssignment {
        agent_id: String,
        text: String,
    },
    Interrupt {
        agent_id: String,
    },
}

/// Which screen is showing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Overview,
    Session,
    Inbox,
    /// The forward panel: named forwards with backer/unbacked state.
    Forwards,
}

/// What the client is attached to: a session, optionally scoped to one agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attachment {
    pub session_id: String,
    /// `None` is session-root attachment, which sees everything.
    pub agent_id: Option<String>,
}

/// One row of the global inbox: a pending escalation and the session holding
/// it. Owned rather than borrowed so key handling can act on a row while
/// mutating the App.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxEntry {
    pub session_id: String,
    pub title: String,
    pub permission: PendingPermission,
}

/// The whole client-side view state.
#[derive(Debug, Clone, PartialEq)]
pub struct App {
    sessions: BTreeMap<String, SessionState>,
    screen: Screen,
    attachment: Option<Attachment>,
    overview_selected: usize,
    inbox_selected: usize,
    forwards_selected: usize,
    /// The last `ListForwards` answer, whole. Held rather than folded
    /// because backing and reachability are connection facts the log cannot
    /// carry; `r` re-asks.
    forwards: Vec<horde_proto::v1::ForwardStatus>,
    /// The session view's compose line.
    input: String,
    /// How many feed lines back from the tail the session view is scrolled.
    scroll: usize,
    /// The last refusal or notice, shown until the next one replaces it.
    status: String,
    /// The agent scope `horde attach --agent` launched with, remembered so
    /// detaching and re-attaching to that session restores it (fix a14) —
    /// before this, a detach silently widened the view to the session root.
    launched_scope: Option<Attachment>,
    /// When the stream last said anything — an event, a caught-up, a
    /// heartbeat. `None` until it first speaks.
    last_stream_activity: Option<std::time::Instant>,
    /// Judged on each [`Update::Tick`] against [`STALE_AFTER`].
    stream_stale: bool,
    /// Set once the follow stream has closed, with its reason (possibly
    /// empty). Never cleared: this loop opens one stream and does not
    /// reopen it, so an ended stream stays ended.
    stream_ended: Option<String>,
    quit: bool,
}

impl App {
    /// Starts at the multi-session overview — plain `horde`.
    #[must_use]
    pub fn overview() -> Self {
        Self {
            sessions: BTreeMap::new(),
            screen: Screen::Overview,
            attachment: None,
            overview_selected: 0,
            inbox_selected: 0,
            forwards_selected: 0,
            forwards: Vec::new(),
            input: String::new(),
            scroll: 0,
            status: String::new(),
            launched_scope: None,
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
        app.sessions
            .insert(session_id.clone(), SessionState::new(session_id.clone()));
        let attachment = Attachment {
            session_id,
            agent_id,
        };
        app.attachment = Some(attachment.clone());
        // Remembered so a detach and re-attach restores the same scope
        // rather than silently widening to the session root (fix a14).
        app.launched_scope = Some(attachment);
        app.screen = Screen::Session;
        app
    }

    // -- What the runtime reads --------------------------------------------

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

    #[must_use]
    pub fn input(&self) -> &str {
        &self.input
    }

    #[must_use]
    pub fn scroll(&self) -> usize {
        self.scroll
    }

    #[must_use]
    pub fn session(&self, session_id: &str) -> Option<&SessionState> {
        self.sessions.get(session_id)
    }

    /// What a human calls the session with this id: its title once known,
    /// a short id prefix otherwise — including for a session this App has
    /// never heard of (a forward can name one the stream has not replayed).
    #[must_use]
    pub fn session_name(&self, session_id: &str) -> String {
        self.sessions.get(session_id).map_or_else(
            || crate::state::short_id(session_id),
            SessionState::display_name,
        )
    }

    /// Sessions in stable id order — the overview's rows.
    #[must_use]
    pub fn sessions_in_order(&self) -> Vec<&SessionState> {
        self.sessions.values().collect()
    }

    #[must_use]
    pub fn overview_selected(&self) -> usize {
        self.overview_selected
    }

    #[must_use]
    pub fn inbox_selected(&self) -> usize {
        self.inbox_selected
    }

    #[must_use]
    pub fn forwards_selected(&self) -> usize {
        self.forwards_selected
    }

    /// The forward panel's rows: the last `ListForwards` answer, verbatim.
    #[must_use]
    pub fn forwards(&self) -> &[horde_proto::v1::ForwardStatus] {
        &self.forwards
    }

    /// A server refusal worth showing; the TUI outlives it (multi-attach means
    /// someone else may answer an escalation first, and losing the whole
    /// screen to their race would be absurd).
    pub fn report_error(&mut self, message: impl Into<String>) {
        self.status = message.into();
    }

    // -- The inbox ----------------------------------------------------------

    /// Every escalation awaiting a human, across all sessions, oldest first
    /// within a session. The primary "what needs me" surface.
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

    /// How many escalations are waiting anywhere — the overview badge.
    #[must_use]
    pub fn waiting_total(&self) -> usize {
        self.sessions
            .values()
            .map(|session| session.pending.len())
            .sum()
    }

    /// The escalations the current attachment is scoped to, oldest first.
    ///
    /// Session-root attachment sees the whole session; attachment to an agent
    /// sees that agent's subtree only (DESIGN.md §11).
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

    // -- Updates ------------------------------------------------------------

    /// Folds in something the server said.
    pub fn apply(&mut self, update: Update) {
        match update {
            Update::Sessions(summaries) => {
                for summary in &summaries {
                    let Some(session_id) = summary.session_id.as_ref().map(ToString::to_string)
                    else {
                        // A summary with no id is unaddressable; there is
                        // nothing coherent to fold it into.
                        continue;
                    };
                    self.session_entry(session_id).absorb_summary(summary);
                }
            }
            Update::Event(envelope) => {
                let session_id = envelope
                    .session_id
                    .as_ref()
                    .map_or_else(String::new, ToString::to_string);
                self.session_entry(session_id).apply(&envelope);
                self.mark_stream_alive();
            }
            Update::CaughtUp { session_id, head } => {
                let session = self.session_entry(session_id);
                session.caught_up = true;
                session.head = session.head.max(head);
                self.mark_stream_alive();
            }
            // Proof of life and nothing else — the variant's whole job.
            Update::Heartbeat => self.mark_stream_alive(),
            Update::Tick { now } => {
                self.stream_stale = is_stale(self.last_stream_activity, now);
            }
            Update::Forwards(forwards) => {
                self.forwards = forwards;
                self.forwards_selected = self
                    .forwards_selected
                    .min(self.forwards.len().saturating_sub(1));
            }
            Update::StreamEnded { reason } => self.stream_ended = Some(reason),
        }
    }

    /// Why the follow stream ended — `Some("")` for a clean close, `None`
    /// while it is still open.
    #[must_use]
    pub fn stream_ended(&self) -> Option<&str> {
        self.stream_ended.as_deref()
    }

    /// Every stream-borne update — an event, a caught-up, a heartbeat — is
    /// proof of life. The session list is an RPC answer and deliberately is
    /// not: a live control plane says nothing about a dead follow stream.
    fn mark_stream_alive(&mut self) {
        self.last_stream_activity = Some(std::time::Instant::now());
        self.stream_stale = false;
    }

    /// Whether the follow stream has said nothing for [`STALE_AFTER`] — the
    /// badge that keeps a dead stream from looking like a quiet one.
    #[must_use]
    pub fn stream_stale(&self) -> bool {
        self.stream_stale
    }

    fn session_entry(&mut self, session_id: String) -> &mut SessionState {
        self.sessions
            .entry(session_id.clone())
            .or_insert_with(|| SessionState::new(session_id))
    }

    // -- Input --------------------------------------------------------------

    /// Handles one terminal event, returning what the runtime should do.
    pub fn on_input(&mut self, event: &TermEvent) -> Vec<Directive> {
        match event {
            TermEvent::Key(key) if is_press(key) => self.on_key(key),
            // Resizes redraw on the next frame; everything else is noise.
            _ => Vec::new(),
        }
    }

    /// Asks the whole TUI to exit, from any screen. The runtime also calls
    /// this when the terminal's input stream closes under it.
    pub fn request_quit(&mut self) {
        self.quit = true;
    }

    fn on_key(&mut self, key: &KeyEvent) -> Vec<Directive> {
        if is_quit_combo(key) {
            self.request_quit();
            return Vec::new();
        }
        match self.screen {
            Screen::Overview => self.overview_key(key),
            Screen::Inbox => self.inbox_key(key),
            Screen::Session => self.session_key(key),
            Screen::Forwards => self.forwards_key(key),
        }
    }

    fn overview_key(&mut self, key: &KeyEvent) -> Vec<Directive> {
        match key.code {
            KeyCode::Char('q') => self.request_quit(),
            KeyCode::Char('r') => return vec![Directive::RefreshSessions],
            KeyCode::Char('i') => self.screen = Screen::Inbox,
            KeyCode::Char('f') => {
                // Entering the panel asks for fresh state: backing and
                // reachability are connection facts that a stale copy lies
                // about.
                self.screen = Screen::Forwards;
                return vec![Directive::RefreshForwards];
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.overview_selected = step_back(self.overview_selected);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.overview_selected = step_forward(self.overview_selected, self.sessions.len());
            }
            KeyCode::Enter => self.attach_selected(),
            _ => {}
        }
        Vec::new()
    }

    fn attach_selected(&mut self) {
        let Some(session) = self.sessions.values().nth(self.overview_selected) else {
            return;
        };
        self.attach_to(session.session_id.clone());
    }

    fn attach_to(&mut self, session_id: String) {
        // Re-attaching to the session this TUI was launched scoped into
        // restores its agent scope (fix a14): detaching must not silently
        // widen a subtree attach to the session root.
        let agent_id = self
            .launched_scope
            .as_ref()
            .filter(|scope| scope.session_id == session_id)
            .and_then(|scope| scope.agent_id.clone());
        self.attachment = Some(Attachment {
            session_id,
            agent_id,
        });
        self.screen = Screen::Session;
        self.scroll = 0;
        self.input.clear();
    }

    fn inbox_key(&mut self, key: &KeyEvent) -> Vec<Directive> {
        let entries = self.inbox_entries();
        match key.code {
            KeyCode::Char('q') => self.request_quit(),
            KeyCode::Esc | KeyCode::Char('o') => self.screen = Screen::Overview,
            KeyCode::Up | KeyCode::Char('k') => {
                self.inbox_selected = step_back(self.inbox_selected);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.inbox_selected = step_forward(self.inbox_selected, entries.len());
            }
            KeyCode::Char('y') => {
                return answer(selected_entry(&entries, self.inbox_selected), true);
            }
            KeyCode::Char(c) if c.is_ascii_digit() => {
                return choose(
                    selected_entry(&entries, self.inbox_selected).map(|entry| &entry.permission),
                    c,
                );
            }
            KeyCode::Char('n') => {
                return answer(selected_entry(&entries, self.inbox_selected), false);
            }
            KeyCode::Enter => {
                if let Some(entry) = selected_entry(&entries, self.inbox_selected) {
                    self.attach_to(entry.session_id.clone());
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn forwards_key(&mut self, key: &KeyEvent) -> Vec<Directive> {
        match key.code {
            KeyCode::Char('q') => self.request_quit(),
            KeyCode::Esc | KeyCode::Char('o') => self.screen = Screen::Overview,
            KeyCode::Char('r') => return vec![Directive::RefreshForwards],
            KeyCode::Up | KeyCode::Char('k') => {
                self.forwards_selected = step_back(self.forwards_selected);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.forwards_selected = step_forward(self.forwards_selected, self.forwards.len());
            }
            _ => {}
        }
        Vec::new()
    }

    fn session_key(&mut self, key: &KeyEvent) -> Vec<Directive> {
        match key.code {
            KeyCode::Esc => self.session_escape(),
            KeyCode::Enter => return self.session_submit(),
            KeyCode::Backspace => {
                self.input.pop();
            }
            KeyCode::Up => self.scroll = self.scroll_ceiling().min(self.scroll + 1),
            KeyCode::Down => self.scroll = step_back(self.scroll),
            KeyCode::Char('x') if has_control(key) => return self.interrupt_directive(),
            KeyCode::Char(c) if !has_control(key) => return self.session_char(c),
            _ => {}
        }
        Vec::new()
    }

    /// Esc clears an unfinished compose line before it detaches: throwing away
    /// typed text *and* the screen on one keypress would make the key
    /// dangerous to rest a finger on.
    fn session_escape(&mut self) {
        if self.input.is_empty() {
            self.screen = Screen::Overview;
            self.attachment = None;
        } else {
            self.input.clear();
        }
    }

    fn session_submit(&mut self) -> Vec<Directive> {
        if self.input.is_empty() {
            return Vec::new();
        }
        let Some(agent_id) = self.steer_target() else {
            self.report_error("the session has no root agent to send to yet");
            return Vec::new();
        };
        let text = std::mem::take(&mut self.input);
        vec![Directive::SendAssignment { agent_id, text }]
    }

    fn interrupt_directive(&mut self) -> Vec<Directive> {
        let Some(agent_id) = self.steer_target() else {
            self.report_error("the session has no root agent to interrupt yet");
            return Vec::new();
        };
        vec![Directive::Interrupt { agent_id }]
    }

    fn session_char(&mut self, c: char) -> Vec<Directive> {
        if self.answers_inline() {
            if c == 'y' {
                return answer_one(self.scoped_pending().first(), true);
            }
            if c == 'n' {
                return answer_one(self.scoped_pending().first(), false);
            }
            // A digit picks an option only when the front prompt is a
            // question; otherwise it is the start of a message.
            let pending = self.scoped_pending();
            if c.is_ascii_digit() && pending.first().is_some_and(PendingPermission::is_question) {
                return choose(pending.first(), c);
            }
        }
        self.input.push(c);
        Vec::new()
    }

    /// Whether `y`/`n` answer the frontmost prompt instead of composing text.
    ///
    /// Extracted as a named predicate: this is a compound condition inside a
    /// key dispatch, and inline it would be untestable as a truth table
    /// (docs/QUALITY_GATES.md on compound conditions).
    fn answers_inline(&self) -> bool {
        self.input.is_empty() && !self.scoped_pending().is_empty()
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

    /// How far back the feed can scroll: to its first line, not past it.
    fn scroll_ceiling(&self) -> usize {
        let Some(attachment) = self.attachment.as_ref() else {
            return 0;
        };
        self.sessions
            .get(&attachment.session_id)
            .map_or(0, |session| session.feed.len().saturating_sub(1))
    }
}

/// Whether the attachment's scope claims this escalation.
///
/// Session-root attachment (`agent_id: None`) sees everything in the session;
/// agent attachment sees the agent's subtree only.
fn in_scope(
    session: &SessionState,
    attachment: &Attachment,
    permission: &PendingPermission,
) -> bool {
    match attachment.agent_id.as_ref() {
        None => true,
        Some(root) => session.in_subtree(root, &permission.agent_id),
    }
}

fn selected_entry(entries: &[InboxEntry], selected: usize) -> Option<&InboxEntry> {
    entries.get(selected.min(entries.len().saturating_sub(1)))
}

fn answer(entry: Option<&InboxEntry>, allow: bool) -> Vec<Directive> {
    answer_one(entry.map(|entry| &entry.permission), allow)
}

fn answer_one(permission: Option<&PendingPermission>, allow: bool) -> Vec<Directive> {
    let Some(permission) = permission else {
        return Vec::new();
    };
    let request_id = permission.request_id.clone();
    if !allow {
        vec![Directive::Deny { request_id }]
    } else if permission.is_question() {
        // A question cannot be "allowed": only answers settle it, and the
        // Server would refuse a bare allow. The digit keys answer.
        Vec::new()
    } else {
        vec![Directive::Approve { request_id }]
    }
}

/// Answers a single-question request with option `digit` (1-based). A
/// request asking several questions — or a digit naming no option — is left
/// to `horde answer`, where every question can be answered at once.
fn choose(permission: Option<&PendingPermission>, digit: char) -> Vec<Directive> {
    let Some(permission) = permission else {
        return Vec::new();
    };
    let [question] = permission.questions.as_slice() else {
        return Vec::new();
    };
    let label = digit
        .to_digit(10)
        .and_then(|number| usize::try_from(number).ok())
        .and_then(|number| number.checked_sub(1))
        .and_then(|index| question.options.get(index));
    let Some(label) = label else {
        return Vec::new();
    };
    vec![Directive::Answer {
        request_id: permission.request_id.clone(),
        answers: std::collections::BTreeMap::from([(question.id.clone(), vec![label.clone()])]),
    }]
}

/// One step toward the top of a list, stopping there.
fn step_back(selected: usize) -> usize {
    selected.saturating_sub(1)
}

/// One step toward the bottom of a `len`-row list, stopping on the last row.
fn step_forward(selected: usize, len: usize) -> usize {
    (selected + 1).min(len.saturating_sub(1))
}

/// Key-down (or auto-repeat), as opposed to the release events some terminals
/// also deliver — acting on both would double every keystroke.
fn is_press(key: &KeyEvent) -> bool {
    matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
}

fn has_control(key: &KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::CONTROL)
}

/// Ctrl-C quits from every screen, including mid-compose: it is the one
/// escape hatch that must never be captured as text.
fn is_quit_combo(key: &KeyEvent) -> bool {
    key.code == KeyCode::Char('c') && has_control(key)
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentId, AgentSpawned, Event, EventEnvelope, PermissionRequest, PermissionRequestId,
        PermissionRequested, SessionId, SessionSummary, event::Kind,
    };
    use ratatui::crossterm::event::{
        Event as TermEvent, KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers,
    };

    use super::{App, Directive, Screen, Update};

    fn key(code: KeyCode) -> TermEvent {
        TermEvent::Key(KeyEvent::from(code))
    }

    fn ctrl(c: char) -> TermEvent {
        TermEvent::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL))
    }

    fn chars(app: &mut App, text: &str) -> Vec<Directive> {
        text.chars()
            .flat_map(|c| app.on_input(&key(KeyCode::Char(c))))
            .collect()
    }

    fn summary(session_id: &str, title: &str, root: &str) -> SessionSummary {
        SessionSummary {
            root_agent_id: Some(AgentId::new(root)),
            title: title.to_owned(),
            session_id: Some(SessionId::new(session_id)),
            ..SessionSummary::default()
        }
    }

    fn event_in(session_id: &str, sequence: u64, agent: &str, kind: Kind) -> Update {
        let event = Event {
            agent_id: Some(AgentId::new(agent)),
            kind: Some(kind),
            ..Event::default()
        };
        Update::Event(Box::new(EventEnvelope {
            event: Some(event),
            sequence,
            session_id: Some(SessionId::new(session_id)),
            ..EventEnvelope::default()
        }))
    }

    fn spawn_kind(parent: Option<&str>) -> Kind {
        Kind::AgentSpawned(AgentSpawned {
            parent_agent_id: parent.map(AgentId::new),
            spawned_by: None,
            spec: None,
            tree_path: Vec::new(),
        })
    }

    fn request_kind(request_id: &str) -> Kind {
        Kind::PermissionRequested(PermissionRequested {
            request: Some(PermissionRequest {
                request_id: Some(PermissionRequestId::new(request_id)),
                ..PermissionRequest::default()
            }),
        })
    }

    /// A question request: one question per `(id, options)` pair (W12c).
    fn question_kind(request_id: &str, questions: &[(&str, &[&str])]) -> Kind {
        Kind::PermissionRequested(PermissionRequested {
            request: Some(PermissionRequest {
                request_id: Some(PermissionRequestId::new(request_id)),
                tool_call: Some(horde_proto::v1::ProposedToolCall {
                    tool_name: "AskUserQuestion".to_owned(),
                    input: None,
                    rendered: "AskUserQuestion".to_owned(),
                    questions: questions
                        .iter()
                        .map(|(id, options)| horde_proto::v1::Question {
                            id: (*id).to_owned(),
                            question: format!("{id}?"),
                            options: options
                                .iter()
                                .map(|label| horde_proto::v1::QuestionOption {
                                    label: (*label).to_owned(),
                                    description: String::new(),
                                })
                                .collect(),
                            ..horde_proto::v1::Question::default()
                        })
                        .collect(),
                }),
                ..PermissionRequest::default()
            }),
        })
    }

    /// The inbox answers a single question by option number; `y` cannot
    /// "allow" a question; a digit naming no option, or a request asking
    /// several questions, sends nothing; `n` still denies.
    #[test]
    fn digits_answer_a_single_question_and_nothing_else() {
        let mut app = App::overview();
        app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
        app.apply(event_in(
            "s1",
            1,
            "root-1",
            question_kind("q-1", &[("colour", &["Red", "Blue"])]),
        ));
        app.on_input(&key(KeyCode::Char('i')));
        let entries = app.inbox_entries();
        assert!(entries[0].permission.is_question());
        assert_eq!(
            entries[0].permission.questions[0].options,
            vec!["Red", "Blue"]
        );

        assert_eq!(
            app.on_input(&key(KeyCode::Char('2'))),
            vec![Directive::Answer {
                request_id: "q-1".to_owned(),
                answers: std::collections::BTreeMap::from([(
                    "colour".to_owned(),
                    vec!["Blue".to_owned()]
                )]),
            }]
        );
        assert!(
            app.on_input(&key(KeyCode::Char('3'))).is_empty(),
            "no option 3"
        );
        assert!(
            app.on_input(&key(KeyCode::Char('0'))).is_empty(),
            "options count from 1"
        );
        assert!(
            app.on_input(&key(KeyCode::Char('y'))).is_empty(),
            "a question cannot be allowed, only answered"
        );
        assert_eq!(
            app.on_input(&key(KeyCode::Char('n'))),
            vec![Directive::Deny {
                request_id: "q-1".to_owned()
            }]
        );

        let mut several = App::overview();
        several.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
        several.apply(event_in(
            "s1",
            1,
            "root-1",
            question_kind("q-2", &[("a", &["x"]), ("b", &["y"])]),
        ));
        several.on_input(&key(KeyCode::Char('i')));
        assert!(
            several.on_input(&key(KeyCode::Char('1'))).is_empty(),
            "several questions are answered with `horde answer`"
        );
    }

    /// Attached, a digit answers the front prompt only when it is a question;
    /// otherwise it starts a message.
    #[test]
    fn attached_digits_answer_a_question_and_otherwise_compose() {
        let mut app = App::attached("s1", None);
        app.apply(event_in(
            "s1",
            1,
            "root-1",
            question_kind("q-1", &[("colour", &["Red"])]),
        ));
        assert_eq!(
            app.on_input(&key(KeyCode::Char('1'))),
            vec![Directive::Answer {
                request_id: "q-1".to_owned(),
                answers: std::collections::BTreeMap::from([(
                    "colour".to_owned(),
                    vec!["Red".to_owned()]
                )]),
            }]
        );
        assert!(app.input().is_empty());

        let mut plain = App::attached("s2", None);
        plain.apply(event_in("s2", 1, "root-2", request_kind("req-1")));
        assert!(plain.on_input(&key(KeyCode::Char('1'))).is_empty());
        assert_eq!(plain.input(), "1", "a digit is a letter when nothing asks");
    }

    /// An app watching two sessions, the second holding one escalation.
    fn two_sessions() -> App {
        let mut app = App::overview();
        app.apply(Update::Sessions(vec![
            summary("s1", "ci-fix", "root-1"),
            summary("s2", "docs", "root-2"),
        ]));
        app.apply(event_in("s2", 1, "root-2", request_kind("req-1")));
        app
    }

    #[test]
    fn navigation_moves_between_screens_without_crossing_the_wire() {
        let mut app = App::overview();
        assert_eq!(app.screen(), Screen::Overview);

        assert!(app.on_input(&key(KeyCode::Char('i'))).is_empty());
        assert_eq!(app.screen(), Screen::Inbox);

        assert!(app.on_input(&key(KeyCode::Char('o'))).is_empty());
        assert_eq!(app.screen(), Screen::Overview);

        assert!(app.on_input(&key(KeyCode::Char('q'))).is_empty());
        assert!(app.should_quit());
    }

    /// One forward panel row per status, for the key tests.
    fn forward_rows(count: usize) -> Vec<horde_proto::v1::ForwardStatus> {
        (0..count)
            .map(|ordinal| horde_proto::v1::ForwardStatus {
                intent: Some(horde_proto::v1::ForwardIntent {
                    forward_id: Some(horde_proto::v1::ForwardId::new(format!("fwd-{ordinal}"))),
                    ..horde_proto::v1::ForwardIntent::default()
                }),
                worker_reachable: true,
                backed: false,
            })
            .collect()
    }

    /// The forward panel's whole key table (ADR-0006): entry refreshes,
    /// `r` re-asks, the selection moves and clamps, `o`/Esc leave, and `q`
    /// quits — each arm asserted, because a dropped arm is a dead key that
    /// no snapshot notices.
    #[test]
    fn the_forward_panel_keys_refresh_move_leave_and_quit() {
        let mut app = App::overview();
        assert_eq!(
            app.on_input(&key(KeyCode::Char('f'))),
            vec![Directive::RefreshForwards],
            "entering the panel asks for fresh backing state"
        );
        assert_eq!(app.screen(), Screen::Forwards);

        app.apply(Update::Forwards(forward_rows(2)));
        assert_eq!(app.forwards_selected(), 0);
        assert!(app.on_input(&key(KeyCode::Down)).is_empty());
        assert_eq!(app.forwards_selected(), 1);
        assert!(app.on_input(&key(KeyCode::Up)).is_empty());
        assert_eq!(app.forwards_selected(), 0);

        assert_eq!(
            app.on_input(&key(KeyCode::Char('r'))),
            vec![Directive::RefreshForwards],
            "r re-asks without leaving"
        );
        assert_eq!(app.screen(), Screen::Forwards);

        assert!(app.on_input(&key(KeyCode::Char('o'))).is_empty());
        assert_eq!(app.screen(), Screen::Overview);

        app.on_input(&key(KeyCode::Char('f')));
        assert!(app.on_input(&key(KeyCode::Esc)).is_empty());
        assert_eq!(app.screen(), Screen::Overview);

        app.on_input(&key(KeyCode::Char('f')));
        assert!(app.on_input(&key(KeyCode::Char('q'))).is_empty());
        assert!(app.should_quit());
    }

    /// A shrinking answer clamps the selection instead of leaving it past
    /// the end — the row under the cursor may be the one that closed.
    #[test]
    fn a_shrinking_forward_list_clamps_the_selection() {
        let mut app = App::overview();
        app.on_input(&key(KeyCode::Char('f')));
        app.apply(Update::Forwards(forward_rows(3)));
        app.on_input(&key(KeyCode::Down));
        app.on_input(&key(KeyCode::Down));
        assert_eq!(app.forwards_selected(), 2);

        app.apply(Update::Forwards(forward_rows(1)));
        assert_eq!(app.forwards_selected(), 0, "clamped to the last real row");
    }

    #[test]
    fn ctrl_c_quits_from_every_screen_even_mid_compose() {
        for setup in [
            App::overview,
            || {
                let mut app = App::overview();
                app.on_input(&key(KeyCode::Char('i')));
                app
            },
            || {
                let mut app = App::attached("s1", None);
                app.on_input(&key(KeyCode::Char('h')));
                app
            },
        ] {
            let mut app = setup();
            app.on_input(&ctrl('c'));
            assert!(app.should_quit());
        }
    }

    /// Acting on key releases too would double every keystroke on terminals
    /// that report them.
    #[test]
    fn a_key_release_is_ignored() {
        let mut app = App::overview();
        let release = TermEvent::Key(KeyEvent {
            code: KeyCode::Char('q'),
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: KeyEventState::NONE,
        });
        assert!(app.on_input(&release).is_empty());
        assert!(!app.should_quit(), "a release must not act");
    }

    #[test]
    fn refresh_is_asked_for_rather_than_performed() {
        let mut app = App::overview();
        assert_eq!(
            app.on_input(&key(KeyCode::Char('r'))),
            vec![Directive::RefreshSessions]
        );
    }

    #[test]
    fn overview_selection_clamps_at_both_ends() {
        let mut app = two_sessions();
        assert_eq!(app.overview_selected(), 0);
        app.on_input(&key(KeyCode::Up));
        assert_eq!(app.overview_selected(), 0, "the top is the top");
        app.on_input(&key(KeyCode::Down));
        assert_eq!(app.overview_selected(), 1);
        app.on_input(&key(KeyCode::Char('j')));
        assert_eq!(app.overview_selected(), 1, "the bottom is the bottom");
        app.on_input(&key(KeyCode::Char('k')));
        assert_eq!(app.overview_selected(), 0);
    }

    #[test]
    fn enter_attaches_to_the_selected_session_at_its_root() {
        let mut app = two_sessions();
        app.on_input(&key(KeyCode::Down));
        app.on_input(&key(KeyCode::Enter));
        assert_eq!(app.screen(), Screen::Session);
        let attachment = app.attachment().expect("attached");
        assert_eq!(attachment.session_id, "s2");
        assert_eq!(attachment.agent_id, None, "overview attach is session-root");
    }

    #[test]
    fn enter_on_an_empty_overview_stays_put() {
        let mut app = App::overview();
        app.on_input(&key(KeyCode::Enter));
        assert_eq!(app.screen(), Screen::Overview);
        assert!(app.attachment().is_none());
    }

    #[test]
    fn a_summary_without_an_id_is_not_folded_in() {
        let mut app = App::overview();
        app.apply(Update::Sessions(vec![SessionSummary::default()]));
        assert!(app.sessions_in_order().is_empty());
    }

    #[test]
    fn caught_up_marks_the_session_and_never_rewinds_its_head() {
        let mut app = App::attached("s1", None);
        app.apply(event_in("s1", 9, "root-1", spawn_kind(None)));
        app.apply(Update::CaughtUp {
            session_id: "s1".to_owned(),
            head: 4,
        });
        let session = app.session("s1").expect("attached session exists");
        assert!(session.caught_up);
        assert_eq!(session.head, 9);
    }

    #[test]
    fn typing_composes_and_enter_sends_to_the_session_root() {
        let mut app = App::attached("s1", None);
        app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));

        assert!(chars(&mut app, "fix it").is_empty());
        assert_eq!(app.input(), "fix it");

        app.on_input(&key(KeyCode::Backspace));
        assert_eq!(app.input(), "fix i");

        assert_eq!(
            app.on_input(&key(KeyCode::Enter)),
            vec![Directive::SendAssignment {
                agent_id: "root-1".to_owned(),
                text: "fix i".to_owned(),
            }]
        );
        assert_eq!(app.input(), "", "a sent line leaves the compose box empty");
    }

    #[test]
    fn enter_with_nothing_typed_sends_nothing() {
        let mut app = App::attached("s1", None);
        app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
        assert!(app.on_input(&key(KeyCode::Enter)).is_empty());
    }

    #[test]
    fn a_send_with_no_known_root_is_refused_with_a_status_line() {
        let mut app = App::attached("s1", None);
        chars(&mut app, "hello");
        assert!(app.on_input(&key(KeyCode::Enter)).is_empty());
        assert!(
            app.status().contains("no root agent"),
            "the refusal must say why: {:?}",
            app.status()
        );
    }

    /// Detaching from an agent-scoped attach and re-attaching to the same
    /// session restores the agent scope (fix a14) — before this, the
    /// re-attach silently widened the view to the session root. A different
    /// session attaches at its root, as ever.
    #[test]
    fn re_attaching_restores_the_launched_agent_scope() {
        let mut app = App::attached("s1", Some("child-1".to_owned()));
        app.apply(Update::Sessions(vec![
            summary("s1", "ci-fix", "root-1"),
            summary("s2", "docs", "root-2"),
        ]));

        // Detach: back at the overview, no attachment.
        app.on_input(&key(KeyCode::Esc));
        assert_eq!(app.attachment(), None, "escape detaches");

        // Re-attach to s1 (first row): the agent scope comes back.
        app.on_input(&key(KeyCode::Enter));
        let attachment = app.attachment().expect("re-attached");
        assert_eq!(attachment.session_id, "s1");
        assert_eq!(
            attachment.agent_id.as_deref(),
            Some("child-1"),
            "the launched scope survives detach/re-attach"
        );

        // A different session is not scoped by s1's launch.
        app.on_input(&key(KeyCode::Esc));
        app.on_input(&key(KeyCode::Down));
        app.on_input(&key(KeyCode::Enter));
        let attachment = app.attachment().expect("attached to s2");
        assert_eq!(attachment.session_id, "s2");
        assert_eq!(
            attachment.agent_id, None,
            "another session attaches at its root"
        );
    }

    /// The staleness boundary, exactly: silence of the ceiling itself is the
    /// third heartbeat on the buzzer, one instant more is a dead stream, and
    /// a stream that never spoke is connecting rather than stale.
    #[test]
    fn staleness_begins_strictly_past_the_ceiling() {
        let last = std::time::Instant::now();
        assert!(!super::is_stale(None, last + super::STALE_AFTER * 2));
        assert!(!super::is_stale(Some(last), last));
        assert!(
            !super::is_stale(Some(last), last + super::STALE_AFTER),
            "exactly the ceiling is on time, not stale"
        );
        assert!(super::is_stale(
            Some(last),
            last + super::STALE_AFTER + std::time::Duration::from_millis(1)
        ));
    }

    /// The staleness clock (fix a9): quiet past the ceiling turns the badge
    /// on, any stream activity — a heartbeat included — turns it off, and a
    /// stream that has never spoken is connecting, not stale.
    #[test]
    fn the_stream_goes_stale_on_silence_and_recovers_on_a_heartbeat() {
        let mut app = App::overview();
        let long_past = super::STALE_AFTER + std::time::Duration::from_secs(1);

        // Never spoke: not stale, however late the clock runs.
        app.apply(Update::Tick {
            now: std::time::Instant::now() + long_past,
        });
        assert!(!app.stream_stale(), "no stream yet is connecting, not dead");

        // A heartbeat is proof of life...
        app.apply(Update::Heartbeat);
        app.apply(Update::Tick {
            now: std::time::Instant::now(),
        });
        assert!(!app.stream_stale(), "fresh activity is not stale");

        // ...and silence past the ceiling is not.
        app.apply(Update::Tick {
            now: std::time::Instant::now() + long_past,
        });
        assert!(app.stream_stale(), "silence past the ceiling is stale");

        // The next heartbeat clears it without waiting for a tick.
        app.apply(Update::Heartbeat);
        assert!(!app.stream_stale(), "life observed clears the badge");
    }

    #[test]
    fn attachment_to_an_agent_steers_that_agent_not_the_root() {
        let mut app = App::attached("s1", Some("child-1".to_owned()));
        app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
        chars(&mut app, "go");
        assert_eq!(
            app.on_input(&key(KeyCode::Enter)),
            vec![Directive::SendAssignment {
                agent_id: "child-1".to_owned(),
                text: "go".to_owned(),
            }]
        );
        assert_eq!(
            app.on_input(&ctrl('x')),
            vec![Directive::Interrupt {
                agent_id: "child-1".to_owned(),
            }]
        );
    }

    #[test]
    fn ctrl_x_interrupts_without_disturbing_the_compose_line() {
        let mut app = App::attached("s1", None);
        app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
        chars(&mut app, "dra");
        assert_eq!(
            app.on_input(&ctrl('x')),
            vec![Directive::Interrupt {
                agent_id: "root-1".to_owned(),
            }]
        );
        assert_eq!(app.input(), "dra", "an interrupt is not a send");
    }

    #[test]
    fn escape_clears_the_compose_line_before_it_detaches() {
        let mut app = App::attached("s1", None);
        chars(&mut app, "oops");
        app.on_input(&key(KeyCode::Esc));
        assert_eq!(app.input(), "");
        assert_eq!(app.screen(), Screen::Session, "typed text costs one escape");

        app.on_input(&key(KeyCode::Esc));
        assert_eq!(app.screen(), Screen::Overview);
        assert!(app.attachment().is_none());
    }

    #[test]
    fn y_and_n_answer_the_frontmost_prompt_only_when_composing_nothing() {
        let mut app = App::attached("s2", None);
        app.apply(event_in("s2", 1, "root-2", request_kind("req-1")));
        app.apply(event_in("s2", 2, "root-2", request_kind("req-2")));

        assert_eq!(
            app.on_input(&key(KeyCode::Char('y'))),
            vec![Directive::Approve {
                request_id: "req-1".to_owned(),
            }],
            "the frontmost prompt is the oldest"
        );
        assert_eq!(
            app.on_input(&key(KeyCode::Char('n'))),
            vec![Directive::Deny {
                request_id: "req-1".to_owned(),
            }],
            "pending stays until the decision event arrives"
        );

        chars(&mut app, "sa");
        assert!(
            app.on_input(&key(KeyCode::Char('y'))).is_empty(),
            "mid-compose, y is a letter"
        );
        assert_eq!(app.input(), "say");
    }

    #[test]
    fn with_nothing_pending_y_and_n_are_ordinary_letters() {
        let mut app = App::attached("s1", None);
        app.on_input(&key(KeyCode::Char('y')));
        app.on_input(&key(KeyCode::Char('n')));
        assert_eq!(app.input(), "yn");
    }

    #[test]
    fn scroll_walks_back_through_the_feed_and_clamps() {
        let mut app = App::attached("s1", None);
        for sequence in 1..=3 {
            app.apply(event_in("s1", sequence, "root-1", spawn_kind(None)));
        }
        assert_eq!(app.scroll(), 0);
        app.on_input(&key(KeyCode::Down));
        assert_eq!(app.scroll(), 0, "the tail is the floor");
        for _ in 0..5 {
            app.on_input(&key(KeyCode::Up));
        }
        assert_eq!(app.scroll(), 2, "the first line is the ceiling");
        app.on_input(&key(KeyCode::Down));
        assert_eq!(app.scroll(), 1);
    }

    #[test]
    fn the_inbox_lists_every_session_and_answers_the_selected_row() {
        let mut app = two_sessions();
        app.apply(event_in("s1", 2, "root-1", request_kind("req-0")));
        app.on_input(&key(KeyCode::Char('i')));

        let entries = app.inbox_entries();
        assert_eq!(
            entries
                .iter()
                .map(|entry| (
                    entry.session_id.as_str(),
                    entry.permission.request_id.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![("s1", "req-0"), ("s2", "req-1")],
            "session order then arrival order"
        );
        assert_eq!(entries[0].title, "ci-fix");

        assert_eq!(
            app.on_input(&key(KeyCode::Char('y'))),
            vec![Directive::Approve {
                request_id: "req-0".to_owned(),
            }]
        );
        app.on_input(&key(KeyCode::Down));
        assert_eq!(
            app.on_input(&key(KeyCode::Char('n'))),
            vec![Directive::Deny {
                request_id: "req-1".to_owned(),
            }]
        );
    }

    /// Three rows, because two cannot tell "step forward" from "always 1".
    #[test]
    fn inbox_selection_moves_both_ways_and_clamps_at_both_ends() {
        let mut app = two_sessions();
        app.apply(event_in("s1", 2, "root-1", request_kind("req-a")));
        app.apply(event_in("s1", 3, "root-1", request_kind("req-b")));
        app.on_input(&key(KeyCode::Char('i')));

        assert_eq!(app.inbox_selected(), 0, "selection starts at the oldest");
        app.on_input(&key(KeyCode::Down));
        app.on_input(&key(KeyCode::Down));
        assert_eq!(app.inbox_selected(), 2);
        app.on_input(&key(KeyCode::Down));
        assert_eq!(app.inbox_selected(), 2, "the last row is the floor");
        app.on_input(&key(KeyCode::Up));
        assert_eq!(app.inbox_selected(), 1);
        app.on_input(&key(KeyCode::Char('k')));
        assert_eq!(app.inbox_selected(), 0);
        app.on_input(&key(KeyCode::Char('k')));
        assert_eq!(app.inbox_selected(), 0, "the top is the top");
    }

    #[test]
    fn q_quits_from_the_inbox_too() {
        let mut app = App::overview();
        app.on_input(&key(KeyCode::Char('i')));
        app.on_input(&key(KeyCode::Char('q')));
        assert!(app.should_quit());
    }

    /// A control chord that is not one of the bound ones must do nothing at
    /// all — in particular it must not shed its modifier and type its letter.
    #[test]
    fn an_unbound_control_chord_is_not_text() {
        let mut app = App::attached("s1", None);
        assert!(app.on_input(&ctrl('a')).is_empty());
        assert_eq!(app.input(), "", "ctrl-a must not type an a");
    }

    #[test]
    fn the_inbox_attaches_to_the_selected_escalations_session() {
        let mut app = two_sessions();
        app.on_input(&key(KeyCode::Char('i')));
        app.on_input(&key(KeyCode::Enter));
        assert_eq!(app.screen(), Screen::Session);
        assert_eq!(
            app.attachment()
                .map(|attachment| attachment.session_id.as_str()),
            Some("s2"),
            "the only escalation lives in s2"
        );
    }

    #[test]
    fn an_empty_inbox_answers_nothing() {
        let mut app = App::overview();
        app.on_input(&key(KeyCode::Char('i')));
        assert!(app.on_input(&key(KeyCode::Char('y'))).is_empty());
        assert!(app.on_input(&key(KeyCode::Enter)).is_empty());
    }

    #[test]
    fn escalations_scope_to_the_attached_subtree() {
        let mut app = App::attached("s1", Some("child-1".to_owned()));
        app.apply(event_in("s1", 1, "root-1", spawn_kind(None)));
        app.apply(event_in("s1", 2, "child-1", spawn_kind(Some("root-1"))));
        app.apply(event_in("s1", 3, "child-2", spawn_kind(Some("root-1"))));
        app.apply(event_in("s1", 4, "child-1", request_kind("req-mine")));
        app.apply(event_in("s1", 5, "child-2", request_kind("req-elsewhere")));

        assert_eq!(
            app.scoped_pending()
                .iter()
                .map(|permission| permission.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["req-mine"],
            "a sibling's prompt must not bubble into this scope"
        );
        assert_eq!(app.waiting_elsewhere(), 1);
        assert_eq!(app.waiting_total(), 2);
    }

    #[test]
    fn session_root_attachment_sees_the_whole_session() {
        let mut app = App::attached("s1", None);
        app.apply(event_in("s1", 1, "root-1", spawn_kind(None)));
        app.apply(event_in("s1", 2, "child-1", spawn_kind(Some("root-1"))));
        app.apply(event_in("s1", 3, "child-1", request_kind("req-1")));

        assert_eq!(app.scoped_pending().len(), 1);
        assert_eq!(app.waiting_elsewhere(), 0);
    }

    #[test]
    fn a_reported_error_lands_in_the_status_line() {
        let mut app = App::overview();
        app.report_error("the server said no");
        assert_eq!(app.status(), "the server said no");
    }

    /// An open stream has not ended; an ended one keeps its reason, and a
    /// later heartbeat-shaped surprise does not reopen it.
    #[test]
    fn a_stream_ends_once_and_stays_ended() {
        let mut app = App::overview();
        assert_eq!(app.stream_ended(), None);
        app.apply(Update::StreamEnded {
            reason: "server unavailable".to_owned(),
        });
        app.apply(Update::Heartbeat);
        assert_eq!(app.stream_ended(), Some("server unavailable"));
    }

    /// Sessions answer to their titles; a known but untitled session, and
    /// one this App has never heard of, answer to a short id.
    #[test]
    fn a_session_is_named_by_title_or_short_id() {
        let mut app = App::attached("0123456789-untitled", None);
        app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
        assert_eq!(app.session_name("s1"), "ci-fix");
        assert_eq!(app.session_name("0123456789-untitled"), "01234567");
        assert_eq!(app.session_name("abcdefghij-unknown"), "abcdefgh");
    }
}
