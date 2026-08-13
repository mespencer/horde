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
use ratatui::crossterm::event::{Event as TermEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

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
}

/// Something the runtime should ask the server for on the App's behalf.
///
/// Screen navigation never appears here — it is the App's own state. Only the
/// verbs that cross the wire do, which keeps the interactive loop a dispatch
/// table over [`crate::App::on_input`] with no decisions of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Directive {
    RefreshSessions,
    Approve { request_id: String },
    Deny { request_id: String },
    SendAssignment { agent_id: String, text: String },
    Interrupt { agent_id: String },
}

/// Which screen is showing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Overview,
    Session,
    Inbox,
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
    /// The session view's compose line.
    input: String,
    /// How many feed lines back from the tail the session view is scrolled.
    scroll: usize,
    /// The last refusal or notice, shown until the next one replaces it.
    status: String,
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
            input: String::new(),
            scroll: 0,
            status: String::new(),
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
        app.attachment = Some(Attachment {
            session_id,
            agent_id,
        });
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
                session.pending_in_order().into_iter().map(|permission| InboxEntry {
                    session_id: session.session_id.clone(),
                    title: session.title.clone(),
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
            }
            Update::CaughtUp { session_id, head } => {
                let session = self.session_entry(session_id);
                session.caught_up = true;
                session.head = session.head.max(head);
            }
        }
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
        }
    }

    fn overview_key(&mut self, key: &KeyEvent) -> Vec<Directive> {
        match key.code {
            KeyCode::Char('q') => self.request_quit(),
            KeyCode::Char('r') => return vec![Directive::RefreshSessions],
            KeyCode::Char('i') => self.screen = Screen::Inbox,
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
        self.attachment = Some(Attachment {
            session_id,
            agent_id: None,
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
            KeyCode::Char('y') => return answer(selected_entry(&entries, self.inbox_selected), true),
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
fn in_scope(session: &SessionState, attachment: &Attachment, permission: &PendingPermission) -> bool {
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
    if allow {
        vec![Directive::Approve { request_id }]
    } else {
        vec![Directive::Deny { request_id }]
    }
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
