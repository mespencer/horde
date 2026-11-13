//! Input: terminal events become intents (via the tables in `keys`), and
//! intents become state changes and directives.

use ratatui::crossterm::event::{Event as TermEvent, KeyEvent, KeyEventKind};

use super::{App, Confirm, Directive, Screen, TreeRow};
use crate::keys::{self, Edit, Intent};
use crate::prompt::{Decision, PromptCursor, PromptKey};
use crate::slash::{self, Slash};
use crate::state::PendingPermission;

/// Rows one page of scrolling moves.
const PAGE: usize = 10;

impl App {
    /// Handles one terminal event, returning what the runtime should do.
    pub fn on_input(&mut self, event: &TermEvent) -> Vec<Directive> {
        match event {
            TermEvent::Key(key) if is_press(key) => self.on_key(key),
            TermEvent::Paste(text) if self.screen == Screen::Session => {
                self.compose.insert_str(text);
                Vec::new()
            }
            TermEvent::Resize(width, height) => {
                self.viewport = (*width, *height);
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    /// Asks the whole TUI to exit, from any screen.
    pub fn request_quit(&mut self) {
        self.quit = true;
    }

    fn on_key(&mut self, key: &KeyEvent) -> Vec<Directive> {
        if let Some(confirm) = self.confirm.take() {
            return confirmed(confirm, keys::typed(key) == Some('y'));
        }
        let intent = match self.screen {
            Screen::Session if self.prompt_focused() => {
                keys::resolve(&[keys::GLOBAL, keys::PROMPT], key, keys::as_prompt)
            }
            Screen::Session => keys::resolve(&[keys::GLOBAL, keys::COMPOSE], key, keys::as_text),
            Screen::Inbox if self.inbox_card => {
                keys::resolve(&[keys::GLOBAL, keys::PROMPT], key, keys::as_prompt)
            }
            Screen::Inbox => {
                keys::resolve(&[keys::GLOBAL, keys::INBOX, keys::LIST], key, keys::ignored)
            }
            _ => keys::resolve(&[keys::GLOBAL, keys::LIST], key, keys::ignored),
        };
        intent.map_or_else(Vec::new, |intent| self.act(intent))
    }

    /// Carries out one intent.
    pub fn act(&mut self, intent: Intent) -> Vec<Directive> {
        match intent {
            Intent::Cancel => self.cancel(),
            Intent::Quit => self.request_quit(),
            Intent::Goto(screen) => return self.goto(screen),
            Intent::ToggleDetails => self.details = !self.details,
            Intent::ScrollUp => self.scroll = (self.scroll + PAGE).min(self.scroll_ceiling()),
            Intent::ScrollDown => self.scroll = self.scroll.saturating_sub(PAGE),
            Intent::ScrollEnd => self.scroll = 0,
            Intent::Interrupt => return self.interrupt(),
            Intent::Escape => return self.escape(),
            Intent::Prompt(key) => return self.prompt_key(key),
            other => return self.act_compose_or_list(other),
        }
        Vec::new()
    }

    fn act_compose_or_list(&mut self, intent: Intent) -> Vec<Directive> {
        match intent {
            Intent::Submit => return self.submit(),
            Intent::SendNow => return self.send_now(),
            Intent::Newline => self.compose.newline(),
            Intent::Tab => self.tab(),
            Intent::Edit(edit) => self.edit(edit),
            Intent::Type(c) => return self.type_char(c),
            Intent::Up => self.step(-1),
            Intent::Down => self.step(1),
            Intent::Activate => return self.activate(),
            Intent::Attach => self.attach_inbox_entry(),
            Intent::Refresh => return self.refresh(),
            Intent::NewSession => return vec![Directive::NewSession { title: None }],
            Intent::Archive => self.ask_archive_selected(),
            _ => {}
        }
        Vec::new()
    }

    /// Ctrl-C clears the compose box first; with nothing to clear, it quits.
    fn cancel(&mut self) {
        if self.screen == Screen::Session && !self.compose.is_empty() {
            self.compose.clear();
        } else {
            self.request_quit();
        }
    }

    fn goto(&mut self, screen: Screen) -> Vec<Directive> {
        if screen == Screen::Session && self.attachment.is_none() {
            self.report_error("not attached — pick a session or agent and press enter");
            return Vec::new();
        }
        if self.screen != screen {
            self.previous = self.screen;
        }
        self.screen = screen;
        self.inbox_card = false;
        self.entering(screen)
    }

    /// What entering a screen asks the server for.
    fn entering(&mut self, screen: Screen) -> Vec<Directive> {
        match screen {
            Screen::Overview | Screen::Inbox => {
                self.replay_everything();
                vec![Directive::RefreshSessions, Directive::RefreshWorkers]
            }
            Screen::Workspaces => vec![Directive::RefreshWorkspaces, Directive::RefreshWorkers],
            Screen::Usage => vec![self.usage_directive()],
            Screen::Forwards => vec![Directive::RefreshForwards],
            _ => Vec::new(),
        }
    }

    fn usage_directive(&self) -> Directive {
        Directive::RefreshUsage {
            session_id: self.attachment.as_ref().map(|a| a.session_id.clone()),
        }
    }

    fn refresh(&mut self) -> Vec<Directive> {
        let screen = self.screen;
        self.entering(screen)
    }

    /// Esc: close the card or menu, interrupt a working agent, or go back.
    fn escape(&mut self) -> Vec<Directive> {
        match self.screen {
            Screen::Session => self.session_escape(),
            Screen::Inbox if self.inbox_card => {
                self.inbox_card = false;
                Vec::new()
            }
            _ => {
                let back = self.back_target();
                self.screen = back;
                Vec::new()
            }
        }
    }

    /// Where Esc leaves a list screen for: the screen it came from, unless
    /// that is the session screen with nothing attached.
    fn back_target(&self) -> Screen {
        match (self.previous, self.attachment.is_some()) {
            (Screen::Session, false) => Screen::Overview,
            (previous, _) if previous != self.screen => previous,
            _ if self.attachment.is_some() => Screen::Session,
            _ => Screen::Overview,
        }
    }

    fn session_escape(&mut self) -> Vec<Directive> {
        if self.prompt_focused() {
            self.compose_focus_for = self.front_prompt().map(|front| front.request_id);
            return Vec::new();
        }
        if slash::is_command(&self.compose.text()) {
            self.compose.clear();
            return Vec::new();
        }
        if self.agent_working() {
            return self.interrupt();
        }
        self.compose.clear();
        Vec::new()
    }

    fn interrupt(&mut self) -> Vec<Directive> {
        let Some(agent_id) = self.steer_target() else {
            self.report_error("the session has no root agent to interrupt yet");
            return Vec::new();
        };
        vec![Directive::Interrupt { agent_id }]
    }

    /// Tab: complete the highlighted command, or move focus to the prompt.
    fn tab(&mut self) {
        let text = self.compose.text();
        let completions = slash::completions(&text);
        if let Some(command) = completions.get(self.menu.min(completions.len().saturating_sub(1))) {
            let space = if command.args.is_empty() { "" } else { " " };
            self.compose.set_text(&format!("/{}{space}", command.name));
            self.menu = 0;
            return;
        }
        if self.prompt_focused() {
            self.compose_focus_for = self.front_prompt().map(|front| front.request_id);
        } else {
            self.compose_focus_for = None;
        }
    }

    fn edit(&mut self, edit: Edit) {
        let menu_open = !slash::completions(&self.compose.text()).is_empty();
        match edit {
            Edit::Up if menu_open => self.menu = self.menu.saturating_sub(1),
            Edit::Down if menu_open => self.menu += 1,
            other => self.edit_text(other),
        }
    }

    fn edit_text(&mut self, edit: Edit) {
        match edit {
            Edit::Backspace => self.compose.backspace(),
            Edit::Delete => self.compose.delete(),
            Edit::Left => self.compose.left(),
            Edit::Right => self.compose.right(),
            Edit::Up => self.compose.up(),
            Edit::Down => self.compose.down(),
            Edit::Home => self.compose.home(),
            Edit::End => self.compose.end(),
            Edit::DeleteWord => self.compose.delete_word(),
            Edit::DeleteToLineStart => self.compose.delete_to_line_start(),
        }
        self.menu = 0;
    }

    /// `?` on an empty compose box opens help, the way Claude Code's does;
    /// anything else is text.
    fn type_char(&mut self, c: char) -> Vec<Directive> {
        if c == '?' && self.compose.is_empty() {
            return self.goto(Screen::Help);
        }
        self.compose.insert_char(c);
        self.menu = 0;
        Vec::new()
    }

    /// Enter in the compose box: run a command, or send a message.
    fn submit(&mut self) -> Vec<Directive> {
        let text = self.compose.text();
        if slash::is_command(&text) {
            return self.submit_command(&text);
        }
        if text.trim().is_empty() {
            return Vec::new();
        }
        self.send(|agent_id, text| Directive::SendAssignment { agent_id, text })
    }

    /// Ctrl-S: send with interrupt delivery.
    fn send_now(&mut self) -> Vec<Directive> {
        if self.compose.text().trim().is_empty() || slash::is_command(&self.compose.text()) {
            return Vec::new();
        }
        self.send(|agent_id, text| Directive::SendNow { agent_id, text })
    }

    fn send(&mut self, directive: fn(String, String) -> Directive) -> Vec<Directive> {
        let Some(agent_id) = self.steer_target() else {
            self.report_error("the session has no root agent to send to yet");
            return Vec::new();
        };
        self.scroll = 0;
        vec![directive(agent_id, self.compose.take())]
    }

    /// A command line: a partly typed command word runs the highlighted
    /// completion; refusals land in the status line with the text kept.
    fn submit_command(&mut self, text: &str) -> Vec<Directive> {
        let completions = slash::completions(text);
        let line = match completions.get(self.menu.min(completions.len().saturating_sub(1))) {
            Some(command)
                if command.args.starts_with('<') && text != format!("/{}", command.name) =>
            {
                // A partly typed command that needs an argument completes
                // and waits for it rather than refusing.
                self.compose.set_text(&format!("/{} ", command.name));
                self.menu = 0;
                return Vec::new();
            }
            Some(command) => format!("/{}", command.name),
            None => text.to_owned(),
        };
        self.menu = 0;
        match slash::parse(&line) {
            Ok(command) => {
                self.compose.take();
                self.run(command)
            }
            Err(refusal) => {
                self.report_error(refusal);
                Vec::new()
            }
        }
    }

    /// Carries out a parsed slash command.
    pub fn run(&mut self, command: Slash) -> Vec<Directive> {
        match command {
            Slash::Attach(target) => return self.attach_named(target),
            Slash::New(title) => return vec![Directive::NewSession { title }],
            Slash::Interrupt => return self.interrupt(),
            Slash::Now(text) => {
                return self
                    .message_with(text, |agent_id, text| Directive::SendNow { agent_id, text });
            }
            Slash::Note(text) => {
                return self.message_with(text, |agent_id, text| Directive::SendNote {
                    agent_id,
                    text,
                });
            }
            Slash::Details => self.details = !self.details,
            Slash::Archive(target) => self.ask_archive(target),
            Slash::Quit => self.request_quit(),
            other => return self.goto(screen_for(&other)),
        }
        Vec::new()
    }

    fn message_with(
        &mut self,
        text: String,
        directive: fn(String, String) -> Directive,
    ) -> Vec<Directive> {
        match self.steer_target() {
            Some(agent_id) => vec![directive(agent_id, text)],
            None => {
                self.report_error("the session has no root agent to send to yet");
                Vec::new()
            }
        }
    }

    /// `/attach name`: an agent of the attached session by name, else a
    /// session by title, slug, id or id prefix. Ambiguity is refused.
    fn attach_named(&mut self, target: Option<String>) -> Vec<Directive> {
        let Some(target) = target else {
            return self.goto(Screen::Overview);
        };
        if let Some(agent_id) = self.agent_named(&target) {
            let session_id = self
                .attachment
                .as_ref()
                .map(|a| a.session_id.clone())
                .unwrap_or_default();
            self.attach_agent(session_id, agent_id);
            return Vec::new();
        }
        let matches: Vec<String> = self
            .sessions
            .values()
            .filter(|session| session_answers_to(session, &target))
            .map(|session| session.session_id.clone())
            .collect();
        match matches.as_slice() {
            [only] => self.attach_to(only.clone(), None),
            [] => self.report_error(format!("no session or agent is named {target}")),
            _ => self.report_error(format!("{target} is ambiguous: {}", matches.join(", "))),
        }
        Vec::new()
    }

    /// An agent of the attached session answering to `name` (its display
    /// name or its id).
    fn agent_named(&self, name: &str) -> Option<String> {
        let session = self.attached_session()?;
        session
            .agents
            .keys()
            .find(|id| id.as_str() == name || session.agent_name(id) == name)
            .cloned()
    }

    /// Attaches to an agent: the root attaches at the session root (which
    /// sees the whole tree), any other agent scopes to its subtree.
    fn attach_agent(&mut self, session_id: String, agent_id: String) {
        let is_root = self
            .sessions
            .get(&session_id)
            .is_some_and(|session| session.root_agent_id.as_deref() == Some(&agent_id));
        self.attach_to(session_id, (!is_root).then_some(agent_id));
    }

    fn ask_archive(&mut self, target: Option<String>) {
        let session_id = match target {
            Some(name) => self
                .sessions
                .values()
                .find(|session| session_answers_to(session, &name))
                .map(|session| session.session_id.clone()),
            None => self.attachment.as_ref().map(|a| a.session_id.clone()),
        };
        match session_id {
            Some(session_id) => self.confirm = Some(Confirm::Archive { session_id }),
            None => self.report_error("archive which session? /archive <name>"),
        }
    }

    fn ask_archive_selected(&mut self) {
        if self.screen != Screen::Overview {
            return;
        }
        let session_id = self.selected_tree_row().map(|row| match row {
            TreeRow::Session { session_id } | TreeRow::Agent { session_id, .. } => session_id,
        });
        if let Some(session_id) = session_id {
            self.confirm = Some(Confirm::Archive { session_id });
        }
    }

    // -- Lists ----------------------------------------------------------------

    fn list_len(&self) -> usize {
        match self.screen {
            Screen::Overview => self.tree_rows().len(),
            Screen::Inbox => self.inbox_entries().len(),
            Screen::Forwards => self.forwards.len(),
            Screen::Workspaces => self.workspaces.as_ref().map_or(0, Vec::len),
            _ => 0,
        }
    }

    fn step(&mut self, by: isize) {
        let len = self.list_len();
        let screen = self.screen;
        let next = self
            .selected(screen)
            .saturating_add_signed(by)
            .min(len.saturating_sub(1));
        self.selected.insert(screen, next);
    }

    fn selected_tree_row(&self) -> Option<TreeRow> {
        let rows = self.tree_rows();
        rows.get(self.selected(Screen::Overview)).cloned()
    }

    /// Enter on a list: attach to the tree row, open the inbox card.
    fn activate(&mut self) -> Vec<Directive> {
        match self.screen {
            Screen::Overview => match self.selected_tree_row() {
                Some(TreeRow::Session { session_id }) => self.attach_to(session_id, None),
                Some(TreeRow::Agent {
                    session_id,
                    agent_id,
                    ..
                }) => self.attach_agent(session_id, agent_id),
                None => {}
            },
            Screen::Inbox => self.inbox_card = self.selected_inbox().is_some(),
            _ => {}
        }
        Vec::new()
    }

    fn selected_inbox(&self) -> Option<super::InboxEntry> {
        let entries = self.inbox_entries();
        let index = self
            .selected(Screen::Inbox)
            .min(entries.len().saturating_sub(1));
        entries.into_iter().nth(index)
    }

    /// `g` in the inbox: go to the agent that asked.
    fn attach_inbox_entry(&mut self) {
        if let Some(entry) = self.selected_inbox() {
            self.attach_agent(entry.session_id, entry.permission.agent_id);
        }
    }

    // -- Prompts ----------------------------------------------------------------

    /// A key for a prompt: the inline front prompt on the session screen,
    /// the selected entry in the inbox.
    fn prompt_key(&mut self, key: PromptKey) -> Vec<Directive> {
        let request = match self.screen {
            Screen::Inbox => self.selected_inbox().map(|entry| entry.permission),
            _ => self.front_prompt(),
        };
        let Some(request) = request else {
            return Vec::new();
        };
        if self.submitted.contains(&request.request_id) {
            return Vec::new();
        }
        let mut cursor = self.cursor_for(&request);
        let decision = cursor.press(&request, key);
        self.cursor = Some(cursor);
        decision.map_or_else(Vec::new, |decision| self.decide(&request, decision))
    }

    fn decide(&mut self, request: &PendingPermission, decision: Decision) -> Vec<Directive> {
        let request_id = request.request_id.clone();
        self.submitted.insert(request_id.clone());
        self.cursor = None;
        self.inbox_card = false;
        vec![match decision {
            Decision::Allow { remember } => Directive::Approve {
                request_id,
                remember,
            },
            Decision::Deny => Directive::Deny { request_id },
            Decision::Answer(answers) => Directive::Answer {
                request_id,
                answers,
            },
        }]
    }

    /// The live cursor, for tests and the inbox card.
    #[must_use]
    pub fn cursor(&self) -> Option<&PromptCursor> {
        self.cursor.as_ref()
    }
}

/// The answer to a confirmation: `y` does it, anything else cancels.
fn confirmed(confirm: Confirm, yes: bool) -> Vec<Directive> {
    match (confirm, yes) {
        (Confirm::Archive { session_id }, true) => vec![Directive::Archive { session_id }],
        _ => Vec::new(),
    }
}

/// The screen a navigation command opens.
fn screen_for(command: &Slash) -> Screen {
    match command {
        Slash::Inbox => Screen::Inbox,
        Slash::Workspaces => Screen::Workspaces,
        Slash::Usage => Screen::Usage,
        Slash::Logs => Screen::Logs,
        Slash::Forwards => Screen::Forwards,
        Slash::Help => Screen::Help,
        _ => Screen::Overview,
    }
}

/// Whether a session answers to a reference: its title, the title's slug,
/// its id, or a prefix of its id.
fn session_answers_to(session: &crate::state::SessionState, reference: &str) -> bool {
    session.title == reference
        || slug(&session.title) == reference
        || session.session_id.starts_with(reference)
}

fn slug(title: &str) -> String {
    title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("-")
        .to_lowercase()
}

/// Key-down (or auto-repeat), not the release events some terminals send.
fn is_press(key: &KeyEvent) -> bool {
    matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
}
