//! The front door's session picker (ADR-0008).
//!
//! A picker exists only where a human does — the bare-invocation flow in
//! [`crate::front_door`] decides that — so what lives here is only the
//! choosing: rows rendered by the same [`crate::render::session_rows`] the
//! list verb prints (one rendering; choosing and listing look identical),
//! a "new session" top row, and a key loop with the same seam the TUI has —
//! [`drive`] is generic over the terminal backend and takes its input as a
//! channel, so tests run the whole loop on a `TestBackend` with scripted
//! keys. The raw-terminal shell around it lives in `front_door`, which owns
//! one terminal session across choosing and attaching.

use horde_proto::v1::SessionSummary;
use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::Backend;
use ratatui::crossterm::event::{Event as TermEvent, KeyCode, KeyEventKind};
use ratatui::layout::{Constraint, Layout};
use ratatui::text::Line;
use ratatui::widgets::Paragraph;
use tokio::sync::mpsc;

use crate::ClientError;

/// What the human chose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Choice {
    /// The "new session" top row: create auto-named, then attach.
    NewSession,
    /// One live session, by id.
    Attach(String),
    /// Esc or q — leave without entering anything.
    Dismissed,
}

/// The picker's state: rows in the order given (the front door sorts newest
/// first), a cursor, and nothing else — no client, no stream.
#[derive(Debug)]
pub struct Picker {
    /// Session rows, pre-rendered by the shared row format.
    rows: Vec<String>,
    session_ids: Vec<String>,
    /// 0 is the "new session" row; `1..=rows.len()` are sessions.
    selected: usize,
}

impl Picker {
    /// A picker over `sessions`, shown in the order given.
    #[must_use]
    pub fn new(sessions: &[SessionSummary], now_unix_ms: i64) -> Self {
        Self {
            rows: crate::render::session_rows(sessions, now_unix_ms),
            session_ids: sessions
                .iter()
                .map(|summary| {
                    summary
                        .session_id
                        .as_ref()
                        .map_or_else(String::new, ToString::to_string)
                })
                .collect(),
            selected: 0,
        }
    }

    /// Applies one terminal event; `Some` is the loop's exit.
    pub fn on_input(&mut self, event: &TermEvent) -> Option<Choice> {
        let TermEvent::Key(key) = event else {
            return None;
        };
        if key.kind == KeyEventKind::Release {
            return None;
        }
        // Raw mode turns Ctrl-C into a keypress rather than SIGINT, so
        // unless the picker honours it the one universal way out of a
        // terminal program does nothing at all.
        if is_interrupt(key) {
            return Some(Choice::Dismissed);
        }
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected = self.selected.saturating_sub(1);
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.selected = (self.selected + 1).min(self.rows.len());
                None
            }
            KeyCode::Enter => Some(self.chosen()),
            KeyCode::Esc | KeyCode::Char('q') => Some(Choice::Dismissed),
            _ => None,
        }
    }

    /// What Enter on the current row means.
    fn chosen(&self) -> Choice {
        match self.selected.checked_sub(1) {
            None => Choice::NewSession,
            Some(index) => self
                .session_ids
                .get(index)
                .map_or(Choice::Dismissed, |id| Choice::Attach(id.clone())),
        }
    }

    /// Where the cursor is — for tests, which assert on state rather than
    /// scraping the marker back out of pixels.
    #[must_use]
    pub fn selected(&self) -> usize {
        self.selected
    }
}

/// Draws the picker: a header, the marked rows, and the key footer.
fn render(picker: &Picker, frame: &mut Frame<'_>) {
    let areas = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .split(frame.area());

    let mut lines = vec![Line::from(format!(
        "{} new session",
        marker(picker.selected == 0)
    ))];
    lines.extend(
        picker.rows.iter().enumerate().map(|(index, row)| {
            Line::from(format!("{} {row}", marker(picker.selected == index + 1)))
        }),
    );

    frame.render_widget(Paragraph::new("horde — pick a session"), areas[0]);
    frame.render_widget(Paragraph::new(lines), areas[1]);
    frame.render_widget(
        Paragraph::new("[enter] attach  [j/k] move  [esc/ctrl-c] leave"),
        areas[2],
    );
}

/// Ctrl-C, and only Ctrl-C: a plain `c` is not a way out.
fn is_interrupt(key: &ratatui::crossterm::event::KeyEvent) -> bool {
    key.code == KeyCode::Char('c')
        && key
            .modifiers
            .contains(ratatui::crossterm::event::KeyModifiers::CONTROL)
}

fn marker(selected: bool) -> &'static str {
    if selected { ">" } else { " " }
}

/// The choosing loop, generic over where the pixels go and where the keys
/// come from — the same seam [`crate::tui::drive`] has, for the same
/// reason: the whole loop runs in tests against a `TestBackend`.
///
/// Borrows the input channel rather than owning it, because choosing is only
/// the first half of one terminal session: attaching continues on the same
/// reader, and a second reader thread would swallow the first keystroke
/// after the handoff.
///
/// # Errors
///
/// A failed draw. A closed input channel is a dismissal, not an error: the
/// terminal went away, and there is no one left to choose.
pub async fn drive<B>(
    mut terminal: Terminal<B>,
    input: &mut mpsc::Receiver<TermEvent>,
    mut picker: Picker,
    mut observe: impl FnMut(&Picker),
) -> Result<(Terminal<B>, Choice), ClientError>
where
    B: Backend,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    loop {
        terminal
            .draw(|frame| render(&picker, frame))
            .map_err(|error| ClientError::Terminal(std::io::Error::other(error)))?;
        observe(&picker);

        match input.recv().await {
            Some(event) => {
                if let Some(choice) = picker.on_input(&event) {
                    return Ok((terminal, choice));
                }
            }
            None => return Ok((terminal, Choice::Dismissed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use ratatui::crossterm::event::{Event as TermEvent, KeyCode, KeyEvent};

    use super::{Choice, Picker};

    fn session(id: &str, title: &str) -> horde_proto::v1::SessionSummary {
        horde_proto::v1::SessionSummary {
            session_id: Some(horde_proto::v1::SessionId::new(id)),
            title: title.to_owned(),
            ..horde_proto::v1::SessionSummary::default()
        }
    }

    fn picker() -> Picker {
        Picker::new(
            &[session("session-1", "one"), session("session-2", "two")],
            0,
        )
    }

    fn press(picker: &mut Picker, code: KeyCode) -> Option<Choice> {
        picker.on_input(&TermEvent::Key(KeyEvent::from(code)))
    }

    /// The top row is "new session" and Enter there creates; the cursor
    /// walks the session rows in the order given and stops at both ends
    /// rather than wrapping into a misclick.
    #[test]
    fn the_cursor_starts_on_new_session_and_stays_in_bounds() {
        let mut picker = picker();
        assert_eq!(press(&mut picker, KeyCode::Enter), Some(Choice::NewSession));

        assert_eq!(press(&mut picker, KeyCode::Up), None);
        assert_eq!(picker.selected(), 0, "the top does not wrap");

        press(&mut picker, KeyCode::Down);
        assert_eq!(
            press(&mut picker, KeyCode::Enter),
            Some(Choice::Attach("session-1".to_owned()))
        );
        press(&mut picker, KeyCode::Char('j'));
        assert_eq!(
            press(&mut picker, KeyCode::Enter),
            Some(Choice::Attach("session-2".to_owned()))
        );
        press(&mut picker, KeyCode::Down);
        assert_eq!(picker.selected(), 2, "the bottom does not wrap either");
        assert_eq!(
            press(&mut picker, KeyCode::Char('k')),
            None,
            "k moves like Up"
        );
        assert_eq!(picker.selected(), 1);
    }

    /// Esc and q both leave without attaching, and keys that mean nothing
    /// change nothing.
    #[test]
    fn esc_and_q_dismiss_and_unknown_keys_are_inert() {
        let mut picker = picker();
        assert_eq!(press(&mut picker, KeyCode::Char('x')), None);
        assert_eq!(picker.selected(), 0);
        assert_eq!(press(&mut picker, KeyCode::Esc), Some(Choice::Dismissed));
        assert_eq!(
            press(&mut picker, KeyCode::Char('q')),
            Some(Choice::Dismissed)
        );
    }

    /// The cursor marker is a real glyph on exactly the selected row —
    /// without it, every row of the picker reads as equally chosen.
    #[tokio::test]
    async fn the_cursor_marker_paints_on_exactly_the_selected_row() {
        let terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(80, 24))
            .expect("a test terminal always constructs");
        let (keys, mut receiver) = tokio::sync::mpsc::channel(4);
        keys.send(TermEvent::Key(KeyEvent::from(KeyCode::Enter)))
            .await
            .expect("the loop is about to listen");
        // The script's own length is the ceiling: with the sender gone, a
        // picker that ignores its keys drains the queue, meets the closed
        // channel, and comes back Dismissed — a wrong answer now, never a
        // hang the harness has to time out.
        drop(keys);

        let (terminal, choice) = super::drive(terminal, &mut receiver, picker(), |_| {})
            .await
            .expect("the loop finishes");
        assert_eq!(choice, Choice::NewSession);

        let screen = terminal.backend().to_string();
        assert!(
            screen.contains("> new session"),
            "the selected row carries the marker: {screen}"
        );
        for line in screen.lines() {
            if line.contains("session-1") || line.contains("session-2") {
                assert!(
                    !line.trim_start().starts_with('>'),
                    "an unselected row must not claim the cursor: {line:?}"
                );
            }
        }
    }

    /// Ctrl-C leaves the picker, like Esc; a plain `c`, or Ctrl with any
    /// other key, does nothing.
    #[test]
    fn ctrl_c_dismisses_and_nothing_near_it_does() {
        use ratatui::crossterm::event::KeyModifiers;
        let mut picker = picker();
        let ctrl = |code| TermEvent::Key(KeyEvent::new(code, KeyModifiers::CONTROL));
        assert_eq!(picker.on_input(&ctrl(KeyCode::Char('x'))), None);
        assert_eq!(press(&mut picker, KeyCode::Char('c')), None);
        assert_eq!(
            picker.on_input(&ctrl(KeyCode::Char('c'))),
            Some(Choice::Dismissed)
        );
    }

    /// A key release is not a keypress: terminals that report both would
    /// otherwise double every cursor move.
    #[test]
    fn key_releases_are_ignored() {
        use ratatui::crossterm::event::{KeyEventKind, KeyModifiers};
        let mut picker = picker();
        let mut key = KeyEvent::new(KeyCode::Down, KeyModifiers::NONE);
        key.kind = KeyEventKind::Release;
        let release = TermEvent::Key(key);
        assert_eq!(picker.on_input(&release), None);
        assert_eq!(picker.selected(), 0);
    }
}
