//! Rendering: the App drawn onto a ratatui frame.
//!
//! Deliberately unstyled — layout and words only. Every screen renders from
//! [`App`]'s public accessors, so what the snapshots pin is exactly what an
//! attached human sees, and a `TestBackend` sees the same thing a terminal
//! does.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::text::Line;
use ratatui::widgets::Paragraph;

use crate::app::{App, Screen};
use crate::state::{FeedLine, PendingPermission, SessionState};

/// Draws the whole client: one screen at a time, same machinery at both zoom
/// levels (DESIGN.md §11).
pub fn render(app: &App, frame: &mut Frame<'_>) {
    match app.screen() {
        Screen::Overview => render_overview(app, frame),
        Screen::Inbox => render_inbox(app, frame),
        Screen::Session => render_session(app, frame),
    }
}

/// The multi-session overview: what `horde` with no verb shows.
fn render_overview(app: &App, frame: &mut Frame<'_>) {
    let sessions = app.sessions_in_order();
    let rows = if sessions.is_empty() {
        vec!["  no sessions — `horde session new <title>` starts one".to_owned()]
    } else {
        sessions
            .iter()
            .enumerate()
            .map(|(index, session)| overview_row(session, index == app.overview_selected()))
            .collect()
    };

    render_rows(
        frame,
        header_line("horde — sessions", app.waiting_total(), app.status()),
        rows,
        "[enter] attach  [i] inbox  [r] refresh  [q] quit",
    );
}

/// The global inbox: every escalation awaiting a human, across all sessions.
fn render_inbox(app: &App, frame: &mut Frame<'_>) {
    let entries = app.inbox_entries();
    let rows = if entries.is_empty() {
        vec!["  nothing needs you".to_owned()]
    } else {
        entries
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                format!(
                    "{} {}  {}",
                    marker(index == app.inbox_selected()),
                    entry.session_id,
                    permission_summary(&entry.permission),
                )
            })
            .collect()
    };

    render_rows(
        frame,
        header_line("inbox", entries.len(), app.status()),
        rows,
        "[y] allow  [n] deny  [enter] attach  [o] overview  [q] quit",
    );
}

/// One attached session: the live feed, the frontmost prompt, the compose
/// line.
fn render_session(app: &App, frame: &mut Frame<'_>) {
    let Some(attachment) = app.attachment() else {
        // Unreachable through the key dispatch (detaching leaves the screen),
        // but a render function that can panic on a state it finds odd is a
        // worse deal than an honest blank.
        render_rows(frame, Line::raw("not attached"), Vec::new(), "[q] quit");
        return;
    };
    let Some(session) = app.session(&attachment.session_id) else {
        render_rows(frame, Line::raw("not attached"), Vec::new(), "[q] quit");
        return;
    };

    let pending = app.scoped_pending();
    let prompt_rows = prompt_panel(&pending);
    let areas = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(u16::try_from(prompt_rows.len()).unwrap_or(u16::MAX)),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .split(frame.area());

    frame.render_widget(
        Paragraph::new(session_header(app, session, attachment.agent_id.as_deref())),
        areas[0],
    );
    frame.render_widget(
        Paragraph::new(feed_lines(&session.feed, app.scroll(), areas[1].height)),
        areas[1],
    );
    frame.render_widget(Paragraph::new(rows_to_lines(prompt_rows)), areas[2]);
    frame.render_widget(Paragraph::new(format!("> {}", app.input())), areas[3]);
    frame.render_widget(Paragraph::new(session_footer(&pending)), areas[4]);
}

/// The header/rows/footer shape the overview and the inbox share.
fn render_rows(frame: &mut Frame<'_>, header: Line<'_>, rows: Vec<String>, footer: &str) {
    let areas = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .split(frame.area());

    frame.render_widget(Paragraph::new(header), areas[0]);
    frame.render_widget(Paragraph::new(rows_to_lines(rows)), areas[1]);
    frame.render_widget(Paragraph::new(footer), areas[2]);
}

fn rows_to_lines(rows: Vec<String>) -> Vec<Line<'static>> {
    rows.into_iter().map(Line::from).collect()
}

/// A screen title, the waiting badge when anything waits, and the last
/// refusal when there was one.
fn header_line(title: &str, waiting: usize, status: &str) -> Line<'static> {
    let mut text = format!(" {title}");
    if waiting > 0 {
        text.push_str(&format!("  — {waiting} waiting"));
    }
    if !status.is_empty() {
        text.push_str(&format!("  ⚠ {status}"));
    }
    Line::from(text)
}

fn overview_row(session: &SessionState, selected: bool) -> String {
    let mut row = format!(
        "{} {}  {}  head={}  {}",
        marker(selected),
        session.session_id,
        session.lifecycle,
        session.head,
        session.title,
    );
    if !session.pending.is_empty() {
        row.push_str(&format!("  [{} waiting]", session.pending.len()));
    }
    row
}

fn marker(selected: bool) -> &'static str {
    if selected { ">" } else { " " }
}

/// The attached header: where you are, what you are scoped to, what is
/// happening elsewhere — the passive badge DESIGN.md §11 asks for.
fn session_header(app: &App, session: &SessionState, agent_id: Option<&str>) -> Line<'static> {
    let scope = agent_id.map_or_else(|| "session root".to_owned(), ToString::to_string);
    let mut text = format!(
        " {}  {}  attached: {scope}",
        session.session_id, session.title
    );
    if !session.caught_up {
        text.push_str("  (syncing)");
    }
    let elsewhere = app.waiting_elsewhere();
    if elsewhere > 0 {
        text.push_str(&format!("  — {elsewhere} waiting elsewhere"));
    }
    if !app.status().is_empty() {
        text.push_str(&format!("  ⚠ {}", app.status()));
    }
    Line::from(text)
}

/// The last lines that fit, honouring how far back the view is scrolled.
fn feed_lines(feed: &[FeedLine], scroll: usize, height: u16) -> Vec<Line<'static>> {
    let end = feed.len().saturating_sub(scroll);
    let start = end.saturating_sub(height as usize);
    feed[start..end].iter().map(rendered_feed_line).collect()
}

fn rendered_feed_line(line: &FeedLine) -> Line<'static> {
    Line::from(format!(
        "{:>5}  {:<11} {:<14} {}",
        line.sequence,
        line.label,
        clipped(&line.agent_id, 14),
        line.text,
    ))
}

/// The frontmost prompt in scope, rendered as a panel; empty when nothing
/// waits, so the feed gets the row back.
fn prompt_panel(pending: &[PendingPermission]) -> Vec<String> {
    let Some(front) = pending.first() else {
        return Vec::new();
    };
    let mut row = format!(" needs you: {}", permission_summary(front));
    if pending.len() > 1 {
        row.push_str(&format!("  (+{} more)", pending.len() - 1));
    }
    vec![row]
}

/// One escalation in a row: who wants what, under which mode.
fn permission_summary(permission: &PendingPermission) -> String {
    let who = if permission.tree_path.is_empty() {
        permission.agent_id.clone()
    } else {
        permission.tree_path.join(" > ")
    };
    let mut text = format!("{who} wants `{}`", permission.rendered);
    if !permission.mode.is_empty() {
        text.push_str(&format!("  (mode {})", permission.mode));
    }
    text
}

fn session_footer(pending: &[PendingPermission]) -> String {
    let mut footer = String::new();
    if !pending.is_empty() {
        footer.push_str("[y] allow  [n] deny  ");
    }
    footer.push_str("[enter] send  [ctrl-x] interrupt  [esc] detach  [ctrl-c] quit");
    footer
}

fn clipped(text: &str, limit: usize) -> String {
    text.chars().take(limit).collect()
}

#[cfg(test)]
mod tests {
    use crate::state::PendingPermission;

    use super::prompt_panel;

    fn pending(request_id: &str) -> PendingPermission {
        PendingPermission {
            request_id: request_id.to_owned(),
            agent_id: "agent-1".to_owned(),
            tree_path: Vec::new(),
            rendered: "ls".to_owned(),
            mode: String::new(),
            sequence: 1,
        }
    }

    /// Asserted here rather than left to the screen snapshots, because on an
    /// 80-column screen a long prompt pushes the suffix past the right edge —
    /// where every miscounted variant clips to the same pixels.
    #[test]
    fn the_prompt_panel_counts_only_what_it_hides() {
        assert!(
            prompt_panel(&[]).is_empty(),
            "no panel row when nothing waits"
        );

        let one = prompt_panel(&[pending("a")]);
        assert!(
            !one.concat().contains("more"),
            "a lone prompt hides nothing, so it counts nothing: {one:?}"
        );

        let two = prompt_panel(&[pending("a"), pending("b")]);
        assert!(
            two.concat().contains("(+1 more)"),
            "two waiting means one hidden: {two:?}"
        );
    }
}
