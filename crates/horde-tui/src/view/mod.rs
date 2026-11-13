//! Rendering: the App drawn onto a ratatui frame.
//!
//! Pure over [`App`]'s public accessors: what the snapshots pin is exactly
//! what an attached human sees, and a `TestBackend` sees the same thing a
//! terminal does. Colour is spent on state only (see `theme`), so the
//! snapshots — which record characters, not colours — carry the whole
//! layout and every word.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::app::App;
use crate::keys::Screen;
use crate::theme;

mod card;
mod lists;
mod panels;
mod session;

/// Draws the whole client, one screen at a time.
pub fn render(app: &App, frame: &mut Frame<'_>) {
    match app.screen() {
        Screen::Session => session::render(app, frame),
        screen => render_list_screen(app, frame, screen),
    }
}

/// The frame every non-session screen shares: tabs, body, hints, status.
fn render_list_screen(app: &App, frame: &mut Frame<'_>, screen: Screen) {
    let areas = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .split(frame.area());
    frame.render_widget(Paragraph::new(tabs(app, screen)), areas[0]);
    let (body, hints) = match screen {
        Screen::Overview => lists::overview(app, areas[2]),
        Screen::Inbox => lists::inbox(app, areas[2]),
        Screen::Forwards => lists::forwards(app),
        Screen::Workspaces => panels::workspaces(app),
        Screen::Usage => panels::usage(app),
        Screen::Logs => panels::logs(),
        _ => panels::help(),
    };
    frame.render_widget(
        Paragraph::new(scrolled(body, app.selected(screen), areas[2])),
        areas[2],
    );
    frame.render_widget(Paragraph::new(hint_line(app, hints)), areas[3]);
    frame.render_widget(Paragraph::new(status_line(app)), areas[4]);
}

/// Keeps the selected row on screen: the body scrolls once the selection
/// passes the bottom. Rows are the body's lines; `selected` indexes the
/// list's *items*, which for these screens start at the top and run one
/// item per line or more, so this is a floor, not an exact fit.
fn scrolled(body: Vec<Line<'static>>, selected: usize, area: Rect) -> Vec<Line<'static>> {
    let height = usize::from(area.height);
    let skip = selected.saturating_sub(height.saturating_sub(2));
    body.into_iter().skip(skip).collect()
}

/// The screen names across the top, the current one highlighted, the
/// inbox counting what waits.
fn tabs(app: &App, current: Screen) -> Line<'static> {
    let waiting = app.waiting_total();
    let inbox = if waiting > 0 {
        format!("inbox {waiting}")
    } else {
        "inbox".to_owned()
    };
    let entries = [
        (Screen::Session, "session".to_owned()),
        (Screen::Overview, "agents".to_owned()),
        (Screen::Inbox, inbox),
        (Screen::Workspaces, "workspaces".to_owned()),
        (Screen::Usage, "usage".to_owned()),
        (Screen::Logs, "logs".to_owned()),
        (Screen::Forwards, "forwards".to_owned()),
    ];
    let mut spans = vec![Span::styled(" horde ", theme::bold())];
    for (index, (screen, name)) in entries.into_iter().enumerate() {
        let style = if screen == current {
            theme::selected()
        } else {
            theme::dim()
        };
        spans.push(Span::styled(format!(" {}:{name} ", index + 1), style));
    }
    Line::from(spans)
}

/// The hint line: a pending confirmation outranks the screen's own hints,
/// and the last refusal outranks both — the human needs to see why.
fn hint_line(app: &App, hints: &str) -> Line<'static> {
    if let Some(crate::app::Confirm::Archive { session_id }) = app.confirm() {
        return Line::styled(
            format!(
                " archive {}? y archives · any other key cancels",
                app.session_name(session_id)
            ),
            theme::warn(),
        );
    }
    if !app.status().is_empty() {
        return Line::styled(format!(" ⚠ {}", app.status()), theme::error());
    }
    Line::styled(format!(" {hints}"), theme::dim())
}

/// The status line: where you are and whether the stream is alive.
fn status_line(app: &App) -> Line<'static> {
    let mut spans = Vec::new();
    match app.attached_session() {
        Some(session) => spans.extend(attached_status(app, session)),
        None => spans.push(Span::styled(" not attached", theme::dim())),
    }
    spans.push(Span::styled(" · ", theme::dim()));
    spans.push(connection_span(app));
    let elsewhere = app.waiting_elsewhere();
    if app.attachment().is_some() && elsewhere > 0 {
        spans.push(Span::styled(
            format!(" · {elsewhere} waiting elsewhere"),
            theme::needs_you(),
        ));
    }
    Line::from(spans)
}

fn attached_status(app: &App, session: &crate::state::SessionState) -> Vec<Span<'static>> {
    let mut spans = vec![Span::styled(
        format!(" {}", session.display_name()),
        theme::bold(),
    )];
    let Some(agent_id) = app.viewed_agent() else {
        return spans;
    };
    let node = session.agents.get(&agent_id).cloned().unwrap_or_default();
    let mut facts = vec![session.agent_name(&agent_id)];
    facts.extend(
        [node.harness.clone(), node.model.clone()]
            .into_iter()
            .filter(|fact| !fact.is_empty()),
    );
    if let Some(worker) = node.worker_id.as_ref() {
        facts.push(format!("on {}", app.worker_name(worker)));
    }
    facts.push(node.state_word().to_owned());
    spans.push(Span::styled(
        format!(" · {}", facts.join(" · ")),
        theme::dim(),
    ));
    spans
}

/// The connection word, coloured by health, with the ended reason.
fn connection_span(app: &App) -> Span<'static> {
    let word = app.connection_word();
    match word {
        "ended" => {
            let reason = app.stream_ended().filter(|reason| !reason.is_empty());
            let text = reason.map_or_else(
                || "⚠ stream ended".to_owned(),
                |reason| format!("⚠ stream ended ({reason})"),
            );
            Span::styled(text, theme::error())
        }
        "stale" => Span::styled("⚠ stream stale — no heartbeat", theme::warn()),
        "live" => Span::styled("● live", theme::ok()),
        other => Span::styled(format!("○ {other}"), theme::dim()),
    }
}

/// A list row's leading marker.
fn marker(selected: bool) -> Span<'static> {
    if selected {
        Span::styled("❯ ", theme::selected())
    } else {
        Span::raw("  ")
    }
}
#[cfg(test)]
mod tests;
