//! The list screens: the session/agent tree, the inbox, the forward panel.

use ratatui::layout::Rect;
use ratatui::text::{Line, Span};

use super::{card, marker};
use crate::app::{App, TreeRow};
use crate::keys::Screen;
use crate::state::{PendingPermission, SessionState};
use crate::theme;
use crate::wrap::{clip, fit};

/// The tree: every session, its agents nested by spawn ancestry, each with
/// its state (`LOST` included), worker and what waits on it.
pub(super) fn overview(app: &App, area: Rect) -> (Vec<Line<'static>>, &'static str) {
    let rows = app.tree_rows();
    let hints = "enter attach · n new session · x archive · r refresh · esc back · q quit";
    if rows.is_empty() {
        return (
            vec![Line::styled(
                "  no sessions yet — press n to start one, or run `horde new`",
                theme::dim(),
            )],
            hints,
        );
    }
    let width = usize::from(area.width);
    let selected = app.selected(Screen::Overview);
    let lines = rows
        .iter()
        .enumerate()
        .map(|(index, row)| tree_line(app, row, index == selected, width))
        .collect();
    (lines, hints)
}

fn tree_line(app: &App, row: &TreeRow, selected: bool, width: usize) -> Line<'static> {
    match row {
        TreeRow::Session { session_id } => app.session(session_id).map_or_else(
            || Line::raw(""),
            |session| session_line(app, session, selected),
        ),
        TreeRow::Agent {
            session_id,
            agent_id,
            depth,
        } => app.session(session_id).map_or_else(
            || Line::raw(""),
            |session| agent_line(app, session, agent_id, *depth, selected, width),
        ),
    }
}

fn session_line(app: &App, session: &SessionState, selected: bool) -> Line<'static> {
    let attached = app
        .attachment()
        .is_some_and(|attachment| attachment.session_id == session.session_id);
    let mut spans = vec![
        marker(selected),
        Span::styled(
            session.display_name(),
            if selected {
                theme::selected()
            } else {
                theme::bold()
            },
        ),
        Span::styled(
            format!("  {}", crate::state::short_id(&session.session_id)),
            theme::dim(),
        ),
        Span::styled(format!("  {}", session.lifecycle), theme::dim()),
    ];
    if !session.pending.is_empty() {
        spans.push(Span::styled(
            format!("  {} waiting", session.pending.len()),
            theme::needs_you(),
        ));
    }
    if attached {
        spans.push(Span::styled("  (attached)", theme::focus()));
    }
    Line::from(spans)
}

fn agent_line(
    app: &App,
    session: &SessionState,
    agent_id: &str,
    depth: usize,
    selected: bool,
    width: usize,
) -> Line<'static> {
    let node = session.agents.get(agent_id).cloned().unwrap_or_default();
    let indent = "  ".repeat(depth + 1);
    let name = fit(
        &format!("{indent}{}", session.agent_name(agent_id)),
        24.min(width / 3),
    );
    let state = node.state_word();
    let state_style = match state {
        "LOST" | "failed" => theme::error(),
        "working" | "queued" | "waiting on human" => theme::warn(),
        "running" | "idle" => theme::ok(),
        _ => theme::dim(),
    };
    let worker = node
        .worker_id
        .as_ref()
        .map_or_else(|| "—".to_owned(), |worker| app.worker_name(worker));
    let waiting = session
        .pending
        .values()
        .filter(|permission| permission.agent_id == agent_id)
        .count();
    let mut spans = vec![
        marker(selected),
        Span::styled(
            name,
            if selected {
                theme::selected()
            } else {
                theme::plain()
            },
        ),
        Span::styled(format!(" {}", fit(state, 16)), state_style),
        Span::styled(format!(" {}", fit(&worker, 14)), theme::dim()),
        Span::styled(format!(" {}", clip(&node.model, 20)), theme::dim()),
    ];
    if waiting > 0 {
        spans.push(Span::styled(
            format!("  {waiting} waiting"),
            theme::needs_you(),
        ));
    }
    Line::from(spans)
}

/// The inbox: every prompt waiting anywhere, and the selected one's card.
pub(super) fn inbox(app: &App, area: Rect) -> (Vec<Line<'static>>, &'static str) {
    let entries = app.inbox_entries();
    if entries.is_empty() {
        return (
            vec![Line::styled("  nothing needs you", theme::dim())],
            "esc back · q quit",
        );
    }
    let selected = app.selected(Screen::Inbox).min(entries.len() - 1);
    let width = usize::from(area.width);
    let mut lines: Vec<Line<'static>> = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            Line::from(vec![
                marker(index == selected),
                Span::styled(fit(&entry.title, 18), theme::bold()),
                Span::raw(" "),
                Span::raw(clip(
                    &prompt_summary(&entry.permission),
                    width.saturating_sub(22),
                )),
            ])
        })
        .collect();
    lines.push(Line::raw(""));
    let entry = &entries[selected];
    let cursor = app.cursor_for(&entry.permission);
    lines.extend(card::lines(
        app,
        &entry.permission,
        &cursor,
        app.inbox_card(),
        area.width,
    ));
    let hints = if app.inbox_card() {
        "answer in the card · esc back to the list"
    } else {
        "y allow · n deny · 1-9 answer · enter open · g go to agent · esc back"
    };
    (lines, hints)
}

/// One prompt in a row: who wants what.
#[must_use]
pub(super) fn prompt_summary(permission: &PendingPermission) -> String {
    let who = permission
        .tree_path
        .last()
        .cloned()
        .unwrap_or_else(|| crate::state::short_id(&permission.agent_id));
    match permission.questions.as_slice() {
        [] => format!("{who} wants `{}`", permission.rendered),
        [question] => format!("{who} asks: {}", question.question),
        [question, rest @ ..] => {
            format!("{who} asks: {} (+{} more)", question.question, rest.len())
        }
    }
}

/// The forward panel (ADR-0006): named forwards, where they lead, and the
/// backing only the server's connection registry can vouch for.
pub(super) fn forwards(app: &App) -> (Vec<Line<'static>>, &'static str) {
    let hints = "r refresh · esc back · q quit";
    let forwards = app.forwards();
    if forwards.is_empty() {
        return (
            vec![Line::styled(
                "  no live forwards — agents declare them, or `horde forward expose`",
                theme::dim(),
            )],
            hints,
        );
    }
    let selected = app.selected(Screen::Forwards);
    let lines = forwards
        .iter()
        .enumerate()
        .map(|(index, status)| {
            Line::from(vec![
                marker(index == selected),
                Span::raw(forward_row(app, status)),
            ])
        })
        .collect();
    (lines, hints)
}

/// `vite  expose 5173  session ci-fix  worker ok  backed by laptop`.
fn forward_row(app: &App, status: &horde_proto::v1::ForwardStatus) -> String {
    let intent = status.intent.clone().unwrap_or_default();
    let spec = intent.spec.unwrap_or_default();
    let direction = crate::state::forward_direction_name(spec.direction);
    let session_id = intent
        .session_id
        .as_ref()
        .map_or_else(String::new, ToString::to_string);
    let mut row = format!(
        "{}  {} {}  session {}",
        spec.name,
        direction,
        spec.worker_port,
        app.session_name(&session_id),
    );
    row.push_str(if status.worker_reachable {
        "  worker ok"
    } else {
        "  worker unreachable"
    });
    if direction == "inject" {
        row.push_str(&backing_note(
            intent.backer_client_id.as_ref().map(ToString::to_string),
            status.backed,
        ));
    }
    row
}

/// The inject row's backing suffix. Backed and unbacked both name the
/// backer, because the unbacked state's remedy is that principal's return.
fn backing_note(backer: Option<String>, backed: bool) -> String {
    match (backer, backed) {
        (Some(backer), true) => format!("  backed by {backer}"),
        (Some(backer), false) => format!("  UNBACKED — {backer} is detached"),
        (None, _) => "  UNBACKED — no backer recorded".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{backing_note, prompt_summary};
    use crate::state::{PendingPermission, PendingQuestion};

    #[test]
    fn the_backing_note_always_names_the_backer_it_has() {
        assert_eq!(
            backing_note(Some("laptop".to_owned()), true),
            "  backed by laptop"
        );
        assert_eq!(
            backing_note(Some("laptop".to_owned()), false),
            "  UNBACKED — laptop is detached"
        );
        assert_eq!(backing_note(None, false), "  UNBACKED — no backer recorded");
        assert_eq!(backing_note(None, true), "  UNBACKED — no backer recorded");
    }

    #[test]
    fn a_prompt_summary_names_who_and_what() {
        let mut permission = PendingPermission {
            agent_id: "agent-abcdefghij".to_owned(),
            rendered: "rm x".to_owned(),
            ..PendingPermission::default()
        };
        assert_eq!(prompt_summary(&permission), "abcdefgh wants `rm x`");
        permission.tree_path = vec!["root".to_owned(), "kid".to_owned()];
        let asked = |text: &str| PendingQuestion {
            question: text.to_owned(),
            ..PendingQuestion::default()
        };
        permission.questions = vec![asked("Why?")];
        assert_eq!(prompt_summary(&permission), "kid asks: Why?");
        permission.questions.push(asked("How?"));
        assert_eq!(prompt_summary(&permission), "kid asks: Why? (+1 more)");
    }
}
