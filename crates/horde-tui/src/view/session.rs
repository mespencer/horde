//! The session screen: the attached agent's transcript, a prompt card when
//! something waits on the human, the compose box with its command menu,
//! and the hint and status lines.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Position, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Paragraph};

use super::{card, hint_line, status_line};
use crate::app::App;
use crate::slash;
use crate::theme;

/// The compose box never grows past this many text rows.
const COMPOSE_MAX_ROWS: usize = 6;

pub(super) fn render(app: &App, frame: &mut Frame<'_>) {
    let area = frame.area();
    let width = area.width;
    let card = app.front_prompt().map(|request| {
        card::lines(app, &request, &app.cursor_for(&request), app.prompt_focused(), width)
    });
    let menu = menu_lines(app);
    let compose_rows = compose_rows(app, width.saturating_sub(4));
    let working = app.agent_working() && card.is_none();

    let areas = Layout::vertical([
        Constraint::Min(1),
        Constraint::Length(u16::from(working)),
        Constraint::Length(height(card.as_ref().map_or(0, Vec::len))),
        Constraint::Length(height(compose_rows.len() + 2)),
        Constraint::Length(height(menu.len())),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .split(area);

    frame.render_widget(Paragraph::new(transcript(app, areas[0])), areas[0]);
    if working {
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled("✻ ", theme::warn()),
                Span::styled("Working… ", theme::warn()),
                Span::styled("(esc to interrupt)", theme::dim()),
            ])),
            areas[1],
        );
    }
    if let Some(card) = card {
        frame.render_widget(Paragraph::new(card), areas[2]);
    }
    render_compose(app, frame, areas[3], &compose_rows);
    frame.render_widget(Paragraph::new(menu), areas[4]);
    frame.render_widget(Paragraph::new(hint_line(app, hints(app))), areas[5]);
    frame.render_widget(Paragraph::new(status_line(app)), areas[6]);
}

fn height(rows: usize) -> u16 {
    u16::try_from(rows).unwrap_or(u16::MAX)
}

/// The rows of the agent's transcript that fit, honouring the scroll.
fn transcript(app: &App, area: Rect) -> Vec<Line<'static>> {
    let Some(session) = app.attached_session() else {
        return vec![Line::styled("not attached", theme::dim())];
    };
    let transcript = app
        .viewed_agent()
        .and_then(|agent| session.transcripts.get(&agent));
    let Some(transcript) = transcript.filter(|transcript| !transcript.blocks.is_empty()) else {
        return empty_transcript(app, session);
    };
    let rows = crate::layout::transcript_lines(transcript, app.details(), area.width);
    let end = rows.len().saturating_sub(app.scroll());
    let start = end.saturating_sub(usize::from(area.height));
    let mut shown: Vec<Line<'static>> = rows[start..end].to_vec();
    // Bottom-anchored like a chat: short transcripts sit on the compose box.
    let pad = usize::from(area.height).saturating_sub(shown.len());
    let mut lines = vec![Line::raw(""); pad];
    lines.append(&mut shown);
    lines
}

/// What an empty transcript says: syncing while the replay runs, and
/// otherwise how to start.
fn empty_transcript(app: &App, session: &crate::state::SessionState) -> Vec<Line<'static>> {
    let text = if !session.caught_up {
        format!("syncing {}…", session.display_name())
    } else if app.viewed_agent().is_none() {
        format!(
            "{} has no agent yet — type a message to start one",
            session.display_name()
        )
    } else {
        "nothing said yet — type a message below".to_owned()
    };
    vec![Line::styled(format!(" {text}"), theme::dim())]
}

/// The compose text hard-wrapped to the box's inner width.
fn compose_rows(app: &App, width: u16) -> Vec<String> {
    let width = usize::from(width).max(1);
    let mut rows = Vec::new();
    for line in app.compose().lines() {
        let chars: Vec<char> = line.chars().collect();
        if chars.is_empty() {
            rows.push(String::new());
        }
        for chunk in chars.chunks(width) {
            rows.push(chunk.iter().collect());
        }
    }
    let skip = rows.len().saturating_sub(COMPOSE_MAX_ROWS);
    rows.split_off(skip)
}

fn render_compose(app: &App, frame: &mut Frame<'_>, area: Rect, rows: &[String]) {
    let focused = !app.prompt_focused();
    let border = if focused { theme::plain() } else { theme::dim() };
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(border);
    let placeholder = app.compose().is_empty();
    let lines: Vec<Line<'static>> = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let gutter = if index == 0 { "› " } else { "  " };
            let body = if placeholder && focused {
                Span::styled(placeholder_text(app), theme::dim())
            } else {
                Span::raw(row.clone())
            };
            Line::from(vec![Span::styled(gutter, theme::focus()), body])
        })
        .collect();
    frame.render_widget(Paragraph::new(lines).block(block), area);
    if focused {
        frame.set_cursor_position(cursor_position(app, area));
    }
}

fn placeholder_text(app: &App) -> &'static str {
    if app.agent_working() {
        "queue a message for the next turn (ctrl-s sends now)"
    } else {
        "message the agent, or / for commands"
    }
}

/// Where the terminal cursor sits: after the compose cursor's character,
/// accounting for the hard wrap.
fn cursor_position(app: &App, area: Rect) -> Position {
    let width = usize::from(area.width.saturating_sub(4)).max(1);
    let (row, col) = app.compose().cursor();
    let above: usize = app.compose().lines()[..row]
        .iter()
        .map(|line| line.chars().count() / width + 1)
        .sum();
    let visual_row = (above + col / width).min(COMPOSE_MAX_ROWS - 1);
    Position {
        x: area.x + 3 + u16::try_from(col % width).unwrap_or(0),
        y: area.y + 1 + u16::try_from(visual_row).unwrap_or(0),
    }
}

/// The slash-command menu under the compose box while a command word is
/// being typed.
fn menu_lines(app: &App) -> Vec<Line<'static>> {
    let completions = slash::completions(&app.input());
    let highlighted = app.menu().min(completions.len().saturating_sub(1));
    completions
        .iter()
        .take(8)
        .enumerate()
        .map(|(index, command)| {
            let style = if index == highlighted {
                theme::selected()
            } else {
                theme::plain()
            };
            Line::from(vec![
                Span::styled(format!("  /{:<11}", command.name), style),
                Span::styled(format!("{:<17}", command.args), theme::dim()),
                Span::styled(command.help, theme::dim()),
            ])
        })
        .collect()
}

/// The hints for what the keys do right now.
fn hints(app: &App) -> &'static str {
    if app.prompt_focused() {
        "↑↓ choose · enter select · tab to compose · ctrl-t agents · ? help"
    } else if !slash::completions(&app.input()).is_empty() {
        "↑↓ choose · tab complete · enter run · esc cancel"
    } else if app.agent_working() {
        "enter queue · ctrl-s send now · esc interrupt · ctrl-t agents · ? help"
    } else {
        "enter send · alt-enter newline · / commands · ctrl-t agents · ? help"
    }
}
