//! The prompt card: a permission request or a question, answerable in
//! place. Drawn inline on the session screen and under the inbox list.

use ratatui::style::Style;
use ratatui::text::{Line, Span};

use crate::app::App;
use crate::prompt::{PERMISSION_CHOICES, PromptCursor, has_free_text};
use crate::state::{PendingPermission, PendingQuestion};
use crate::theme;
use crate::wrap::{clip, wrap};

/// The card's rows, borders included, at `width` cells.
pub(super) fn lines(
    app: &App,
    request: &PendingPermission,
    cursor: &PromptCursor,
    focused: bool,
    width: u16,
) -> Vec<Line<'static>> {
    let inner = usize::from(width).saturating_sub(4).max(8);
    let border = if focused {
        theme::needs_you()
    } else {
        theme::dim()
    };
    let mut body = if request.is_question() {
        question_body(request, cursor, inner)
    } else {
        permission_body(request, cursor, inner)
    };
    if app.is_submitted(&request.request_id) {
        body.push(Line::styled(
            "sent — waiting for the server to record it…",
            theme::dim(),
        ));
    }
    let mut rows = vec![edge(&title(request, cursor), inner, border, true)];
    rows.extend(body.into_iter().map(|line| framed(line, inner, border)));
    rows.push(edge(footer(request, focused), inner, border, false));
    rows
}

/// `Permission · ci-fix > runner` or `Question 1/2 · Colour · ci-fix`.
fn title(request: &PendingPermission, cursor: &PromptCursor) -> String {
    let who = if request.tree_path.is_empty() {
        crate::state::short_id(&request.agent_id)
    } else {
        request.tree_path.join(" > ")
    };
    match request.questions.get(cursor.question) {
        Some(question) => {
            let mut title = format!(
                "Question {}/{}",
                cursor.question + 1,
                request.questions.len()
            );
            if !question.header.is_empty() {
                title.push_str(&format!(" · {}", question.header));
            }
            format!("{title} · {who}")
        }
        None => format!("Permission · {who}"),
    }
}

fn footer(request: &PendingPermission, focused: bool) -> &'static str {
    match (focused, request.is_question()) {
        (false, _) => "tab to answer",
        (true, false) => "↑↓ enter · y yes · n no · tab compose",
        (true, true) => "↑↓ enter · space toggle · ←→ question · n decline",
    }
}

fn permission_body(
    request: &PendingPermission,
    cursor: &PromptCursor,
    inner: usize,
) -> Vec<Line<'static>> {
    let tool = if request.tool_name.is_empty() {
        "The agent".to_owned()
    } else {
        request.tool_name.clone()
    };
    let mut body = vec![Line::styled(format!("{tool} wants to run:"), theme::PLAIN)];
    for row in wrap(&request.rendered, inner.saturating_sub(2)) {
        body.push(Line::from(vec![
            Span::raw("  "),
            Span::styled(row, theme::bold()),
        ]));
    }
    if !request.mode.is_empty() {
        body.push(Line::styled(format!("mode {}", request.mode), theme::dim()));
    }
    for (index, choice) in PERMISSION_CHOICES.iter().enumerate() {
        body.push(choice_line(
            index == cursor.row(),
            &format!("{}. {choice}", index + 1),
            None,
        ));
    }
    body
}

fn question_body(
    request: &PendingPermission,
    cursor: &PromptCursor,
    inner: usize,
) -> Vec<Line<'static>> {
    let Some(question) = request.questions.get(cursor.question) else {
        return Vec::new();
    };
    let mut body: Vec<Line<'static>> = wrap(&question.question, inner)
        .into_iter()
        .map(|row| Line::styled(row, theme::bold()))
        .collect();
    let chosen = &cursor.chosen[cursor.question];
    for (index, option) in question.options.iter().enumerate() {
        let check = question.multi_select.then(|| chosen.contains(&index));
        let mut label = format!("{}. {}", index + 1, option.label);
        if !option.description.is_empty() {
            label.push_str(&format!(" — {}", option.description));
        }
        body.push(choice_line(
            index == cursor.row(),
            &clip(&label, inner.saturating_sub(6)),
            check,
        ));
    }
    if has_free_text(question) {
        body.push(free_text_line(question, cursor, inner));
    }
    body
}

/// One choosable row: the highlight marker, a checkbox for multi-select.
fn choice_line(highlighted: bool, label: &str, check: Option<bool>) -> Line<'static> {
    let marker = if highlighted {
        Span::styled("❯ ", theme::selected())
    } else {
        Span::raw("  ")
    };
    let box_ = match check {
        Some(true) => "[x] ",
        Some(false) => "[ ] ",
        None => "",
    };
    let style = if highlighted {
        theme::selected()
    } else {
        theme::PLAIN
    };
    Line::from(vec![marker, Span::styled(format!("{box_}{label}"), style)])
}

/// The free-text row, secrets masked.
fn free_text_line(
    question: &PendingQuestion,
    cursor: &PromptCursor,
    inner: usize,
) -> Line<'static> {
    let typed = &cursor.free_text[cursor.question];
    let shown = if question.secret {
        "•".repeat(typed.chars().count())
    } else {
        typed.clone()
    };
    let highlighted = cursor.row() == question.options.len();
    let label = if question.options.is_empty() {
        "Answer"
    } else {
        "Other"
    };
    let caret = if highlighted { "▏" } else { "" };
    choice_line(
        highlighted,
        &clip(&format!("{label}: {shown}{caret}"), inner.saturating_sub(2)),
        None,
    )
}

/// A top or bottom border with text set into it.
fn edge(text: &str, inner: usize, style: Style, top: bool) -> Line<'static> {
    let (left, right) = if top {
        ("╭─ ", "╮")
    } else {
        ("╰─ ", "╯")
    };
    let text = clip(text, inner.saturating_sub(1));
    let fill = inner.saturating_sub(crate::wrap::width(&text) + 1);
    Line::from(vec![
        Span::styled(left, style),
        Span::styled(text, style),
        Span::styled(format!(" {}{right}", "─".repeat(fill)), style),
    ])
}

/// A body row between the side borders, padded to the card's width.
fn framed(line: Line<'static>, inner: usize, style: Style) -> Line<'static> {
    let text: String = line
        .spans
        .iter()
        .map(|span| span.content.as_ref())
        .collect();
    let pad = inner.saturating_sub(crate::wrap::width(&text));
    let mut spans = vec![Span::styled("│ ", style)];
    spans.extend(line.spans);
    spans.push(Span::raw(" ".repeat(pad)));
    spans.push(Span::styled(" │", style));
    Line::from(spans)
}
