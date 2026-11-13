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
        Screen::Forwards => render_forwards(app, frame),
    }
}

/// The forward panel (ADR-0006): named forwards, where they lead, and the
/// backer/unbacked state only the server's connection registry can vouch for.
fn render_forwards(app: &App, frame: &mut Frame<'_>) {
    let forwards = app.forwards();
    let rows = if forwards.is_empty() {
        vec!["  no live forwards — agents declare them, or `horde forward expose`".to_owned()]
    } else {
        forwards
            .iter()
            .enumerate()
            .map(|(index, status)| forward_row(app, status, index == app.forwards_selected()))
            .collect()
    };

    render_rows(
        frame,
        header_line("forwards", 0, app),
        rows,
        "[r] refresh  [o] overview  [q] quit",
    );
}

/// One forward as a panel row: `> vite  expose 5173  session ci-fix  worker ok  backed by laptop`.
fn forward_row(app: &App, status: &horde_proto::v1::ForwardStatus, selected: bool) -> String {
    let intent = status.intent.clone().unwrap_or_default();
    let spec = intent.spec.unwrap_or_default();
    let direction = crate::state::forward_direction_name(spec.direction);
    let session_id = intent
        .session_id
        .as_ref()
        .map_or_else(String::new, ToString::to_string);
    let mut row = format!(
        "{} {}  {} {}  session {}",
        marker(selected),
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
        // An inject with no recorded backer should be unjournalable; saying
        // so beats hiding the row that proves otherwise.
        (None, _) => "  UNBACKED — no backer recorded".to_owned(),
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
        header_line("horde — sessions", app.waiting_total(), app),
        rows,
        "[enter] attach  [i] inbox  [f] forwards  [r] refresh  [q] quit",
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
                    entry.title,
                    permission_summary(&entry.permission),
                )
            })
            .collect()
    };

    render_rows(
        frame,
        header_line("inbox", entries.len(), app),
        rows,
        "[y] allow  [1-9] answer  [n] deny  [enter] attach  [o] overview  [q] quit",
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
        Paragraph::new(feed_lines(session, app.scroll(), areas[1].height)),
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

/// A screen title, the waiting badge when anything waits, and the stream and
/// status badges.
fn header_line(title: &str, waiting: usize, app: &App) -> Line<'static> {
    let mut text = format!(" {title}");
    if waiting > 0 {
        text.push_str(&format!("  — {waiting} waiting"));
    }
    text.push_str(&badges(app));
    Line::from(text)
}

/// The badges every header ends with: the stream's health, then the last
/// refusal when there was one.
fn badges(app: &App) -> String {
    let mut text = stream_badge(app.stream_ended(), app.stream_stale()).to_owned();
    if let Some(reason) = app.stream_ended().filter(|reason| !reason.is_empty()) {
        text.push_str(&format!(" ({reason})"));
    }
    if !app.status().is_empty() {
        text.push_str(&format!("  ⚠ {}", app.status()));
    }
    text
}

/// The stream-health badge. An ended stream outranks a stale one: once the
/// server has hung up, "no heartbeat" is a guess about something known.
fn stream_badge(ended: Option<&str>, stale: bool) -> &'static str {
    match (ended, stale) {
        (Some(_), _) => ENDED_BADGE,
        (None, true) => STALE_BADGE,
        (None, false) => "",
    }
}

/// The staleness badge (fix a9): rendered when the follow stream has been
/// silent past the heartbeat ceiling, on every screen — a dead stream must
/// not look like a quiet one anywhere.
const STALE_BADGE: &str = "  ⚠ stream stale — no heartbeat";

/// The ended badge: the server closed the follow stream, so nothing on
/// screen will move again until the TUI is restarted. Short, because the
/// reason follows it on a header line that is often 80 columns.
const ENDED_BADGE: &str = "  ⚠ stream ended";

/// A session as a human names it, with the short id beside a title so two
/// same-titled sessions stay tellable apart — the id alone when untitled.
fn session_label(session: &SessionState) -> String {
    let short = crate::state::short_id(&session.session_id);
    if session.title.is_empty() {
        short
    } else {
        format!("{}  {short}", session.title)
    }
}

fn overview_row(session: &SessionState, selected: bool) -> String {
    let mut row = format!(
        "{} {}  {}  head={}",
        marker(selected),
        session_label(session),
        session.lifecycle,
        session.head,
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
    let scope = agent_id.map_or_else(|| "session root".to_owned(), |id| session.agent_name(id));
    let mut text = format!(" {}  attached: {scope}", session_label(session));
    if !session.caught_up {
        text.push_str("  (syncing)");
    }
    let elsewhere = app.waiting_elsewhere();
    if elsewhere > 0 {
        text.push_str(&format!("  — {elsewhere} waiting elsewhere"));
    }
    text.push_str(&badges(app));
    Line::from(text)
}

/// The last lines that fit, honouring how far back the view is scrolled.
fn feed_lines(session: &SessionState, scroll: usize, height: u16) -> Vec<Line<'static>> {
    let end = session.feed.len().saturating_sub(scroll);
    let start = end.saturating_sub(height as usize);
    session
        .feed
        .range(start..end)
        .map(|line| rendered_feed_line(session, line))
        .collect()
}

/// One feed line, its agent named the way a human knows it.
fn rendered_feed_line(session: &SessionState, line: &FeedLine) -> Line<'static> {
    Line::from(format!(
        "{:>5}  {:<11} {:<14} {}",
        line.sequence,
        line.label,
        clipped(&session.agent_name(&line.agent_id), 14),
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
        crate::state::short_id(&permission.agent_id)
    } else {
        permission.tree_path.join(" > ")
    };
    let mut text = match permission.questions.as_slice() {
        [] => format!("{who} wants `{}`", permission.rendered),
        [question, rest @ ..] => {
            let mut asked = format!("{who} asks: {}", question.question);
            for (index, option) in question.options.iter().enumerate() {
                asked.push_str(&format!("  [{}] {option}", index + 1));
            }
            if !rest.is_empty() {
                asked.push_str(&format!(
                    "  (+{} more; `horde answer {}`)",
                    rest.len(),
                    permission.request_id
                ));
            }
            asked
        }
    };
    if !permission.mode.is_empty() {
        text.push_str(&format!("  (mode {})", permission.mode));
    }
    text
}

fn session_footer(pending: &[PendingPermission]) -> String {
    let mut footer = String::new();
    match pending.first() {
        Some(front) if front.is_question() => footer.push_str("[1-9] answer  [n] deny  "),
        Some(_) => footer.push_str("[y] allow  [n] deny  "),
        None => {}
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
            questions: Vec::new(),
        }
    }

    /// The backing suffix's table. Asserted here beside the snapshots
    /// because the 80-column screen clips the unbacked row's tail — exactly
    /// where the detached backer's name lives, and the name is the remedy.
    #[test]
    fn the_backing_note_always_names_the_backer_it_has() {
        assert_eq!(
            super::backing_note(Some("laptop".to_owned()), true),
            "  backed by laptop"
        );
        assert_eq!(
            super::backing_note(Some("laptop".to_owned()), false),
            "  UNBACKED — laptop is detached"
        );
        assert_eq!(
            super::backing_note(None, false),
            "  UNBACKED — no backer recorded"
        );
        assert_eq!(
            super::backing_note(None, true),
            "  UNBACKED — no backer recorded",
            "a 'backed' inject with no recorded backer is a contradiction; \
             the row says the alarming half"
        );
    }

    /// Asserted here rather than left to the screen snapshots, because on an
    /// 80-column screen a long prompt pushes the suffix past the right edge —
    /// where every miscounted variant clips to the same pixels.
    /// The stream badge's truth table: ended outranks stale, either alone
    /// shows its own badge, and a healthy stream shows nothing.
    #[test]
    fn an_ended_stream_outranks_a_stale_one() {
        use super::{ENDED_BADGE, STALE_BADGE, stream_badge};
        assert_eq!(stream_badge(Some(""), true), ENDED_BADGE);
        assert_eq!(stream_badge(Some("why"), false), ENDED_BADGE);
        assert_eq!(stream_badge(None, true), STALE_BADGE);
        assert_eq!(stream_badge(None, false), "");
    }

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

    /// A question renders what it asks and its numbered options, the footer
    /// offers answers instead of allow, and a multi-question request points
    /// at `horde answer`.
    #[test]
    fn a_question_renders_its_options_and_the_answer_keys() {
        use crate::state::PendingQuestion;

        let mut asking = pending("q-1");
        asking.tree_path = vec!["root".to_owned()];
        asking.questions = vec![PendingQuestion {
            id: "colour".to_owned(),
            question: "Which colour?".to_owned(),
            options: vec!["Red".to_owned(), "Blue".to_owned()],
        }];
        assert_eq!(
            super::permission_summary(&asking),
            "root asks: Which colour?  [1] Red  [2] Blue"
        );
        assert!(
            super::session_footer(std::slice::from_ref(&asking))
                .starts_with("[1-9] answer  [n] deny")
        );
        assert!(super::session_footer(&[pending("p")]).starts_with("[y] allow  [n] deny"));
        assert!(super::session_footer(&[]).starts_with("[enter] send"));

        asking.questions.push(PendingQuestion {
            id: "size".to_owned(),
            question: "Size?".to_owned(),
            options: Vec::new(),
        });
        assert!(
            super::permission_summary(&asking).ends_with("(+1 more; `horde answer q-1`)"),
            "{}",
            super::permission_summary(&asking)
        );
    }
}
