//! The read-only panels: workspaces, usage, logs and help.
//!
//! Each tolerates missing data — an unasked server, a definition with no
//! instances, an instance with no repos — and says which it is, because a
//! blank panel cannot tell "nothing" from "not loaded".

use ratatui::text::{Line, Span};

use super::marker;
use crate::app::App;
use crate::keys::{self, Screen};
use crate::theme;
use crate::wrap::fit;

/// Workspace definitions, their instances per worker, and each instance's
/// checkouts.
pub(super) fn workspaces(app: &App) -> (Vec<Line<'static>>, &'static str) {
    let hints = "r refresh · esc back · q quit";
    let Some(entries) = app.workspaces() else {
        return (vec![Line::styled("  loading workspaces…", theme::dim())], hints);
    };
    if entries.is_empty() {
        return (
            vec![Line::styled(
                "  no workspaces defined — `horde workspace create <name> --repo <url>`",
                theme::dim(),
            )],
            hints,
        );
    }
    let selected = app.selected(Screen::Workspaces);
    let mut lines = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        workspace_lines(app, &mut lines, entry, index == selected);
    }
    (lines, hints)
}

fn workspace_lines(
    app: &App,
    lines: &mut Vec<Line<'static>>,
    entry: &crate::app::WorkspaceEntry,
    selected: bool,
) {
    let definition = &entry.definition;
    let repos: Vec<String> = definition
        .repos
        .iter()
        .map(|repo| {
            if repo.default_branch.is_empty() {
                repo.name.clone()
            } else {
                format!("{}@{}", repo.name, repo.default_branch)
            }
        })
        .collect();
    lines.push(Line::from(vec![
        marker(selected),
        Span::styled(definition.name.clone(), if selected { theme::selected() } else { theme::bold() }),
        Span::styled(format!("  {}", repos.join(", ")), theme::dim()),
    ]));
    if entry.instances.is_empty() {
        lines.push(Line::styled("      no instances on any worker yet", theme::dim()));
    }
    for instance in &entry.instances {
        instance_lines(app, lines, instance);
    }
}

fn instance_lines(app: &App, lines: &mut Vec<Line<'static>>, instance: &horde_proto::v1::WorkspaceInstanceReport) {
    let worker = instance
        .worker_id
        .as_ref()
        .map_or_else(|| "unknown worker".to_owned(), |id| app.worker_name(&id.to_string()));
    let state = instance_state_name(instance.state);
    let style = match state {
        "ready" => theme::ok(),
        "degraded" | "materializing" => theme::warn(),
        "missing" => theme::error(),
        _ => theme::dim(),
    };
    let mut spans = vec![
        Span::raw("    "),
        Span::styled(fit(&worker, 16), theme::plain()),
        Span::styled(format!(" {}", fit(state, 14)), style),
        Span::styled(format!(" {}", instance.root_path), theme::dim()),
    ];
    if !instance.detail.is_empty() {
        spans.push(Span::styled(format!("  {}", instance.detail), theme::warn()));
    }
    lines.push(Line::from(spans));
    for repo in &instance.repos {
        let sha: String = repo.head_commit_sha.chars().take(8).collect();
        let dirty = if repo.dirty { "  dirty" } else { "" };
        lines.push(Line::styled(
            format!("        {} {sha}{dirty}  {}", fit(&repo.repo_name, 14), repo.path),
            theme::dim(),
        ));
    }
}

fn instance_state_name(state: i32) -> &'static str {
    use horde_proto::v1::WorkspaceInstanceState as State;
    match State::try_from(state) {
        Ok(State::Materializing) => "materializing",
        Ok(State::Ready) => "ready",
        Ok(State::Degraded) => "degraded",
        Ok(State::Missing) => "missing",
        _ => "unknown",
    }
}

/// Token usage per agent, per tree path and per account. Display only —
/// the measured counts the log carries, and the provider's own window
/// statement beside them.
pub(super) fn usage(app: &App) -> (Vec<Line<'static>>, &'static str) {
    let hints = "r refresh · esc back · q quit";
    let Some(report) = app.usage() else {
        return (vec![Line::styled("  loading usage…", theme::dim())], hints);
    };
    let mut lines = vec![Line::styled(format!(" {}", report.scope), theme::dim())];
    section(&mut lines, "by agent", &report.by_agent, |key| {
        key.agent_id.as_ref().map_or_else(|| "—".to_owned(), |id| crate::state::short_id(&id.to_string()))
    });
    section(&mut lines, "by tree", &report.by_tree, |key| key.tree_path.join(" > "));
    lines.push(Line::raw(""));
    lines.push(Line::styled(" by account", theme::bold()));
    if report.accounts.is_empty() {
        lines.push(Line::styled("   no usage recorded", theme::dim()));
    }
    for account in &report.accounts {
        lines.push(Line::raw(format!(
            "   {} {}{}",
            fit(&format!("{}/{}", account.provider, account.account), 28),
            tokens(account.tokens.as_ref()),
            window(account),
        )));
    }
    (lines, hints)
}

fn section(
    lines: &mut Vec<Line<'static>>,
    title: &str,
    rows: &[horde_proto::v1::UsageAggregate],
    label: fn(&horde_proto::v1::UsageGroupKey) -> String,
) {
    lines.push(Line::raw(""));
    lines.push(Line::styled(format!(" {title}"), theme::bold()));
    if rows.is_empty() {
        lines.push(Line::styled("   no usage recorded", theme::dim()));
    }
    for row in rows {
        let key = row.key.clone().unwrap_or_default();
        lines.push(Line::raw(format!(
            "   {} {}",
            fit(&label(&key), 28),
            tokens(row.tokens.as_ref())
        )));
    }
}

fn tokens(tokens: Option<&horde_proto::v1::TokenUsage>) -> String {
    let tokens = tokens.cloned().unwrap_or_default();
    format!(
        "in {:>9}  out {:>9}  cache {:>9}",
        tokens.input_tokens,
        tokens.output_tokens,
        tokens.cache_read_input_tokens + tokens.cache_write_input_tokens
    )
}

/// The account's window as the provider stated it, when it stated one.
fn window(account: &horde_proto::v1::AccountHeadroom) -> String {
    let remaining = account
        .remaining_fraction
        .map(|fraction| format!("  {:.0}% left", fraction * 100.0))
        .unwrap_or_default();
    let exhausted = if account.exhausted { "  EXHAUSTED" } else { "" };
    format!("{remaining}{exhausted}")
}

/// The logs pane: `horde logs` (W12h) is not on this server's surface yet,
/// so the pane says so rather than inventing an RPC.
pub(super) fn logs() -> (Vec<Line<'static>>, &'static str) {
    (
        vec![
            Line::styled(" Logs are not available from this build.", theme::bold()),
            Line::raw(""),
            Line::styled(
                "  Worker and server logs arrive with `horde logs` (wave 12, W12h); this",
                theme::dim(),
            ),
            Line::styled(
                "  pane will follow them once the server offers the RPC.",
                theme::dim(),
            ),
        ],
        "esc back · q quit",
    )
}

/// Keys and commands, printed from the same tables the dispatcher uses.
pub(super) fn help() -> (Vec<Line<'static>>, &'static str) {
    let mut lines = vec![Line::styled(" Session (compose box)", theme::bold())];
    bindings(&mut lines, keys::COMPOSE);
    lines.push(Line::styled(" Prompt card", theme::bold()));
    bindings(&mut lines, keys::PROMPT);
    lines.push(Line::styled(" Everywhere", theme::bold()));
    bindings(&mut lines, keys::GLOBAL);
    lines.push(Line::styled(" Lists", theme::bold()));
    bindings(&mut lines, keys::LIST);
    lines.push(Line::styled(" Commands", theme::bold()));
    for command in crate::slash::COMMANDS {
        lines.push(Line::raw(format!(
            "   /{} {}  {}",
            command.name,
            fit(command.args, 16),
            command.help
        )));
    }
    (lines, "j/k scroll · esc back · q quit")
}

fn bindings(lines: &mut Vec<Line<'static>>, table: &[(ratatui::crossterm::event::KeyCode, ratatui::crossterm::event::KeyModifiers, keys::Intent)]) {
    for (code, modifiers, intent) in table {
        lines.push(Line::raw(format!(
            "   {} {}",
            fit(&keys::key_name(*code, *modifiers), 14),
            intent_words(*intent)
        )));
    }
}

/// What each non-navigation intent does, in the help screen's words.
const INTENT_WORDS: &[(keys::Intent, &str)] = {
    use keys::{Edit, Intent};
    &[
        (Intent::Cancel, "clear the compose box, or quit"),
        (Intent::Quit, "quit"),
        (Intent::ToggleDetails, "expand or collapse tool detail"),
        (Intent::Interrupt, "interrupt the agent's turn"),
        (Intent::SendNow, "send now (interrupts the turn)"),
        (Intent::Submit, "send, or run a /command"),
        (Intent::Newline, "new line"),
        (Intent::Escape, "interrupt a working agent · close · back"),
        (Intent::Tab, "complete a command · switch prompt/compose"),
        (Intent::Edit(Edit::DeleteWord), "delete a word"),
        (Intent::Edit(Edit::DeleteToLineStart), "delete to line start"),
        (Intent::Edit(Edit::Up), "move up · older message"),
        (Intent::Edit(Edit::Down), "move down · newer message"),
        (Intent::Edit(Edit::Home), "line start"),
        (Intent::Edit(Edit::End), "line end"),
        (Intent::ScrollUp, "scroll the transcript back"),
        (Intent::ScrollDown, "scroll the transcript forward"),
        (Intent::ScrollEnd, "back to the latest"),
        (Intent::Up, "move the selection"),
        (Intent::Down, "move the selection"),
        (Intent::Activate, "attach · open"),
        (Intent::Refresh, "refresh"),
        (Intent::NewSession, "new session"),
        (Intent::Archive, "archive the session (asks first)"),
        (Intent::Attach, "go to the agent"),
    ]
};

/// What an intent does, in the help screen's words.
fn intent_words(intent: keys::Intent) -> String {
    if let keys::Intent::Goto(screen) = intent {
        return format!("open {}", screen_name(screen));
    }
    if let keys::Intent::Prompt(_) = intent {
        return "choose · answer".to_owned();
    }
    INTENT_WORDS
        .iter()
        .find(|(known, _)| *known == intent)
        .map_or("edit", |(_, words)| words)
        .to_owned()
}

fn screen_name(screen: Screen) -> &'static str {
    match screen {
        Screen::Session => "the session",
        Screen::Overview => "agents",
        Screen::Inbox => "the inbox",
        Screen::Workspaces => "workspaces",
        Screen::Usage => "usage",
        Screen::Logs => "logs",
        Screen::Forwards => "forwards",
        Screen::Help => "help",
    }
}
