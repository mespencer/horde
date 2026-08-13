//! The interactive loop: a thin pump between the terminal, the server, and
//! [`horde_tui::App`].
//!
//! Everything that *decides* lives in `horde-tui`, where a test can reach it
//! without a TTY. What lives here is only plumbing, in two layers with a seam
//! between them:
//!
//! * [`drive`] is generic over the terminal backend and takes its input as a
//!   channel, so the whole loop — streams in, directives out, redraw each
//!   turn — runs in tests against a real server with a `TestBackend` and
//!   scripted keys.
//! * [`launch`] is the raw-terminal shell around it: enable the real
//!   terminal, spawn the blocking input reader, restore on the way out. It is
//!   exercised by the PTY test in `tests/tui.rs`, because a real terminal is
//!   the one thing a `TestBackend` cannot vouch for.
//!
//! The TUI speaks the same wire the CLI speaks — every directive lands on a
//! [`Client`] verb that also exists as `horde <verb>` (DESIGN.md §11: CLI
//! parity is a hard rule; the TUI is optional).

use horde_proto::v1::DeliveryMode;
use horde_tui::{App, Directive, Update};
use ratatui::Terminal;
use ratatui::backend::Backend;
use ratatui::crossterm::event::Event as TermEvent;
use tokio::sync::mpsc;

use crate::client::{PermissionAnswer, StreamUpdate};
use crate::{Client, ClientError};

/// What the user asked to open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Launch {
    /// `horde` — the multi-session overview.
    Overview,
    /// `horde attach <session> [--agent <id>]` — one session, optionally
    /// scoped to one agent's subtree.
    Attach {
        session_id: String,
        agent_id: Option<String>,
    },
}

/// Runs the TUI on the real terminal until the user quits.
///
/// # Errors
///
/// Anything the first refresh refuses (an unknown session on attach fails
/// here, before the terminal is touched), terminal setup failing, or the
/// event loop dying mid-flight. Terminal state is restored on every path out.
pub async fn launch(mut client: Client, launch: Launch) -> Result<(), ClientError> {
    let app = app_for(&mut client, launch).await?;
    let terminal = ratatui::try_init().map_err(ClientError::Terminal)?;
    let input = spawn_input_reader();
    let outcome = drive(terminal, input, client, app, |_| {}).await;
    // Restored before the error is reported: a panic message or an error line
    // printed into raw mode is unreadable, which is nearly as bad as losing it.
    ratatui::restore();
    outcome.map(|_| ())
}

/// Builds the starting [`App`], validating an attach target against the
/// server first — "session not found" belongs on stderr as an error, not
/// painted into an empty TUI.
async fn app_for(client: &mut Client, launch: Launch) -> Result<App, ClientError> {
    match launch {
        Launch::Overview => Ok(App::overview()),
        Launch::Attach {
            session_id,
            agent_id,
        } => {
            client.get_session(&session_id).await?;
            Ok(App::attached(session_id, agent_id))
        }
    }
}

/// Forwards blocking terminal reads onto a channel the async loop can select
/// on. The thread ends when the receiver is dropped and the next event
/// arrives; a leftover blocked read at process exit is harmless.
fn spawn_input_reader() -> mpsc::Receiver<TermEvent> {
    let (sender, receiver) = mpsc::channel(64);
    std::thread::spawn(move || forward_terminal_input(&sender));
    receiver
}

fn forward_terminal_input(sender: &mpsc::Sender<TermEvent>) {
    while let Ok(event) = ratatui::crossterm::event::read() {
        if sender.blocking_send(event).is_err() {
            return;
        }
    }
}

/// The event loop, generic over where the pixels go and where the keys come
/// from.
///
/// Owns everything and hands it back so a test can inspect the final screen
/// and state after the loop exits.
///
/// # Errors
///
/// A failed draw or a failed initial refresh. Directive failures do *not*
/// end the loop — they land in the App's status line, because with several
/// clients attached someone else answering first is an ordinary Tuesday,
/// not a reason to lose the screen.
pub async fn drive<B>(
    mut terminal: Terminal<B>,
    mut input: mpsc::Receiver<TermEvent>,
    mut client: Client,
    mut app: App,
    mut observe: impl FnMut(&App),
) -> Result<(Terminal<B>, App), ClientError>
where
    B: Backend,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let (updates, mut inbox) = mpsc::channel::<Update>(256);

    let sessions = refresh_sessions(&mut client, &mut app).await?;

    // One merged stream serves the whole TUI. This used to be a fan-in of
    // one stream per session; the server grew `all_sessions`, so it
    // collapsed.
    let attachment = app
        .attachment()
        .map(|attachment| attachment.session_id.clone());
    let cursors = overview_cursors(&sessions, attachment.as_deref());
    tokio::spawn(pump(client.clone(), cursors, updates));

    while !app.should_quit() {
        terminal
            .draw(|frame| horde_tui::view::render(&app, frame))
            .map_err(|error| ClientError::Terminal(std::io::Error::other(error)))?;
        observe(&app);

        tokio::select! {
            event = input.recv() => match event {
                Some(event) => {
                    let directives = app.on_input(&event);
                    perform(directives, &mut client, &mut app).await;
                }
                // The terminal went away under us; there is no one left to draw for.
                None => app.request_quit(),
            },
            update = inbox.recv() => {
                if let Some(update) = update {
                    app.apply(update);
                }
                // `None` cannot happen while `streams` holds a sender; if it
                // ever does, the next input still exits cleanly.
            }
        }
    }

    Ok((terminal, app))
}

/// Executes what the App asked for, reporting refusals into its status line.
async fn perform(directives: Vec<Directive>, client: &mut Client, app: &mut App) {
    for directive in directives {
        if let Err(error) = execute(directive, client, app).await {
            app.report_error(error.to_string());
        }
    }
}

/// One directive, one client verb — the same verbs the CLI exposes.
async fn execute(
    directive: Directive,
    client: &mut Client,
    app: &mut App,
) -> Result<(), ClientError> {
    match directive {
        Directive::RefreshSessions => refresh_sessions(client, app).await.map(drop),
        Directive::Approve { request_id } => client
            .answer_permission(PermissionAnswer::allow(&request_id))
            .await
            .map(drop),
        Directive::Deny { request_id } => client
            .answer_permission(PermissionAnswer::deny(&request_id, "denied from the TUI"))
            .await
            .map(drop),
        Directive::SendAssignment { agent_id, text } => client
            .send_assignment(&agent_id, &text, DeliveryMode::TurnBoundary, "")
            .await
            .map(drop),
        Directive::Interrupt { agent_id } => client
            .interrupt(&agent_id, "interrupted from the TUI")
            .await
            .map(drop),
    }
}

/// Lists sessions and folds the answer in, returning the summaries so the
/// startup path can name its stream cursors. No stream management happens
/// here: the one merged stream already carries every session, listed or not.
async fn refresh_sessions(
    client: &mut Client,
    app: &mut App,
) -> Result<Vec<horde_proto::v1::SessionSummary>, ClientError> {
    let sessions = client.list_sessions(false).await?;
    app.apply(Update::Sessions(sessions.clone()));
    Ok(sessions)
}

/// The cursors the TUI's one merged stream opens with: every listed session
/// replayed from its start, plus the attach target when the list omits it —
/// an archived session is attachable but unlisted. The server's all-sessions
/// net carries everything born later, so nothing else needs naming.
///
/// Extracted as a pure function because the deduplication condition inside
/// `drive` is a compound predicate in an event loop — unreachable as a truth
/// table there, ordinary here (docs/QUALITY_GATES.md).
fn overview_cursors(
    sessions: &[horde_proto::v1::SessionSummary],
    attachment: Option<&str>,
) -> Vec<(String, u64)> {
    let mut cursors: Vec<(String, u64)> = sessions
        .iter()
        .filter_map(|summary| summary.session_id.as_ref().map(ToString::to_string))
        .map(|session_id| (session_id, 0))
        .collect();
    if let Some(target) = attachment
        && !cursors.iter().any(|(session_id, _)| session_id == target)
    {
        cursors.push((target.to_owned(), 0));
    }
    cursors
}

/// The TUI's one stream: replays the named sessions from their cursors,
/// follows everything else live, and forwards it all onto the shared
/// channel. Ends silently when the receiver goes away (the loop exited) or
/// the stream does (the server went away — the heartbeat gap will show soon
/// enough).
async fn pump(mut client: Client, cursors: Vec<(String, u64)>, updates: mpsc::Sender<Update>) {
    let _ = follow_into(&mut client, cursors, &updates).await;
}

async fn follow_into(
    client: &mut Client,
    cursors: Vec<(String, u64)>,
    updates: &mpsc::Sender<Update>,
) -> Result<(), ClientError> {
    let mut stream = client.follow_everything(&cursors).await?;
    while let Some(update) = stream.next_update().await? {
        let update = match update {
            StreamUpdate::Event(envelope) => Update::Event(envelope),
            StreamUpdate::CaughtUp { session_id, head } => Update::CaughtUp { session_id, head },
        };
        if updates.send(update).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{SessionId, SessionSummary};

    use super::overview_cursors;

    fn listed(session_id: &str) -> SessionSummary {
        SessionSummary {
            session_id: Some(SessionId::new(session_id)),
            ..SessionSummary::default()
        }
    }

    /// The truth table the drive loop cannot exercise: an attach target
    /// already in the list must not be named twice (a duplicate cursor
    /// replays the session's history twice into the feed), and one the list
    /// omits — archived, say — is still owed its replay.
    #[test]
    fn the_attach_target_is_named_exactly_once() {
        let sessions = [listed("s1"), listed("s2")];

        assert_eq!(
            overview_cursors(&sessions, Some("s2")),
            vec![("s1".to_owned(), 0), ("s2".to_owned(), 0)],
            "a listed attach target must not become a duplicate cursor"
        );
        assert_eq!(
            overview_cursors(&sessions, Some("archived-one")),
            vec![
                ("s1".to_owned(), 0),
                ("s2".to_owned(), 0),
                ("archived-one".to_owned(), 0),
            ],
            "an unlisted attach target is still owed its replay"
        );
        assert_eq!(
            overview_cursors(&sessions, None),
            vec![("s1".to_owned(), 0), ("s2".to_owned(), 0)],
            "no attachment adds nothing"
        );
    }

    /// A summary with no id is unaddressable and yields no cursor — same
    /// rule the App applies when folding summaries.
    #[test]
    fn a_summary_without_an_id_yields_no_cursor() {
        let cursors = overview_cursors(&[SessionSummary::default()], Some("s9"));
        assert_eq!(cursors, vec![("s9".to_owned(), 0)]);
    }
}
