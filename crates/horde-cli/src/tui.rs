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

use std::collections::HashSet;

use horde_proto::v1::DeliveryMode;
use horde_tui::{App, Directive, Update};
use ratatui::Terminal;
use ratatui::backend::Backend;
use ratatui::crossterm::event::Event as TermEvent;
use tokio::sync::mpsc;

use crate::client::{PermissionAnswer, StreamUpdate, SyncOptions};
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
    let mut streams = StreamSet::new(updates);

    refresh_sessions(&mut client, &mut app, &mut streams).await?;
    // The attach target may be archived and so missing from the list; its
    // stream is owed either way.
    if let Some(attachment) = app.attachment() {
        streams.ensure(&client, &attachment.session_id.clone());
    }

    while !app.should_quit() {
        terminal
            .draw(|frame| horde_tui::view::render(&app, frame))
            .map_err(|error| ClientError::Terminal(std::io::Error::other(error)))?;
        observe(&app);

        tokio::select! {
            event = input.recv() => match event {
                Some(event) => {
                    let directives = app.on_input(&event);
                    perform(directives, &mut client, &mut app, &mut streams).await;
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
async fn perform(
    directives: Vec<Directive>,
    client: &mut Client,
    app: &mut App,
    streams: &mut StreamSet,
) {
    for directive in directives {
        if let Err(error) = execute(directive, client, app, streams).await {
            app.report_error(error.to_string());
        }
    }
}

/// One directive, one client verb — the same verbs the CLI exposes.
async fn execute(
    directive: Directive,
    client: &mut Client,
    app: &mut App,
    streams: &mut StreamSet,
) -> Result<(), ClientError> {
    match directive {
        Directive::RefreshSessions => refresh_sessions(client, app, streams).await,
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

/// Lists sessions, folds the answer in, and makes sure each listed session
/// has a live follow stream.
async fn refresh_sessions(
    client: &mut Client,
    app: &mut App,
    streams: &mut StreamSet,
) -> Result<(), ClientError> {
    let sessions = client.list_sessions(false).await?;
    for summary in &sessions {
        if let Some(session_id) = summary.session_id.as_ref() {
            streams.ensure(client, &session_id.to_string());
        }
    }
    app.apply(Update::Sessions(sessions));
    Ok(())
}

/// One follow stream per session, opened once.
///
/// The server serves one session per sync call for now, so the overview and
/// the inbox are fan-in: every session's stream feeds the same channel and
/// the App does the merging. When the server grows `all_sessions`, this set
/// collapses to one stream without the App noticing.
struct StreamSet {
    opened: HashSet<String>,
    updates: mpsc::Sender<Update>,
}

impl StreamSet {
    fn new(updates: mpsc::Sender<Update>) -> Self {
        Self {
            opened: HashSet::new(),
            updates,
        }
    }

    /// Opens a follow stream for the session unless one is already running.
    fn ensure(&mut self, client: &Client, session_id: &str) {
        if self.opened.insert(session_id.to_owned()) {
            tokio::spawn(pump(
                client.clone(),
                session_id.to_owned(),
                self.updates.clone(),
            ));
        }
    }
}

/// Replays one session from the beginning and follows it, forwarding
/// everything onto the shared channel. Ends silently when the receiver goes
/// away (the loop exited) or the stream does (the server went away — the
/// heartbeat gap will show soon enough).
async fn pump(mut client: Client, session_id: String, updates: mpsc::Sender<Update>) {
    let _ = follow_into(&mut client, &session_id, &updates).await;
}

async fn follow_into(
    client: &mut Client,
    session_id: &str,
    updates: &mpsc::Sender<Update>,
) -> Result<(), ClientError> {
    let mut stream = client
        .follow(&SyncOptions::new(session_id).following(true))
        .await?;
    while let Some(update) = stream.next_update().await? {
        let update = match update {
            StreamUpdate::Event(envelope) => Update::Event(envelope),
            StreamUpdate::CaughtUp(head) => Update::CaughtUp {
                session_id: session_id.to_owned(),
                head,
            },
        };
        if updates.send(update).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}
