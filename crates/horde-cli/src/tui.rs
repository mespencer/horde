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
    // Restored before the error is reported — the guard drops at the end of
    // this function, before the caller prints anything: a panic message or an
    // error line printed into raw mode is unreadable, which is nearly as bad
    // as losing it.
    let (terminal, _restore) = RawTerminal::engage()?;
    let input = spawn_input_reader();
    drive(terminal, input, client, app, |_| {}).await.map(drop)
}

/// Ownership of the real terminal's raw mode: engaging enters raw mode and
/// the alternate screen; dropping restores both *and* shows the cursor.
///
/// A guard rather than a call at the end of each happy path, so every way
/// out — an early `?`, an error from the loop, a panic unwinding through —
/// restores the same way. `ratatui::restore` alone leaves the cursor hidden
/// (ratatui shows it only from `Terminal`'s own drop, which a leaked or
/// moved terminal may never reach), and stops at the first failing step;
/// here each step is attempted regardless of the others.
pub(crate) struct RawTerminal(());

impl RawTerminal {
    /// Enters raw mode and the alternate screen.
    ///
    /// # Errors
    ///
    /// [`ClientError::Terminal`] when the terminal refuses either.
    pub(crate) fn engage() -> Result<(ratatui::DefaultTerminal, Self), ClientError> {
        // The guard exists before the fallible init, so a half-done init
        // (raw mode on, alternate screen refused) is still undone.
        let guard = Self(());
        let terminal = ratatui::try_init().map_err(ClientError::Terminal)?;
        Ok((terminal, guard))
    }
}

impl Drop for RawTerminal {
    fn drop(&mut self) {
        use ratatui::crossterm::{cursor, execute, terminal};
        // Best effort, every step: a failure to leave raw mode must not also
        // cost the user their cursor.
        let _ = terminal::disable_raw_mode();
        let _ = execute!(std::io::stdout(), terminal::LeaveAlternateScreen);
        let _ = execute!(std::io::stdout(), cursor::Show);
    }
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
/// arrives; a leftover blocked read at process exit is harmless. Shared with
/// the front door's picker, which is the same shell around a smaller loop.
pub(crate) fn spawn_input_reader() -> mpsc::Receiver<TermEvent> {
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

    // One stream serves the whole TUI. An attach scoped to an agent's
    // subtree scopes it server-side (`subtree_root`, fix a14) — the server
    // has honored the field all along; the TUI simply never sent it. An
    // unscoped launch keeps the merged all-sessions stream.
    let subscription = subscription_for(app.attachment(), &sessions);
    tokio::spawn(pump(client.clone(), subscription, updates));

    // The staleness clock (fix a9): a dead stream produces no updates, so
    // only a tick can notice its silence — and redraw the badge in.
    let mut ticker = tokio::time::interval(TICK_PERIOD);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Once the pump hangs up the channel answers `None` instantly, forever;
    // polling it then would spin the loop redrawing at full speed.
    let mut stream_open = true;

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
            update = inbox.recv(), if stream_open => match update {
                Some(update) => drain_into(&mut app, update, &mut inbox),
                // The pump already said why in its `StreamEnded`; the
                // channel closing is only its sender going away.
                None => stream_open = false,
            },
            _ = ticker.tick() => {
                app.apply(Update::Tick { now: std::time::Instant::now() });
            }
        }
    }

    Ok((terminal, app))
}

/// Folds `first` and then everything already queued behind it before the
/// next redraw. One redraw per update would make a replay of a few thousand
/// events cost a few thousand frames, and the screen would crawl through
/// history the human only wants the end of.
///
/// Bounded by [`DRAIN_CEILING`] all the same: the pump refills the channel
/// while this drains it, and a firehose must not starve keys and redraws.
fn drain_into(app: &mut App, first: Update, inbox: &mut mpsc::Receiver<Update>) {
    app.apply(first);
    for _ in 1..DRAIN_CEILING {
        match inbox.try_recv() {
            Ok(update) => app.apply(update),
            Err(_) => return,
        }
    }
}

/// The most updates one frame folds. Comfortably above the channel's
/// capacity, so a full channel drains in one frame.
const DRAIN_CEILING: usize = 1_024;

/// How often the staleness clock ticks. A third of the badge threshold: the
/// badge may appear up to one tick late, and ticking faster buys nothing.
const TICK_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);

/// What the TUI subscribes to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Subscription {
    /// The merged all-sessions stream, with these replay cursors.
    Everything(Vec<(String, u64)>),
    /// One session, scoped server-side to one agent's subtree.
    Subtree {
        session_id: String,
        agent_id: String,
    },
}

/// The subscription a launch shape calls for: `--agent` scopes the stream
/// server-side to that agent's subtree; a session attach replays only the
/// target session; the overview replays everything listed.
///
/// An attach still follows every session — the all-sessions net carries the
/// unnamed ones live from their current heads — so detaching to the
/// overview shows the rest moving. What it no longer does is replay every
/// other session's whole history into memory before the one the human
/// asked for: that cost grew with the server's lifetime, not with the
/// attach.
fn subscription_for(
    attachment: Option<&horde_tui::app::Attachment>,
    sessions: &[horde_proto::v1::SessionSummary],
) -> Subscription {
    match attachment {
        Some(horde_tui::app::Attachment {
            session_id,
            agent_id: Some(agent_id),
        }) => Subscription::Subtree {
            session_id: session_id.clone(),
            agent_id: agent_id.clone(),
        },
        Some(attachment) => Subscription::Everything(vec![(attachment.session_id.clone(), 0)]),
        None => Subscription::Everything(overview_cursors(sessions)),
    }
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
        Directive::RefreshForwards => {
            let forwards = client.list_forwards(None).await?;
            app.apply(Update::Forwards(forwards));
            Ok(())
        }
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

/// The cursors the overview's merged stream opens with: every listed session
/// replayed from its start. The server's all-sessions net carries everything
/// born later, so nothing else needs naming.
fn overview_cursors(sessions: &[horde_proto::v1::SessionSummary]) -> Vec<(String, u64)> {
    sessions
        .iter()
        .filter_map(|summary| summary.session_id.as_ref().map(ToString::to_string))
        .map(|session_id| (session_id, 0))
        .collect()
}

/// The TUI's one stream: opens the subscription and forwards it onto the
/// shared channel — heartbeats included, because they are what the
/// staleness clock judges against. When the stream ends — the server hung
/// up, or refused the subscription — it says so with a final
/// [`Update::StreamEnded`] rather than letting the feed freeze silently. A
/// send that fails means the loop already exited, and there is no one to tell.
async fn pump(mut client: Client, subscription: Subscription, updates: mpsc::Sender<Update>) {
    if let Some(ended) = ended_update(follow_into(&mut client, subscription, &updates).await) {
        let _ = updates.send(ended).await;
    }
}

/// What the end of [`follow_into`] tells the App: `None` when the loop hung
/// up first (nothing to tell), otherwise why the stream closed — empty for a
/// clean end-of-stream, the refusal's words for an error.
fn ended_update(outcome: Result<FollowEnd, ClientError>) -> Option<Update> {
    match outcome {
        Ok(FollowEnd::LoopGone) => None,
        Ok(FollowEnd::StreamClosed) => Some(Update::StreamEnded {
            reason: String::new(),
        }),
        Err(error) => Some(Update::StreamEnded {
            reason: error.to_string(),
        }),
    }
}

/// How [`follow_into`] stopped without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowEnd {
    /// The receiving loop is gone; nobody is listening.
    LoopGone,
    /// The server ended the stream.
    StreamClosed,
}

async fn follow_into(
    client: &mut Client,
    subscription: Subscription,
    updates: &mpsc::Sender<Update>,
) -> Result<FollowEnd, ClientError> {
    let mut stream = match subscription {
        Subscription::Everything(cursors) => client.follow_everything(&cursors).await?,
        Subscription::Subtree {
            session_id,
            agent_id,
        } => {
            client
                .follow(
                    &crate::client::SyncOptions::new(session_id)
                        .following(true)
                        .scoped_to(agent_id),
                )
                .await?
        }
    };
    while let Some(update) = stream.next_update().await? {
        let update = match update {
            StreamUpdate::Event(envelope) => Update::Event(envelope),
            StreamUpdate::CaughtUp { session_id, head } => Update::CaughtUp { session_id, head },
            StreamUpdate::Heartbeat => Update::Heartbeat,
        };
        if updates.send(update).await.is_err() {
            return Ok(FollowEnd::LoopGone);
        }
    }
    Ok(FollowEnd::StreamClosed)
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{EventEnvelope, SessionId, SessionSummary};
    use horde_tui::app::Attachment;
    use horde_tui::{App, Update};

    use super::{
        DRAIN_CEILING, FollowEnd, Subscription, drain_into, ended_update, subscription_for,
    };
    use crate::ClientError;

    fn listed(session_id: &str) -> SessionSummary {
        SessionSummary {
            session_id: Some(SessionId::new(session_id)),
            ..SessionSummary::default()
        }
    }

    fn attachment(session_id: &str, agent_id: Option<&str>) -> Attachment {
        Attachment {
            session_id: session_id.to_owned(),
            agent_id: agent_id.map(ToOwned::to_owned),
        }
    }

    /// The overview replays every listed session (an id-less summary is
    /// unaddressable and yields no cursor); a session attach replays only
    /// its target — listed or not, archived ones are attachable but
    /// unlisted — and an agent attach scopes to that agent's subtree.
    #[test]
    fn each_launch_shape_replays_only_what_it_shows() {
        let sessions = [listed("s1"), SessionSummary::default(), listed("s2")];

        assert_eq!(
            subscription_for(None, &sessions),
            Subscription::Everything(vec![("s1".to_owned(), 0), ("s2".to_owned(), 0)]),
        );
        assert_eq!(
            subscription_for(Some(&attachment("s2", None)), &sessions),
            Subscription::Everything(vec![("s2".to_owned(), 0)]),
            "attaching must not replay the other sessions' histories"
        );
        assert_eq!(
            subscription_for(Some(&attachment("archived-one", None)), &sessions),
            Subscription::Everything(vec![("archived-one".to_owned(), 0)]),
            "an unlisted target is still owed its replay"
        );
        assert_eq!(
            subscription_for(Some(&attachment("s1", Some("agent-7"))), &sessions),
            Subscription::Subtree {
                session_id: "s1".to_owned(),
                agent_id: "agent-7".to_owned(),
            },
        );
    }

    fn event(sequence: u64) -> Update {
        Update::Event(Box::new(EventEnvelope {
            sequence,
            session_id: Some(SessionId::new("s1")),
            ..EventEnvelope::default()
        }))
    }

    fn feed_len(app: &App) -> usize {
        app.session("s1").map_or(0, |session| session.feed.len())
    }

    /// One frame folds everything already queued, not one update per
    /// redraw — but never more than the ceiling, so a firehose cannot
    /// starve the keys. What the ceiling leaves stays queued for the next
    /// frame, in order.
    #[tokio::test]
    async fn a_frame_drains_the_queue_up_to_the_ceiling() {
        let (sender, mut inbox) = tokio::sync::mpsc::channel(DRAIN_CEILING * 2);
        for sequence in 2..=4 {
            sender
                .send(event(sequence))
                .await
                .expect("the channel has room");
        }
        let mut app = App::attached("s1", None);
        drain_into(&mut app, event(1), &mut inbox);
        assert_eq!(feed_len(&app), 4, "the first update and all three queued");
        assert!(inbox.try_recv().is_err(), "nothing is left behind");

        let backlog = DRAIN_CEILING as u64 + 5;
        for sequence in 10..10 + backlog {
            sender
                .send(event(sequence))
                .await
                .expect("the channel has room");
        }
        let mut app = App::attached("s1", None);
        drain_into(&mut app, event(9), &mut inbox);
        assert_eq!(feed_len(&app), DRAIN_CEILING, "one frame folds the ceiling");
        let next = inbox
            .try_recv()
            .expect("the remainder waits for the next frame");
        assert_eq!(next, event(10 + DRAIN_CEILING as u64 - 1));
    }

    /// The end of the stream reaches the App as words: nothing when the loop
    /// is already gone, an empty reason for a clean close, the error's own
    /// text for a refusal.
    #[test]
    fn a_closed_stream_tells_the_app_why() {
        assert_eq!(ended_update(Ok(FollowEnd::LoopGone)), None);
        assert_eq!(
            ended_update(Ok(FollowEnd::StreamClosed)),
            Some(Update::StreamEnded {
                reason: String::new()
            })
        );
        let refusal = ClientError::Terminal(std::io::Error::other("server went away"));
        let words = refusal.to_string();
        assert_eq!(
            ended_update(Err(refusal)),
            Some(Update::StreamEnded { reason: words })
        );
    }
}
