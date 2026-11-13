//! The TUI event loop against a real server.
//!
//! [`horde_cli::tui::drive`] is the whole interactive loop minus the raw
//! terminal: pixels go to a ratatui `TestBackend`, keys come from a channel,
//! and everything else — the client, the server, the follow streams — is the
//! real thing on a loopback socket. The `observe` seam reports the [`App`]
//! after every loop turn, which is what lets a test wait for a *state* (the
//! escalation arrived, the decision came back) instead of sleeping and
//! hoping; every wait is bounded by a ceiling and asserts on what arrived.
//!
//! The raw-terminal shell around this loop is exercised separately, by the
//! pty suite in `tui_pty.rs`.

use std::future::Future;
use std::time::Duration;

use crate::common::{Setup, escalation, journal_event, seeded, serve};
use horde_cli::tui::drive;
use horde_cli::{Client, ClientError};
use horde_tui::{App, Screen};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{Event as TermEvent, KeyCode, KeyEvent, KeyModifiers};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A running `drive` loop and the levers a test holds over it.
struct Harness {
    keys: mpsc::Sender<TermEvent>,
    probes: mpsc::UnboundedReceiver<App>,
    loop_task: JoinHandle<Result<(Terminal<TestBackend>, App), ClientError>>,
}

/// Spawns the loop on an 80×24 test terminal.
async fn start(endpoint: &str, app: App) -> Setup<Harness> {
    let client = Client::connect(endpoint).await?;
    let terminal = Terminal::new(TestBackend::new(80, 24))?;
    let (keys, key_receiver) = mpsc::channel(16);
    let (probe_sender, probes) = mpsc::unbounded_channel();
    let loop_task = tokio::spawn(drive(terminal, key_receiver, client, app, move |app| {
        let _ = probe_sender.send(app.clone());
    }));
    Ok(Harness {
        keys,
        probes,
        loop_task,
    })
}

/// Every wait in this file goes through here: a ceiling, and the reason the
/// test was waiting when it fires.
async fn within<T>(what: &str, future: impl Future<Output = Setup<T>>) -> Setup<T> {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .map_err(|_| format!("timed out waiting for {what}"))?
}

impl Harness {
    async fn press(&self, code: KeyCode) -> Setup<()> {
        self.keys
            .send(TermEvent::Key(KeyEvent::from(code)))
            .await
            .map_err(|_| "the loop hung up its key channel")?;
        Ok(())
    }

    async fn press_ctrl(&self, c: char) -> Setup<()> {
        self.keys
            .send(TermEvent::Key(KeyEvent::new(
                KeyCode::Char(c),
                KeyModifiers::CONTROL,
            )))
            .await
            .map_err(|_| "the loop hung up its key channel")?;
        Ok(())
    }

    async fn type_line(&self, text: &str) -> Setup<()> {
        for c in text.chars() {
            self.press(KeyCode::Char(c)).await?;
        }
        Ok(())
    }

    /// The next observed state satisfying `accept` — how a test waits for a
    /// server round trip without racing it.
    async fn state(&mut self, what: &str, mut accept: impl FnMut(&App) -> bool) -> Setup<App> {
        within(what, async {
            loop {
                match self.probes.recv().await {
                    Some(app) if accept(&app) => return Ok(app),
                    Some(_) => {}
                    None => return Err(format!("the loop exited while waiting for {what}").into()),
                }
            }
        })
        .await
    }

    /// Quits and hands back the final screen and state.
    async fn quit_from_list_screen(mut self) -> Setup<(Terminal<TestBackend>, App)> {
        self.press(KeyCode::Char('q')).await?;
        self.probes.close();
        let outcome = within("the loop to exit", async { Ok(self.loop_task.await) }).await??;
        Ok(outcome?)
    }
}

/// Whether this session's feed carries a line with this label whose text
/// contains the needle — the shape every round-trip assertion here takes.
fn feed_has(app: &App, session_id: &str, label: &str, needle: &str) -> bool {
    app.session(session_id).is_some_and(|session| {
        session
            .feed
            .iter()
            .any(|line| line.label == label && line.text.contains(needle))
    })
}

#[tokio::test]
async fn the_overview_shows_a_live_session_and_quits_cleanly() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = seeded(&endpoint, "tui-overview")
        .await
        .expect("a seeded session");

    let mut harness = start(&endpoint, App::overview())
        .await
        .expect("a running loop");
    harness
        .state("the session to appear and catch up", |app| {
            app.session(&session_id)
                .is_some_and(|session| session.caught_up && session.title == "tui-overview")
        })
        .await
        .expect("the overview fills from the real list and stream");

    let (terminal, app) = harness
        .quit_from_list_screen()
        .await
        .expect("q exits the loop");
    assert!(app.should_quit());
    let screen = terminal.backend().to_string();
    assert!(
        screen.contains("tui-overview")
            && screen.contains(&horde_tui::state::short_id(&session_id)),
        "the last drawn frame must show the session: {screen}"
    );
}

/// Seeds a session, journals one escalation into it the way a worker would,
/// and hands back a loop already sitting in the inbox looking at it — the
/// shared preamble of the approve and deny round trips.
async fn inbox_with_one_escalation(title: &str, request_id: &str) -> Setup<(String, Harness)> {
    let endpoint = serve().await?;
    let (_, session_id, root) = seeded(&endpoint, title).await?;
    journal_event(
        &endpoint,
        &session_id,
        escalation(&root, request_id, "docker rm x", Vec::new()),
    )
    .await?;

    let mut harness = start(&endpoint, App::overview()).await?;
    harness
        .state("the escalation to arrive", |app| app.waiting_total() == 1)
        .await?;
    harness.press(KeyCode::Char('i')).await?;
    Ok((session_id, harness))
}

#[tokio::test]
async fn an_escalation_reaches_the_inbox_and_y_approves_it_on_the_server() {
    let (session_id, mut harness) = inbox_with_one_escalation("tui-inbox", "req-tui-1")
        .await
        .expect("an inbox holding one journaled escalation");
    harness.press(KeyCode::Char('y')).await.expect("approve it");

    // The pending set only empties when the server's PermissionDecided event
    // comes back down the follow stream: this is the round trip, not an
    // optimistic local removal.
    let app = harness
        .state("the decision to round-trip", |app| app.waiting_total() == 0)
        .await
        .expect("an approval must clear the inbox via the log");
    assert!(
        feed_has(&app, &session_id, "decided", "allowed"),
        "the decision event itself must land in the session feed"
    );

    let (terminal, _) = harness.quit_from_list_screen().await.expect("q exits");
    assert!(
        terminal.backend().to_string().contains("nothing needs you"),
        "an emptied inbox says so"
    );
}

#[tokio::test]
async fn n_denies_with_the_tui_reason() {
    let (session_id, mut harness) = inbox_with_one_escalation("tui-deny", "req-tui-2")
        .await
        .expect("an inbox holding one journaled escalation");
    harness.press(KeyCode::Char('n')).await.expect("deny it");

    let app = harness
        .state("the denial to round-trip", |app| app.waiting_total() == 0)
        .await
        .expect("a denial must clear the inbox via the log");
    assert!(
        feed_has(&app, &session_id, "decided", "denied: denied from the TUI"),
        "the denial must carry the TUI's reason back through the server"
    );
    harness.quit_from_list_screen().await.expect("q exits");
}

#[tokio::test]
async fn composing_in_an_attached_session_sends_a_real_assignment() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = seeded(&endpoint, "tui-send")
        .await
        .expect("a seeded session");

    let mut harness = start(&endpoint, App::attached(session_id.clone(), None))
        .await
        .expect("a running loop");
    harness
        .state("the replay to teach the app its root agent", |app| {
            app.steer_target().is_some()
        })
        .await
        .expect("the root agent arrives via the session's own AgentSpawned event");

    harness
        .type_line("fix the flaky test")
        .await
        .expect("compose");
    harness.press(KeyCode::Enter).await.expect("send");

    harness
        .state("the assignment to round-trip", |app| {
            feed_has(app, &session_id, "assign", "fix the flaky test")
        })
        .await
        .expect("the send must come back as an AssignmentSent event");

    // Ctrl-T leaves the transcript for the session/agent tree, then q.
    harness.press_ctrl('t').await.expect("to the tree");
    harness
        .state("the overview to return", |app| {
            app.screen() == Screen::Overview
        })
        .await
        .expect("ctrl-t opens the tree");
    harness.quit_from_list_screen().await.expect("q exits");
}

#[tokio::test]
async fn ctrl_x_interrupts_the_attached_agent_for_real() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = seeded(&endpoint, "tui-interrupt")
        .await
        .expect("a seeded session");

    let mut harness = start(&endpoint, App::attached(session_id.clone(), None))
        .await
        .expect("a running loop");
    harness
        .state("the root agent to be known", |app| {
            app.steer_target().is_some()
        })
        .await
        .expect("the root arrives via replay");

    harness.press_ctrl('x').await.expect("interrupt");
    harness
        .state("the interrupt to round-trip", |app| {
            feed_has(app, &session_id, "interrupt", "interrupted from the TUI")
        })
        .await
        .expect("the interrupt must come back as a TurnInterrupted event");

    harness.press_ctrl('t').await.expect("to the tree");
    harness.quit_from_list_screen().await.expect("q exits");
}

/// A refusal lands in the status line and the TUI keeps running — with
/// several clients attached, losing the screen to a race would be absurd.
#[tokio::test]
async fn a_server_refusal_shows_in_the_status_line_instead_of_killing_the_tui() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = seeded(&endpoint, "tui-refusal")
        .await
        .expect("a seeded session");

    let ghost = "agent-that-was-never-spawned";
    let mut harness = start(
        &endpoint,
        App::attached(session_id.clone(), Some(ghost.to_owned())),
    )
    .await
    .expect("a running loop");
    harness
        .state("the replay to catch up", |app| {
            app.session(&session_id)
                .is_some_and(|session| session.caught_up)
        })
        .await
        .expect("attach replays first");

    harness.type_line("hello?").await.expect("compose");
    harness
        .press(KeyCode::Enter)
        .await
        .expect("send to a ghost");

    let app = harness
        .state("the refusal to surface", |app| !app.status().is_empty())
        .await
        .expect("the server's refusal must land in the status line");
    assert!(
        app.status().contains(ghost),
        "the status must name what was refused: {:?}",
        app.status()
    );
    assert!(
        !app.should_quit(),
        "a refusal must not end the session view"
    );

    harness.press_ctrl('t').await.expect("to the tree");
    harness.quit_from_list_screen().await.expect("q exits");
}

/// Two sessions, "the-target" and "the-other": a client, both session ids,
/// and the neighbour's root agent.
async fn target_and_neighbour(endpoint: &str) -> Setup<(Client, String, String, String)> {
    let (mut client, target, _) = seeded(endpoint, "the-target").await?;
    let (neighbour, other_root) = client.create_session("the-other", "key-2").await?;
    let other = neighbour
        .session_id
        .map(|id| id.to_string())
        .ok_or("a created session has an id")?;
    Ok((client, target, other, other_root.to_string()))
}

/// A loop attached to `target` whose replay has caught up and carried
/// something.
async fn attached_and_replayed(endpoint: &str, target: &str) -> Setup<Harness> {
    let mut harness = start(endpoint, App::attached(target.to_owned(), None)).await?;
    harness
        .state("the target's replay to catch up", |app| {
            app.session(target)
                .is_some_and(|session| session.caught_up && !session.feed.is_empty())
        })
        .await?;
    Ok(harness)
}

/// Attaching replays the target session alone: another session's history
/// stays on the server, while its *live* traffic still arrives — the
/// all-sessions net — so detaching shows it moving. The live event is the
/// proof the stream reached that session at all, and because one stream
/// delivers in order, any replay of the other's past would have landed
/// before it.
#[tokio::test]
async fn attaching_replays_only_the_target_while_the_rest_stays_live() {
    let endpoint = serve().await.expect("a listening server");
    let (mut client, target, other, other_root) =
        target_and_neighbour(&endpoint).await.expect("two sessions");

    let mut harness = attached_and_replayed(&endpoint, &target)
        .await
        .expect("the target replays");

    client
        .send_assignment(
            &other_root,
            "live next door",
            horde_proto::v1::DeliveryMode::TurnBoundary,
            "",
        )
        .await
        .expect("a live event in the other session");
    let app = harness
        .state("the neighbour's live event", |app| {
            feed_has(app, &other, "assign", "live next door")
        })
        .await
        .expect("the rest of the server stays live");
    let neighbour = app.session(&other).expect("the neighbour is known");
    assert!(
        neighbour.feed.iter().all(|line| line.label != "spawned"),
        "the neighbour's history must not have been replayed: {:?}",
        neighbour.feed
    );

    harness.press_ctrl('t').await.expect("to the tree");
    harness.quit_from_list_screen().await.expect("q exits");
}

/// The picker's loop through the same seam: a `TestBackend`, scripted keys,
/// rows listed from a real server. What the screen shows must be exactly the
/// rows `session list` prints — ADR-0008's "one rendering" is an equality,
/// not a resemblance — and the scripted walk must land on the session the
/// cursor pointed at.
#[tokio::test]
async fn the_picker_paints_the_shared_rows_and_enter_chooses_the_pointed_session() {
    use horde_cli::picker::{Choice, Picker};

    let endpoint = serve().await.expect("a listening server");
    let (mut client, first_id, _) = seeded(&endpoint, "pick-one").await.expect("seed");
    client
        .create_session("pick-two", "key-2")
        .await
        .expect("a second session");

    let mut sessions = client.list_sessions(false).await.expect("the list");
    horde_cli::front_door::sort_newest_first(&mut sessions);
    let now = 1_000_000_i64;
    let rows = horde_cli::render::session_rows(&sessions, now);

    let terminal =
        Terminal::new(TestBackend::new(80, 24)).expect("a test terminal always constructs");
    let (keys, mut key_receiver) = mpsc::channel(16);
    for code in [KeyCode::Down, KeyCode::Down, KeyCode::Enter] {
        keys.send(TermEvent::Key(KeyEvent::from(code)))
            .await
            .expect("the loop is listening");
    }
    // Closing the channel bounds the loop by the script's own length: a
    // picker deaf to its keys drains them, meets the closed channel, and
    // returns Dismissed — failing the choice assertion at once instead of
    // spending the wall-clock ceiling.
    drop(keys);

    let (terminal, choice) = within("the picker loop to finish", async {
        Ok(horde_cli::picker::drive(
            terminal,
            &mut key_receiver,
            Picker::new(&sessions, now),
            |_| {},
        )
        .await?)
    })
    .await
    .expect("the loop finishes");

    assert_eq!(
        choice,
        Choice::Attach(first_id),
        "two downs from the new-session row is the second (older) session"
    );

    let screen = terminal.backend().to_string();
    assert!(
        screen.contains("new session"),
        "the top row is the create: {screen}"
    );
    for row in &rows {
        assert!(
            screen.contains(row.trim_end()),
            "the picker must paint the same row `session list` prints:\n{row}\n--- screen:\n{screen}"
        );
    }
}

/// `/new <title>` from the compose box creates a session on the server and
/// attaches the view to it.
#[tokio::test]
async fn slash_new_creates_a_session_and_attaches_to_it() {
    let endpoint = serve().await.expect("a listening server");
    let (mut client, first, _) = seeded(&endpoint, "tui-before-new")
        .await
        .expect("a seeded session");

    let mut harness = start(&endpoint, App::attached(first.clone(), None))
        .await
        .expect("a running loop");
    harness
        .type_line("/new fresh-start")
        .await
        .expect("compose");
    harness.press(KeyCode::Enter).await.expect("run it");

    let app = harness
        .state("the view to attach to the new session", |app| {
            app.attachment()
                .is_some_and(|attachment| attachment.session_id != first)
        })
        .await
        .expect("the created session becomes the attachment");
    let created = app
        .attachment()
        .map(|attachment| attachment.session_id.clone())
        .expect("attached");
    let sessions = client.list_sessions(false).await.expect("the list");
    assert!(
        sessions.iter().any(|summary| summary.title == "fresh-start"
            && summary.session_id.as_ref().map(ToString::to_string) == Some(created.clone())),
        "the server holds the session the view attached to: {sessions:?}"
    );

    harness.press_ctrl('t').await.expect("to the tree");
    harness.quit_from_list_screen().await.expect("q exits");
}

/// An attach replays only its target, but opening the tree widens the
/// replay: the loop resubscribes and the neighbour's history arrives.
#[tokio::test]
async fn opening_the_tree_after_an_attach_replays_every_session() {
    let endpoint = serve().await.expect("a listening server");
    let (_, target, other, _) = target_and_neighbour(&endpoint).await.expect("two sessions");

    let mut harness = attached_and_replayed(&endpoint, &target)
        .await
        .expect("the target replays");

    harness.press_ctrl('t').await.expect("to the tree");
    harness
        .state("the neighbour's history", |app| {
            app.session(&other)
                .is_some_and(|session| session.feed.iter().any(|line| line.label == "spawned"))
        })
        .await
        .expect("the tree must replay the sessions it lists");
    harness.quit_from_list_screen().await.expect("q exits");
}

/// The workspaces and usage views ask the real server and show its answer:
/// the one definition it holds, and usage it has none of.
#[tokio::test]
async fn the_workspaces_and_usage_views_fill_from_the_server() {
    let endpoint = serve().await.expect("a listening server");
    seeded(&endpoint, "tui-views")
        .await
        .expect("a seeded session");

    let mut client = Client::connect(&endpoint).await.expect("a client");
    client
        .create_workspace_definition(
            "tui-ws",
            vec![horde_proto::v1::WorkspaceRepo {
                name: "horde".to_owned(),
                remote_url: "https://example.invalid/horde.git".to_owned(),
                default_branch: "main".to_owned(),
                layout_path: "horde".to_owned(),
            }],
            Vec::new(),
        )
        .await
        .expect("a workspace definition");

    let mut harness = start(&endpoint, App::overview())
        .await
        .expect("a running loop");
    harness.press(KeyCode::Char('w')).await.expect("workspaces");
    let app = harness
        .state("the workspaces answer", |app| app.workspaces().is_some())
        .await
        .expect("the workspaces view lists definitions from the server");
    assert_eq!(app.screen(), Screen::Workspaces);
    let names: Vec<&str> = app
        .workspaces()
        .unwrap_or_default()
        .iter()
        .map(|entry| entry.definition.name.as_str())
        .collect();
    assert_eq!(names, ["tui-ws"], "the server's definition is listed");

    harness.press(KeyCode::Char('u')).await.expect("usage");
    let app = harness
        .state("the usage answer", |app| app.usage().is_some())
        .await
        .expect("the usage view queries the server");
    assert_eq!(app.screen(), Screen::Usage);
    assert_eq!(
        app.usage().map(|usage| usage.scope.as_str()),
        Some("every session"),
        "with nothing attached, usage covers every session"
    );

    harness.press(KeyCode::Char('l')).await.expect("logs");
    harness
        .state("the logs placeholder", |app| app.screen() == Screen::Logs)
        .await
        .expect("the logs view opens");
    harness.quit_from_list_screen().await.expect("q exits");
}

/// `x` on a tree row asks first; `y` archives the session on the server.
#[tokio::test]
async fn x_then_y_archives_the_selected_session() {
    let endpoint = serve().await.expect("a listening server");
    let (mut client, session_id, _) = seeded(&endpoint, "tui-archive")
        .await
        .expect("a seeded session");

    let mut harness = start(&endpoint, App::overview())
        .await
        .expect("a running loop");
    harness
        .state("the session to be listed", |app| {
            app.session(&session_id).is_some()
        })
        .await
        .expect("listed");
    harness.press(KeyCode::Char('x')).await.expect("ask");
    harness
        .state("the confirmation", |app| app.confirm().is_some())
        .await
        .expect("archive asks first");
    harness.press(KeyCode::Char('y')).await.expect("confirm");
    // The loop performs a key's directives before its next redraw, so the
    // first frame without the question is drawn after the archive returned.
    harness
        .state("the confirmation to close", |app| app.confirm().is_none())
        .await
        .expect("answered");
    let listed = client.list_sessions(false).await.expect("the list");
    assert!(
        listed.is_empty(),
        "the session must leave the live list: {listed:?}"
    );
    harness.quit_from_list_screen().await.expect("q exits");
}

/// `/note <text>` sends a note, not an assignment, to the attached agent.
#[tokio::test]
async fn slash_note_sends_a_real_note() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = seeded(&endpoint, "tui-note")
        .await
        .expect("a seeded session");
    let mut harness = start(&endpoint, App::attached(session_id.clone(), None))
        .await
        .expect("a running loop");
    harness
        .state("the root agent to be known", |app| {
            app.steer_target().is_some()
        })
        .await
        .expect("the root arrives via replay");

    harness.type_line("/note heads up").await.expect("compose");
    harness.press(KeyCode::Enter).await.expect("run it");
    let app = harness
        .state("the note to round-trip", |app| {
            feed_has(app, &session_id, "note", "heads up")
        })
        .await
        .expect("the note must come back as a NoteSent event");
    assert!(
        !feed_has(&app, &session_id, "assign", "heads up"),
        "a note is not an assignment"
    );
    harness.press_ctrl('t').await.expect("to the tree");
    harness.quit_from_list_screen().await.expect("q exits");
}

/// Ctrl-S sends the compose text with interrupt delivery: it reaches the
/// agent as an assignment at once rather than queueing.
#[tokio::test]
async fn ctrl_s_sends_now() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = seeded(&endpoint, "tui-now")
        .await
        .expect("a seeded session");
    let mut harness = start(&endpoint, App::attached(session_id.clone(), None))
        .await
        .expect("a running loop");
    harness
        .state("the root agent to be known", |app| {
            app.steer_target().is_some()
        })
        .await
        .expect("the root arrives via replay");

    harness
        .type_line("stop and do this")
        .await
        .expect("compose");
    harness.press_ctrl('s').await.expect("send now");
    harness
        .state("the urgent assignment to round-trip", |app| {
            feed_has(app, &session_id, "assign", "stop and do this")
        })
        .await
        .expect("ctrl-s must send for real");
    harness.press_ctrl('t').await.expect("to the tree");
    harness.quit_from_list_screen().await.expect("q exits");
}
