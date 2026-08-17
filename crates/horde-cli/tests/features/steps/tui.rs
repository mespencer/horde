//! Attaching: the real TUI event loop (`horde_cli::tui::drive`) on a test
//! terminal, against the scenario's real server. Keys go down a channel,
//! every loop turn reports the [`App`], and the final frame is what the
//! screen actually held — the same seam the interactive binary runs behind.

use cucumber::{given, then, when};
use horde_proto::v1::{AgentId, AgentOutputChannel, AgentOutputChunk, Event, event::Kind};
use horde_tui::{App, Screen};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{Event as TermEvent, KeyCode, KeyEvent};

use crate::world::{HordeWorld, StepResult, Tui, within};

/// An output chunk journaled the way a Worker journals one — the control
/// plane cannot author agent output (DESIGN.md §5), so history a scenario
/// needs on the log before attaching enters through the worker spine.
#[given(expr = "the agent has already said {string}")]
async fn the_agent_has_already_said(world: &mut HordeWorld, text: String) -> StepResult {
    let event = Event {
        agent_id: Some(AgentId::new(world.root()?)),
        occurred_at: None,
        idempotency_key: "said-1".to_owned(),
        kind: Some(Kind::AgentOutputChunk(AgentOutputChunk {
            channel: AgentOutputChannel::Assistant as i32,
            text,
            turn_id: "turn-1".to_owned(),
            chunk_index: 0,
        })),
    };
    horde_server_testkit::journal_event(world.endpoint()?, world.session()?, event).await?;
    Ok(())
}

#[when("the user attaches to the session")]
async fn the_user_attaches(world: &mut HordeWorld) -> StepResult {
    let client = horde_cli::Client::connect(world.endpoint()?).await?;
    let app = App::attached(world.session()?.to_owned(), None);
    let terminal = Terminal::new(TestBackend::new(80, 24))?;
    let (keys, key_receiver) = tokio::sync::mpsc::channel(16);
    let (report, probes) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(horde_cli::tui::drive(
        terminal,
        key_receiver,
        client,
        app,
        move |app| {
            let _ = report.send(app.clone());
        },
    ));
    let mut tui = Tui {
        keys,
        probes,
        last: None,
        task: Some(task),
    };

    let session = world.session()?.to_owned();
    observed(
        &mut tui,
        "the attach to catch up on the replay",
        move |app| app.session(&session).is_some_and(|view| view.caught_up),
    )
    .await?;
    world.tui = Some(tui);
    Ok(())
}

#[then(expr = "the attached view shows {string}")]
async fn the_attached_view_shows(world: &mut HordeWorld, needle: String) -> StepResult {
    let session = world.session()?.to_owned();
    let tui = world.tui.as_mut().ok_or("nothing is attached")?;
    observed(tui, "the feed to carry the agent's words", move |app| {
        app.session(&session)
            .is_some_and(|view| view.feed.iter().any(|line| line.text.contains(&needle)))
    })
    .await
}

#[when("the user detaches")]
async fn the_user_detaches(world: &mut HordeWorld) -> StepResult {
    let tui = world.tui.as_mut().ok_or("nothing is attached")?;
    press(tui, KeyCode::Esc).await?;
    observed(tui, "the overview to return", |app| {
        app.screen() == Screen::Overview
    })
    .await
}

/// Quits from the overview and reads the *final frame* — the pixels, not the
/// state — asserting the session is listed on it.
#[then(expr = "the overview lists {string}")]
async fn the_overview_lists(world: &mut HordeWorld, title: String) -> StepResult {
    let mut tui = world.tui.take().ok_or("nothing is attached")?;
    press(&mut tui, KeyCode::Char('q')).await?;
    tui.probes.close();
    let task = tui.task.take().ok_or("the loop was already joined")?;
    let (terminal, _) = within("the loop to exit", async { Ok(task.await??) }).await?;
    let screen = terminal.backend().to_string();
    if !screen.contains(&title) {
        return Err(format!("the last frame does not list {title:?}: {screen}").into());
    }
    Ok(())
}

async fn press(tui: &mut Tui, code: KeyCode) -> StepResult {
    Ok(tui
        .keys
        .send(TermEvent::Key(KeyEvent::from(code)))
        .await
        .map_err(|_| "the loop hung up its key channel")?)
}

/// The first [`App`] state — the already-seen one, or the next reported —
/// accepted by `accept`, under the ceiling.
async fn observed(
    tui: &mut Tui,
    what: &str,
    mut accept: impl FnMut(&App) -> bool + Send,
) -> StepResult {
    if tui.last.as_ref().is_some_and(&mut accept) {
        return Ok(());
    }
    let (probes, last) = (&mut tui.probes, &mut tui.last);
    within(what, async {
        loop {
            match probes.recv().await {
                Some(app) => {
                    let accepted = accept(&app);
                    *last = Some(app);
                    if accepted {
                        return Ok(());
                    }
                }
                None => return Err(format!("the loop exited while waiting for {what}").into()),
            }
        }
    })
    .await
}
