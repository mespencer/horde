//! Bare `horde` is the front door (ADR-0008).
//!
//! With no subcommand: no live sessions means create one, auto-named, and
//! attach; exactly one means attach it; several mean the picker, newest
//! first, with "new session" as its top row. `horde new [title]` is the
//! explicit spelling of the same motion. Pickers exist only where a human
//! does — stdin a TTY, `--json` absent — and a headless invocation that
//! would have prompted fails fast with the candidate list in the error.

use horde_proto::v1::{AgentId, AgentSpec, SessionSummary};

use crate::picker::{Choice, Picker};
use crate::{Client, ClientError, Format, render, resolve};

/// What a bare invocation does, given what is live.
///
/// Pure, so the three-way rule is a truth table a test can hold whole; the
/// I/O around each arm lives in [`enter`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    /// No live sessions: create one, auto-named, and attach.
    Create,
    /// Exactly one: attach it, no picker.
    Attach(String),
    /// Several: the human picks.
    Pick,
}

/// The arm `sessions` selects. Assumes the list is already newest first.
#[must_use]
pub fn entry_for(sessions: &[SessionSummary]) -> Entry {
    match sessions {
        [] => Entry::Create,
        [only] => Entry::Attach(id_of(only)),
        _ => Entry::Pick,
    }
}

/// Runs the bare invocation: the front door itself.
///
/// # Errors
///
/// Headless (the fast candidate-list refusal), the server refusing any step,
/// or the terminal failing under the picker or the attach TUI.
pub async fn enter(client: &mut Client, interactive: bool) -> Result<(), ClientError> {
    let sessions = live_newest_first(client).await?;
    if !interactive {
        return Err(ClientError::Headless {
            candidates: render::session_rows(&sessions, render::now_unix_ms()),
        });
    }
    match entry_for(&sessions) {
        Entry::Create => create_and_attach(client).await,
        Entry::Attach(session_id) => attach_if_still_live(client, &session_id).await,
        Entry::Pick => {
            // Restored (cursor included) before any error is reported, same
            // as `tui::launch`: a refusal printed into raw mode is unreadable.
            let (terminal, _restore) = crate::tui::RawTerminal::engage()?;
            let input = crate::tui::spawn_input_reader();
            pick_and_enter(client, terminal, input, &sessions).await
        }
    }
}

/// Drives the picker and then the chosen session's view on one terminal
/// session and one input reader. One reader, deliberately: the reader thread
/// eats the event that ends it, so a second reader spawned for the attach
/// view would swallow the human's first keystroke after choosing.
async fn pick_and_enter(
    client: &mut Client,
    terminal: ratatui::DefaultTerminal,
    mut input: tokio::sync::mpsc::Receiver<ratatui::crossterm::event::Event>,
    sessions: &[SessionSummary],
) -> Result<(), ClientError> {
    let picker = Picker::new(sessions, render::now_unix_ms());
    let (terminal, choice) = crate::picker::drive(terminal, &mut input, picker, |_| {}).await?;
    let session_id = match choice {
        Choice::Dismissed => return Ok(()),
        Choice::NewSession => {
            let (summary, _root) = create(client, None, AgentSpec::default(), "").await?;
            id_of(&summary)
        }
        Choice::Attach(session_id) => {
            refuse_archived(client, &session_id).await?;
            session_id
        }
    };
    let app = horde_tui::App::attached(session_id, None);
    crate::tui::drive(terminal, input, client.clone(), app, |_| {})
        .await
        .map(drop)
}

/// Runs `horde new [title]`: create, then attach — create only with
/// `--detach`, which is the scripting form.
///
/// # Errors
///
/// No terminal to attach in (refused before anything is created, so a
/// headless mistake has no side effect), or any step failing.
pub async fn new_session(
    client: &mut Client,
    title: Option<&str>,
    spec: AgentSpec,
    detach: bool,
    format: Format,
    interactive: bool,
) -> Result<(), ClientError> {
    if !detach && !interactive {
        return Err(ClientError::Terminal(std::io::Error::other(
            "no terminal to attach in — `horde new --detach` creates without attaching",
        )));
    }
    let (summary, root) = create(client, title, spec, "").await?;
    if detach {
        println!("{}", render::created(&summary, &root, format));
        return Ok(());
    }
    attach(client, &id_of(&summary)).await
}

/// Creates a session titled `title`, or auto-named when the title is
/// omitted: a thematic draw, collision-checked against the live sessions
/// only (ADR-0008 — archived names are free to come around again).
///
/// # Errors
///
/// Listing or creating failing, or every name being taken.
pub async fn create(
    client: &mut Client,
    title: Option<&str>,
    spec: AgentSpec,
    idempotency_key: &str,
) -> Result<(SessionSummary, AgentId), ClientError> {
    let title = match title {
        Some(title) => title.to_owned(),
        None => {
            let live = client.list_sessions(false).await?;
            let taken: Vec<String> = live.iter().map(resolve::name_of).collect();
            crate::names::generate(crate::names::entropy_seed(), &taken)
                .ok_or(ClientError::NamesExhausted)?
        }
    };
    client
        .create_session_with(&title, spec, idempotency_key)
        .await
}

/// The live sessions, newest first — the order the picker shows and the
/// single-session shortcut assumes.
async fn live_newest_first(client: &mut Client) -> Result<Vec<SessionSummary>, ClientError> {
    let mut sessions = client.list_sessions(false).await?;
    sort_newest_first(&mut sessions);
    Ok(sessions)
}

/// Newest first by creation time. Two creates can land in one millisecond,
/// so the id breaks ties — determinism beats a coin flip when the picker's
/// row order is what Enter acts on. Summaries the server never stamped sink
/// to the tail.
pub fn sort_newest_first(sessions: &mut [SessionSummary]) {
    sessions.sort_by_key(|summary| std::cmp::Reverse((created_ms(summary), id_of(summary))));
}

fn created_ms(summary: &SessionSummary) -> i64 {
    summary.created_at.as_ref().map_or(i64::MIN, |at| {
        at.seconds
            .saturating_mul(1_000)
            .saturating_add(i64::from(at.nanos) / 1_000_000)
    })
}

async fn create_and_attach(client: &mut Client) -> Result<(), ClientError> {
    let (summary, _root) = create(client, None, AgentSpec::default(), "").await?;
    attach(client, &id_of(&summary)).await
}

/// Attaches, unless the session was archived while the human chose — the
/// race gets a fast named refusal rather than a TUI over a dead session.
async fn attach_if_still_live(client: &mut Client, session_id: &str) -> Result<(), ClientError> {
    refuse_archived(client, session_id).await?;
    attach(client, session_id).await
}

/// The archived-while-you-chose check, shared by both attach routes.
async fn refuse_archived(client: &mut Client, session_id: &str) -> Result<(), ClientError> {
    let summary = client.get_session(session_id).await?;
    if summary.state == horde_proto::v1::SessionLifecycleState::Archived as i32 {
        let name = resolve::name_of(&summary);
        return Err(ClientError::ArchivedWhileChoosing {
            reference: if name.is_empty() {
                session_id.to_owned()
            } else {
                name
            },
        });
    }
    Ok(())
}

async fn attach(client: &mut Client, session_id: &str) -> Result<(), ClientError> {
    crate::tui::launch(
        client.clone(),
        crate::tui::Launch::Attach {
            session_id: session_id.to_owned(),
            agent_id: None,
        },
    )
    .await
}

fn id_of(summary: &SessionSummary) -> String {
    summary
        .session_id
        .as_ref()
        .map_or_else(String::new, ToString::to_string)
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{SessionId, SessionSummary};

    use super::{Entry, entry_for, sort_newest_first};

    fn session(id: &str, created_ms: i64) -> SessionSummary {
        SessionSummary {
            session_id: Some(SessionId::new(id)),
            created_at: Some(horde_proto::prost_types::Timestamp {
                seconds: created_ms.div_euclid(1_000),
                nanos: i32::try_from(created_ms.rem_euclid(1_000)).unwrap_or(0) * 1_000_000,
            }),
            ..SessionSummary::default()
        }
    }

    /// The three-way rule, whole: none creates, one attaches (that one),
    /// several pick.
    #[test]
    fn zero_creates_one_attaches_and_several_pick() {
        assert_eq!(entry_for(&[]), Entry::Create);
        assert_eq!(
            entry_for(&[session("session-1", 5)]),
            Entry::Attach("session-1".to_owned())
        );
        assert_eq!(
            entry_for(&[session("session-1", 5), session("session-2", 9)]),
            Entry::Pick
        );
    }

    /// Sorts and hands back the ids in their new order — both sort tests
    /// read the same fact, so the extraction lives once.
    fn sorted_ids(mut sessions: Vec<SessionSummary>) -> Vec<String> {
        sort_newest_first(&mut sessions);
        sessions
            .iter()
            .filter_map(|summary| summary.session_id.as_ref().map(ToString::to_string))
            .collect()
    }

    /// Newest first is by creation time, not list order — and a summary
    /// with no stamp sinks to the tail instead of poisoning the sort. The
    /// stamps differ only inside one second, so the nanosecond half must be
    /// *divided* into milliseconds: multiplied through, nanos out-weigh the
    /// seconds; reduced modulo, one-millisecond neighbors (1.999s vs
    /// 1.998s) collapse to a tie and the id — the wrong key — decides.
    #[test]
    fn sessions_sort_newest_first_by_creation_time() {
        let unstamped = SessionSummary {
            session_id: Some(SessionId::new("session-unstamped")),
            ..SessionSummary::default()
        };
        assert_eq!(
            sorted_ids(vec![
                unstamped,
                session("session-old", 1_900),
                session("session-b", 1_998),
                session("session-a", 1_999),
                session("session-new", 2_000),
            ]),
            vec![
                "session-new",
                "session-a",
                "session-b",
                "session-old",
                "session-unstamped"
            ],
            "newest first, unstamped last"
        );
    }

    /// The archived-while-you-chose race, against a real server: a live
    /// session passes the check, and the same session archived is refused
    /// *by name* — an explained refusal beats a TUI over a dead session.
    #[tokio::test]
    async fn the_archived_while_you_chose_race_is_refused_by_name() {
        let (_core, mut client) = crate::forward::testing::served().await;
        let (summary, _root) = client
            .create_session("Done Work", "key-1")
            .await
            .expect("a session creates");
        let id = summary.session_id.map(|id| id.to_string()).expect("an id");

        super::refuse_archived(&mut client, &id)
            .await
            .expect("a live session attaches");

        client
            .archive_session(&id, "wrapped up")
            .await
            .expect("it archives");
        let refused = super::refuse_archived(&mut client, &id)
            .await
            .expect_err("the race gets the fast refusal");
        assert!(
            refused.to_string().contains("done-work"),
            "the refusal names the session: {refused}"
        );
    }

    /// Two creates in one millisecond still order deterministically — the
    /// id breaks the tie, so the picker's rows cannot shuffle between the
    /// paint and the Enter.
    #[test]
    fn a_creation_time_tie_breaks_on_the_id_deterministically() {
        assert_eq!(
            sorted_ids(vec![
                session("session-key-1", 5_000),
                session("session-key-2", 5_000),
            ]),
            vec!["session-key-2", "session-key-1"]
        );
    }
}
