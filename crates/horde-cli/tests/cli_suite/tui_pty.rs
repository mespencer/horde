//! The real `horde` TUI on a real terminal.
//!
//! `tui_drive.rs` proves the event loop; what it cannot vouch for is the
//! raw-terminal shell around it — `try_init`'s raw mode and alternate
//! screen, the blocking input-reader thread, `restore` on the way out —
//! because those need a TTY. A pty is the one terminal a test can own, so
//! this suite spawns the actual binary (`CARGO_BIN_EXE_horde`, the one cargo
//! just built) inside one, reads what it paints, types at it, and watches it
//! exit.
//!
//! Every read is a positive wait with a ceiling, asserting on the bytes that
//! arrived; a child that outlives its ceiling is killed, not leaked — the
//! shared [`PtyProcess`] driver's discipline.

use crate::common::{Setup, seeded, serve};
use horde_server_testkit::pty::PtyProcess;

/// Spawns the freshly built binary in an 80×24 pty.
fn spawn_horde(endpoint: &str, args: &[&str]) -> Setup<PtyProcess> {
    let mut command = portable_pty::CommandBuilder::new(env!("CARGO_BIN_EXE_horde"));
    command.arg("--endpoint");
    command.arg(endpoint);
    for arg in args {
        command.arg(arg);
    }
    PtyProcess::spawn(command)
}

/// ADR-0008's front door, arm two: exactly one live session skips the picker
/// and attaches — bare `horde` puts the human in front of that session.
#[tokio::test]
async fn bare_horde_with_one_session_attaches_it_directly() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = seeded(&endpoint, "tui-pty-only")
        .await
        .expect("a seeded session");

    let mut horde = spawn_horde(&endpoint, &[]).expect("the binary spawns in a pty");
    // Needles are single words: the terminal skips blank cells with cursor
    // moves, so a phrase never paints as one contiguous byte run.
    horde
        .await_painted("attached:")
        .await
        .expect("one live session means the session view, no picker");
    horde
        .await_painted("tui-pty-only")
        .await
        .expect("the attached session is the seeded one");
    assert!(
        horde.seen.contains(&session_id),
        "the header names the session that was entered"
    );

    horde.type_bytes(&[0x03]).expect("ctrl-c reaches the pty");
    let status = horde.exit_status().await.expect("the TUI exits");
    assert!(
        status.success(),
        "ctrl-c must be a clean exit, got {status:?}"
    );
}

/// Arm one: no live sessions means create one, auto-named, and attach. The
/// name is checked against the word list through a client, because the
/// binary owns its own entropy.
#[tokio::test]
async fn bare_horde_with_no_sessions_creates_an_auto_named_session_and_attaches() {
    let endpoint = serve().await.expect("a listening server");

    let mut horde = spawn_horde(&endpoint, &[]).expect("the binary spawns in a pty");
    horde
        .await_painted("attached:")
        .await
        .expect("an empty server still puts the human in front of a session");
    horde.type_bytes(&[0x03]).expect("ctrl-c reaches the pty");
    let status = horde.exit_status().await.expect("the TUI exits");
    assert!(status.success(), "got {status:?}");

    let mut client = horde_cli::Client::connect(&endpoint)
        .await
        .expect("a client dials");
    let sessions = client.list_sessions(false).await.expect("the list");
    assert_eq!(sessions.len(), 1, "the front door created exactly one");
    let (adjective, noun) = sessions[0]
        .title
        .split_once('-')
        .expect("an auto-name is adjective-noun");
    assert!(
        horde_cli::names::ADJECTIVES.contains(&adjective)
            && horde_cli::names::NOUNS.contains(&noun),
        "the drawn name comes from the fixed word list: {:?}",
        sessions[0].title
    );
}

/// Arm three: several live sessions open the picker — rendered rows on a
/// real terminal — and Enter on a session row attaches it.
#[tokio::test]
async fn bare_horde_with_several_sessions_opens_the_picker_and_enter_attaches() {
    let endpoint = serve().await.expect("a listening server");
    let (mut client, _, _) = seeded(&endpoint, "pick-one")
        .await
        .expect("a seeded session");
    client
        .create_session("pick-two", "key-2")
        .await
        .expect("a second session");

    let mut horde = spawn_horde(&endpoint, &[]).expect("the binary spawns in a pty");
    horde
        .await_painted("pick")
        .await
        .expect("several sessions mean the picker, not a silent attach");
    horde
        .await_painted("pick-one")
        .await
        .expect("every live session is a row");
    horde
        .await_painted("pick-two")
        .await
        .expect("both rows paint");

    // Down once: past the "new session" top row, onto the newest session.
    horde.type_bytes(b"j").expect("j reaches the pty");
    horde.type_bytes(b"\r").expect("enter reaches the pty");
    horde
        .await_painted("attached:")
        .await
        .expect("enter on a session row attaches it");

    horde.type_bytes(&[0x03]).expect("ctrl-c reaches the pty");
    let status = horde.exit_status().await.expect("the TUI exits");
    assert!(status.success(), "got {status:?}");
}

/// Esc in the picker leaves without entering anything — cleanly, because a
/// dismissal is a decision, not a failure.
#[tokio::test]
async fn the_picker_dismisses_on_esc_without_attaching() {
    let endpoint = serve().await.expect("a listening server");
    let (mut client, _, _) = seeded(&endpoint, "stay-out")
        .await
        .expect("a seeded session");
    client
        .create_session("stay-away", "key-2")
        .await
        .expect("a second session");

    let mut horde = spawn_horde(&endpoint, &[]).expect("the binary spawns in a pty");
    horde
        .await_painted("pick")
        .await
        .expect("the picker paints");
    horde.type_bytes(&[0x1b]).expect("esc reaches the pty");
    let seen = horde.seen.clone();
    let status = horde.exit_status().await.expect("the process exits");
    assert!(
        status.success(),
        "a dismissal exits cleanly, got {status:?}"
    );
    assert!(
        !seen.contains("attached:"),
        "nothing may have been attached: {seen:?}"
    );
}

#[tokio::test]
async fn attach_paints_the_session_view_and_ctrl_c_exits_cleanly() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = seeded(&endpoint, "tui-pty-attach")
        .await
        .expect("a seeded session");

    // Attached by *name*, not id: the resolution ADR-0008 adds runs in the
    // real binary's attach path, and the painted header proves it landed on
    // the session the name meant.
    let mut horde =
        spawn_horde(&endpoint, &["attach", "tui-pty-attach"]).expect("the binary spawns in a pty");
    horde
        .await_painted("attached:")
        .await
        .expect("the session view must reach the real terminal");
    horde
        .await_painted(&session_id)
        .await
        .expect("the name resolved to the seeded session's id");
    horde
        .await_painted("spawned")
        .await
        .expect("the replayed root spawn must be painted into the feed");

    horde.type_bytes(&[0x03]).expect("ctrl-c reaches the pty");
    let status = horde.exit_status().await.expect("the TUI exits");
    assert!(
        status.success(),
        "ctrl-c must be a clean exit, got {status:?}"
    );
}

/// A typed key must do something *other* than exit — the one effect a broken
/// input path can fake. Delete `forward_terminal_input` and the reader thread
/// drops its sender at birth; `drive` sees the closed channel, requests quit,
/// and exits cleanly — indistinguishable, from outside, from ctrl-c working.
/// The tests above type ctrl-c and assert a clean exit, so they pass either
/// way. Here Esc must swap the attach view for the overview (which paints
/// "sessions", a word the session view never shows), and `i` must swap that
/// for the inbox, whose footer paints "allow". The route also pins that the
/// overview is still reachable behind the front door: attach first, Esc out.
/// Under a severed input path the process is gone before Esc lands and the
/// wait fails on the closed pty, without spending its ceiling.
#[tokio::test]
async fn a_typed_key_reaches_the_loop_and_repaints_the_screen() {
    let endpoint = serve().await.expect("a listening server");
    seeded(&endpoint, "tui-pty-keys")
        .await
        .expect("a seeded session");

    let mut horde = spawn_horde(&endpoint, &[]).expect("the binary spawns in a pty");
    // Raw mode is on once the attach view has painted, so the keystrokes
    // below arrive at the running loop, not a half-initialised terminal.
    horde
        .await_painted("attached:")
        .await
        .expect("one session, so the front door attaches before anything is typed");

    // The needle is the overview footer's "inbox": the header repaints only
    // the cells that changed under the attach header, so no header word is
    // guaranteed a contiguous byte run — the footer's new words are.
    horde.type_bytes(&[0x1b]).expect("esc reaches the pty");
    horde
        .await_painted("inbox")
        .await
        .expect("esc must detach into the overview — typed bytes must reach the loop");

    horde.type_bytes(b"i").expect("i reaches the pty");
    horde
        .await_painted("allow")
        .await
        .expect("i must swap the overview for the inbox");

    horde.type_bytes(b"q").expect("q reaches the pty");
    let status = horde.exit_status().await.expect("the TUI exits");
    assert!(status.success(), "q must be a clean exit, got {status:?}");
}

/// An unknown session must die as a CLI error before the terminal is ever
/// switched into raw mode — a blank TUI over a typo helps no one.
#[tokio::test]
async fn attaching_to_a_missing_session_fails_before_the_terminal_is_touched() {
    let endpoint = serve().await.expect("a listening server");

    let mut horde = spawn_horde(&endpoint, &["attach", "session-that-never-was"])
        .expect("the binary spawns in a pty");
    horde
        .await_painted("error")
        .await
        .expect("the refusal must be printed, not painted");
    let seen = horde.seen.clone();
    let status = horde.exit_status().await.expect("the process exits");
    assert!(!status.success(), "a refused attach is a failed run");
    assert!(
        !seen.contains("\u{1b}[?1049h"),
        "the alternate screen must never have been entered: {seen:?}"
    );
}
