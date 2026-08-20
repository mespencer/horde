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

#[tokio::test]
async fn the_overview_paints_on_a_real_terminal_and_q_exits_cleanly() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = seeded(&endpoint, "tui-pty-overview")
        .await
        .expect("a seeded session");

    let mut horde = spawn_horde(&endpoint, &[]).expect("the binary spawns in a pty");
    // Needles are single words: the terminal skips blank cells with cursor
    // moves, so a phrase never paints as one contiguous byte run.
    horde
        .await_painted("horde")
        .await
        .expect("the overview header must reach the real terminal");
    horde
        .await_painted("tui-pty-overview")
        .await
        .expect("the seeded session must be painted");
    assert!(
        horde.seen.contains(&session_id),
        "the session id is the row a user copies from"
    );

    horde.type_bytes(b"q").expect("q reaches the pty");
    let status = horde.exit_status().await.expect("the TUI exits");
    assert!(status.success(), "q must be a clean exit, got {status:?}");
}

#[tokio::test]
async fn attach_paints_the_session_view_and_ctrl_c_exits_cleanly() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = seeded(&endpoint, "tui-pty-attach")
        .await
        .expect("a seeded session");

    let mut horde =
        spawn_horde(&endpoint, &["attach", &session_id]).expect("the binary spawns in a pty");
    horde
        .await_painted("attached:")
        .await
        .expect("the session view must reach the real terminal");
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
/// and exits cleanly — indistinguishable, from outside, from `q` working.
/// The tests above type `q` and ctrl-c and assert a clean exit, so they pass
/// either way. Here `i` must swap the overview for the inbox, whose footer
/// paints "allow" — a word no overview screen ever shows (the seeded session
/// id is the deterministic `session-key-1`, so no row can collide). Under a
/// severed input path the process is gone before `i` lands and the wait fails
/// on the closed pty, without spending its ceiling.
#[tokio::test]
async fn a_typed_key_reaches_the_loop_and_repaints_the_screen() {
    let endpoint = serve().await.expect("a listening server");
    seeded(&endpoint, "tui-pty-keys")
        .await
        .expect("a seeded session");

    let mut horde = spawn_horde(&endpoint, &[]).expect("the binary spawns in a pty");
    // Raw mode is on once the overview has painted, so the keystroke below
    // arrives at the running loop, not a half-initialised terminal.
    horde
        .await_painted("sessions")
        .await
        .expect("the overview must paint before anything is typed");

    horde.type_bytes(b"i").expect("i reaches the pty");
    horde
        .await_painted("allow")
        .await
        .expect("i must swap the overview for the inbox — typed bytes must reach the loop");

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
