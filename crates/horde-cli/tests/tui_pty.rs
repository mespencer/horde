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
//! arrived; a child that outlives its ceiling is killed, not leaked.

mod common;

use std::io::Read;
use std::time::Duration;

use common::{Setup, seeded, serve};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use tokio::sync::mpsc;

/// A `horde` process running inside a pty.
struct PtyHorde {
    /// Chunks of terminal output, as the child paints them.
    output: mpsc::UnboundedReceiver<Vec<u8>>,
    /// Everything read so far, accumulated across waits.
    seen: String,
    writer: Box<dyn std::io::Write + Send>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
}

/// Spawns the freshly built binary in an 80×24 pty.
fn spawn_horde(endpoint: &str, args: &[&str]) -> Setup<PtyHorde> {
    let pair = native_pty_system().openpty(PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    })?;

    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_horde"));
    command.arg("--endpoint");
    command.arg(endpoint);
    for arg in args {
        command.arg(arg);
    }
    command.env("TERM", "xterm-256color");

    let child = pair.slave.spawn_command(command)?;
    // The slave side belongs to the child now; holding it open here would
    // keep the pty from ever reporting EOF.
    drop(pair.slave);

    let mut reader = pair.master.try_clone_reader()?;
    let writer = pair.master.take_writer()?;
    let (sender, output) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        while let Ok(count) = reader.read(&mut buffer) {
            if count == 0 || sender.send(buffer[..count].to_vec()).is_err() {
                return;
            }
        }
    });

    Ok(PtyHorde {
        output,
        seen: String::new(),
        writer,
        child,
    })
}

impl PtyHorde {
    /// Reads until the screen has shown `needle`, within a ceiling.
    async fn await_painted(&mut self, needle: &str) -> Setup<()> {
        let deadline = Duration::from_secs(15);
        let step = async {
            while !self.seen.contains(needle) {
                match self.output.recv().await {
                    Some(chunk) => self.seen.push_str(&String::from_utf8_lossy(&chunk)),
                    None => {
                        return Err(format!(
                            "the pty closed before painting {needle:?}; saw: {:?}",
                            self.seen
                        )
                        .into());
                    }
                }
            }
            Ok(())
        };
        match tokio::time::timeout(deadline, step).await {
            Ok(outcome) => outcome,
            Err(_) => {
                // A child that outlives its ceiling is killed, not leaked.
                let _ = self.child.kill();
                Err(format!("{needle:?} was never painted; saw: {:?}", self.seen).into())
            }
        }
    }

    fn type_bytes(&mut self, bytes: &[u8]) -> Setup<()> {
        self.writer.write_all(bytes)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Waits for the child to exit, killing it if it overstays.
    async fn exit_status(mut self) -> Setup<portable_pty::ExitStatus> {
        let mut killer = self.child.clone_killer();
        let waited = tokio::time::timeout(
            Duration::from_secs(15),
            tokio::task::spawn_blocking(move || self.child.wait()),
        )
        .await;
        match waited {
            Ok(joined) => Ok(joined??),
            Err(_) => {
                let _ = killer.kill();
                Err("the TUI did not exit within its ceiling".into())
            }
        }
    }
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
