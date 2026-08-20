//! A product binary driven inside a real pty.
//!
//! Two suites hold raw-terminal behavior to account — the TUI's shell
//! (`horde-cli`'s pty suite) and the interactive login relay (the e2e
//! tier) — and both need the same instrument: spawn the binary on a pty,
//! read what it paints, type at it, and watch it exit. It lives here so
//! neither suite restates the reader thread and the ceilinged waits (the
//! duplication gate's rule applies to test code too).
//!
//! Every wait is a positive wait with a ceiling, asserting on the bytes
//! that arrived; a child that outlives its ceiling is killed, not leaked.

use std::io::{Read, Write};
use std::time::Duration;

use crate::Setup;

/// How long a paint or an exit may take before the child is killed.
const CEILING: Duration = Duration::from_secs(30);

/// A process running inside an 80×24 pty.
pub struct PtyProcess {
    /// Chunks of terminal output, as the child paints them.
    output: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    /// Everything read so far, accumulated across waits.
    pub seen: String,
    writer: Box<dyn Write + Send>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
}

impl PtyProcess {
    /// Spawns `command` in a fresh pty, with `TERM` set the way every
    /// terminal application needs.
    ///
    /// # Errors
    ///
    /// The pty failing to open or the command failing to spawn.
    pub fn spawn(mut command: portable_pty::CommandBuilder) -> Setup<Self> {
        let pair = portable_pty::native_pty_system().openpty(portable_pty::PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })?;
        command.env("TERM", "xterm-256color");

        let child = pair.slave.spawn_command(command)?;
        // The slave side belongs to the child now; holding it open here
        // would keep the pty from ever reporting EOF.
        drop(pair.slave);

        let mut reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;
        let (sender, output) = tokio::sync::mpsc::unbounded_channel();
        std::thread::spawn(move || {
            let mut buffer = [0_u8; 4096];
            loop {
                // EOF and error retire the reader alike — a closed pty
                // reports either, platform's choice — and only real bytes
                // are worth a send.
                match reader.read(&mut buffer) {
                    Ok(0) | Err(_) => return,
                    Ok(count) => {
                        if sender.send(buffer[..count].to_vec()).is_err() {
                            return;
                        }
                    }
                }
            }
        });

        Ok(Self {
            output,
            seen: String::new(),
            writer,
            child,
        })
    }

    /// Reads until the terminal has shown `needle`, within the ceiling.
    ///
    /// # Errors
    ///
    /// The pty closing first, or the ceiling passing — either way with
    /// everything seen so far, and the child killed rather than leaked.
    pub async fn await_painted(&mut self, needle: &str) -> Setup<()> {
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
        match tokio::time::timeout(CEILING, step).await {
            Ok(outcome) => outcome,
            Err(_) => {
                let _ = self.child.kill();
                Err(format!("{needle:?} was never painted; saw: {:?}", self.seen).into())
            }
        }
    }

    /// Types at the child, as an operator would.
    ///
    /// # Errors
    ///
    /// The pty refusing the write — a child already gone.
    pub fn type_bytes(&mut self, bytes: &[u8]) -> Setup<()> {
        self.writer.write_all(bytes)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Waits for the child to exit, killing it if it overstays.
    ///
    /// # Errors
    ///
    /// The ceiling passing, or the wait itself failing.
    pub async fn exit_status(mut self) -> Setup<portable_pty::ExitStatus> {
        let mut killer = self.child.clone_killer();
        let waited = tokio::time::timeout(
            CEILING,
            tokio::task::spawn_blocking(move || self.child.wait()),
        )
        .await;
        match waited {
            Ok(joined) => Ok(joined??),
            Err(_) => {
                let _ = killer.kill();
                Err("the child did not exit within its ceiling".into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PtyProcess;

    /// The driver against a real child: a paint that arrives *late* is
    /// still awaited (a wait that returned without reading would miss it),
    /// everything painted accumulates in `seen`, and the exit is collected.
    #[tokio::test]
    async fn painted_output_is_awaited_accumulated_and_the_exit_collected() {
        let mut command = portable_pty::CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("printf one; sleep 0.3; printf two");
        let mut child = PtyProcess::spawn(command).expect("the shell spawns");

        child
            .await_painted("two")
            .await
            .expect("the late paint must be waited for, not assumed");
        assert!(
            child.seen.contains("one") && child.seen.contains("two"),
            "everything painted accumulates: {:?}",
            child.seen
        );

        let status = child.exit_status().await.expect("the shell exits");
        assert!(status.success());
    }

    /// A child that exits without painting the needle: the closed pty must
    /// answer as an error naming what was seen — not hang, not succeed.
    #[tokio::test]
    async fn a_closed_pty_fails_an_unpainted_wait_with_what_it_saw() {
        let mut command = portable_pty::CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("printf only-this");
        let mut child = PtyProcess::spawn(command).expect("the shell spawns");

        let missing = child
            .await_painted("never-painted")
            .await
            .expect_err("a closed pty cannot paint it");
        assert!(
            missing.to_string().contains("closed"),
            "the failure names the closed pty, promptly — a reader thread \
             that survives EOF turns this into a full-ceiling hang: {missing}"
        );
        assert!(child.seen.contains("only-this"));
    }

    /// Typed bytes reach the child — the half `type_bytes` owns.
    #[tokio::test]
    async fn typed_bytes_reach_the_child() {
        let mut command = portable_pty::CommandBuilder::new("/bin/sh");
        command.arg("-c");
        command.arg("read line; printf \"got %s\" \"$line\"");
        let mut child = PtyProcess::spawn(command).expect("the shell spawns");

        child.type_bytes(b"hello\r").expect("typing lands");
        child
            .await_painted("got hello")
            .await
            .expect("the child heard what was typed");
        let status = child.exit_status().await.expect("the shell exits");
        assert!(status.success());
    }
}
