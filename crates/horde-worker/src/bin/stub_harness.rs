//! A stand-in for a provider harness, for deterministic process-level tests.
//!
//! Reads a transcript path from `HORDE_STUB_TRANSCRIPT` and replays its lines to
//! stdout, one turn per line it reads on stdin. That makes the supervisor's
//! tests real process tests — actual `fork`/`exec`, actual pipes, actual EOF and
//! exit codes — without a login, a network, or a token.
//!
//! It is deliberately a separate binary rather than an in-process fake: the
//! things most likely to be wrong in a supervisor (stdio deadlock, a child that
//! outlives its handle, EOF handling) do not exist in an in-process fake.
//!
//! Behaviour is driven by env vars so one binary covers every scenario:
//!
//! * `HORDE_STUB_TRANSCRIPT` — file of stdout lines, blank-line separated turns.
//! * `HORDE_STUB_EXIT_AFTER` — exit after N turns, to test mid-session death.
//! * `HORDE_STUB_EXIT_CODE` — the code to exit with (default 0).
//! * `HORDE_STUB_IGNORE_INTERRUPT` — swallow control requests, to prove the
//!   supervisor does not hang waiting for an ack that never comes.
//! * `HORDE_STUB_CONTROL_LOG` — append each control request to this file.
//!   The acknowledgement the harness sends back is classified as a non-event by
//!   the codec, so a test watching only the event stream cannot tell an
//!   interrupt that was sent from one that was never written. This file is the
//!   observation point that closes that gap.
//! * `HORDE_STUB_GRANDCHILD` — start a long-lived child that ignores SIGTERM
//!   (a tool command, a dev server) and write its pid to this file, so the
//!   process-group tests can watch whether stopping the harness reaches it.
//! * `HORDE_STUB_HANG_UP` — close stdin on the first turn, before serving it,
//!   then exit. A supervisor that has read that turn is guaranteed its next
//!   write meets EPIPE: the write-side face of a death, forced rather than
//!   left to the scheduler.
//!
//! A transcript line reading exactly `@await-control-response` is not
//! written: the stub stops there until a `control_response` arrives on
//! stdin, records it in `HORDE_STUB_CONTROL_LOG`, and carries on — the stall
//! a real harness imposes while a `can_use_tool` prompt is unanswered
//! (W12c, transcript `12e`).

use std::io::{BufRead, Write};

fn main() -> std::process::ExitCode {
    let Some(contents) = transcript() else {
        return std::process::ExitCode::FAILURE;
    };
    if let Ok(pidfile) = std::env::var("HORDE_STUB_GRANDCHILD") {
        start_grandchild(&pidfile);
    }
    replay(&turns(&contents), &Behaviour::from_env())
}

/// Starts a SIGTERM-ignoring sleeper in this harness's process group and
/// records its pid — written after the spawn, so a test that reads the file
/// knows the child exists.
fn start_grandchild(pidfile: &str) {
    let spawned = std::process::Command::new("/bin/sh")
        .args(["-c", "trap '' TERM; exec sleep 600"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .spawn();
    if let Ok(child) = spawned {
        let _ = std::fs::write(pidfile, child.id().to_string());
    }
}

/// The transcript this run replays, or `None` having said why on stderr.
fn transcript() -> Option<String> {
    let Ok(path) = std::env::var("HORDE_STUB_TRANSCRIPT") else {
        eprintln!("HORDE_STUB_TRANSCRIPT is required");
        return None;
    };
    match std::fs::read_to_string(&path) {
        Ok(contents) => Some(contents),
        Err(_) => {
            eprintln!("could not read transcript {path}");
            None
        }
    }
}

/// The turns a transcript describes.
///
/// Blank lines separate them, so one file describes a whole session.
fn turns(contents: &str) -> Vec<Vec<&str>> {
    contents
        .split("\n\n")
        .map(|turn| turn.lines().filter(|l| !l.trim().is_empty()).collect())
        .filter(|turn: &Vec<&str>| !turn.is_empty())
        .collect()
}

/// Whether this stdin line is a control request rather than a turn.
///
/// The real harness answers one out of band and keeps going, so the stub must
/// not spend a scripted turn on it.
fn is_control(line: &str) -> bool {
    line.contains("\"control_request\"")
}

/// Whether this stdin line answers a control request the stub sent.
fn is_control_response(line: &str) -> bool {
    line.contains("\"control_response\"")
}

/// The transcript marker that waits for Horde's answer to a prompt.
const AWAIT_CONTROL_RESPONSE: &str = "@await-control-response";

/// What the environment asked this run to do.
struct Behaviour {
    exit_after: Option<usize>,
    exit_code: u8,
    ignore_interrupt: bool,
    control_log: Option<String>,
    hang_up: bool,
}

impl Behaviour {
    fn from_env() -> Self {
        Self {
            exit_after: numeric("HORDE_STUB_EXIT_AFTER"),
            exit_code: numeric("HORDE_STUB_EXIT_CODE").unwrap_or(0),
            ignore_interrupt: std::env::var("HORDE_STUB_IGNORE_INTERRUPT").is_ok(),
            control_log: std::env::var("HORDE_STUB_CONTROL_LOG").ok(),
            hang_up: std::env::var("HORDE_STUB_HANG_UP").is_ok(),
        }
    }

    /// Whether the script says to stop having served this many turns.
    fn done_after(&self, served: usize) -> bool {
        self.exit_after.is_some_and(|n| served >= n)
    }

    /// Records a control request where a test can see it.
    ///
    /// The acknowledgement is a non-event to the codec, so without this file a
    /// test cannot tell an interrupt that was sent from one never written.
    fn record(&self, line: &str) {
        let Some(log) = &self.control_log else { return };
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
        {
            let _ = writeln!(file, "{line}");
        }
    }

    /// Answers a control request unless told to swallow it.
    ///
    /// `false` means the pipe is gone, which ends the session.
    fn acknowledge(&self, stdout: &mut std::io::Stdout) -> bool {
        if self.ignore_interrupt {
            return true;
        }
        let ack = r#"{"type":"control_response","response":{"subtype":"success"}}"#;
        writeln!(stdout, "{ack}").is_ok() && stdout.flush().is_ok()
    }
}

/// A numeric environment setting, absent when unset or unparseable.
fn numeric<T: std::str::FromStr>(key: &str) -> Option<T> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Serves one turn per stdin line until the script or the pipe runs out.
///
/// Every way out of the loop ends at the same exit code, which is why the
/// error paths can break rather than each returning for themselves.
fn replay(turns: &[Vec<&str>], behaviour: &Behaviour) -> std::process::ExitCode {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut served = 0_usize;
    let mut lines = stdin.lock().lines();

    while let Some(line) = lines.next() {
        let Ok(line) = line else { break };

        // An answer that arrived outside any await (late, or unsolicited):
        // recorded, never spent as a turn.
        if is_control_response(&line) {
            behaviour.record(&line);
            continue;
        }
        if is_control(&line) {
            behaviour.record(&line);
            if behaviour.acknowledge(&mut stdout) {
                continue;
            }
            break;
        }

        // Out of script: behave like a harness that has nothing left.
        let Some(turn) = turns.get(served) else { break };
        served += 1;

        // Closed before the turn is served, so a supervisor that has read
        // the turn knows its next write meets EPIPE. The next read here
        // fails on the closed descriptor, which ends the loop.
        if behaviour.hang_up {
            let _ = nix::unistd::close(0);
        }

        if !serve(&mut stdout, turn, &mut lines, behaviour) || behaviour.done_after(served) {
            break;
        }
    }

    std::process::ExitCode::from(behaviour.exit_code)
}

/// Writes one turn's lines, stopping at each await marker until stdin
/// brings a `control_response`. `false` means a pipe is gone.
fn serve(
    stdout: &mut std::io::Stdout,
    turn: &[&str],
    stdin: &mut impl Iterator<Item = std::io::Result<String>>,
    behaviour: &Behaviour,
) -> bool {
    for out in turn {
        if *out == AWAIT_CONTROL_RESPONSE {
            if !stdout.flush().is_ok() || !await_response(stdin, stdout, behaviour) {
                return false;
            }
            continue;
        }
        if writeln!(stdout, "{out}").is_err() {
            return false;
        }
    }
    stdout.flush().is_ok()
}

/// Reads stdin until a `control_response` arrives, recording it. Control
/// requests met on the way (an interrupt) are recorded and acknowledged as
/// they would be between turns. `false` means stdin closed first.
fn await_response(
    stdin: &mut impl Iterator<Item = std::io::Result<String>>,
    stdout: &mut std::io::Stdout,
    behaviour: &Behaviour,
) -> bool {
    for line in stdin.by_ref() {
        let Ok(line) = line else { return false };
        behaviour.record(&line);
        if is_control_response(&line) {
            return true;
        }
        if is_control(&line) && !behaviour.acknowledge(stdout) {
            return false;
        }
    }
    false
}
