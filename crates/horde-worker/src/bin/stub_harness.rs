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

use std::io::{BufRead, Write};

fn main() -> std::process::ExitCode {
    let Ok(path) = std::env::var("HORDE_STUB_TRANSCRIPT") else {
        eprintln!("HORDE_STUB_TRANSCRIPT is required");
        return std::process::ExitCode::FAILURE;
    };
    let Ok(contents) = std::fs::read_to_string(&path) else {
        eprintln!("could not read transcript {path}");
        return std::process::ExitCode::FAILURE;
    };

    // Blank lines separate turns, so one file describes a whole session.
    let turns: Vec<Vec<&str>> = contents
        .split("\n\n")
        .map(|turn| turn.lines().filter(|l| !l.trim().is_empty()).collect())
        .filter(|turn: &Vec<&str>| !turn.is_empty())
        .collect();

    let exit_after: Option<usize> = std::env::var("HORDE_STUB_EXIT_AFTER")
        .ok()
        .and_then(|v| v.parse().ok());
    let exit_code: u8 = std::env::var("HORDE_STUB_EXIT_CODE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let ignore_interrupt = std::env::var("HORDE_STUB_IGNORE_INTERRUPT").is_ok();

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut served = 0_usize;

    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };

        // A control request is not a turn; the real harness answers it out of
        // band and keeps going.
        if line.contains("\"control_request\"") {
            if let Ok(log) = std::env::var("HORDE_STUB_CONTROL_LOG")
                && let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log)
            {
                let _ = writeln!(file, "{line}");
            }
            if !ignore_interrupt {
                let ack = r#"{"type":"control_response","response":{"subtype":"success"}}"#;
                if writeln!(stdout, "{ack}").is_err() || stdout.flush().is_err() {
                    break;
                }
            }
            continue;
        }

        let Some(turn) = turns.get(served) else {
            break; // Out of script: behave like a harness that has nothing left.
        };
        served += 1;

        for out in turn {
            if writeln!(stdout, "{out}").is_err() {
                return std::process::ExitCode::from(exit_code);
            }
        }
        if stdout.flush().is_err() {
            break;
        }

        if exit_after.is_some_and(|n| served >= n) {
            return std::process::ExitCode::from(exit_code);
        }
    }

    std::process::ExitCode::from(exit_code)
}
