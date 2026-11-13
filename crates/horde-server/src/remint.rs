//! `horde-server --remint`: ask the server running on a state directory to
//! re-mint its bootstrap client token, and print the fresh one.
//!
//! The bootstrap token is printed once at start and expires with its code;
//! before this verb, the only re-mint was a restart (ADR-0007). The running
//! server re-mints on SIGHUP ([`crate::startup::hold_until`]); this side
//! finds it by the pid its state lock records, signals it, and waits —
//! bounded — for the token file to change. Same-machine by construction: the
//! state directory that holds the CA is the trust, exactly as it is for the
//! token file itself.

use std::path::Path;
use std::time::Duration;

use crate::startup::{STATE_LOCK_FILE, bootstrap_token_path};

/// How long the verb waits for the server to rewrite the token file.
pub const REMINT_DEADLINE: Duration = Duration::from_secs(10);

/// How often the token file is re-read while waiting.
const POLL: Duration = Duration::from_millis(25);

/// Re-mints the bootstrap token of the server holding `state_dir`, returning
/// the fresh token.
///
/// `hang_up` delivers the re-mint request to a pid — [`send_hangup`] in the
/// binary, injectable so the wait is testable without signals.
///
/// # Errors
///
/// A refusal in operator words: no server running there, a client already
/// enrolled (the bootstrap door is closed, so there is nothing to re-mint),
/// the signal failing, or no fresh token within `deadline`.
pub async fn remint_running(
    state_dir: &Path,
    hang_up: impl FnOnce(u32) -> std::io::Result<()>,
    deadline: Duration,
) -> Result<String, String> {
    let pid = holder_pid(state_dir)?;
    let token_file = bootstrap_token_path(state_dir);
    // A running server keeps the file truthful: present exactly while no
    // client is enrolled. Absent means the door is closed — refused now,
    // rather than after a wait for a token that will never come.
    let Some(before) = read_token(&token_file) else {
        return Err(format!(
            "the server on {} has a client enrolled, so its bootstrap door is closed; mint a \
             code from that client with `horde enroll client <name>`",
            state_dir.display()
        ));
    };
    hang_up(pid).map_err(|error| format!("could not signal the server (pid {pid}): {error}"))?;
    fresh_token(&token_file, &before, deadline)
        .await
        .ok_or_else(|| {
            format!(
                "the server (pid {pid}) did not re-mint within {deadline:?}; its stdout says why"
            )
        })
}

/// The pid of the live server holding `state_dir`'s lock.
fn holder_pid(state_dir: &Path) -> Result<u32, String> {
    let not_running = || {
        format!(
            "no server is running on {}; `horde-server --serve` starts one, and starting mints \
             a fresh bootstrap token",
            state_dir.display()
        )
    };
    if !horde_identity::StateLock::is_held(state_dir, STATE_LOCK_FILE) {
        return Err(not_running());
    }
    std::fs::read_to_string(state_dir.join(STATE_LOCK_FILE))
        .ok()
        .and_then(|text| text.trim().parse::<u32>().ok())
        .ok_or_else(not_running)
}

/// The token in `file`, trimmed; `None` when there is no file.
fn read_token(file: &Path) -> Option<String> {
    std::fs::read_to_string(file)
        .ok()
        .map(|text| text.trim().to_owned())
        .filter(|token| !token.is_empty())
}

/// Polls `file` until it holds a token other than `before`, or `deadline`.
async fn fresh_token(file: &Path, before: &str, deadline: Duration) -> Option<String> {
    let give_up = tokio::time::Instant::now() + deadline;
    loop {
        if let Some(token) = read_token(file).filter(|token| token != before) {
            return Some(token);
        }
        if tokio::time::Instant::now() >= give_up {
            return None;
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Sends SIGHUP to `pid`, the re-mint request a running server listens for.
///
/// The pid comes from a file on disk, not from a process this one spawned,
/// so [`hangup_target`] vets it first. Windows has no SIGHUP (ADR-0017), so
/// there this refuses with [`unsupported_remint`]
/// ([`crate::windows::send_hangup`]).
///
/// # Errors
///
/// The pid refused, or the signal failing (the pid gone, or not ours).
pub fn send_hangup(pid: u32) -> std::io::Result<()> {
    #[cfg(windows)]
    return crate::windows::send_hangup(pid);
    #[cfg(unix)]
    {
        let target = hangup_target(pid)?;
        nix::sys::signal::kill(target, nix::sys::signal::Signal::SIGHUP)
            .map_err(|errno| std::io::Error::other(format!("SIGHUP to {pid} failed: {errno}")))
    }
}

/// The refusal `--remint` gives on a platform without SIGHUP.
#[cfg_attr(unix, allow(dead_code))]
pub(crate) fn unsupported_remint(pid: u32) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!(
            "this platform has no SIGHUP to ask the server (pid {pid}) for a fresh token; \
             restart it, and starting mints one"
        ),
    )
}

/// `pid` as a single process this verb may hang up: never 0 (our own group,
/// to `kill`), 1 (init), a value that would go negative as a `pid_t` (a
/// group, or every process), this process, or its own process group.
///
/// # Errors
///
/// A refusal naming the pid.
#[cfg(unix)]
fn hangup_target(pid: u32) -> std::io::Result<nix::unistd::Pid> {
    let refused = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("refusing to signal pid {pid}: it is not a running server's"),
        )
    };
    let raw = i32::try_from(pid).map_err(|_| refused())?;
    let ours = [nix::unistd::getpid(), nix::unistd::getpgrp()];
    if raw <= 1 || ours.iter().any(|own| own.as_raw() == raw) {
        return Err(refused());
    }
    Ok(nix::unistd::Pid::from_raw(raw))
}

#[cfg(test)]
mod tests {
    /// The refusal a platform without SIGHUP gives names the pid, the
    /// missing signal, and the restart that works instead.
    #[test]
    fn a_remint_without_sighup_says_to_restart() {
        let error = super::unsupported_remint(4242);
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
        let text = error.to_string();
        assert!(text.contains("4242") && text.contains("SIGHUP") && text.contains("restart"));
    }

    /// On Windows the binary's hang-up refuses with that same refusal.
    #[cfg(windows)]
    #[test]
    fn send_hangup_refuses_without_sighup() {
        let error = super::send_hangup(4242).expect_err("no SIGHUP here");
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    }

    /// The guard alone answers: no signal is sent to learn any of this.
    #[cfg(unix)]
    #[test]
    fn a_hangup_never_targets_reserved_or_own_pids() {
        let own_pid = std::process::id();
        let own_group =
            u32::try_from(nix::unistd::getpgrp().as_raw()).expect("a positive process group");
        for refused in [0, 1, u32::MAX, 1 << 31, own_pid, own_group] {
            let error = super::hangup_target(refused).expect_err("refused");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput, "{refused}");
            assert!(error.to_string().contains(&refused.to_string()), "{error}");
        }
        let stranger = [2, 3]
            .into_iter()
            .find(|pid| ![own_pid, own_group].contains(pid))
            .expect("two pids cannot both be ours");
        assert_eq!(
            super::hangup_target(stranger)
                .map(|pid| i64::from(pid.as_raw()))
                .ok(),
            Some(i64::from(stranger)),
            "an ordinary pid passes (it is only vetted here, never signalled)"
        );
    }
}
