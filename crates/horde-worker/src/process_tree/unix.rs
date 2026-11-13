//! The Unix tree: a process group, signalled by id with `killpg`.

use std::process::ExitStatus;

use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};

use super::Stop;

pub(super) struct Tree {
    child: Child,
    /// The group the child leads — its pid at spawn.
    pgid: Option<u32>,
}

impl Tree {
    pub(super) fn spawn(mut command: Command) -> std::io::Result<Self> {
        // Its own group, so stopping it reaches everything it started.
        command.process_group(0);
        let child = command.spawn()?;
        let pgid = child.id();
        Ok(Self { child, pgid })
    }

    pub(super) fn id(&self) -> Option<u32> {
        self.child.id()
    }

    pub(super) fn group(&self) -> Option<u32> {
        self.pgid
    }

    pub(super) fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.stdin.take()
    }

    pub(super) fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    pub(super) fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }

    pub(super) async fn wait(&mut self) -> std::io::Result<ExitStatus> {
        self.child.wait().await
    }

    pub(super) async fn kill_root(&mut self) -> std::io::Result<()> {
        self.child.kill().await
    }

    pub(super) fn signal(&mut self, stop: Stop) -> bool {
        let signal = match stop {
            Stop::Terminate => Signal::SIGTERM,
            Stop::Kill => Signal::SIGKILL,
        };
        self.pgid
            .is_some_and(|leader| signal_spawned_group(leader, signal))
    }
}

/// Signals the process group led by a child this Worker spawned with
/// `process_group(0)` — a harness, or a soft-reclaim command (ADR-0014) —
/// returning whether the signal was sent. Refuses what [`group_pid`]
/// refuses, and this process's own group: a spawned tree never shares the
/// Worker's group, so a leader id naming it is a bug, and signalling it
/// would stop the Worker itself. (ESRCH — everyone already left — is
/// reported as not sent.)
pub(crate) fn signal_spawned_group(leader: u32, signal: Signal) -> bool {
    let Some(pgid) = group_pid(leader) else {
        return false;
    };
    if pgid == nix::unistd::getpgrp() || pgid == nix::unistd::getpid() {
        return false;
    }
    killpg(pgid, signal).is_ok()
}

/// A pid as nix wants it, refusing the ids that would broadcast: 0 is the
/// caller's own group and 1 is init, and neither is ever a harness.
pub(crate) fn group_pid(pgid: u32) -> Option<Pid> {
    let raw = i32::try_from(pgid).ok()?;
    (raw > 1).then(|| Pid::from_raw(raw))
}

#[cfg(test)]
mod tests {
    use super::group_pid;

    /// 0 and 1 would broadcast (the caller's own group, init); neither is a
    /// harness, and both are refused rather than signalled.
    #[test]
    fn broadcast_ids_are_never_a_group_to_signal() {
        assert_eq!(group_pid(0), None);
        assert_eq!(group_pid(1), None);
        assert_eq!(group_pid(u32::MAX), None, "out of pid_t range");
        assert_eq!(group_pid(4242).map(nix::unistd::Pid::as_raw), Some(4242));
    }

    /// The Worker's own group and pid are never a spawned tree's group:
    /// asking to signal them is refused before any signal is sent. (Signal 0
    /// would only probe, but the refusal must come first either way.)
    #[test]
    fn a_tree_signal_never_reaches_the_workers_own_group() {
        let own_group = u32::try_from(nix::unistd::getpgrp().as_raw()).expect("a pid fits");
        let own_pid = std::process::id();
        for refused in [0, 1, own_group, own_pid] {
            assert!(
                !super::signal_spawned_group(refused, nix::sys::signal::Signal::SIGCONT),
                "{refused} must be refused"
            );
        }
    }
}
