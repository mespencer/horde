//! The Unix tree's guarded group signal: a process group, signalled by id
//! with `killpg`, never one that would broadcast or reach the Worker itself.

use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;

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
    if is_own(pgid, nix::unistd::getpgrp(), nix::unistd::getpid()) {
        return false;
    }
    killpg(pgid, signal).is_ok()
}

/// Whether `pgid` names the caller's own group, or a group the caller
/// itself would lead.
fn is_own(pgid: Pid, own_group: Pid, own_pid: Pid) -> bool {
    pgid == own_group || pgid == own_pid
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
        assert_eq!(
            group_pid(2).map(nix::unistd::Pid::as_raw),
            Some(2),
            "2 is the first id that is only ever one process"
        );
    }

    /// Either of the caller's own ids is its own, whether or not it leads
    /// its group.
    #[test]
    fn the_callers_group_and_pid_are_each_its_own() {
        let pid = nix::unistd::Pid::from_raw;
        assert!(super::is_own(pid(10), pid(10), pid(20)));
        assert!(super::is_own(pid(20), pid(10), pid(20)));
        assert!(!super::is_own(pid(30), pid(10), pid(20)));
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

    /// The group this process merely belongs to — led by someone else — is
    /// refused too. Run in a child of this test binary, which shares its
    /// parent's group without leading it; there a refusal-free mutant would
    /// send the (harmless) SIGCONT and say it had.
    #[test]
    fn a_tree_signal_never_reaches_a_group_this_process_only_belongs_to() {
        const PROBE: &str = "HORDE_TEST_SIGNAL_PROBE";
        if std::env::var_os(PROBE).is_some() {
            let group = nix::unistd::getpgrp();
            assert_ne!(group, nix::unistd::getpid(), "the probe leads no group");
            let group = u32::try_from(group.as_raw()).expect("a pid fits");
            assert!(!super::signal_spawned_group(
                group,
                nix::sys::signal::Signal::SIGCONT
            ));
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().expect("this binary"))
            .args([
                "--exact",
                "process_tree::unix::tests::a_tree_signal_never_reaches_a_group_this_process_only_belongs_to",
                "--test-threads=1",
            ])
            .env(PROBE, "1")
            .output()
            .expect("the probe runs");
        let printed = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && printed.contains("1 passed"),
            "the probe ran and refused: {printed}"
        );
    }
}
