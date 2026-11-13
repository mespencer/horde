//! Every signal the testkit sends, and the one sweep that picks targets it
//! did not spawn itself.
//!
//! # Why this file is excluded from mutation
//!
//! Code that sends real signals to targets chosen at runtime must never be
//! mutated, because a mutant runs for real. An earlier version of this file
//! hunted descendants through a system-wide `ps -A`; cargo-mutants flipped
//! `ppid == parent` to `!=`, the test ran the mutant, and it SIGKILLed every
//! process on the machine. So this file holds only the guard, the sweep's
//! selection and the sending, and `gamma.toml` excludes it. What
//! is harmless to mutate — reading the table — lives in [`crate::ps`].
//!
//! # The two leaks it closes
//!
//! * **Grandchildren on drop.** [`crate::process::SpawnedProcess`] starts
//!   every child in a process group of its own, so a spawned `horde up`'s
//!   server and worker share `up`'s group, and one [`signal_group`] reaps
//!   them all. No process table is consulted.
//! * **A test process that never drops.** On timeout cargo-mutants
//!   SIGTERMs the process group of the test run, and an uncaught SIGTERM
//!   runs no destructor — and the products, in groups of their own, are not
//!   in that group. They are adopted by init. The first spawn in each test
//!   process sweeps for those ([`reap_orphans_once`]).
//!
//! # Every signal is guarded
//!
//! [`guard`] refuses, immediately before any send: 0 and 1, anything that
//! is not a positive `pid_t`, this process, its group, its session, its
//! parent and its parent's group. A refusal is an error value, never a panic.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use nix::errno::Errno;
use nix::sys::signal::{Signal, kill, killpg};
use nix::unistd::{Pid, getpgid, getpgrp, getpid, getppid, getsid};

use crate::ps::{ProcessRow, process_table};

/// The product binaries a sweep may reap.
pub const PRODUCTS: &[&str] = &["horde-server", "horde-worker", "horde"];

/// Why a signal was not sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unsent {
    /// The guard refused the target outright.
    Refused(Refusal),
    /// The OS refused it (`ESRCH` for a target already gone).
    Failed(Errno),
}

impl std::fmt::Display for Unsent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refusal) => write!(f, "refused to signal: {refusal:?}"),
            Self::Failed(errno) => write!(f, "the signal failed: {errno}"),
        }
    }
}

impl std::error::Error for Unsent {}

/// A target the guard will never signal, with the value it was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// 0 (our own group, to `kill`) or 1 (init, or every process to `kill -1`).
    Reserved(u32),
    /// Not a positive `pid_t`: as one it would go negative, and a negative
    /// target names a group, or every process.
    OutOfRange(u32),
    /// This very process.
    Ourselves(u32),
    /// This process's own group, its session leader, its parent, or its
    /// parent's group: the test runner and the terminal around it.
    Ancestry(u32),
}

/// The pid or pgid `raw` may be signalled as, or why it may not.
///
/// # Errors
///
/// The [`Refusal`] naming which protected value `raw` is.
pub fn guard(raw: u32) -> Result<Pid, Refusal> {
    if raw <= 1 {
        return Err(Refusal::Reserved(raw));
    }
    let target = i32::try_from(raw).map_err(|_| Refusal::OutOfRange(raw))?;
    if target == getpid().as_raw() {
        return Err(Refusal::Ourselves(raw));
    }
    let parent = getppid();
    let ancestry = [
        Some(getpgrp()),
        getsid(None).ok(),
        Some(parent),
        getpgid(Some(parent)).ok(),
    ];
    if ancestry
        .into_iter()
        .flatten()
        .any(|pid| pid.as_raw() == target)
    {
        return Err(Refusal::Ancestry(raw));
    }
    Ok(Pid::from_raw(target))
}

/// Sends `signal` to the one process `pid`, through [`guard`]. `None` sends
/// nothing and only asks whether the target exists.
///
/// # Errors
///
/// The guard's refusal, or the OS's.
pub fn signal_pid(pid: u32, signal: Option<Signal>) -> Result<(), Unsent> {
    let target = guard(pid).map_err(Unsent::Refused)?;
    kill(target, signal).map_err(Unsent::Failed)
}

/// Sends `signal` to every process in group `pgid`, through [`guard`].
/// `None` sends nothing and only asks whether the group exists.
///
/// # Errors
///
/// The guard's refusal, or the OS's.
pub fn signal_group(pgid: u32, signal: Option<Signal>) -> Result<(), Unsent> {
    let target = guard(pgid).map_err(Unsent::Refused)?;
    killpg(target, signal).map_err(Unsent::Failed)
}

/// The orphans a sweep reaps. Every condition must hold:
///
/// * adopted by init (`ppid` 1) — a live test process's own children have it
///   as their parent, so a sibling suite's daemons are never chosen;
/// * a group leader (`pgid == pid`) — the shape
///   [`crate::process::SpawnedProcess`] gives each product it starts. A
///   product `horde up` started shares `up`'s group, so even when a test
///   kills `up` and keeps using its server, that server is not a leader and
///   is never chosen;
/// * running one of `names` from under `root`, an absolute path below `/`.
#[must_use]
pub fn orphans_matching(table: &[ProcessRow], root: &Path, names: &[&str]) -> Vec<u32> {
    if !root.is_absolute() || root.parent().is_none() {
        return Vec::new();
    }
    table
        .iter()
        .filter(|row| row.ppid == 1 && row.pgid == row.pid && runs_from(&row.command, root, names))
        .map(|row| row.pid)
        .collect()
}

/// Whether `command`'s executable is one of `names` under `root`.
fn runs_from(command: &str, root: &Path, names: &[&str]) -> bool {
    let Some(executable) = command.split_whitespace().next().map(Path::new) else {
        return false;
    };
    let named = executable
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| names.contains(&name));
    named && executable.starts_with(root)
}

/// SIGKILLs the group of each orphan [`orphans_matching`] finds, returning
/// the groups the signal reached.
pub fn reap_orphans(root: &Path, names: &[&str]) -> Vec<u32> {
    orphans_matching(&process_table(), root, names)
        .into_iter()
        .filter(|&leader| signal_group(leader, Some(Signal::SIGKILL)).is_ok())
        .collect()
}

/// The target directory a test executable runs from:
/// `target/<profile>/deps/<test>` → `target`.
#[must_use]
pub fn target_root(test_executable: &Path) -> Option<PathBuf> {
    let mut root = test_executable.parent()?.to_path_buf();
    if root.ends_with("deps") {
        root.pop();
    }
    root.parent().map(Path::to_path_buf)
}

/// Sweeps this target directory's orphaned products, once per test process.
pub fn reap_orphans_once() {
    static SWEPT: OnceLock<()> = OnceLock::new();
    SWEPT.get_or_init(|| {
        if let Some(root) = std::env::current_exe()
            .ok()
            .as_deref()
            .and_then(target_root)
        {
            reap_orphans(&root, PRODUCTS);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pid: u32, ppid: u32, pgid: u32, command: &str) -> ProcessRow {
        ProcessRow {
            pid,
            ppid,
            pgid,
            command: command.to_owned(),
        }
    }

    fn raw(pid: Pid) -> u32 {
        u32::try_from(pid.as_raw()).expect("a positive pid")
    }

    /// Every protected value is refused by the guard itself — no signal is
    /// sent anywhere to learn it.
    #[test]
    fn the_guard_refuses_every_protected_target() {
        assert_eq!(guard(0), Err(Refusal::Reserved(0)));
        assert_eq!(guard(1), Err(Refusal::Reserved(1)));
        for wraps_negative in [u32::MAX, 1 << 31] {
            assert_eq!(
                guard(wraps_negative),
                Err(Refusal::OutOfRange(wraps_negative))
            );
        }
        let me = raw(getpid());
        assert_eq!(guard(me), Err(Refusal::Ourselves(me)));
        let parent = getppid();
        for ancestor in [
            Some(getpgrp()),
            Some(parent),
            getpgid(Some(parent)).ok(),
            getsid(None).ok(),
        ]
        .into_iter()
        .flatten()
        .map(raw)
        .filter(|&pid| pid > 1 && pid != me)
        {
            assert_eq!(guard(ancestor), Err(Refusal::Ancestry(ancestor)));
        }
    }

    /// The sending functions refuse through the guard. Asked with the null
    /// signal, so even a broken guard would send nothing.
    #[test]
    fn the_senders_refuse_before_sending() {
        let me = raw(getpid());
        assert_eq!(
            signal_pid(me, None),
            Err(Unsent::Refused(Refusal::Ourselves(me)))
        );
        assert_eq!(
            signal_group(0, None),
            Err(Unsent::Refused(Refusal::Reserved(0)))
        );
        let shown = Unsent::Refused(Refusal::Reserved(1)).to_string();
        assert!(shown.contains("refused"), "{shown}");
        let shown = Unsent::Failed(Errno::ESRCH).to_string();
        assert!(shown.contains("failed"), "{shown}");
    }

    #[test]
    fn only_init_adopted_group_leaders_from_this_target_are_orphans() {
        let root = Path::new("/w/target");
        let table = [
            row(1, 0, 1, "/sbin/launchd"),
            row(30, 1, 30, "/w/target/debug/horde-server --serve"),
            row(
                31,
                1,
                31,
                "/w/target/llvm-cov-target/debug/horde-worker --join x",
            ),
            row(32, 77, 32, "/w/target/debug/horde-server --serve"),
            row(33, 1, 33, "/other/target/debug/horde-server --serve"),
            row(34, 1, 34, "/w/target/debug/deps/cli_suite-abc"),
            row(35, 1, 35, "/w/target/debug/horde"),
            row(36, 1, 35, "/w/target/debug/horde-server --serve"),
            row(37, 1, 37, ""),
        ];
        assert_eq!(orphans_matching(&table, root, PRODUCTS), vec![30, 31, 35]);
        for unscoped in ["", "target", "/"] {
            assert!(
                orphans_matching(&table, Path::new(unscoped), PRODUCTS).is_empty(),
                "{unscoped:?} scopes nothing"
            );
        }
    }

    #[test]
    fn the_target_root_is_above_the_profile() {
        assert_eq!(
            target_root(Path::new("/w/target/debug/deps/suite-1")),
            Some(PathBuf::from("/w/target"))
        );
        assert_eq!(
            target_root(Path::new("/w/target/debug/suite")),
            Some(PathBuf::from("/w/target"))
        );
        assert_eq!(target_root(Path::new("suite")), None);
    }

    /// Whether `pid` is still a live (non-zombie) process.
    fn alive(pid: u32) -> bool {
        std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .is_ok_and(|out| {
                let stat = String::from_utf8_lossy(&out.stdout);
                !stat.trim().is_empty() && !stat.trim().starts_with('Z')
            })
    }

    async fn gone_within_ten_seconds(pid: u32) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            if !alive(pid) {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        false
    }

    /// The one real kill: `sleep` named like a product, under a scratch
    /// `target`, started as its own group leader and abandoned to init by
    /// its shell — reaped by a sweep over that root, and nothing else is.
    ///
    /// A symlink, not a copy: macOS kills a copied platform binary at exec,
    /// and `ps` reports the path as invoked either way.
    #[tokio::test]
    async fn a_real_orphaned_leader_is_reaped_by_its_directory() {
        let scratch = tempfile::tempdir().expect("scratch");
        let root = scratch.path().join("target");
        let profile = root.join("debug");
        std::fs::create_dir_all(&profile).expect("a profile dir");
        let product = profile.join("horde-server");
        std::os::unix::fs::symlink("/bin/sleep", &product).expect("a product-named sleep");
        // `set -m` gives the background job a group of its own, as
        // `SpawnedProcess` gives each product.
        let shell = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(format!(
                    "set -m; '{}' 60 </dev/null >/dev/null 2>&1 & echo $!",
                    product.display()
                ))
                .output(),
        )
        .await
        .expect("the shell returns")
        .expect("the shell runs");
        let orphan: u32 = String::from_utf8_lossy(&shell.stdout)
            .trim()
            .parse()
            .expect("the orphan's pid");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !process_table()
            .iter()
            .any(|row| row.pid == orphan && row.ppid == 1 && row.pgid == orphan)
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "init never adopted a leader {orphan}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(reap_orphans(&scratch.path().join("elsewhere"), PRODUCTS).is_empty());
        assert_eq!(reap_orphans(&root, PRODUCTS), vec![orphan]);
        assert!(
            gone_within_ten_seconds(orphan).await,
            "the orphan was reaped"
        );
    }

    /// Dropping a spawned process takes its whole group with it.
    #[tokio::test]
    async fn dropping_a_spawned_process_reaps_its_group() {
        let (spawned, grandchild) = a_shell_with_a_grandchild().await;
        drop(spawned);
        assert!(
            gone_within_ten_seconds(grandchild).await,
            "the grandchild outlived the drop"
        );
    }

    /// Killing a spawned process takes its whole group with it too.
    #[tokio::test]
    async fn killing_a_spawned_process_kills_its_group() {
        let (mut spawned, grandchild) = a_shell_with_a_grandchild().await;
        spawned.kill().await.expect("the kill sends");
        assert!(
            gone_within_ten_seconds(grandchild).await,
            "the grandchild outlived the kill"
        );
    }

    /// A spawned shell parked on a backgrounded `sleep`, and that sleep's pid.
    async fn a_shell_with_a_grandchild() -> (crate::process::SpawnedProcess, u32) {
        let spawned = crate::process::SpawnedProcess::spawn(
            "tree",
            Path::new("/bin/sh"),
            &["-c", "sleep 60 & echo grandchild=$!; wait"],
            &[],
        )
        .expect("the shell spawns");
        let seen = spawned
            .await_stdout("the grandchild's pid", |out| out.contains("grandchild="))
            .await
            .expect("the pid is announced");
        let grandchild: u32 = crate::process::value_after(&seen, "grandchild=")
            .and_then(|pid| pid.parse().ok())
            .expect("a pid");
        assert!(alive(grandchild));
        (spawned, grandchild)
    }

    #[test]
    fn a_sweep_runs_once() {
        reap_orphans_once();
        reap_orphans_once();
    }
}
