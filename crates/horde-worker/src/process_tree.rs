//! A child spawned as the root of its own process tree, on every platform
//! the Worker runs on (ADR-0010, ADR-0017).
//!
//! A harness or a reclaim command is never one process: everything it starts
//! must stop with it. The two platforms name "everything it started"
//! differently:
//!
//! * **Unix** — a process group. The child leads a new group
//!   (`process_group(0)`), and the tree is signalled by group id with
//!   `killpg`: SIGTERM to ask, SIGKILL to insist. A group id is only safe to
//!   signal while the leader is unreaped, which [`crate::process`] owns.
//! * **Windows** — a Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
//!   The child starts suspended, joins the job, and only then runs, so
//!   nothing it starts can escape. The tree is reached by *handle*, never by
//!   id: `TerminateJobObject` kills every member, and closing the last
//!   handle — dropping this value, or the Worker dying — kills them too, so a
//!   crashed Worker leaves nothing behind for a sweep to find. Windows has no
//!   console-less SIGTERM: [`Stop::Terminate`] sends nothing there, and the
//!   graceful half of a stop is the caller's (a harness has its stdin closed).
//!
//! Everything above this module is platform-neutral: it asks a
//! [`ProcessTree`] to spawn, wait, and [`ProcessTree::signal`] with a
//! [`Stop`].

use std::process::ExitStatus;

use tokio::process::{ChildStderr, ChildStdin, ChildStdout, Command};

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(crate) use unix::{group_pid, signal_spawned_group};

/// How hard to stop a tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// Ask: SIGTERM to the group on Unix; nothing on Windows, where a
    /// console-less child has no signal to receive.
    Terminate,
    /// Insist: SIGKILL to the group on Unix; `TerminateJobObject` on Windows.
    Kill,
}

/// The root's piped standard streams, each taken at most once.
#[derive(Debug, Default)]
pub struct Pipes {
    pub stdin: Option<ChildStdin>,
    pub stdout: Option<ChildStdout>,
    pub stderr: Option<ChildStderr>,
}

/// A spawned child and the tree it roots.
pub struct ProcessTree {
    #[cfg(unix)]
    child: tokio::process::Child,
    /// The group the child leads — its pid at spawn.
    #[cfg(unix)]
    pgid: Option<u32>,
    /// The child inside its job; dropping it closes the job's last handle.
    #[cfg(windows)]
    child: Box<dyn process_wrap::tokio::ChildWrapper>,
}

impl std::fmt::Debug for ProcessTree {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessTree")
            .field("id", &self.id())
            .finish_non_exhaustive()
    }
}

impl ProcessTree {
    /// Spawns `command` as the root of a new tree: a new process group on
    /// Unix, a new kill-on-close Job Object on Windows
    /// ([`crate::windows::spawn_in_job`]). `kill_on_drop` is set here, so a
    /// tree dropped without a stop still loses its root.
    ///
    /// # Errors
    ///
    /// The spawn failing (a missing binary, most often), or on Windows the
    /// job failing to form — in which case the child is not left running.
    pub fn spawn(mut command: Command) -> std::io::Result<Self> {
        command.kill_on_drop(true);
        #[cfg(windows)]
        return crate::windows::spawn_in_job(command).map(|child| Self { child });
        #[cfg(unix)]
        {
            // Its own group, so stopping it reaches everything it started.
            command.process_group(0);
            let child = command.spawn()?;
            let pgid = child.id();
            Ok(Self { child, pgid })
        }
    }

    /// The root's OS process id while it is unreaped.
    #[must_use]
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }

    /// The process group the root leads — its pid at spawn — on Unix;
    /// always `None` on Windows, where the tree is a handle, not an id.
    #[must_use]
    pub fn group(&self) -> Option<u32> {
        #[cfg(unix)]
        return self.pgid;
        #[cfg(windows)]
        return None;
    }

    /// Takes the root's piped streams; a stream already taken, or never
    /// piped, is `None`.
    pub fn take_pipes(&mut self) -> Pipes {
        let child = &mut self.child;
        #[cfg(unix)]
        return Pipes {
            stdin: child.stdin.take(),
            stdout: child.stdout.take(),
            stderr: child.stderr.take(),
        };
        #[cfg(windows)]
        return Pipes {
            stdin: child.stdin().take(),
            stdout: child.stdout().take(),
            stderr: child.stderr().take(),
        };
    }

    /// Reaps the root, returning its status. The rest of the tree is left to
    /// the caller's sweep ([`Self::signal`] with [`Stop::Kill`]).
    ///
    /// # Errors
    ///
    /// The wait itself failing.
    pub async fn wait(&mut self) -> std::io::Result<ExitStatus> {
        self.child.wait().await
    }

    /// Kills the root by handle and reaps it.
    ///
    /// # Errors
    ///
    /// The kill or the reap failing.
    pub async fn kill_root(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        return self.child.kill().await;
        #[cfg(windows)]
        return Box::into_pin(self.child.kill()).await;
    }

    /// Stops the tree as hard as `stop` says, returning whether anything was
    /// sent. On Unix the group is signalled through [`signal_spawned_group`],
    /// refusing what it refuses; on Windows `Terminate` sends nothing (the
    /// graceful half of a stop is the caller's) and `Kill` terminates the
    /// job — every member, at once, by handle.
    pub fn signal(&mut self, stop: Stop) -> bool {
        #[cfg(unix)]
        {
            use nix::sys::signal::Signal;
            let signal = match stop {
                Stop::Terminate => Signal::SIGTERM,
                Stop::Kill => Signal::SIGKILL,
            };
            self.pgid
                .is_some_and(|leader| signal_spawned_group(leader, signal))
        }
        #[cfg(windows)]
        match stop {
            Stop::Terminate => false,
            Stop::Kill => self.child.start_kill().is_ok(),
        }
    }
}

#[cfg(test)]
#[cfg(unix)]
mod unix_tests {
    use super::ProcessTree;

    /// The debug view names the tree and its live root's pid.
    #[tokio::test]
    async fn the_debug_view_names_the_root() {
        let mut command = tokio::process::Command::new("sleep");
        command.arg("30");
        let mut tree = ProcessTree::spawn(command).expect("sleep spawns");
        let pid = tree.id().expect("a live root");
        let shown = format!("{tree:?}");
        assert!(shown.starts_with("ProcessTree"), "{shown}");
        assert!(shown.contains(&pid.to_string()), "{shown}");
        tree.kill_root().await.expect("kills");
    }
}

#[cfg(test)]
#[cfg(windows)]
mod windows_tests {
    use std::process::Stdio;
    use std::time::Duration;

    use horde_server_testkit::windows::gone_soon;
    use tokio::io::AsyncBufReadExt;

    use super::{ProcessTree, Stop};

    /// A command that starts a long-lived grandchild, prints its pid, and
    /// waits — the shape of a harness whose tool left a server running.
    fn spawner() -> tokio::process::Command {
        let mut command = tokio::process::Command::new("powershell.exe");
        command
            .args([
                "-NoProfile",
                "-Command",
                "$p = Start-Process -PassThru -WindowStyle Hidden ping.exe \
                 -ArgumentList '-n','600','127.0.0.1'; Write-Output $p.Id; \
                 Start-Sleep -Seconds 600",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        command
    }

    async fn grandchild_of(tree: &mut ProcessTree) -> u32 {
        let stdout = tree.take_pipes().stdout.expect("piped stdout");
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        let line = tokio::time::timeout(Duration::from_secs(30), lines.next_line())
            .await
            .expect("the spawner prints in time")
            .expect("reads")
            .expect("a line");
        line.trim().parse().expect("a pid")
    }

    /// Kill reaches the grandchild: the job holds the whole tree.
    #[tokio::test]
    async fn a_kill_reaches_every_member_of_the_job() {
        let mut tree = ProcessTree::spawn(spawner()).expect("spawns");
        let grandchild = grandchild_of(&mut tree).await;
        assert!(tree.signal(Stop::Kill), "TerminateJobObject was sent");
        let _ = tree.wait().await;
        assert!(gone_soon(grandchild).await, "{grandchild} outlived its job");
    }

    /// Dropping the tree closes the job's last handle, which kills it all —
    /// the property that makes a crashed Worker leave nothing behind.
    #[tokio::test]
    async fn dropping_the_tree_kills_the_job() {
        let mut tree = ProcessTree::spawn(spawner()).expect("spawns");
        let grandchild = grandchild_of(&mut tree).await;
        drop(tree);
        assert!(gone_soon(grandchild).await, "{grandchild} outlived its job");
    }

    /// Terminate is honest about sending nothing on Windows.
    #[tokio::test]
    async fn terminate_sends_nothing_and_says_so() {
        let mut tree = ProcessTree::spawn(spawner()).expect("spawns");
        assert!(!tree.signal(Stop::Terminate));
        assert_eq!(tree.group(), None, "no process groups here");
        assert!(tree.id().is_some());
        tree.kill_root().await.expect("kills");
    }
}
