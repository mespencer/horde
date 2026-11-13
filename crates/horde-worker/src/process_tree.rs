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
use unix as imp;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as imp;

#[cfg(unix)]
pub(crate) use imp::group_pid;

/// How hard to stop a tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// Ask: SIGTERM to the group on Unix; nothing on Windows, where a
    /// console-less child has no signal to receive.
    Terminate,
    /// Insist: SIGKILL to the group on Unix; `TerminateJobObject` on Windows.
    Kill,
}

/// A spawned child and the tree it roots.
pub struct ProcessTree {
    inner: imp::Tree,
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
    /// Unix, a new kill-on-close Job Object on Windows. `kill_on_drop` is set
    /// here, so a tree dropped without a stop still loses its root.
    ///
    /// # Errors
    ///
    /// The spawn failing (a missing binary, most often), or on Windows the
    /// job failing to form — in which case the child is not left running.
    pub fn spawn(mut command: Command) -> std::io::Result<Self> {
        command.kill_on_drop(true);
        imp::Tree::spawn(command).map(|inner| Self { inner })
    }

    /// The root's OS process id while it is unreaped.
    #[must_use]
    pub fn id(&self) -> Option<u32> {
        self.inner.id()
    }

    /// The process group the root leads — its pid at spawn — on Unix;
    /// always `None` on Windows, where the tree is a handle, not an id.
    #[must_use]
    pub fn group(&self) -> Option<u32> {
        self.inner.group()
    }

    /// Takes the root's piped stdin, once.
    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.inner.take_stdin()
    }

    /// Takes the root's piped stdout, once.
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.inner.take_stdout()
    }

    /// Takes the root's piped stderr, once.
    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.inner.take_stderr()
    }

    /// Reaps the root, returning its status. The rest of the tree is left to
    /// the caller's sweep ([`Self::signal`] with [`Stop::Kill`]).
    ///
    /// # Errors
    ///
    /// The wait itself failing.
    pub async fn wait(&mut self) -> std::io::Result<ExitStatus> {
        self.inner.wait().await
    }

    /// Kills the root by handle and reaps it.
    ///
    /// # Errors
    ///
    /// The kill or the reap failing.
    pub async fn kill_root(&mut self) -> std::io::Result<()> {
        self.inner.kill_root().await
    }

    /// Stops the tree as hard as `stop` says, returning whether anything was
    /// sent. Refuses on Unix what [`signal_spawned_group`] refuses.
    pub fn signal(&mut self, stop: Stop) -> bool {
        self.inner.signal(stop)
    }
}
