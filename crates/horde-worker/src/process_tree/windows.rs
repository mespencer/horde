//! The Windows tree: a Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`,
//! reached by handle (ADR-0017).
//!
//! `process-wrap` does the Win32 work — `CREATE_SUSPENDED`, the job, the
//! assignment, the resume — because the workspace forbids `unsafe`. The
//! child is in the job before its first instruction runs, so nothing it
//! starts can be outside it, and the job's last handle closing (this value
//! dropped, or the Worker process gone) kills every member.

use std::process::ExitStatus;

use process_wrap::tokio::{ChildWrapper, CommandWrap, JobObject, KillOnDrop};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout, Command};

use super::Stop;

pub(super) struct Tree {
    child: Box<dyn ChildWrapper>,
}

impl Tree {
    pub(super) fn spawn(command: Command) -> std::io::Result<Self> {
        let mut wrapped = CommandWrap::from(command);
        // KillOnDrop first: it is what makes the job kill-on-close.
        wrapped.wrap(KillOnDrop).wrap(JobObject);
        wrapped.spawn().map(|child| Self { child })
    }

    pub(super) fn id(&self) -> Option<u32> {
        self.child.id()
    }

    #[expect(
        clippy::unused_self,
        reason = "the Unix tree answers from self; the signatures match"
    )]
    pub(super) fn group(&self) -> Option<u32> {
        None
    }

    pub(super) fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.stdin().take()
    }

    pub(super) fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout().take()
    }

    pub(super) fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr().take()
    }

    pub(super) async fn wait(&mut self) -> std::io::Result<ExitStatus> {
        self.child.wait().await
    }

    pub(super) async fn kill_root(&mut self) -> std::io::Result<()> {
        Box::into_pin(self.child.kill()).await
    }

    pub(super) fn signal(&mut self, stop: Stop) -> bool {
        match stop {
            // No console, no ctrl event, no SIGTERM: the graceful half of a
            // stop is the caller's (a harness has its stdin closed).
            Stop::Terminate => false,
            // TerminateJobObject: every member, at once, by handle.
            Stop::Kill => self.child.start_kill().is_ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::process::Stdio;
    use std::time::Duration;

    use tokio::io::AsyncBufReadExt;

    use super::super::{ProcessTree, Stop};

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
        let stdout = tree.take_stdout().expect("piped stdout");
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        let line = tokio::time::timeout(Duration::from_secs(30), lines.next_line())
            .await
            .expect("the spawner prints in time")
            .expect("reads")
            .expect("a line");
        line.trim().parse().expect("a pid")
    }

    /// Whether `pid` is still a running process, by `tasklist`'s answer.
    async fn running(pid: u32) -> bool {
        let output = tokio::process::Command::new("tasklist.exe")
            .args(["/FI", &format!("PID eq {pid}"), "/NH", "/FO", "CSV"])
            .output()
            .await
            .expect("tasklist runs");
        String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\""))
    }

    async fn gone_soon(pid: u32) -> bool {
        for _ in 0..50 {
            if !running(pid).await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    /// Kill reaches the grandchild: the job holds the whole tree.
    #[tokio::test]
    async fn a_kill_reaches_every_member_of_the_job() {
        let mut tree = ProcessTree::spawn(spawner()).expect("spawns");
        let grandchild = grandchild_of(&mut tree).await;
        assert!(running(grandchild).await, "the grandchild started");
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
