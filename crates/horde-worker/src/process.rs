//! Spawning and supervising one harness process — and everything it starts.
//!
//! A harness is not one process. Claude's Bash tool, a dev server an agent
//! launched, a stdio MCP server: all are the harness's children, and killing
//! the harness's pid alone leaves every one of them running after the agent
//! is archived. So each harness is spawned as the leader of its own process
//! group, and stopping it signals the **group** — SIGTERM, a bounded grace,
//! then SIGKILL for whatever ignored it (ADR-0010).
//!
//! Signalling a group by id is only safe while that id cannot have been
//! reissued, which is exactly while the leader is unreaped: a zombie still
//! holds its pid, and a pid that names a live group cannot be handed out
//! again. Every group signal here is therefore sent either before the leader
//! is reaped, or straight after reaping it while the group's surviving
//! members still hold the id.
//!
//! A Worker that crashes never gets to stop anything, so each group is also
//! written to a [`GroupLedger`] (the journal, in production) when it starts
//! and erased when it is reaped; [`reap_leftover_groups`] sweeps whatever a
//! previous run left behind.
//!
//! On Windows the tree is a kill-on-close Job Object instead (ADR-0017, via
//! [`crate::process_tree`]): stopping it closes the harness's stdin — the
//! only graceful signal a console-less child has — then terminates the job
//! after the same grace. A job dies with the last handle to it, so a crashed
//! Worker leaves nothing running and nothing is written to the ledger there.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{ChildStdin, ChildStdout, Command};

use crate::process_tree::{ProcessTree, Stop};

/// How many stderr lines a harness keeps. The newest win: the lines that
/// explain a death are the last ones, and an unbounded buffer is a memory
/// leak in any harness that logs for days.
pub const STDERR_LINES: usize = 256;

/// How long one kept stderr line may be, in bytes. A harness that writes a
/// megabyte without a newline gets its first few kilobytes remembered.
pub const STDERR_LINE_BYTES: usize = 4096;

/// How long a stopping harness group gets between SIGTERM and SIGKILL, when
/// the spec names nothing else.
pub const DEFAULT_STOP_GRACE: std::time::Duration = std::time::Duration::from_secs(3);

/// How long the one-shot `ps` that fingerprints a group leader may take.
#[cfg(unix)]
const FINGERPRINT_CEILING: std::time::Duration = std::time::Duration::from_secs(5);

/// Why supervision failed, as distinct from the harness protocol failing.
#[derive(Debug)]
pub enum SupervisorError {
    Spawn {
        program: String,
        source: std::io::Error,
    },
    /// A pipe the supervisor requires was not there. Structural, not transient.
    MissingPipe(&'static str),
    Io(std::io::Error),
    /// The child ended while the supervisor was still talking to it.
    ///
    /// Reported rather than waited on: a supervisor that blocks for output from
    /// a dead process hangs the agent forever, which is the failure this whole
    /// crate exists to make impossible.
    Exited {
        code: Option<i32>,
    },
    /// The child closed its stdin while the supervisor was writing to it.
    ///
    /// The write-side face of a death: a child that exits closes both pipes,
    /// and whether the supervisor's next write (EPIPE) or its next read (EOF)
    /// notices first is the scheduler's choice. Kept apart from [`Self::Io`]
    /// so an adapter reports it as the harness's death, exactly as it does
    /// EOF, and never as a protocol accident.
    HungUp,
}

impl core::fmt::Display for SupervisorError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Spawn { program, source } => write!(f, "could not spawn {program}: {source}"),
            Self::MissingPipe(which) => write!(f, "the child exposed no {which} pipe"),
            Self::Io(source) => write!(f, "harness io failed: {source}"),
            Self::Exited { code } => match code {
                Some(code) => write!(f, "the harness exited with status {code}"),
                None => write!(f, "the harness exited without a status"),
            },
            Self::HungUp => write!(f, "the harness closed its stdin"),
        }
    }
}

impl core::error::Error for SupervisorError {}

impl From<std::io::Error> for SupervisorError {
    fn from(source: std::io::Error) -> Self {
        Self::Io(source)
    }
}

/// One harness process group, as remembered across a Worker crash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessGroup {
    /// The group id — the leader's pid at spawn.
    pub pgid: u32,
    /// The leader's start time as `ps` printed it, or empty when `ps` could
    /// not say. What tells this Worker's leader apart from a stranger that
    /// was handed the same pid after a reboot.
    pub fingerprint: String,
}

/// Where harness process groups are remembered between spawn and reap.
///
/// The journal in production: a group written here survives the Worker, so
/// the next start can sweep what a crash left running.
#[async_trait::async_trait]
pub trait GroupLedger: std::fmt::Debug + Send + Sync {
    /// Remembers a freshly spawned group.
    async fn opened(&self, group: &HarnessGroup) -> Result<(), String>;
    /// Forgets a group that has been reaped.
    async fn closed(&self, pgid: u32);
    /// Every group still remembered.
    async fn open_groups(&self) -> Result<Vec<HarnessGroup>, String>;
}

/// How to start one harness process.
#[derive(Debug, Clone)]
pub struct HarnessSpec {
    pub program: String,
    pub args: Vec<String>,
    /// The config directory, exported under `config_env_var`.
    ///
    /// Isolating this is what lets one Worker hold several accounts for the same
    /// provider (DESIGN.md §4), and what keeps the operator's own harness
    /// configuration out of an agent (ADR-0010). Production always sets it,
    /// through the [`crate::harness::HarnessProfile`]; `None` inherits the
    /// ambient directory and is left for the process tests.
    pub config_dir: Option<PathBuf>,
    /// The variable the harness reads its config directory from —
    /// `CLAUDE_CONFIG_DIR`, `CODEX_HOME`.
    pub config_env_var: &'static str,
    pub cwd: Option<PathBuf>,
    /// Extra environment for the child.
    ///
    /// Set on the child rather than on the Worker's own process: a supervisor
    /// that mutated its own environment to configure a child would leak that
    /// setting into every later spawn, and into any concurrent one.
    pub env: Vec<(String, String)>,
    /// Owner-only files the launch references (an MCP config carrying a
    /// bearer token). Held by the harness and deleted when it is dropped, so
    /// a secret lives exactly as long as the process that reads it.
    pub held_files: Vec<crate::harness::SecretFile>,
    /// Where this harness's process group is remembered, if anywhere.
    pub ledger: Option<Arc<dyn GroupLedger>>,
    /// SIGTERM-to-SIGKILL grace when the group is stopped.
    pub stop_grace: std::time::Duration,
}

impl HarnessSpec {
    /// A spec running `program` with no arguments, ambient config, and the
    /// default stop grace.
    #[must_use]
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            config_dir: None,
            config_env_var: "CLAUDE_CONFIG_DIR",
            cwd: None,
            env: Vec::new(),
            held_files: Vec::new(),
            ledger: None,
            stop_grace: DEFAULT_STOP_GRACE,
        }
    }
}

/// A bounded tail of a harness's stderr.
type StderrTail = Arc<tokio::sync::Mutex<VecDeque<String>>>;

/// A running harness, owned by its handle.
pub struct SupervisedHarness {
    child: ProcessTree,
    /// `None` once a Windows stop has closed it — the graceful half of a
    /// stop there; always present on Unix.
    stdin: Option<ChildStdin>,
    stdout: Lines<BufReader<ChildStdout>>,
    /// The harness's own diagnostics, drained in the background.
    ///
    /// Drained rather than left in the pipe because a full stderr pipe blocks
    /// the child — a harness that logs enough would deadlock against a
    /// supervisor that never reads it. Bounded to the last [`STDERR_LINES`].
    stderr: StderrTail,
    /// The process group the harness leads — its pid at spawn — on Unix;
    /// `None` on Windows, where the tree is a job reached by handle.
    pgid: Option<u32>,
    /// Set once the leader has been reaped: from then on its pid is the
    /// kernel's to reissue, and only a sweep straight after the reap may
    /// still name the group.
    reaped: bool,
    ledger: Option<Arc<dyn GroupLedger>>,
    stop_grace: std::time::Duration,
    /// Kept alive for the harness's lifetime; deleted on drop.
    _held_files: Vec<crate::harness::SecretFile>,
}

impl SupervisedHarness {
    /// Spawns the harness with its stdio piped, as the leader of a new
    /// process group, and remembers the group in the spec's ledger.
    ///
    /// # Errors
    ///
    /// [`SupervisorError::Spawn`] if the binary cannot start — a missing harness
    /// is an ordinary condition on a Worker that has not installed it yet, not a
    /// panic.
    pub async fn spawn(spec: &HarnessSpec) -> Result<Self, SupervisorError> {
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Piped, not null: the harness's diagnostics are not protocol and
            // must never be parsed as such, but they are the only explanation a
            // dead harness leaves behind. Discarding them turns every startup
            // failure into "it exited" with no reason attached.
            .stderr(Stdio::piped());
        // Its own tree (a process group, or a job), so stopping it reaches
        // everything it started; and the last resort, for a handle dropped
        // without a stop: the leader dies with it, and `Drop` sweeps the
        // group.

        for (key, value) in &spec.env {
            command.env(key, value);
        }
        if let Some(dir) = &spec.config_dir {
            command.env(spec.config_env_var, dir);
        }
        if let Some(cwd) = &spec.cwd {
            command.current_dir(cwd);
        }

        let mut child = ProcessTree::spawn(command).map_err(|source| SupervisorError::Spawn {
            program: spec.program.clone(),
            source,
        })?;
        let pgid = child.group();

        let pipes = child.take_pipes();
        let stdin = pipes.stdin.ok_or(SupervisorError::MissingPipe("stdin"))?;
        let stdout = pipes.stdout.ok_or(SupervisorError::MissingPipe("stdout"))?;
        let stderr_pipe = pipes.stderr.ok_or(SupervisorError::MissingPipe("stderr"))?;

        let stderr: StderrTail = Arc::new(tokio::sync::Mutex::new(VecDeque::new()));
        let sink = Arc::clone(&stderr);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr_pipe).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                keep_tail(&mut *sink.lock().await, line);
            }
        });

        let harness = Self {
            child,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout).lines(),
            stderr,
            pgid,
            reaped: false,
            ledger: spec.ledger.clone(),
            stop_grace: spec.stop_grace,
            _held_files: spec.held_files.clone(),
        };
        harness.remember_group().await;
        Ok(harness)
    }

    /// Writes this harness's group to the ledger. Best-effort: a ledger that
    /// cannot write costs the crash sweep one group, which is no reason to
    /// refuse an agent that is already running.
    #[cfg(unix)]
    async fn remember_group(&self) {
        let (Some(ledger), Some(pgid)) = (&self.ledger, self.pgid) else {
            return;
        };
        let group = HarnessGroup {
            pgid,
            fingerprint: leader_fingerprint(pgid).await,
        };
        if let Err(error) = ledger.opened(&group).await {
            tracing::warn!(pgid, %error, "could not remember a harness process group");
        }
    }

    /// Nothing to remember on Windows: the job dies with the Worker, so no
    /// later run has anything to sweep.
    #[cfg(not(unix))]
    #[expect(
        clippy::unused_async,
        reason = "the Unix ledger write is async; the call site is shared"
    )]
    async fn remember_group(&self) {}

    /// Writes one protocol line to the harness.
    ///
    /// # Errors
    ///
    /// [`SupervisorError::HungUp`] when the child has closed its stdin — a
    /// dead child, seen from the write side — and [`SupervisorError::Io`]
    /// for any other failure.
    pub async fn write_line(&mut self, line: &str) -> Result<(), SupervisorError> {
        self.write_raw(line).await.map_err(write_failure)
    }

    async fn write_raw(&mut self, line: &str) -> std::io::Result<()> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
        stdin.write_all(line.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await
    }

    /// Reads one protocol line, or `None` at EOF.
    ///
    /// `None` means the harness closed its stdout — which is how a dead child
    /// announces itself to a reader. Callers turn that into
    /// [`SupervisorError::Exited`] rather than waiting.
    pub async fn read_line(&mut self) -> Result<Option<String>, SupervisorError> {
        Ok(self.stdout.next_line().await?)
    }

    /// Reaps the leader, returning its exit status, then sweeps whatever it
    /// left running in its group: a harness that died on its own leaves its
    /// tool children with no supervisor at all.
    pub async fn wait(&mut self) -> Result<Option<i32>, SupervisorError> {
        let status = self.child.wait().await?;
        self.after_reap().await;
        Ok(status.code())
    }

    /// Kills the harness and its whole group **by handle**, at once.
    ///
    /// Never by name or pattern: a supervisor that matches on a command line
    /// reaches outside its own subtree and takes down processes it did not
    /// start. This crate learned that the expensive way during spike W2c.
    pub async fn kill(&mut self) -> Result<(), SupervisorError> {
        if self.reaped {
            // Already reaped, so already swept: nothing of ours is left.
            return Ok(());
        }
        self.signal_group(Stop::Kill);
        self.child.kill_root().await?;
        self.after_reap().await;
        Ok(())
    }

    /// Stops the harness group gracefully: SIGTERM to the group, the stop
    /// grace for the leader to exit, then SIGKILL to whatever is left. On
    /// Windows the ask is the harness's stdin closing (EOF ends a
    /// stream-json or app-server session), and the insistence is the job
    /// terminated.
    ///
    /// Bounded by the grace plus one SIGKILL-and-reap; never a wait on a
    /// process that chose not to exit.
    pub async fn stop(&mut self) -> Result<(), SupervisorError> {
        self.signal_group(Stop::Terminate);
        #[cfg(not(unix))]
        drop(self.stdin.take());
        match tokio::time::timeout(self.stop_grace, self.child.wait()).await {
            Ok(exited) => {
                exited?;
                self.after_reap().await;
                Ok(())
            }
            // Still running: SIGKILL while the leader is unreaped, which is
            // the moment the group id is provably still ours.
            Err(_elapsed) => self.kill().await,
        }
    }

    /// Bookkeeping once the leader is reaped: one SIGKILL sweep for members
    /// that outlived it (they still hold the group id, so it is still ours),
    /// and the ledger entry erased.
    async fn after_reap(&mut self) {
        if self.reaped {
            return;
        }
        self.signal_group(Stop::Kill);
        self.reaped = true;
        if let (Some(ledger), Some(pgid)) = (&self.ledger, self.pgid) {
            ledger.closed(pgid).await;
        }
    }

    /// Signals the group, while it can still be this harness's.
    fn signal_group(&mut self, stop: Stop) {
        if self.reaped {
            return;
        }
        // ESRCH just means everyone already left.
        let _ = self.child.signal(stop);
    }

    /// What the harness wrote to stderr — its last [`STDERR_LINES`] lines.
    ///
    /// The per-agent log the Worker attaches to a failure: without it, a harness
    /// that refused to start is indistinguishable from one that crashed.
    pub async fn diagnostics(&self) -> Vec<String> {
        self.stderr.lock().await.iter().cloned().collect()
    }

    /// The child's OS process id while it is running.
    ///
    /// `None` once the child has been reaped by [`Self::wait`] — the id stops
    /// being meaningful at that point, and returning a stale one invites a
    /// caller to signal a pid the kernel has since handed to somebody else.
    /// That is the same class of mistake as killing by pattern, arrived at from
    /// the other direction.
    ///
    /// Diagnostics only. Nothing in this crate signals by pid; killing goes
    /// through [`Self::kill`], which owns the handle.
    #[must_use]
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }

    /// The process group the harness leads, while it is unreaped; always
    /// `None` on Windows, where the harness's tree is a job, not a group.
    #[must_use]
    pub fn process_group(&self) -> Option<u32> {
        if self.reaped { None } else { self.pgid }
    }
}

impl Drop for SupervisedHarness {
    /// A handle dropped without a stop: SIGKILL the group while the leader
    /// is still unreaped (`kill_on_drop` then takes the leader), and erase
    /// the ledger entry if a runtime is there to do it.
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        self.signal_group(Stop::Kill);
        if let (Some(ledger), Some(pgid)) = (self.ledger.clone(), self.pgid)
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move { ledger.closed(pgid).await });
        }
    }
}

/// Classifies a failed write: EPIPE means nobody holds the child's stdin any
/// more, which for a harness is its death; anything else is plain I/O.
fn write_failure(source: std::io::Error) -> SupervisorError {
    if source.kind() == std::io::ErrorKind::BrokenPipe {
        SupervisorError::HungUp
    } else {
        SupervisorError::Io(source)
    }
}

/// Appends one stderr line to a bounded tail, dropping the oldest.
fn keep_tail(tail: &mut VecDeque<String>, mut line: String) {
    // The longest char-boundary prefix within the bound (a short line is
    // its own boundary, so it is kept whole).
    let cut = (0..=STDERR_LINE_BYTES.min(line.len()))
        .rev()
        .find(|&index| line.is_char_boundary(index))
        .unwrap_or_default();
    line.truncate(cut);
    if tail.len() == STDERR_LINES {
        tail.pop_front();
    }
    tail.push_back(line);
}

/// The leader's start time, as `ps` prints it — empty when `ps` cannot say
/// (an absent pid prints nothing on stdout; with `lstart=` there is no
/// header either).
#[cfg(unix)]
async fn leader_fingerprint(pid: u32) -> String {
    let probe = Command::new("ps")
        .args(["-o", "lstart=", "-p", &pid.to_string()])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .output();
    match tokio::time::timeout(FINGERPRINT_CEILING, probe).await {
        Ok(Ok(output)) => String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        _ => String::new(),
    }
}

/// What a remembered group turned out to be at startup.
#[cfg_attr(not(unix), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leftover {
    /// Nothing in the group survives: forget it.
    Gone,
    /// This Worker's group: its leader is still the one remembered, or the
    /// leader is gone and its orphans still hold the id (which, while they
    /// live, cannot have been reissued to anybody else).
    Ours,
    /// The id now names somebody else's leader — a reused pid after a
    /// reboot. Never signalled.
    Stranger,
}

/// Classifies one remembered group from what the OS says now.
///
/// Pure, so every row is pinned by a test without a process in sight.
#[cfg_attr(not(unix), allow(dead_code))]
fn classify(group_alive: bool, current_leader: Option<&str>, remembered: &str) -> Leftover {
    match (group_alive, current_leader) {
        (false, _) => Leftover::Gone,
        (true, None) => Leftover::Ours,
        (true, Some(current)) if !remembered.is_empty() && current == remembered => Leftover::Ours,
        (true, Some(_)) => Leftover::Stranger,
    }
}

/// Sweeps the harness groups a previous run of this Worker left behind,
/// returning how many it killed.
///
/// Called once at startup, before any agent is spawned, so every group the
/// ledger names is a previous run's. Each is classified first and only this
/// Worker's own are SIGKILLed; every row is forgotten either way. On Windows
/// nothing is ever left to kill — a job dies with the Worker that held it —
/// so every row (written by no Windows run) is only forgotten.
pub async fn reap_leftover_groups(ledger: &dyn GroupLedger) -> usize {
    let groups = match ledger.open_groups().await {
        Ok(groups) => groups,
        Err(error) => {
            tracing::warn!(%error, "could not read the harness groups a previous run left");
            return 0;
        }
    };
    let mut reaped = 0;
    for group in groups {
        reaped += usize::from(sweep_leftover(&group).await);
        ledger.closed(group.pgid).await;
    }
    reaped
}

/// Classifies one remembered group and SIGKILLs it when it is provably
/// this Worker's, returning whether it was killed.
#[cfg(unix)]
async fn sweep_leftover(group: &HarnessGroup) -> bool {
    use nix::sys::signal::{Signal, kill, killpg};
    let Some(pgid) = crate::process_tree::group_pid(group.pgid) else {
        return false;
    };
    let group_alive = killpg(pgid, None).is_ok();
    let current = if kill(pgid, None).is_ok() {
        Some(leader_fingerprint(group.pgid).await)
    } else {
        None
    };
    match classify(group_alive, current.as_deref(), &group.fingerprint) {
        Leftover::Ours => killpg(pgid, Signal::SIGKILL).is_ok(),
        Leftover::Stranger => {
            tracing::warn!(
                pgid = group.pgid,
                "a remembered harness group id now names another process; left alone"
            );
            false
        }
        Leftover::Gone => false,
    }
}

/// Never anything to sweep on Windows (see [`reap_leftover_groups`]).
#[cfg(not(unix))]
#[expect(
    clippy::unused_async,
    reason = "the Unix sweep probes asynchronously; the call site is shared"
)]
async fn sweep_leftover(_group: &HarnessGroup) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::{
        Leftover, STDERR_LINE_BYTES, STDERR_LINES, SupervisorError, classify, keep_tail,
        write_failure,
    };

    /// The messages an operator reads when a harness will not start.
    ///
    /// Worth pinning: a supervisor whose errors all render the same is one an
    /// operator cannot act on, and `Display` is the only place the distinction
    /// between "never started" and "died" survives.
    #[test]
    fn each_failure_explains_itself_distinctly() {
        let spawn = SupervisorError::Spawn {
            program: "claude".to_owned(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "no such file"),
        }
        .to_string();
        assert!(spawn.contains("claude") && spawn.contains("no such file"));

        assert!(
            SupervisorError::MissingPipe("stdout")
                .to_string()
                .contains("stdout")
        );

        let with_code = SupervisorError::Exited { code: Some(3) }.to_string();
        assert!(with_code.contains('3'), "the status is the diagnosis");

        let without = SupervisorError::Exited { code: None }.to_string();
        assert!(
            without.contains("without a status"),
            "a signal death has no code and must say so rather than print a fake one"
        );

        assert!(
            SupervisorError::Io(std::io::Error::other("pipe broke"))
                .to_string()
                .contains("pipe broke")
        );

        assert!(
            SupervisorError::HungUp
                .to_string()
                .contains("closed its stdin"),
            "a hang-up names which pipe the child let go of"
        );
    }

    /// EPIPE is the child's death seen from the write side; every other
    /// write failure stays plain I/O.
    #[test]
    fn a_broken_pipe_is_a_hang_up_and_nothing_else_is() {
        assert!(matches!(
            write_failure(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
            SupervisorError::HungUp
        ));
        assert!(matches!(
            write_failure(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
            SupervisorError::Io(_)
        ));
    }

    /// The stderr tail keeps the newest lines and never grows past its bound.
    #[test]
    fn the_stderr_tail_keeps_the_newest_lines_within_its_bound() {
        let mut tail = VecDeque::new();
        for n in 0..STDERR_LINES + 10 {
            keep_tail(&mut tail, format!("line {n}"));
        }
        assert_eq!(tail.len(), STDERR_LINES, "bounded, not unbounded");
        assert_eq!(tail.front().map(String::as_str), Some("line 10"));
        assert_eq!(
            tail.back().map(String::as_str),
            Some(format!("line {}", STDERR_LINES + 9).as_str()),
            "the newest line explains the death; it must be kept"
        );
    }

    /// One endless line is cut to the byte bound, on a char boundary.
    #[test]
    fn an_overlong_stderr_line_is_cut_on_a_char_boundary() {
        let mut tail = VecDeque::new();
        keep_tail(&mut tail, "é".repeat(STDERR_LINE_BYTES));
        let kept = tail.front().map(String::len).unwrap_or_default();
        // "é" is two bytes and the bound is even, so the cut lands exactly.
        assert_eq!(kept, STDERR_LINE_BYTES);
        keep_tail(&mut tail, format!("a{}", "é".repeat(STDERR_LINE_BYTES)));
        assert_eq!(
            tail.back().map(String::len),
            Some(STDERR_LINE_BYTES - 1),
            "a cut mid-character backs off to the boundary"
        );

        keep_tail(&mut tail, "short".to_owned());
        assert_eq!(tail.back().map(String::as_str), Some("short"));
    }

    /// The fingerprint is `ps`'s start-time string for a live process — the
    /// shape a reboot-reused pid will not match — and empty for a pid that
    /// does not exist.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_fingerprint_is_the_start_time_of_a_live_process_only() {
        let own = super::leader_fingerprint(std::process::id()).await;
        assert!(
            own.contains(':') && own.chars().any(|c| c.is_ascii_digit()),
            "a start time like `Fri Oct 30 22:13:34 2026`, got {own:?}"
        );
        assert_eq!(
            super::leader_fingerprint(999_999_999).await,
            "",
            "no process, no fingerprint"
        );
    }

    /// Every row of the startup classification: a dead group is forgotten,
    /// orphans and a still-matching leader are ours, and a leader whose start
    /// time differs — or that was never fingerprinted — is a stranger.
    #[test]
    fn a_leftover_group_is_killed_only_when_it_is_provably_ours() {
        let started = "Fri Oct 30 22:13:34 2026";
        assert_eq!(classify(false, None, started), Leftover::Gone);
        assert_eq!(classify(false, Some(started), started), Leftover::Gone);
        assert_eq!(classify(true, None, started), Leftover::Ours);
        assert_eq!(classify(true, Some(started), started), Leftover::Ours);
        assert_eq!(
            classify(true, Some("Sat Oct 31 08:00:00 2026"), started),
            Leftover::Stranger,
            "a reused pid after a reboot is somebody else's shell"
        );
        assert_eq!(
            classify(true, Some(""), ""),
            Leftover::Stranger,
            "an unfingerprinted leader cannot be proven ours"
        );
    }
}
