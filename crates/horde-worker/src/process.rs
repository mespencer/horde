//! Spawning and supervising one harness process.

use std::path::PathBuf;
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

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
        }
    }
}

impl core::error::Error for SupervisorError {}

impl From<std::io::Error> for SupervisorError {
    fn from(source: std::io::Error) -> Self {
        Self::Io(source)
    }
}

/// How to start one harness process.
#[derive(Debug, Clone)]
pub struct HarnessSpec {
    pub program: String,
    pub args: Vec<String>,
    /// The account's config directory, exported under `env_var`.
    ///
    /// Isolating this is what lets one Worker hold several accounts for the same
    /// provider (DESIGN.md §4). `None` inherits the ambient directory, which is
    /// correct for a single-account Worker and a latent bug for any other.
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
}

/// A running harness, owned by its handle.
pub struct SupervisedHarness {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl SupervisedHarness {
    /// Spawns the harness with its stdio piped.
    ///
    /// # Errors
    ///
    /// [`SupervisorError::Spawn`] if the binary cannot start — a missing harness
    /// is an ordinary condition on a Worker that has not installed it yet, not a
    /// panic.
    pub fn spawn(spec: &HarnessSpec) -> Result<Self, SupervisorError> {
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // The harness's own diagnostics are not protocol and must never be
            // parsed as such; the Worker surfaces them separately.
            .stderr(Stdio::null())
            .kill_on_drop(true);

        for (key, value) in &spec.env {
            command.env(key, value);
        }
        if let Some(dir) = &spec.config_dir {
            command.env(spec.config_env_var, dir);
        }
        if let Some(cwd) = &spec.cwd {
            command.current_dir(cwd);
        }

        let mut child = command.spawn().map_err(|source| SupervisorError::Spawn {
            program: spec.program.clone(),
            source,
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or(SupervisorError::MissingPipe("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(SupervisorError::MissingPipe("stdout"))?;

        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
        })
    }

    /// Writes one protocol line to the harness.
    pub async fn write_line(&mut self, line: &str) -> Result<(), SupervisorError> {
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        Ok(())
    }

    /// Reads one protocol line, or `None` at EOF.
    ///
    /// `None` means the harness closed its stdout — which is how a dead child
    /// announces itself to a reader. Callers turn that into
    /// [`SupervisorError::Exited`] rather than waiting.
    pub async fn read_line(&mut self) -> Result<Option<String>, SupervisorError> {
        Ok(self.stdout.next_line().await?)
    }

    /// Reaps the child, returning its exit status.
    pub async fn wait(&mut self) -> Result<Option<i32>, SupervisorError> {
        Ok(self.child.wait().await?.code())
    }

    /// Kills the child **by handle**.
    ///
    /// Never by name or pattern: a supervisor that matches on a command line
    /// reaches outside its own subtree and takes down processes it did not
    /// start. This crate learned that the expensive way during spike W2c.
    pub async fn kill(&mut self) -> Result<(), SupervisorError> {
        self.child.kill().await?;
        Ok(())
    }

    /// The OS process id, for diagnostics only.
    #[must_use]
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }
}
