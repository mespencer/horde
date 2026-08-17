//! The per-scenario state the step definitions share.
//!
//! Everything in here is *references to* real running things — a served
//! endpoint, a dialed worker, a stored identity — never a substitute for one.
//! The steps arrange and observe through production surfaces only: the `horde`
//! binary, the client library, the worker's own `Connection`/`Runtime`, and
//! the testkit's worker-shaped journal push for facts only a Worker can
//! author (DESIGN.md §5: observed facts enter through the worker spine).
//!
//! This file compiles without `cfg(test)` (`harness = false`), so the
//! workspace deny-list applies at full strength: no `unwrap`, no `expect`,
//! no `panic!`. Steps thread errors instead, and cucumber fails the step on
//! `Err` — which also names *which* arrangement broke.

use std::collections::HashMap;
use std::time::Duration;

use horde_cli::Client;
use horde_cli::client::SyncOptions;
use horde_proto::v1::EventEnvelope;
use horde_server_testkit::secure::SecureHarness;

/// Step failures are ordinary errors, threaded to cucumber.
pub(crate) type StepResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// What one `horde` invocation produced — the whole user-visible surface of a
/// CLI run: both streams and the exit status.
#[derive(Debug)]
pub(crate) struct RunOutput {
    pub stdout: String,
    pub stderr: String,
    pub success: bool,
}

/// A real `horde-worker` connection running in the background of a scenario.
///
/// Dropped with the world: the connection task is aborted so no worker
/// outlives the scenario that dialed it. The journal's tempdir rides along
/// for the same lifetime.
#[derive(Debug)]
pub(crate) struct RunningWorker {
    pub task: tokio::task::JoinHandle<Result<(), horde_worker::WorkerError>>,
    _journal_dir: tempfile::TempDir,
}

impl RunningWorker {
    pub(crate) fn new(
        task: tokio::task::JoinHandle<Result<(), horde_worker::WorkerError>>,
        journal_dir: tempfile::TempDir,
    ) -> Self {
        Self {
            task,
            _journal_dir: journal_dir,
        }
    }
}

impl Drop for RunningWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A secured server and the identities scenarios enrolled against it.
#[derive(Debug)]
pub(crate) struct Secured {
    pub harness: SecureHarness,
    /// Identity directories by machine name, for `--identity`.
    pub identity_dirs: HashMap<String, std::path::PathBuf>,
    /// Identities held in memory (a worker joined through the library).
    pub identities: HashMap<String, horde_identity::StoredIdentity>,
    dir: tempfile::TempDir,
}

impl Secured {
    pub(crate) fn new(harness: SecureHarness, dir: tempfile::TempDir) -> Self {
        Self {
            harness,
            identity_dirs: HashMap::new(),
            identities: HashMap::new(),
            dir,
        }
    }

    /// Where this scenario's identity directories live.
    pub(crate) fn base(&self) -> &std::path::Path {
        self.dir.path()
    }

    /// The identity directory enrolled for `name`, or a step-failing error.
    pub(crate) fn identity_dir(&self, name: &str) -> StepResult<&std::path::Path> {
        Ok(self
            .identity_dirs
            .get(name)
            .map(std::path::PathBuf::as_path)
            .ok_or_else(|| format!("no identity was enrolled for {name:?}"))?)
    }
}

/// The shared world: one per scenario, dropped (and with it every server
/// task, worker task and tempdir) when the scenario ends.
#[derive(Debug, Default, cucumber::World)]
pub(crate) struct HordeWorld {
    /// The plaintext loopback endpoint, when "a running server" was given.
    pub endpoint: Option<String>,
    /// The created session and its root agent.
    pub session_id: Option<String>,
    pub root_agent: Option<String>,
    /// What the last `horde` invocation produced.
    pub last: Option<RunOutput>,
    /// The scenario's background worker, if one joined.
    pub worker: Option<RunningWorker>,
    /// The secured arrangement, when "a secured server" was given.
    pub secure: Option<Secured>,
    /// The attached TUI event loop, when "the user attaches" ran.
    pub tui: Option<Tui>,
    /// Named values steps mint and later steps reference: `<child>`, the
    /// printed enrollment code, a blob address, file paths.
    pub notes: HashMap<String, String>,
    /// Directories that must outlive the steps that created them.
    pub keep: Vec<tempfile::TempDir>,
}

/// What the TUI loop hands back when it exits: the final frame and state.
pub(crate) type TuiExit = Result<
    (
        ratatui::Terminal<ratatui::backend::TestBackend>,
        horde_tui::App,
    ),
    horde_cli::ClientError,
>;

/// A running TUI event loop on a test terminal: real client, real follow
/// streams, keys from a channel, and the [`horde_tui::App`] observed after
/// every loop turn — the same seam `horde_cli::tui::drive` gives the binary.
pub(crate) struct Tui {
    pub keys: tokio::sync::mpsc::Sender<ratatui::crossterm::event::Event>,
    pub probes: tokio::sync::mpsc::UnboundedReceiver<horde_tui::App>,
    /// The most recently observed state. A quiet loop reports nothing new, so
    /// an assertion about "now" starts from what was already seen rather than
    /// waiting for a turn that may never come.
    pub last: Option<horde_tui::App>,
    /// `Some` until the quit step takes it to read the final frame. `Option`
    /// because [`Drop`] otherwise could not let the handle be moved out.
    pub task: Option<tokio::task::JoinHandle<TuiExit>>,
}

impl std::fmt::Debug for Tui {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tui").finish_non_exhaustive()
    }
}

impl Drop for Tui {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl HordeWorld {
    /// The plaintext endpoint, or a step-failing error naming the miss.
    pub(crate) fn endpoint(&self) -> StepResult<&str> {
        Ok(self
            .endpoint
            .as_deref()
            .ok_or("no server is running; a Given is missing")?)
    }

    /// The created session's id.
    pub(crate) fn session(&self) -> StepResult<&str> {
        Ok(self
            .session_id
            .as_deref()
            .ok_or("no session was created; a Given is missing")?)
    }

    /// The session's root agent id.
    pub(crate) fn root(&self) -> StepResult<&str> {
        Ok(self
            .root_agent
            .as_deref()
            .ok_or("no session was created; a Given is missing")?)
    }

    /// The secured arrangement.
    pub(crate) fn secure(&self) -> StepResult<&Secured> {
        Ok(self
            .secure
            .as_ref()
            .ok_or("no secured server is running; a Given is missing")?)
    }

    /// What the last invocation produced.
    pub(crate) fn last_run(&self) -> StepResult<&RunOutput> {
        Ok(self
            .last
            .as_ref()
            .ok_or("nothing was run yet; a When is missing")?)
    }

    /// A noted value by name.
    pub(crate) fn note(&self, name: &str) -> StepResult<&str> {
        Ok(self
            .notes
            .get(name)
            .map(String::as_str)
            .ok_or_else(|| format!("nothing was noted under {name:?}"))?)
    }

    /// Replaces `<session>`, `<root>` and every noted `<name>` in a command
    /// line from the feature file with the runtime ids they stand for.
    pub(crate) fn substituted(&self, text: &str) -> String {
        let mut out = text.to_owned();
        if let Some(session) = &self.session_id {
            out = out.replace("<session>", session);
        }
        if let Some(root) = &self.root_agent {
            out = out.replace("<root>", root);
        }
        for (name, value) in &self.notes {
            out = out.replace(&format!("<{name}>"), value);
        }
        out
    }

    /// Runs the real `horde` binary — the one cargo just built — against
    /// `endpoint`, recording everything the user would see.
    ///
    /// The environment is scrubbed the way every product-binary suite scrubs
    /// it, so nothing leaks in from the harness's own shell.
    pub(crate) async fn run_horde(&mut self, endpoint: &str, args: &[&str]) -> StepResult {
        let mut full: Vec<&str> = vec!["--endpoint", endpoint];
        full.extend_from_slice(args);
        self.capture(&full, &[]).await
    }

    /// [`Self::run_horde`], speaking as an enrolled identity directory.
    pub(crate) async fn run_horde_as(
        &mut self,
        endpoint: &str,
        identity_dir: &str,
        args: &[&str],
    ) -> StepResult {
        let mut full: Vec<&str> = vec!["--endpoint", endpoint, "--identity", identity_dir];
        full.extend_from_slice(args);
        self.capture(&full, &[]).await
    }

    async fn capture(&mut self, args: &[&str], environment: &[(&str, &str)]) -> StepResult {
        let output =
            horde_server_testkit::otlp_scrubbed_run(env!("CARGO_BIN_EXE_horde"), args, environment)
                .await?;
        self.last = Some(RunOutput {
            stdout: String::from_utf8(output.stdout)?,
            stderr: String::from_utf8(output.stderr)?,
            success: output.status.success(),
        });
        Ok(())
    }

    /// Waits — under [`within`]'s ceiling — until the session's log holds an
    /// event `matches` accepts, and returns what the matcher extracted.
    ///
    /// Subscribes through the real client library's follow stream: replay
    /// first, then live events, so a fact already on the log and a fact still
    /// in flight are the same wait.
    pub(crate) async fn await_event<T: Send + 'static>(
        &self,
        what: &str,
        matches: impl FnMut(&EventEnvelope) -> Option<T> + Send,
    ) -> StepResult<T> {
        let endpoint = self.endpoint()?.to_owned();
        let session = self.session()?.to_owned();
        within(what, async move {
            let mut client = Client::connect(&endpoint).await?;
            let mut stream = client.follow(&SyncOptions::new(session)).await?;
            Ok(stream
                .next_matching(matches)
                .await?
                .ok_or_else(|| format!("the stream ended before {what}"))?)
        })
        .await
    }
}

/// Every wait in the suite goes through here: a fifteen-second ceiling and
/// the reason the step was waiting when it fires.
pub(crate) async fn within<T>(
    what: &str,
    future: impl std::future::Future<Output = StepResult<T>>,
) -> StepResult<T> {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .map_err(|_| format!("timed out waiting for {what}"))?
}
