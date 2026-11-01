//! Process-level fault harnesses for the fault-injection suite
//! (docs/TESTING.md §2).
//!
//! The network half of the injector lives in `horde_testkit::fault` — a
//! programmable TCP proxy any suite can front a transport with. This crate is
//! the half that needs real processes: spawning the
//! [`fault-server`](../src/bin/fault_server.rs) fixture — the production
//! `horde_server::startup` stack in its own process — so a test can
//! `kill -9` the server mid-append, and spawning the
//! [`worker-runner`](../src/bin/worker_runner.rs) fixture — the production
//! worker library composed around a scripted FakeAdapter — so a test can
//! `kill -9` a worker mid-turn and then resurrect it over the same journal.
//!
//! Everything here reports failures as values: the suite injects faults on
//! purpose and must never trip over its own harness panicking.
//!
//! # Why the worker fixture is a binary of this crate
//!
//! DESIGN.md §2 names three binaries, but the worker's own `main` has not
//! been built yet — the worker is a library that `horde-server
//! --with-local-worker` will one day supervise. A process-kill scenario needs
//! a worker *process* today, so the runner composes the production pieces
//! (`Journal::open`, `Runtime`, `Connection::dial`, the redial loop) exactly
//! as the eventual binary will. It is fixture in the mutants sense — the
//! suite runs it, it tests nothing — and is excluded in `.cargo/mutants.toml`
//! under the same criterion as `stub_harness`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

/// What the harness can fail at. Every variant names enough to act on.
#[derive(Debug, thiserror::Error)]
pub enum FaultHarnessError {
    /// A product binary was not where the build puts it.
    #[error(
        "{name:?} is not at {looked:?}; build it first (the workspace test \
         build compiles every binary the suites spawn)"
    )]
    MissingBinary { name: String, looked: PathBuf },
    /// The test executable's own location could not anchor the target dir.
    #[error("could not locate the target directory from the running test executable: {0}")]
    NoTargetDir(String),
    /// Spawning or signalling a process failed.
    #[error("could not {doing}: {source}")]
    Process {
        doing: String,
        source: std::io::Error,
    },
    /// The server's stdout never announced what `--serve` owes the operator.
    #[error("the server's operator lines never completed: {0}")]
    Announcement(String),
}

/// The absolute path of a product binary in this build's `target/debug`.
///
/// Resolved from the running test executable (`target/debug/deps/<test>` →
/// `target/debug/<name>`), which follows `CARGO_TARGET_DIR` wherever it
/// points. A missing binary is an error, never a skip: TESTING.md §1 forbids
/// degrading to green on missing infrastructure.
///
/// # Errors
///
/// [`FaultHarnessError::MissingBinary`] when the binary is not there and
/// [`FaultHarnessError::NoTargetDir`] when the anchor itself is missing.
pub fn product_binary(name: &str) -> Result<PathBuf, FaultHarnessError> {
    let own = std::env::current_exe()
        .map_err(|error| FaultHarnessError::NoTargetDir(error.to_string()))?;
    let debug_dir = own
        .parent() // deps/
        .and_then(Path::parent) // debug/
        .ok_or_else(|| {
            FaultHarnessError::NoTargetDir(format!("{} has no deps/debug parents", own.display()))
        })?;
    let candidate = debug_dir.join(name);
    if candidate.is_file() {
        return Ok(candidate);
    }
    Err(FaultHarnessError::MissingBinary {
        name: name.to_owned(),
        looked: candidate,
    })
}

/// What `horde-server --serve` prints on stdout, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorAnnouncement {
    pub main: SocketAddr,
    pub enrollment: SocketAddr,
    pub fingerprint: String,
    /// The one-token bootstrap line (ADR-0007), present exactly when the
    /// server minted a first-client code.
    pub bootstrap_token: Option<String>,
}

impl OperatorAnnouncement {
    /// The mTLS endpoint both spines share, in the form peers dial.
    #[must_use]
    pub fn main_endpoint(&self) -> String {
        format!("https://localhost:{}", self.main.port())
    }

    /// The TLS-only enrollment endpoint.
    #[must_use]
    pub fn enrollment_endpoint(&self) -> String {
        format!("https://localhost:{}", self.enrollment.port())
    }
}

/// Folds one operator line into a partial announcement. Unknown lines are
/// ignored — stdout is the operator interface, but holding the parser to
/// "exactly these lines, in order" would break on a new line nobody removed.
fn fold_operator_line(partial: &mut PartialAnnouncement, line: &str) {
    if let Some(rest) = line.strip_prefix("listening main=") {
        partial.main = rest.trim().parse().ok();
    } else if let Some(rest) = line.strip_prefix("listening enrollment=") {
        partial.enrollment = rest.trim().parse().ok();
    } else if let Some(rest) = line.strip_prefix("server-fingerprint ") {
        partial.fingerprint = Some(rest.trim().to_owned());
    } else if let Some(rest) = line.strip_prefix("bootstrap-client-token ") {
        partial.bootstrap_token = Some(rest.trim().to_owned());
    }
}

#[derive(Debug, Default)]
struct PartialAnnouncement {
    main: Option<SocketAddr>,
    enrollment: Option<SocketAddr>,
    fingerprint: Option<String>,
    bootstrap_token: Option<String>,
}

impl PartialAnnouncement {
    /// Whether everything owed has arrived. The bootstrap token is owed only
    /// when the caller expects one — a restart with an enrolled client prints
    /// three lines and stopping to wait for a fourth would hang on nothing.
    fn complete(&self, expect_bootstrap: bool) -> bool {
        self.main.is_some()
            && self.enrollment.is_some()
            && self.fingerprint.is_some()
            && (!expect_bootstrap || self.bootstrap_token.is_some())
    }

    fn conclude(self) -> Option<OperatorAnnouncement> {
        Some(OperatorAnnouncement {
            main: self.main?,
            enrollment: self.enrollment?,
            fingerprint: self.fingerprint?,
            bootstrap_token: self.bootstrap_token,
        })
    }
}

/// Parses the operator lines a `--serve` start printed.
///
/// # Errors
///
/// [`FaultHarnessError::Announcement`] naming what never arrived.
pub fn parse_operator_lines(
    lines: &[String],
    expect_bootstrap: bool,
) -> Result<OperatorAnnouncement, FaultHarnessError> {
    let mut partial = PartialAnnouncement::default();
    for line in lines {
        fold_operator_line(&mut partial, line);
    }
    if !partial.complete(expect_bootstrap) {
        return Err(FaultHarnessError::Announcement(format!(
            "main={:?} enrollment={:?} fingerprint={:?} bootstrap={:?} (expected bootstrap: \
             {expect_bootstrap}) from {lines:?}",
            partial.main, partial.enrollment, partial.fingerprint, partial.bootstrap_token
        )));
    }
    partial
        .conclude()
        .ok_or_else(|| FaultHarnessError::Announcement("incomplete announcement".to_owned()))
}

/// How to start (or restart) a real `horde-server` process.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// The SQLite log — the only thing a `kill -9` is allowed to preserve.
    pub database: PathBuf,
    /// The CA, credential and blob state directory.
    pub state_dir: PathBuf,
    /// Fixed binds, so a restarted server answers on the addresses its
    /// workers and clients are already dialing.
    pub bind: SocketAddr,
    pub enrollment_bind: SocketAddr,
}

/// A real serving child process — the production startup stack behind the
/// same `--serve` command line — killable without ceremony.
#[derive(Debug)]
pub struct ServerProcess {
    child: Child,
    announcement: OperatorAnnouncement,
}

impl ServerProcess {
    /// Spawns the real binary and waits (bounded by line count; the caller
    /// ceilings the wall clock) for its operator lines.
    ///
    /// # Errors
    ///
    /// The binary missing, the spawn failing, or the announcement never
    /// completing before stdout ends.
    pub async fn spawn(
        config: &ServerConfig,
        expect_bootstrap: bool,
    ) -> Result<Self, FaultHarnessError> {
        let arguments = vec![
            config.database.display().to_string(),
            "--serve".to_owned(),
            "--state-dir".to_owned(),
            config.state_dir.display().to_string(),
            "--bind".to_owned(),
            config.bind.to_string(),
            "--enrollment-bind".to_owned(),
            config.enrollment_bind.to_string(),
        ];
        // The fault-server fixture: `horde_server::startup`'s parse, serve
        // and announce, in a binary this package's own test build always
        // produces — see its module docs for why the shipped binary's
        // `[[bin]]` cannot be relied on here.
        let mut child = spawn_product("fault-server", &arguments, Stdio::piped())?;

        let stdout = child.stdout.take().ok_or_else(|| {
            FaultHarnessError::Announcement("the child has no piped stdout".to_owned())
        })?;
        let mut lines = BufReader::new(stdout).lines();
        let mut seen = Vec::new();
        let mut partial = PartialAnnouncement::default();
        // Count-bounded: the operator interface is four lines; sixteen leaves
        // room for growth without ever spinning on a chatty stream.
        for _ in 0_u32..16 {
            if partial.complete(expect_bootstrap) {
                break;
            }
            match lines.next_line().await {
                Ok(Some(line)) => {
                    fold_operator_line(&mut partial, &line);
                    seen.push(line);
                }
                Ok(None) => break,
                Err(error) => {
                    return Err(FaultHarnessError::Announcement(format!(
                        "stdout failed after {seen:?}: {error}"
                    )));
                }
            }
        }
        let announcement = parse_operator_lines(&seen, expect_bootstrap)?;
        Ok(Self {
            child,
            announcement,
        })
    }

    /// What the server announced at startup.
    #[must_use]
    pub fn announcement(&self) -> &OperatorAnnouncement {
        &self.announcement
    }

    /// SIGKILL, then reap: no shutdown, no goodbye — the log on disk is what
    /// survives. Returns how the child ended, so a scenario can hold the
    /// kill to its word (`ExitStatusExt::signal() == Some(9)`): a kill that
    /// answered `Ok` without killing is the injector lying about its fault.
    ///
    /// # Errors
    ///
    /// The signal or the reap failing.
    pub async fn kill_now(mut self) -> Result<std::process::ExitStatus, FaultHarnessError> {
        kill_child(&mut self.child, "fault-server").await
    }
}

/// One scripted turn of the worker-runner's FakeAdapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnSpec {
    /// Output emitted as soon as the turn opens — the observable "mid-turn"
    /// marker a kill or a cut is aimed after.
    pub opens_with: Option<String>,
    /// A stall after the opener, holding the turn deliberately in flight.
    pub stall_ms: Option<u64>,
    /// The turn's final answer.
    pub answer: String,
}

impl TurnSpec {
    /// A turn that just answers.
    #[must_use]
    pub fn answering(answer: impl Into<String>) -> Self {
        Self {
            opens_with: None,
            stall_ms: None,
            answer: answer.into(),
        }
    }

    /// A turn that announces itself, then hangs in flight for `stall_ms`.
    #[must_use]
    pub fn opening_then_stalling(
        opens_with: impl Into<String>,
        stall_ms: u64,
        answer: impl Into<String>,
    ) -> Self {
        Self {
            opens_with: Some(opens_with.into()),
            stall_ms: Some(stall_ms),
            answer: answer.into(),
        }
    }

    /// The `--turn` encoding the runner parses: `answer`, or
    /// `opener>stall_ms>answer`. `>` is the separator and may not appear in
    /// the texts — the runner is fixture, not a codec.
    #[must_use]
    pub fn encode(&self) -> String {
        match (&self.opens_with, self.stall_ms) {
            (Some(opener), Some(stall)) => format!("{opener}>{stall}>{}", self.answer),
            _ => self.answer.clone(),
        }
    }
}

/// How to start (or restart) a worker-runner process.
#[derive(Debug, Clone)]
pub struct WorkerRunnerConfig {
    /// The server endpoint the runner dials (possibly a fault proxy's).
    pub endpoint: String,
    pub worker_id: String,
    /// The journal file — what survives a `kill -9` and seeds the next life.
    pub journal: PathBuf,
    /// The FakeAdapter script, one entry per scripted turn.
    pub turns: Vec<TurnSpec>,
    /// An enrolled identity directory for an mTLS dial; `None` dials
    /// plaintext, which only loopback test servers accept.
    pub identity_dir: Option<PathBuf>,
}

/// The runner's argument vector for `config` — pure, so the encoding the
/// suite relies on is a truth table rather than a spawn away.
#[must_use]
pub fn runner_args(config: &WorkerRunnerConfig) -> Vec<String> {
    let mut args = vec![
        "--endpoint".to_owned(),
        config.endpoint.clone(),
        "--worker-id".to_owned(),
        config.worker_id.clone(),
        "--journal".to_owned(),
        config.journal.display().to_string(),
    ];
    for turn in &config.turns {
        args.push("--turn".to_owned());
        args.push(turn.encode());
    }
    if let Some(identity) = &config.identity_dir {
        args.push("--identity".to_owned());
        args.push(identity.display().to_string());
    }
    args
}

/// A real worker process: the production worker library in its own process,
/// killable mid-turn.
#[derive(Debug)]
pub struct WorkerProcess {
    child: Child,
}

impl WorkerProcess {
    /// Spawns the runner. It dials, registers and redials on its own; the
    /// suite observes it through the server it connects to.
    ///
    /// # Errors
    ///
    /// The fixture binary missing or the spawn failing.
    pub fn spawn(config: &WorkerRunnerConfig) -> Result<Self, FaultHarnessError> {
        let child = spawn_product("worker-runner", &runner_args(config), Stdio::null())?;
        Ok(Self { child })
    }

    /// The child's OS pid, while it runs — what a suite probes (`kill -0`)
    /// to prove a kill was a kill and not a harness that answered `Ok` over
    /// a process still breathing.
    #[must_use]
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }

    /// SIGKILL mid-whatever, then reap. The journal file is what survives.
    /// Returns how the child ended, as [`ServerProcess::kill_now`] does and
    /// for the same reason: the kill is the injected fault, and a scenario
    /// holds it to `signal() == Some(9)`.
    ///
    /// # Errors
    ///
    /// The signal or the reap failing.
    pub async fn kill_now(mut self) -> Result<std::process::ExitStatus, FaultHarnessError> {
        kill_child(&mut self.child, "worker-runner").await
    }
}

/// The fault-server fixture's invocation: the production parser
/// (`horde_server::startup::invocation`), narrowed to the one shape the
/// fixture serves. Lives here rather than in the binary because a binary's
/// functions are reachable only by running the binary — which coverage
/// cannot see (the same argument `horde-server`'s own `startup` makes).
///
/// # Errors
///
/// The production parser's refusal, or a command line that did not ask to
/// serve.
pub fn fixture_serve_config(
    arguments: impl Iterator<Item = String>,
) -> Result<horde_server::startup::ServeConfig, String> {
    match horde_server::startup::invocation(arguments)? {
        horde_server::startup::Invocation::Serve(config) => Ok(*config),
        _ => Err("this fixture only serves; pass --serve".to_owned()),
    }
}

/// Spawns one product binary as a killable fixture child: stderr discarded,
/// stdout as the caller needs it, and `kill_on_drop` so a panicking test
/// cannot leak the process.
fn spawn_product(
    name: &str,
    arguments: &[String],
    stdout: Stdio,
) -> Result<Child, FaultHarnessError> {
    let binary = product_binary(name)?;
    Command::new(&binary)
        .args(arguments)
        .stdout(stdout)
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|source| FaultHarnessError::Process {
            doing: format!("spawn {}", binary.display()),
            source,
        })
}

/// SIGKILL by handle, never by name or pattern (the supervision rule the
/// worker itself lives by), then reap so no zombie outlives the test.
///
/// Returning the reaped [`std::process::ExitStatus`] is load-bearing: it is
/// how the wrappers stay mutation-honest. `kill_on_drop` means a body
/// replaced with a bare `Ok` would *still* end the child — at drop, not at
/// the call — which is behaviorally close enough to slip a scenario. An
/// `ExitStatus` cannot be conjured from nothing, so the only way to answer
/// is to have actually waited.
async fn kill_child(
    child: &mut Child,
    name: &str,
) -> Result<std::process::ExitStatus, FaultHarnessError> {
    child
        .start_kill()
        .map_err(|source| FaultHarnessError::Process {
            doing: format!("SIGKILL {name}"),
            source,
        })?;
    child
        .wait()
        .await
        .map_err(|source| FaultHarnessError::Process {
            doing: format!("reap {name}"),
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::{
        OperatorAnnouncement, TurnSpec, WorkerRunnerConfig, parse_operator_lines, runner_args,
    };

    fn lines(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn a_fresh_start_parses_all_four_operator_lines() {
        let parsed = parse_operator_lines(
            &lines(&[
                "listening main=127.0.0.1:7777",
                "listening enrollment=127.0.0.1:7778",
                "server-fingerprint abc123",
                "bootstrap-client-token deadbeef",
            ]),
            true,
        )
        .expect("a complete announcement parses");
        assert_eq!(
            parsed,
            OperatorAnnouncement {
                main: "127.0.0.1:7777".parse().expect("an address"),
                enrollment: "127.0.0.1:7778".parse().expect("an address"),
                fingerprint: "abc123".to_owned(),
                bootstrap_token: Some("deadbeef".to_owned()),
            }
        );
        assert_eq!(parsed.main_endpoint(), "https://localhost:7777");
        assert_eq!(parsed.enrollment_endpoint(), "https://localhost:7778");
    }

    #[test]
    fn a_restart_with_an_enrolled_client_needs_no_bootstrap_line() {
        let three = lines(&[
            "listening main=127.0.0.1:7777",
            "listening enrollment=127.0.0.1:7778",
            "server-fingerprint abc123",
        ]);
        let parsed = parse_operator_lines(&three, false).expect("three lines suffice");
        assert_eq!(parsed.bootstrap_token, None);

        // The same three lines are NOT enough when a bootstrap is owed: a
        // fresh server that failed to mint would otherwise pass silently.
        let refused = parse_operator_lines(&three, true);
        assert!(
            refused.is_err(),
            "an owed bootstrap code must be waited for, not shrugged off"
        );
    }

    #[test]
    fn a_missing_line_is_refused_naming_what_never_arrived() {
        let refused = parse_operator_lines(&lines(&["listening main=127.0.0.1:7777"]), false);
        let message = match refused {
            Err(error) => error.to_string(),
            Ok(parsed) => panic!("an incomplete announcement must not parse: {parsed:?}"),
        };
        assert!(
            message.contains("enrollment=None") && message.contains("fingerprint=None"),
            "the refusal names the gaps: {message}"
        );
    }

    /// The fixture parses the same command line the shipped binary does —
    /// through the shipped binary's own parser — and refuses to be anything
    /// but a server.
    #[test]
    fn the_fixture_invocation_is_the_production_serve_line() {
        let config = super::fixture_serve_config(
            [
                "/tmp/x.db",
                "--serve",
                "--bind",
                "127.0.0.1:7071",
                "--enrollment-bind",
                "127.0.0.1:7072",
                "--state-dir",
                "/tmp/state",
            ]
            .iter()
            .map(|s| (*s).to_owned()),
        )
        .expect("a serve line parses");
        assert_eq!(config.database, "/tmp/x.db");
        assert_eq!(config.bind.to_string(), "127.0.0.1:7071");
        assert_eq!(config.enrollment_bind.to_string(), "127.0.0.1:7072");
        assert_eq!(config.state_dir, std::path::PathBuf::from("/tmp/state"));

        let bare =
            super::fixture_serve_config(["--check".to_owned(), "/tmp/x.db".to_owned()].into_iter())
                .expect_err("the health-check shape is refused");
        assert!(bare.contains("--serve"), "the refusal says what to pass");

        let wrong = super::fixture_serve_config(["--bogus".to_owned()].into_iter())
            .expect_err("the production parser's refusals pass through");
        assert!(wrong.contains("--bogus"), "naming the flag: {wrong}");
    }

    #[test]
    fn turn_specs_encode_the_shape_the_runner_parses() {
        assert_eq!(TurnSpec::answering("done").encode(), "done");
        assert_eq!(
            TurnSpec::opening_then_stalling("opened", 30_000, "answer").encode(),
            "opened>30000>answer"
        );
    }

    #[test]
    fn runner_args_carry_every_field_in_flag_order() {
        let config = WorkerRunnerConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            worker_id: "worker-a".to_owned(),
            journal: "/tmp/j.sqlite".into(),
            turns: vec![
                TurnSpec::opening_then_stalling("opened", 5, "one"),
                TurnSpec::answering("two"),
            ],
            identity_dir: Some("/tmp/id".into()),
        };
        assert_eq!(
            runner_args(&config),
            vec![
                "--endpoint",
                "http://127.0.0.1:1",
                "--worker-id",
                "worker-a",
                "--journal",
                "/tmp/j.sqlite",
                "--turn",
                "opened>5>one",
                "--turn",
                "two",
                "--identity",
                "/tmp/id",
            ]
        );

        let bare = WorkerRunnerConfig {
            identity_dir: None,
            turns: Vec::new(),
            ..config
        };
        let args = runner_args(&bare);
        assert!(
            !args
                .iter()
                .any(|arg| arg == "--identity" || arg == "--turn"),
            "absent options must stay absent: {args:?}"
        );
    }
}
