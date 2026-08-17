//! What the `horde-worker` binary does, in a form that can be tested.
//!
//! `bin/horde_worker.rs` is left as subscriber setup, signal wiring and an
//! exit code, exactly like `horde-server`'s binary: a binary's own functions
//! are reachable only by running the binary, which coverage cannot see, so
//! every decision lives here where the suite can hold it.
//!
//! # What the daemon is
//!
//! The third binary of DESIGN.md §2, standing up the Worker this crate has so
//! far only run inside tests: it enrolls (or loads) an identity, dials the
//! Server over mTLS, registers the capabilities its flags configured, and then
//! runs the full [`Runtime`] — spawns, journaling, revival, steering,
//! interrupts, workspaces, payload shares — until a termination signal ends it
//! cleanly. Reconnection is this module's policy (the [`Connection`] docs
//! place it with the caller): redial forever on transport weather, stop loudly
//! on refusals that a redial cannot heal.
//!
//! # The fake adapter is a product surface here
//!
//! `--adapter fake` runs the scripted provider TESTING.md §3 calls
//! load-bearing. The end-to-end suite runs this daemon for real, and a suite
//! that cannot run it hermetically would burn tokens or not exist — the same
//! reasoning that ships an alternate backend in any system with one.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use horde_adapter::{TokenUsage, TurnStatus};
use horde_adapter_fake::{FakeAdapter, Script, ScriptedTurn, Step};
use horde_identity::StoredIdentity;

use crate::capabilities;
use crate::client::{Connection, DialConfig};
use crate::error::WorkerError;
use crate::journal::Journal;
use crate::runtime::Runtime;

/// Where a Server is expected when the operator does not say.
pub const DEFAULT_SERVER: &str = "https://127.0.0.1:7777";

/// Where identity and journal live when the operator does not say.
pub const DEFAULT_STATE_DIR: &str = "horde-worker-state";

/// The journal's file name inside the state directory.
pub const JOURNAL_FILE: &str = "journal.sqlite3";

/// The identity directory inside the state directory.
pub const IDENTITY_DIR: &str = "identity";

/// The display name used before any identity names this Worker.
pub const DEFAULT_NAME: &str = "horde-worker";

/// How long the daemon waits before redialing a Server it lost.
///
/// A constant rather than a backoff schedule on purpose: the Worker is
/// designed for spotty networks (DESIGN.md §2), a half-second is slow enough
/// not to be a hot spin and fast enough that reconciliation feels immediate,
/// and a schedule would be tuning nobody has measured a need for.
pub const RETRY_DELAY: core::time::Duration = core::time::Duration::from_millis(500);

/// A provider harness the daemon can register.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterChoice {
    /// The scripted hermetic provider (TESTING.md §3).
    Fake,
    /// The Claude Code harness.
    Claude,
    /// The Codex app-server harness.
    Codex,
}

impl AdapterChoice {
    /// Parses one `--adapter` value, refusing anything unknown by name.
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "fake" => Ok(Self::Fake),
            "claude" => Ok(Self::Claude),
            "codex" => Ok(Self::Codex),
            other => Err(format!(
                "--adapter {other} is not one of fake, claude, codex"
            )),
        }
    }
}

/// Everything one command line asks the daemon to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    /// The Server's main (mTLS) endpoint.
    pub server: String,
    /// Holds `identity/` and the journal. Losing it loses both.
    pub state_dir: PathBuf,
    /// A one-time enrollment code (`secret.fingerprint`) to redeem first.
    pub join: Option<String>,
    /// The TLS-only enrollment listener `--join` redeems against.
    pub enrollment: Option<String>,
    /// The display name, and the plaintext worker id under `--insecure`.
    /// An enrolled identity's principal always outranks it.
    pub name: Option<String>,
    /// Which harnesses to register, in order; the first is the default
    /// provider for spawns that name none.
    pub adapters: Vec<AdapterChoice>,
    /// A JSON script for the fake adapter; absent means the echo script.
    pub fake_script: Option<PathBuf>,
    /// Enables workspace support rooted here.
    pub workspace_root: Option<PathBuf>,
    /// The git Relay payload shares stage to and fetch through.
    pub relay: Option<String>,
    /// Dial plaintext with no identity — the in-process-test-server shape.
    pub insecure: bool,
}

/// What the flags said, before the shape rules are applied.
#[derive(Debug, Default)]
struct ParsedFlags {
    server: Option<String>,
    state_dir: Option<PathBuf>,
    join: Option<String>,
    enrollment: Option<String>,
    name: Option<String>,
    adapters: Vec<AdapterChoice>,
    fake_script: Option<PathBuf>,
    workspace_root: Option<PathBuf>,
    relay: Option<String>,
    insecure: bool,
}

/// Whether `flag` is one of the daemon's value-carrying flags.
///
/// Decided *before* the value is consumed: an unknown flag must be refused
/// as unknown, not have the next argument eaten and then complain about a
/// missing value for a flag that never existed.
fn takes_value(flag: &str) -> bool {
    matches!(
        flag,
        "--server"
            | "--state-dir"
            | "--join"
            | "--enrollment"
            | "--name"
            | "--adapter"
            | "--fake-script"
            | "--workspace-root"
            | "--relay"
    )
}

/// Consumes the next argument as `flag`'s value, refusing blanks.
fn take_value(flag: &str, arguments: &mut impl Iterator<Item = String>) -> Result<String, String> {
    arguments
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{flag} needs a value"))
}

impl ParsedFlags {
    /// Folds one argument into the parse. Every flag except `--insecure`
    /// carries a value; a mistyped flag silently ignored would dial the
    /// wrong server, so unknown ones are refusals by name.
    fn apply(
        &mut self,
        argument: &str,
        arguments: &mut impl Iterator<Item = String>,
    ) -> Result<(), String> {
        if argument == "--insecure" {
            self.insecure = true;
            return Ok(());
        }
        if !takes_value(argument) {
            return Err(format!("unknown argument {argument}"));
        }
        let value = take_value(argument, arguments)?;
        self.apply_identity_value(argument, value)
    }

    /// Applies the flags that decide who and where this Worker is; the rest
    /// fall through to [`Self::apply_runtime_value`]. Split by subject so
    /// neither half outgrows the complexity bar as the surface grows.
    fn apply_identity_value(&mut self, flag: &str, value: String) -> Result<(), String> {
        match flag {
            "--server" => self.server = Some(value),
            "--state-dir" => self.state_dir = Some(PathBuf::from(value)),
            "--join" => self.join = Some(value),
            "--enrollment" => self.enrollment = Some(value),
            "--name" => self.name = Some(value),
            _ => return self.apply_runtime_value(flag, value),
        }
        Ok(())
    }

    /// Applies the flags that decide what this Worker runs.
    fn apply_runtime_value(&mut self, flag: &str, value: String) -> Result<(), String> {
        match flag {
            "--adapter" => {
                let choice = AdapterChoice::parse(&value)?;
                if self.adapters.contains(&choice) {
                    return Err(format!("--adapter {choice:?} was given twice").to_lowercase());
                }
                self.adapters.push(choice);
            }
            "--fake-script" => self.fake_script = Some(PathBuf::from(value)),
            "--workspace-root" => self.workspace_root = Some(PathBuf::from(value)),
            "--relay" => self.relay = Some(value),
            // Unreachable while `takes_value` and these two matches agree;
            // refusing (rather than ignoring) is what keeps a future drift
            // between them loud.
            other => return Err(format!("unknown argument {other}")),
        }
        Ok(())
    }

    /// Applies the shape rules and defaults, yielding the configuration.
    fn conclude(self) -> Result<WorkerConfig, String> {
        let config = WorkerConfig {
            server: self.server.unwrap_or_else(|| DEFAULT_SERVER.to_owned()),
            state_dir: self
                .state_dir
                .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE_DIR)),
            join: self.join,
            enrollment: self.enrollment,
            name: self.name,
            adapters: self.adapters,
            fake_script: self.fake_script,
            workspace_root: self.workspace_root,
            relay: self.relay,
            insecure: self.insecure,
        };
        config.check_shape()?;
        Ok(config)
    }
}

impl WorkerConfig {
    /// The rules that make a parse a usable configuration.
    ///
    /// Each is a refusal rather than a repair: joining without an enrollment
    /// listener has nowhere to go, an insecure join is a contradiction (the
    /// point of joining is the certificate), a fake script without the fake
    /// adapter configures nothing, and a scheme that disagrees with the
    /// security mode would otherwise fail later with a TLS error that names
    /// neither flag.
    fn check_shape(&self) -> Result<(), String> {
        if self.join.is_some() != self.enrollment.is_some() {
            return Err("--join and --enrollment go together".to_owned());
        }
        if self.join.is_some() && self.insecure {
            return Err("--join enrolls an identity; it cannot combine with --insecure".to_owned());
        }
        if self.fake_script.is_some() && !self.adapters.contains(&AdapterChoice::Fake) {
            return Err("--fake-script needs --adapter fake".to_owned());
        }
        if self.insecure && !self.server.starts_with("http://") {
            return Err(format!(
                "--insecure dials plaintext, so --server must be http://…, not {}",
                self.server
            ));
        }
        if !self.insecure && !self.server.starts_with("https://") {
            return Err(format!(
                "an enrolled dial is mTLS, so --server must be https://…, not {} \
                 (or pass --insecure for a plaintext test server)",
                self.server
            ));
        }
        Ok(())
    }

    /// Where this configuration keeps its identity.
    #[must_use]
    pub fn identity_dir(&self) -> PathBuf {
        self.state_dir.join(IDENTITY_DIR)
    }

    /// Where this configuration keeps its journal.
    #[must_use]
    pub fn journal_path(&self) -> PathBuf {
        self.state_dir.join(JOURNAL_FILE)
    }
}

/// Parses the command line.
///
/// Pure so every flag combination is testable without a process. Unknown
/// flags are errors rather than ignored, exactly as in `horde-server`.
///
/// # Errors
///
/// A human-readable refusal naming the flag that was wrong.
pub fn invocation(arguments: impl IntoIterator<Item = String>) -> Result<WorkerConfig, String> {
    let mut stream = arguments.into_iter();
    let mut flags = ParsedFlags::default();
    loop {
        let Some(argument) = stream.next() else {
            return flags.conclude();
        };
        flags.apply(&argument, &mut stream)?;
    }
}

/// Parses a fake-adapter script from its JSON form.
///
/// The [`Script`] type is test vocabulary; this is its operator form, kept
/// deliberately small: an array of turns, each either a bare string (a turn
/// that says it and finishes) or an object `{"steps": […], "text": …}` whose
/// steps are `{"say": …}`, `{"think": …}` or
/// `{"ask_permission": {"id": …, "tool": …, "input": …}}`. After the scripted
/// turns run out the agent echoes, which is what the revival and steering
/// scenarios need from a conversation of unknowable length.
///
/// # Errors
///
/// A refusal naming the element that did not parse.
pub fn fake_script(json: &str) -> Result<Script, String> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|error| format!("the script is not JSON: {error}"))?;
    let turns = value
        .as_array()
        .ok_or("the script must be a JSON array of turns")?;
    let turns = turns
        .iter()
        .enumerate()
        .map(|(index, turn)| {
            scripted_turn(turn).map_err(|reason| format!("turn {index}: {reason}"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Script::of(turns))
}

/// One JSON turn, in either of its two shapes.
fn scripted_turn(turn: &serde_json::Value) -> Result<ScriptedTurn, String> {
    if let Some(text) = turn.as_str() {
        return Ok(ScriptedTurn::saying(text));
    }
    let object = turn
        .as_object()
        .ok_or("a turn is a string or an object with steps")?;
    let steps = match object.get("steps") {
        None => Vec::new(),
        Some(steps) => steps
            .as_array()
            .ok_or("steps must be an array")?
            .iter()
            .map(scripted_step)
            .collect::<Result<Vec<_>, String>>()?,
    };
    let text = match object.get("text") {
        None => None,
        Some(text) => Some(text.as_str().ok_or("text must be a string")?.to_owned()),
    };
    Ok(ScriptedTurn {
        steps,
        text,
        status: TurnStatus::Completed,
        // The same nominal spend `ScriptedTurn::saying` charges: the script
        // format is about behavior, and usage plumbing has its own suites.
        usage: TokenUsage {
            input_tokens: 10,
            output_tokens: 20,
            ..TokenUsage::default()
        },
        cost_usd: 0.001,
    })
}

/// One JSON step: exactly one of the recognized keys.
fn scripted_step(step: &serde_json::Value) -> Result<Step, String> {
    let object = step.as_object().ok_or("a step must be an object")?;
    if let Some(text) = object.get("say") {
        return Ok(Step::Say(
            text.as_str().ok_or("say must be a string")?.to_owned(),
        ));
    }
    if let Some(text) = object.get("think") {
        return Ok(Step::Think(
            text.as_str().ok_or("think must be a string")?.to_owned(),
        ));
    }
    if let Some(ask) = object.get("ask_permission") {
        let (tool_call_id, tool_name, input) = tool_step_fields("ask_permission", ask)?;
        return Ok(Step::AskPermission {
            tool_call_id,
            tool_name,
            input,
        });
    }
    if let Some(call) = object.get("use_horde") {
        // A real MCP call against the agent's own endpoint — the operator
        // form of "here the agent reports its result" or "here it spawns a
        // child" (DESIGN §5's verbs).
        let (tool_call_id, tool_name, input) = tool_step_fields("use_horde", call)?;
        return Ok(Step::UseHordeTool {
            tool_call_id,
            tool_name,
            input,
        });
    }
    Err("a step must be one of say, think, ask_permission, use_horde".to_owned())
}

/// The `{id, tool, input}` triple both tool-shaped steps share.
fn tool_step_fields(
    step: &str,
    value: &serde_json::Value,
) -> Result<(String, String, serde_json::Value), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{step} must be an object"))?;
    let field = |name: &str| {
        object
            .get(name)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| format!("{step} needs a non-empty {name}"))
    };
    Ok((
        field("id")?,
        field("tool")?,
        object
            .get("input")
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new())),
    ))
}

/// How this run comes to hold an identity, decided before any I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityPlan {
    /// No identity: a plaintext dial to a loopback test server.
    Insecure,
    /// Load what an earlier join persisted.
    Load(PathBuf),
    /// Redeem a one-time code and persist the result.
    Join {
        enrollment: String,
        code: String,
        directory: PathBuf,
    },
}

/// Decides the identity plan from the configuration and one observed fact.
///
/// `already_enrolled` is passed in rather than probed here so the whole truth
/// table is testable without a filesystem. A join over an existing identity
/// is refused rather than resolved either way: silently loading would ignore
/// the operator's code, silently re-joining would discard a working identity
/// — both are the wrong thing done quietly.
///
/// # Errors
///
/// The join-over-existing refusal, naming the directory to clear.
pub fn identity_plan(
    config: &WorkerConfig,
    already_enrolled: bool,
) -> Result<IdentityPlan, String> {
    if config.insecure {
        return Ok(IdentityPlan::Insecure);
    }
    let directory = config.identity_dir();
    match (&config.join, &config.enrollment) {
        (Some(code), Some(enrollment)) => {
            if already_enrolled {
                return Err(format!(
                    "{} already holds an identity; remove it to re-enroll, \
                     or drop --join to use it",
                    directory.display()
                ));
            }
            Ok(IdentityPlan::Join {
                enrollment: enrollment.clone(),
                code: code.clone(),
                directory,
            })
        }
        _ => Ok(IdentityPlan::Load(directory)),
    }
}

/// Carries out an identity plan.
///
/// # Errors
///
/// A malformed or refused code, an unreachable enrollment listener, a
/// missing or unreadable identity directory — each named, with the fix.
pub async fn resolve_identity(
    plan: IdentityPlan,
    display_name: &str,
    announce: &mut dyn FnMut(String),
) -> Result<Option<StoredIdentity>, String> {
    match plan {
        IdentityPlan::Insecure => Ok(None),
        IdentityPlan::Load(directory) => {
            let identity = StoredIdentity::load(&directory).map_err(|error| {
                format!(
                    "no usable identity in {} ({error}); enroll with \
                     --join CODE --enrollment https://…",
                    directory.display()
                )
            })?;
            Ok(Some(identity))
        }
        IdentityPlan::Join {
            enrollment,
            code,
            directory,
        } => {
            let code = horde_identity::JoinCode::parse(&code).ok_or(
                "the join code is malformed; paste it exactly as the server \
                 printed it (secret.fingerprint)",
            )?;
            let enrolled =
                horde_identity::join(&enrollment, &code, capabilities::hello(display_name))
                    .await
                    .map_err(|error| format!("enrollment failed: {error}"))?;
            enrolled
                .identity
                .save(&directory)
                .map_err(|error| format!("could not persist the identity: {error}"))?;
            announce(format!(
                "enrolled principal={} identity={}",
                enrolled.principal.common_name(),
                directory.display()
            ));
            Ok(Some(enrolled.identity))
        }
    }
}

/// The name this Worker goes by: the enrolled principal's, or the flag's,
/// or the default — in that order, because the certificate is what the
/// Server will believe regardless of what the flag said.
#[must_use]
pub fn worker_name(identity: Option<&StoredIdentity>, configured: Option<&str>) -> String {
    identity
        .and_then(|identity| horde_identity::principal_of_pem(&identity.certificate_pem).ok())
        .map(|principal| principal.name().to_owned())
        .or_else(|| configured.map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_NAME.to_owned())
}

/// The enrolled principal held by a state directory, for stamping telemetry.
///
/// Best-effort by design, like the CLI's: enrichment must never turn an
/// absent identity into a refusal — the dial refuses loudly where it matters.
#[must_use]
pub fn enrolled_principal(state_dir: &Path) -> Option<String> {
    let stored = StoredIdentity::load(&state_dir.join(IDENTITY_DIR)).ok()?;
    horde_identity::principal_of_pem(&stored.certificate_pem)
        .ok()
        .map(|principal| principal.common_name())
}

/// A daemon ready to run: the Runtime built, the journal open, the dial
/// facts fixed.
pub struct PreparedWorker {
    pub runtime: Runtime,
    pub journal: Journal,
    /// The main endpoint every dial and the blob channel share.
    pub server: String,
    pub identity: Option<StoredIdentity>,
    /// The id claimed at registration and the display name in the hello.
    pub name: String,
}

impl std::fmt::Debug for PreparedWorker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedWorker")
            .field("server", &self.server)
            .field("name", &self.name)
            .field("enrolled", &self.identity.is_some())
            .finish_non_exhaustive()
    }
}

/// Brings the daemon to the ready-to-dial state: identity resolved, state
/// directory created, journal opened, adapters registered, workspace and
/// relay support wired.
///
/// # Errors
///
/// A startup refusal in operator words — nothing here is retried, because
/// everything here is local or a one-time enrollment.
pub async fn prepare(
    config: &WorkerConfig,
    announce: &mut dyn FnMut(String),
) -> Result<PreparedWorker, String> {
    let plan = identity_plan(config, StoredIdentity::exists(&config.identity_dir()))?;
    let provisional_name = config.name.as_deref().unwrap_or(DEFAULT_NAME);
    let identity = resolve_identity(plan, provisional_name, announce).await?;
    let name = worker_name(identity.as_ref(), config.name.as_deref());

    std::fs::create_dir_all(&config.state_dir).map_err(|error| {
        format!(
            "could not create the state directory {}: {error}",
            config.state_dir.display()
        )
    })?;
    let journal = Journal::open(&config.journal_path())
        .map_err(|error| format!("could not open the journal: {error}"))?;

    let runtime = build_runtime(config, identity.as_ref(), &name, journal.clone())?;
    Ok(PreparedWorker {
        runtime,
        journal,
        server: config.server.clone(),
        identity,
        name,
    })
}

/// Assembles the Runtime a configuration asks for.
fn build_runtime(
    config: &WorkerConfig,
    identity: Option<&StoredIdentity>,
    name: &str,
    journal: Journal,
) -> Result<Runtime, String> {
    // One lazy channel for everything that rides the Worker's own connection
    // to the Server: attachment transfer, and the per-agent MCP endpoints'
    // control-plane verbs. Lazy: a Server that is down at startup is the
    // reconnect loop's business, not a reason to refuse to start.
    let channel = server_channel(&config.server, identity)?;
    let mut runtime = Runtime::new(journal)
        .with_blob_service(channel.clone())
        // The wire ADR-0004 left to the Worker: every spawned agent gets its
        // Session's MCP endpoint and a minted identity pair.
        .with_agent_endpoints(channel);

    for adapter in &config.adapters {
        runtime = match adapter {
            AdapterChoice::Fake => {
                runtime.with_adapter(Arc::new(FakeAdapter::new(configured_script(config)?)))
            }
            AdapterChoice::Claude => runtime.with_adapter(Arc::new(crate::ClaudeAdapter::new())),
            AdapterChoice::Codex => runtime.with_adapter(Arc::new(crate::CodexAdapter::new())),
        };
    }

    if let Some(root) = &config.workspace_root {
        runtime = runtime.with_workspaces(crate::WorkspaceManager::new(name, root.clone()));
    }
    if let Some(relay) = &config.relay {
        runtime = runtime.with_relay(horde_relay::RelayLocation::new(relay));
    }
    Ok(runtime)
}

/// The fake adapter's script: the file's, or the echo default.
fn configured_script(config: &WorkerConfig) -> Result<Script, String> {
    match &config.fake_script {
        None => Ok(Script::echoing()),
        Some(path) => {
            let json = std::fs::read_to_string(path).map_err(|error| {
                format!("could not read the fake script {}: {error}", path.display())
            })?;
            fake_script(&json)
        }
    }
}

/// The lazily-connecting channel the blob service and the agent endpoints
/// share.
fn server_channel(
    server: &str,
    identity: Option<&StoredIdentity>,
) -> Result<tonic::transport::Channel, String> {
    let endpoint = tonic::transport::Endpoint::from_shared(server.to_owned())
        .map_err(|error| format!("--server {server} is not a usable endpoint: {error}"))?;
    let endpoint = match identity {
        Some(identity) => {
            let domain = endpoint.uri().host().unwrap_or("localhost").to_owned();
            endpoint
                .tls_config(horde_identity::tls::grpc_client_mtls(identity, &domain))
                .map_err(|error| format!("the identity was refused for TLS: {error}"))?
        }
        None => endpoint,
    };
    Ok(endpoint.connect_lazy())
}

/// The dial for one connection attempt, advertisement rebuilt fresh so a
/// registration after a materialize advertises the instances now on disk.
async fn fresh_dial_config(prepared: &PreparedWorker) -> DialConfig {
    let warm = match prepared.runtime.workspaces() {
        Some(workspaces) => Some(workspaces.warm_instances().await),
        None => None,
    };
    DialConfig {
        endpoint: prepared.server.clone(),
        worker_id: prepared.name.clone(),
        hello: capabilities::hello(&prepared.name),
        capabilities: capabilities::advertise(prepared.runtime.adapters(), warm),
        identity: prepared.identity.clone(),
    }
}

/// What one connection's end means for the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fate {
    /// Wait [`RETRY_DELAY`] and dial again — transport weather, a Server
    /// restart, a stream the Server closed.
    Redial(String),
    /// Stop the daemon with this reason — a refusal a redial cannot heal.
    Fatal(String),
}

/// Classifies how a dial or a connection ended.
///
/// The split is what keeps a revoked Worker from hammering the door forever
/// while a partitioned one reconnects promptly: transport failures and
/// retryable RPC statuses redial; an authentication or contract refusal —
/// and every local defect, journal corruption included — stops the daemon,
/// because retrying local corruption re-runs the corruption.
#[must_use]
pub fn fate_of(outcome: &Result<(), WorkerError>) -> Fate {
    match outcome {
        Ok(()) => Fate::Redial("the server closed the stream".to_owned()),
        Err(error @ WorkerError::Transport(_)) => Fate::Redial(error.to_string()),
        Err(WorkerError::Rpc(status)) => match status.code() {
            tonic::Code::Unauthenticated
            | tonic::Code::PermissionDenied
            | tonic::Code::InvalidArgument
            | tonic::Code::FailedPrecondition
            | tonic::Code::Unimplemented => Fate::Fatal(format!("the server refused: {status}")),
            _ => Fate::Redial(format!("the connection failed: {status}")),
        },
        Err(error) => Fate::Fatal(error.to_string()),
    }
}

/// The whole binary lifecycle except argv and signals: builds the tokio
/// runtime, brings up telemetry (DESIGN §12) stamped with the enrolled
/// principal, installs the stderr subscriber, runs `main_loop`, and flushes
/// telemetry on the way out.
///
/// Lives here rather than in `bin/horde_worker.rs` so the ordering decisions
/// — runtime before telemetry (the OTLP exporter's channel lives on it),
/// logs to stderr (stdout is the operator interface), flush before exit but
/// never letting a dead collector change an exit code — are all testable.
///
/// # Errors
///
/// A startup refusal: the runtime, the telemetry configuration, or the
/// subscriber slot (already claimed means a second init, which must refuse
/// rather than silently swap subscribers).
pub fn with_observability(
    state_dir: &Path,
    main_loop: impl FnOnce(&tokio::runtime::Runtime) -> std::process::ExitCode,
) -> Result<std::process::ExitCode, String> {
    // Who the spans speak for is known before any runtime exists; the
    // runtime still comes before telemetry, because the OTLP exporter's
    // gRPC channel has to live on it.
    let identity = horde_observe::ServiceIdentity::new("horde-worker")
        .with_principal(enrolled_principal(state_dir));
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => return Err(format!("could not build a runtime: {error}")),
    };
    let entered = runtime.enter();
    let telemetry = horde_observe::init_from_env(identity, |name| std::env::var(name).ok())
        .map_err(|error| error.to_string())?;
    drop(entered);
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,horde_worker=debug"));
    tracing::dispatcher::set_global_default(horde_observe::stderr_subscriber(
        filter,
        telemetry.as_ref(),
    ))
    .map_err(|error| format!("could not install the subscriber: {error}"))?;

    let code = main_loop(&runtime);

    if let Some(telemetry) = telemetry {
        // With the runtime still alive for the flush to travel its channel.
        // The verdict is discarded deliberately: a collector that is down
        // must not change an exit code.
        let _ = telemetry.shutdown();
    }
    Ok(code)
}

/// Resolves when either future does — the daemon's two-signal wait
/// (SIGTERM or ctrl-c), kept here so the binary's own functions stay free
/// of macro-expanded branching nothing can test.
pub async fn first_of(a: impl Future<Output = ()>, b: impl Future<Output = ()>) {
    tokio::select! {
        () = a => {}
        () = b => {}
    }
}

/// Runs the daemon: dial, register, serve, redial — until `shutdown`
/// completes or a fatal refusal stops it.
///
/// The Runtime lives across connections, which is the whole point: agents
/// keep running and the journal keeps filling while the Server is away, and
/// each redial resumes both streams from what this Worker durably recorded.
/// On the way out — signalled or fatal alike — every pump is reaped, and the
/// count is announced because "shutdown finished" and "shutdown reaped the
/// agents" are different facts.
///
/// `announce` receives the operator lines (stable, greppable keys, matching
/// `horde-server`'s convention): `registered worker=… resume=…` per
/// connection, `disconnected …`/`dial-failed …` per loss, and one final
/// `shutdown reaped=…`.
///
/// # Errors
///
/// The fatal reason, after the agents were reaped.
pub async fn run(
    prepared: PreparedWorker,
    shutdown: impl Future<Output = ()>,
    announce: &mut dyn FnMut(String),
) -> Result<(), String> {
    tokio::pin!(shutdown);
    let mut prepared = prepared;
    let failure = connection_loop(&mut prepared, shutdown.as_mut(), announce).await;

    let reaped = prepared.runtime.shutdown().await;
    announce(format!("shutdown reaped={reaped}"));
    match failure {
        None => Ok(()),
        Some(reason) => Err(reason),
    }
}

/// The dial-serve-redial loop. `None` means shutdown was requested;
/// `Some(reason)` a fatal end.
async fn connection_loop(
    prepared: &mut PreparedWorker,
    mut shutdown: core::pin::Pin<&mut impl Future<Output = ()>>,
    announce: &mut dyn FnMut(String),
) -> Option<String> {
    loop {
        let config = fresh_dial_config(prepared).await;
        let dialed = tokio::select! {
            () = shutdown.as_mut() => return None,
            dialed = Connection::dial(&config) => dialed,
        };
        let (mut connection, registration) = match dialed {
            Ok(dialed) => dialed,
            Err(error) => match fate_of(&Err(error)) {
                Fate::Fatal(reason) => return Some(reason),
                Fate::Redial(reason) => {
                    announce(format!("dial-failed reason={reason}"));
                    if pause_before_redial(shutdown.as_mut()).await {
                        continue;
                    }
                    return None;
                }
            },
        };
        announce(format!(
            "registered worker={} resume={}",
            connection.worker_id(),
            registration.resume_from_journal_sequence
        ));

        let ended = tokio::select! {
            () = shutdown.as_mut() => return None,
            ended = connection.run(&prepared.journal, &mut prepared.runtime) => ended,
        };
        match fate_of(&ended) {
            Fate::Fatal(reason) => return Some(reason),
            Fate::Redial(reason) => {
                announce(format!("disconnected reason={reason}"));
                if !pause_before_redial(shutdown.as_mut()).await {
                    return None;
                }
            }
        }
    }
}

/// Parks for [`RETRY_DELAY`] unless shutdown arrives first.
///
/// `true` means "keep going"; `false` means shutdown won the wait.
async fn pause_before_redial(shutdown: core::pin::Pin<&mut impl Future<Output = ()>>) -> bool {
    tokio::select! {
        () = shutdown => false,
        () = tokio::time::sleep(RETRY_DELAY) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdapterChoice, DEFAULT_NAME, DEFAULT_SERVER, DEFAULT_STATE_DIR, Fate, IdentityPlan,
        WorkerConfig, fake_script, fate_of, identity_plan, invocation, worker_name,
    };
    use crate::error::WorkerError;

    fn parsed(arguments: &[&str]) -> Result<WorkerConfig, String> {
        invocation(arguments.iter().map(|s| (*s).to_owned()))
    }

    #[test]
    fn a_bare_invocation_takes_every_default() {
        let config = parsed(&[]).expect("no flags is a valid daemon");
        assert_eq!(config.server, DEFAULT_SERVER);
        assert_eq!(
            config.state_dir,
            std::path::PathBuf::from(DEFAULT_STATE_DIR)
        );
        assert!(
            config.adapters.is_empty(),
            "no adapter is honest: nothing is advertised"
        );
        assert!(!config.insecure);
        assert!(config.join.is_none() && config.enrollment.is_none());
        assert!(config.workspace_root.is_none() && config.relay.is_none());
    }

    #[test]
    fn a_full_command_line_parses_every_flag() {
        let config = parsed(&[
            "--server",
            "https://horde.example:7777",
            "--state-dir",
            "/tmp/w",
            "--join",
            "00ff.abcd",
            "--enrollment",
            "https://horde.example:7778",
            "--name",
            "w-1",
            "--adapter",
            "fake",
            "--adapter",
            "claude",
            "--fake-script",
            "/tmp/script.json",
            "--workspace-root",
            "/tmp/ws",
            "--relay",
            "https://horde.example:7779",
        ])
        .expect("a full line parses");
        assert_eq!(config.server, "https://horde.example:7777");
        assert_eq!(config.state_dir, std::path::PathBuf::from("/tmp/w"));
        assert_eq!(config.join.as_deref(), Some("00ff.abcd"));
        assert_eq!(
            config.enrollment.as_deref(),
            Some("https://horde.example:7778")
        );
        assert_eq!(config.name.as_deref(), Some("w-1"));
        assert_eq!(
            config.adapters,
            vec![AdapterChoice::Fake, AdapterChoice::Claude],
            "order is meaning: the first adapter is the default provider"
        );
        assert_eq!(
            config.fake_script,
            Some(std::path::PathBuf::from("/tmp/script.json"))
        );
        assert_eq!(
            config.workspace_root,
            Some(std::path::PathBuf::from("/tmp/ws"))
        );
        assert_eq!(config.relay.as_deref(), Some("https://horde.example:7779"));
        assert_eq!(
            config.identity_dir(),
            std::path::PathBuf::from("/tmp/w/identity")
        );
        assert_eq!(
            config.journal_path(),
            std::path::PathBuf::from("/tmp/w/journal.sqlite3")
        );
    }

    /// A mistyped flag silently ignored would dial the wrong server; every
    /// wrong shape is a refusal that names what was wrong.
    #[test]
    fn wrong_shapes_are_refused_by_name() {
        for (arguments, needle) in [
            // "unknown argument", precisely: a parser that instead treated
            // the stray as a valued flag would eat the next argument and
            // complain about a missing value for a flag that never existed.
            (&["--bogus"][..], "unknown argument --bogus"),
            (&["positional"][..], "unknown argument positional"),
            (&["--server"][..], "--server"),
            (&["--adapter", "gpt"][..], "gpt"),
            (&["--adapter", "fake", "--adapter", "fake"][..], "twice"),
            (&["--join", "c"][..], "--enrollment"),
            (&["--enrollment", "https://x:1"][..], "--join"),
            (
                &[
                    "--join",
                    "c",
                    "--enrollment",
                    "https://x:1",
                    "--insecure",
                    "--server",
                    "http://x:1",
                ][..],
                "--insecure",
            ),
            (&["--fake-script", "/tmp/s.json"][..], "--adapter fake"),
            (&["--insecure"][..], "http://"),
            (&["--server", "http://x:1"][..], "https://"),
        ] {
            let refused = parsed(arguments).expect_err("must refuse");
            assert!(
                refused.contains(needle),
                "{arguments:?} must be refused naming {needle}: {refused}"
            );
        }
    }

    #[test]
    fn the_insecure_shape_pairs_a_plaintext_server_with_no_identity() {
        let config = parsed(&[
            "--insecure",
            "--server",
            "http://127.0.0.1:1",
            "--name",
            "w-1",
        ])
        .expect("the in-process-test shape parses");
        assert!(config.insecure);
        assert_eq!(
            identity_plan(&config, false).expect("a plan"),
            IdentityPlan::Insecure
        );
    }

    /// The identity truth table, pure: every row is a decision the daemon
    /// makes before touching the network.
    #[test]
    fn the_identity_plan_covers_load_join_and_the_join_over_existing_refusal() {
        let load = parsed(&["--state-dir", "/tmp/w"]).expect("a load shape");
        assert_eq!(
            identity_plan(&load, true).expect("a plan"),
            IdentityPlan::Load(std::path::PathBuf::from("/tmp/w/identity"))
        );
        // Not yet enrolled and not joining: still a Load, whose failure will
        // say how to enroll — deciding "refuse" here would duplicate that
        // message without the filesystem's detail.
        assert_eq!(
            identity_plan(&load, false).expect("a plan"),
            IdentityPlan::Load(std::path::PathBuf::from("/tmp/w/identity"))
        );

        let join = parsed(&[
            "--state-dir",
            "/tmp/w",
            "--join",
            "00ff.abcd",
            "--enrollment",
            "https://x:1",
        ])
        .expect("a join shape");
        assert_eq!(
            identity_plan(&join, false).expect("a plan"),
            IdentityPlan::Join {
                enrollment: "https://x:1".to_owned(),
                code: "00ff.abcd".to_owned(),
                directory: std::path::PathBuf::from("/tmp/w/identity"),
            }
        );
        let refused = identity_plan(&join, true).expect_err("join over existing must refuse");
        assert!(
            refused.contains("/tmp/w/identity"),
            "the refusal names the directory to clear: {refused}"
        );
    }

    #[test]
    fn the_script_form_parses_strings_objects_and_permission_asks() {
        let script = fake_script(
            r#"[
                "hello there",
                {"steps": [
                    {"say": "checking"},
                    {"think": "hmm"},
                    {"ask_permission": {"id": "call-1", "tool": "Bash",
                                        "input": {"command": "true"}}}
                ], "text": "done"}
            ]"#,
        )
        .expect("a valid script parses");

        assert_eq!(script.turns.len(), 2);
        assert_eq!(script.turns[0].text.as_deref(), Some("hello there"));
        assert_eq!(script.turns[1].text.as_deref(), Some("done"));
        assert_eq!(script.turns[1].steps.len(), 3);
        let horde_adapter_fake::Step::AskPermission {
            tool_call_id,
            tool_name,
            input,
        } = &script.turns[1].steps[2]
        else {
            panic!("the third step must be the permission ask");
        };
        assert_eq!(tool_call_id, "call-1");
        assert_eq!(tool_name, "Bash");
        assert_eq!(input["command"], "true");

        // The object form charges the same nominal spend `saying` does:
        // scripted turns must flow through the usage pipeline like every
        // other turn, or a scripted daemon reports free work.
        assert_eq!(script.turns[1].usage.input_tokens, 10);
        assert_eq!(script.turns[1].usage.output_tokens, 20);
        assert!((script.turns[1].cost_usd - 0.001).abs() < f64::EPSILON);
    }

    /// The operator form of a verb call: how an e2e script says "here the
    /// agent reports its result through its endpoint".
    #[test]
    fn the_script_form_parses_horde_verb_calls() {
        let script = fake_script(
            r#"[{"steps": [{"use_horde": {"id": "call-2", "tool": "report_result",
                                          "input": {"status": "success", "summary": "done"}}}]}]"#,
        )
        .expect("a verb-call script parses");
        let horde_adapter_fake::Step::UseHordeTool {
            tool_call_id,
            tool_name,
            input,
        } = &script.turns[0].steps[0]
        else {
            panic!("the step must be the verb call");
        };
        assert_eq!(tool_call_id, "call-2");
        assert_eq!(tool_name, "report_result");
        assert_eq!(input["status"], "success");
    }

    /// A defaulted `input` must still be an object: the wire converts the
    /// proposed call's arguments to a Struct, and `null` would be refused
    /// downstream with a message about neither the script nor the flag.
    #[test]
    fn an_ask_without_input_defaults_to_an_empty_object() {
        let script = fake_script(r#"[{"steps": [{"ask_permission": {"id": "c", "tool": "T"}}]}]"#)
            .expect("input is optional");
        let horde_adapter_fake::Step::AskPermission { input, .. } = &script.turns[0].steps[0]
        else {
            panic!("the step must be the permission ask");
        };
        assert!(input.is_object(), "the default input is an empty object");
    }

    #[test]
    fn malformed_scripts_are_refused_naming_the_turn() {
        for (json, needle) in [
            ("{}", "array"),
            ("not json", "not JSON"),
            ("[42]", "turn 0"),
            (r#"[{"steps": 42}]"#, "steps must be an array"),
            (
                r#"[{"steps": [{"dance": true}]}]"#,
                "say, think, ask_permission",
            ),
            (r#"[{"steps": [{"say": 42}]}]"#, "say must be a string"),
            (r#"[{"text": 42}]"#, "text must be a string"),
            (r#"[{"steps": [{"ask_permission": {"tool": "T"}}]}]"#, "id"),
            (r#"[{"steps": [{"ask_permission": {"id": "c"}}]}]"#, "tool"),
            (r#"[{"steps": [{"use_horde": {"tool": "T"}}]}]"#, "id"),
            (r#"[{"steps": [{"use_horde": {"id": "c"}}]}]"#, "tool"),
            (r#"[{"steps": [{"use_horde": 42}]}]"#, "object"),
        ] {
            let refused = fake_script(json).expect_err("must refuse");
            assert!(
                refused.contains(needle),
                "{json} must be refused naming {needle}: {refused}"
            );
        }
    }

    #[test]
    fn the_worker_name_prefers_the_certificate_then_the_flag_then_the_default() {
        assert_eq!(worker_name(None, Some("w-9")), "w-9");
        assert_eq!(worker_name(None, None), DEFAULT_NAME);
        // The certificate half is proven with a production-issued identity in
        // `an_enrolled_identity_names_the_worker`, which needs a server.
    }

    /// The classifier's truth table. What redials and what stops is the
    /// difference between a partition (healed by patience) and a revocation
    /// (healed by nothing), and a swapped arm here turns one into the other.
    #[test]
    fn the_fate_table_separates_weather_from_refusals() {
        let redials = [
            fate_of(&Ok(())),
            fate_of(&Err(WorkerError::Rpc(tonic::Status::unavailable("down")))),
            fate_of(&Err(WorkerError::Rpc(tonic::Status::internal("oops")))),
        ];
        for fate in redials {
            assert!(
                matches!(fate, Fate::Redial(_)),
                "weather must redial: {fate:?}"
            );
        }

        let fatals = [
            fate_of(&Err(WorkerError::Rpc(tonic::Status::unauthenticated(
                "revoked",
            )))),
            fate_of(&Err(WorkerError::Rpc(tonic::Status::permission_denied(
                "no",
            )))),
            fate_of(&Err(WorkerError::Rpc(tonic::Status::invalid_argument(
                "bad hello",
            )))),
            fate_of(&Err(WorkerError::Rpc(tonic::Status::failed_precondition(
                "below the minimum version",
            )))),
            fate_of(&Err(WorkerError::Rpc(tonic::Status::unimplemented(
                "wrong server",
            )))),
            fate_of(&Err(WorkerError::TruncatedPast {
                requested: 1,
                floor: 5,
            })),
            fate_of(&Err(WorkerError::Incomplete("worker id"))),
        ];
        for fate in fatals {
            assert!(
                matches!(fate, Fate::Fatal(_)),
                "a refusal must stop the daemon: {fate:?}"
            );
        }
    }

    /// Both orders of the two-signal wait: whichever side resolves first
    /// ends it, and a pending other side never blocks it.
    #[tokio::test]
    async fn first_of_resolves_on_either_side() {
        super::first_of(async {}, std::future::pending()).await;
        super::first_of(std::future::pending(), async {}).await;
    }

    /// The redial pause's whole contract, on the paused clock: with no
    /// shutdown it waits exactly [`RETRY_DELAY`] and says keep-going (a
    /// pause that returns early is a hot spin against a dead server; one
    /// that returns `false` shuts a healthy daemon down), and a completed
    /// shutdown wins the pause immediately.
    #[tokio::test(start_paused = true)]
    async fn the_redial_pause_waits_the_delay_and_yields_to_shutdown() {
        let pending = std::future::pending::<()>();
        tokio::pin!(pending);
        let before = tokio::time::Instant::now();
        assert!(
            super::pause_before_redial(pending.as_mut()).await,
            "no shutdown means keep going"
        );
        assert_eq!(
            before.elapsed(),
            super::RETRY_DELAY,
            "the pause is the delay, exactly — the paused clock makes this deterministic"
        );

        let ready = async {};
        tokio::pin!(ready);
        let before = tokio::time::Instant::now();
        assert!(
            !super::pause_before_redial(ready.as_mut()).await,
            "a completed shutdown wins the pause"
        );
        assert_eq!(
            before.elapsed(),
            core::time::Duration::ZERO,
            "shutdown must not wait out the delay first"
        );
    }

    /// The binary lifecycle wrapper actually runs its loop on a live
    /// runtime, and the process-global subscriber slot behaves like one: the
    /// first init claims it, a second init is a refusal rather than a
    /// silent subscriber swap. One test for both, because the slot is
    /// process-global and a second test could not get the first-claim half.
    #[test]
    fn with_observability_runs_the_loop_once_and_then_refuses_the_slot() {
        let state = tempfile::tempdir().expect("a temp dir");
        let mut ran = false;
        let first = super::with_observability(state.path(), |runtime| {
            runtime.block_on(async {});
            ran = true;
            std::process::ExitCode::SUCCESS
        });
        assert!(ran, "the main loop must actually run");
        assert!(first.is_ok(), "{first:?}");

        let refused = super::with_observability(state.path(), |_| {
            panic!("the loop must not run when startup refuses")
        })
        .expect_err("the subscriber slot is already claimed");
        assert!(
            refused.contains("subscriber"),
            "the refusal names what was already installed: {refused}"
        );
    }

    /// The transport case carries its own reason so a `dial-failed` line is
    /// diagnosable; asserted separately because building a
    /// `tonic::transport::Error` requires actually failing a dial.
    #[tokio::test]
    async fn a_transport_failure_redials_with_the_reason() {
        let error = tonic::transport::Endpoint::from_static("http://127.0.0.1:1")
            .connect()
            .await
            .expect_err("nobody listens on port 1");
        let fate = fate_of(&Err(WorkerError::Transport(error)));
        let Fate::Redial(reason) = fate else {
            panic!("an unreachable server is weather, not a refusal: {fate:?}");
        };
        assert!(
            reason.contains("could not reach the server"),
            "the reason survives into the operator line: {reason}"
        );
    }
}
