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
    /// A one-time join token (`horde://join/<host:port>/<secret>.<fingerprint>`,
    /// ADR-0007) to redeem first: the enrollment listener, the secret, and
    /// the pinned server fingerprint in one string.
    pub join: Option<String>,
    /// The display name, and the plaintext worker id under `--insecure`.
    /// An enrolled identity's principal always outranks it.
    pub name: Option<String>,
    /// Which harnesses to register, in order; the first is the default
    /// provider for spawns that name none.
    pub adapters: Vec<AdapterChoice>,
    /// A JSON script for the fake adapter; absent means the echo script.
    pub fake_script: Option<PathBuf>,
    /// The fake adapter's login command (DESIGN §4's relay, hermetically):
    /// a program the login PTY spawns instead of a real provider CLI.
    pub fake_login: Option<PathBuf>,
    /// The browser-facing callback port the fake login recipe declares —
    /// what the client binds exactly and the URL the command prints names.
    pub fake_login_callback_port: Option<u16>,
    /// Where the fake login command actually listens on this machine, when
    /// it differs from the callback port. The hermetic e2e needs the split:
    /// on one machine, one port cannot be both the client's listener and
    /// the worker's.
    pub fake_login_listen_port: Option<u16>,
    /// Enables workspace support rooted here.
    pub workspace_root: Option<PathBuf>,
    /// The git Relay payload shares stage to and fetch through.
    pub relay: Option<String>,
    /// Placement tags this Worker advertises (`--tag`, repeatable).
    pub tags: Vec<String>,
    /// Remote URLs this machine's credentials can reach
    /// (`--advertise-remote`, repeatable).
    pub advertised_remotes: Vec<String>,
    /// Dial plaintext with no identity — the in-process-test-server shape.
    pub insecure: bool,
    /// The codex `approvalsReviewer`, already validated at parse: only
    /// `"user"` survives [`horde_adapter_codex::approvals_reviewer_from_config`].
    /// `auto_review` is the transcript-23 self-approval hazard and is refused
    /// before any process starts.
    pub codex_approvals_reviewer: Option<String>,
}

/// What the flags said, before the shape rules are applied.
#[derive(Debug, Default)]
struct ParsedFlags {
    server: Option<String>,
    state_dir: Option<PathBuf>,
    join: Option<String>,
    name: Option<String>,
    adapters: Vec<AdapterChoice>,
    fake_login: Option<PathBuf>,
    fake_login_callback_port: Option<u16>,
    fake_login_listen_port: Option<u16>,
    fake_script: Option<PathBuf>,
    workspace_root: Option<PathBuf>,
    relay: Option<String>,
    tags: Vec<String>,
    advertised_remotes: Vec<String>,
    insecure: bool,
    codex_approvals_reviewer: Option<String>,
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
            | "--name"
            | "--adapter"
            | "--fake-script"
            | "--fake-login"
            | "--fake-login-callback-port"
            | "--fake-login-listen-port"
            | "--workspace-root"
            | "--relay"
            | "--tag"
            | "--advertise-remote"
            | "--codex-approvals-reviewer"
    )
}

/// A flag's value as a TCP port, refused with the flag's own name.
fn parsed_port(flag: &str, value: &str) -> Result<u16, String> {
    value
        .parse::<u16>()
        .map_err(|_| format!("{flag} is not a port: {value}"))
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
            "--name" => self.name = Some(value),
            _ => return self.apply_runtime_value(flag, value),
        }
        Ok(())
    }

    /// Applies the flags that decide what this Worker runs; the fake login
    /// recipe's fall through to [`Self::apply_login_value`]. Split by
    /// subject so neither half outgrows the complexity bar.
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
            "--tag" => self.tags.push(value),
            "--advertise-remote" => self.advertised_remotes.push(value),
            "--codex-approvals-reviewer" => {
                // Rejected at parse, not at spawn: `auto_review` had codex's
                // own subagent approve a sandbox escalation and run the write
                // under a read-only sandbox (transcript 23). The validator
                // admits only "user", which the codec pins anyway.
                horde_adapter_codex::approvals_reviewer_from_config(Some(&value))?;
                self.codex_approvals_reviewer = Some(value);
            }
            _ => return self.apply_login_value(flag, value),
        }
        Ok(())
    }

    /// Applies the flags that shape the fake adapter's login recipe
    /// (DESIGN §4's relay, hermetically).
    fn apply_login_value(&mut self, flag: &str, value: String) -> Result<(), String> {
        match flag {
            "--fake-login" => self.fake_login = Some(PathBuf::from(value)),
            "--fake-login-callback-port" => {
                self.fake_login_callback_port = Some(parsed_port(flag, &value)?);
            }
            "--fake-login-listen-port" => {
                self.fake_login_listen_port = Some(parsed_port(flag, &value)?);
            }
            // Unreachable while `takes_value` and these matches agree;
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
            name: self.name,
            adapters: self.adapters,
            fake_script: self.fake_script,
            fake_login: self.fake_login,
            fake_login_callback_port: self.fake_login_callback_port,
            fake_login_listen_port: self.fake_login_listen_port,
            workspace_root: self.workspace_root,
            relay: self.relay,
            tags: self.tags,
            advertised_remotes: self.advertised_remotes,
            insecure: self.insecure,
            codex_approvals_reviewer: self.codex_approvals_reviewer,
        };
        config.check_shape()?;
        Ok(config)
    }
}

impl WorkerConfig {
    /// The rules that make a parse a usable configuration.
    ///
    /// Each is a refusal rather than a repair: an insecure join is a
    /// contradiction (the point of joining is the certificate), a fake
    /// script without the fake adapter configures nothing, and a scheme that
    /// disagrees with the security mode would otherwise fail later with a
    /// TLS error that names neither flag.
    fn check_shape(&self) -> Result<(), String> {
        if self.join.is_some() && self.insecure {
            return Err("--join enrolls an identity; it cannot combine with --insecure".to_owned());
        }
        self.check_fake_shape()?;
        if self.codex_approvals_reviewer.is_some() && !self.adapters.contains(&AdapterChoice::Codex)
        {
            return Err("--codex-approvals-reviewer needs --adapter codex".to_owned());
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

    /// The fake adapter's own shape rules: scripts and login recipes
    /// configure nothing without the adapter they belong to, and the login
    /// ports form a dependency chain (listen needs callback needs command).
    fn check_fake_shape(&self) -> Result<(), String> {
        if self.fake_script.is_some() && !self.adapters.contains(&AdapterChoice::Fake) {
            return Err("--fake-script needs --adapter fake".to_owned());
        }
        if self.fake_login.is_some() && !self.adapters.contains(&AdapterChoice::Fake) {
            return Err("--fake-login needs --adapter fake".to_owned());
        }
        if self.fake_login_callback_port.is_some() && self.fake_login.is_none() {
            return Err("--fake-login-callback-port needs --fake-login".to_owned());
        }
        if self.fake_login_listen_port.is_some() && self.fake_login_callback_port.is_none() {
            return Err("--fake-login-listen-port needs --fake-login-callback-port".to_owned());
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
    /// Redeem a one-time join token and persist the result.
    Join { token: String, directory: PathBuf },
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
    match &config.join {
        Some(token) => {
            if already_enrolled {
                return Err(format!(
                    "{} already holds an identity; remove it to re-enroll, \
                     or drop --join to use it",
                    directory.display()
                ));
            }
            Ok(IdentityPlan::Join {
                token: token.clone(),
                directory,
            })
        }
        None => Ok(IdentityPlan::Load(directory)),
    }
}

/// Carries out an identity plan.
///
/// # Errors
///
/// A malformed or refused token, an unreachable enrollment listener, a
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
                     --join 'horde://join/<host:port>/<secret>.<fingerprint>' \
                     (the token a mint prints)",
                    directory.display()
                )
            })?;
            Ok(Some(identity))
        }
        IdentityPlan::Join { token, directory } => {
            // The parse error itself names the expected horde://join/…
            // shape — the refusal diagnoses the mistake, not the paste.
            let token =
                horde_identity::JoinToken::parse(&token).map_err(|error| error.to_string())?;
            let enrolled = horde_identity::join(
                &token.enrollment_endpoint(),
                &token.code,
                capabilities::hello(display_name),
            )
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
    /// The operator-declared placement facts every advertisement carries.
    pub declared: capabilities::DeclaredFacts,
    /// The main endpoint every dial and the blob channel share.
    pub server: String,
    pub identity: Option<StoredIdentity>,
    /// The id claimed at registration and the display name in the hello.
    pub name: String,
    /// Where the login plane's operator lines go (`login spawned …`,
    /// `login reaped …`). Stdout by default — the operator interface — and
    /// injectable so in-process suites can read the reap witness.
    pub login_announce: crate::login::Announce,
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
    // Before any agent starts: whatever a crashed previous run left running
    // in its harness process groups is swept now (ADR-0010).
    let swept = crate::process::reap_leftover_groups(&journal).await;
    if swept > 0 {
        announce(format!(
            "swept {swept} harness process group(s) a previous run left behind"
        ));
    }

    let runtime = build_runtime(config, identity.as_ref(), &name, journal.clone()).await?;
    Ok(PreparedWorker {
        runtime,
        journal,
        declared: capabilities::DeclaredFacts {
            tags: config.tags.clone(),
            reachable_remotes: config.advertised_remotes.clone(),
        },
        server: config.server.clone(),
        identity,
        name,
        login_announce: Arc::new(|line| println!("{line}")),
    })
}

/// Assembles the Runtime a configuration asks for.
///
/// Async for one reason: the codex adapter probes its binary's version before
/// claiming exec interception, and a probe is a bounded child process, not a
/// guess.
async fn build_runtime(
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
    let mut runtime = Runtime::new(journal.clone())
        .with_blob_service(channel.clone())
        // The wire ADR-0004 left to the Worker: every spawned agent gets its
        // Session's MCP endpoint and a minted identity pair — and its hub
        // tools forwarded over this same channel, attributed under this
        // Worker's name (the certificate outranks the claim at the Server).
        .with_agent_endpoints(channel, name);

    for adapter in &config.adapters {
        runtime = match adapter {
            AdapterChoice::Fake => {
                let mut fake = FakeAdapter::new(configured_script(config)?);
                if let Some(recipe) = configured_fake_login(config) {
                    fake = fake.with_login_recipe(recipe);
                }
                runtime.with_adapter(Arc::new(fake))
            }
            AdapterChoice::Claude => runtime.with_adapter(Arc::new(crate::ClaudeAdapter::new(
                harness_profile(config, "claude", &journal),
            ))),
            AdapterChoice::Codex => {
                // Belt and braces on the transcript-23 hazard: the flag was
                // validated at parse, and the value is re-validated here so a
                // config constructed programmatically gets the same gate. The
                // codec pins the wire value regardless.
                horde_adapter_codex::approvals_reviewer_from_config(
                    config.codex_approvals_reviewer.as_deref(),
                )?;
                // Probed, not assumed: the exec-gating capability claim is
                // version-gated (ADR-0005, 2026-08-19 amendment).
                let adapter = crate::CodexAdapter::new(harness_profile(config, "codex", &journal))
                    .probe_exec_interception()
                    .await;
                runtime.with_adapter(Arc::new(adapter))
            }
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

/// The Worker-owned harness profile one provider's adapter launches under
/// (ADR-0010): config directories under the state directory, and every
/// harness process group remembered in the journal so a crash can be swept.
fn harness_profile(
    config: &WorkerConfig,
    provider: &str,
    journal: &Journal,
) -> crate::harness::HarnessProfile {
    crate::harness::HarnessProfile::in_state_dir(&config.state_dir, provider)
        .with_ledger(Arc::new(journal.clone()))
}

/// The fake adapter's login recipe, when `--fake-login` gave it one. The
/// ports ride both halves of the recipe: declared (so the relay exposes the
/// callback and the worker dials the listener) and passed to the command as
/// argv (so it binds and prints the same ports).
pub fn configured_fake_login(config: &WorkerConfig) -> Option<horde_adapter::LoginRecipe> {
    let program = config.fake_login.as_ref()?;
    let program = program.to_string_lossy().into_owned();
    let Some(callback) = config.fake_login_callback_port else {
        return Some(horde_adapter::LoginRecipe::terminal_only(
            program,
            Vec::new(),
        ));
    };
    let mut args = vec!["--callback-port".to_owned(), callback.to_string()];
    let mut recipe = horde_adapter::LoginRecipe::with_callback(program, Vec::new(), callback);
    if let Some(listen) = config.fake_login_listen_port {
        args.extend(["--listen-port".to_owned(), listen.to_string()]);
        recipe = recipe.listening_on(listen);
    }
    recipe.args = args;
    Some(recipe)
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
        capabilities: capabilities::advertise(
            prepared.runtime.adapters(),
            warm,
            &prepared.declared,
        ),
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

        // Rebuilt per connection from the registered adapters: recipes are
        // static data, and collecting them here keeps the borrow on the
        // runtime out of the mutable run below.
        let logins = Arc::new(crate::login::LoginPlane::from_adapters(
            prepared.runtime.adapters(),
            prepared.login_announce.clone(),
        ));
        let ended = tokio::select! {
            () = shutdown.as_mut() => return None,
            ended = connection.run(&prepared.journal, &mut prepared.runtime, logins) => ended,
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
        assert!(config.join.is_none());
        assert!(config.workspace_root.is_none() && config.relay.is_none());
        assert!(config.tags.is_empty() && config.advertised_remotes.is_empty());
    }

    /// The fake login recipe's flags, whole: parsed into the config, turned
    /// into the recipe with the split listener, and each dependency rule
    /// refusing by name — port typos included.
    #[test]
    fn the_fake_login_flags_parse_validate_and_build_the_recipe() {
        let config = parsed(&[
            "--adapter",
            "fake",
            "--fake-login",
            "/tmp/fake-login",
            "--fake-login-callback-port",
            "14555",
            "--fake-login-listen-port",
            "14556",
        ])
        .expect("the login flags parse");
        assert_eq!(config.fake_login_callback_port, Some(14555));
        assert_eq!(config.fake_login_listen_port, Some(14556));
        let recipe = super::configured_fake_login(&config).expect("a recipe");
        assert_eq!(recipe.program, "/tmp/fake-login");
        assert_eq!(recipe.callback_port, Some(14555));
        assert_eq!(recipe.listen_port, Some(14556));
        assert_eq!(
            recipe.args,
            vec!["--callback-port", "14555", "--listen-port", "14556"],
            "the command binds and prints the same ports the recipe declares"
        );

        // A command without ports is a terminal-only recipe; no flags at
        // all is no recipe.
        let bare = parsed(&["--adapter", "fake", "--fake-login", "/tmp/fake-login"])
            .expect("a portless login parses");
        let recipe = super::configured_fake_login(&bare).expect("a recipe");
        assert_eq!((recipe.callback_port, recipe.listen_port), (None, None));
        assert!(recipe.args.is_empty());
        let none = parsed(&["--adapter", "fake"]).expect("parses");
        assert!(super::configured_fake_login(&none).is_none());

        // The refusal ladder, each rule by name.
        let refused = parsed(&["--fake-login", "/tmp/fake-login"]).expect_err("needs the adapter");
        assert!(refused.contains("--adapter fake"), "{refused}");
        let refused = parsed(&["--adapter", "fake", "--fake-login-callback-port", "1"])
            .expect_err("a port without a command configures nothing");
        assert!(refused.contains("needs --fake-login"), "{refused}");
        let refused = parsed(&[
            "--adapter",
            "fake",
            "--fake-login",
            "/tmp/fake-login",
            "--fake-login-listen-port",
            "1",
        ])
        .expect_err("a listener without a redirect is meaningless");
        assert!(
            refused.contains("needs --fake-login-callback-port"),
            "{refused}"
        );
        let refused = parsed(&[
            "--adapter",
            "fake",
            "--fake-login",
            "/tmp/fake-login",
            "--fake-login-callback-port",
            "not-a-port",
        ])
        .expect_err("a port typo is the typist's, by name");
        assert!(
            refused.contains("--fake-login-callback-port is not a port"),
            "{refused}"
        );
        let refused = super::parsed_port("--fake-login-listen-port", "70000")
            .expect_err("no TCP port is that big");
        assert!(refused.contains("not a port"), "{refused}");
        assert_eq!(super::parsed_port("--x", "8080"), Ok(8080));
    }

    /// The transcript-23 pin at the config seam: `user` (with the codex
    /// adapter) parses; the self-approval reviewers are refused by name,
    /// citing the measurement; and the flag without the adapter it configures
    /// is a refusal too.
    #[test]
    fn the_codex_reviewer_flag_admits_user_and_refuses_the_hazard() {
        let config = parsed(&["--adapter", "codex", "--codex-approvals-reviewer", "user"])
            .expect("user is the one reviewer that routes approvals to the chain");
        assert_eq!(config.codex_approvals_reviewer.as_deref(), Some("user"));

        for hazard in ["auto_review", "guardian"] {
            let refusal = parsed(&["--adapter", "codex", "--codex-approvals-reviewer", hazard])
                .expect_err("the self-approving reviewer must be refused at parse");
            assert!(
                refusal.contains("23-exec-autoreview-onrequest-ro-horde-write"),
                "the refusal cites the transcript that measured the hazard: {refusal}"
            );
        }

        let orphaned = parsed(&["--adapter", "fake", "--codex-approvals-reviewer", "user"])
            .expect_err("a reviewer without the codex adapter configures nothing");
        assert!(orphaned.contains("--adapter codex"), "{orphaned}");
    }

    #[test]
    fn a_full_command_line_parses_every_flag() {
        let config = parsed(&[
            "--server",
            "https://horde.example:7777",
            "--state-dir",
            "/tmp/w",
            "--join",
            "horde://join/horde.example:7778/00ff.abcd",
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
            "--tag",
            "gpu",
            "--tag",
            "fast-disk",
            "--advertise-remote",
            "https://git.example/atlas.git",
        ])
        .expect("a full line parses");
        assert_eq!(config.server, "https://horde.example:7777");
        assert_eq!(config.state_dir, std::path::PathBuf::from("/tmp/w"));
        assert_eq!(
            config.join.as_deref(),
            Some("horde://join/horde.example:7778/00ff.abcd"),
            "the token rides the flag whole; its shape is judged at redeem"
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
        assert_eq!(config.tags, vec!["gpu", "fast-disk"]);
        assert_eq!(
            config.advertised_remotes,
            vec!["https://git.example/atlas.git"]
        );
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
            (
                &[
                    "--join",
                    "horde://join/x:1/c.d",
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
            "horde://join/x:1/00ff.abcd",
        ])
        .expect("a join shape");
        assert_eq!(
            identity_plan(&join, false).expect("a plan"),
            IdentityPlan::Join {
                token: "horde://join/x:1/00ff.abcd".to_owned(),
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

    /// A garbled token is refused before any network exists, and the
    /// refusal teaches the expected `horde://join/…` shape — ADR-0007's
    /// errors-diagnose-the-mistake rule at the worker's redeem surface. The
    /// old ceremony's inputs (a bare code, a bare listener URL) are exactly
    /// the pastes that must be diagnosed.
    #[tokio::test]
    async fn a_malformed_join_token_refuses_naming_the_expected_shape() {
        for garbled in ["00ff.abcd", "https://horde.example:7778", "nonsense"] {
            let refused = super::resolve_identity(
                IdentityPlan::Join {
                    token: garbled.to_owned(),
                    directory: std::path::PathBuf::from("/tmp/never-used"),
                },
                "w-1",
                &mut |_| {},
            )
            .await
            .expect_err("a garbled token must refuse");
            assert!(
                refused.contains("horde://join/<host:port>/<secret>.<fingerprint>"),
                "{garbled:?}: the refusal must teach the form: {refused}"
            );
        }
    }

    /// The missing-identity refusal says how to enroll in token vocabulary:
    /// the fix it names must be the flag that exists.
    #[tokio::test]
    async fn a_missing_identity_refusal_names_the_token_join() {
        let state = tempfile::tempdir().expect("a temp dir");
        let refused = super::resolve_identity(
            IdentityPlan::Load(state.path().join("identity")),
            "w-1",
            &mut |_| {},
        )
        .await
        .expect_err("an empty directory holds no identity");
        assert!(
            refused.contains("--join") && refused.contains("horde://join/"),
            "the refusal names the one flag and its token: {refused}"
        );
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
