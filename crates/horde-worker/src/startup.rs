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

// The command line and the configuration it yields live in `flags`; these
// re-exports keep `startup` the one place a caller reaches for both.
pub use crate::flags::{AdapterChoice, IDENTITY_DIR, JOURNAL_FILE, JoinRequest, WorkerConfig};

/// The lock a running daemon holds on its state directory: a second worker
/// on the same state (a second `horde up`, a supervisor double-start)
/// refuses instead of sharing a journal.
pub const STATE_LOCK_FILE: &str = "worker.lock";

/// The display name used before any identity names this Worker.
pub const DEFAULT_NAME: &str = "horde-worker";

/// The ceiling on the first redial's wait — and on every redial right after
/// a connection that registered. A half-second keeps reconnection after a
/// blip feeling immediate (DESIGN.md §2: spotty networks are the norm).
pub const RETRY_BASE: core::time::Duration = core::time::Duration::from_millis(500);

/// The most any redial waits. Failures in a row double the ceiling up to
/// here: a Server that is down for minutes is dialed every few seconds, not
/// twice a second by every Worker at once.
pub const RETRY_CAP: core::time::Duration = core::time::Duration::from_secs(10);

/// The redial schedule: jittered exponential backoff.
///
/// The ceiling doubles per consecutive failure from [`RETRY_BASE`] to
/// [`RETRY_CAP`], and each wait is drawn from the upper half of it ("equal
/// jitter"): never shorter than half the ceiling, so a run of failures
/// genuinely backs off, and never in lockstep with every other Worker that
/// lost the same Server at the same instant — a restart is not followed by
/// a synchronized stampede. A connection that registers resets it. A push
/// that fails ends its connection, so push retries ride the same schedule.
#[derive(Debug, Clone)]
pub struct Backoff {
    failures: u32,
    random: fn() -> u64,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::with_random(entropy)
    }
}

impl Backoff {
    /// A schedule drawing its jitter from `random` — the seam that lets a
    /// test pin the draws.
    #[must_use]
    pub fn with_random(random: fn() -> u64) -> Self {
        Self {
            failures: 0,
            random,
        }
    }

    /// The wait before the next redial, counting this as one more failure.
    pub fn next_delay(&mut self) -> core::time::Duration {
        let delay = jittered(backoff_ceiling(self.failures), (self.random)());
        self.failures = self.failures.saturating_add(1);
        delay
    }

    /// A connection registered: the next loss starts from the base again.
    pub fn reset(&mut self) {
        self.failures = 0;
    }
}

/// The ceiling after `failures` consecutive failures: the base doubled that
/// many times, capped.
fn backoff_ceiling(failures: u32) -> core::time::Duration {
    RETRY_BASE
        .saturating_mul(2_u32.saturating_pow(failures.min(16)))
        .min(RETRY_CAP)
}

/// A draw from the upper half of `ceiling`: `[ceiling/2, ceiling]`.
fn jittered(ceiling: core::time::Duration, random: u64) -> core::time::Duration {
    let millis = u64::try_from(ceiling.as_millis()).unwrap_or(u64::MAX);
    let half = millis / 2;
    core::time::Duration::from_millis(half + random % (millis - half + 1))
}

/// Jitter entropy from the OS, falling back to the clock: a jitter source
/// needs to differ between Workers, not to be unpredictable.
fn entropy() -> u64 {
    getrandom::u64().unwrap_or_else(|_| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| u64::from(since.subsec_nanos()))
            .unwrap_or_default()
    })
}

/// Parses the command line against the process environment and `$HOME`
/// (see [`crate::flags`] for the precedence rules), running or refusing:
/// `--help` and `--version` come back as the refusal text here, because
/// this is the run-or-refuse path — the binary asks [`crate::flags::parse`]
/// directly so it can print them and exit zero.
///
/// # Errors
///
/// A human-readable refusal naming the flag that was wrong.
pub fn invocation(arguments: impl IntoIterator<Item = String>) -> Result<WorkerConfig, String> {
    match crate::flags::parse(
        arguments,
        |name| std::env::var(name).ok(),
        std::env::var_os("HOME").map(PathBuf::from),
    )? {
        crate::flags::Parsed::Run(config) => Ok(*config),
        crate::flags::Parsed::Print(text) => Err(text),
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
    if let Some(ask) = object.get("ask_question") {
        return ask_question_step(ask);
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
    Err("a step must be one of say, think, ask_permission, ask_question, use_horde".to_owned())
}

/// `{"ask_question": {"id", "tool", "questions": [{"id", "question", "options": [..]}]}}`
/// — the operator form of a harness question (W12c).
fn ask_question_step(value: &serde_json::Value) -> Result<Step, String> {
    let (tool_call_id, tool_name, _) = tool_step_fields("ask_question", value)?;
    let questions = value
        .get("questions")
        .and_then(serde_json::Value::as_array)
        .filter(|questions| !questions.is_empty())
        .ok_or("ask_question needs a non-empty questions array")?
        .iter()
        .map(|question| {
            let text = |field: &str| {
                question
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            };
            let id = text("id");
            if id.is_empty() {
                return Err("every question needs a non-empty id".to_owned());
            }
            let options = question
                .get("options")
                .and_then(serde_json::Value::as_array)
                .map(|options| {
                    options
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(|label| horde_adapter::QuestionOption {
                            label: label.to_owned(),
                            description: String::new(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            Ok(horde_adapter::Question {
                id,
                header: text("header"),
                question: text("question"),
                options,
                multi_select: question
                    .get("multi_select")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                allows_free_text: true,
                secret: false,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Step::AskQuestion {
        tool_call_id,
        tool_name,
        questions,
    })
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
/// — both are the wrong thing done quietly. The one exception is an ambient
/// token (`HORDE_JOIN_TOKEN`): nobody asked for it on this command line, so
/// over an existing identity it is ignored and the identity loads.
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
        Some(join) if already_enrolled && join.ambient => Ok(IdentityPlan::Load(directory)),
        Some(_) if already_enrolled => Err(format!(
            "{} already holds an identity; remove it to re-enroll, \
             or drop --join to use it",
            directory.display()
        )),
        Some(join) => Ok(IdentityPlan::Join {
            token: join.token.clone(),
            directory,
        }),
        None => Ok(IdentityPlan::Load(directory)),
    }
}

/// The operator line owed when an ambient `HORDE_JOIN_TOKEN` is ignored —
/// exactly when there is one *and* an identity already exists (see
/// [`identity_plan`]); `None` otherwise.
#[must_use]
pub fn ignored_ambient_join(config: &WorkerConfig, enrolled: bool) -> Option<String> {
    let ambient = config.join.as_ref().is_some_and(|join| join.ambient);
    (enrolled && ambient).then(|| {
        format!(
            "ignoring {}: {} already holds an identity",
            crate::flags::JOIN_TOKEN_VAR,
            config.identity_dir().display()
        )
    })
}

/// Where this run dials (ADR-0007, 2026-10-31 amendment): `--server`, then
/// `HORDE_SERVER` (both already folded into [`WorkerConfig::server`]), then
/// the endpoint the identity dir stored at join. Nothing from any source is
/// a refusal that says where endpoints come from — there is no default
/// server to guess at.
///
/// # Errors
///
/// No endpoint from any source.
pub fn resolve_server(config: &WorkerConfig) -> Result<String, String> {
    config
        .server
        .clone()
        .or_else(|| horde_identity::store::load_endpoint(&config.identity_dir()))
        .ok_or_else(|| {
            format!(
                "no server to dial: pass --server https://host:port, set {}, or join with \
                 --join <token> (a join stores the server in {})",
                crate::flags::SERVER_VAR,
                config.identity_dir().display()
            )
        })
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
            remember_server(&directory, &token, enrolled.control_plane_port, announce);
            Ok(Some(enrolled.identity))
        }
    }
}

/// Stores the control-plane endpoint a join taught (ADR-0007): the token's
/// host and the answer's control-plane port, beside the credential, so a
/// restart needs no `--server`. An answer without the port (an older
/// server), or an endpoint the store refuses, stores nothing — announced,
/// not fatal, because the identity itself is already safe and a `--server`
/// still works.
fn remember_server(
    directory: &Path,
    token: &horde_identity::JoinToken,
    control_plane_port: Option<u16>,
    announce: &mut dyn FnMut(String),
) {
    let Some(port) = control_plane_port else {
        return;
    };
    let endpoint = format!("https://{}:{port}", token.host());
    match horde_identity::store::save_endpoint(directory, &endpoint) {
        Ok(()) => announce(format!("stored server={endpoint}")),
        Err(error) => announce(format!("server not stored: {error}")),
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
    /// This daemon's hold on its state directory, for as long as it runs.
    pub state_lock: horde_identity::StateLock,
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
    // First, before an enrollment spends a one-time code: a second daemon
    // on this state would share its journal. Taking the lock also creates
    // the state directory, owner-only.
    let state_lock = horde_identity::StateLock::acquire(&config.state_dir, STATE_LOCK_FILE)
        .map_err(|error| format!("could not hold the state directory: {error}"))?;
    let enrolled = StoredIdentity::exists(&config.identity_dir());
    if let Some(line) = ignored_ambient_join(config, enrolled) {
        announce(line);
    }
    let plan = identity_plan(config, enrolled)?;
    let provisional_name = config.name.as_deref().unwrap_or(DEFAULT_NAME);
    let identity = resolve_identity(plan, provisional_name, announce).await?;
    let name = worker_name(identity.as_ref(), config.name.as_deref());
    let server = resolve_server(config)?;

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

    let runtime = build_runtime(config, &server, identity.as_ref(), &name, journal.clone()).await?;
    Ok(PreparedWorker {
        runtime,
        journal,
        declared: capabilities::DeclaredFacts {
            tags: config.tags.clone(),
            reachable_remotes: config.advertised_remotes.clone(),
        },
        server,
        identity,
        name,
        login_announce: Arc::new(|line| println!("{line}")),
        state_lock,
    })
}

/// Whether a probed harness may be registered: a refused version stops the
/// Worker with the reason and the fix; an untested one is registered with a
/// warning here (and journaled again on each agent's first turn).
///
/// # Errors
///
/// The refusal, naming the version, the floor, and what to do.
pub fn admit_harness(version: &horde_adapter::VersionReport) -> Result<(), String> {
    match version.verdict {
        horde_adapter::SupportVerdict::Refused => Err(format!(
            "{}; upgrade it on this machine, or start the worker without --adapter {}",
            version.describe(),
            version.table.provider
        )),
        horde_adapter::SupportVerdict::Untested => {
            tracing::warn!(harness = version.table.provider, "{}", version.describe());
            Ok(())
        }
        horde_adapter::SupportVerdict::Supported => {
            tracing::info!(harness = version.table.provider, "{}", version.describe());
            Ok(())
        }
    }
}

/// Assembles the Runtime a configuration asks for.
///
/// Async for one reason: the codex adapter probes its binary's version before
/// claiming exec interception, and a probe is a bounded child process, not a
/// guess.
async fn build_runtime(
    config: &WorkerConfig,
    server: &str,
    identity: Option<&StoredIdentity>,
    name: &str,
    journal: Journal,
) -> Result<Runtime, String> {
    // One lazy channel for everything that rides the Worker's own connection
    // to the Server: attachment transfer, and the per-agent MCP endpoints'
    // control-plane verbs. Lazy: a Server that is down at startup is the
    // reconnect loop's business, not a reason to refuse to start.
    let channel = server_channel(server, identity)?;
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
            AdapterChoice::Claude => {
                // Probed, not assumed: the version picks the permission route
                // (stdio from 2.1.323) and a refused version stops the Worker
                // here, loudly, rather than failing every spawn (W12c).
                let adapter =
                    crate::ClaudeAdapter::new(harness_profile(config, "claude", &journal))
                        .probe_version()
                        .await;
                admit_harness(&adapter.version)?;
                runtime.with_adapter(Arc::new(adapter))
            }
            AdapterChoice::Codex => {
                // Belt and braces on the transcript-23 hazard: the flag was
                // validated at parse, and the value is re-validated here so a
                // config constructed programmatically gets the same gate. The
                // codec pins the wire value regardless.
                horde_adapter_codex::approvals_reviewer_from_config(
                    config.codex_approvals_reviewer.as_deref(),
                )?;
                // Probed, not assumed: the per-kind gating claims are
                // version-gated (ADR-0005, 2026-08-19 and 2026-11-13
                // amendments), and a refused version stops the Worker here.
                let adapter = crate::CodexAdapter::new(harness_profile(config, "codex", &journal))
                    .probe_version()
                    .await;
                admit_harness(&adapter.version)?;
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
/// registration after a materialize advertises the instances now on disk —
/// and after a login, the accounts `logins` now holds a sign-in for.
async fn fresh_dial_config(
    prepared: &PreparedWorker,
    logins: &crate::login::LoginPlane,
) -> DialConfig {
    let warm = match prepared.runtime.workspaces() {
        Some(workspaces) => Some(workspaces.warm_instances().await),
        None => None,
    };
    let mut advertised =
        capabilities::advertise(prepared.runtime.adapters(), warm, &prepared.declared);
    advertised.provider_accounts = logins.held_accounts();
    DialConfig {
        endpoint: prepared.server.clone(),
        worker_id: prepared.name.clone(),
        hello: capabilities::hello(&prepared.name),
        capabilities: advertised,
        identity: prepared.identity.clone(),
        // An unreadable head reports nothing rather than zero: zero would
        // read as a lost journal and refuse a healthy Worker.
        last_local_journal_sequence: prepared.journal.last_local_sequence().await.ok(),
    }
}

/// What one connection's end means for the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fate {
    /// Wait out the [`Backoff`] and dial again — transport weather, a Server
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
    // What the last run accepted and never finished runs first, connected or
    // not (ADR-0012): a revival owed to a held message must not wait for the
    // Server to come back.
    let failure = match prepared.runtime.replay_inbox().await {
        Ok(replayed) => {
            announce(format!("inbox-replayed commands={replayed}"));
            connection_loop(&mut prepared, shutdown.as_mut(), announce).await
        }
        Err(error) => Some(error.to_string()),
    };

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
    let mut backoff = Backoff::default();
    loop {
        // Rebuilt per connection from the registered adapters: recipes are
        // static data, and collecting them here keeps the borrow on the
        // runtime out of the mutable run below. Built before the dial,
        // because the accounts its recipes sign in are advertised.
        let logins = Arc::new(
            crate::login::LoginPlane::from_adapters(
                prepared.runtime.adapters(),
                prepared.login_announce.clone(),
            )
            .with_workspaces(prepared.runtime.workspaces().cloned()),
        );
        let config = fresh_dial_config(prepared, &logins).await;
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
                    if pause_before_redial(shutdown.as_mut(), backoff.next_delay()).await {
                        continue;
                    }
                    return None;
                }
            },
        };
        backoff.reset();
        announce(format!(
            "registered worker={} resume={}",
            connection.worker_id(),
            registration.resume_from_journal_sequence
        ));

        let ended = tokio::select! {
            () = shutdown.as_mut() => return None,
            ended = connection.run(&prepared.journal, &mut prepared.runtime, logins) => ended,
        };
        match fate_of(&ended) {
            Fate::Fatal(reason) => return Some(reason),
            Fate::Redial(reason) => {
                announce(format!("disconnected reason={reason}"));
                if !pause_before_redial(shutdown.as_mut(), backoff.next_delay()).await {
                    return None;
                }
            }
        }
    }
}

/// Parks for `delay` unless shutdown arrives first.
///
/// `true` means "keep going"; `false` means shutdown won the wait.
async fn pause_before_redial(
    shutdown: core::pin::Pin<&mut impl Future<Output = ()>>,
    delay: core::time::Duration,
) -> bool {
    tokio::select! {
        () = shutdown => false,
        () = tokio::time::sleep(delay) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdapterChoice, DEFAULT_NAME, Fate, IdentityPlan, JoinRequest, WorkerConfig, fake_script,
        fate_of, identity_plan, invocation, worker_name,
    };
    use crate::error::WorkerError;

    fn parsed(arguments: &[&str]) -> Result<WorkerConfig, String> {
        invocation(arguments.iter().map(|s| (*s).to_owned()))
    }

    /// The backoff ceiling doubles per consecutive failure from the base and
    /// stops at the cap — including past any shift that could overflow.
    #[test]
    fn the_backoff_ceiling_doubles_to_the_cap() {
        use super::{RETRY_BASE, RETRY_CAP, backoff_ceiling};
        use core::time::Duration;
        assert_eq!(backoff_ceiling(0), RETRY_BASE);
        assert_eq!(backoff_ceiling(1), Duration::from_secs(1));
        assert_eq!(backoff_ceiling(2), Duration::from_secs(2));
        assert_eq!(backoff_ceiling(4), Duration::from_secs(8));
        assert_eq!(backoff_ceiling(5), RETRY_CAP, "16 s is capped to 10");
        assert_eq!(backoff_ceiling(u32::MAX), RETRY_CAP);
    }

    /// Each wait is drawn from the upper half of its ceiling: the lowest
    /// draw is exactly half, the highest exactly the ceiling, and the draw
    /// wraps rather than overshooting.
    #[test]
    fn a_jittered_wait_lies_in_the_upper_half_of_its_ceiling() {
        use super::jittered;
        use core::time::Duration;
        let ceiling = Duration::from_millis(1000);
        assert_eq!(jittered(ceiling, 0), Duration::from_millis(500));
        assert_eq!(jittered(ceiling, 500), Duration::from_millis(1000));
        assert_eq!(jittered(ceiling, 501), Duration::from_millis(500));
        assert_eq!(jittered(ceiling, 123), Duration::from_millis(623));
        assert_eq!(
            jittered(Duration::from_millis(1), 7),
            Duration::from_millis(1)
        );
    }

    /// The schedule counts failures and a registration resets it; real
    /// entropy stays within the same bounds.
    #[test]
    fn the_backoff_grows_with_failures_and_resets_on_registration() {
        use super::{Backoff, RETRY_BASE};
        use core::time::Duration;
        let mut backoff = Backoff::with_random(|| 0);
        assert_eq!(backoff.next_delay(), Duration::from_millis(250));
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
        assert_eq!(backoff.next_delay(), Duration::from_millis(1000));
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(250));

        // The jitter source genuinely varies: eight equal 64-bit draws would
        // be Workers redialing in lockstep, which is what jitter is for.
        let draws: std::collections::BTreeSet<u64> = (0..8).map(|_| super::entropy()).collect();
        assert!(draws.len() > 1, "the jitter never varies: {draws:?}");

        let mut real = Backoff::default();
        let first = real.next_delay();
        assert!(first >= RETRY_BASE / 2 && first <= RETRY_BASE, "{first:?}");
    }

    #[test]
    fn a_bare_invocation_takes_every_default() {
        let config = parsed(&[]).expect("no flags is a valid daemon");
        assert_eq!(
            config.server, None,
            "no default server: the stored endpoint or a refusal decides"
        );
        assert!(
            config.state_dir.ends_with(".horde/worker"),
            "state defaults under HOME, never the working directory: {}",
            config.state_dir.display()
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
            refused.contains("--fake-login-callback-port") && refused.contains("not-a-port"),
            "{refused}"
        );
        let refused = parsed(&[
            "--adapter",
            "fake",
            "--fake-login",
            "/tmp/fake-login",
            "--fake-login-callback-port",
            "70000",
        ])
        .expect_err("no TCP port is that big");
        assert!(refused.contains("70000"), "{refused}");
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
        assert_eq!(config.server.as_deref(), Some("https://horde.example:7777"));
        assert_eq!(config.state_dir, std::path::PathBuf::from("/tmp/w"));
        assert_eq!(
            config.join,
            Some(JoinRequest::explicit(
                "horde://join/horde.example:7778/00ff.abcd".to_owned()
            )),
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
            // Named as unexpected, precisely: a parser that instead treated
            // the stray as a valued flag would eat the next argument and
            // complain about a missing value for a flag that never existed.
            (&["--bogus", "x"][..], "unexpected argument '--bogus'"),
            (&["positional"][..], "unexpected argument 'positional'"),
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

        // An ambient token joins a fresh state and is ignored over an
        // existing identity — a variable left set must not fail restarts.
        let ambient = WorkerConfig {
            join: Some(JoinRequest::ambient(
                "horde://join/x:1/00ff.abcd".to_owned(),
            )),
            ..load
        };
        assert_eq!(
            identity_plan(&ambient, true).expect("a plan"),
            IdentityPlan::Load(std::path::PathBuf::from("/tmp/w/identity"))
        );
        assert!(matches!(
            identity_plan(&ambient, false),
            Ok(IdentityPlan::Join { .. })
        ));
    }

    /// The ignored-ambient-token line is owed only for an ambient token
    /// over an existing identity — not for an explicit one, not for no
    /// token, not before anything is enrolled.
    #[test]
    fn an_ambient_token_is_announced_ignored_only_over_an_identity() {
        let bare = parsed(&["--state-dir", "/tmp/w"]).expect("parses");
        let with = |join: Option<JoinRequest>| WorkerConfig {
            join,
            ..bare.clone()
        };
        let ambient = with(Some(JoinRequest::ambient("t".to_owned())));
        assert_eq!(
            super::ignored_ambient_join(&ambient, true),
            Some("ignoring HORDE_JOIN_TOKEN: /tmp/w/identity already holds an identity".to_owned())
        );
        assert_eq!(super::ignored_ambient_join(&ambient, false), None);
        let explicit = with(Some(JoinRequest::explicit("t".to_owned())));
        assert_eq!(super::ignored_ambient_join(&explicit, true), None);
        assert_eq!(super::ignored_ambient_join(&with(None), true), None);
    }

    /// Where to dial, by precedence: the configured server, then the
    /// endpoint a join stored, else a refusal naming every source.
    #[test]
    fn the_server_is_configured_then_stored_then_refused() {
        let state = tempfile::tempdir().expect("a temp dir");
        let bare = WorkerConfig {
            server: None,
            ..parsed(&["--state-dir", &state.path().to_string_lossy()]).expect("parses")
        };
        let refused = super::resolve_server(&bare).expect_err("nothing to dial");
        assert!(
            refused.contains("--server")
                && refused.contains("HORDE_SERVER")
                && refused.contains("--join"),
            "{refused}"
        );
        horde_identity::store::save_endpoint(&bare.identity_dir(), "https://stored:7777")
            .expect("stores");
        assert_eq!(
            super::resolve_server(&bare).as_deref(),
            Ok("https://stored:7777")
        );
        let flagged = WorkerConfig {
            server: Some("https://flag:7777".to_owned()),
            ..bare
        };
        assert_eq!(
            super::resolve_server(&flagged).as_deref(),
            Ok("https://flag:7777"),
            "the flag outranks the stored endpoint"
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

    /// A scripted question parses into the fake's question step, every
    /// question needing an id and the step needing at least one.
    #[test]
    fn a_scripted_question_parses_and_its_ids_are_required() {
        let script = fake_script(
            r#"[{"steps": [{"ask_question": {"id": "q-1", "tool": "AskUserQuestion",
                "questions": [{"id": "colour", "header": "C", "question": "Which?",
                               "options": ["Red", "Blue"], "multi_select": true}]}}]}]"#,
        )
        .expect("a valid question");
        let horde_adapter_fake::Step::AskQuestion {
            tool_call_id,
            tool_name,
            questions,
        } = &script.turns[0].steps[0]
        else {
            panic!("the step must be the question");
        };
        assert_eq!(
            (tool_call_id.as_str(), tool_name.as_str()),
            ("q-1", "AskUserQuestion")
        );
        let [question] = questions.as_slice() else {
            panic!("one question, got {questions:?}");
        };
        assert_eq!(question.id, "colour");
        assert_eq!(question.header, "C");
        assert_eq!(question.question, "Which?");
        assert_eq!(
            question
                .options
                .iter()
                .map(|o| o.label.as_str())
                .collect::<Vec<_>>(),
            vec!["Red", "Blue"]
        );
        assert!(question.multi_select && question.allows_free_text);

        for (json, needle) in [
            (
                r#"[{"steps": [{"ask_question": {"id": "q", "tool": "T", "questions": [{"question": "?"}]}}]}]"#,
                "non-empty id",
            ),
            (
                r#"[{"steps": [{"ask_question": {"id": "q", "tool": "T", "questions": []}}]}]"#,
                "non-empty questions",
            ),
            (r#"[{"steps": [{"ask_question": {"tool": "T"}}]}]"#, "id"),
        ] {
            let refused = fake_script(json).expect_err("must refuse");
            assert!(refused.contains(needle), "{json}: {refused}");
        }
    }

    /// A refused harness stops the worker with the version, the floor and
    /// the fix; untested and supported ones are admitted.
    #[test]
    fn only_a_refused_harness_is_turned_away() {
        let codex = horde_adapter_codex::SUPPORT;
        let refused = super::admit_harness(&codex.classify("codex-cli 0.145.0"))
            .expect_err("below the floor");
        assert!(refused.contains("codex 0.145.0 is refused"), "{refused}");
        assert!(refused.contains("--adapter codex"), "{refused}");
        assert_eq!(
            super::admit_harness(&codex.classify("codex-cli 0.167.0")),
            Ok(())
        );
        assert_eq!(
            super::admit_harness(&codex.classify("codex-cli 9.0.0")),
            Ok(())
        );
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
    /// shutdown it waits exactly the delay it was given and says keep-going
    /// (a pause that returns early is a hot spin against a dead server; one
    /// that returns `false` shuts a healthy daemon down), and a completed
    /// shutdown wins the pause immediately.
    #[tokio::test(start_paused = true)]
    async fn the_redial_pause_waits_the_delay_and_yields_to_shutdown() {
        let pending = std::future::pending::<()>();
        tokio::pin!(pending);
        let before = tokio::time::Instant::now();
        assert!(
            super::pause_before_redial(pending.as_mut(), super::RETRY_BASE).await,
            "no shutdown means keep going"
        );
        assert_eq!(
            before.elapsed(),
            super::RETRY_BASE,
            "the pause is the delay, exactly — the paused clock makes this deterministic"
        );

        let ready = async {};
        tokio::pin!(ready);
        let before = tokio::time::Instant::now();
        assert!(
            !super::pause_before_redial(ready.as_mut(), super::RETRY_BASE).await,
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
