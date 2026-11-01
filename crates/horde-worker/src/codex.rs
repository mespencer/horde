//! The live Codex adapter: the pure codec driven over `codex app-server`.
//!
//! The protocol layer lives in `horde-adapter-codex` and is proven against
//! recorded bytes; this file is the process half — spawning the binary, the
//! `initialize`/`thread/start` handshake, request/response correlation, and
//! the wire discipline around server→client requests. Its rules, each one
//! measured rather than assumed:
//!
//! * **Every server request gets a reply.** An unanswered one parks the
//!   harness at `waitingOnApproval` forever (transcript 19). MCP tool-call
//!   elicitations and exec approvals (`item/commandExecution/requestApproval`,
//!   re-probe transcripts 20–33) become permission events the chain
//!   adjudicates; everything else is refused — deny-by-default, because Horde
//!   has no chain seam for it (ADR-0005, as amended 2026-08-19).
//! * **`send` never awaits a turn response.** A turn request's failure comes
//!   back interleaved with the stream, and blocking the pump on it would
//!   deadlock against an elicitation only the pump can answer. Failures
//!   surface through `next_event`, where the pump is actually listening.
//! * **The sandbox tier is chosen here, from the agent's permission mode**,
//!   and the choice plus its reason ride the first turn into the journal as a
//!   harness diagnostic — the tier *is* the enforcement, so it must be
//!   auditable.

use std::collections::VecDeque;

use async_trait::async_trait;
use horde_adapter::{
    Adapter, AdapterError, AgentEvent, AgentSession, AgentSpec, Capabilities, Delivery,
    McpEndpoint, PermissionDecision, ProviderSessionId, Result, ToolCallId, TurnInput,
};
use horde_adapter_codex::codec::{
    ExecDecision, ThreadConfig, decode_line, encode_decision_decline, encode_elicitation_response,
    encode_exec_approval_response, encode_initialize, encode_initialized, encode_interrupt,
    encode_method_not_found, encode_steer, encode_thread_resume, encode_thread_start,
    encode_turn_start,
};
use horde_adapter_codex::sandbox::SandboxPolicy;
use horde_adapter_codex::turn::{ApprovalKind, TurnAssembler};
use horde_adapter_codex::wire::{EXEC_APPROVAL_METHOD, Frame, MCP_ELICITATION_METHOD, RpcError};

use crate::harness::HarnessProfile;
use crate::process::{HarnessSpec, SupervisedHarness};

/// The variable `codex` reads its home (config, credentials, rollouts) from.
const CONFIG_ENV_VAR: &str = "CODEX_HOME";

/// How many frames a handshake response may be preceded by before the adapter
/// concludes the harness is not going to answer. Bounds the wait by count, not
/// clock: each frame read is real progress, and a harness that floods without
/// answering is as broken as one that hangs.
const MAX_FRAMES_AWAITING_RESPONSE: u32 = 4096;

/// How to launch `codex` on this Worker.
#[derive(Debug, Clone)]
pub struct CodexAdapter {
    /// Binary to run. A path, so tests can point at a stub and never touch the
    /// operator's real CLI.
    pub program: String,
    /// The Worker-owned `CODEX_HOME`s and process-group ledger every launch
    /// runs under (ADR-0010) — never the operator's `~/.codex`, whose
    /// `config.toml`, MCP servers, and trusted projects would all apply.
    pub profile: HarnessProfile,
    /// Permission-mode name → sandbox tier. Everything unmapped runs
    /// read-only; see `horde_adapter_codex::sandbox`.
    pub sandbox_policy: SandboxPolicy,
    /// Extra environment for every child this adapter spawns. Per-child,
    /// never process-wide.
    pub extra_env: Vec<(String, String)>,
    /// How long a handshake request may go unanswered before the spawn is
    /// declared broken. Generous for a real harness; tests shrink it so a
    /// child that answers nothing fails in seconds.
    pub response_ceiling: std::time::Duration,
    /// Whether the binary this adapter launches routes exec approvals — the
    /// version gate on [`Capability::PreAuthorizeExecCommands`]. Set by
    /// [`Self::probe_exec_interception`]; defaults to the floor, because a
    /// claim nobody measured is a chain that silently gates nothing.
    pub exec_interception: ExecInterception,
}

/// What is known about the launched codex's exec-approval routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecInterception {
    /// Unprobed, unparseable, or below 0.147: only the sandbox tier enforces
    /// exec, and the capability claim stays at the floor. The reason is kept
    /// for the journal, so "why was this agent not gated?" has an answer.
    SandboxOnly { reason: String },
    /// The probed binary is 0.147+ (transcripts 25/33): exec approvals route
    /// to the client under the granular policy this adapter always sends.
    Routed { version: String },
}

impl ExecInterception {
    /// The unprobed default.
    fn unprobed() -> Self {
        Self::SandboxOnly {
            reason: "codex version not probed".to_owned(),
        }
    }

    /// One journal line naming the enforcement exec calls actually get.
    fn diagnostic(&self) -> String {
        match self {
            Self::Routed { version } => {
                format!("codex exec approvals: routed to the permission chain ({version})")
            }
            Self::SandboxOnly { reason } => {
                format!("codex exec approvals: sandbox tier only ({reason})")
            }
        }
    }
}

impl CodexAdapter {
    /// An adapter that runs the real `codex` on `PATH` under `profile`.
    #[must_use]
    pub fn new(profile: HarnessProfile) -> Self {
        Self {
            program: "codex".to_owned(),
            profile,
            sandbox_policy: SandboxPolicy::default(),
            extra_env: Vec::new(),
            response_ceiling: std::time::Duration::from_secs(30),
            exec_interception: ExecInterception::unprobed(),
        }
    }

    /// Points the adapter at an explicit binary — how the process tests reach
    /// the stub harness without ever risking the operator's real CLI.
    /// Builder-style, like [`SandboxPolicy::with_mode`].
    #[must_use]
    pub fn with_program(mut self, program: impl Into<String>) -> Self {
        self.program = program.into();
        self
    }

    /// Probes the launched binary's version and records whether it routes
    /// exec approvals (0.147+, transcripts 25/33 — the gate is
    /// [`horde_adapter_codex::routes_exec_approvals`]).
    ///
    /// One `--version` invocation, bounded by the response ceiling. Every
    /// failure — a binary that is missing, hangs, errs, or prints something
    /// unreadable — fails closed to [`ExecInterception::SandboxOnly`] with the
    /// reason kept for the journal: the floor claim is always honest, whereas
    /// a guessed `Routed` advertises a chain that would gate nothing.
    #[must_use]
    pub async fn probe_exec_interception(mut self) -> Self {
        self.exec_interception = probe_version(&self.program, self.response_ceiling).await;
        self
    }

    /// The full launch: the account's Worker-owned `CODEX_HOME`, and the
    /// endpoint as `-c` overrides whose header values ride the child's
    /// environment rather than its argv.
    fn harness_spec(&self, spec: &AgentSpec) -> core::result::Result<HarnessSpec, String> {
        let overrides = mcp_server_overrides(spec.horde_endpoint.as_ref());
        let mut args = vec!["app-server".to_owned(), "--stdio".to_owned()];
        args.extend(overrides.args);
        let mut env = self.extra_env.clone();
        env.extend(overrides.env);

        Ok(HarnessSpec {
            args,
            env,
            cwd: spec.cwd.clone(),
            ..self
                .profile
                .spec(&self.program, CONFIG_ENV_VAR, spec.account.as_ref())?
        })
    }

    /// Starts a supervised `codex app-server` and drives the handshake.
    ///
    /// Spawning and reviving differ only in the thread request, so they share
    /// the launch: a second copy is a second place for the handshake to drift.
    async fn start(
        &self,
        spec: &AgentSpec,
        resume_thread: Option<&ProviderSessionId>,
    ) -> Result<Box<dyn AgentSession>> {
        let launch = self.harness_spec(spec).map_err(launch_failure)?;
        let harness = SupervisedHarness::spawn(&launch)
            .await
            .map_err(|error| launch_failure(error.to_string()))?;

        // The tier decision, made before the harness hears anything: this is
        // the enforcement for every exec call the agent will ever make.
        let tier = self
            .sandbox_policy
            .tier_for(spec.permission_mode.as_deref().unwrap_or(""));
        let config = ThreadConfig {
            cwd: spec.cwd.as_ref().map(|cwd| cwd.display().to_string()),
            sandbox: tier.tier,
            model: spec.model.clone(),
            developer_instructions: spec.append_system_prompt.clone(),
        };

        let mut session = LiveCodexSession::new(harness);
        session.response_ceiling = self.response_ceiling;
        session.assembler.note_startup_diagnostic(format!(
            "codex sandbox {}: {}",
            tier.tier.wire_name(),
            tier.reason
        ));
        // The other half of the enforcement audit trail: whether exec calls
        // are chain-gated on this binary, or the tier is all there is.
        session
            .assembler
            .note_startup_diagnostic(self.exec_interception.diagnostic());

        // initialize → initialized → thread/start|thread/resume. Awaiting is
        // safe here: no turn is running, so nothing the pump must answer can
        // arrive before the thread response.
        let id = session.ids.next();
        session.write(&encode_initialize(id)).await?;
        session.await_response(id).await?.map_err(spawn_failure)?;
        session.write(&encode_initialized()).await?;

        let id = session.ids.next();
        let request = match resume_thread {
            Some(thread) => encode_thread_resume(id, thread.as_str(), &config),
            None => encode_thread_start(id, &config),
        };
        session.write(&request).await?;
        let result = session
            .await_response(id)
            .await?
            .map_err(|error| thread_failure(spec, error))?;

        let thread_id = result
            .pointer("/thread/id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| AdapterError::Protocol {
                message: "the thread response named no thread id".to_owned(),
                raw: result.to_string(),
            })?;
        let model = result
            .get("model")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();

        // Announced from the response rather than awaited from the
        // thread/started notification, which the harness may not emit until
        // the first turn (transcript 03): the id that makes the agent
        // revivable must reach the journal before anything can be lost. The
        // notification re-announces the same id later, which conformance
        // permits.
        session.thread_id = Some(thread_id.clone());
        session.session_id = Some(ProviderSessionId::new(thread_id));
        session.pending.push_back(AgentEvent::SessionEstablished {
            provider_session_id: session
                .session_id
                .clone()
                .unwrap_or_else(|| ProviderSessionId::new("")),
            model,
        });

        Ok(Box::new(session))
    }
}

#[async_trait]
impl Adapter for CodexAdapter {
    fn provider(&self) -> &str {
        horde_adapter_codex::PROVIDER
    }

    fn capabilities(&self) -> Capabilities {
        // The exec-interception claim is earned by the probe, never assumed:
        // an unprobed adapter claims the floor, so placement can only rely on
        // per-call exec gating where a 0.147+ binary actually answered.
        match &self.exec_interception {
            ExecInterception::Routed { .. } => {
                horde_adapter_codex::capabilities_with_exec_interception()
            }
            ExecInterception::SandboxOnly { .. } => horde_adapter_codex::capabilities(),
        }
    }

    fn login_recipe(&self) -> Option<horde_adapter::LoginRecipe> {
        // `codex login` runs a local OAuth-callback server on loopback port
        // 1455, fixed by the CLI. PROVISIONAL until verified against the
        // live CLI (see the W9a handoff flags).
        // It signs in the Worker-owned CODEX_HOME the agents run under
        // (ADR-0010), not the operator's own.
        Some(
            horde_adapter::LoginRecipe::with_callback(
                self.program.clone(),
                vec!["login".to_owned()],
                1455,
            )
            .with_env(self.profile.login_env(CONFIG_ENV_VAR))
            .with_accounts(CONFIG_ENV_VAR, self.profile.root()),
        )
    }

    async fn spawn(&self, spec: &AgentSpec) -> Result<Box<dyn AgentSession>> {
        self.start(spec, None).await
    }

    async fn resume(
        &self,
        spec: &AgentSpec,
        provider_session_id: &ProviderSessionId,
    ) -> Result<Box<dyn AgentSession>> {
        // Under the SAME CODEX_HOME: rollouts live inside the account
        // directory, and resuming under another account fails loudly rather
        // than starting a silently fresh conversation. Replayed usage from the
        // rollout is deduplicated by the assembler (transcript 16).
        self.start(spec, Some(provider_session_id)).await
    }
}

/// The `-c mcp_servers.…` overrides that give one agent its Horde endpoint,
/// and the environment their header values ride.
#[derive(Debug, Default, PartialEq, Eq)]
struct McpOverrides {
    args: Vec<String>,
    env: Vec<(String, String)>,
}

/// The variable one header value travels in. `TOKEN` in the name is
/// deliberate: codex's default shell-environment policy strips variables
/// whose names match `*TOKEN*` from the commands it runs, so the agent's own
/// shell does not inherit the credential.
fn header_env_var(index: usize) -> String {
    format!("HORDE_MCP_HEADER_TOKEN_{index}")
}

/// Builds the overrides for one endpoint.
///
/// Per-process flags rather than `config.toml` writes, because `CODEX_HOME` is
/// shared by every agent on the same Provider Account and a config write from
/// one spawn would race every concurrent one. Measured working in the
/// transcript 17–19 captures.
///
/// Header *values* never touch argv (ADR-0010): `env_http_headers` maps each
/// header name to a variable the child's environment carries, so `ps` shows
/// the variable's name, never the bearer token. Verified against codex-cli
/// 0.165.0 (`codex mcp list --json` reports the mapping as
/// `env_http_headers`).
fn mcp_server_overrides(endpoint: Option<&McpEndpoint>) -> McpOverrides {
    let mut overrides = McpOverrides::default();
    let mut push = |assignment: String| {
        overrides.args.push("-c".to_owned());
        overrides.args.push(assignment);
    };
    match endpoint {
        None => {}
        Some(McpEndpoint::Http { url, headers }) => {
            push(format!("mcp_servers.horde.url={}", toml_string(url)));
            if !headers.is_empty() {
                // The identity pair must ride *every* request (the
                // endpoint's gate refuses unidentified ones), which is what
                // the http-headers mechanisms do.
                let rendered: Vec<String> = headers
                    .iter()
                    .enumerate()
                    .map(|(index, (name, _))| {
                        format!(
                            "{}={}",
                            toml_string(name),
                            toml_string(&header_env_var(index))
                        )
                    })
                    .collect();
                push(format!(
                    "mcp_servers.horde.env_http_headers={{{}}}",
                    rendered.join(",")
                ));
                overrides.env = headers
                    .iter()
                    .enumerate()
                    .map(|(index, (_, value))| (header_env_var(index), value.clone()))
                    .collect();
            }
        }
        Some(McpEndpoint::Stdio { command, args }) => {
            push(format!(
                "mcp_servers.horde.command={}",
                toml_string(command)
            ));
            let rendered: Vec<String> = args.iter().map(|arg| toml_string(arg)).collect();
            push(format!("mcp_servers.horde.args=[{}]", rendered.join(",")));
        }
    }
    overrides
}

/// A TOML basic string. The value side of `-c` is parsed as TOML, so anything
/// unescaped in a path or URL would silently truncate the override.
fn toml_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

/// One bounded `--version` run, classified.
///
/// The child is killed if the ceiling fires (`kill_on_drop`), so a wedged
/// binary costs the ceiling once and leaves nothing behind.
async fn probe_version(program: &str, ceiling: std::time::Duration) -> ExecInterception {
    let probe = tokio::process::Command::new(program)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output();
    match tokio::time::timeout(ceiling, probe).await {
        Err(_elapsed) => ExecInterception::SandboxOnly {
            reason: format!("`{program} --version` answered nothing within {ceiling:?}"),
        },
        Ok(Err(error)) => ExecInterception::SandboxOnly {
            reason: format!("`{program} --version` could not run: {error}"),
        },
        Ok(Ok(output)) => {
            let text = String::from_utf8_lossy(&output.stdout);
            let text = text.trim();
            if horde_adapter_codex::routes_exec_approvals(text) {
                ExecInterception::Routed {
                    version: text.to_owned(),
                }
            } else {
                ExecInterception::SandboxOnly {
                    reason: format!(
                        "`{program} --version` reported {text:?}, which does not route \
                         exec approvals (0.147+ does — transcripts 25/33)"
                    ),
                }
            }
        }
    }
}

/// Whether an RPC failure means the account is not signed in, as opposed to
/// the harness being broken.
///
/// Named predicate: the scheduler's answer differs (try another account, not
/// another Worker), so misclassifying strands work on a healthy machine.
fn is_auth_failure(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("unauthorized")
        || lowered.contains("401")
        || lowered.contains("not logged in")
        || lowered.contains("login")
}

/// A launch that never got as far as a running harness.
fn launch_failure(message: String) -> AdapterError {
    AdapterError::Spawn {
        provider: horde_adapter_codex::PROVIDER.to_owned(),
        message,
    }
}

fn spawn_failure(error: RpcError) -> AdapterError {
    AdapterError::Spawn {
        provider: horde_adapter_codex::PROVIDER.to_owned(),
        message: format!("initialize failed: {} (code {})", error.message, error.code),
    }
}

fn thread_failure(spec: &AgentSpec, error: RpcError) -> AdapterError {
    if is_auth_failure(&error.message) {
        AdapterError::Unauthenticated {
            provider: horde_adapter_codex::PROVIDER.to_owned(),
            account: spec.account.as_ref().map(ToString::to_string),
            message: error.message,
        }
    } else {
        AdapterError::Spawn {
            provider: horde_adapter_codex::PROVIDER.to_owned(),
            message: format!(
                "the thread request failed: {} (code {})",
                error.message, error.code
            ),
        }
    }
}

/// Mints the session's JSON-RPC request ids: distinct, increasing, never
/// zero (zero would collide with nothing, but an id repeated once is a reply
/// mis-correlated forever — so the property is pinned by its own test).
#[derive(Debug, Default)]
struct RequestIds(u64);

impl RequestIds {
    fn next(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}

/// What the session still owes an answer for on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OutstandingRequest {
    /// A `turn/start`; an error reply fails the agent's turn loudly.
    TurnStart,
    /// A `turn/steer`, with the text to fall back to `turn/start` with if the
    /// steer lost its race against the turn ending.
    Steer { text: String },
    /// A `turn/interrupt`; its reply carries nothing.
    Interrupt,
}

/// One live `codex app-server` conversation.
pub struct LiveCodexSession {
    harness: SupervisedHarness,
    assembler: TurnAssembler,
    pending: VecDeque<AgentEvent>,
    session_id: Option<ProviderSessionId>,
    thread_id: Option<String>,
    ids: RequestIds,
    /// Client requests whose replies are still in flight, by JSON-RPC id.
    in_flight: Vec<(u64, OutstandingRequest)>,
    /// Input parked behind an interrupt: delivered as a fresh `turn/start`
    /// once the interrupted turn actually closes. Starting it earlier would
    /// race the idle that ends the old turn (transcript 11).
    deferred_input: Option<String>,
    /// See [`CodexAdapter::response_ceiling`].
    response_ceiling: std::time::Duration,
    /// Set once the child's stdout has closed, so later reads report the exit
    /// instead of blocking on a pipe nobody will write to again.
    ended: bool,
}

impl LiveCodexSession {
    fn new(harness: SupervisedHarness) -> Self {
        Self {
            harness,
            assembler: TurnAssembler::new(),
            pending: VecDeque::new(),
            session_id: None,
            thread_id: None,
            ids: RequestIds::default(),
            in_flight: Vec::new(),
            deferred_input: None,
            response_ceiling: std::time::Duration::from_secs(30),
            ended: false,
        }
    }

    async fn write(&mut self, line: &str) -> Result<()> {
        self.harness
            .write_line(line)
            .await
            .map_err(|error| AdapterError::Protocol {
                message: error.to_string(),
                raw: String::new(),
            })
    }

    /// Reads one line, or reports the harness gone.
    async fn read_frame(&mut self) -> Result<Option<Frame>> {
        let line = self
            .harness
            .read_line()
            .await
            .map_err(|error| AdapterError::Protocol {
                message: error.to_string(),
                raw: String::new(),
            })?;
        match line {
            Some(line) => decode_line(&line).map(Some),
            None => {
                self.ended = true;
                Ok(None)
            }
        }
    }

    /// Handshake-only: reads until the reply to `id`, routing everything else
    /// through the ordinary frame handling.
    ///
    /// Bounded twice, per QUALITY_GATES "bound every wait": by structure (a
    /// for-range over the frame budget — every iteration consumes a frame, so
    /// there is no counter whose arithmetic could quietly stop counting) and
    /// by a clock on each read (each read genuinely awaits the pipe, which is
    /// exactly the case a ceiling is legitimate for — a harness that writes
    /// nothing must become an error, not a hang).
    async fn await_response(
        &mut self,
        id: u64,
    ) -> Result<core::result::Result<serde_json::Value, RpcError>> {
        for _ in 0..MAX_FRAMES_AWAITING_RESPONSE {
            let read = tokio::time::timeout(self.response_ceiling, self.read_frame());
            let Ok(frame) = read.await else {
                return Err(AdapterError::Protocol {
                    message: format!(
                        "no reply to request {id} within {:?}",
                        self.response_ceiling
                    ),
                    raw: String::new(),
                });
            };
            let Some(frame) = frame? else {
                // EOF: the child is gone. Reported without reaping — waiting
                // on a child that might not have exited yet is a hang wearing
                // an error's clothes.
                return Err(AdapterError::HarnessExited { exit_code: None });
            };
            if let Frame::Response {
                id: got,
                result,
                error,
            } = &frame
                && *got == id
            {
                return Ok(match error {
                    Some(error) => Err(error.clone()),
                    None => Ok(result.clone().unwrap_or(serde_json::Value::Null)),
                });
            }
            self.route_frame(frame).await?;
        }
        Err(AdapterError::Protocol {
            message: format!(
                "no reply to request {id} within {MAX_FRAMES_AWAITING_RESPONSE} frames"
            ),
            raw: String::new(),
        })
    }

    /// Refuses work on a session whose harness is gone — a named predicate,
    /// because "may this session still be driven?" is one question asked from
    /// several places.
    fn refuse_if_ended(&self) -> Result<()> {
        if self.ended {
            return Err(AdapterError::HarnessExited { exit_code: None });
        }
        Ok(())
    }

    /// Writes a `turn/interrupt`, telling the assembler first: the turn will
    /// close on thread/status → idle with no terminal frame while
    /// already-generated deltas keep arriving (transcript 11), and only a
    /// noted interrupt lets that idle be read as a cancellation.
    ///
    /// Any approval still parked is answered first — `cancel` in the exec
    /// vocabulary, `decline` in the elicitation one — because the dying turn
    /// does not release the harness from waiting on it: an unanswered request
    /// parks the thread whatever else happens (transcripts 19/25/28). A
    /// withdrawal is not a chain refusal, so nothing lands in the denials.
    async fn request_interrupt(&mut self) -> Result<()> {
        for withdrawn in self.assembler.withdraw_outstanding() {
            let (reply, encoding) = match &withdrawn.kind {
                ApprovalKind::ExecCommand { .. } => (
                    encode_exec_approval_response(withdrawn.rpc_id, &ExecDecision::Cancel),
                    ExecDecision::Cancel.wire_name(),
                ),
                ApprovalKind::McpElicitation => (
                    encode_elicitation_response(withdrawn.rpc_id, false),
                    "decline",
                ),
            };
            if let Some(event) = self.assembler.note_decision(format!(
                "codex approval withdrawn by interrupt → {encoding}"
            )) {
                self.pending.push_back(event);
            }
            self.write(&reply).await?;
        }
        self.assembler.note_interrupt_requested();
        let thread = self.thread_id.clone().unwrap_or_default();
        let id = self.ids.next();
        self.in_flight.push((id, OutstandingRequest::Interrupt));
        self.write(&encode_interrupt(id, &thread)).await
    }

    /// Whether a `turn/start` is still awaiting its reply — a turn the
    /// harness has been asked for but has not announced yet.
    fn turn_start_outstanding(&self) -> bool {
        self.in_flight
            .iter()
            .any(|(_, request)| *request == OutstandingRequest::TurnStart)
    }

    /// Starts a fresh turn with `text`.
    async fn start_turn(&mut self, text: &str) -> Result<()> {
        let thread = self.thread_id.clone().unwrap_or_default();
        let id = self.ids.next();
        self.in_flight.push((id, OutstandingRequest::TurnStart));
        self.write(&encode_turn_start(id, &thread, text)).await
    }

    /// Offers a request on an adjudicable method to the assembler; what could
    /// not become a permission event is refused in its own vocabulary.
    ///
    /// The refusal exists because a non-tool-call elicitation, or an approval
    /// whose shape drifted past recognition, still holds the harness: nothing
    /// in Horde can answer it, and leaving it unanswered parks the thread
    /// forever (transcript 19).
    async fn adjudicate_server_request(
        &mut self,
        id: u64,
        method: String,
        params: serde_json::Value,
    ) -> Result<()> {
        let events = self.assembler.feed(&Frame::ServerRequest {
            id,
            method: method.clone(),
            params,
        });
        if events.is_empty() {
            tracing::warn!(method, id, "declining an unadjudicable approval request");
            return self.write(&unadjudicable_refusal(id, &method)).await;
        }
        self.pending.extend(events);
        Ok(())
    }

    /// Handles one frame that is not the reply the caller was waiting on.
    ///
    /// This is the wire discipline in one place: notifications feed the
    /// assembler; elicitations and exec approvals become permission events;
    /// every other server request is refused; replies to our own requests
    /// settle their follow-up.
    async fn route_frame(&mut self, frame: Frame) -> Result<()> {
        match frame {
            Frame::Notification(_) => {
                let events = self.assembler.feed(&frame);
                let turn_closed = events
                    .iter()
                    .any(|event| matches!(event, AgentEvent::TurnEnded(_)));
                self.pending.extend(events);
                if let Some(id) = self.assembler.provider_session_id() {
                    self.thread_id = Some(id.as_str().to_owned());
                    self.session_id = Some(id);
                }
                if turn_closed && let Some(text) = self.deferred_input.take() {
                    // The message an interrupt delivery parked; the old turn
                    // is closed, so its replacement can start.
                    self.start_turn(&text).await?;
                }
                Ok(())
            }
            Frame::ServerRequest { id, method, params } if is_adjudicable_method(&method) => {
                self.adjudicate_server_request(id, method, params).await
            }
            Frame::ServerRequest { id, method, .. } => {
                let reply = refusal_for(id, &method);
                tracing::warn!(
                    method,
                    id,
                    "refusing a server request horde cannot adjudicate"
                );
                self.write(&reply).await
            }
            Frame::Response { id, error, .. } => {
                let outstanding = self
                    .in_flight
                    .iter()
                    .position(|(pending, _)| *pending == id)
                    .map(|index| self.in_flight.remove(index).1);
                match (outstanding, error) {
                    (Some(OutstandingRequest::Steer { text }), Some(_)) => {
                        // The steer lost its race against the turn ending —
                        // exactly what expectedTurnId exists to detect. The
                        // message still gets delivered, as the next turn.
                        self.start_turn(&text).await
                    }
                    (Some(OutstandingRequest::TurnStart), Some(error)) => {
                        // A turn that could not start is the agent failing,
                        // loudly, where the pump listens.
                        Err(AdapterError::Protocol {
                            message: format!(
                                "turn/start was refused: {} (code {})",
                                error.message, error.code
                            ),
                            raw: String::new(),
                        })
                    }
                    // Successful replies settle silently; an interrupt's
                    // reply carries nothing either way.
                    _ => Ok(()),
                }
            }
        }
    }
}

/// Whether a server request is one the assembler may turn into a permission
/// event — the two methods the §10 chain has a seam for.
///
/// Named predicate: everything else is refused outright, so widening this by
/// accident would offer the chain requests it has no vocabulary to answer.
fn is_adjudicable_method(method: &str) -> bool {
    method == MCP_ELICITATION_METHOD || method == EXEC_APPROVAL_METHOD
}

/// The refusal an adjudicable-method request gets when it could not become a
/// permission event, in that method's own reply vocabulary.
///
/// The split is the same one `refusal_for` exists for, in the other
/// direction: an elicitation answered with `decision` (or an exec approval
/// answered with `action`) deserializes to nothing server-side, and then
/// nothing was answered at all — the transcript-19 park.
fn unadjudicable_refusal(id: u64, method: &str) -> String {
    if method == MCP_ELICITATION_METHOD {
        encode_elicitation_response(id, false)
    } else {
        encode_decision_decline(id)
    }
}

/// A narrowing the exec vocabulary cannot express — the caller declines the
/// request and journals why, rather than run the un-narrowed original or
/// leave the harness parked on an unanswered request.
#[derive(Debug, PartialEq, Eq)]
struct InexpressibleNarrowing;

/// The chain's decision, mapped into codex's exec-approval vocabulary.
///
/// The full table (each row pinned by its own test):
///
/// | chain decision                            | encoding                        |
/// |-------------------------------------------|---------------------------------|
/// | deny                                      | `decline`                       |
/// | allow                                     | `accept`                        |
/// | allow + remember for session              | `acceptForSession`              |
/// | allow + narrowing == the proposal, and codex proposed an amendment | `acceptWithExecpolicyAmendment` (codex's own argv) |
/// | allow + narrowing == the proposal, no amendment proposed | `accept` (`acceptForSession` with remember) |
/// | allow + narrowing ≠ the proposal          | `InexpressibleNarrowing` → the caller declines |
///
/// The reply cannot change what runs — codex executes the command it proposed
/// or nothing — so the only "narrowing" this vocabulary can express is one
/// that restates the proposal: verbatim, it is an accept, and when it names
/// exactly the argv codex offered to whitelist, `acceptWithExecpolicyAmendment`
/// is that statement made persistent (transcript 30). A narrowing that
/// actually rewrites arguments is refused, never downgraded to running the
/// un-narrowed original — the same reject-don't-drop rule the wire layer
/// enforces. This session never advertises `RewritesToolInput`, but a
/// production Server *can* still deliver a narrowing here (nothing upstream
/// is guaranteed to gate it), so the refusal must degrade to a journaled
/// decline, not an error.
fn exec_decision_for(
    decision: &PermissionDecision,
    proposal_input: &serde_json::Value,
    proposed_amendment: Option<&[String]>,
) -> core::result::Result<ExecDecision, InexpressibleNarrowing> {
    if !decision.is_allow() {
        return Ok(ExecDecision::Decline);
    }
    match decision.updated_input() {
        Some(updated) if updated != proposal_input => Err(InexpressibleNarrowing),
        Some(_restatement) => match proposed_amendment {
            Some(amendment) => Ok(ExecDecision::AcceptWithExecpolicyAmendment {
                execpolicy_amendment: amendment.to_vec(),
            }),
            None if decision.remembers_for_session() => Ok(ExecDecision::AcceptForSession),
            None => Ok(ExecDecision::Accept),
        },
        None if decision.remembers_for_session() => Ok(ExecDecision::AcceptForSession),
        None => Ok(ExecDecision::Accept),
    }
}

/// The reply an unanswerable server request gets.
///
/// Approval-shaped requests are declined in their own vocabulary — the
/// `decision` field, not the elicitation `action` — and anything else is a
/// method-not-found error. Both are refusals; the split exists because the
/// wrong shape fails to deserialize server-side and then nothing was answered
/// at all.
fn refusal_for(id: u64, method: &str) -> String {
    if is_approval_method(method) {
        encode_decision_decline(id)
    } else {
        encode_method_not_found(id, method)
    }
}

/// Whether a server request is one of codex's `decision`-shaped approvals.
///
/// From the 0.147.0 schema's ServerRequest list. None of these reached a
/// client in any measured configuration (ADR-0005) — but the granular policy
/// this adapter sends names `sandbox_approval` and `rules`, so if a future
/// codex starts routing them, deny-by-default is what keeps the sandbox tier
/// the only granted authority.
fn is_approval_method(method: &str) -> bool {
    matches!(
        method,
        "execCommandApproval"
            | "applyPatchApproval"
            | "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
    )
}

#[async_trait]
impl AgentSession for LiveCodexSession {
    fn provider_session_id(&self) -> Option<&ProviderSessionId> {
        self.session_id.as_ref()
    }

    async fn send(&mut self, input: TurnInput) -> Result<()> {
        self.refuse_if_ended()?;

        let active = self.assembler.active_turn_id().cloned();
        // A turn/start still awaiting its reply is a turn: `turn/started`
        // just has not been read yet. Starting a second one would race it, so
        // an interrupt treats it as the active turn.
        let starting = self.turn_start_outstanding();
        match (input.delivery, active) {
            (Delivery::Interrupt, active) if active.is_some() || starting => {
                // Cancel first, deliver after the turn actually closes: the
                // interrupted turn ends on idle while deltas keep arriving
                // (transcript 11), and a turn/start racing that idle would
                // hand the assembler a new turn while the old one is open.
                self.deferred_input = Some(input.text);
                self.request_interrupt().await
            }
            (Delivery::TurnBoundary, Some(expected)) => {
                // Steering: the message amends the ACTIVE turn (transcript
                // 15). `expectedTurnId` is the precondition that stops it
                // racing a turn that ended while the request was in flight —
                // the error reply falls back to an ordinary turn/start.
                let thread = self.thread_id.clone().unwrap_or_default();
                let id = self.ids.next();
                self.in_flight.push((
                    id,
                    OutstandingRequest::Steer {
                        text: input.text.clone(),
                    },
                ));
                self.write(&encode_steer(id, &thread, expected.as_str(), &input.text))
                    .await
            }
            // No turn in flight: both deliveries are an ordinary turn start.
            (Delivery::Interrupt | Delivery::TurnBoundary, _) => self.start_turn(&input.text).await,
        }
    }

    async fn next_event(&mut self) -> Result<Option<AgentEvent>> {
        while self.pending.is_empty() {
            if self.ended {
                return Ok(None);
            }
            match self.read_frame().await? {
                Some(frame) => self.route_frame(frame).await?,
                // EOF: the harness is gone. Reported, never waited on.
                None => return Ok(None),
            }
        }
        Ok(self.pending.pop_front())
    }

    async fn answer_permission(
        &mut self,
        tool_call_id: &ToolCallId,
        decision: PermissionDecision,
    ) -> Result<()> {
        // Peek the vocabulary before resolving: a decision this session cannot
        // encode must leave the request outstanding (still answerable), never
        // consume it.
        let Some(kind) = self.assembler.approval_kind(tool_call_id).cloned() else {
            return Err(AdapterError::UnknownPermissionRequest {
                tool_call_id: tool_call_id.clone(),
            });
        };

        // A narrowing neither reply vocabulary can carry is declined — never
        // downgraded to running the un-narrowed original, and never left
        // unanswered either: an error here reaches nobody who would re-answer,
        // and an unanswered request parks the harness forever (transcript 19).
        // The decline is journaled with why, below.
        let (exec_decision, inexpressible) = match &kind {
            // Accept-or-decline: there is nowhere to carry replacement
            // arguments, so any narrowing is inexpressible.
            ApprovalKind::McpElicitation => (None, decision.updated_input().is_some()),
            ApprovalKind::ExecCommand { proposed_amendment } => {
                let proposal = self
                    .assembler
                    .approval_input(tool_call_id)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                match exec_decision_for(&decision, &proposal, proposed_amendment.as_deref()) {
                    Ok(exec_decision) => (Some(exec_decision), false),
                    Err(InexpressibleNarrowing) => (Some(ExecDecision::Decline), true),
                }
            }
        };
        let allowed = decision.is_allow() && !inexpressible;
        let Some(resolved) = self.assembler.resolve_approval(tool_call_id, allowed) else {
            return Err(AdapterError::UnknownPermissionRequest {
                tool_call_id: tool_call_id.clone(),
            });
        };
        let (reply, encoding) = match exec_decision {
            None => (
                encode_elicitation_response(resolved.rpc_id, allowed),
                if allowed { "accept" } else { "decline" }.to_owned(),
            ),
            Some(exec_decision) => (
                encode_exec_approval_response(resolved.rpc_id, &exec_decision),
                exec_decision.wire_name().to_owned(),
            ),
        };

        // Every decision is journaled with the codex encoding it became, so
        // the audit trail shows both what the chain said and what the harness
        // was actually told.
        let chain_said = if decision.is_allow() { "allow" } else { "deny" };
        let remembering = if decision.remembers_for_session() {
            " (remember for session)"
        } else {
            ""
        };
        let narrowing = if inexpressible {
            " with a narrowing codex cannot express, so declined rather than run un-narrowed"
        } else {
            ""
        };
        if let Some(event) = self.assembler.note_decision(format!(
            "codex approval {tool_call_id}: chain {chain_said}{remembering}{narrowing} → {encoding}"
        )) {
            self.pending.push_back(event);
        }
        self.write(&reply).await
    }

    async fn interrupt(&mut self) -> Result<()> {
        self.refuse_if_ended()?;
        self.request_interrupt().await
    }

    async fn shutdown(&mut self) -> Result<()> {
        // Stop by handle — the harness and everything it started, SIGTERM
        // then SIGKILL. Never by name or pattern.
        let _ = self.harness.stop().await;
        self.ended = true;
        self.pending.clear();
        self.in_flight.clear();
        self.deferred_input = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use horde_adapter::{Adapter, Capability, McpEndpoint};

    use super::{
        CodexAdapter, McpOverrides, is_approval_method, is_auth_failure, mcp_server_overrides,
        toml_string,
    };
    use crate::harness::HarnessProfile;

    /// An adapter for the tests that never launch: its profile is a path
    /// nothing is created under.
    fn unspawned() -> CodexAdapter {
        CodexAdapter::new(HarnessProfile::new("/nonexistent/horde-test-profile"))
    }

    /// The login recipe rides the adapter's own binary with codex's
    /// documented 1455 callback — the port the relay's ephemeral expose
    /// binds exactly. `None` here would silently unplug
    /// `horde worker login <w> codex`.
    #[test]
    fn the_login_recipe_runs_this_adapters_own_binary_with_its_callback() {
        let recipe = unspawned()
            .with_program("/stub/codex")
            .login_recipe()
            .expect("codex is loginable");
        assert_eq!(recipe.program, "/stub/codex");
        assert_eq!(recipe.args, vec!["login".to_owned()]);
        assert_eq!(recipe.callback_port, Some(1455));
        assert_eq!(
            recipe.listen_port, None,
            "stock CLIs listen where they redirect"
        );
    }

    #[test]
    fn the_adapter_reports_the_provider_key_the_wire_contract_uses() {
        assert_eq!(unspawned().provider(), "codex");
    }

    /// The adapter must pass the codec's measured capabilities through
    /// unchanged — a supervisor that reported its own guess would let callers
    /// rely on something no spike ever demonstrated.
    #[test]
    fn capabilities_come_from_the_codec_not_from_the_supervisor() {
        let claimed = unspawned().capabilities();

        assert_eq!(claimed, horde_adapter_codex::capabilities());
        assert!(claimed.supports(Capability::Resumable));
        assert!(claimed.supports(Capability::PreAuthorizeMcpToolCalls));
        assert!(!claimed.supports(Capability::PreAuthorizeToolCalls));
    }

    /// The exec-interception claim is earned by the probe, never assumed: an
    /// unprobed adapter claims the floor, a probed 0.147+ one claims the exec
    /// gate, and neither ever claims blanket gating (file changes park,
    /// transcripts 28/31).
    #[test]
    fn the_exec_gating_claim_follows_the_probe_verdict() {
        let unprobed = unspawned();
        assert!(
            !unprobed
                .capabilities()
                .supports(Capability::PreAuthorizeExecCommands),
            "no probe, no claim"
        );

        let mut routed = unspawned();
        routed.exec_interception = super::ExecInterception::Routed {
            version: "codex-cli 0.148.0".to_owned(),
        };
        assert!(
            routed
                .capabilities()
                .supports(Capability::PreAuthorizeExecCommands),
            "a probed 0.147+ binary earns the claim"
        );
        assert!(
            !routed
                .capabilities()
                .supports(Capability::PreAuthorizeToolCalls),
            "and never the blanket claim — file changes stay ungated"
        );

        let mut refused = unspawned();
        refused.exec_interception = super::ExecInterception::SandboxOnly {
            reason: "codex-cli 0.145.0".to_owned(),
        };
        assert_eq!(refused.capabilities(), horde_adapter_codex::capabilities());
    }

    /// The probe fails closed: a binary that cannot run yields the floor with
    /// a reason the journal can quote, never a guessed claim.
    #[tokio::test]
    async fn a_probe_that_cannot_run_fails_closed_with_its_reason() {
        let probed = super::probe_version(
            "/nonexistent/horde-test-codex-binary",
            std::time::Duration::from_secs(2),
        )
        .await;
        match probed {
            super::ExecInterception::SandboxOnly { reason } => {
                assert!(
                    reason.contains("could not run"),
                    "the journal must say why the claim is absent: {reason}"
                );
            }
            other => panic!("a missing binary must claim nothing, got {other:?}"),
        }
    }

    /// The probe builder records its verdict on the adapter it was called on
    /// — not on a fresh default, which would silently discard the program,
    /// the policy, and every other field the caller configured.
    #[tokio::test]
    async fn the_probe_builder_keeps_the_adapter_it_probed() {
        let mut adapter = unspawned().with_program("/nonexistent/horde-test-codex");
        adapter.response_ceiling = std::time::Duration::from_secs(2);
        adapter.extra_env = vec![("MARKER".to_owned(), "kept".to_owned())];

        let probed = adapter.probe_exec_interception().await;

        assert_eq!(
            probed.program, "/nonexistent/horde-test-codex",
            "the probed binary is the configured one, not a default"
        );
        assert_eq!(
            probed.extra_env,
            vec![("MARKER".to_owned(), "kept".to_owned())],
            "unrelated configuration survives the probe"
        );
        match &probed.exec_interception {
            super::ExecInterception::SandboxOnly { reason } => assert!(
                reason.contains("/nonexistent/horde-test-codex"),
                "the verdict names the binary that was actually probed: {reason}"
            ),
            other => panic!("a missing binary must claim nothing, got {other:?}"),
        }
    }

    /// The unadjudicable refusal speaks each method's own reply vocabulary —
    /// swapped, either reply deserializes to nothing server-side and the
    /// harness parks (transcript 19). And only the two chain-seam methods are
    /// adjudicable at all.
    #[test]
    fn unadjudicable_refusals_speak_the_asking_methods_vocabulary() {
        use horde_adapter_codex::wire::{EXEC_APPROVAL_METHOD, MCP_ELICITATION_METHOD};

        let elicitation: serde_json::Value =
            serde_json::from_str(&super::unadjudicable_refusal(3, MCP_ELICITATION_METHOD))
                .expect("we produced this");
        assert_eq!(elicitation["result"]["action"], "decline");
        assert!(elicitation["result"].get("decision").is_none());

        let exec: serde_json::Value =
            serde_json::from_str(&super::unadjudicable_refusal(4, EXEC_APPROVAL_METHOD))
                .expect("we produced this");
        assert_eq!(exec["result"]["decision"], "decline");
        assert!(exec["result"].get("action").is_none());

        assert!(super::is_adjudicable_method(MCP_ELICITATION_METHOD));
        assert!(super::is_adjudicable_method(EXEC_APPROVAL_METHOD));
        assert!(
            !super::is_adjudicable_method("item/fileChange/requestApproval"),
            "file-change approvals have no chain seam (transcripts 28/31)"
        );
        assert!(!super::is_adjudicable_method("attestation/generate"));
    }

    /// The enforcement audit line each verdict journals at spawn.
    #[test]
    fn the_interception_diagnostic_names_the_enforcement_and_the_evidence() {
        let routed = super::ExecInterception::Routed {
            version: "codex-cli 0.148.0".to_owned(),
        };
        assert_eq!(
            routed.diagnostic(),
            "codex exec approvals: routed to the permission chain (codex-cli 0.148.0)"
        );
        let floor = super::ExecInterception::SandboxOnly {
            reason: "codex version not probed".to_owned(),
        };
        assert_eq!(
            floor.diagnostic(),
            "codex exec approvals: sandbox tier only (codex version not probed)"
        );
    }

    /// The endpoint rides `-c` overrides, never a `config.toml` write:
    /// `CODEX_HOME` is shared per account and a write would race every
    /// concurrent spawn.
    #[test]
    fn the_horde_endpoint_becomes_per_process_config_overrides() {
        let http = mcp_server_overrides(Some(&McpEndpoint::Http {
            url: "http://127.0.0.1:8931/mcp".to_owned(),
            headers: Vec::new(),
        }));
        assert_eq!(
            http,
            McpOverrides {
                args: vec![
                    "-c".to_owned(),
                    "mcp_servers.horde.url=\"http://127.0.0.1:8931/mcp\"".to_owned(),
                ],
                env: Vec::new(),
            }
        );

        let stdio = mcp_server_overrides(Some(&McpEndpoint::Stdio {
            command: "horde-mcp".to_owned(),
            args: vec!["--agent".to_owned(), "a1".to_owned()],
        }));
        assert!(
            stdio
                .args
                .contains(&"mcp_servers.horde.command=\"horde-mcp\"".to_owned())
        );
        assert!(
            stdio
                .args
                .contains(&"mcp_servers.horde.args=[\"--agent\",\"a1\"]".to_owned())
        );
        assert!(stdio.env.is_empty());

        assert_eq!(
            mcp_server_overrides(None),
            McpOverrides::default(),
            "no endpoint is no override, not an empty one"
        );
    }

    /// ADR-0010: the per-agent identity pair rides every request, but its
    /// values never touch argv — `env_http_headers` names a variable per
    /// header, and the values travel in the child's environment, where `ps`
    /// cannot show them.
    #[test]
    fn the_bearer_token_rides_the_environment_never_argv() {
        let overrides = mcp_server_overrides(Some(&McpEndpoint::Http {
            url: "http://127.0.0.1:8931/mcp".to_owned(),
            headers: vec![
                ("Authorization".to_owned(), "Bearer tok-1".to_owned()),
                ("X-Horde-Agent".to_owned(), "agent-1".to_owned()),
            ],
        }));
        let joined = overrides.args.join(" ");
        assert!(
            !joined.contains("tok-1") && !joined.contains("agent-1"),
            "no header value on the command line: {joined}"
        );
        assert!(
            overrides.args.contains(
                &"mcp_servers.horde.env_http_headers=\
                  {\"Authorization\"=\"HORDE_MCP_HEADER_TOKEN_0\",\
                  \"X-Horde-Agent\"=\"HORDE_MCP_HEADER_TOKEN_1\"}"
                    .to_owned()
            ),
            "each header is mapped to the variable that carries it: {joined}"
        );
        assert_eq!(
            overrides.env,
            vec![
                (
                    "HORDE_MCP_HEADER_TOKEN_0".to_owned(),
                    "Bearer tok-1".to_owned()
                ),
                ("HORDE_MCP_HEADER_TOKEN_1".to_owned(), "agent-1".to_owned()),
            ]
        );
    }

    /// The launch puts the overrides' environment on the child beside the
    /// configured extras, and always exports a Worker-owned `CODEX_HOME`.
    #[test]
    fn a_launch_runs_under_a_worker_owned_codex_home() {
        let scratch = tempfile::tempdir().expect("scratch");
        let mut adapter = CodexAdapter::new(HarnessProfile::new(scratch.path().join("codex")));
        adapter.extra_env = vec![("MARKER".to_owned(), "kept".to_owned())];
        let launch = adapter
            .harness_spec(&horde_adapter::AgentSpec {
                cwd: Some(std::path::PathBuf::from("/checkout/agent-1")),
                horde_endpoint: Some(McpEndpoint::Http {
                    url: "http://127.0.0.1:1/mcp".to_owned(),
                    headers: vec![("Authorization".to_owned(), "Bearer t".to_owned())],
                }),
                ..horde_adapter::AgentSpec::default()
            })
            .expect("a launch");

        assert_eq!(launch.config_env_var, "CODEX_HOME");
        assert_eq!(
            launch.config_dir,
            Some(scratch.path().join("codex").join("default"))
        );
        assert_eq!(&launch.args[..2], ["app-server", "--stdio"]);
        assert_eq!(
            launch.cwd,
            Some(std::path::PathBuf::from("/checkout/agent-1")),
            "the harness process runs in the agent's checkout"
        );
        assert_eq!(
            launch.env,
            vec![
                ("MARKER".to_owned(), "kept".to_owned()),
                ("HORDE_MCP_HEADER_TOKEN_0".to_owned(), "Bearer t".to_owned()),
            ]
        );

        let recipe = adapter.login_recipe().expect("codex is loginable");
        assert_eq!(
            recipe.env,
            vec![(
                "CODEX_HOME".to_owned(),
                scratch
                    .path()
                    .join("codex")
                    .join("default")
                    .display()
                    .to_string()
            )],
            "the login signs in the home the agents run under"
        );
        assert_eq!(
            recipe.accounts,
            Some(("CODEX_HOME".to_owned(), scratch.path().join("codex"))),
            "an account's login signs in under the same profile"
        );
    }

    /// The value side of `-c` parses as TOML; a quote or backslash smuggled
    /// through unescaped truncates the override silently.
    #[test]
    fn toml_strings_escape_quotes_and_backslashes() {
        assert_eq!(toml_string("plain"), "\"plain\"");
        assert_eq!(toml_string("with \"quote\""), "\"with \\\"quote\\\"\"");
        assert_eq!(toml_string("back\\slash"), "\"back\\\\slash\"");
    }

    /// Misclassifying auth strands work: the scheduler's answer to
    /// Unauthenticated is another account, to Spawn another day. Each
    /// disjunct gets a message only *it* matches, so no `||` can quietly
    /// become an `&&`.
    #[test]
    fn auth_failures_are_recognized_and_ordinary_failures_are_not() {
        assert!(is_auth_failure("unauthorized"), "the code alone");
        assert!(
            is_auth_failure("HTTP 401 from provider"),
            "the status alone"
        );
        assert!(is_auth_failure("You are not logged in"));
        assert!(is_auth_failure("please run codex login"));
        assert!(is_auth_failure("401 Unauthorized"), "and both together");
        assert!(!is_auth_failure("thread not found"));
        assert!(!is_auth_failure("internal server error"));
    }

    /// Request ids must be distinct and increasing, and never zero — a
    /// repeated id is a reply mis-correlated forever.
    #[test]
    fn request_ids_are_distinct_increasing_and_never_zero() {
        let mut ids = super::RequestIds::default();
        let minted = [ids.next(), ids.next(), ids.next()];
        assert_eq!(minted, [1, 2, 3]);
    }

    /// The refusal an unanswerable server request gets, in the right
    /// vocabulary for each family — the wrong shape deserializes to nothing
    /// and answers nobody (the waitingOnApproval hang, transcript 19).
    #[test]
    fn refusals_speak_each_familys_own_vocabulary() {
        let approval: serde_json::Value =
            serde_json::from_str(&super::refusal_for(4, "execCommandApproval"))
                .expect("we produced this");
        assert_eq!(approval["id"], 4);
        assert_eq!(approval["result"]["decision"], "decline");

        let unknown: serde_json::Value =
            serde_json::from_str(&super::refusal_for(9, "attestation/generate"))
                .expect("we produced this");
        assert_eq!(unknown["id"], 9);
        assert_eq!(unknown["error"]["code"], -32601);
        assert!(
            unknown["error"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("attestation/generate")),
            "the refusal names the method so the log is actionable"
        );
    }

    /// The whole decision-mapping table, row by row — what the chain said
    /// versus what codex is told, per the re-probe's measured vocabulary.
    #[test]
    fn the_exec_decision_mapping_truth_table() {
        use horde_adapter::PermissionDecision;
        use horde_adapter_codex::ExecDecision;
        use serde_json::json;

        let proposal = json!({"command": "git push", "cwd": "/w"});
        let amendment = vec![
            "/bin/zsh".to_owned(),
            "-lc".to_owned(),
            "git push".to_owned(),
        ];
        let map = |decision: &PermissionDecision, amendment: Option<&[String]>| {
            super::exec_decision_for(decision, &proposal, amendment)
        };

        // Deny → decline, whatever else the request offered.
        assert_eq!(
            map(&PermissionDecision::deny("no"), Some(&amendment)),
            Ok(ExecDecision::Decline)
        );

        // Plain allow → accept.
        assert_eq!(
            map(&PermissionDecision::allow(), Some(&amendment)),
            Ok(ExecDecision::Accept)
        );

        // Allow + remember → acceptForSession: the session-scoped grant, and
        // never the amendment, which would outlive the session it was asked
        // for.
        assert_eq!(
            map(&PermissionDecision::allow().remembered(), Some(&amendment)),
            Ok(ExecDecision::AcceptForSession)
        );

        // A narrowing that restates the proposal verbatim, with codex's
        // amendment on offer → acceptWithExecpolicyAmendment carrying codex's
        // own argv: "exactly this command" made persistent (transcript 30).
        assert_eq!(
            map(
                &PermissionDecision::allow_with(proposal.clone()),
                Some(&amendment)
            ),
            Ok(ExecDecision::AcceptWithExecpolicyAmendment {
                execpolicy_amendment: amendment.clone(),
            })
        );

        // The same restatement with nothing to amend → plain accept (the
        // journaled "narrowing was a restatement" case), remember → session.
        assert_eq!(
            map(&PermissionDecision::allow_with(proposal.clone()), None),
            Ok(ExecDecision::Accept)
        );
        assert_eq!(
            map(
                &PermissionDecision::allow_with(proposal.clone()).remembered(),
                None
            ),
            Ok(ExecDecision::AcceptForSession)
        );

        // A real rewrite is inexpressible — codex runs its proposed command
        // or nothing — and must be refused, never downgraded to running the
        // un-narrowed original.
        assert_eq!(
            map(
                &PermissionDecision::allow_with(json!({"command": "git status", "cwd": "/w"})),
                Some(&amendment)
            ),
            Err(super::InexpressibleNarrowing)
        );
    }

    /// The refusal vocabulary split: approval-shaped requests speak
    /// `decision`, everything else gets a JSON-RPC error. Both are refusals;
    /// the wrong shape deserializes to nothing and answers nobody.
    #[test]
    fn approval_methods_are_the_schemas_own_list() {
        for approval in [
            "execCommandApproval",
            "applyPatchApproval",
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "item/permissions/requestApproval",
        ] {
            assert!(is_approval_method(approval), "{approval}");
        }
        assert!(!is_approval_method("mcpServer/elicitation/request"));
        assert!(!is_approval_method("attestation/generate"));
        assert!(!is_approval_method("account/chatgptAuthTokens/refresh"));
    }
}
