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
//!   elicitations become permission events; everything else is refused —
//!   deny-by-default, because Horde has no chain that can adjudicate it
//!   (ADR-0005).
//! * **`send` never awaits a turn response.** A turn request's failure comes
//!   back interleaved with the stream, and blocking the pump on it would
//!   deadlock against an elicitation only the pump can answer. Failures
//!   surface through `next_event`, where the pump is actually listening.
//! * **The sandbox tier is chosen here, from the agent's permission mode**,
//!   and the choice plus its reason ride the first turn into the journal as a
//!   harness diagnostic — the tier *is* the enforcement, so it must be
//!   auditable.

use std::collections::VecDeque;
use std::path::PathBuf;

use async_trait::async_trait;
use horde_adapter::{
    Adapter, AdapterError, AgentEvent, AgentSession, AgentSpec, Capabilities, Capability, Delivery,
    McpEndpoint, PermissionDecision, ProviderSessionId, Result, ToolCallId, TurnInput,
};
use horde_adapter_codex::codec::{
    ThreadConfig, decode_line, encode_decision_decline, encode_elicitation_response,
    encode_initialize, encode_initialized, encode_interrupt, encode_method_not_found, encode_steer,
    encode_thread_resume, encode_thread_start, encode_turn_start,
};
use horde_adapter_codex::sandbox::SandboxPolicy;
use horde_adapter_codex::turn::TurnAssembler;
use horde_adapter_codex::wire::{Frame, MCP_ELICITATION_METHOD, RpcError};

use crate::process::{HarnessSpec, SupervisedHarness};

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
    /// Root under which each account's `CODEX_HOME` lives.
    pub accounts_root: Option<PathBuf>,
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
}

impl CodexAdapter {
    /// An adapter that runs the real `codex` on `PATH`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            program: "codex".to_owned(),
            accounts_root: None,
            sandbox_policy: SandboxPolicy::default(),
            extra_env: Vec::new(),
            response_ceiling: std::time::Duration::from_secs(30),
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

    fn harness_spec(&self, spec: &AgentSpec) -> HarnessSpec {
        let mut args = vec!["app-server".to_owned(), "--stdio".to_owned()];
        args.extend(mcp_server_overrides(spec.horde_endpoint.as_ref()));

        HarnessSpec {
            program: self.program.clone(),
            args,
            env: self.extra_env.clone(),
            config_dir: horde_adapter_codex::identity::codex_home_for(
                spec.account.as_ref(),
                self.accounts_root
                    .as_deref()
                    .unwrap_or_else(|| std::path::Path::new(".")),
            ),
            config_env_var: "CODEX_HOME",
            cwd: spec.cwd.clone(),
        }
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
        let harness = SupervisedHarness::spawn(&self.harness_spec(spec)).map_err(|error| {
            AdapterError::Spawn {
                provider: horde_adapter_codex::PROVIDER.to_owned(),
                message: error.to_string(),
            }
        })?;

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

impl Default for CodexAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Adapter for CodexAdapter {
    fn provider(&self) -> &str {
        horde_adapter_codex::PROVIDER
    }

    fn capabilities(&self) -> Capabilities {
        horde_adapter_codex::capabilities()
    }

    fn login_recipe(&self) -> Option<horde_adapter::LoginRecipe> {
        // `codex login` runs a local OAuth-callback server on loopback port
        // 1455, fixed by the CLI. PROVISIONAL until verified against the
        // live CLI (see the W9a handoff flags).
        Some(horde_adapter::LoginRecipe::with_callback(
            self.program.clone(),
            vec!["login".to_owned()],
            1455,
        ))
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

/// The `-c mcp_servers.…` overrides that give one agent its Horde endpoint.
///
/// Per-process flags rather than `config.toml` writes, because `CODEX_HOME` is
/// shared by every agent on the same Provider Account and a config write from
/// one spawn would race every concurrent one. Measured working in the
/// transcript 17–19 captures.
fn mcp_server_overrides(endpoint: Option<&McpEndpoint>) -> Vec<String> {
    let mut overrides = Vec::new();
    let mut push = |assignment: String| {
        overrides.push("-c".to_owned());
        overrides.push(assignment);
    };
    match endpoint {
        None => {}
        Some(McpEndpoint::Http { url, headers }) => {
            push(format!("mcp_servers.horde.url={}", toml_string(url)));
            if !headers.is_empty() {
                // An inline TOML table: the identity pair must ride *every*
                // request (the endpoint's gate refuses unidentified ones), and
                // `http_headers` is the config.toml mechanism the spike
                // confirmed Codex sends on all of them.
                let rendered: Vec<String> = headers
                    .iter()
                    .map(|(name, value)| format!("{}={}", toml_string(name), toml_string(value)))
                    .collect();
                push(format!(
                    "mcp_servers.horde.http_headers={{{}}}",
                    rendered.join(",")
                ));
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
    async fn request_interrupt(&mut self) -> Result<()> {
        self.assembler.note_interrupt_requested();
        let thread = self.thread_id.clone().unwrap_or_default();
        let id = self.ids.next();
        self.in_flight.push((id, OutstandingRequest::Interrupt));
        self.write(&encode_interrupt(id, &thread)).await
    }

    /// Starts a fresh turn with `text`.
    async fn start_turn(&mut self, text: &str) -> Result<()> {
        let thread = self.thread_id.clone().unwrap_or_default();
        let id = self.ids.next();
        self.in_flight.push((id, OutstandingRequest::TurnStart));
        self.write(&encode_turn_start(id, &thread, text)).await
    }

    /// Handles one frame that is not the reply the caller was waiting on.
    ///
    /// This is the wire discipline in one place: notifications feed the
    /// assembler; elicitations become permission events; every other server
    /// request is refused; replies to our own requests settle their follow-up.
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
            Frame::ServerRequest { id, method, params } => {
                if method == MCP_ELICITATION_METHOD {
                    let events = self.assembler.feed(&Frame::ServerRequest {
                        id,
                        method: method.clone(),
                        params,
                    });
                    if events.is_empty() {
                        // An elicitation that is not a tool-call approval —
                        // nothing in Horde can answer it, and leaving it
                        // unanswered parks the harness (transcript 19).
                        tracing::warn!(method, id, "declining an unrecognized elicitation");
                        self.write(&encode_elicitation_response(id, false)).await?;
                    } else {
                        self.pending.extend(events);
                    }
                    return Ok(());
                }
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

        match (input.delivery, self.assembler.active_turn_id().cloned()) {
            (Delivery::Interrupt, Some(_)) => {
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
            (Delivery::Interrupt | Delivery::TurnBoundary, None) => {
                self.start_turn(&input.text).await
            }
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
        if decision.updated_input().is_some() {
            // The elicitation reply is accept-or-decline; there is nowhere to
            // carry replacement arguments. Refused so a narrowing that could
            // not apply is never mistaken for one that did.
            return Err(AdapterError::Unsupported {
                provider: horde_adapter_codex::PROVIDER.to_owned(),
                capability: Capability::RewritesToolInput,
            });
        }
        let allowed = decision.is_allow();
        match self.assembler.resolve_elicitation(tool_call_id, allowed) {
            Some(rpc_id) => {
                self.write(&encode_elicitation_response(rpc_id, allowed))
                    .await
            }
            None => Err(AdapterError::UnknownPermissionRequest {
                tool_call_id: tool_call_id.clone(),
            }),
        }
    }

    async fn interrupt(&mut self) -> Result<()> {
        self.refuse_if_ended()?;
        self.request_interrupt().await
    }

    async fn shutdown(&mut self) -> Result<()> {
        // Kill by handle. Never by name or pattern.
        let _ = self.harness.kill().await;
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
        CodexAdapter, is_approval_method, is_auth_failure, mcp_server_overrides, toml_string,
    };

    #[test]
    fn the_adapter_reports_the_provider_key_the_wire_contract_uses() {
        assert_eq!(CodexAdapter::new().provider(), "codex");
    }

    /// The adapter must pass the codec's measured capabilities through
    /// unchanged — a supervisor that reported its own guess would let callers
    /// rely on something no spike ever demonstrated.
    #[test]
    fn capabilities_come_from_the_codec_not_from_the_supervisor() {
        let claimed = CodexAdapter::new().capabilities();

        assert_eq!(claimed, horde_adapter_codex::capabilities());
        assert!(claimed.supports(Capability::Resumable));
        assert!(claimed.supports(Capability::PreAuthorizeMcpToolCalls));
        assert!(!claimed.supports(Capability::PreAuthorizeToolCalls));
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
            vec![
                "-c".to_owned(),
                "mcp_servers.horde.url=\"http://127.0.0.1:8931/mcp\"".to_owned(),
            ]
        );

        // The per-agent identity pair rides as an inline `http_headers`
        // table — on every request, which is what the endpoint's gate
        // demands; a headerless entry would be refused 401 on each call.
        let with_identity = mcp_server_overrides(Some(&McpEndpoint::Http {
            url: "http://127.0.0.1:8931/mcp".to_owned(),
            headers: vec![
                ("Authorization".to_owned(), "Bearer tok-1".to_owned()),
                ("X-Horde-Agent".to_owned(), "agent-1".to_owned()),
            ],
        }));
        let expected_headers = "mcp_servers.horde.http_headers=\
                                {\"Authorization\"=\"Bearer tok-1\",\"X-Horde-Agent\"=\"agent-1\"}";
        assert!(
            with_identity.contains(&expected_headers.to_owned()),
            "the identity pair must ride the config override: {with_identity:?}"
        );

        let stdio = mcp_server_overrides(Some(&McpEndpoint::Stdio {
            command: "horde-mcp".to_owned(),
            args: vec!["--agent".to_owned(), "a1".to_owned()],
        }));
        assert!(stdio.contains(&"mcp_servers.horde.command=\"horde-mcp\"".to_owned()));
        assert!(stdio.contains(&"mcp_servers.horde.args=[\"--agent\",\"a1\"]".to_owned()));

        assert!(
            mcp_server_overrides(None).is_empty(),
            "no endpoint is no override, not an empty one"
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
