//! The MCP tool surface: what an Agent actually sees on the endpoint.
//!
//! One rmcp [`ServerHandler`] exposing the permission prompt tool and the four
//! orchestrator verbs. The modules behind it ([`crate::permission`],
//! [`crate::verbs`]) hold the reasoning; this one is the wire adaptation and
//! deliberately thin.
//!
//! # Identity comes from the request, per call
//!
//! Every handler reads its caller out of the HTTP request parts rmcp injects
//! into the request context, where [`crate::server::RequireIdentity`] put it
//! after validating the header pair. A handler that could not find one refuses:
//! there is no ambient "current agent" here, because one endpoint serves many
//! agents and the legacy era gives no session to hang identity on.
//!
//! # Why the state is cloned per call rather than locked
//!
//! rmcp hands handlers `&self`, and the control-plane client needs `&mut` to
//! issue an RPC. A mutex would serialize every agent's calls behind whichever
//! one is currently blocked — and the permission tool blocks for as long as a
//! human takes to answer, so that mutex would be held for minutes. Cloning the
//! tonic channel is cheap and shares the underlying connection, so each call
//! gets its own handle and a blocked approval never stalls anybody else.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rmcp::handler::server::tool::{ToolCallContext, ToolRouter};
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, JsonObject, ListToolsResult,
    PaginatedRequestParams, ProtocolVersion, RequestMetaObject, ServerCapabilities, ServerInfo,
    Tool,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};

use crate::context::AgentContext;
use crate::hub::ToolProxy;
use crate::identity::AgentIdentity;
use crate::permission::PermissionBroker;
use crate::verbs::{MessageKind, Verbs};

/// The arguments Claude's `--permission-prompt-tool` sends.
///
/// Field-for-field the harness's payload. Named here rather than reused from
/// the adapter because rmcp needs a `JsonSchema` to advertise the tool, and the
/// adapter's type is deliberately free of that dependency.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ApproveParams {
    /// The tool the model wants to run.
    pub tool_name: String,
    /// The arguments it proposed.
    pub input: serde_json::Value,
    /// The harness's own id for this call.
    #[serde(default)]
    pub tool_use_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SpawnParams {
    /// What the child is for. Becomes the child's initial assignment — its
    /// first turn input on whatever worker it lands on. Empty spawns an
    /// idle child to be assigned later.
    #[serde(default)]
    pub task: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SpawnResult {
    pub agent_id: String,
    /// False means it exists but has nowhere to run yet.
    pub placed: bool,
    /// Why it is queued, when it is.
    pub reason: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SendParams {
    pub to_agent_id: String,
    pub text: String,
    /// True sends a note (owes no result) rather than an assignment.
    #[serde(default)]
    pub note: bool,
    /// Cancel the recipient's turn rather than waiting for its boundary.
    #[serde(default)]
    pub interrupt: bool,
    /// Commits the message names. The receiving worker makes each present in
    /// the recipient's checkout before the text is read — name a commit here
    /// rather than telling the recipient how to fetch it.
    #[serde(default)]
    pub git_refs: Vec<SendGitRef>,
}

/// One commit riding a message's payload.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SendGitRef {
    /// The repo's workspace-definition name.
    pub repo: String,
    /// The commit id. Authoritative — the branch is advisory.
    pub commit: String,
    #[serde(default)]
    pub branch: String,
}

/// The wire payload a send's `git_refs` describe, or `None` for none.
#[must_use]
pub fn payload_from_refs(git_refs: &[SendGitRef]) -> Option<horde_proto::v1::Payload> {
    if git_refs.is_empty() {
        return None;
    }
    Some(horde_proto::v1::Payload {
        git_refs: git_refs
            .iter()
            .map(|git_ref| horde_proto::v1::GitRef {
                repo_name: git_ref.repo.clone(),
                commit_sha: git_ref.commit.clone(),
                branch: git_ref.branch.clone(),
            })
            .collect(),
        attachments: Vec::new(),
    })
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReportParams {
    /// `success`, `partial`, or `blocked`.
    pub status: String,
    pub summary: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WaitParams {
    pub agent_id: String,
}

/// `expose <port> as <name>` (ADR-0006).
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExposeParams {
    /// The port your workspace serves on this worker's localhost.
    pub port: u16,
    /// The forward's name, e.g. "vite". Forwards are named, not numbered.
    pub name: String,
}

/// An agent-initiated inject (ADR-0006).
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct InjectParams {
    /// The port on the accepting human's machine you want to reach.
    pub port: u16,
    /// The forward's name, e.g. "db".
    pub name: String,
    /// The localhost port on this worker where the injected service appears.
    /// Defaults to `port`.
    #[serde(default)]
    pub worker_port: Option<u16>,
}

/// What a forward declaration came back as.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct ForwardResult {
    /// False means the chain (or a human) refused; the message says why.
    pub granted: bool,
    /// The journaled intent's id, when granted.
    pub forward_id: String,
    pub message: String,
}

/// The verb name a forward proposal is judged under — the tool name modes
/// write rules against.
#[must_use]
pub fn forward_verb_name(spec: &horde_proto::v1::ForwardSpec) -> &'static str {
    if spec.direction == horde_proto::v1::ForwardDirection::Inject as i32 {
        "inject"
    } else {
        "expose"
    }
}

/// What a granted forward means, in words the agent can act on.
#[must_use]
pub fn forward_granted_message(verb: &str, name: &str, worker_port: u32) -> String {
    if verb == "inject" {
        format!("granted: {name} will appear at 127.0.0.1:{worker_port} on this worker")
    } else {
        format!(
            "granted: attached clients will realize {name} from this worker's port {worker_port}"
        )
    }
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct ResultView {
    pub status: String,
    pub summary: String,
}

/// Renders any verb failure as an MCP tool error.
///
/// One conversion rather than a closure at each call site: the handlers should
/// differ in what they do, not in how they spell the same `map_err`.
fn internal<E: std::fmt::Display>(error: E) -> ErrorData {
    ErrorData::internal_error(error.to_string(), None)
}

/// The `ResultStatus` an agent named, or `None` if it named nothing valid.
///
/// A free function rather than a `match` inside the handler, for the reason
/// QUALITY_GATES records: a decision buried in a handler cannot have its truth
/// table exercised, because arranging the handler means arranging a whole MCP
/// request. Lifted out, it is four assertions and no runtime.
#[must_use]
pub fn status_from_name(name: &str) -> Option<horde_proto::v1::ResultStatus> {
    match name {
        "success" => Some(horde_proto::v1::ResultStatus::Success),
        "partial" => Some(horde_proto::v1::ResultStatus::Partial),
        "blocked" => Some(horde_proto::v1::ResultStatus::Blocked),
        _ => None,
    }
}

/// The name for a status coming back out of the log.
///
/// An unrecognized or unspecified status reports as `unknown` rather than
/// being rounded to `success`: a parent acting on an invented outcome is worse
/// than one told the outcome is unreadable. Delegates to `horde-wire`, the
/// vocabulary's one owner, so the endpoint and the CLI's renders cannot
/// drift apart.
#[must_use]
pub fn status_name(status: i32) -> &'static str {
    horde_wire::result_status_name(status)
}

/// Which message kind a `note` flag asks for.
///
/// One bit, one function, so the inversion is catchable — the same shape as
/// `horde_cli::cli::delivery_for`, and for the same reason.
#[must_use]
pub fn message_kind_for(note: bool) -> MessageKind {
    if note {
        MessageKind::Note
    } else {
        MessageKind::Assign
    }
}

/// The tools one Agent's endpoint serves.
#[derive(Clone)]
pub struct HordeEndpoint {
    context: AgentContext,
    tool_router: ToolRouter<Self>,
    /// The proxied half of the toolset (DESIGN §5 items 2 and 3): the hub
    /// link and this Agent's `local` servers. Empty by default — an endpoint
    /// with no hub configured serves exactly the verbs.
    proxy: Arc<ToolProxy>,
    /// Ordinal for minted permission-request ids, when a caller's `_meta`
    /// carries no correlation id of its own.
    minted: Arc<AtomicU64>,
}

impl std::fmt::Debug for HordeEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HordeEndpoint")
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

#[tool_router]
impl HordeEndpoint {
    #[must_use]
    pub fn new(context: AgentContext) -> Self {
        Self {
            context,
            tool_router: Self::tool_router(),
            proxy: Arc::new(ToolProxy::new()),
            minted: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Attaches the proxied toolset: the hub link and the Agent's `local`
    /// servers, already connected.
    #[must_use]
    pub fn with_proxy(mut self, proxy: Arc<ToolProxy>) -> Self {
        self.proxy = proxy;
        self
    }

    /// The tool names this endpoint serves — the verbs and every proxied
    /// tool under its namespaced name.
    ///
    /// Reads the router the `#[tool_handler]` macro dispatches through, so a
    /// test can assert on *this instance's* surface rather than on what the
    /// static constructor would have built.
    #[must_use]
    pub fn tool_names(&self) -> Vec<String> {
        self.tool_router
            .list_all()
            .into_iter()
            .chain(self.proxy.tools())
            .map(|tool| tool.name.to_string())
            .collect()
    }

    /// The authenticated caller, or a refusal.
    fn caller(context: &RequestContext<RoleServer>) -> Result<AgentIdentity, ErrorData> {
        context
            .extensions
            .get::<http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<AgentIdentity>())
            .cloned()
            .ok_or_else(|| {
                // Reaching a handler without an identity means the gate in front
                // of rmcp was bypassed or removed. Refusing loudly is the only
                // safe answer: the alternative is serving one agent's verbs to
                // an unidentified caller.
                ErrorData::invalid_request(
                    "this request carried no authenticated agent identity",
                    None,
                )
            })
    }

    fn broker(&self) -> PermissionBroker {
        PermissionBroker::new(self.context.clone())
    }

    fn verbs(&self) -> Verbs {
        Verbs::new(self.context.clone())
    }

    /// The tool Claude's `--permission-prompt-tool` points at.
    ///
    /// Returns the decision as a JSON *string*, which is the shape the harness
    /// reads: it wants `{behavior, updatedInput}` stringified inside a text
    /// content block, not as structured content. rmcp wraps a returned `String`
    /// in exactly that block.
    #[tool(
        name = "approve",
        description = "Decide whether a proposed tool call may run. Horde routes this through its permission chain, which may allow, deny, or narrow the call's arguments."
    )]
    async fn approve(
        &self,
        Parameters(params): Parameters<ApproveParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<String, ErrorData> {
        let identity = Self::caller(&context)?;
        let prompt = horde_adapter_claude::permission::PromptRequest {
            tool_name: params.tool_name,
            input: params.input,
            tool_use_id: params.tool_use_id,
        };

        let decision = self
            .broker()
            .adjudicate(&identity, &prompt)
            .await
            .map_err(internal)?;

        Ok(horde_adapter_claude::permission::encode_decision(
            &decision,
            &prompt.input,
        ))
    }

    #[tool(
        name = "spawn_agent",
        description = "Spawn a child agent under yourself. Never blocks: an agent with nowhere to run comes back queued with the reason."
    )]
    async fn spawn_agent(
        &self,
        Parameters(params): Parameters<SpawnParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SpawnResult>, ErrorData> {
        let identity = Self::caller(&context)?;
        // Minted unique, never derived from the task: two children spawned
        // for the same task text are two children, and a key built from the
        // text would silently collapse them into one (the audit's collision
        // #1). The task itself rides the spec as the initial assignment.
        let key = self.request_id_for(&identity, None);
        let outcome = self
            .verbs()
            .spawn_agent(&identity, &params.task, &key)
            .await
            .map_err(internal)?;

        Ok(Json(SpawnResult {
            agent_id: outcome.agent_id,
            placed: outcome.placed,
            reason: outcome.reason,
        }))
    }

    #[tool(
        name = "send_message",
        description = "Send an assignment (expects exactly one result) or a note (owes no result) to another agent."
    )]
    async fn send_message(
        &self,
        Parameters(params): Parameters<SendParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<String, ErrorData> {
        let identity = Self::caller(&context)?;
        // Keyed on the sender and the text so a retried send is not delivered
        // twice; see `Verbs::report_result` on why the key must be unique.
        let key = format!("{}:{}", identity.agent_id(), params.text);

        let sequence = self
            .verbs()
            .send_message(
                &params.to_agent_id,
                message_kind_for(params.note),
                &params.text,
                payload_from_refs(&params.git_refs),
                params.interrupt,
                &key,
            )
            .await
            .map_err(internal)?;

        Ok(format!("delivered at {sequence}"))
    }

    #[tool(
        name = "report_result",
        description = "Report your result. You go idle with context retained rather than ending, and can be revived by another message. Your commits are detected automatically — do not list them."
    )]
    async fn report_result(
        &self,
        Parameters(params): Parameters<ReportParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<String, ErrorData> {
        let identity = Self::caller(&context)?;
        self.record(&identity, &params).await
    }

    /// The body of `report_result`, without the request wrapping.
    ///
    /// Split out so it can be tested: a handler taking a `RequestContext` can
    /// only be exercised by arranging a whole MCP request, which is why the
    /// status validation and the failure path here would otherwise go
    /// unmeasured. The handler above is now the two lines that cannot be tested
    /// any other way.
    ///
    /// # Errors
    ///
    /// Invalid params for a status that is not one of the three; an internal
    /// error when the journal would not take the result.
    async fn record(
        &self,
        identity: &AgentIdentity,
        params: &ReportParams,
    ) -> Result<String, ErrorData> {
        let status = status_from_name(&params.status).ok_or_else(|| {
            ErrorData::invalid_params(
                format!(
                    "status must be success, partial, or blocked; got {}",
                    params.status
                ),
                None,
            )
        })?;

        let key = format!("result:{}:{}", identity.agent_id(), params.summary);
        self.verbs()
            .report_result(identity, status, &params.summary, &key)
            .await
            .map_err(internal)?;

        Ok("recorded".to_owned())
    }

    #[tool(
        name = "expose",
        description = "Expose a port your workspace serves (e.g. a dev server) so it appears on each attached human's localhost. Flows the permission chain like any proposed action; the humans' clients realize it."
    )]
    async fn expose(
        &self,
        Parameters(params): Parameters<ExposeParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ForwardResult>, ErrorData> {
        let identity = Self::caller(&context)?;
        let spec = crate::forwards::expose_spec(params.port, &params.name);
        self.declare_forward(&identity, spec).await
    }

    #[tool(
        name = "inject",
        description = "Ask for a port on an attached human's machine (e.g. a database only they can reach) to appear on this worker's localhost. Surfaces in every attached client's inbox and blocks until a human accepts; the first to accept serves the traffic."
    )]
    async fn inject(
        &self,
        Parameters(params): Parameters<InjectParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ForwardResult>, ErrorData> {
        let identity = Self::caller(&context)?;
        let worker_port = params.worker_port.unwrap_or(params.port);
        if !crate::forwards::forwardable_port(worker_port) {
            return Err(ErrorData::invalid_params(
                "worker_port must be nonzero: the intent is journaled, and the log cannot \
                 hold an ephemeral port",
                None,
            ));
        }
        let spec = crate::forwards::inject_spec(params.port, worker_port, &params.name);
        self.declare_forward(&identity, spec).await
    }

    /// The body both forward verbs share: validate, raise through the chain
    /// with the spec riding the request, and report what the decision means.
    ///
    /// On ALLOW the server's chain driver journals the `ForwardOpened` (the
    /// decision *is* the open — this endpoint never writes one), under the id
    /// derived from this raise's request id.
    async fn declare_forward(
        &self,
        identity: &AgentIdentity,
        spec: horde_proto::v1::ForwardSpec,
    ) -> Result<Json<ForwardResult>, ErrorData> {
        if !crate::forwards::forwardable_port(spec.worker_port as u16) || spec.name.is_empty() {
            return Err(ErrorData::invalid_params(
                "a forward needs a nonzero port and a name: forwards are named, not numbered",
                None,
            ));
        }

        let prompt = horde_adapter_claude::permission::PromptRequest {
            tool_name: forward_verb_name(&spec).to_owned(),
            input: crate::forwards::forward_proposal(&spec),
            // Minted, not harness-carried: verbs dispatched through the tool
            // router cannot see `_meta`, so a retried declare re-raises
            // rather than deduping — a safe failure (the chain re-asks), not
            // a silent one.
            tool_use_id: self.request_id_for(identity, None),
        };
        let name = spec.name.clone();
        let worker_port = spec.worker_port;
        let decision = self
            .broker()
            .adjudicate_forward(identity, &prompt, spec)
            .await
            .map_err(internal)?;

        Ok(Json(match decision {
            horde_adapter::PermissionDecision::Deny { message } => ForwardResult {
                granted: false,
                forward_id: String::new(),
                message: format!("denied: {message}"),
            },
            horde_adapter::PermissionDecision::Allow { .. } => ForwardResult {
                granted: true,
                forward_id: horde_proto::forward::derived_forward_id(&prompt.tool_use_id),
                message: forward_granted_message(&prompt.tool_name, &name, worker_port),
            },
        }))
    }

    #[tool(
        name = "wait_for_agent",
        description = "Block until an agent reports a result. There is no timeout: a child may legitimately take a long time."
    )]
    async fn wait_for_agent(
        &self,
        Parameters(params): Parameters<WaitParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<Json<ResultView>, ErrorData> {
        let result = self
            .verbs()
            .wait_for_agent(&params.agent_id)
            .await
            .map_err(internal)?;

        Ok(Json(ResultView {
            status: status_name(result.status).to_owned(),
            summary: result.summary,
        }))
    }

    /// One proxied tool call, with the permission chain in its path.
    ///
    /// This is where DESIGN §10 puts the chain for hub and `local` tools:
    /// every proxied call is raised as an ordinary `PermissionRequested` and
    /// blocks until the chain answers — deterministically in microseconds
    /// when a rule covers it, or from a human's phone when nothing does. The
    /// forwarded call is the *decided* one: a narrowing that arrives with the
    /// allow replaces the arguments, and a narrowing that cannot be applied
    /// refuses rather than running the original, wider call.
    ///
    /// When a harness pre-authorizes through `approve` (Claude's
    /// `--permission-prompt-tool`), its `_meta.claudecode/toolUseId` rides
    /// the `tools/call` too — and using it as the request id here makes the
    /// second raise a keyed replay of the first: the journal deduplicates it
    /// and the wait finds the decision already written, so one human answer
    /// covers both gates.
    async fn call_proxied(
        &self,
        identity: &AgentIdentity,
        name: &str,
        arguments: Option<JsonObject>,
        tool_use_id: String,
    ) -> Result<CallToolResult, ErrorData> {
        let prompt = horde_adapter_claude::permission::PromptRequest {
            tool_name: name.to_owned(),
            input: arguments_as_input(arguments.as_ref()),
            tool_use_id,
        };

        let decision = self
            .broker()
            .adjudicate(identity, &prompt)
            .await
            .map_err(internal)?;

        match decision {
            horde_adapter::PermissionDecision::Deny { message } => {
                // A denial the model reads, not a protocol error it cannot:
                // the model must know the call was refused (and why) to plan
                // around it rather than retry it.
                Ok(CallToolResult::error(vec![
                    rmcp::model::ContentBlock::text(format!("permission denied: {message}")),
                ]))
            }
            horde_adapter::PermissionDecision::Allow { updated_input } => {
                let forwarded = match narrowed_arguments(arguments, updated_input) {
                    Ok(forwarded) => forwarded,
                    Err(refusal) => {
                        // Never downgraded to the original call: a narrowing
                        // that could not be carried would otherwise run the
                        // wider call it existed to prevent.
                        return Ok(CallToolResult::error(vec![
                            rmcp::model::ContentBlock::text(refusal),
                        ]));
                    }
                };
                self.proxy
                    .call(name, forwarded)
                    .await
                    .map_or_else(crate::hub::endpoint::render_failure, Ok)
            }
        }
    }

    /// The permission-request id for one proxied call.
    ///
    /// The harness's own correlation id when it sent one — that is what lets
    /// an `approve`-stage decision cover the call stage too — and a minted,
    /// session-unique id otherwise. Minted from wall-clock nanos plus a
    /// process-local ordinal: the ordinal separates calls within this
    /// process, the nanos separate this process's ids from a predecessor's
    /// (a restarted endpoint must not re-mint a key the journal already
    /// holds, because the dedupe would silently swallow the new request).
    fn request_id_for(&self, identity: &AgentIdentity, meta: Option<&RequestMetaObject>) -> String {
        if let Some(carried) = harness_tool_use_id(meta) {
            return carried;
        }
        let ordinal = self.minted.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        format!("mcp:{}:{nanos}:{ordinal}", identity.agent_id())
    }
}

/// The harness's own id for this call, when its `_meta` carried one.
///
/// Claude Code stamps `claudecode/toolUseId` on every `tools/call` (measured
/// in the dual-era spike §2) — the same id its permission prompt quotes,
/// which is what makes the two gates one permission event.
#[must_use]
pub fn harness_tool_use_id(meta: Option<&RequestMetaObject>) -> Option<String> {
    meta?
        .0
        .0
        .get("claudecode/toolUseId")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

/// The proposal the chain adjudicates, from the call's arguments.
///
/// No arguments is an empty object rather than `null`: the chain's rules and
/// the approver's rendering both address fields, and `null` would read as a
/// different shape from "called with no arguments".
#[must_use]
pub fn arguments_as_input(arguments: Option<&JsonObject>) -> serde_json::Value {
    match arguments {
        Some(fields) => serde_json::Value::Object(fields.clone()),
        None => serde_json::Value::Object(serde_json::Map::new()),
    }
}

/// The arguments actually forwarded, once the chain has spoken.
///
/// An allow without a narrowing forwards the original arguments unchanged.
/// A narrowing replaces them — and must be an object, because tool arguments
/// are a named bag; anything else is refused in words rather than quietly
/// running the original call.
///
/// # Errors
///
/// The refusal text for a narrowing that is not an object.
pub fn narrowed_arguments(
    original: Option<JsonObject>,
    updated: Option<serde_json::Value>,
) -> Result<Option<JsonObject>, String> {
    match updated {
        None => Ok(original),
        Some(serde_json::Value::Object(fields)) => Ok(Some(fields)),
        Some(other) => Err(format!(
            "the permission decision narrowed this call to a non-object ({}), which cannot be \
             tool arguments; refusing to run the original call instead",
            kind_of(&other)
        )),
    }
}

/// The JSON kind name, for refusals that must say what arrived.
fn kind_of(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

#[tool_handler]
impl ServerHandler for HordeEndpoint {
    /// Verbs first, then the proxied namespace, then refusal.
    ///
    /// Hand-written (the `#[tool_handler]` macro generates this only when it
    /// is absent) because the router alone no longer covers the surface: a
    /// namespaced name routes through the proxy, and its dispatch is where
    /// the permission chain runs.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if self.tool_router.has_route(&request.name) {
            let tcc = ToolCallContext::new(self, request, context);
            return self.tool_router.call(tcc).await;
        }

        if self.proxy.serves(&request.name) {
            let identity = Self::caller(&context)?;
            let request_id = self.request_id_for(&identity, request.meta.as_ref());
            let result = self
                .call_proxied(&identity, &request.name, request.arguments, request_id)
                .await?;
            return Ok(CallToolResponse::Complete(result));
        }

        Err(ErrorData::new(
            rmcp::model::ErrorCode::METHOD_NOT_FOUND,
            format!("no tool named {}", request.name),
            None,
        ))
    }

    /// The verbs plus every proxied tool this Agent may see.
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let mut tools = self.tool_router.list_all();
        tools.extend(self.proxy.tools());
        Ok(crate::hub::endpoint::listing(tools, &context))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_router.get(name).cloned().or_else(|| {
            self.proxy
                .tools()
                .into_iter()
                .find(|tool| tool.name == name)
        })
    }

    fn get_info(&self) -> ServerInfo {
        // `ServerInfo` and `Implementation` are `#[non_exhaustive]`, so they are
        // built by mutation rather than as struct expressions.
        let mut info = ServerInfo::default();
        // 2025-11-25 in rmcp 3.1.2, and served deliberately rather than
        // inherited: every harness client Horde ships an adapter for is
        // legacy-era, and the constant will move.
        info.protocol_version = ProtocolVersion::LATEST;
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info.name = "horde".to_owned();
        info.server_info.version = env!("CARGO_PKG_VERSION").to_owned();
        info.instructions = Some(
            "Horde orchestrator. You have your own worktree; commit as you go. Agents you \
             spawn start from your latest commit, and your commits ride your result \
             automatically — report a summary, not a list of commits."
                .to_owned(),
        );
        info
    }
}

#[cfg(test)]
mod tests {
    use super::HordeEndpoint;
    use super::{AgentIdentity, ReportParams};
    use crate::verbs::MessageKind;
    use rmcp::ServerHandler;

    fn endpoint() -> HordeEndpoint {
        // A client that dials nothing: these tests are about the tool surface,
        // which is decided before any call reaches the control plane.
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        HordeEndpoint::new(crate::context::AgentContext::new(
            std::sync::Arc::new(crate::journal::testing::RecordingJournal::new()),
            horde_cli::client::Client::with_channel(channel),
            "s1",
        ))
    }

    /// `internal` must keep both the code and the cause. A default-constructed
    /// `ErrorData` reports neither, and a failure that reaches the harness as
    /// a blank error is one nobody can diagnose from the harness side.
    #[test]
    fn internal_keeps_the_code_and_the_cause() {
        let error = super::internal("the journal is gone");
        assert_eq!(error.code, rmcp::model::ErrorCode::INTERNAL_ERROR);
        assert_eq!(error.message, "the journal is gone");
    }

    /// The Debug rendering names the type and shows the context. A `fmt` that
    /// writes nothing turns every log line carrying an endpoint into silence.
    #[tokio::test]
    async fn debug_names_the_endpoint_rather_than_hiding_it() {
        let rendered = format!("{:?}", endpoint());
        assert!(
            rendered.contains("HordeEndpoint"),
            "the Debug form must say what it is; got {rendered:?}"
        );
    }

    /// The endpoint must advertise the legacy revision deliberately. A modern
    /// -only endpoint would ship zero tools to every agent Horde can drive,
    /// silently and with no error event.
    #[tokio::test]
    async fn the_advertised_revision_is_the_legacy_one() {
        let info = endpoint().get_info();
        assert_eq!(
            info.protocol_version.to_string(),
            "2025-11-25",
            "every harness client we ship an adapter for is legacy-era"
        );
    }

    #[tokio::test]
    async fn tools_are_advertised_at_all() {
        assert!(
            endpoint().get_info().capabilities.tools.is_some(),
            "an endpoint that advertises no tools ships none"
        );
    }

    /// The names are the agent-facing contract — `--permission-prompt-tool` is
    /// pointed at `approve` by name, and renaming a verb silently breaks every
    /// prompt that mentions it.
    fn identity() -> AgentIdentity {
        crate::testing::identity("agent-42")
    }

    /// A status the agent invented is refused before anything is written.
    #[tokio::test]
    async fn reporting_an_unknown_status_is_refused_rather_than_guessed() {
        let error = endpoint()
            .record(
                &identity(),
                &ReportParams {
                    status: "finished".to_owned(),
                    summary: "done".to_owned(),
                },
            )
            .await
            .expect_err("`finished` is not one of the three statuses");

        assert!(
            error.message.contains("success, partial, or blocked"),
            "the message must say what is allowed: {}",
            error.message
        );
        assert!(
            error.message.contains("finished"),
            "and what was received: {}",
            error.message
        );
    }

    /// A result that could not be journaled surfaces as an error. This endpoint
    /// dials nowhere, so the journal is the in-memory one and the control plane
    /// is unreachable — the interesting half is that a valid status gets past
    /// validation and the failure comes from the write, not the parse.
    #[tokio::test]
    async fn a_valid_status_passes_validation_and_reaches_the_journal() {
        let recorded = endpoint()
            .record(
                &identity(),
                &ReportParams {
                    status: "partial".to_owned(),
                    summary: "got halfway".to_owned(),
                },
            )
            .await
            .expect("the in-memory journal accepts it");

        assert_eq!(recorded, "recorded");
    }

    /// The three statuses an agent may name, and everything else.
    #[test]
    fn only_the_three_real_statuses_are_accepted() {
        assert_eq!(
            super::status_from_name("success"),
            Some(horde_proto::v1::ResultStatus::Success)
        );
        assert_eq!(
            super::status_from_name("partial"),
            Some(horde_proto::v1::ResultStatus::Partial)
        );
        assert_eq!(
            super::status_from_name("blocked"),
            Some(horde_proto::v1::ResultStatus::Blocked)
        );
        for rejected in ["", "Success", "done", "ok", "unspecified"] {
            assert_eq!(
                super::status_from_name(rejected),
                None,
                "{rejected} is not a status and must be refused rather than \
                 rounded to one"
            );
        }
    }

    /// An unreadable status must not become a success. A parent acts on this.
    #[test]
    fn an_unrecognized_status_reports_as_unknown_rather_than_success() {
        assert_eq!(
            super::status_name(horde_proto::v1::ResultStatus::Success as i32),
            "success"
        );
        assert_eq!(
            super::status_name(horde_proto::v1::ResultStatus::Partial as i32),
            "partial"
        );
        assert_eq!(
            super::status_name(horde_proto::v1::ResultStatus::Blocked as i32),
            "blocked"
        );
        assert_eq!(
            super::status_name(horde_proto::v1::ResultStatus::Unspecified as i32),
            "unknown"
        );
        assert_eq!(
            super::status_name(9_999),
            "unknown",
            "a status from a newer build is unreadable, not successful"
        );
    }

    /// A send naming refs carries them as its payload; naming none carries
    /// no payload rather than an empty one (an empty payload would read as
    /// "work: nothing" instead of "no work named").
    #[test]
    fn send_git_refs_become_the_payload_or_nothing() {
        assert_eq!(super::payload_from_refs(&[]), None);

        let payload = super::payload_from_refs(&[super::SendGitRef {
            repo: "atlas".to_owned(),
            commit: "abc123".to_owned(),
            branch: "main".to_owned(),
        }])
        .expect("a named ref is a payload");
        assert_eq!(payload.git_refs.len(), 1);
        assert_eq!(payload.git_refs[0].repo_name, "atlas");
        assert_eq!(payload.git_refs[0].commit_sha, "abc123");
        assert_eq!(payload.git_refs[0].branch, "main");
    }

    /// One bit, and the inversion is the whole risk.
    #[test]
    fn the_note_flag_selects_the_message_kind() {
        assert_eq!(super::message_kind_for(true), MessageKind::Note);
        assert_eq!(super::message_kind_for(false), MessageKind::Assign);
    }

    /// The verb name is what modes write rules against, so the mapping from
    /// direction to name is contract, not cosmetics.
    #[test]
    fn a_forward_is_judged_under_its_directions_verb() {
        let spec = |direction: horde_proto::v1::ForwardDirection| horde_proto::v1::ForwardSpec {
            direction: direction as i32,
            ..horde_proto::v1::ForwardSpec::default()
        };
        assert_eq!(
            super::forward_verb_name(&spec(horde_proto::v1::ForwardDirection::Expose)),
            "expose"
        );
        assert_eq!(
            super::forward_verb_name(&spec(horde_proto::v1::ForwardDirection::Inject)),
            "inject"
        );
    }

    /// The grant message is what the agent acts on: an inject names the local
    /// address to dial, an expose says who realizes it.
    #[test]
    fn the_grant_message_tells_the_agent_what_to_do_next() {
        let inject = super::forward_granted_message("inject", "db", 15432);
        assert!(inject.contains("127.0.0.1:15432"), "{inject}");
        assert!(inject.contains("db"), "{inject}");

        let expose = super::forward_granted_message("expose", "vite", 5173);
        assert!(expose.contains("5173"), "{expose}");
        assert!(expose.contains("vite"), "{expose}");
        assert!(
            !expose.contains("127.0.0.1"),
            "an expose is not dialable worker-side: {expose}"
        );
    }

    #[tokio::test]
    async fn the_tool_names_are_the_ones_agents_are_told_about() {
        let names = endpoint().tool_names();

        for expected in [
            "approve",
            "spawn_agent",
            "send_message",
            "report_result",
            "wait_for_agent",
            "expose",
            "inject",
        ] {
            assert!(
                names.iter().any(|name| name == expected),
                "{expected} must be served; got {names:?}"
            );
        }
    }

    /// The harness's correlation id is what lets an `approve`-stage decision
    /// cover the call stage; a missing or non-string one falls back to
    /// minting, never to a panic or an empty id.
    #[test]
    fn the_harness_tool_use_id_is_read_from_meta_when_it_is_there() {
        assert_eq!(super::harness_tool_use_id(None), None);

        let mut meta = rmcp::model::RequestMetaObject::new();
        meta.0.0.insert(
            "claudecode/toolUseId".to_owned(),
            serde_json::Value::String("toolu_01".to_owned()),
        );
        assert_eq!(
            super::harness_tool_use_id(Some(&meta)),
            Some("toolu_01".to_owned())
        );

        let mut wrong_shape = rmcp::model::RequestMetaObject::new();
        wrong_shape.0.0.insert(
            "claudecode/toolUseId".to_owned(),
            serde_json::Value::from(42),
        );
        assert_eq!(
            super::harness_tool_use_id(Some(&wrong_shape)),
            None,
            "a non-string id is not an id"
        );
    }

    /// Two shapes of proposal, one rule: the chain always adjudicates an
    /// object, and "no arguments" and "null" must not be conflated.
    #[test]
    fn the_adjudicated_input_is_always_an_object() {
        assert_eq!(
            super::arguments_as_input(None),
            serde_json::json!({}),
            "no arguments is an empty object, not null"
        );

        let mut fields = rmcp::model::JsonObject::new();
        fields.insert("path".to_owned(), serde_json::Value::String("/x".into()));
        assert_eq!(
            super::arguments_as_input(Some(&fields)),
            serde_json::json!({"path": "/x"})
        );
    }

    /// The narrowing truth table: absent forwards the original, an object
    /// replaces it, anything else refuses — running the original call under
    /// a narrowing that failed to apply is the one wrong answer.
    #[test]
    fn a_narrowing_replaces_the_arguments_or_refuses_never_widens() {
        let mut original = rmcp::model::JsonObject::new();
        original.insert("path".to_owned(), serde_json::Value::String("/etc".into()));

        assert_eq!(
            super::narrowed_arguments(Some(original.clone()), None),
            Ok(Some(original.clone())),
            "an unmodified allow forwards the proposal"
        );

        let narrowed = super::narrowed_arguments(
            Some(original.clone()),
            Some(serde_json::json!({"path": "/tmp/safe"})),
        )
        .unwrap_or_else(|refusal| panic!("an object narrowing applies: {refusal}"))
        .unwrap_or_else(|| panic!("the narrowing must produce arguments"));
        assert_eq!(
            narrowed.get("path"),
            Some(&serde_json::Value::String("/tmp/safe".into()))
        );

        let refused = super::narrowed_arguments(Some(original), Some(serde_json::json!("rm -rf")))
            .expect_err("a string is not tool arguments");
        assert!(refused.contains("a string"), "{refused}");
        assert!(
            refused.contains("refusing"),
            "the refusal must say the original call will not run: {refused}"
        );
    }

    /// Every JSON kind names itself in a refusal — the message is the only
    /// diagnosis an operator gets, and "a string" where an array arrived
    /// sends them hunting the wrong producer.
    #[test]
    fn every_json_kind_names_itself() {
        let cases = [
            (serde_json::json!(null), "null"),
            (serde_json::json!(true), "a boolean"),
            (serde_json::json!(7), "a number"),
            (serde_json::json!("rm -rf"), "a string"),
            (serde_json::json!([1, 2]), "an array"),
            (serde_json::json!({"a": 1}), "an object"),
        ];
        for (value, expected) in cases {
            assert_eq!(super::kind_of(&value), expected);
        }
    }

    /// Minted ids must differ call to call — a repeated id is a permission
    /// request the journal silently swallows as a duplicate.
    #[tokio::test]
    async fn minted_request_ids_never_repeat() {
        let endpoint = endpoint();
        let identity = identity();
        let first = endpoint.request_id_for(&identity, None);
        let second = endpoint.request_id_for(&identity, None);
        assert_ne!(first, second);
        assert!(first.contains("agent-42"), "{first}");

        // And a carried harness id wins over minting.
        let mut meta = rmcp::model::RequestMetaObject::new();
        meta.0.0.insert(
            "claudecode/toolUseId".to_owned(),
            serde_json::Value::String("toolu_02".to_owned()),
        );
        assert_eq!(endpoint.request_id_for(&identity, Some(&meta)), "toolu_02");
    }
}
