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
use crate::hub::{AgentProxies, ToolProxy};
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
    /// Another agent type for the child. Omitted, it is yours. A type that
    /// would see a tool server yours cannot is refused.
    #[serde(default)]
    pub agent_type: Option<String>,
    /// Another model tier for the child, e.g. "fast". Omitted, it is yours.
    #[serde(default)]
    pub tier: Option<String>,
    /// The repos the child checks out at start (the rest on demand with
    /// workspace_checkout). Naming any implies a lazy checkout.
    #[serde(default)]
    pub repos: Vec<String>,
    /// Where a repo's checkout starts: {repo: commit, origin branch or tag}.
    /// Repos you have checked out and do not name start from your latest
    /// commit.
    #[serde(default)]
    pub bases: std::collections::BTreeMap<String, String>,
    /// The branch the child creates in every repo it checks out; one that
    /// exists on origin is continued. Omitted: horde/<child-id>.
    #[serde(default)]
    pub branch: Option<String>,
    /// "all" or "lazy"; omitted, the workspace's mode.
    #[serde(default)]
    pub checkout_mode: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WorkspaceCheckoutParams {
    /// The repo, by its workspace name.
    pub repo: String,
    /// Where to start: a commit, an origin branch or a tag. Omitted, the
    /// latest origin default branch.
    #[serde(default)]
    pub base: Option<String>,
}

/// The wire mode a `checkout_mode` argument names.
///
/// # Errors
///
/// Anything but `all` or `lazy`.
pub fn checkout_mode_named(name: Option<&str>) -> Result<horde_proto::v1::CheckoutMode, ErrorData> {
    match name {
        None => Ok(horde_proto::v1::CheckoutMode::Unspecified),
        Some("all") => Ok(horde_proto::v1::CheckoutMode::All),
        Some("lazy") => Ok(horde_proto::v1::CheckoutMode::Lazy),
        Some(other) => Err(ErrorData::invalid_params(
            format!("checkout_mode is \"all\" or \"lazy\", not {other:?}"),
            None,
        )),
    }
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

/// The raise a forward declaration becomes: judged under its direction's
/// verb name, with the spec's proposal as the arguments modes constrain.
#[must_use]
pub fn forward_prompt(
    spec: &horde_proto::v1::ForwardSpec,
    tool_use_id: String,
) -> horde_adapter_claude::permission::PromptRequest {
    horde_adapter_claude::permission::PromptRequest {
        tool_name: forward_verb_name(spec).to_owned(),
        input: crate::forwards::forward_proposal(spec),
        tool_use_id,
    }
}

/// What a settled declaration means to the agent: a denial with its reason,
/// or a grant naming the journaled intent's derived id.
#[must_use]
pub fn declared_result(
    decision: &horde_adapter::PermissionDecision,
    verb: &str,
    name: &str,
    worker_port: u32,
    request_id: &str,
) -> ForwardResult {
    match decision {
        horde_adapter::PermissionDecision::Deny { message } => ForwardResult {
            granted: false,
            forward_id: String::new(),
            message: format!("denied: {message}"),
        },
        horde_adapter::PermissionDecision::Allow { .. } => ForwardResult {
            granted: true,
            forward_id: horde_proto::forward::derived_forward_id(request_id),
            message: forward_granted_message(verb, name, worker_port),
        },
    }
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
    /// The proxied half of the toolset (DESIGN §5 items 2 and 3), per Agent:
    /// each Agent's hub link and its own `local` servers. One endpoint serves
    /// a whole Session's agents, and their proxied toolsets differ — by
    /// visibility and by which `local` children are theirs — so the mapping
    /// is per caller, resolved from the request's authenticated identity.
    /// Empty by default: an endpoint with no proxies registered serves
    /// exactly the verbs.
    proxies: Arc<AgentProxies>,
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
            proxies: Arc::new(AgentProxies::new()),
            minted: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Attaches the per-Agent proxied toolsets. The registry stays shared
    /// with whoever registers agents into it (the Worker's endpoint manager),
    /// so spawning and archiving reshape a serving endpoint's surface live.
    #[must_use]
    pub fn with_agent_proxies(mut self, proxies: Arc<AgentProxies>) -> Self {
        self.proxies = proxies;
        self
    }

    /// The tool names this endpoint serves to `agent_id` — the verbs and
    /// every proxied tool the agent may see, under its namespaced name.
    ///
    /// Reads the router the `#[tool_handler]` macro dispatches through, so a
    /// test can assert on *this instance's* surface rather than on what the
    /// static constructor would have built.
    #[must_use]
    pub fn tool_names_for(&self, agent_id: &str) -> Vec<String> {
        self.tool_router
            .list_all()
            .into_iter()
            .chain(self.proxies.for_agent(agent_id).tools())
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
        description = "Spawn a child agent under yourself, in your workspace, with your agent type and model unless you name another type or tier. Never blocks: an agent with nowhere to run comes back queued with the reason."
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
        let key = self.minted_request_id(&identity);
        let checkout_mode = checkout_mode_named(params.checkout_mode.as_deref())?;
        // The parent's committed HEADs, staged to the Relay, unless the
        // caller named the repo's base itself (ADR-0014).
        let parent_bases = match self.context.workspace() {
            Some(workspace) => workspace
                .parent_bases(identity.agent_id())
                .await
                .map_err(|error| ErrorData::internal_error(error, None))?,
            None => Vec::new(),
        };
        let overrides = crate::verbs::ChildOverrides {
            agent_type: params.agent_type,
            tier: params.tier,
            repos: params.repos,
            bases: crate::workspace::merged_bases(&params.bases, parent_bases),
            branch: params.branch.filter(|branch| !branch.is_empty()),
            checkout_mode,
        };
        let outcome = self
            .verbs()
            .spawn_agent(&identity, &params.task, &overrides, &key)
            .await
            .map_err(internal)?;

        Ok(Json(SpawnResult {
            agent_id: outcome.agent_id,
            placed: outcome.placed,
            reason: outcome.reason,
        }))
    }

    #[tool(
        name = "workspace_checkout",
        description = "Check out one more repo of your workspace, on your branch, and get its path. Call it before reading or changing a repo your workspace manifest lists as not checked out. Calling it again for a repo you have is harmless."
    )]
    async fn workspace_checkout(
        &self,
        Parameters(params): Parameters<WorkspaceCheckoutParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<crate::workspace::CheckoutAnswer>, ErrorData> {
        let identity = Self::caller(&context)?;
        let workspace = self.context.workspace().ok_or_else(|| {
            ErrorData::invalid_params(
                "this agent runs without a workspace; there is nothing to check out",
                None,
            )
        })?;
        workspace
            .checkout(identity.agent_id(), &params.repo, params.base.as_deref())
            .await
            .map(Json)
            .map_err(|reason| ErrorData::invalid_params(reason, None))
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
        let worker_port = crate::forwards::inject_worker_port(params.port, params.worker_port)
            .map_err(|refusal| ErrorData::invalid_params(refusal, None))?;
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
        crate::forwards::declaration_gate(&spec)
            .map_err(|refusal| ErrorData::invalid_params(refusal, None))?;

        // The request id is minted, not harness-carried: verbs dispatched
        // through the tool router cannot see `_meta`, so a retried declare
        // re-raises rather than deduping — a safe failure (the chain
        // re-asks), not a silent one.
        let prompt = forward_prompt(&spec, self.minted_request_id(identity));
        let name = spec.name.clone();
        let worker_port = spec.worker_port;
        let decision = self
            .broker()
            .adjudicate_forward(identity, &prompt, spec)
            .await
            .map_err(internal)?;

        Ok(Json(declared_result(
            &decision,
            &prompt.tool_name,
            &name,
            worker_port,
            &prompt.tool_use_id,
        )))
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

    /// The chain's decision on one proxied call — reached exactly once.
    ///
    /// This is where DESIGN §10 puts the chain for hub and `local` tools:
    /// every proxied call is raised as an ordinary `PermissionRequested` and
    /// blocks until the chain answers — deterministically in microseconds
    /// when a rule covers it, or from a human's phone when nothing does.
    ///
    /// Each harness also gates the call on its own side first, so the work
    /// here is making the two gates one permission event under one name:
    ///
    /// * **Claude** pre-authorizes through `approve` (`--permission-prompt-tool`)
    ///   and stamps the same `_meta.claudecode/toolUseId` on the `tools/call`.
    ///   Raising under that id makes this raise a keyed replay of the first:
    ///   the journal deduplicates it and the wait finds the decision already
    ///   written, so one answer covers both gates.
    /// * **Codex** asks through an elicitation the Worker raises under the item
    ///   id and `horde/{tool}`, and its `tools/call` carries no id to correlate
    ///   by. The call is matched to that decision instead — same agent, same
    ///   name, same arguments, consumed once ([`PermissionBroker::decided_earlier`]).
    ///   A call codex did not ask about is gated here, under the same
    ///   `horde/{tool}` name the elicitation would have used, so one rule
    ///   covers the tool whichever gate runs and the chain stays the authority
    ///   over every call that reaches the tool.
    async fn decide_proxied(
        &self,
        identity: &AgentIdentity,
        name: &str,
        input: serde_json::Value,
        meta: &RequestMetaObject,
    ) -> Result<horde_adapter::PermissionDecision, ErrorData> {
        let broker = self.broker();
        let (tool_name, tool_use_id) = match harness_tool_use_id(Some(meta)) {
            Some(carried) => (name.to_owned(), carried),
            None if is_codex_call(meta) => {
                let tool_name = codex_tool_name(name);
                let earlier = broker
                    .decided_earlier(identity, &tool_name, &input)
                    .await
                    .map_err(internal)?;
                if let Some(decision) = earlier {
                    return Ok(decision);
                }
                (tool_name, self.minted_request_id(identity))
            }
            None => (name.to_owned(), self.minted_request_id(identity)),
        };
        let prompt = horde_adapter_claude::permission::PromptRequest {
            tool_name,
            input,
            tool_use_id,
        };
        broker.adjudicate(identity, &prompt).await.map_err(internal)
    }

    /// One proxied tool call, run as the chain decided it.
    ///
    /// The forwarded call is the *decided* one: a narrowing that arrives with
    /// the allow replaces the arguments, and a narrowing that cannot be
    /// applied refuses rather than running the original, wider call.
    async fn call_proxied(
        proxy: &ToolProxy,
        name: &str,
        arguments: Option<JsonObject>,
        decision: horde_adapter::PermissionDecision,
    ) -> Result<CallToolResult, ErrorData> {
        match decision {
            horde_adapter::PermissionDecision::Deny { message } => {
                // A denial the model reads, not a protocol error it cannot:
                // the model must know the call was refused (and why) to plan
                // around it rather than retry it.
                Ok(CallToolResult::error(vec![
                    rmcp::model::ContentBlock::text(format!("permission denied: {message}")),
                ]))
            }
            horde_adapter::PermissionDecision::Allow { updated_input, .. } => {
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
                proxy
                    .call(name, forwarded)
                    .await
                    .map_or_else(crate::hub::endpoint::render_failure, Ok)
            }
        }
    }

    /// A session-unique permission-request id, for a call whose harness sent
    /// no correlation id of its own.
    ///
    /// Minted from wall-clock nanos plus a process-local ordinal: the ordinal
    /// separates calls within this process, the nanos separate this process's
    /// ids from a predecessor's (a restarted endpoint must not re-mint a key
    /// the journal already holds, because the dedupe would silently swallow
    /// the new request).
    fn minted_request_id(&self, identity: &AgentIdentity) -> String {
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

/// The server name every harness is configured to reach this endpoint under:
/// Claude sees its tools as `mcp__horde__…`, and codex is launched with
/// `mcp_servers.horde` (the Worker's codex launch). It is the `{server}` in
/// the `{server}/{tool}` name a codex elicitation is raised under.
pub const SERVER_NAME: &str = "horde";

/// The `_meta` key codex stamps on every `tools/call` it sends (transcript
/// 17's MCP-stub log: thread, turn, sandbox, model — never the item id).
pub const CODEX_TURN_METADATA: &str = "x-codex-turn-metadata";

/// Whether a `tools/call` came from codex.
///
/// Used to pick the gate's vocabulary and to look for the elicitation that
/// already gated the call — never to skip the gate: a caller that forges the
/// key gains nothing but a name, because a call is only covered by a decision
/// the chain gave this agent for exactly this call.
#[must_use]
pub fn is_codex_call(meta: &RequestMetaObject) -> bool {
    meta.0.0.contains_key(CODEX_TURN_METADATA)
}

/// The canonical name a codex agent's proxied call is gated under — the
/// `{server}/{tool}` its elicitation names (ADR-0005), so a rule written for
/// the tool matches whichever gate runs.
#[must_use]
pub fn codex_tool_name(name: &str) -> String {
    format!("{SERVER_NAME}/{name}")
}

/// How Claude Code spells this server's tools: `mcp__{server}__{tool}`.
pub const CLAUDE_TOOL_PREFIX: &str = "mcp__horde__";

/// The tool a Horde-served name spells, under either harness's spelling —
/// `horde/{tool}` (codex, and every elicitation) or `mcp__horde__{tool}`
/// (Claude) — or `None` for a tool this server does not serve (`Bash`,
/// another MCP server's tool). Hub tools ride the same prefix
/// (`horde/github__create_issue`), so they are covered too.
#[must_use]
pub fn horde_tool(name: &str) -> Option<&str> {
    name.strip_prefix(CLAUDE_TOOL_PREFIX)
        .or_else(|| name.strip_prefix("horde/"))
        .filter(|tool| !tool.is_empty())
}

/// The one canonical name a permission rule, a remembered decision, and
/// the chain compare by (ADR-0004, 2026-10-31 amendment): `horde/{tool}`
/// for anything this server serves, whichever harness spelled it; any
/// other name unchanged. Harness-visible names never change — each harness
/// still sees and sends its own spelling — so one rule written as
/// `horde/report_result` covers a Claude agent and a codex agent alike.
#[must_use]
pub fn canonical_tool_name(name: &str) -> std::borrow::Cow<'_, str> {
    // Only Claude's spelling is rewritten; `horde/{tool}` already is the
    // canonical form and passes through borrowed with everything else.
    match name
        .strip_prefix(CLAUDE_TOOL_PREFIX)
        .filter(|tool| !tool.is_empty())
    {
        Some(tool) => std::borrow::Cow::Owned(codex_tool_name(tool)),
        None => std::borrow::Cow::Borrowed(name),
    }
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

        // Everything past the verbs is per-caller: which proxy serves the
        // name depends on who is asking, so an unidentified caller is refused
        // here rather than answered from anybody's toolset.
        let identity = Self::caller(&context)?;
        let proxy = self.proxies.for_agent(identity.agent_id());
        if proxy.serves(&request.name) {
            // The harness's `_meta` is read off the *context*, not the params:
            // rmcp moves a request's `_meta` into `RequestContext.meta`
            // (service/server.rs, `std::mem::take(request.get_meta_mut())`),
            // so by the time a handler sees the params their `meta` is always
            // empty. Measured the hard way: the deployed-stack e2e's approve
            // targets the script's tool-use id, and reading `request.meta`
            // here left every proxied call under a minted id instead.
            let decision = self
                .decide_proxied(
                    &identity,
                    &request.name,
                    arguments_as_input(request.arguments.as_ref()),
                    &context.meta,
                )
                .await?;
            let result =
                Self::call_proxied(&proxy, &request.name, request.arguments, decision).await?;
            return Ok(CallToolResponse::Complete(result));
        }

        Err(ErrorData::new(
            rmcp::model::ErrorCode::METHOD_NOT_FOUND,
            format!("no tool named {}", request.name),
            None,
        ))
    }

    /// The verbs plus every proxied tool this caller may see. A listing with
    /// no authenticated caller shows the verbs alone — fail closed, since a
    /// proxied toolset is somebody's in particular.
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let mut tools = self.tool_router.list_all();
        if let Ok(identity) = Self::caller(&context) {
            tools.extend(self.proxies.for_agent(identity.agent_id()).tools());
        }
        Ok(crate::hub::endpoint::listing(tools, &context))
    }

    /// Identity-free by rmcp's signature, so the proxied half searches every
    /// registered proxy. Advisory metadata only: authorization stays in
    /// `call_tool`, which resolves the *caller's* proxy before dispatching.
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_router.get(name).cloned().or_else(|| {
            self.proxies
                .all()
                .into_iter()
                .flat_map(|proxy| proxy.tools())
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

    /// A declaration's raise and its settled result, both pure: the prompt
    /// is judged under the direction's verb with string ports, and the
    /// result carries the derived id on a grant and the reason on a denial.
    #[test]
    fn a_declaration_raises_and_settles_as_the_adr_says() {
        let spec = horde_proto::v1::ForwardSpec {
            direction: horde_proto::v1::ForwardDirection::Expose as i32,
            name: "vite".to_owned(),
            worker_port: 5173,
            client_port: 0,
        };
        let prompt = super::forward_prompt(&spec, "req-9".to_owned());
        assert_eq!(prompt.tool_name, "expose");
        assert_eq!(prompt.tool_use_id, "req-9");
        assert_eq!(prompt.input["port"], "5173", "ports ride as strings");

        let granted = super::declared_result(
            &horde_adapter::PermissionDecision::allow(),
            "expose",
            "vite",
            5173,
            "req-9",
        );
        assert!(granted.granted);
        assert_eq!(
            granted.forward_id, "fwd-req-9",
            "the id the agent hears is the one the log derives"
        );

        let denied = super::declared_result(
            &horde_adapter::PermissionDecision::Deny {
                message: "not on this session".to_owned(),
            },
            "expose",
            "vite",
            5173,
            "req-9",
        );
        assert!(!denied.granted);
        assert!(denied.forward_id.is_empty(), "a denial names no intent");
        assert!(denied.message.contains("not on this session"));
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

    /// `checkout_mode` takes the two modes or nothing; anything else is the
    /// caller's mistake, named, never a guess.
    #[test]
    fn a_checkout_mode_argument_is_all_lazy_or_absent() {
        use horde_proto::v1::CheckoutMode;
        assert_eq!(
            super::checkout_mode_named(None).ok(),
            Some(CheckoutMode::Unspecified)
        );
        assert_eq!(
            super::checkout_mode_named(Some("all")).ok(),
            Some(CheckoutMode::All)
        );
        assert_eq!(
            super::checkout_mode_named(Some("lazy")).ok(),
            Some(CheckoutMode::Lazy)
        );
        let refused = super::checkout_mode_named(Some("some")).expect_err("refused");
        assert!(refused.message.contains("\"some\""), "{refused:?}");
    }

    #[tokio::test]
    async fn the_tool_names_are_the_ones_agents_are_told_about() {
        let names = endpoint().tool_names_for("agent-42");

        for expected in [
            "approve",
            "spawn_agent",
            "send_message",
            "report_result",
            "wait_for_agent",
            "expose",
            "inject",
            "workspace_checkout",
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
        let first = endpoint.minted_request_id(&identity);
        let second = endpoint.minted_request_id(&identity);
        assert_ne!(first, second);
        assert!(first.contains("agent-42"), "{first}");
    }

    /// Codex is recognized by the turn metadata it stamps on every call, and
    /// its gate speaks the `{server}/{tool}` vocabulary its elicitation does.
    #[test]
    fn a_codex_call_is_named_the_way_its_elicitation_names_it() {
        let mut meta = rmcp::model::RequestMetaObject::new();
        assert!(!super::is_codex_call(&meta), "no metadata, not codex");
        meta.0.0.insert(
            "claudecode/toolUseId".to_owned(),
            serde_json::Value::String("toolu_01".to_owned()),
        );
        assert!(
            !super::is_codex_call(&meta),
            "claude's stamp is not codex's"
        );
        meta.0.0.insert(
            super::CODEX_TURN_METADATA.to_owned(),
            serde_json::json!({"turn_id": "t1"}),
        );
        assert!(super::is_codex_call(&meta));

        assert_eq!(
            super::codex_tool_name("github__create_issue"),
            "horde/github__create_issue"
        );
    }

    /// One canonical name per Horde tool, whichever harness spelled it;
    /// everything else passes through untouched.
    #[test]
    fn both_harness_spellings_canonicalize_to_one_name() {
        use super::{canonical_tool_name, horde_tool};
        for (spelled, canonical) in [
            ("mcp__horde__report_result", "horde/report_result"),
            ("horde/report_result", "horde/report_result"),
            (
                "mcp__horde__github__create_issue",
                "horde/github__create_issue",
            ),
            ("Bash", "Bash"),
            ("mcp__other__tool", "mcp__other__tool"),
            ("mcp__horde__", "mcp__horde__"),
        ] {
            assert_eq!(canonical_tool_name(spelled), canonical, "{spelled}");
        }
        assert_eq!(horde_tool("mcp__horde__spawn_agent"), Some("spawn_agent"));
        assert_eq!(horde_tool("horde/spawn_agent"), Some("spawn_agent"));
        assert_eq!(horde_tool("horde/"), None, "no tool is no tool");
        assert_eq!(horde_tool("Write"), None);
        assert!(
            matches!(
                canonical_tool_name("horde/report_result"),
                std::borrow::Cow::Borrowed(_)
            ),
            "the canonical spelling is never rebuilt"
        );
    }
}
