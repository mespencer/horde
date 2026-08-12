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

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{ProtocolVersion, ServerCapabilities, ServerInfo};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};

use crate::context::AgentContext;
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
    /// What the child is for. Carried for the approver and the tree view.
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
/// than one told the outcome is unreadable.
#[must_use]
pub fn status_name(status: i32) -> &'static str {
    match horde_proto::v1::ResultStatus::try_from(status) {
        Ok(horde_proto::v1::ResultStatus::Success) => "success",
        Ok(horde_proto::v1::ResultStatus::Partial) => "partial",
        Ok(horde_proto::v1::ResultStatus::Blocked) => "blocked",
        Ok(horde_proto::v1::ResultStatus::Unspecified) | Err(_) => "unknown",
    }
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
        }
    }

    /// The tool names this endpoint serves.
    ///
    /// Reads the router the `#[tool_handler]` macro dispatches through, so a
    /// test can assert on *this instance's* surface rather than on what the
    /// static constructor would have built.
    #[must_use]
    pub fn tool_names(&self) -> Vec<String> {
        self.tool_router
            .list_all()
            .into_iter()
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
        let outcome = self
            .verbs()
            .spawn_agent(&identity, &params.task)
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
}

#[tool_handler]
impl ServerHandler for HordeEndpoint {
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
        let mut registry = crate::identity::AgentRegistry::new();
        registry.insert("agent-42", "token");
        let mut headers = http::HeaderMap::new();
        headers.insert("authorization", "Bearer token".parse().expect("header"));
        headers.insert("x-horde-agent", "agent-42".parse().expect("header"));
        registry.authenticate(&headers).expect("authenticates")
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

    /// One bit, and the inversion is the whole risk.
    #[test]
    fn the_note_flag_selects_the_message_kind() {
        assert_eq!(super::message_kind_for(true), MessageKind::Note);
        assert_eq!(super::message_kind_for(false), MessageKind::Assign);
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
        ] {
            assert!(
                names.iter().any(|name| name == expected),
                "{expected} must be served; got {names:?}"
            );
        }
    }
}
