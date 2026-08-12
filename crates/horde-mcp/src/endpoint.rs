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

use crate::identity::AgentIdentity;
use crate::journal::JournalSink;
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

/// The tools one Agent's endpoint serves.
#[derive(Clone)]
pub struct HordeEndpoint {
    journal: std::sync::Arc<dyn JournalSink>,
    client: horde_cli::client::Client,
    session_id: String,
    tool_router: ToolRouter<Self>,
}

impl std::fmt::Debug for HordeEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HordeEndpoint")
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

#[tool_router]
impl HordeEndpoint {
    #[must_use]
    pub fn new(
        journal: std::sync::Arc<dyn JournalSink>,
        client: horde_cli::client::Client,
        session_id: impl Into<String>,
    ) -> Self {
        Self {
            journal,
            client,
            session_id: session_id.into(),
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
        PermissionBroker::new(
            std::sync::Arc::clone(&self.journal),
            self.client.clone(),
            self.session_id.clone(),
        )
    }

    fn verbs(&self) -> Verbs {
        Verbs::new(
            std::sync::Arc::clone(&self.journal),
            self.client.clone(),
            self.session_id.clone(),
        )
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
            .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;

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
            .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;

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
        let kind = if params.note {
            MessageKind::Note
        } else {
            MessageKind::Assign
        };
        // Keyed on the sender and the text so a retried send is not delivered
        // twice; see `Verbs::report_result` on why the key must be unique.
        let key = format!("{}:{}", identity.agent_id(), params.text);

        let sequence = self
            .verbs()
            .send_message(
                &params.to_agent_id,
                kind,
                &params.text,
                params.interrupt,
                &key,
            )
            .await
            .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;

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
        let status = match params.status.as_str() {
            "success" => horde_proto::v1::ResultStatus::Success,
            "partial" => horde_proto::v1::ResultStatus::Partial,
            "blocked" => horde_proto::v1::ResultStatus::Blocked,
            other => {
                return Err(ErrorData::invalid_params(
                    format!("status must be success, partial, or blocked; got {other}"),
                    None,
                ));
            }
        };

        let key = format!("result:{}:{}", identity.agent_id(), params.summary);
        self.verbs()
            .report_result(&identity, status, &params.summary, &key)
            .await
            .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;

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
            .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;

        let status = match horde_proto::v1::ResultStatus::try_from(result.status) {
            Ok(horde_proto::v1::ResultStatus::Success) => "success",
            Ok(horde_proto::v1::ResultStatus::Partial) => "partial",
            Ok(horde_proto::v1::ResultStatus::Blocked) => "blocked",
            // Never guessed into a success: a result whose status this build
            // does not recognize is reported as unknown so a parent does not
            // act on an invented outcome.
            Ok(horde_proto::v1::ResultStatus::Unspecified) | Err(_) => "unknown",
        };

        Ok(Json(ResultView {
            status: status.to_owned(),
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
    use rmcp::ServerHandler;

    fn endpoint() -> HordeEndpoint {
        // A client that dials nothing: these tests are about the tool surface,
        // which is decided before any call reaches the control plane.
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        HordeEndpoint::new(
            std::sync::Arc::new(crate::journal::testing::RecordingJournal::new()),
            horde_cli::client::Client::with_channel(channel),
            "s1",
        )
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
