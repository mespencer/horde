//! The Worker's hub link: forwarded calls over the authenticated channel.
//!
//! ADR-0004's 2026-08-19 amendment rules how an Agent's hub tools are
//! reached: the Worker forwards each call to the Server over its own
//! authenticated mTLS channel (`WorkerService.CallHubTool`), with the Agent's
//! identity riding the request — the same attribution every other subsystem
//! uses, and the reason no per-agent hub credential exists anywhere. One link
//! is built per Agent at spawn, so the identity a call carries is fixed at
//! construction, never chosen per call by the toolset's consumer.
//!
//! Revocation is inherited, not implemented: these RPCs pass the same
//! `require_enrolled` door as every other call on the channel, so a revoked
//! Worker's forwarded hub calls die with its channel.

use horde_mcp::hub::{HubError, HubLink, link};
use horde_mcp::rmcp::model::{CallToolResult, JsonObject, Tool};
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::{AgentId, CallHubToolRequest, ListHubToolsRequest, SessionId, WorkerId};
use tonic::transport::Channel;

use crate::error::WorkerError;

/// One Agent's route to the hub, over the Worker's channel.
#[derive(Debug, Clone)]
pub struct ForwardedHubLink {
    client: WorkerServiceClient<Channel>,
    worker_id: String,
    session_id: String,
    agent_id: String,
    /// The connect-time snapshot, fetched at construction — the same
    /// contract every upstream's tool list has.
    tools: Vec<Tool>,
}

impl ForwardedHubLink {
    /// Builds the link for one Agent, fetching its visible hub toolset from
    /// the Server over `channel`.
    ///
    /// # Errors
    ///
    /// [`WorkerError::McpSurface`] when the Server refuses the listing or an
    /// entry does not decode — either way the spawn fails loudly rather than
    /// launching an agent missing a promised toolset.
    pub async fn fetch(
        channel: Channel,
        worker_id: &str,
        session_id: &str,
        agent_id: &str,
    ) -> Result<Self, WorkerError> {
        let mut client = WorkerServiceClient::new(channel);
        let listed = client
            .list_hub_tools(ListHubToolsRequest {
                worker_id: Some(WorkerId::new(worker_id)),
                session_id: Some(SessionId::new(session_id)),
                agent_id: Some(AgentId::new(agent_id)),
            })
            .await
            .map_err(|status| {
                WorkerError::McpSurface(format!("the hub tool listing was refused: {status}"))
            })?
            .into_inner();

        let tools = listed
            .tools_json
            .iter()
            .map(|json| link::tool_from_json(json))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                WorkerError::McpSurface(format!("a listed hub tool does not decode: {error}"))
            })?;

        Ok(Self {
            client,
            worker_id: worker_id.to_owned(),
            session_id: session_id.to_owned(),
            agent_id: agent_id.to_owned(),
            tools,
        })
    }
}

#[async_trait::async_trait]
impl HubLink for ForwardedHubLink {
    fn tools(&self) -> &[Tool] {
        &self.tools
    }

    async fn call(
        &self,
        name: &str,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, HubError> {
        let mut client = self.client.clone();
        let outcome = client
            .call_hub_tool(CallHubToolRequest {
                worker_id: Some(WorkerId::new(self.worker_id.clone())),
                session_id: Some(SessionId::new(self.session_id.clone())),
                agent_id: Some(AgentId::new(self.agent_id.clone())),
                tool_name: name.to_owned(),
                arguments_json: link::arguments_to_json(arguments.as_ref()),
            })
            .await;

        match outcome {
            Ok(response) => {
                link::result_from_json(&response.into_inner().result_json).map_err(|error| {
                    HubError::Upstream {
                        server: "hub".to_owned(),
                        tool: name.to_owned(),
                        message: format!("the forwarded result does not decode: {error}"),
                    }
                })
            }
            // NOT_FOUND is the Server saying "nothing routes this name for
            // you" — the same unroutable the proxy's own vocabulary has, so
            // the endpoint renders it as the protocol error it is.
            Err(status) if status.code() == tonic::Code::NotFound => Err(HubError::UnknownTool {
                name: name.to_owned(),
            }),
            // Everything else — the channel severed (revocation included),
            // the Server down, the caller gate refusing — is a failure the
            // model should read in words, not silence.
            Err(status) => Err(HubError::Upstream {
                server: "hub".to_owned(),
                tool: name.to_owned(),
                message: status.message().to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    // The link's whole behavior is one RPC on each side of a socket, so its
    // proof lives where both ends are real: `tests/worker_suite/hub_link.rs`
    // drives `fetch` and `call` against an actual `horde-server` — listing,
    // round trip, the unroutable NOT_FOUND mapping, the wrong-worker
    // refusal, and revocation severing the forwarded path.
}
