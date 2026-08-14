//! The hub's own serving surface at the Server.
//!
//! One MCP server exposing every hub-placed user server's tools, namespaced,
//! to the Workers that dial it on behalf of their Agents. It rides the same
//! machinery as the agent endpoint — the dual-era `StreamableHttpService`
//! with the spike's config corrections, behind [`crate::server::RequireIdentity`]
//! — so serving it is [`crate::server::serve`] with this handler.
//!
//! No permission chain here, deliberately: the chain gates proxied calls at
//! the *agent's* endpoint, where the caller's identity and the Worker's
//! journal live. See the module docs on [`crate::hub`].

use std::sync::Arc;

use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ErrorData, ListToolsResult,
    PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler};

use super::proxy::ToolProxy;
use super::upstream::HubError;

/// The hub, as an MCP server.
#[derive(Clone)]
pub struct HubEndpoint {
    proxy: Arc<ToolProxy>,
}

impl std::fmt::Debug for HubEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubEndpoint")
            .field("tools", &self.proxy.tools().len())
            .finish_non_exhaustive()
    }
}

impl HubEndpoint {
    #[must_use]
    pub fn new(proxy: Arc<ToolProxy>) -> Self {
        Self { proxy }
    }
}

/// The complete `tools/list` answer for `tools`, with the cache hints the
/// caller's era wants.
///
/// Mirrors what rmcp's `#[tool_handler]` generates for router-backed
/// handlers, so a proxy-backed listing is indistinguishable on the wire from
/// a macro-backed one.
#[must_use]
pub(crate) fn listing(
    tools: Vec<rmcp::model::Tool>,
    context: &RequestContext<RoleServer>,
) -> ListToolsResult {
    let supports_cache_hints = context
        .protocol_version()
        .is_some_and(|version| version >= ProtocolVersion::V_2026_07_28);
    ListToolsResult {
        result_type: Some(rmcp::model::ResultType::COMPLETE),
        tools,
        meta: None,
        next_cursor: None,
        ttl_ms: supports_cache_hints.then_some(0),
        cache_scope: supports_cache_hints.then_some(rmcp::model::CacheScope::Public),
    }
}

/// Renders a proxied-call failure the way MCP distinguishes failures.
///
/// An *unroutable* name is a protocol error (`method_not_found`) — the request
/// never matched a tool. An upstream that failed is a **tool result** carrying
/// `is_error`, because the model is the one who must read it and adjust;
/// protocol errors reach it as an opaque "internal error", which teaches it
/// nothing. rmcp's own `call_tool` docs draw exactly this line.
#[must_use]
pub(crate) fn render_failure(error: HubError) -> Result<CallToolResult, ErrorData> {
    match error {
        HubError::UnknownTool { name } => Err(ErrorData::new(
            rmcp::model::ErrorCode::METHOD_NOT_FOUND,
            format!("no tool named {name}"),
            None,
        )),
        other => Ok(CallToolResult::error(vec![
            rmcp::model::ContentBlock::text(other.to_string()),
        ])),
    }
}

impl ServerHandler for HubEndpoint {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        // The legacy revision, deliberately, for the same measured reason as
        // the agent endpoint (see `crate::endpoint`): the constant will move.
        info.protocol_version = ProtocolVersion::LATEST;
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info.name = "horde-hub".to_owned();
        info.server_info.version = env!("CARGO_PKG_VERSION").to_owned();
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(listing(self.proxy.tools(), &context))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        match self.proxy.call(&request.name, request.arguments).await {
            Ok(result) => Ok(CallToolResponse::Complete(result)),
            Err(error) => render_failure(error).map(CallToolResponse::Complete),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HubEndpoint, render_failure};
    use crate::hub::proxy::ToolProxy;
    use crate::hub::upstream::HubError;
    use rmcp::ServerHandler;

    fn endpoint() -> HubEndpoint {
        HubEndpoint::new(std::sync::Arc::new(ToolProxy::new()))
    }

    #[test]
    fn the_hub_advertises_tools_on_the_legacy_revision() {
        let info = endpoint().get_info();
        assert_eq!(info.protocol_version.to_string(), "2025-11-25");
        assert!(info.capabilities.tools.is_some());
        assert_eq!(info.server_info.name, "horde-hub");
    }

    #[test]
    fn debug_names_the_hub_rather_than_hiding_it() {
        let rendered = format!("{:?}", endpoint());
        assert!(rendered.contains("HubEndpoint"), "{rendered}");
    }

    /// The two failure planes must not swap: an unknown name is a protocol
    /// error (the caller's menu was wrong), an upstream failure is a readable
    /// tool result (the model must see it to adjust).
    #[test]
    fn unknown_names_are_protocol_errors_and_upstream_failures_are_readable() {
        let unknown = render_failure(HubError::UnknownTool {
            name: "linear__x".to_owned(),
        });
        let error = unknown.expect_err("unroutable is a protocol error");
        assert_eq!(error.code, rmcp::model::ErrorCode::METHOD_NOT_FOUND);
        assert!(error.message.contains("linear__x"), "{}", error.message);

        let failed = render_failure(HubError::Upstream {
            server: "linear".to_owned(),
            tool: "create".to_owned(),
            message: "rate limited".to_owned(),
        })
        .expect("an upstream failure is a result the model reads");
        assert_eq!(failed.is_error, Some(true));
        let text = format!("{:?}", failed.content);
        assert!(text.contains("rate limited"), "{text}");
    }
}
