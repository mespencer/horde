//! The spike's MCP server: three trivial tools on an rmcp
//! [`StreamableHttpService`](rmcp::transport::streamable_http_server::tower::StreamableHttpService).
//!
//! `whoami` is the load-bearing one. It answers from the HTTP request parts
//! rmcp injects into the request context, so a successful call proves the
//! per-agent identity headers reached the *handler* — not merely the socket,
//! which the tee already covers.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use rmcp::{
    handler::server::{
        tool::ToolRouter,
        wrapper::{Json, Parameters},
    },
    model::{ProtocolVersion, ServerCapabilities, ServerInfo},
    service::RequestContext,
    tool, tool_handler, tool_router, RoleServer, ServerHandler,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct EchoParams {
    /// Text to echo back verbatim.
    pub message: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AddParams {
    /// Left addend.
    pub left: i64,
    /// Right addend.
    pub right: i64,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct AddResult {
    pub sum: i64,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct WhoAmI {
    /// Protocol version rmcp associates with this request — from per-request
    /// `_meta` on the modern path, from the legacy handshake otherwise.
    pub protocol_version: Option<String>,
    /// Client identity, where the era makes one available.
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    /// Whether `http::request::Parts` reached the handler at all.
    pub http_parts_visible: bool,
    /// Every inbound HTTP header, lowercased, as the handler saw it.
    pub headers: Vec<(String, String)>,
}

#[derive(Clone)]
pub struct SpikeServer {
    tool_router: ToolRouter<Self>,
}

impl Default for SpikeServer {
    fn default() -> Self {
        Self::new()
    }
}

#[tool_router]
impl SpikeServer {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    #[tool(name = "echo", description = "Echo the supplied message back verbatim.")]
    fn echo(&self, Parameters(EchoParams { message }): Parameters<EchoParams>) -> String {
        message
    }

    #[tool(name = "add", description = "Add two integers.")]
    fn add(&self, Parameters(AddParams { left, right }): Parameters<AddParams>) -> Json<AddResult> {
        Json(AddResult {
            sum: left.saturating_add(right),
        })
    }

    #[tool(
        name = "whoami",
        description = "Report the protocol version and HTTP headers this server observed for the calling agent."
    )]
    fn whoami(&self, context: RequestContext<RoleServer>) -> Json<WhoAmI> {
        let parts = context.extensions.get::<http::request::Parts>();
        let headers = parts
            .map(|parts| {
                parts
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_owned(),
                            String::from_utf8_lossy(value.as_bytes()).into_owned(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let client = context.client_info();
        Json(WhoAmI {
            protocol_version: context.protocol_version().map(|v| v.to_string()),
            client_name: client.as_ref().map(|c| c.name.clone()),
            client_version: client.as_ref().map(|c| c.version.clone()),
            http_parts_visible: parts.is_some(),
            headers,
        })
    }
}

#[tool_handler]
impl ServerHandler for SpikeServer {
    fn get_info(&self) -> ServerInfo {
        // `ServerInfo` and `Implementation` are `#[non_exhaustive]`, so they
        // have to be built by mutation rather than a struct expression.
        let mut info = ServerInfo::default();
        info.protocol_version = ProtocolVersion::LATEST;
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info.name = "horde-rmcp-dual-era-spike".to_owned();
        info.server_info.version = env!("CARGO_PKG_VERSION").to_owned();
        info.instructions = Some(
            "Dual-era wire spike. Tools: echo, add, whoami. Call whoami to see the \
             identity headers the server observed."
                .to_owned(),
        );
        info
    }
}

/// A tower layer that records, for every inbound HTTP request, the method,
/// path, and full header set the rmcp service is about to be handed.
///
/// The tee proves headers reached the socket; this proves they survived into
/// the service — including on `initialize` and the GET/SSE stream, neither of
/// which routes through a tool where `whoami` could observe them.
#[derive(Clone)]
pub struct ObserveHeaders<S> {
    inner: S,
}

impl<S> ObserveHeaders<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, B> tower_service::Service<http::Request<B>> for ObserveHeaders<S>
where
    S: tower_service::Service<http::Request<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let mut rendered = format!(
            "SERVER-SAW {} {}",
            request.method(),
            request.uri().path_and_query().map_or("/", |p| p.as_str())
        );
        for (name, value) in request.headers() {
            rendered.push_str(&format!(
                "\n  {}: {}",
                name.as_str(),
                String::from_utf8_lossy(value.as_bytes())
            ));
        }
        eprintln!("{rendered}");

        // Standard tower clone-and-swap: the clone that was polled to readiness
        // is the one that must serve the request.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move { inner.call(request).await })
    }
}
