//! One live connection to one MCP server, as a client.
//!
//! The hub is deliberately transport-omnivorous on this side (DESIGN §9): the
//! integration long tail lives in deployed user servers, so a stdio child and
//! a streamable-HTTP peer are both first-class. The same machinery serves
//! three hosts: the Server connecting to hub-placed configs, the Worker
//! spawning `local` configs per Agent, and the Agent's endpoint dialing the
//! hub itself over the authenticated channel.
//!
//! # Lifetime is ownership
//!
//! An [`Upstream`] holds the rmcp `RunningService`; dropping the last clone
//! cancels the client task, which closes the transport — and for a stdio
//! child, rmcp's `TokioChildProcess` kills the child *by handle* on drop.
//! Never by name or pattern, per the workspace rule.

use std::sync::Arc;

use rmcp::RoleClient;
use rmcp::model::{CallToolRequestParams, CallToolResult, Tool};
use rmcp::service::RunningService;

use super::config::{HubServerConfig, HubTransport};

/// Why the hub could not reach, list, or call an upstream.
#[derive(Debug, thiserror::Error)]
pub enum HubError {
    /// The server's child process could not be spawned.
    #[error("could not spawn MCP server {server}: {message}")]
    Spawn { server: String, message: String },

    /// The MCP handshake or the initial tool listing failed.
    #[error("could not connect to MCP server {server}: {message}")]
    Connect { server: String, message: String },

    /// No upstream serves a tool by this name.
    ///
    /// Distinct from an upstream failure: the caller asked for something that
    /// is not on the menu, and the fix is the caller's.
    #[error("no proxied tool named {name}")]
    UnknownTool { name: String },

    /// The upstream refused or failed the call itself.
    #[error("MCP server {server} failed {tool}: {message}")]
    Upstream {
        server: String,
        tool: String,
        message: String,
    },
}

/// One connected upstream MCP server and its advertised tools.
///
/// Cheap to clone; all clones share the one connection. The tool list is a
/// snapshot taken at connect time — re-listing on change notifications is
/// deliberately not built yet, and a snapshot that can only be refreshed by
/// reconnecting is honest about that.
#[derive(Clone)]
pub struct Upstream {
    server: String,
    service: Arc<RunningService<RoleClient, ()>>,
    tools: Arc<Vec<Tool>>,
}

impl std::fmt::Debug for Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Upstream")
            .field("server", &self.server)
            .field("tools", &self.tools.len())
            .finish_non_exhaustive()
    }
}

impl Upstream {
    /// Connects per the config: spawns a stdio child, or dials an HTTP peer.
    ///
    /// # Errors
    ///
    /// [`HubError::Spawn`] when a child could not start; [`HubError::Connect`]
    /// when the handshake or the initial `tools/list` failed.
    pub async fn connect(config: &HubServerConfig) -> Result<Self, HubError> {
        match &config.transport {
            HubTransport::Stdio { command, args, env } => {
                let mut child = tokio::process::Command::new(command);
                child.args(args);
                for (key, value) in env {
                    child.env(key, value);
                }
                let transport = rmcp::transport::TokioChildProcess::new(child).map_err(|e| {
                    HubError::Spawn {
                        server: config.name.clone(),
                        message: e.to_string(),
                    }
                })?;
                Self::over(&config.name, transport).await
            }
            HubTransport::Http { url } => Self::dial(&config.name, url, None, &[]).await,
        }
    }

    /// Dials a streamable-HTTP MCP server, riding `headers` (and a bearer
    /// `token`, if given) on every request — the per-agent identity pair when
    /// the peer is the hub, nothing when it is a user server.
    ///
    /// # Errors
    ///
    /// [`HubError::Connect`] for an unusable header value, a failed handshake,
    /// or a failed initial listing.
    pub async fn dial(
        server: &str,
        url: &str,
        token: Option<&str>,
        headers: &[(&str, &str)],
    ) -> Result<Self, HubError> {
        use rmcp::transport::streamable_http_client::{
            StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
        };

        let mut config = StreamableHttpClientTransportConfig::with_uri(url.to_owned());
        config.auth_header = token.map(str::to_owned);
        for (name, value) in headers {
            let name = name
                .parse::<http::HeaderName>()
                .map_err(|e| connect_error(server, &e))?;
            let value = value
                .parse::<http::HeaderValue>()
                .map_err(|e| connect_error(server, &e))?;
            config.custom_headers.insert(name, value);
        }

        let transport =
            StreamableHttpClientTransport::with_client(reqwest::Client::default(), config);
        Self::over(server, transport).await
    }

    /// Wraps an already-built transport — the seam the duplex-stream tests
    /// drive a real rmcp server through without a process or a socket.
    ///
    /// # Errors
    ///
    /// [`HubError::Connect`] when the handshake or initial listing failed.
    pub async fn over<T, E, A>(server: &str, transport: T) -> Result<Self, HubError>
    where
        T: rmcp::transport::IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
    {
        use rmcp::ServiceExt;

        let service = ().serve(transport).await.map_err(|e| connect_error(server, &e))?;

        // Listed once, here, so a server that cannot even enumerate its tools
        // fails at connect — where the operator is looking — rather than at
        // an agent's first call.
        let tools = service
            .list_all_tools()
            .await
            .map_err(|e| connect_error(server, &e))?;

        Ok(Self {
            server: server.to_owned(),
            service: Arc::new(service),
            tools: Arc::new(tools),
        })
    }

    /// The server name this upstream was configured under.
    #[must_use]
    pub fn server(&self) -> &str {
        &self.server
    }

    /// The tools the server advertised at connect time.
    #[must_use]
    pub fn tools(&self) -> &[Tool] {
        &self.tools
    }

    /// Calls one tool by its *upstream* name (no namespace prefix).
    ///
    /// # Errors
    ///
    /// [`HubError::Upstream`] when the transport or the server failed the
    /// call. A tool-level failure the server *reported* (`is_error: true`)
    /// is not an error here — it is a result the agent should read.
    pub async fn call(
        &self,
        tool: &str,
        arguments: Option<rmcp::model::JsonObject>,
    ) -> Result<CallToolResult, HubError> {
        let mut params = CallToolRequestParams::new(tool.to_owned());
        params.arguments = arguments;

        self.service
            .call_tool(params)
            .await
            .map_err(|e| HubError::Upstream {
                server: self.server.clone(),
                tool: tool.to_owned(),
                message: e.to_string(),
            })
    }
}

/// A connect-stage failure, with the upstream's own words kept.
fn connect_error(server: &str, error: &dyn std::fmt::Display) -> HubError {
    HubError::Connect {
        server: server.to_owned(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::HubError;

    /// Each failure's message is what an operator debugs from, so each must
    /// name the server (and the tool, where there is one).
    #[test]
    fn every_failure_names_what_failed() {
        let cases = [
            (
                HubError::Spawn {
                    server: "linear".to_owned(),
                    message: "no such file".to_owned(),
                },
                "could not spawn MCP server linear: no such file",
            ),
            (
                HubError::Connect {
                    server: "linear".to_owned(),
                    message: "handshake refused".to_owned(),
                },
                "could not connect to MCP server linear: handshake refused",
            ),
            (
                HubError::UnknownTool {
                    name: "linear__create".to_owned(),
                },
                "no proxied tool named linear__create",
            ),
            (
                HubError::Upstream {
                    server: "linear".to_owned(),
                    tool: "create".to_owned(),
                    message: "boom".to_owned(),
                },
                "MCP server linear failed create: boom",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }
    }
}
