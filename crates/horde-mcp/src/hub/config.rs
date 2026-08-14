//! The domain shape of one configured MCP server.
//!
//! The wire config (`horde.v1.McpServerConfig`) is journaled through the
//! event log and validated at authoring time by the Server. This module is
//! the boundary where protobuf's optional-everything shape stops: a
//! [`HubServerConfig`] always has a name and a transport, so nothing
//! downstream re-checks either.

use std::collections::BTreeMap;

use horde_proto::v1::{McpServerConfig, mcp_server_config::Transport as WireTransport};

/// Where a configured server runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// At the Server, authenticated once, multiplexed to every Agent.
    Hub,
    /// Worker-side, one instance per Agent (ADR-0004's pressure valve).
    Local,
}

/// How to reach (or start) the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubTransport {
    /// A child process speaking MCP over stdio, spawned by whoever hosts it.
    Stdio {
        command: String,
        args: Vec<String>,
        /// Ordered map rather than the wire's hash map, so a config's
        /// environment is deterministic in logs and tests.
        env: BTreeMap<String, String>,
    },
    /// A running server reached over streamable HTTP.
    Http { url: String },
}

/// One configured server, as the hub machinery consumes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubServerConfig {
    /// The namespace prefix on every proxied tool. `[a-z0-9-]`, enforced at
    /// authoring time by the Server's validation.
    pub name: String,
    pub transport: HubTransport,
    pub placement: Placement,
}

/// Why a wire config could not become a domain one.
///
/// The Server validates at authoring time, so hitting this on a journaled
/// config means the log predates a rule or was written around the RPC — worth
/// a loud refusal either way, never a silently skipped server: an Agent
/// missing a toolset with no error anywhere is the failure shape ADR-0004's
/// audit spent a page on.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("the MCP server config has no name")]
    Unnamed,
    #[error("MCP server {name} names no transport")]
    NoTransport { name: String },
    #[error("MCP server {name} has a stdio transport with no command")]
    NoCommand { name: String },
    #[error("MCP server {name} has an http transport with no url")]
    NoUrl { name: String },
}

impl TryFrom<&McpServerConfig> for HubServerConfig {
    type Error = ConfigError;

    fn try_from(wire: &McpServerConfig) -> Result<Self, ConfigError> {
        if wire.name.is_empty() {
            return Err(ConfigError::Unnamed);
        }
        let transport = match &wire.transport {
            None => {
                return Err(ConfigError::NoTransport {
                    name: wire.name.clone(),
                });
            }
            Some(WireTransport::Stdio(stdio)) => {
                if stdio.command.is_empty() {
                    return Err(ConfigError::NoCommand {
                        name: wire.name.clone(),
                    });
                }
                HubTransport::Stdio {
                    command: stdio.command.clone(),
                    args: stdio.args.clone(),
                    env: stdio
                        .env
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                }
            }
            Some(WireTransport::Http(http)) => {
                if http.url.is_empty() {
                    return Err(ConfigError::NoUrl {
                        name: wire.name.clone(),
                    });
                }
                HubTransport::Http {
                    url: http.url.clone(),
                }
            }
        };

        Ok(Self {
            name: wire.name.clone(),
            transport,
            placement: if wire.local {
                Placement::Local
            } else {
                Placement::Hub
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        McpHttpTransport, McpServerConfig, McpStdioTransport, mcp_server_config::Transport,
    };

    use super::{ConfigError, HubServerConfig, HubTransport, Placement};

    fn wire_stdio(name: &str, local: bool) -> McpServerConfig {
        McpServerConfig {
            name: name.to_owned(),
            transport: Some(Transport::Stdio(McpStdioTransport {
                command: "linear-mcp".to_owned(),
                args: vec!["--stdio".to_owned()],
                env: [("LINEAR_TOKEN".to_owned(), "tok".to_owned())].into(),
            })),
            local,
            allowed_agent_types: Vec::new(),
            etag: "3".to_owned(),
        }
    }

    #[test]
    fn a_stdio_config_converts_whole() {
        let config = HubServerConfig::try_from(&wire_stdio("linear", false))
            .unwrap_or_else(|error| panic!("must convert: {error}"));

        assert_eq!(config.name, "linear");
        assert_eq!(config.placement, Placement::Hub);
        let HubTransport::Stdio { command, args, env } = &config.transport else {
            panic!("expected stdio, got {:?}", config.transport);
        };
        assert_eq!(command, "linear-mcp");
        assert_eq!(args, &["--stdio".to_owned()]);
        assert_eq!(env.get("LINEAR_TOKEN").map(String::as_str), Some("tok"));
    }

    #[test]
    fn the_local_flag_selects_worker_side_placement() {
        let config = HubServerConfig::try_from(&wire_stdio("browser", true))
            .unwrap_or_else(|error| panic!("must convert: {error}"));
        assert_eq!(config.placement, Placement::Local);
    }

    #[test]
    fn an_http_config_converts_and_an_empty_url_is_refused() {
        let mut wire = wire_stdio("grafana", false);
        wire.transport = Some(Transport::Http(McpHttpTransport {
            url: "http://127.0.0.1:9931/mcp".to_owned(),
        }));
        let config = HubServerConfig::try_from(&wire)
            .unwrap_or_else(|error| panic!("must convert: {error}"));
        assert_eq!(
            config.transport,
            HubTransport::Http {
                url: "http://127.0.0.1:9931/mcp".to_owned()
            }
        );

        wire.transport = Some(Transport::Http(McpHttpTransport::default()));
        assert_eq!(
            HubServerConfig::try_from(&wire),
            Err(ConfigError::NoUrl {
                name: "grafana".to_owned()
            })
        );
    }

    #[test]
    fn the_malformed_shapes_are_refused_loudly_not_skipped() {
        assert_eq!(
            HubServerConfig::try_from(&McpServerConfig::default()),
            Err(ConfigError::Unnamed)
        );

        let mut no_transport = wire_stdio("linear", false);
        no_transport.transport = None;
        assert_eq!(
            HubServerConfig::try_from(&no_transport),
            Err(ConfigError::NoTransport {
                name: "linear".to_owned()
            })
        );

        let mut no_command = wire_stdio("linear", false);
        no_command.transport = Some(Transport::Stdio(McpStdioTransport::default()));
        assert_eq!(
            HubServerConfig::try_from(&no_command),
            Err(ConfigError::NoCommand {
                name: "linear".to_owned()
            })
        );
    }

    #[test]
    fn every_refusal_explains_itself_distinctly() {
        let cases = [
            (
                ConfigError::Unnamed,
                "the MCP server config has no name".to_owned(),
            ),
            (
                ConfigError::NoTransport {
                    name: "a".to_owned(),
                },
                "MCP server a names no transport".to_owned(),
            ),
            (
                ConfigError::NoCommand {
                    name: "a".to_owned(),
                },
                "MCP server a has a stdio transport with no command".to_owned(),
            ),
            (
                ConfigError::NoUrl {
                    name: "a".to_owned(),
                },
                "MCP server a has an http transport with no url".to_owned(),
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }
    }
}
