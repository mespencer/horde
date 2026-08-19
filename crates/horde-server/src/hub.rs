//! The MCP Hub's Server half: connected upstreams and the routing of
//! worker-forwarded calls (ADR-0004, 2026-08-19 amendment).
//!
//! Hub-placed user servers run *here*, authenticated exactly once, and their
//! tools reach each Agent through the Agent's endpoint on its Worker. The
//! Worker forwards those calls over its own authenticated mTLS channel
//! (`WorkerService.ListHubTools` / `CallHubTool`), with the Agent's identity
//! riding the request — so this module answers two questions per call, in
//! order:
//!
//! 1. **May this caller speak for this Agent?** The Agent must exist in the
//!    named Session and be *placed on the calling Worker* — the same
//!    log-derived placement every command routes by. A Worker forwarding for
//!    an Agent it does not hold is refused, whatever it asks for.
//! 2. **May this Agent see this tool?** Visibility is re-resolved here from
//!    the journaled catalog and the Agent's type, never trusted from the
//!    request — the spawn-time allowlist told the Worker what to *list*; this
//!    is what makes invisible tools unroutable too, and it fails closed.
//!
//! The permission chain is deliberately *not* here: it gates every proxied
//! call at the Agent's endpoint (DESIGN §10), where the caller's identity and
//! the Worker's journal live. See `horde_mcp::hub`'s module docs.

use std::collections::BTreeMap;

use horde_mcp::hub::{HubError, HubServerConfig, ToolProxy, Upstream, link};
use horde_proto::v1::McpServerConfig;

use crate::error::ServerError;
use crate::mcp::{MCP_CATALOG_STREAM, McpServerCatalog, visible_to};
use crate::service::shared::ServerCore;
use crate::state::SessionState;

/// The hub's connected upstreams, keyed by server name.
///
/// Connections are made on demand and reused — "authenticated exactly once"
/// is the ADR's whole point — and keyed by the config's etag, so a catalog
/// change (new token, new command) reconnects with the new config instead of
/// serving the stale session forever. A deleted server simply stops being
/// asked for; its connection drops with the entry on the next change.
#[derive(Debug, Default)]
pub struct HubUpstreams {
    connected: tokio::sync::Mutex<BTreeMap<String, Connected>>,
}

/// One live upstream and the config version it was connected under.
#[derive(Debug, Clone)]
struct Connected {
    etag: String,
    upstream: Upstream,
}

impl HubUpstreams {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The connected upstream for `config`, connecting on first use and
    /// reconnecting when the config's etag moved.
    ///
    /// The connect itself runs outside the lock — a slow child handshake must
    /// not stall every other server's calls — so two racing first calls may
    /// both connect; last writer wins and the loser's connection drops (a
    /// stdio child dies by handle with it).
    ///
    /// # Errors
    ///
    /// [`HubError`] for a config that cannot convert, spawn, or connect —
    /// the caller decides whether that is fatal (a listing) or a readable
    /// tool result (a call).
    pub async fn upstream_for(&self, config: &McpServerConfig) -> Result<Upstream, HubError> {
        if let Some(current) = self.lookup(&config.name, &config.etag).await {
            return Ok(current);
        }

        let domain = HubServerConfig::try_from(config).map_err(|error| HubError::Connect {
            server: config.name.clone(),
            message: error.to_string(),
        })?;
        let upstream = Upstream::connect(&domain).await?;

        let mut connected = self.connected.lock().await;
        connected.insert(
            config.name.clone(),
            Connected {
                etag: config.etag.clone(),
                upstream: upstream.clone(),
            },
        );
        Ok(upstream)
    }

    /// The held connection for `name`, if it was made under `etag`.
    async fn lookup(&self, name: &str, etag: &str) -> Option<Upstream> {
        let connected = self.connected.lock().await;
        connected
            .get(name)
            .filter(|held| held.etag == etag)
            .map(|held| held.upstream.clone())
    }
}

/// Connects every live hub-placed server in the journaled catalog — the
/// `--serve` warm-up, so a misconfigured upstream fails where the operator is
/// looking rather than at an agent's first call. Failures are reported, not
/// fatal: one dead user server must not keep the whole control plane down,
/// and the connection is retried at the next forwarded call anyway.
///
/// Returns one operator-readable line per hub-placed server.
///
/// # Errors
///
/// Only the log failing to yield the catalog; connect failures are lines.
pub async fn warm(core: &ServerCore) -> Result<Vec<String>, ServerError> {
    let catalog = catalog(core).await?;
    let mut lines = Vec::new();
    for config in catalog.live().filter(|config| !config.local) {
        let line = match core.hub.upstream_for(config).await {
            Ok(upstream) => format!(
                "hub-server {} connected ({} tools)",
                config.name,
                upstream.tools().len()
            ),
            Err(error) => format!("hub-server {} unavailable: {error}", config.name),
        };
        lines.push(line);
    }
    Ok(lines)
}

/// The hub tools one Agent may see, JSON-encoded and namespaced — the answer
/// to a Worker's `ListHubTools`.
///
/// # Errors
///
/// The caller gate's refusals; an upstream that cannot connect or list. The
/// last is deliberately fatal to the listing rather than skipped: an Agent
/// silently missing a promised toolset is the failure shape ADR-0004's audit
/// spent a page on, and the Worker fails the spawn loudly instead.
pub async fn list_hub_tools(
    core: &ServerCore,
    worker_id: &str,
    session_id: &str,
    agent_id: &str,
) -> Result<Vec<String>, ServerError> {
    let caller = admitted_caller(core, worker_id, session_id, agent_id).await?;
    let catalog = catalog(core).await?;

    let mut tools_json = Vec::new();
    for config in catalog
        .live()
        .filter(|config| !config.local && visible_to(config, &caller.agent_type))
    {
        let upstream = core
            .hub
            .upstream_for(config)
            .await
            .map_err(|error| ServerError::Internal(error.to_string()))?;
        // The proxy owns the namespacing discipline; reusing it here is what
        // keeps the listed names and the routed names the same function.
        for tool in ToolProxy::new().with_server(upstream).tools() {
            tools_json.push(
                link::tool_to_json(&tool)
                    .map_err(|error| ServerError::Internal(error.to_string()))?,
            );
        }
    }
    Ok(tools_json)
}

/// One Agent's forwarded call to one hub tool — the answer to a Worker's
/// `CallHubTool`, as the JSON-encoded MCP result.
///
/// An upstream that fails (unreachable, or refusing the call) is a *readable*
/// error result, not an RPC error: the model must see it to adjust. Only an
/// unroutable name — unknown, deleted, `local`-placed, or invisible to this
/// Agent, indistinguishably — refuses the RPC itself.
///
/// # Errors
///
/// The caller gate's refusals; [`ServerError::NoSuchHubTool`] for an
/// unroutable name; [`ServerError::Invalid`] for arguments that are not a
/// JSON object.
pub async fn call_hub_tool(
    core: &ServerCore,
    worker_id: &str,
    session_id: &str,
    agent_id: &str,
    tool_name: &str,
    arguments_json: &str,
) -> Result<String, ServerError> {
    let caller = admitted_caller(core, worker_id, session_id, agent_id).await?;
    let catalog = catalog(core).await?;
    let config = routed_config(&catalog, &caller.agent_type, tool_name)?;
    let arguments = link::arguments_from_json(arguments_json).map_err(ServerError::Invalid)?;

    let result = match core.hub.upstream_for(config).await {
        Ok(upstream) => {
            match ToolProxy::new()
                .with_server(upstream)
                .call(tool_name, arguments)
                .await
            {
                Ok(result) => result,
                // The upstream advertised no such tool: unroutable, same as
                // an unknown server — the menu the Worker holds is stale.
                Err(HubError::UnknownTool { name }) => {
                    return Err(ServerError::NoSuchHubTool { name });
                }
                Err(failed) => link::readable_failure(&failed),
            }
        }
        Err(unreachable) => link::readable_failure(&unreachable),
    };
    link::result_to_json(&result).map_err(|error| ServerError::Internal(error.to_string()))
}

/// The facts a forwarded call is judged against, once the caller gate passed.
#[derive(Debug)]
struct ForwardedCaller {
    agent_type: String,
}

/// The caller gate: the Agent exists in the Session and is placed on the
/// calling Worker.
async fn admitted_caller(
    core: &ServerCore,
    worker_id: &str,
    session_id: &str,
    agent_id: &str,
) -> Result<ForwardedCaller, ServerError> {
    let events = core.log.read_all(session_id).await?;
    if events.is_empty() {
        return Err(ServerError::NoSuchSession {
            session_id: session_id.to_owned(),
        });
    }
    let state = SessionState::project(session_id, &events)?;
    let agent = state
        .agents
        .get(agent_id)
        .ok_or_else(|| ServerError::NoSuchAgent {
            agent_id: agent_id.to_owned(),
        })?;
    if !holds_agent(agent.worker_id.as_deref(), worker_id) {
        return Err(ServerError::AgentNotOnWorker {
            agent_id: agent_id.to_owned(),
            worker_id: worker_id.to_owned(),
        });
    }
    Ok(ForwardedCaller {
        agent_type: agent.agent_type.clone(),
    })
}

/// Whether the worker forwarding a call is the one the log placed the agent
/// on. A named predicate because it is the whole attribution check: an
/// unplaced agent (`None`) matches no worker — fail closed — and an empty
/// worker id (a plaintext caller that named nobody) matches nothing either,
/// because placement never records an empty worker.
fn holds_agent(placed_on: Option<&str>, worker_id: &str) -> bool {
    !worker_id.is_empty() && placed_on == Some(worker_id)
}

/// The catalog config a namespaced tool routes to for this agent type.
///
/// Unknown server, deleted server, `local` placement, and invisibility all
/// land on the same refusal on purpose: invisible hub tools are unlisted
/// *and* unroutable, and a distinct message would reveal what the allowlist
/// hides.
///
/// # Errors
///
/// [`ServerError::NoSuchHubTool`].
fn routed_config<'a>(
    catalog: &'a McpServerCatalog,
    agent_type: &str,
    tool_name: &str,
) -> Result<&'a McpServerConfig, ServerError> {
    let no_such_tool = || ServerError::NoSuchHubTool {
        name: tool_name.to_owned(),
    };
    let (server, _tool) = horde_mcp::hub::split_namespaced(tool_name).ok_or_else(no_such_tool)?;
    let config = catalog.get(server).ok_or_else(no_such_tool)?;
    if config.local || !visible_to(config, agent_type) {
        return Err(no_such_tool());
    }
    Ok(config)
}

/// The journaled catalog, folded fresh — the same read every other consumer
/// makes, so a `horde mcp add` is routable on the next forwarded call with
/// no cache to invalidate.
async fn catalog(core: &ServerCore) -> Result<McpServerCatalog, ServerError> {
    McpServerCatalog::project(&core.log.read_all(MCP_CATALOG_STREAM).await?)
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{McpServerConfig, McpStdioTransport, mcp_server_config::Transport};

    use super::{holds_agent, routed_config};
    use crate::error::ServerError;
    use crate::mcp::McpServerCatalog;

    /// The attribution predicate's whole truth table: only the placed worker
    /// passes, and the two empty-ish shapes fail closed.
    #[test]
    fn only_the_worker_holding_the_agent_passes_the_gate() {
        assert!(holds_agent(Some("w1"), "w1"));
        assert!(!holds_agent(Some("w1"), "w2"));
        assert!(!holds_agent(None, "w1"), "an unplaced agent has no worker");
        assert!(
            !holds_agent(None, ""),
            "an anonymous caller must not match an unplaced agent"
        );
        assert!(
            !holds_agent(Some(""), ""),
            "empty never matches empty; placement never records an empty worker"
        );
    }

    fn catalog_with(configs: Vec<McpServerConfig>) -> McpServerCatalog {
        let mut catalog = McpServerCatalog::default();
        for (index, config) in configs.into_iter().enumerate() {
            catalog.fold_change(
                index as u64 + 1,
                horde_proto::v1::McpServerConfigChanged {
                    config: Some(config),
                    deleted: false,
                },
            );
        }
        catalog
    }

    fn server(name: &str, local: bool, allowed: &[&str]) -> McpServerConfig {
        McpServerConfig {
            name: name.to_owned(),
            transport: Some(Transport::Stdio(McpStdioTransport {
                command: "stub".to_owned(),
                ..McpStdioTransport::default()
            })),
            local,
            allowed_agent_types: allowed.iter().map(|s| (*s).to_owned()).collect(),
            etag: String::new(),
        }
    }

    /// Routing's refusal table: every unroutable shape — malformed name,
    /// unknown server, `local` placement, invisible to the caller's type —
    /// lands on the *same* refusal, and only a visible hub server routes.
    #[test]
    fn every_unroutable_shape_is_the_same_not_found() {
        let catalog = catalog_with(vec![
            server("linear", false, &[]),
            server("browser", true, &[]),
            server("payments", false, &["reviewer"]),
        ]);

        let routed = routed_config(&catalog, "", "linear__echo")
            .unwrap_or_else(|error| panic!("a visible hub server routes: {error}"));
        assert_eq!(routed.name, "linear");
        let scoped = routed_config(&catalog, "reviewer", "payments__refund")
            .unwrap_or_else(|error| panic!("the allowed type routes: {error}"));
        assert_eq!(scoped.name, "payments");

        for (agent_type, tool) in [
            ("", "bare-name"),              // no namespace at all
            ("", "grafana__query"),         // no such server
            ("", "browser__echo"),          // local-placed: not the hub's to route
            ("", "payments__refund"),       // scoped server, untyped agent
            ("intern", "payments__refund"), // scoped server, wrong type
        ] {
            let refused = routed_config(&catalog, agent_type, tool).expect_err("must not route");
            assert!(
                matches!(&refused, ServerError::NoSuchHubTool { name } if name == tool),
                "{tool} for {agent_type:?} must be the uniform refusal, got {refused:?}"
            );
        }
    }

    /// A deleted server stops routing even though the log still records it.
    #[test]
    fn a_deleted_server_stops_routing() {
        let mut catalog = catalog_with(vec![server("linear", false, &[])]);
        catalog.fold_change(
            9,
            horde_proto::v1::McpServerConfigChanged {
                config: Some(server("linear", false, &[])),
                deleted: true,
            },
        );
        assert!(matches!(
            routed_config(&catalog, "", "linear__echo"),
            Err(ServerError::NoSuchHubTool { .. })
        ));
    }

    /// The caller gate, over a projected session: the placed worker passes,
    /// everyone else's refusal names what failed — wrong worker, unknown
    /// agent, unknown session.
    #[tokio::test]
    async fn the_caller_gate_admits_only_the_placed_worker() {
        let core = crate::service::ServerCore::new(
            crate::log::EventLog::in_memory().expect("an in-memory log"),
        );
        crate::forward::testfix::placed_expose(&core.log, "s1", "agent-1", "w1").await;

        let admitted = super::admitted_caller(&core, "w1", "s1", "agent-1")
            .await
            .unwrap_or_else(|error| panic!("the placed worker passes: {error}"));
        assert_eq!(admitted.agent_type, "");

        let wrong_worker = super::admitted_caller(&core, "w2", "s1", "agent-1")
            .await
            .expect_err("another worker must not speak for this agent");
        assert!(matches!(wrong_worker, ServerError::AgentNotOnWorker { .. }));

        let unknown_agent = super::admitted_caller(&core, "w1", "s1", "agent-9")
            .await
            .expect_err("an unknown agent is refused");
        assert!(matches!(unknown_agent, ServerError::NoSuchAgent { .. }));

        let unknown_session = super::admitted_caller(&core, "w1", "s9", "agent-1")
            .await
            .expect_err("an unknown session is refused");
        assert!(matches!(unknown_session, ServerError::NoSuchSession { .. }));
    }

    /// An empty catalog warms to no lines and no failures — the fresh-server
    /// shape `--serve` hits on every start before any `horde mcp add`.
    #[tokio::test]
    async fn warming_an_empty_catalog_is_a_no_op() {
        let core = crate::service::ServerCore::new(
            crate::log::EventLog::in_memory().expect("an in-memory log"),
        );
        let lines = super::warm(&core)
            .await
            .unwrap_or_else(|error| panic!("an empty catalog warms: {error}"));
        assert!(lines.is_empty());
    }

    // A catalog whose server cannot spawn warms to a failure *line*, not a
    // failed startup. That case seeds the journaled catalog, which the
    // testkit owns a helper for — and the testkit's `horde_server` is the
    // plain build, which a lib test's cfg(test) `crate::` types cannot meet
    // — so it lives in `tests/server_suite/mcp_catalog.rs` instead.
}
