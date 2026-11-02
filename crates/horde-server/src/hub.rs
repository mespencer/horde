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
    let state =
        core.sessions
            .state(session_id)
            .await?
            .ok_or_else(|| ServerError::NoSuchSession {
                session_id: session_id.to_owned(),
            })?;
    let agent = state
        .agents
        .get(agent_id)
        .ok_or_else(|| ServerError::NoSuchAgent {
            agent_id: agent_id.to_owned(),
        })?;
    if !crate::trust::holds_agent(agent.worker_id.as_deref(), worker_id) {
        return Err(ServerError::AgentNotOnWorker {
            agent_id: agent_id.to_owned(),
            worker_id: worker_id.to_owned(),
        });
    }
    Ok(ForwardedCaller {
        agent_type: agent.agent_type.clone(),
    })
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

/// Log fixtures the hub suites share (this module's, the worker spine's, and
/// startup's): one journaled catalog entry, and the stub MCP server to point
/// it at — kept here so none of them restates the arrangement (the
/// duplication gate's rule applies to test code too). Integration tests use
/// `horde_server_testkit::seed_mcp_server` instead; a lib test cannot, since
/// its cfg(test) `crate::` types are not the testkit's plain-build ones.
#[cfg(test)]
pub(crate) mod testfix {
    use horde_proto::v1::McpServerConfig;
    use prost::Message;

    use crate::log::{EventLog, PendingEvent};

    pub(crate) async fn seed_catalog(log: &EventLog, config: McpServerConfig) {
        let name = config.name.clone();
        let kind = horde_proto::v1::event::Kind::McpServerConfigChanged(
            horde_proto::v1::McpServerConfigChanged {
                config: Some(config),
                deleted: false,
            },
        );
        let payload = horde_proto::v1::Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: format!("mcp:{name}"),
            kind: Some(kind),
        }
        .encode_to_vec();
        log.append(PendingEvent {
            session_id: crate::mcp::MCP_CATALOG_STREAM.to_owned(),
            origin_worker_id: String::new(),
            journal_sequence: None,
            idempotency_key: format!("mcp:{name}"),
            payload,
        })
        .await
        .expect("the catalog change appends");
    }

    /// The stub MCP server as a hub-placed catalog config: `name` is the
    /// namespace prefix, `identity` what its `whoami` tool answers, `allowed`
    /// the agent-type allowlist.
    pub(crate) async fn stub_config(
        name: &str,
        identity: &str,
        local: bool,
        allowed: &[&str],
    ) -> McpServerConfig {
        let binary = horde_server_testkit::process::product_binary("horde-mcp", "mcp_stub_server")
            .await
            .expect("the stub server binary exists");
        McpServerConfig {
            name: name.to_owned(),
            transport: Some(horde_proto::v1::mcp_server_config::Transport::Stdio(
                horde_proto::v1::McpStdioTransport {
                    command: binary.to_string_lossy().into_owned(),
                    args: Vec::new(),
                    env: [("HORDE_STUB_IDENTITY".to_owned(), identity.to_owned())].into(),
                },
            )),
            local,
            allowed_agent_types: allowed.iter().map(|s| (*s).to_owned()).collect(),
            etag: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{McpServerConfig, McpStdioTransport, mcp_server_config::Transport};

    use horde_mcp::hub::link;

    use super::routed_config;
    use crate::error::ServerError;
    use crate::mcp::McpServerCatalog;

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

    /// One core over an in-memory log with agent-1 placed on w1 — the
    /// arrangement every forwarded-surface test opens with.
    async fn placed_core() -> crate::service::ServerCore {
        let core = crate::service::ServerCore::new(
            crate::log::EventLog::in_memory().expect("an in-memory log"),
        );
        crate::forward::testfix::placed_expose(&core.log, "s1", "agent-1", "w1").await;
        core
    }

    /// The caller gate, over a projected session: the placed worker passes,
    /// everyone else's refusal names what failed — wrong worker, unknown
    /// agent, unknown session.
    #[tokio::test]
    async fn the_caller_gate_admits_only_the_placed_worker() {
        let core = placed_core().await;
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

    /// The connection cache's whole contract, behaviorally: connected once
    /// per config version — the same etag reuses the live session even if
    /// the config bytes changed underneath (the catalog's etag is the truth
    /// about versions), and a moved etag reconnects under the new config.
    /// The stub's `whoami` answers its spawn-time env, so which *process*
    /// answered is observable.
    #[tokio::test]
    async fn upstreams_connect_once_per_etag_and_reconnect_when_it_moves() {
        let hub = super::HubUpstreams::new();
        let mut first = super::testfix::stub_config("stub", "first", false, &[]).await;
        first.etag = "1".to_owned();

        let connected = hub
            .upstream_for(&first)
            .await
            .unwrap_or_else(|error| panic!("the stub connects: {error}"));
        let answered = connected
            .call("whoami", None)
            .await
            .unwrap_or_else(|error| panic!("whoami answers: {error}"));
        assert_eq!(link::first_text(&answered), Some("first"));

        // Same etag, different env: the cached connection must answer —
        // "authenticated exactly once" means no second child.
        let mut same_version = super::testfix::stub_config("stub", "second", false, &[]).await;
        same_version.etag = "1".to_owned();
        let cached = hub
            .upstream_for(&same_version)
            .await
            .unwrap_or_else(|error| panic!("the cache answers: {error}"));
        let answered = cached
            .call("whoami", None)
            .await
            .unwrap_or_else(|error| panic!("whoami answers: {error}"));
        assert_eq!(
            link::first_text(&answered),
            Some("first"),
            "an unchanged etag must reuse the live connection, never respawn"
        );

        // The etag moved: the change that produced it deserves a fresh
        // connection under the new config.
        let mut next_version = super::testfix::stub_config("stub", "second", false, &[]).await;
        next_version.etag = "2".to_owned();
        let reconnected = hub
            .upstream_for(&next_version)
            .await
            .unwrap_or_else(|error| panic!("the new version connects: {error}"));
        let answered = reconnected
            .call("whoami", None)
            .await
            .unwrap_or_else(|error| panic!("whoami answers: {error}"));
        assert_eq!(
            link::first_text(&answered),
            Some("second"),
            "a moved etag must reconnect under the new config"
        );
    }

    /// The forwarded surface against real upstream children, both verbs:
    /// the listing is exactly the visible hub server's namespaced tools —
    /// `local` placements and scoped servers filtered out — and a call
    /// round-trips to the upstream's own answer.
    #[tokio::test]
    async fn forwarded_listing_and_calls_route_real_upstreams_and_fail_closed() {
        let core = placed_core().await;
        for config in [
            super::testfix::stub_config("stub", "hub", false, &[]).await,
            super::testfix::stub_config("loc", "loc", true, &[]).await,
            super::testfix::stub_config("scoped", "scoped", false, &["reviewer"]).await,
        ] {
            super::testfix::seed_catalog(&core.log, config).await;
        }

        let listed = super::list_hub_tools(&core, "w1", "s1", "agent-1")
            .await
            .unwrap_or_else(|error| panic!("the listing answers: {error}"));
        let names: Vec<String> = listed
            .iter()
            .map(|json| {
                horde_mcp::hub::link::tool_from_json(json)
                    .unwrap_or_else(|error| panic!("a listed tool decodes: {error}"))
                    .name
                    .to_string()
            })
            .collect();
        assert!(
            names.contains(&"stub__echo".to_owned()),
            "the visible hub server's tools are listed, namespaced: {names:?}"
        );
        assert!(
            names.iter().all(|name| name.starts_with("stub__")),
            "local placements and scoped servers must not be listed: {names:?}"
        );

        let result_json = super::call_hub_tool(
            &core,
            "w1",
            "s1",
            "agent-1",
            "stub__echo",
            r#"{"message":"across the seam"}"#,
        )
        .await
        .unwrap_or_else(|error| panic!("the call answers: {error}"));
        let result = horde_mcp::hub::link::result_from_json(&result_json)
            .unwrap_or_else(|error| panic!("the result decodes: {error}"));
        assert_ne!(result.is_error, Some(true), "{result:?}");
        assert_eq!(link::first_text(&result), Some("echo:across the seam"));
    }
}
