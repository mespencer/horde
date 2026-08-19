//! The aggregation an endpoint serves proxied tools from.
//!
//! One Agent's endpoint carries tools from two kinds of place (DESIGN §5):
//! `local` servers this Worker spawned for this Agent, whose tools are
//! namespaced *here*, and the hub link — one upstream whose tools arrive
//! already namespaced by the hub — filtered to the server names this Agent
//! may see. The hub's own serving surface is the degenerate case: a proxy of
//! prefixed sources and no link.
//!
//! Routing and visibility are pure functions over the advertised tool lists,
//! because a misroute here runs one server's tool with another's arguments
//! and no error anywhere.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rmcp::model::{CallToolResult, JsonObject, Tool};

use super::link::HubLink;
use super::names::{namespaced, split_namespaced};
use super::upstream::{HubError, Upstream};

/// How one source's tools appear on the endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Naming {
    /// Prefix every tool with the upstream's server name — a directly
    /// connected server (`local`, or a hub-side source).
    Prefixed,
    /// Tools arrive already namespaced (the hub link); admit only those whose
    /// prefix is a server this Agent may see.
    Passthrough { visible: BTreeSet<String> },
}

/// One source and how its tools are named.
#[derive(Debug, Clone)]
struct Source {
    naming: Naming,
    body: SourceBody,
}

/// What actually answers a source's calls.
///
/// Directly connected servers are always [`Upstream`]s — the proxy's host
/// spawned or dialed them itself. The hub link is a [`HubLink`] because the
/// wire it rides is the host's business: the Worker forwards over its
/// authenticated channel, a test dials an upstream straight.
#[derive(Debug, Clone)]
enum SourceBody {
    Server(Upstream),
    Link(Arc<dyn HubLink>),
}

impl SourceBody {
    /// The name prefixed sources namespace under. The hub link's tools arrive
    /// pre-namespaced, so its own name never reaches a tool.
    fn server(&self) -> &str {
        match self {
            Self::Server(upstream) => upstream.server(),
            Self::Link(_) => "hub",
        }
    }

    fn tools(&self) -> &[Tool] {
        match self {
            Self::Server(upstream) => upstream.tools(),
            Self::Link(link) => link.tools(),
        }
    }

    async fn call(
        &self,
        name: &str,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, HubError> {
        match self {
            Self::Server(upstream) => upstream.call(name, arguments).await,
            Self::Link(link) => link.call(name, arguments).await,
        }
    }
}

/// The proxied half of an endpoint's toolset.
#[derive(Debug, Clone, Default)]
pub struct ToolProxy {
    sources: Vec<Source>,
}

impl ToolProxy {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a directly connected server; its tools appear as
    /// `{server}__{tool}`.
    #[must_use]
    pub fn with_server(mut self, upstream: Upstream) -> Self {
        self.sources.push(Source {
            naming: Naming::Prefixed,
            body: SourceBody::Server(upstream),
        });
        self
    }

    /// Adds the hub link: a source whose tools are already namespaced,
    /// admitted only for the `visible` server names.
    ///
    /// The filter is the Agent's resolved visibility (computed server-side
    /// from the catalog and the Agent's type); everything else the hub serves
    /// is invisible here — not listed, and refused if called by name.
    #[must_use]
    pub fn with_hub_link(
        mut self,
        link: impl HubLink + 'static,
        visible: impl IntoIterator<Item = String>,
    ) -> Self {
        self.sources.push(Source {
            naming: Naming::Passthrough {
                visible: visible.into_iter().collect(),
            },
            body: SourceBody::Link(Arc::new(link)),
        });
        self
    }

    /// Every tool this proxy serves, under its endpoint-facing name.
    #[must_use]
    pub fn tools(&self) -> Vec<Tool> {
        self.sources
            .iter()
            .flat_map(|source| {
                visible_tools(&source.naming, source.body.server(), source.body.tools())
            })
            .collect()
    }

    /// Whether `name` routes to some upstream here.
    #[must_use]
    pub fn serves(&self, name: &str) -> bool {
        self.resolve(name).is_some()
    }

    /// Calls the proxied tool `name` with `arguments`.
    ///
    /// # Errors
    ///
    /// [`HubError::UnknownTool`] when nothing here serves `name`;
    /// [`HubError::Upstream`] when the serving upstream failed the call.
    pub async fn call(
        &self,
        name: &str,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, HubError> {
        let (body, upstream_name) =
            self.resolve(name).ok_or_else(|| HubError::UnknownTool {
                name: name.to_owned(),
            })?;
        body.call(&upstream_name, arguments).await
    }

    /// The source serving `name`, and the name to call it by there.
    fn resolve(&self, name: &str) -> Option<(&SourceBody, String)> {
        self.sources.iter().find_map(|source| {
            route(
                &source.naming,
                source.body.server(),
                source.body.tools(),
                name,
            )
            .map(|upstream_name| (&source.body, upstream_name))
        })
    }
}

/// One endpoint's proxies, per Agent.
///
/// A Session's endpoint serves many Agents on one port (identity rides every
/// request), but the proxied half of the toolset is per-Agent: `local`
/// servers are spawned one instance per Agent, and hub visibility is resolved
/// per Agent's type. This registry is the mapping — interior-mutable for the
/// same reason [`crate::AgentRegistry`] is: agents are spawned and archived
/// while the endpoint is already serving.
///
/// An unregistered agent gets the empty proxy: exactly the verbs, no proxied
/// tools — fail closed, never another agent's toolset.
#[derive(Debug, Default)]
pub struct AgentProxies {
    proxies: std::sync::RwLock<BTreeMap<String, Arc<ToolProxy>>>,
}

impl AgentProxies {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one Agent's proxied toolset, replacing any previous one —
    /// a revival's fresh `local` children must not serve beside the old.
    pub fn insert(&self, agent_id: impl Into<String>, proxy: Arc<ToolProxy>) {
        self.write().insert(agent_id.into(), proxy);
    }

    /// Forgets an Agent. Dropping the last clone of its proxy drops its
    /// `local` upstreams, which kills their child processes by handle.
    pub fn remove(&self, agent_id: &str) {
        self.write().remove(agent_id);
    }

    /// Forgets every Agent — the shutdown sweep, so no `local` child
    /// outlives the manager that spawned it.
    pub fn clear(&self) {
        self.write().clear();
    }

    /// The proxy serving `agent_id`, or the empty one.
    #[must_use]
    pub fn for_agent(&self, agent_id: &str) -> Arc<ToolProxy> {
        self.read().get(agent_id).cloned().unwrap_or_default()
    }

    /// Every registered proxy, for the identity-free lookups (`get_tool`)
    /// rmcp makes outside any request. Advisory only: authorization happens
    /// per call against [`Self::for_agent`].
    #[must_use]
    pub fn all(&self) -> Vec<Arc<ToolProxy>> {
        self.read().values().cloned().collect()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, Arc<ToolProxy>>> {
        // A poisoned lock means a panic mid-insert; the map itself is still
        // coherent (single-key operations), and refusing every later request
        // would turn one panic into a dead endpoint.
        self.proxies
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, Arc<ToolProxy>>> {
        self.proxies
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The endpoint-facing tools one source contributes.
fn visible_tools(naming: &Naming, server: &str, tools: &[Tool]) -> Vec<Tool> {
    match naming {
        Naming::Prefixed => tools
            .iter()
            .map(|tool| {
                let mut named = tool.clone();
                named.name = namespaced(server, &tool.name).into();
                named
            })
            .collect(),
        Naming::Passthrough { visible } => tools
            .iter()
            .filter(|tool| passes_filter(visible, &tool.name))
            .cloned()
            .collect(),
    }
}

/// The upstream-facing name `name` routes to through one source, if it does.
///
/// Routing consults the advertised list rather than trusting the prefix
/// alone: a name shaped like `linear__x` for a linear that never advertised
/// `x` must be *unknown*, not forwarded — the upstream's answer to an
/// invented tool is its own error in its own vocabulary, and surfacing those
/// as if the endpoint served the tool teaches agents wrong menus.
fn route(naming: &Naming, server: &str, tools: &[Tool], name: &str) -> Option<String> {
    match naming {
        Naming::Prefixed => {
            let (prefix, tool) = split_namespaced(name)?;
            if prefix != server {
                return None;
            }
            tools
                .iter()
                .any(|advertised| advertised.name == tool)
                .then(|| tool.to_owned())
        }
        Naming::Passthrough { visible } => {
            if !passes_filter(visible, name) {
                return None;
            }
            tools
                .iter()
                .any(|advertised| advertised.name == name)
                .then(|| name.to_owned())
        }
    }
}

/// Whether a pre-namespaced tool name belongs to a server this proxy admits.
///
/// A named predicate because it is the whole allowlist: `visible` came from
/// the Server's per-agent-type resolution, and a tool with no recognizable
/// prefix fails closed — the hub only serves namespaced tools, so an
/// unprefixed name from it is malformed, never "unrestricted".
fn passes_filter(visible: &BTreeSet<String>, name: &str) -> bool {
    match split_namespaced(name) {
        Some((server, _tool)) => visible.contains(server),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use rmcp::model::Tool;

    use super::{Naming, passes_filter, route, visible_tools};

    fn tool(name: &str) -> Tool {
        let mut tool = Tool::default();
        tool.name = name.to_owned().into();
        tool.input_schema = Arc::new(rmcp::model::JsonObject::default());
        tool
    }

    fn visible(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn prefixed_sources_rename_and_keep_everything_else() {
        let mut described = tool("create_issue");
        described.description = Some("makes an issue".into());

        let listed = visible_tools(&Naming::Prefixed, "linear", &[described]);
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "linear__create_issue");
        assert_eq!(
            listed[0].description.as_deref(),
            Some("makes an issue"),
            "the rename must not strip the schema the model reads"
        );
    }

    #[test]
    fn passthrough_sources_filter_by_the_agents_visibility() {
        let naming = Naming::Passthrough {
            visible: visible(&["linear"]),
        };
        let tools = [tool("linear__create"), tool("payments__refund")];

        let listed = visible_tools(&naming, "hub", &tools);
        assert_eq!(
            listed.iter().map(|t| t.name.as_ref()).collect::<Vec<_>>(),
            vec!["linear__create"],
            "a server outside the agent's visibility must not be listed"
        );
    }

    #[test]
    fn prefixed_routing_strips_the_prefix_and_demands_both_halves_match() {
        let naming = Naming::Prefixed;
        let tools = [tool("create"), tool("close")];

        assert_eq!(
            route(&naming, "linear", &tools, "linear__create"),
            Some("create".to_owned())
        );
        assert_eq!(
            route(&naming, "linear", &tools, "grafana__create"),
            None,
            "another server's prefix must not route here"
        );
        assert_eq!(
            route(&naming, "linear", &tools, "linear__delete"),
            None,
            "a tool the server never advertised must be unknown, not forwarded"
        );
        assert_eq!(
            route(&naming, "linear", &tools, "create"),
            None,
            "an unprefixed name is the endpoint's own namespace"
        );
    }

    #[test]
    fn passthrough_routing_keeps_the_name_and_honours_the_filter() {
        let naming = Naming::Passthrough {
            visible: visible(&["linear"]),
        };
        let tools = [tool("linear__create"), tool("payments__refund")];

        assert_eq!(
            route(&naming, "hub", &tools, "linear__create"),
            Some("linear__create".to_owned()),
            "the hub already namespaced it; forward as-is"
        );
        assert_eq!(
            route(&naming, "hub", &tools, "payments__refund"),
            None,
            "advertised by the hub but outside this agent's visibility"
        );
        assert_eq!(
            route(&naming, "hub", &tools, "linear__delete"),
            None,
            "visible server, unadvertised tool"
        );
    }

    /// The per-Agent registry's whole contract: an unregistered agent gets
    /// the empty proxy (the verbs and nothing else), a registered one gets
    /// exactly its own, a re-insert replaces, and a remove forgets — each of
    /// which, done wrong, serves one agent another agent's toolset.
    #[test]
    fn agent_proxies_fail_closed_and_never_cross_agents() {
        let proxies = super::AgentProxies::new();
        assert!(
            proxies.for_agent("agent-1").tools().is_empty(),
            "an unregistered agent must get the empty proxy"
        );
        assert!(proxies.all().is_empty());

        let visible = |names: &[&str]| {
            super::ToolProxy::new().with_hub_link(
                StaticLink {
                    tools: names.iter().map(|name| tool(name)).collect(),
                },
                names
                    .iter()
                    .filter_map(|name| crate::hub::split_namespaced(name))
                    .map(|(server, _)| server.to_owned()),
            )
        };
        proxies.insert("agent-1", Arc::new(visible(&["linear__create"])));
        proxies.insert("agent-2", Arc::new(visible(&["payments__refund"])));

        let listed = |agent: &str| {
            proxies
                .for_agent(agent)
                .tools()
                .iter()
                .map(|tool| tool.name.to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(listed("agent-1"), vec!["linear__create"]);
        assert_eq!(
            listed("agent-2"),
            vec!["payments__refund"],
            "each agent sees its own proxy, never a sibling's"
        );
        assert_eq!(proxies.all().len(), 2);

        proxies.insert("agent-1", Arc::new(visible(&["grafana__query"])));
        assert_eq!(
            listed("agent-1"),
            vec!["grafana__query"],
            "a re-insert replaces the previous incarnation's toolset"
        );

        proxies.remove("agent-1");
        assert!(
            proxies.for_agent("agent-1").tools().is_empty(),
            "a removed agent falls back to the empty proxy"
        );
    }

    /// A link whose snapshot is fixed — the pure-registry tests need a
    /// `HubLink` with no wire behind it.
    #[derive(Debug)]
    struct StaticLink {
        tools: Vec<Tool>,
    }

    #[async_trait::async_trait]
    impl crate::hub::HubLink for StaticLink {
        fn tools(&self) -> &[Tool] {
            &self.tools
        }

        async fn call(
            &self,
            name: &str,
            _arguments: Option<rmcp::model::JsonObject>,
        ) -> Result<rmcp::model::CallToolResult, crate::hub::HubError> {
            Err(crate::hub::HubError::UnknownTool {
                name: name.to_owned(),
            })
        }
    }

    /// The allowlist's own truth table, including the malformed case.
    #[test]
    fn the_visibility_filter_fails_closed_on_unprefixed_names() {
        let allowed = visible(&["linear"]);
        assert!(passes_filter(&allowed, "linear__create"));
        assert!(!passes_filter(&allowed, "payments__refund"));
        assert!(
            !passes_filter(&allowed, "bare_name"),
            "an unprefixed name from the hub is malformed, never unrestricted"
        );
        assert!(!passes_filter(&BTreeSet::new(), "linear__create"));
    }
}
