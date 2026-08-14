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

use std::collections::BTreeSet;

use rmcp::model::{CallToolResult, JsonObject, Tool};

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

/// One upstream and how its tools are named.
#[derive(Debug, Clone)]
struct Source {
    naming: Naming,
    upstream: Upstream,
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
            upstream,
        });
        self
    }

    /// Adds the hub link: an upstream whose tools are already namespaced,
    /// admitted only for the `visible` server names.
    ///
    /// The filter is the Agent's resolved visibility (computed server-side
    /// from the catalog and the Agent's type); everything else the hub serves
    /// is invisible here — not listed, and refused if called by name.
    #[must_use]
    pub fn with_hub_link(
        mut self,
        upstream: Upstream,
        visible: impl IntoIterator<Item = String>,
    ) -> Self {
        self.sources.push(Source {
            naming: Naming::Passthrough {
                visible: visible.into_iter().collect(),
            },
            upstream,
        });
        self
    }

    /// Whether this proxy serves anything at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Every tool this proxy serves, under its endpoint-facing name.
    #[must_use]
    pub fn tools(&self) -> Vec<Tool> {
        self.sources
            .iter()
            .flat_map(|source| {
                visible_tools(
                    &source.naming,
                    source.upstream.server(),
                    source.upstream.tools(),
                )
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
        let (upstream, upstream_name) =
            self.resolve(name).ok_or_else(|| HubError::UnknownTool {
                name: name.to_owned(),
            })?;
        upstream.call(&upstream_name, arguments).await
    }

    /// The upstream serving `name`, and the name to call it by there.
    fn resolve(&self, name: &str) -> Option<(&Upstream, String)> {
        self.sources.iter().find_map(|source| {
            route(
                &source.naming,
                source.upstream.server(),
                source.upstream.tools(),
                name,
            )
            .map(|upstream_name| (&source.upstream, upstream_name))
        })
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
