//! The MCP Hub: user MCP servers proxied onto every Agent's one endpoint.
//!
//! ADR-0004's ruling, mechanized. User servers (Linear, Grafana, …) are
//! configured at the Server and run there — authenticated exactly once — and
//! their tools reach each Agent through the Agent's existing endpoint,
//! namespaced by server name. A config marked `local` runs Worker-side
//! instead, one instance per Agent: the pressure valve for machine-bound or
//! stateful servers.
//!
//! # The pieces, and where they run
//!
//! * [`config`] — the domain shape of one configured server, converted from
//!   the journaled wire config.
//! * [`names`] — the namespace discipline: `{server}__{tool}`, split back on
//!   the first `__`. Pure functions, because getting this wrong misroutes
//!   tool calls silently.
//! * [`upstream`] — one live connection to one MCP server, as a client:
//!   a stdio child (spawned here, killed by handle on drop) or a streamable
//!   HTTP peer.
//! * [`proxy`] — the aggregation an endpoint serves from: local upstreams
//!   namespaced here, plus the hub link whose tools arrive pre-namespaced
//!   and are filtered to the server names this Agent may see.
//! * [`endpoint`] — the hub's own serving surface at the Server: the same
//!   dual-era `StreamableHttpService` the agent endpoint uses, gated by the
//!   same identity layer, reached by Workers over the authenticated channel.
//!
//! # Where the permission chain sits
//!
//! Not here. The chain gates proxied calls at the *agent's* endpoint
//! ([`crate::endpoint`]), where the caller's identity and the Worker's
//! journal both live — DESIGN §10 puts the chain in the tool-call path, and
//! the tool-call path starts at the tool the Agent actually calls. The hub
//! trusts its callers to that extent deliberately: they are authenticated
//! Workers, and a Worker that skipped the chain would be a compromised
//! Worker, which no hub-side re-check of tool names could contain.

pub mod config;
pub mod endpoint;
pub mod names;
pub mod proxy;
pub mod upstream;

pub use config::{HubServerConfig, HubTransport, Placement};
pub use endpoint::HubEndpoint;
pub use names::{namespaced, split_namespaced};
pub use proxy::ToolProxy;
pub use upstream::{HubError, Upstream};
