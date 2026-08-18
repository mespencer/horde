//! The agent-facing MCP endpoint.
//!
//! Every Agent gets exactly one Horde-controlled MCP endpoint (DESIGN.md §5).
//! It carries the orchestrator verbs, and it is where permission prompts arrive
//! — Claude Code's `--permission-prompt-tool` is pointed here, so a tool call
//! the harness wants to make becomes an ordinary Horde event that any attached
//! Client can answer.
//!
//! # Two eras, one endpoint
//!
//! The endpoint serves both MCP revisions from one `StreamableHttpService`, and
//! the split is the spec's own: an `initialize` request selects the legacy
//! handshake flow, a `_meta`-versioned request the stateless 2026-07-28 one.
//! The dual-era spike measured all three of `2025-06-18`, `2025-11-25`, and
//! `2026-07-28` served concurrently by one process.
//!
//! **The legacy era carries all current traffic**, and that is a measurement
//! rather than a preference: no harness client Horde ships an adapter for can
//! speak 2026-07-28 yet, and neither falls forward. Against a modern-only
//! endpoint Claude Code takes the `UnsupportedProtocolVersionError` and stops —
//! so such an endpoint would ship zero tools to every Agent, silently and with
//! no error event. See `docs/research/mcp-2026-audit.md` and DESIGN §9.
//!
//! # The two planes
//!
//! The verbs do not all go to the same place, and which plane a verb belongs to
//! is a design commitment rather than an implementation convenience:
//!
//! * **Requests for orchestration** — `spawn_agent`, `send_message` — are
//!   ControlPlane RPCs, because placement and delivery are Server decisions.
//! * **Observed facts of execution** — permission requests, results — go into
//!   the event log through the Worker's journal, because DESIGN §5 prefers the
//!   orchestrator *observing* what happened over Agents *declaring* it. Only
//!   the Worker can see an Agent's workspace, so only the Worker can report a
//!   result's commits honestly. An endpoint that let an Agent author those
//!   events directly would be taking the Agent's word for its own output.
//!
//! [`journal::JournalSink`] is the seam for the second plane.

pub mod context;
pub mod endpoint;
pub mod forwards;
pub mod hub;
pub mod identity;
pub mod journal;
pub mod permission;
pub mod server;

#[cfg(test)]
mod testing;
pub mod verbs;

pub use identity::{AgentIdentity, AgentRegistry, IdentityError};
pub use journal::{JournalEntry, JournalError, JournalSink};
pub use server::{EndpointConfig, RequireIdentity};
