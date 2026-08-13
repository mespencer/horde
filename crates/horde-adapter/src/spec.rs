//! What Horde hands an adapter to bring an agent into existence.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::ids::ProviderAccount;

/// Everything an adapter needs to start (or revive) one agent.
///
/// Provider-agnostic on purpose: the fields here are the ones every harness can
/// honour. Anything genuinely provider-shaped goes in [`Self::provider_options`]
/// rather than growing this struct a field that only one adapter reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentSpec {
    /// Concrete model name, in the provider's own vocabulary. `None` leaves the
    /// harness on its default — tier resolution happens above this seam.
    pub model: Option<String>,

    /// Horde's prompt overlay, appended to whatever system prompt the harness
    /// already uses. Never replaces it: the harness's own prompt is what makes
    /// its tools work.
    pub append_system_prompt: Option<String>,

    /// The agent's working directory.
    pub cwd: Option<PathBuf>,

    /// Which isolated credential bundle to run under.
    ///
    /// Part of the agent's revival identity, not merely a spawn-time choice —
    /// a provider session recorded under one account cannot be resumed under
    /// another.
    pub account: Option<ProviderAccount>,

    /// The MCP endpoint carrying Horde's orchestrator verbs, proxied hub tools,
    /// and permission prompts (DESIGN.md §5).
    pub horde_endpoint: Option<McpEndpoint>,

    /// The named permission mode the agent runs under (DESIGN.md §10).
    ///
    /// Provider-agnostic on purpose, but providers consume it differently:
    /// the chain itself is evaluated server-side, while an adapter whose
    /// enforcement is a spawn-time sandbox tier (Codex, ADR-0005) derives the
    /// tier from this name. `None` means the spawn named no mode, which every
    /// consumer must treat as its most conservative case.
    pub permission_mode: Option<String>,

    /// Escape hatch for provider-specific settings. Adapters ignore keys they
    /// do not understand.
    pub provider_options: BTreeMap<String, String>,
}

/// Where an agent's one Horde-controlled MCP endpoint lives.
///
/// Both shapes are real: spike W1c drove permission prompting successfully
/// against a stdio child *and* an HTTP service, so the Worker is free to pick
/// whichever suits its process model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpEndpoint {
    /// A child process speaking MCP over stdio.
    Stdio { command: String, args: Vec<String> },
    /// An HTTP service the Worker already hosts.
    Http { url: String },
}

/// A message sent into a running agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnInput {
    pub text: String,
    /// How to treat an agent that is mid-turn.
    pub delivery: Delivery,
}

impl TurnInput {
    /// A message that waits for the current turn to finish.
    #[must_use]
    pub fn queued(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            delivery: Delivery::TurnBoundary,
        }
    }

    /// A message that cancels the current turn and is delivered immediately.
    #[must_use]
    pub fn interrupting(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            delivery: Delivery::Interrupt,
        }
    }
}

/// When a message reaches a busy agent.
///
/// Mirrors `horde.v1.DeliveryMode`. Steering is a delivery mode, not a third
/// message kind: the same message either waits for the turn boundary or cancels
/// the turn in flight.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Delivery {
    /// Queue until the agent's current turn ends. The default for steering.
    #[default]
    TurnBoundary,
    /// Cancel the current turn and deliver immediately. Requires
    /// [`Capability::InterruptibleTurn`](crate::capability::Capability::InterruptibleTurn).
    Interrupt,
}

#[cfg(test)]
mod tests {
    use super::{Delivery, TurnInput};

    #[test]
    fn queued_is_the_default_delivery() {
        assert_eq!(TurnInput::queued("hi").delivery, Delivery::TurnBoundary);
        assert_eq!(Delivery::default(), Delivery::TurnBoundary);
    }

    #[test]
    fn interrupting_messages_declare_themselves() {
        assert_eq!(
            TurnInput::interrupting("stop").delivery,
            Delivery::Interrupt
        );
    }
}
