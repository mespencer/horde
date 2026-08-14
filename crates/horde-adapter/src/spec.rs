//! What Horde hands an adapter to bring an agent into existence.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::ids::ProviderAccount;

/// [`AgentSpec::provider_options`] key: how many turns the conversation being
/// resumed has already consumed, across every previous process.
///
/// Adapters derive replay-deterministic turn ids from a per-session ordinal
/// (`{session}#{n}`), and a Worker keys journal entries by turn id. A revived
/// session whose assembler restarted at zero would re-mint turn ids the journal
/// has already seen — and because the Server deduplicates on those keys, the
/// revived turns' events would be **acked and silently discarded**, the worst
/// failure shape there is (docs/QUALITY_GATES.md). Every resumable adapter
/// honours this option by numbering its next turn *after* the count given.
pub const TURNS_TAKEN_OPTION: &str = "turns-taken";

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

    /// Escape hatch for provider-specific settings. Adapters ignore keys they
    /// do not understand.
    pub provider_options: BTreeMap<String, String>,
}

impl AgentSpec {
    /// Declares how many turns the conversation being resumed has already
    /// consumed. See [`TURNS_TAKEN_OPTION`].
    #[must_use]
    pub fn with_turns_taken(mut self, turns: u64) -> Self {
        self.provider_options
            .insert(TURNS_TAKEN_OPTION.to_owned(), turns.to_string());
        self
    }

    /// Reads [`TURNS_TAKEN_OPTION`]: absent means a fresh conversation.
    ///
    /// # Errors
    ///
    /// A value that does not parse as a count, spelled out for the adapter to
    /// wrap in its own spawn error. Loud on purpose: silently reading a
    /// malformed count as zero would re-mint turn ids the journal already
    /// holds, and the revived turns' events would be deduplicated away.
    pub fn turns_taken(&self) -> Result<u64, String> {
        match self.provider_options.get(TURNS_TAKEN_OPTION) {
            None => Ok(0),
            Some(value) => value.parse().map_err(|_| {
                format!("the {TURNS_TAKEN_OPTION} option must be a turn count, got {value:?}")
            }),
        }
    }
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
    use super::{AgentSpec, Delivery, TurnInput};

    #[test]
    fn an_unmarked_spec_is_a_fresh_conversation() {
        assert_eq!(AgentSpec::default().turns_taken(), Ok(0));
    }

    #[test]
    fn the_turns_taken_option_round_trips_through_the_spec() {
        let spec = AgentSpec::default().with_turns_taken(7);
        assert_eq!(spec.turns_taken(), Ok(7));
    }

    /// Malformed is loud, never zero: a count silently read as zero re-mints
    /// turn ids the journal already holds, and the Server's dedupe then
    /// discards the revived turns' events while acking them.
    #[test]
    fn a_malformed_turn_count_is_refused_rather_than_read_as_zero() {
        let mut spec = AgentSpec::default();
        spec.provider_options
            .insert(super::TURNS_TAKEN_OPTION.to_owned(), "three".to_owned());

        let error = spec.turns_taken().expect_err("must refuse");
        assert!(
            error.contains("three"),
            "the offending value is the diagnosis: {error}"
        );
    }

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
