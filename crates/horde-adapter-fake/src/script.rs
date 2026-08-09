//! What a scripted agent does.
//!
//! A script is written by a test to describe agent behavior an orchestration
//! test needs: "emit these turns, ask permission here, stall, fail". It is the
//! test's half of the FakeAdapter contract.

use horde_adapter::{TokenUsage, TurnStatus};
use serde_json::Value;

/// One thing a scripted agent does within a turn.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    /// Emit assistant-visible prose.
    Say(String),
    /// Emit reasoning.
    Think(String),
    /// Propose a tool call and **wait** for a permission decision.
    ///
    /// The turn does not advance until
    /// [`AgentSession::answer_permission`](horde_adapter::AgentSession::answer_permission)
    /// is called. That waiting is the point: it is what lets an orchestration
    /// test exercise the permission chain, including the human-approval layer,
    /// without a provider login.
    AskPermission {
        tool_call_id: String,
        tool_name: String,
        input: Value,
    },
}

/// One scripted turn.
#[derive(Debug, Clone, PartialEq)]
pub struct ScriptedTurn {
    pub steps: Vec<Step>,
    /// The agent's final answer for the turn.
    pub text: Option<String>,
    pub status: TurnStatus,
    /// Tokens this turn consumes. The fake accumulates these into a running
    /// total internally and reports the difference, exactly as a real harness
    /// forces its adapter to.
    pub usage: TokenUsage,
    pub cost_usd: f64,
}

impl ScriptedTurn {
    /// A turn that says one thing and finishes.
    #[must_use]
    pub fn saying(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            steps: vec![Step::Say(text.clone())],
            text: Some(text),
            status: TurnStatus::Completed,
            usage: TokenUsage {
                input_tokens: 10,
                output_tokens: 20,
                ..TokenUsage::default()
            },
            cost_usd: 0.001,
        }
    }

    #[must_use]
    pub fn with_step(mut self, step: Step) -> Self {
        self.steps.push(step);
        self
    }

    #[must_use]
    pub fn with_status(mut self, status: TurnStatus) -> Self {
        self.status = status;
        self
    }

    #[must_use]
    pub fn with_usage(mut self, usage: TokenUsage, cost_usd: f64) -> Self {
        self.usage = usage;
        self.cost_usd = cost_usd;
        self
    }
}

/// A whole scripted conversation.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Script {
    pub turns: Vec<ScriptedTurn>,
    /// What happens when a message arrives after the script runs out.
    pub exhausted: Exhausted,
}

/// How a scripted agent behaves once its script is spent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Exhausted {
    /// Keep answering, echoing the message back. Suits tests that care about
    /// orchestration rather than about what the agent says.
    #[default]
    Echo,
    /// Behave as a harness that died.
    Exit,
}

impl Script {
    /// A script that echoes every message. The default agent for tests that do
    /// not care what the agent says.
    #[must_use]
    pub fn echoing() -> Self {
        Self::default()
    }

    /// A script that plays these turns, then echoes.
    #[must_use]
    pub fn of(turns: Vec<ScriptedTurn>) -> Self {
        Self {
            turns,
            exhausted: Exhausted::Echo,
        }
    }

    #[must_use]
    pub fn ending_after_the_script(mut self) -> Self {
        self.exhausted = Exhausted::Exit;
        self
    }
}

#[cfg(test)]
mod tests {
    use horde_adapter::TurnStatus;

    use super::{Exhausted, Script, ScriptedTurn, Step};

    #[test]
    fn a_saying_turn_answers_with_what_it_said() {
        let turn = ScriptedTurn::saying("hello");
        assert_eq!(turn.text.as_deref(), Some("hello"));
        assert_eq!(turn.steps, vec![Step::Say("hello".to_owned())]);
        assert_eq!(turn.status, TurnStatus::Completed);
    }

    #[test]
    fn scripts_echo_past_their_end_unless_told_otherwise() {
        assert_eq!(Script::echoing().exhausted, Exhausted::Echo);
        assert_eq!(
            Script::of(Vec::new()).ending_after_the_script().exhausted,
            Exhausted::Exit
        );
    }
}
