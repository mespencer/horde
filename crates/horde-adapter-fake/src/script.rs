//! What a scripted agent does.
//!
//! A script is written by a test to describe agent behavior an orchestration
//! test needs: "emit these turns, ask permission here, stall, fail". It is the
//! test's half of the FakeAdapter contract.

use core::time::Duration;

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
    /// Put questions to the human and **wait** for the answers (W12c) — the
    /// shape Claude's `AskUserQuestion` and codex's `requestUserInput` reach
    /// the seam as: a permission request carrying questions, settled only by
    /// [`PermissionDecision::Answer`](horde_adapter::PermissionDecision::Answer)
    /// or a denial.
    AskQuestion {
        tool_call_id: String,
        tool_name: String,
        questions: Vec<horde_adapter::Question>,
    },
    /// Invoke one of Horde's own MCP verbs, which need no approval round-trip.
    ///
    /// How a test says "here the agent spawns a child" or "here it reports a
    /// result" — the orchestrator verbs of DESIGN.md §5 reach the adapter as
    /// ordinary tool calls, so that is how the script expresses them.
    ///
    /// When the session was spawned with a `horde_endpoint`, the call is
    /// **real**: the fake dials the endpoint over HTTP with its identity
    /// headers, exactly as a live harness would, and the result (or refusal)
    /// comes back as the tool's output. Without an endpoint the step stays
    /// what it always was — the intent surfaced as a tool-call event — so
    /// conformance suites that spawn bare sessions keep measuring the stream
    /// shape rather than a network.
    UseHordeTool {
        tool_call_id: String,
        tool_name: String,
        input: Value,
    },
    /// Go quiet for a while mid-turn.
    ///
    /// Real agents stall: a long tool call, a slow model, a human taking their
    /// time over an approval. Tests should pause the clock
    /// (`#[tokio::test(start_paused = true)]`) so the wait costs nothing in
    /// wall-clock time while still exercising whatever timeout is under test.
    Stall(Duration),
    /// The harness process dies here, quietly: the turn still plays out and
    /// ends, but the next send finds nobody listening — the between-turns
    /// death a Worker only discovers as a broken pipe. A *resumed* session
    /// carries on with the rest of the script, as a real revival would.
    Exit,
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
///
/// ```
/// use horde_adapter_fake::{Script, ScriptedTurn, Step};
/// use serde_json::json;
///
/// let script = Script::of(vec![
///     ScriptedTurn::saying("looking into it").with_step(Step::AskPermission {
///         tool_call_id: "call-1".to_owned(),
///         tool_name: "Bash".to_owned(),
///         input: json!({ "command": "docker rm horde-test" }),
///     }),
///     ScriptedTurn::saying("done"),
/// ]);
///
/// // The turn holding the AskPermission blocks until a decision arrives, which
/// // is what lets a test exercise the permission chain for real.
/// assert_eq!(script.turns.len(), 2);
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Script {
    pub turns: Vec<ScriptedTurn>,
    /// What happens when a message arrives after the script runs out.
    pub exhausted: Exhausted,
    /// A contract violation to commit deliberately.
    pub misbehaviour: Option<Misbehaviour>,
}

/// A way for a scripted agent to break the adapter contract on purpose.
///
/// This exists so the conformance suite's own checks can be tested. A check
/// that has never been shown to fail is not known to work — docs/TESTING.md §1
/// calls for positive controls in both directions, and these turn what would
/// otherwise be a manual experiment into permanent tests.
///
/// A real adapter must never do any of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Misbehaviour {
    /// Emit output without ever naming the session, so nothing produced can be
    /// attributed to a resumable conversation.
    SkipSessionEstablished,
    /// End the event stream without closing the turn.
    NeverEndTurn,
    /// Close the same turn twice.
    EndTurnTwice,
    /// Repeat a chunk index within a channel, defeating gap detection.
    RepeatChunkIndex,
    /// Rename the session mid-conversation, so revival would target the wrong
    /// conversation.
    ChangeSessionIdMidConversation,
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
            misbehaviour: None,
        }
    }

    /// Makes this agent break the adapter contract in a specific way.
    ///
    /// Only for testing the conformance suite itself.
    #[must_use]
    pub fn misbehaving(mut self, misbehaviour: Misbehaviour) -> Self {
        self.misbehaviour = Some(misbehaviour);
        self
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

    use super::{Exhausted, Misbehaviour, Script, ScriptedTurn, Step};

    #[test]
    fn a_saying_turn_answers_with_what_it_said() {
        let turn = ScriptedTurn::saying("hello");
        assert_eq!(turn.text.as_deref(), Some("hello"));
        assert_eq!(turn.steps, vec![Step::Say("hello".to_owned())]);
        assert_eq!(turn.status, TurnStatus::Completed);
    }

    #[test]
    fn a_well_behaved_script_declares_no_misbehaviour() {
        assert_eq!(Script::echoing().misbehaviour, None);
        assert_eq!(
            Script::echoing()
                .misbehaving(Misbehaviour::NeverEndTurn)
                .misbehaviour,
            Some(Misbehaviour::NeverEndTurn)
        );
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

#[cfg(test)]
mod builder_tests {
    use super::{Exhausted, Script, ScriptedTurn};

    /// A `saying` turn reports usage, and both counts are real.
    ///
    /// The fake is what every conformance verdict is measured against, so a
    /// scripted turn that silently billed nothing would let a usage bug pass
    /// the suite in any adapter compared to it. Mutants deleting either field
    /// survived until now.
    #[test]
    fn a_spoken_turn_bills_both_directions() {
        let turn = ScriptedTurn::saying("hello");

        assert_eq!(turn.usage.input_tokens, 10, "a turn consumes input");
        assert_eq!(turn.usage.output_tokens, 20, "and produces output");
        assert!(
            turn.usage.total() > 0,
            "a scripted turn that billed nothing would hide usage bugs in \
             every adapter measured against this fake"
        );
    }

    /// `echoing()` is the default script, and defaults matter here.
    ///
    /// It is the script a conformance run gets when nobody chose one, so what
    /// it does when it runs out of turns is the behaviour most suites actually
    /// exercise.
    #[test]
    fn the_echoing_script_starts_empty_and_echoes_forever() {
        let script = Script::echoing();

        assert!(script.turns.is_empty(), "nothing is scripted up front");
        assert_eq!(
            script.exhausted,
            Exhausted::Echo,
            "and running out means echo, not stop — a fake that quietly \
             stopped would end conformance turns nobody asked to end"
        );
        assert!(script.misbehaviour.is_none());
    }
}
