//! The adapter end of the permission chain (DESIGN.md §10).
//!
//! An adapter's job here is narrow: surface the tool call the harness wants to
//! make, block until Horde answers, and apply the answer faithfully. The four
//! layers — deterministic deny, deterministic allow, policy judge, human — live
//! above this seam and are not the adapter's business.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::ids::ToolCallId;

/// A tool call as the harness proposed it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProposedToolCall {
    pub tool_call_id: ToolCallId,
    pub tool_name: String,
    /// Arguments as structured data, not a JSON string, so deterministic rules
    /// and the policy judge read the same values.
    pub input: Value,
    /// The questions this call puts to the human, when the harness is asking
    /// rather than (or as well as) acting — Claude's `AskUserQuestion`,
    /// codex's `item/tool/requestUserInput` (W12c).
    ///
    /// Non-empty means only a human can settle the request: no rule or judge
    /// can answer a question on someone's behalf, so the chain escalates it,
    /// and the answer comes back as [`PermissionDecision::Answer`].
    pub questions: Vec<Question>,
}

impl ProposedToolCall {
    /// A call that asks nothing — the shape every plain approval has.
    #[must_use]
    pub fn new(tool_call_id: ToolCallId, tool_name: impl Into<String>, input: Value) -> Self {
        Self {
            tool_call_id,
            tool_name: tool_name.into(),
            input,
            questions: Vec::new(),
        }
    }

    /// The same call, putting these questions to the human.
    #[must_use]
    pub fn asking(mut self, questions: Vec<Question>) -> Self {
        self.questions = questions;
        self
    }

    /// Whether this call is a question only a human can answer.
    #[must_use]
    pub fn is_question(&self) -> bool {
        !self.questions.is_empty()
    }
}

/// One question a harness put to the human, with the options it offered.
///
/// Mirrors `horde.v1.Question`. The two harnesses that ask questions shape
/// them almost identically (header, question, labelled options), and differ
/// only in how answers are keyed: codex by an explicit question id, Claude by
/// the question text. [`Self::id`] is whichever the harness keys by, so an
/// answer can be filed back without a translation table.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Question {
    /// The key the answer is filed under.
    pub id: String,
    /// A short label, e.g. "Colour".
    pub header: String,
    /// The question as asked.
    pub question: String,
    /// The choices offered; empty means free text only.
    pub options: Vec<QuestionOption>,
    /// Whether several options may be chosen.
    pub multi_select: bool,
    /// Whether an answer outside the options is acceptable.
    pub allows_free_text: bool,
    /// Whether the answer is a secret a client should not echo.
    pub secret: bool,
}

/// One selectable answer to a [`Question`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QuestionOption {
    pub label: String,
    pub description: String,
}

/// Answers to a call's questions: each question's id to the chosen option
/// labels (or free text). Ordered so encodings are deterministic.
pub type Answers = BTreeMap<String, Vec<String>>;

/// Horde's answer to a [`ProposedToolCall`].
///
/// ```
/// use horde_adapter::PermissionDecision;
/// use serde_json::json;
///
/// // Permitting a call as proposed.
/// let allowed = PermissionDecision::allow();
/// assert!(allowed.is_allow() && allowed.updated_input().is_none());
///
/// // Narrowing it: the harness runs these arguments, not the model's.
/// let narrowed = PermissionDecision::allow_with(json!({ "path": "/workspace" }));
/// assert_eq!(narrowed.updated_input(), Some(&json!({ "path": "/workspace" })));
///
/// // An allow that widens the deterministic-allow layer for the Session also
/// // tells the adapter, so a harness with its own session cache (codex's
/// // `acceptForSession`) can mirror the grant instead of re-asking.
/// let cached = PermissionDecision::allow().remembered();
/// assert!(cached.remembers_for_session());
///
/// let refused = PermissionDecision::deny("outside the workspace");
/// assert!(!refused.is_allow() && !refused.remembers_for_session());
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum PermissionDecision {
    /// Let the call proceed.
    Allow {
        /// Replacement arguments, when the chain narrowed the call rather than
        /// merely permitting it — clamping a path, dropping a flag.
        ///
        /// Spike W1c proved this is real rather than theoretical: Claude Code
        /// executes the arguments the permission tool returns, not the ones the
        /// model proposed. Adapters whose harness cannot rewrite arguments must
        /// report [`Capability::RewritesToolInput`] as unsupported and reject a
        /// decision carrying `updated_input`, so a narrowing that silently
        /// failed to apply can never be mistaken for one that took effect.
        ///
        /// [`Capability::RewritesToolInput`]: crate::capability::Capability::RewritesToolInput
        updated_input: Option<Value>,
        /// Whether the decision also widened the deterministic-allow layer for
        /// the rest of the Session (`PermissionDecision.remember_for_session`
        /// on the wire).
        ///
        /// The Server owns that memory — repeats are auto-allowed by the chain
        /// whether or not the adapter does anything here. The bit rides to the
        /// adapter so a harness with its own session-scoped grant vocabulary
        /// (codex's `acceptForSession`, re-probe transcripts 20–33) can encode
        /// the same scope and skip the round trip; adapters without one ignore
        /// it, which loses nothing but the round trip.
        remember_for_session: bool,
    },
    /// Refuse the call. The message is shown to the agent as the tool's error.
    Deny { message: String },
    /// The human answered the call's questions; the call proceeds with these
    /// answers (W12c). Only meaningful for a [`ProposedToolCall::is_question`]
    /// request — an adapter refuses it anywhere else rather than run a tool
    /// with answers it would silently drop.
    Answer { answers: Answers },
}

impl PermissionDecision {
    /// Allows the call exactly as proposed.
    #[must_use]
    pub fn allow() -> Self {
        Self::Allow {
            updated_input: None,
            remember_for_session: false,
        }
    }

    /// Allows the call, but with these arguments instead.
    #[must_use]
    pub fn allow_with(updated_input: Value) -> Self {
        Self::Allow {
            updated_input: Some(updated_input),
            remember_for_session: false,
        }
    }

    /// Marks an allow as one that widened the Session's deterministic-allow
    /// layer. A denial is returned unchanged: remembering a refusal as a grant
    /// would be the silently widened permission the wire's own docs forbid,
    /// so the bit is unrepresentable on `Deny` rather than merely unread.
    #[must_use]
    pub fn remembered(self) -> Self {
        match self {
            Self::Allow { updated_input, .. } => Self::Allow {
                updated_input,
                remember_for_session: true,
            },
            // Neither a refusal nor an answer may widen the allow layer: an
            // answer remembered as a grant would answer the next question
            // unasked.
            other @ (Self::Deny { .. } | Self::Answer { .. }) => other,
        }
    }

    /// Whether this decision also widened the Session's allow layer.
    #[must_use]
    pub fn remembers_for_session(&self) -> bool {
        matches!(
            self,
            Self::Allow {
                remember_for_session: true,
                ..
            }
        )
    }

    /// Refuses the call.
    #[must_use]
    pub fn deny(message: impl Into<String>) -> Self {
        Self::Deny {
            message: message.into(),
        }
    }

    /// Answers a question request.
    #[must_use]
    pub fn answer(answers: Answers) -> Self {
        Self::Answer { answers }
    }

    /// Whether the call proceeds: an allow, or an answered question.
    #[must_use]
    pub fn is_allow(&self) -> bool {
        matches!(self, Self::Allow { .. } | Self::Answer { .. })
    }

    /// The replacement arguments this decision carries, if any.
    #[must_use]
    pub fn updated_input(&self) -> Option<&Value> {
        match self {
            Self::Allow { updated_input, .. } => updated_input.as_ref(),
            Self::Deny { .. } | Self::Answer { .. } => None,
        }
    }

    /// The answers this decision carries, if it answers questions.
    #[must_use]
    pub fn answers(&self) -> Option<&Answers> {
        match self {
            Self::Answer { answers } => Some(answers),
            Self::Allow { .. } | Self::Deny { .. } => None,
        }
    }
}

/// A tool call the permission chain refused, as reported at turn end.
///
/// Kept distinct from the live [`ProposedToolCall`] because this is the audit
/// record: it is what the Worker journals, and it survives the turn.
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionDenial {
    pub tool_call_id: ToolCallId,
    pub tool_name: String,
    pub attempted_input: Value,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::PermissionDecision;

    #[test]
    fn a_bare_allow_carries_no_rewrite() {
        let decision = PermissionDecision::allow();
        assert!(decision.is_allow());
        assert!(decision.updated_input().is_none());
    }

    #[test]
    fn a_narrowing_allow_carries_the_replacement_arguments() {
        let decision = PermissionDecision::allow_with(json!({"path": "/safe"}));
        assert_eq!(decision.updated_input(), Some(&json!({"path": "/safe"})));
    }

    #[test]
    fn a_denial_is_not_an_allow_and_rewrites_nothing() {
        let decision = PermissionDecision::deny("nope");
        assert!(!decision.is_allow());
        assert!(decision.updated_input().is_none());
    }

    /// An answer proceeds, rewrites nothing, carries its answers, and can
    /// never be remembered — a remembered answer would answer the next
    /// question nobody saw.
    #[test]
    fn an_answer_proceeds_with_its_answers_and_is_never_remembered() {
        let answers = super::Answers::from([("Colour".to_owned(), vec!["Red".to_owned()])]);
        let decision = PermissionDecision::answer(answers.clone());
        assert!(decision.is_allow());
        assert!(decision.updated_input().is_none());
        assert_eq!(decision.answers(), Some(&answers));
        assert!(!decision.clone().remembered().remembers_for_session());
        assert_eq!(decision.clone().remembered(), decision);
        assert_eq!(PermissionDecision::allow().answers(), None);
        assert_eq!(PermissionDecision::deny("no").answers(), None);
    }

    #[test]
    fn a_call_is_a_question_exactly_when_it_asks_something() {
        let plain =
            super::ProposedToolCall::new(crate::ids::ToolCallId::new("t1"), "Write", json!({}));
        assert!(!plain.is_question());
        assert!(plain.questions.is_empty());
        let asking = plain.asking(vec![super::Question {
            id: "q".to_owned(),
            ..super::Question::default()
        }]);
        assert!(asking.is_question());
        assert_eq!(asking.tool_name, "Write");
    }

    /// The remember bit: settable on an allow (with or without a narrowing,
    /// which must survive the marking), and unrepresentable on a denial — a
    /// remembered refusal read back as a grant would be the silently widened
    /// permission the wire's own docs forbid.
    #[test]
    fn remembering_marks_allows_and_cannot_mark_denials() {
        assert!(!PermissionDecision::allow().remembers_for_session());
        assert!(
            PermissionDecision::allow()
                .remembered()
                .remembers_for_session()
        );

        let narrowed = PermissionDecision::allow_with(json!({"path": "/safe"})).remembered();
        assert!(narrowed.remembers_for_session());
        assert_eq!(
            narrowed.updated_input(),
            Some(&json!({"path": "/safe"})),
            "marking must not drop the narrowing"
        );

        let refused = PermissionDecision::deny("no").remembered();
        assert!(
            !refused.remembers_for_session(),
            "a denial cannot carry the remember bit"
        );
        assert_eq!(refused, PermissionDecision::deny("no"));
    }
}
