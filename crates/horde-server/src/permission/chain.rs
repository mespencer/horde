//! The chain's evaluation order, and the judge seam.
//!
//! DESIGN §10, in order: deterministic deny, deterministic allow, Policy
//! Judge, human. Evaluation is a pure function so the order — the security
//! property — is a truth table rather than something an integration test has
//! to stage. The one impure stage, asking a judge, is behind [`PolicyJudge`]:
//! a cheap model applying plain-text mode instructions is an I/O concern the
//! chain consumes, not one it owns.
//!
//! Remember-for-session lives in the deterministic-allow layer, which is what
//! `PermissionDecision.remember_for_session` promises ("widen the
//! deterministic-allow layer for the rest of the Session") — and it is
//! evaluated *after* deny, because "never Y" is authored policy and a
//! remembered convenience must not override it.

use std::collections::BTreeMap;

use horde_proto::prost_types::Struct;
use horde_proto::v1::{PermissionLayer, ProposedToolCall};

use super::modes::PermissionMode;
use crate::state::RememberedCall;

/// What the chain concluded about one proposed call.
#[derive(Debug, Clone, PartialEq)]
pub enum ChainRuling {
    /// A deny rule matched. Final: nothing later in the chain runs.
    Deny { reason: String },
    /// The deterministic-allow layer covers the call. `updated_input` carries
    /// a remembered narrowing to re-apply, when the layer was widened by a
    /// decision that had one.
    Allow { updated_input: Option<Struct> },
    /// The rules settled nothing and the mode carries judge instructions.
    Consult { instructions: String },
    /// Nothing before the human decided. `from` is the last layer that ran,
    /// for `PermissionRequest.escalated_from` — HUMAN never appears here.
    Escalate { from: PermissionLayer },
}

/// The facts about the requesting agent the deterministic layers consult,
/// gathered by the caller from the session projection so [`evaluate`] stays
/// a pure function of its arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Requester<'a> {
    /// Ancestry display names, root first — what rules scope by.
    pub ancestry: &'a [String],
    /// Whether the agent's adapter can apply a narrowing's replacement
    /// arguments (ADR-0009). A remembered narrowing replayed to an agent that
    /// cannot apply it escalates instead.
    pub rewrites_tool_input: bool,
    /// Whether the call is one of the agent's own orchestrator verbs, scoped
    /// to its own subtree ([`super::orchestrator::own_orchestrator_verb`]).
    pub own_orchestrator_verb: bool,
}

/// Runs the deterministic layers of the chain over one call.
///
/// The judge is not called here — a pure function cannot ask a model
/// anything. [`ChainRuling::Consult`] hands the caller the instructions to
/// ask with; everything else is settled.
#[must_use]
pub fn evaluate(
    mode: &PermissionMode,
    call: &ProposedToolCall,
    requester: &Requester<'_>,
    remembered: &BTreeMap<RememberedCall, Option<Struct>>,
) -> ChainRuling {
    let ancestry = requester.ancestry;
    if let Some(denied) = mode
        .deny
        .iter()
        .find(|deny| deny.rule.matches(call, ancestry))
    {
        return ChainRuling::Deny {
            reason: denied.reason.clone(),
        };
    }

    if let Some(narrowing) = remembered.get(&RememberedCall::of(call)) {
        // A clamp the agent's adapter cannot apply would be either dropped —
        // the unclamped call runs — or refused by the worker mid-turn. A human
        // decides instead (ADR-0009).
        if narrowing.is_some() && !requester.rewrites_tool_input {
            return ChainRuling::Escalate {
                from: PermissionLayer::DeterministicAllow,
            };
        }
        return ChainRuling::Allow {
            updated_input: narrowing.clone(),
        };
    }
    if mode.own_orchestrator_verbs && requester.own_orchestrator_verb {
        return ChainRuling::Allow {
            updated_input: None,
        };
    }
    if mode.allow.iter().any(|rule| rule.matches(call, ancestry)) {
        return ChainRuling::Allow {
            updated_input: None,
        };
    }

    match &mode.judge_instructions {
        Some(instructions) => ChainRuling::Consult {
            instructions: instructions.clone(),
        },
        None => ChainRuling::Escalate {
            from: PermissionLayer::DeterministicAllow,
        },
    }
}

/// What a consulted judge is handed: the policy, the call, and where in the
/// tree it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct Consultation<'a> {
    /// The mode's plain-text policy, e.g. "allow read-only git, never network".
    pub instructions: &'a str,
    pub call: &'a ProposedToolCall,
    /// Ancestry display names, root first — the same context a human approver
    /// reads.
    pub ancestry: &'a [String],
}

/// What a judge concluded.
///
/// There is deliberately no default: a judge that answers nothing usable
/// abstains, and abstention escalates to a human rather than resolving to
/// either outcome — both defaults are wrong in a way that hides itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JudgeRuling {
    Allow {
        rationale: String,
    },
    Deny {
        rationale: String,
    },
    /// The instructions do not settle this call, or the judge could not run.
    Abstain {
        rationale: String,
    },
}

/// The Policy Judge seam: a cheap model applying a mode's plain-text
/// instructions to one proposed call (DESIGN §10 layer 3).
///
/// Implementations own the model call entirely — provider, prompt shape,
/// retries. The chain's contract is only that a failure surfaces as
/// [`JudgeRuling::Abstain`], never as a guessed outcome.
#[async_trait::async_trait]
pub trait PolicyJudge: Send + Sync + std::fmt::Debug {
    async fn rule(&self, consultation: Consultation<'_>) -> JudgeRuling;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use horde_proto::prost_types::{Struct, Value, value::Kind as ValueKind};
    use horde_proto::v1::{PermissionLayer, ProposedToolCall};

    use super::{ChainRuling, Requester, evaluate};
    use crate::permission::modes::PermissionMode;
    use crate::permission::rules::Rule;
    use crate::state::RememberedCall;

    fn call(tool: &str, path: &str) -> ProposedToolCall {
        let mut input = Struct::default();
        input.fields.insert(
            "path".to_owned(),
            Value {
                kind: Some(ValueKind::StringValue(path.to_owned())),
            },
        );
        ProposedToolCall {
            tool_name: tool.to_owned(),
            input: Some(input),
            rendered: format!("{tool} {path}"),
        }
    }

    /// A requester at `ancestry` whose adapter rewrites and whose call is no
    /// orchestrator verb — the shape every rule-order test wants.
    fn at(ancestry: &[String]) -> Requester<'_> {
        Requester {
            ancestry,
            rewrites_tool_input: true,
            own_orchestrator_verb: false,
        }
    }

    fn remembered(
        entries: &[(&ProposedToolCall, Option<Struct>)],
    ) -> BTreeMap<RememberedCall, Option<Struct>> {
        entries
            .iter()
            .map(|(call, narrowing)| (RememberedCall::of(call), narrowing.clone()))
            .collect()
    }

    #[test]
    fn an_empty_mode_escalates_from_the_allow_layer() {
        let ruling = evaluate(
            &PermissionMode::escalate_everything(),
            &call("Bash", "/x"),
            &at(&[]),
            &BTreeMap::new(),
        );
        assert_eq!(
            ruling,
            ChainRuling::Escalate {
                from: PermissionLayer::DeterministicAllow
            },
            "the layer that gave up is what escalated_from reports"
        );
    }

    #[test]
    fn deny_runs_first_and_is_final() {
        // The same rule shape in both layers: deny must win, in that order,
        // which is the whole of DESIGN §10's "never Y".
        let mode = PermissionMode::escalate_everything()
            .deny(Rule::tool("Bash"), "no shell here")
            .allow(Rule::tool("Bash"))
            .judged_by("anything goes");

        assert_eq!(
            evaluate(&mode, &call("Bash", "/x"), &at(&[]), &BTreeMap::new()),
            ChainRuling::Deny {
                reason: "no shell here".to_owned()
            }
        );
    }

    #[test]
    fn deny_beats_a_remembered_allow() {
        let bash = call("Bash", "/x");
        let mode = PermissionMode::escalate_everything().deny(Rule::tool("Bash"), "never Y");

        assert_eq!(
            evaluate(&mode, &bash, &at(&[]), &remembered(&[(&bash, None)])),
            ChainRuling::Deny {
                reason: "never Y".to_owned()
            },
            "a remembered convenience must not override authored policy"
        );
    }

    #[test]
    fn an_allow_rule_settles_without_a_judge_or_a_human() {
        let mode = PermissionMode::escalate_everything()
            .allow(Rule::tool("Read"))
            .judged_by("would have been asked");

        assert_eq!(
            evaluate(&mode, &call("Read", "/x"), &at(&[]), &BTreeMap::new()),
            ChainRuling::Allow {
                updated_input: None
            }
        );
    }

    #[test]
    fn a_remembered_call_is_allowed_with_its_narrowing_reapplied() {
        let proposal = call("Write", "/etc/hosts");
        let clamp = call("Write", "/tmp/safe").input;

        assert_eq!(
            evaluate(
                &PermissionMode::escalate_everything(),
                &proposal,
                &at(&[]),
                &remembered(&[(&proposal, clamp.clone())]),
            ),
            ChainRuling::Allow {
                updated_input: clamp
            },
            "the repeat runs under the same clamp the human applied"
        );
    }

    #[test]
    fn an_unsettled_call_goes_to_the_judge_when_the_mode_has_instructions() {
        let mode = PermissionMode::escalate_everything().judged_by("allow read-only git");

        assert_eq!(
            evaluate(&mode, &call("Bash", "/x"), &at(&[]), &BTreeMap::new()),
            ChainRuling::Consult {
                instructions: "allow read-only git".to_owned()
            }
        );
    }

    /// Tree-path routing through the whole chain: the same call, decided
    /// differently depending on where in the tree it came from.
    #[test]
    fn the_same_call_routes_differently_by_ancestry() {
        let mode = PermissionMode::escalate_everything()
            .deny(
                Rule::tool("Bash").under(["ci-fix", "test-runner"]),
                "test runners do not get a shell",
            )
            .allow(Rule::tool("Bash").under(["ci-fix", "reviewer"]));
        let bash = call("Bash", "/x");

        assert_eq!(
            evaluate(
                &mode,
                &bash,
                &at(&["ci-fix".to_owned(), "test-runner".to_owned()]),
                &BTreeMap::new()
            ),
            ChainRuling::Deny {
                reason: "test runners do not get a shell".to_owned()
            }
        );
        assert_eq!(
            evaluate(
                &mode,
                &bash,
                &at(&["ci-fix".to_owned(), "reviewer".to_owned()]),
                &BTreeMap::new()
            ),
            ChainRuling::Allow {
                updated_input: None
            }
        );
        assert_eq!(
            evaluate(
                &mode,
                &bash,
                &at(&["ci-fix".to_owned(), "doc-writer".to_owned()]),
                &BTreeMap::new()
            ),
            ChainRuling::Escalate {
                from: PermissionLayer::DeterministicAllow
            },
            "an agent neither scope covers falls through to a human"
        );
    }

    /// A remembered clamp replays only to an agent that can apply it; for one
    /// that cannot, the chain escalates rather than allowing the unclamped
    /// call or shipping a clamp the worker must refuse (ADR-0009). A plain
    /// remembered allow carries no clamp and replays to anyone.
    #[test]
    fn a_remembered_clamp_escalates_for_an_agent_that_cannot_rewrite() {
        let proposal = call("Write", "/etc/hosts");
        let clamp = call("Write", "/tmp/safe").input;
        let cannot = Requester {
            rewrites_tool_input: false,
            ..at(&[])
        };

        assert_eq!(
            evaluate(
                &PermissionMode::escalate_everything(),
                &proposal,
                &cannot,
                &remembered(&[(&proposal, clamp)]),
            ),
            ChainRuling::Escalate {
                from: PermissionLayer::DeterministicAllow
            }
        );
        assert_eq!(
            evaluate(
                &PermissionMode::escalate_everything(),
                &proposal,
                &cannot,
                &remembered(&[(&proposal, None)]),
            ),
            ChainRuling::Allow {
                updated_input: None
            },
            "an unclamped remembered allow needs no rewriting"
        );
    }

    /// The orchestrator-verb allowance: only a mode that grants it, only for
    /// a call the requester owns, and never past a deny.
    #[test]
    fn own_orchestrator_verbs_are_allowed_only_where_the_mode_grants_them() {
        let verb = call("horde/report_result", "/x");
        let own = Requester {
            own_orchestrator_verb: true,
            ..at(&[])
        };
        let granting = PermissionMode::escalate_everything().allowing_own_orchestrator_verbs();

        assert_eq!(
            evaluate(&granting, &verb, &own, &BTreeMap::new()),
            ChainRuling::Allow {
                updated_input: None
            }
        );
        assert_eq!(
            evaluate(&granting, &verb, &at(&[]), &BTreeMap::new()),
            ChainRuling::Escalate {
                from: PermissionLayer::DeterministicAllow
            },
            "a verb the requester does not own is not covered"
        );
        assert_eq!(
            evaluate(
                &PermissionMode::escalate_everything(),
                &verb,
                &own,
                &BTreeMap::new()
            ),
            ChainRuling::Escalate {
                from: PermissionLayer::DeterministicAllow
            },
            "a mode that does not grant it escalates"
        );
        assert_eq!(
            evaluate(
                &granting.deny(Rule::tool("horde/report_result"), "not here"),
                &verb,
                &own,
                &BTreeMap::new()
            ),
            ChainRuling::Deny {
                reason: "not here".to_owned()
            },
            "deny still runs first"
        );
    }

    #[test]
    fn the_first_matching_deny_names_the_reason() {
        let mode = PermissionMode::escalate_everything()
            .deny(Rule::tool("Bash"), "specific")
            .deny(Rule::any_tool(), "general");

        assert_eq!(
            evaluate(&mode, &call("Bash", "/x"), &at(&[]), &BTreeMap::new()),
            ChainRuling::Deny {
                reason: "specific".to_owned()
            },
            "authoring order is precedence order within a layer"
        );
    }
}
