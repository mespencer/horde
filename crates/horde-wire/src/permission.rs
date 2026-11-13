//! Permission decisions, both directions.
//!
//! This is the conversion most worth owning in one place. A decision may
//! *narrow* a tool call rather than merely permit it, and spike W1c proved
//! Claude Code executes whatever arguments the permission tool returns — so a
//! rewrite that quietly fails to cross the wire means the original, unnarrowed
//! call runs. The failure mode is a silently widened permission, which is
//! exactly the kind of thing that must be an error rather than a dropped field.
//!
//! The two sides also disagree about what an argument bag is. The domain uses
//! `serde_json::Value`, which may be any JSON value; `google.protobuf.Struct`
//! is specifically an object. That asymmetry is surfaced as
//! [`ConversionError::NotAnObject`] rather than smoothed over.

use std::collections::HashMap;

use horde_adapter::permission::{
    Answers, PermissionDecision, ProposedToolCall, Question, QuestionOption,
};
use horde_proto::prost_types::value::Kind;
use horde_proto::prost_types::{ListValue, Struct, Value as WireValue};
use horde_proto::v1::{
    Answer as WireAnswer, PermissionDecision as WirePermissionDecision, PermissionOutcome,
    ProposedToolCall as WireProposedToolCall, Question as WireQuestion,
    QuestionOption as WireQuestionOption,
};
use serde_json::{Map, Number, Value as JsonValue};

/// Why a permission decision could not cross the seam.
#[derive(Debug, Clone, PartialEq)]
pub enum ConversionError {
    /// `updated_input` was not a JSON object, so it cannot be a `Struct`.
    ///
    /// Tool arguments are always a named bag in practice; a bare array or
    /// scalar means the caller built something the wire cannot carry, and
    /// truncating it would silently drop the narrowing.
    NotAnObject,
    /// A JSON number that has no `f64` representation — protobuf's `Struct`
    /// has only doubles, so a `u64`/`i64` past 2^53 cannot round-trip exactly.
    UnrepresentableNumber(String),
    /// An outcome this build does not recognize, or `UNSPECIFIED`.
    ///
    /// Never guessed: defaulting an unknown outcome to allow would be a
    /// security failure, and defaulting it to deny would silently stall agents
    /// on a newer Server.
    UnusableOutcome(i32),
    /// The wire said ALLOW and carried replacement arguments the Worker's
    /// adapter cannot apply.
    ///
    /// The proto documents reject-don't-drop for exactly this: an adapter
    /// without the rewrite capability must refuse, so a narrowing that failed
    /// to apply is never mistaken for one that took effect.
    RewriteUnsupported,
    /// The wire said ALLOW with both answers and replacement arguments.
    ///
    /// An answer is how a question proceeds and a narrowing is how a tool
    /// call does; one decision carrying both means the client meant one of
    /// them and the adapter would have to guess which to drop.
    AnswersWithRewrite,
    /// Answers on a decision whose outcome is not ALLOW: a refusal answers
    /// nothing, and reading answers off it would run a question the human
    /// declined.
    AnswersWithoutAllow,
}

impl core::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotAnObject => write!(f, "updated_input must be a JSON object"),
            Self::UnrepresentableNumber(n) => {
                write!(f, "number {n} has no exact f64 representation")
            }
            Self::UnusableOutcome(v) => write!(f, "unusable PermissionOutcome {v}"),
            Self::RewriteUnsupported => {
                write!(f, "decision rewrites tool input but the adapter cannot")
            }
            Self::AnswersWithRewrite => {
                write!(f, "decision carries both answers and replacement arguments")
            }
            Self::AnswersWithoutAllow => {
                write!(f, "decision carries answers without outcome ALLOW")
            }
        }
    }
}

impl core::error::Error for ConversionError {}

/// A double that represents `n` exactly, or an error.
///
/// Protobuf `Struct` has only doubles. An integer past 2^53 would come back a
/// different number, and a permission decision that silently altered an
/// argument is worse than one that failed loudly.
fn exact_f64(n: &Number) -> Result<f64, ConversionError> {
    let as_f64 = n
        .as_f64()
        .ok_or_else(|| ConversionError::UnrepresentableNumber(n.to_string()))?;

    if let Some(i) = n.as_i64()
        && i128::from(i) != as_f64 as i128
    {
        return Err(ConversionError::UnrepresentableNumber(n.to_string()));
    }
    if let Some(u) = n.as_u64()
        && u128::from(u) != as_f64 as u128
    {
        return Err(ConversionError::UnrepresentableNumber(n.to_string()));
    }
    Ok(as_f64)
}

fn json_to_wire(value: &JsonValue) -> Result<WireValue, ConversionError> {
    let kind = match value {
        JsonValue::Null => Kind::NullValue(0),
        JsonValue::Bool(b) => Kind::BoolValue(*b),
        JsonValue::Number(n) => Kind::NumberValue(exact_f64(n)?),
        JsonValue::String(s) => Kind::StringValue(s.clone()),
        JsonValue::Array(items) => Kind::ListValue(ListValue {
            values: items.iter().map(json_to_wire).collect::<Result<_, _>>()?,
        }),
        JsonValue::Object(map) => Kind::StructValue(object_to_struct(map)?),
    };
    Ok(WireValue { kind: Some(kind) })
}

fn object_to_struct(map: &Map<String, JsonValue>) -> Result<Struct, ConversionError> {
    let mut fields = std::collections::BTreeMap::new();
    for (key, value) in map {
        fields.insert(key.clone(), json_to_wire(value)?);
    }
    Ok(Struct { fields })
}

fn wire_to_json(value: &WireValue) -> JsonValue {
    // An unset kind is an empty Value, which ProtoJSON reads as null.
    match &value.kind {
        None | Some(Kind::NullValue(_)) => JsonValue::Null,
        Some(Kind::BoolValue(b)) => JsonValue::Bool(*b),
        Some(Kind::NumberValue(n)) => number_from_wire(*n),
        Some(Kind::StringValue(s)) => JsonValue::String(s.clone()),
        Some(Kind::ListValue(list)) => {
            JsonValue::Array(list.values.iter().map(wire_to_json).collect())
        }
        Some(Kind::StructValue(s)) => struct_to_json(s),
    }
}

/// Reconstructs a JSON number from protobuf's only numeric type, a double.
///
/// `Struct` cannot represent integers, so `{"depth": 3}` arrives as `3.0`.
/// Integral doubles are turned back into JSON integers because tool arguments
/// are overwhelmingly integers — a harness handed `{"depth": 3.0}` may render
/// or type-check it differently than the `{"depth": 3}` the chain intended.
///
/// The narrowing this cannot undo: a caller who genuinely meant the float
/// `3.0` gets the integer `3` back. In JSON those are the same number, so the
/// value is preserved even though the spelling is not. Magnitudes beyond
/// 2^53 cannot survive the double at all, which is why the outbound direction
/// rejects them rather than rounding.
fn number_from_wire(n: f64) -> JsonValue {
    const EXACT_INTEGER_LIMIT: f64 = 9_007_199_254_740_992.0; // 2^53

    if n.fract() == 0.0 && n.abs() <= EXACT_INTEGER_LIMIT {
        #[allow(clippy::cast_possible_truncation)] // guarded by the check above
        return JsonValue::Number(Number::from(n as i64));
    }
    Number::from_f64(n).map_or(JsonValue::Null, JsonValue::Number)
}

/// A wire `Struct` as the JSON object it encodes — the render-side inverse
/// of the narrowing conversions above, public so a client can *show* a
/// decision's `updated_input` without pretending to be an adapter.
#[must_use]
pub fn struct_to_json(value: &Struct) -> JsonValue {
    JsonValue::Object(
        value
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), wire_to_json(v)))
            .collect(),
    )
}

/// Domain → wire, for the Worker journaling a `PermissionRequested` event.
///
/// Fills only what the call itself carries: the tool name, the arguments, and
/// a deterministic rendering for approval UIs. The correlation identity is
/// deliberately *not* here — the wire `ProposedToolCall` has no id field, and
/// `PermissionRequest.request_id` is minted by whoever raises the request
/// (the Worker uses the harness's `tool_call_id`), which is a policy this
/// conversion must not own.
///
/// An empty argument object survives as an empty `Struct`: "a call with no
/// arguments" and "arguments unknown" are different facts, same as the
/// absent-versus-empty rule on `PermissionDecision.updated_input`.
///
/// # Errors
///
/// [`ConversionError::NotAnObject`] when the arguments are not a JSON object —
/// tool arguments are always a named bag, and truncating a bare array or
/// scalar would journal a call other than the one proposed.
/// [`ConversionError::UnrepresentableNumber`] when an argument has no exact
/// `f64` representation, for the same reason the decision path refuses one.
pub fn proposed_tool_call_to_wire(
    call: &ProposedToolCall,
) -> Result<WireProposedToolCall, ConversionError> {
    let input = match &call.input {
        JsonValue::Object(map) => object_to_struct(map)?,
        _ => return Err(ConversionError::NotAnObject),
    };
    Ok(WireProposedToolCall {
        tool_name: call.tool_name.clone(),
        input: Some(input),
        rendered: format!("{} {}", call.tool_name, call.input),
        questions: questions_to_wire(&call.questions),
    })
}

/// Domain questions → wire, field for field.
#[must_use]
pub fn questions_to_wire(questions: &[Question]) -> Vec<WireQuestion> {
    questions
        .iter()
        .map(|question| WireQuestion {
            id: question.id.clone(),
            header: question.header.clone(),
            question: question.question.clone(),
            options: question
                .options
                .iter()
                .map(|option| WireQuestionOption {
                    label: option.label.clone(),
                    description: option.description.clone(),
                })
                .collect(),
            multi_select: question.multi_select,
            allows_free_text: question.allows_free_text,
            secret: question.secret,
        })
        .collect()
}

/// Wire questions → domain, field for field — what a client renders.
#[must_use]
pub fn questions_from_wire(questions: &[WireQuestion]) -> Vec<Question> {
    questions.iter().map(question_from_wire).collect()
}

fn question_from_wire(wire: &WireQuestion) -> Question {
    let WireQuestion {
        id,
        header,
        question,
        options,
        multi_select,
        allows_free_text,
        secret,
    } = wire;
    Question {
        id: id.clone(),
        header: header.clone(),
        question: question.clone(),
        options: options
            .iter()
            .map(|WireQuestionOption { label, description }| QuestionOption {
                label: label.clone(),
                description: description.clone(),
            })
            .collect(),
        multi_select: *multi_select,
        allows_free_text: *allows_free_text,
        secret: *secret,
    }
}

/// Domain answers → the wire map.
#[must_use]
pub fn answers_to_wire(answers: &Answers) -> HashMap<String, WireAnswer> {
    answers
        .iter()
        .map(|(id, values)| {
            (
                id.clone(),
                WireAnswer {
                    values: values.clone(),
                },
            )
        })
        .collect()
}

/// The wire map → domain answers, ordered.
#[must_use]
pub fn answers_from_wire(answers: &HashMap<String, WireAnswer>) -> Answers {
    answers
        .iter()
        .map(|(id, answer)| (id.clone(), answer.values.clone()))
        .collect()
}

/// Domain → wire.
///
/// Fills only what the domain owns: the outcome, the denial message, any
/// replacement arguments, and the remember-for-session bit. `request_id`,
/// `decided_by`, and `decided_by_actor` belong to the Server that ran the
/// permission chain and are left at their defaults for it to set.
pub fn permission_decision_to_wire(
    decision: &PermissionDecision,
) -> Result<WirePermissionDecision, ConversionError> {
    let (outcome, reason, updated_input) = match decision {
        PermissionDecision::Allow { updated_input, .. } => {
            let rewritten = match updated_input {
                Some(JsonValue::Object(map)) => Some(object_to_struct(map)?),
                Some(_) => return Err(ConversionError::NotAnObject),
                None => None,
            };
            (PermissionOutcome::Allow, String::new(), rewritten)
        }
        PermissionDecision::Deny { message } => (PermissionOutcome::Deny, message.clone(), None),
        PermissionDecision::Answer { .. } => (PermissionOutcome::Allow, String::new(), None),
    };

    Ok(WirePermissionDecision {
        outcome: outcome.into(),
        reason,
        updated_input,
        remember_for_session: decision.remembers_for_session(),
        answers: decision.answers().map(answers_to_wire).unwrap_or_default(),
        ..WirePermissionDecision::default()
    })
}

/// Wire → domain — the direction a Worker actually runs.
///
/// `can_rewrite_input` is the adapter's
/// [`Capability::RewritesToolInput`](horde_adapter::Capability::RewritesToolInput)
/// claim. When a decision carries replacement arguments and the adapter cannot
/// apply them, this fails rather than downgrading to a plain allow.
pub fn permission_decision_from_wire(
    decision: &WirePermissionDecision,
    can_rewrite_input: bool,
) -> Result<PermissionDecision, ConversionError> {
    let outcome = PermissionOutcome::try_from(decision.outcome)
        .map_err(|_| ConversionError::UnusableOutcome(decision.outcome))?;

    if !decision.answers.is_empty() {
        // Answers settle a question; they mean nothing beside a refusal or a
        // narrowing, and guessing which half the client meant is how a
        // declined question would run anyway.
        return match (outcome, &decision.updated_input) {
            (PermissionOutcome::Allow, None) => Ok(PermissionDecision::answer(answers_from_wire(
                &decision.answers,
            ))),
            (PermissionOutcome::Allow, Some(_)) => Err(ConversionError::AnswersWithRewrite),
            _ => Err(ConversionError::AnswersWithoutAllow),
        };
    }

    match outcome {
        PermissionOutcome::Allow => {
            let allowed = match &decision.updated_input {
                Some(rewritten) => {
                    if !can_rewrite_input {
                        return Err(ConversionError::RewriteUnsupported);
                    }
                    PermissionDecision::allow_with(struct_to_json(rewritten))
                }
                None => PermissionDecision::allow(),
            };
            // The Server owns the session memory either way; the bit rides to
            // the adapter so a harness with its own session-grant vocabulary
            // (codex's `acceptForSession`) can encode the same scope.
            Ok(if decision.remember_for_session {
                allowed.remembered()
            } else {
                allowed
            })
        }
        PermissionOutcome::Deny => Ok(PermissionDecision::deny(decision.reason.clone())),
        PermissionOutcome::Unspecified => Err(ConversionError::UnusableOutcome(decision.outcome)),
    }
}

#[cfg(test)]
mod tests {
    use super::{ConversionError, permission_decision_from_wire, permission_decision_to_wire};
    use crate as horde_wire_self;
    use horde_adapter::Capability;
    use horde_adapter::permission::PermissionDecision;
    use horde_proto::prost_types::Struct;
    use horde_proto::v1::{PermissionDecision as WirePermissionDecision, PermissionOutcome};
    use serde_json::json;

    #[test]
    fn a_narrowing_allow_round_trips_its_arguments() {
        let narrowed = PermissionDecision::allow_with(json!({
            "path": "/workspace/atlas",
            "recursive": false,
            "depth": 3,
            "tags": ["a", "b"],
            "nested": { "keep": null },
        }));

        let wire = permission_decision_to_wire(&narrowed).expect("object input converts");
        let back = permission_decision_from_wire(&wire, true).expect("adapter can rewrite");

        assert_eq!(back, narrowed, "the narrowing must survive both directions");
    }

    #[test]
    fn a_bare_allow_stays_bare() {
        let wire = permission_decision_to_wire(&PermissionDecision::allow()).expect("converts");
        assert_eq!(wire.updated_input, None);
        assert!(!wire.remember_for_session, "nobody asked to remember");
        assert_eq!(
            permission_decision_from_wire(&wire, false),
            Ok(PermissionDecision::allow()),
            "no rewrite means no capability is needed"
        );
    }

    /// The remember bit crosses the seam in both directions. Without it, an
    /// adapter whose harness caches session grants itself (codex's
    /// `acceptForSession`) could never learn the human asked for that scope.
    #[test]
    fn remember_for_session_survives_both_directions() {
        let remembered = PermissionDecision::allow().remembered();
        let wire = permission_decision_to_wire(&remembered).expect("converts");
        assert!(wire.remember_for_session);
        assert_eq!(
            permission_decision_from_wire(&wire, false),
            Ok(remembered),
            "the bit must reach the adapter, not just the Server's memory"
        );

        // And a denial never smuggles one, whatever the wire claims: the
        // domain type cannot represent a remembered refusal.
        let denial_claiming_memory = WirePermissionDecision {
            outcome: PermissionOutcome::Deny.into(),
            reason: "no".to_owned(),
            remember_for_session: true,
            ..WirePermissionDecision::default()
        };
        let decoded =
            permission_decision_from_wire(&denial_claiming_memory, false).expect("decodes");
        assert!(!decoded.remembers_for_session());
    }

    #[test]
    fn a_narrowing_decision_is_undeliverable_to_a_worker_that_cannot_rewrite() {
        // End to end: the Server reads the Worker's advertisement, and a
        // decision it would have sent anyway is refused rather than silently
        // widening the permission.
        use horde_wire_self::capability::advertises_tool_input_rewrite;

        let codex_advertisement: Vec<i32> = [Capability::Resumable, Capability::SandboxTierAtSpawn]
            .into_iter()
            .filter_map(horde_wire_self::capability::capability_to_wire)
            .map(i32::from)
            .collect();

        let narrowing =
            permission_decision_to_wire(&PermissionDecision::allow_with(json!({"path": "/safe"})))
                .expect("converts");

        let can_rewrite = advertises_tool_input_rewrite(&codex_advertisement);
        assert!(!can_rewrite);
        assert_eq!(
            permission_decision_from_wire(&narrowing, can_rewrite),
            Err(ConversionError::RewriteUnsupported)
        );
    }

    #[test]
    fn a_rewrite_is_refused_rather_than_dropped() {
        // The whole point of the proto's reject-don't-drop note: silently
        // discarding the replacement arguments would run the original call.
        let wire =
            permission_decision_to_wire(&PermissionDecision::allow_with(json!({"path": "/safe"})))
                .expect("converts");

        assert_eq!(
            permission_decision_from_wire(&wire, false),
            Err(ConversionError::RewriteUnsupported)
        );
    }

    /// Answers cross both directions intact, and a question's ALLOW without
    /// them stays a plain allow (the Server is where that is refused).
    #[test]
    fn answers_round_trip_and_never_ride_a_refusal_or_a_narrowing() {
        use horde_adapter::permission::Answers;

        let answers = Answers::from([
            ("colour".to_owned(), vec!["Red".to_owned()]),
            ("sizes".to_owned(), vec!["S".to_owned(), "M".to_owned()]),
        ]);
        let answered = PermissionDecision::answer(answers.clone());
        let wire = permission_decision_to_wire(&answered).expect("converts");
        assert_eq!(wire.outcome, i32::from(PermissionOutcome::Allow));
        assert_eq!(wire.updated_input, None);
        assert!(!wire.remember_for_session, "an answer is never remembered");
        assert_eq!(wire.answers.len(), 2);
        assert_eq!(
            permission_decision_from_wire(&wire, false),
            Ok(answered),
            "answering needs no rewrite capability"
        );

        let refusal = WirePermissionDecision {
            outcome: PermissionOutcome::Deny.into(),
            ..wire.clone()
        };
        assert_eq!(
            permission_decision_from_wire(&refusal, true),
            Err(ConversionError::AnswersWithoutAllow)
        );
        let unspecified = WirePermissionDecision {
            outcome: PermissionOutcome::Unspecified.into(),
            ..wire.clone()
        };
        assert_eq!(
            permission_decision_from_wire(&unspecified, true),
            Err(ConversionError::AnswersWithoutAllow)
        );
        let both = WirePermissionDecision {
            updated_input: Some(Struct::default()),
            ..wire
        };
        assert_eq!(
            permission_decision_from_wire(&both, true),
            Err(ConversionError::AnswersWithRewrite)
        );
    }

    #[test]
    fn questions_cross_the_seam_field_for_field() {
        use horde_adapter::permission::{Question, QuestionOption};

        let questions = vec![Question {
            id: "colour".to_owned(),
            header: "Colour".to_owned(),
            question: "Which?".to_owned(),
            options: vec![QuestionOption {
                label: "Red".to_owned(),
                description: "warm".to_owned(),
            }],
            multi_select: true,
            allows_free_text: true,
            secret: true,
        }];
        let wire = horde_wire_self::permission::questions_to_wire(&questions);
        assert_eq!(wire[0].options[0].description, "warm");
        assert!(wire[0].multi_select && wire[0].allows_free_text && wire[0].secret);
        assert_eq!(
            horde_wire_self::permission::questions_from_wire(&wire),
            questions
        );

        let call = horde_adapter::ProposedToolCall::new(
            horde_adapter::ToolCallId::new("t"),
            "AskUserQuestion",
            json!({}),
        )
        .asking(questions.clone());
        let on_wire =
            horde_wire_self::permission::proposed_tool_call_to_wire(&call).expect("converts");
        assert_eq!(
            horde_wire_self::permission::questions_from_wire(&on_wire.questions),
            questions
        );
    }

    #[test]
    fn every_conversion_error_explains_itself() {
        for (error, needle) in [
            (ConversionError::AnswersWithRewrite, "both answers"),
            (
                ConversionError::AnswersWithoutAllow,
                "without outcome ALLOW",
            ),
        ] {
            assert!(error.to_string().contains(needle), "{error}");
        }
    }

    #[test]
    fn a_denial_carries_its_message() {
        let denial = PermissionDecision::deny("outside the workspace");
        let wire = permission_decision_to_wire(&denial).expect("converts");

        assert_eq!(wire.outcome, i32::from(PermissionOutcome::Deny));
        assert_eq!(wire.reason, "outside the workspace");
        assert_eq!(permission_decision_from_wire(&wire, true), Ok(denial));
    }

    #[test]
    fn an_empty_rewrite_is_not_the_same_as_no_rewrite() {
        // "Allow with no arguments" is a real instruction and must not decay
        // into "allow as proposed".
        let emptied = WirePermissionDecision {
            outcome: PermissionOutcome::Allow.into(),
            updated_input: Some(Struct::default()),
            ..WirePermissionDecision::default()
        };
        let as_proposed = WirePermissionDecision {
            outcome: PermissionOutcome::Allow.into(),
            updated_input: None,
            ..WirePermissionDecision::default()
        };

        assert_eq!(
            permission_decision_from_wire(&emptied, true),
            Ok(PermissionDecision::allow_with(json!({})))
        );
        assert_eq!(
            permission_decision_from_wire(&as_proposed, true),
            Ok(PermissionDecision::allow())
        );
        assert_ne!(
            permission_decision_from_wire(&emptied, true),
            permission_decision_from_wire(&as_proposed, true)
        );
    }

    #[test]
    fn integers_survive_the_double_only_representation() {
        // protobuf Struct has no integer type. Integral values come back as
        // integers rather than 3.0, because a harness handed {"depth": 3.0}
        // may treat it differently from the {"depth": 3} the chain meant.
        let wire = permission_decision_to_wire(&PermissionDecision::allow_with(
            json!({ "depth": 3, "ratio": 0.5, "negative": -7 }),
        ))
        .expect("converts");

        let back = permission_decision_from_wire(&wire, true).expect("converts back");
        assert_eq!(
            back.updated_input(),
            Some(&json!({ "depth": 3, "ratio": 0.5, "negative": -7 }))
        );
    }

    #[test]
    fn an_integer_too_large_for_a_double_is_refused() {
        // 2^53 + 1 cannot round-trip through a double. Silently altering an
        // argument inside a permission decision is worse than failing.
        let huge = PermissionDecision::allow_with(json!({ "id": 9_007_199_254_740_993_i64 }));
        assert_eq!(
            permission_decision_to_wire(&huge),
            Err(ConversionError::UnrepresentableNumber(
                "9007199254740993".to_owned()
            ))
        );
    }

    #[test]
    fn a_non_object_rewrite_is_an_error_not_a_truncation() {
        let bad = PermissionDecision::allow_with(json!(["not", "an", "object"]));
        assert_eq!(
            permission_decision_to_wire(&bad),
            Err(ConversionError::NotAnObject)
        );
    }

    #[test]
    fn an_unusable_outcome_is_never_guessed() {
        // Defaulting to allow would be a security failure; defaulting to deny
        // would stall agents against a newer Server. Both are refused.
        for outcome in [0, 9_999] {
            let wire = WirePermissionDecision {
                outcome,
                ..WirePermissionDecision::default()
            };
            assert_eq!(
                permission_decision_from_wire(&wire, true),
                Err(ConversionError::UnusableOutcome(outcome))
            );
        }
    }

    /// Every variant's message, because a conversion error's whole job is to
    /// tell a human which of four different refusals happened. An untested
    /// Display is a message nobody has ever read.
    #[test]
    fn every_conversion_error_says_which_refusal_it_is() {
        let unrepresentable = ConversionError::UnrepresentableNumber("9007199254740993".to_owned());
        let cases = [
            (
                ConversionError::NotAnObject,
                "updated_input must be a JSON object",
            ),
            (
                unrepresentable,
                "number 9007199254740993 has no exact f64 representation",
            ),
            (
                ConversionError::UnusableOutcome(9_999),
                "unusable PermissionOutcome 9999",
            ),
            (
                ConversionError::RewriteUnsupported,
                "decision rewrites tool input but the adapter cannot",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn a_conversion_error_is_a_std_error() {
        let error: &dyn core::error::Error = &ConversionError::NotAnObject;
        assert!(error.source().is_none());
    }
}

#[cfg(test)]
mod proposed_tool_call_tests {
    use horde_adapter::ToolCallId;
    use horde_adapter::permission::ProposedToolCall;
    use serde_json::json;

    use super::{ConversionError, proposed_tool_call_to_wire, struct_to_json};

    fn call(input: serde_json::Value) -> ProposedToolCall {
        ProposedToolCall::new(ToolCallId::new("toolu_01"), "Bash", input)
    }

    #[test]
    fn a_proposal_carries_its_arguments_structurally() {
        let proposed = call(json!({
            "command": "docker rm horde-test",
            "timeout": 30,
            "flags": ["-f"],
        }));

        let wire = proposed_tool_call_to_wire(&proposed).expect("an object converts");

        assert_eq!(wire.tool_name, "Bash");
        let arguments = struct_to_json(wire.input.as_ref().expect("arguments survive"));
        assert_eq!(
            arguments,
            json!({ "command": "docker rm horde-test", "timeout": 30, "flags": ["-f"] }),
            "deterministic rules and the judge must read the values the harness sent"
        );
    }

    /// The rendering is what a human approves from a phone. It must name the
    /// tool and show the arguments — an empty or tool-less rendering turns the
    /// approval UI into a yes/no with no question.
    #[test]
    fn the_rendering_names_the_tool_and_shows_the_arguments() {
        let wire = proposed_tool_call_to_wire(&call(json!({ "command": "docker rm horde-test" })))
            .expect("converts");

        assert!(wire.rendered.contains("Bash"), "got {}", wire.rendered);
        assert!(
            wire.rendered.contains("docker rm horde-test"),
            "got {}",
            wire.rendered
        );
    }

    /// Absent and empty are different states, exactly as they are on
    /// `updated_input`: a call with no arguments is still a concrete call.
    #[test]
    fn a_call_with_no_arguments_is_an_empty_struct_not_an_absence() {
        let wire = proposed_tool_call_to_wire(&call(json!({}))).expect("converts");
        let input = wire.input.expect("an empty bag is still a bag");
        assert!(input.fields.is_empty());
    }

    #[test]
    fn a_non_object_proposal_is_refused_rather_than_truncated() {
        for input in [json!("bare string"), json!([1, 2]), json!(3)] {
            assert_eq!(
                proposed_tool_call_to_wire(&call(input)),
                Err(ConversionError::NotAnObject)
            );
        }
    }

    /// The same 2^53 rule as the decision path: silently altering an argument
    /// inside a permission artifact is worse than failing loudly.
    #[test]
    fn an_unrepresentable_argument_is_refused() {
        assert_eq!(
            proposed_tool_call_to_wire(&call(json!({ "id": 9_007_199_254_740_993_i64 }))),
            Err(ConversionError::UnrepresentableNumber(
                "9007199254740993".to_owned()
            ))
        );
    }
}
