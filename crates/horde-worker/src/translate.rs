//! What an adapter's events look like in the Session log.
//!
//! One direction only — [`AgentEvent`] to [`JournalEntry`] — because the
//! Worker is a producer here: nothing ever turns a journaled `horde.v1.Event`
//! back into an adapter event. The value-level conversions (arguments, usage,
//! rate windows) go through `horde-wire`, which owns the correspondence; this
//! module owns only the *event vocabulary* mapping, and the decisions below
//! about what is journaled at all.
//!
//! # Recorded decisions
//!
//! * **`SessionEstablished` journals a lifecycle transition, not the provider
//!   session id as data.** The wire vocabulary has no session-established
//!   kind, and the revival identity is Worker-local by design — the Worker,
//!   not the Server, revives agents (`RevivalIdentity`). The Session log gets
//!   the honest observable fact: the agent went from starting to running, on
//!   which model.
//! * **`ToolCompleted` journals nothing.** The tool's output already rode
//!   `AgentOutputChunk` on the tool-result channel, and the wire has no
//!   completion kind; inventing one out of a lifecycle event would be a side
//!   channel wearing an event's name.
//! * **`TurnEnded` denials journal nothing.** Every denial an adapter reports
//!   was a decision the chain (Server-side) already made and journaled as
//!   `PermissionDecided`; the adapter's copy is an audit convenience, and
//!   journaling it again would double-count refusals in every client.
//! * **The permission `request_id` is the harness's `tool_call_id`.** The wire
//!   `ProposedToolCall` carries no id, so `PermissionRequest.request_id` is
//!   the only correlation between a request and the decision that answers it.
//!   The tool call id is stable, harness-minted, and already what
//!   [`AgentSession::answer_permission`](horde_adapter::AgentSession) wants
//!   back — minting anything else would force a translation table someone has
//!   to persist.

use horde_adapter::{AdapterError, AgentEvent, OutputChannel, TurnOutcome, TurnStatus};
use horde_proto::v1::event::Kind;
use horde_proto::v1::{
    AgentId, AgentLifecycleChanged, AgentLifecycleState, AgentOutputChannel, AgentOutputChunk,
    ModelBinding, PermissionRequest, PermissionRequestId, PermissionRequested, UsageRecord,
    UsageRecorded,
};
use horde_wire::{proposed_tool_call_to_wire, rate_window_to_wire, token_usage_to_wire};

use crate::error::WorkerError;
use crate::journal::JournalEntry;

/// Failure-reason category: the harness process is gone.
///
/// The scheduler's answer is a retry (or a revival) on this Worker — the
/// binding is fine, the process is not.
pub const PROCESS_DEATH: &str = "the harness process died";

/// Failure-reason category: the provider refused the work.
///
/// Bad credentials, an unclaimed capability, a turn the API errored — the
/// scheduler's answer is a different account or a different provider, and
/// retrying here only burns time.
pub const REFUSAL: &str = "the provider refused";

/// Failure-reason category: the harness's output could not be understood.
///
/// The protocol moved, or the stream is corrupt. Diagnosed from the raw line
/// the error carries; retrying may work, upgrading the adapter may be due.
pub const MALFORMED_STREAM: &str = "the harness stream was malformed";

/// Classifies an adapter error into the lifecycle-reason taxonomy.
///
/// `AgentLifecycleChanged.reason` is free text on the wire, so the taxonomy
/// lives in a stable prefix: process death, refusal, and malformed stream ask
/// for three different responses (retry here, rebind elsewhere, fix the
/// adapter), and a reason that does not say which it is leaves the reader
/// rediagnosing from scratch.
#[must_use]
pub fn failure_reason(error: &AdapterError) -> String {
    let category = match error {
        AdapterError::Spawn { .. } | AdapterError::HarnessExited { .. } | AdapterError::Io(_) => {
            PROCESS_DEATH
        }
        AdapterError::Protocol { .. } => MALFORMED_STREAM,
        AdapterError::Unauthenticated { .. }
        | AdapterError::RefusedVersion { .. }
        | AdapterError::Unsupported { .. }
        | AdapterError::UnknownPermissionRequest { .. }
        | AdapterError::AwaitingDecision { .. } => REFUSAL,
    };
    format!("{category}: {error}")
}

/// The standing facts about one agent that its events are journaled under.
///
/// Everything here arrived on the `SpawnAgentCommand`; the pump holds one per
/// agent and stamps it on every translation.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentContext {
    pub session_id: String,
    pub agent_id: String,
    /// What the scheduler bound, carried onto usage records for attribution.
    pub binding: Option<ModelBinding>,
    /// The named permission mode in force, carried onto permission requests.
    pub permission_mode: String,
    /// Ancestry display names, root first, carried onto permission requests
    /// so an approver sees where in the tree the ask came from.
    pub tree_path: Vec<String>,
}

impl AgentContext {
    fn entry(&self, idempotency_key: String, kind: Kind) -> JournalEntry {
        JournalEntry {
            session_id: self.session_id.clone(),
            agent_id: Some(self.agent_id.clone()),
            idempotency_key,
            kind,
        }
    }
}

/// Translates one adapter event into the journal entries it causes.
///
/// Zero entries is a real answer (see the module doc's recorded decisions),
/// and so is two — a failed turn is both a usage record and a lifecycle
/// change.
///
/// # Errors
///
/// [`WorkerError::Translate`] when a value cannot be put on the wire — a
/// non-object argument bag or an unrepresentable number. Fatal to the event
/// rather than smoothed over: a permission request journaled without its
/// arguments would be approved blind.
pub fn journal_entries(
    context: &AgentContext,
    event: &AgentEvent,
) -> Result<Vec<JournalEntry>, WorkerError> {
    Ok(match event {
        AgentEvent::SessionEstablished {
            provider_session_id,
            model,
        } => vec![context.entry(
            // Minted, not derived: a revived agent re-establishes the SAME
            // provider session, and a key derived from it would make the
            // server dedupe the second incarnation's Running transition away —
            // the agent would read as forever Starting after every revival.
            String::new(),
            Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                previous_state: AgentLifecycleState::Starting.into(),
                state: AgentLifecycleState::Running.into(),
                reason: format!("provider session {provider_session_id} established on {model}"),
            }),
        )],

        AgentEvent::Output {
            turn_id,
            channel,
            text,
            chunk_index,
        } => vec![context.entry(
            format!(
                "{}:{turn_id}:{}:{chunk_index}",
                context.agent_id,
                channel_code(*channel)
            ),
            Kind::AgentOutputChunk(AgentOutputChunk {
                channel: channel_to_wire(*channel).into(),
                text: text.clone(),
                turn_id: turn_id.to_string(),
                chunk_index: *chunk_index,
            }),
        )],

        AgentEvent::PermissionRequested { request, .. } => vec![context.entry(
            format!("{}:{}:requested", context.agent_id, request.tool_call_id),
            Kind::PermissionRequested(PermissionRequested {
                request: Some(PermissionRequest {
                    request_id: Some(PermissionRequestId::new(request.tool_call_id.as_str())),
                    agent_id: Some(AgentId::new(context.agent_id.clone())),
                    tool_call: Some(proposed_tool_call_to_wire(request)?),
                    tree_path: context.tree_path.clone(),
                    permission_mode: context.permission_mode.clone(),
                    // The Worker runs no chain layer; saying which layer
                    // escalated would be inventing one.
                    escalated_from: 0,
                    expires_at: None,
                    // A harness escalation proposes a tool call, never a
                    // forward; the forward verbs raise their own requests
                    // through the endpoint's broker (ADR-0006).
                    forward: None,
                }),
            }),
        )],

        AgentEvent::ToolCompleted { .. } => Vec::new(),

        AgentEvent::HeadroomReported { window } => vec![context.entry(
            // Auto-minted: headroom reports repeat and carry no natural key.
            String::new(),
            Kind::UsageRecorded(UsageRecorded {
                record: Some(UsageRecord {
                    binding: context.binding.clone(),
                    tokens: None,
                    rate_window: Some(rate_window_to_wire(window)),
                    turn_id: String::new(),
                }),
            }),
        )],

        AgentEvent::TurnEnded(outcome) => turn_ended_entries(context, outcome),
    })
}

/// A turn end is always a usage record, and additionally a lifecycle failure
/// when the harness failed the turn.
fn turn_ended_entries(context: &AgentContext, outcome: &TurnOutcome) -> Vec<JournalEntry> {
    let mut entries = vec![context.entry(
        format!("{}:{}:usage", context.agent_id, outcome.turn_id),
        Kind::UsageRecorded(UsageRecorded {
            record: Some(UsageRecord {
                binding: context.binding.clone(),
                tokens: Some(token_usage_to_wire(outcome.usage.tokens)),
                rate_window: None,
                turn_id: outcome.turn_id.to_string(),
            }),
        }),
    )];

    if let TurnStatus::Failed { message } = &outcome.status {
        entries.push(context.entry(
            format!("{}:{}:failed", context.agent_id, outcome.turn_id),
            Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                previous_state: AgentLifecycleState::Running.into(),
                state: AgentLifecycleState::Failed.into(),
                // A failed turn is the provider refusing the work — the
                // process is alive and the stream decoded; what broke is the
                // turn itself. Prefixed so the taxonomy holds here too.
                reason: format!("{REFUSAL}: {message}"),
            }),
        ));
    }
    entries
}

/// Domain output channel → wire, exhaustively.
///
/// Lives here rather than in `horde-wire` for now because the Worker is the
/// only converter of output channels in the workspace; if a second consumer
/// appears, this match moves there under the single-owner rule.
fn channel_to_wire(channel: OutputChannel) -> AgentOutputChannel {
    match channel {
        OutputChannel::Assistant => AgentOutputChannel::Assistant,
        OutputChannel::Reasoning => AgentOutputChannel::Reasoning,
        OutputChannel::ToolCall => AgentOutputChannel::ToolCall,
        OutputChannel::ToolResult => AgentOutputChannel::ToolResult,
        OutputChannel::HarnessDiagnostic => AgentOutputChannel::HarnessDiagnostic,
    }
}

/// A short stable code for idempotency keys — `Debug` formatting is not a
/// stable identifier, and a renamed variant must not re-key old events.
fn channel_code(channel: OutputChannel) -> &'static str {
    match channel {
        OutputChannel::Assistant => "assistant",
        OutputChannel::Reasoning => "reasoning",
        OutputChannel::ToolCall => "tool-call",
        OutputChannel::ToolResult => "tool-result",
        OutputChannel::HarnessDiagnostic => "diagnostic",
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use horde_adapter::permission::ProposedToolCall;
    use horde_adapter::usage::{HeadroomSource, RateWindow, Utilization};
    use horde_adapter::{
        AgentEvent, OutputChannel, ProviderSessionId, TokenUsage, ToolCallId, TurnId, TurnOutcome,
        TurnStatus, UsageDelta,
    };
    use horde_proto::v1::event::Kind;
    use horde_proto::v1::{AgentLifecycleState, AgentOutputChannel, ModelBinding};
    use serde_json::json;

    use super::{AgentContext, journal_entries};
    use crate::error::WorkerError;

    fn context() -> AgentContext {
        AgentContext {
            session_id: "s1".to_owned(),
            agent_id: "agent-1".to_owned(),
            binding: Some(ModelBinding {
                tier: "standard".to_owned(),
                provider: "fake".to_owned(),
                model: "fake-model".to_owned(),
                provider_account: "work".to_owned(),
            }),
            permission_mode: "cautious".to_owned(),
            tree_path: vec!["ci-fix".to_owned(), "reviewer".to_owned()],
        }
    }

    fn outcome(status: TurnStatus) -> TurnOutcome {
        TurnOutcome {
            turn_id: TurnId::new("t1"),
            status,
            text: Some("done".to_owned()),
            usage: UsageDelta {
                tokens: TokenUsage {
                    input_tokens: 10,
                    output_tokens: 20,
                    ..TokenUsage::default()
                },
                cost_usd: 0.001,
            },
            denials: Vec::new(),
            duration: Duration::from_secs(1),
        }
    }

    #[test]
    fn a_session_established_is_a_starting_to_running_transition() {
        let entries = journal_entries(
            &context(),
            &AgentEvent::SessionEstablished {
                provider_session_id: ProviderSessionId::new("sess-abc"),
                model: "fake-model".to_owned(),
            },
        )
        .expect("translates");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].session_id, "s1");
        assert_eq!(entries[0].agent_id.as_deref(), Some("agent-1"));
        assert!(
            entries[0].idempotency_key.is_empty(),
            "minted per incarnation: a key derived from the session id would \
             make the server dedupe a revival's Running transition away"
        );
        match &entries[0].kind {
            Kind::AgentLifecycleChanged(changed) => {
                assert_eq!(
                    changed.previous_state,
                    i32::from(AgentLifecycleState::Starting)
                );
                assert_eq!(changed.state, i32::from(AgentLifecycleState::Running));
                assert!(
                    changed.reason.contains("sess-abc") && changed.reason.contains("fake-model"),
                    "the reason is where the identity is human-visible: {}",
                    changed.reason
                );
            }
            other => panic!("expected a lifecycle change, got {other:?}"),
        }
    }

    /// Every channel, exhaustively, because a swapped arm renders reasoning as
    /// prose in every client and nothing else would notice.
    #[test]
    fn each_output_channel_maps_to_its_own_wire_channel() {
        let expectations = [
            (OutputChannel::Assistant, AgentOutputChannel::Assistant),
            (OutputChannel::Reasoning, AgentOutputChannel::Reasoning),
            (OutputChannel::ToolCall, AgentOutputChannel::ToolCall),
            (OutputChannel::ToolResult, AgentOutputChannel::ToolResult),
            (
                OutputChannel::HarnessDiagnostic,
                AgentOutputChannel::HarnessDiagnostic,
            ),
        ];

        for (domain, wire) in expectations {
            let entries = journal_entries(
                &context(),
                &AgentEvent::Output {
                    turn_id: TurnId::new("t1"),
                    channel: domain,
                    text: "text".to_owned(),
                    chunk_index: 3,
                },
            )
            .expect("translates");

            match &entries[0].kind {
                Kind::AgentOutputChunk(chunk) => {
                    assert_eq!(chunk.channel, i32::from(wire), "{domain:?}");
                    assert_eq!(chunk.turn_id, "t1");
                    assert_eq!(chunk.chunk_index, 3);
                    assert_eq!(chunk.text, "text");
                }
                other => panic!("expected an output chunk, got {other:?}"),
            }
        }
    }

    /// Every channel's key code, pinned as distinct and stable.
    ///
    /// The codes are identity, not display: two channels sharing a code would
    /// collide keys for the same turn and index (the server would silently
    /// drop one chunk as a duplicate), and a changed code re-keys events a
    /// journal already holds.
    #[test]
    fn channel_key_codes_are_distinct_and_stable() {
        let codes: Vec<&str> = [
            (OutputChannel::Assistant, "assistant"),
            (OutputChannel::Reasoning, "reasoning"),
            (OutputChannel::ToolCall, "tool-call"),
            (OutputChannel::ToolResult, "tool-result"),
            (OutputChannel::HarnessDiagnostic, "diagnostic"),
        ]
        .into_iter()
        .map(|(channel, expected)| {
            let code = super::channel_code(channel);
            assert_eq!(code, expected, "{channel:?} must keep its stored code");
            code
        })
        .collect();

        let deduplicated: std::collections::BTreeSet<&str> = codes.iter().copied().collect();
        assert_eq!(deduplicated.len(), codes.len(), "no two channels may share");
    }

    #[test]
    fn output_chunks_key_by_turn_channel_and_index() {
        let chunk = |channel, index| AgentEvent::Output {
            turn_id: TurnId::new("t1"),
            channel,
            text: "same text".to_owned(),
            chunk_index: index,
        };
        let key = |event: &AgentEvent| {
            journal_entries(&context(), event).expect("translates")[0]
                .idempotency_key
                .clone()
        };

        assert_ne!(
            key(&chunk(OutputChannel::Assistant, 0)),
            key(&chunk(OutputChannel::Assistant, 1)),
            "positions differ"
        );
        assert_ne!(
            key(&chunk(OutputChannel::Assistant, 0)),
            key(&chunk(OutputChannel::Reasoning, 0)),
            "channels number independently, so the key must carry the channel"
        );
    }

    #[test]
    fn a_permission_request_correlates_by_the_harness_tool_call_id() {
        let entries = journal_entries(
            &context(),
            &AgentEvent::PermissionRequested {
                turn_id: TurnId::new("t1"),
                request: ProposedToolCall {
                    tool_call_id: ToolCallId::new("toolu_07"),
                    tool_name: "Bash".to_owned(),
                    input: json!({ "command": "docker rm horde-test" }),
                    questions: Vec::new(),
                },
            },
        )
        .expect("translates");

        match &entries[0].kind {
            Kind::PermissionRequested(requested) => {
                let request = requested.request.as_ref().expect("a request");
                assert_eq!(
                    request.request_id.as_ref().map(ToString::to_string),
                    Some("toolu_07".to_owned()),
                    "the tool call id is the only correlation the decision can answer with"
                );
                assert_eq!(
                    request.agent_id.as_ref().map(ToString::to_string),
                    Some("agent-1".to_owned())
                );
                assert_eq!(request.permission_mode, "cautious");
                assert_eq!(request.tree_path, vec!["ci-fix", "reviewer"]);
                let call = request.tool_call.as_ref().expect("the call rides along");
                assert_eq!(call.tool_name, "Bash");
                assert!(
                    call.input.is_some(),
                    "arguments must arrive structurally or the chain judges blind"
                );
            }
            other => panic!("expected a permission request, got {other:?}"),
        }
    }

    #[test]
    fn a_permission_request_with_unwirable_arguments_fails_the_translation() {
        let error = journal_entries(
            &context(),
            &AgentEvent::PermissionRequested {
                turn_id: TurnId::new("t1"),
                request: ProposedToolCall {
                    tool_call_id: ToolCallId::new("toolu_08"),
                    tool_name: "Bash".to_owned(),
                    input: json!(["not", "an", "object"]),
                    questions: Vec::new(),
                },
            },
        )
        .expect_err("a request journaled without its arguments would be approved blind");
        assert!(matches!(error, WorkerError::Translate(_)));
    }

    /// The recorded decision, pinned: the tool's output already rode the
    /// tool-result channel, and the wire has no completion kind.
    #[test]
    fn a_tool_completion_journals_nothing() {
        let entries = journal_entries(
            &context(),
            &AgentEvent::ToolCompleted {
                turn_id: TurnId::new("t1"),
                tool_call_id: ToolCallId::new("toolu_07"),
                is_error: false,
            },
        )
        .expect("translates");
        assert!(entries.is_empty());
    }

    #[test]
    fn headroom_is_a_usage_record_carrying_only_the_rate_window() {
        let entries = journal_entries(
            &context(),
            &AgentEvent::HeadroomReported {
                window: RateWindow {
                    source: HeadroomSource::ProviderReported,
                    window: "five_hour".to_owned(),
                    utilization: Utilization::Fraction(0.25),
                    window_duration: None,
                    resets_at_unix: None,
                },
            },
        )
        .expect("translates");

        match &entries[0].kind {
            Kind::UsageRecorded(recorded) => {
                let record = recorded.record.as_ref().expect("a record");
                assert!(record.tokens.is_none(), "headroom is not consumption");
                assert!(record.rate_window.is_some());
                assert_eq!(
                    record.binding.as_ref().map(|b| b.provider.clone()),
                    Some("fake".to_owned()),
                    "attribution needs the binding"
                );
            }
            other => panic!("expected a usage record, got {other:?}"),
        }
        assert!(
            entries[0].idempotency_key.is_empty(),
            "headroom repeats with no natural key; the journal mints one"
        );
    }

    #[test]
    fn a_completed_turn_journals_its_usage_delta() {
        let entries = journal_entries(
            &context(),
            &AgentEvent::TurnEnded(outcome(TurnStatus::Completed)),
        )
        .expect("translates");

        assert_eq!(entries.len(), 1, "a clean turn is usage and nothing else");
        match &entries[0].kind {
            Kind::UsageRecorded(recorded) => {
                let record = recorded.record.as_ref().expect("a record");
                let tokens = record.tokens.as_ref().expect("the delta");
                assert_eq!(tokens.input_tokens, 10);
                assert_eq!(tokens.output_tokens, 20);
                assert_eq!(record.turn_id, "t1", "usage correlates to its turn");
            }
            other => panic!("expected a usage record, got {other:?}"),
        }
    }

    #[test]
    fn a_failed_turn_additionally_records_the_failure() {
        let entries = journal_entries(
            &context(),
            &AgentEvent::TurnEnded(outcome(TurnStatus::Failed {
                message: "harness exited with status 1".to_owned(),
            })),
        )
        .expect("translates");

        assert_eq!(entries.len(), 2, "usage still counts on a failed turn");
        match &entries[1].kind {
            Kind::AgentLifecycleChanged(changed) => {
                assert_eq!(changed.state, i32::from(AgentLifecycleState::Failed));
                assert_eq!(
                    changed.reason,
                    format!("{}: harness exited with status 1", super::REFUSAL),
                    "a failed turn is the provider refusing the work"
                );
            }
            other => panic!("expected a lifecycle change, got {other:?}"),
        }
    }

    /// The taxonomy, one variant per row: process death, refusal, and
    /// malformed stream ask for three different responses, so an error landing
    /// in the wrong category sends the scheduler to the wrong repair.
    #[test]
    fn each_adapter_failure_lands_in_its_own_category() {
        use horde_adapter::AdapterError;

        let expectations: Vec<(AdapterError, &str, &str)> = vec![
            (
                AdapterError::HarnessExited { exit_code: Some(3) },
                super::PROCESS_DEATH,
                "code 3",
            ),
            (
                AdapterError::Io(std::io::Error::other("pipe broke")),
                super::PROCESS_DEATH,
                "pipe broke",
            ),
            (
                AdapterError::Spawn {
                    provider: "claude".to_owned(),
                    message: "no such binary".to_owned(),
                },
                super::PROCESS_DEATH,
                "no such binary",
            ),
            (
                AdapterError::Protocol {
                    message: "not json".to_owned(),
                    raw: "{oops".to_owned(),
                },
                super::MALFORMED_STREAM,
                "{oops",
            ),
            (
                AdapterError::Unauthenticated {
                    provider: "claude".to_owned(),
                    account: Some("work".to_owned()),
                    message: "Not logged in".to_owned(),
                },
                super::REFUSAL,
                "Not logged in",
            ),
            (
                AdapterError::Unsupported {
                    provider: "fake".to_owned(),
                    capability: horde_adapter::Capability::InterruptibleTurn,
                },
                super::REFUSAL,
                "InterruptibleTurn",
            ),
        ];

        for (error, category, detail) in expectations {
            let reason = super::failure_reason(&error);
            assert!(
                reason.starts_with(category),
                "{error} must classify as {category}, got {reason}"
            );
            assert!(
                reason.contains(detail),
                "the category must not eat the diagnosis: {reason}"
            );
        }
    }

    /// The three categories must stay distinguishable — two sharing a prefix
    /// would silently merge in every reader that branches on them.
    #[test]
    fn the_categories_are_distinct() {
        let categories = [
            super::PROCESS_DEATH,
            super::REFUSAL,
            super::MALFORMED_STREAM,
        ];
        for (index, category) in categories.iter().enumerate() {
            for other in categories.iter().skip(index + 1) {
                assert!(
                    !category.starts_with(other) && !other.starts_with(category),
                    "{category:?} and {other:?} must not shadow each other"
                );
            }
        }
    }

    #[test]
    fn an_interrupted_turn_records_usage_only() {
        // The TurnInterrupted event is journaled where the interrupt was
        // applied — as the command's effect — not re-derived here.
        let entries = journal_entries(
            &context(),
            &AgentEvent::TurnEnded(outcome(TurnStatus::Interrupted)),
        )
        .expect("translates");
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0].kind, Kind::UsageRecorded(_)));
    }
}
