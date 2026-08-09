//! Assembles decoded envelopes into the provider-neutral event vocabulary.
//!
//! A pure state machine: envelopes in, [`AgentEvent`]s out, no clock and no
//! I/O. Everything time-related comes from the harness's own numbers, so
//! replaying a recorded transcript produces byte-identical results to driving a
//! live process — which is what makes the conformance suite meaningful rather
//! than decorative.

use core::time::Duration;

use horde_adapter::{
    AgentEvent, HeadroomSource, OutputChannel, PermissionDenial, ProviderSessionId, RateWindow,
    TokenUsage, ToolCallId, TurnId, TurnOutcome, TurnStatus, UsageAccumulator,
};
use serde_json::Value;

use crate::wire::{ContentBlock, Envelope, Event, Message, TurnResult};

/// Number of variants in [`OutputChannel`], for the per-channel chunk counters.
const CHANNEL_COUNT: usize = 5;

const fn channel_index(channel: OutputChannel) -> usize {
    match channel {
        OutputChannel::Assistant => 0,
        OutputChannel::Reasoning => 1,
        OutputChannel::ToolCall => 2,
        OutputChannel::ToolResult => 3,
        OutputChannel::HarnessDiagnostic => 4,
    }
}

/// Turns a stream of [`Envelope`]s into a stream of [`AgentEvent`]s.
#[derive(Debug, Default)]
pub struct TurnAssembler {
    session_id: Option<String>,
    announced_session: bool,
    turn_ordinal: u64,
    chunk_counters: [u64; CHANNEL_COUNT],
    usage: UsageAccumulator,
}

impl TurnAssembler {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The harness conversation id seen so far.
    #[must_use]
    pub fn provider_session_id(&self) -> Option<ProviderSessionId> {
        self.session_id.as_deref().map(ProviderSessionId::new)
    }

    /// Feeds one envelope, returning the domain events it produced.
    ///
    /// Most envelopes produce none: thinking-token tallies, hook lifecycle, and
    /// partial-message deltas carry nothing the assembled messages do not.
    pub fn feed(&mut self, envelope: &Envelope) -> Vec<AgentEvent> {
        match &envelope.event {
            Event::Init(init) => self.begin_turn(&init.session_id, &init.model),
            Event::Assistant(message) => self.assistant_events(message),
            Event::User(message) => self.tool_result_events(message),
            Event::Result(result) => vec![self.end_turn(result)],
            Event::RateLimit(rate_limit) => vec![AgentEvent::HeadroomReported {
                window: RateWindow {
                    source: HeadroomSource::ProviderReported,
                    window: rate_limit.rate_limit_info.rate_limit_type.clone(),
                    allowed: rate_limit.rate_limit_info.status == "allowed",
                    resets_at_unix: rate_limit.rate_limit_info.resets_at,
                },
            }],
            Event::ControlResponse(_)
            | Event::StreamEvent
            | Event::System { .. }
            | Event::Unknown { .. } => Vec::new(),
        }
    }

    /// `system`/`init` opens each turn — it is a turn header, not a process
    /// handshake, and arrives once per user message.
    fn begin_turn(&mut self, session_id: &str, model: &str) -> Vec<AgentEvent> {
        let is_new_session = self.session_id.as_deref() != Some(session_id);
        if is_new_session {
            self.session_id = Some(session_id.to_owned());
            self.announced_session = false;
        }

        self.turn_ordinal = self.turn_ordinal.saturating_add(1);
        self.chunk_counters = [0; CHANNEL_COUNT];

        if self.announced_session {
            return Vec::new();
        }
        self.announced_session = true;
        vec![AgentEvent::SessionEstablished {
            provider_session_id: ProviderSessionId::new(session_id),
            model: model.to_owned(),
        }]
    }

    /// A stable, replay-deterministic turn identifier.
    ///
    /// Derived rather than random because a golden transcript must produce the
    /// same events every time it is replayed.
    fn turn_id(&self) -> TurnId {
        TurnId::new(format!(
            "{}#{}",
            self.session_id.as_deref().unwrap_or("unknown"),
            self.turn_ordinal
        ))
    }

    fn output(&mut self, channel: OutputChannel, text: String) -> AgentEvent {
        let slot = &mut self.chunk_counters[channel_index(channel)];
        let chunk_index = *slot;
        *slot = slot.saturating_add(1);
        AgentEvent::Output {
            turn_id: self.turn_id(),
            channel,
            text,
            chunk_index,
        }
    }

    fn assistant_events(&mut self, message: &Message) -> Vec<AgentEvent> {
        let mut events = Vec::new();
        for block in &message.message.content {
            match block {
                ContentBlock::Text { text } => {
                    events.push(self.output(OutputChannel::Assistant, text.clone()));
                }
                ContentBlock::Thinking { thinking } => {
                    events.push(self.output(OutputChannel::Reasoning, thinking.clone()));
                }
                ContentBlock::ToolUse { name, input, .. } => {
                    let rendered = format!("{name} {input}");
                    events.push(self.output(OutputChannel::ToolCall, rendered));
                }
                ContentBlock::ToolResult { .. } | ContentBlock::Other => {}
            }
        }
        events
    }

    /// Tool results arrive echoed back through the user role.
    fn tool_result_events(&mut self, message: &Message) -> Vec<AgentEvent> {
        let mut events = Vec::new();
        for block in &message.message.content {
            if let ContentBlock::ToolResult {
                tool_use_id,
                is_error,
                content,
            } = block
            {
                events.push(self.output(OutputChannel::ToolResult, render_content(content)));
                events.push(AgentEvent::ToolCompleted {
                    turn_id: self.turn_id(),
                    tool_call_id: ToolCallId::new(tool_use_id.clone()),
                    is_error: *is_error,
                });
            }
        }
        events
    }

    fn end_turn(&mut self, result: &TurnResult) -> AgentEvent {
        // Billing truth is `modelUsage`, not `usage`: the latter is per-turn but
        // counts only the requested model, while a turn can bill auxiliary
        // models too (a `--model sonnet` run billed haiku alongside it). The
        // former is a running total, which is what the accumulator is for.
        let cumulative = result
            .model_usage
            .values()
            .fold(TokenUsage::default(), |total, model| {
                total
                    + TokenUsage {
                        input_tokens: model.input_tokens,
                        output_tokens: model.output_tokens,
                        cache_read_input_tokens: model.cache_read_input_tokens,
                        cache_write_input_tokens: model.cache_creation_input_tokens,
                    }
            });

        let usage = self.usage.observe(cumulative, result.total_cost_usd);

        AgentEvent::TurnEnded(TurnOutcome {
            turn_id: self.turn_id(),
            status: turn_status(result),
            text: result.result.clone(),
            usage,
            denials: result
                .permission_denials
                .iter()
                .map(|denial| PermissionDenial {
                    tool_call_id: ToolCallId::new(denial.tool_use_id.clone()),
                    tool_name: denial.tool_name.clone(),
                    attempted_input: denial.tool_input.clone(),
                })
                .collect(),
            duration: Duration::from_millis(result.duration_ms),
        })
    }
}

/// Classifies a result envelope.
///
/// Order matters. An interrupted turn is flagged `is_error` and would otherwise
/// be reported as a failure, when in fact it is a turn that ended the way Horde
/// asked it to and left the session usable.
fn turn_status(result: &TurnResult) -> TurnStatus {
    if result.terminal_reason.as_deref() == Some("aborted_streaming") {
        return TurnStatus::Interrupted;
    }
    if result.succeeded() {
        return TurnStatus::Completed;
    }
    TurnStatus::Failed {
        message: result
            .result
            .clone()
            .unwrap_or_else(|| result.subtype.clone()),
    }
}

/// Renders a tool result's free-form content for display.
fn render_content(content: &Value) -> String {
    match content {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use horde_adapter::{AgentEvent, OutputChannel, TurnStatus};
    use serde_json::json;

    use super::TurnAssembler;
    use crate::codec::decode_line;

    fn feed(assembler: &mut TurnAssembler, line: &str) -> Vec<AgentEvent> {
        let envelope = decode_line(line).expect("test fixtures must decode");
        assembler.feed(&envelope)
    }

    fn init(session: &str) -> String {
        json!({"type":"system","subtype":"init","session_id":session,"model":"claude-haiku-4-5"})
            .to_string()
    }

    fn result_line(session: &str, extra: serde_json::Value) -> String {
        let mut value = json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "session_id": session,
            "duration_ms": 1000,
        });
        if let (Some(base), Some(extra)) = (value.as_object_mut(), extra.as_object()) {
            for (key, val) in extra {
                base.insert(key.clone(), val.clone());
            }
        }
        value.to_string()
    }

    #[test]
    fn the_session_is_announced_once_even_though_init_repeats_each_turn() {
        let mut assembler = TurnAssembler::new();

        let first = feed(&mut assembler, &init("s1"));
        assert!(matches!(
            first.as_slice(),
            [AgentEvent::SessionEstablished { .. }]
        ));

        let second = feed(&mut assembler, &init("s1"));
        assert!(
            second.is_empty(),
            "init is a turn header, not a new session: {second:?}"
        );
    }

    #[test]
    fn turn_ids_are_stable_and_advance_with_each_init() {
        let mut assembler = TurnAssembler::new();
        feed(&mut assembler, &init("s1"));
        let first = feed(&mut assembler, &result_line("s1", json!({})));
        feed(&mut assembler, &init("s1"));
        let second = feed(&mut assembler, &result_line("s1", json!({})));

        let ids: Vec<String> = [first, second]
            .iter()
            .flatten()
            .filter_map(|event| match event {
                AgentEvent::TurnEnded(outcome) => Some(outcome.turn_id.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec!["s1#1".to_owned(), "s1#2".to_owned()]);
    }

    #[test]
    fn chunk_indices_advance_per_channel_and_reset_each_turn() {
        let mut assembler = TurnAssembler::new();
        feed(&mut assembler, &init("s1"));
        let line = json!({"type":"assistant","message":{"role":"assistant","content":[
            {"type":"thinking","thinking":"hmm"},
            {"type":"text","text":"one"},
            {"type":"text","text":"two"}]}})
        .to_string();

        let events = feed(&mut assembler, &line);
        let indices: Vec<(OutputChannel, u64)> = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::Output {
                    channel,
                    chunk_index,
                    ..
                } => Some((*channel, *chunk_index)),
                _ => None,
            })
            .collect();
        assert_eq!(
            indices,
            vec![
                (OutputChannel::Reasoning, 0),
                (OutputChannel::Assistant, 0),
                (OutputChannel::Assistant, 1),
            ]
        );

        feed(&mut assembler, &result_line("s1", json!({})));
        feed(&mut assembler, &init("s1"));
        let next = feed(&mut assembler, &line);
        let first_assistant = next.iter().find_map(|event| match event {
            AgentEvent::Output {
                channel: OutputChannel::Assistant,
                chunk_index,
                ..
            } => Some(*chunk_index),
            _ => None,
        });
        assert_eq!(first_assistant, Some(0), "counters must reset per turn");
    }

    #[test]
    fn an_aborted_turn_is_interrupted_rather_than_failed() {
        let mut assembler = TurnAssembler::new();
        feed(&mut assembler, &init("s1"));
        let events = feed(
            &mut assembler,
            &result_line(
                "s1",
                json!({
                    "subtype": "error_during_execution",
                    "is_error": true,
                    "terminal_reason": "aborted_streaming"
                }),
            ),
        );

        match events.as_slice() {
            [AgentEvent::TurnEnded(outcome)] => {
                assert_eq!(outcome.status, TurnStatus::Interrupted);
            }
            other => panic!("expected one TurnEnded, got {other:?}"),
        }
    }

    /// The trap the wire format sets: `subtype` says success, `is_error` says
    /// otherwise, and only the second one is telling the truth.
    #[test]
    fn a_successful_looking_result_carrying_an_error_flag_is_a_failure() {
        let mut assembler = TurnAssembler::new();
        feed(&mut assembler, &init("s1"));
        let events = feed(
            &mut assembler,
            &result_line(
                "s1",
                json!({
                    "subtype": "success",
                    "is_error": true,
                    "terminal_reason": "api_error",
                    "result": "Not logged in \u{b7} Please run /login"
                }),
            ),
        );

        match events.as_slice() {
            [AgentEvent::TurnEnded(outcome)] => match &outcome.status {
                TurnStatus::Failed { message } => assert!(message.contains("Not logged in")),
                other => panic!("expected Failed, got {other:?}"),
            },
            other => panic!("expected one TurnEnded, got {other:?}"),
        }
    }

    #[test]
    fn cumulative_model_usage_is_reported_as_per_turn_deltas() {
        let mut assembler = TurnAssembler::new();
        let model_usage = |output: u64, cost: f64| {
            json!({
                "total_cost_usd": cost,
                "modelUsage": { "claude-haiku-4-5": { "inputTokens": 10, "outputTokens": output, "costUSD": cost } }
            })
        };

        let mut outputs = Vec::new();
        for (output, cost) in [(451_u64, 0.0037098), (654, 0.0063184), (713, 0.0077755)] {
            feed(&mut assembler, &init("s1"));
            for event in feed(
                &mut assembler,
                &result_line("s1", model_usage(output, cost)),
            ) {
                if let AgentEvent::TurnEnded(outcome) = event {
                    outputs.push(outcome.usage.tokens.output_tokens);
                }
            }
        }

        assert_eq!(outputs, vec![451, 203, 59]);
    }

    #[test]
    fn a_denied_tool_call_is_carried_into_the_turn_outcome() {
        let mut assembler = TurnAssembler::new();
        feed(&mut assembler, &init("s1"));
        let events = feed(
            &mut assembler,
            &result_line(
                "s1",
                json!({"permission_denials": [
                    {"tool_name":"Write","tool_use_id":"toolu_01","tool_input":{"content":"BLOCKED"}}
                ]}),
            ),
        );

        match events.as_slice() {
            [AgentEvent::TurnEnded(outcome)] => {
                assert_eq!(outcome.denials.len(), 1);
                assert_eq!(outcome.denials[0].tool_name, "Write");
                assert!(
                    !outcome.is_clean_success(),
                    "a turn that hit a denial is not a clean success"
                );
            }
            other => panic!("expected one TurnEnded, got {other:?}"),
        }
    }

    #[test]
    fn a_tool_result_reports_completion_and_its_text() {
        let mut assembler = TurnAssembler::new();
        feed(&mut assembler, &init("s1"));
        let line = json!({"type":"user","message":{"role":"user","content":[
            {"type":"tool_result","tool_use_id":"toolu_01","content":"File created","is_error":false}]}})
        .to_string();

        let events = feed(&mut assembler, &line);
        match events.as_slice() {
            [
                AgentEvent::Output {
                    channel: OutputChannel::ToolResult,
                    text,
                    ..
                },
                AgentEvent::ToolCompleted {
                    tool_call_id,
                    is_error,
                    ..
                },
            ] => {
                assert_eq!(text, "File created");
                assert_eq!(tool_call_id.as_str(), "toolu_01");
                assert!(!is_error);
            }
            other => panic!("unexpected events: {other:?}"),
        }
    }

    #[test]
    fn rate_limit_state_becomes_a_headroom_report() {
        let mut assembler = TurnAssembler::new();
        let line = json!({"type":"rate_limit_event","rate_limit_info":{
            "status":"allowed","rateLimitType":"five_hour","resetsAt":1786317600_i64}})
        .to_string();

        match feed(&mut assembler, &line).as_slice() {
            [AgentEvent::HeadroomReported { window }] => {
                assert!(window.allowed);
                assert_eq!(window.window, "five_hour");
                assert_eq!(window.resets_at_unix, Some(1786317600));
            }
            other => panic!("expected one HeadroomReported, got {other:?}"),
        }
    }
}
