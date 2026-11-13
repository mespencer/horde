//! Assembles decoded envelopes into the provider-neutral event vocabulary.
//!
//! A pure state machine: envelopes in, [`AgentEvent`]s out, no clock and no
//! I/O. Everything time-related comes from the harness's own numbers, so
//! replaying a recorded transcript produces byte-identical results to driving a
//! live process — which is what makes the conformance suite meaningful rather
//! than decorative.

use core::time::Duration;
use std::collections::BTreeMap;

use horde_adapter::{
    AgentEvent, HeadroomSource, OutputChannel, PermissionDenial, ProviderSessionId, RateWindow,
    TokenUsage, ToolCallId, TurnId, TurnOutcome, TurnStatus, UsageAccumulator, Utilization,
};
use serde_json::Value;

use crate::wire::{
    ContentBlock, ControlRequest, ControlRequestKind, Envelope, Event, Init, Message, TurnResult,
};

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
    /// Whether a turn is open: between an `init` and its `result`.
    turn_open: bool,
    /// `can_use_tool` prompts awaiting Horde's answer, by the tool-call id the
    /// [`AgentEvent::PermissionRequested`] carried.
    outstanding: BTreeMap<ToolCallId, OutstandingPrompt>,
    /// Facts owed to the journal, emitted on the next turn to open — an
    /// output event needs a turn to belong to.
    startup_diagnostics: Vec<String>,
    /// Whether the init's own version report has been checked yet.
    checked_init_version: bool,
}

/// A permission prompt Horde has been offered and not yet answered.
#[derive(Debug, Clone, PartialEq)]
pub struct OutstandingPrompt {
    /// The harness's correlation id the `control_response` must echo.
    pub request_id: String,
    pub tool_name: String,
    /// The proposal, which an unmodified allow echoes back.
    pub input: Value,
}

impl TurnAssembler {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An assembler for a conversation revived by `--resume`, numbering its
    /// turns after the `turns_taken` the previous processes consumed.
    ///
    /// Turn ids are `{session}#{ordinal}` and the Worker keys journal entries
    /// by them, so an assembler that restarted at zero would re-mint ids the
    /// journal has already seen — and the Server's dedupe would then discard
    /// the revived turns' events while acking them. Continuing the numbering
    /// is what makes a revived turn's usage count once and land once.
    #[must_use]
    pub fn resuming_after(turns_taken: u64) -> Self {
        Self {
            turn_ordinal: turns_taken,
            ..Self::default()
        }
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
            Event::Init(init) => self.begin_turn(init),
            Event::Assistant(message) => self.assistant_events(message),
            Event::User(message) => self.tool_result_events(message),
            Event::Result(result) => vec![self.end_turn(result)],
            Event::RateLimit(rate_limit) => vec![AgentEvent::HeadroomReported {
                window: RateWindow {
                    source: HeadroomSource::ProviderReported,
                    window: rate_limit.rate_limit_info.rate_limit_type.clone(),
                    // Claude Code reports only whether it is still serving, with
                    // no counts or percentage to derive headroom from.
                    utilization: Utilization::AllowedOnly {
                        allowed: rate_limit.rate_limit_info.status == "allowed",
                    },
                    window_duration: None,
                    resets_at_unix: rate_limit.rate_limit_info.resets_at,
                },
            }],
            Event::ControlRequest(request) => self.control_request(request),
            Event::ControlCancel { request_id } => {
                self.outstanding
                    .retain(|_, prompt| &prompt.request_id != request_id);
                Vec::new()
            }
            Event::ControlResponse(_)
            | Event::StreamEvent
            | Event::System { .. }
            | Event::Unknown { .. } => Vec::new(),
        }
    }

    /// A `can_use_tool` prompt becomes a permission event, tracked until
    /// answered. Anything else — another subtype, or a prompt with no open
    /// turn to attribute it to — produces nothing, and the caller (which owns
    /// stdin) must refuse it: the harness waits on every control request.
    fn control_request(&mut self, request: &ControlRequest) -> Vec<AgentEvent> {
        let ControlRequestKind::CanUseTool(can_use) = &request.request else {
            return Vec::new();
        };
        if !self.turn_open {
            return Vec::new();
        }
        let mut proposed = can_use.to_proposed_call();
        if proposed.tool_call_id.as_str().is_empty() {
            // Shape drift tolerated: an answerable prompt beats a dropped one.
            proposed.tool_call_id = ToolCallId::new(format!("control-{}", request.request_id));
        }
        self.outstanding.insert(
            proposed.tool_call_id.clone(),
            OutstandingPrompt {
                request_id: request.request_id.clone(),
                tool_name: can_use.tool_name.clone(),
                input: can_use.input.clone(),
            },
        );
        vec![AgentEvent::PermissionRequested {
            turn_id: self.turn_id(),
            request: proposed,
        }]
    }

    /// The prompt outstanding under `tool_call_id`, if any — a peek, so a
    /// caller can encode its answer before consuming it.
    #[must_use]
    pub fn outstanding_prompt(&self, tool_call_id: &ToolCallId) -> Option<&OutstandingPrompt> {
        self.outstanding.get(tool_call_id)
    }

    /// Consumes the prompt outstanding under `tool_call_id`. `None` means
    /// nothing was waiting there, which the caller reports as an error.
    pub fn resolve_prompt(&mut self, tool_call_id: &ToolCallId) -> Option<OutstandingPrompt> {
        self.outstanding.remove(tool_call_id)
    }

    /// Every prompt still outstanding, consumed — the interrupt path, which
    /// must answer them all (the harness waits on each).
    pub fn withdraw_prompts(&mut self) -> Vec<OutstandingPrompt> {
        core::mem::take(&mut self.outstanding)
            .into_values()
            .collect()
    }

    /// The prompt the session is blocked on, if any.
    #[must_use]
    pub fn awaiting_decision(&self) -> Option<ToolCallId> {
        self.outstanding.keys().next().cloned()
    }

    /// Queues a fact for the journal — the version verdict, the permission
    /// route — emitted as a harness diagnostic when the next turn opens.
    pub fn note_startup_diagnostic(&mut self, text: impl Into<String>) {
        self.startup_diagnostics.push(text.into());
    }

    /// A diagnostic on the open turn now, or queued for the next one when no
    /// turn is open — delayed rather than lost.
    pub fn note_diagnostic(&mut self, text: String) -> Option<AgentEvent> {
        if self.turn_open {
            Some(self.output(OutputChannel::HarnessDiagnostic, text))
        } else {
            self.startup_diagnostics.push(text);
            None
        }
    }

    /// `system`/`init` opens each turn — it is a turn header, not a process
    /// handshake, and arrives once per user message.
    fn begin_turn(&mut self, init: &Init) -> Vec<AgentEvent> {
        let session_id = init.session_id.as_str();
        let is_new_session = self.session_id.as_deref() != Some(session_id);
        if is_new_session {
            self.session_id = Some(session_id.to_owned());
            self.announced_session = false;
        }

        self.turn_ordinal = self.turn_ordinal.saturating_add(1);
        self.chunk_counters = [0; CHANNEL_COUNT];
        self.turn_open = true;

        let mut events = Vec::new();
        if !self.announced_session {
            self.announced_session = true;
            events.push(AgentEvent::SessionEstablished {
                provider_session_id: ProviderSessionId::new(session_id),
                model: init.model.clone(),
            });
        }
        if !self.checked_init_version {
            self.checked_init_version = true;
            if let Some(reported) = &init.claude_code_version {
                let report = crate::version::SUPPORT.classify(reported);
                if report.verdict != horde_adapter::SupportVerdict::Supported {
                    self.startup_diagnostics.push(format!(
                        "harness version warning (from the init): {}",
                        report.describe()
                    ));
                }
            }
        }
        // After SessionEstablished, which must precede any output.
        for text in core::mem::take(&mut self.startup_diagnostics) {
            events.push(self.output(OutputChannel::HarnessDiagnostic, text));
        }
        events
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
        self.turn_open = false;
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
                        // Claude Code does not break reasoning out of output.
                        reasoning_output_tokens: 0,
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

    fn prompt(request_id: &str, tool_use_id: &str) -> String {
        json!({"type": "control_request", "request_id": request_id, "request": {
            "subtype": "can_use_tool", "tool_name": "Write", "input": {"a": 1},
            "tool_use_id": tool_use_id}})
        .to_string()
    }

    /// A prompt inside a turn is surfaced and tracked under its tool-call id
    /// until resolved, withdrawn, or cancelled by the harness — and only the
    /// cancelled one goes.
    #[test]
    fn prompts_are_tracked_until_resolved_withdrawn_or_cancelled() {
        use horde_adapter::ToolCallId;

        let mut assembler = TurnAssembler::new();
        assert!(
            feed(&mut assembler, &prompt("early", "toolu_0")).is_empty(),
            "no turn, no event: the caller refuses it"
        );
        assert_eq!(assembler.awaiting_decision(), None);

        feed(&mut assembler, &init("s1"));
        let events = feed(&mut assembler, &prompt("r1", "toolu_1"));
        assert!(matches!(
            events.as_slice(),
            [AgentEvent::PermissionRequested { request, .. }] if request.tool_call_id.as_str() == "toolu_1"
        ));
        feed(&mut assembler, &prompt("r2", "toolu_2"));
        feed(&mut assembler, &prompt("r3", ""));

        let one = ToolCallId::new("toolu_1");
        let outstanding = assembler.outstanding_prompt(&one).cloned();
        assert_eq!(
            outstanding
                .as_ref()
                .map(|prompt| (prompt.request_id.as_str(), prompt.tool_name.as_str())),
            Some(("r1", "Write"))
        );
        assert_eq!(
            outstanding.map(|prompt| prompt.input),
            Some(json!({"a": 1}))
        );
        assert!(
            assembler
                .outstanding_prompt(&ToolCallId::new("control-r3"))
                .is_some(),
            "a prompt with no tool_use_id is minted one from its request id"
        );

        feed(
            &mut assembler,
            &json!({"type": "control_cancel_request", "request_id": "r2"}).to_string(),
        );
        assert!(
            assembler
                .outstanding_prompt(&ToolCallId::new("toolu_2"))
                .is_none()
        );
        assert!(
            assembler.outstanding_prompt(&one).is_some(),
            "a cancel withdraws only the request it names"
        );

        assert_eq!(
            assembler.resolve_prompt(&one).map(|p| p.request_id),
            Some("r1".to_owned())
        );
        assert!(assembler.resolve_prompt(&one).is_none(), "answered once");
        let withdrawn: Vec<String> = assembler
            .withdraw_prompts()
            .into_iter()
            .map(|prompt| prompt.request_id)
            .collect();
        assert_eq!(withdrawn, vec!["r3".to_owned()]);
        assert_eq!(assembler.awaiting_decision(), None);
    }

    /// Startup facts ride the next turn after its SessionEstablished; a
    /// diagnostic noted mid-turn is emitted now, one noted between turns
    /// waits for the next.
    #[test]
    fn diagnostics_ride_the_open_turn_or_wait_for_the_next() {
        let mut assembler = TurnAssembler::new();
        assembler.note_startup_diagnostic("route: stdio");
        assert_eq!(
            assembler.note_diagnostic("early".to_owned()),
            None,
            "no turn yet"
        );

        let opened = feed(&mut assembler, &init("s1"));
        let texts: Vec<&str> = opened
            .iter()
            .filter_map(|event| match event {
                AgentEvent::Output {
                    channel: OutputChannel::HarnessDiagnostic,
                    text,
                    ..
                } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert!(matches!(
            opened.first(),
            Some(AgentEvent::SessionEstablished { .. })
        ));
        assert_eq!(texts, vec!["route: stdio", "early"]);

        let now = assembler.note_diagnostic("mid-turn".to_owned());
        assert!(matches!(
            now,
            Some(AgentEvent::Output { channel: OutputChannel::HarnessDiagnostic, ref text, chunk_index: 2, .. })
                if text == "mid-turn"
        ));
        feed(&mut assembler, &result_line("s1", json!({})));
        assert_eq!(
            assembler.note_diagnostic("late".to_owned()),
            None,
            "the turn closed"
        );
        let next = feed(&mut assembler, &init("s1"));
        assert!(
            next.iter()
                .any(|event| matches!(event, AgentEvent::Output { text, .. } if text == "late")),
            "{next:?}"
        );
    }

    /// The init's own version is checked once: a supported one adds nothing,
    /// an untested one a warning, and only on the first turn.
    #[test]
    fn the_init_version_is_checked_once_and_warns_only_when_untested() {
        let versioned = |version: &str| {
            json!({"type":"system","subtype":"init","session_id":"s1","model":"m",
                   "claude_code_version": version})
            .to_string()
        };
        let mut supported = TurnAssembler::new();
        assert_eq!(feed(&mut supported, &versioned("2.1.323")).len(), 1);

        let mut newer = TurnAssembler::new();
        let first = feed(&mut newer, &versioned("2.9.0"));
        assert!(
            first.iter().any(|event| matches!(event, AgentEvent::Output { text, .. }
                if text.starts_with("harness version warning (from the init): claude 2.9.0 is untested"))),
            "{first:?}"
        );
        feed(&mut newer, &result_line("s1", json!({})));
        let second = feed(&mut newer, &versioned("2.9.0"));
        assert!(second.is_empty(), "checked once: {second:?}");
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

    /// Revival continues the turn numbering instead of restarting it, and the
    /// resumed process's usage is read whole rather than differenced against a
    /// process that no longer exists.
    #[test]
    fn a_resumed_assembler_continues_the_numbering_and_counts_usage_once() {
        let mut assembler = TurnAssembler::resuming_after(4);
        feed(&mut assembler, &init("s1"));

        let events = feed(
            &mut assembler,
            &result_line(
                "s1",
                serde_json::json!({
                    "total_cost_usd": 0.0011,
                    "modelUsage": { "claude-haiku-4-5": { "inputTokens": 10, "outputTokens": 51 } }
                }),
            ),
        );

        match events.as_slice() {
            [AgentEvent::TurnEnded(outcome)] => {
                assert_eq!(
                    outcome.turn_id.to_string(),
                    "s1#5",
                    "the revived turn must not re-mint an id the journal already keyed"
                );
                assert_eq!(
                    outcome.usage.tokens.output_tokens, 51,
                    "a fresh process reports its own totals; they are the delta"
                );
            }
            other => panic!("expected one TurnEnded, got {other:?}"),
        }
    }

    /// A revived session announces itself again — the identifier that makes
    /// the agent revivable must reach the journal in every incarnation.
    #[test]
    fn a_resumed_assembler_still_announces_the_session() {
        let mut assembler = TurnAssembler::resuming_after(4);
        let first = feed(&mut assembler, &init("s1"));
        assert!(matches!(
            first.as_slice(),
            [AgentEvent::SessionEstablished { .. }]
        ));
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
                assert_eq!(
                    window.utilization,
                    horde_adapter::Utilization::AllowedOnly { allowed: true }
                );
                assert_eq!(window.window, "five_hour");
                assert_eq!(window.resets_at_unix, Some(1786317600));
            }
            other => panic!("expected one HeadroomReported, got {other:?}"),
        }
    }
}
