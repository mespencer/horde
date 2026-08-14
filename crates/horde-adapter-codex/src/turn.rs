//! Assembling app-server frames into [`AgentEvent`]s.
//!
//! A pure state machine: frames in, events out, no clock and no I/O. All three
//! traps spike W2c measured live here, because each is a place where following
//! the protocol's apparent shape produces a bug that only shows under load.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::time::Duration;

use horde_adapter::permission::{PermissionDenial, ProposedToolCall};
use horde_adapter::usage::{HeadroomSource, RateWindow, TokenUsage, UsageDelta, Utilization};
use horde_adapter::{
    AgentEvent, OutputChannel, ProviderSessionId, ToolCallId, TurnId, TurnOutcome, TurnStatus,
};

use crate::wire::{
    ErrorBody, Frame, Item, MCP_ELICITATION_METHOD, McpElicitation, Notification, TokenCounts,
};

/// Turns a stream of frames into agent events.
#[derive(Debug, Default)]
pub struct TurnAssembler {
    thread_id: Option<String>,
    model: Option<String>,
    turn: Option<OpenTurn>,
    /// `(turnId, itemId)` pairs whose usage has already been counted.
    ///
    /// The resume trap: `thread/resume` replays the previous turn's
    /// `thread/tokenUsage/updated` before the new turn's (transcript 16), so an
    /// assembler that simply added what it saw would bill the prior turn again
    /// on every revival.
    counted_usage: HashSet<(String, String)>,
    /// An interrupt asked for while no turn was open yet.
    ///
    /// A Worker calls `interrupt()` on wall-clock time, not on frame arrival,
    /// so the request can land before `turn/started` has been read. Latching it
    /// means the cancellation is not silently lost in that race; the latch is
    /// consumed by the next turn and cleared when a turn closes, so it can
    /// never leak into a later, unrelated turn.
    interrupt_pending: bool,
    /// Elicitations awaiting Horde's answer, by the tool-call id the
    /// [`AgentEvent::PermissionRequested`] carried. The value holds the JSON-RPC
    /// request id the reply must correlate to, plus what a denial record needs.
    outstanding: BTreeMap<ToolCallId, OutstandingElicitation>,
    /// Spawn-time facts to journal on the first turn — see
    /// [`Self::note_startup_diagnostic`].
    startup_diagnostics: Vec<String>,
}

/// One elicitation Horde has been offered and not yet answered.
#[derive(Debug)]
struct OutstandingElicitation {
    /// The server->client JSON-RPC id the reply correlates to.
    rpc_id: u64,
    tool_name: String,
    input: serde_json::Value,
}

/// An `mcpToolCall` item that has started and may be about to elicit.
///
/// The elicitation names the server but **not the tool** (transcript 17); the
/// item started just before it carries the tool's name, its arguments, and the
/// id everything later correlates by. This is the join record.
#[derive(Debug, Clone)]
struct StartedMcpCall {
    item_id: String,
    server: String,
    tool: String,
    arguments: serde_json::Value,
}

#[derive(Debug)]
struct OpenTurn {
    id: TurnId,
    /// Per-channel chunk numbering, for gap detection downstream.
    chunks: horde_adapter::ChunkCounters,
    /// Final-answer texts. A steered turn produces more than one (transcript
    /// 15); the last is the answer to the steer, which is what a Result should
    /// carry.
    final_answers: Vec<String>,
    usage: TokenUsage,
    interrupt_requested: bool,
    duration: Duration,
    /// MCP tool calls that have started and not yet elicited, oldest first.
    started_mcp_calls: VecDeque<StartedMcpCall>,
    /// Calls the permission chain refused during this turn.
    denials: Vec<PermissionDenial>,
}

impl OpenTurn {
    fn new(id: TurnId) -> Self {
        Self {
            id,
            chunks: horde_adapter::ChunkCounters::new(),
            final_answers: Vec::new(),
            usage: TokenUsage::default(),
            interrupt_requested: false,
            duration: Duration::ZERO,
            started_mcp_calls: VecDeque::new(),
            denials: Vec::new(),
        }
    }

    fn next_chunk_index(&mut self, channel: OutputChannel) -> u64 {
        self.chunks.next(channel)
    }
}

impl TurnAssembler {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The thread id, once the harness has named it.
    #[must_use]
    pub fn provider_session_id(&self) -> Option<ProviderSessionId> {
        self.thread_id.as_deref().map(ProviderSessionId::new)
    }

    /// Records that an interrupt was sent, so the next `thread/status → idle`
    /// is read as a cancellation rather than an ordinary quiet moment.
    ///
    /// Without this the assembler cannot tell the two apart: `idle` is also
    /// what a thread reports between turns, and Codex emits no distinct
    /// "aborted" frame.
    pub fn note_interrupt_requested(&mut self) {
        match self.turn.as_mut() {
            Some(turn) => turn.interrupt_requested = true,
            None => self.interrupt_pending = true,
        }
    }

    /// The id of the turn currently open, when one is.
    ///
    /// This is what `turn/steer` needs as its `expectedTurnId` precondition —
    /// steering amends the *active* turn, so the caller has to name it.
    #[must_use]
    pub fn active_turn_id(&self) -> Option<&TurnId> {
        self.turn.as_ref().map(|turn| &turn.id)
    }

    /// Queues a spawn-time fact for the journal — which sandbox tier was
    /// chosen and why, notably (ADR-0005: the tier *is* the enforcement, so
    /// its choice must be auditable). Emitted as a
    /// [`OutputChannel::HarnessDiagnostic`] chunk when the next turn opens,
    /// because an output event needs a turn to be attributed to.
    pub fn note_startup_diagnostic(&mut self, text: impl Into<String>) {
        self.startup_diagnostics.push(text.into());
    }

    /// Feeds one frame, returning the events it produced.
    ///
    /// A [`Frame::ServerRequest`] carrying an MCP tool-call elicitation
    /// becomes an [`AgentEvent::PermissionRequested`] and is tracked until
    /// [`Self::resolve_elicitation`] answers it. Every other server request
    /// produces nothing here — the caller owns the wire and must reply to it
    /// (deny-by-default), or the harness parks forever (transcript 19).
    pub fn feed(&mut self, frame: &Frame) -> Vec<AgentEvent> {
        match frame {
            Frame::Notification(notification) => self.feed_notification(notification),
            Frame::ServerRequest { id, method, params } if method == MCP_ELICITATION_METHOD => {
                self.feed_elicitation(*id, params)
            }
            // Responses correlate to requests the Worker made and carry nothing
            // the event stream needs; other server requests are the caller's to
            // refuse.
            Frame::Response { .. } | Frame::ServerRequest { .. } => Vec::new(),
        }
    }

    /// The JSON-RPC id the answer to `tool_call_id` must correlate to, while
    /// that elicitation is still outstanding.
    #[must_use]
    pub fn elicitation_rpc_id(&self, tool_call_id: &ToolCallId) -> Option<u64> {
        self.outstanding.get(tool_call_id).map(|e| e.rpc_id)
    }

    /// The elicitation the session is blocked on, if any.
    ///
    /// The harness will not run the call — or write the frames behind it —
    /// until this is answered, so a caller that keeps polling for events while
    /// owing an answer deserves to be told what it owes.
    #[must_use]
    pub fn awaiting_decision(&self) -> Option<ToolCallId> {
        self.outstanding.keys().next().cloned()
    }

    /// Records Horde's answer to an outstanding elicitation, returning the
    /// JSON-RPC id the caller must reply to — `None` means nothing was waiting
    /// under that id, which the caller reports as an error rather than a no-op.
    ///
    /// A denial is recorded on the open turn, because "the chain refused a
    /// call" is part of the turn's outcome (`TurnOutcome::denials`) — the
    /// harness itself only ever shows the agent the fixed rejection string.
    pub fn resolve_elicitation(&mut self, tool_call_id: &ToolCallId, allowed: bool) -> Option<u64> {
        let outstanding = self.outstanding.remove(tool_call_id)?;
        if !allowed && let Some(turn) = self.turn.as_mut() {
            turn.denials.push(PermissionDenial {
                tool_call_id: tool_call_id.clone(),
                tool_name: outstanding.tool_name.clone(),
                attempted_input: outstanding.input.clone(),
            });
        }
        Some(outstanding.rpc_id)
    }

    /// An MCP tool-call elicitation: the interception point, as an event.
    fn feed_elicitation(&mut self, rpc_id: u64, params: &serde_json::Value) -> Vec<AgentEvent> {
        let Ok(elicitation) = serde_json::from_value::<McpElicitation>(params.clone()) else {
            return Vec::new();
        };
        if !elicitation.is_mcp_tool_call_approval() {
            return Vec::new();
        }

        // The elicitation names the server but not the tool; the started item
        // is the join. Absent one (shape drift), the event still carries the
        // server and the _meta arguments, and mints an id from the request —
        // an approval the chain can act on beats a dropped frame.
        let started = self
            .turn
            .as_mut()
            .and_then(|turn| turn.started_mcp_calls.pop_front());
        let (tool_call_id, tool_name, input) = match started {
            Some(call) => (
                ToolCallId::new(call.item_id),
                format!("{}/{}", call.server, call.tool),
                call.arguments,
            ),
            None => (
                ToolCallId::new(format!("elicitation-{rpc_id}")),
                elicitation.server_name.clone(),
                elicitation
                    .meta
                    .as_ref()
                    .and_then(|meta| meta.tool_params.clone())
                    .unwrap_or(serde_json::Value::Null),
            ),
        };

        let turn_id = self
            .turn
            .as_ref()
            .map(|turn| turn.id.clone())
            .or_else(|| elicitation.turn_id.as_deref().map(TurnId::new));
        let Some(turn_id) = turn_id else {
            // No turn to attribute the request to — nothing downstream could
            // journal or answer it coherently.
            return Vec::new();
        };

        self.outstanding.insert(
            tool_call_id.clone(),
            OutstandingElicitation {
                rpc_id,
                tool_name: tool_name.clone(),
                input: input.clone(),
            },
        );
        vec![AgentEvent::PermissionRequested {
            turn_id,
            request: ProposedToolCall {
                tool_call_id,
                tool_name,
                input,
            },
        }]
    }

    fn feed_notification(&mut self, notification: &Notification) -> Vec<AgentEvent> {
        match notification {
            Notification::ThreadStarted(started) => self.thread_started(started),
            Notification::TurnStarted(started) => self.turn_started(started),
            Notification::AgentMessageDelta(delta) => self.message_delta(delta),
            Notification::ItemStarted(started) => self.item_started(&started.item),
            Notification::ItemCompleted(completed) => self.item_completed(&completed.item),
            Notification::Error(error) => self.error_notified(error),
            Notification::TokenUsageUpdated(update) => {
                self.record_usage(update.turn_id.as_deref(), update.token_usage.last.as_ref());
                Vec::new()
            }
            Notification::RateLimitsUpdated(limits) => headroom_reported(limits),
            Notification::TurnCompleted(completed) => self.turn_completed(completed),
            Notification::ThreadStatusChanged(status) => self.status_changed(status),
            Notification::Other { .. } => Vec::new(),
        }
    }

    fn thread_started(&mut self, started: &crate::wire::ThreadStarted) -> Vec<AgentEvent> {
        let id = started
            .thread
            .as_ref()
            .map(|t| t.id.clone())
            .or_else(|| started.thread_id.clone());
        let model = started.thread.as_ref().and_then(|t| t.model.clone());

        match id {
            Some(id) => {
                self.thread_id = Some(id.clone());
                self.model.clone_from(&model);
                vec![AgentEvent::SessionEstablished {
                    provider_session_id: ProviderSessionId::new(id),
                    model: model.unwrap_or_default(),
                }]
            }
            None => Vec::new(),
        }
    }

    fn turn_started(&mut self, started: &crate::wire::TurnRef) -> Vec<AgentEvent> {
        let mut turn = OpenTurn::new(TurnId::new(&started.turn.id));
        // Consume any interrupt that arrived before this turn existed.
        turn.interrupt_requested = std::mem::take(&mut self.interrupt_pending);
        // Spawn-time facts owed to the journal ride the first turn as
        // harness diagnostics: an Output event needs a turn to belong
        // to, and this is the first moment one exists.
        let events = std::mem::take(&mut self.startup_diagnostics)
            .into_iter()
            .map(|text| AgentEvent::Output {
                turn_id: turn.id.clone(),
                channel: OutputChannel::HarnessDiagnostic,
                chunk_index: turn.next_chunk_index(OutputChannel::HarnessDiagnostic),
                text,
            })
            .collect();
        self.turn = Some(turn);
        events
    }

    fn message_delta(&mut self, delta: &crate::wire::AgentMessageDelta) -> Vec<AgentEvent> {
        let Some(turn) = self.turn.as_mut() else {
            return Vec::new();
        };
        let chunk_index = turn.next_chunk_index(OutputChannel::Assistant);
        vec![AgentEvent::Output {
            turn_id: turn.id.clone(),
            channel: OutputChannel::Assistant,
            text: delta.delta.clone(),
            chunk_index,
        }]
    }

    /// Only mcpToolCall starts matter: they carry the tool identity the
    /// elicitation that follows will not repeat.
    fn item_started(&mut self, item: &Item) -> Vec<AgentEvent> {
        if item.is_mcp_tool_call()
            && let Some(turn) = self.turn.as_mut()
        {
            turn.started_mcp_calls.push_back(StartedMcpCall {
                item_id: item.id.clone().unwrap_or_default(),
                server: item.server.clone().unwrap_or_default(),
                tool: item.tool.clone().unwrap_or_default(),
                arguments: item.arguments.clone().unwrap_or(serde_json::Value::Null),
            });
        }
        Vec::new()
    }

    /// `turn/completed` is a framing terminal, not a verdict: the 0.147.0
    /// schema gives the turn itself a status of completed, interrupted, or
    /// failed, and reporting a failed turn as Completed would hand the
    /// orchestrator a Result built from an error.
    fn turn_completed(&mut self, completed: &crate::wire::TurnCompleted) -> Vec<AgentEvent> {
        let duration = completed.turn.duration_ms.map(Duration::from_millis);
        let status = match completed.turn.status.as_deref() {
            Some("failed") => TurnStatus::Failed {
                message: completed
                    .turn
                    .error
                    .as_ref()
                    .map_or_else(|| "the harness failed the turn".to_owned(), turn_error),
            },
            Some("interrupted") => TurnStatus::Interrupted,
            _ => TurnStatus::Completed,
        };
        self.close_turn(status, duration)
    }

    /// The interrupt trap. `turn/interrupt` produces no terminal frame at
    /// all — transcript 11 has the thread go idle with no `turn/completed`
    /// and no `turn/aborted`, while 394 already-generated deltas keep
    /// arriving. Keying on idle is the only way the turn ever closes; an
    /// assembler waiting for a terminal frame hangs forever.
    fn status_changed(&mut self, status: &crate::wire::ThreadStatus) -> Vec<AgentEvent> {
        let interrupted = self
            .turn
            .as_ref()
            .is_some_and(|turn| turn.interrupt_requested);
        if status.is_idle() && interrupted {
            self.close_turn(TurnStatus::Interrupted, None)
        } else {
            // Idle between turns is ordinary and closes nothing; non-idle
            // statuses close nothing either way.
            Vec::new()
        }
    }

    fn item_completed(&mut self, item: &Item) -> Vec<AgentEvent> {
        let Some(turn) = self.turn.as_mut() else {
            return Vec::new();
        };

        if item.is_final_answer() {
            // The steer trap: this may be the second final answer of one turn.
            // Both are kept; `close_turn` takes the last, because after a steer
            // the answer to the steer is the turn's answer.
            if let Some(text) = item.text.clone() {
                turn.final_answers.push(text);
            }
            return Vec::new();
        }

        if item.is_command_execution() || item.is_mcp_tool_call() {
            let turn_id = turn.id.clone();
            // An mcpToolCall can fail without an exit code — a declined
            // elicitation reads `status: "failed"` with an error message and
            // nothing else (transcript 18) — so all three signals count.
            let is_error = item.exit_code.is_some_and(|code| code != 0)
                || item.status.as_deref() == Some("failed")
                || item.error.is_some();
            // A call that completed without ever eliciting must not linger as
            // a candidate join for the *next* elicitation.
            let item_id = item.id.clone().unwrap_or_default();
            turn.started_mcp_calls
                .retain(|started| started.item_id != item_id);
            return vec![AgentEvent::ToolCompleted {
                turn_id,
                // Codex numbers its own items; there is no separate tool-call
                // id, so the item id is the identity.
                tool_call_id: ToolCallId::new(item_id),
                is_error,
            }];
        }

        Vec::new()
    }

    /// The `error` notification: a retryable hiccup is surfaced as harness
    /// diagnostics; a terminal one is the turn's ending.
    fn error_notified(&mut self, error: &ErrorBody) -> Vec<AgentEvent> {
        if error.will_retry {
            let Some(turn) = self.turn.as_mut() else {
                return Vec::new();
            };
            let chunk_index = turn.next_chunk_index(OutputChannel::HarnessDiagnostic);
            return vec![AgentEvent::Output {
                turn_id: turn.id.clone(),
                channel: OutputChannel::HarnessDiagnostic,
                text: format!("codex will retry: {}", turn_error(&error.error)),
                chunk_index,
            }];
        }
        // Terminal. The spike's interrupt finding generalizes: nothing
        // promises a turn/completed after a fatal error, so the error is the
        // turn's ending or the turn never ends.
        self.close_turn(
            TurnStatus::Failed {
                message: turn_error(&error.error),
            },
            None,
        )
    }

    /// Adds a per-turn usage reading, once.
    ///
    /// Idempotent per `(turnId, itemId)` — here the reading itself is the item,
    /// so a replayed reading for a turn already counted is dropped.
    fn record_usage(&mut self, turn_id: Option<&str>, counts: Option<&TokenCounts>) {
        let (Some(turn_id), Some(counts)) = (turn_id, counts) else {
            return;
        };

        // One usage reading per turn is what Codex sends; the key still carries
        // both halves so a future per-item breakdown does not silently merge.
        let key = (turn_id.to_owned(), "tokenUsage".to_owned());
        if !self.counted_usage.insert(key) {
            return;
        }

        let Some(turn) = self.turn.as_mut() else {
            return;
        };
        if turn.id.as_str() != turn_id {
            // A reading for a turn that is not the open one — exactly what
            // resume replays. Recorded as counted above, applied to nothing.
            return;
        }

        turn.usage = TokenUsage {
            input_tokens: counts.input_tokens,
            output_tokens: counts.output_tokens,
            cache_read_input_tokens: counts.cached_input_tokens,
            cache_write_input_tokens: counts.cache_write_input_tokens,
            reasoning_output_tokens: counts.reasoning_output_tokens,
        };
    }

    fn close_turn(&mut self, status: TurnStatus, duration: Option<Duration>) -> Vec<AgentEvent> {
        let Some(mut turn) = self.turn.take() else {
            return Vec::new();
        };
        // A closed turn ends the interrupt's reach; the next one starts clean.
        self.interrupt_pending = false;

        vec![AgentEvent::TurnEnded(TurnOutcome {
            turn_id: turn.id,
            // An interrupted turn has no answer even if text was streaming when
            // it was cut off; reporting a partial as the answer would let a
            // Result be built from work the user cancelled.
            text: if status == TurnStatus::Interrupted {
                None
            } else {
                turn.final_answers.last().cloned()
            },
            status,
            usage: UsageDelta {
                tokens: turn.usage,
                // Codex prices nothing per turn under a subscription.
                cost_usd: 0.0,
            },
            // Exec refusals never reach Horde — Codex adjudicates those
            // internally (ADR-0005) — but MCP calls the chain declined through
            // the elicitation seam are this turn's refusals, and they are what
            // keeps `is_clean_success` honest for a Codex agent.
            denials: std::mem::take(&mut turn.denials),
            duration: duration.unwrap_or(turn.duration),
        })]
    }
}

/// The provider-reported rate window, when the frame carries one.
fn headroom_reported(limits: &crate::wire::RateLimits) -> Vec<AgentEvent> {
    limits
        .rate_limits
        .primary
        .map(|primary| {
            vec![AgentEvent::HeadroomReported {
                window: RateWindow {
                    source: HeadroomSource::ProviderReported,
                    window: limits
                        .rate_limits
                        .limit_id
                        .clone()
                        .unwrap_or_else(|| "primary".to_owned()),
                    // Codex reports a percentage and never counts, so this is
                    // the Fraction arm by construction.
                    utilization: Utilization::Fraction(
                        (primary.used_percent / 100.0).clamp(0.0, 1.0),
                    ),
                    window_duration: primary
                        .window_duration_mins
                        .map(|mins| Duration::from_secs(mins.saturating_mul(60))),
                    resets_at_unix: primary.resets_at,
                },
            }]
        })
        .unwrap_or_default()
}

/// One line for a turn failure, code first when codex named one.
fn turn_error(error: &crate::wire::TurnErrorBody) -> String {
    match error.code() {
        Some(code) => format!("{code}: {}", error.message),
        None => error.message.clone(),
    }
}

#[cfg(test)]
mod tests {
    use horde_adapter::{AgentEvent, TurnStatus};
    use serde_json::json;

    use super::TurnAssembler;
    use crate::codec::decode_line;

    fn feed(assembler: &mut TurnAssembler, frame: serde_json::Value) -> Vec<AgentEvent> {
        let decoded = decode_line(&frame.to_string()).expect("test frames are well formed");
        assembler.feed(&decoded)
    }

    fn open_turn(assembler: &mut TurnAssembler) {
        feed(
            assembler,
            json!({"method":"thread/started","params":{"thread":{"id":"t1"}}}),
        );
        feed(
            assembler,
            json!({"method":"turn/started","params":{"turn":{"id":"turn-1"}}}),
        );
    }

    fn status(kind: &str) -> serde_json::Value {
        json!({"method":"thread/status/changed","params":{"status":{"type":kind}}})
    }

    /// Only `idle` ends an interrupted turn. An `active` status arriving after
    /// the interrupt must not close it early, or output still in flight is
    /// attributed to a turn that already ended.
    #[test]
    fn a_non_idle_status_does_not_close_an_interrupted_turn() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);
        assembler.note_interrupt_requested();

        let events = feed(&mut assembler, status("active"));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentEvent::TurnEnded(_))),
            "only idle ends an interrupted turn"
        );

        let events = feed(&mut assembler, status("idle"));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, AgentEvent::TurnEnded(_))),
            "idle must end it"
        );
    }

    fn command_item(status: &str, exit_code: Option<i64>) -> serde_json::Value {
        let mut item = json!({
            "type": "commandExecution",
            "id": "exec-1",
            "command": "/bin/zsh -lc 'echo hi'",
            "status": status,
        });
        if let Some(code) = exit_code {
            item["exitCode"] = json!(code);
        }
        json!({"method":"item/completed","params":{"item": item}})
    }

    fn tool_error(assembler: &mut TurnAssembler, frame: serde_json::Value) -> Option<bool> {
        feed(assembler, frame)
            .into_iter()
            .find_map(|event| match event {
                AgentEvent::ToolCompleted { is_error, .. } => Some(is_error),
                _ => None,
            })
    }

    /// A command is failed if it exited non-zero **or** the harness said so.
    /// Both halves are load-bearing: Codex reports `status: "failed"` for a
    /// sandbox denial that never produced an exit code at all.
    #[test]
    fn a_command_fails_on_a_bad_exit_code_or_a_failed_status() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);

        assert_eq!(
            tool_error(&mut assembler, command_item("completed", Some(0))),
            Some(false),
            "exit 0 and completed is a success"
        );
        assert_eq!(
            tool_error(&mut assembler, command_item("completed", Some(6))),
            Some(true),
            "a non-zero exit is a failure even when the status says completed"
        );
        assert_eq!(
            tool_error(&mut assembler, command_item("failed", None)),
            Some(true),
            "a failed status is a failure even with no exit code — the sandbox-denial shape"
        );
    }

    /// Items that are not command executions produce no tool event.
    #[test]
    fn a_non_command_item_is_not_reported_as_a_tool() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);

        let events = feed(
            &mut assembler,
            json!({"method":"item/completed",
                   "params":{"item":{"type":"userMessage","id":"u1"}}}),
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentEvent::ToolCompleted { .. })),
            "a user message is not a tool call"
        );
    }

    /// The permission request among `events`, or a panic naming `what`.
    fn requested(events: &[AgentEvent], what: &str) -> horde_adapter::ProposedToolCall {
        for event in events {
            if let AgentEvent::PermissionRequested { request, .. } = event {
                return request.clone();
            }
        }
        panic!("{what}: no PermissionRequested in {events:?}");
    }

    /// The turn ending among `events`, or a panic naming `what`.
    fn closed(events: Vec<AgentEvent>, what: &str) -> horde_adapter::TurnOutcome {
        for event in events {
            if let AgentEvent::TurnEnded(outcome) = event {
                return outcome;
            }
        }
        panic!("{what}: no TurnEnded arrived");
    }

    fn mcp_started(id: &str) -> serde_json::Value {
        json!({"method":"item/started","params":{"item":{
            "type":"mcpToolCall","id":id,"server":"spike","tool":"horde_probe",
            "status":"inProgress","arguments":{"token":"HORDE_MCP_OK"}}}})
    }

    fn elicitation_request(rpc_id: u64) -> serde_json::Value {
        json!({"id":rpc_id,"method":"mcpServer/elicitation/request","params":{
            "threadId":"t1","turnId":"turn-1","serverName":"spike","mode":"form",
            "_meta":{"codex_approval_kind":"mcp_tool_call",
                     "tool_params":{"token":"HORDE_MCP_OK"}},
            "message":"Allow?","requestedSchema":{"type":"object","properties":{}}}})
    }

    /// The interception seam end to end in the assembler: the started item
    /// supplies the tool identity the elicitation omits, the event carries the
    /// full arguments, and the answer correlates back to the JSON-RPC id.
    #[test]
    fn an_elicitation_joins_its_started_item_and_correlates_its_answer() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);
        feed(&mut assembler, mcp_started("exec-45"));

        let events = feed(&mut assembler, elicitation_request(7));
        let request = requested(&events, "the elicitation must surface");

        assert_eq!(request.tool_call_id.as_str(), "exec-45");
        assert_eq!(
            request.tool_name, "spike/horde_probe",
            "the tool identity comes from the started item, not the elicitation"
        );
        assert_eq!(request.input, json!({"token":"HORDE_MCP_OK"}));

        assert_eq!(
            assembler.elicitation_rpc_id(&request.tool_call_id),
            Some(7),
            "the reply must correlate to the server's request id"
        );
        assert_eq!(
            assembler.resolve_elicitation(&request.tool_call_id, true),
            Some(7)
        );
        assert_eq!(
            assembler.resolve_elicitation(&request.tool_call_id, true),
            None,
            "an answer consumes the outstanding request; a second one has \
             nothing to go to"
        );
    }

    /// A denied elicitation is the turn's refusal record — this is what keeps
    /// `is_clean_success` honest for a Codex agent.
    #[test]
    fn a_denied_elicitation_lands_in_the_turn_outcomes_denials() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);
        feed(&mut assembler, mcp_started("exec-45"));
        let events = feed(&mut assembler, elicitation_request(3));
        let tool_call_id = requested(&events, "the request surfaces").tool_call_id;

        assert_eq!(assembler.resolve_elicitation(&tool_call_id, false), Some(3));

        let events = feed(
            &mut assembler,
            json!({"method":"turn/completed",
                   "params":{"turn":{"id":"turn-1","status":"completed","durationMs":5}}}),
        );
        let outcome = closed(events, "the turn closes");
        assert_eq!(outcome.denials.len(), 1);
        assert_eq!(outcome.denials[0].tool_name, "spike/horde_probe");
        assert_eq!(
            outcome.denials[0].attempted_input,
            json!({"token":"HORDE_MCP_OK"})
        );
        assert!(
            !outcome.is_clean_success(),
            "a turn that hit a refusal is not a clean success"
        );

        // And an allowed elicitation records no denial.
        let mut clean = TurnAssembler::new();
        open_turn(&mut clean);
        feed(&mut clean, mcp_started("exec-1"));
        let events = feed(&mut clean, elicitation_request(4));
        let id = requested(&events, "surfaces").tool_call_id;
        clean.resolve_elicitation(&id, true);
        let events = feed(
            &mut clean,
            json!({"method":"turn/completed",
                   "params":{"turn":{"id":"turn-1","status":"completed"}}}),
        );
        let outcome = closed(events, "closes");
        assert!(outcome.denials.is_empty(), "an allow is not a refusal");
    }

    /// Shape drift tolerated: an elicitation with no started item to join
    /// still surfaces, carrying the `_meta` arguments and a minted id — an
    /// approval the chain can act on beats a dropped frame.
    #[test]
    fn an_elicitation_without_a_started_item_still_surfaces() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);

        let events = feed(&mut assembler, elicitation_request(9));
        let request = requested(&events, "must surface without the join");
        assert_eq!(request.tool_call_id.as_str(), "elicitation-9");
        assert_eq!(request.tool_name, "spike");
        assert_eq!(request.input, json!({"token":"HORDE_MCP_OK"}));
    }

    /// A non-approval elicitation must not be surfaced as a permission
    /// request — the discriminator is `codex_approval_kind`, and the caller
    /// declines what produced no event.
    #[test]
    fn a_non_tool_call_elicitation_produces_no_permission_request() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);

        let events = feed(
            &mut assembler,
            json!({"id":5,"method":"mcpServer/elicitation/request","params":{
                "threadId":"t1","serverName":"spike",
                "_meta":{"codex_approval_kind":"oauth_login"}}}),
        );
        assert!(events.is_empty(), "not a tool-call approval: {events:?}");
    }

    /// The mcpToolCall completion is a tool event, and a declined call — which
    /// fails with an error message and no exit code (transcript 18) — must
    /// read as an error.
    #[test]
    fn an_mcp_tool_call_completion_reports_success_and_rejection_honestly() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);

        let ok = tool_error(
            &mut assembler,
            json!({"method":"item/completed","params":{"item":{
                "type":"mcpToolCall","id":"exec-1","server":"spike","tool":"horde_probe",
                "status":"completed","arguments":{},
                "result":{"content":[{"type":"text","text":"PROBE:HORDE_MCP_OK"}]}}}}),
        );
        assert_eq!(ok, Some(false), "a completed call is not an error");

        let rejected = tool_error(
            &mut assembler,
            json!({"method":"item/completed","params":{"item":{
                "type":"mcpToolCall","id":"exec-2","server":"spike","tool":"horde_probe",
                "status":"failed","arguments":{},
                "error":{"message":"user rejected MCP tool call"}}}}),
        );
        assert_eq!(rejected, Some(true), "a rejected call is an error");
    }

    /// The `error` notification's retry bit is the difference between a
    /// diagnostic and a verdict: willRetry surfaces as harness noise, a
    /// terminal error is the turn's ending — nothing promises a
    /// `turn/completed` after one.
    #[test]
    fn a_retryable_error_is_diagnostic_and_a_terminal_one_ends_the_turn() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);

        let events = feed(
            &mut assembler,
            json!({"method":"error","params":{
                "error":{"message":"stream disconnected"},
                "threadId":"t1","turnId":"turn-1","willRetry":true}}),
        );
        assert!(
            events.iter().any(|event| matches!(
                event,
                AgentEvent::Output {
                    channel: horde_adapter::OutputChannel::HarnessDiagnostic,
                    ..
                }
            )),
            "a retryable error is worth showing: {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentEvent::TurnEnded(_))),
            "and must not end the turn"
        );

        let events = feed(
            &mut assembler,
            json!({"method":"error","params":{
                "error":{"message":"401 Unauthorized","codexErrorInfo":"unauthorized"},
                "threadId":"t1","turnId":"turn-1","willRetry":false}}),
        );
        let outcome = closed(events, "a terminal error ends the turn");
        match outcome.status {
            TurnStatus::Failed { message } => {
                assert!(
                    message.contains("unauthorized") && message.contains("401"),
                    "the lifecycle reason carries both the code and the text: {message}"
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// `turn/completed` is framing, not a verdict: a turn whose own status is
    /// failed must not be reported as Completed.
    #[test]
    fn a_completed_frame_with_a_failed_status_reports_the_failure() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);

        let events = feed(
            &mut assembler,
            json!({"method":"turn/completed","params":{"turn":{
                "id":"turn-1","status":"failed",
                "error":{"message":"context window exceeded",
                         "codexErrorInfo":"contextWindowExceeded"}}}}),
        );
        let outcome = closed(events, "closes");
        assert!(
            matches!(
                &outcome.status,
                TurnStatus::Failed { message } if message.contains("contextWindowExceeded")
            ),
            "got {:?}",
            outcome.status
        );
    }

    /// Steering needs the active turn's id as its precondition; between turns
    /// there is nothing to steer.
    #[test]
    fn the_active_turn_id_exists_exactly_while_a_turn_is_open() {
        let mut assembler = TurnAssembler::new();
        assert!(assembler.active_turn_id().is_none());

        open_turn(&mut assembler);
        assert_eq!(
            assembler.active_turn_id().map(|id| id.as_str().to_owned()),
            Some("turn-1".to_owned())
        );

        feed(
            &mut assembler,
            json!({"method":"turn/completed","params":{"turn":{"id":"turn-1"}}}),
        );
        assert!(
            assembler.active_turn_id().is_none(),
            "a closed turn cannot be steered"
        );
    }

    /// A `turn/completed` whose own status is "interrupted" closes the turn
    /// as Interrupted even when nobody noted an interrupt — the harness's
    /// verdict, not the client's request, is what the frame carries.
    #[test]
    fn a_completed_frame_with_an_interrupted_status_reports_the_interruption() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);

        let events = feed(
            &mut assembler,
            json!({"method":"turn/completed",
                   "params":{"turn":{"id":"turn-1","status":"interrupted"}}}),
        );
        let outcome = closed(events, "an interrupted status still closes the turn");
        assert_eq!(outcome.status, TurnStatus::Interrupted);
        assert_eq!(outcome.text, None, "a cancelled turn has no answer");
    }

    /// A call that completed without eliciting must stop being a join
    /// candidate — and *only* that call: the next elicitation joins the still
    /// -open one, not the finished one and not nothing.
    #[test]
    fn a_completed_call_stops_being_a_join_candidate_but_others_remain() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);
        feed(&mut assembler, mcp_started("exec-1"));
        feed(&mut assembler, mcp_started("exec-2"));

        // exec-1 completes without ever eliciting (pre-approved, say).
        feed(
            &mut assembler,
            json!({"method":"item/completed","params":{"item":{
                "type":"mcpToolCall","id":"exec-1","server":"spike","tool":"horde_probe",
                "status":"completed","arguments":{}}}}),
        );

        let events = feed(&mut assembler, elicitation_request(5));
        let request = requested(&events, "the elicitation joins the open call");
        assert_eq!(
            request.tool_call_id.as_str(),
            "exec-2",
            "the finished call must not absorb the next elicitation"
        );
    }

    /// `awaiting_decision` is the session's stall signal: present exactly
    /// while an elicitation is outstanding.
    #[test]
    fn awaiting_decision_tracks_the_outstanding_elicitation() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);
        assert_eq!(assembler.awaiting_decision(), None, "nothing asked yet");

        feed(&mut assembler, mcp_started("exec-1"));
        let events = feed(&mut assembler, elicitation_request(5));
        let id = requested(&events, "surfaces").tool_call_id;
        assert_eq!(
            assembler.awaiting_decision(),
            Some(id.clone()),
            "the outstanding request is the stall"
        );

        assembler.resolve_elicitation(&id, true);
        assert_eq!(
            assembler.awaiting_decision(),
            None,
            "an answered request stalls nothing"
        );
    }

    /// The method is the discriminator: a request that merely *looks* like an
    /// elicitation (right params, wrong method) must not become a permission
    /// event — it is some other protocol surface the session must refuse.
    #[test]
    fn an_elicitation_shaped_request_on_another_method_is_not_an_approval() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);
        feed(&mut assembler, mcp_started("exec-1"));

        let mut frame = elicitation_request(7);
        frame["method"] = json!("item/tool/requestUserInput");
        let events = feed(&mut assembler, frame);
        assert!(
            events.is_empty(),
            "only mcpServer/elicitation/request is the approval seam: {events:?}"
        );
        assert_eq!(
            assembler.awaiting_decision(),
            None,
            "and nothing may be left outstanding for it"
        );
    }

    /// The tier-choice audit trail: a startup diagnostic rides the first turn
    /// exactly once, on the diagnostic channel, and never repeats.
    #[test]
    fn a_startup_diagnostic_rides_the_first_turn_once() {
        let mut assembler = TurnAssembler::new();
        assembler.note_startup_diagnostic("sandbox tier read-only: no permission mode");

        let mut events = feed(
            &mut assembler,
            json!({"method":"thread/started","params":{"thread":{"id":"t1"}}}),
        );
        events.extend(feed(
            &mut assembler,
            json!({"method":"turn/started","params":{"turn":{"id":"turn-1"}}}),
        ));
        let diagnostic = events.iter().find_map(|event| match event {
            AgentEvent::Output {
                channel: horde_adapter::OutputChannel::HarnessDiagnostic,
                text,
                chunk_index,
                ..
            } => Some((text.clone(), *chunk_index)),
            _ => None,
        });
        let (text, chunk_index) = diagnostic.expect("the tier choice must reach the journal");
        assert!(text.contains("read-only"), "got {text}");
        assert_eq!(chunk_index, 0);

        feed(
            &mut assembler,
            json!({"method":"turn/completed","params":{"turn":{"id":"turn-1"}}}),
        );
        let second = feed(
            &mut assembler,
            json!({"method":"turn/started","params":{"turn":{"id":"turn-2"}}}),
        );
        assert!(
            second.is_empty(),
            "a spawn-time fact is journaled once, not per turn: {second:?}"
        );
    }

    #[test]
    fn a_completed_turn_reports_its_recorded_duration() {
        let mut assembler = TurnAssembler::new();
        open_turn(&mut assembler);

        let events = feed(
            &mut assembler,
            json!({"method":"turn/completed",
                   "params":{"turn":{"id":"turn-1","durationMs":6101}}}),
        );
        let ended = closed(events, "turn/completed closes the turn");
        assert_eq!(ended.status, TurnStatus::Completed);
        assert_eq!(ended.duration.as_millis(), 6101);
    }
}
