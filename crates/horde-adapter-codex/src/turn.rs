//! Assembling app-server frames into [`AgentEvent`]s.
//!
//! A pure state machine: frames in, events out, no clock and no I/O. All three
//! traps spike W2c measured live here, because each is a place where following
//! the protocol's apparent shape produces a bug that only shows under load.

use std::collections::HashSet;
use std::time::Duration;

use horde_adapter::usage::{HeadroomSource, RateWindow, TokenUsage, UsageDelta, Utilization};
use horde_adapter::{
    AgentEvent, OutputChannel, ProviderSessionId, TurnId, TurnOutcome, TurnStatus,
};

use crate::wire::{Frame, Item, Notification, TokenCounts};

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
}

#[derive(Debug)]
struct OpenTurn {
    id: TurnId,
    /// Deltas seen, per channel, for gap detection downstream.
    chunk_index: u64,
    /// Final-answer texts. A steered turn produces more than one (transcript
    /// 15); the last is the answer to the steer, which is what a Result should
    /// carry.
    final_answers: Vec<String>,
    usage: TokenUsage,
    interrupt_requested: bool,
    duration: Duration,
}

impl OpenTurn {
    fn new(id: TurnId) -> Self {
        Self {
            id,
            chunk_index: 0,
            final_answers: Vec::new(),
            usage: TokenUsage::default(),
            interrupt_requested: false,
            duration: Duration::ZERO,
        }
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

    /// Feeds one frame, returning the events it produced.
    pub fn feed(&mut self, frame: &Frame) -> Vec<AgentEvent> {
        match frame {
            Frame::Notification(notification) => self.feed_notification(notification),
            // Responses correlate to requests the Worker made and carry nothing
            // the event stream needs; server requests are approvals, which
            // Codex does not send (ADR-0005).
            Frame::Response { .. } | Frame::ServerRequest { .. } => Vec::new(),
        }
    }

    fn feed_notification(&mut self, notification: &Notification) -> Vec<AgentEvent> {
        match notification {
            Notification::ThreadStarted(started) => {
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

            Notification::TurnStarted(started) => {
                let mut turn = OpenTurn::new(TurnId::new(&started.turn.id));
                // Consume any interrupt that arrived before this turn existed.
                turn.interrupt_requested = std::mem::take(&mut self.interrupt_pending);
                self.turn = Some(turn);
                Vec::new()
            }

            Notification::AgentMessageDelta(delta) => {
                let Some(turn) = self.turn.as_mut() else {
                    return Vec::new();
                };
                let chunk_index = turn.chunk_index;
                turn.chunk_index += 1;
                vec![AgentEvent::Output {
                    turn_id: turn.id.clone(),
                    channel: OutputChannel::Assistant,
                    text: delta.delta.clone(),
                    chunk_index,
                }]
            }

            Notification::ItemCompleted(completed) => self.item_completed(&completed.item),

            Notification::TokenUsageUpdated(update) => {
                self.record_usage(update.turn_id.as_deref(), update.token_usage.last.as_ref());
                Vec::new()
            }

            Notification::RateLimitsUpdated(limits) => limits
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
                            // Codex reports a percentage and never counts, so
                            // this is the Fraction arm by construction.
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
                .unwrap_or_default(),

            Notification::TurnCompleted(completed) => {
                let duration = completed.turn.duration_ms.map(Duration::from_millis);
                self.close_turn(TurnStatus::Completed, duration)
            }

            // The interrupt trap. `turn/interrupt` produces no terminal frame at
            // all — transcript 11 has the thread go idle with no
            // `turn/completed` and no `turn/aborted`, while 394 already-
            // generated deltas keep arriving. Keying on idle is the only way
            // the turn ever closes; an assembler waiting for a terminal frame
            // hangs forever.
            Notification::ThreadStatusChanged(status) if status.is_idle() => {
                let interrupted = self
                    .turn
                    .as_ref()
                    .is_some_and(|turn| turn.interrupt_requested);
                if interrupted {
                    self.close_turn(TurnStatus::Interrupted, None)
                } else {
                    // Idle between turns is ordinary and closes nothing.
                    Vec::new()
                }
            }

            Notification::ThreadStatusChanged(_) | Notification::Other { .. } => Vec::new(),
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

        if item.is_command_execution() {
            let turn_id = turn.id.clone();
            let is_error = item.exit_code.is_some_and(|code| code != 0)
                || item.status.as_deref() == Some("failed");
            return vec![AgentEvent::ToolCompleted {
                turn_id,
                // Codex numbers its own items; there is no separate tool-call
                // id, so the item id is the identity.
                tool_call_id: horde_adapter::ToolCallId::new(item.id.clone().unwrap_or_default()),
                is_error,
            }];
        }

        Vec::new()
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
        let Some(turn) = self.turn.take() else {
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
            // Codex never surfaces a refusal to Horde — it adjudicates
            // internally (ADR-0005), so there is nothing to report here.
            denials: Vec::new(),
            duration: duration.unwrap_or(turn.duration),
        })]
    }
}
