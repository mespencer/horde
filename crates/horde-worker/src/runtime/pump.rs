//! One agent's event loop: owns the session, obeys control, journals events.
//!
//! Each running agent has one pump task that owns its [`AgentSession`]
//! outright. Control reaches it over a channel from the agent's lane; events
//! leave through [`translate`](crate::translate) into the journal, shaped on
//! the way ([`OutputShaper`]). The select over control and `next_event` is
//! `biased` toward control so a shutdown or an interrupt is never starved by
//! a chatty harness, and it relies on `next_event` being cancel-safe — true of
//! both sessions this Worker drives (tokio's `Lines::next_line` documents it;
//! the fake's only await is a stall sleep). The same cancel-safety is what
//! lets the shaper peek for an already-produced next event without waiting.
//!
//! # A message is settled when the harness takes it
//!
//! A delivery carries its inbox command id. The pump settles that row only
//! once `send` succeeded — a message held for the turn boundary, or one whose
//! harness died first, stays in the inbox for the revival that replays it
//! (ADR-0012). A send that fails while the pump is idle — the harness died
//! between turns and nobody noticed, the classic EPIPE — is not the agent's
//! failure at all: the pump ends with [`PumpExit::HarnessGone`] and the lane
//! revives the agent to deliver what it was handed.

use std::collections::VecDeque;

use horde_adapter::{AgentEvent, AgentSession, PermissionDecision, ToolCallId, TurnInput};
use horde_proto::v1::{AgentLifecycleChanged, AgentLifecycleState, event::Kind};
use tokio::sync::mpsc;

use super::shape::{COALESCE_EVENTS, OutputShaper};
use crate::journal::{AgentProgress, Journal, JournalEntry};
use crate::translate::{AgentContext, failure_reason, journal_entries};

/// What a lane can tell a running pump.
#[derive(Debug)]
pub(super) enum AgentControl {
    /// A message for the agent; `command_id` is its inbox row, settled once
    /// the harness takes the text (`None` for a spawn's initial assignment,
    /// which rides the spawn command, not the inbox).
    Deliver {
        input: TurnInput,
        command_id: Option<String>,
    },
    Answer {
        tool_call_id: ToolCallId,
        decision: PermissionDecision,
    },
    Interrupt,
    Archive,
}

/// How a pump ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PumpExit {
    /// Stopped for good: archived, shut down, or its failure journaled.
    Ended,
    /// The harness was found dead by a send between turns. Nothing was
    /// journaled; whatever it was handed is still in the inbox, and the lane
    /// revives the agent to deliver it.
    HarnessGone,
}

/// What one pump iteration is about to handle.
enum Step {
    Control(Option<AgentControl>),
    Event(Result<Option<AgentEvent>, horde_adapter::AdapterError>),
}

/// Whether the pump should keep polling the session for events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PumpState {
    /// No turn in flight; only control can make something happen.
    Idle,
    /// A turn is producing events.
    Streaming,
    /// The harness is blocked on a permission decision. Polling `next_event`
    /// here would be answered with `AwaitingDecision` by an honest session and
    /// a hang by a piped one; only an answer can advance things.
    AwaitingDecision,
}

/// What the pump does after handling one step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Flow {
    Continue,
    Stop,
    /// See [`PumpExit::HarnessGone`].
    Gone,
}

/// Runs one agent's pump until it stops.
pub(super) async fn pump(
    session: Box<dyn AgentSession>,
    journal: Journal,
    context: AgentContext,
    mut control: mpsc::Receiver<AgentControl>,
    activity: std::sync::Arc<super::activity::Activity>,
) -> PumpExit {
    let agent_id = context.agent_id.clone();
    let mut pump = Pump::new(session, journal, context);
    loop {
        // Whether a turn is in flight: what keeps a soft reclaim out from
        // under a working agent, and dates its idleness (ADR-0014).
        activity.set_busy(&agent_id, pump.state != PumpState::Idle);
        let step = if pump.state == PumpState::Streaming {
            tokio::select! {
                // Biased toward control so a shutdown or interrupt is never
                // starved by a chatty harness.
                biased;
                message = control.recv() => Step::Control(message),
                event = pump.session.next_event() => Step::Event(event),
            }
        } else {
            Step::Control(control.recv().await)
        };

        let flow = match step {
            Step::Control(message) => pump.apply_control(message).await,
            Step::Event(event) => pump.apply_event(event).await,
        };
        match flow {
            Flow::Continue => {}
            Flow::Stop => return PumpExit::Ended,
            Flow::Gone => return PumpExit::HarnessGone,
        }
    }
}

/// One agent's pump: the session it owns, and the steering it holds.
pub(super) struct Pump {
    pub(super) session: Box<dyn AgentSession>,
    pub(super) journal: Journal,
    pub(super) context: AgentContext,
    pub(super) state: PumpState,
    /// Turns sent whose `TurnEnded` has not arrived yet. Usually zero or one,
    /// but an interrupting send makes it two for a moment: the aborted turn's
    /// ending and the new turn's events both still owe the stream something,
    /// and a pump that went idle on the first ending would strand the second
    /// turn unread.
    pub(super) turns_in_flight: u32,
    /// Steering held for the next turn boundary (DESIGN.md §11): a
    /// turn-boundary message that arrives mid-turn waits *here*, in the
    /// orchestrator, because holding the message is the one mechanism that
    /// works against every provider. Interrupt-mode deliveries never queue.
    /// Each keeps its inbox id, settled only when it is finally sent.
    pub(super) queued: VecDeque<(TurnInput, Option<String>)>,
    pub(super) shaper: OutputShaper,
    /// Whether this incarnation's harness has taken a message yet. Only a
    /// proven harness found dead between turns is revived: one that refuses
    /// its very first message — a revival that died at once, or a spawn —
    /// fails loudly, or a harness that cannot start would revive forever.
    pub(super) proven: bool,
}

impl Pump {
    pub(super) fn new(
        session: Box<dyn AgentSession>,
        journal: Journal,
        context: AgentContext,
    ) -> Self {
        Self {
            session,
            journal,
            context,
            state: PumpState::Idle,
            turns_in_flight: 0,
            queued: VecDeque::new(),
            shaper: OutputShaper::default(),
            proven: false,
        }
    }

    /// Obeys one control message.
    async fn apply_control(&mut self, message: Option<AgentControl>) -> Flow {
        match message {
            // The lane dropped this agent (archive, cancel, shutdown).
            None | Some(AgentControl::Archive) => {
                let _ = self.session.shutdown().await;
                Flow::Stop
            }
            Some(AgentControl::Deliver { input, command_id }) => {
                if should_hold_for_boundary(&input, self.state) {
                    self.queued.push_back((input, command_id));
                    return Flow::Continue;
                }
                self.deliver(input, command_id).await
            }
            Some(AgentControl::Answer {
                tool_call_id,
                decision,
            }) => match self
                .session
                .answer_permission(&tool_call_id, decision)
                .await
            {
                Ok(()) => {
                    self.state = PumpState::Streaming;
                    Flow::Continue
                }
                Err(error) => {
                    // A decision for a call that is not waiting (raced with an
                    // interrupt, or duplicated). The agent is not broken;
                    // saying so in the log is the whole handling.
                    tracing::warn!(agent_id = %self.context.agent_id, %error, "permission answer went nowhere");
                    Flow::Continue
                }
            },
            Some(AgentControl::Interrupt) => {
                if let Err(error) = self.session.interrupt().await {
                    tracing::warn!(agent_id = %self.context.agent_id, %error, "interrupt failed");
                }
                // The interrupted turn still closes with TurnEnded, which
                // arrives as an ordinary event — so the pump must be willing
                // to read one even if it was parked on a decision that will
                // now never be answered.
                self.state = state_after_interrupt(self.state);
                Flow::Continue
            }
        }
    }

    /// Sends one message. Taken: its inbox row is settled. Refused by a
    /// proven, idle harness: the harness died between turns and the lane
    /// revives it. Refused otherwise: the agent fails loudly.
    async fn deliver(&mut self, input: TurnInput, command_id: Option<String>) -> Flow {
        let interrupting = input.delivery == horde_adapter::Delivery::Interrupt;
        let revivable = self.proven && self.state == PumpState::Idle;
        match self.session.send(input).await {
            Ok(()) => {
                self.proven = true;
                if let Some(command_id) = command_id
                    && let Err(error) = self.journal.settle_delivery(&command_id).await
                {
                    // The text reached the harness; failing to forget the row
                    // only means a later revival may replay it — at-least-once,
                    // never lost.
                    tracing::warn!(agent_id = %self.context.agent_id, %error, "a delivered message stays in the inbox");
                }
                self.turns_in_flight =
                    turns_owed_after_send(interrupting, self.state, self.turns_in_flight);
                self.state = PumpState::Streaming;
                Flow::Continue
            }
            // Only the harness's *death* is revivable: a write that hung up
            // (`SupervisorError::HungUp`, which the adapters report as
            // `HarnessExited`). Any other refusal is the agent's failure.
            Err(error @ horde_adapter::AdapterError::HarnessExited { .. }) if revivable => {
                tracing::info!(agent_id = %self.context.agent_id, %error, "the harness died between turns; reviving it");
                let _ = self.session.shutdown().await;
                Flow::Gone
            }
            Err(error) => {
                self.record_failure(&failure_reason(&error)).await;
                let _ = self.session.shutdown().await;
                Flow::Stop
            }
        }
    }

    /// Handles one session event, or the session's end. Output is shaped:
    /// whatever further output the harness has *already* produced is merged
    /// in before anything is journaled (never waiting for more), and the
    /// first non-output event found that way is handled right after.
    pub(super) async fn apply_event(
        &mut self,
        event: Result<Option<AgentEvent>, horde_adapter::AdapterError>,
    ) -> Flow {
        let mut next = Some(event);
        while let Some(event) = next.take() {
            let flow = match event {
                Ok(Some(AgentEvent::Output {
                    turn_id,
                    channel,
                    text,
                    ..
                })) => {
                    let mut shaped = self.shaper.absorb(turn_id, channel, &text);
                    next = self.coalesce_ready(&mut shaped).await;
                    shaped.extend(self.shaper.flush());
                    self.journal_events(&shaped).await
                }
                Ok(Some(event)) => self.apply_session_event(event).await,
                Ok(None) => {
                    // EOF mid-conversation: the harness is gone.
                    self.record_failure(&format!(
                        "{}: the harness ended its event stream",
                        crate::translate::PROCESS_DEATH
                    ))
                    .await;
                    Flow::Stop
                }
                Err(error) => {
                    self.record_failure(&failure_reason(&error)).await;
                    let _ = self.session.shutdown().await;
                    Flow::Stop
                }
            };
            if flow != Flow::Continue {
                return flow;
            }
        }
        Flow::Continue
    }

    /// Absorbs output the session already has ready, without waiting,
    /// returning the first event that is not output (or the session's end)
    /// for the caller to handle after journaling what was merged.
    async fn coalesce_ready(
        &mut self,
        shaped: &mut Vec<AgentEvent>,
    ) -> Option<Result<Option<AgentEvent>, horde_adapter::AdapterError>> {
        for _ in 0..COALESCE_EVENTS {
            // A zero timeout polls `next_event` exactly once: ready output is
            // taken, and anything not yet produced is left for the select
            // (cancel-safe, per the module docs).
            match tokio::time::timeout(core::time::Duration::ZERO, self.session.next_event()).await
            {
                Ok(Ok(Some(AgentEvent::Output {
                    turn_id,
                    channel,
                    text,
                    ..
                }))) => shaped.extend(self.shaper.absorb(turn_id, channel, &text)),
                Ok(other) => return Some(other),
                Err(_not_ready) => return None,
            }
        }
        None
    }

    /// Handles one non-output event: state, journal, and the turn boundary.
    async fn apply_session_event(&mut self, event: AgentEvent) -> Flow {
        if let AgentEvent::TurnEnded(outcome) = &event {
            self.turns_in_flight = self.turns_in_flight.saturating_sub(1);
            self.shaper.turn_ended(&outcome.turn_id);
        }
        self.state = state_after(&event, self.turns_in_flight);
        let flow = self.journal_event(&event).await;
        if flow != Flow::Continue {
            return flow;
        }
        // The turn boundary: whatever steering was held for it goes out now,
        // before the harness can start anything else.
        if self.state == PumpState::Idle
            && let Some((next, command_id)) = self.queued.pop_front()
        {
            return self.deliver(next, command_id).await;
        }
        Flow::Continue
    }

    /// Journals shaped output in one transaction.
    async fn journal_events(&mut self, events: &[AgentEvent]) -> Flow {
        let mut entries = Vec::new();
        for event in events {
            match journal_entries(&self.context, event) {
                Ok(more) => entries.extend(more),
                Err(error) => {
                    tracing::error!(agent_id = %self.context.agent_id, %error, "an event could not be translated and was not journaled");
                }
            }
        }
        self.append(entries, AgentProgress::None).await
    }

    /// Journals one event's entries and progress atomically.
    pub(super) async fn journal_event(&mut self, event: &AgentEvent) -> Flow {
        match journal_entries(&self.context, event) {
            Ok(entries) => {
                let progress = agent_progress(&self.context.agent_id, event);
                self.append(entries, progress).await
            }
            Err(error) => {
                tracing::error!(agent_id = %self.context.agent_id, %error, "an event could not be translated and was not journaled");
                Flow::Continue
            }
        }
    }

    /// Appends entries and progress, or stops the agent if the journal
    /// cannot take them. Nothing to append touches nothing — not even the
    /// head watch, which would wake the push pump for nothing.
    async fn append(&mut self, entries: Vec<JournalEntry>, progress: AgentProgress) -> Flow {
        if entries.is_empty() && progress == AgentProgress::None {
            return Flow::Continue;
        }
        if let Err(error) = self.journal.record_batch(entries, progress).await {
            // A journal that cannot append has lost the partition-survival
            // property; running an agent whose evidence is being dropped is
            // worse than stopping it.
            tracing::error!(agent_id = %self.context.agent_id, %error, "journal append failed; stopping the agent");
            let _ = self.session.shutdown().await;
            return Flow::Stop;
        }
        Flow::Continue
    }

    /// Journals this agent's failure, best-effort — the pump is already dying.
    async fn record_failure(&mut self, reason: &str) {
        let entry = JournalEntry {
            session_id: self.context.session_id.clone(),
            agent_id: Some(self.context.agent_id.clone()),
            idempotency_key: String::new(),
            kind: Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                previous_state: AgentLifecycleState::Running.into(),
                state: AgentLifecycleState::Failed.into(),
                reason: reason.to_owned(),
            }),
        };
        if let Err(error) = self.journal.record(entry).await {
            tracing::error!(agent_id = %self.context.agent_id, %error, "could not journal an agent failure");
        }
    }
}

/// Whether a delivery waits for the turn boundary instead of going out now.
///
/// Both halves carry weight, and inline they are not separately reachable in
/// most runs: a turn-boundary message *during* a turn is held (that is what
/// steering means by default), while an interrupt-mode message never waits and
/// an idle agent never makes anything wait.
pub(super) fn should_hold_for_boundary(input: &TurnInput, state: PumpState) -> bool {
    let mid_turn = state != PumpState::Idle;
    let boundary_delivery = input.delivery == horde_adapter::Delivery::TurnBoundary;
    mid_turn && boundary_delivery
}

/// How many turn endings a send leaves outstanding.
///
/// An interrupting send into a live turn aborts it AND starts its own, so
/// both endings are owed; every other send owes exactly one. Both halves of
/// the condition matter and are not separately reachable through the pump —
/// it never sends a queued message mid-turn (it queues them itself), and an
/// idle pump has nothing in flight to add to — which is exactly the shape
/// that hides a flipped operator from an end-to-end suite.
pub(super) fn turns_owed_after_send(interrupting: bool, state: PumpState, owed: u32) -> u32 {
    if interrupting && state != PumpState::Idle {
        owed.saturating_add(1)
    } else {
        1
    }
}

/// Where the pump stands after asking for an interrupt.
///
/// A pump parked on a permission decision must go back to reading events:
/// the interrupt ends the turn, that ending arrives as an ordinary event, and
/// a pump still waiting for a decision nobody will ever send would hold the
/// agent forever. A streaming pump keeps streaming, and an idle one stays
/// idle — there is no turn whose ending it should go looking for.
pub(super) fn state_after_interrupt(state: PumpState) -> PumpState {
    match state {
        PumpState::AwaitingDecision => PumpState::Streaming,
        other => other,
    }
}

/// Where the pump stands after seeing `event`, given how many turns still
/// owe their ending.
///
/// A `TurnEnded` is the boundary only when nothing else is in flight: after
/// an interrupting send, the aborted turn's ending arrives while the new
/// turn is still running, and a pump that went idle there would never read
/// the new turn's events.
pub(super) fn state_after(event: &AgentEvent, turns_in_flight: u32) -> PumpState {
    match event {
        AgentEvent::PermissionRequested { .. } => PumpState::AwaitingDecision,
        AgentEvent::TurnEnded(_) if turns_in_flight == 0 => PumpState::Idle,
        _ => PumpState::Streaming,
    }
}

/// The revival bookkeeping one event advances, keyed to commit with it.
fn agent_progress(agent_id: &str, event: &AgentEvent) -> AgentProgress {
    match event {
        AgentEvent::SessionEstablished {
            provider_session_id,
            ..
        } => AgentProgress::Established {
            agent_id: agent_id.to_owned(),
            provider_session_id: provider_session_id.to_string(),
        },
        AgentEvent::TurnEnded(_) => AgentProgress::TurnTaken {
            agent_id: agent_id.to_owned(),
        },
        _ => AgentProgress::None,
    }
}
