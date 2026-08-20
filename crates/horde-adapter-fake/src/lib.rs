//! A scripted provider harness.
//!
//! Orchestration logic has to be testable without burning tokens or needing
//! provider logins, and that is what this crate is for (docs/TESTING.md §3).
//! Every end-to-end and fault-injection test runs on it by default; a small
//! tagged suite runs against real harnesses on demand.
//!
//! **It is not a mock.** There is no expectation-setting, no verification DSL,
//! and nothing here reimplements what production code does. It is a real
//! implementation of [`Adapter`] and [`AgentSession`] with a different backend:
//! instead of a child process it consults a [`Script`] a test wrote. The
//! sequence and shape of what it emits is held to the same conformance suite as
//! the Claude adapter, which is the mechanism that keeps it from drifting into
//! a convenient fiction.
//!
//! Two details exist specifically to keep it honest:
//!
//! * **Usage is accumulated and then differenced.** The script declares
//!   per-turn tokens, and the fake adds them into a running total which it
//!   feeds through [`UsageAccumulator`] — the same path the Claude adapter is
//!   forced down by a harness that reports lifetime totals. A fake that simply
//!   handed back the script's numbers would exercise none of that.
//! * **Permission requests genuinely block the turn.** A scripted
//!   [`Step::AskPermission`] stops the turn until it is answered, so a test can
//!   exercise the permission chain — including the human-approval layer — with
//!   real waiting rather than a simulated decision.

mod endpoint_client;
pub mod script;

pub use script::{Exhausted, Misbehaviour, Script, ScriptedTurn, Step};

use core::time::Duration;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use horde_adapter::{
    Adapter, AdapterError, AgentEvent, AgentSession, AgentSpec, Capabilities, Capability,
    OutputChannel, PermissionDecision, PermissionDenial, ProposedToolCall, ProviderSessionId,
    Result, TokenUsage, ToolCallId, TurnId, TurnInput, TurnOutcome, TurnStatus, UsageAccumulator,
};

/// The adapter key the fake advertises.
pub const PROVIDER: &str = "fake";

/// A scripted harness.
pub struct FakeAdapter {
    provider: String,
    capabilities: Capabilities,
    script: Script,
    login: Option<horde_adapter::LoginRecipe>,
    next_session: AtomicU64,
}

impl FakeAdapter {
    /// A fully capable scripted harness — the default for orchestration tests.
    #[must_use]
    pub fn new(script: Script) -> Self {
        Self {
            provider: PROVIDER.to_owned(),
            capabilities: Capabilities::none()
                .with(Capability::Resumable)
                .with(Capability::InterruptibleTurn)
                .with(Capability::AmendActiveTurn)
                .with(Capability::PreAuthorizeToolCalls)
                .with(Capability::RewritesToolInput)
                .with(Capability::SystemPromptOverlay)
                .with(Capability::ModelSelection),
            script,
            login: None,
            next_session: AtomicU64::new(1),
        }
    }

    /// Narrows what this harness claims to support.
    ///
    /// Lets a test stand up a deliberately limited provider — the
    /// honestly-terminal, non-resumable kind DESIGN.md §5 has to cope with —
    /// and check that the orchestrator respects the limit instead of assuming
    /// every provider behaves like Claude.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Overrides the advertised provider key, for tests that route by provider.
    #[must_use]
    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = provider.into();
        self
    }

    /// Gives this fake an interactive login recipe — the scripted stand-in
    /// for a provider CLI's login command, so the interactive-auth relay
    /// (DESIGN §4) is testable without a provider account.
    #[must_use]
    pub fn with_login_recipe(mut self, recipe: horde_adapter::LoginRecipe) -> Self {
        self.login = Some(recipe);
        self
    }

    fn mint_session_id(&self) -> ProviderSessionId {
        let ordinal = self.next_session.fetch_add(1, Ordering::Relaxed);
        ProviderSessionId::new(format!("fake-session-{ordinal}"))
    }

    fn session(
        &self,
        provider_session_id: ProviderSessionId,
        turns_taken: u64,
        endpoint: Option<horde_adapter::McpEndpoint>,
    ) -> FakeSession {
        FakeSession {
            script: self.script.clone(),
            capabilities: self.capabilities.clone(),
            provider: self.provider.clone(),
            provider_session_id,
            endpoint,
            pending_call: None,
            in_flight_call: None,
            announced: false,
            turns_taken,
            // The script resumes where the conversation left off, and turn
            // numbering continues from the same point — the two counters only
            // diverge when a steer amends a turn mid-script.
            script_cursor: turns_taken,
            pending: VecDeque::new(),
            steps: VecDeque::new(),
            current: None,
            waiting_on: None,
            denials: Vec::new(),
            chunk_counters: horde_adapter::ChunkCounters::new(),
            cumulative_tokens: TokenUsage::default(),
            cumulative_cost: 0.0,
            usage: UsageAccumulator::new(),
            finished: false,
            misbehaviour: self.script.misbehaviour,
            stall: None,
        }
    }
}

impl Default for FakeAdapter {
    fn default() -> Self {
        Self::new(Script::echoing())
    }
}

#[async_trait]
impl Adapter for FakeAdapter {
    fn provider(&self) -> &str {
        &self.provider
    }

    fn capabilities(&self) -> Capabilities {
        self.capabilities.clone()
    }

    fn login_recipe(&self) -> Option<horde_adapter::LoginRecipe> {
        self.login.clone()
    }

    async fn spawn(&self, spec: &AgentSpec) -> Result<Box<dyn AgentSession>> {
        Ok(Box::new(self.session(
            self.mint_session_id(),
            0,
            spec.horde_endpoint.clone(),
        )))
    }

    async fn resume(
        &self,
        spec: &AgentSpec,
        provider_session_id: &ProviderSessionId,
    ) -> Result<Box<dyn AgentSession>> {
        if !self.capabilities.supports(Capability::Resumable) {
            return Err(AdapterError::Unsupported {
                provider: self.provider.clone(),
                capability: Capability::Resumable,
            });
        }
        // Turn numbering continues where the previous incarnation stopped —
        // the same reconciliation a real harness's adapter performs, so the
        // orchestration tests measured against this fake exercise it. The
        // consumed turns also consume their scripted entries: a script models
        // the whole conversation, not one process's slice of it.
        let turns_taken = spec.turns_taken().map_err(|message| AdapterError::Spawn {
            provider: self.provider.clone(),
            message,
        })?;
        Ok(Box::new(self.session(
            provider_session_id.clone(),
            turns_taken,
            spec.horde_endpoint.clone(),
        )))
    }
}

/// One scheduled Horde verb call, between `advance` and `next_event`.
struct PendingCall {
    tool_call_id: ToolCallId,
    tool_name: String,
    input: serde_json::Value,
}

/// A Horde endpoint call already launched, running in its own task.
///
/// A task rather than an inline await because `next_event` is raced against
/// the control channel in the Worker's pump (`tokio::select!`), and a select
/// arm that loses is *dropped*: an inline HTTP call would be severed mid-wait
/// by any control message — exactly what a permission decision's delivery is.
/// A real harness is its own process and cannot be cancelled that way; the
/// fake owes the same shape. Awaiting the `JoinHandle` by `&mut` is
/// cancellation-safe: losing the race leaves the task running, and the next
/// `next_event` resumes waiting on it.
struct InFlightCall {
    tool_call_id: ToolCallId,
    task: tokio::task::JoinHandle<core::result::Result<String, String>>,
}

impl Drop for InFlightCall {
    fn drop(&mut self) {
        // A session dropped mid-call (archive, shutdown) takes the call with
        // it: kill by handle, never leave a detached HTTP wait behind.
        self.task.abort();
    }
}

/// The turn currently being played.
struct CurrentTurn {
    turn_id: TurnId,
    text: Option<String>,
    status: TurnStatus,
    tokens: TokenUsage,
    cost_usd: f64,
}

/// How many passes one `next_event` call may make without returning.
///
/// See the loop's own comment: the bound is by count because the failure it
/// guards against — a pass that changes nothing — is an awaitless busy spin
/// that starves every clock.
const EVENT_LOOP_CEILING: u32 = 10_000;

/// A live scripted agent.
pub struct FakeSession {
    script: Script,
    capabilities: Capabilities,
    provider: String,
    provider_session_id: ProviderSessionId,
    /// The Horde endpoint this agent was spawned with, when the Worker
    /// supplied one. Makes [`Step::UseHordeTool`] a real MCP call.
    endpoint: Option<horde_adapter::McpEndpoint>,
    /// A verb call `advance` scheduled; launched by `next_event`, which is
    /// where the session is allowed to be async — the same shape as `stall`.
    pending_call: Option<PendingCall>,
    /// The launched call being awaited. Held here rather than on the stack so
    /// a cancelled `next_event` resumes it instead of losing it.
    in_flight_call: Option<InFlightCall>,
    announced: bool,
    turns_taken: u64,
    /// Which scripted turn the next send consumes. Distinct from
    /// [`Self::turns_taken`] because an *amending* send (steering) consumes a
    /// script entry without opening a new turn — the real providers that
    /// steer (Codex, transcript 15) keep the turn id and Horde's turn-id
    /// contract holds the fake to the same shape.
    script_cursor: u64,
    pending: VecDeque<AgentEvent>,
    steps: VecDeque<Step>,
    current: Option<CurrentTurn>,
    waiting_on: Option<ToolCallId>,
    denials: Vec<PermissionDenial>,
    chunk_counters: horde_adapter::ChunkCounters,
    cumulative_tokens: TokenUsage,
    cumulative_cost: f64,
    usage: UsageAccumulator,
    finished: bool,
    misbehaviour: Option<Misbehaviour>,
    /// Set by a `Stall` step; awaited by `next_event`, which is the only async
    /// point in the session.
    stall: Option<Duration>,
}

impl FakeSession {
    fn next_chunk_index(&mut self, channel: OutputChannel) -> u64 {
        if self.misbehaviour == Some(Misbehaviour::RepeatChunkIndex) {
            return 0;
        }
        self.chunk_counters.next(channel)
    }

    fn emit_output(&mut self, channel: OutputChannel, text: String) {
        let chunk_index = self.next_chunk_index(channel);
        let turn_id = self.turn_id();
        self.pending.push_back(AgentEvent::Output {
            turn_id,
            channel,
            text,
            chunk_index,
        });
    }

    fn turn_id(&self) -> TurnId {
        TurnId::new(format!("{}#{}", self.provider_session_id, self.turns_taken))
    }

    /// Plays steps until the queue produces an event, a decision is needed, or
    /// the turn runs out of steps and ends.
    fn advance(&mut self) {
        while self.nothing_more_to_play() {
            let Some(step) = self.steps.pop_front() else {
                self.end_turn();
                return;
            };

            match step {
                Step::Say(text) => self.emit_output(OutputChannel::Assistant, text),
                Step::Think(text) => self.emit_output(OutputChannel::Reasoning, text),
                Step::Stall(duration) => self.stall = Some(duration),
                Step::UseHordeTool {
                    tool_call_id,
                    tool_name,
                    input,
                } => {
                    let tool_call_id = ToolCallId::new(tool_call_id);
                    self.emit_output(OutputChannel::ToolCall, format!("{tool_name} {input}"));
                    if self.endpoint.is_some() {
                        // The real thing: `next_event` dials the endpoint,
                        // and the completion carries the endpoint's verdict.
                        self.pending_call = Some(PendingCall {
                            tool_call_id,
                            tool_name,
                            input,
                        });
                    } else {
                        // No endpoint to dial (a bare-spec session): the
                        // intent still reaches the stream, which is what the
                        // conformance suite measures.
                        let turn_id = self.turn_id();
                        self.pending.push_back(AgentEvent::ToolCompleted {
                            turn_id,
                            tool_call_id,
                            is_error: false,
                        });
                    }
                }
                Step::AskPermission {
                    tool_call_id,
                    tool_name,
                    input,
                } => {
                    let tool_call_id = ToolCallId::new(tool_call_id);
                    self.waiting_on = Some(tool_call_id.clone());
                    let turn_id = self.turn_id();
                    self.pending.push_back(AgentEvent::PermissionRequested {
                        turn_id,
                        request: ProposedToolCall {
                            tool_call_id,
                            tool_name,
                            input,
                        },
                    });
                }
            }
        }
    }

    /// Cuts the rest of the turn in flight, leaving it to end as
    /// [`TurnStatus::Interrupted`] — an interrupt is a way for a turn to end,
    /// not a way to skip ending it.
    /// The next scripted turn, honouring the exhaustion policy.
    fn take_scripted_turn(&mut self, input_text: &str) -> Result<ScriptedTurn> {
        let scripted = usize::try_from(self.script_cursor)
            .ok()
            .and_then(|index| self.script.turns.get(index))
            .cloned();

        let turn = match scripted {
            Some(turn) => turn,
            None => match self.script.exhausted {
                Exhausted::Echo => ScriptedTurn::saying(input_text.to_owned()),
                Exhausted::Exit => {
                    self.finished = true;
                    return Err(AdapterError::HarnessExited { exit_code: Some(0) });
                }
            },
        };
        self.script_cursor = self.script_cursor.saturating_add(1);
        Ok(turn)
    }

    /// What a send does about a turn already in flight.
    ///
    /// `Ok(None)` means the message amended the active turn (steering — the
    /// transcript-15 shape: same turn id, more steps, the amendment's text
    /// becomes the turn's answer, and the chunk counters keep running).
    /// `Ok(Some(turn))` means a fresh turn should open — either nothing was
    /// in flight, or an interrupt delivery just closed what was (the aborted
    /// turn ends first, with no answer, ahead of the new turn's events).
    fn settle_turn_in_flight(
        &mut self,
        delivery: horde_adapter::Delivery,
        turn: ScriptedTurn,
    ) -> Result<Option<ScriptedTurn>> {
        if self.current.is_none() {
            return Ok(Some(turn));
        }
        match delivery {
            horde_adapter::Delivery::TurnBoundary => {
                if !self.capabilities.supports(Capability::AmendActiveTurn) {
                    // A provider without the capability never sees a mid-turn
                    // send — the orchestrator holds the message — so receiving
                    // one is a caller bug to refuse, never a turn to silently
                    // clobber.
                    return Err(AdapterError::Unsupported {
                        provider: self.provider.clone(),
                        capability: Capability::AmendActiveTurn,
                    });
                }
                if let Some(current) = self.current.as_mut() {
                    current.text = turn.text;
                    current.tokens += turn.usage;
                    current.cost_usd += turn.cost_usd;
                }
                self.steps.extend(turn.steps);
                Ok(None)
            }
            horde_adapter::Delivery::Interrupt => {
                self.cut_turn();
                self.end_turn();
                Ok(Some(turn))
            }
        }
    }

    /// Opens `turn` as the one in flight.
    fn open_turn(&mut self, turn: ScriptedTurn) {
        self.turns_taken = self.turns_taken.saturating_add(1);
        self.chunk_counters.reset();
        self.steps = turn.steps.into();
        self.current = Some(CurrentTurn {
            turn_id: self.turn_id(),
            text: turn.text,
            status: turn.status,
            tokens: turn.usage,
            cost_usd: turn.cost_usd,
        });
    }

    /// Announces the session id once — or per-turn, for the misbehaviour that
    /// checks the conformance suite notices a renamed session.
    fn announce(&mut self) {
        match self.misbehaviour {
            Some(Misbehaviour::SkipSessionEstablished) => {}
            Some(Misbehaviour::ChangeSessionIdMidConversation) => {
                // A different name every turn, so revival would target the
                // wrong conversation.
                self.announced = true;
                self.provider_session_id =
                    ProviderSessionId::new(format!("fake-session-{}", self.turns_taken));
                self.pending.push_back(AgentEvent::SessionEstablished {
                    provider_session_id: self.provider_session_id.clone(),
                    model: "fake-model".to_owned(),
                });
            }
            _ => {
                if !self.announced {
                    self.announced = true;
                    self.pending.push_back(AgentEvent::SessionEstablished {
                        provider_session_id: self.provider_session_id.clone(),
                        model: "fake-model".to_owned(),
                    });
                }
            }
        }
    }

    /// Dials the endpoint for one scheduled verb call and turns the verdict
    /// into stream events: the result text (or refusal) as tool output, and
    /// a completion whose error bit is the verdict.
    ///
    /// Launched into its own task — see [`InFlightCall`] on why an inline
    /// await here would be severed by the Worker pump's `select!`.
    fn launch_horde_call(&mut self, call: PendingCall) {
        let endpoint = self.endpoint.clone();
        let tool_call_id = call.tool_call_id.clone();
        let task = tokio::spawn(async move {
            endpoint_client::call_horde_tool(
                endpoint.as_ref(),
                &call.tool_name,
                &call.input,
                // The script's id rides `_meta` the way the real harness's
                // does, so a proxied call's permission request is
                // addressable by it.
                &call.tool_call_id.to_string(),
            )
            .await
        });
        self.in_flight_call = Some(InFlightCall { tool_call_id, task });
    }

    /// Waits out the launched call and turns its outcome into events.
    ///
    /// The `&mut` await on the *stored* handle is the cancellation-safety:
    /// losing the pump's select race drops this future, but the handle stays
    /// in `in_flight_call` and the next `next_event` resumes it — the call
    /// itself never dies with the future (see [`InFlightCall`]).
    async fn settle_horde_call(&mut self) {
        let Some(in_flight) = self.in_flight_call.as_mut() else {
            return;
        };
        let outcome = match (&mut in_flight.task).await {
            Ok(outcome) => outcome,
            Err(join_error) => Err(format!("the endpoint call died: {join_error}")),
        };
        if let Some(finished) = self.in_flight_call.take() {
            // Cloned out rather than moved: the guard's Drop (an abort, a
            // no-op on this finished task) keeps the field.
            self.finish_horde_call(finished.tool_call_id.clone(), outcome);
        }
    }

    /// Whether the turn has nothing left to deliver or wait on: no queued
    /// events, no pending decision, no stall, no call scheduled or in
    /// flight. A named predicate because it is both `advance`'s "keep
    /// playing steps" condition and `next_event`'s "the turn is spent" stop
    /// — a clause dropped from either ends turns early or never.
    fn nothing_more_to_play(&self) -> bool {
        self.pending.is_empty()
            && self.waiting_on.is_none()
            && self.stall.is_none()
            && self.pending_call.is_none()
            && self.in_flight_call.is_none()
    }

    /// Turns a finished call's outcome into stream events.
    fn finish_horde_call(
        &mut self,
        tool_call_id: ToolCallId,
        outcome: core::result::Result<String, String>,
    ) {
        let turn_id = self.turn_id();
        let (text, is_error) = match outcome {
            Ok(text) => (text, false),
            Err(message) => (message, true),
        };
        self.emit_output(OutputChannel::ToolResult, text);
        self.pending.push_back(AgentEvent::ToolCompleted {
            turn_id,
            tool_call_id,
            is_error,
        });
    }

    fn cut_turn(&mut self) {
        self.steps.clear();
        self.waiting_on = None;
        self.stall = None;
        self.pending_call = None;
        // An interrupted call must actually stop: aborting the task drops
        // the HTTP wait, the way killing a real harness would.
        if let Some(in_flight) = self.in_flight_call.take() {
            in_flight.task.abort();
        }
        if let Some(current) = self.current.as_mut() {
            current.status = TurnStatus::Interrupted;
            current.text = None;
        }
    }

    fn end_turn(&mut self) {
        let Some(current) = self.current.take() else {
            return;
        };

        // A real harness reports lifetime totals and leaves the adapter to
        // difference them; the fake does the same so the same code runs.
        self.cumulative_tokens += current.tokens;
        self.cumulative_cost += current.cost_usd;
        let usage = self
            .usage
            .observe(self.cumulative_tokens, self.cumulative_cost);

        if self.misbehaviour == Some(Misbehaviour::NeverEndTurn) {
            return;
        }

        let ended = AgentEvent::TurnEnded(TurnOutcome {
            turn_id: current.turn_id,
            status: current.status,
            text: current.text,
            usage,
            denials: std::mem::take(&mut self.denials),
            duration: Duration::from_millis(1),
        });
        if self.misbehaviour == Some(Misbehaviour::EndTurnTwice) {
            self.pending.push_back(ended.clone());
        }
        self.pending.push_back(ended);
    }
}

#[async_trait]
impl AgentSession for FakeSession {
    fn provider_session_id(&self) -> Option<&ProviderSessionId> {
        self.announced.then_some(&self.provider_session_id)
    }

    async fn send(&mut self, input: TurnInput) -> Result<()> {
        if input.delivery == horde_adapter::Delivery::Interrupt
            && !self.capabilities.supports(Capability::InterruptibleTurn)
        {
            return Err(AdapterError::Unsupported {
                provider: self.provider.clone(),
                capability: Capability::InterruptibleTurn,
            });
        }
        if self.finished {
            return Err(AdapterError::HarnessExited { exit_code: Some(0) });
        }

        let turn = self.take_scripted_turn(&input.text)?;
        let Some(turn) = self.settle_turn_in_flight(input.delivery, turn)? else {
            // The message amended the active turn; nothing new opens.
            return Ok(());
        };
        self.open_turn(turn);
        self.announce();
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<AgentEvent>> {
        // `advance` is synchronous and stops when it hits a `Stall`, leaving the
        // duration behind for this loop to await. Looping — rather than
        // sleeping once at the top — is what keeps a stall from truncating the
        // turn: after waking, there is still a turn to play out.
        //
        // Count-bounded, not clock-bounded: every pass returns, sleeps a
        // stall, launches or settles a call, or plays script steps, so a
        // real script is served in a handful of passes and the ceiling is
        // orders of magnitude above all of them. A pass that changes
        // *nothing* is a defect — and an unbounded no-progress pass here is
        // a busy spin with no await point, which starves the runtime's
        // timers, so no wrapping timeout can catch it; only a count can
        // (docs/QUALITY_GATES.md: a clock cannot bound a hot spin).
        for _ in 0..EVENT_LOOP_CEILING {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }

            if let Some(tool_call_id) = &self.waiting_on {
                return Err(AdapterError::AwaitingDecision {
                    tool_call_id: tool_call_id.clone(),
                });
            }

            if let Some(duration) = self.stall.take() {
                tokio::time::sleep(duration).await;
                continue;
            }

            if let Some(call) = self.pending_call.take() {
                self.launch_horde_call(call);
            }
            if self.in_flight_call.is_some() {
                self.settle_horde_call().await;
                continue;
            }

            if self.current.is_none() {
                return Ok(None);
            }
            self.advance();

            if self.nothing_more_to_play() {
                // `advance` ran the turn to its end and produced nothing more.
                return Ok(None);
            }
        }
        Err(AdapterError::Protocol {
            message: format!(
                "the scripted session made {EVENT_LOOP_CEILING} event-loop passes without \
                 producing an event; a pass that changes nothing is a defect"
            ),
            raw: String::new(),
        })
    }

    async fn answer_permission(
        &mut self,
        tool_call_id: &ToolCallId,
        decision: PermissionDecision,
    ) -> Result<()> {
        if self.waiting_on.as_ref() != Some(tool_call_id) {
            return Err(AdapterError::UnknownPermissionRequest {
                tool_call_id: tool_call_id.clone(),
            });
        }
        if decision.updated_input().is_some()
            && !self.capabilities.supports(Capability::RewritesToolInput)
        {
            return Err(AdapterError::Unsupported {
                provider: self.provider.clone(),
                capability: Capability::RewritesToolInput,
            });
        }

        self.waiting_on = None;
        let turn_id = self.turn_id();

        match decision {
            PermissionDecision::Allow {
                updated_input,
                remember_for_session,
            } => {
                // The remember bit is rendered so an orchestration test can
                // see the adapter received the session-grant scope — the fake
                // models what a codex-shaped harness encodes as
                // `acceptForSession`.
                let mut rendered = updated_input
                    .map_or_else(|| "allowed".to_owned(), |input| format!("allowed {input}"));
                if remember_for_session {
                    rendered.push_str(" (remembered for session)");
                }
                self.emit_output(OutputChannel::ToolResult, rendered);
                self.pending.push_back(AgentEvent::ToolCompleted {
                    turn_id,
                    tool_call_id: tool_call_id.clone(),
                    is_error: false,
                });
            }
            PermissionDecision::Deny { message } => {
                self.denials.push(PermissionDenial {
                    tool_call_id: tool_call_id.clone(),
                    tool_name: "scripted".to_owned(),
                    attempted_input: serde_json::Value::Null,
                });
                self.emit_output(OutputChannel::ToolResult, message);
                self.pending.push_back(AgentEvent::ToolCompleted {
                    turn_id,
                    tool_call_id: tool_call_id.clone(),
                    // A refusal is a tool failure from the agent's point of view.
                    is_error: true,
                });
            }
        }
        Ok(())
    }

    async fn interrupt(&mut self) -> Result<()> {
        if !self.capabilities.supports(Capability::InterruptibleTurn) {
            return Err(AdapterError::Unsupported {
                provider: self.provider.clone(),
                capability: Capability::InterruptibleTurn,
            });
        }

        self.cut_turn();
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.finished = true;
        self.steps.clear();
        self.pending.clear();
        self.current = None;
        Ok(())
    }
}

/// Scaffolding the test modules below share.
///
/// Three copies of the same `spec`, and the same spawn-and-send preamble beside
/// them, is the shape the duplication gate exists to catch. The fix it asks for
/// is one shared helper rather than a wider detection window, and putting these
/// behind `#[cfg(test)] mod` is also what lets them use `expect`.
#[cfg(test)]
mod harness {
    use horde_adapter::{Adapter, AgentSession, AgentSpec, TurnInput};
    use serde_json::json;

    use super::FakeAdapter;
    use crate::{Script, ScriptedTurn, Step};

    /// A spec with nothing configured: what a test asks for when it cares about
    /// the script rather than the spawn.
    pub(crate) fn spec() -> AgentSpec {
        AgentSpec::default()
    }

    /// A script that asks permission for `call-1`, then takes a turn saying
    /// `afterwards` — the step a session must not reach before the decision.
    pub(crate) fn asking_before(afterwards: &str) -> Script {
        Script::of(vec![
            ScriptedTurn::saying("looking into it").with_step(Step::AskPermission {
                tool_call_id: "call-1".to_owned(),
                tool_name: "Bash".to_owned(),
                input: json!({ "command": "docker rm horde-test" }),
            }),
            ScriptedTurn::saying(afterwards),
        ])
    }

    /// A session playing `script` on a fully capable adapter, with its first
    /// message already sent.
    pub(crate) async fn started(script: Script) -> Box<dyn AgentSession> {
        started_on(FakeAdapter::new(script)).await
    }

    /// The same preamble, on an adapter the test has already narrowed.
    ///
    /// Takes the built adapter rather than a script and a `Capabilities` so the
    /// builder chain stays at the call site, where a test that is *about* which
    /// capabilities are set says so in its own body.
    pub(crate) async fn started_on(adapter: FakeAdapter) -> Box<dyn AgentSession> {
        let mut session = adapter.spawn(&spec()).await.expect("spawns");
        session.send(TurnInput::queued("go")).await.expect("sends");
        session
    }
}

#[cfg(test)]
mod horde_call_tests {
    use std::time::Duration;

    use horde_adapter::{ProviderSessionId, ToolCallId};

    use super::{FakeAdapter, FakeSession, InFlightCall};
    use crate::Script;

    /// A bare session with no endpoint and nothing pending — the state the
    /// direct field-level tests arrange from.
    fn session() -> FakeSession {
        FakeAdapter::new(Script::echoing()).session(ProviderSessionId::new("fake-s1"), 0, None)
    }

    /// Dropping an in-flight call aborts its task — kill by handle, the
    /// workspace rule. A `Drop` that silently did nothing would leave a
    /// detached HTTP wait running past the session (archive, shutdown).
    #[tokio::test]
    async fn dropping_an_in_flight_call_aborts_its_task() {
        let task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(String::new())
        });
        let abort = task.abort_handle();
        drop(InFlightCall {
            tool_call_id: ToolCallId::new("call-1"),
            task,
        });

        // Count-bounded: the abort resolves at the sleep's next poll.
        for _ in 0..200_u32 {
            if abort.is_finished() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("dropping the guard must abort the task it holds");
    }

    /// Settling a finished call clears the slot and reports the outcome as
    /// events — directly, so the mutant that replaces the settle with a
    /// no-op fails here in milliseconds instead of spinning `next_event`
    /// into a timeout nobody can read.
    #[tokio::test]
    async fn settling_a_finished_call_clears_the_slot_and_reports() {
        let mut session = session();
        session.in_flight_call = Some(InFlightCall {
            tool_call_id: ToolCallId::new("call-1"),
            task: tokio::spawn(async { Ok("echo:done".to_owned()) }),
        });

        session.settle_horde_call().await;

        assert!(
            session.in_flight_call.is_none(),
            "a settled call must vacate the slot"
        );
        let rendered = format!("{:?}", session.pending);
        assert!(
            rendered.contains("echo:done"),
            "the outcome must become events the pump can read: {rendered}"
        );
        assert!(
            rendered.contains("ToolCompleted"),
            "the completion must follow the output: {rendered}"
        );

        // And with nothing in flight, settling is a no-op, not a hang.
        let mut idle = session;
        idle.pending.clear();
        idle.settle_horde_call().await;
        assert!(idle.pending.is_empty());
    }

    /// The spent-turn predicate's truth table, one clause at a time. It is
    /// both `advance`'s "keep playing" condition and `next_event`'s stop:
    /// a predicate stuck false spins `next_event` forever, stuck true ends
    /// every turn before its steps — this table fails both in microseconds.
    #[tokio::test]
    async fn the_spent_turn_predicate_reads_every_clause() {
        let fresh = session();
        assert!(fresh.nothing_more_to_play(), "a bare session is spent");

        let mut with_pending = session();
        with_pending
            .pending
            .push_back(horde_adapter::AgentEvent::Output {
                turn_id: horde_adapter::TurnId::new("t1"),
                channel: horde_adapter::OutputChannel::Assistant,
                text: "queued".to_owned(),
                chunk_index: 0,
            });
        assert!(!with_pending.nothing_more_to_play(), "queued events count");

        let mut waiting = session();
        waiting.waiting_on = Some(ToolCallId::new("call-1"));
        assert!(!waiting.nothing_more_to_play(), "a pending decision counts");

        let mut stalled = session();
        stalled.stall = Some(Duration::from_millis(1));
        assert!(!stalled.nothing_more_to_play(), "a stall counts");

        let mut scheduled = session();
        scheduled.pending_call = Some(super::PendingCall {
            tool_call_id: ToolCallId::new("call-1"),
            tool_name: "report_result".to_owned(),
            input: serde_json::json!({}),
        });
        assert!(!scheduled.nothing_more_to_play(), "a scheduled call counts");

        let mut in_flight = session();
        in_flight.in_flight_call = Some(InFlightCall {
            tool_call_id: ToolCallId::new("call-1"),
            task: tokio::spawn(async { Ok(String::new()) }),
        });
        assert!(
            !in_flight.nothing_more_to_play(),
            "an in-flight call counts"
        );
    }
}

#[cfg(test)]
mod guard_tests {
    use horde_adapter::{Adapter, Capabilities, Capability, Delivery, TurnInput};

    use super::FakeAdapter;
    use super::harness::{asking_before, spec, started, started_on};

    /// The adapter reports the identity it was configured with.
    ///
    /// `with_provider` and `provider()` both had surviving mutants: the builder
    /// could ignore its argument and the getter could return a constant. The
    /// fake stands in for real providers in conformance, so an adapter that
    /// misreports which provider it is makes every provider-specific assertion
    /// meaningless.
    #[test]
    fn the_configured_provider_is_the_one_reported() {
        assert_eq!(
            FakeAdapter::new(crate::Script::echoing())
                .with_provider("codex")
                .provider(),
            "codex"
        );
        assert_eq!(
            FakeAdapter::new(crate::Script::echoing())
                .with_provider("claude")
                .provider(),
            "claude",
            "two different configurations must not report the same provider"
        );
    }

    /// Capabilities are the fake's contract with the conformance suite.
    ///
    /// A `Default::default()` mutant survived, which would have claimed
    /// *nothing* is supported — and a suite that asks a capability-less adapter
    /// to do capability-gated things gets refusals it would wrongly accept.
    #[test]
    fn configured_capabilities_are_reported_not_defaulted() {
        let claimed = FakeAdapter::new(crate::Script::echoing())
            .with_capabilities(Capabilities::none().with(Capability::Resumable))
            .capabilities();

        assert!(claimed.supports(Capability::Resumable));
        assert_ne!(
            claimed,
            Capabilities::none(),
            "a capabilities() that returned the empty set would make every \
             capability-gated conformance check vacuous"
        );
    }

    /// An interrupt against an adapter that cannot interrupt must be refused.
    ///
    /// Both halves of the guard carry weight, and mutants flipping the `&&`,
    /// the `==`, and the `!` all survived. Getting this wrong in the fake is
    /// the worst case: the suite would either accept a refusal that should not
    /// happen, or miss one that should.
    #[tokio::test]
    async fn interrupt_delivery_is_refused_only_when_unsupported() {
        // Cannot interrupt: the refusal must happen.
        let mut session = FakeAdapter::new(crate::Script::echoing())
            .with_capabilities(Capabilities::none())
            .spawn(&spec())
            .await
            .expect("spawns");
        let refused = session
            .send(TurnInput {
                text: "stop".to_owned(),
                delivery: Delivery::Interrupt,
            })
            .await;
        assert!(refused.is_err(), "an unsupported interrupt must be refused");

        // A queued message on the same adapter must NOT be refused — the guard
        // is about the delivery mode, not about sending at all.
        let mut session = FakeAdapter::new(crate::Script::echoing())
            .with_capabilities(Capabilities::none())
            .spawn(&spec())
            .await
            .expect("spawns");
        assert!(
            session.send(TurnInput::queued("hello")).await.is_ok(),
            "queued delivery needs no interrupt capability"
        );

        // And an interrupt on an adapter that supports it must go through.
        let mut session = FakeAdapter::new(crate::Script::echoing())
            .with_capabilities(Capabilities::none().with(Capability::InterruptibleTurn))
            .spawn(&spec())
            .await
            .expect("spawns");
        assert!(
            session
                .send(TurnInput {
                    text: "stop".to_owned(),
                    delivery: Delivery::Interrupt,
                })
                .await
                .is_ok(),
            "a supported interrupt must not be refused"
        );
    }

    /// A pending decision must stop the script from advancing.
    ///
    /// `advance` loops while there is nothing pending, nothing awaited, and no
    /// stall. The `&&`-to-`||` mutant survived, which would have kept playing
    /// steps straight past an outstanding permission request — the fake would
    /// run tool calls nobody approved, and every conformance run measured
    /// against it would believe the permission chain had been honoured.
    #[tokio::test]
    async fn a_pending_permission_request_halts_the_script() {
        let mut session = started_on(
            FakeAdapter::new(asking_before("done")).with_capabilities(
                Capabilities::none()
                    .with(Capability::PreAuthorizeToolCalls)
                    .with(Capability::RewritesToolInput),
            ),
        )
        .await;

        // Drain until the request surfaces, then confirm the turn has NOT
        // ended: a turn that finished while a decision was outstanding is the
        // bug this guard prevents.
        let mut asked = false;
        let mut ended_early = false;
        for _ in 0..32 {
            match session.next_event().await {
                Ok(Some(horde_adapter::AgentEvent::PermissionRequested { .. })) => {
                    asked = true;
                    break;
                }
                Ok(Some(horde_adapter::AgentEvent::TurnEnded(_))) => {
                    ended_early = true;
                    break;
                }
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => break,
            }
        }

        assert!(asked, "the scripted permission request must surface");
        assert!(
            !ended_early,
            "the turn must not end while a decision is outstanding"
        );
        // Stronger than yielding nothing: the session names what it is waiting
        // for, so a caller that forgot to answer gets told which call is stuck
        // rather than an empty stream it might read as a finished turn.
        let stalled = session.next_event().await;
        assert!(
            stalled
                .as_ref()
                .err()
                .map(ToString::to_string)
                .unwrap_or_default()
                .contains("call-1"),
            "the session must name the outstanding decision, got {stalled:?}"
        );
    }

    /// `shutdown()` must discard work that was still queued.
    ///
    /// The earlier version of this test asserted `next_event` is `None` after
    /// shutdown — which a fresh session does anyway, so it never depended on
    /// shutdown doing anything and the `Ok(())` mutant survived it. This one
    /// leaves events *pending* first: a no-op shutdown hands them out, a real
    /// one drops them.
    #[tokio::test]
    async fn shutdown_discards_events_that_were_still_queued() {
        use crate::{Script, ScriptedTurn};

        let mut session = started(Script::of(vec![
            ScriptedTurn::saying("one"),
            ScriptedTurn::saying("two"),
        ]))
        .await;

        // Take a single event, deliberately leaving the rest queued.
        let first = session.next_event().await.expect("no error");
        assert!(first.is_some(), "the turn must have produced something");

        session.shutdown().await.expect("shuts down");

        assert!(
            session.next_event().await.expect("no error").is_none(),
            "queued events must be discarded — a shutdown that returned success \
             while handing out the rest of the turn would let a Worker believe \
             it had torn the agent down"
        );
    }
}

#[cfg(test)]
mod permission_path_tests {
    use horde_adapter::{
        AgentEvent, AgentSession, Capabilities, Capability, OutputChannel, PermissionDecision,
        ToolCallId,
    };
    use serde_json::json;

    use super::FakeAdapter;
    use super::harness::{asking_before, started_on};

    /// Drives a session to the point where it is genuinely waiting on `call-1`.
    async fn waiting_session(capabilities: Capabilities) -> Box<dyn AgentSession> {
        let mut session = started_on(
            FakeAdapter::new(asking_before("second turn")).with_capabilities(capabilities),
        )
        .await;

        for _ in 0..32 {
            match session.next_event().await {
                Ok(Some(AgentEvent::PermissionRequested { .. })) => return session,
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => break,
            }
        }
        panic!("the scripted permission request must surface");
    }

    /// A plain allow must succeed even on an adapter that cannot rewrite input.
    ///
    /// This is the case the earlier test missed entirely: it only exercised the
    /// unknown-request early return, so control never reached the guard. The
    /// `&&`-to-`||` mutant lives here — with `||`, an adapter lacking
    /// `RewritesToolInput` would refuse *every* decision, including ones
    /// carrying no rewrite at all, and the permission chain would jam on
    /// providers that are perfectly able to answer yes or no.
    #[tokio::test]
    async fn a_plain_allow_is_accepted_by_an_adapter_that_cannot_rewrite() {
        let mut session =
            waiting_session(Capabilities::none().with(Capability::PreAuthorizeToolCalls)).await;

        session
            .answer_permission(&ToolCallId::new("call-1"), PermissionDecision::allow())
            .await
            .expect("a decision carrying no rewrite needs no rewrite capability");
    }

    /// A remembering allow is accepted and observably remembered: the tool
    /// result names the session scope, so an orchestration test can assert
    /// the adapter received what a codex-shaped harness would encode as
    /// `acceptForSession` — the fake models the decision shape end to end.
    #[tokio::test]
    async fn a_remembering_allow_is_rendered_with_its_session_scope() {
        let mut session =
            waiting_session(Capabilities::none().with(Capability::PreAuthorizeToolCalls)).await;

        session
            .answer_permission(
                &ToolCallId::new("call-1"),
                PermissionDecision::allow().remembered(),
            )
            .await
            .expect("the remember bit needs no extra capability");

        let next = session.next_event().await.expect("no error");
        match next {
            Some(AgentEvent::Output {
                channel: OutputChannel::ToolResult,
                text,
                ..
            }) => assert_eq!(text, "allowed (remembered for session)"),
            other => panic!("the tool result must name the scope, got {other:?}"),
        }
    }

    /// A rewriting allow must be refused by an adapter that cannot rewrite.
    ///
    /// The other side of the same guard: reject-don't-drop, so a narrowing that
    /// could not be applied is never mistaken for one that was.
    #[tokio::test]
    async fn a_rewriting_allow_is_refused_by_an_adapter_that_cannot_rewrite() {
        let mut session =
            waiting_session(Capabilities::none().with(Capability::PreAuthorizeToolCalls)).await;

        let refused = session
            .answer_permission(
                &ToolCallId::new("call-1"),
                PermissionDecision::allow_with(json!({ "command": "echo safe" })),
            )
            .await;

        assert!(
            refused.is_err(),
            "a rewrite an adapter cannot apply must be refused, not dropped"
        );
    }

    /// Answering must release exactly the decision's own events, not run ahead.
    ///
    /// `advance` loops while nothing is pending, nothing is awaited and nothing
    /// stalls. Once a decision lands, `waiting_on` clears and the tool result is
    /// queued — so the `&&`-to-`||` mutant makes the loop run anyway and play
    /// the *next* scripted step before those queued events are consumed. The
    /// agent would appear to have moved on from a tool call whose result nobody
    /// had seen yet.
    #[tokio::test]
    async fn answering_releases_the_tool_result_before_anything_else() {
        let mut session = waiting_session(
            Capabilities::none()
                .with(Capability::PreAuthorizeToolCalls)
                .with(Capability::RewritesToolInput),
        )
        .await;

        session
            .answer_permission(&ToolCallId::new("call-1"), PermissionDecision::allow())
            .await
            .expect("answers");

        // The very next event must be the tool's own result.
        let next = session.next_event().await.expect("no error");
        assert!(
            matches!(
                next,
                Some(AgentEvent::Output {
                    channel: OutputChannel::ToolResult,
                    ..
                })
            ),
            "the answered call's result must come first, got {next:?}"
        );

        let after = session.next_event().await.expect("no error");
        assert!(
            matches!(after, Some(AgentEvent::ToolCompleted { .. })),
            "then the call completes, got {after:?}"
        );
    }
}

#[cfg(test)]
mod steering_tests {
    use horde_adapter::{AgentEvent, Capabilities, Capability, TurnInput, TurnStatus};

    use super::FakeAdapter;
    use super::harness::started_on;
    use crate::{Script, ScriptedTurn};

    /// The turn endings among `events`, in order.
    fn endings(events: &[AgentEvent]) -> Vec<&horde_adapter::TurnOutcome> {
        let mut endings = Vec::new();
        for event in events {
            if let AgentEvent::TurnEnded(outcome) = event {
                endings.push(outcome);
            }
        }
        endings
    }

    /// Drains everything currently available without blocking on a stall.
    async fn drain(session: &mut Box<dyn horde_adapter::AgentSession>) -> Vec<AgentEvent> {
        let mut events = Vec::new();
        while let Ok(Some(event)) = session.next_event().await {
            events.push(event);
            if events.len() > 64 {
                break;
            }
        }
        events
    }

    /// A mid-turn send on an amending provider is a steer: one turn, one
    /// ending, and the answer is the answer to the steer — the same shape
    /// codex's transcript 15 pins on the real adapter.
    #[tokio::test]
    async fn a_mid_turn_send_amends_the_active_turn() {
        let mut session = started_on(FakeAdapter::new(Script::of(vec![
            ScriptedTurn::saying("FIRST").with_usage(
                horde_adapter::TokenUsage {
                    input_tokens: 10,
                    output_tokens: 3,
                    ..horde_adapter::TokenUsage::default()
                },
                0.5,
            ),
            ScriptedTurn::saying("STEERED").with_usage(
                horde_adapter::TokenUsage {
                    input_tokens: 7,
                    output_tokens: 2,
                    ..horde_adapter::TokenUsage::default()
                },
                0.25,
            ),
        ])))
        .await;

        // The first turn is open (its steps unplayed); steer into it.
        session
            .send(TurnInput::queued("actually, do this instead"))
            .await
            .expect("an amending provider accepts a mid-turn send");

        let events = drain(&mut session).await;
        let endings = endings(&events);
        assert_eq!(
            endings.len(),
            1,
            "steering amends the turn; it must not open a second one: {events:?}"
        );
        assert_eq!(
            endings[0].text.as_deref(),
            Some("STEERED"),
            "the turn's answer is the answer to the steer"
        );
        assert_eq!(
            endings[0].usage.tokens.input_tokens, 17,
            "an amended turn consumed both halves' work, so it bills both"
        );
        assert_eq!(endings[0].usage.tokens.output_tokens, 5);
        assert!(
            (endings[0].usage.cost_usd - 0.75).abs() < f64::EPSILON,
            "and both halves' cost: {}",
            endings[0].usage.cost_usd
        );

        // And every event of the amended turn carries one turn id.
        let mut ids: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::Output { turn_id, .. } => Some(turn_id.clone()),
                AgentEvent::TurnEnded(outcome) => Some(outcome.turn_id.clone()),
                _ => None,
            })
            .collect();
        ids.dedup();
        assert_eq!(ids.len(), 1, "one turn, one id: {ids:?}");
    }

    /// A provider without `AmendActiveTurn` never legitimately receives a
    /// mid-turn send — the orchestrator holds the message — so the fake must
    /// refuse rather than silently clobber the open turn.
    #[tokio::test]
    async fn a_mid_turn_send_without_the_capability_is_refused() {
        let mut session = started_on(
            FakeAdapter::new(Script::of(vec![
                ScriptedTurn::saying("open"),
                ScriptedTurn::saying("never reached"),
            ]))
            .with_capabilities(Capabilities::none().with(Capability::InterruptibleTurn)),
        )
        .await;

        let refused = session.send(TurnInput::queued("mid-turn")).await;
        assert!(
            refused.is_err(),
            "an open turn must not be silently replaced"
        );
        assert!(
            refused
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default()
                .contains("AmendActiveTurn"),
            "the refusal names what is missing"
        );
    }

    /// An interrupt delivery mid-turn ends the cancelled turn first — with no
    /// answer — and the message then runs as its own turn.
    #[tokio::test]
    async fn an_interrupt_delivery_closes_the_turn_then_runs_its_own() {
        let mut session = started_on(FakeAdapter::new(Script::of(vec![
            ScriptedTurn::saying("doomed work"),
            ScriptedTurn::saying("REPLACEMENT"),
        ])))
        .await;

        session
            .send(TurnInput::interrupting("stop, do this"))
            .await
            .expect("an interrupting send is accepted");

        let events = drain(&mut session).await;
        let endings = endings(&events);
        assert_eq!(
            endings.len(),
            2,
            "the cancelled turn and the new one both end"
        );
        assert_eq!(endings[0].status, TurnStatus::Interrupted);
        assert_eq!(
            endings[0].text, None,
            "a cancelled turn has no answer, even though one was scripted"
        );
        assert_eq!(endings[1].status, TurnStatus::Completed);
        assert_eq!(endings[1].text.as_deref(), Some("REPLACEMENT"));
        assert_ne!(
            endings[0].turn_id, endings[1].turn_id,
            "the replacement is its own turn"
        );
    }
}

#[cfg(test)]
mod steering_and_revival_tests {
    use horde_adapter::{Adapter, AgentEvent, ProviderSessionId, TurnInput, TurnStatus};

    use super::FakeAdapter;
    use super::harness::spec;
    use crate::{Script, ScriptedTurn, Step};

    /// An interrupting message aborts the turn in flight, and that turn still
    /// closes — `Interrupted`, queued ahead of anything the new turn emits.
    #[tokio::test]
    async fn an_interrupting_message_closes_the_turn_in_flight_first() {
        let adapter = FakeAdapter::new(Script::of(vec![
            ScriptedTurn::saying("one")
                .with_step(Step::Say("never played".to_owned()))
                .with_step(Step::Say("nor this".to_owned())),
            ScriptedTurn::saying("the steered answer"),
        ]));
        let mut session = adapter.spawn(&spec()).await.expect("spawns");

        session.send(TurnInput::queued("go")).await.expect("sends");
        // Take one event, leaving the turn genuinely in flight.
        assert!(session.next_event().await.expect("no error").is_some());

        session
            .send(TurnInput::interrupting("change of plan"))
            .await
            .expect("an interrupting send is accepted");

        // The very next turn ending must be the aborted turn's, and it must
        // be Interrupted — a fake that dropped the old turn silently would
        // hide a missing TurnEnded from every runtime test.
        let mut endings = Vec::new();
        let mut said = Vec::new();
        while let Ok(Some(event)) = session.next_event().await {
            match event {
                AgentEvent::TurnEnded(outcome) => endings.push(outcome.status),
                AgentEvent::Output { text, .. } => said.push(text),
                _ => {}
            }
        }

        assert_eq!(
            endings,
            vec![TurnStatus::Interrupted, TurnStatus::Completed],
            "the aborted turn ends first, then the steered turn runs"
        );
        assert!(
            said.iter().any(|text| text == "the steered answer"),
            "the interrupting message starts its own turn: {said:?}"
        );
        assert!(
            !said.iter().any(|text| text == "never played"),
            "the aborted turn's remaining steps must not play: {said:?}"
        );
    }

    /// The other half of the abort-first guard: a *queued* send mid-turn must
    /// not fabricate an Interrupted ending — only an interrupting delivery
    /// aborts the turn in flight.
    #[tokio::test]
    async fn a_queued_send_mid_turn_does_not_interrupt_the_turn_in_flight() {
        let adapter = FakeAdapter::new(Script::of(vec![
            ScriptedTurn::saying("one").with_step(Step::Say("more".to_owned())),
            ScriptedTurn::saying("two"),
        ]));
        let mut session = adapter.spawn(&spec()).await.expect("spawns");

        session.send(TurnInput::queued("go")).await.expect("sends");
        assert!(session.next_event().await.expect("no error").is_some());

        session
            .send(TurnInput::queued("a second message"))
            .await
            .expect("a queued send is accepted");

        let mut endings = Vec::new();
        while let Ok(Some(event)) = session.next_event().await {
            if let AgentEvent::TurnEnded(outcome) = event {
                endings.push(outcome.status);
            }
        }
        assert!(
            !endings.contains(&TurnStatus::Interrupted),
            "nothing asked for an interrupt, so nothing may end as one: {endings:?}"
        );
    }

    /// Revival continues the turn numbering and the script position, so a
    /// revived fake behaves like a revived harness rather than a fresh one.
    #[tokio::test]
    async fn a_resumed_session_continues_numbering_and_script_position() {
        let adapter = FakeAdapter::new(Script::of(vec![
            ScriptedTurn::saying("turn one"),
            ScriptedTurn::saying("turn two"),
            ScriptedTurn::saying("turn three"),
        ]));

        let mut revived = adapter
            .resume(
                &spec().with_turns_taken(2),
                &ProviderSessionId::new("fake-session-1"),
            )
            .await
            .expect("resumes");
        revived
            .send(TurnInput::queued("continue"))
            .await
            .expect("sends");

        let mut turn_ids = Vec::new();
        let mut said = Vec::new();
        while let Ok(Some(event)) = revived.next_event().await {
            match event {
                AgentEvent::TurnEnded(outcome) => turn_ids.push(outcome.turn_id.to_string()),
                AgentEvent::Output { text, .. } => said.push(text),
                _ => {}
            }
        }

        assert_eq!(
            turn_ids,
            vec!["fake-session-1#3".to_owned()],
            "a revived session must not re-mint turn ids the journal already keyed"
        );
        assert_eq!(
            said,
            vec!["turn three".to_owned()],
            "the consumed turns consumed their scripted entries too"
        );
    }

    /// A malformed turn count is refused at resume, never read as zero — zero
    /// would silently re-key turns the journal already holds.
    #[tokio::test]
    async fn a_malformed_turn_count_fails_the_resume_loudly() {
        let adapter = FakeAdapter::default();
        let mut bad = spec();
        bad.provider_options.insert(
            horde_adapter::TURNS_TAKEN_OPTION.to_owned(),
            "many".to_owned(),
        );

        let refused = adapter
            .resume(&bad, &ProviderSessionId::new("fake-session-1"))
            .await;
        assert!(
            refused.is_err(),
            "a count that cannot be read must refuse the revival"
        );
    }
}

#[cfg(test)]
mod stall_tests {
    use std::time::Duration;

    use horde_adapter::AgentEvent;

    use super::harness::started;
    use crate::{Script, ScriptedTurn, Step};

    /// Long enough that reaching it means the stall was not honoured.
    const FOREVER: Duration = Duration::from_secs(3600);

    /// A stall must stop the turn where it stands, with steps still to play.
    ///
    /// `advance` loops while nothing is pending, nothing is awaited **and**
    /// nothing stalls. The `&&`-to-`||` mutant reassociates that into "nothing
    /// pending **or** (nothing awaited and nothing stalling)", so an empty queue
    /// alone keeps it playing — straight through the stall and into the steps
    /// behind it.
    ///
    /// The stall has to be the turn's *first* step for that to be visible.
    /// `advance` is only ever entered with an empty queue, and under the mutant
    /// the queue stops being empty the moment any step emits something: a
    /// `Stall` reached after a `Say` finds the queue full, and the mutant leaves
    /// the loop exactly as the original does. Both then sleep, both hold the
    /// caller, and a test that only asks whether the drain completed cannot tell
    /// them apart. Reaching the stall on an empty queue is what separates them,
    /// and then the difference is not *whether* the caller is held but *what it
    /// was handed first*.
    ///
    /// Real agents stall on a long tool call or a slow model. A fake that played
    /// on regardless would make every timeout and backpressure test measured
    /// against it meaningless, because the condition they exist to exercise
    /// would never occur.
    #[tokio::test]
    async fn a_turn_that_stalls_first_plays_nothing_behind_the_stall() {
        let mut session = started(Script::of(vec![ScriptedTurn {
            steps: vec![
                Step::Stall(FOREVER),
                Step::Say("played on regardless".to_owned()),
            ],
            ..ScriptedTurn::saying("done")
        }]))
        .await;

        // Bounded, because the stall is real time and an unbounded drain would
        // wait it out. Whatever arrives before the bound is what the session was
        // willing to hand over while stalled.
        let mut seen: Vec<AgentEvent> = Vec::new();
        let held = tokio::time::timeout(Duration::from_millis(300), async {
            while let Ok(Some(event)) = session.next_event().await {
                seen.push(event);
                if seen.len() > 16 {
                    break;
                }
            }
        })
        .await;

        assert!(
            held.is_err(),
            "a stalled turn must hold the caller rather than run to its end: {seen:?}"
        );
        assert!(
            !seen.iter().any(is_the_step_behind_the_stall),
            "the step behind the stall was played before the stall elapsed: {seen:?}"
        );
    }

    fn is_the_step_behind_the_stall(event: &AgentEvent) -> bool {
        matches!(event, AgentEvent::Output { text, .. } if text == "played on regardless")
    }
}

#[cfg(test)]
mod endpoint_call_tests {
    use horde_adapter::{Adapter, AgentEvent, AgentSpec, McpEndpoint, OutputChannel, TurnInput};
    use serde_json::json;

    use crate::endpoint_client::scripted_server;
    use crate::{FakeAdapter, Script, ScriptedTurn, Step};

    /// A session spawned with an endpoint really dials it: the scheduled
    /// call executes against the wire, its result text lands as tool output,
    /// and the completion carries the verdict. An `execute_horde_call` that
    /// silently did nothing would swallow the step whole — the turn would
    /// end with the intent announced and no result ever heard.
    #[tokio::test]
    async fn a_scripted_verb_call_executes_against_the_endpoint() {
        let url = scripted_server::serve(vec![
            scripted_server::http(
                "200 OK",
                Some("sess-1"),
                r#"{"jsonrpc":"2.0","id":0,"result":{}}"#,
            ),
            scripted_server::http("202 Accepted", None, ""),
            scripted_server::http(
                "200 OK",
                None,
                r#"{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"recorded"}]}}"#,
            ),
        ])
        .await;

        let adapter = FakeAdapter::new(Script::of(vec![
            ScriptedTurn::saying("reporting").with_step(Step::UseHordeTool {
                tool_call_id: "call-1".to_owned(),
                tool_name: "report_result".to_owned(),
                input: json!({"status": "success", "summary": "done"}),
            }),
        ]));
        let spec = AgentSpec {
            horde_endpoint: Some(McpEndpoint::Http {
                url,
                headers: vec![("Authorization".to_owned(), "Bearer t".to_owned())],
            }),
            ..AgentSpec::default()
        };
        let mut session = adapter.spawn(&spec).await.expect("spawns");
        session
            .send(TurnInput::queued("go"))
            .await
            .expect("the first message sends");

        let mut tool_results = Vec::new();
        let mut completions = Vec::new();
        while let Some(event) = session.next_event().await.expect("a healthy stream") {
            match event {
                AgentEvent::Output {
                    channel: OutputChannel::ToolResult,
                    text,
                    ..
                } => tool_results.push(text),
                AgentEvent::ToolCompleted { is_error, .. } => completions.push(is_error),
                AgentEvent::TurnEnded(_) => break,
                _ => {}
            }
        }

        assert_eq!(
            tool_results,
            vec!["recorded".to_owned()],
            "the endpoint's own answer is the tool's output"
        );
        assert_eq!(
            completions,
            vec![false],
            "a served call completes without error"
        );
    }
}
