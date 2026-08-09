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

pub mod script;

pub use script::{Exhausted, Script, ScriptedTurn, Step};

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
                .with(Capability::PermissionPrompt)
                .with(Capability::RewritesToolInput)
                .with(Capability::SystemPromptOverlay)
                .with(Capability::ModelSelection),
            script,
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

    fn mint_session_id(&self) -> ProviderSessionId {
        let ordinal = self.next_session.fetch_add(1, Ordering::Relaxed);
        ProviderSessionId::new(format!("fake-session-{ordinal}"))
    }

    fn session(&self, provider_session_id: ProviderSessionId) -> FakeSession {
        FakeSession {
            script: self.script.clone(),
            capabilities: self.capabilities.clone(),
            provider: self.provider.clone(),
            provider_session_id,
            announced: false,
            turns_taken: 0,
            pending: VecDeque::new(),
            steps: VecDeque::new(),
            current: None,
            waiting_on: None,
            denials: Vec::new(),
            chunk_counters: Vec::new(),
            cumulative_tokens: TokenUsage::default(),
            cumulative_cost: 0.0,
            usage: UsageAccumulator::new(),
            finished: false,
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

    async fn spawn(&self, _spec: &AgentSpec) -> Result<Box<dyn AgentSession>> {
        Ok(Box::new(self.session(self.mint_session_id())))
    }

    async fn resume(
        &self,
        _spec: &AgentSpec,
        provider_session_id: &ProviderSessionId,
    ) -> Result<Box<dyn AgentSession>> {
        if !self.capabilities.supports(Capability::Resumable) {
            return Err(AdapterError::Unsupported {
                provider: self.provider.clone(),
                capability: Capability::Resumable,
            });
        }
        Ok(Box::new(self.session(provider_session_id.clone())))
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

/// A live scripted agent.
pub struct FakeSession {
    script: Script,
    capabilities: Capabilities,
    provider: String,
    provider_session_id: ProviderSessionId,
    announced: bool,
    turns_taken: u64,
    pending: VecDeque<AgentEvent>,
    steps: VecDeque<Step>,
    current: Option<CurrentTurn>,
    waiting_on: Option<ToolCallId>,
    denials: Vec<PermissionDenial>,
    chunk_counters: Vec<(OutputChannel, u64)>,
    cumulative_tokens: TokenUsage,
    cumulative_cost: f64,
    usage: UsageAccumulator,
    finished: bool,
}

impl FakeSession {
    fn next_chunk_index(&mut self, channel: OutputChannel) -> u64 {
        match self
            .chunk_counters
            .iter_mut()
            .find(|(known, _)| *known == channel)
        {
            Some((_, next)) => {
                let index = *next;
                *next = next.saturating_add(1);
                index
            }
            None => {
                self.chunk_counters.push((channel, 1));
                0
            }
        }
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
        while self.pending.is_empty() && self.waiting_on.is_none() {
            let Some(step) = self.steps.pop_front() else {
                self.end_turn();
                return;
            };

            match step {
                Step::Say(text) => self.emit_output(OutputChannel::Assistant, text),
                Step::Think(text) => self.emit_output(OutputChannel::Reasoning, text),
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

        self.pending.push_back(AgentEvent::TurnEnded(TurnOutcome {
            turn_id: current.turn_id,
            status: current.status,
            text: current.text,
            usage,
            denials: std::mem::take(&mut self.denials),
            duration: core::time::Duration::from_millis(1),
        }));
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

        let scripted = usize::try_from(self.turns_taken)
            .ok()
            .and_then(|index| self.script.turns.get(index))
            .cloned();

        let turn = match scripted {
            Some(turn) => turn,
            None => match self.script.exhausted {
                Exhausted::Echo => ScriptedTurn::saying(input.text.clone()),
                Exhausted::Exit => {
                    self.finished = true;
                    return Err(AdapterError::HarnessExited { exit_code: Some(0) });
                }
            },
        };

        self.turns_taken = self.turns_taken.saturating_add(1);
        self.chunk_counters.clear();
        self.steps = turn.steps.into();
        self.current = Some(CurrentTurn {
            turn_id: self.turn_id(),
            text: turn.text,
            status: turn.status,
            tokens: turn.usage,
            cost_usd: turn.cost_usd,
        });

        if !self.announced {
            self.announced = true;
            self.pending.push_back(AgentEvent::SessionEstablished {
                provider_session_id: self.provider_session_id.clone(),
                model: "fake-model".to_owned(),
            });
        }

        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<AgentEvent>> {
        if self.pending.is_empty()
            && let Some(tool_call_id) = &self.waiting_on
        {
            return Err(AdapterError::AwaitingDecision {
                tool_call_id: tool_call_id.clone(),
            });
        }

        if self.pending.is_empty() {
            if self.current.is_none() {
                return Ok(None);
            }
            self.advance();
        }

        Ok(self.pending.pop_front())
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
            PermissionDecision::Allow { updated_input } => {
                let rendered = updated_input
                    .map_or_else(|| "allowed".to_owned(), |input| format!("allowed {input}"));
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

        // Drop the rest of the turn, but still close it — an interrupt is a way
        // for a turn to end, not a way to skip ending it.
        self.steps.clear();
        self.waiting_on = None;
        if let Some(current) = self.current.as_mut() {
            current.status = TurnStatus::Interrupted;
            current.text = None;
        }
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
