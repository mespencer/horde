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
        while self.pending.is_empty() && self.waiting_on.is_none() && self.stall.is_none() {
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
                    let turn_id = self.turn_id();
                    self.emit_output(OutputChannel::ToolCall, format!("{tool_name} {input}"));
                    self.pending.push_back(AgentEvent::ToolCompleted {
                        turn_id,
                        tool_call_id,
                        is_error: false,
                    });
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

        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<AgentEvent>> {
        // `advance` is synchronous and stops when it hits a `Stall`, leaving the
        // duration behind for this loop to await. Looping — rather than
        // sleeping once at the top — is what keeps a stall from truncating the
        // turn: after waking, there is still a turn to play out.
        loop {
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

            if self.current.is_none() {
                return Ok(None);
            }
            self.advance();

            if self.pending.is_empty() && self.stall.is_none() && self.waiting_on.is_none() {
                // `advance` ran the turn to its end and produced nothing more.
                return Ok(None);
            }
        }
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

#[cfg(test)]
mod guard_tests {
    use horde_adapter::{
        Adapter, AgentSpec, Capabilities, Capability, Delivery, PermissionDecision, ToolCallId,
        TurnInput,
    };
    use serde_json::json;

    use super::FakeAdapter;

    fn spec() -> AgentSpec {
        AgentSpec {
            model: None,
            append_system_prompt: None,
            cwd: None,
            account: None,
            horde_endpoint: None,
            provider_options: std::collections::BTreeMap::new(),
        }
    }

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

    /// A rewriting decision against an adapter that cannot rewrite is refused.
    ///
    /// The `&&` mutant survived, which would have refused *every* decision or
    /// none. Both are wrong in opposite directions, and the fake is where the
    /// suite learns which behaviour to expect.
    #[tokio::test]
    async fn a_rewrite_is_refused_only_when_the_adapter_cannot_rewrite() {
        // Not waiting on anything: the unknown-request guard fires first, which
        // is itself the contract — a decision with no matching request is an
        // error rather than a silent success.
        let mut session = FakeAdapter::new(crate::Script::echoing())
            .with_capabilities(Capabilities::none())
            .spawn(&spec())
            .await
            .expect("spawns");

        let error = session
            .answer_permission(
                &ToolCallId::new("never-asked"),
                PermissionDecision::allow_with(json!({"path": "/safe"})),
            )
            .await
            .expect_err("a decision nobody asked for must be refused");
        assert!(
            error.to_string().contains("never-asked"),
            "and must name the call it could not answer, got {error}"
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
        use crate::{Script, ScriptedTurn, Step};

        let script = Script::of(vec![
            ScriptedTurn::saying("looking into it").with_step(Step::AskPermission {
                tool_call_id: "call-1".to_owned(),
                tool_name: "Bash".to_owned(),
                input: json!({ "command": "docker rm horde-test" }),
            }),
            ScriptedTurn::saying("done"),
        ]);

        let mut session = FakeAdapter::new(script)
            .with_capabilities(
                Capabilities::none()
                    .with(Capability::PreAuthorizeToolCalls)
                    .with(Capability::RewritesToolInput),
            )
            .spawn(&spec())
            .await
            .expect("spawns");
        session.send(TurnInput::queued("go")).await.expect("sends");

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

    /// `shutdown()` must actually finish the session.
    ///
    /// The `Ok(())` mutant survived: a shutdown that returned success without
    /// finishing anything leaves a conformance run believing it cleaned up.
    #[tokio::test]
    async fn shutdown_finishes_the_session() {
        let mut session = FakeAdapter::new(crate::Script::echoing())
            .spawn(&spec())
            .await
            .expect("spawns");
        session.shutdown().await.expect("shuts down");

        assert!(
            session.next_event().await.expect("no error").is_none(),
            "a shut-down session produces nothing further"
        );
    }
}
