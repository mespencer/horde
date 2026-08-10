//! The provider-harness seam.
//!
//! Horde treats every provider the same way: a supervised child process with a
//! wire protocol (DESIGN.md §4). This crate is the shape of that treatment —
//! one trait pair plus the domain vocabulary each harness is normalized into.
//! It holds no I/O and knows about no particular provider, so the rules it
//! states are enforceable against all of them.
//!
//! # The seam in one paragraph
//!
//! An [`Adapter`] is a factory bound to one provider: it advertises
//! [`Capabilities`] and mints [`AgentSession`]s, either fresh
//! ([`Adapter::spawn`]) or revived from a previous conversation
//! ([`Adapter::resume`]). A session is a live agent you send messages to and
//! pull events from, one [`AgentEvent`] at a time, until a
//! [`AgentEvent::TurnEnded`] closes the turn.
//!
//! # Why the trait is shaped this way
//!
//! *Split into factory and session* because placement binds an agent to one
//! provider account for its life (DESIGN.md §5): the factory carries the
//! binding, the session carries the conversation.
//!
//! *Pull-based [`AgentSession::next_event`] rather than a [`Stream`] impl*
//! because the Worker's journaling loop wants to decide when to read, back
//! pressure included, and because a plain `async fn` stays object-safe under
//! `async_trait` while a `Stream` associated type would not. The Worker holds
//! `Box<dyn Adapter>` — it picks an adapter by provider name at runtime.
//!
//! *Permission answers on the session rather than through a callback* because
//! the decision is made far away — possibly on someone's phone, minutes later
//! (DESIGN.md §10) — and a callback would either block the adapter's internals
//! or invent a second scheduling context inside it. Spike W1c confirmed a real
//! harness will wait: a 180-second stall was answered with no timeout.
//!
//! # What an implementation owes its caller
//!
//! These are the invariants the shared conformance suite in `horde-testkit`
//! checks, and they are the reason a Worker can trust an adapter it has never
//! seen:
//!
//! 1. **Exactly one [`AgentEvent::TurnEnded`] per message sent.** No message
//!    ends silently, and none ends twice.
//! 2. **[`AgentEvent::SessionEstablished`] precedes any output**, so the
//!    identifier that makes an agent revivable reaches the journal before
//!    anything can be lost.
//! 3. **Usage is reported as a per-turn delta**, never as a running total. See
//!    [`usage`] for why this is the adapter's job and not the Worker's.
//! 4. **Capabilities are honest.** Asking for something unclaimed returns
//!    [`AdapterError::Unsupported`]; it never silently no-ops. This matters
//!    most for [`Capability::PreAuthorizeToolCalls`]: a provider that cannot
//!    offer a tool call to Horde before it runs must say so, because the
//!    permission chain would otherwise appear to gate calls it never saw.
//! 5. **A claimed [`Capability::InterruptibleTurn`] means the session survives
//!    the interrupt** and accepts the next message.
//!
//! [`Stream`]: https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html

pub mod capability;
pub mod error;
pub mod event;
pub mod identity;
pub mod ids;
pub mod permission;
pub mod replay;
pub mod spec;
pub mod usage;

pub use capability::{Capabilities, Capability, PermissionEnforcement};
pub use error::AdapterError;
pub use event::{AgentEvent, OutputChannel, TurnOutcome, TurnStatus};
pub use identity::RevivalIdentity;
pub use ids::{ProviderAccount, ProviderSessionId, ToolCallId, TurnId, account_dir};
pub use permission::{PermissionDecision, PermissionDenial, ProposedToolCall};
pub use replay::{LineDecoder, ReplaySession};
pub use spec::{AgentSpec, Delivery, McpEndpoint, TurnInput};
pub use usage::{
    HeadroomSource, RateWindow, TokenUsage, UsageAccumulator, UsageDelta, Utilization,
};

use async_trait::async_trait;

/// Shorthand for fallible adapter operations.
pub type Result<T> = core::result::Result<T, AdapterError>;

/// One provider harness, bound to one account.
#[async_trait]
pub trait Adapter: Send + Sync {
    /// Stable adapter key, e.g. `"claude"` or `"codex"`. Matches
    /// `horde.v1.ConcreteModel.provider`.
    fn provider(&self) -> &str;

    /// What this harness can do. Callers are entitled to rely on this.
    fn capabilities(&self) -> Capabilities;

    /// Starts a new conversation.
    ///
    /// **Not guaranteed to be side-effect-free.** A harness may write to
    /// ambient machine state as part of starting up: Codex's `thread/start`
    /// records the working directory as a trusted project in
    /// `~/.codex/config.toml` (spike W2c), which then changes how that harness
    /// behaves for every later agent in the same directory. Adapters do not
    /// promise purity here, and callers must not assume an unmodified
    /// environment between spawns. Containing those effects — sandbox profiles
    /// around the whole process (DESIGN.md §10) — is the Worker's job, not the
    /// adapter's.
    async fn spawn(&self, spec: &AgentSpec) -> Result<Box<dyn AgentSession>>;

    /// Revives a conversation from a previous process.
    ///
    /// Requires [`Capability::Resumable`]; adapters without it return
    /// [`AdapterError::Unsupported`] rather than quietly starting a fresh
    /// conversation that has lost its context.
    ///
    /// The `spec` must carry the same account the session was recorded under —
    /// provider sessions are scoped to their credential bundle.
    async fn resume(
        &self,
        spec: &AgentSpec,
        provider_session_id: &ProviderSessionId,
    ) -> Result<Box<dyn AgentSession>>;
}

/// A live agent: send messages in, pull events out.
#[async_trait]
pub trait AgentSession: Send {
    /// The harness's conversation identifier, once it has named one.
    ///
    /// `None` before the first [`AgentEvent::SessionEstablished`]. Journal it
    /// as soon as it appears: without it the agent cannot be revived.
    fn provider_session_id(&self) -> Option<&ProviderSessionId>;

    /// Sends a message to the agent, starting a turn.
    ///
    /// A [`Delivery::Interrupt`] message aborts any turn in flight first, and
    /// requires [`Capability::InterruptibleTurn`].
    async fn send(&mut self, input: TurnInput) -> Result<()>;

    /// Pulls the next event, waiting if the agent is still working.
    ///
    /// `None` means the session is finished and will produce nothing further.
    async fn next_event(&mut self) -> Result<Option<AgentEvent>>;

    /// Answers an outstanding [`AgentEvent::PermissionRequested`].
    ///
    /// Unblocks the turn. Answering a call that is not waiting is
    /// [`AdapterError::UnknownPermissionRequest`] rather than a no-op, because
    /// a decision that went nowhere must not look like one that was applied.
    async fn answer_permission(
        &mut self,
        tool_call_id: &ToolCallId,
        decision: PermissionDecision,
    ) -> Result<()>;

    /// Aborts the turn in flight, leaving the session usable.
    ///
    /// The interrupted turn still closes with an [`AgentEvent::TurnEnded`],
    /// carrying [`TurnStatus::Interrupted`] — an interrupt is a way for a turn
    /// to end, not a way to skip ending it.
    ///
    /// Requires [`Capability::InterruptibleTurn`].
    async fn interrupt(&mut self) -> Result<()>;

    /// Ends the session and releases the harness.
    ///
    /// The conversation itself outlives this call: an agent goes idle with
    /// context retained, and dies at archive, not at shutdown (DESIGN.md §5).
    async fn shutdown(&mut self) -> Result<()>;
}
