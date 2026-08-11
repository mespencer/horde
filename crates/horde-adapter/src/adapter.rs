//! The two traits every provider implements.
//!
//! They live beside the vocabulary modules rather than in the crate root, so
//! that a module which the root re-exports — [`crate::replay`], for one — can
//! depend on them without depending back on the root.

use async_trait::async_trait;

use crate::capability::Capabilities;
use crate::error::Result;
use crate::event::AgentEvent;
use crate::ids::{ProviderSessionId, ToolCallId};
use crate::permission::PermissionDecision;
use crate::spec::{AgentSpec, TurnInput};

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
