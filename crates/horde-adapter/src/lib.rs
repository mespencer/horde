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

pub mod adapter;
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
pub use spec::{AgentSpec, Delivery, McpEndpoint, TURNS_TAKEN_OPTION, TurnInput};
pub use usage::{
    HeadroomSource, RateWindow, TokenUsage, UsageAccumulator, UsageDelta, Utilization,
};

pub use adapter::{Adapter, AgentSession};
pub use error::Result;
