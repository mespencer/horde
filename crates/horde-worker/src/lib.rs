//! The Worker: provider harnesses as supervised child processes.
//!
//! The codecs (`horde-adapter-claude`, `horde-adapter-codex`) are deliberately
//! pure — no process, no clock, no network — which is what lets them be replayed
//! against recorded bytes. This crate is the half they left out: spawning the
//! binary, wiring stdio, isolating an account's config directory, and turning a
//! child's death into an honest error rather than a hang.
//!
//! # Why supervision gets its own crate and its own process tests
//!
//! The bugs that live here do not exist in an in-process fake. A supervisor can
//! deadlock on a pipe, leak a child that outlives its handle, or block forever
//! waiting for an acknowledgement a dead harness will never send. Those are all
//! properties of real `fork`/`exec`/pipe/EOF behaviour, so the tests spawn a
//! real [`stub-harness`](../bin/stub_harness.rs) binary that replays golden
//! transcripts over real stdio. No login, no network, no quota — but real
//! processes.
//!
//! # Process lifetime
//!
//! A supervised child is killed by **handle**, never by name or pattern. Killing
//! by pattern is how a supervisor reaches outside its own subtree and takes down
//! something it did not start.

pub mod blob;
pub mod capabilities;
pub mod claude;
pub mod client;
pub mod codex;
pub mod endpoint;
pub mod error;
pub mod forward;
pub mod harness;
pub mod hub;
pub mod journal;
pub mod login;
pub mod process;
pub mod runtime;
pub mod share;
pub mod startup;
pub mod translate;
pub mod workspace;

pub use blob::AttachmentBroker;
pub use claude::{ClaudeAdapter, LiveClaudeSession};
pub use client::{ApplyCommand, Connection, DialConfig};
pub use codex::{CodexAdapter, ExecInterception, LiveCodexSession};
pub use error::WorkerError;
pub use harness::HarnessProfile;
pub use journal::{
    AgentBookkeeping, AgentProgress, CommandApplication, Journal, JournalEntry, JournalSink,
    RevivableAgent,
};
pub use process::{SupervisedHarness, SupervisorError};
pub use runtime::Runtime;
pub use share::{AgentCheckout, ShareBroker, StampingSink};
pub use translate::AgentContext;
pub use workspace::WorkspaceManager;
