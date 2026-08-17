//! Shared test infrastructure for Horde.
//!
//! Three things live here, all of which exist so that quality checks are
//! written once and applied everywhere:
//!
//! * [`transcripts`] loads the golden recordings captured by spike W1c.
//! * [`conformance`] is the adapter conformance suite — the same checks run
//!   against real adapters and the FakeAdapter, which is what stops the fake
//!   from drifting away from how real harnesses behave (docs/TESTING.md §3).
//! * [`fault`] is the network fault injector (docs/TESTING.md §2): a
//!   programmable TCP proxy every fault-injection suite fronts its transport
//!   with, grown from the `SeverableProxy` seed in `horde-worker`'s suite.
//!
//! This is a normal library crate, not a test-only one, and it is held to the
//! same lint policy as production code. In particular it may not panic: the
//! conformance suite reports findings as values, and the fault injector's
//! job is to cause other code's errors, never its own.

pub mod conformance;
pub mod fault;
pub mod transcripts;

pub use conformance::{
    ConformanceFailure, Policy, TurnRecord, check_capability_honesty, check_session,
    check_session_with, run_turn, run_turn_with,
};
pub use fault::{FaultError, FaultProxy};
pub use transcripts::{HarnessOutputKey, Transcript, TranscriptError};
