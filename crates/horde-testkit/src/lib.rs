//! Shared test infrastructure for Horde.
//!
//! Two things live here, both of which exist so that quality checks are written
//! once and applied everywhere:
//!
//! * [`transcripts`] loads the golden recordings captured by spike W1c.
//! * [`conformance`] is the adapter conformance suite — the same checks run
//!   against real adapters and the FakeAdapter, which is what stops the fake
//!   from drifting away from how real harnesses behave (docs/TESTING.md §3).
//!
//! This is a normal library crate, not a test-only one, and it is held to the
//! same lint policy as production code. In particular it may not panic: the
//! conformance suite reports findings as values.

pub mod conformance;
pub mod transcripts;

pub use conformance::{
    ConformanceFailure, Policy, TurnRecord, check_capability_honesty, check_session,
    check_session_with, run_turn, run_turn_with,
};
pub use transcripts::{Transcript, TranscriptError};
