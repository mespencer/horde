//! The fault-injection scenarios (docs/TESTING.md §2), one consolidated
//! binary.
//!
//! Every test here asserts the RECOVERY, not the fault: the fault is the
//! arrangement, and the assertion is that the resilience machinery —
//! journal + catch-up cursor, placement re-queue + registration epochs,
//! revival, heartbeats, blob integrity — holds what DESIGN.md §2 claims.
//! Faults are injected with `horde_testkit::FaultProxy` (network) and
//! `horde_fault`'s process harnesses (kill -9); transports are real TCP
//! everywhere, and the processes are real binaries where the fault demands a
//! process to kill.
//!
//! This suite deliberately creates the conditions for hangs, so it holds
//! itself to QUALITY_GATES.md harder than anywhere else: every wait is
//! ceilinged, every poll is count-bounded, and progress is bounded by count
//! or structure, never by clock alone.

// Every module is `#[cfg(test)]` so the workspace's expect-in-tests
// exemption covers the suites' free helper functions, not only `#[test]`
// fns (docs/QUALITY_GATES.md, the clippy gate's canonical fix).
#[cfg(test)]
mod support;

#[cfg(test)]
mod blob_partition;
#[cfg(test)]
mod partition_turn;
#[cfg(test)]
mod server_kill;
#[cfg(test)]
mod slow_link;
#[cfg(test)]
mod worker_kill;
