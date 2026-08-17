//! The end-to-end tier (docs/TESTING.md §2), as a crate of its own.
//!
//! Everything of substance is in `tests/e2e_suite/`: scenarios that run
//! `horde-server`, one or two `horde-worker` daemons and the `horde` CLI as
//! **real separate processes** over real TCP with real, production-issued
//! identities — enrollment, spawn-to-output, permission approval, revival
//! across a worker restart, revocation, and a payload crossing two workers
//! through the git Relay. The hermetic provider is the fake adapter
//! (TESTING.md §3); nothing here spends a token.
//!
//! # Why a separate crate
//!
//! The suite needs all three product binaries at once, which no single
//! product crate's test target can honestly claim to own; and keeping it out
//! of `horde-server-testkit` keeps that crate's mutation runs from paying
//! for whole-system scenarios on every mutant. This library target is
//! deliberately empty — the crate *is* its tests.
