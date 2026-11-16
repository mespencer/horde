//! The multi-node tier: Horde's three product binaries, each in its own
//! Docker container with its own hostname and address, joined through the
//! real enrollment flows and talking mTLS over a real network.
//!
//! This library target is empty on purpose — the crate is its test suites
//! (`tests/multinode_suite`, `tests/real_harness`), which drive
//! `docker/multinode/compose.yaml`. See docs/TESTING.md, "Multi-node over
//! Docker".
