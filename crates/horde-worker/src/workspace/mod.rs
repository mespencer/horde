//! Workspace Instances on this Worker's disk.
//!
//! The Server owns Definitions; this module owns their materialization —
//! "Instances are never self-initiated: they exist because the Server asked"
//! (`MaterializeWorkspaceCommand`) — and the per-agent Checkout Policies from
//! DESIGN §7: `shared` works in the Instance itself, `worktree` (the default)
//! composes one git worktree per repo under an agent-private root, and
//! `fresh-clone` clones every repo for total isolation.
//!
//! ADR-0014 adds how a checkout starts and ends: every new checkout starts
//! from a freshly fetched `origin/<default_branch>` ([`base`]), a lazy
//! workspace checks out only the repos an agent asks for ([`manifest`]), one
//! branch name may span every repo, each checkout is recorded in a ledger
//! beside it ([`ledger`]), and disk comes back in two tiers ([`reclaim`],
//! [`removal`], [`cleanup`]).
//!
//! # The state file is the truth
//!
//! Each Instance root carries the full `WorkspaceDefinition` it was
//! materialized from, encoded protobuf, at [`STATE_FILE`]. That file is what
//! DESIGN §7 calls the worker-local truth: the Server's view is a cache
//! corrected by reports built from it, and checkout reads it rather than
//! needing the Server to repeat the definition on every spawn.
//!
//! # Failure is a report, not a crash
//!
//! Materialization never errors: a repo that fails to clone produces a
//! `DEGRADED` report whose detail says which repo and why. Checkout *does*
//! error, into the spawn-failed lifecycle path: an agent cannot run in a
//! checkout that does not exist.

pub mod base;
pub mod cleanup;
pub(crate) mod git;
pub mod ledger;
pub mod manager;
pub mod manifest;
pub mod paths;
pub mod policy;
pub mod reclaim;
pub mod removal;
#[cfg(test)]
pub(crate) mod test_git;

pub use manager::{
    AddedRepo, CheckoutRequest, FsStats, PreparedCheckout, StatvfsStats, WorkspaceManager,
    mode_name, resolution_name,
};
pub use paths::{STATE_FILE, timestamp};
