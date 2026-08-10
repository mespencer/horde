//! Horde's quality gauntlet: the gates that let agent-written code be trusted
//! through what it passed rather than through line-by-line reading.
//!
//! One binary owns the tools, flags, thresholds and enforcement levels, so a
//! laptop and CI run byte-identical commands and any number in a report
//! reproduces with `gauntlet run <gate>`.
//!
//! Four outcomes, and the differences between them are the point:
//!
//! | outcome | meaning | effect |
//! |---|---|---|
//! | `pass` | measured, clean | none |
//! | `fail` | measured, the code does not meet the bar | reds the run for blocking gates |
//! | `skipped` | there was nothing to measure | none |
//! | `error` | the tooling could not measure | reds the run, always |
//!
//! A gate that never reported at all is rendered as `no result` and reds the
//! run too — an unmeasured gate is a broken gate.
//!
//! # Example
//!
//! ```
//! use horde_gauntlet::gate::{Enforcement, GateOutcome};
//!
//! // `error` is fatal even for a gate that is only observing.
//! assert!(GateOutcome::error("cargo-crap is not installed").reds_run(Enforcement::Observe));
//! // `fail` is a verdict about code, so it only reds a blocking gate.
//! assert!(!GateOutcome::fail("two functions over CRAP 15").reds_run(Enforcement::Observe));
//! ```

// Held here rather than in the workspace lint table: the rest of the workspace
// has not adopted them yet, and a gate that reds on other people's undocumented
// items is a gate that gets turned off. See the root Cargo.toml.
#![warn(missing_docs, missing_debug_implementations)]

pub mod cli;
pub mod command;
pub mod cycles;
pub mod gate;
pub mod gates;
pub mod logs;
pub mod metadata;
pub mod registry;
pub mod report;
pub mod runner;
pub mod tool;

#[cfg(test)]
mod testing;
