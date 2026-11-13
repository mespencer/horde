//! Every automated `horde-worker` integration suite, in one test binary.
//!
//! One binary instead of six, deliberately: cargo-mutants relinks every
//! test target of the mutated package for every mutant, and the wave-5
//! profile showed that per-mutant build-and-link floor — not test time —
//! dominating the `--full` sweep (docs/QUALITY_GATES.md, "A faster test
//! tool is a measurement, not an assumption"). Folding the suites into one
//! crate root turns six links per mutant into one, and costs the plain
//! test gate nothing: the runner still schedules the tests in parallel
//! within the binary.
//!
//! `live_smoke.rs` stays a separate binary on purpose — it drives the
//! *real* provider binaries, is `#[ignore]`d for automation, and its
//! documented invocation is `--test live_smoke -- --ignored`.
//!
//! Each module below is a former standalone test file, unchanged beyond
//! module-path hygiene; `support` is the process-test toolbox they share.

#[cfg(test)]
mod support;

mod attachments;
mod codex_supervision;
mod connection;
mod daemon;
// Gated like `support`: this suite keeps its helpers as free functions, and
// the lint policy exempts them under `cfg(test)` — which an integration test
// binary always compiles with (docs/QUALITY_GATES.md, the clippy section).
#[cfg(test)]
mod forwards;
// Gated like `forwards`, for the same free-helper reason.
#[cfg(test)]
mod hub_link;
// Gated like `forwards`, for the same free-helper reason.
#[cfg(test)]
mod logins;
mod payload_share;
mod supervision;
// Gated like `forwards`, for the same free-helper reason.
#[cfg(test)]
mod workspace_lifecycle;
mod workspaces;
