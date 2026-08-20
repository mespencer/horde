//! Every `horde-cli` integration suite, in one test binary.
//!
//! One binary instead of eight, deliberately: cargo-mutants relinks every
//! test target of the mutated package for every mutant, and the wave-5
//! profile showed that per-mutant build-and-link floor — not test time —
//! dominating the `--full` sweep (docs/QUALITY_GATES.md, "A faster test
//! tool is a measurement, not an assumption"). Folding the suites into one
//! crate root turns eight links per mutant into one, and costs the plain
//! test gate nothing: the runner still schedules the tests in parallel
//! within the binary.
//!
//! Each module below is a former standalone test file, unchanged beyond
//! module-path hygiene; `common` is the loopback-server harness they share.

mod common;

mod binary;
mod blob;
mod cli;
mod login_pty;
mod round_trip;
mod secure;
mod tui_drive;
mod tui_pty;
mod verbs;
