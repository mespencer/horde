//! Helpers shared by the process-level test binaries.
//!
//! Lives in `tests/support/` (not a test binary itself) so `supervision.rs`
//! and `codex_supervision.rs` drive their stub harnesses through one set of
//! primitives instead of two drifting copies.

use std::path::PathBuf;

/// A binary cargo built alongside the running test.
pub(crate) fn built_binary(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().unwrap_or_default();
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.join(name)
}

/// How long any wait in a process test may take before it is a failure.
///
/// Generous against a loaded machine, and still three orders of magnitude
/// below the point where cargo-mutants gives up.
pub(crate) const CEILING: std::time::Duration = std::time::Duration::from_secs(5);

/// Runs `future` with a ceiling, failing the test if it does not finish.
///
/// Every wait against a child process goes through here, because an unbounded
/// await turns a defect into a hang — and a hang into a mutant nobody
/// evaluated (docs/QUALITY_GATES.md): a `write_line` that writes nothing or a
/// `send` that sends nothing leaves a test waiting for something that never
/// arrives, records a TIMEOUT, and protects nothing. With a ceiling the same
/// mutants fail in seconds and are caught.
pub(crate) async fn within<F: std::future::Future>(what: &str, future: F) -> F::Output {
    match tokio::time::timeout(CEILING, future).await {
        Ok(value) => value,
        Err(_) => panic!("{what} must finish rather than hang"),
    }
}

/// Polls a child's control log, bounded by count, until `expected` holds.
///
/// The predicate is the full expected content rather than "non-empty" because
/// a slow machine can expose a partially flushed line, and reading it early
/// is a flake, not a finding.
pub(crate) async fn control_log_reaches(
    log_path: &std::path::Path,
    expected: impl Fn(&str) -> bool,
) -> String {
    let mut recorded = String::new();
    for _ in 0..250 {
        recorded = std::fs::read_to_string(log_path).unwrap_or_default();
        if expected(&recorded) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    recorded
}
