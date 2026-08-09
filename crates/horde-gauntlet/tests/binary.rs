//! End-to-end tests of the `gauntlet` binary.
//!
//! Everything the binary does is library code except one thing: turning a
//! verdict into a process exit code. That last step has no seam a unit test can
//! reach — it reads the process's own arguments and working directory — so it is
//! tested the only honest way, by running the real binary and inspecting what
//! the operating system got back.

use std::path::Path;
use std::process::{Command, Output};

/// Runs the built binary in `cwd`.
fn gauntlet(cwd: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_gauntlet"))
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("the gauntlet binary is built as a dependency of this test")
}

/// A throwaway directory containing a workspace manifest.
fn workspace() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp dir");
    std::fs::write(dir.path().join("Cargo.toml"), "[workspace]\nmembers = []\n")
        .expect("workspace manifest");
    dir
}

#[test]
fn listing_the_gates_inside_a_workspace_succeeds() {
    let dir = workspace();
    let output = gauntlet(dir.path(), &["list"]);

    assert!(
        output.status.success(),
        "a listing is always green: {output:?}"
    );
    let listing = String::from_utf8_lossy(&output.stdout);
    assert!(listing.contains("module-structure"), "{listing}");
    assert!(listing.contains("mutants"), "{listing}");
}

#[test]
fn running_outside_a_workspace_exits_non_zero() {
    let dir = tempfile::tempdir().expect("temp dir");
    let output = gauntlet(dir.path(), &["list"]);

    assert!(
        !output.status.success(),
        "measuring the wrong tree is worse than refusing to run: {output:?}"
    );
    let message = String::from_utf8_lossy(&output.stderr);
    assert!(message.contains("[workspace]"), "{message}");
}

#[test]
fn reporting_before_any_run_exits_non_zero() {
    let dir = workspace();
    let output = gauntlet(dir.path(), &["report"]);

    assert!(
        !output.status.success(),
        "there are no results to report yet: {output:?}"
    );
}

#[test]
fn an_unknown_gate_name_exits_non_zero() {
    let dir = workspace();
    let output = gauntlet(dir.path(), &["run", "clipy"]);

    assert!(!output.status.success(), "{output:?}");
    let message = String::from_utf8_lossy(&output.stderr);
    assert!(message.contains("unknown gate `clipy`"), "{message}");
}
