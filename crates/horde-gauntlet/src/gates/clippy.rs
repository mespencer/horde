//! Lints: `cargo clippy --all-targets --all-features -- -D warnings`.
//!
//! This gate is also where the workspace lint policy (no `unwrap`, no `expect`
//! outside tests, no `panic`/`todo`/`unimplemented`/`dbg!`) becomes an error
//! rather than a warning — the deny levels live in the workspace manifest, and
//! `-D warnings` lives here, in one place.

use crate::gates::prelude::*;
use crate::gates::{unexpected_exit, with_packages};

/// Exit code cargo uses when compilation (including deny-level lints) fails.
const COMPILE_FAILURE: i32 = 101;

/// Runs clippy over every target and every feature, warnings denied.
pub fn gate() -> CommandGate {
    CommandGate {
        pin: None,
        build: spec,
        read: Reader::AnyExit(outcome_of),
    }
}

/// The command this gate runs.
pub fn spec(ctx: &GateContext) -> CommandSpec {
    let mut args = with_packages(&["clippy", "--all-targets", "--all-features"], ctx);
    args.push("--".to_owned());
    args.push("-D".to_owned());
    args.push("warnings".to_owned());
    CommandSpec::owned("cargo", args, &ctx.workspace_root)
}

/// Reads a clippy run.
pub fn outcome_of(spec: &CommandSpec, output: &CommandOutput) -> GateOutcome {
    if output.success() {
        return GateOutcome::pass("no clippy or rustc warnings");
    }
    let diagnostics = count_diagnostics(&output.combined());
    if output.status != Some(COMPILE_FAILURE) || diagnostics == 0 {
        return unexpected_exit(spec, output);
    }
    GateOutcome::fail(format!(
        "{diagnostics} clippy/rustc diagnostic(s); see the gate log"
    ))
}

/// Counts the individual diagnostics in a clippy run.
///
/// Cargo's own tallies (`could not compile …`, `aborting due to …`) are also
/// printed as `error:` lines and would double-count.
pub fn count_diagnostics(text: &str) -> usize {
    text.lines()
        .filter(|line| line.starts_with("error"))
        .filter(|line| !is_cargo_tally(line))
        .count()
}

fn is_cargo_tally(line: &str) -> bool {
    line.contains("could not compile") || line.contains("aborting due to")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{exited, failed, repo_ctx};

    const DIRTY: &str = include_str!("../../fixtures/cargo-clippy-denied.txt");

    fn read(output: &CommandOutput) -> GateOutcome {
        outcome_of(&spec(&repo_ctx()), output)
    }

    /// The gate wiring itself is covered once in `gates::tests`.
    #[test]
    fn the_gate_is_a_cargo_subcommand_that_reports_through_its_exit_code() {
        let wiring = gate();
        assert!(
            wiring.pin.is_none(),
            "clippy is pinned by rust-toolchain.toml, not by an install command"
        );
        assert!(matches!(wiring.read, Reader::AnyExit(_)));
    }

    #[test]
    fn the_command_covers_every_target_and_feature_and_denies_warnings() {
        assert_eq!(
            spec(&repo_ctx()).display(),
            "cargo clippy --all-targets --all-features --workspace -- -D warnings"
        );
    }

    #[test]
    fn a_narrowed_run_replaces_the_workspace_flag_with_packages() {
        let ctx = GateContext::new(std::path::Path::new("/repo"), vec!["a".to_owned()]);
        assert_eq!(
            spec(&ctx).display(),
            "cargo clippy --all-targets --all-features -p a -- -D warnings"
        );
    }

    #[test]
    fn a_clean_run_passes() {
        assert_eq!(
            read(&exited(0, "Finished")),
            GateOutcome::pass("no clippy or rustc warnings")
        );
    }

    #[test]
    fn real_clippy_output_is_read_as_a_failure_with_a_diagnostic_count() {
        assert_eq!(
            read(&failed(COMPILE_FAILURE, DIRTY)),
            GateOutcome::fail("2 clippy/rustc diagnostic(s); see the gate log")
        );
    }

    #[test]
    fn cargos_own_tallies_are_not_counted_as_diagnostics() {
        let text = "error: use of `unwrap`\n\
                    error: aborting due to 1 previous error\n\
                    error: could not compile `horde-gauntlet` (lib) due to 1 previous error\n";
        assert_eq!(count_diagnostics(text), 1);
    }

    #[test]
    fn warnings_and_notes_are_not_counted_as_diagnostics() {
        let text = "warning: unused variable\nnote: `-D clippy::unwrap-used` implied here\n";
        assert_eq!(count_diagnostics(text), 0);
    }

    #[test]
    fn error_codes_are_counted() {
        assert_eq!(count_diagnostics("error[E0433]: failed to resolve\n"), 1);
    }

    #[test]
    fn a_crash_with_no_diagnostics_is_an_instrument_error() {
        let GateOutcome::Error { .. } = read(&failed(COMPILE_FAILURE, "")) else {
            panic!("clippy exiting without diagnostics means it did not measure");
        };
    }

    #[test]
    fn an_unexpected_exit_code_is_an_instrument_error_even_with_diagnostics() {
        let GateOutcome::Error { .. } = read(&failed(1, DIRTY)) else {
            panic!("only exit 101 is a lint verdict");
        };
    }
}
