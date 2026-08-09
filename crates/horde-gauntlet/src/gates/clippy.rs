//! Lints: `cargo clippy --all-targets --all-features -- -D warnings`.
//!
//! This gate is also where the workspace lint policy (no `unwrap`, no `expect`
//! outside tests, no `panic`/`todo`/`unimplemented`/`dbg!`) becomes an error
//! rather than a warning — the deny levels live in the workspace manifest, and
//! `-D warnings` lives here, in one place.

use crate::command::{CommandOutput, CommandRunner, CommandSpec};
use crate::gate::{Gate, GateContext, GateOutcome};
use crate::gates::{invoke, unexpected_exit, with_packages};

/// Exit code cargo uses when compilation (including deny-level lints) fails.
const COMPILE_FAILURE: i32 = 101;

/// Runs clippy over every target and every feature, warnings denied.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClippyGate;

impl Gate for ClippyGate {
    fn run(&self, ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
        let spec = spec(ctx);
        match invoke(runner, &spec) {
            Err(outcome) => outcome,
            Ok(output) => outcome_of(&spec, &output),
        }
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
    use std::path::Path;

    use super::*;
    use crate::testing::ScriptedRunner;

    const DIRTY: &str = include_str!("../../fixtures/cargo-clippy-denied.txt");

    #[test]
    fn the_command_covers_every_target_and_feature_and_denies_warnings() {
        let ctx = GateContext::new(Path::new("/repo"), Vec::new());
        assert_eq!(
            spec(&ctx).display(),
            "cargo clippy --all-targets --all-features --workspace -- -D warnings"
        );
    }

    #[test]
    fn a_narrowed_run_replaces_the_workspace_flag_with_packages() {
        let ctx = GateContext::new(Path::new("/repo"), vec!["a".to_owned()]);
        assert_eq!(
            spec(&ctx).display(),
            "cargo clippy --all-targets --all-features -p a -- -D warnings"
        );
    }

    #[test]
    fn a_clean_run_passes() {
        let ctx = GateContext::new(Path::new("/repo"), Vec::new());
        let runner = ScriptedRunner::new(vec![CommandOutput::new(Some(0), "", "Finished")]);
        assert_eq!(
            ClippyGate.run(&ctx, &runner),
            GateOutcome::pass("no clippy or rustc warnings")
        );
    }

    #[test]
    fn real_clippy_output_is_read_as_a_failure_with_a_diagnostic_count() {
        let ctx = GateContext::new(Path::new("/repo"), Vec::new());
        let runner = ScriptedRunner::new(vec![CommandOutput::new(Some(101), "", DIRTY)]);
        assert_eq!(
            ClippyGate.run(&ctx, &runner),
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
        let ctx = GateContext::new(Path::new("/repo"), Vec::new());
        let runner = ScriptedRunner::new(vec![CommandOutput::new(Some(101), "", "")]);
        let GateOutcome::Error { .. } = ClippyGate.run(&ctx, &runner) else {
            panic!("clippy exiting without diagnostics means it did not measure");
        };
    }

    #[test]
    fn an_unexpected_exit_code_is_an_instrument_error_even_with_diagnostics() {
        let ctx = GateContext::new(Path::new("/repo"), Vec::new());
        let runner = ScriptedRunner::new(vec![CommandOutput::new(Some(1), "", DIRTY)]);
        let GateOutcome::Error { .. } = ClippyGate.run(&ctx, &runner) else {
            panic!("only exit 101 is a lint verdict");
        };
    }

    #[test]
    fn a_missing_cargo_is_an_instrument_error() {
        let ctx = GateContext::new(Path::new("/repo"), Vec::new());
        let runner = ScriptedRunner::failing("could not launch `cargo clippy`");
        assert_eq!(
            ClippyGate.run(&ctx, &runner),
            GateOutcome::error("could not launch `cargo clippy`")
        );
    }
}
