//! The gates themselves: one module per measurement.
//!
//! Each module owns its tool, flags, exclusions and thresholds, because gate
//! definitions live in Rust rather than YAML (docs/TESTING.md §1). Each also
//! separates *building the command* and *reading the output* from *running it*,
//! so both halves are testable against captured tool output.

pub mod clippy;
pub mod coverage;
pub mod crap;
pub mod duplication;
pub mod fmt;
pub mod machete;
pub mod module_structure;
pub mod mutants;
pub mod proto_contract;
pub mod test;

use std::path::Path;

use crate::command::{CommandOutput, CommandRunner, CommandSpec};
use crate::gate::{Gate, GateContext, GateOutcome};
use crate::tool::{ToolPin, check_pin};

/// How a gate turns one command's output into a verdict.
#[derive(Debug, Clone, Copy)]
pub enum Reader {
    /// The gate interprets the exit code itself, because the tool uses a
    /// non-zero code to report a finding (rustfmt, clippy, cargo-machete).
    AnyExit(fn(&CommandSpec, &CommandOutput) -> GateOutcome),
    /// The gate reads only after a clean exit; any other exit means the tool
    /// did not measure, which is an instrument error rather than a verdict.
    CleanExitOnly(fn(&GateContext, &CommandOutput) -> GateOutcome),
}

/// A gate that checks a tool's version, runs one command, and reads the result.
///
/// Six of the ten gates have exactly this shape, so they are values of this
/// type rather than six near-identical `impl Gate` blocks. Only the multi-step
/// gates — the ones that enumerate targets or compute a diff first — implement
/// [`Gate`] themselves.
#[derive(Debug, Clone, Copy)]
pub struct CommandGate {
    /// The tool's pinned version, or `None` for cargo's own subcommands, which
    /// `rust-toolchain.toml` pins instead.
    pub pin: Option<&'static ToolPin>,
    /// Builds the command for this run.
    pub build: fn(&GateContext) -> CommandSpec,
    /// Turns the result into a verdict.
    pub read: Reader,
}

impl Gate for CommandGate {
    fn run(&self, ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
        if let Some(pin) = self.pin
            && let Err(reason) = check_pin(runner, pin, &ctx.workspace_root)
        {
            return GateOutcome::error(reason);
        }
        let spec = (self.build)(ctx);
        let output = match invoke(runner, &spec) {
            Ok(output) => output,
            Err(outcome) => return outcome,
        };
        match self.read {
            Reader::AnyExit(read) => read(&spec, &output),
            Reader::CleanExitOnly(read) if output.success() => read(ctx, &output),
            Reader::CleanExitOnly(_) => unexpected_exit(&spec, &output),
        }
    }
}

/// A gate that checks a tool's version and then measures in several steps.
///
/// The multi-step gates enumerate targets or compute a diff before they can run
/// the tool that produces their verdict, so they cannot be a [`CommandGate`];
/// what they do share is the version check, which lives here rather than being
/// repeated at the top of each one.
#[derive(Debug, Clone, Copy)]
pub struct PinnedGate {
    /// The tool's pinned version.
    pub pin: &'static ToolPin,
    /// Asked before the version probe. `Some(reason)` means there is nothing in
    /// this workspace for the tool to look at, so the gate skips *without*
    /// requiring the tool to be installed — demanding an instrument for a
    /// measurement that does not exist would make an absent tool look like a
    /// broken one.
    pub nothing_to_measure: Option<fn(&GateContext) -> Option<String>>,
    /// Measures, once the pin has been confirmed.
    pub measure: fn(&GateContext, &dyn CommandRunner) -> GateOutcome,
}

impl Gate for PinnedGate {
    fn run(&self, ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
        if let Some(reason) = self.nothing_to_measure.and_then(|ask| ask(ctx)) {
            return GateOutcome::skipped(reason);
        }
        match check_pin(runner, self.pin, &ctx.workspace_root) {
            Err(reason) => GateOutcome::error(reason),
            Ok(()) => (self.measure)(ctx, runner),
        }
    }
}

/// What every gate module needs in order to define a gate.
pub mod prelude {
    pub use crate::command::{CommandOutput, CommandSpec};
    pub use crate::gate::{GateContext, GateOutcome};
    pub use crate::gates::{CommandGate, PinnedGate, Reader};
    pub use crate::tool::ToolPin;
}

/// Runs a command and hands its output to `read`.
///
/// A launch failure short-circuits to an instrument error, so the reader only
/// ever sees a process that actually ran.
pub(crate) fn read_output(
    runner: &dyn CommandRunner,
    spec: &CommandSpec,
    read: impl FnOnce(&CommandOutput) -> GateOutcome,
) -> GateOutcome {
    match invoke(runner, spec) {
        Err(outcome) => outcome,
        Ok(output) => read(&output),
    }
}

/// Reads a command's result only when the command succeeded.
///
/// A tool that exited non-zero for a reason its gate does not model did not
/// measure, so its silence must not be read as a clean sheet.
pub(crate) fn require_success(
    spec: &CommandSpec,
    output: &CommandOutput,
    read: impl FnOnce() -> GateOutcome,
) -> GateOutcome {
    if output.success() {
        read()
    } else {
        unexpected_exit(spec, output)
    }
}

/// Runs a command, turning "could not launch" into an instrument error.
pub(crate) fn invoke(
    runner: &dyn CommandRunner,
    spec: &CommandSpec,
) -> Result<CommandOutput, GateOutcome> {
    runner.run(spec).map_err(GateOutcome::error)
}

/// Fixed cargo arguments followed by this run's package selection.
pub(crate) fn with_packages(fixed: &[&str], ctx: &GateContext) -> Vec<String> {
    let mut args: Vec<String> = fixed.iter().map(|arg| (*arg).to_owned()).collect();
    args.extend(ctx.package_args());
    args
}

/// Reads a file the gate's tool was supposed to have produced.
///
/// A missing or unreadable artifact is an instrument error: the measurement did
/// not happen, whatever the tool's exit code claimed.
pub(crate) fn read_artifact(path: &Path, produced_by: &str) -> Result<String, GateOutcome> {
    std::fs::read_to_string(path).map_err(|err| {
        GateOutcome::error(format!(
            "`{produced_by}` did not leave a readable {}: {err}",
            path.display()
        ))
    })
}

/// The number immediately before `label` in a tool's summary line.
///
/// Tools write their tallies as `42 passed, 3 failed` or `7 missed, 9 caught`
/// with assorted separators, so the reader looks for the label and steps back
/// one token.
pub(crate) fn labelled_count(tokens: &[&str], label: &str) -> Option<usize> {
    let position = tokens
        .iter()
        .position(|token| token.trim_end_matches([',', ';', '.']) == label)?;
    let previous = tokens.get(position.checked_sub(1)?)?;
    previous.trim_end_matches([',', ';', '.']).parse().ok()
}

/// Removes ANSI escape sequences.
///
/// Some tools colour their output whether or not a terminal is attached, and a
/// parser that matches on `error` must not be defeated by an escape sequence
/// sitting between the words.
pub(crate) fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(character) = chars.next() {
        if character != '\u{1b}' {
            out.push(character);
            continue;
        }
        for escaped in chars.by_ref() {
            if escaped.is_ascii_alphabetic() {
                break;
            }
        }
    }
    out
}

/// Renders an exit status for an error message.
pub(crate) fn describe_status(output: &CommandOutput) -> String {
    match output.status {
        Some(code) => format!("exit {code}"),
        None => "killed by a signal".to_owned(),
    }
}

/// The catch-all for an exit code a gate does not know how to interpret.
pub(crate) fn unexpected_exit(spec: &CommandSpec, output: &CommandOutput) -> GateOutcome {
    GateOutcome::error(format!(
        "`{}` ended with {} and output the gate cannot interpret:\n{}",
        spec.display(),
        describe_status(output),
        output.combined().trim()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{ScriptedRunner, exited, failed, repo_ctx};

    const PIN: crate::tool::ToolPin = crate::tool::ToolPin {
        program: "cargo",
        version_args: &["crap", "--version"],
        expected: "0.3.1",
        install: "cargo install --locked cargo-crap@0.3.1",
    };

    fn tool_spec() -> CommandSpec {
        CommandSpec::new("cargo", &["crap"], Path::new("/repo"))
    }

    fn any_exit(_spec: &CommandSpec, output: &CommandOutput) -> GateOutcome {
        GateOutcome::fail(format!("read {:?} {}", output.status, output.stdout))
    }

    fn clean_exit_only(ctx: &GateContext, output: &CommandOutput) -> GateOutcome {
        GateOutcome::pass(format!(
            "read {} in {}",
            output.stdout,
            ctx.workspace_root.display()
        ))
    }

    fn command_gate(pin: Option<&'static crate::tool::ToolPin>, read: Reader) -> CommandGate {
        CommandGate {
            pin,
            build: |ctx| CommandSpec::new("cargo", &["crap"], &ctx.workspace_root),
            read,
        }
    }

    #[test]
    fn a_pinned_gate_checks_the_version_before_running_the_tool() {
        let runner = ScriptedRunner::new(vec![exited(0, "cargo-crap 0.2.0")]);
        let outcome = command_gate(Some(&PIN), Reader::AnyExit(any_exit)).run(&repo_ctx(), &runner);
        assert_eq!(outcome.label(), "error");
        assert_eq!(
            runner.calls().len(),
            1,
            "a gate must not measure with the wrong tool version"
        );
    }

    #[test]
    fn an_unpinned_gate_runs_the_tool_straight_away() {
        let runner = ScriptedRunner::new(vec![exited(7, "output")]);
        let outcome = command_gate(None, Reader::AnyExit(any_exit)).run(&repo_ctx(), &runner);
        assert_eq!(outcome, GateOutcome::fail("read Some(7) output"));
        assert_eq!(runner.command_lines(), vec!["cargo crap"]);
    }

    #[test]
    fn an_any_exit_reader_sees_a_nonzero_exit_because_it_models_the_codes_itself() {
        let runner = ScriptedRunner::new(vec![exited(0, "cargo-crap 0.3.1"), exited(3, "report")]);
        let outcome = command_gate(Some(&PIN), Reader::AnyExit(any_exit)).run(&repo_ctx(), &runner);
        assert_eq!(outcome, GateOutcome::fail("read Some(3) report"));
    }

    #[test]
    fn a_clean_exit_only_reader_runs_after_a_zero_exit() {
        let runner = ScriptedRunner::new(vec![exited(0, "report")]);
        let outcome =
            command_gate(None, Reader::CleanExitOnly(clean_exit_only)).run(&repo_ctx(), &runner);
        assert_eq!(outcome, GateOutcome::pass("read report in /repo"));
    }

    #[test]
    fn a_clean_exit_only_reader_never_sees_a_failed_run() {
        let runner = ScriptedRunner::new(vec![exited(2, "boom")]);
        let outcome =
            command_gate(None, Reader::CleanExitOnly(clean_exit_only)).run(&repo_ctx(), &runner);
        let GateOutcome::Error { reason } = outcome else {
            panic!("a tool that failed did not measure");
        };
        assert!(reason.contains("boom"), "{reason}");
    }

    #[test]
    fn an_unlaunchable_tool_is_an_instrument_error() {
        let runner = ScriptedRunner::failing("no such binary");
        let outcome = command_gate(None, Reader::AnyExit(any_exit)).run(&repo_ctx(), &runner);
        assert_eq!(outcome, GateOutcome::error("no such binary"));
    }

    #[test]
    fn reading_output_hands_the_reader_a_process_that_actually_ran() {
        let runner = ScriptedRunner::new(vec![exited(3, "out")]);
        let outcome = read_output(&runner, &tool_spec(), |output| {
            GateOutcome::pass(format!("{:?} {}", output.status, output.stdout))
        });
        assert_eq!(outcome, GateOutcome::pass("Some(3) out"));
    }

    #[test]
    fn reading_output_short_circuits_a_launch_failure() {
        let runner = ScriptedRunner::failing("no such binary");
        let outcome = read_output(&runner, &tool_spec(), |_| {
            GateOutcome::pass("should not be reached")
        });
        assert_eq!(outcome, GateOutcome::error("no such binary"));
    }

    #[test]
    fn requiring_success_reads_only_after_a_clean_exit() {
        let outcome = require_success(&tool_spec(), &exited(0, ""), || GateOutcome::pass("read"));
        assert_eq!(outcome, GateOutcome::pass("read"));
    }

    #[test]
    fn requiring_success_turns_any_other_exit_into_an_instrument_error() {
        let outcome = require_success(&tool_spec(), &exited(2, "boom"), || {
            GateOutcome::pass("should not be reached")
        });
        let GateOutcome::Error { reason } = outcome else {
            panic!("a tool that failed did not measure");
        };
        assert!(reason.contains("boom"), "{reason}");
    }

    #[test]
    fn a_launch_failure_becomes_an_instrument_error() {
        let runner = ScriptedRunner::failing("no such binary");
        let spec = CommandSpec::new("nope", &[], Path::new("/repo"));
        let outcome = invoke(&runner, &spec).expect_err("launch failed");
        assert_eq!(outcome, GateOutcome::error("no such binary"));
    }

    #[test]
    fn a_nonzero_exit_is_not_a_launch_failure() {
        let runner = ScriptedRunner::new(vec![failed(7, "")]);
        let spec = CommandSpec::new("tool", &[], Path::new("/repo"));
        let output = invoke(&runner, &spec).expect("the process ran");
        assert_eq!(output.status, Some(7));
    }

    #[test]
    fn package_selection_is_appended_to_the_fixed_arguments() {
        let ctx = GateContext::new(Path::new("/repo"), vec!["horde-gauntlet".to_owned()]);
        assert_eq!(
            with_packages(&["clippy", "--all-targets"], &ctx),
            vec!["clippy", "--all-targets", "-p", "horde-gauntlet"]
        );
    }

    #[test]
    fn an_unnarrowed_run_still_states_its_scope() {
        let ctx = GateContext::new(Path::new("/repo"), Vec::new());
        assert_eq!(
            with_packages(&["clippy"], &ctx),
            vec!["clippy", "--workspace"]
        );
    }

    #[test]
    fn reading_an_artifact_returns_its_contents() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("report.json");
        std::fs::write(&path, "{}").expect("write");
        assert_eq!(read_artifact(&path, "cargo crap"), Ok("{}".to_owned()));
    }

    #[test]
    fn a_missing_artifact_is_an_instrument_error_naming_the_tool_and_path() {
        let outcome = read_artifact(Path::new("/nope/report.json"), "cargo crap")
            .expect_err("file is absent");
        let GateOutcome::Error { reason } = outcome else {
            panic!("a missing artifact must be an error, not a verdict");
        };
        assert!(reason.contains("cargo crap"), "{reason}");
        assert!(reason.contains("/nope/report.json"), "{reason}");
    }

    #[test]
    fn a_tally_is_read_from_the_token_before_its_label() {
        let tokens = ["42", "passed,", "3", "failed;", "1", "skipped"];
        assert_eq!(labelled_count(&tokens, "passed"), Some(42));
        assert_eq!(labelled_count(&tokens, "failed"), Some(3));
        assert_eq!(labelled_count(&tokens, "skipped"), Some(1));
    }

    #[test]
    fn an_absent_label_has_no_tally() {
        assert_eq!(labelled_count(&["42", "passed"], "missed"), None);
    }

    #[test]
    fn a_label_with_nothing_before_it_has_no_tally() {
        assert_eq!(labelled_count(&["passed"], "passed"), None);
    }

    #[test]
    fn a_non_numeric_token_before_a_label_has_no_tally() {
        assert_eq!(labelled_count(&["some", "passed"], "passed"), None);
    }

    #[test]
    fn colour_codes_are_removed_but_the_text_between_them_survives() {
        assert_eq!(
            strip_ansi("\u{1b}[1;38;2;219;77;89merror\u{1b}[0m: orphaned"),
            "error: orphaned"
        );
    }

    #[test]
    fn uncoloured_text_passes_through_unchanged() {
        assert_eq!(strip_ansi("No orphans found."), "No orphans found.");
    }

    #[test]
    fn a_truncated_escape_sequence_does_not_leak_into_the_output() {
        assert_eq!(strip_ansi("a\u{1b}[38;2"), "a");
    }

    #[test]
    fn a_status_is_described_by_code_or_by_signal() {
        assert_eq!(describe_status(&failed(2, "")), "exit 2");
        assert_eq!(
            describe_status(&CommandOutput::new(None, "", "")),
            "killed by a signal"
        );
    }

    #[test]
    fn an_uninterpretable_exit_names_the_command_the_status_and_the_output() {
        let spec = CommandSpec::new("cargo", &["crap"], Path::new("/repo"));
        let outcome = unexpected_exit(&spec, &exited(9, "boom\n"));
        let GateOutcome::Error { reason } = outcome else {
            panic!("an uninterpretable exit must be an error");
        };
        assert!(reason.contains("cargo crap"), "{reason}");
        assert!(reason.contains("exit 9"), "{reason}");
        assert!(reason.contains("boom"), "{reason}");
    }
}
