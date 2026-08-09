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
pub mod test;

use std::path::Path;

use crate::command::{CommandOutput, CommandRunner, CommandSpec};
use crate::gate::{GateContext, GateOutcome};

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
    use crate::testing::ScriptedRunner;

    #[test]
    fn a_launch_failure_becomes_an_instrument_error() {
        let runner = ScriptedRunner::failing("no such binary");
        let spec = CommandSpec::new("nope", &[], Path::new("/repo"));
        let outcome = invoke(&runner, &spec).expect_err("launch failed");
        assert_eq!(outcome, GateOutcome::error("no such binary"));
    }

    #[test]
    fn a_nonzero_exit_is_not_a_launch_failure() {
        let runner = ScriptedRunner::new(vec![CommandOutput::new(Some(7), "", "")]);
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
        assert_eq!(
            describe_status(&CommandOutput::new(Some(2), "", "")),
            "exit 2"
        );
        assert_eq!(
            describe_status(&CommandOutput::new(None, "", "")),
            "killed by a signal"
        );
    }

    #[test]
    fn an_uninterpretable_exit_names_the_command_the_status_and_the_output() {
        let spec = CommandSpec::new("cargo", &["crap"], Path::new("/repo"));
        let outcome = unexpected_exit(&spec, &CommandOutput::new(Some(9), "boom\n", ""));
        let GateOutcome::Error { reason } = outcome else {
            panic!("an uninterpretable exit must be an error");
        };
        assert!(reason.contains("cargo crap"), "{reason}");
        assert!(reason.contains("exit 9"), "{reason}");
        assert!(reason.contains("boom"), "{reason}");
    }
}
