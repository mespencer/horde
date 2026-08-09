//! CRAP: complexity weighted by how little of it is covered.
//!
//! CRAP = `cc + cc² × (1 − coverage)³`. A fully covered function scores its own
//! cyclomatic complexity, so a threshold of 15 means "no function may exceed
//! CC 15 even when perfectly tested, and an untested function may not exceed
//! about 3 branches". That is the number docs/TESTING.md §4 names, and it is a
//! hard gate from day one: this workspace starts from zero, so there is no debt
//! to grandfather.

use serde::Deserialize;

use crate::gates::prelude::*;

/// The cargo-crap version the threshold was calibrated against.
pub const PIN: ToolPin = ToolPin {
    program: "cargo",
    version_args: &["crap", "--version"],
    expected: "0.3.1",
    install: "cargo install --locked cargo-crap@0.3.1",
};

/// No function may score above this.
pub const THRESHOLD: f64 = 15.0;

/// One scored function.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Entry {
    /// Source file, relative to the workspace root.
    pub file: String,
    /// Function name.
    pub function: String,
    /// Line the function starts on.
    pub line: u64,
    /// Cyclomatic complexity.
    pub cyclomatic: f64,
    /// Percentage of the function's lines that are covered.
    pub coverage: f64,
    /// The CRAP score.
    pub crap: f64,
}

#[derive(Deserialize)]
struct Report {
    entries: Vec<Entry>,
}

/// Scores every function against the LCOV the coverage gate produced.
pub fn gate() -> CommandGate {
    CommandGate {
        pin: Some(&PIN),
        build: spec,
        read: Reader::CleanExitOnly(read_scores),
    }
}

/// Reads the JSON report cargo-crap printed.
pub fn read_scores(_ctx: &GateContext, output: &CommandOutput) -> GateOutcome {
    match parse_report(&output.stdout) {
        Err(reason) => GateOutcome::error(reason),
        Ok(entries) => outcome_of(&entries, THRESHOLD),
    }
}

/// The command this gate runs.
///
/// cargo-crap has no package filter, so this gate always scores the whole
/// workspace; `-p` narrowing does not apply and the detail line says so.
pub fn spec(ctx: &GateContext) -> CommandSpec {
    CommandSpec::owned(
        "cargo",
        vec![
            "crap".to_owned(),
            "--workspace".to_owned(),
            "--lcov".to_owned(),
            ctx.lcov_path().display().to_string(),
            "--threshold".to_owned(),
            THRESHOLD.to_string(),
            "--format".to_owned(),
            "json".to_owned(),
        ],
        &ctx.workspace_root,
    )
}

/// Reads cargo-crap's JSON report.
///
/// # Errors
///
/// Returns a message when the output is not a cargo-crap report — the gate
/// measured nothing and must not report a clean zero.
pub fn parse_report(json: &str) -> Result<Vec<Entry>, String> {
    let report: Report = serde_json::from_str(json)
        .map_err(|err| format!("could not read cargo-crap's JSON report: {err}"))?;
    Ok(report.entries)
}

/// Functions scoring above `threshold`, worst first.
pub fn over_threshold(entries: &[Entry], threshold: f64) -> Vec<&Entry> {
    let mut over: Vec<&Entry> = entries
        .iter()
        .filter(|entry| entry.crap > threshold)
        .collect();
    over.sort_by(|a, b| b.crap.total_cmp(&a.crap));
    over
}

/// The worst score in the report, if there is one.
pub fn worst(entries: &[Entry]) -> Option<&Entry> {
    entries.iter().max_by(|a, b| a.crap.total_cmp(&b.crap))
}

/// Turns scored functions into a verdict.
pub fn outcome_of(entries: &[Entry], threshold: f64) -> GateOutcome {
    let Some(worst) = worst(entries) else {
        return GateOutcome::skipped("cargo-crap scored no functions");
    };
    let over = over_threshold(entries, threshold);
    if over.is_empty() {
        return GateOutcome::pass(format!(
            "{} function(s) scored, worst {:.1} ({} at {}:{}), threshold {threshold:.0}",
            entries.len(),
            worst.crap,
            worst.function,
            worst.file,
            worst.line
        ));
    }
    let mut detail = format!(
        "{} function(s) over CRAP {threshold:.0} (whole workspace; cargo-crap has no package filter)",
        over.len()
    );
    for entry in over {
        detail.push_str(&format!(
            "\n  {:.1}  {} at {}:{} (cc {:.0}, coverage {:.1}%)",
            entry.crap, entry.function, entry.file, entry.line, entry.cyclomatic, entry.coverage
        ));
    }
    GateOutcome::fail(detail)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::command::CommandOutput;
    use crate::testing::{ScriptedRunner, exited, failed, repo_ctx, run_gate};

    const REPORT: &str = include_str!("../../fixtures/cargo-crap.json");

    fn entry(function: &str, crap: f64) -> Entry {
        Entry {
            file: "./src/lib.rs".to_owned(),
            function: function.to_owned(),
            line: 1,
            cyclomatic: 4.0,
            coverage: 100.0,
            crap,
        }
    }

    #[test]
    fn the_command_scores_the_workspace_against_the_coverage_gates_lcov() {
        assert_eq!(
            spec(&repo_ctx()).display(),
            "cargo crap --workspace --lcov /repo/target/quality/lcov.info --threshold 15 --format json"
        );
    }

    #[test]
    fn a_real_report_is_read_into_scored_functions() {
        let entries = parse_report(REPORT).expect("the fixture is real cargo-crap output");
        assert_eq!(entries.len(), 2);
        let untested = entries
            .iter()
            .find(|entry| entry.function == "untested")
            .expect("present in the fixture");
        assert!((untested.crap - 6.0).abs() < f64::EPSILON);
        assert!((untested.coverage - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn output_that_is_not_a_report_is_an_error_not_an_empty_report() {
        let message = parse_report("not json").expect_err("invalid");
        assert!(message.contains("could not read cargo-crap"), "{message}");
    }

    #[test]
    fn a_score_exactly_at_the_threshold_is_allowed() {
        assert!(over_threshold(&[entry("f", 15.0)], 15.0).is_empty());
    }

    #[test]
    fn a_score_above_the_threshold_is_reported() {
        let entries = [entry("f", 15.1)];
        assert_eq!(over_threshold(&entries, 15.0).len(), 1);
    }

    #[test]
    fn offenders_are_listed_worst_first() {
        let entries = [entry("mild", 16.0), entry("awful", 40.0)];
        let over = over_threshold(&entries, 15.0);
        assert_eq!(over[0].function, "awful");
        assert_eq!(over[1].function, "mild");
    }

    #[test]
    fn the_worst_score_is_the_maximum_not_the_first() {
        let entries = [entry("a", 3.0), entry("b", 9.0), entry("c", 5.0)];
        assert_eq!(worst(&entries).expect("non-empty").function, "b");
        assert_eq!(worst(&[]), None);
    }

    #[test]
    fn a_report_with_no_functions_is_skipped_not_passed() {
        assert_eq!(
            outcome_of(&[], THRESHOLD),
            GateOutcome::skipped("cargo-crap scored no functions")
        );
    }

    #[test]
    fn a_clean_report_states_the_worst_score_so_the_headroom_is_visible() {
        let GateOutcome::Pass { detail } = outcome_of(&[entry("f", 6.0)], THRESHOLD) else {
            panic!("nothing is over the threshold");
        };
        assert!(detail.contains("1 function(s) scored"), "{detail}");
        assert!(detail.contains("worst 6.0"), "{detail}");
        assert!(detail.contains("threshold 15"), "{detail}");
    }

    #[test]
    fn a_dirty_report_names_every_offender_with_its_score_and_location() {
        let entries = [entry("bad", 22.5), entry("fine", 2.0)];
        let GateOutcome::Fail { detail } = outcome_of(&entries, THRESHOLD) else {
            panic!("a function over the threshold is a verdict about the code");
        };
        assert!(detail.starts_with("1 function(s) over CRAP 15"), "{detail}");
        assert!(detail.contains("22.5  bad at ./src/lib.rs:1"), "{detail}");
        assert!(!detail.contains("fine"), "{detail}");
    }

    #[test]
    fn the_real_fixture_passes_at_the_projects_threshold() {
        let entries = parse_report(REPORT).expect("valid");
        assert_eq!(outcome_of(&entries, THRESHOLD).label(), "pass");
    }

    #[test]
    fn the_gate_is_pinned_to_the_calibrated_cargo_crap() {
        assert_eq!(gate().pin, Some(&PIN));
        assert_eq!(PIN.expected, "0.3.1");
    }

    #[test]
    fn the_gate_runs_the_probe_then_the_scoring_command() {
        let runner = ScriptedRunner::new(vec![
            exited(0, "cargo-crap 0.3.1"),
            CommandOutput::new(Some(0), REPORT, ""),
        ]);
        assert_eq!(run_gate(&gate(), &repo_ctx(), &runner).label(), "pass");
        assert_eq!(runner.command_lines()[0], "cargo crap --version");
        assert!(runner.command_lines()[1].contains("--format json"));
    }

    #[test]
    fn a_failing_scoring_run_is_an_instrument_error() {
        let runner = ScriptedRunner::new(vec![
            exited(0, "cargo-crap 0.3.1"),
            failed(2, "error: no LCOV file"),
        ]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &repo_ctx(), &runner) else {
            panic!("a scoring run that failed did not pass");
        };
        assert!(reason.contains("no LCOV file"), "{reason}");
    }
}
