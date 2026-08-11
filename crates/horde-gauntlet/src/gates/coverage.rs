//! Coverage: `cargo llvm-cov`, producing the LCOV the crap gate consumes.
//!
//! Blocking here means the measurement must *happen*. There is deliberately no
//! percentage threshold yet: docs/TESTING.md §4 says an absolute threshold is
//! chosen from evidence, and on day one there is no evidence. The gate records
//! the number so the evidence accumulates.

use std::path::Path;

use crate::gates::prelude::*;
use crate::gates::read_artifact;

/// The cargo-llvm-cov version the gate is calibrated against.
pub const PIN: ToolPin = ToolPin {
    program: "cargo",
    version_args: &["llvm-cov", "--version"],
    expected: "0.8.7",
    install: "cargo install --locked cargo-llvm-cov@0.8.7",
};

/// Measures line coverage and writes LCOV to `target/quality/lcov.info`.
pub fn gate() -> CommandGate {
    CommandGate {
        pin: Some(&PIN),
        build: spec,
        read: Reader::CleanExitOnly(read_lcov),
    }
}

/// Reads the LCOV file the run was supposed to leave behind.
pub fn read_lcov(ctx: &GateContext, _output: &CommandOutput) -> GateOutcome {
    match read_artifact(&ctx.lcov_path(), "cargo llvm-cov") {
        Err(outcome) => outcome,
        Ok(lcov) => outcome_of(&lcov, &ctx.lcov_path()),
    }
}

/// The command this gate runs.
pub fn spec(ctx: &GateContext) -> CommandSpec {
    let mut args = vec![
        "llvm-cov".to_owned(),
        "--all-features".to_owned(),
        "--lcov".to_owned(),
        "--output-path".to_owned(),
        ctx.lcov_path().display().to_string(),
    ];
    args.extend(ctx.package_args());
    CommandSpec::owned("cargo", args, &ctx.workspace_root)
}

/// Reads the LCOV the run produced.
pub fn outcome_of(lcov: &str, path: &Path) -> GateOutcome {
    let Some(totals) = line_totals(lcov) else {
        return GateOutcome::error(format!(
            "{} contains no LCOV line records — coverage was not measured",
            path.display()
        ));
    };
    if totals.found == 0 {
        return GateOutcome::skipped("no instrumented lines to cover".to_owned());
    }
    GateOutcome::pass(format!(
        "lines {:.1}% ({}/{}); no threshold yet, recording the number",
        totals.percent(),
        totals.hit,
        totals.found
    ))
}

/// Line-coverage totals summed across every LCOV record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineTotals {
    /// Instrumented lines that were executed at least once.
    pub hit: u64,
    /// Instrumented lines in total.
    pub found: u64,
}

impl LineTotals {
    /// Percentage of instrumented lines that were executed.
    pub fn percent(self) -> f64 {
        if self.found == 0 {
            return 0.0;
        }
        #[expect(clippy::cast_precision_loss, reason = "line counts are far below 2^53")]
        {
            (self.hit as f64 / self.found as f64) * 100.0
        }
    }
}

/// Sums the `LH:`/`LF:` records of an LCOV file.
///
/// Returns `None` when the file carries no line records at all, which means the
/// run did not measure anything rather than measuring zero.
pub fn line_totals(lcov: &str) -> Option<LineTotals> {
    let mut totals = LineTotals { hit: 0, found: 0 };
    let mut saw_a_record = false;
    for line in lcov.lines().map(str::trim) {
        if let Some(count) = record_value(line, "LH:") {
            totals.hit += count;
            saw_a_record = true;
        } else if let Some(count) = record_value(line, "LF:") {
            totals.found += count;
            saw_a_record = true;
        }
    }
    saw_a_record.then_some(totals)
}

fn record_value(line: &str, prefix: &str) -> Option<u64> {
    line.strip_prefix(prefix)?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandOutput;
    use crate::testing::{ScriptedRunner, ctx_at, exited, failed, repo_ctx, run_gate};

    const LCOV: &str = include_str!("../../fixtures/lcov.info");

    #[test]
    fn the_command_writes_lcov_where_the_crap_gate_looks_for_it() {
        let ctx = repo_ctx();
        assert_eq!(
            spec(&ctx).display(),
            "cargo llvm-cov --all-features --lcov --output-path /repo/target/quality/lcov.info --workspace"
        );
    }

    #[test]
    fn a_narrowed_run_passes_the_package_flags_through() {
        let ctx = GateContext::new(Path::new("/repo"), vec!["a".to_owned()]);
        assert!(
            spec(&ctx).display().ends_with("-p a"),
            "{}",
            spec(&ctx).display()
        );
    }

    #[test]
    fn real_lcov_totals_are_summed_across_records() {
        let totals = line_totals(LCOV).expect("the fixture is real llvm-cov output");
        assert_eq!(totals.hit, 10);
        assert_eq!(totals.found, 13);
    }

    #[test]
    fn a_file_without_line_records_is_unmeasured_rather_than_zero() {
        assert_eq!(line_totals("TN:\nSF:src/lib.rs\nend_of_record\n"), None);
        assert_eq!(line_totals(""), None);
    }

    #[test]
    fn a_percentage_is_computed_from_the_totals() {
        assert!((LineTotals { hit: 3, found: 4 }.percent() - 75.0).abs() < f64::EPSILON);
        assert!(LineTotals { hit: 0, found: 0 }.percent().abs() < f64::EPSILON);
    }

    #[test]
    fn a_measured_run_reports_the_number_and_states_that_there_is_no_threshold() {
        let GateOutcome::Pass { detail } = outcome_of(LCOV, Path::new("/repo/lcov.info")) else {
            panic!("a coverage run that happened is a pass");
        };
        assert!(detail.contains("76.9%"), "{detail}");
        assert!(detail.contains("10/13"), "{detail}");
        assert!(detail.contains("no threshold yet"), "{detail}");
    }

    #[test]
    fn an_lcov_without_records_is_an_instrument_error() {
        let GateOutcome::Error { reason } = outcome_of("", Path::new("/repo/lcov.info")) else {
            panic!("coverage that was not measured did not pass");
        };
        assert!(reason.contains("/repo/lcov.info"), "{reason}");
    }

    #[test]
    fn a_workspace_with_no_instrumented_lines_is_skipped_not_passed() {
        let outcome = outcome_of("LF:0\nLH:0\n", Path::new("/repo/lcov.info"));
        assert_eq!(outcome.label(), "skipped");
    }

    #[test]
    fn the_gate_is_pinned_to_the_calibrated_cargo_llvm_cov() {
        assert_eq!(gate().pin, Some(&PIN));
        assert_eq!(PIN.expected, "0.8.7");
    }

    /// Runs the gate against a throwaway root, optionally seeded with LCOV.
    fn run_against(lcov: Option<&str>, run: CommandOutput) -> GateOutcome {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = ctx_at(dir.path());
        if let Some(lcov) = lcov {
            std::fs::create_dir_all(&ctx.log_dir).expect("log dir");
            std::fs::write(ctx.lcov_path(), lcov).expect("lcov");
        }
        let runner = ScriptedRunner::new(vec![exited(0, "cargo-llvm-cov 0.8.7"), run]);
        run_gate(&gate(), &ctx, &runner)
    }

    #[test]
    fn the_gate_reads_the_lcov_the_tool_actually_wrote() {
        let GateOutcome::Pass { detail } = run_against(Some(LCOV), exited(0, "")) else {
            panic!("expected a pass");
        };
        assert!(detail.contains("10/13"), "{detail}");
    }

    #[test]
    fn a_missing_lcov_is_an_instrument_error_even_when_the_tool_exited_zero() {
        let GateOutcome::Error { reason } = run_against(None, exited(0, "")) else {
            panic!("a tool that claims success but leaves no artifact did not measure");
        };
        assert!(reason.contains("lcov.info"), "{reason}");
    }

    #[test]
    fn a_failing_coverage_run_is_an_instrument_error() {
        let outcome = run_against(Some(LCOV), failed(101, "error: could not compile"));
        let GateOutcome::Error { reason } = outcome else {
            panic!("coverage that could not run did not pass");
        };
        assert!(reason.contains("could not compile"), "{reason}");
    }
}
