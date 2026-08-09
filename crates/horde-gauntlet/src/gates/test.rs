//! Tests: cargo-nextest when it is installed, `cargo test` otherwise, plus
//! doctests either way (nextest cannot run them).

use crate::command::CommandRunner;
use crate::gate::Gate;
use crate::gates::prelude::*;
use crate::gates::{invoke, labelled_count, unexpected_exit, with_packages};
use crate::tool::{extract_version, is_missing_cargo_subcommand};

/// The nextest version the gate is calibrated against.
pub const NEXTEST_PIN: ToolPin = ToolPin {
    program: "cargo",
    version_args: &["nextest", "--version"],
    expected: "0.9.82",
    install: "cargo install --locked cargo-nextest@0.9.82",
};

/// Which test harness a run used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Harness {
    /// cargo-nextest, at the pinned version.
    Nextest,
    /// `cargo test`, because nextest is not installed.
    CargoTest,
}

impl Harness {
    /// The name shown in the gate's detail line.
    pub fn label(self) -> &'static str {
        match self {
            Self::Nextest => "cargo nextest",
            Self::CargoTest => "cargo test (nextest not installed)",
        }
    }
}

/// What the nextest version probe told us to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarnessChoice {
    /// nextest is installed at the pinned version.
    Nextest,
    /// nextest is absent; fall back without complaint.
    Fallback,
    /// nextest is present but unusable — the instrument is wrong.
    Error(String),
}

/// Runs the test suite and reports what it measured.
#[derive(Debug, Clone, Copy, Default)]
pub struct TestGate;

impl Gate for TestGate {
    fn run(&self, ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
        let harness = match select_harness(ctx, runner) {
            Ok(harness) => harness,
            Err(outcome) => return outcome,
        };
        let unit = match count(runner, unit_spec(ctx, harness), harness) {
            Ok(counts) => counts,
            Err(outcome) => return outcome,
        };
        let doc = match count(runner, doc_spec(ctx), Harness::CargoTest) {
            Ok(counts) => counts,
            Err(outcome) => return outcome,
        };
        verdict(harness, unit, doc)
    }
}

fn select_harness(ctx: &GateContext, runner: &dyn CommandRunner) -> Result<Harness, GateOutcome> {
    let spec = CommandSpec::new(
        NEXTEST_PIN.program,
        NEXTEST_PIN.version_args,
        &ctx.workspace_root,
    );
    let probe = invoke(runner, &spec)?;
    match harness_from_probe(&probe) {
        HarnessChoice::Nextest => Ok(Harness::Nextest),
        HarnessChoice::Fallback => Ok(Harness::CargoTest),
        HarnessChoice::Error(reason) => Err(GateOutcome::error(reason)),
    }
}

/// Decides which harness to use from `cargo nextest --version`.
///
/// An absent nextest is a legitimate fallback; a *present* nextest at the wrong
/// version is an instrument error, because the pin exists to keep CI and the
/// laptop measuring the same thing.
pub fn harness_from_probe(probe: &CommandOutput) -> HarnessChoice {
    let text = probe.combined();
    if !probe.success() {
        if is_missing_cargo_subcommand(&text) {
            return HarnessChoice::Fallback;
        }
        return HarnessChoice::Error(format!(
            "`cargo nextest --version` failed: {}\ninstall with: {}",
            text.trim(),
            NEXTEST_PIN.install
        ));
    }
    match extract_version(&text) {
        Some(found) if found == NEXTEST_PIN.expected => HarnessChoice::Nextest,
        Some(found) => HarnessChoice::Error(format!(
            "cargo-nextest is at {found}, gate is calibrated against {}\ninstall with: {}",
            NEXTEST_PIN.expected, NEXTEST_PIN.install
        )),
        None => HarnessChoice::Error(format!(
            "could not read a version from `cargo nextest --version`: {}",
            text.trim()
        )),
    }
}

/// The command that runs the unit and integration tests.
pub fn unit_spec(ctx: &GateContext, harness: Harness) -> CommandSpec {
    let args = match harness {
        Harness::Nextest => with_packages(&["nextest", "run", "--all-features"], ctx),
        Harness::CargoTest => with_packages(&["test", "--all-targets", "--all-features"], ctx),
    };
    CommandSpec::owned("cargo", args, &ctx.workspace_root)
}

/// The command that runs the doctests.
pub fn doc_spec(ctx: &GateContext) -> CommandSpec {
    CommandSpec::owned(
        "cargo",
        with_packages(&["test", "--doc", "--all-features"], ctx),
        &ctx.workspace_root,
    )
}

fn count(
    runner: &dyn CommandRunner,
    spec: CommandSpec,
    harness: Harness,
) -> Result<TestCounts, GateOutcome> {
    let output = invoke(runner, &spec)?;
    let text = output.combined();
    let parsed = match harness {
        Harness::Nextest => parse_nextest_counts(&text),
        Harness::CargoTest => parse_cargo_test_counts(&text),
    };
    parsed.ok_or_else(|| unexpected_exit(&spec, &output))
}

/// Tests passed, failed and skipped in one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TestCounts {
    /// Tests that passed.
    pub passed: usize,
    /// Tests that failed.
    pub failed: usize,
    /// Tests that were filtered out or marked ignored.
    pub skipped: usize,
}

impl TestCounts {
    /// Tests that actually executed.
    pub fn executed(self) -> usize {
        self.passed + self.failed
    }
}

/// Reads cargo-nextest's `Summary` line.
pub fn parse_nextest_counts(text: &str) -> Option<TestCounts> {
    let line = text.lines().find(|line| line.contains("tests run:"))?;
    let tokens: Vec<&str> = line.split_whitespace().collect();
    Some(TestCounts {
        passed: labelled_count(&tokens, "passed")?,
        failed: labelled_count(&tokens, "failed").unwrap_or(0),
        skipped: labelled_count(&tokens, "skipped").unwrap_or(0),
    })
}

/// Reads and sums every `test result:` line `cargo test` printed.
pub fn parse_cargo_test_counts(text: &str) -> Option<TestCounts> {
    let mut total = TestCounts::default();
    let mut saw_a_result = false;
    for line in text.lines().filter(|line| line.starts_with("test result:")) {
        saw_a_result = true;
        let tokens: Vec<&str> = line.split_whitespace().collect();
        total.passed += labelled_count(&tokens, "passed").unwrap_or(0);
        total.failed += labelled_count(&tokens, "failed").unwrap_or(0);
        total.skipped += labelled_count(&tokens, "ignored").unwrap_or(0);
    }
    saw_a_result.then_some(total)
}

/// Turns counts into a verdict.
///
/// Zero tests is `skipped`, never `pass`: a green row must not be able to mean
/// "nothing was measured".
pub fn verdict(harness: Harness, unit: TestCounts, doc: TestCounts) -> GateOutcome {
    let failed = unit.failed + doc.failed;
    let executed = unit.executed() + doc.executed();
    let summary = format!(
        "{} test(s) and {} doctest(s) via {}",
        unit.executed(),
        doc.executed(),
        harness.label()
    );
    if failed > 0 {
        return GateOutcome::fail(format!("{failed} failing; {summary}"));
    }
    if executed == 0 {
        return GateOutcome::skipped(format!("no tests to run; {summary}"));
    }
    let ignored = unit.skipped + doc.skipped;
    if ignored > 0 {
        return GateOutcome::pass(format!("{summary} ({ignored} skipped)"));
    }
    GateOutcome::pass(summary)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::testing::{ScriptedRunner, failed, repo_ctx, run_gate};

    const NEXTEST_PASS: &str = include_str!("../../fixtures/cargo-nextest-pass.txt");
    const NEXTEST_FAIL: &str = include_str!("../../fixtures/cargo-nextest-fail.txt");
    const CARGO_TEST_PASS: &str = include_str!("../../fixtures/cargo-test-pass.txt");
    const CARGO_TEST_FAIL: &str = include_str!("../../fixtures/cargo-test-fail.txt");
    const CARGO_DOC_PASS: &str = include_str!("../../fixtures/cargo-test-doc.txt");

    fn version(text: &str) -> CommandOutput {
        CommandOutput::new(Some(0), text, "")
    }

    #[test]
    fn the_pinned_version_selects_nextest() {
        assert_eq!(
            harness_from_probe(&version("cargo-nextest 0.9.82 (351e914a2 2024-10-28)")),
            HarnessChoice::Nextest
        );
    }

    #[test]
    fn an_absent_nextest_falls_back_without_complaint() {
        let probe = failed(101, "error: no such command: `nextest`");
        assert_eq!(harness_from_probe(&probe), HarnessChoice::Fallback);
    }

    #[test]
    fn a_present_nextest_at_the_wrong_version_is_an_instrument_error() {
        let HarnessChoice::Error(reason) = harness_from_probe(&version("cargo-nextest 0.9.70"))
        else {
            panic!("an unpinned tool silently moves the calibration");
        };
        assert!(reason.contains("0.9.70"), "{reason}");
        assert!(reason.contains("0.9.82"), "{reason}");
    }

    #[test]
    fn a_nextest_that_fails_for_another_reason_is_an_instrument_error() {
        let probe = failed(1, "error: could not read config");
        let HarnessChoice::Error(reason) = harness_from_probe(&probe) else {
            panic!("a broken nextest is not a silent fallback");
        };
        assert!(reason.contains("could not read config"), "{reason}");
    }

    #[test]
    fn an_unreadable_version_banner_is_an_instrument_error() {
        let HarnessChoice::Error(reason) = harness_from_probe(&version("cargo-nextest ???")) else {
            panic!("an unparseable version means the pin was not checked");
        };
        assert!(reason.contains("could not read a version"), "{reason}");
    }

    #[test]
    fn nextest_runs_every_feature_across_the_selected_packages() {
        assert_eq!(
            unit_spec(&repo_ctx(), Harness::Nextest).display(),
            "cargo nextest run --all-features --workspace"
        );
    }

    #[test]
    fn the_fallback_runs_every_target_because_nextest_flags_do_not_apply() {
        assert_eq!(
            unit_spec(&repo_ctx(), Harness::CargoTest).display(),
            "cargo test --all-targets --all-features --workspace"
        );
    }

    #[test]
    fn doctests_always_go_through_cargo_test() {
        assert_eq!(
            doc_spec(&repo_ctx()).display(),
            "cargo test --doc --all-features --workspace"
        );
    }

    #[test]
    fn real_nextest_output_is_read_correctly() {
        assert_eq!(
            parse_nextest_counts(NEXTEST_PASS),
            Some(TestCounts {
                passed: 8,
                failed: 0,
                skipped: 1
            })
        );
    }

    #[test]
    fn real_failing_nextest_output_is_read_correctly() {
        assert_eq!(
            parse_nextest_counts(NEXTEST_FAIL),
            Some(TestCounts {
                passed: 4,
                failed: 2,
                skipped: 1
            })
        );
    }

    #[test]
    fn output_without_a_summary_line_is_unreadable_rather_than_zero() {
        assert_eq!(parse_nextest_counts("error: could not compile"), None);
        assert_eq!(parse_cargo_test_counts("error: could not compile"), None);
    }

    #[test]
    fn real_cargo_test_output_is_summed_across_targets() {
        assert_eq!(
            parse_cargo_test_counts(CARGO_TEST_PASS),
            Some(TestCounts {
                passed: 8,
                failed: 0,
                skipped: 1
            })
        );
    }

    #[test]
    fn real_failing_cargo_test_output_is_summed_across_targets() {
        assert_eq!(
            parse_cargo_test_counts(CARGO_TEST_FAIL),
            Some(TestCounts {
                passed: 5,
                failed: 3,
                skipped: 1
            })
        );
    }

    #[test]
    fn ignored_cargo_tests_are_counted_as_skipped() {
        let text = "test result: ok. 3 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out\n";
        assert_eq!(
            parse_cargo_test_counts(text),
            Some(TestCounts {
                passed: 3,
                failed: 0,
                skipped: 2
            })
        );
    }

    #[test]
    fn a_run_with_no_tests_at_all_is_skipped_not_passed() {
        let outcome = verdict(
            Harness::Nextest,
            TestCounts::default(),
            TestCounts::default(),
        );
        assert_eq!(outcome.label(), "skipped");
    }

    #[test]
    fn tests_and_doctests_both_count_towards_having_run_something() {
        let two = TestCounts {
            passed: 2,
            failed: 0,
            skipped: 0,
        };
        assert_eq!(
            verdict(Harness::Nextest, two, two),
            GateOutcome::pass("2 test(s) and 2 doctest(s) via cargo nextest")
        );
    }

    #[test]
    fn a_passing_run_names_the_harness_and_both_counts() {
        let unit = TestCounts {
            passed: 96,
            failed: 0,
            skipped: 0,
        };
        let doc = TestCounts {
            passed: 2,
            failed: 0,
            skipped: 0,
        };
        assert_eq!(
            verdict(Harness::Nextest, unit, doc),
            GateOutcome::pass("96 test(s) and 2 doctest(s) via cargo nextest")
        );
    }

    #[test]
    fn skipped_tests_are_surfaced_on_a_passing_row() {
        let unit = TestCounts {
            passed: 1,
            failed: 0,
            skipped: 4,
        };
        let GateOutcome::Pass { detail } = verdict(Harness::Nextest, unit, TestCounts::default())
        else {
            panic!("skipped tests do not fail the gate");
        };
        assert!(detail.contains("4 skipped"), "{detail}");
    }

    #[test]
    fn failures_from_either_run_fail_the_gate() {
        let doc = TestCounts {
            passed: 0,
            failed: 1,
            skipped: 0,
        };
        let GateOutcome::Fail { detail } = verdict(Harness::CargoTest, TestCounts::default(), doc)
        else {
            panic!("a failing doctest is still a failing test");
        };
        assert!(detail.starts_with("1 failing"), "{detail}");
    }

    #[test]
    fn the_fallback_is_visible_in_the_reported_detail() {
        let unit = TestCounts {
            passed: 1,
            failed: 0,
            skipped: 0,
        };
        let GateOutcome::Pass { detail } = verdict(Harness::CargoTest, unit, TestCounts::default())
        else {
            panic!("expected a pass");
        };
        assert!(detail.contains("nextest not installed"), "{detail}");
    }

    #[test]
    fn the_gate_runs_probe_then_tests_then_doctests() {
        let runner = ScriptedRunner::new(vec![
            version("cargo-nextest 0.9.82"),
            CommandOutput::new(Some(0), NEXTEST_PASS, ""),
            CommandOutput::new(Some(0), CARGO_DOC_PASS, ""),
        ]);
        let outcome = run_gate(&TestGate, &repo_ctx(), &runner);
        assert_eq!(
            outcome,
            GateOutcome::pass("8 test(s) and 2 doctest(s) via cargo nextest (1 skipped)")
        );
        assert_eq!(
            runner.command_lines(),
            vec![
                "cargo nextest --version",
                "cargo nextest run --all-features --workspace",
                "cargo test --doc --all-features --workspace",
            ]
        );
    }

    #[test]
    fn the_gate_falls_back_to_cargo_test_when_nextest_is_absent() {
        let runner = ScriptedRunner::new(vec![
            failed(101, "error: no such command: `nextest`"),
            CommandOutput::new(Some(0), CARGO_TEST_PASS, ""),
            CommandOutput::new(Some(0), CARGO_DOC_PASS, ""),
        ]);
        let GateOutcome::Pass { detail } = run_gate(&TestGate, &repo_ctx(), &runner) else {
            panic!("the fallback is a supported configuration");
        };
        assert!(detail.contains("nextest not installed"), "{detail}");
        assert_eq!(
            runner.command_lines()[1],
            "cargo test --all-targets --all-features --workspace"
        );
    }

    #[test]
    fn a_build_failure_is_an_instrument_error_not_a_silent_zero() {
        let runner = ScriptedRunner::new(vec![
            version("cargo-nextest 0.9.82"),
            failed(101, "error: could not compile `horde-gauntlet`"),
        ]);
        let GateOutcome::Error { reason } = run_gate(&TestGate, &repo_ctx(), &runner) else {
            panic!("tests that never ran did not pass");
        };
        assert!(reason.contains("could not compile"), "{reason}");
    }
}
