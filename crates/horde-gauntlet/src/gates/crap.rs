//! CRAP: complexity weighted by how little of it is covered.
//!
//! CRAP = `cc + cc² × (1 − coverage)³`. A fully covered function scores its own
//! cyclomatic complexity, so a threshold of 15 means "no function may exceed
//! CC 15 even when perfectly tested, and an untested function may not exceed
//! about 3 branches". That is the number docs/TESTING.md §4 names, and it is a
//! hard gate from day one: this workspace starts from zero, so there is no debt
//! to grandfather.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::command::CommandRunner;
use crate::gates::coverage::line_totals;
use crate::gates::prelude::*;
use crate::gates::{invoke, read_artifact, unexpected_exit};
use crate::metadata::{WorkspacePackage, parse_workspace_packages};

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
    /// Percentage of the function's lines that are covered, or `None` when the
    /// coverage run produced no data for this function at all.
    pub coverage: Option<f64>,
    /// The CRAP score.
    pub crap: f64,
}

#[derive(Deserialize)]
struct Report {
    entries: Vec<Entry>,
}

/// Scores every function against the LCOV the coverage gate produced.
pub fn gate() -> PinnedGate {
    PinnedGate {
        pin: &PIN,
        nothing_to_measure: None,
        measure: score,
    }
}

/// What cargo-crap is pointed at for this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// Every workspace member.
    Workspace,
    /// One package, narrowed by running in its directory.
    Package {
        /// Package name, for the detail line.
        name: String,
        /// Directory cargo-crap is run in; it analyses its working directory.
        dir: PathBuf,
    },
}

impl Scope {
    /// How the scope is described in the gate's detail line.
    pub fn label(&self) -> String {
        match self {
            Self::Workspace => "the whole workspace".to_owned(),
            Self::Package { name, .. } => name.clone(),
        }
    }
}

fn score(ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
    let lcov = match read_artifact(&ctx.lcov_path(), "the coverage gate") {
        Ok(lcov) => lcov,
        Err(outcome) => return outcome,
    };
    if let Some(reason) = unusable_lcov(&lcov, &ctx.lcov_path()) {
        return GateOutcome::error(reason);
    }
    let scope = match resolve_scope(ctx, runner) {
        Ok(scope) => scope,
        Err(outcome) => return outcome,
    };
    let spec = spec(ctx, &scope);
    let output = match invoke(runner, &spec) {
        Ok(output) => output,
        Err(outcome) => return outcome,
    };
    if !output.success() {
        return unexpected_exit(&spec, &output);
    }
    match parse_report(&output.stdout) {
        Err(reason) => GateOutcome::error(reason),
        Ok(entries) => outcome_of(&entries, THRESHOLD, &scope),
    }
}

/// Why the LCOV cannot be scored against, if it cannot.
///
/// This is the gate's load-bearing precondition. cargo-crap run without real
/// coverage data does not fail — it scores every function as 0% covered, which
/// is complexity dressed up as CRAP, and it produces a plausible-looking list of
/// violations that are simply wrong. An empty or record-less LCOV exits zero, so
/// nothing downstream would notice. The gate refuses instead.
pub fn unusable_lcov(lcov: &str, path: &Path) -> Option<String> {
    if let Some(totals) = line_totals(lcov)
        && totals.found > 0
    {
        return None;
    }
    Some(format!(
        "{} records no instrumented lines; cargo-crap without real coverage scores complexity, not CRAP",
        path.display()
    ))
}

fn resolve_scope(ctx: &GateContext, runner: &dyn CommandRunner) -> Result<Scope, GateOutcome> {
    if ctx.packages.is_empty() {
        return Ok(Scope::Workspace);
    }
    let spec = CommandSpec::new(
        "cargo",
        &["metadata", "--no-deps", "--format-version", "1"],
        &ctx.workspace_root,
    );
    let output = invoke(runner, &spec)?;
    if !output.success() {
        return Err(unexpected_exit(&spec, &output));
    }
    let packages = parse_workspace_packages(&output.stdout).map_err(GateOutcome::error)?;
    narrow(&packages, &ctx.packages).map_err(GateOutcome::error)
}

/// Turns a package selection into a scope cargo-crap can actually take.
///
/// cargo-crap has no `--package`; it analyses a directory. That means exactly
/// one package can be narrowed to at a time, and asking for two is refused
/// rather than quietly widened back to the workspace — measuring more than was
/// asked for is how a narrowed run comes to report other people's findings.
///
/// # Errors
///
/// Returns a message when the selection names an unknown package, or more than
/// one.
pub fn narrow(packages: &[WorkspacePackage], wanted: &[String]) -> Result<Scope, String> {
    let [name] = wanted else {
        return Err(format!(
            "cargo-crap analyses one directory, so `-p` narrows to at most one package; got {}",
            wanted.join(", ")
        ));
    };
    let package = packages
        .iter()
        .find(|package| package.name == *name)
        .ok_or_else(|| format!("`{name}` is not a member of this workspace"))?;
    Ok(Scope::Package {
        name: package.name.clone(),
        dir: package.manifest_dir.clone(),
    })
}

/// Build scripts, which the coverage instrument structurally cannot see.
///
/// A build script runs at compile time, not under the test harness, so
/// llvm-cov never produces coverage data for it and cargo-crap scores it as if
/// it were 0% covered. That is the instrument measuring its own blind spot, not
/// a fact about the code — a `build.rs` cannot be made to score better by
/// testing it harder. This is the only exclusion the gate carries, and it is
/// narrow on purpose: "the instrument cannot see it" is a very different claim
/// from "nobody has tested it", and only the first one earns an exclusion.
pub const UNMEASURABLE: &str = "**/build.rs";

/// The command this gate runs, pointed at `scope`.
///
/// Narrowing is by working directory: cargo-crap analyses `--path`, which
/// defaults to the directory it runs in, and has no package flag.
pub fn spec(ctx: &GateContext, scope: &Scope) -> CommandSpec {
    let mut args = vec!["crap".to_owned()];
    let cwd = match scope {
        Scope::Workspace => {
            args.push("--workspace".to_owned());
            ctx.workspace_root.clone()
        }
        Scope::Package { dir, .. } => dir.clone(),
    };
    args.extend([
        "--lcov".to_owned(),
        ctx.lcov_path().display().to_string(),
        "--threshold".to_owned(),
        THRESHOLD.to_string(),
        "--exclude".to_owned(),
        UNMEASURABLE.to_owned(),
        "--format".to_owned(),
        "json".to_owned(),
    ]);
    CommandSpec::owned("cargo", args, &cwd)
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
pub fn outcome_of(entries: &[Entry], threshold: f64, scope: &Scope) -> GateOutcome {
    let Some(worst) = worst(entries) else {
        return GateOutcome::skipped(format!(
            "cargo-crap scored no functions in {}",
            scope.label()
        ));
    };
    let over = over_threshold(entries, threshold);
    if over.is_empty() {
        return GateOutcome::pass(format!(
            "{} function(s) scored in {}, worst {:.1} ({} at {}:{}), threshold {threshold:.0}",
            entries.len(),
            scope.label(),
            worst.crap,
            worst.function,
            worst.file,
            worst.line
        ));
    }
    let mut detail = format!(
        "{} function(s) over CRAP {threshold:.0} in {}",
        over.len(),
        scope.label()
    );
    for entry in over {
        detail.push_str(&format!(
            "\n  {:.1}  {} at {}:{} (cc {:.0}, coverage {})",
            entry.crap,
            entry.function,
            entry.file,
            entry.line,
            entry.cyclomatic,
            describe_coverage(entry.coverage)
        ));
    }
    GateOutcome::fail(detail)
}

fn describe_coverage(coverage: Option<f64>) -> String {
    match coverage {
        Some(percent) => format!("{percent:.1}%"),
        None => "not measured".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{ScriptedRunner, ctx_at, exited, failed, repo_ctx, run_gate};

    const REPORT: &str = include_str!("../../fixtures/cargo-crap.json");
    const LCOV: &str = include_str!("../../fixtures/lcov.info");
    const METADATA: &str = include_str!("../../fixtures/cargo-metadata.json");

    fn entry(function: &str, crap: f64) -> Entry {
        Entry {
            file: "./src/lib.rs".to_owned(),
            function: function.to_owned(),
            line: 1,
            cyclomatic: 4.0,
            coverage: Some(100.0),
            crap,
        }
    }

    fn a_package(name: &str, dir: &str) -> WorkspacePackage {
        WorkspacePackage {
            name: name.to_owned(),
            has_lib: true,
            bins: Vec::new(),
            manifest_dir: PathBuf::from(dir),
        }
    }

    /// A throwaway root whose LCOV says what `lcov` says.
    fn root_with_lcov(lcov: &str) -> (tempfile::TempDir, GateContext) {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = ctx_at(dir.path());
        std::fs::create_dir_all(&ctx.log_dir).expect("log dir");
        std::fs::write(ctx.lcov_path(), lcov).expect("lcov");
        (dir, ctx)
    }

    #[test]
    fn the_gate_is_pinned_to_the_calibrated_cargo_crap() {
        assert_eq!(gate().pin, &PIN);
        assert_eq!(PIN.expected, "0.3.1");
    }

    #[test]
    fn a_workspace_run_scores_every_member_from_the_root() {
        let spec = spec(&repo_ctx(), &Scope::Workspace);
        assert_eq!(
            spec.display(),
            "cargo crap --workspace --lcov /repo/target/quality/lcov.info --threshold 15 \
             --exclude **/build.rs --format json"
        );
        assert_eq!(spec.cwd, Path::new("/repo"));
    }

    #[test]
    fn a_narrowed_run_points_cargo_crap_at_the_package_directory() {
        let scope = Scope::Package {
            name: "horde-gauntlet".to_owned(),
            dir: PathBuf::from("/repo/crates/horde-gauntlet"),
        };
        let spec = spec(&repo_ctx(), &scope);
        assert_eq!(
            spec.cwd,
            Path::new("/repo/crates/horde-gauntlet"),
            "cargo-crap has no package flag; it analyses its working directory"
        );
        assert!(
            !spec.args.contains(&"--workspace".to_owned()),
            "a narrowed run must not widen back to the workspace: {:?}",
            spec.args
        );
        assert!(
            spec.args
                .contains(&"/repo/target/quality/lcov.info".to_owned()),
            "the LCOV path stays absolute: {:?}",
            spec.args
        );
    }

    #[test]
    fn build_scripts_are_the_only_thing_excluded_from_scoring() {
        assert_eq!(UNMEASURABLE, "**/build.rs");
        let args = spec(&repo_ctx(), &Scope::Workspace).args;
        assert_eq!(
            args.iter().filter(|arg| *arg == "--exclude").count(),
            1,
            "an exclusion list that grows is a threshold that quietly falls: {args:?}"
        );
    }

    #[test]
    fn an_empty_selection_scores_the_whole_workspace() {
        let packages = [a_package("a", "/repo/crates/a")];
        assert_eq!(
            narrow(&packages, &["a".to_owned()]),
            Ok(Scope::Package {
                name: "a".to_owned(),
                dir: PathBuf::from("/repo/crates/a")
            })
        );
    }

    #[test]
    fn narrowing_to_two_packages_is_refused_rather_than_widened() {
        let packages = [
            a_package("a", "/repo/crates/a"),
            a_package("b", "/repo/crates/b"),
        ];
        let message = narrow(&packages, &["a".to_owned(), "b".to_owned()])
            .expect_err("cargo-crap analyses one directory");
        assert!(message.contains("at most one package"), "{message}");
    }

    #[test]
    fn narrowing_to_an_unknown_package_is_refused() {
        let packages = [a_package("a", "/repo/crates/a")];
        let message = narrow(&packages, &["nope".to_owned()]).expect_err("not a member");
        assert!(message.contains("not a member"), "{message}");
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
        assert_eq!(untested.coverage, Some(0.0));
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
        assert_eq!(over_threshold(&[entry("f", 15.1)], 15.0).len(), 1);
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
        let outcome = outcome_of(&[], THRESHOLD, &Scope::Workspace);
        assert_eq!(outcome.label(), "skipped");
    }

    #[test]
    fn a_clean_report_states_the_worst_score_and_what_was_scored() {
        let GateOutcome::Pass { detail } =
            outcome_of(&[entry("f", 6.0)], THRESHOLD, &Scope::Workspace)
        else {
            panic!("nothing is over the threshold");
        };
        assert!(detail.contains("1 function(s) scored"), "{detail}");
        assert!(detail.contains("the whole workspace"), "{detail}");
        assert!(detail.contains("worst 6.0"), "{detail}");
        assert!(detail.contains("threshold 15"), "{detail}");
    }

    #[test]
    fn a_narrowed_verdict_names_the_package_it_scored() {
        let scope = Scope::Package {
            name: "horde-gauntlet".to_owned(),
            dir: PathBuf::from("/repo/crates/horde-gauntlet"),
        };
        let GateOutcome::Pass { detail } = outcome_of(&[entry("f", 6.0)], THRESHOLD, &scope) else {
            panic!("nothing is over the threshold");
        };
        assert!(detail.contains("horde-gauntlet"), "{detail}");
        assert!(!detail.contains("whole workspace"), "{detail}");
    }

    #[test]
    fn a_dirty_report_names_every_offender_with_its_score_and_location() {
        let entries = [entry("bad", 22.5), entry("fine", 2.0)];
        let GateOutcome::Fail { detail } = outcome_of(&entries, THRESHOLD, &Scope::Workspace)
        else {
            panic!("a function over the threshold is a verdict about the code");
        };
        assert!(detail.starts_with("1 function(s) over CRAP 15"), "{detail}");
        assert!(detail.contains("22.5  bad at ./src/lib.rs:1"), "{detail}");
        assert!(!detail.contains("fine"), "{detail}");
    }

    #[test]
    fn the_real_fixture_passes_at_the_projects_threshold() {
        let entries = parse_report(REPORT).expect("valid");
        assert_eq!(
            outcome_of(&entries, THRESHOLD, &Scope::Workspace).label(),
            "pass"
        );
    }

    #[test]
    fn a_real_lcov_is_usable() {
        assert_eq!(unusable_lcov(LCOV, Path::new("/repo/lcov.info")), None);
    }

    #[test]
    fn an_lcov_with_no_instrumented_lines_is_refused() {
        let reason = unusable_lcov("LF:0\nLH:0\n", Path::new("/repo/lcov.info"))
            .expect("nothing was instrumented");
        assert!(reason.contains("complexity, not CRAP"), "{reason}");
    }

    #[test]
    fn an_lcov_with_no_records_at_all_is_refused_by_the_gate() {
        let (_dir, ctx) = root_with_lcov("");
        let runner = ScriptedRunner::new(vec![exited(0, "cargo-crap 0.3.1")]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx, &runner) else {
            panic!("scoring against an empty LCOV reports complexity dressed as CRAP");
        };
        assert!(reason.contains("lcov.info"), "{reason}");
    }

    #[test]
    fn a_missing_lcov_is_refused_before_cargo_crap_runs() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = ScriptedRunner::new(vec![exited(0, "cargo-crap 0.3.1")]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx_at(dir.path()), &runner) else {
            panic!("there is nothing to score against");
        };
        assert!(reason.contains("the coverage gate"), "{reason}");
        assert_eq!(
            runner.calls().len(),
            1,
            "cargo-crap must not run without coverage data"
        );
    }

    #[test]
    fn the_gate_probes_then_scores_the_workspace() {
        let (_dir, ctx) = root_with_lcov(LCOV);
        let runner = ScriptedRunner::new(vec![exited(0, "cargo-crap 0.3.1"), exited(0, REPORT)]);
        assert_eq!(run_gate(&gate(), &ctx, &runner).label(), "pass");
        assert_eq!(runner.command_lines()[0], "cargo crap --version");
        assert!(runner.command_lines()[1].contains("--workspace"));
    }

    #[test]
    fn a_narrowed_gate_asks_cargo_where_the_package_lives() {
        let (_dir, dir_ctx) = root_with_lcov(LCOV);
        let ctx = GateContext::new(&dir_ctx.workspace_root, vec!["horde-gauntlet".to_owned()]);
        let runner = ScriptedRunner::new(vec![
            exited(0, "cargo-crap 0.3.1"),
            exited(0, METADATA),
            exited(0, REPORT),
        ]);
        assert_eq!(run_gate(&gate(), &ctx, &runner).label(), "pass");
        assert!(runner.command_lines()[1].starts_with("cargo metadata"));
        assert_eq!(
            runner.calls()[2].cwd,
            Path::new("/repo/crates/horde-gauntlet")
        );
    }

    #[test]
    fn a_failing_scoring_run_is_an_instrument_error() {
        let (_dir, ctx) = root_with_lcov(LCOV);
        let runner = ScriptedRunner::new(vec![
            exited(0, "cargo-crap 0.3.1"),
            failed(2, "error: no LCOV file"),
        ]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx, &runner) else {
            panic!("a scoring run that failed did not pass");
        };
        assert!(reason.contains("no LCOV file"), "{reason}");
    }
}
