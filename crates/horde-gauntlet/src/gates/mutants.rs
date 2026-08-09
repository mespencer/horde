//! Mutation testing: cargo-mutants, zero missed mutants.
//!
//! Coverage says a line ran; a mutant says the line's behaviour was *checked*.
//! Agent-written tests are unusually good at the first and unusually bad at the
//! second — asserting that a call returned `Ok`, that a vector is non-empty,
//! that no panic occurred. Every one of those passes when the code under it is
//! quietly wrong, and every one of them is exactly what a surviving mutant
//! points at. A mutation that stays green is a finding, not a relief.

use std::path::{Path, PathBuf};

use crate::command::CommandRunner;
use crate::gates::prelude::*;
use crate::gates::{invoke, labelled_count, unexpected_exit};

/// The cargo-mutants version the gate is calibrated against.
pub const PIN: ToolPin = ToolPin {
    program: "cargo",
    version_args: &["mutants", "--version"],
    expected: "27.1.0",
    install: "cargo install --locked cargo-mutants@27.1.0",
};

/// Branch the diff is taken against.
pub const TRUNK: &str = "main";

/// How many mutants to build and test at once.
///
/// Almost all of a mutant's cost is rebuilding the crate, which is largely
/// single-threaded, so running several at once is close to a linear speed-up.
/// A gate nobody can afford to run is a gate nobody runs. This changes only how
/// long the measurement takes, never what it measures; the timeouts in
/// `.cargo/mutants.toml` are generous enough that the extra load does not turn
/// into spurious ones.
pub const JOBS: &str = "4";

/// Which mutants a run covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// Only code changed since the merge-base with `main`.
    Diff(PathBuf),
    /// Every mutable function in the tree.
    FullTree,
}

impl Scope {
    /// How the scope is described in the gate's detail line.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Diff(_) => "changed code",
            Self::FullTree => "the whole tree",
        }
    }
}

/// Runs cargo-mutants and requires that nothing survived.
pub fn gate() -> PinnedGate {
    PinnedGate {
        pin: &PIN,
        measure: mutate,
    }
}

/// Scopes the run, then mutates.
fn mutate(ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
    let scope = match scope(ctx, runner) {
        Ok(scope) => scope,
        Err(outcome) => return outcome,
    };
    let spec = spec(ctx, &scope);
    let output = match invoke(runner, &spec) {
        Ok(output) => output,
        Err(outcome) => return outcome,
    };
    match parse_summary(&output.combined()) {
        None => unexpected_exit(&spec, &output),
        Some(counts) => outcome_of(&scope, counts),
    }
}

/// The command that finds where this branch left `main`.
pub fn merge_base_spec(root: &Path) -> CommandSpec {
    CommandSpec::new("git", &["merge-base", "HEAD", TRUNK], root)
}

/// The command that produces the diff cargo-mutants will be scoped to.
pub fn diff_spec(root: &Path, base: &str) -> CommandSpec {
    CommandSpec::owned(
        "git",
        vec!["diff".to_owned(), "--no-color".to_owned(), base.to_owned()],
        root,
    )
}

/// Where the diff is written for cargo-mutants to read.
pub fn diff_path(ctx: &GateContext) -> PathBuf {
    ctx.log_dir.join("mutants.diff")
}

/// Decides whether to mutate the diff or the whole tree.
///
/// Falling back to the whole tree is the *stricter* choice, so an absent `main`
/// or an empty diff can never quietly shrink the gate to nothing.
fn scope(ctx: &GateContext, runner: &dyn CommandRunner) -> Result<Scope, GateOutcome> {
    let base_output = invoke(runner, &merge_base_spec(&ctx.workspace_root))?;
    if !base_output.success() {
        return Ok(Scope::FullTree);
    }
    let base = base_output.stdout.trim().to_owned();
    if base.is_empty() {
        return Ok(Scope::FullTree);
    }
    let spec = diff_spec(&ctx.workspace_root, &base);
    let diff_output = invoke(runner, &spec)?;
    if !diff_output.success() {
        return Err(unexpected_exit(&spec, &diff_output));
    }
    if diff_output.stdout.trim().is_empty() {
        return Ok(Scope::FullTree);
    }
    let path = diff_path(ctx);
    write_diff(&path, &diff_output.stdout)?;
    Ok(Scope::Diff(path))
}

fn write_diff(path: &Path, diff: &str) -> Result<(), GateOutcome> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            GateOutcome::error(format!("could not create {}: {err}", parent.display()))
        })?;
    }
    std::fs::write(path, diff)
        .map_err(|err| GateOutcome::error(format!("could not write {}: {err}", path.display())))
}

/// The command this gate runs.
///
/// Timeouts and exclusions live in the committed `.cargo/mutants.toml`, which
/// cargo-mutants reads itself.
pub fn spec(ctx: &GateContext, scope: &Scope) -> CommandSpec {
    let mut args = vec![
        "mutants".to_owned(),
        "--no-shuffle".to_owned(),
        "--colors".to_owned(),
        "never".to_owned(),
        "--jobs".to_owned(),
        JOBS.to_owned(),
        "--output".to_owned(),
        ctx.log_dir.display().to_string(),
    ];
    if let Scope::Diff(path) = scope {
        args.push("--in-diff".to_owned());
        args.push(path.display().to_string());
    }
    for package in &ctx.packages {
        args.push("--package".to_owned());
        args.push(package.clone());
    }
    CommandSpec::owned("cargo", args, &ctx.workspace_root)
}

/// What a cargo-mutants run concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MutantCounts {
    /// Mutants no test noticed.
    pub missed: usize,
    /// Mutants some test failed on.
    pub caught: usize,
    /// Mutants that did not compile.
    pub unviable: usize,
    /// Mutants whose test run hung.
    pub timeout: usize,
}

impl MutantCounts {
    /// Mutants that were built and tested.
    pub fn tested(self) -> usize {
        self.missed + self.caught + self.timeout
    }
}

/// Reads cargo-mutants' final tally.
///
/// Returns `None` when there is no tally, which means the run did not finish —
/// an instrument error, not a clean sheet.
pub fn parse_summary(text: &str) -> Option<MutantCounts> {
    let line = text.lines().find(|line| line.contains("mutants tested"))?;
    let tokens: Vec<&str> = line.split_whitespace().collect();
    Some(MutantCounts {
        missed: labelled_count(&tokens, "missed").unwrap_or(0),
        caught: labelled_count(&tokens, "caught").unwrap_or(0),
        unviable: labelled_count(&tokens, "unviable").unwrap_or(0),
        timeout: labelled_count(&tokens, "timeouts")
            .or_else(|| labelled_count(&tokens, "timeout"))
            .unwrap_or(0),
    })
}

/// Turns a tally into a verdict.
///
/// A timeout counts as caught — the mutant changed behaviour enough to hang the
/// suite — but the number is reported so a creeping timeout count is visible
/// rather than hidden inside a green row.
pub fn outcome_of(scope: &Scope, counts: MutantCounts) -> GateOutcome {
    let where_ = scope.label();
    if counts.missed > 0 {
        return GateOutcome::fail(format!(
            "{} mutant(s) survived in {where_}; see mutants.out/missed.txt ({} caught, {} unviable)",
            counts.missed, counts.caught, counts.unviable
        ));
    }
    if counts.tested() == 0 {
        return GateOutcome::skipped(format!(
            "no viable mutants in {where_} ({} unviable)",
            counts.unviable
        ));
    }
    let timeouts = match counts.timeout {
        0 => String::new(),
        count => format!(", {count} timed out"),
    };
    GateOutcome::pass(format!(
        "{} mutant(s) caught in {where_}, none survived ({} unviable{timeouts})",
        counts.caught, counts.unviable
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandOutput;
    use crate::testing::{ScriptedRunner, ctx_at, exited, failed, repo_ctx, run_gate};

    const CLEAN: &str = include_str!("../../fixtures/cargo-mutants-clean.txt");

    fn version() -> CommandOutput {
        exited(0, "cargo-mutants 27.1.0")
    }

    #[test]
    fn the_diff_is_taken_against_the_merge_base_with_main() {
        assert_eq!(
            merge_base_spec(Path::new("/repo")).display(),
            "git merge-base HEAD main"
        );
        assert_eq!(
            diff_spec(Path::new("/repo"), "abc123").display(),
            "git diff --no-color abc123"
        );
    }

    #[test]
    fn a_full_tree_run_names_no_diff() {
        let spec = spec(&repo_ctx(), &Scope::FullTree);
        assert_eq!(
            spec.display(),
            "cargo mutants --no-shuffle --colors never --jobs 4 --output /repo/target/quality"
        );
    }

    #[test]
    fn a_diff_scoped_run_points_cargo_mutants_at_the_diff() {
        let spec = spec(
            &repo_ctx(),
            &Scope::Diff(PathBuf::from("/repo/target/quality/mutants.diff")),
        );
        assert!(
            spec.display()
                .ends_with("--in-diff /repo/target/quality/mutants.diff"),
            "{}",
            spec.display()
        );
    }

    #[test]
    fn package_narrowing_is_passed_through() {
        let ctx = GateContext::new(Path::new("/repo"), vec!["horde-gauntlet".to_owned()]);
        assert!(
            spec(&ctx, &Scope::FullTree)
                .display()
                .ends_with("--package horde-gauntlet"),
            "{}",
            spec(&ctx, &Scope::FullTree).display()
        );
    }

    #[test]
    fn a_real_clean_run_is_read_correctly() {
        let counts = parse_summary(CLEAN).expect("the fixture is real cargo-mutants output");
        assert_eq!(counts.missed, 0);
        assert!(counts.caught > 0, "{counts:?}");
    }

    #[test]
    fn a_run_with_survivors_is_read_correctly() {
        let text = "858 mutants tested in 5m 3s: 12 missed, 800 caught, 40 unviable, 6 timeouts\n";
        assert_eq!(
            parse_summary(text),
            Some(MutantCounts {
                missed: 12,
                caught: 800,
                unviable: 40,
                timeout: 6,
            })
        );
    }

    #[test]
    fn a_singular_timeout_is_read_too() {
        let text = "3 mutants tested: 0 missed, 2 caught, 0 unviable, 1 timeout\n";
        assert_eq!(parse_summary(text).map(|counts| counts.timeout), Some(1));
    }

    #[test]
    fn output_without_a_tally_means_the_run_did_not_finish() {
        assert_eq!(
            parse_summary("error: failed to build in the clean tree"),
            None
        );
    }

    #[test]
    fn a_surviving_mutant_fails_the_gate_and_says_where_to_look() {
        let counts = MutantCounts {
            missed: 2,
            caught: 10,
            unviable: 1,
            timeout: 0,
        };
        let GateOutcome::Fail { detail } = outcome_of(&Scope::FullTree, counts) else {
            panic!("a mutation that stays green is a finding");
        };
        assert!(
            detail.starts_with("2 mutant(s) survived in the whole tree"),
            "{detail}"
        );
        assert!(detail.contains("missed.txt"), "{detail}");
    }

    #[test]
    fn a_clean_run_passes_and_reports_the_timeout_count() {
        let counts = MutantCounts {
            missed: 0,
            caught: 10,
            unviable: 1,
            timeout: 3,
        };
        let GateOutcome::Pass { detail } = outcome_of(&Scope::FullTree, counts) else {
            panic!("nothing survived");
        };
        assert!(detail.contains("10 mutant(s) caught"), "{detail}");
        assert!(detail.contains("3 timed out"), "{detail}");
    }

    #[test]
    fn a_run_with_nothing_to_mutate_is_skipped_not_passed() {
        let counts = MutantCounts {
            missed: 0,
            caught: 0,
            unviable: 4,
            timeout: 0,
        };
        assert_eq!(
            outcome_of(&Scope::Diff(PathBuf::new()), counts).label(),
            "skipped"
        );
    }

    #[test]
    fn the_scope_is_named_in_the_detail() {
        assert_eq!(Scope::FullTree.label(), "the whole tree");
        assert_eq!(Scope::Diff(PathBuf::new()).label(), "changed code");
    }

    /// What one scripted run of the gate produced.
    struct Run {
        _dir: tempfile::TempDir,
        ctx: GateContext,
        outcome: GateOutcome,
        lines: Vec<String>,
    }

    /// Runs the gate against a throwaway root, scripting the git probes and
    /// then a clean cargo-mutants run.
    fn run_after(git: Vec<CommandOutput>) -> Run {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = ctx_at(dir.path());
        let mut script = vec![version()];
        script.extend(git);
        script.push(exited(0, CLEAN));
        let runner = ScriptedRunner::new(script);
        let outcome = run_gate(&gate(), &ctx, &runner);
        Run {
            _dir: dir,
            ctx,
            outcome,
            lines: runner.command_lines(),
        }
    }

    #[test]
    fn an_absent_trunk_branch_falls_back_to_the_whole_tree() {
        let run = run_after(vec![failed(128, "fatal: Not a valid object name main")]);
        assert_eq!(run.outcome.label(), "pass");
        assert!(!run.lines[2].contains("--in-diff"), "{:?}", run.lines);
    }

    #[test]
    fn an_empty_diff_falls_back_to_the_whole_tree() {
        let run = run_after(vec![exited(0, "abc123\n"), exited(0, "")]);
        assert_eq!(run.outcome.label(), "pass");
        assert!(!run.lines[3].contains("--in-diff"), "{:?}", run.lines);
    }

    #[test]
    fn a_real_diff_scopes_the_run_and_is_written_to_disk() {
        let diff = "diff --git a/src/lib.rs b/src/lib.rs\n";
        let run = run_after(vec![exited(0, "abc123\n"), exited(0, diff)]);
        assert_eq!(run.outcome.label(), "pass");
        assert!(run.lines[3].contains("--in-diff"), "{:?}", run.lines);
        assert_eq!(
            std::fs::read_to_string(diff_path(&run.ctx)).expect("the diff was written"),
            diff
        );
    }

    #[test]
    fn a_run_that_never_reported_a_tally_is_an_instrument_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = ScriptedRunner::new(vec![
            version(),
            failed(128, "fatal: no main"),
            failed(4, "error: build failed in an unmutated tree"),
        ]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx_at(dir.path()), &runner) else {
            panic!("a mutation run that did not finish did not pass");
        };
        assert!(reason.contains("build failed"), "{reason}");
    }

    #[test]
    fn the_gate_is_pinned_to_the_calibrated_cargo_mutants() {
        assert_eq!(gate().pin, &PIN);
        assert_eq!(PIN.expected, "27.1.0");
    }
}
