//! Wire contracts: `buf lint` and `buf breaking`.
//!
//! The proto files are the one artefact three independently-upgrading binaries
//! agree on, so this gate *is* the version-skew guarantee from DESIGN §2. Lint
//! keeps the schema idiomatic; breaking keeps today's schema compatible with the
//! last one everybody else built against.

use std::path::Path;

use crate::command::CommandRunner;
use crate::gates::prelude::*;
use crate::gates::{invoke, unexpected_exit};

/// The buf version the gate is calibrated against.
pub const PIN: ToolPin = ToolPin {
    program: "buf",
    version_args: &["--version"],
    expected: "1.69.0",
    install: "brew install bufbuild/buf/buf, or see https://buf.build/docs/installation",
};

/// Directory the proto modules live in, relative to the workspace root.
pub const PROTO_DIR: &str = "proto";

/// Branch whose committed schema is the compatibility baseline.
pub const TRUNK: &str = "main";

/// Exit code buf uses when it found violations, as opposed to failing to run.
const FOUND_VIOLATIONS: i32 = 100;

/// Lints the schema and checks it against the last committed one.
pub fn gate() -> PinnedGate {
    PinnedGate {
        pin: &PIN,
        nothing_to_measure: Some(no_proto_tree),
        measure: check_contracts,
    }
}

/// Whether this workspace has any wire contracts at all.
///
/// A workspace with no `proto/` is `skipped`, not `pass`: there is nothing to
/// measure, and the difference matters the day someone deletes the directory by
/// accident. It skips before the version probe, so an absent buf on a machine
/// with no protos is not reported as a broken instrument.
pub fn no_proto_tree(ctx: &GateContext) -> Option<String> {
    if ctx.workspace_root.join(PROTO_DIR).is_dir() {
        return None;
    }
    Some(format!(
        "no {PROTO_DIR}/ directory — this workspace has no wire contracts"
    ))
}

fn check_contracts(ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
    let lint = match run_buf(runner, &lint_spec(ctx)) {
        Ok(violations) => violations,
        Err(outcome) => return outcome,
    };
    let baseline = match baseline_ref(ctx, runner) {
        Ok(baseline) => baseline,
        Err(outcome) => return outcome,
    };
    let Some(baseline) = baseline else {
        return outcome_of(&lint, None);
    };
    match run_buf(runner, &breaking_spec(ctx, &baseline)) {
        Ok(breaking) => outcome_of(&lint, Some((baseline, breaking))),
        Err(outcome) => outcome,
    }
}

/// The command that lints the schema.
pub fn lint_spec(ctx: &GateContext) -> CommandSpec {
    CommandSpec::new("buf", &["lint"], &ctx.workspace_root)
}

/// The command that finds the committed schema to compare against.
pub fn merge_base_spec(root: &Path) -> CommandSpec {
    CommandSpec::new("git", &["merge-base", "HEAD", TRUNK], root)
}

/// The command that compares the working tree against `baseline`.
///
/// The baseline is always the merge-base with `main`, which needs no
/// special-casing for main itself: on a branch it is the branch point, and on
/// `main` it is `main`'s own tip, so the gate compares the working tree to the
/// last commit — exactly the "no uncommitted breakage" check wanted there.
pub fn breaking_spec(ctx: &GateContext, baseline: &str) -> CommandSpec {
    CommandSpec::owned(
        "buf",
        vec![
            "breaking".to_owned(),
            "--against".to_owned(),
            format!(".git#ref={baseline}"),
        ],
        &ctx.workspace_root,
    )
}

/// Resolves the compatibility baseline, or `None` when there is no `main`.
fn baseline_ref(
    ctx: &GateContext,
    runner: &dyn CommandRunner,
) -> Result<Option<String>, GateOutcome> {
    let output = invoke(runner, &merge_base_spec(&ctx.workspace_root))?;
    if !output.success() {
        return Ok(None);
    }
    let baseline = output.stdout.trim().to_owned();
    Ok((!baseline.is_empty()).then_some(baseline))
}

/// Runs one buf command, separating "found violations" from "could not run".
fn run_buf(runner: &dyn CommandRunner, spec: &CommandSpec) -> Result<Vec<String>, GateOutcome> {
    let output = invoke(runner, spec)?;
    if output.success() {
        return Ok(Vec::new());
    }
    let violations = parse_violations(&output.combined());
    if output.status != Some(FOUND_VIOLATIONS) || violations.is_empty() {
        return Err(unexpected_exit(spec, &output));
    }
    Ok(violations)
}

/// Reads buf's violation lines, which are `path:line:column:message`.
///
/// Anything that is not shaped like a located violation is not one: buf writes
/// its own failures as prose, and prose must never be counted as a finding.
pub fn parse_violations(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| is_located_violation(line))
        .map(str::to_owned)
        .collect()
}

fn is_located_violation(line: &str) -> bool {
    let mut parts = line.split(':');
    let (Some(path), Some(line_number), Some(column)) = (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    !path.is_empty()
        && line_number.parse::<u32>().is_ok()
        && column.parse::<u32>().is_ok()
        && parts.next().is_some()
}

/// Turns the two checks into a verdict.
///
/// `breaking` is `None` when there is no `main` to compare against. That is not
/// silently folded into a pass: the detail line says the comparison did not
/// happen, so a green row can never be read as "compatible" when it only means
/// "unchecked".
pub fn outcome_of(lint: &[String], breaking: Option<(String, Vec<String>)>) -> GateOutcome {
    let (baseline, breaks): (Option<String>, &[String]) = match &breaking {
        Some((baseline, breaks)) => (Some(baseline.clone()), breaks),
        None => (None, &[]),
    };
    if !lint.is_empty() || !breaks.is_empty() {
        return GateOutcome::fail(describe_violations(lint, breaks));
    }
    match baseline {
        Some(baseline) => GateOutcome::pass(format!(
            "schema lints clean and is compatible with {} at {}",
            TRUNK,
            short(&baseline)
        )),
        None => GateOutcome::pass(format!(
            "schema lints clean; compatibility unchecked — no `{TRUNK}` to compare against"
        )),
    }
}

fn describe_violations(lint: &[String], breaking: &[String]) -> String {
    let mut detail = format!(
        "{} lint violation(s) and {} breaking change(s)",
        lint.len(),
        breaking.len()
    );
    for violation in lint {
        detail.push_str(&format!("\n  lint: {violation}"));
    }
    for violation in breaking {
        detail.push_str(&format!("\n  breaking: {violation}"));
    }
    detail
}

fn short(commit: &str) -> &str {
    commit.get(..12).unwrap_or(commit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{ScriptedRunner, ctx_at, exited, failed, repo_ctx, run_gate};

    const LINT_VIOLATIONS: &str = include_str!("../../fixtures/buf-lint-violations.txt");
    const LINT_CLEAN: &str = include_str!("../../fixtures/buf-lint-clean.txt");
    const BREAKING_VIOLATIONS: &str = include_str!("../../fixtures/buf-breaking-violations.txt");

    /// A workspace root that actually has a proto tree.
    fn with_protos() -> (tempfile::TempDir, GateContext) {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::create_dir_all(dir.path().join(PROTO_DIR)).expect("proto dir");
        let ctx = ctx_at(dir.path());
        (dir, ctx)
    }

    #[test]
    fn the_gate_is_pinned_to_the_calibrated_buf() {
        assert_eq!(gate().pin, &PIN);
        assert_eq!(PIN.expected, "1.69.0");
    }

    #[test]
    fn a_workspace_without_protos_has_nothing_to_measure() {
        let dir = tempfile::tempdir().expect("temp dir");
        let reason = no_proto_tree(&ctx_at(dir.path())).expect("there is no proto directory");
        assert!(reason.contains("proto/"), "{reason}");
    }

    #[test]
    fn a_workspace_with_protos_has_something_to_measure() {
        let (_dir, ctx) = with_protos();
        assert_eq!(no_proto_tree(&ctx), None);
    }

    #[test]
    fn an_absent_proto_tree_skips_without_demanding_buf() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = ScriptedRunner::failing("buf is not installed");
        let outcome = run_gate(&gate(), &ctx_at(dir.path()), &runner);

        assert_eq!(outcome.label(), "skipped");
        assert!(
            runner.calls().is_empty(),
            "a gate with nothing to measure must not demand its tool: {:?}",
            runner.calls()
        );
    }

    #[test]
    fn the_lint_command_runs_at_the_workspace_root() {
        let spec = lint_spec(&repo_ctx());
        assert_eq!(spec.display(), "buf lint");
        assert_eq!(spec.cwd, Path::new("/repo"));
    }

    #[test]
    fn the_baseline_is_the_merge_base_with_main() {
        assert_eq!(
            merge_base_spec(Path::new("/repo")).display(),
            "git merge-base HEAD main"
        );
    }

    #[test]
    fn the_breaking_command_compares_against_a_committed_ref() {
        assert_eq!(
            breaking_spec(&repo_ctx(), "abc123").display(),
            "buf breaking --against .git#ref=abc123"
        );
    }

    #[test]
    fn real_lint_output_is_read_as_one_violation_per_line() {
        let violations = parse_violations(LINT_VIOLATIONS);
        assert_eq!(violations.len(), 2, "{violations:?}");
        assert!(
            violations[0].contains("should be PascalCase"),
            "{violations:?}"
        );
        assert!(
            violations[1].contains("should be lower_snake_case"),
            "{violations:?}"
        );
    }

    #[test]
    fn real_breaking_output_is_read_as_one_violation_per_line() {
        let violations = parse_violations(BREAKING_VIOLATIONS);
        assert_eq!(violations.len(), 2, "{violations:?}");
        assert!(violations[0].contains("json_name"), "{violations:?}");
    }

    #[test]
    fn clean_output_yields_no_violations() {
        assert_eq!(parse_violations(LINT_CLEAN), Vec::<String>::new());
        assert_eq!(parse_violations(""), Vec::<String>::new());
    }

    #[test]
    fn bufs_own_prose_failures_are_not_counted_as_violations() {
        let text = "Failure: buf.yaml: unknown field \"lynt\"\n";
        assert_eq!(parse_violations(text), Vec::<String>::new());
    }

    #[test]
    fn a_line_without_a_numeric_location_is_not_a_violation() {
        assert_eq!(
            parse_violations("proto/a.proto:x:1:oops"),
            Vec::<String>::new()
        );
        assert_eq!(parse_violations("proto/a.proto:1:1"), Vec::<String>::new());
        assert_eq!(parse_violations(":1:1:no path"), Vec::<String>::new());
    }

    #[test]
    fn a_clean_pair_of_checks_names_the_baseline_it_compared_against() {
        let outcome = outcome_of(&[], Some(("0123456789abcdef".to_owned(), Vec::new())));
        let GateOutcome::Pass { detail } = outcome else {
            panic!("a clean schema is a pass");
        };
        assert!(detail.contains("0123456789ab"), "{detail}");
        assert!(!detail.contains("cdef"), "the ref is shortened: {detail}");
    }

    #[test]
    fn a_pass_with_no_baseline_says_compatibility_was_not_checked() {
        let GateOutcome::Pass { detail } = outcome_of(&[], None) else {
            panic!("lint still measured, so the gate is not skipped");
        };
        assert!(detail.contains("unchecked"), "{detail}");
        assert!(detail.contains("main"), "{detail}");
    }

    #[test]
    fn lint_violations_fail_the_gate_and_are_labelled_as_lint() {
        let lint = parse_violations(LINT_VIOLATIONS);
        let GateOutcome::Fail { detail } = outcome_of(&lint, Some((String::new(), Vec::new())))
        else {
            panic!("an unidiomatic schema is a verdict about the schema");
        };
        assert!(
            detail.starts_with("2 lint violation(s) and 0 breaking change(s)"),
            "{detail}"
        );
        assert!(detail.contains("lint: proto/x/v1/bad.proto"), "{detail}");
    }

    #[test]
    fn breaking_changes_fail_the_gate_and_are_labelled_as_breaking() {
        let breaks = parse_violations(BREAKING_VIOLATIONS);
        let GateOutcome::Fail { detail } = outcome_of(&[], Some((String::new(), breaks))) else {
            panic!("a broken contract is a verdict about the schema");
        };
        assert!(
            detail.starts_with("0 lint violation(s) and 2 breaking change(s)"),
            "{detail}"
        );
        assert!(
            detail.contains("breaking: proto/x/v1/bad.proto"),
            "{detail}"
        );
    }

    #[test]
    fn the_gate_lints_then_finds_the_baseline_then_compares() {
        let (_dir, ctx) = with_protos();
        let runner = ScriptedRunner::new(vec![
            exited(0, "buf 1.69.0"),
            exited(0, ""),
            exited(0, "abc123def456\n"),
            exited(0, ""),
        ]);
        let outcome = run_gate(&gate(), &ctx, &runner);

        assert_eq!(outcome.label(), "pass", "{outcome:?}");
        assert_eq!(
            runner.command_lines(),
            vec![
                "buf --version",
                "buf lint",
                "git merge-base HEAD main",
                "buf breaking --against .git#ref=abc123def456",
            ]
        );
    }

    #[test]
    fn an_absent_trunk_still_lints_and_says_so() {
        let (_dir, ctx) = with_protos();
        let runner = ScriptedRunner::new(vec![
            exited(0, "buf 1.69.0"),
            exited(0, ""),
            failed(128, "fatal: Not a valid object name main"),
        ]);
        let GateOutcome::Pass { detail } = run_gate(&gate(), &ctx, &runner) else {
            panic!("lint measured and was clean");
        };
        assert!(detail.contains("unchecked"), "{detail}");
    }

    #[test]
    fn a_wrong_buf_version_is_an_instrument_error() {
        let (_dir, ctx) = with_protos();
        let runner = ScriptedRunner::new(vec![exited(0, "1.50.0")]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx, &runner) else {
            panic!("a different buf enforces a different rule set");
        };
        assert!(reason.contains("1.50.0"), "{reason}");
    }

    #[test]
    fn a_buf_failure_that_is_not_a_violation_is_an_instrument_error() {
        let (_dir, ctx) = with_protos();
        let runner = ScriptedRunner::new(vec![
            exited(0, "buf 1.69.0"),
            failed(1, "Failure: buf.yaml: unknown field \"lynt\""),
        ]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx, &runner) else {
            panic!("a misconfigured buf did not measure the schema");
        };
        assert!(reason.contains("unknown field"), "{reason}");
    }

    #[test]
    fn claiming_violations_without_naming_any_is_an_instrument_error() {
        let (_dir, ctx) = with_protos();
        let runner = ScriptedRunner::new(vec![exited(0, "buf 1.69.0"), exited(100, "")]);
        let GateOutcome::Error { .. } = run_gate(&gate(), &ctx, &runner) else {
            panic!("a violation exit with no violations named is not a measurement");
        };
    }
}
