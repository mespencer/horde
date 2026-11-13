use super::*;
use crate::command::CommandOutput;
use crate::testing::{ScriptedRunner, ctx_at, exited, failed, repo_ctx, run_gate};

/// Real `df -P -k` output with 75 GiB available — comfortably over the line.
const DF: &str = include_str!("../../../fixtures/df-portable.txt");

/// The same shape with 5 GiB available.
const DF_NEARLY_FULL: &str = "Filesystem   1024-blocks      Used Available Capacity  Mounted on\n\
     /dev/disk3s5   971350180 966107300   5242880    99%    /System/Volumes/Data\n";

/// A real report: `horde-observe` with three survivors.
const SURVIVORS: &str = include_str!("../../../fixtures/gamma-report-survivors.json");

const PRODUCT: &str = "crates/a/src/lib.rs";
const OTHER: &str = "crates/b/src/lib.rs";
const REAP: &str = "crates/horde-server-testkit/src/reap.rs";

fn settings() -> Settings {
    Settings {
        jobs: 16,
        memory: MemoryCap::Limit(DEFAULT_MEMORY_MIB * MIB),
        cache_dir: None,
        rustflags: "--cap-lints=allow".to_owned(),
    }
}

/// `cargo gamma list --json` naming one mutant in each of `files`.
fn listing(files: &[(&str, &str)]) -> String {
    let mutants: Vec<serde_json::Value> = files
        .iter()
        .enumerate()
        .map(|(at, (file, package))| {
            serde_json::json!({
                "file": file, "package": package, "line": at + 1, "column": 5,
                "mutator": "relational.gt_to_ge", "item_path": "f",
                "replacement": ">=", "suppression": null
            })
        })
        .collect();
    serde_json::Value::Array(mutants).to_string()
}

/// A gamma report with one mutant per status.
fn report(statuses: &[&str]) -> String {
    let mutants: Vec<serde_json::Value> = statuses
        .iter()
        .enumerate()
        .map(|(at, status)| {
            serde_json::json!({
                "status": status, "mutatorName": "relational.gt_to_ge", "replacement": ">=",
                "location": {"start": {"line": at + 1, "column": 3}}, "statusReason": ""
            })
        })
        .collect();
    serde_json::json!({"schemaVersion": "2", "files": {PRODUCT: {"mutants": mutants}}}).to_string()
}

fn ok(text: &str) -> CommandOutput {
    exited(0, text)
}

/// What one scripted run of the gate produced.
struct Run {
    _dir: tempfile::TempDir,
    ctx: GateContext,
    outcome: GateOutcome,
    lines: Vec<String>,
}

impl Run {
    fn mutation_command(&self) -> &str {
        self.lines
            .iter()
            .find(|line| line.contains("cargo gamma run"))
            .map_or("", String::as_str)
    }

    fn ran(&self) -> bool {
        !self.mutation_command().is_empty()
    }
}

/// The patch check and the disk pre-flight, both healthy.
fn preflight() -> Vec<CommandOutput> {
    vec![ok("patched cargo-gamma 0.2.1"), ok(DF)]
}

/// The two workspace listings, agreeing on `workspace`.
fn audit(workspace: &str) -> Vec<CommandOutput> {
    vec![ok(workspace), ok(workspace)]
}

/// Runs `measure` against a throwaway root whose context `shape` adjusts,
/// scripting `responses`; the report `report` is written when the call at
/// `report_at` (gamma's run) happens.
fn measure_with(
    shape: impl FnOnce(GateContext) -> GateContext,
    responses: Vec<CommandOutput>,
    report: Option<(usize, &str)>,
) -> Run {
    let dir = tempfile::tempdir().expect("temp dir");
    let ctx = shape(ctx_at(dir.path()));
    let mut runner = ScriptedRunner::new(responses);
    if let Some((at, text)) = report {
        runner = runner.writing(at, &artifact_dir(&ctx).join("gamma-report.json"), text);
    }
    let outcome = measure(&ctx, &runner, &settings());
    Run {
        _dir: dir,
        ctx,
        outcome,
        lines: runner.command_lines(),
    }
}

/// A `--full` run over a one-file workspace whose gamma report says `report`.
fn full_run(report: &str, after: Vec<CommandOutput>) -> Run {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.push(ok(&workspace)); // the run's own listing
    script.push(ok("Summary ...")); // the run
    script.extend(after);
    measure_with(
        |ctx| ctx.measuring_everything(true),
        script,
        Some((5, report)),
    )
}

#[test]
fn the_gate_is_pinned_to_cargo_gamma_0_2_1_and_installs_with_the_repo_script() {
    assert_eq!(gate().pin, &PIN);
    assert_eq!(PIN.expected, "0.2.1");
    assert_eq!(PIN.version_args, &["gamma", "--version"]);
    assert_eq!(PIN.install, "tools/cargo-gamma/install.sh");
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert!(
        root.join(INSTALLER).is_file(),
        "the installer the error names must exist"
    );
}

#[test]
fn a_stock_cargo_gamma_is_refused_with_the_install_command() {
    let dir = tempfile::tempdir().expect("temp dir");
    let runner = ScriptedRunner::new(vec![
        ok("cargo-gamma 0.2.1"),
        failed(
            1,
            "/home/u/.cargo/bin/cargo-gamma is not the patched cargo-gamma 0.2.1",
        ),
    ]);
    let GateOutcome::Error { reason } = run_gate(&gate(), &ctx_at(dir.path()), &runner) else {
        panic!("a stock build is not the instrument the gate is calibrated against");
    };
    assert!(reason.contains("not the patched build"), "{reason}");
    assert!(
        reason.contains("install with: tools/cargo-gamma/install.sh"),
        "{reason}"
    );
    assert_eq!(
        runner.command_lines(),
        vec![
            "cargo gamma --version",
            "bash tools/cargo-gamma/install.sh --check"
        ]
    );
}

#[test]
fn a_nearly_full_volume_stops_the_gate_before_anything_is_listed() {
    let run = measure_with(|ctx| ctx, vec![ok("patched"), ok(DF_NEARLY_FULL)], None);
    let GateOutcome::Error { reason } = &run.outcome else {
        panic!("a volume too full to hold the run is a broken instrument");
    };
    assert!(reason.contains("only 5.0 GiB free"), "{reason}");
    assert_eq!(run.lines.len(), 2);
}

#[test]
fn a_disk_probe_that_fails_is_an_instrument_error() {
    let run = measure_with(
        |ctx| ctx,
        vec![ok("patched"), failed(1, "df: cannot stat")],
        None,
    );
    assert_eq!(run.outcome.label(), "error");
    assert!(
        run.outcome
            .detail()
            .contains("free space could not be checked"),
        "{:?}",
        run.outcome
    );
}

#[test]
fn a_clean_full_sweep_passes_and_records_head_as_the_last_green_one() {
    let run = full_run(
        &report(&["Killed", "Killed", "CompileError"]),
        vec![ok(""), ok("abc123def4567890\n")],
    );
    let GateOutcome::Pass { detail } = &run.outcome else {
        panic!("nothing was missed: {:?}", run.outcome);
    };
    assert!(
        detail.starts_with("2 mutant(s) caught in the whole tree, none missed (1 unviable"),
        "{detail}"
    );
    assert!(
        detail.contains("recorded abc123def456 as the last green full sweep"),
        "{detail}"
    );
    assert_eq!(
        last_full_sweep(&run.ctx),
        Some("abc123def4567890".to_owned())
    );
    assert_eq!(
        run.lines[2],
        "cargo gamma list --color never --progress never --workspace --json"
    );
    assert_eq!(
        run.lines[3],
        "cargo gamma list --color never --progress never --workspace --json --no-config"
    );
    assert!(
        !run.mutation_command().contains("--in-diff"),
        "{:?}",
        run.lines
    );
    assert_eq!(run.lines[6], "git status --porcelain --untracked-files=no");
}

#[test]
fn a_green_sweep_of_a_dirty_tree_is_reported_but_not_recorded() {
    let run = full_run(&report(&["Killed"]), vec![ok(" M crates/a/src/lib.rs\n")]);
    assert_eq!(run.outcome.label(), "pass");
    assert!(
        run.outcome.detail().contains("uncommitted changes"),
        "{}",
        run.outcome.detail()
    );
    assert_eq!(last_full_sweep(&run.ctx), None);
}

#[test]
fn a_full_sweep_with_survivors_fails_lists_them_and_records_nothing() {
    let run = full_run(SURVIVORS, Vec::new());
    let GateOutcome::Fail { detail } = &run.outcome else {
        panic!(
            "a mutation that stays green is a finding: {:?}",
            run.outcome
        );
    };
    assert!(
        detail.starts_with("3 mutant(s) missed in the whole tree (3 survived, 0 timed out"),
        "{detail}"
    );
    assert!(
        detail.contains("target/quality/mutants/missed.txt"),
        "{detail}"
    );
    let missed =
        std::fs::read_to_string(artifact_dir(&run.ctx).join("missed.txt")).expect("written");
    assert_eq!(missed.lines().count(), 3, "{missed}");
    assert!(
        missed.contains("pipeline.rs:179:47: literal.str_to_empty"),
        "{missed}"
    );
    assert_eq!(last_full_sweep(&run.ctx), None);
    assert!(
        !run.lines.iter().any(|line| line.starts_with("git")),
        "{:?}",
        run.lines
    );
}

#[test]
fn a_timeout_fails_the_gate() {
    let run = full_run(&report(&["Killed", "Timeout"]), Vec::new());
    assert_eq!(run.outcome.label(), "fail");
    assert!(
        run.outcome.detail().contains("1 timed out"),
        "{}",
        run.outcome.detail()
    );
}

/// Every response up to and including a clean run, for a one-file
/// workspace measured whole.
fn whole_tree_script() -> Vec<CommandOutput> {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.extend([ok(&workspace), ok("")]);
    script
}

/// An `--incremental` context whose last green full sweep is recorded.
fn after_a_recorded_sweep(dir: &Path) -> GateContext {
    let ctx = ctx_at(dir).since_last_full_sweep(true);
    std::fs::create_dir_all(&ctx.log_dir).expect("log dir");
    std::fs::write(ctx.log_dir.join(LAST_FULL_SWEEP), "0123456789abcdef\n").expect("marker");
    ctx
}

#[test]
fn a_narrowed_full_sweep_is_not_recorded_as_a_whole_tree_sweep() {
    let script = whole_tree_script();
    let run = measure_with(
        |ctx| {
            GateContext::new(&ctx.workspace_root, vec!["a".to_owned()]).measuring_everything(true)
        },
        script,
        Some((5, &report(&["Killed"]))),
    );
    assert_eq!(run.outcome.label(), "pass");
    assert!(
        run.mutation_command().contains("--package a"),
        "{:?}",
        run.lines
    );
    assert_eq!(
        run.lines.len(),
        6,
        "no git, because nothing is recorded: {:?}",
        run.lines
    );
}

#[test]
fn the_run_command_carries_scope_cache_jobs_memory_and_capped_lints() {
    let scope = Scope::Diff {
        path: PathBuf::from("/repo/target/quality/mutants.diff"),
        since: Since::BranchPoint,
    };
    let settings = Settings {
        cache_dir: Some(PathBuf::from("/cache")),
        ..settings()
    };
    assert_eq!(
        run_spec(
            &repo_ctx(),
            &selection(&["a".to_owned()]),
            &scope,
            &settings
        )
        .display(),
        "CARGO_ENCODED_RUSTFLAGS=--cap-lints=allow cargo gamma run --color never --progress never \
         --in-diff /repo/target/quality/mutants.diff --package a \
         --artifact-dir /repo/target/quality/mutants --cache-dir /cache --jobs 16 \
         --memory enforce --baseline-memory-limit 2147483648B --memory-multiplier 2 \
         --memory-headroom 134217728B -- --skip \
         reap::tests::a_real_orphaned_leader_is_reaped_by_its_directory"
    );
}

#[test]
fn every_skipped_test_exists_and_tests_only_unmutated_code() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let reap = std::fs::read_to_string(root.join(audit::DANGEROUS[0])).expect("reap.rs");
    for (test, why) in SKIPPED_TESTS {
        assert!(!why.is_empty(), "{test} needs its reason");
        let (module, name) = test.rsplit_once("::").expect("a test path");
        assert_eq!(
            module, "reap::tests",
            "only tests of the never-mutated reaper may be skipped"
        );
        assert!(
            reap.contains(&format!("fn {name}()")),
            "{test} no longer exists"
        );
    }
}

#[test]
fn without_a_named_cache_gamma_keeps_its_own() {
    let spec = run_spec(&repo_ctx(), &selection(&[]), &Scope::FullTree, &settings());
    assert!(
        !spec.display().contains("--cache-dir"),
        "{}",
        spec.display()
    );
    assert!(spec.display().contains("--workspace"), "{}", spec.display());
}

#[test]
fn a_listing_shares_the_run_s_scope_and_never_builds() {
    let diff = Path::new("/repo/target/quality/mutants.diff");
    assert_eq!(
        list_spec(&repo_ctx(), &selection(&[]), Some(diff), Config::Read).display(),
        "cargo gamma list --color never --progress never --in-diff /repo/target/quality/mutants.diff \
         --workspace --json"
    );
}

#[test]
fn selection_names_packages_or_the_whole_workspace() {
    assert_eq!(selection(&[]), vec!["--workspace"]);
    assert_eq!(
        selection(&["a".to_owned(), "b".to_owned()]),
        vec!["--package", "a", "--package", "b"]
    );
}

/// The responses for a branch whose diff against its merge-base is `diff`.
fn on_a_branch(diff: &str) -> Vec<CommandOutput> {
    vec![ok("branchpoint\n"), ok("tip\n"), ok(diff)]
}

fn section(path: &str) -> String {
    format!("diff --git a/{path} b/{path}\n--- a/{path}\n+++ b/{path}\n@@ -1 +1 @@\n+x\n")
}

#[test]
fn on_a_branch_the_run_is_scoped_to_the_mutable_part_of_the_diff() {
    let workspace = listing(&[(PRODUCT, "a"), (OTHER, "b")]);
    let diff = format!(
        "{}{}{}",
        section("README.md"),
        section(PRODUCT),
        section("crates/a/tests/it.rs")
    );
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.extend(on_a_branch(&diff));
    script.push(ok(&listing(&[(PRODUCT, "a")])));
    script.push(ok(""));
    let run = measure_with(|ctx| ctx, script, Some((8, &report(&["Killed"]))));
    assert_eq!(run.outcome.label(), "pass", "{:?}", run.outcome);
    assert!(
        run.outcome.detail().contains("in changed code"),
        "{}",
        run.outcome.detail()
    );
    assert_eq!(run.lines[4], "git merge-base HEAD main");
    assert_eq!(
        std::fs::read_to_string(diff_path(&run.ctx)).expect("written"),
        section(PRODUCT),
        "gamma refuses tests and docs it does not survey, so only source reaches it"
    );
    assert!(
        run.lines[7].contains("--in-diff") && run.lines[7].contains("--workspace"),
        "{:?}",
        run.lines
    );
    assert!(
        run.mutation_command().contains("--package a")
            && !run.mutation_command().contains("--workspace"),
        "only the packages with mutants in the diff are built: {}",
        run.mutation_command()
    );
}

#[test]
fn a_diff_touching_only_tests_and_docs_is_skipped_without_building() {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.extend(on_a_branch(&format!(
        "{}{}",
        section("README.md"),
        section("crates/a/tests/it.rs")
    )));
    let run = measure_with(|ctx| ctx, script, None);
    assert_eq!(run.outcome.label(), "skipped", "{:?}", run.outcome);
    assert!(
        run.outcome
            .detail()
            .starts_with("nothing mutable in changed code"),
        "{}",
        run.outcome.detail()
    );
    assert!(!run.ran());
}

#[test]
fn a_diff_whose_run_listing_is_empty_is_skipped() {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.extend(on_a_branch(&section(PRODUCT)));
    script.push(ok("[]"));
    let run = measure_with(|ctx| ctx, script, None);
    assert_eq!(run.outcome.label(), "skipped");
    assert!(
        run.outcome.detail().contains("gamma lists no mutant"),
        "{}",
        run.outcome.detail()
    );
    assert!(!run.ran());
}

#[test]
fn on_trunk_the_scope_is_what_the_tip_commit_brought_in() {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.extend([
        ok("tip\n"),
        ok("tip\n"),
        ok("previous\n"),
        ok(&section(PRODUCT)),
    ]);
    script.extend([ok(&workspace), ok("")]);
    let run = measure_with(|ctx| ctx, script, Some((9, &report(&["Killed"]))));
    assert_eq!(run.lines[6], "git rev-parse HEAD^1");
    assert_eq!(run.lines[7], "git diff --no-color previous");
    assert!(
        run.mutation_command().contains("--in-diff"),
        "{:?}",
        run.lines
    );
}

#[test]
fn an_absent_trunk_falls_back_to_the_whole_tree() {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.extend([
        failed(128, "fatal: Not a valid object name main"),
        ok("tip\n"),
    ]);
    script.extend([ok(&workspace), ok(""), ok(""), ok("abc\n")]);
    let run = measure_with(|ctx| ctx, script, Some((7, &report(&["Killed"]))));
    assert!(
        run.outcome.detail().contains("the whole tree"),
        "{}",
        run.outcome.detail()
    );
    assert!(!run.mutation_command().contains("--in-diff"));
}

#[test]
fn an_empty_branch_diff_falls_back_to_the_whole_tree() {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.extend(on_a_branch(""));
    script.extend([ok(&workspace), ok(""), ok(""), ok("abc\n")]);
    let run = measure_with(|ctx| ctx, script, Some((8, &report(&["Killed"]))));
    assert!(
        run.outcome.detail().contains("the whole tree"),
        "{}",
        run.outcome.detail()
    );
}

#[test]
fn an_incremental_run_diffs_against_the_last_green_full_sweep() {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.extend([ok(&section(PRODUCT)), ok(&workspace), ok("")]);
    let dir = tempfile::tempdir().expect("temp dir");
    let ctx = after_a_recorded_sweep(dir.path());
    let runner = ScriptedRunner::new(script).writing(
        6,
        &artifact_dir(&ctx).join("gamma-report.json"),
        &report(&["Killed"]),
    );
    let outcome = measure(&ctx, &runner, &settings());
    let lines = runner.command_lines();
    assert_eq!(lines[4], "git diff --no-color 0123456789abcdef");
    assert!(
        outcome
            .detail()
            .contains("code changed since the last green full sweep (0123456789ab)"),
        "{}",
        outcome.detail()
    );
    assert!(
        !lines.iter().any(|line| line.contains("merge-base")),
        "{lines:?}"
    );
}

#[test]
fn an_incremental_run_with_nothing_changed_is_skipped() {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.push(ok(""));
    let dir = tempfile::tempdir().expect("temp dir");
    let ctx = after_a_recorded_sweep(dir.path());
    let runner = ScriptedRunner::new(script);
    let outcome = measure(&ctx, &runner, &settings());
    assert_eq!(outcome.label(), "skipped");
    assert!(
        outcome.detail().starts_with("nothing changed"),
        "{}",
        outcome.detail()
    );
}

#[test]
fn an_incremental_run_with_no_green_sweep_recorded_measures_the_whole_tree() {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.extend([ok(&workspace), ok(""), ok(""), ok("abc\n")]);
    let run = measure_with(
        |ctx| ctx.since_last_full_sweep(true),
        script,
        Some((5, &report(&["Killed"]))),
    );
    assert!(
        run.outcome.detail().contains("the whole tree"),
        "{}",
        run.outcome.detail()
    );
    assert!(
        !run.lines.iter().any(|line| line.contains("git diff")),
        "{:?}",
        run.lines
    );
}

#[test]
fn a_workspace_listing_naming_the_reaper_stops_everything() {
    let dangerous = listing(&[(PRODUCT, "a"), (REAP, "horde-server-testkit")]);
    let run = measure_with(
        |ctx| ctx.measuring_everything(true),
        [preflight(), vec![ok(&dangerous)]].concat(),
        None,
    );
    let GateOutcome::Error { reason } = &run.outcome else {
        panic!("a mutant of the reaper must never run");
    };
    assert!(reason.starts_with("REFUSING TO MUTATE"), "{reason}");
    assert_eq!(
        run.lines.len(),
        3,
        "nothing after the listing: {:?}",
        run.lines
    );
}

#[test]
fn a_run_listing_naming_the_reaper_stops_the_run() {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.push(ok(&listing(&[(REAP, "horde-server-testkit")])));
    let run = measure_with(|ctx| ctx.measuring_everything(true), script, None);
    assert!(
        run.outcome.detail().starts_with("REFUSING TO MUTATE"),
        "{:?}",
        run.outcome
    );
    assert!(
        run.outcome.detail().contains("the run's own listing"),
        "{:?}",
        run.outcome
    );
    assert!(!run.ran());
}

#[test]
fn an_unproven_exclusion_stops_the_gate_before_it_scopes() {
    let dir = tempfile::tempdir().expect("temp dir");
    std::fs::write(
        dir.path().join(audit::CONFIG),
        "exclude-files = [\n    # a fixture\n    \"crates/gone/src/bin/stub.rs\",\n]\n",
    )
    .expect("config");
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    let runner = ScriptedRunner::new(script);
    let outcome = measure(&ctx_at(dir.path()), &runner, &settings());
    assert!(outcome.detail().contains("not in the tree"), "{outcome:?}");
    assert_eq!(runner.calls().len(), 4);
}

#[test]
fn a_listing_that_fails_is_an_instrument_error() {
    let run = measure_with(
        |ctx| ctx,
        [preflight(), vec![failed(2, "error: bad config")]].concat(),
        None,
    );
    assert_eq!(run.outcome.label(), "error");
    assert!(
        run.outcome.detail().contains("bad config"),
        "{:?}",
        run.outcome
    );
}

#[test]
fn a_run_that_exits_non_zero_is_an_instrument_error() {
    let workspace = listing(&[(PRODUCT, "a")]);
    let mut script = preflight();
    script.extend(audit(&workspace));
    script.extend([ok(&workspace), failed(1, "error: the baseline failed")]);
    let run = measure_with(|ctx| ctx.measuring_everything(true), script, None);
    assert_eq!(run.outcome.label(), "error");
    assert!(
        run.outcome.detail().contains("the baseline failed"),
        "{:?}",
        run.outcome
    );
}

#[test]
fn a_report_left_by_an_earlier_run_is_never_read_as_this_one() {
    let script = whole_tree_script();
    let dir = tempfile::tempdir().expect("temp dir");
    let ctx = ctx_at(dir.path()).measuring_everything(true);
    std::fs::create_dir_all(artifact_dir(&ctx)).expect("artifacts");
    std::fs::write(
        artifact_dir(&ctx).join("gamma-report.json"),
        report(&["Killed"]),
    )
    .expect("stale");
    let runner = ScriptedRunner::new(script);
    let outcome = measure(&ctx, &runner, &settings());
    let GateOutcome::Error { reason } = outcome else {
        panic!("a run that wrote no report did not measure");
    };
    assert!(reason.contains("gamma-report.json"), "{reason}");
}

#[test]
fn a_report_with_nothing_scored_is_skipped_not_passed() {
    assert_eq!(
        outcome_of(&Scope::FullTree, &Tally::default()).label(),
        "skipped"
    );
    let tally = report::parse(&report(&["CompileError", "Ignored"])).expect("valid");
    let outcome = outcome_of(&Scope::FullTree, &tally);
    assert_eq!(outcome.label(), "skipped");
    assert!(
        outcome.detail().contains("1 unviable, 1 excluded"),
        "{}",
        outcome.detail()
    );
}

#[test]
fn every_kind_of_miss_is_named_in_a_failure() {
    let tally =
        report::parse(&report(&["Survived", "Timeout", "NoCoverage", "Killed"])).expect("valid");
    let GateOutcome::Fail { detail } = outcome_of(&Scope::FullTree, &tally) else {
        panic!("three missed");
    };
    assert!(
        detail.starts_with("3 mutant(s) missed in the whole tree (1 survived, 1 timed out, 0 out of memory, 1 uncovered)"),
        "{detail}"
    );
    assert!(
        detail.contains("(1 caught, 0 unviable, 0 flaky)"),
        "{detail}"
    );
}

#[test]
fn scopes_are_named_in_the_detail() {
    assert_eq!(Scope::FullTree.label(), "the whole tree");
    let diff = |since| Scope::Diff {
        path: PathBuf::new(),
        since,
    };
    assert_eq!(diff(Since::BranchPoint).label(), "changed code");
    assert_eq!(
        diff(Since::LastFullSweep("abc".to_owned())).label(),
        "code changed since the last green full sweep (abc)"
    );
}

#[test]
fn jobs_default_to_half_the_cores_up_to_sixteen() {
    assert_eq!(jobs(None, Some(32)), Ok(16));
    assert_eq!(jobs(None, Some(64)), Ok(16));
    assert_eq!(jobs(None, Some(8)), Ok(4));
    assert_eq!(jobs(None, Some(1)), Ok(1));
    assert_eq!(jobs(None, None), Ok(1));
    assert_eq!(jobs(Some("  ".to_owned()), Some(8)), Ok(4));
}

#[test]
fn jobs_can_be_set_and_a_bad_value_is_refused() {
    assert_eq!(jobs(Some("24".to_owned()), Some(8)), Ok(24));
    for bad in ["0", "-1", "lots"] {
        let message = jobs(Some(bad.to_owned()), Some(8)).expect_err("not a job count");
        assert!(message.contains(JOBS_ENV), "{message}");
    }
}

#[test]
fn memory_defaults_to_four_gib_and_can_be_set_or_turned_off() {
    assert_eq!(memory_cap(None), Ok(MemoryCap::Limit(4096 * MIB)));
    assert_eq!(
        memory_cap(Some("2048".to_owned())),
        Ok(MemoryCap::Limit(2048 * MIB))
    );
    assert_eq!(memory_cap(Some("off".to_owned())), Ok(MemoryCap::Disabled));
    assert_eq!(memory_cap(Some("0".to_owned())), Ok(MemoryCap::Disabled));
}

#[test]
fn an_unreadable_memory_cap_is_refused_rather_than_defaulted() {
    let message = memory_cap(Some("4G".to_owned())).expect_err("not MiB");
    assert!(message.contains("not a whole number of MiB"), "{message}");
    let message = memory_cap(Some(u64::MAX.to_string())).expect_err("overflows");
    assert!(message.contains("more MiB than"), "{message}");
}

#[test]
fn the_memory_flags_bound_every_binary_by_the_cap() {
    assert_eq!(
        memory_args(MemoryCap::Limit(4096 * MIB)),
        vec![
            "--memory",
            "enforce",
            "--baseline-memory-limit",
            "2147483648B",
            "--memory-multiplier",
            "2",
            "--memory-headroom",
            "134217728B"
        ]
    );
    // A cap so small the headroom would exceed the baseline guard is held to it.
    assert_eq!(
        memory_args(MemoryCap::Limit(100 * MIB))[7],
        format!("{}B", 50 * MIB)
    );
    assert_eq!(memory_args(MemoryCap::Disabled), vec!["--memory", "off"]);
}

#[test]
fn lints_are_capped_while_keeping_the_flags_already_set() {
    assert_eq!(cap_lints(None, None), "--cap-lints=allow");
    assert_eq!(cap_lints(Some(String::new()), None), "--cap-lints=allow");
    assert_eq!(
        cap_lints(
            Some("-Ctarget-cpu=native".to_owned()),
            Some("ignored".to_owned())
        ),
        "-Ctarget-cpu=native\u{1f}--cap-lints=allow"
    );
    assert_eq!(
        cap_lints(None, Some("-D warnings".to_owned())),
        "-D\u{1f}warnings\u{1f}--cap-lints=allow"
    );
}

#[test]
fn settings_are_read_from_the_environment() {
    let env = |key: &str| match key {
        JOBS_ENV => Some("3".to_owned()),
        MEMORY_ENV => Some("off".to_owned()),
        CACHE_ENV => Some("/fast/cache".to_owned()),
        _ => None,
    };
    assert_eq!(
        Settings::from_env(env, Some(32)),
        Ok(Settings {
            jobs: 3,
            memory: MemoryCap::Disabled,
            cache_dir: Some(PathBuf::from("/fast/cache")),
            rustflags: "--cap-lints=allow".to_owned(),
        })
    );
    assert!(Settings::from_env(|key| (key == JOBS_ENV).then(|| "x".to_owned()), None).is_err());
}

#[test]
fn the_profile_gamma_builds_with_is_defined_in_the_workspace_manifest() {
    let config = include_str!("../../../../../gamma.toml");
    assert!(
        config.contains(&format!("profile = \"{PROFILE}\"")),
        "gamma.toml selects the profile"
    );
    let manifest = include_str!("../../../../../Cargo.toml");
    assert!(manifest.contains(&format!("[profile.{PROFILE}]")));
    assert!(
        manifest.contains("debug = false"),
        "the profile exists to drop debuginfo"
    );
}

#[test]
fn every_committed_exclusion_is_one_literal_path_with_a_reason() {
    let config = include_str!("../../../../../gamma.toml");
    let entries = audit::configured_exclusions(config).expect("readable");
    assert!(!entries.is_empty());
    for entry in entries {
        assert_eq!(
            audit::literal_path(&entry.pattern),
            Ok(entry.pattern.as_str())
        );
        assert!(!entry.why.is_empty(), "{} has no reason", entry.pattern);
    }
}

#[test]
fn the_disk_probe_asks_df_for_portable_kilobyte_output() {
    assert_eq!(free_space_spec(Path::new("/repo")).display(), "df -P -k .");
}

#[test]
fn free_space_is_read_from_real_df_output_in_bytes() {
    assert_eq!(parse_free_bytes(DF), Ok(78_666_364 * 1024));
}

#[test]
fn df_output_the_gate_cannot_read_is_refused_not_assumed_healthy() {
    for text in [
        "",
        "\n   \n",
        "Filesystem   1024-blocks      Used Available Capacity  Mounted on\n",
        "/dev/disk3s5 mounted\n",
        "/dev/disk3s5   971350180 966107300   lots    99%   /\n",
    ] {
        assert!(parse_free_bytes(text).is_err(), "{text:?}");
    }
}

#[test]
fn the_disk_line_is_forty_gib_exactly() {
    assert_eq!(MIN_FREE_BYTES, 40 * 1024 * 1024 * 1024);
    assert_eq!(enough_disk(MIN_FREE_BYTES), Ok(()));
    assert!(enough_disk(MIN_FREE_BYTES - 1).is_err());
    let message = enough_disk(5 * GIB).expect_err("far under");
    assert!(message.contains("only 5.0 GiB free"), "{message}");
    assert!(message.contains("40 GiB"), "{message}");
    assert!(message.contains("cargo-gamma"), "{message}");
}
