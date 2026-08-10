//! Module structure: no orphaned source files, no new module cycles.
//!
//! Two failure modes, one instrument. An agent asked to "move that helper out"
//! creates `src/helper.rs` and forgets `mod helper;` — the file is dead, still
//! compiles nothing, and reviews as if it were live. Separately, an agent
//! resolving a borrow error by reaching back into the calling module turns a
//! layered crate into a knot. Neither is a measurement needing calibration, so
//! both are blocking from the start.

use std::path::{Path, PathBuf};

use crate::command::CommandRunner;
use crate::cycles::{
    Cycle, CycleDiff, diff_cycles, find_cycles, parse_baseline, parse_module_graph,
};
use crate::gates::prelude::*;
use crate::gates::{invoke, read_artifact, strip_ansi, unexpected_exit};
use crate::metadata::{WorkspacePackage, parse_workspace_packages};

/// The cargo-modules version the gate is calibrated against.
pub const PIN: ToolPin = ToolPin {
    program: "cargo",
    version_args: &["modules", "--version"],
    expected: "0.26.0",
    install: "cargo install --locked cargo-modules@0.26.0",
};

/// Committed baseline of module cycles, relative to the workspace root.
pub const BASELINE: &str = "baselines/module-cycles.txt";

/// Checks for orphaned source files and ratchets module cycles.
pub fn gate() -> PinnedGate {
    PinnedGate {
        pin: &PIN,
        nothing_to_measure: None,
        measure: survey_workspace,
    }
}

/// Surveys every selected package, then ratchets what it found.
fn survey_workspace(ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
    let packages = match packages(ctx, runner) {
        Ok(packages) => packages,
        Err(outcome) => return outcome,
    };
    if packages.is_empty() {
        return GateOutcome::skipped("no workspace packages to analyse");
    }
    let findings = match survey(ctx, runner, &packages) {
        Ok(findings) => findings,
        Err(outcome) => return outcome,
    };
    let baseline = match read_artifact(&baseline_path(ctx), "the module-cycle baseline") {
        Ok(text) => parse_baseline(&text),
        Err(outcome) => return outcome,
    };
    outcome_of(&findings, &baseline)
}

/// Where the committed cycle baseline lives.
pub fn baseline_path(ctx: &GateContext) -> PathBuf {
    ctx.workspace_root.join(BASELINE)
}

/// The command that lists the workspace's packages and targets.
pub fn metadata_spec(root: &Path) -> CommandSpec {
    CommandSpec::new(
        "cargo",
        &["metadata", "--no-deps", "--format-version", "1"],
        root,
    )
}

/// The command that looks for orphaned files in one target.
pub fn orphans_spec(ctx: &GateContext, package: &str, target: Target<'_>) -> CommandSpec {
    let mut args = vec![
        "modules".to_owned(),
        "orphans".to_owned(),
        "--deny".to_owned(),
        // Without this, every `#[cfg(test)] mod tests` reads as an orphan.
        "--cfg-test".to_owned(),
        "-p".to_owned(),
        package.to_owned(),
    ];
    args.extend(target.args());
    CommandSpec::owned("cargo", args, &ctx.workspace_root)
}

/// The command that draws one library's internal dependency graph.
pub fn graph_spec(ctx: &GateContext, package: &str) -> CommandSpec {
    CommandSpec::owned(
        "cargo",
        vec![
            "modules".to_owned(),
            "dependencies".to_owned(),
            "--lib".to_owned(),
            "--cfg-test".to_owned(),
            "--no-externs".to_owned(),
            "--no-sysroot".to_owned(),
            "-p".to_owned(),
            package.to_owned(),
        ],
        &ctx.workspace_root,
    )
}

/// Which target of a package to analyse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target<'a> {
    /// The package's library.
    Lib,
    /// One of the package's binaries.
    Bin(&'a str),
}

impl Target<'_> {
    fn args(self) -> Vec<String> {
        match self {
            Self::Lib => vec!["--lib".to_owned()],
            Self::Bin(name) => vec!["--bin".to_owned(), name.to_owned()],
        }
    }
}

/// What the structural survey found.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Findings {
    /// Orphaned source files, as cargo-modules described them.
    pub orphans: Vec<String>,
    /// Module cycles observed across the workspace.
    pub cycles: Vec<Cycle>,
}

fn packages(
    ctx: &GateContext,
    runner: &dyn CommandRunner,
) -> Result<Vec<WorkspacePackage>, GateOutcome> {
    let spec = metadata_spec(&ctx.workspace_root);
    let output = invoke(runner, &spec)?;
    if !output.success() {
        return Err(unexpected_exit(&spec, &output));
    }
    let all = parse_workspace_packages(&output.stdout).map_err(GateOutcome::error)?;
    Ok(select(all, &ctx.packages))
}

/// Narrows the workspace to the packages this run asked for.
pub fn select(all: Vec<WorkspacePackage>, wanted: &[String]) -> Vec<WorkspacePackage> {
    if wanted.is_empty() {
        return all;
    }
    all.into_iter()
        .filter(|package| wanted.contains(&package.name))
        .collect()
}

fn survey(
    ctx: &GateContext,
    runner: &dyn CommandRunner,
    packages: &[WorkspacePackage],
) -> Result<Findings, GateOutcome> {
    let mut findings = Findings::default();
    for package in packages {
        for target in targets(package) {
            let spec = orphans_spec(ctx, &package.name, target);
            let output = invoke(runner, &spec)?;
            findings
                .orphans
                .extend(orphan_files(&strip_ansi(&output.combined())));
        }
        if package.has_lib {
            let spec = graph_spec(ctx, &package.name);
            let output = invoke(runner, &spec)?;
            if !output.success() {
                return Err(unexpected_exit(&spec, &output));
            }
            findings
                .cycles
                .extend(find_cycles(&parse_module_graph(&output.stdout)));
        }
    }
    findings.orphans.sort();
    findings.cycles.sort();
    Ok(findings)
}

/// The targets whose module trees are checked for orphans.
///
/// cargo-modules scans the whole crate directory for whichever target it was
/// pointed at, so in a package that has both a library and a binary, asking
/// about the binary reports every one of the library's modules as an orphan —
/// they are, from `main.rs`'s point of view. The library pass already covers
/// every file under `src/`, so a package with a library is checked through it
/// alone, and binaries are checked directly only when there is no library.
pub fn targets(package: &WorkspacePackage) -> Vec<Target<'_>> {
    if package.has_lib {
        return vec![Target::Lib];
    }
    package.bins.iter().map(|name| Target::Bin(name)).collect()
}

/// The orphaned files named in a cargo-modules run.
pub fn orphan_files(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|line| line.split_once("orphaned module "))
        .filter_map(|(_, rest)| rest.split_once(" at "))
        .map(|(module, path)| format!("{} ({module})", path.trim()))
        .collect()
}

/// Turns the survey into a verdict.
pub fn outcome_of(findings: &Findings, baseline: &[Cycle]) -> GateOutcome {
    let diff = diff_cycles(&findings.cycles, baseline);
    if findings.orphans.is_empty() && diff.is_clean() {
        return GateOutcome::pass(format!(
            "no orphaned files; {} module cycle(s), matching the baseline",
            findings.cycles.len()
        ));
    }
    let mut detail = String::new();
    for orphan in &findings.orphans {
        detail.push_str(&format!("\n  orphaned file: {orphan}"));
    }
    detail.push_str(&describe_cycle_diff(&diff));
    GateOutcome::fail(format!("module structure regressed{detail}"))
}

/// Explains a cycle ratchet failure in both directions.
pub fn describe_cycle_diff(diff: &CycleDiff) -> String {
    let mut detail = String::new();
    for cycle in &diff.added {
        detail.push_str(&format!("\n  new module cycle: {}", cycle.join(" <-> ")));
    }
    for cycle in &diff.removed {
        detail.push_str(&format!(
            "\n  baselined cycle no longer exists, remove it from {BASELINE}: {}",
            cycle.join(" <-> ")
        ));
    }
    detail
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandOutput;
    use crate::testing::{ScriptedRunner, ctx_at, exited, failed, repo_ctx, run_gate};

    const ORPHANS_FOUND: &str = include_str!("../../fixtures/cargo-modules-orphans-found.txt");
    const ORPHANS_CLEAN: &str = include_str!("../../fixtures/cargo-modules-orphans-clean.txt");
    const METADATA: &str = include_str!("../../fixtures/cargo-metadata.json");
    const DOT: &str = include_str!("../../fixtures/cargo-modules-dependencies.dot");

    fn cycle(members: &[&str]) -> Cycle {
        members.iter().map(|member| (*member).to_owned()).collect()
    }

    fn package(name: &str, has_lib: bool, bins: &[&str]) -> WorkspacePackage {
        WorkspacePackage {
            name: name.to_owned(),
            has_lib,
            bins: bins.iter().map(|bin| (*bin).to_owned()).collect(),
            manifest_dir: std::path::PathBuf::from("/repo/crates").join(name),
        }
    }

    #[test]
    fn the_orphan_check_enables_cfg_test_so_test_modules_are_not_orphans() {
        let spec = orphans_spec(&repo_ctx(), "horde-gauntlet", Target::Lib);
        assert_eq!(
            spec.display(),
            "cargo modules orphans --deny --cfg-test -p horde-gauntlet --lib"
        );
    }

    #[test]
    fn each_binary_is_checked_by_name() {
        let spec = orphans_spec(&repo_ctx(), "horde-gauntlet", Target::Bin("gauntlet"));
        assert!(
            spec.display().ends_with("--bin gauntlet"),
            "{}",
            spec.display()
        );
    }

    #[test]
    fn the_graph_command_keeps_items_so_cross_module_edges_survive() {
        let spec = graph_spec(&repo_ctx(), "horde-gauntlet");
        assert_eq!(
            spec.display(),
            "cargo modules dependencies --lib --cfg-test --no-externs --no-sysroot -p horde-gauntlet"
        );
    }

    #[test]
    fn a_package_with_a_library_is_checked_through_the_library_alone() {
        assert_eq!(
            targets(&package("a", true, &["one", "two"])),
            vec![Target::Lib],
            "asking about a binary in a package that also has a library reports every library \
             module as an orphan"
        );
    }

    #[test]
    fn a_binary_only_package_is_checked_through_each_binary() {
        assert_eq!(
            targets(&package("a", false, &["one", "two"])),
            vec![Target::Bin("one"), Target::Bin("two")]
        );
    }

    #[test]
    fn a_package_with_neither_target_is_not_checked() {
        assert_eq!(targets(&package("a", false, &[])), Vec::new());
    }

    #[test]
    fn narrowing_keeps_only_the_named_packages() {
        let all = vec![package("a", true, &[]), package("b", true, &[])];
        assert_eq!(
            select(all.clone(), &["b".to_owned()]),
            vec![package("b", true, &[])]
        );
        assert_eq!(select(all.clone(), &[]), all);
    }

    #[test]
    fn real_coloured_orphan_output_is_read_despite_the_escape_codes() {
        let orphans = orphan_files(&strip_ansi(ORPHANS_FOUND));
        assert_eq!(orphans, vec!["src/ghost.rs (`ghost`)".to_owned()]);
    }

    #[test]
    fn a_clean_orphan_run_reports_nothing() {
        assert_eq!(
            orphan_files(&strip_ansi(ORPHANS_CLEAN)),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_clean_survey_passes_and_states_how_many_cycles_are_baselined() {
        let findings = Findings {
            orphans: Vec::new(),
            cycles: vec![cycle(&["a", "b"])],
        };
        let GateOutcome::Pass { detail } = outcome_of(&findings, &[cycle(&["a", "b"])]) else {
            panic!("matching the baseline is a pass");
        };
        assert!(detail.contains("no orphaned files"), "{detail}");
        assert!(detail.contains("1 module cycle(s)"), "{detail}");
    }

    #[test]
    fn an_orphaned_file_fails_the_gate() {
        let findings = Findings {
            orphans: vec!["src/ghost.rs (`ghost`)".to_owned()],
            cycles: Vec::new(),
        };
        let GateOutcome::Fail { detail } = outcome_of(&findings, &[]) else {
            panic!("a dead source file is a verdict about the code");
        };
        assert!(detail.contains("orphaned file: src/ghost.rs"), "{detail}");
    }

    #[test]
    fn a_new_cycle_fails_the_gate() {
        let findings = Findings {
            orphans: Vec::new(),
            cycles: vec![cycle(&["a", "b"])],
        };
        let GateOutcome::Fail { detail } = outcome_of(&findings, &[]) else {
            panic!("a new cycle is a regression");
        };
        assert!(detail.contains("new module cycle: a <-> b"), "{detail}");
    }

    #[test]
    fn a_baselined_cycle_that_no_longer_exists_also_fails_the_gate() {
        let GateOutcome::Fail { detail } = outcome_of(&Findings::default(), &[cycle(&["a", "b"])])
        else {
            panic!("the ratchet is bidirectional so the baseline cannot overstate the debt");
        };
        assert!(detail.contains("no longer exists"), "{detail}");
        assert!(detail.contains(BASELINE), "{detail}");
    }

    #[test]
    fn the_gate_is_pinned_to_the_calibrated_cargo_modules() {
        assert_eq!(gate().pin, &PIN);
        assert_eq!(PIN.expected, "0.26.0");
    }

    /// Runs a clean survey against a throwaway root, optionally with a baseline.
    fn survey_with(baseline: Option<&str>) -> (GateOutcome, Vec<String>) {
        let dir = tempfile::tempdir().expect("temp dir");
        if let Some(baseline) = baseline {
            std::fs::create_dir_all(dir.path().join("baselines")).expect("baselines dir");
            std::fs::write(dir.path().join(BASELINE), baseline).expect("baseline");
        }
        let runner = ScriptedRunner::new(vec![
            exited(0, "cargo-modules 0.26.0"),
            exited(0, METADATA),
            exited(0, ORPHANS_CLEAN),
            exited(0, DOT),
        ]);
        let outcome = run_gate(&gate(), &ctx_at(dir.path()), &runner);
        (outcome, runner.command_lines())
    }

    #[test]
    fn the_gate_surveys_every_target_then_reads_the_baseline() {
        let (outcome, lines) = survey_with(Some("# none\n"));
        assert_eq!(outcome.label(), "pass", "{outcome:?}");
        assert!(lines[1].starts_with("cargo metadata"), "{lines:?}");
        assert!(lines[2].ends_with("--lib"), "{lines:?}");
        assert!(
            lines[3].starts_with("cargo modules dependencies"),
            "{lines:?}"
        );
        assert_eq!(
            lines.len(),
            4,
            "the binary is covered by the library pass: {lines:?}"
        );
    }

    #[test]
    fn a_missing_baseline_file_is_an_instrument_error() {
        let GateOutcome::Error { reason } = survey_with(None).0 else {
            panic!("a ratchet with no baseline is not a measurement");
        };
        assert!(reason.contains("module-cycles.txt"), "{reason}");
    }

    #[test]
    fn unreadable_metadata_is_an_instrument_error() {
        let runner = ScriptedRunner::new(vec![
            exited(0, "cargo-modules 0.26.0"),
            failed(101, "error: failed to parse manifest"),
        ]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &repo_ctx(), &runner) else {
            panic!("a workspace that cannot be enumerated cannot be surveyed");
        };
        assert!(reason.contains("failed to parse manifest"), "{reason}");
    }

    #[test]
    fn a_workspace_with_no_packages_is_skipped_not_passed() {
        let runner = ScriptedRunner::new(vec![
            exited(0, "cargo-modules 0.26.0"),
            CommandOutput::new(Some(0), r#"{"packages": [], "workspace_members": []}"#, ""),
        ]);
        let outcome = run_gate(&gate(), &repo_ctx(), &runner);
        assert_eq!(outcome.label(), "skipped");
    }
}
