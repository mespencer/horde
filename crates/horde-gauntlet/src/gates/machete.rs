//! Unused dependencies: cargo-machete, dev-dependencies included.
//!
//! Dev-dependencies matter as much as normal ones: a `proptest` or `criterion`
//! left in `[dev-dependencies]` after the tests that used it were deleted reads,
//! to anyone auditing the manifest, as a property-testing suite that no longer
//! exists. cargo-machete checks both tables, so the gate needs no extra flag —
//! only a test proving it stays that way.

use crate::gates::prelude::*;
use crate::gates::unexpected_exit;

/// The cargo-machete version the gate is calibrated against.
///
/// Invoked as `cargo-machete`, not `cargo machete`. cargo-machete decides
/// whether its first argument is the subcommand name or a path to scan by
/// looking at the `CARGO` environment variable, which cargo sets when it
/// dispatches a subcommand. Run it any other way and `cargo machete --version`
/// becomes "scan the directories `machete` and `--version`" — which is exactly
/// what happened on main, where the gate reported `error` because the tool
/// exited 2 with output no version parser could read.
///
/// Calling the binary directly removes the whole chain. A version probe should
/// not depend on an environment variable set three processes away deciding how
/// its own arguments are parsed.
pub const PIN: ToolPin = ToolPin {
    program: "cargo-machete",
    version_args: &["--version"],
    expected: "0.9.2",
    install: "cargo install --locked cargo-machete@0.9.2",
};

/// Exit code cargo-machete uses when it found unused dependencies.
const FOUND_UNUSED: i32 = 1;

/// Finds dependencies no crate in the workspace actually uses.
pub fn gate() -> CommandGate {
    CommandGate {
        pin: Some(&PIN),
        build: spec,
        read: Reader::AnyExit(outcome_of),
    }
}

/// The command this gate runs.
///
/// cargo-machete takes no package filter; it walks the directory tree, honouring
/// `.gitignore`, so `-p` narrowing does not apply.
pub fn spec(ctx: &GateContext) -> CommandSpec {
    CommandSpec::owned(
        PIN.program,
        vec![
            "--with-metadata".to_owned(),
            "--skip-target-dir".to_owned(),
            ctx.workspace_root.display().to_string(),
        ],
        &ctx.workspace_root,
    )
}

/// Reads a cargo-machete run.
pub fn outcome_of(spec: &CommandSpec, output: &CommandOutput) -> GateOutcome {
    let unused = unused_dependencies(&output.combined());
    if output.success() {
        return GateOutcome::pass("no unused dependencies, dev-dependencies included");
    }
    if output.status != Some(FOUND_UNUSED) || unused.is_empty() {
        return unexpected_exit(spec, output);
    }
    let mut detail = format!("{} unused dependenc(ies)", unused.len());
    for (package, dependency) in unused {
        detail.push_str(&format!("\n  {package}: {dependency}"));
    }
    GateOutcome::fail(detail)
}

/// The `(package, dependency)` pairs cargo-machete reported.
///
/// The report is a header line per package (`name -- path/Cargo.toml:`) followed
/// by indented dependency names, and ends at the first blank line — after which
/// cargo-machete prints advice that must not be read as findings.
pub fn unused_dependencies(text: &str) -> Vec<(String, String)> {
    let mut found = Vec::new();
    let mut package: Option<String> = None;
    for line in text.lines() {
        if line.trim().is_empty() {
            package = None;
        } else if let Some(name) = package_header(line) {
            package = Some(name.to_owned());
        } else if let (Some(name), Some(dependency)) = (package.as_ref(), indented(line)) {
            found.push((name.clone(), dependency.to_owned()));
        }
    }
    found
}

/// A package header: unindented, `name -- path/Cargo.toml:`.
///
/// Indentation alone decides header from entry — an *indented* line is a
/// dependency name whatever else it looks like — so the two conditions are
/// checked separately rather than as one guard.
fn package_header(line: &str) -> Option<&str> {
    if line.starts_with(char::is_whitespace) {
        return None;
    }
    let (name, manifest) = line.split_once(" -- ")?;
    if name.is_empty() || !manifest.ends_with(':') {
        return None;
    }
    Some(name)
}

fn indented(line: &str) -> Option<&str> {
    if !line.starts_with(char::is_whitespace) {
        return None;
    }
    Some(line.trim()).filter(|trimmed| !trimmed.is_empty())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::testing::{ScriptedRunner, exited, failed, repo_ctx, run_gate};

    const UNUSED: &str = include_str!("../../fixtures/cargo-machete-unused.txt");
    const CLEAN: &str = include_str!("../../fixtures/cargo-machete-clean.txt");

    #[test]
    fn the_command_resolves_renamed_dependencies_and_skips_build_output() {
        assert_eq!(
            spec(&repo_ctx()).display(),
            "cargo-machete --with-metadata --skip-target-dir /repo"
        );
    }

    #[test]
    fn a_real_report_lists_every_unused_dependency_with_its_package() {
        assert_eq!(
            unused_dependencies(UNUSED),
            vec![
                ("mach".to_owned(), "serde".to_owned()),
                ("mach".to_owned(), "tempfile".to_owned()),
            ]
        );
    }

    #[test]
    fn an_unused_dev_dependency_is_a_finding_like_any_other() {
        let found = unused_dependencies(UNUSED);
        assert!(
            found.contains(&("mach".to_owned(), "tempfile".to_owned())),
            "tempfile is a dev-dependency in the fixture: {found:?}"
        );
    }

    #[test]
    fn the_advice_cargo_machete_prints_afterwards_is_not_read_as_findings() {
        let found = unused_dependencies(UNUSED);
        assert!(
            found.iter().all(|(_, dependency)| dependency != "prost"),
            "`prost` appears only in the example advice block: {found:?}"
        );
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn a_clean_report_yields_no_findings() {
        assert_eq!(unused_dependencies(CLEAN), Vec::new());
    }

    #[test]
    fn several_packages_keep_their_own_dependencies() {
        let text = "found the following:\n\
                    alpha -- ./a/Cargo.toml:\n\
                    \tserde\n\
                    beta -- ./b/Cargo.toml:\n\
                    \tclap\n";
        assert_eq!(
            unused_dependencies(text),
            vec![
                ("alpha".to_owned(), "serde".to_owned()),
                ("beta".to_owned(), "clap".to_owned()),
            ]
        );
    }

    #[test]
    fn an_indented_line_is_a_dependency_however_much_it_looks_like_a_header() {
        let text = "alpha -- ./a/Cargo.toml:\n\tbeta -- ./b/Cargo.toml:\n";
        assert_eq!(
            unused_dependencies(text),
            vec![("alpha".to_owned(), "beta -- ./b/Cargo.toml:".to_owned())],
            "indentation decides header from entry, not shape"
        );
    }

    #[test]
    fn an_unindented_line_that_is_not_a_manifest_reference_is_not_a_header() {
        let text = "cargo-machete found the following unused dependencies:\n\tserde\n";
        assert_eq!(
            unused_dependencies(text),
            Vec::new(),
            "the preamble must not become a package name"
        );
    }

    #[test]
    fn a_header_whose_manifest_lacks_the_trailing_colon_is_not_a_header() {
        let text = "alpha -- ./a/Cargo.toml\n\tserde\n";
        assert_eq!(unused_dependencies(text), Vec::new());
    }

    #[test]
    fn indented_lines_with_no_package_header_are_ignored() {
        assert_eq!(unused_dependencies("\tserde\n"), Vec::new());
    }

    #[test]
    fn a_clean_run_passes_and_says_dev_dependencies_were_included() {
        let output = CommandOutput::new(Some(0), CLEAN, "");
        let GateOutcome::Pass { detail } = outcome_of(&spec(&repo_ctx()), &output) else {
            panic!("exit zero means no unused dependencies");
        };
        assert!(detail.contains("dev-dependencies"), "{detail}");
    }

    #[test]
    fn a_dirty_run_fails_and_names_each_dependency() {
        let output = CommandOutput::new(Some(1), UNUSED, "");
        let GateOutcome::Fail { detail } = outcome_of(&spec(&repo_ctx()), &output) else {
            panic!("an unused dependency is a verdict about the code");
        };
        assert!(detail.starts_with("2 unused dependenc(ies)"), "{detail}");
        assert!(detail.contains("mach: serde"), "{detail}");
        assert!(detail.contains("mach: tempfile"), "{detail}");
    }

    #[test]
    fn cargo_machetes_own_error_exit_is_an_instrument_error() {
        let output = failed(2, "error: could not read Cargo.toml");
        let GateOutcome::Error { reason } = outcome_of(&spec(&repo_ctx()), &output) else {
            panic!("exit 2 is cargo-machete failing, not a finding");
        };
        assert!(reason.contains("could not read Cargo.toml"), "{reason}");
    }

    #[test]
    fn a_failure_with_no_parseable_findings_is_an_instrument_error() {
        let output = failed(1, "");
        let GateOutcome::Error { .. } = outcome_of(&spec(&repo_ctx()), &output) else {
            panic!("claiming unused dependencies without naming any is not a measurement");
        };
    }

    #[test]
    fn the_tool_is_invoked_directly_rather_than_through_cargo() {
        // cargo-machete reads its own argv differently depending on whether
        // `CARGO` is set, so going through `cargo machete` makes the version
        // probe depend on an environment variable set three processes away.
        // Observed on main: it scanned directories named `machete` and
        // `--version` and exited 2.
        assert_eq!(PIN.program, "cargo-machete");
        assert_eq!(PIN.version_args, &["--version"]);
        let spec = spec(&repo_ctx());
        assert_eq!(spec.program, "cargo-machete");
        assert!(
            !spec.args.iter().any(|arg| arg == "machete"),
            "a bare `machete` argument is a path to scan, not a subcommand: {:?}",
            spec.args
        );
    }

    #[test]
    fn the_gate_is_pinned_to_the_calibrated_cargo_machete() {
        assert_eq!(gate().pin, Some(&PIN));
        assert_eq!(PIN.expected, "0.9.2");
    }

    #[test]
    fn the_gate_runs_the_probe_then_the_check() {
        let runner = ScriptedRunner::new(vec![
            exited(0, "0.9.2"),
            CommandOutput::new(Some(0), CLEAN, ""),
        ]);
        assert_eq!(run_gate(&gate(), &repo_ctx(), &runner).label(), "pass");
        assert_eq!(
            runner.command_lines(),
            vec![
                "cargo-machete --version",
                "cargo-machete --with-metadata --skip-target-dir /repo",
            ]
        );
    }
}
