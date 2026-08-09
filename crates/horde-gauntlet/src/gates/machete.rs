//! Unused dependencies: cargo-machete, dev-dependencies included.
//!
//! Dev-dependencies matter as much as normal ones: a `proptest` or `criterion`
//! left in `[dev-dependencies]` after the tests that used it were deleted reads,
//! to anyone auditing the manifest, as a property-testing suite that no longer
//! exists. cargo-machete checks both tables, so the gate needs no extra flag —
//! only a test proving it stays that way.

use crate::command::{CommandOutput, CommandRunner, CommandSpec};
use crate::gate::{Gate, GateContext, GateOutcome};
use crate::gates::{invoke, unexpected_exit};
use crate::tool::{ToolPin, check_pin};

/// The cargo-machete version the gate is calibrated against.
pub const PIN: ToolPin = ToolPin {
    program: "cargo",
    version_args: &["machete", "--version"],
    expected: "0.9.2",
    install: "cargo install --locked cargo-machete@0.9.2",
};

/// Exit code cargo-machete uses when it found unused dependencies.
const FOUND_UNUSED: i32 = 1;

/// Finds dependencies no crate in the workspace actually uses.
#[derive(Debug, Clone, Copy, Default)]
pub struct MacheteGate;

impl Gate for MacheteGate {
    fn run(&self, ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
        if let Err(reason) = check_pin(runner, &PIN, &ctx.workspace_root) {
            return GateOutcome::error(reason);
        }
        let spec = spec(ctx);
        match invoke(runner, &spec) {
            Err(outcome) => outcome,
            Ok(output) => outcome_of(&spec, &output),
        }
    }
}

/// The command this gate runs.
///
/// cargo-machete takes no package filter; it walks the directory tree, honouring
/// `.gitignore`, so `-p` narrowing does not apply.
pub fn spec(ctx: &GateContext) -> CommandSpec {
    CommandSpec::owned(
        "cargo",
        vec![
            "machete".to_owned(),
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

fn package_header(line: &str) -> Option<&str> {
    if line.starts_with(char::is_whitespace) || !line.ends_with(':') {
        return None;
    }
    let (name, _) = line.split_once(" -- ")?;
    (!name.is_empty()).then_some(name)
}

fn indented(line: &str) -> Option<&str> {
    if !line.starts_with(char::is_whitespace) {
        return None;
    }
    Some(line.trim()).filter(|trimmed| !trimmed.is_empty())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::testing::ScriptedRunner;

    const UNUSED: &str = include_str!("../../fixtures/cargo-machete-unused.txt");
    const CLEAN: &str = include_str!("../../fixtures/cargo-machete-clean.txt");

    fn ctx() -> GateContext {
        GateContext::new(Path::new("/repo"), Vec::new())
    }

    #[test]
    fn the_command_resolves_renamed_dependencies_and_skips_build_output() {
        assert_eq!(
            spec(&ctx()).display(),
            "cargo machete --with-metadata --skip-target-dir /repo"
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
    fn indented_lines_with_no_package_header_are_ignored() {
        assert_eq!(unused_dependencies("\tserde\n"), Vec::new());
    }

    #[test]
    fn a_clean_run_passes_and_says_dev_dependencies_were_included() {
        let output = CommandOutput::new(Some(0), CLEAN, "");
        let GateOutcome::Pass { detail } = outcome_of(&spec(&ctx()), &output) else {
            panic!("exit zero means no unused dependencies");
        };
        assert!(detail.contains("dev-dependencies"), "{detail}");
    }

    #[test]
    fn a_dirty_run_fails_and_names_each_dependency() {
        let output = CommandOutput::new(Some(1), UNUSED, "");
        let GateOutcome::Fail { detail } = outcome_of(&spec(&ctx()), &output) else {
            panic!("an unused dependency is a verdict about the code");
        };
        assert!(detail.starts_with("2 unused dependenc(ies)"), "{detail}");
        assert!(detail.contains("mach: serde"), "{detail}");
        assert!(detail.contains("mach: tempfile"), "{detail}");
    }

    #[test]
    fn cargo_machetes_own_error_exit_is_an_instrument_error() {
        let output = CommandOutput::new(Some(2), "", "error: could not read Cargo.toml");
        let GateOutcome::Error { reason } = outcome_of(&spec(&ctx()), &output) else {
            panic!("exit 2 is cargo-machete failing, not a finding");
        };
        assert!(reason.contains("could not read Cargo.toml"), "{reason}");
    }

    #[test]
    fn a_failure_with_no_parseable_findings_is_an_instrument_error() {
        let output = CommandOutput::new(Some(1), "", "");
        let GateOutcome::Error { .. } = outcome_of(&spec(&ctx()), &output) else {
            panic!("claiming unused dependencies without naming any is not a measurement");
        };
    }

    #[test]
    fn a_version_mismatch_stops_the_gate_before_it_runs() {
        let runner = ScriptedRunner::new(vec![CommandOutput::new(Some(0), "0.8.0", "")]);
        let GateOutcome::Error { .. } = MacheteGate.run(&ctx(), &runner) else {
            panic!("an unpinned tool moves the calibration");
        };
        assert_eq!(runner.calls().len(), 1);
    }

    #[test]
    fn the_gate_runs_the_probe_then_the_check() {
        let runner = ScriptedRunner::new(vec![
            CommandOutput::new(Some(0), "0.9.2", ""),
            CommandOutput::new(Some(0), CLEAN, ""),
        ]);
        assert_eq!(MacheteGate.run(&ctx(), &runner).label(), "pass");
        assert_eq!(
            runner.command_lines(),
            vec![
                "cargo machete --version",
                "cargo machete --with-metadata --skip-target-dir /repo",
            ]
        );
    }
}
