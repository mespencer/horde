//! The `gauntlet` command line.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};

use crate::command::{CommandRunner, ProcessRunner};
use crate::gate::GateContext;
use crate::registry::{registry, render_list};
use crate::report::{render_markdown, render_table};
use crate::runner::{load_results, resolve, run};

/// Horde's quality gates: one binary, the same on a laptop and in CI.
#[derive(Debug, Parser)]
#[command(name = "gauntlet", version, about, long_about = None)]
pub struct Cli {
    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// The subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run gates and report. With no names, runs every gate.
    Run {
        /// Gates to run; dependencies are pulled in automatically.
        gates: Vec<String>,
        /// Narrow to these packages (repeatable).
        #[arg(short = 'p', long = "package")]
        packages: Vec<String>,
        /// Measure the whole tree, not just the change under test.
        ///
        /// Only `mutants` scopes by change; everything else already measures
        /// everything. Use this for the periodic sweep that catches behaviour
        /// no recent diff has touched.
        #[arg(long)]
        full: bool,
    },
    /// List the registered gates and their enforcement levels.
    List,
    /// Re-render the last run's results.
    Report {
        /// Output shape.
        #[arg(long, value_enum, default_value_t = Format::Terminal)]
        format: Format,
    },
}

/// How a report is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    /// Aligned plain text for a terminal.
    Terminal,
    /// A markdown table, for a CI step summary.
    Markdown,
}

/// Runs one subcommand, writing its output to `out`.
///
/// Returns whether the run is green, so the caller decides the exit code.
///
/// # Errors
///
/// Returns an I/O error if output, logs or results cannot be written, or if a
/// gate selection names a gate that does not exist.
pub fn dispatch(
    command: &Command,
    root: &Path,
    runner: &dyn CommandRunner,
    out: &mut dyn Write,
) -> io::Result<bool> {
    let gates = registry();
    match command {
        Command::List => {
            write!(out, "{}", render_list(&gates))?;
            Ok(true)
        }
        Command::Run {
            gates: names,
            packages,
            full,
        } => {
            let selected = resolve(&gates, names).map_err(io::Error::other)?;
            let ctx = GateContext::new(root, packages.clone()).measuring_everything(*full);
            let results = run(&selected, &ctx, runner)?;
            write!(out, "{}", render_table(&results))?;
            Ok(results.iter().all(|result| !result.reds_run()))
        }
        Command::Report { format } => {
            let ctx = GateContext::new(root, Vec::new());
            let results = load_results(&ctx.log_dir)?;
            let rendered = match format {
                Format::Terminal => render_table(&results),
                Format::Markdown => render_markdown(&results),
            };
            write!(out, "{rendered}")?;
            Ok(results.iter().all(|result| !result.reds_run()))
        }
    }
}

/// Finds the cargo workspace root at or above `start`.
///
/// A gate that measured the wrong tree is worse than one that refused to run,
/// so this looks for the manifest that declares `[workspace]` rather than the
/// nearest `Cargo.toml`.
pub fn workspace_root(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .find(|directory| declares_workspace(&directory.join("Cargo.toml")))
        .map(Path::to_path_buf)
}

fn declares_workspace(manifest: &Path) -> bool {
    match std::fs::read_to_string(manifest) {
        Err(_) => false,
        Ok(body) => body.lines().any(|line| line.trim_end() == "[workspace]"),
    }
}

/// The binary's entry point.
///
/// Everything except reading the process's arguments and working directory
/// lives in [`run_in`], which tests drive directly.
pub fn main() -> ExitCode {
    let cli = Cli::parse();
    match std::env::current_dir() {
        Ok(cwd) => run_in(&cwd, &cli.command, &ProcessRunner, &mut io::stdout()),
        Err(error) => fail(&format!("could not read the current directory: {error}")),
    }
}

/// Locates the workspace above `cwd`, runs `command`, and turns the verdict
/// into an exit code.
pub fn run_in(
    cwd: &Path,
    command: &Command,
    runner: &dyn CommandRunner,
    out: &mut dyn Write,
) -> ExitCode {
    let Some(root) = workspace_root(cwd) else {
        return fail("no Cargo.toml declaring [workspace] at or above the current directory");
    };
    match dispatch(command, &root, runner, out) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => fail(&error.to_string()),
    }
}

fn fail(message: &str) -> ExitCode {
    let _ = writeln!(io::stderr(), "gauntlet: {message}");
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::{Enforcement, GateOutcome, GateResult};
    use crate::runner::save_results;
    use crate::testing::ScriptedRunner;

    fn render(command: &Command, root: &Path) -> (bool, String) {
        let mut out: Vec<u8> = Vec::new();
        let runner = ScriptedRunner::failing("no tools in this test");
        let green = dispatch(command, root, &runner, &mut out).expect("io");
        (green, String::from_utf8(out).expect("utf-8"))
    }

    fn result(name: &str, outcome: GateOutcome) -> GateResult {
        GateResult {
            name: name.to_owned(),
            enforcement: Enforcement::Blocking,
            outcome: Some(outcome),
            millis: Some(10),
        }
    }

    #[test]
    fn list_prints_every_registered_gate() {
        let (green, listing) = render(&Command::List, Path::new("/repo"));
        assert!(green);
        assert_eq!(listing.lines().count(), registry().len());
        assert!(listing.contains("module-structure"), "{listing}");
    }

    #[test]
    fn an_unknown_gate_name_is_an_error_rather_than_an_empty_green_run() {
        let mut out: Vec<u8> = Vec::new();
        let runner = ScriptedRunner::failing("no tools");
        let command = Command::Run {
            gates: vec!["nope".to_owned()],
            packages: Vec::new(),
            full: false,
        };
        let error = dispatch(&command, Path::new("/repo"), &runner, &mut out)
            .expect_err("the gate does not exist");
        assert!(error.to_string().contains("unknown gate `nope`"), "{error}");
    }

    #[test]
    fn a_run_whose_tools_are_all_missing_is_red_and_every_gate_errors() {
        let dir = tempfile::tempdir().expect("temp dir");
        let command = Command::Run {
            gates: vec!["fmt".to_owned()],
            packages: Vec::new(),
            full: false,
        };
        let (green, table) = render(&command, dir.path());
        assert!(!green, "{table}");
        assert!(table.contains("error"), "{table}");
        assert!(table.contains("1 of 1 gates red: fmt"), "{table}");
    }

    #[test]
    fn report_re_renders_the_last_run_as_a_table() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = GateContext::new(dir.path(), Vec::new());
        save_results(&ctx.log_dir, &[result("fmt", GateOutcome::pass("clean"))]).expect("io");
        let (green, table) = render(
            &Command::Report {
                format: Format::Terminal,
            },
            dir.path(),
        );
        assert!(green, "{table}");
        assert!(table.contains("1 gates green"), "{table}");
        assert!(
            !table.contains('|'),
            "the terminal format is not markdown: {table}"
        );
    }

    #[test]
    fn report_can_re_render_the_last_run_as_markdown() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = GateContext::new(dir.path(), Vec::new());
        save_results(
            &ctx.log_dir,
            &[result("crap", GateOutcome::fail("two over"))],
        )
        .expect("io");
        let (green, markdown) = render(
            &Command::Report {
                format: Format::Markdown,
            },
            dir.path(),
        );
        assert!(!green, "{markdown}");
        assert!(markdown.starts_with("| gate |"), "{markdown}");
    }

    #[test]
    fn reporting_before_any_run_is_an_error_not_a_green_empty_table() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut out: Vec<u8> = Vec::new();
        let runner = ScriptedRunner::failing("no tools");
        let command = Command::Report {
            format: Format::Terminal,
        };
        assert!(dispatch(&command, dir.path(), &runner, &mut out).is_err());
    }

    #[test]
    fn the_workspace_root_is_the_manifest_that_declares_a_workspace() {
        let dir = tempfile::tempdir().expect("temp dir");
        let nested = dir.path().join("crates").join("thing").join("src");
        std::fs::create_dir_all(&nested).expect("dirs");
        std::fs::write(dir.path().join("Cargo.toml"), "[workspace]\nmembers = []\n").expect("root");
        std::fs::write(
            dir.path().join("crates").join("thing").join("Cargo.toml"),
            "[package]\nname = \"thing\"\n",
        )
        .expect("member");

        assert_eq!(
            workspace_root(&nested).map(|root| std::fs::canonicalize(root).expect("exists")),
            Some(std::fs::canonicalize(dir.path()).expect("exists"))
        );
    }

    #[test]
    fn a_tree_with_no_workspace_manifest_has_no_root() {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("Cargo.toml"), "[package]\nname = \"x\"\n").expect("write");
        assert_eq!(workspace_root(&dir.path().join("does-not-exist")), None);
    }

    /// Runs `command` under a throwaway root, optionally a real workspace.
    fn exit_code(command: &Command, with_manifest: bool) -> (ExitCode, Vec<u8>) {
        let dir = tempfile::tempdir().expect("temp dir");
        if with_manifest {
            std::fs::write(dir.path().join("Cargo.toml"), "[workspace]\n").expect("root");
        }
        let mut out: Vec<u8> = Vec::new();
        let runner = ScriptedRunner::failing("no tools in this test");
        let code = run_in(dir.path(), command, &runner, &mut out);
        (code, out)
    }

    #[test]
    fn a_green_run_exits_successfully() {
        assert_eq!(exit_code(&Command::List, true).0, ExitCode::SUCCESS);
    }

    #[test]
    fn a_red_run_exits_with_a_failure_code() {
        let command = Command::Run {
            gates: vec!["fmt".to_owned()],
            packages: Vec::new(),
            full: false,
        };
        assert_eq!(exit_code(&command, true).0, ExitCode::FAILURE);
    }

    #[test]
    fn running_outside_a_workspace_fails_rather_than_measuring_the_wrong_tree() {
        let (code, out) = exit_code(&Command::List, false);
        assert_eq!(code, ExitCode::FAILURE);
        assert!(out.is_empty(), "nothing should have been measured");
    }

    #[test]
    fn a_reporting_error_exits_with_a_failure_code() {
        let command = Command::Report {
            format: Format::Terminal,
        };
        assert_eq!(exit_code(&command, true).0, ExitCode::FAILURE);
    }

    #[test]
    fn the_cli_parses_a_run_with_gates_and_packages() {
        let cli = Cli::try_parse_from(["gauntlet", "run", "crap", "-p", "horde-gauntlet"])
            .expect("valid arguments");
        let Command::Run {
            gates,
            packages,
            full,
        } = cli.command
        else {
            panic!("expected a run");
        };
        assert_eq!(gates, vec!["crap".to_owned()]);
        assert_eq!(packages, vec!["horde-gauntlet".to_owned()]);
        assert!(!full, "the change under test is the default scope");
    }

    #[test]
    fn the_cli_defaults_a_report_to_the_terminal_format() {
        let cli = Cli::try_parse_from(["gauntlet", "report"]).expect("valid arguments");
        let Command::Report { format } = cli.command else {
            panic!("expected a report");
        };
        assert_eq!(format, Format::Terminal);
    }

    #[test]
    fn the_cli_rejects_an_unknown_subcommand() {
        assert!(Cli::try_parse_from(["gauntlet", "sprint"]).is_err());
    }

    #[test]
    fn the_cli_definition_is_internally_consistent() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }
}
