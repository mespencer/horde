//! Formatting: `cargo fmt --check`.

use crate::gates::prelude::*;
use crate::gates::unexpected_exit;

/// Checks that every tracked source file is already rustfmt-clean.
pub fn gate() -> CommandGate {
    CommandGate {
        pin: None,
        build: spec,
        read: Reader::AnyExit(outcome_of),
    }
}

/// The command this gate runs.
///
/// `cargo fmt` takes `--all` rather than `--workspace`, so package narrowing is
/// spelled out here instead of reusing the shared helper.
pub fn spec(ctx: &GateContext) -> CommandSpec {
    let mut args = vec!["fmt".to_owned()];
    if ctx.packages.is_empty() {
        args.push("--all".to_owned());
    } else {
        for package in &ctx.packages {
            args.push("-p".to_owned());
            args.push(package.clone());
        }
    }
    args.push("--".to_owned());
    args.push("--check".to_owned());
    CommandSpec::owned("cargo", args, &ctx.workspace_root)
}

/// Reads a `cargo fmt --check` run.
pub fn outcome_of(spec: &CommandSpec, output: &CommandOutput) -> GateOutcome {
    if output.success() {
        return GateOutcome::pass("every file is rustfmt-clean");
    }
    let files = unformatted_files(&output.combined());
    if files.is_empty() {
        return unexpected_exit(spec, output);
    }
    GateOutcome::fail(format!(
        "{} file(s) are not rustfmt-clean\n{}",
        files.len(),
        files.join("\n")
    ))
}

/// The distinct files rustfmt would rewrite.
///
/// rustfmt prints one `Diff in <path>:<line>:` header per hunk, so the same
/// file appears many times; the gate reports files, not hunks. A rustfmt whose
/// header format changed produces no files here, which the caller turns into an
/// instrument error rather than a pass.
pub fn unformatted_files(text: &str) -> Vec<String> {
    let mut files: Vec<String> = text
        .lines()
        .filter_map(diff_header)
        .map(str::to_owned)
        .collect();
    files.sort();
    files.dedup();
    files
}

fn diff_header(line: &str) -> Option<&str> {
    let rest = line.trim().strip_prefix("Diff in ")?;
    let (path, line_number) = rest.strip_suffix(':')?.rsplit_once(':')?;
    line_number.parse::<u32>().ok()?;
    Some(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{exited, failed, repo_ctx};

    const DIRTY: &str = include_str!("../../fixtures/cargo-fmt-check.txt");

    /// The gate wiring itself — that a `CommandGate` checks its pin, runs its
    /// command and routes the output — is covered once in `gates::tests`.
    #[test]
    fn the_gate_is_a_cargo_subcommand_that_reports_through_its_exit_code() {
        let wiring = gate();
        assert!(
            wiring.pin.is_none(),
            "rustfmt is pinned by rust-toolchain.toml, not by an install command"
        );
        assert!(matches!(wiring.read, Reader::AnyExit(_)));
    }

    #[test]
    fn a_whole_workspace_run_uses_all() {
        assert_eq!(spec(&repo_ctx()).display(), "cargo fmt --all -- --check");
        assert_eq!(spec(&repo_ctx()).cwd, std::path::Path::new("/repo"));
    }

    #[test]
    fn a_narrowed_run_names_each_package() {
        let ctx = GateContext::new(
            std::path::Path::new("/repo"),
            vec!["a".to_owned(), "b".to_owned()],
        );
        assert_eq!(spec(&ctx).display(), "cargo fmt -p a -p b -- --check");
    }

    #[test]
    fn a_clean_tree_passes() {
        let outcome = outcome_of(&spec(&repo_ctx()), &exited(0, ""));
        assert_eq!(outcome, GateOutcome::pass("every file is rustfmt-clean"));
    }

    #[test]
    fn real_rustfmt_output_is_read_as_a_failure_listing_each_file_once() {
        let GateOutcome::Fail { detail } = outcome_of(&spec(&repo_ctx()), &exited(1, DIRTY)) else {
            panic!("unformatted code is a verdict about the code, not an instrument error");
        };
        assert!(
            detail.starts_with("2 file(s) are not rustfmt-clean"),
            "{detail}"
        );
        assert!(detail.contains("src/lib.rs"), "{detail}");
        assert!(detail.contains("src/report.rs"), "{detail}");
    }

    #[test]
    fn each_file_is_counted_once_however_many_hunks_it_has() {
        let text = "Diff in src/a.rs:1:\nDiff in src/a.rs:9:\n";
        assert_eq!(unformatted_files(text), vec!["src/a.rs".to_owned()]);
    }

    #[test]
    fn files_are_reported_in_a_stable_order() {
        let text = "Diff in src/z.rs:1:\nDiff in src/a.rs:1:\n";
        assert_eq!(
            unformatted_files(text),
            vec!["src/a.rs".to_owned(), "src/z.rs".to_owned()]
        );
    }

    #[test]
    fn unrelated_output_contributes_no_files() {
        assert_eq!(
            unformatted_files("error: failed to load config"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_header_without_a_line_number_is_not_a_diff_header() {
        assert_eq!(
            unformatted_files("Diff in src/a.rs at line one:"),
            Vec::<String>::new()
        );
        assert_eq!(unformatted_files("Diff in src/a.rs"), Vec::<String>::new());
    }

    #[test]
    fn a_failure_rustfmt_cannot_explain_is_an_instrument_error() {
        let output = failed(1, "error: failed to load config file");
        let GateOutcome::Error { reason } = outcome_of(&spec(&repo_ctx()), &output) else {
            panic!("rustfmt failing to run is not a verdict about the code");
        };
        assert!(reason.contains("failed to load config file"), "{reason}");
    }
}
