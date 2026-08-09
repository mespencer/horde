//! Pinned external tools.
//!
//! An unpinned tool silently moves a calibration baseline (docs/TESTING.md §1),
//! so every external measurement tool declares the exact version the thresholds
//! were chosen against. A mismatch is an [`GateOutcome::Error`], never a `fail`:
//! the instrument is wrong, not the code.

use std::path::Path;

use crate::command::{CommandRunner, CommandSpec};

/// An external tool at a known version, with the command that installs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolPin {
    /// Program to invoke, e.g. `"cargo"`.
    pub program: &'static str,
    /// Arguments that make it print its version.
    pub version_args: &'static [&'static str],
    /// The exact version the gate's thresholds were calibrated against.
    pub expected: &'static str,
    /// Copy-pasteable install command for the pinned version.
    pub install: &'static str,
}

/// Pulls the first `major.minor.patch` token out of a tool's version banner.
///
/// Tools are inconsistent — `cargo-machete` prints a bare `0.9.2`, `cargo-nextest`
/// prints `cargo-nextest 0.9.82 (351e914a2 2024-10-28)` — so the parser looks
/// for the shape rather than a position.
pub fn extract_version(text: &str) -> Option<&str> {
    text.split_whitespace()
        .find(|token| is_version_triple(token))
}

fn is_version_triple(token: &str) -> bool {
    let mut parts = token.split('.');
    let triple = [parts.next(), parts.next(), parts.next()];
    if parts.next().is_some() {
        return false;
    }
    triple.iter().all(|part| match part {
        Some(digits) => !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    })
}

/// Whether the tool is absent rather than merely unhappy.
///
/// `cargo <subcommand>` for an uninstalled subcommand exits non-zero with
/// `no such command`, which a gate may legitimately treat as "fall back",
/// while any other failure is an instrument error.
pub fn is_missing_cargo_subcommand(text: &str) -> bool {
    text.contains("no such command")
}

/// Confirms `pin` is installed at exactly the pinned version.
///
/// # Errors
///
/// Returns a message that names the mismatch and the install command whenever
/// the tool is missing, unrunnable, or at a different version.
pub fn check_pin(runner: &dyn CommandRunner, pin: &ToolPin, cwd: &Path) -> Result<(), String> {
    let spec = CommandSpec::new(pin.program, pin.version_args, cwd);
    let output = runner
        .run(&spec)
        .map_err(|err| format!("{err}\ninstall with: {}", pin.install))?;
    if !output.success() {
        return Err(format!(
            "`{}` failed: {}\ninstall with: {}",
            spec.display(),
            output.combined().trim(),
            pin.install
        ));
    }
    match extract_version(&output.combined()) {
        Some(found) if found == pin.expected => Ok(()),
        Some(found) => Err(format!(
            "`{}` is at {found}, gate is calibrated against {}\ninstall with: {}",
            spec.display(),
            pin.expected,
            pin.install
        )),
        None => Err(format!(
            "could not read a version from `{}`: {}\ninstall with: {}",
            spec.display(),
            output.combined().trim(),
            pin.install
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandOutput;
    use crate::testing::ScriptedRunner;

    const PIN: ToolPin = ToolPin {
        program: "cargo",
        version_args: &["crap", "--version"],
        expected: "0.3.1",
        install: "cargo install --locked cargo-crap@0.3.1",
    };

    #[test]
    fn version_is_read_from_a_bare_number() {
        assert_eq!(extract_version("0.9.2\n"), Some("0.9.2"));
    }

    #[test]
    fn version_is_read_past_a_leading_program_name() {
        assert_eq!(extract_version("cargo-crap 0.3.1"), Some("0.3.1"));
    }

    #[test]
    fn version_ignores_trailing_build_metadata() {
        assert_eq!(
            extract_version("cargo-nextest 0.9.82 (351e914a2 2024-10-28)"),
            Some("0.9.82")
        );
    }

    #[test]
    fn two_component_versions_are_not_triples() {
        assert_eq!(extract_version("tool 1.2"), None);
    }

    #[test]
    fn four_component_versions_are_not_triples() {
        assert_eq!(extract_version("tool 1.2.3.4"), None);
    }

    #[test]
    fn non_numeric_components_are_not_triples() {
        assert_eq!(extract_version("tool 1.2.x"), None);
        assert_eq!(extract_version("tool 1..3"), None);
    }

    #[test]
    fn text_without_any_version_yields_none() {
        assert_eq!(extract_version("error: no such command: `crap`"), None);
    }

    #[test]
    fn missing_cargo_subcommand_is_recognised() {
        assert!(is_missing_cargo_subcommand(
            "error: no such command: `nextest`"
        ));
        assert!(!is_missing_cargo_subcommand("cargo-nextest 0.9.82"));
    }

    #[test]
    fn matching_version_passes_the_pin_check() {
        let runner = ScriptedRunner::new(vec![CommandOutput::new(Some(0), "cargo-crap 0.3.1", "")]);
        assert_eq!(check_pin(&runner, &PIN, Path::new("/repo")), Ok(()));
        assert_eq!(runner.calls()[0].display(), "cargo crap --version");
    }

    #[test]
    fn mismatched_version_names_both_versions_and_the_install_command() {
        let runner = ScriptedRunner::new(vec![CommandOutput::new(Some(0), "cargo-crap 0.4.0", "")]);
        let message = check_pin(&runner, &PIN, Path::new("/repo")).expect_err("version differs");
        assert!(message.contains("0.4.0"), "{message}");
        assert!(message.contains("0.3.1"), "{message}");
        assert!(
            message.contains("cargo install --locked cargo-crap@0.3.1"),
            "{message}"
        );
    }

    #[test]
    fn unparseable_version_output_is_an_error_not_a_pass() {
        let runner = ScriptedRunner::new(vec![CommandOutput::new(Some(0), "who knows", "")]);
        let message = check_pin(&runner, &PIN, Path::new("/repo")).expect_err("no version");
        assert!(message.contains("could not read a version"), "{message}");
    }

    #[test]
    fn nonzero_exit_from_the_version_probe_is_an_error() {
        let runner = ScriptedRunner::new(vec![CommandOutput::new(
            Some(101),
            "",
            "error: no such command: `crap`",
        )]);
        let message = check_pin(&runner, &PIN, Path::new("/repo")).expect_err("probe failed");
        assert!(message.contains("no such command"), "{message}");
        assert!(message.contains("cargo install"), "{message}");
    }

    #[test]
    fn unlaunchable_tool_reports_the_install_command() {
        let runner = ScriptedRunner::failing("could not launch `cargo crap --version`");
        let message = check_pin(&runner, &PIN, Path::new("/repo")).expect_err("cannot launch");
        assert!(message.contains("could not launch"), "{message}");
        assert!(message.contains("cargo install"), "{message}");
    }
}
