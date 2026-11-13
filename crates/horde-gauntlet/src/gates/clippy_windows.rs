//! Windows lints: `cargo xwin clippy --target x86_64-pc-windows-msvc
//! --all-targets --all-features -- -D warnings` (ADR-0017).
//!
//! Every crate builds for Windows — the Worker above all, so a Windows
//! machine can run `claude.exe` and `codex.exe` agents — and the platform
//! seams (`cfg(unix)` / `cfg(windows)`) are exactly where code rots unseen
//! on a Linux-only gauntlet: a Windows branch nobody compiles stops
//! compiling. This gate compiles and lints every target for Windows from
//! Linux, under the same `-D warnings` and the same deny-list as `clippy`.
//!
//! `cargo-xwin` supplies what a cross-check needs and the host lacks: the
//! MSVC CRT and Windows SDK headers (downloaded once, to its cache) for the
//! C that `ring` and `rusqlite` compile, driven through the host's `clang`.
//! It checks; it does not run tests — Windows tests run on a Windows
//! machine (ADR-0017 lists them).

use crate::gates::clippy::outcome_of;
use crate::gates::prelude::*;
use crate::gates::with_packages;

/// The target every Windows check builds for.
pub const TARGET: &str = "x86_64-pc-windows-msvc";

/// The cargo-xwin version the gate is calibrated against.
///
/// Probed as `cargo-xwin --version`, the binary itself, for the reason
/// `machete`'s pin gives: a version probe should not depend on cargo's
/// subcommand dispatch deciding how its arguments are parsed.
pub const PIN: ToolPin = ToolPin {
    program: "cargo-xwin",
    version_args: &["--version"],
    expected: "0.23.1",
    install: "cargo install --locked cargo-xwin@0.23.1",
};

/// Lints every target and feature for Windows, warnings denied.
pub fn gate() -> CommandGate {
    CommandGate {
        pin: Some(&PIN),
        build: spec,
        read: Reader::AnyExit(outcome_of),
    }
}

/// The command this gate runs.
pub fn spec(ctx: &GateContext) -> CommandSpec {
    let mut args = vec!["xwin".to_owned()];
    args.extend(with_packages(
        &["clippy", "--target", TARGET, "--all-targets", "--all-features"],
        ctx,
    ));
    args.push("--".to_owned());
    args.push("-D".to_owned());
    args.push("warnings".to_owned());
    CommandSpec::owned("cargo", args, &ctx.workspace_root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{exited, failed, repo_ctx};

    const DIRTY: &str = include_str!("../../fixtures/cargo-xwin-clippy-denied.txt");

    fn read(output: &CommandOutput) -> GateOutcome {
        (gate_reader())(&spec(&repo_ctx()), output)
    }

    fn gate_reader() -> fn(&CommandSpec, &CommandOutput) -> GateOutcome {
        match gate().read {
            Reader::AnyExit(read) => read,
            Reader::CleanExitOnly(_) => panic!("clippy reports lints through a non-zero exit"),
        }
    }

    /// cargo-xwin is an install, so it carries a pin; clippy itself rides
    /// the toolchain's.
    #[test]
    fn the_gate_is_pinned_to_cargo_xwin() {
        let wiring = gate();
        assert_eq!(wiring.pin.map(|pin| pin.expected), Some("0.23.1"));
        assert_eq!(wiring.pin.map(|pin| pin.program), Some("cargo-xwin"));
    }

    #[test]
    fn the_command_lints_every_target_for_windows_and_denies_warnings() {
        assert_eq!(
            spec(&repo_ctx()).display(),
            "cargo xwin clippy --target x86_64-pc-windows-msvc --all-targets --all-features \
             --workspace -- -D warnings"
        );
    }

    #[test]
    fn a_narrowed_run_replaces_the_workspace_flag_with_packages() {
        let ctx = GateContext::new(std::path::Path::new("/repo"), vec!["a".to_owned()]);
        assert_eq!(
            spec(&ctx).display(),
            "cargo xwin clippy --target x86_64-pc-windows-msvc --all-targets --all-features \
             -p a -- -D warnings"
        );
    }

    #[test]
    fn a_clean_run_passes() {
        assert_eq!(
            read(&exited(0, "Finished")),
            GateOutcome::pass("no clippy or rustc warnings")
        );
    }

    /// A real Windows cross-check that failed — a Unix-only API reached
    /// from code Windows compiles — is a failure with its diagnostic count.
    #[test]
    fn real_cross_check_output_is_read_as_a_failure_with_a_diagnostic_count() {
        assert_eq!(
            read(&failed(101, DIRTY)),
            GateOutcome::fail("2 clippy/rustc diagnostic(s); see the gate log")
        );
    }
}
