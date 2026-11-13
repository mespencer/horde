//! Windows lints: `cargo xwin clippy … --target x86_64-pc-windows-msvc --
//! -D warnings` (ADR-0017).
//!
//! Every crate builds for Windows — the Worker above all, so a Windows
//! machine can run `claude.exe` and `codex.exe` agents — and the platform
//! seams (`cfg(unix)` / `cfg(windows)`) are exactly where code rots unseen
//! on a Linux-only gauntlet: a Windows branch nobody compiles stops
//! compiling. This gate compiles and lints every target for Windows from
//! Linux, with the same arguments, `-D warnings` and deny-list as `clippy`
//! (one builder, [`lint_args`], makes both commands).
//!
//! `cargo-xwin` supplies what a cross-check needs and the host lacks: the
//! MSVC CRT and Windows SDK headers (downloaded once, to its cache) for the
//! C that `ring` and `rusqlite` compile, driven through the host's `clang`.
//! It checks; it does not run tests — Windows tests run on a Windows
//! machine (ADR-0017 lists them).

use crate::gates::clippy::{lint_args, outcome_of};
use crate::gates::prelude::*;

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
    CommandSpec::owned(
        "cargo",
        lint_args(&["xwin"], &["--target", TARGET], ctx),
        &ctx.workspace_root,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{failed, repo_ctx};

    const DIRTY: &str = include_str!("../../fixtures/cargo-xwin-clippy-denied.txt");

    /// cargo-xwin is an install, so it carries a pin; clippy itself rides
    /// the toolchain's.
    #[test]
    fn the_gate_is_pinned_to_cargo_xwin() {
        let wiring = gate();
        assert_eq!(wiring.pin.map(|pin| pin.expected), Some("0.23.1"));
        assert_eq!(wiring.pin.map(|pin| pin.program), Some("cargo-xwin"));
    }

    /// The `clippy` command, run through cargo-xwin for the Windows target;
    /// a narrowed run names its packages instead of the workspace.
    #[test]
    fn the_command_is_clippys_through_xwin_for_the_windows_target() {
        assert_eq!(
            spec(&repo_ctx()).display(),
            "cargo xwin clippy --all-targets --all-features --workspace \
             --target x86_64-pc-windows-msvc -- -D warnings"
        );
        let narrowed = GateContext::new(std::path::Path::new("/repo"), vec!["a".to_owned()]);
        let display = spec(&narrowed).display();
        assert!(display.contains(" -p a --target "), "{display}");
        assert!(!display.contains("--workspace"), "{display}");
    }

    /// A real Windows cross-check that failed — a Unix-only API reached
    /// from code Windows compiles — is a failure with its diagnostic count,
    /// read by the wired reader.
    #[test]
    fn real_cross_check_output_is_read_as_a_failure_with_a_diagnostic_count() {
        let Reader::AnyExit(read) = gate().read else {
            panic!("clippy reports lints through a non-zero exit");
        };
        assert_eq!(
            read(&spec(&repo_ctx()), &failed(101, DIRTY)),
            GateOutcome::fail("2 clippy/rustc diagnostic(s); see the gate log")
        );
    }
}
