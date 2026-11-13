//! Which codex releases this adapter drives, and which per-kind claims each
//! one earns.
//!
//! Measured, not assumed:
//!
//! * **Exec approvals** — under the granular approval policy this adapter
//!   always sends, `item/commandExecution/requestApproval` reaches the client
//!   at codex-cli **0.147.0** (transcript 33), **0.148.0** (25) and
//!   **0.167.0** (34–35, 40), while **0.145.0** routed nothing in six
//!   configurations (ADR-0005) and 0.147 `on-request` parks (32).
//! * **File-change approvals** — park forever at 0.148 (28, 31); route at
//!   **0.167.0** and are honoured both ways (36–37, W12c).
//!
//! [`SUPPORT`] therefore refuses anything below 0.147 — without exec routing
//! the chain cannot gate the agent's shell at all, and the protocol shapes
//! below it were never recorded against the granular policy — and calls
//! 0.147.0–0.167.0 supported. Each claim holds only once the running binary
//! has said its version: unreadable output earns nothing.

use horde_adapter::{HarnessVersion, SupportTable, VersionReport};

/// codex's measured range.
pub const SUPPORT: SupportTable = SupportTable {
    provider: crate::PROVIDER,
    floor: FIRST_EXEC_ROUTING,
    tested_from: FIRST_EXEC_ROUTING,
    tested_through: HarnessVersion::new(0, 167, 0),
    floor_reason: "exec approvals route to the client from 0.147 (transcripts 25/33); below \
                   it the chain cannot gate the agent's shell",
};

/// The first release whose granular policy routes exec approvals.
pub const FIRST_EXEC_ROUTING: HarnessVersion = HarnessVersion::new(0, 147, 0);

/// The first release measured routing file-change approvals (transcripts
/// 36–37). Somewhere between 0.148 (parks) and here it changed; the gate sits
/// at the first measured version, not a guessed earlier one.
pub const FIRST_FILE_CHANGE_ROUTING: HarnessVersion = HarnessVersion::new(0, 167, 0);

/// Whether `--version` output names a codex that routes exec approvals.
/// No version found is `false` — fail closed.
#[must_use]
pub fn routes_exec_approvals(version_output: &str) -> bool {
    SUPPORT.classify(version_output).at_least(FIRST_EXEC_ROUTING)
}

/// Whether a probed binary routes file-change approvals.
#[must_use]
pub fn routes_file_change_approvals(report: &VersionReport) -> bool {
    report.at_least(FIRST_FILE_CHANGE_ROUTING)
}

#[cfg(test)]
mod tests {
    use horde_adapter::SupportVerdict;

    use super::{SUPPORT, routes_exec_approvals, routes_file_change_approvals};

    /// The measured boundary, both sides of it, in the CLI's own format.
    #[test]
    fn the_measured_versions_land_on_the_right_sides_of_the_exec_gate() {
        assert!(routes_exec_approvals("codex-cli 0.167.0"), "transcript 34");
        assert!(routes_exec_approvals("codex-cli 0.148.0"), "transcript 25");
        assert!(routes_exec_approvals("codex-cli 0.147.0"), "transcript 33");
        assert!(
            !routes_exec_approvals("codex-cli 0.145.0"),
            "the six-configuration null result (ADR-0005)"
        );
        assert!(!routes_exec_approvals("codex-cli 0.146.9"));
        assert!(routes_exec_approvals("codex-cli 1.0.0"), "later majors keep it");
        for output in ["", "command not found", "codex-cli", "v.next"] {
            assert!(!routes_exec_approvals(output), "{output:?} must fail closed");
        }
    }

    #[test]
    fn file_changes_route_only_from_the_first_measured_version() {
        assert!(routes_file_change_approvals(
            &SUPPORT.classify("codex-cli 0.167.0")
        ));
        assert!(routes_file_change_approvals(
            &SUPPORT.classify("codex-cli 0.170.2")
        ));
        assert!(
            !routes_file_change_approvals(&SUPPORT.classify("codex-cli 0.166.9")),
            "unmeasured below 0.167; 0.148 parks (transcripts 28/31)"
        );
        assert!(!routes_file_change_approvals(&SUPPORT.classify("codex-cli")));
    }

    #[test]
    fn the_support_table_matches_the_recordings() {
        let verdict = |output: &str| SUPPORT.classify(output).verdict;
        assert_eq!(verdict("codex-cli 0.146.0"), SupportVerdict::Refused);
        assert_eq!(verdict("codex-cli 0.147.0"), SupportVerdict::Supported);
        assert_eq!(verdict("codex-cli 0.167.0"), SupportVerdict::Supported);
        assert_eq!(verdict("codex-cli 0.168.0"), SupportVerdict::Untested);
        assert_eq!(verdict("codex-cli ?"), SupportVerdict::Untested);
        assert_eq!(SUPPORT.provider, "codex");
    }
}
