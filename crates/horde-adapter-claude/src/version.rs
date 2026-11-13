//! Which `claude` releases this adapter drives, and which permission route
//! each one gets.
//!
//! Measured, not assumed:
//!
//! * **2.1.226 / 2.1.228** — spike W1c and the 2026-08-11 re-verification:
//!   stream-json, `--resume`, the interrupt control message, and the MCP
//!   permission-tool route (transcripts `01`–`11`).
//! * **2.1.323** — the W12c re-probe (2026-11-13): the same protocol plus the
//!   stdio permission route, `can_use_tool` control requests answered on
//!   stdin, `AskUserQuestion` answers (transcripts `12a`–`17`).
//!
//! [`SUPPORT`] refuses anything older than the oldest recording — nothing
//! there was ever measured, and the isolation flags ADR-0010 depends on were
//! verified from 2.1.226 up — and calls 2.1.226–2.1.323 supported. `claude`
//! ships several releases a week, so a newer binary is the common case: it
//! runs, as untested, with a warning on the record.

use horde_adapter::{HarnessVersion, SupportTable, VersionReport};

/// claude's measured range.
pub const SUPPORT: SupportTable = SupportTable {
    provider: crate::PROVIDER,
    floor: HarnessVersion::new(2, 1, 226),
    tested_from: HarnessVersion::new(2, 1, 226),
    tested_through: HarnessVersion::new(2, 1, 323),
    floor_reason: "the oldest release Horde's transcripts cover (spike W1c); the isolation \
                   flags ADR-0010 depends on were verified from there",
};

/// The first release the stdio permission route was measured on.
pub const STDIO_ROUTE_FROM: HarnessVersion = HarnessVersion::new(2, 1, 323);

/// Which channel carries a launch's permission prompts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionRoute {
    /// `--permission-prompt-tool stdio`: `can_use_tool` control requests on
    /// stdout, answered on stdin. One channel; questions and answers ride it.
    Stdio,
    /// `--permission-prompt-tool mcp__horde__approve`: the harness calls the
    /// approve tool on the agent's Horde endpoint (the W1c route). The
    /// fallback, for binaries the stdio route was never measured on.
    McpTool,
}

impl PermissionRoute {
    /// The route a probed binary gets: stdio where it was measured (2.1.323)
    /// and beyond, the MCP tool where only that was measured. An unreadable
    /// version takes stdio — it is far likelier a newer banner than a binary
    /// older than the floor — and its untested verdict is journaled anyway.
    #[must_use]
    pub fn for_version(report: &VersionReport) -> Self {
        match report.version {
            Some(version) if version < STDIO_ROUTE_FROM => Self::McpTool,
            _ => Self::Stdio,
        }
    }
}

#[cfg(test)]
mod tests {
    use horde_adapter::SupportVerdict;

    use super::{PermissionRoute, SUPPORT};

    #[test]
    fn the_support_table_matches_the_recordings() {
        let verdict = |output: &str| SUPPORT.classify(output).verdict;
        assert_eq!(verdict("2.1.225 (Claude Code)"), SupportVerdict::Refused);
        assert_eq!(verdict("2.1.226 (Claude Code)"), SupportVerdict::Supported);
        assert_eq!(verdict("2.1.323 (Claude Code)"), SupportVerdict::Supported);
        assert_eq!(verdict("2.1.324 (Claude Code)"), SupportVerdict::Untested);
        assert_eq!(verdict("2.2.0 (Claude Code)"), SupportVerdict::Untested);
        assert_eq!(verdict("Claude Code"), SupportVerdict::Untested);
        assert_eq!(SUPPORT.provider, "claude");
    }

    #[test]
    fn the_route_follows_where_each_was_measured() {
        let route = |output: &str| PermissionRoute::for_version(&SUPPORT.classify(output));
        assert_eq!(route("2.1.228 (Claude Code)"), PermissionRoute::McpTool);
        assert_eq!(route("2.1.322 (Claude Code)"), PermissionRoute::McpTool);
        assert_eq!(route("2.1.323 (Claude Code)"), PermissionRoute::Stdio);
        assert_eq!(route("2.3.0 (Claude Code)"), PermissionRoute::Stdio);
        assert_eq!(route("banner"), PermissionRoute::Stdio);
    }
}
