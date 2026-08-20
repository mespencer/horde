//! Whether the launched codex routes exec approvals — the version gate on
//! [`Capability::PreAuthorizeExecCommands`].
//!
//! Measured, not assumed (re-probe transcripts 20–33): under the granular
//! approval policy this adapter always sends, `item/commandExecution/
//! requestApproval` reaches the client at codex-cli **0.147.0** (transcript
//! 33) and **0.148.0** (transcript 25), while **0.145.0** routed nothing in
//! six configurations (ADR-0005) and the 0.147 `on-request` control shows the
//! boundary is real per policy (transcript 32). The claim therefore holds from
//! 0.147 up, and only after the running binary has actually said so — an
//! unprobed or unparseable version fails closed to the sandbox-only claim,
//! because advertising interception a CLI will never perform leaves the chain
//! silently doing nothing.
//!
//! [`Capability::PreAuthorizeExecCommands`]: horde_adapter::Capability::PreAuthorizeExecCommands

/// The first release whose granular policy routes exec approvals.
const FIRST_ROUTING_VERSION: (u64, u64, u64) = (0, 147, 0);

/// Whether `--version` output names a codex that routes exec approvals.
///
/// Accepts the CLI's own format (`codex-cli 0.148.0`) by scanning for the
/// first dotted version anywhere in the output, so a future banner line does
/// not break the probe. No version found is `false` — fail closed.
#[must_use]
pub fn routes_exec_approvals(version_output: &str) -> bool {
    parse_first_version(version_output).is_some_and(|version| version >= FIRST_ROUTING_VERSION)
}

/// The first `major.minor.patch` (patch optional) in `text`, as numbers.
fn parse_first_version(text: &str) -> Option<(u64, u64, u64)> {
    text.split(|c: char| c.is_whitespace() || c == '(' || c == ')')
        .find_map(parse_version_token)
}

/// One token as a dotted version, or `None`.
fn parse_version_token(token: &str) -> Option<(u64, u64, u64)> {
    let mut parts = token.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    // Patch is optional ("0.147"), and trailing junk ("0.147.0-rc1") is not a
    // version this probe trusts: an unreadable component fails the token, and
    // the scan moves on.
    let patch = match parts.next() {
        None => 0,
        Some(part) => part.parse().ok()?,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::{parse_first_version, routes_exec_approvals};

    /// The measured boundary, both sides of it, in the CLI's own format.
    #[test]
    fn the_measured_versions_land_on_the_right_sides_of_the_gate() {
        assert!(routes_exec_approvals("codex-cli 0.148.0"), "transcript 25");
        assert!(routes_exec_approvals("codex-cli 0.147.0"), "transcript 33");
        assert!(
            !routes_exec_approvals("codex-cli 0.145.0"),
            "the six-configuration null result (ADR-0005)"
        );
        assert!(!routes_exec_approvals("codex-cli 0.146.9"));
        assert!(
            routes_exec_approvals("codex-cli 1.0.0"),
            "later majors keep it"
        );
    }

    /// Fail closed: silence, prose, and unreadable versions all claim nothing.
    #[test]
    fn unparseable_output_never_claims_interception() {
        for output in ["", "command not found", "codex-cli", "v.next", "0.x.1"] {
            assert!(
                !routes_exec_approvals(output),
                "{output:?} must fail closed"
            );
        }
    }

    /// The scanner's own corners: the version may sit anywhere in the output,
    /// a two-part version reads as patch zero, and suffixed tokens are not
    /// trusted as versions.
    #[test]
    fn the_first_dotted_version_is_found_wherever_it_sits() {
        assert_eq!(
            parse_first_version("codex-cli 0.148.0 (homebrew)"),
            Some((0, 148, 0))
        );
        assert_eq!(parse_first_version("0.147"), Some((0, 147, 0)));
        assert_eq!(
            parse_first_version("codex 0.147.0-rc1 0.146.0"),
            Some((0, 146, 0)),
            "a suffixed token is skipped, not misread"
        );
        assert_eq!(parse_first_version("1.2.3.4"), None, "too many parts");
    }

    /// Each split delimiter must work alone: a version wrapped in parens with
    /// no adjoining whitespace is still found. This is the corner that
    /// separates `||` from `&&` in the tokenizer — a character is a delimiter
    /// if it is *any* of them, never all of them at once.
    #[test]
    fn a_parenthesized_version_is_still_found() {
        assert_eq!(parse_first_version("codex(0.147.0)"), Some((0, 147, 0)));
        assert_eq!(parse_first_version("(0.148.0"), Some((0, 148, 0)));
        assert_eq!(parse_first_version("0.149.0)"), Some((0, 149, 0)));
        assert!(routes_exec_approvals("codex(0.147.0)"));
    }
}
