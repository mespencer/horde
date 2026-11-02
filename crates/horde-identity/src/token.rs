//! The one-token joining surface (ADR-0007).
//!
//! A join token is `horde://join/<host:port>/<secret>.<fingerprint>`: the
//! enrollment listener, the one-time secret and the pinned server
//! fingerprint in one shell-safe string (path segments only, so nothing
//! needs quoting). The `secret.fingerprint` interior is [`JoinCode`]
//! unchanged — a token is packaging around it, not a new trust artifact.

use crate::fingerprint::JoinCode;

/// The scheme-and-verb prefix every join token starts with.
const PREFIX: &str = "horde://join/";

/// The shape a refusal teaches, verbatim in every parse error.
const EXPECTED: &str = "horde://join/<host:port>/<secret>.<fingerprint>";

/// Why a pasted token was refused.
///
/// One variant per distinct mistake so the error diagnoses the actual
/// mistake, not the paste — the trap ADR-0007 exists to close. Every
/// rendering names the expected form.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    #[error("the join token must start with {PREFIX}…; expected {EXPECTED}")]
    WrongPrefix,

    #[error("the join token names no enrollment listener; expected {EXPECTED}")]
    MissingEndpoint,

    #[error(
        "the join token's enrollment listener {endpoint:?} is not host:port; expected {EXPECTED}"
    )]
    MalformedEndpoint { endpoint: String },

    #[error(
        "the join token's code half is not secret.fingerprint (64 hex digits after the dot); \
         expected {EXPECTED}"
    )]
    MalformedCode,

    /// A link of another kind — an endpoint URL, say — pasted where the
    /// token goes.
    #[error(
        "the join token starts with {found:?}, not {PREFIX}: that is another kind of link (an \
         endpoint URL is not a token); expected {EXPECTED}"
    )]
    WrongScheme { found: String },

    /// The right shape, cut short — the commonest copy-and-paste failure.
    #[error("the join token is truncated: {detail}; copy the whole line. Expected {EXPECTED}")]
    Truncated { detail: String },

    /// Whitespace inside the token: a terminal wrapped the line and the
    /// paste kept the break, or two pieces were pasted apart.
    #[error(
        "the join token has whitespace inside it (at character {at}): a wrapped or split \
         paste; rejoin it on one line. Expected {EXPECTED}"
    )]
    Whitespace { at: usize },
}

/// The hex digits a code half's fingerprint carries when whole.
const FINGERPRINT_DIGITS: usize = 64;

/// One self-contained join token, ready to redeem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinToken {
    /// The enrollment listener as `host:port` — exactly the path segment.
    pub host_port: String,
    /// The unchanged `secret.fingerprint` interior.
    pub code: JoinCode,
}

impl JoinToken {
    /// Packages a minted code with the listener it redeems against.
    #[must_use]
    pub fn new(host_port: impl Into<String>, code: JoinCode) -> Self {
        Self {
            host_port: host_port.into(),
            code,
        }
    }

    /// Reads a token back from its `horde://join/<host:port>/<code>` form.
    ///
    /// Strict on shape for the same reason [`JoinCode::parse`] is: a garbled
    /// token must fail here, naming the expected form, rather than downstream
    /// with a message about trust.
    ///
    /// # Errors
    ///
    /// [`TokenError`], naming which part was wrong and the expected form.
    pub fn parse(token: &str) -> Result<Self, TokenError> {
        let token = token.trim();
        if let Some(at) = token.chars().position(char::is_whitespace) {
            return Err(TokenError::Whitespace { at });
        }
        let rest = token
            .strip_prefix(PREFIX)
            .ok_or_else(|| wrong_start(token))?;
        let (host_port, code) = match rest.split_once('/') {
            Some(split) => split,
            None if is_host_port(rest) => (rest, ""),
            None => return Err(TokenError::MissingEndpoint),
        };
        if !is_host_port(host_port) {
            return Err(TokenError::MalformedEndpoint {
                endpoint: host_port.to_owned(),
            });
        }
        let code = JoinCode::parse(code).ok_or_else(|| code_refusal(code))?;
        Ok(Self {
            host_port: host_port.to_owned(),
            code,
        })
    }

    /// The `https://host:port` URI the join dials. Always TLS: the
    /// enrollment listener has no plaintext mode, which is why the token
    /// carries no scheme for anyone to mistype.
    #[must_use]
    pub fn enrollment_endpoint(&self) -> String {
        format!("https://{}", self.host_port)
    }

    /// The host half alone — what a control-plane endpoint learned at join
    /// time sits on (the token's listener and the control plane share a
    /// machine, ports apart).
    #[must_use]
    pub fn host(&self) -> &str {
        self.host_port
            .rsplit_once(':')
            .map_or(self.host_port.as_str(), |(host, _)| host)
    }
}

/// The refusal for a token that does not start with [`PREFIX`]: a link of
/// another scheme names its scheme; anything else gets the plain prefix
/// refusal.
fn wrong_start(token: &str) -> TokenError {
    match token.split_once("://") {
        Some((scheme, _)) if !scheme.is_empty() => TokenError::WrongScheme {
            found: format!("{scheme}://"),
        },
        _ => TokenError::WrongPrefix,
    }
}

/// Why a code half did not parse: missing or cut short (a hex fingerprint
/// with too few digits) is truncation; anything else is malformed.
fn code_refusal(code: &str) -> TokenError {
    if code.is_empty() {
        return TokenError::Truncated {
            detail: "it ends after the enrollment listener, with no code half".to_owned(),
        };
    }
    match code.split_once('.') {
        Some((_, fingerprint))
            if fingerprint.len() < FINGERPRINT_DIGITS
                && fingerprint.chars().all(|c| c.is_ascii_hexdigit()) =>
        {
            TokenError::Truncated {
                detail: format!(
                    "its fingerprint has {} of {FINGERPRINT_DIGITS} hex digits",
                    fingerprint.len()
                ),
            }
        }
        _ => TokenError::MalformedCode,
    }
}

/// Who can reach a host a token or endpoint names (ADR-0007, 2026-10-31
/// amendment) — the fact minting checks before it hands a token to a
/// machine that may not be this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostScope {
    /// `0.0.0.0` or `::` — a bind address, never a dial target.
    Unspecified,
    /// `localhost` or a loopback address: this machine only.
    Loopback,
    /// Anything else: a name or address other machines may dial.
    Routable,
}

/// The scope of one host, bracketed IPv6 included.
#[must_use]
pub fn host_scope(host: &str) -> HostScope {
    let bare = host
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(host);
    match bare.parse::<std::net::IpAddr>() {
        Ok(ip) if ip.is_unspecified() => HostScope::Unspecified,
        Ok(ip) if ip.is_loopback() => HostScope::Loopback,
        Ok(_) => HostScope::Routable,
        Err(_) if bare.eq_ignore_ascii_case("localhost") => HostScope::Loopback,
        Err(_) => HostScope::Routable,
    }
}

impl JoinToken {
    /// Who can redeem this token, judged by the listener host it names.
    #[must_use]
    pub fn scope(&self) -> HostScope {
        host_scope(self.host())
    }
}

impl std::fmt::Display for JoinToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{PREFIX}{}/{}", self.host_port, self.code)
    }
}

/// Whether a path segment is a usable `host:port`: a non-empty host and a
/// real TCP port, nothing else. Port 0 is refused — a token is minted from a
/// bound listener, and 0 names none.
fn is_host_port(candidate: &str) -> bool {
    match candidate.rsplit_once(':') {
        Some((host, port)) => !host.is_empty() && port.parse::<u16>().is_ok_and(|port| port != 0),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{JoinCode, JoinToken, TokenError};

    fn code() -> JoinCode {
        JoinCode {
            secret: "00ff".to_owned(),
            server_fingerprint: "a".repeat(64),
        }
    }

    #[test]
    fn a_token_round_trips_through_display() {
        let token = JoinToken::new("horde.example:7778", code());
        assert_eq!(
            token.to_string(),
            format!("horde://join/horde.example:7778/00ff.{}", "a".repeat(64))
        );
        assert_eq!(JoinToken::parse(&token.to_string()), Ok(token));
    }

    #[test]
    fn the_token_derives_the_dial_and_the_host() {
        let token = JoinToken::new("horde.example:7778", code());
        assert_eq!(token.enrollment_endpoint(), "https://horde.example:7778");
        assert_eq!(token.host(), "horde.example");
    }

    /// Every wrong shape is refused naming its own mistake, and every
    /// refusal teaches the expected form — the errors diagnose the mistake,
    /// not the paste (ADR-0007).
    #[test]
    fn malformed_tokens_are_refused_naming_the_mistake_and_the_form() {
        let fp = "a".repeat(64);
        let cases: Vec<(String, TokenError)> = vec![
            (String::new(), TokenError::WrongPrefix),
            // The pre-token ceremony's inputs, pasted whole: a bare code and
            // a bare endpoint were exactly the two things people swapped.
            (format!("00ff.{fp}"), TokenError::WrongPrefix),
            ("://host".to_owned(), TokenError::WrongPrefix),
            (
                "https://host:7778".to_owned(),
                TokenError::WrongScheme {
                    found: "https://".to_owned(),
                },
            ),
            (
                format!("horde://enroll/host:7778/00ff.{fp}"),
                TokenError::WrongScheme {
                    found: "horde://".to_owned(),
                },
            ),
            ("horde://join/".to_owned(), TokenError::MissingEndpoint),
            // Cut short: the code half gone, or the fingerprint clipped.
            (
                "horde://join/host:7778".to_owned(),
                TokenError::Truncated {
                    detail: "it ends after the enrollment listener, with no code half".to_owned(),
                },
            ),
            (
                "horde://join/host:7778/".to_owned(),
                TokenError::Truncated {
                    detail: "it ends after the enrollment listener, with no code half".to_owned(),
                },
            ),
            (
                format!("horde://join/host:7778/00ff.{}", "a".repeat(40)),
                TokenError::Truncated {
                    detail: "its fingerprint has 40 of 64 hex digits".to_owned(),
                },
            ),
            // A wrapped paste: the break survives inside the token.
            (
                format!(
                    "horde://join/host:7778/00ff.{}\n{}",
                    "a".repeat(30),
                    "a".repeat(34)
                ),
                TokenError::Whitespace { at: 58 },
            ),
            (
                format!("horde://join/host:7778/00ff. {fp}"),
                TokenError::Whitespace { at: 28 },
            ),
            (
                format!("horde://join/00ff.{fp}"),
                TokenError::MissingEndpoint,
            ),
            (
                format!("horde://join/hostonly/00ff.{fp}"),
                TokenError::MalformedEndpoint {
                    endpoint: "hostonly".to_owned(),
                },
            ),
            (
                format!("horde://join/:7778/00ff.{fp}"),
                TokenError::MalformedEndpoint {
                    endpoint: ":7778".to_owned(),
                },
            ),
            (
                format!("horde://join/host:0/00ff.{fp}"),
                TokenError::MalformedEndpoint {
                    endpoint: "host:0".to_owned(),
                },
            ),
            (
                format!("horde://join/host:notaport/00ff.{fp}"),
                TokenError::MalformedEndpoint {
                    endpoint: "host:notaport".to_owned(),
                },
            ),
            (
                "horde://join/host:7778/not-a-code".to_owned(),
                TokenError::MalformedCode,
            ),
            (
                format!("horde://join/host:7778/00ff.{}", "a".repeat(63)),
                TokenError::Truncated {
                    detail: "its fingerprint has 63 of 64 hex digits".to_owned(),
                },
            ),
            (
                format!("horde://join/host:7778/00ff.{}", "a".repeat(65)),
                TokenError::MalformedCode,
            ),
            // A whole 64-digit fingerprint is not truncation, whatever
            // else is wrong with the code.
            (
                format!("horde://join/host:7778/zz.{fp}"),
                TokenError::MalformedCode,
            ),
            (
                format!("horde://join/host:7778/00ff.{}", "g".repeat(10)),
                TokenError::MalformedCode,
            ),
        ];
        for (bad, expected) in cases {
            let refused = JoinToken::parse(&bad).expect_err("must refuse");
            assert_eq!(refused, expected, "{bad:?}");
            assert!(
                refused
                    .to_string()
                    .contains("horde://join/<host:port>/<secret>.<fingerprint>"),
                "every refusal teaches the form: {refused}"
            );
        }
    }

    /// Host scope as a table: bind addresses, this machine, everyone else.
    #[test]
    fn hosts_are_scoped_unspecified_loopback_or_routable() {
        use super::{HostScope, host_scope};
        for (host, scope) in [
            ("0.0.0.0", HostScope::Unspecified),
            ("[::]", HostScope::Unspecified),
            ("::", HostScope::Unspecified),
            ("127.0.0.1", HostScope::Loopback),
            ("127.8.0.1", HostScope::Loopback),
            ("[::1]", HostScope::Loopback),
            ("localhost", HostScope::Loopback),
            ("LocalHost", HostScope::Loopback),
            ("10.0.0.5", HostScope::Routable),
            ("[fe80::1]", HostScope::Routable),
            ("horde.example", HostScope::Routable),
        ] {
            assert_eq!(host_scope(host), scope, "{host}");
        }
        assert_eq!(
            JoinToken::new("0.0.0.0:7778", code()).scope(),
            HostScope::Unspecified
        );
        assert_eq!(
            JoinToken::new("box.lan:7778", code()).scope(),
            HostScope::Routable
        );
    }

    /// Surrounding whitespace is a paste artifact, not a different token.
    #[test]
    fn a_pasted_token_survives_surrounding_whitespace() {
        let token = JoinToken::new("host:7778", code());
        assert_eq!(
            JoinToken::parse(&format!("  {token}\n")),
            Ok(token),
            "trim before judging"
        );
    }

    /// The interior rides [`JoinCode`]'s own normalization: an uppercase pin
    /// is lowercased there, not re-decided here.
    #[test]
    fn the_interior_is_joincode_unchanged() {
        let parsed = JoinToken::parse(&format!("horde://join/h:1/00FF.{}", "A".repeat(64)))
            .expect("uppercase hex is still hex");
        assert_eq!(parsed.code.secret, "00FF");
        assert_eq!(parsed.code.server_fingerprint, "a".repeat(64));
    }
}
