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
}

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
        let rest = token
            .trim()
            .strip_prefix(PREFIX)
            .ok_or(TokenError::WrongPrefix)?;
        let (host_port, code) = rest.split_once('/').ok_or(TokenError::MissingEndpoint)?;
        if !is_host_port(host_port) {
            return Err(TokenError::MalformedEndpoint {
                endpoint: host_port.to_owned(),
            });
        }
        let code = JoinCode::parse(code).ok_or(TokenError::MalformedCode)?;
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
            ("https://host:7778".to_owned(), TokenError::WrongPrefix),
            ("horde://join/".to_owned(), TokenError::MissingEndpoint),
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
