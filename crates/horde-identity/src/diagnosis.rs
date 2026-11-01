//! Plain-words diagnosis for a failed dial (ADR-0007, 2026-10-31 amendment).
//!
//! A transport failure reaches the operator as `transport error`: the cause
//! that matters — the server's certificate came from another CA, it does
//! not cover the dialed name, nothing is listening — sits several sources
//! down the chain, inside an `io::Error` that hides its own payload from
//! `source()`. This module walks the whole chain, opens those wrappers, and
//! names the one cause an operator can act on, with the fix.
//!
//! Here rather than in each binary because the rustls error vocabulary is
//! this crate's: every TLS configuration Horde dials with is built in
//! [`crate::tls`], so what its failures mean has one owner too.

use rustls::{AlertDescription, CertificateError};

/// The actionable cause of a failed dial, as a fact a caller can branch on
/// (`horde up` retries only the transient ones) and render (its `Display`
/// is the plain-words diagnosis with the fix).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cause {
    /// The server's certificate was not issued by the CA this side trusts.
    UnknownIssuer,
    /// The server's certificate does not cover the dialed host.
    NameMismatch,
    /// The server's certificate has expired.
    Expired,
    /// Any other refusal of the server's certificate, by name.
    CertificateRefused(String),
    /// The server refused this side's certificate (the alert it sent).
    PeerRefusedCertificate(String),
    /// The server ended the handshake with another alert.
    HandshakeEnded(String),
    /// Any other TLS failure.
    Tls(String),
    /// Nothing accepted the connection.
    NothingListening,
    /// The address did not answer in time.
    TimedOut,
}

impl Cause {
    /// Whether waiting could cure it: a server still starting refuses or
    /// stalls; a certificate problem never heals by retrying.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::NothingListening | Self::TimedOut)
    }
}

impl std::fmt::Display for Cause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownIssuer => f.write_str(
                "the server's certificate was not issued by the CA this identity trusts: the \
                 identity belongs to a different server (or the server's state dir was \
                 recreated); re-join with a fresh token (`horde enroll join <token>`)",
            ),
            Self::NameMismatch => f.write_str(
                "the server's certificate does not cover the host you dialed: dial a name it \
                 answers to (localhost, 127.0.0.1, or one of its --san names), or restart the \
                 server with --san <host>",
            ),
            Self::Expired => f.write_str(
                "the server's certificate has expired; restart the server to issue a fresh one",
            ),
            Self::CertificateRefused(what) => {
                write!(f, "the server's certificate was refused: {what}")
            }
            Self::PeerRefusedCertificate(alert) => write!(
                f,
                "the server refused this machine's certificate ({alert}): it is revoked, \
                 expired, or from another server's CA; re-join with a fresh token"
            ),
            Self::HandshakeEnded(alert) => {
                write!(f, "the server ended the TLS handshake ({alert})")
            }
            Self::Tls(what) => write!(f, "the TLS handshake failed: {what}"),
            Self::NothingListening => f.write_str(
                "nothing is listening at that address: is the server running? (`horde up` \
                 starts one on this machine)",
            ),
            Self::TimedOut => f.write_str(
                "the address did not answer in time: check the host, the port, and any \
                 firewall between here and the server",
            ),
        }
    }
}

/// The actionable cause of a failed dial, if the chain names one.
#[must_use]
pub fn cause(error: &(dyn std::error::Error + 'static)) -> Option<Cause> {
    let mut current = Some(error);
    while let Some(link) = current {
        if let Some(found) = cause_of_link(link) {
            return Some(found);
        }
        current = link.source();
    }
    None
}

/// [`cause`], rendered: `None` means nothing in the chain is a cause this
/// module recognizes, and the caller's own message stands alone.
#[must_use]
pub fn diagnose(error: &(dyn std::error::Error + 'static)) -> Option<String> {
    cause(error).map(|found| found.to_string())
}

/// One link of the chain: a rustls error, or an `io::Error` — whose
/// wrapped payload `source()` skips, so it is opened here.
fn cause_of_link(link: &(dyn std::error::Error + 'static)) -> Option<Cause> {
    if let Some(tls) = link.downcast_ref::<rustls::Error>() {
        return Some(tls_cause(tls));
    }
    let io = link.downcast_ref::<std::io::Error>()?;
    match io.get_ref() {
        Some(inner) => cause(inner),
        None => io_cause(io.kind()),
    }
}

fn tls_cause(error: &rustls::Error) -> Cause {
    match error {
        rustls::Error::InvalidCertificate(certificate) => certificate_cause(certificate),
        rustls::Error::AlertReceived(alert) => alert_cause(*alert),
        other => Cause::Tls(other.to_string()),
    }
}

fn certificate_cause(error: &CertificateError) -> Cause {
    match error {
        CertificateError::UnknownIssuer | CertificateError::BadSignature => Cause::UnknownIssuer,
        CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. } => {
            Cause::NameMismatch
        }
        CertificateError::Expired | CertificateError::ExpiredContext { .. } => Cause::Expired,
        other => Cause::CertificateRefused(format!("{other:?}")),
    }
}

fn alert_cause(alert: AlertDescription) -> Cause {
    match alert {
        AlertDescription::BadCertificate
        | AlertDescription::UnknownCA
        | AlertDescription::CertificateRevoked
        | AlertDescription::CertificateExpired
        | AlertDescription::CertificateUnknown
        | AlertDescription::CertificateRequired
        | AlertDescription::AccessDenied => Cause::PeerRefusedCertificate(format!("{alert:?}")),
        other => Cause::HandshakeEnded(format!("{other:?}")),
    }
}

fn io_cause(kind: std::io::ErrorKind) -> Option<Cause> {
    match kind {
        std::io::ErrorKind::ConnectionRefused => Some(Cause::NothingListening),
        std::io::ErrorKind::TimedOut => Some(Cause::TimedOut),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use rustls::{AlertDescription, CertificateError};

    use super::{Cause, cause, diagnose};

    /// The shape the dial produces: rustls's error boxed in an `io::Error`
    /// (whose `source()` skips it), under an outer error that names it as
    /// its source.
    #[derive(Debug)]
    struct Outer(std::io::Error);

    impl std::fmt::Display for Outer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("transport error")
        }
    }

    impl std::error::Error for Outer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    fn wrapped(tls: rustls::Error) -> Outer {
        Outer(std::io::Error::new(std::io::ErrorKind::InvalidData, tls))
    }

    fn diagnosed(tls: rustls::Error) -> String {
        diagnose(&wrapped(tls)).expect("a TLS cause is always diagnosed")
    }

    #[test]
    fn each_certificate_cause_names_its_fix() {
        let cases = [
            (CertificateError::UnknownIssuer, "not issued by the CA"),
            (CertificateError::BadSignature, "not issued by the CA"),
            (CertificateError::NotValidForName, "--san"),
            (CertificateError::Expired, "expired"),
            (CertificateError::NotValidYet, "refused: NotValidYet"),
        ];
        for (certificate, expected) in cases {
            let text = diagnosed(rustls::Error::InvalidCertificate(certificate.clone()));
            assert!(text.contains(expected), "{certificate:?}: {text}");
        }
        assert_eq!(
            cause(&wrapped(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer
            ))),
            Some(Cause::UnknownIssuer)
        );
    }

    #[test]
    fn a_refusing_alert_reads_as_this_machines_certificate() {
        for alert in [
            AlertDescription::BadCertificate,
            AlertDescription::UnknownCA,
            AlertDescription::CertificateRevoked,
            AlertDescription::CertificateExpired,
            AlertDescription::CertificateUnknown,
            AlertDescription::CertificateRequired,
            AlertDescription::AccessDenied,
        ] {
            let text = diagnosed(rustls::Error::AlertReceived(alert));
            assert!(text.contains("refused this machine's"), "{alert:?}: {text}");
        }
        let other = diagnosed(rustls::Error::AlertReceived(
            AlertDescription::HandshakeFailure,
        ));
        assert!(other.contains("ended the TLS handshake"), "{other}");
        let generic = diagnosed(rustls::Error::HandshakeNotComplete);
        assert!(generic.contains("TLS handshake failed"), "{generic}");
    }

    #[test]
    fn socket_causes_are_named_transient_and_the_rest_are_left_alone() {
        let refused = Outer(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        assert!(
            diagnose(&refused).is_some_and(|text| text.contains("horde up")),
            "a refused connect points at starting a server"
        );
        let timed_out = Outer(std::io::Error::from(std::io::ErrorKind::TimedOut));
        assert!(diagnose(&timed_out).is_some_and(|text| text.contains("firewall")));
        assert!(Cause::NothingListening.is_transient() && Cause::TimedOut.is_transient());
        assert!(!Cause::UnknownIssuer.is_transient() && !Cause::NameMismatch.is_transient());
        let other = Outer(std::io::Error::from(std::io::ErrorKind::BrokenPipe));
        assert_eq!(diagnose(&other), None, "nothing to add");
        let plain = std::fmt::Error;
        assert_eq!(diagnose(&plain), None, "a chain with no cause in it");
    }
}
