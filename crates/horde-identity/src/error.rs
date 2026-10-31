//! What can go wrong building, storing, or reading an identity.

/// Failures across the identity crate.
///
/// One error type rather than one per module: every consumer of this crate is
/// a channel being stood up or a peer being checked, and both react the same
/// way to any of these — refuse loudly. The variants exist so the refusal
/// says *which* material was bad, not so callers can branch.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// The OS randomness source failed — nothing key- or code-shaped may be
    /// minted from a bad source, so this is fatal to the operation.
    #[error("the system's randomness source failed: {0}")]
    Randomness(String),

    /// rcgen refused to generate or sign.
    #[error("could not build a key or certificate: {0}")]
    Certificate(#[from] rcgen::Error),

    /// PEM that did not contain what its label promised.
    #[error("{what} is not valid PEM of the expected kind")]
    UnreadablePem {
        /// Which input was unreadable, e.g. "the CA certificate".
        what: &'static str,
    },

    /// A DER certificate x509-parser could not read.
    #[error("the peer certificate could not be parsed")]
    UnreadableCertificate,

    /// A certificate that parses but names no Horde principal.
    #[error("the certificate subject names no principal: {common_name:?}")]
    NoPrincipal {
        /// The subject common name that failed to parse, if there was one.
        common_name: Option<String>,
    },

    /// A principal name this crate refuses to put into a certificate.
    #[error("{name:?} is not a usable principal name (1..=64 of a-z A-Z 0-9 . _ -)")]
    InvalidName { name: String },

    /// rustls refused a configuration.
    #[error("TLS configuration was refused: {0}")]
    Tls(String),

    /// A validity instant that does not fit a certificate.
    #[error("a validity time is out of range")]
    TimeOutOfRange,

    /// An endpoint refused before it is persisted (ADR-0007, 2026-10-31
    /// amendment): a stored endpoint is dialed by every later invocation,
    /// so a garbled one would fail far from the join that wrote it.
    #[error("{endpoint:?} is not a storable endpoint: {reason}")]
    InvalidEndpoint {
        endpoint: String,
        reason: &'static str,
    },

    /// A state directory another live process already holds.
    #[error("{path} is held by another running process{holder}")]
    StateHeld {
        /// The lock file.
        path: String,
        /// `" (pid N)"` when the holder wrote its pid, else empty.
        holder: String,
    },

    /// Reading or writing identity material on disk.
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
}

impl IdentityError {
    /// An I/O failure, with the path or action that failed.
    pub(crate) fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}
