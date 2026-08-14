//! Certificate fingerprints, and the join code that embeds one.
//!
//! DESIGN §3: the enrollment code embeds the Server's certificate
//! fingerprint, so the joining machine can authenticate the Server *before*
//! sending it a CSR — the one moment no CA is trusted yet. The fingerprint is
//! SHA-256 over the certificate's DER, lowercase hex, the same shape every
//! TLS tool prints.

use sha2::{Digest, Sha256};

use crate::error::IdentityError;

/// SHA-256 over a certificate's DER encoding, as 64 lowercase hex digits.
#[must_use]
pub fn certificate_fingerprint(certificate_der: &[u8]) -> String {
    hex::encode(Sha256::digest(certificate_der))
}

/// The fingerprint of the first certificate in a PEM document.
///
/// # Errors
///
/// [`IdentityError::UnreadablePem`] when the input holds no certificate.
pub fn fingerprint_of_pem(certificate_pem: &str) -> Result<String, IdentityError> {
    use rustls_pki_types::{CertificateDer, pem::PemObject};

    let der = CertificateDer::from_pem_slice(certificate_pem.as_bytes()).map_err(|_| {
        IdentityError::UnreadablePem {
            what: "the certificate",
        }
    })?;
    Ok(certificate_fingerprint(&der))
}

/// Whether two fingerprints name the same certificate.
///
/// A named predicate so the comparison is case-insensitive in exactly one
/// place: fingerprints are minted lowercase here, but a human relaying one
/// out of another tool may carry uppercase hex.
#[must_use]
pub fn fingerprints_match(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// What a human carries to the joining machine: the one-time secret and the
/// Server fingerprint, in one copy-pasteable token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinCode {
    /// The one-time secret the Server minted; redeemed exactly once.
    pub secret: String,
    /// The Server certificate fingerprint the joining machine pins.
    pub server_fingerprint: String,
}

impl JoinCode {
    /// Reads a join code back from its `secret.fingerprint` form.
    ///
    /// Strict on shape — a fingerprint that is not 64 hex digits is a
    /// mistyped code, and joining with a garbled pin would fail with a
    /// misleading "server not trusted" instead of "code mistyped".
    #[must_use]
    pub fn parse(code: &str) -> Option<Self> {
        let (secret, fingerprint) = code.split_once('.')?;
        let well_formed = !secret.is_empty()
            && secret.bytes().all(|b| b.is_ascii_hexdigit())
            && fingerprint.len() == 64
            && fingerprint.bytes().all(|b| b.is_ascii_hexdigit());
        well_formed.then(|| Self {
            secret: secret.to_owned(),
            server_fingerprint: fingerprint.to_ascii_lowercase(),
        })
    }
}

impl std::fmt::Display for JoinCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.secret, self.server_fingerprint)
    }
}

#[cfg(test)]
mod tests {
    use super::{JoinCode, certificate_fingerprint, fingerprints_match};

    #[test]
    fn a_fingerprint_is_sha256_hex_of_the_der() {
        // Fixed input, fixed answer: the empty DER's SHA-256 is a published
        // constant, so this pins the algorithm rather than restating the code.
        assert_eq!(
            certificate_fingerprint(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(certificate_fingerprint(b"x").len(), 64);
        assert_ne!(certificate_fingerprint(b"x"), certificate_fingerprint(b"y"));
    }

    #[test]
    fn matching_is_case_insensitive_and_nothing_else() {
        assert!(fingerprints_match("abc123", "ABC123"));
        assert!(!fingerprints_match("abc123", "abc124"));
        assert!(!fingerprints_match("abc123", "abc12"));
    }

    #[test]
    fn a_join_code_round_trips_through_display() {
        let code = JoinCode {
            secret: "00ff".to_owned(),
            server_fingerprint: "a".repeat(64),
        };
        assert_eq!(JoinCode::parse(&code.to_string()), Some(code));
    }

    #[test]
    fn parsing_uppercases_nothing_but_the_pin_it_normalizes() {
        let parsed = JoinCode::parse(&format!("00FF.{}", "A".repeat(64)))
            .expect("uppercase hex is still hex");
        assert_eq!(
            parsed.secret, "00FF",
            "the secret is compared by hash, not re-cased"
        );
        assert_eq!(parsed.server_fingerprint, "a".repeat(64));
    }

    #[test]
    fn malformed_join_codes_parse_to_nothing() {
        let fp = "a".repeat(64);
        for bad in [
            String::new(),
            "just-a-secret".to_owned(),
            format!(".{fp}"),                   // empty secret
            format!("00ff.{}", "a".repeat(63)), // short pin
            format!("00ff.{}", "g".repeat(64)), // non-hex pin
            format!("zz.{fp}"),                 // non-hex secret
        ] {
            assert_eq!(JoinCode::parse(&bad), None, "{bad:?} must be refused");
        }
    }
}
