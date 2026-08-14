//! What a certificate means: the principal it carries, and how it is read
//! back at the door.
//!
//! The principal is encoded in the certificate's subject common name as
//! `worker:<name>` or `client:<name>`. The Server writes it at issuance —
//! never copied from the CSR, so a joining machine cannot name itself — and
//! every channel reads it back from the presented certificate. Names are
//! restricted to a shell- and header-safe alphabet because they travel into
//! places that interpolate strings: the Relay's `REMOTE_USER` CGI variable,
//! event payloads, log lines.

use crate::error::IdentityError;

/// Which kind of principal a certificate names (DESIGN §3: workers *and*
/// clients, same mechanism).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrincipalRole {
    /// A machine-resident supervisor that dials the Server.
    Worker,
    /// A user-facing program attached to the Server.
    Client,
}

impl PrincipalRole {
    /// The role's wire word, as it appears in a common name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Client => "client",
        }
    }

    fn from_str(word: &str) -> Option<Self> {
        match word {
            "worker" => Some(Self::Worker),
            "client" => Some(Self::Client),
            _ => None,
        }
    }
}

impl std::fmt::Display for PrincipalRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a name may become a certificate subject.
///
/// A named predicate rather than a condition inline in [`Principal::new`],
/// because its truth table is the security property: the alphabet excludes
/// `:` (the common-name separator — a name containing it would parse back as
/// a different principal), whitespace and control characters (`REMOTE_USER`),
/// and `/` (path-adjacent surfaces).
#[must_use]
pub fn is_valid_principal_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

/// One enrolled identity: a role and a name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal {
    role: PrincipalRole,
    name: String,
}

impl Principal {
    /// Builds a principal, refusing names that cannot safely become one.
    ///
    /// # Errors
    ///
    /// [`IdentityError::InvalidName`] when the name fails
    /// [`is_valid_principal_name`].
    pub fn new(role: PrincipalRole, name: impl Into<String>) -> Result<Self, IdentityError> {
        let name = name.into();
        if !is_valid_principal_name(&name) {
            return Err(IdentityError::InvalidName { name });
        }
        Ok(Self { role, name })
    }

    #[must_use]
    pub fn role(&self) -> PrincipalRole {
        self.role
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The subject common name this principal is issued under.
    #[must_use]
    pub fn common_name(&self) -> String {
        format!("{}:{}", self.role.as_str(), self.name)
    }

    /// Reads a principal back out of a subject common name.
    ///
    /// The inverse of [`Self::common_name`], and only that: an unknown role
    /// word or an invalid name is `None`, never a guess.
    #[must_use]
    pub fn parse_common_name(common_name: &str) -> Option<Self> {
        let (role, name) = common_name.split_once(':')?;
        let role = PrincipalRole::from_str(role)?;
        is_valid_principal_name(name).then(|| Self {
            role,
            name: name.to_owned(),
        })
    }
}

impl std::fmt::Display for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.role, self.name)
    }
}

/// What the door learns from a presented certificate: who, and which exact
/// certificate — the serial is what revocation is keyed by (DESIGN §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    pub principal: Principal,
    /// Lowercase hex of the certificate's serial number, as issued.
    pub serial_hex: String,
}

/// Reads the principal and serial out of a peer's end-entity certificate.
///
/// Called after rustls has verified the chain against the CA, so this is
/// interpretation, not verification: the certificate is already known to be
/// ours, and the question is who it names.
///
/// # Errors
///
/// [`IdentityError::UnreadableCertificate`] when the DER does not parse, and
/// [`IdentityError::NoPrincipal`] when the subject carries no common name in
/// the principal encoding.
pub fn peer_identity(certificate_der: &[u8]) -> Result<PeerIdentity, IdentityError> {
    let (_, certificate) = x509_parser::parse_x509_certificate(certificate_der)
        .map_err(|_| IdentityError::UnreadableCertificate)?;

    let common_name = certificate
        .subject()
        .iter_common_name()
        .next()
        .and_then(|attribute| attribute.as_str().ok())
        .map(str::to_owned);

    let principal = common_name
        .as_deref()
        .and_then(Principal::parse_common_name)
        .ok_or(IdentityError::NoPrincipal { common_name })?;

    Ok(PeerIdentity {
        principal,
        serial_hex: hex::encode(certificate.raw_serial()),
    })
}

#[cfg(test)]
mod tests {
    use super::{Principal, PrincipalRole, is_valid_principal_name};

    #[test]
    fn the_name_alphabet_admits_machine_names_and_nothing_hostile() {
        for good in ["mac-studio", "laptop.home", "w_1", "A", &"x".repeat(64)] {
            assert!(is_valid_principal_name(good), "{good:?} must be usable");
        }
        for bad in [
            "",
            " ",
            "two words",
            "a:b",  // would parse back as a different principal
            "a/b",  // path-adjacent surfaces
            "über", // non-ASCII
            "a\nb", // REMOTE_USER
            &"x".repeat(65),
        ] {
            assert!(!is_valid_principal_name(bad), "{bad:?} must be refused");
        }
    }

    #[test]
    fn a_principal_round_trips_through_its_common_name() {
        for role in [PrincipalRole::Worker, PrincipalRole::Client] {
            let principal = Principal::new(role, "mac-studio").expect("a valid name");
            let parsed = Principal::parse_common_name(&principal.common_name())
                .expect("its own encoding must parse");
            assert_eq!(parsed, principal);
            assert_eq!(parsed.role(), role);
            assert_eq!(parsed.name(), "mac-studio");
        }
    }

    #[test]
    fn common_names_that_are_not_the_encoding_parse_to_nothing() {
        for bad in [
            "",
            "mac-studio",        // no role
            "worker",            // no separator
            "worker:",           // empty name
            "gpu:mac-studio",    // unknown role
            "worker:two words",  // invalid name
            "Worker:mac-studio", // roles are lowercase exactly
        ] {
            assert_eq!(
                Principal::parse_common_name(bad),
                None,
                "{bad:?} must not become a principal"
            );
        }
    }

    #[test]
    fn an_invalid_name_is_refused_at_construction() {
        let refused = Principal::new(PrincipalRole::Worker, "a:b");
        assert!(
            matches!(refused, Err(crate::IdentityError::InvalidName { ref name }) if name == "a:b"),
            "got {refused:?}"
        );
    }

    #[test]
    fn display_matches_the_common_name() {
        let principal = Principal::new(PrincipalRole::Client, "laptop").expect("a valid name");
        assert_eq!(principal.to_string(), "client:laptop");
        assert_eq!(principal.to_string(), principal.common_name());
    }

    // `peer_identity` is exercised in `ca.rs`'s tests against certificates the
    // production issuance path actually signed — a hand-built DER here would be
    // a fixture that rots.
}
