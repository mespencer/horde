//! The Server's internal CA (DESIGN §3): generation, persistence, and the
//! two things it signs.
//!
//! It signs exactly two shapes. The Server's own listener credential
//! (`serverAuth`), so every channel a peer dials presents a certificate this
//! CA vouches for; and enrolled principals' certificates (`clientAuth`),
//! from CSRs — private keys never travel, and the *subject is written by the
//! Server*, never copied from the CSR, so a joining machine cannot name
//! itself.
//!
//! # What DESIGN §3 leaves open, and what this module chose
//!
//! The design names the mechanism (internal CA, CSR enrollment, per-serial
//! revocation) but not the parameters. Chosen here, flagged rather than
//! silent: the CA key lives unencrypted on the Server's disk next to the
//! event log (whoever owns that disk already owns the control plane); CA
//! certificates last twenty years, listener credentials two, principals one
//! (re-enrollment is the renewal story until the design says otherwise); and
//! validity is backdated five minutes so a joining machine with a slightly
//! slow clock is not refused its own brand-new certificate.

use std::path::Path;

use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType, SerialNumber,
};
use time::OffsetDateTime;

use crate::error::IdentityError;
use crate::principal::Principal;

/// How long the CA vouches for itself: twenty years.
pub const CA_VALIDITY_MS: i64 = 20 * 365 * 24 * 60 * 60 * 1_000;

/// How long a listener credential lasts: two years.
pub const SERVER_VALIDITY_MS: i64 = 2 * 365 * 24 * 60 * 60 * 1_000;

/// How long an enrolled principal's certificate lasts: one year.
pub const PRINCIPAL_VALIDITY_MS: i64 = 365 * 24 * 60 * 60 * 1_000;

/// Validity begins this far in the past, absorbing clock skew between the
/// Server and a joining machine.
pub const BACKDATE_MS: i64 = 5 * 60 * 1_000;

const CA_CERT_FILE: &str = "ca.pem";
const CA_KEY_FILE: &str = "ca.key";

/// The Server's own listener credential, signed by the CA.
///
/// One credential for every listener the Server binds — gRPC, enrollment,
/// Relay, MCP — because they are one Server; the joining machine pins this
/// certificate's fingerprint.
#[derive(Clone)]
pub struct ServerCredential {
    pub certificate_pem: String,
    pub key_pem: String,
}

impl ServerCredential {
    /// The fingerprint a joining machine pins (DESIGN §3).
    ///
    /// # Errors
    ///
    /// [`IdentityError::UnreadablePem`] if the credential's own PEM is
    /// unreadable, which would mean issuance itself is broken.
    pub fn fingerprint(&self) -> Result<String, IdentityError> {
        crate::fingerprint::fingerprint_of_pem(&self.certificate_pem)
    }
}

impl std::fmt::Debug for ServerCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The key is deliberately absent: Debug output lands in logs.
        f.debug_struct("ServerCredential")
            .field("certificate_pem", &self.certificate_pem)
            .finish_non_exhaustive()
    }
}

/// What enrollment hands back: the signed certificate and the serial that
/// revocation will be keyed by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedCertificate {
    pub certificate_pem: String,
    /// Lowercase hex, exactly as [`crate::principal::peer_identity`] will
    /// read it back off the wire.
    pub serial_hex: String,
}

/// The CA: a signing key and the certificate that anchors trust in it.
pub struct CertificateAuthority {
    issuer: Issuer<'static, KeyPair>,
    certificate_pem: String,
}

impl std::fmt::Debug for CertificateAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertificateAuthority")
            .field("certificate_pem", &self.certificate_pem)
            .finish_non_exhaustive()
    }
}

impl CertificateAuthority {
    /// Generates a fresh CA, valid from `now_unix_ms`.
    ///
    /// # Errors
    ///
    /// Key generation or self-signing failing, or a time out of range.
    pub fn generate(now_unix_ms: i64) -> Result<Self, IdentityError> {
        let key = KeyPair::generate()?;

        let mut params = CertificateParams::default();
        params.distinguished_name = common_name("horde-ca");
        // Depth 0: this CA signs leaves and nothing below them, so a stolen
        // leaf key cannot mint further identities.
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        (params.not_before, params.not_after) = validity(now_unix_ms, CA_VALIDITY_MS)?;
        params.serial_number = Some(random_serial()?.0);

        let certificate = params.self_signed(&key)?;
        let certificate_pem = certificate.pem();
        Ok(Self {
            issuer: Issuer::new(params, key),
            certificate_pem,
        })
    }

    /// The PEM every peer must trust as its root.
    #[must_use]
    pub fn certificate_pem(&self) -> &str {
        &self.certificate_pem
    }

    /// Writes the CA to `directory` as `ca.pem` and `ca.key` (key readable
    /// by owner only).
    ///
    /// # Errors
    ///
    /// Any filesystem failure, with the path in the message.
    pub fn persist(&self, directory: &Path) -> Result<(), IdentityError> {
        std::fs::create_dir_all(directory)
            .map_err(|e| IdentityError::io(directory.display().to_string(), e))?;
        let cert_path = directory.join(CA_CERT_FILE);
        std::fs::write(&cert_path, &self.certificate_pem)
            .map_err(|e| IdentityError::io(cert_path.display().to_string(), e))?;
        let key_path = directory.join(CA_KEY_FILE);
        crate::store::write_secret(&key_path, &self.issuer.key().serialize_pem())
    }

    /// Loads a persisted CA back from `directory`.
    ///
    /// The issuer is rebuilt from the persisted *certificate* rather than
    /// from this crate's current defaults, so certificates issued after a
    /// restart chain to the same root byte-for-byte.
    ///
    /// # Errors
    ///
    /// Missing or unreadable files, or a certificate/key pair rcgen refuses.
    pub fn load(directory: &Path) -> Result<Self, IdentityError> {
        let cert_path = directory.join(CA_CERT_FILE);
        let certificate_pem = std::fs::read_to_string(&cert_path)
            .map_err(|e| IdentityError::io(cert_path.display().to_string(), e))?;
        let key_path = directory.join(CA_KEY_FILE);
        let key_pem = std::fs::read_to_string(&key_path)
            .map_err(|e| IdentityError::io(key_path.display().to_string(), e))?;

        let key = KeyPair::from_pem(&key_pem)?;
        let issuer = Issuer::from_ca_cert_pem(&certificate_pem, key)?;
        Ok(Self {
            issuer,
            certificate_pem,
        })
    }

    /// Whether a persisted CA exists in `directory`.
    #[must_use]
    pub fn exists(directory: &Path) -> bool {
        directory.join(CA_CERT_FILE).is_file() && directory.join(CA_KEY_FILE).is_file()
    }

    /// Issues the Server's own listener credential for these names.
    ///
    /// # Errors
    ///
    /// A DNS name that is not IA5, generation or signing failing, or a time
    /// out of range.
    pub fn issue_server(
        &self,
        now_unix_ms: i64,
        dns_names: &[String],
        ip_addresses: &[std::net::IpAddr],
    ) -> Result<ServerCredential, IdentityError> {
        let key = KeyPair::generate()?;

        let mut params = CertificateParams::default();
        params.distinguished_name = common_name("horde-server");
        params.is_ca = IsCa::ExplicitNoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        for name in dns_names {
            params
                .subject_alt_names
                .push(SanType::DnsName(name.as_str().try_into()?));
        }
        for ip in ip_addresses {
            params.subject_alt_names.push(SanType::IpAddress(*ip));
        }
        (params.not_before, params.not_after) = validity(now_unix_ms, SERVER_VALIDITY_MS)?;
        params.serial_number = Some(random_serial()?.0);

        let certificate = params.signed_by(&key, &self.issuer)?;
        Ok(ServerCredential {
            certificate_pem: certificate.pem(),
            key_pem: key.serialize_pem(),
        })
    }

    /// [`issue_server`](Self::issue_server) for the names every loopback
    /// deployment shares — `localhost` and `127.0.0.1` — which is also what
    /// every in-workspace suite serves as.
    ///
    /// # Errors
    ///
    /// As [`issue_server`](Self::issue_server).
    pub fn issue_localhost_server(
        &self,
        now_unix_ms: i64,
    ) -> Result<ServerCredential, IdentityError> {
        self.issue_server(
            now_unix_ms,
            &["localhost".to_owned()],
            &[std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
        )
    }

    /// Signs a joining machine's CSR as `principal`.
    ///
    /// Only the public key survives from the CSR. Subject, SANs, usages,
    /// validity and serial are all written here: the CSR is a key-possession
    /// proof, not a request for attributes.
    ///
    /// # Errors
    ///
    /// [`IdentityError::UnreadablePem`] for a CSR that does not parse;
    /// signing failures otherwise.
    pub fn sign_join(
        &self,
        csr_pem: &str,
        principal: &Principal,
        now_unix_ms: i64,
    ) -> Result<IssuedCertificate, IdentityError> {
        let mut csr = CertificateSigningRequestParams::from_pem(csr_pem)
            .map_err(|_| IdentityError::UnreadablePem { what: "the CSR" })?;

        csr.params.distinguished_name = common_name(&principal.common_name());
        csr.params.subject_alt_names.clear();
        csr.params.is_ca = IsCa::ExplicitNoCa;
        csr.params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        csr.params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        (csr.params.not_before, csr.params.not_after) =
            validity(now_unix_ms, PRINCIPAL_VALIDITY_MS)?;
        let (serial, serial_hex) = random_serial()?;
        csr.params.serial_number = Some(serial);

        let certificate = csr.signed_by(&self.issuer)?;
        Ok(IssuedCertificate {
            certificate_pem: certificate.pem(),
            serial_hex,
        })
    }
}

/// A distinguished name that is exactly one common name.
fn common_name(name: &str) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, name);
    dn
}

/// A random 16-byte serial, and the hex the log will record for it.
///
/// The first byte is clamped into `0x01..=0x7f` so the DER integer is
/// positive with no leading-zero normalization — which is what makes
/// `hex::encode(bytes)` here equal to the hex of `raw_serial()` read back
/// off the certificate. Revocation matches on that equality.
fn random_serial() -> Result<(SerialNumber, String), IdentityError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| IdentityError::Randomness(e.to_string()))?;
    bytes[0] = (bytes[0] & 0x7f).max(1);
    Ok((SerialNumber::from(bytes.to_vec()), hex::encode(bytes)))
}

/// `[now - backdate, now + lifetime]` as rcgen wants it.
fn validity(
    now_unix_ms: i64,
    lifetime_ms: i64,
) -> Result<(OffsetDateTime, OffsetDateTime), IdentityError> {
    let at = |ms: i64| {
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
            .map_err(|_| IdentityError::TimeOutOfRange)
    };
    Ok((
        at(now_unix_ms.saturating_sub(BACKDATE_MS))?,
        at(now_unix_ms.saturating_add(lifetime_ms))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::{BACKDATE_MS, CertificateAuthority, PRINCIPAL_VALIDITY_MS};
    use crate::enroll::generate_csr;
    use crate::principal::{PeerIdentity, Principal, PrincipalRole, peer_identity};

    const NOW_MS: i64 = 1_754_956_800_000; // 2025-08-12T00:00:00Z

    fn authority() -> CertificateAuthority {
        CertificateAuthority::generate(NOW_MS).expect("a CA generates")
    }

    fn worker() -> Principal {
        Principal::new(PrincipalRole::Worker, "mac-studio").expect("a valid name")
    }

    /// PEM → DER for reading an issued certificate back the way a channel
    /// door does.
    fn der_of(pem: &str) -> Vec<u8> {
        use rustls_pki_types::{CertificateDer, pem::PemObject};
        CertificateDer::from_pem_slice(pem.as_bytes())
            .expect("issued PEM must parse")
            .to_vec()
    }

    #[test]
    fn an_issued_certificate_reads_back_as_its_principal_and_serial() {
        let ca = authority();
        let (_key_pem, csr_pem) = generate_csr().expect("a CSR generates");

        let issued = ca
            .sign_join(&csr_pem, &worker(), NOW_MS)
            .expect("the CA signs a fresh CSR");

        let read_back = peer_identity(&der_of(&issued.certificate_pem))
            .expect("an issued certificate must carry a readable identity");
        assert_eq!(
            read_back,
            PeerIdentity {
                principal: worker(),
                serial_hex: issued.serial_hex.clone(),
            },
            "what the door reads must be exactly what issuance recorded"
        );
    }

    #[test]
    fn two_issued_certificates_get_distinct_serials() {
        let ca = authority();
        let (_key, csr) = generate_csr().expect("a CSR generates");
        let first = ca.sign_join(&csr, &worker(), NOW_MS).expect("signs");
        let second = ca.sign_join(&csr, &worker(), NOW_MS).expect("signs");
        assert_ne!(
            first.serial_hex, second.serial_hex,
            "revocation is keyed by serial, so serials must never repeat"
        );
    }

    /// The subject is the Server's to write. A CSR that names itself
    /// `client:admin` must come out as the principal the code was minted for.
    #[test]
    fn the_csr_subject_is_overridden_not_honored() {
        use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};

        let ca = authority();
        let key = KeyPair::generate().expect("a keypair generates");
        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "client:admin");
        params.distinguished_name = dn;
        let csr_pem = params
            .serialize_request(&key)
            .expect("a CSR serializes")
            .pem()
            .expect("as PEM");

        let issued = ca
            .sign_join(&csr_pem, &worker(), NOW_MS)
            .expect("the CA signs");
        let read_back =
            peer_identity(&der_of(&issued.certificate_pem)).expect("a readable identity");
        assert_eq!(
            read_back.principal,
            worker(),
            "a joining machine must not be able to name itself"
        );
    }

    #[test]
    fn garbage_is_not_a_csr() {
        let refused = authority().sign_join("not a csr", &worker(), NOW_MS);
        assert!(
            matches!(
                refused,
                Err(crate::IdentityError::UnreadablePem { what: "the CSR" })
            ),
            "got {refused:?}"
        );
    }

    #[test]
    fn validity_is_backdated_and_bounded() {
        let ca = authority();
        let (_key, csr) = generate_csr().expect("a CSR generates");
        let issued = ca.sign_join(&csr, &worker(), NOW_MS).expect("signs");

        let der = der_of(&issued.certificate_pem);
        let (_, certificate) =
            x509_parser::parse_x509_certificate(&der).expect("issued DER parses");
        let not_before_ms = certificate.validity().not_before.timestamp() * 1_000;
        let not_after_ms = certificate.validity().not_after.timestamp() * 1_000;
        assert_eq!(not_before_ms, NOW_MS - BACKDATE_MS, "skew allowance");
        assert_eq!(not_after_ms, NOW_MS + PRINCIPAL_VALIDITY_MS, "one year");
    }

    #[test]
    fn a_loaded_ca_issues_under_the_same_root() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let ca = authority();
        ca.persist(directory.path()).expect("the CA persists");

        assert!(CertificateAuthority::exists(directory.path()));
        let reloaded = CertificateAuthority::load(directory.path()).expect("the CA loads");
        assert_eq!(
            reloaded.certificate_pem(),
            ca.certificate_pem(),
            "the trust root must survive a restart byte-for-byte"
        );

        // The real proof — a certificate issued *after* the restart still
        // chains to the persisted root — is a TLS handshake, held in
        // `tls::tests::a_reloaded_ca_still_authenticates_its_peers`.
        let (_key, csr) = generate_csr().expect("a CSR generates");
        reloaded
            .sign_join(&csr, &worker(), NOW_MS)
            .expect("a reloaded CA must still sign");
    }

    #[test]
    fn a_missing_directory_is_not_a_ca() {
        let directory = tempfile::tempdir().expect("a temp dir");
        assert!(!CertificateAuthority::exists(directory.path()));
        assert!(CertificateAuthority::load(directory.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn the_persisted_key_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("a temp dir");
        authority().persist(directory.path()).expect("persists");
        let mode = std::fs::metadata(directory.path().join("ca.key"))
            .expect("the key exists")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "a CA key is not group/world readable");
    }
}
