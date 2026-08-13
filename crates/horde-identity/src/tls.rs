//! Every rustls configuration Horde uses, in one place.
//!
//! Four shapes, one owner:
//!
//! * the tonic server config for the mTLS gRPC listeners and the
//!   server-auth-only enrollment listener ([`grpc_server_mtls`],
//!   [`grpc_server_tls`]);
//! * the tonic channel config an enrolled peer dials with
//!   ([`grpc_client_mtls`]);
//! * the acceptor the hyper-served channels (Relay, MCP endpoint) terminate
//!   with ([`mtls_acceptor`]), whose verifier also refuses revoked serials
//!   *at the handshake* — those channels have no per-RPC interceptor to do
//!   it later;
//! * the fingerprint-pinned verifier ([`pinned_verifier`]) the join flow
//!   uses in the one moment no CA is trusted yet (DESIGN §3).
//!
//! Everything is ring-backed, explicitly: the provider is named at every
//! builder, so a second crypto provider appearing in the dependency tree can
//! never make a builder ambiguous.

use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

use crate::ca::ServerCredential;
use crate::error::IdentityError;
use crate::fingerprint::{certificate_fingerprint, fingerprints_match};
use crate::principal::{PeerIdentity, peer_identity};
use crate::revocation::RevocationList;
use crate::store::StoredIdentity;

/// The one crypto provider this workspace uses.
fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// All certificates in a PEM document, as DER.
fn pem_certificates(
    pem: &str,
    what: &'static str,
) -> Result<Vec<CertificateDer<'static>>, IdentityError> {
    let certificates: Vec<_> = CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<_, _>>()
        .map_err(|_| IdentityError::UnreadablePem { what })?;
    if certificates.is_empty() {
        return Err(IdentityError::UnreadablePem { what });
    }
    Ok(certificates)
}

/// The private key in a PEM document.
fn pem_key(pem: &str, what: &'static str) -> Result<PrivateKeyDer<'static>, IdentityError> {
    PrivateKeyDer::from_pem_slice(pem.as_bytes()).map_err(|_| IdentityError::UnreadablePem { what })
}

/// A root store holding exactly the CA.
fn ca_roots(ca_pem: &str) -> Result<Arc<rustls::RootCertStore>, IdentityError> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in pem_certificates(ca_pem, "the CA certificate")? {
        roots
            .add(certificate)
            .map_err(|e| IdentityError::Tls(e.to_string()))?;
    }
    Ok(Arc::new(roots))
}

// ---------------------------------------------------------------------------
// tonic configurations
// ---------------------------------------------------------------------------

/// The gRPC listener config: present the Server credential, require a client
/// certificate chaining to the CA.
///
/// Revocation on this channel is enforced per-RPC by
/// [`crate::interceptor::require_enrolled`], which also maps the certificate
/// to a [`crate::Principal`] — tonic owns the handshake here, so the door
/// check lives in the interceptor rather than a custom verifier.
#[must_use]
pub fn grpc_server_mtls(
    credential: &ServerCredential,
    ca_pem: &str,
) -> tonic::transport::ServerTlsConfig {
    grpc_server_tls(credential)
        .client_ca_root(tonic::transport::Certificate::from_pem(ca_pem))
        .client_auth_optional(false)
}

/// The enrollment listener config: present the Server credential, require
/// nothing back.
///
/// The joining machine has no certificate yet by definition; it
/// authenticates the Server by pinned fingerprint, and the code proves the
/// human (DESIGN §3).
#[must_use]
pub fn grpc_server_tls(credential: &ServerCredential) -> tonic::transport::ServerTlsConfig {
    tonic::transport::ServerTlsConfig::new().identity(tonic::transport::Identity::from_pem(
        &credential.certificate_pem,
        &credential.key_pem,
    ))
}

/// The channel config an enrolled peer dials any Server listener with.
///
/// `domain` is the name the Server credential was issued for — what the
/// peer typed after `https://`, DNS name or IP literal alike.
#[must_use]
pub fn grpc_client_mtls(
    identity: &StoredIdentity,
    domain: &str,
) -> tonic::transport::ClientTlsConfig {
    tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&identity.ca_pem))
        .identity(tonic::transport::Identity::from_pem(
            &identity.certificate_pem,
            &identity.key_pem,
        ))
        .domain_name(domain)
}

// ---------------------------------------------------------------------------
// The fingerprint-pinned verifier (the join flow's trust root)
// ---------------------------------------------------------------------------

/// A server verifier that trusts exactly one certificate: the one whose
/// SHA-256 fingerprint the enrollment code embedded.
///
/// Used only by the join flow, where it is *stronger* than PKI, not weaker:
/// nothing chains, nothing is delegated, the human carried the pin. Names
/// and validity windows are deliberately not checked — the pin identifies
/// the certificate itself, and the handshake proves the server holds its
/// key.
#[derive(Debug)]
struct FingerprintVerifier {
    expected: String,
    provider: Arc<CryptoProvider>,
}

/// A verifier for [`tonic::transport::Endpoint::tls_config_with_verifier`]
/// pinning `server_fingerprint`.
#[must_use]
pub fn pinned_verifier(
    server_fingerprint: &str,
) -> Arc<dyn rustls::client::danger::ServerCertVerifier> {
    Arc::new(FingerprintVerifier {
        expected: server_fingerprint.to_owned(),
        provider: provider(),
    })
}

impl rustls::client::danger::ServerCertVerifier for FingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if fingerprints_match(&certificate_fingerprint(end_entity), &self.expected) {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

// ---------------------------------------------------------------------------
// The hyper-side acceptor (Relay, MCP endpoint)
// ---------------------------------------------------------------------------

/// A client verifier that requires a chain to the CA *and* refuses revoked
/// serials during the handshake.
///
/// The hyper-served channels have no per-RPC interceptor, so the handshake
/// is their only door; webpki does the cryptography, this adds the deny-list.
#[derive(Debug)]
struct RevocationAwareVerifier {
    inner: Arc<dyn rustls::server::danger::ClientCertVerifier>,
    deny: RevocationList,
}

impl rustls::server::danger::ClientCertVerifier for RevocationAwareVerifier {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        let identity = peer_identity(end_entity).map_err(|_| {
            rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            )
        })?;
        if self.deny.is_revoked(&identity.serial_hex) {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::Revoked,
            ));
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// The acceptor a hyper-served channel terminates mTLS with.
///
/// HTTP/1.1 only, deliberately: both consumers (the Relay's smart-HTTP
/// bridge and the MCP endpoint) serve http1, and advertising h2 here would
/// negotiate a protocol neither speaks.
///
/// # Errors
///
/// Unreadable credential or CA PEM, or a rustls builder refusal.
pub fn mtls_acceptor(
    credential: &ServerCredential,
    ca_pem: &str,
    deny: RevocationList,
) -> Result<tokio_rustls::TlsAcceptor, IdentityError> {
    let inner =
        rustls::server::WebPkiClientVerifier::builder_with_provider(ca_roots(ca_pem)?, provider())
            .build()
            .map_err(|e| IdentityError::Tls(e.to_string()))?;

    let mut config = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| IdentityError::Tls(e.to_string()))?
        .with_client_cert_verifier(Arc::new(RevocationAwareVerifier { inner, deny }))
        .with_single_cert(
            pem_certificates(&credential.certificate_pem, "the server certificate")?,
            pem_key(&credential.key_pem, "the server key")?,
        )
        .map_err(|e| IdentityError::Tls(e.to_string()))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];

    // No session resumption, deliberately. A resumed session restores the
    // client certificate *without re-running the verifier*, so a serial
    // revoked after one successful handshake would ride its ticket straight
    // past the deny-list — measured here, not hypothesized: the revocation
    // test answered `200` before these two lines. Enrollment-scale channels
    // do not need resumption's latency win; they need the door to stay a
    // door.
    config.send_tls13_tickets = 0;
    config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});

    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// The client counterpart of [`mtls_acceptor`], for anything in-process that
/// dials a hyper-served channel: trust the CA, present the identity.
///
/// # Errors
///
/// Unreadable PEM in the identity, or a rustls builder refusal.
pub fn client_mtls_config(
    identity: &StoredIdentity,
) -> Result<rustls::ClientConfig, IdentityError> {
    let mut config = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| IdentityError::Tls(e.to_string()))?
        .with_root_certificates(ca_roots(&identity.ca_pem)?)
        .with_client_auth_cert(
            pem_certificates(&identity.certificate_pem, "the identity certificate")?,
            pem_key(&identity.key_pem, "the identity key")?,
        )
        .map_err(|e| IdentityError::Tls(e.to_string()))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// Reads the authenticated peer off an accepted TLS stream.
///
/// # Errors
///
/// [`IdentityError::UnreadableCertificate`] when the handshake admitted no
/// client certificate — which [`mtls_acceptor`] makes impossible, so seeing
/// it means the stream was not accepted by that acceptor; certificate
/// interpretation errors otherwise.
pub fn peer_of_stream<IO>(
    stream: &tokio_rustls::server::TlsStream<IO>,
) -> Result<PeerIdentity, IdentityError> {
    let (_, connection) = stream.get_ref();
    let end_entity = connection
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .ok_or(IdentityError::UnreadableCertificate)?;
    peer_identity(end_entity)
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::{client_mtls_config, mtls_acceptor, peer_of_stream, pinned_verifier};
    use crate::ca::CertificateAuthority;
    use crate::enroll::generate_csr;
    use crate::principal::{Principal, PrincipalRole};
    use crate::revocation::RevocationList;
    use crate::store::StoredIdentity;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// The real clock, because these handshakes are verified by webpki
    /// against the real clock — a pinned epoch here would start failing the
    /// day the leaf validity window slid past it.
    fn now_ms() -> i64 {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_millis())
                .unwrap_or_default(),
        )
        .unwrap_or_default()
    }

    struct Harness {
        ca: CertificateAuthority,
        credential: crate::ca::ServerCredential,
    }

    fn harness() -> Harness {
        let ca = CertificateAuthority::generate(now_ms()).expect("a CA generates");
        let credential = ca
            .issue_server(
                now_ms(),
                &["localhost".to_owned()],
                &[std::net::IpAddr::V4(Ipv4Addr::LOCALHOST)],
            )
            .expect("a server credential issues");
        Harness { ca, credential }
    }

    /// Enrolls a principal through the production issuance path — CSR in,
    /// certificate out — never a hand-baked fixture.
    fn enrolled(harness: &Harness, name: &str) -> (StoredIdentity, String) {
        let principal = Principal::new(PrincipalRole::Worker, name).expect("a valid name");
        let (key_pem, csr_pem) = generate_csr().expect("a CSR generates");
        let issued = harness
            .ca
            .sign_join(&csr_pem, &principal, now_ms())
            .expect("the CA signs");
        (
            StoredIdentity {
                certificate_pem: issued.certificate_pem,
                key_pem,
                ca_pem: harness.ca.certificate_pem().to_owned(),
            },
            issued.serial_hex,
        )
    }

    /// One accept + one dial over a real socket; answers what each side saw.
    ///
    /// The server half answers the peer identity it read (or the handshake
    /// error), which is what every assertion here is actually about.
    async fn handshake(
        acceptor: tokio_rustls::TlsAcceptor,
        client: rustls::ClientConfig,
    ) -> (Result<crate::PeerIdentity, String>, Result<(), String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binds");
        let address = listener.local_addr().expect("has an address");

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let mut tls = acceptor.accept(tcp).await.map_err(|e| e.to_string())?;
            let peer = peer_of_stream(&tls).map_err(|e| e.to_string());
            // One byte each way so the handshake is *completed*, not merely
            // begun — TLS 1.3 servers only learn of a refused certificate
            // from the client's alert on the next read.
            let mut byte = [0_u8; 1];
            tls.read_exact(&mut byte).await.map_err(|e| e.to_string())?;
            tls.write_all(&byte).await.map_err(|e| e.to_string())?;
            peer
        });

        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
        let client_outcome = async {
            let tcp = tokio::net::TcpStream::connect(address)
                .await
                .map_err(|e| e.to_string())?;
            let name =
                rustls_pki_types::ServerName::try_from("localhost").map_err(|e| e.to_string())?;
            let mut tls = connector
                .connect(name, tcp)
                .await
                .map_err(|e| e.to_string())?;
            tls.write_all(&[0_u8]).await.map_err(|e| e.to_string())?;
            let mut byte = [0_u8; 1];
            tls.read_exact(&mut byte).await.map_err(|e| e.to_string())?;
            Ok(())
        }
        .await;

        let server_outcome = server
            .await
            .unwrap_or_else(|join| Err(format!("server task died: {join}")));
        (server_outcome, client_outcome)
    }

    #[tokio::test]
    async fn an_enrolled_peer_handshakes_and_is_read_back_exactly() {
        let harness = harness();
        let (identity, serial_hex) = enrolled(&harness, "mac-studio");

        let acceptor = mtls_acceptor(
            &harness.credential,
            harness.ca.certificate_pem(),
            RevocationList::new(),
        )
        .expect("an acceptor builds");
        let client = client_mtls_config(&identity).expect("a client config builds");

        let (server_saw, client_saw) = handshake(acceptor, client).await;
        assert_eq!(client_saw, Ok(()), "the enrolled peer must get through");
        let peer = server_saw.expect("the server must read an identity");
        assert_eq!(peer.principal.to_string(), "worker:mac-studio");
        assert_eq!(peer.serial_hex, serial_hex);
    }

    #[tokio::test]
    async fn a_peer_with_no_certificate_is_refused_at_the_handshake() {
        let harness = harness();
        let acceptor = mtls_acceptor(
            &harness.credential,
            harness.ca.certificate_pem(),
            RevocationList::new(),
        )
        .expect("an acceptor builds");

        // Trusts our CA but presents nothing.
        let bare = rustls::ClientConfig::builder_with_provider(super::provider())
            .with_safe_default_protocol_versions()
            .expect("versions")
            .with_root_certificates(super::ca_roots(harness.ca.certificate_pem()).expect("roots"))
            .with_no_client_auth();

        let (server_saw, client_saw) = handshake(acceptor, bare).await;
        assert!(server_saw.is_err(), "no certificate must not be accepted");
        assert!(client_saw.is_err(), "the refusal reaches the client too");
    }

    #[tokio::test]
    async fn a_certificate_from_a_different_ca_is_refused() {
        let ours = harness();
        let stranger = harness(); // a second, unrelated CA
        let (foreign_identity, _) = enrolled(&stranger, "impostor");
        // The impostor trusts *our* server (same CA pem for roots) but holds
        // a foreign certificate.
        let foreign = StoredIdentity {
            ca_pem: ours.ca.certificate_pem().to_owned(),
            ..foreign_identity
        };

        let acceptor = mtls_acceptor(
            &ours.credential,
            ours.ca.certificate_pem(),
            RevocationList::new(),
        )
        .expect("an acceptor builds");
        let client = client_mtls_config(&foreign).expect("a client config builds");

        let (server_saw, client_saw) = handshake(acceptor, client).await;
        assert!(server_saw.is_err(), "a foreign certificate must be refused");
        assert!(client_saw.is_err());
    }

    #[tokio::test]
    async fn a_revoked_certificate_is_refused_at_the_handshake() {
        let harness = harness();
        let (identity, serial_hex) = enrolled(&harness, "mac-studio");

        let deny = RevocationList::new();
        deny.revoke(serial_hex);
        let acceptor = mtls_acceptor(&harness.credential, harness.ca.certificate_pem(), deny)
            .expect("an acceptor builds");
        let client = client_mtls_config(&identity).expect("a client config builds");

        let (server_saw, client_saw) = handshake(acceptor, client).await;
        assert!(server_saw.is_err(), "a revoked serial must be refused");
        assert!(client_saw.is_err());
    }

    /// The proof `ca::tests::a_loaded_ca_issues_under_the_same_root` defers
    /// to: a certificate issued by a *reloaded* CA completes a handshake
    /// against the originally persisted root.
    #[tokio::test]
    async fn a_reloaded_ca_still_authenticates_its_peers() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let harness = harness();
        harness.ca.persist(directory.path()).expect("persists");
        let reloaded = CertificateAuthority::load(directory.path()).expect("loads");

        let reloaded_harness = Harness {
            ca: reloaded,
            credential: harness.credential.clone(),
        };
        let (identity, _) = enrolled(&reloaded_harness, "after-restart");

        let acceptor = mtls_acceptor(
            &reloaded_harness.credential,
            harness.ca.certificate_pem(), // the ORIGINAL root
            RevocationList::new(),
        )
        .expect("an acceptor builds");
        let client = client_mtls_config(&identity).expect("a client config builds");

        let (server_saw, client_saw) = handshake(acceptor, client).await;
        assert_eq!(client_saw, Ok(()));
        assert_eq!(
            server_saw.expect("an identity").principal.to_string(),
            "worker:after-restart",
            "issuance must survive a Server restart without re-enrolling anyone"
        );
    }

    #[tokio::test]
    async fn the_pinned_verifier_admits_exactly_the_pinned_certificate() {
        let harness = harness();
        let fingerprint = harness.credential.fingerprint().expect("a fingerprint");

        let pinned = |pin: &str| {
            rustls::ClientConfig::builder_with_provider(super::provider())
                .with_safe_default_protocol_versions()
                .expect("versions")
                .dangerous()
                .with_custom_certificate_verifier(pinned_verifier(pin))
                .with_no_client_auth()
        };

        // The enrollment listener requires no client certificate; model that
        // with a server-auth-only acceptor.
        let server_only = |harness: &Harness| {
            let config = rustls::ServerConfig::builder_with_provider(super::provider())
                .with_safe_default_protocol_versions()
                .expect("versions")
                .with_no_client_auth()
                .with_single_cert(
                    super::pem_certificates(&harness.credential.certificate_pem, "cert")
                        .expect("certs"),
                    super::pem_key(&harness.credential.key_pem, "key").expect("key"),
                )
                .expect("a server config builds");
            tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config))
        };

        let (server_saw, client_saw) = handshake(server_only(&harness), pinned(&fingerprint)).await;
        assert_eq!(client_saw, Ok(()), "the right pin must admit the server");
        assert!(
            server_saw.is_err(),
            "no client certificate exists on this channel, and peer_of_stream must say so"
        );

        let wrong_pin = "b".repeat(64);
        let (_, refused) = handshake(server_only(&harness), pinned(&wrong_pin)).await;
        assert!(
            refused.is_err(),
            "a wrong pin must refuse the server before anything is sent"
        );
    }
}
