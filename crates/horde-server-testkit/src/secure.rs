//! The secured harness: the wave-5 server shape, real TLS on real sockets.
//!
//! [`crate::serve`] stands up the plaintext loopback harness that pre-mTLS
//! suites arrange around; this stands up the *secured* one — CA, enrollment
//! listener, mTLS main listener — using only production components
//! (`EnrollmentAuthority`, `transport::serve_main`/`serve_enrollment`, the
//! `join` flow). Identities exist here exclusively by being issued through
//! the production path: a mint through the authority, a join over the real
//! enrollment listener. There are no fixture certificates anywhere in this
//! workspace to rot — the process-wide trust root below
//! reuses what the production path issued, never a baked artifact.

use std::sync::{Arc, OnceLock};

use horde_identity::{
    CertificateAuthority, JoinCode, Principal, PrincipalRole, ServerCredential, StoredIdentity,
};
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::{Hello, Role};
use horde_server::service::enrollment::{EnrollmentAuthority, wall_clock_ms};
use horde_server::transport::BoundListener;
use horde_server::{EventLog, ServerCore};

use crate::Setup;

/// A future bounded by a fifteen-second ceiling, failing with `what`.
///
/// The one wait helper every secured suite shares: a refused handshake must
/// fail in milliseconds, and a test that would instead hang has found a
/// worse bug than the one it was looking for.
///
/// # Errors
///
/// The ceiling elapsing, named.
pub async fn within<T>(what: &str, future: impl Future<Output = T>) -> Setup<T> {
    tokio::time::timeout(std::time::Duration::from_secs(15), future)
        .await
        .map_err(|_| format!("timed out waiting for {what}").into())
}

/// A tonic client TLS config that trusts `ca_pem` and presents nothing —
/// the state every peer is in before its first join.
#[must_use]
pub fn tls_trusting_ca(ca_pem: &str) -> tonic::transport::ClientTlsConfig {
    tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca_pem))
        .domain_name("localhost")
}

/// A secured server, and the levers a suite needs around it.
///
/// The authority handle is deliberately exposed: minting is a control-plane
/// verb behind mTLS, which a suite cannot call before its first identity
/// exists — the same bootstrap the real server solves by minting on its own
/// stdout, solved here by minting on its own handle.
pub struct SecureHarness {
    pub authority: Arc<EnrollmentAuthority>,
    pub log: EventLog,
    main: BoundListener,
    enrollment: BoundListener,
}

impl std::fmt::Debug for SecureHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecureHarness")
            .field("main", &self.main)
            .field("enrollment", &self.enrollment)
            .finish_non_exhaustive()
    }
}

/// Starts a secured server on OS-chosen loopback ports.
///
/// # Errors
///
/// Any failure to build the CA, the log, or either listener.
pub async fn serve_secured() -> Setup<SecureHarness> {
    serve_secured_with_blobs(None).await
}

/// [`serve_secured`], with a real blob store rooted at `blob_root` — the
/// full wave-5 shape plus wave-6's attachment transfer, for suites proving
/// blobs move over the authenticated channel. The caller owns the
/// directory, exactly as [`crate::serve_with_blob_store`] has it.
///
/// # Errors
///
/// As [`serve_secured`], plus the store refusing to open.
pub async fn serve_secured_with_blob_store(blob_root: &std::path::Path) -> Setup<SecureHarness> {
    let store = horde_server::blob::BlobStore::open(blob_root)?;
    serve_secured_with_blobs(Some(Arc::new(store))).await
}

/// The process-wide trust root every secured harness shares.
///
/// Generated once per test process through the production path — a real
/// [`CertificateAuthority::generate`], persisted with the same `persist` a
/// first server boot uses — and reloaded per harness through
/// [`CertificateAuthority::load`], the production restart path. One root per
/// process instead of one per test because every secured test repeats the
/// mint otherwise. Suites whose subject is the trust material itself — a
/// foreign CA, a fingerprint mismatch — build their own CA and are
/// untouched by this.
struct TrustRoot {
    /// Held for the process's life so the CA files stay loadable.
    directory: tempfile::TempDir,
    /// The one localhost listener credential every harness presents. Its
    /// fields are cloned out rather than the struct because
    /// [`ServerCredential`] is a pair of PEM strings without a `Clone`.
    certificate_pem: String,
    key_pem: String,
}

fn trust_root() -> Setup<&'static TrustRoot> {
    static ROOT: OnceLock<Result<TrustRoot, String>> = OnceLock::new();
    ROOT.get_or_init(|| {
        let now = wall_clock_ms();
        let ca = CertificateAuthority::generate(now).map_err(|e| e.to_string())?;
        let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
        ca.persist(directory.path()).map_err(|e| e.to_string())?;
        let credential = ca.issue_localhost_server(now).map_err(|e| e.to_string())?;
        Ok(TrustRoot {
            directory,
            certificate_pem: credential.certificate_pem,
            key_pem: credential.key_pem,
        })
    })
    .as_ref()
    .map_err(|error| format!("the shared trust root failed to build: {error}").into())
}

async fn serve_secured_with_blobs(
    blobs: Option<Arc<horde_server::blob::BlobStore>>,
) -> Setup<SecureHarness> {
    let log = EventLog::in_memory()?;
    let root = trust_root()?;
    let ca = CertificateAuthority::load(root.directory.path())?;
    let credential = ServerCredential {
        certificate_pem: root.certificate_pem.clone(),
        key_pem: root.key_pem.clone(),
    };
    let authority = Arc::new(EnrollmentAuthority::new(log.clone(), ca, credential).await?);

    let mut core = ServerCore::new(log.clone());
    if let Some(store) = blobs {
        core = core.with_blob_store(store);
    }

    let loopback = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
    let main = horde_server::transport::serve_main(loopback, core, Arc::clone(&authority)).await?;
    let enrollment =
        horde_server::transport::serve_enrollment(loopback, Arc::clone(&authority)).await?;

    Ok(SecureHarness {
        authority,
        log,
        main,
        enrollment,
    })
}

/// The `Hello` a joining peer introduces itself with.
#[must_use]
pub fn hello(role: PrincipalRole, display_name: &str) -> Hello {
    Hello {
        semver: "0.1.0".to_owned(),
        capability_flags: Vec::new(),
        role: match role {
            PrincipalRole::Worker => Role::Worker,
            PrincipalRole::Client => Role::Client,
        } as i32,
        display_name: display_name.to_owned(),
    }
}

impl SecureHarness {
    /// The mTLS endpoint both spines share.
    #[must_use]
    pub fn main_endpoint(&self) -> String {
        format!("https://localhost:{}", self.main.addr.port())
    }

    /// The TLS-only enrollment endpoint.
    #[must_use]
    pub fn enrollment_endpoint(&self) -> String {
        format!("https://localhost:{}", self.enrollment.addr.port())
    }

    /// Mints a code and redeems it over the real enrollment listener —
    /// the production issuance path, end to end.
    ///
    /// # Errors
    ///
    /// The mint, the join, or the code round-trip failing.
    pub async fn enroll(&self, role: PrincipalRole, name: &str) -> Setup<StoredIdentity> {
        let principal = Principal::new(role, name)?;
        let minted = self.authority.mint(&principal, 0, wall_clock_ms()).await?;
        let code = JoinCode::parse(&minted.join_code()).ok_or("the minted join code must parse")?;
        let enrolled =
            horde_identity::join(&self.enrollment_endpoint(), &code, hello(role, name)).await?;
        Ok(enrolled.identity)
    }

    /// A tonic endpoint dialing the main listener as `identity`.
    ///
    /// # Errors
    ///
    /// tonic refusing the endpoint or the TLS material.
    pub fn main_endpoint_as(&self, identity: &StoredIdentity) -> Setup<tonic::transport::Endpoint> {
        Ok(
            tonic::transport::Endpoint::from_shared(self.main_endpoint())?
                .tls_config(horde_identity::tls::grpc_client_mtls(identity, "localhost"))?,
        )
    }

    /// A connected control-plane client speaking as `identity`.
    ///
    /// # Errors
    ///
    /// The dial failing — including the server refusing the certificate.
    pub async fn client(&self, identity: &StoredIdentity) -> Setup<horde_cli::Client> {
        let channel = self.main_endpoint_as(identity)?.connect().await?;
        Ok(horde_cli::Client::with_channel(channel))
    }

    /// A connected worker-spine client speaking as `identity`.
    ///
    /// # Errors
    ///
    /// The dial failing — including the server refusing the certificate.
    pub async fn worker_client(
        &self,
        identity: &StoredIdentity,
    ) -> Setup<WorkerServiceClient<tonic::transport::Channel>> {
        let channel = self.main_endpoint_as(identity)?.connect().await?;
        Ok(WorkerServiceClient::new(channel))
    }
}
