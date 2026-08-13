//! The secured harness: the wave-5 server shape, real TLS on real sockets.
//!
//! [`crate::serve`] stands up the plaintext loopback harness that pre-mTLS
//! suites arrange around; this stands up the *secured* one — CA, enrollment
//! listener, mTLS main listener — using only production components
//! (`EnrollmentAuthority`, `transport::serve_main`/`serve_enrollment`, the
//! `join` flow). Identities exist here exclusively by being issued through
//! the production path: a mint through the authority, a join over the real
//! enrollment listener. There are no fixture certificates anywhere in this
//! workspace to rot.

use std::sync::Arc;

use horde_identity::{JoinCode, Principal, PrincipalRole, StoredIdentity};
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::{Hello, Role};
use horde_server::service::enrollment::{EnrollmentAuthority, wall_clock_ms};
use horde_server::transport::BoundListener;
use horde_server::{EventLog, ServerCore};

use crate::Setup;

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
    let log = EventLog::in_memory()?;
    let now = wall_clock_ms();
    let ca = horde_identity::CertificateAuthority::generate(now)?;
    let credential = ca.issue_server(
        now,
        &["localhost".to_owned()],
        &[std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
    )?;
    let authority = Arc::new(EnrollmentAuthority::new(log.clone(), ca, credential).await?);

    let loopback = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
    let main = horde_server::transport::serve_main(
        loopback,
        ServerCore::new(log.clone()),
        Arc::clone(&authority),
    )
    .await?;
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
