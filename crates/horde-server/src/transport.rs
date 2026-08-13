//! The Server's listeners: mTLS for every principal, TLS for the one door
//! that mints principals.
//!
//! This is the wave-5 moment DESIGN §3 sequences: before it, nothing bound
//! beyond localhost because no auth existed; from it, the networked listeners
//! exist *because* every one of them authenticates. Two listeners:
//!
//! * **main** — both gRPC spines (control plane and worker-facing), mTLS
//!   required, every RPC passing `require_enrolled` so a revoked certificate
//!   stops working at its next call;
//! * **enrollment** — `EnrollmentService` alone, server-auth only, because a
//!   joining machine has no certificate yet by definition. A separate
//!   listener rather than optional client auth on the main one: "may present
//!   a certificate" is a weaker property than "must", and the main listener
//!   keeps the stronger one.
//!
//! This module owns binding and TLS termination only. What the services do
//! with the injected identity is service logic and lives with the services.

use std::net::SocketAddr;
use std::sync::Arc;

use horde_proto::v1::control_plane_service_server::ControlPlaneServiceServer;
use horde_proto::v1::enrollment_service_server::EnrollmentServiceServer;
use horde_proto::v1::worker_service_server::WorkerServiceServer;
use tokio_stream::wrappers::TcpListenerStream;

use crate::error::ServerError;
use crate::service::enrollment::{Enrollment, EnrollmentAuthority};
use crate::service::{ControlPlane, ServerCore, WorkerFacing};

/// A listener that is up, and how to reach and stop it.
///
/// Aborting the task stops accepting; in-flight RPCs are cut with it, which
/// is the same lifetime every harness task in this workspace has.
#[derive(Debug)]
pub struct BoundListener {
    pub addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for BoundListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Binds the main mTLS listener: both spines, every RPC behind the enrolled
/// door.
///
/// # Errors
///
/// The bind failing, or tonic refusing the TLS material.
pub async fn serve_main(
    bind: SocketAddr,
    core: ServerCore,
    authority: Arc<EnrollmentAuthority>,
) -> Result<BoundListener, ServerError> {
    let deny = authority.deny_list();
    let tls = horde_identity::tls::grpc_server_mtls(authority.credential(), authority.ca_pem());
    let (listener, addr) = bound(bind).await?;

    let control_plane = ControlPlaneServiceServer::with_interceptor(
        ControlPlane::with_enrollment(core.clone(), authority),
        horde_identity::require_enrolled(deny.clone()),
    );
    let worker_facing = WorkerServiceServer::with_interceptor(
        WorkerFacing::new(core),
        horde_identity::require_enrolled(deny),
    );

    let router = tonic::transport::Server::builder()
        .tls_config(tls)
        .map_err(|e| ServerError::Internal(format!("mTLS configuration refused: {e}")))?
        .add_service(control_plane)
        .add_service(worker_facing);

    let task = tokio::spawn(async move {
        if let Err(error) = router
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
        {
            tracing::error!(%error, "the main listener stopped");
        }
    });

    Ok(BoundListener { addr, task })
}

/// Binds the enrollment listener: `Join` alone, server-auth TLS.
///
/// # Errors
///
/// The bind failing, or tonic refusing the TLS material.
pub async fn serve_enrollment(
    bind: SocketAddr,
    authority: Arc<EnrollmentAuthority>,
) -> Result<BoundListener, ServerError> {
    let tls = horde_identity::tls::grpc_server_tls(authority.credential());
    let (listener, addr) = bound(bind).await?;

    let router = tonic::transport::Server::builder()
        .tls_config(tls)
        .map_err(|e| ServerError::Internal(format!("TLS configuration refused: {e}")))?
        .add_service(EnrollmentServiceServer::new(Enrollment::new(authority)));

    let task = tokio::spawn(async move {
        if let Err(error) = router
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
        {
            tracing::error!(%error, "the enrollment listener stopped");
        }
    });

    Ok(BoundListener { addr, task })
}

/// Binds and reports the address the OS actually chose.
async fn bound(bind: SocketAddr) -> Result<(tokio::net::TcpListener, SocketAddr), ServerError> {
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| ServerError::Internal(format!("binding {bind}: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| ServerError::Internal(e.to_string()))?;
    Ok((listener, addr))
}

#[cfg(test)]
mod tests {
    // Everything here is bind-and-serve plumbing whose observable behavior
    // is a connection outcome, and connection outcomes have no meaningful
    // unit below "both ends of a socket agree" — the same judgment the
    // worker's dial made. The listeners are exercised end-to-end, refusals
    // included, in `tests/enrollment.rs`.
}
