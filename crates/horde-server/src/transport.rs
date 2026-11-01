//! The Server's listeners: mTLS for every principal, TLS for the one door
//! that mints principals.
//!
//! This is the wave-5 moment DESIGN §3 sequences: before it, nothing bound
//! beyond localhost because no auth existed; from it, the networked listeners
//! exist *because* every one of them authenticates. Two listeners:
//!
//! * **main** — both gRPC spines (control plane and worker-facing), mTLS
//!   required, every RPC passing the trust door (`crate::trust::door`) so a revoked certificate
//!   stops working at its next call — and the long-lived streams re-check
//!   the same deny-list themselves, so revocation severs them too rather
//!   than waiting for a reconnect;
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

use horde_proto::v1::blob_service_server::BlobServiceServer;
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

/// How often the server pings an idle HTTP/2 connection (ADR-0011).
pub const KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(20);

/// How long a ping may go unanswered before the connection is declared dead.
pub const KEEPALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Binds the main mTLS listener: both spines, every RPC behind the enrolled
/// door.
///
/// # Errors
///
/// The bind failing, or tonic refusing the TLS material.
pub async fn serve_main(
    prebound: Prebound,
    core: ServerCore,
    authority: Arc<EnrollmentAuthority>,
) -> Result<BoundListener, ServerError> {
    let deny = authority.deny_list();
    let tls = horde_identity::tls::grpc_server_mtls(authority.credential(), authority.ca_pem());
    let Prebound { listener, addr } = prebound;

    // Every guarded service sits behind the trust door (ADR-0009): enrolled
    // and unrevoked, *and* of a role the called method admits.
    let control_plane = ControlPlaneServiceServer::with_interceptor(
        ControlPlane::with_enrollment(core.clone(), authority),
        crate::trust::door(deny.clone()),
    );
    // Blob transfer rides the same listener and the same enrolled door:
    // workers and clients both move attachments, so the service is
    // principal-agnostic rather than duplicated onto each spine.
    let blobs = BlobServiceServer::with_interceptor(
        crate::service::Blobs::new(core.blobs.clone()),
        crate::trust::door(deny.clone()),
    );
    let worker_facing = tonic::service::interceptor::InterceptedService::new(
        // The deny-list rides into the service too: the per-RPC interceptor
        // cannot reach a stream already in flight, so the streams re-check it
        // themselves and are severed on revocation (fix a4).
        worker_service(WorkerFacing::new(core).with_deny(deny.clone())),
        crate::trust::door(deny),
    );

    // HTTP/2 keepalive (ADR-0011). Without it a peer that vanished without a
    // FIN — a laptop lid, a pulled cable, a NAT that forgot the mapping —
    // leaves a half-open connection the server never notices: its streams'
    // senders never close, the worker stays registered, and its agents stay
    // placed on a machine nobody can reach. An unanswered ping tears the
    // connection down, which closes every stream on it and lets
    // `sender.closed()` (and so the worker-lost lease) see the death within
    // `KEEPALIVE_INTERVAL + KEEPALIVE_TIMEOUT`.
    let router = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .tls_config(tls)
        .map_err(|e| ServerError::Internal(format!("mTLS configuration refused: {e}")))?
        // The door rules by method path, which only this layer can see.
        .layer(tower::util::MapRequestLayer::new(
            crate::trust::stamp_rpc_path::<tonic::body::Body>,
        ))
        .add_service(control_plane)
        .add_service(blobs)
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

/// The worker spine with the message limits every Worker client agrees to
/// ([`horde_proto::limits`]) — set here deliberately rather than inherited
/// from tonic's silent 4 MiB, which refused an oversized push batch with an
/// opaque status the Worker could only redial into.
#[must_use]
pub fn worker_service(service: WorkerFacing) -> WorkerServiceServer<WorkerFacing> {
    WorkerServiceServer::new(service)
        .max_decoding_message_size(horde_proto::limits::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(horde_proto::limits::MAX_MESSAGE_BYTES)
}

/// Binds the enrollment listener: `Join` alone, server-auth TLS.
///
/// # Errors
///
/// The bind failing, or tonic refusing the TLS material.
pub async fn serve_enrollment(
    prebound: Prebound,
    authority: Arc<EnrollmentAuthority>,
) -> Result<BoundListener, ServerError> {
    let tls = horde_identity::tls::grpc_server_tls(authority.credential());
    let Prebound { listener, addr } = prebound;

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

/// A bound-but-not-yet-serving listener.
///
/// Binding is split from serving (ADR-0007) because each listener's service
/// advertises the *other* listener's port — the mint answer names the
/// enrollment port, the join answer the control-plane port — so both real
/// addresses must exist before either service is built. A `:0` bind is only
/// resolved by the OS at bind time, which is why the split is here and not a
/// startup-ordering convention.
#[derive(Debug)]
pub struct Prebound {
    listener: tokio::net::TcpListener,
    pub addr: SocketAddr,
}

/// Binds and reports the address the OS actually chose.
///
/// # Errors
///
/// The bind failing.
pub async fn prebind(bind: SocketAddr) -> Result<Prebound, ServerError> {
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| ServerError::Internal(format!("binding {bind}: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| ServerError::Internal(e.to_string()))?;
    Ok(Prebound { listener, addr })
}

#[cfg(test)]
mod tests {
    // Everything here is bind-and-serve plumbing whose observable behavior
    // is a connection outcome, and connection outcomes have no meaningful
    // unit below "both ends of a socket agree" — the same judgment the
    // worker's dial made. The listeners are exercised end-to-end, refusals
    // included, in `tests/enrollment.rs`.
}
