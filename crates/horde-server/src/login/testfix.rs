//! Scripted fixtures the login suites share — the relay tests and the
//! service-driver tests both play a leg's peer through these, kept here so
//! neither restates them (the duplication gate's rule applies to test code
//! too, exactly as `forward::testfix` says).

use horde_proto::v1::LoginFrame;
use tonic::Status;

use super::relay::LoginLegIo;

/// A [`LoginLegIo`] the test plays the peer of: feed the returned sender,
/// read the returned receiver.
pub(crate) fn scripted_leg() -> (
    LoginLegIo,
    tokio::sync::mpsc::Sender<Result<LoginFrame, Status>>,
    tokio::sync::mpsc::Receiver<LoginFrame>,
) {
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(8);
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(8);
    let io = LoginLegIo {
        inbound: Box::pin(tokio_stream::wrappers::ReceiverStream::new(inbound_rx)),
        outbound: outbound_tx,
    };
    (io, inbound_tx, outbound_rx)
}

/// One scripted leg's handles: its feed, its outbound, its task.
pub(crate) type ScriptedPeer = (
    tokio::sync::mpsc::Sender<Result<LoginFrame, Status>>,
    tokio::sync::mpsc::Receiver<LoginFrame>,
    tokio::task::JoinHandle<()>,
);

/// The worker tunnel driver run as `caller` over a scripted leg — the
/// shared opening of every tunnel-answering test, in both login suites.
pub(crate) fn answering_tunnel(
    logins: &super::registry::LoginRegistry,
    caller: crate::forward::registry::TunnelPrincipal,
) -> ScriptedPeer {
    let (io, feed, out) = scripted_leg();
    let logins = logins.clone();
    let task = tokio::spawn(async move {
        super::relay::run_worker_login_tunnel(&logins, caller, io).await;
    });
    (feed, out, task)
}
