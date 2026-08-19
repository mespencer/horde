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
