//! The diagnostic-log RPCs' service glue (ADR-0015), shared by both spines
//! the way [`super::logins`] is: `ReadLogs` on the control plane, and the
//! worker log stream pair. The decisions live in [`crate::diagnostics`].

use std::pin::Pin;
use std::time::SystemTime;

use horde_proto::v1::{
    ReadLogsRequest, ReadLogsResponse, StreamLogRequestsResponse, WorkerLogTunnelRequest,
    WorkerLogTunnelResponse, read_logs_response, worker_log_tunnel_request,
};
use horde_wire::logs::Frame;
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use super::shared::ServerCore;
use crate::diagnostics::registry::Claim;

/// The `ReadLogs` response stream.
pub type ReadLogsStream = Pin<Box<dyn Stream<Item = Result<ReadLogsResponse, Status>> + Send>>;

/// A read's frame as the client sees it.
#[must_use]
pub fn to_response(frame: Frame) -> ReadLogsResponse {
    ReadLogsResponse {
        body: Some(match frame {
            Frame::Records(records) => read_logs_response::Body::Records(records),
            Frame::Gap(gap) => read_logs_response::Body::Gap(gap),
            Frame::End(end) => read_logs_response::Body::End(end),
        }),
    }
}

/// A worker tunnel frame as a read's frame; `None` for an open frame out of
/// place, or an empty one.
#[must_use]
pub fn from_tunnel(frame: WorkerLogTunnelRequest) -> Option<Frame> {
    match frame.frame? {
        worker_log_tunnel_request::Frame::Records(records) => Some(Frame::Records(records)),
        worker_log_tunnel_request::Frame::Gap(gap) => Some(Frame::Gap(gap)),
        worker_log_tunnel_request::Frame::End(end) => Some(Frame::End(end)),
        worker_log_tunnel_request::Frame::Open(_) => None,
    }
}

/// Serves one `ReadLogs`: authorize, open, and lift the frames into the
/// response stream.
///
/// # Errors
///
/// [`crate::diagnostics::authorize`]'s and [`crate::diagnostics::open`]'s
/// refusals.
pub async fn read_logs(
    core: &ServerCore,
    request: Request<ReadLogsRequest>,
) -> Result<Response<ReadLogsStream>, Status> {
    let conduit = crate::trust::conduit_worker(&request);
    let request = request.into_inner();
    let authorized = crate::diagnostics::authorize(
        core,
        conduit.as_deref(),
        &request,
        std::time::Instant::now(),
    )
    .await?;
    let frames = crate::diagnostics::open(core, authorized, &request).await?;
    let (sender, receiver) = mpsc::channel(16);
    super::shared::lift_frames(frames, sender, to_response);
    Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
}

/// The `StreamLogRequests` response stream.
pub type LogRequestsStream =
    Pin<Box<dyn Stream<Item = Result<StreamLogRequestsResponse, Status>> + Send>>;

/// Serves one worker's dispatch stream until either side ends it.
pub fn stream_log_requests(
    core: &ServerCore,
    worker_id: String,
    deny: Option<horde_identity::RevocationList>,
    serial: Option<String>,
) -> LogRequestsStream {
    let (epoch, mut lane) = core.logs.registry.register_worker(&worker_id);
    let (sender, receiver) = mpsc::channel(16);
    let registry = core.logs.registry.clone();
    tokio::spawn(async move {
        let pump = async {
            while let Some(query) = lane.recv().await {
                let response = StreamLogRequestsResponse { query: Some(query) };
                if sender.send(Ok(response)).await.is_err() {
                    return;
                }
            }
        };
        super::shared::run_severable(pump, deny, serial, &sender).await;
        registry.deregister_worker_if_current(&worker_id, epoch, SystemTime::now());
    });
    Box::pin(ReceiverStream::new(receiver))
}

/// The `WorkerLogTunnel` response stream: nothing is ever sent on it, and
/// its end is the worker's order to stop.
pub type LogTunnelStream =
    Pin<Box<dyn Stream<Item = Result<WorkerLogTunnelResponse, Status>> + Send>>;

/// Serves one worker log tunnel: the first frame claims a pending read,
/// then frames flow to that read until either side goes away.
///
/// # Errors
///
/// `InvalidArgument` for a tunnel that does not open with its request id;
/// `NotFound` for a read nobody is waiting on; `PermissionDenied` for a
/// read pending for another worker.
pub async fn worker_log_tunnel(
    core: &ServerCore,
    request: Request<Streaming<WorkerLogTunnelRequest>>,
) -> Result<Response<LogTunnelStream>, Status> {
    let caller = crate::trust::conduit_worker(&request);
    let mut inbound = request.into_inner();
    let request_id = match inbound.message().await? {
        Some(WorkerLogTunnelRequest {
            frame: Some(worker_log_tunnel_request::Frame::Open(open)),
        }) => open.request_id,
        _ => {
            return Err(Status::invalid_argument(
                "a log tunnel opens with the request id it answers",
            ));
        }
    };
    let pair = match core.logs.registry.claim(&request_id, caller.as_deref()) {
        Claim::Matched(pair) => pair,
        Claim::WrongWorker => {
            return Err(Status::permission_denied(format!(
                "log read {request_id} was not asked of this worker"
            )));
        }
        Claim::Unknown => {
            return Err(Status::not_found(format!(
                "no log read {request_id} is waiting"
            )));
        }
    };
    let (frames, reader) = mpsc::channel(16);
    if pair.send(reader).is_err() {
        return Err(Status::cancelled("the reader went away"));
    }
    let (keepalive, responses) = mpsc::channel::<Result<WorkerLogTunnelResponse, Status>>(1);
    tokio::spawn(async move {
        // Held for the pump's life: dropping it ends the worker's response
        // stream, which is the worker's order to stop serving.
        let _keepalive = keepalive;
        loop {
            let message = tokio::select! {
                message = inbound.message() => message,
                () = frames.closed() => return,
            };
            let Ok(Some(message)) = message else {
                return;
            };
            if let Some(frame) = from_tunnel(message)
                && frames.send(frame).await.is_err()
            {
                return;
            }
        }
    });
    Ok(Response::new(Box::pin(ReceiverStream::new(responses))))
}
