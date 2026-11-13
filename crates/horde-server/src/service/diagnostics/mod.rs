//! The diagnostic-log RPCs' service glue (ADR-0015), shared by both spines
//! the way [`super::logins`] is: `ReadLogs` on the control plane, and the
//! worker log stream pair. The decisions live in [`crate::diagnostics`].

use std::ops::ControlFlow;
use std::pin::Pin;
use std::time::{Instant, SystemTime};

use horde_observe::reader::Target;
use horde_proto::v1::{
    LogQuery, ReadLogsRequest, ReadLogsResponse, StreamLogRequestsResponse, WorkerLogTunnelRequest,
    WorkerLogTunnelResponse, read_logs_request, read_logs_response, worker_log_tunnel_request,
};
use horde_wire::logs::{Frame, LocalRead, filter_from_wire, serve_local};
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use super::shared::ServerCore;
use crate::diagnostics::registry::{Claim, LaneState, LogRegistry};
use crate::diagnostics::{Authorized, FRAME_DEPTH, Resolved, TUNNEL_DEADLINE, rate_limited, relay};

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
/// [`authorize`]'s and [`open`]'s
/// refusals.
pub async fn read_logs(
    core: &ServerCore,
    request: Request<ReadLogsRequest>,
) -> Result<Response<ReadLogsStream>, Status> {
    let conduit = crate::trust::conduit_worker(&request);
    let request = request.into_inner();
    let authorized = authorize(core, conduit.as_deref(), &request, Instant::now()).await?;
    let frames = open(core, authorized, &request).await?;
    let (sender, receiver) = mpsc::channel(16);
    tokio::spawn(lift_until_gone(frames, sender));
    Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
}

/// Lifts a read's frames into the response stream until either side ends.
///
/// Unlike the relays' shared lift, this one watches the client too: an idle
/// follow sends nothing for minutes, and a lift that only notices a departed
/// client on its next send would keep the read — and the worker's tunnel —
/// alive for nobody.
pub async fn lift_until_gone(
    mut frames: mpsc::Receiver<Frame>,
    client: mpsc::Sender<Result<ReadLogsResponse, Status>>,
) {
    loop {
        let frame = tokio::select! {
            frame = frames.recv() => frame,
            () = client.closed() => return,
        };
        let Some(frame) = frame else { return };
        if client.send(Ok(to_response(frame))).await.is_err() {
            return;
        }
    }
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

/// An agent's session and the worker it is placed on.
async fn locate(core: &ServerCore, agent_id: &str) -> Result<(String, Option<String>), Status> {
    core.sessions
        .walk(crate::enrollment::is_session_stream, |state| {
            match state.agents.get(agent_id) {
                Some(agent) => {
                    ControlFlow::Break((state.session_id.clone(), agent.worker_id.clone()))
                }
                None => ControlFlow::Continue(()),
            }
        })
        .await
        .map_err(Status::from)?
        .ok_or_else(|| Status::not_found(format!("no agent {agent_id:?}")))
}

fn agent_of(id: Option<&horde_proto::v1::AgentId>) -> String {
    id.map(|id| id.value.clone()).unwrap_or_default()
}

/// Decides whether a read may happen and what it is held to.
///
/// `conduit` is the worker a certificate names (`None` for a client, and
/// for a plaintext test channel). `now` drives the rate limit.
///
/// # Errors
///
/// `InvalidArgument` without a target; `PermissionDenied` for a worker
/// reading on nobody's behalf, for an agent placed elsewhere, or for
/// another session's agent; `NotFound` for an unknown agent;
/// `ResourceExhausted` (with `retry-after`) past the rate limit;
/// `FailedPrecondition` for an agent target placed nowhere.
pub async fn authorize(
    core: &ServerCore,
    conduit: Option<&str>,
    request: &ReadLogsRequest,
    now: Instant,
) -> Result<Authorized, Status> {
    let target = request.target.as_ref().ok_or_else(|| {
        Status::invalid_argument("name a target: the server, a worker, or an agent")
    })?;
    let (session_scope, max_bytes) = match conduit {
        None => (None, request.max_bytes),
        Some(worker_id) => {
            let session = conduit_scope(core, worker_id, request, target).await?;
            let reader = agent_of(request.on_behalf_of.as_ref());
            if let Err(wait) = core.logs.buckets.take(&reader, now) {
                return Err(rate_limited(&reader, wait));
            }
            (
                Some(session),
                core.logs.buckets.limits().clamp(request.max_bytes),
            )
        }
    };
    let target = match target {
        read_logs_request::Target::Server(_) => Resolved::Server,
        read_logs_request::Target::Worker(worker_id) => Resolved::Worker {
            worker_id: worker_id.clone(),
            agent: None,
        },
        read_logs_request::Target::Agent(agent) => {
            let (_, placed) = locate(core, &agent.value).await?;
            let worker_id = placed.ok_or_else(|| {
                Status::failed_precondition(format!(
                    "agent {} is not placed on any worker",
                    agent.value
                ))
            })?;
            Resolved::Worker {
                worker_id,
                agent: Some(agent.value.clone()),
            }
        }
    };
    Ok(Authorized {
        target,
        session_scope,
        max_bytes,
    })
}

/// A worker caller's session scope: the session of the agent it reads for,
/// which must be placed on it, and which an agent target must share.
async fn conduit_scope(
    core: &ServerCore,
    worker_id: &str,
    request: &ReadLogsRequest,
    target: &read_logs_request::Target,
) -> Result<String, Status> {
    let reader = agent_of(request.on_behalf_of.as_ref());
    if reader.is_empty() {
        return Err(Status::permission_denied(
            "a worker reads logs only on behalf of one of its agents (on_behalf_of)",
        ));
    }
    let (session, placed) = locate(core, &reader).await?;
    if !crate::trust::holds_agent(placed.as_deref(), worker_id) {
        return Err(Status::permission_denied(format!(
            "agent {reader} is not placed on worker {worker_id}, which may not read for it"
        )));
    }
    if let read_logs_request::Target::Agent(agent) = target {
        let (theirs, _) = locate(core, &agent.value).await?;
        if theirs != session {
            return Err(Status::permission_denied(format!(
                "agent {reader} may read the logs of its own session's agents only"
            )));
        }
    }
    Ok(session)
}

/// Opens an authorized read: the frames it produces, in order, ending with
/// an end frame.
///
/// # Errors
///
/// `InvalidArgument` for an unparseable filter or cursor; `Unavailable`
/// for a server without log files, a worker not connected (with when it
/// was last seen), or a tunnel that never came; `FailedPrecondition` for a
/// worker that predates log serving.
pub async fn open(
    core: &ServerCore,
    authorized: Authorized,
    request: &ReadLogsRequest,
) -> Result<mpsc::Receiver<Frame>, Status> {
    let filter = request.filter.clone().unwrap_or_default();
    // Validated here, so a bad pattern is refused before any worker is asked.
    filter_from_wire(&filter, authorized.session_scope.clone(), SystemTime::now())
        .map_err(Status::invalid_argument)?;
    let (sender, receiver) = mpsc::channel(FRAME_DEPTH);
    tracing::info!(
        target: horde_observe::tap::QUIET_TARGET,
        target_kind = ?authorized.target,
        follow = request.follow,
        scoped = authorized.session_scope.is_some(),
        "log read"
    );
    match authorized.target {
        Resolved::Server => {
            let local = core
                .logs
                .local
                .clone()
                .ok_or_else(|| Status::unavailable("this server keeps no log files to read"))?;
            let read = LocalRead::from_wire(
                Target::Process,
                &filter,
                authorized.session_scope,
                request.follow,
                request.after.as_ref(),
                authorized.max_bytes,
            )
            .map_err(Status::invalid_argument)?;
            tokio::spawn(async move { serve_local(&local, read, &sender).await });
        }
        Resolved::Worker { worker_id, agent } => {
            let query = LogQuery {
                request_id: String::new(),
                agent: agent.map(horde_proto::v1::AgentId::new),
                filter: Some(filter),
                follow: request.follow,
                after: request.after.clone(),
                max_bytes: authorized.max_bytes,
                session_scope: authorized.session_scope.clone(),
            };
            let frames = dispatch(&core.logs.registry, &worker_id, query).await?;
            let cap = usize::try_from(authorized.max_bytes)
                .ok()
                .filter(|cap| *cap != 0);
            tokio::spawn(relay(frames, sender, authorized.session_scope, cap));
        }
    }
    Ok(receiver)
}

/// Pushes `query` down `worker_id`'s lane and waits for its tunnel.
async fn dispatch(
    registry: &LogRegistry,
    worker_id: &str,
    mut query: LogQuery,
) -> Result<mpsc::Receiver<Frame>, Status> {
    let lane = match registry.lane(worker_id) {
        LaneState::Connected(lane) => lane,
        LaneState::Away(seen) => {
            let seen = seen
                .map(|at| format!(" (last seen {})", horde_observe::record::format_ts(at)))
                .unwrap_or_default();
            return Err(Status::unavailable(format!(
                "worker {worker_id} is not connected{seen}; read its files on that machine \
                 with `horde logs --dir`"
            )));
        }
        LaneState::Unsupported => {
            return Err(Status::failed_precondition(format!(
                "worker {worker_id} predates log serving (it does not advertise {})",
                horde_wire::logs::LOGS_CAPABILITY
            )));
        }
        LaneState::Unknown => {
            return Err(Status::unavailable(format!(
                "worker {worker_id} is not connected"
            )));
        }
    };
    let (request_id, paired) = registry.begin(worker_id);
    query.request_id.clone_from(&request_id);
    if lane.try_send(query).is_err() {
        registry.abandon(&request_id);
        return Err(Status::unavailable(format!(
            "worker {worker_id} is not taking log reads right now"
        )));
    }
    match tokio::time::timeout(TUNNEL_DEADLINE, paired).await {
        Ok(Ok(frames)) => Ok(frames),
        _ => {
            registry.abandon(&request_id);
            Err(Status::unavailable(format!(
                "worker {worker_id} did not open its log tunnel"
            )))
        }
    }
}

#[cfg(test)]
mod tests;
