//! Diagnostic logs, server half (ADR-0015).
//!
//! `ControlPlane/ReadLogs` serves three targets: the server's own files,
//! a worker's (through the worker log stream pair), and an agent's (its
//! placement's worker, filtered to it and merged with its harness stderr).
//! Nothing here is journaled and nothing here decides anything: a log read
//! is ephemeral intent, living exactly as long as its streams, and logs are
//! read only by people and agents.
//!
//! # Who may read what (ADR-0009)
//!
//! A client may read every target: Horde is single-user and a client is
//! the operator. A worker caller is the agent conduit: it must name, in
//! `on_behalf_of`, an agent placed on it; it may read the server, any
//! worker, and agents of that agent's own session; and every record naming
//! another session is dropped before it leaves the machine that wrote it.
//! A worker caller's reads are clamped and rate-limited per agent.

pub mod registry;

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use horde_observe::limits::{RateBuckets, retry_after_secs};
use horde_observe::reader::{LocalLogs, Target, egress_lines, scope_admits};
use horde_observe::record::Record;
use horde_observe::settings::AgentReadLimits;
use horde_proto::v1::{LogEndReason, LogQuery, ReadLogsRequest, read_logs_request};
use horde_wire::logs::{Frame, LocalRead, end, filter_from_wire, serve_local};
use tokio::sync::mpsc;
use tonic::Status;

use crate::service::shared::ServerCore;
use registry::{LaneState, LogRegistry};

/// The prefix of the server's own log files.
pub const BINARY: &str = "horde-server";

/// How long a dispatched read waits for its worker's tunnel.
pub const TUNNEL_DEADLINE: Duration = Duration::from_secs(10);

/// The frames buffered between a read's source and its client.
const FRAME_DEPTH: usize = 16;

/// The server's log plane: its own files, the workers' lanes, and the
/// agent read limits.
#[derive(Debug, Clone)]
pub struct LogPlane {
    /// The server's own log directory, when it keeps one (the deployed
    /// binary does; in-process harnesses may not).
    pub local: Option<LocalLogs>,
    pub registry: LogRegistry,
    pub buckets: Arc<RateBuckets>,
}

impl Default for LogPlane {
    fn default() -> Self {
        Self {
            local: None,
            registry: LogRegistry::new(),
            buckets: Arc::new(RateBuckets::new(AgentReadLimits::default())),
        }
    }
}

/// Where one read resolved to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    Server,
    Worker {
        worker_id: String,
        agent: Option<String>,
    },
}

/// A read the caller may make, and what it is held to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorized {
    pub target: Resolved,
    /// Records naming another session are dropped (agent callers).
    pub session_scope: Option<String>,
    /// Zero is unbounded (clients only).
    pub max_bytes: u32,
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

/// The refusal past the rate limit: a code the endpoint turns into a tool
/// error the model can read, with the wait in both the text and the
/// `retry-after` metadata.
#[must_use]
pub fn rate_limited(agent: &str, wait: Duration) -> Status {
    let secs = retry_after_secs(wait);
    let mut status = Status::resource_exhausted(format!(
        "agent {agent} is reading logs too often; retry after {secs}s"
    ));
    if let Ok(value) = secs.to_string().parse() {
        status.metadata_mut().insert("retry-after", value);
    }
    status
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

/// Relays a worker's frames to the client: the session scope and the byte
/// cap re-applied, every line redacted again for the server's own secrets,
/// and a worker that vanishes mid-read reported as such.
pub async fn relay(
    mut from: mpsc::Receiver<Frame>,
    to: mpsc::Sender<Frame>,
    scope: Option<String>,
    cap: Option<usize>,
) {
    let mut used = 0;
    loop {
        let frame = tokio::select! {
            frame = from.recv() => frame,
            () = to.closed() => return,
        };
        let frame = match frame {
            None => end(
                LogEndReason::WorkerDisconnected,
                "the worker disconnected mid-read",
            ),
            Some(Frame::Records(mut records)) => {
                if let Some(scope) = scope.as_deref() {
                    records.lines.retain(|line| {
                        Record::parse(line).is_some_and(|record| scope_admits(&record, Some(scope)))
                    });
                }
                let again = egress_lines(records.lines.iter().map(String::as_str));
                records.redacted += again.redacted;
                records.lines = again.lines;
                used += records.lines.iter().map(String::len).sum::<usize>();
                if cap.is_some_and(|cap| used > cap) {
                    end(LogEndReason::ByteCap, "the byte cap was reached")
                } else {
                    Frame::Records(records)
                }
            }
            Some(other) => other,
        };
        let ended = matches!(frame, Frame::End(_));
        if to.send(frame).await.is_err() || ended {
            return;
        }
    }
}

#[cfg(test)]
pub(crate) mod testfix;
#[cfg(test)]
mod tests;
