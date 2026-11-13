//! Diagnostic logs, worker half (ADR-0015).
//!
//! The worker keeps two families of files under `<state>/logs`: its own
//! tracing records, and each harness's stderr in `agents/`. It serves reads
//! of both to the server — and only to the server, on a stream it dialed
//! itself: the server pushes one [`LogQuery`] per read on
//! `StreamLogRequests`, and the worker answers each on its own
//! `WorkerLogTunnel`. Nothing about a read outlives its streams, nothing is
//! journaled, and every record is redacted before it leaves (the shared
//! serving loop in `horde_wire::logs`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use horde_observe::agents::{AgentLogs, AgentStderr};
use horde_observe::reader::{AGENTS_DIR, LocalLogs, Target};
use horde_observe::settings::AgentLogSettings;
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::{
    LogEndReason, LogQuery, LogTunnelOpen, StreamLogRequestsRequest, WorkerId,
    WorkerLogTunnelRequest, worker_log_tunnel_request,
};
use horde_wire::logs::{Frame, LocalRead, end, serve_local};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::error::WorkerError;

/// The prefix of the worker's own log files.
pub const BINARY: &str = "horde-worker";

/// Where a worker's logs live: `<state>/logs`.
#[must_use]
pub fn log_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("logs")
}

/// What a worker serves log reads from: its own files (followable when it
/// is the process writing them) and its agents' stderr files.
#[derive(Debug, Clone)]
pub struct LogServing {
    local: LocalLogs,
    agents: Arc<AgentLogs>,
    /// Reads being served right now; each ends with its tunnel.
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
}

/// Counts one read as in flight until dropped.
struct Serving(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for Serving {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl LogServing {
    /// The logs under `dir`. The tap, when given, is the running process's
    /// — the one its file layer and its agents' stderr both publish to.
    #[must_use]
    pub fn new(dir: &Path, boot: &str, tap: Option<horde_observe::tap::LogTap>) -> Self {
        Self {
            local: LocalLogs::new(dir, BINARY, tap.clone()),
            agents: AgentLogs::new(dir.join(AGENTS_DIR), AgentLogSettings::default(), boot, tap),
            in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// How many reads this worker is serving right now — zero once every
    /// tunnel has ended, whichever side ended it.
    #[must_use]
    pub fn reads_in_flight(&self) -> usize {
        self.in_flight.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn serving(&self) -> Serving {
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Serving(Arc::clone(&self.in_flight))
    }

    /// The running daemon's logs: `<state>/logs`, stamped with the boot of
    /// the file logging the binary installed.
    #[must_use]
    pub fn for_daemon(state_dir: &Path) -> Self {
        let process = horde_observe::layer::process_logging();
        let boot = process.map_or_else(horde_observe::layer::new_boot, |logging| {
            logging.boot().to_owned()
        });
        Self::new(
            &log_dir(state_dir),
            &boot,
            process.map(|logging| logging.tap().clone()),
        )
    }

    /// The worker's own records and its agents' stderr, as one directory.
    #[must_use]
    pub fn local(&self) -> &LocalLogs {
        &self.local
    }

    /// The stderr writer for one agent's harness.
    #[must_use]
    pub fn agent_stderr(&self, agent: &str, session: &str) -> AgentStderr {
        self.agents.writer(agent, session)
    }
}

/// An agent's stderr file as the harness seam's sink.
struct StderrFile(AgentStderr);

impl horde_adapter::stderr::StderrSink for StderrFile {
    fn line(&self, text: &str) {
        self.0.line(text);
    }
}

/// The sink a spawn or revival is scoped with, when this worker keeps logs.
#[must_use]
pub fn stderr_sink(
    serving: Option<&LogServing>,
    agent: &str,
    session: &str,
) -> Option<Arc<dyn horde_adapter::stderr::StderrSink>> {
    serving.map(|serving| {
        Arc::new(StderrFile(serving.agent_stderr(agent, session)))
            as Arc<dyn horde_adapter::stderr::StderrSink>
    })
}

/// Serves log reads over one connection until the server ends the stream.
///
/// A server that predates log serving answers `Unimplemented`; that is not
/// this connection's failure, so the plane simply goes quiet.
///
/// # Errors
///
/// The dispatch stream's failure, reconnection being the daemon's policy.
pub async fn serve(
    mut client: WorkerServiceClient<Channel>,
    worker_id: &str,
    serving: Arc<LogServing>,
) -> Result<(), WorkerError> {
    let opened = client
        .stream_log_requests(StreamLogRequestsRequest {
            worker_id: Some(WorkerId::new(worker_id)),
        })
        .await;
    let mut stream = match opened {
        Ok(response) => response.into_inner(),
        Err(status) if status.code() == tonic::Code::Unimplemented => {
            tracing::warn!("the server does not serve log reads; this worker's logs stay local");
            return std::future::pending().await;
        }
        Err(status) => return Err(status.into()),
    };
    while let Some(update) = stream.message().await? {
        if let Some(query) = update.query {
            // Detached: a read ends when its own tunnel does.
            tokio::spawn(answer(client.clone(), query, Arc::clone(&serving)));
        }
    }
    Ok(())
}

fn tunnel_frame(frame: Frame) -> WorkerLogTunnelRequest {
    WorkerLogTunnelRequest {
        frame: Some(match frame {
            Frame::Records(records) => worker_log_tunnel_request::Frame::Records(records),
            Frame::Gap(gap) => worker_log_tunnel_request::Frame::Gap(gap),
            Frame::End(end) => worker_log_tunnel_request::Frame::End(end),
        }),
    }
}

/// The read a query describes.
///
/// # Errors
///
/// An unparseable filter or cursor, in the words the end frame carries.
pub fn read_of(query: &LogQuery) -> Result<LocalRead, String> {
    let target = match query.agent.as_ref().filter(|agent| !agent.value.is_empty()) {
        Some(agent) => Target::Agent(agent.value.clone()),
        None => Target::Process,
    };
    LocalRead::from_wire(
        target,
        &query.filter.clone().unwrap_or_default(),
        query.session_scope.clone(),
        query.follow,
        query.after.as_ref(),
        query.max_bytes,
    )
}

/// Answers one query on its own tunnel, until the read is done or either
/// side drops the tunnel.
async fn answer(
    mut client: WorkerServiceClient<Channel>,
    query: LogQuery,
    serving: Arc<LogServing>,
) {
    let _serving = serving.serving();
    let (to_server, outbound) = mpsc::channel(16);
    let open = WorkerLogTunnelRequest {
        frame: Some(worker_log_tunnel_request::Frame::Open(LogTunnelOpen {
            request_id: query.request_id.clone(),
        })),
    };
    if to_server.send(open).await.is_err() {
        return;
    }
    let Ok(response) = client
        .worker_log_tunnel(ReceiverStream::new(outbound))
        .await
    else {
        return;
    };
    let mut cancelled = response.into_inner();
    tracing::info!(
        target: horde_observe::tap::QUIET_TARGET,
        request_id = query.request_id,
        follow = query.follow,
        "serving a log read"
    );
    let read = match read_of(&query) {
        Ok(read) => read,
        Err(detail) => {
            let _ = to_server
                .send(tunnel_frame(end(LogEndReason::Unspecified, detail)))
                .await;
            return;
        }
    };
    let (frames, mut produced) = mpsc::channel(16);
    let serve = async move {
        serve_local(serving.local(), read, &frames).await;
    };
    // Owns the sending half, so finishing closes the request side.
    let forward = async move {
        while let Some(frame) = produced.recv().await {
            if to_server.send(tunnel_frame(frame)).await.is_err() {
                return;
            }
        }
    };
    // The server never sends; its stream ending is the order to stop — and,
    // after a finished read, the sign that everything sent was received.
    // Dropping the response early would reset the stream and lose the tail.
    let watch = async { while let Ok(Some(_)) = cancelled.message().await {} };
    tokio::pin!(watch);
    tokio::select! {
        ((), ()) = async { tokio::join!(serve, forward) } => {
            let _ = tokio::time::timeout(DRAIN_DEADLINE, &mut watch).await;
        }
        () = &mut watch => {}
    }
}

/// How long a finished read waits for the server to take its last frames.
const DRAIN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

#[cfg(test)]
mod tests {
    use horde_observe::reader::Target;
    use horde_proto::v1::{AgentId, LogFilter, LogQuery};

    use super::{LogServing, log_dir, read_of, serve, stderr_sink};

    #[test]
    fn a_workers_logs_live_under_its_state() {
        assert_eq!(
            log_dir(std::path::Path::new("/state")),
            std::path::PathBuf::from("/state/logs")
        );
    }

    fn client(
        endpoint: &str,
    ) -> horde_proto::v1::worker_service_client::WorkerServiceClient<tonic::transport::Channel>
    {
        horde_proto::v1::worker_service_client::WorkerServiceClient::new(
            tonic::transport::Endpoint::from_shared(endpoint.to_owned())
                .expect("an endpoint")
                .connect_lazy(),
        )
    }

    /// A server that predates log serving answers every log RPC
    /// `Unimplemented`; the plane goes quiet rather than failing the
    /// connection.
    #[tokio::test]
    async fn a_server_without_log_serving_leaves_the_plane_quiet() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_routes(tonic::service::Routes::default())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let serving = std::sync::Arc::new(LogServing::new(
            tempfile::tempdir().expect("tempdir").path(),
            "boot",
            None,
        ));
        let quiet = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            serve(client(&format!("http://{address}")), "w1", serving),
        )
        .await;
        assert!(quiet.is_err(), "it should still be waiting: {quiet:?}");
        server.abort();
    }

    /// Any other refusal is this connection's failure, for the daemon to
    /// redial.
    #[tokio::test]
    async fn an_unreachable_server_fails_the_plane() {
        let serving = std::sync::Arc::new(LogServing::new(
            tempfile::tempdir().expect("tempdir").path(),
            "boot",
            None,
        ));
        let failed = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            serve(client("http://127.0.0.1:1"), "w1", serving),
        )
        .await
        .expect("a dead server is refused promptly");
        assert!(failed.is_err());
    }

    #[test]
    fn a_query_reads_the_worker_or_one_agent() {
        let process = read_of(&LogQuery::default()).expect("process");
        assert_eq!(process.target, Target::Process);
        let agent = read_of(&LogQuery {
            agent: Some(AgentId::new("a1")),
            session_scope: Some("s1".to_owned()),
            max_bytes: 100,
            ..LogQuery::default()
        })
        .expect("agent");
        assert_eq!(agent.target, Target::Agent("a1".to_owned()));
        assert_eq!(agent.filter.session_scope.as_deref(), Some("s1"));
        assert_eq!(agent.max_bytes, Some(100));
        let blank = read_of(&LogQuery {
            agent: Some(AgentId::new("")),
            ..LogQuery::default()
        })
        .expect("blank agent");
        assert_eq!(blank.target, Target::Process);
        let bad = read_of(&LogQuery {
            filter: Some(LogFilter {
                level: "loud".to_owned(),
                ..LogFilter::default()
            }),
            ..LogQuery::default()
        });
        assert!(bad.is_err());
    }

    #[test]
    fn a_sink_exists_exactly_when_the_worker_keeps_logs() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(stderr_sink(None, "a1", "s1").is_none());
        let serving = LogServing::new(dir.path(), "boot", None);
        let sink = stderr_sink(Some(&serving), "a1", "s1").expect("a sink");
        sink.line("to the file");
        let records = serving.local().records(&Target::Agent("a1".to_owned()));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].message(), "to the file");
    }
}
