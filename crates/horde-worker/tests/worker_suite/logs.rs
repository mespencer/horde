//! Log serving (ADR-0015) through a real server and a real worker over
//! loopback: the query → tunnel → records path, an agent's harness stderr
//! arriving redacted, follow, a worker vanishing mid-follow, and a worker
//! that predates log serving.

use std::sync::Arc;
use std::time::Duration;

use horde_adapter_fake::{FakeAdapter, Script, ScriptedTurn, Step};
use horde_observe::FileLogging;
use horde_observe::record::Record;
use horde_observe::settings::LogSettings;
use horde_proto::v1::{
    AgentId, LogEndReason, LogFilter, ReadLogsRequest, read_logs_request, read_logs_response,
};
use horde_server::EventLog;
use horde_server::service::ServerCore;
use horde_worker::logs::LogServing;
use horde_worker::{DialConfig, Journal, Runtime, capabilities};

use crate::support::{place_agent, serve, server_log_at_least};

fn config(endpoint: &str, runtime: &Runtime, serves_logs: bool) -> DialConfig {
    DialConfig {
        endpoint: endpoint.to_owned(),
        worker_id: "worker-a".to_owned(),
        hello: capabilities::hello_serving_logs("worker-a", serves_logs),
        capabilities: capabilities::advertise(
            runtime.adapters(),
            None,
            &capabilities::DeclaredFacts::default(),
        ),
        identity: None,
        last_local_journal_sequence: None,
    }
}

/// Dials and runs a worker that serves `logs` when given.
async fn run_worker(
    config: &DialConfig,
    journal: &Journal,
    runtime: Runtime,
    logs: Option<Arc<LogServing>>,
) -> tokio::task::JoinHandle<Result<(), horde_worker::WorkerError>> {
    let (mut connection, _) = horde_worker::Connection::dial(config)
        .await
        .expect("the dial succeeds");
    let journal = journal.clone();
    tokio::spawn(async move {
        let mut runtime = runtime;
        let logins = horde_worker::login::LoginPlane::discarding(runtime.adapters());
        connection
            .run_with_logs(&journal, &mut runtime, logins, logs)
            .await
    })
}

/// Opens a read through the server, retrying while the worker's dispatch
/// stream is still subscribing.
async fn open(
    endpoint: &str,
    request: ReadLogsRequest,
) -> Result<tonic::Streaming<horde_proto::v1::ReadLogsResponse>, tonic::Status> {
    within("the read opens", async {
        let mut client = horde_cli::client::Client::connect(endpoint)
            .await
            .expect("the client connects");
        let mut last = None;
        for _ in 0..100 {
            match client.read_logs(request.clone()).await {
                Ok(stream) => return Ok(stream),
                Err(horde_cli::client::ClientError::Rpc(status))
                    if status.code() == tonic::Code::Unavailable =>
                {
                    last = Some(status);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(horde_cli::client::ClientError::Rpc(status)) => return Err(status),
                Err(other) => panic!("the read could not be opened: {other}"),
            }
        }
        Err(last.unwrap_or_else(|| tonic::Status::unknown("never answered")))
    })
    .await
}

/// Every record line and the end frame's reason, reading to the end.
async fn drain(
    mut stream: tonic::Streaming<horde_proto::v1::ReadLogsResponse>,
) -> (Vec<Record>, Option<i32>) {
    within("the read ends", async {
        let mut records = Vec::new();
        while let Ok(Some(response)) = stream.message().await {
            match response.body {
                Some(read_logs_response::Body::Records(batch)) => {
                    records.extend(batch.lines.iter().filter_map(|line| Record::parse(line)));
                }
                Some(read_logs_response::Body::End(end)) => return (records, Some(end.reason)),
                _ => {}
            }
        }
        (records, None)
    })
    .await
}

/// `future`, or a failure naming `what` once it has taken too long: every
/// wait on a stream here is bounded, so a read that never answers fails in
/// seconds instead of hanging the suite.
async fn within<T>(what: &str, future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .unwrap_or_else(|_| panic!("{what}: timed out"))
}

fn agent_read(agent: &str, source: &str) -> ReadLogsRequest {
    ReadLogsRequest {
        target: Some(read_logs_request::Target::Agent(AgentId::new(agent))),
        filter: Some(LogFilter {
            source: source.to_owned(),
            ..LogFilter::default()
        }),
        ..ReadLogsRequest::default()
    }
}

#[tokio::test]
async fn an_agents_harness_stderr_reaches_the_client_redacted() {
    let core = ServerCore::new(EventLog::in_memory().expect("log"));
    let endpoint = serve(core.clone()).await;
    let dir = tempfile::tempdir().expect("tempdir");
    place_agent(&core.log, "s1", "agent-1", "worker-a", "go").await;

    let journal = Journal::open(&dir.path().join("journal.sqlite")).expect("journal");
    let logs = Arc::new(LogServing::new(&dir.path().join("logs"), "boot-a", None));
    let script = Script::of(vec![ScriptedTurn::saying("done").with_step(Step::Stderr(
        "warning: joined via horde://join/h:7/s3cr3t.fp".to_owned(),
    ))]);
    let runtime = Runtime::new(journal.clone())
        .with_adapter(Arc::new(FakeAdapter::new(script)))
        .with_logs(Arc::clone(&logs));
    let config = config(&endpoint, &runtime, true);
    let run = run_worker(&config, &journal, runtime, Some(logs)).await;
    // Starting, established, the output chunk and usage: the turn ran.
    server_log_at_least(&core.log, "s1", 6).await;

    let (records, end) = drain(
        open(&endpoint, agent_read("agent-1", "stderr"))
            .await
            .expect("read"),
    )
    .await;
    assert_eq!(end, Some(LogEndReason::Done as i32));
    assert_eq!(records.len(), 1, "{records:?}");
    let line = &records[0];
    assert_eq!(line.text("stream"), Some("stderr"));
    assert_eq!(line.text("agent"), Some("agent-1"));
    assert_eq!(line.text("session"), Some("s1"));
    assert_eq!(line.message(), "warning: joined via [REDACTED:join-token]");
    assert!(
        !line.line.contains("s3cr3t"),
        "the secret never left the worker"
    );
    run.abort();
}

/// A worker's own records written by the real file layer, the way the
/// daemon's global subscriber writes them.
fn worker_records(dir: &std::path::Path) -> FileLogging {
    let logging = FileLogging::start(
        dir,
        horde_worker::logs::BINARY,
        &LogSettings::daemon(),
        &horde_observe::ServiceIdentity::new(horde_worker::logs::BINARY),
    )
    .expect("start");
    emit(&logging, "materialized repo before");
    logging
}

fn emit(logging: &FileLogging, message: &str) {
    logging.with_default(|| {
        tracing::info!(target: "horde_worker", "{message}");
    });
    assert!(logging.flush(Duration::from_secs(5)));
}

/// A worker dialing `endpoint` that serves the logs its own file layer
/// writes under `dir`, followable through that layer's tap.
async fn logging_worker(
    dir: &std::path::Path,
    endpoint: &str,
) -> (
    FileLogging,
    Arc<LogServing>,
    tokio::task::JoinHandle<Result<(), horde_worker::WorkerError>>,
) {
    let log_dir = dir.join("logs");
    let logging = worker_records(&log_dir);
    let journal = Journal::open(&dir.join("journal.sqlite")).expect("journal");
    let logs = Arc::new(LogServing::new(
        &log_dir,
        logging.boot(),
        Some(logging.tap().clone()),
    ));
    let runtime = Runtime::new(journal.clone()).with_logs(Arc::clone(&logs));
    let config = config(endpoint, &runtime, true);
    let run = run_worker(&config, &journal, runtime, Some(Arc::clone(&logs))).await;
    (logging, logs, run)
}

fn worker_read(follow: bool) -> ReadLogsRequest {
    ReadLogsRequest {
        target: Some(read_logs_request::Target::Worker("worker-a".to_owned())),
        filter: Some(LogFilter {
            grep: "materialized".to_owned(),
            ..LogFilter::default()
        }),
        follow,
        ..ReadLogsRequest::default()
    }
}

#[tokio::test]
async fn a_followed_worker_log_sees_new_records_and_ends_when_the_worker_vanishes() {
    let core = ServerCore::new(EventLog::in_memory().expect("log"));
    let upstream = serve(core.clone()).await;
    let proxy = horde_testkit::FaultProxy::front(&upstream)
        .await
        .expect("the fault proxy binds");
    let dir = tempfile::tempdir().expect("tempdir");
    let (logging, _logs, _run) = logging_worker(dir.path(), &proxy.endpoint()).await;

    let (records, end) = drain(open(&upstream, worker_read(false)).await.expect("read")).await;
    assert_eq!(end, Some(LogEndReason::Done as i32));
    assert_eq!(
        records.iter().map(Record::message).collect::<Vec<_>>(),
        ["materialized repo before"]
    );

    let mut follow = open(&upstream, worker_read(true)).await.expect("follow");
    let first = within("the initial records", follow.message())
        .await
        .expect("a frame")
        .expect("records");
    assert!(
        matches!(first.body, Some(read_logs_response::Body::Records(batch)) if batch.lines.len() == 1)
    );
    emit(&logging, "materialized repo after");
    let next = within("the followed record", follow.message())
        .await
        .expect("a frame")
        .expect("the new record");
    let Some(read_logs_response::Body::Records(batch)) = next.body else {
        panic!("the followed record, not {next:?}");
    };
    assert!(batch.lines[0].contains("materialized repo after"));

    proxy.cut();
    let ended = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match follow.message().await {
                Ok(Some(response)) => {
                    if let Some(read_logs_response::Body::End(end)) = response.body {
                        return Some(end.reason);
                    }
                }
                _ => return None,
            }
        }
    })
    .await
    .expect("the follow ends once the worker is gone");
    assert_eq!(ended, Some(LogEndReason::WorkerDisconnected as i32));
}

#[tokio::test]
async fn a_client_dropping_its_follow_ends_the_workers_read() {
    let core = ServerCore::new(EventLog::in_memory().expect("log"));
    let endpoint = serve(core.clone()).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let (_logging, logs, run) = logging_worker(dir.path(), &endpoint).await;

    let mut follow = open(&endpoint, worker_read(true)).await.expect("follow");
    within("the initial records", follow.message())
        .await
        .expect("a frame")
        .expect("the initial records");
    assert_eq!(logs.reads_in_flight(), 1, "the follow is being served");
    drop(follow);
    let mut polls = 0;
    while logs.reads_in_flight() != 0 {
        polls += 1;
        assert!(polls < 200, "the worker kept serving a read nobody reads");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    run.abort();
}

#[tokio::test]
async fn a_worker_that_predates_log_serving_is_refused_not_waited_on() {
    let core = ServerCore::new(EventLog::in_memory().expect("log"));
    let endpoint = serve(core.clone()).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open(&dir.path().join("journal.sqlite")).expect("journal");
    let runtime = Runtime::new(journal.clone());
    let config = config(&endpoint, &runtime, false);
    let run = run_worker(&config, &journal, runtime, None).await;
    let refused = open(&endpoint, worker_read(false)).await.err();
    assert_eq!(
        refused.map(|status| status.code()),
        Some(tonic::Code::FailedPrecondition)
    );
    run.abort();
}
