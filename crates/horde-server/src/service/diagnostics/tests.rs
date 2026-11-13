//! `ReadLogs` through the production handler: who may read what, the
//! agent read limits, and the relay's re-applied scope (ADR-0015).

use std::time::{Duration, Instant};

use horde_observe::reader::LocalLogs;
use horde_observe::settings::LogSettings;
use horde_proto::v1::{
    AgentId, LogEndReason, LogFilter, LogRecords, ReadLogsRequest, ReadLogsResponse, ServerTarget,
    control_plane_service_server::ControlPlaneService, read_logs_request, read_logs_response,
};
use horde_wire::logs::Frame;
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tonic::{Code, Request};

use super::authorize;
use crate::diagnostics::{Authorized, BINARY, Resolved, relay};
use crate::log::EventLog;
use crate::service::control_plane::ControlPlane;
use crate::service::control_plane::delivery_tests::as_worker;
use crate::service::control_plane::list_tests::titled;
use crate::service::shared::ServerCore;

/// A core whose server log holds records of two sessions and one
/// process-level record, written by the real file layer.
async fn core_with_logs(dir: &std::path::Path) -> (ControlPlane, ServerCore, String, String) {
    let logging = horde_observe::FileLogging::start(
        dir,
        BINARY,
        &LogSettings::daemon(),
        &horde_observe::ServiceIdentity::new(BINARY),
    )
    .expect("start");
    let core = ServerCore::new(EventLog::in_memory().expect("log"))
        .with_local_logs(LocalLogs::new(dir, BINARY, None));
    let plane = ControlPlane::new(core.clone());
    let (mine, theirs) = (
        titled(&plane, "mine", "logs-mine").await,
        titled(&plane, "theirs", "logs-theirs").await,
    );
    logging.with_default(|| {
        tracing::info!(target: "horde_server", session = mine.as_str(), "mine");
        tracing::info!(target: "horde_server", session = theirs.as_str(), "theirs");
        tracing::info!(target: "horde_server", "process-level");
    });
    assert!(logging.flush(Duration::from_secs(5)));
    crate::service::ingest::placed_for_test(&core, &mine, "agent-mine", "w1").await;
    crate::service::ingest::placed_for_test(&core, &theirs, "agent-theirs", "w2").await;
    (plane, core, mine, theirs)
}

fn server_read(on_behalf_of: Option<&str>) -> ReadLogsRequest {
    ReadLogsRequest {
        target: Some(read_logs_request::Target::Server(ServerTarget {})),
        filter: Some(LogFilter {
            grep: "mine|theirs|process".to_owned(),
            ..LogFilter::default()
        }),
        on_behalf_of: on_behalf_of.map(AgentId::new),
        ..ReadLogsRequest::default()
    }
}

async fn messages(plane: &ControlPlane, request: Request<ReadLogsRequest>) -> Vec<String> {
    let mut stream = plane
        .read_logs(request)
        .await
        .expect("admitted")
        .into_inner();
    let mut messages = Vec::new();
    while let Some(Ok(ReadLogsResponse { body: Some(body) })) = stream.next().await {
        if let read_logs_response::Body::Records(records) = body {
            for line in records.lines {
                let record = horde_observe::record::Record::parse(&line).expect("one record");
                messages.push(record.message().to_owned());
            }
        }
    }
    messages
}

#[tokio::test]
async fn a_client_reads_everything_and_an_agent_only_its_session() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (plane, _core, _mine, _theirs) = core_with_logs(dir.path()).await;
    assert_eq!(
        messages(&plane, Request::new(server_read(None))).await,
        ["mine", "theirs", "process-level"]
    );
    assert_eq!(
        messages(
            &plane,
            as_worker("w1", Request::new(server_read(Some("agent-mine"))))
        )
        .await,
        ["mine", "process-level"],
        "another session's records never reach an agent caller"
    );
}

#[tokio::test]
async fn a_worker_must_read_for_one_of_its_own_agents() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (plane, _core, _mine, _theirs) = core_with_logs(dir.path()).await;
    for (worker, behalf, code) in [
        ("w1", None, Code::PermissionDenied),
        ("w1", Some("agent-theirs"), Code::PermissionDenied),
        ("w1", Some("agent-nobody"), Code::NotFound),
    ] {
        let refused = plane
            .read_logs(as_worker(worker, Request::new(server_read(behalf))))
            .await
            .err()
            .map(|status| status.code());
        assert_eq!(refused, Some(code), "{behalf:?}");
    }
    let foreign_agent = ReadLogsRequest {
        target: Some(read_logs_request::Target::Agent(AgentId::new(
            "agent-theirs",
        ))),
        ..server_read(Some("agent-mine"))
    };
    let refused = plane
        .read_logs(as_worker("w1", Request::new(foreign_agent)))
        .await
        .err()
        .map(|status| status.code());
    assert_eq!(
        refused,
        Some(Code::PermissionDenied),
        "another session's agent"
    );
}

#[tokio::test]
async fn the_byte_clamp_and_rate_limit_hold_for_workers_but_not_clients() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_plane, core, mine, _theirs) = core_with_logs(dir.path()).await;
    let now = Instant::now();
    let huge = ReadLogsRequest {
        max_bytes: 10_000_000,
        ..server_read(Some("agent-mine"))
    };
    let client = authorize(&core, None, &huge, now).await.expect("client");
    assert_eq!(client.max_bytes, 10_000_000);
    assert_eq!(client.session_scope, None);
    let worker = authorize(&core, Some("w1"), &huge, now)
        .await
        .expect("worker");
    assert_eq!(worker.max_bytes, 64 * 1024);
    assert_eq!(worker.session_scope.as_deref(), Some(mine.as_str()));
    let default = authorize(&core, Some("w1"), &server_read(Some("agent-mine")), now)
        .await
        .expect("worker");
    assert_eq!(default.max_bytes, 16 * 1024);
    assert!(
        authorize(&core, Some("w1"), &huge, now).await.is_ok(),
        "the burst's third"
    );
    let limited = authorize(&core, Some("w1"), &huge, now).await.err();
    let limited = limited.expect("the fourth call in a burst of three is refused");
    assert_eq!(limited.code(), Code::ResourceExhausted);
    assert!(
        limited.message().contains("retry after 10s"),
        "{}",
        limited.message()
    );
    assert_eq!(
        limited
            .metadata()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("10")
    );
    for _ in 0..10 {
        assert!(
            authorize(&core, None, &huge, now).await.is_ok(),
            "clients are not limited"
        );
    }
    let later = now + Duration::from_secs(10);
    assert!(
        authorize(&core, Some("w1"), &huge, later).await.is_ok(),
        "the bucket refills"
    );
}

#[tokio::test]
async fn targets_resolve_or_refuse_by_what_the_server_knows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (plane, core, _mine, _theirs) = core_with_logs(dir.path()).await;
    let agent = ReadLogsRequest {
        target: Some(read_logs_request::Target::Agent(AgentId::new("agent-mine"))),
        ..ReadLogsRequest::default()
    };
    let resolved = authorize(&core, None, &agent, Instant::now())
        .await
        .expect("placed");
    assert_eq!(
        resolved.target,
        Resolved::Worker {
            worker_id: "w1".to_owned(),
            agent: Some("agent-mine".to_owned())
        }
    );
    let untargeted = authorize(&core, None, &ReadLogsRequest::default(), Instant::now()).await;
    assert_eq!(
        untargeted.err().map(|s| s.code()),
        Some(Code::InvalidArgument)
    );

    let worker = |name: &str| ReadLogsRequest {
        target: Some(read_logs_request::Target::Worker(name.to_owned())),
        ..ReadLogsRequest::default()
    };
    let code = |request: ReadLogsRequest| {
        let plane = plane.clone();
        async move {
            plane
                .read_logs(Request::new(request))
                .await
                .err()
                .map(|s| s.code())
        }
    };
    assert_eq!(
        code(worker("w1")).await,
        Some(Code::Unavailable),
        "never connected"
    );
    core.logs.registry.note_registration("w-old", false);
    assert_eq!(code(worker("w-old")).await, Some(Code::FailedPrecondition));
    let (epoch, _lane) = core.logs.registry.register_worker("w-gone");
    core.logs
        .registry
        .deregister_worker_if_current("w-gone", epoch, std::time::SystemTime::now());
    let away = plane.read_logs(Request::new(worker("w-gone"))).await.err();
    let away = away.expect("away");
    assert_eq!(away.code(), Code::Unavailable);
    assert!(away.message().contains("last seen"), "{}", away.message());
    assert!(away.message().contains("--dir"), "{}", away.message());

    let bad_grep = ReadLogsRequest {
        filter: Some(LogFilter {
            grep: "(".to_owned(),
            ..LogFilter::default()
        }),
        ..server_read(None)
    };
    assert_eq!(code(bad_grep).await, Some(Code::InvalidArgument));

    let bare = ControlPlane::new(ServerCore::new(EventLog::in_memory().expect("log")));
    let refused = bare.read_logs(Request::new(server_read(None))).await.err();
    assert_eq!(
        refused.map(|s| s.code()),
        Some(Code::Unavailable),
        "no log files"
    );
    // The authorized shape is a plain value; this pins its fields' meaning.
    assert_eq!(
        Authorized {
            target: Resolved::Server,
            session_scope: None,
            max_bytes: 0
        }
        .max_bytes,
        0
    );
}

fn records(lines: &[&str]) -> Frame {
    Frame::Records(LogRecords {
        lines: lines.iter().map(|line| (*line).to_owned()).collect(),
        ..LogRecords::default()
    })
}

async fn relayed(frames: Vec<Frame>, scope: Option<&str>, cap: Option<usize>) -> Vec<Frame> {
    let (worker, from) = mpsc::channel(16);
    let (to, mut client) = mpsc::channel(16);
    for frame in frames {
        worker.send(frame).await.expect("queued");
    }
    drop(worker);
    relay(from, to, scope.map(str::to_owned), cap).await;
    let mut out = Vec::new();
    while let Some(frame) = client.recv().await {
        out.push(frame);
    }
    out
}

#[tokio::test]
async fn the_relay_reapplies_scope_and_redaction_and_reports_a_vanished_worker() {
    let mine = r#"{"ts":"t1","msg":"ok","session":"s1"}"#;
    let theirs = r#"{"ts":"t2","msg":"no","session":"s2"}"#;
    let secret = r#"{"ts":"t3","msg":"horde://join/h:1/a.b"}"#;
    let out = relayed(
        vec![records(&[mine, theirs, secret, "torn"])],
        Some("s1"),
        None,
    )
    .await;
    let [Frame::Records(batch), Frame::End(end)] = out.as_slice() else {
        panic!("records then an end: {out:?}");
    };
    assert_eq!(batch.lines.len(), 2, "{:?}", batch.lines);
    assert_eq!(batch.lines[0], mine);
    assert!(batch.lines[1].contains("[REDACTED:join-token]"));
    assert_eq!(batch.redacted, 1);
    assert_eq!(end.reason, LogEndReason::WorkerDisconnected as i32);

    let unscoped = relayed(vec![records(&[theirs, "torn"])], None, None).await;
    assert!(matches!(&unscoped[0], Frame::Records(batch) if batch.lines.len() == 2));

    let capped = relayed(
        vec![records(&[mine]), records(&[mine])],
        None,
        Some(mine.len() + 1),
    )
    .await;
    assert!(
        matches!(capped.as_slice(), [Frame::Records(_), Frame::End(end)] if end.reason == LogEndReason::ByteCap as i32),
        "{capped:?}"
    );

    let finished = relayed(
        vec![
            horde_wire::logs::end(LogEndReason::Done, ""),
            records(&[mine]),
        ],
        None,
        None,
    )
    .await;
    assert_eq!(finished.len(), 1, "nothing is relayed past an end");
}
