//! `horde logs` run for real (ADR-0015): the binary against a served
//! control plane whose server log was written by the real file layer, and
//! `--dir` over the same files with no server at all.

#[cfg(test)]
mod harness {
    use std::time::Duration;

    use horde_observe::reader::LocalLogs;
    use horde_observe::settings::LogSettings;

    /// A served core whose server log holds three records, one carrying a
    /// join token.
    pub(crate) async fn served(dir: &std::path::Path) -> String {
        let logging = horde_observe::FileLogging::start(
            dir,
            horde_server::diagnostics::BINARY,
            &LogSettings::daemon(),
            &horde_observe::ServiceIdentity::new("horde-server"),
        )
        .expect("start");
        logging.with_default(|| {
            tracing::debug!(target: "horde_server", "placed agent quietly");
            tracing::info!(target: "horde_server", worker = "w1", "placed agent loudly");
            tracing::warn!(target: "horde_server", "minted horde://join/h:7/s3cr3t.fp");
        });
        assert!(logging.flush(Duration::from_secs(5)));
        let core = horde_server::service::ServerCore::new(
            horde_server::EventLog::in_memory().expect("log"),
        )
        .with_local_logs(LocalLogs::new(dir, horde_server::diagnostics::BINARY, None));
        horde_server_testkit::serve_core(core)
            .await
            .expect("the server binds")
    }

    /// Runs the binary to its exit, bounded: a `logs` invocation that never
    /// returns fails here in seconds (and is killed) instead of hanging.
    pub(crate) async fn horde(args: &[&str], home: &std::path::Path) -> std::process::Output {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            horde_server_testkit::otlp_scrubbed_run(
                env!("CARGO_BIN_EXE_horde"),
                args,
                &[("HOME", home.to_str().expect("utf-8 home"))],
            ),
        )
        .await
        .expect("the binary exits")
        .expect("the binary runs")
    }
}

#[tokio::test]
async fn the_binary_reads_the_server_log_filtered_and_redacted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let home = tempfile::tempdir().expect("home");
    let endpoint = harness::served(dir.path()).await;

    let json = harness::horde(
        &[
            "logs",
            "server",
            "--endpoint",
            &endpoint,
            "--level",
            "info",
            "--json",
            "--log-file",
        ],
        home.path(),
    )
    .await;
    assert!(
        json.status.success(),
        "{}",
        String::from_utf8_lossy(&json.stderr)
    );
    let stdout = String::from_utf8_lossy(&json.stdout);
    let records: Vec<serde_json::Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).expect("each line is one JSON record"))
        .collect();
    let messages: Vec<&str> = records.iter().filter_map(|r| r["msg"].as_str()).collect();
    assert!(messages.contains(&"placed agent loudly"), "{messages:?}");
    assert!(!messages.contains(&"placed agent quietly"), "below --level");
    assert!(
        messages.contains(&"minted [REDACTED:join-token]"),
        "redacted before it left the server: {messages:?}"
    );
    assert!(!stdout.contains("s3cr3t"));
    assert!(
        home.path().join(".horde/client/logs").is_dir(),
        "--log-file wrote the CLI's own log"
    );

    let human = harness::horde(
        &[
            "logs",
            "server",
            "--endpoint",
            &endpoint,
            "--grep",
            "loudly",
            "-n",
            "1",
        ],
        home.path(),
    )
    .await;
    let text = String::from_utf8_lossy(&human.stdout);
    assert_eq!(text.lines().count(), 1, "{text}");
    assert!(
        text.contains("INFO  horde_server placed agent loudly worker=w1"),
        "{text}"
    );
}

#[tokio::test]
async fn dir_reads_the_same_files_offline_and_unredacted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let home = tempfile::tempdir().expect("home");
    let _endpoint = harness::served(dir.path()).await;
    let output = harness::horde(
        &[
            "logs",
            "--dir",
            dir.path().to_str().expect("utf-8"),
            "--grep",
            "minted",
        ],
        home.path(),
    )
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.contains("horde://join/h:7/s3cr3t.fp"),
        "the operator's own files: {text}"
    );
}

#[tokio::test]
async fn a_logs_mistake_is_refused_in_words() {
    let home = tempfile::tempdir().expect("home");
    let output = harness::horde(
        &["logs", "worker", "--endpoint", "http://127.0.0.1:1"],
        home.path(),
    )
    .await;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("needs the worker's name"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A worker on the real worker spine whose log tunnels follow a script: the
/// first read's tunnel drops mid-read (the worker "vanished"), the second
/// answers one record and ends.
#[cfg(test)]
mod scripted_worker {
    use horde_proto::v1::worker_service_client::WorkerServiceClient;
    use horde_proto::v1::{
        Hello, LogEnd, LogEndReason, LogRecords, LogTunnelOpen, RegisterWorkerRequest, Role,
        StreamLogRequestsRequest, WorkerId, WorkerLogTunnelRequest, worker_log_tunnel_request,
    };

    fn frame(frame: worker_log_tunnel_request::Frame) -> WorkerLogTunnelRequest {
        WorkerLogTunnelRequest { frame: Some(frame) }
    }

    pub(crate) async fn start(endpoint: &str) {
        let mut client = WorkerServiceClient::connect(endpoint.to_owned())
            .await
            .expect("the worker dials");
        client
            .register_worker(RegisterWorkerRequest {
                hello: Some(Hello {
                    semver: env!("CARGO_PKG_VERSION").to_owned(),
                    capability_flags: vec![horde_wire::logs::LOGS_CAPABILITY.to_owned()],
                    role: Role::Worker.into(),
                    display_name: "scripted".to_owned(),
                }),
                worker_id: Some(WorkerId::new("scripted")),
                ..RegisterWorkerRequest::default()
            })
            .await
            .expect("registered");
        let mut queries = client
            .stream_log_requests(StreamLogRequestsRequest {
                worker_id: Some(WorkerId::new("scripted")),
            })
            .await
            .expect("subscribed")
            .into_inner();
        tokio::spawn(async move {
            let mut answered = 0;
            while let Ok(Some(update)) = queries.message().await {
                let Some(query) = update.query else { continue };
                let (to_server, outbound) = tokio::sync::mpsc::channel(4);
                let _ = to_server
                    .send(frame(worker_log_tunnel_request::Frame::Open(
                        LogTunnelOpen {
                            request_id: query.request_id,
                        },
                    )))
                    .await;
                let mut tunnel = client.clone();
                let response = tunnel
                    .worker_log_tunnel(tokio_stream::wrappers::ReceiverStream::new(outbound))
                    .await;
                answered += 1;
                if answered == 1 {
                    // Vanish: drop both halves with nothing said.
                    drop((to_server, response));
                    continue;
                }
                let line = r#"{"ts":"2026-01-01T00:00:02.000000Z","level":"INFO","msg":"after the redial","boot":"b","seq":2}"#;
                let _ = to_server
                    .send(frame(worker_log_tunnel_request::Frame::Records(
                        LogRecords {
                            lines: vec![line.to_owned()],
                            ..LogRecords::default()
                        },
                    )))
                    .await;
                let _ = to_server
                    .send(frame(worker_log_tunnel_request::Frame::End(LogEnd {
                        reason: LogEndReason::Done.into(),
                        detail: String::new(),
                    })))
                    .await;
                drop(to_server);
                // Held until the server closes it, so the frames are not
                // reset away before they are read.
                if let Ok(response) = response {
                    let mut response = response.into_inner();
                    while let Ok(Some(_)) = response.message().await {}
                }
            }
        });
    }

    pub(crate) async fn remote_read(follow: bool) -> (Result<(), horde_cli::ClientError>, String) {
        let endpoint = horde_server_testkit::serve().await.expect("served");
        start(&endpoint).await;
        let mut client = horde_cli::Client::connect(&endpoint).await.expect("client");
        let args = horde_cli::logs::LogsArgs {
            follow,
            ..horde_cli::logs::LogsArgs::default()
        };
        let mut out = Vec::new();
        let target = horde_cli::logs::RemoteTarget::Worker("scripted".to_owned());
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            horde_cli::logs::run_remote(
                &mut client,
                &target,
                &args,
                horde_cli::Format::Human,
                &mut out,
            ),
        )
        .await
        .expect("the read finishes");
        (result, String::from_utf8_lossy(&out).into_owned())
    }
}

#[tokio::test]
async fn a_read_that_loses_its_worker_fails_unless_following() {
    let (result, _) = scripted_worker::remote_read(false).await;
    assert!(
        matches!(&result, Err(horde_cli::ClientError::Rpc(status)) if status.code() == tonic::Code::Unavailable),
        "{result:?}"
    );
}

#[tokio::test]
async fn a_follow_that_loses_its_worker_says_so_and_redials() {
    let (result, out) = scripted_worker::remote_read(true).await;
    assert!(result.is_ok(), "{result:?}");
    assert!(out.contains("--- lost the stream ("), "{out}");
    assert!(out.contains("after the redial"), "{out}");
}
