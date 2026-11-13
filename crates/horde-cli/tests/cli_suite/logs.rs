//! `horde logs` run for real (ADR-0015): the binary against a served
//! control plane whose server log was written by the real file layer, and
//! `--dir` over the same files with no server at all.

#[cfg(test)]
mod harness {
    use std::time::Duration;

    use horde_observe::reader::LocalLogs;
    use horde_observe::settings::LogSettings;
    use tracing_subscriber::layer::SubscriberExt as _;

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
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(logging.layer()));
        tracing::dispatcher::with_default(&dispatch, || {
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

    pub(crate) async fn horde(args: &[&str], home: &std::path::Path) -> std::process::Output {
        horde_server_testkit::otlp_scrubbed_run(
            env!("CARGO_BIN_EXE_horde"),
            args,
            &[("HOME", home.to_str().expect("utf-8 home"))],
        )
        .await
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
