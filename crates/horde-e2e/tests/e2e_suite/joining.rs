//! The joining UX (ADR-0007), walked end to end over real processes: one
//! token from server boot through the bootstrap file, the client join, the
//! stored endpoint, a worker's token join, and a working `hello` — with
//! every artifact read exactly where an operator (or `horde up`) reads it:
//! stdout lines, the state dir, `--json` renders.

use horde_server_testkit::process::{SpawnedProcess, product_binary, value_after};

use crate::support::{admit, bounded_output};

/// A `horde` invocation that must succeed, with its stdout.
async fn horde_ok(binary: &std::path::Path, args: &[&str]) -> String {
    let mut command = tokio::process::Command::new(binary);
    command.args(args);
    let output = bounded_output(&format!("horde {args:?}"), command).await;
    assert!(
        output.status.success(),
        "horde {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The three product binaries, built if needed and admitted — the opening
/// both scenarios share.
async fn binaries() -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let server_bin = product_binary("horde-server", "horde-server")
        .await
        .expect("the server binary exists");
    let horde_bin = product_binary("horde-cli", "horde")
        .await
        .expect("the CLI binary exists");
    let worker_bin = product_binary("horde-worker", "horde-worker")
        .await
        .expect("the worker binary exists");
    admit(&server_bin).await;
    admit(&horde_bin).await;
    admit(&worker_bin).await;
    (server_bin, horde_bin, worker_bin)
}

/// Server boot → bootstrap-token file → client token join → stored endpoint
/// → minted worker token → worker `--join` → hello. The chain is the test:
/// nothing along it takes an endpoint the token (or the identity dir) did
/// not already carry.
#[tokio::test]
async fn one_token_carries_a_machine_from_boot_to_a_working_hello() {
    let (server_bin, horde_bin, worker_bin) = binaries().await;

    let state = tempfile::tempdir().expect("a state directory");
    let server_state = state.path().join("server-state");
    let db = state.path().join("horde.db");
    let _server = SpawnedProcess::spawn(
        "horde-server",
        &server_bin,
        &[
            &db.to_string_lossy(),
            "--serve",
            "--bind",
            "127.0.0.1:0",
            "--enrollment-bind",
            "127.0.0.1:0",
            "--state-dir",
            &server_state.to_string_lossy(),
        ],
        &[],
    )
    .expect("the server spawns");
    let stdout = _server
        .await_stdout("the bootstrap token line", |out| {
            out.contains("bootstrap-client-token ")
        })
        .await
        .expect("the server announces itself");
    let printed = value_after(&stdout, "bootstrap-client-token ")
        .expect("the bootstrap token")
        .to_owned();
    assert!(
        printed.starts_with("horde://join/"),
        "the bootstrap line carries the one-token form: {printed}"
    );

    // The same token rides the state dir, user-read-only — the file `horde
    // up` trusts instead of a screen (ADR-0007).
    let token_file = server_state.join("bootstrap-token");
    let filed = std::fs::read_to_string(&token_file).expect("the bootstrap token file exists");
    assert_eq!(
        filed.trim(),
        printed,
        "the stdout line and the file carry the same token"
    );
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(&token_file).expect("the token file exists");
        assert_eq!(
            metadata.mode() & 0o777,
            0o600,
            "the token file is user-read-only"
        );
    }

    // The client join takes exactly the token — no endpoint flag exists
    // anywhere in this test after this point.
    let id_dir = state.path().join("id");
    horde_ok(
        &horde_bin,
        &[
            "--identity",
            &id_dir.to_string_lossy(),
            "enroll",
            "join",
            &printed,
        ],
    )
    .await;
    assert!(
        !token_file.exists(),
        "the bootstrap file retires the moment a client exists"
    );

    // A bare hello: the identity dir supplies both the credential and the
    // endpoint the join stored.
    let hello = horde_ok(
        &horde_bin,
        &["--identity", &id_dir.to_string_lossy(), "--json", "hello"],
    )
    .await;
    let hello: serde_json::Value =
        serde_json::from_str(hello.trim()).expect("the hello render is JSON");
    assert!(
        hello["semver"].as_str().is_some_and(|v| !v.is_empty()),
        "the stored endpoint answers a bare hello: {hello}"
    );

    // Mint a worker token through the same identity, join a real worker
    // with it, and see the worker on the roster.
    let minted = horde_ok(
        &horde_bin,
        &[
            "--identity",
            &id_dir.to_string_lossy(),
            "--json",
            "worker",
            "enroll",
            "box-2",
        ],
    )
    .await;
    let minted: serde_json::Value = serde_json::from_str(
        minted
            .lines()
            .find(|line| !line.trim().is_empty())
            .expect("the mint prints"),
    )
    .expect("the mint render is JSON");
    let worker_token = minted["token"].as_str().expect("the mint carries a token");

    let main_addr = value_after(&stdout, "listening main=").expect("a main address");
    let worker = SpawnedProcess::spawn(
        "horde-worker[box-2]",
        &worker_bin,
        &[
            "--server",
            &format!("https://{main_addr}"),
            "--state-dir",
            &state.path().join("box-2").to_string_lossy(),
            "--adapter",
            "fake",
            "--join",
            worker_token,
        ],
        &[],
    )
    .expect("the worker daemon spawns");
    worker
        .await_stdout("the registration line", |out| {
            out.contains("registered worker=box-2")
        })
        .await
        .expect("the token-joined worker registers");

    let listed = horde_ok(
        &horde_bin,
        &[
            "--identity",
            &id_dir.to_string_lossy(),
            "--json",
            "worker",
            "list",
        ],
    )
    .await;
    assert!(
        listed.contains("box-2"),
        "the joined worker is on the roster: {listed}"
    );
}

/// `horde up` against real binaries: one command, one ready line, a worker
/// on the roster through the stored endpoint — and a SIGTERM tears down
/// what it started, leaving the port silent.
#[tokio::test]
async fn horde_up_stands_up_this_machine_and_tears_it_down() {
    let (_server_bin, horde_bin, _worker_bin) = binaries().await;

    let root = tempfile::tempdir().expect("a state directory");
    let mut up = SpawnedProcess::spawn(
        "horde-up",
        &horde_bin,
        &[
            "up",
            "--state-dir",
            &root.path().to_string_lossy(),
            "--bind",
            "127.0.0.1:0",
            "--enrollment-bind",
            "127.0.0.1:0",
            "--adapter",
            "fake",
            "--name",
            "up-e2e",
        ],
        &[],
    )
    .expect("horde up spawns");

    let stdout = up
        .await_stdout("the ready line", |out| out.contains("ready worker=up-e2e "))
        .await
        .expect("up reaches ready");
    let ready = value_after(&stdout, "ready worker=up-e2e ").expect("the ready line parses");
    assert!(
        ready.starts_with("adapters=fake endpoint=https://"),
        "the ready line names the harnesses and the endpoint: {ready}"
    );
    let endpoint = ready
        .rsplit("endpoint=")
        .next()
        .expect("the ready line names the endpoint")
        .to_owned();

    // The machine it stood up is usable through the identity dir alone.
    let id_dir = root.path().join("id");
    let listed = horde_ok(
        &horde_bin,
        &[
            "--identity",
            &id_dir.to_string_lossy(),
            "--json",
            "worker",
            "list",
        ],
    )
    .await;
    assert!(
        listed.contains("up-e2e"),
        "the local worker is on the roster: {listed}"
    );
    assert_eq!(
        std::fs::read_to_string(id_dir.join("endpoint"))
            .expect("the endpoint file exists")
            .trim(),
        endpoint,
        "the enrollment stored the endpoint the ready line names"
    );

    // SIGTERM: up tears down what it started and exits cleanly...
    up.terminate().await.expect("SIGTERM sends");
    let (status, _, stderr) = up.wait().await.expect("up exits");
    assert!(status.success(), "a torn-down up exits cleanly: {stderr}");

    // ...and the server it started is gone with it: the port stops
    // answering. Polled bounded, because the OS reaps at its own pace.
    let addr = endpoint
        .strip_prefix("https://")
        .expect("an https endpoint")
        .to_owned();
    let mut silent = false;
    for _ in 0_u32..100 {
        if tokio::net::TcpStream::connect(&addr).await.is_err() {
            silent = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(silent, "the server up started must die with it: {addr}");
}
