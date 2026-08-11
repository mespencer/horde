//! The real `horde` binary against a real localhost server.
//!
//! Everything else in this crate exercises the library. This file exercises the
//! part only a subprocess can: argument parsing, the `--json` flag actually
//! reaching the renderer, what lands on stdout versus stderr, and the exit
//! code. A library test cannot catch a verb wired to the wrong call or a flag
//! that parses and is then ignored.
//!
//! The server comes from `common::serve`, on a real loopback socket — which is
//! the reason that harness binds one at all: a subprocess cannot be handed an
//! in-process stream.

mod common;

use common::{Setup, serve};
use tokio::process::Command;

/// What a `horde` invocation produced.
struct Run {
    stdout: String,
    stderr: String,
    success: bool,
}

/// Runs the real binary. `CARGO_BIN_EXE_horde` is the one cargo just built, so
/// this can never test a stale copy on `PATH`.
///
/// Async on purpose: the server under test runs as a task on this same runtime,
/// so a blocking `std::process::Command::output` would stop it being polled and
/// the subprocess would wait forever for a server that never accepts.
async fn horde(endpoint: &str, args: &[&str]) -> Setup<Run> {
    let output = Command::new(env!("CARGO_BIN_EXE_horde"))
        .arg("--endpoint")
        .arg(endpoint)
        .args(args)
        .output()
        .await?;

    Ok(Run {
        stdout: String::from_utf8(output.stdout)?,
        stderr: String::from_utf8(output.stderr)?,
        success: output.status.success(),
    })
}

#[tokio::test]
async fn hello_prints_the_server_version() {
    let endpoint = serve().await.expect("a listening server");

    let run = horde(&endpoint, &["hello"])
        .await
        .expect("the binary must run");
    assert!(run.success, "stderr was: {}", run.stderr);
    assert!(
        run.stdout.contains("horde-server"),
        "expected a server banner, got {:?}",
        run.stdout
    );
}

#[tokio::test]
async fn session_list_says_so_when_there_is_nothing() {
    let endpoint = serve().await.expect("a listening server");

    let run = horde(&endpoint, &["session", "list"]).await.expect("run");
    assert!(run.success, "stderr was: {}", run.stderr);
    assert_eq!(
        run.stdout.trim(),
        "no sessions",
        "an empty listing must say so, or it is indistinguishable from a swallowed error"
    );
}

#[tokio::test]
async fn session_list_json_is_machine_readable() {
    let endpoint = serve().await.expect("a listening server");

    // Seed through the library, then read back through the binary — the point
    // is the CLI's output, not another round of the library's write path.
    common::seeded(&endpoint, "ci-fix").await.expect("seed");

    let run = horde(&endpoint, &["--json", "session", "list"])
        .await
        .expect("run");
    assert!(run.success, "stderr was: {}", run.stderr);

    let parsed: serde_json::Value =
        serde_json::from_str(run.stdout.trim()).expect("--json must emit valid JSON on stdout");
    let sessions = parsed["sessions"].as_array().expect("a sessions array");
    assert_eq!(sessions.len(), 1);
    assert_eq!(
        sessions[0]["title"],
        serde_json::Value::String(String::new())
    );
    assert_eq!(sessions[0]["state"], "active");
}

#[tokio::test]
async fn session_new_prints_the_root_agent_the_next_command_needs() {
    let endpoint = serve().await.expect("a listening server");

    let run = horde(&endpoint, &["session", "new", "ci-fix"])
        .await
        .expect("run");
    assert!(run.success, "stderr was: {}", run.stderr);
    assert!(
        run.stdout.contains("root agent:"),
        "the id a user must copy has to be on screen: {:?}",
        run.stdout
    );
}

#[tokio::test]
async fn sync_replays_a_session_to_stdout_and_exits() {
    let endpoint = serve().await.expect("a listening server");

    let (_, session_id, _) = common::seeded(&endpoint, "sync").await.expect("seed");

    // No `--follow`: the scriptable form replays to head and completes, which
    // is what makes this test terminate at all.
    let run = horde(&endpoint, &["sync", &session_id]).await.expect("run");
    assert!(run.success, "stderr was: {}", run.stderr);

    let lines: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(lines.len(), 1, "one event so far: {lines:?}");
    assert!(
        lines[0].contains("agent_spawned"),
        "the session's first event is its root agent: {:?}",
        lines[0]
    );
}

#[tokio::test]
async fn sync_from_an_offset_skips_what_came_before() {
    let endpoint = serve().await.expect("a listening server");

    let (mut client, session_id, root) = common::seeded(&endpoint, "offsets").await.expect("seed");
    common::send_notes(&mut client, &root, 3)
        .await
        .expect("send");

    let all = horde(&endpoint, &["--json", "sync", &session_id])
        .await
        .expect("run");
    let tail = horde(&endpoint, &["--json", "sync", &session_id, "--from", "3"])
        .await
        .expect("run");
    assert!(all.success && tail.success);

    let sequences = |stdout: &str| -> Vec<u64> {
        stdout
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter_map(|value| value["sequence"].as_u64())
            .collect()
    };

    let full = sequences(&all.stdout);
    let partial = sequences(&tail.stdout);
    assert_eq!(full, vec![1, 2, 3, 4]);
    assert_eq!(
        partial,
        vec![3, 4],
        "--from must reach the server rather than being parsed and ignored"
    );
}

/// An unbuilt verb must be recognisable as such, and must not be mistaken for
/// success by a script checking the exit code.
#[tokio::test]
async fn an_unknown_session_fails_with_a_nonzero_exit_and_a_message_on_stderr() {
    let endpoint = serve().await.expect("a listening server");

    let run = horde(&endpoint, &["session", "get", "session-that-never-existed"])
        .await
        .expect("run");
    assert!(!run.success, "a missing session must not exit zero");
    assert!(
        run.stderr.contains("session-that-never-existed"),
        "the diagnosis belongs on stderr: {:?}",
        run.stderr
    );
    assert!(
        run.stdout.trim().is_empty(),
        "nothing may go to stdout on failure, or a script will parse an error as data"
    );
}

#[tokio::test]
async fn a_server_that_is_not_there_is_reported_as_unreachable() {
    // Port 1 on loopback: nothing listens there, and binding it would need
    // privileges, so this is a reliable refusal rather than a race.
    let run = horde("http://127.0.0.1:1", &["hello"])
        .await
        .expect("the binary must run");

    assert!(!run.success);
    assert!(
        run.stderr.contains("could not reach the server"),
        "the message must point at the connection, not at a verb: {:?}",
        run.stderr
    );
}

#[tokio::test]
async fn a_malformed_endpoint_is_refused_before_anything_is_attempted() {
    let run = horde("not-a-uri", &["hello"])
        .await
        .expect("the binary must run");

    assert!(!run.success);
    assert!(
        run.stderr.contains("valid endpoint URI"),
        "a typo must send the user to the address, not to the server: {:?}",
        run.stderr
    );
}
