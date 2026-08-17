//! Helpers shared by the process-level suites.
//!
//! One set of primitives so `supervision.rs` and `codex_supervision.rs`
//! drive their stub harnesses the same way instead of through two
//! drifting copies.

use std::path::PathBuf;

/// A binary cargo built alongside the running test.
pub(crate) fn built_binary(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().unwrap_or_default();
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.join(name)
}

/// How long any wait in a process test may take before it is a failure.
///
/// Generous against a loaded machine, and still three orders of magnitude
/// below the point where cargo-mutants gives up.
pub(crate) const CEILING: std::time::Duration = std::time::Duration::from_secs(5);

/// Runs `future` with a ceiling, failing the test if it does not finish.
///
/// Every wait against a child process goes through here, because an unbounded
/// await turns a defect into a hang — and a hang into a mutant nobody
/// evaluated (docs/QUALITY_GATES.md): a `write_line` that writes nothing or a
/// `send` that sends nothing leaves a test waiting for something that never
/// arrives, records a TIMEOUT, and protects nothing. With a ceiling the same
/// mutants fail in seconds and are caught.
pub(crate) async fn within<F: std::future::Future>(what: &str, future: F) -> F::Output {
    match tokio::time::timeout(CEILING, future).await {
        Ok(value) => value,
        Err(_) => panic!("{what} must finish rather than hang"),
    }
}

/// Polls a child's control log, bounded by count, until `expected` holds.
///
/// The predicate is the full expected content rather than "non-empty" because
/// a slow machine can expose a partially flushed line, and reading it early
/// is a flake, not a finding.
pub(crate) async fn control_log_reaches(
    log_path: &std::path::Path,
    expected: impl Fn(&str) -> bool,
) -> String {
    let mut recorded = String::new();
    for _ in 0..250 {
        recorded = std::fs::read_to_string(log_path).unwrap_or_default();
        if expected(&recorded) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    recorded
}

/// Serves both spines over `core` on a loopback port the OS picks, through
/// the shared testkit harness. Shared by the connection suite (in-process
/// worker) and the daemon suite (the `horde-worker` binary dialing
/// `--insecure`).
pub(crate) async fn serve(core: horde_server::service::ServerCore) -> String {
    horde_server_testkit::serve_core(core)
        .await
        .expect("the server binds loopback")
}

/// Seeds an agent through the shared testkit helper, panicking on the
/// failures a precondition cannot survive.
pub(crate) async fn seed_agent(
    log: &horde_server::EventLog,
    session: &str,
    agent: &str,
    worker: Option<&str>,
    first_message: &str,
) {
    horde_server_testkit::seed_agent(log, session, agent, worker, first_message)
        .await
        .expect("the seed events append");
}

/// [`seed_agent`], placed on `worker` — the common case.
pub(crate) async fn place_agent(
    log: &horde_server::EventLog,
    session: &str,
    agent: &str,
    worker: &str,
    first_message: &str,
) {
    seed_agent(log, session, agent, Some(worker), first_message).await;
}

/// Waits until `session`'s server log reaches `minimum`, returning the
/// head it reached. Bounded by poll count and by a ceiling per park.
pub(crate) async fn server_log_at_least(
    log: &horde_server::EventLog,
    session: &str,
    minimum: u64,
) -> u64 {
    let mut notifications = log.subscribe();
    let mut polls = 0_u32;
    loop {
        let head = log.head(session).await.expect("a readable log");
        if head >= minimum {
            return head;
        }
        polls += 1;
        assert!(polls < 256, "the server log stalled below {minimum}");
        let _ = tokio::time::timeout(core::time::Duration::from_secs(5), notifications.recv())
            .await
            .expect("the server log must keep moving");
    }
}

/// Every event of `session`'s server log, decoded.
pub(crate) async fn decoded_session_log(
    log: &horde_server::EventLog,
    session: &str,
) -> Vec<horde_proto::v1::Event> {
    use horde_proto::prost::Message;

    log.read_all(session)
        .await
        .expect("a readable log")
        .iter()
        .map(|stored| {
            horde_proto::v1::Event::decode(stored.payload.as_slice()).expect("a decodable event")
        })
        .collect()
}
