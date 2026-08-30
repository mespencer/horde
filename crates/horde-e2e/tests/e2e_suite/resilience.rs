//! What survives, and what must not: an agent outliving its worker process,
//! and a revoked identity finding every door closed.

use crate::support::{System, has_kind, outputs};

/// DESIGN §5: sending an agent another message is what revives it. Here the
/// whole worker *process* dies (SIGTERM, clean) and is restarted over the
/// same state directory; the next message revives the agent from the journal
/// and the reply reaches the CLI — with nothing re-run and nothing doubled.
#[tokio::test]
async fn a_worker_restart_revives_the_agent_without_rerunning_anything() {
    let system = System::launch().await;
    let state = system.state.path().join("w-1");
    let mut first_life = system.spawn_worker("w-1", &state, &[]).await;

    let (session_id, root) = system.new_session("e2e revival").await;
    system
        .sync_until(&session_id, "the root agent's placement", |events| {
            has_kind(events, "placement_changed")
        })
        .await;
    system.cli_json(&["send", &root, "first life"]).await;
    system
        .sync_until(&session_id, "the first echo", |events| {
            outputs(events).iter().any(|text| text == "first life")
        })
        .await;

    // The clean death: pumps reaped, journal durable, identity kept.
    first_life.terminate().await.expect("SIGTERM sends");
    let (status, stdout, _) = first_life.wait().await.expect("the first life exits");
    assert!(status.success());
    assert!(stdout.contains("shutdown reaped=1"), "{stdout}");

    // Second life over the same state directory: the identity loads (no
    // --join — the code was one-time and is spent), the journal resumes.
    let mut second_life = system.spawn_worker("w-1", &state, &[]).await;

    system.cli_json(&["send", &root, "second life"]).await;
    let events = system
        .sync_until(&session_id, "the revived echo", |events| {
            outputs(events).iter().any(|text| text == "second life")
        })
        .await;

    // Revival, not re-execution: the first turn's echo appears exactly once
    // in the whole history — a redelivered spawn or a renumbered journal
    // would double it.
    let firsts = outputs(&events)
        .iter()
        .filter(|text| *text == "first life")
        .count();
    assert_eq!(
        firsts, 1,
        "a restart must not re-run applied work: {events:?}"
    );

    second_life.terminate().await.expect("SIGTERM sends");
    let (status, stdout, _) = second_life.wait().await.expect("the second life exits");
    assert!(status.success());
    assert!(
        stdout.contains("shutdown reaped=1"),
        "the revived pump is reaped like any other: {stdout}"
    );
}

/// DESIGN §3: revocation closes doors. The operator revokes the worker by
/// name from the CLI; the worker's next incarnation is refused and stops
/// itself. Then the operator revokes their own client identity, and their
/// own next call is refused too.
#[tokio::test]
async fn revocation_closes_the_doors_for_worker_and_client_alike() {
    let system = System::launch().await;
    let state = system.state.path().join("w-1");
    let mut worker = system.spawn_worker("w-1", &state, &[]).await;

    // Order matters: the worker is shut down cleanly BEFORE the revocation.
    // Revoking first would open a race — the server's deny-list re-check
    // severs a revoked worker's live streams within a beat, and if that
    // severance beats the SIGTERM the exit is non-zero for the *right*
    // reason, failing a clean-shutdown assert for the wrong one. With
    // nothing revoked and nothing in flight, the signalled shutdown is
    // deterministic.
    worker.terminate().await.expect("SIGTERM sends");
    let (status, _, _) = worker.wait().await.expect("the worker exits");
    assert!(
        status.success(),
        "the signalled shutdown itself stays clean"
    );

    // The operator's verb, over the real control plane. The certificate
    // outlives the process, so revocation by name still finds serials.
    let revoked = system.cli_json(&["worker", "revoke", "w-1"]).await;
    assert!(
        revoked["serials"]
            .as_array()
            .is_some_and(|serials| !serials.is_empty()),
        "the worker held a certificate to revoke: {revoked}"
    );

    // The deterministic observation of the closed door: the next incarnation
    // — same identity, refused at the door, daemon stops instead of spinning.
    let refused_life = system.spawn_worker_expecting_refusal(&state).await;
    assert!(
        !refused_life.0.success(),
        "a revoked worker must stop, not redial the closed door: {}",
        refused_life.1
    );
    assert!(
        refused_life.2.contains("refused"),
        "the fatal reason names the refusal: {}",
        refused_life.2
    );

    // The client door: the operator revokes their own identity, and the
    // next call — any call — is refused at the same deny-list.
    let self_revoked = system
        .cli_json(&["enroll", "revoke-client", "operator"])
        .await;
    assert!(
        self_revoked["serials"]
            .as_array()
            .is_some_and(|serials| !serials.is_empty()),
        "the operator held a certificate to revoke: {self_revoked}"
    );
    let refused = system.cli(&["session", "list"]).await;
    assert!(
        !refused.status.success(),
        "a revoked client's next call must fail: {}",
        String::from_utf8_lossy(&refused.stdout)
    );
}

impl System {
    /// Starts the worker daemon over `state_dir` (identity already present)
    /// and waits for it to stop *by itself* — the shape a refused daemon
    /// must take. Returns `(status, stdout, stderr)`.
    pub(crate) async fn spawn_worker_expecting_refusal(
        &self,
        state_dir: &std::path::Path,
    ) -> (std::process::ExitStatus, String, String) {
        let daemon = horde_server_testkit::process::SpawnedProcess::spawn(
            "horde-worker[refused]",
            &self.worker_binary(),
            &[
                "--server",
                &self.main,
                "--state-dir",
                &state_dir.to_string_lossy(),
                "--adapter",
                "fake",
            ],
            &[],
        )
        .expect("the daemon spawns");
        daemon
            .wait()
            .await
            .expect("the refused daemon stops on its own")
    }
}
