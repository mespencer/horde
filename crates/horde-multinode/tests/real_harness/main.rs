//! The opt-in real-harness smoke: `worker-real` (compose profile `real`)
//! runs the host's real `claude` and `codex` binaries against mounted,
//! signed-in profile directories, and each answers a one-line prompt from a
//! container, over the same mTLS topology the fake suite uses.
//!
//! It spends real subscription quota. It needs, in the environment the test
//! runs in (docs/TESTING.md lists them): `HORDE_CLAUDE_BIN`,
//! `HORDE_CODEX_BIN_DIR`, `HORDE_CLAUDE_PROFILE`, `HORDE_CODEX_PROFILE`,
//! `HORDE_UID`, `HORDE_GID`. A missing one fails the test by name — it is
//! `#[ignore]`d precisely so that nobody runs it by accident, and once asked
//! for it never degrades to a skip.

#[cfg(test)]
#[path = "../multinode_suite/common.rs"]
mod common;

#[cfg(test)]
mod smoke {
    use crate::common::cluster::{Cluster, about, field};

    const REQUIRED: [&str; 6] = [
        "HORDE_CLAUDE_BIN",
        "HORDE_CODEX_BIN_DIR",
        "HORDE_CLAUDE_PROFILE",
        "HORDE_CODEX_PROFILE",
        "HORDE_UID",
        "HORDE_GID",
    ];

    /// Asks one agent on `model` for a word and waits for the word in its
    /// output.
    async fn answers(cluster: &Cluster, model: &str, word: &str) {
        let created = cluster
            .horde_json(&[
                "session",
                "new",
                &format!("real {model}"),
                "--model",
                model,
                "--pin-worker",
                "worker-real",
                "--task",
                &format!("Reply with exactly the word {word} and nothing else. Do not use tools."),
            ])
            .await;
        let session = field(&created, "session_id");
        let agent = field(&created, "root_agent_id");
        cluster
            .sync_until_within(
                &session,
                &format!("{model} answering {word}"),
                std::time::Duration::from_secs(240),
                |events| {
                    about(events, &agent).any(|e| {
                        e["kind"] == "agent_output_chunk"
                            && e["detail"].as_str().is_some_and(|text| text.contains(word))
                    })
                },
            )
            .await;
    }

    #[tokio::test]
    #[ignore = "spends real subscription quota and needs signed-in profiles; see docs/TESTING.md"]
    async fn real_claude_and_codex_answer_from_a_container() {
        let missing: Vec<&str> = REQUIRED
            .iter()
            .copied()
            .filter(|name| std::env::var_os(name).is_none())
            .collect();
        assert!(missing.is_empty(), "the real profile needs {missing:?} set");

        let cluster = Cluster::up_with("real", &["real"], &["worker-real"]).await;
        let workers = cluster.workers().await;
        let real = workers
            .iter()
            .find(|w| w["worker_id"] == "worker-real")
            .expect("worker-real is listed");
        let harnesses = real["harnesses"].to_string();
        assert!(
            harnesses.contains("claude") && harnesses.contains("codex"),
            "worker-real advertises both real harnesses: {real}"
        );

        answers(&cluster, "claude:haiku", "PELICAN").await;
        answers(&cluster, "codex:", "OSPREY").await;
    }
}
