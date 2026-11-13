//! ADR-0014 end to end: a real server, a real worker daemon on the fake
//! adapter, and the `horde` CLI as a process. Freshness at spawn, a lazy
//! checkout through the agent's own `workspace_checkout` call, one branch
//! across repos, soft reclaim running `cargo clean`, hard removal refusing
//! an unprovable checkout and taking a provable one, the dry run's output,
//! and orphans reported after a worker reset.

use std::path::{Path, PathBuf};

use crate::support::{System, bounded_output, git, path_appears};

/// An origin repo `name` under the system's state with one seed commit.
async fn origin(system: &System, name: &str) -> PathBuf {
    let origin = system.state.path().join(format!("{name}-origin"));
    std::fs::create_dir_all(&origin).expect("the origin dir");
    git(&origin, &["init", "-b", "main"]).await;
    std::fs::write(origin.join("README.md"), format!("{name}\n")).expect("the seed file");
    git(&origin, &["add", "."]).await;
    git(&origin, &["commit", "-m", "seed"]).await;
    origin
}

fn agent_root(root: &Path, definition_id: &str, agent: &str) -> PathBuf {
    root.join(definition_id).join(".horde-agents").join(agent)
}

/// `session new` with workspace flags, returning `(session, root agent)`.
async fn session(system: &System, title: &str, extra: &[&str]) -> (String, String) {
    let mut args = vec!["session", "new", title, "--pin-worker", "w-ws"];
    args.extend_from_slice(extra);
    let created = system.cli_json(&args).await;
    (
        created["session_id"]
            .as_str()
            .expect("a session id")
            .to_owned(),
        created["root_agent_id"]
            .as_str()
            .expect("a root agent id")
            .to_owned(),
    )
}

/// Waits for an agent's `checkout_prepared` event and returns it.
async fn prepared(system: &System, session_id: &str, agent: &str) -> serde_json::Value {
    let events = system
        .sync_until(
            session_id,
            &format!("{agent}'s checkout_prepared"),
            |events| {
                events.iter().any(|e| {
                    e["kind"] == "checkout_prepared" && e["agent_id"].as_str() == Some(agent)
                })
            },
        )
        .await;
    events
        .into_iter()
        .find(|e| e["kind"] == "checkout_prepared" && e["agent_id"].as_str() == Some(agent))
        .expect("the event")
}

/// Freshness, `--branch` across repos, and a lazy checkout through the
/// agent's own MCP call, on one two-repo workspace.
#[tokio::test]
async fn agents_start_fresh_share_a_branch_and_check_out_lazily() {
    // No throttle: the second spawn follows the first within seconds, and
    // must see the upstream commit made in between.
    let system = System::launch_with(&["--workspace-fetch-throttle", "0s"]).await;
    let atlas = origin(&system, "atlas").await;
    let docs = origin(&system, "docs").await;
    // Every agent's turn: check out `docs` through the endpoint.
    let script = r#"[{"steps": [
        {"use_horde": {"id": "co-1", "tool": "workspace_checkout", "input": {"repo": "docs"}}}
    ], "text": "checked out"}]"#;
    let root = system.state.path().join("ws-root");
    let script_path = system.state.path().join("checkout.json");
    std::fs::write(&script_path, script).expect("the script writes");
    let mut worker = system
        .spawn_worker(
            "w-ws",
            &system.state.path().join("w-ws"),
            &[
                "--workspace-root",
                &root.to_string_lossy(),
                "--fake-script",
                &script_path.to_string_lossy(),
            ],
        )
        .await;
    let definition_id = system
        .cli_field(
            &[
                "workspace",
                "create",
                "suite",
                "--repo",
                &format!("atlas={}", atlas.display()),
                "--repo",
                &format!("docs={}", docs.display()),
            ],
            "workspace_definition_id",
        )
        .await;

    // The first agent starts at the seed.
    let (first_session, first) = session(&system, "first", &["--workspace", &definition_id]).await;
    let event = prepared(&system, &first_session, &first).await;
    let seed = git(&atlas, &["rev-parse", "HEAD"]).await;
    assert!(
        event.to_string().contains(&seed),
        "starts at the seed: {event}"
    );

    // Upstream moves on; the next agent starts from the new tip, with
    // the instance's own tree untouched.
    std::fs::write(atlas.join("new.txt"), "upstream\n").expect("writes");
    git(&atlas, &["add", "."]).await;
    git(&atlas, &["commit", "-m", "upstream work"]).await;
    let tip = git(&atlas, &["rev-parse", "HEAD"]).await;
    let (second_session, second) = session(
        &system,
        "second",
        &["--workspace", &definition_id, "--branch", "feat/login"],
    )
    .await;
    prepared(&system, &second_session, &second).await;
    let second_root = agent_root(&root, &definition_id, &second);
    assert_eq!(
        git(&second_root.join("atlas"), &["rev-parse", "HEAD"]).await,
        tip
    );
    assert!(
        !root
            .join(&definition_id)
            .join("atlas")
            .join("new.txt")
            .exists(),
        "the instance tree stays put"
    );
    for repo in ["atlas", "docs"] {
        assert_eq!(
            git(&second_root.join(repo), &["branch", "--show-current"]).await,
            "feat/login",
            "{repo} is on the shared branch"
        );
    }

    // A third agent, lazy with nothing checked out, checks `docs` out in
    // its turn through `workspace_checkout`.
    let (lazy_session, lazy) = session(
        &system,
        "lazy",
        &["--workspace", &definition_id, "--checkout-mode", "lazy"],
    )
    .await;
    prepared(&system, &lazy_session, &lazy).await;
    let lazy_root = agent_root(&root, &definition_id, &lazy);
    assert!(!lazy_root.join("docs").exists() && !lazy_root.join("atlas").exists());
    system.cli_json(&["send", &lazy, "go"]).await;
    path_appears(
        "the lazily checked-out docs",
        &lazy_root.join("docs").join(".git"),
    )
    .await;
    system
        .sync_until(&lazy_session, "the checkout's provenance", |events| {
            events
                .iter()
                .filter(|e| e["kind"] == "checkout_prepared")
                .count()
                >= 2
        })
        .await;
    assert!(!lazy_root.join("atlas").exists(), "only what it asked for");

    worker.terminate().await.expect("SIGTERM sends");
    let _ = worker.wait().await;
}

/// The workspace verbs as a human runs them (no `--json`), with the exit
/// status.
async fn human(system: &System, args: &[&str]) -> (i32, String) {
    let mut command = tokio::process::Command::new(system.horde_binary());
    command.args(system.operator_args()).args(args);
    let output = bounded_output(&format!("horde {args:?}"), command).await;
    (
        output.status.code().unwrap_or(-1),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

/// A Rust origin: what the built-in reclaim rule runs `cargo clean` in.
async fn crate_origin(system: &System) -> PathBuf {
    let origin = origin(system, "crate").await;
    std::fs::write(
        origin.join("Cargo.toml"),
        "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[workspace]\n",
    )
    .expect("Cargo.toml");
    std::fs::create_dir_all(origin.join("src")).expect("src");
    std::fs::write(origin.join("src/lib.rs"), "").expect("lib.rs");
    std::fs::write(origin.join(".gitignore"), "/target\n").expect(".gitignore");
    git(&origin, &["add", "-A"]).await;
    git(&origin, &["commit", "-m", "a crate"]).await;
    origin
}

/// Soft reclaim runs `cargo clean`; a dry run says so and changes nothing;
/// hard removal refuses the archived checkout that is not provably
/// lossless (exit 1) and removes the one that is (exit 0).
#[tokio::test]
async fn gc_reclaims_softly_dry_runs_and_removes_only_what_is_provable() {
    let system = System::launch().await;
    let origin = crate_origin(&system).await;
    let root = system.state.path().join("ws-root");
    let mut worker = system
        .spawn_worker(
            "w-ws",
            &system.state.path().join("w-ws"),
            &["--workspace-root", &root.to_string_lossy()],
        )
        .await;
    let definition_id = system
        .cli_field(
            &[
                "workspace",
                "create",
                "rusty",
                "--repo",
                &format!("crate={}", origin.display()),
            ],
            "workspace_definition_id",
        )
        .await;
    let (clean_session, clean) = session(&system, "clean", &["--workspace", &definition_id]).await;
    let (dirty_session, dirty) = session(&system, "dirty", &["--workspace", &definition_id]).await;
    prepared(&system, &clean_session, &clean).await;
    prepared(&system, &dirty_session, &dirty).await;
    let clean_repo = agent_root(&root, &definition_id, &clean).join("crate");
    let dirty_repo = agent_root(&root, &definition_id, &dirty).join("crate");
    std::fs::create_dir_all(clean_repo.join("target/debug")).expect("target");
    std::fs::write(clean_repo.join("target/debug/big.rlib"), vec![0_u8; 10_000])
        .expect("an artifact");

    // Soft: cargo clean, the work stays.
    let (status, output) = human(&system, &["workspace", "gc", "--soft"]).await;
    assert_eq!(status, 0, "{output}");
    assert!(output.contains("soft done"), "{output}");
    assert!(
        !clean_repo.join("target").exists(),
        "cargo clean ran: {output}"
    );
    assert!(clean_repo.join("src/lib.rs").exists());

    // Both agents archived; the default 24 h grace keeps both checkouts.
    std::fs::write(dirty_repo.join("wip.txt"), "uncommitted").expect("dirty");
    for session_id in [&clean_session, &dirty_session] {
        system.cli_json(&["session", "archive", session_id]).await;
    }
    // Each archive journals its soft reclaim; wait for both.
    let mut log = String::new();
    for _ in 0..100 {
        let output = system.cli(&["workspace", "log"]).await;
        log = String::from_utf8_lossy(&output.stdout).into_owned();
        if log.matches("\"trigger\":\"archive\"").count() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert_eq!(log.matches("\"trigger\":\"archive\"").count(), 2, "{log}");
    for repo in [&clean_repo, &dirty_repo] {
        assert!(repo.exists(), "kept through the grace: {}", repo.display());
    }

    // The dry run: headed, rows say would-do and refused, nothing changes.
    let (status, output) = human(&system, &["workspace", "gc", "--hard", "--dry-run"]).await;
    assert!(
        output.starts_with("DRY RUN — nothing was changed"),
        "{output}"
    );
    assert!(output.contains("would-do"), "{output}");
    assert!(
        output.contains("refused") && output.contains("untracked files: 1"),
        "{output}"
    );
    assert_eq!(status, 1, "a refusal is exit 1: {output}");
    assert!(
        clean_repo.exists() && dirty_repo.join("wip.txt").exists(),
        "a dry run changes nothing"
    );

    // For real: the provable one goes, the unprovable one stays.
    let (status, output) = human(&system, &["workspace", "gc", "--hard"]).await;
    assert_eq!(status, 1, "{output}");
    assert!(
        !clean_repo.exists(),
        "the clean archived checkout is removed: {output}"
    );
    assert!(
        dirty_repo.join("wip.txt").exists(),
        "the dirty one is retained: {output}"
    );
    let listed = system
        .cli_json(&["workspace", "instance", "list", &definition_id])
        .await;
    let retained = listed.to_string();
    assert!(
        retained.contains("untracked files: 1"),
        "the retained reason is reported: {retained}"
    );

    // Forced, it goes too: exit 0.
    let (status, output) = human(&system, &["workspace", "gc", "--hard", "--force"]).await;
    assert_eq!(status, 0, "{output}");
    assert!(!dirty_repo.exists(), "{output}");

    worker.terminate().await.expect("SIGTERM sends");
    let _ = worker.wait().await;
}

/// After a worker reset, the checkouts the old journal knew are orphans:
/// the restarted worker reports them as such, and the instance list shows
/// it.
#[tokio::test]
async fn orphans_are_reported_after_a_worker_reset() {
    let system = System::launch().await;
    let atlas = origin(&system, "atlas").await;
    let root = system.state.path().join("ws-root");
    let state = system.state.path().join("w-ws");
    let flags = [
        "--workspace-root",
        &root.to_string_lossy(),
        "--workspace-scan-interval",
        "1s",
        "--workspace-report-interval",
        "1s",
    ]
    .map(str::to_owned);
    let borrowed: Vec<&str> = flags.iter().map(String::as_str).collect();
    let mut worker = system.spawn_worker("w-ws", &state, &borrowed).await;
    let definition_id = system
        .cli_field(
            &[
                "workspace",
                "create",
                "solo",
                "--repo",
                &format!("atlas={}", atlas.display()),
            ],
            "workspace_definition_id",
        )
        .await;
    let (session_id, agent) = session(
        &system,
        "before the reset",
        &["--workspace", &definition_id],
    )
    .await;
    prepared(&system, &session_id, &agent).await;
    worker.terminate().await.expect("SIGTERM sends");
    let _ = worker.wait().await;

    // The worker's journal is gone for good; the operator says so.
    // `journal.sqlite3` is the daemon's journal file (horde-worker's
    // `JOURNAL_FILE`); its WAL siblings go with it.
    for file in [
        "journal.sqlite3",
        "journal.sqlite3-wal",
        "journal.sqlite3-shm",
    ] {
        let _ = std::fs::remove_file(state.join(file));
    }
    assert!(
        !state.join("journal.sqlite3").exists(),
        "the journal is gone"
    );
    system
        .cli_json(&["worker", "reset", "w-ws", "--reason", "disk replaced"])
        .await;
    let mut worker = system.spawn_worker("w-ws", &state, &borrowed).await;

    let mut last = serde_json::Value::Null;
    for _ in 0..100 {
        last = system
            .cli_json(&["workspace", "instance", "list", &definition_id])
            .await;
        if last["instances"][0]["checkouts"][0]["state"] == "orphan" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let checkout = &last["instances"][0]["checkouts"][0];
    assert_eq!(checkout["state"], "orphan", "{last}");
    assert_eq!(checkout["agent_id"].as_str(), Some(agent.as_str()));
    assert_eq!(checkout["session_id"].as_str(), Some(session_id.as_str()));
    assert!(
        agent_root(&root, &definition_id, &agent).exists(),
        "reported, never removed by a scan"
    );

    let log = system.cli(&["workspace", "log", "--worker", "w-ws"]).await;
    let log = String::from_utf8_lossy(&log.stdout).into_owned();
    assert!(
        log.contains("\"trigger\":\"scan\""),
        "the orphan's soft reclaim is logged: {log}"
    );

    worker.terminate().await.expect("SIGTERM sends");
    let _ = worker.wait().await;
}
