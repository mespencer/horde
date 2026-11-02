//! A child an agent spawns through its own MCP endpoint is its parent's
//! child (DESIGN §5): the server fills its spec from the parent's as the log
//! recorded it, so the child lands in the parent's workspace, and the worker
//! starts its checkout from the parent's committed HEAD. Every process is
//! real; the spawn is the agent's own `spawn_agent` call, naming nothing but
//! (here, not even) a task.

use crate::support::{System, checkout_path, git};

#[tokio::test]
async fn an_agents_child_checks_out_the_parents_workspace_at_the_parents_commit() {
    let system = System::launch().await;
    let origin = system.seeded_origin().await;

    // The parent's script: one turn that spawns a child through its MCP
    // endpoint with no arguments at all — no workspace, no type, no task, so
    // the child stays idle and spawns nothing itself.
    let script = system.state.path().join("spawn.json");
    std::fs::write(
        &script,
        r#"[{"steps": [
            {"use_horde": {"id": "call-s", "tool": "spawn_agent", "input": {}}}
        ], "text": "spawned"}]"#,
    )
    .expect("the script writes");
    let root = system.state.path().join("root-a");
    let mut worker = system
        .spawn_worker(
            "w-a",
            &system.state.path().join("w-a"),
            &[
                "--workspace-root",
                &root.to_string_lossy(),
                "--fake-script",
                &script.to_string_lossy(),
            ],
        )
        .await;

    let repo_flag = format!("atlas={},path=atlas", origin.to_string_lossy());
    let definition_id = system
        .cli_field(
            &["workspace", "create", "atlas-ws", "--repo", &repo_flag],
            "workspace_definition_id",
        )
        .await;
    let created = system
        .cli_json(&[
            "session",
            "new",
            "e2e lineage",
            "--workspace",
            &definition_id,
            "--pin-worker",
            "w-a",
        ])
        .await;
    let session_id = created["session_id"]
        .as_str()
        .expect("a session id")
        .to_owned();
    let parent = created["root_agent_id"]
        .as_str()
        .expect("a root agent id")
        .to_owned();

    // The parent's work: a commit past the instance's HEAD, in its checkout.
    let parent_checkout = checkout_path(&root, &definition_id, &parent);
    system
        .checkout_composed(&session_id, &parent, &parent_checkout)
        .await;
    std::fs::write(parent_checkout.join("finding.md"), "the parent's work\n")
        .expect("the finding writes");
    git(&parent_checkout, &["add", "."]).await;
    git(&parent_checkout, &["commit", "-m", "the parent's work"]).await;
    let parent_head = git(&parent_checkout, &["rev-parse", "HEAD"]).await;

    // Wake the parent; its turn spawns the child through its endpoint.
    system.cli_json(&["send", &parent, "spawn a helper"]).await;
    let events = system
        .sync_until(&session_id, "the child's spawn", |events| {
            events.iter().any(|event| {
                event["kind"] == "agent_spawned" && event["agent_id"].as_str() != Some(&parent)
            })
        })
        .await;
    let child = events
        .iter()
        .find(|event| {
            event["kind"] == "agent_spawned" && event["agent_id"].as_str() != Some(&parent)
        })
        .and_then(|event| event["agent_id"].as_str())
        .expect("the child's id")
        .to_owned();

    let child_checkout = checkout_path(&root, &definition_id, &child);
    system
        .checkout_composed(&session_id, &child, &child_checkout)
        .await;
    assert_eq!(
        git(&child_checkout, &["rev-parse", "HEAD"]).await,
        parent_head,
        "the child's checkout of the parent's workspace starts at the parent's commit"
    );

    worker.terminate().await.expect("SIGTERM sends");
    let (status, _, _) = worker.wait().await.expect("the worker exits");
    assert!(status.success());
}
