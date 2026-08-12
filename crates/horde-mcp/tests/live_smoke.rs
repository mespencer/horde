//! The one test that proves the whole thing against a real harness.
//!
//! `#[ignore]` because it needs the `claude` binary, a logged-in account, and
//! the network — none of which belong in the default suite. Run it deliberately:
//!
//! ```text
//! cargo test -p horde-mcp --test live_smoke -- --ignored --nocapture
//! ```
//!
//! Everything else in this crate tests Horde's half of the contract against
//! Horde's own types. This is the only place the *harness's* half is exercised,
//! and it is the half that can break without any of our code changing — the
//! flag is undocumented, and the audit already caught it moving from listed to
//! hidden-but-functional between releases.
//!
//! # Why `--settings` is not optional here
//!
//! ADR-0003's amendment, re-verified against 2.1.228: permission interception
//! is **conditional**. An ambient `settings.json` allowlist decides a tool call
//! *before* `--permission-prompt-tool` is consulted, and an allowed call then
//! runs with no prompt, no denial, and no event — a silent bypass of the whole
//! §10 chain rather than a visible failure.
//!
//! So this test passes an explicit `--settings` with empty allow/deny lists and
//! `defaultMode: manual`, and `--strict-mcp-config` so no ambient MCP server
//! joins in. Without those the test would pass on a machine whose operator had
//! allowlisted `Read`, while proving nothing at all. That is the failure mode
//! worth designing the test against: not a red suite, a green one that means
//! nothing.

mod common;

use common::{Setup, harness};
use horde_cli::client::{Client, PermissionAnswer, SyncOptions};
use horde_mcp::endpoint::HordeEndpoint;
use horde_mcp::identity::AgentRegistry;
use horde_mcp::server::{EndpointConfig, serve};
use horde_proto::v1::PermissionOutcome;

const TOKEN: &str = "horde-smoke-token";

/// Answers the first escalation that appears, the way a human's client would.
///
/// Spawned alongside the harness rather than run after it, because the harness
/// blocks on the answer — a test that waited for `claude` to exit before
/// answering would deadlock, which is itself the property being demonstrated.
async fn answer_first_escalation(endpoint: &str, session_id: &str) -> Setup<String> {
    let mut watching = Client::connect(endpoint).await?;
    let mut stream = watching
        .follow(&SyncOptions::new(session_id.to_owned()))
        .await?;

    let request_id = stream
        .next_matching(|envelope| {
            let id = common::escalation_request_id(envelope)?;
            eprintln!("SMOKE escalation raised: {id}");
            Some(id)
        })
        .await?
        .ok_or("the harness never asked for permission")?;

    let mut answering = Client::connect(endpoint).await?;
    answering
        .answer_permission(PermissionAnswer {
            request_id: &request_id,
            outcome: PermissionOutcome::Deny,
            reason: "smoke test denies everything",
            remember_for_session: false,
            updated_input: None,
        })
        .await?;
    eprintln!("SMOKE answered {request_id} with deny");

    Ok(request_id)
}

#[tokio::test]
#[ignore = "needs the real `claude` binary, an account, and the network"]
async fn a_real_claude_routes_its_permission_prompt_through_this_endpoint() -> Setup<()> {
    let horde = harness().await?;

    let mut registry = AgentRegistry::new();
    registry.insert(&horde.agent_id, TOKEN);

    let mcp = HordeEndpoint::new(horde.context().await?);
    let (bound, _serving) = serve(
        &EndpointConfig::loopback(0),
        std::sync::Arc::new(registry),
        mcp,
    )
    .await?;
    eprintln!("SMOKE endpoint on http://{bound}/mcp");

    let scratch = tempfile::tempdir()?;

    // The identity pair rides plain headers, which the spike confirmed on the
    // wire for every request this client makes — including `initialize` and the
    // GET stream, neither of which routes through a tool.
    let mcp_config = serde_json::json!({
        "mcpServers": {
            "horde": {
                "type": "http",
                "url": format!("http://{bound}/mcp"),
                "headers": {
                    "Authorization": format!("Bearer {TOKEN}"),
                    "X-Horde-Agent": horde.agent_id,
                }
            }
        }
    });
    let mcp_path = scratch.path().join("mcp.json");
    std::fs::write(&mcp_path, serde_json::to_string(&mcp_config)?)?;

    // The ADR-0003 conditional. Empty lists and manual mode mean nothing is
    // pre-decided, so the prompt tool is genuinely consulted.
    let settings = serde_json::json!({
        "permissions": { "allow": [], "deny": [], "defaultMode": "manual" }
    });
    let settings_path = scratch.path().join("settings.json");
    std::fs::write(&settings_path, serde_json::to_string(&settings)?)?;

    let answering = tokio::spawn({
        let endpoint = horde.endpoint.clone();
        let session_id = horde.session_id.clone();
        async move { answer_first_escalation(&endpoint, &session_id).await }
    });

    // A prompt that cannot be satisfied without a tool call, so "no escalation"
    // is a real failure rather than the model deciding it did not need one.
    let harness_run = tokio::process::Command::new("claude")
        .arg("--print")
        .arg("--strict-mcp-config")
        .arg("--mcp-config")
        .arg(&mcp_path)
        .arg("--settings")
        .arg(&settings_path)
        .arg("--permission-prompt-tool")
        .arg("mcp__horde__approve")
        .arg("--permission-mode")
        .arg("manual")
        .arg("Create a file called smoke.txt containing the word HORDE. Use the Write tool.")
        .current_dir(scratch.path())
        .output()
        .await?;

    eprintln!(
        "SMOKE claude exited {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        harness_run.status.code(),
        String::from_utf8_lossy(&harness_run.stdout),
        String::from_utf8_lossy(&harness_run.stderr),
    );

    let request_id = answering.await??;
    assert!(
        !request_id.is_empty(),
        "the escalation must carry the harness's tool_use_id, which is what \
         makes a retried prompt dedupe rather than escalate twice"
    );

    // The denial is what proves interception: the harness asked, waited, was
    // refused, and did not write the file. A file on disk here would mean the
    // call ran without ever consulting the chain — the silent bypass ADR-0003
    // warns about, and the reason `--settings` is passed above.
    assert!(
        !scratch.path().join("smoke.txt").exists(),
        "the denied Write must not have run; if this file exists the permission \
         chain was bypassed rather than consulted"
    );

    Ok(())
}
