//! The `logs` verb over the real HTTP stack (ADR-0015): a harness's
//! `tools/call` through the identity gate and rmcp's dispatch, read locally
//! from the worker's files for `self`, and through `ReadLogs` on a real
//! server for `server` — redacted on both paths.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::mcp_client::McpClient;
use common::{Setup, seeded_on, within};
use horde_mcp::endpoint::HordeEndpoint;
use horde_mcp::identity::AgentRegistry;
use horde_mcp::logs::LogAccess;
use horde_mcp::server::{EndpointConfig, serve};
use horde_observe::reader::LocalLogs;
use horde_observe::settings::{AgentLogSettings, AgentReadLimits, LogSettings};

const TOKEN: &str = "logs-round-trip-token";

/// A server whose log holds a secret, a session with an agent, and an
/// endpoint serving that agent's `logs` from a worker log directory whose
/// stderr file holds another secret.
async fn served(
    server_logs: &std::path::Path,
    worker_logs: &std::path::Path,
) -> Setup<(McpClient, horde_mcp::server::ServingEndpoint)> {
    let logging = horde_observe::FileLogging::start(
        server_logs,
        "horde-server",
        &LogSettings::daemon(),
        &horde_observe::ServiceIdentity::new("horde-server"),
    )?;
    logging.with_default(|| {
        tracing::info!(target: "horde_server", "server minted horde://join/h:1/srv.secret");
    });
    if !logging.flush(Duration::from_secs(5)) {
        return Err("the server log never flushed".into());
    }
    let core = horde_server::service::ServerCore::new(horde_server::EventLog::in_memory()?)
        .with_local_logs(LocalLogs::new(server_logs, "horde-server", None));
    let endpoint = horde_server_testkit::serve_core(core).await?;
    let horde = seeded_on(endpoint).await?;

    let agents = horde_observe::agents::AgentLogs::new(
        worker_logs.join("agents"),
        AgentLogSettings::default(),
        "boot",
        None,
    );
    agents
        .writer(&horde.agent_id, &horde.session_id)
        .line("harness saw horde://join/h:2/wrk.secret");

    let registry = AgentRegistry::new();
    registry.insert(&horde.agent_id, TOKEN);
    let access = Arc::new(LogAccess::new(
        Some(LocalLogs::new(worker_logs, "horde-worker", None)),
        "w1",
        // Four calls before the limit, so the fifth meets it.
        AgentReadLimits {
            burst: 4,
            ..AgentReadLimits::default()
        },
    ));
    let endpoint = HordeEndpoint::new(horde.context().await?).with_logs(access);
    let (address, serving) =
        serve(&EndpointConfig::loopback(0), Arc::new(registry), endpoint).await?;
    let mcp = McpClient::connect(address, &horde.agent_id, TOKEN).await?;
    Ok((mcp, serving))
}

fn view(text: &str) -> Setup<serde_json::Value> {
    Ok(serde_json::from_str(text)?)
}

#[tokio::test]
async fn logs_round_trips_locally_and_through_the_server_redacted() {
    let (server_logs, worker_logs) = (
        tempfile::tempdir().expect("dir"),
        tempfile::tempdir().expect("dir"),
    );
    let (mcp, _serving) = served(server_logs.path(), worker_logs.path())
        .await
        .expect("served");
    assert!(
        mcp.list_tool_names()
            .await
            .expect("listed")
            .iter()
            .any(|name| name == "logs")
    );

    let own = view(
        &within("a logs call", mcp.call_tool("logs", serde_json::json!({})))
            .await
            .expect("self"),
    )
    .expect("json");
    let records = own["records"].as_array().expect("records");
    assert_eq!(records.len(), 1, "{own}");
    assert!(
        records[0]
            .as_str()
            .is_some_and(|line| line.contains("harness saw [REDACTED:join-token]"))
    );
    assert_eq!(own["redacted"], 1);
    assert!(own["next_cursor"].is_string());

    let server = view(
        &within(
            "a logs call",
            mcp.call_tool(
                "logs",
                serde_json::json!({"target": "server", "grep": "minted"}),
            ),
        )
        .await
        .expect("server"),
    )
    .expect("json");
    let text = server.to_string();
    assert!(
        text.contains("server minted [REDACTED:join-token]"),
        "{text}"
    );
    assert!(!text.contains("srv.secret"));
    assert_eq!(server["truncated"], false);
    assert_eq!(server["redacted"], 1, "{server}");
    assert!(server["next_cursor"].is_string(), "{server}");

    // Paging past everything: no records, and the cursor stays put.
    let later = "2999-01-01T00:00:00.000000Z~horde~~0";
    let past = view(
        &within(
            "a logs call",
            mcp.call_tool(
                "logs",
                serde_json::json!({"target": "server", "cursor": later}),
            ),
        )
        .await
        .expect("paged"),
    )
    .expect("json");
    assert_eq!(past["records"].as_array().map(Vec::len), Some(0), "{past}");
    assert_eq!(past["next_cursor"], later, "{past}");

    let capped = view(
        &within(
            "a logs call",
            mcp.call_tool(
                "logs",
                serde_json::json!({"target": "server", "max_bytes": 1}),
            ),
        )
        .await
        .expect("capped"),
    )
    .expect("json");
    assert_eq!(capped["truncated"], true, "{capped}");
    assert_eq!(capped["records"].as_array().map(Vec::len), Some(0));

    // The burst of four is spent: the fifth call is a tool error the model
    // reads, not a protocol error.
    let limited = within(
        "a logs call",
        mcp.call_tool_raw("logs", serde_json::json!({})),
    )
    .await
    .expect("an answer");
    assert_eq!(limited["result"]["isError"], true, "{limited}");
    assert!(limited.to_string().contains("retry after"), "{limited}");
}
