//! The MCP Hub catalog RPCs (ADR-0004): configure, list, delete, and the
//! properties that make the catalog safe to author concurrently — the etag
//! discipline and the create/update split.

mod common;

use common::{Harness, Setup, connect};
use horde_proto::v1::{
    ConfigureMcpServerRequest, DeleteMcpServerRequest, ListMcpServersRequest, McpServerConfig,
    McpStdioTransport, mcp_server_config::Transport,
};
use tonic::Code;

fn stdio_config(name: &str) -> McpServerConfig {
    McpServerConfig {
        name: name.to_owned(),
        transport: Some(Transport::Stdio(McpStdioTransport {
            command: "linear-mcp".to_owned(),
            args: vec!["--stdio".to_owned()],
            env: std::collections::HashMap::new(),
        })),
        local: false,
        allowed_agent_types: Vec::new(),
        etag: String::new(),
    }
}

async fn configure(
    h: &mut Harness,
    config: McpServerConfig,
) -> Result<McpServerConfig, tonic::Status> {
    h.control
        .configure_mcp_server(ConfigureMcpServerRequest {
            config: Some(config),
        })
        .await?
        .into_inner()
        .config
        .ok_or_else(|| tonic::Status::internal("no config in the response"))
}

async fn listed_names(h: &mut Harness) -> Setup<Vec<String>> {
    Ok(h.control
        .list_mcp_servers(ListMcpServersRequest {})
        .await?
        .into_inner()
        .servers
        .into_iter()
        .map(|server| server.name)
        .collect())
}

#[tokio::test]
async fn a_configured_server_round_trips_through_the_catalog() {
    let mut h = connect().await.expect("a harness");

    let created = configure(&mut h, stdio_config("linear"))
        .await
        .expect("create");
    assert!(!created.etag.is_empty(), "an etag is minted at creation");

    let listed = h
        .control
        .list_mcp_servers(ListMcpServersRequest {})
        .await
        .expect("list")
        .into_inner()
        .servers;
    assert_eq!(
        listed,
        vec![created],
        "the catalog returns what was created"
    );
}

#[tokio::test]
async fn creating_over_a_live_name_is_already_exists_not_an_overwrite() {
    let mut h = connect().await.expect("a harness");
    configure(&mut h, stdio_config("linear"))
        .await
        .expect("create");

    let refused = configure(&mut h, stdio_config("linear"))
        .await
        .expect_err("the name is taken");
    assert_eq!(refused.code(), Code::AlreadyExists);
    assert!(
        refused.message().contains("linear"),
        "the refusal names the taken name: {}",
        refused.message()
    );
}

#[tokio::test]
async fn an_update_needs_the_current_etag_and_a_stale_one_aborts() {
    let mut h = connect().await.expect("a harness");
    let created = configure(&mut h, stdio_config("linear"))
        .await
        .expect("create");

    // The current etag updates.
    let mut update = stdio_config("linear");
    update.local = true;
    update.etag = created.etag.clone();
    let updated = configure(&mut h, update).await.expect("update");
    assert!(updated.local);
    assert_ne!(updated.etag, created.etag, "each change mints a new etag");

    // The token the first read produced is now stale.
    let mut stale = stdio_config("linear");
    stale.etag = created.etag;
    let refused = configure(&mut h, stale).await.expect_err("stale");
    assert_eq!(refused.code(), Code::Aborted);
}

#[tokio::test]
async fn an_update_naming_an_absent_server_is_not_found() {
    let mut h = connect().await.expect("a harness");

    let mut update = stdio_config("linear");
    update.etag = "7".to_owned();
    let refused = configure(&mut h, update).await.expect_err("nothing there");
    assert_eq!(refused.code(), Code::NotFound);
}

#[tokio::test]
async fn deletion_needs_the_current_etag_and_removes_the_server() {
    let mut h = connect().await.expect("a harness");
    let created = configure(&mut h, stdio_config("linear"))
        .await
        .expect("create");

    let stale = h
        .control
        .delete_mcp_server(DeleteMcpServerRequest {
            name: "linear".to_owned(),
            etag: "0".to_owned(),
        })
        .await
        .expect_err("a stale etag must not delete");
    assert_eq!(stale.code(), Code::Aborted);

    h.control
        .delete_mcp_server(DeleteMcpServerRequest {
            name: "linear".to_owned(),
            etag: created.etag,
        })
        .await
        .expect("delete");

    assert_eq!(
        listed_names(&mut h).await.expect("list"),
        Vec::<String>::new()
    );

    let gone = h
        .control
        .delete_mcp_server(DeleteMcpServerRequest {
            name: "linear".to_owned(),
            etag: "1".to_owned(),
        })
        .await
        .expect_err("already gone");
    assert_eq!(gone.code(), Code::NotFound);
}

#[tokio::test]
async fn an_invalid_config_is_refused_before_anything_is_journaled() {
    let mut h = connect().await.expect("a harness");

    let refused = configure(&mut h, stdio_config("Not_A_Valid_Name"))
        .await
        .expect_err("the name breaks the namespace");
    assert_eq!(refused.code(), Code::InvalidArgument);

    let mut no_transport = stdio_config("linear");
    no_transport.transport = None;
    let refused = configure(&mut h, no_transport)
        .await
        .expect_err("no transport");
    assert_eq!(refused.code(), Code::InvalidArgument);

    assert_eq!(
        listed_names(&mut h).await.expect("list"),
        Vec::<String>::new(),
        "a refused config must leave no trace in the catalog"
    );
}

/// The catalog lives in a reserved stream; configuring a server must not make
/// a phantom session appear in the session list.
#[tokio::test]
async fn the_catalog_stream_never_appears_as_a_session() {
    let mut h = connect().await.expect("a harness");
    configure(&mut h, stdio_config("linear"))
        .await
        .expect("create");

    let sessions = h
        .control
        .list_sessions(horde_proto::v1::ListSessionsRequest::default())
        .await
        .expect("list sessions")
        .into_inner()
        .sessions;
    assert_eq!(sessions, Vec::new(), "no session was ever created here");
}
