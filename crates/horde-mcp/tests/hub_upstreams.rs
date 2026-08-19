//! The hub's client half against real MCP servers: the in-memory rmcp
//! server for the protocol path, and the spawned `mcp_stub_server` fixture
//! for the child-process path a `local` config takes on a Worker.

mod common;

use common::{Setup, stub_mcp, within};
use horde_mcp::hub::{HubError, HubServerConfig, HubTransport, Placement, ToolProxy, Upstream};

/// The result's single text block.
fn text_of(result: &rmcp::model::CallToolResult) -> Setup<String> {
    match result.content.first() {
        Some(rmcp::model::ContentBlock::Text(text)) => Ok(text.text.clone()),
        other => Err(format!("expected one text block, got {other:?}").into()),
    }
}

fn echo_arguments(message: &str) -> Option<rmcp::model::JsonObject> {
    let mut arguments = rmcp::model::JsonObject::new();
    arguments.insert(
        "message".to_owned(),
        serde_json::Value::String(message.to_owned()),
    );
    Some(arguments)
}

#[tokio::test]
async fn an_upstream_lists_and_calls_a_real_server() {
    let upstream = within("the upstream connects", stub_mcp::upstream("stub"))
        .await
        .expect("the in-memory server must connect");

    let names: Vec<&str> = upstream
        .tools()
        .iter()
        .map(|tool| tool.name.as_ref())
        .collect();
    assert!(names.contains(&"echo"), "advertised: {names:?}");

    // The Debug rendering reaches logs; it must say what it is and which
    // server it speaks for, or every log line carrying an upstream is noise.
    let rendered = format!("{upstream:?}");
    assert!(
        rendered.contains("Upstream") && rendered.contains("stub"),
        "the Debug form must name the type and the server: {rendered}"
    );

    let result = within(
        "the call round-trips",
        upstream.call("echo", echo_arguments("wire")),
    )
    .await
    .expect("the call must succeed");
    assert_eq!(
        text_of(&result).expect("a text answer"),
        "echo:wire",
        "the answer is the server's, verbatim"
    );
}

/// The child-process path: a `local` config's exact production route —
/// spawned from the config's command, env delivered per-child, killed by
/// handle when the upstream drops.
#[tokio::test]
async fn a_stdio_config_spawns_a_child_and_its_env_reaches_it() {
    // The OS validates a freshly built binary on its first-ever exec, and on
    // a loaded machine (a cargo-mutants scratch tree, say) that can outlast
    // any protocol ceiling. Pay it here, named, before the handshake that
    // this test is actually about.
    stub_mcp::warm()
        .await
        .expect("the stub binary's first launch must complete");

    let config = HubServerConfig {
        name: "browser".to_owned(),
        transport: HubTransport::Stdio {
            command: stub_mcp::BINARY.to_owned(),
            args: Vec::new(),
            env: [(
                "HORDE_STUB_IDENTITY".to_owned(),
                "agent-42-browser".to_owned(),
            )]
            .into(),
        },
        placement: Placement::Local,
    };

    let upstream = within("the child connects", Upstream::connect(&config))
        .await
        .expect("the child must spawn and answer the handshake");

    let result = within("whoami answers", upstream.call("whoami", None))
        .await
        .expect("the call must succeed");
    assert_eq!(
        text_of(&result).expect("a text answer"),
        "agent-42-browser",
        "the config's env map must reach the child it configures"
    );
}

#[tokio::test]
async fn a_config_naming_a_missing_binary_fails_at_spawn_with_the_server_named() {
    let config = HubServerConfig {
        name: "ghost".to_owned(),
        transport: HubTransport::Stdio {
            command: "/nonexistent/horde-test-no-such-binary".to_owned(),
            args: Vec::new(),
            env: [].into(),
        },
        placement: Placement::Hub,
    };

    let error = within("the spawn fails", Upstream::connect(&config))
        .await
        .expect_err("a missing binary cannot connect");
    assert!(
        matches!(&error, HubError::Spawn { server, .. } if server == "ghost"),
        "got {error}"
    );
}

/// The proxy over real upstreams: prefixed naming, filtered hub link,
/// routing, and the refusal for names nothing serves.
#[tokio::test]
async fn a_proxy_namespaces_filters_and_routes_across_real_upstreams() {
    let local = within("the local upstream connects", stub_mcp::upstream("browser"))
        .await
        .expect("connects");
    // The hub link's tools arrive pre-namespaced: model that by proxying a
    // prefixed source through a second proxy, exactly as the hub serves them.
    let hub_side = within(
        "the hub-side upstream connects",
        stub_mcp::upstream("linear"),
    )
    .await
    .expect("connects");

    let proxy = ToolProxy::new().with_server(local).with_server(hub_side);

    let names: Vec<String> = proxy
        .tools()
        .iter()
        .map(|tool| tool.name.to_string())
        .collect();
    assert!(names.contains(&"browser__echo".to_owned()), "{names:?}");
    assert!(names.contains(&"linear__echo".to_owned()), "{names:?}");

    let result = within(
        "the namespaced call routes to its own server",
        proxy.call("linear__echo", echo_arguments("routed")),
    )
    .await
    .expect("the call must succeed");
    assert_eq!(text_of(&result).expect("text"), "echo:routed");

    let unknown = within(
        "an unserved name refuses",
        proxy.call("payments__refund", None),
    )
    .await
    .expect_err("nothing serves payments");
    assert!(
        matches!(&unknown, HubError::UnknownTool { name } if name == "payments__refund"),
        "got {unknown}"
    );
}
