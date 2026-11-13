//! What is `horde-cli`-shaped about the shared server harness.
//!
//! The harness itself — both spines on a loopback socket, session seeding,
//! and the worker-shaped journal push — lives in `horde-server-testkit`,
//! because `horde-mcp` and `horde-server` test against the same arrangement
//! and three private copies of a transport harness drift. What stays here is
//! only what no other suite wants: the [`Client::with_channel`] entry and the
//! note spammer.
//!
//! Not every sibling suite uses every item here, so some items would be
//! dead code — and some re-exports unused imports — without the allow. It
//! is scoped to this module rather than sprinkled at the call sites, which
//! is the conventional shape for an integration-test `common` module.
#![allow(dead_code, unreachable_pub, unused_imports)]

pub use horde_server_testkit::{Setup, journal_event, seed, seeded, serve};

use horde_cli::Client;
use horde_proto::v1::DeliveryMode;

/// Parses an argument vector against a live endpoint and runs it through
/// `cli::run` — the same code the binary runs.
///
/// A parse failure is returned rather than panicked: `panic!` is denied
/// outside `#[test]` functions, and clippy's in-test allowance does not
/// reach free helpers in an integration test file.
pub async fn invoke(endpoint: &str, args: &[&str]) -> Setup<()> {
    use clap::Parser as _;
    let mut argv = vec!["horde", "--endpoint", endpoint];
    argv.extend_from_slice(args);
    let cli =
        horde_cli::cli::Cli::try_parse_from(argv).map_err(|error| format!("{args:?}: {error}"))?;
    let format = horde_cli::Format::from_flag(cli.json);
    horde_cli::cli::run(&cli, format).await?;
    Ok(())
}

/// A client wired to its own private server, over a channel the caller built
/// rather than one [`Client::connect`] dialled.
///
/// This is the [`Client::with_channel`] seam, and entering through it here is
/// what keeps it exercised: a frontend that owns its transport — an embedded
/// server, a custom connector, a pre-warmed pool — constructs a client this
/// way, and a constructor no test ever calls is one nobody knows still works.
pub async fn client() -> Setup<Client> {
    let endpoint = serve().await?;
    let channel = tonic::transport::Endpoint::try_from(endpoint)?
        .connect()
        .await?;
    Ok(Client::with_channel(channel))
}

/// Appends `count` notes to an agent, which is how a test gets a log with
/// enough in it to replay from an offset.
pub async fn send_notes(client: &mut Client, agent_id: &str, count: usize) -> Setup<()> {
    for index in 0..count {
        client
            .send_note(
                agent_id,
                &format!("note {index}"),
                DeliveryMode::TurnBoundary,
                "",
            )
            .await?;
    }
    Ok(())
}

/// What a worker would journal when an agent's tool call escalates to a
/// human: a permission when `questions` is empty, a question request
/// otherwise. The control plane cannot author this — observed facts enter
/// through the worker spine — so tests journal it the way a worker does.
pub fn escalation(
    agent_id: &str,
    request_id: &str,
    rendered: &str,
    questions: Vec<horde_proto::v1::Question>,
) -> horde_proto::v1::Event {
    use horde_proto::v1::{
        AgentId, Event, PermissionRequest, PermissionRequestId, PermissionRequested,
        ProposedToolCall, event::Kind,
    };
    let tool_name = if questions.is_empty() {
        "Bash"
    } else {
        "AskUserQuestion"
    };
    Event {
        agent_id: Some(AgentId::new(agent_id)),
        occurred_at: None,
        idempotency_key: format!("perm-{request_id}"),
        kind: Some(Kind::PermissionRequested(PermissionRequested {
            request: Some(PermissionRequest {
                request_id: Some(PermissionRequestId::new(request_id)),
                agent_id: Some(AgentId::new(agent_id)),
                tool_call: Some(ProposedToolCall {
                    tool_name: tool_name.to_owned(),
                    input: None,
                    rendered: rendered.to_owned(),
                    questions,
                }),
                tree_path: vec!["tui".to_owned(), "runner".to_owned()],
                permission_mode: "manual".to_owned(),
                escalated_from: 0,
                expires_at: None,
                forward: None,
            }),
        })),
    }
}
