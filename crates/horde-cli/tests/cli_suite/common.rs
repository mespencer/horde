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

pub use horde_server_testkit::{Setup, journal_event, journal_event_at, seed, seeded, serve};

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

/// Registers `worker_id` as a connected worker speaking the ADR-0014
/// workspace protocol, so cleanup verbs target it rather than calling it
/// offline or too old.
pub async fn register_workspace_worker(endpoint: &str, worker_id: &str) -> Setup<()> {
    horde_server_testkit::register_worker_with(
        endpoint,
        worker_id,
        horde_proto::v1::WorkerCapabilities {
            workspace_protocol: horde_server::service::workspace_gc::WORKSPACE_PROTOCOL,
            ..horde_proto::v1::WorkerCapabilities::default()
        },
    )
    .await
}

/// Reports, as `worker_id`, that it holds a ready Instance of
/// `definition_id` — the fact the Server's Instance cache is built from.
pub async fn report_instance(endpoint: &str, worker_id: &str, definition_id: &str) -> Setup<()> {
    use horde_proto::v1::{
        ReportWorkspaceInstancesRequest, WorkerId, WorkspaceDefinitionId, WorkspaceInstanceReport,
        WorkspaceInstanceState,
    };
    let mut worker = horde_server_testkit::worker_spine(endpoint).await?;
    worker
        .report_workspace_instances(ReportWorkspaceInstancesRequest {
            worker_id: Some(WorkerId::new(worker_id)),
            reports: vec![WorkspaceInstanceReport {
                workspace_definition_id: Some(WorkspaceDefinitionId::new(definition_id)),
                worker_id: Some(WorkerId::new(worker_id)),
                state: WorkspaceInstanceState::Ready.into(),
                root_path: format!("/instances/{definition_id}"),
                ..WorkspaceInstanceReport::default()
            }],
            authoritative: true,
        })
        .await?;
    Ok(())
}

/// Every event kind on the workspace housekeeping stream, in order.
pub async fn housekeeping(endpoint: &str) -> Setup<Vec<horde_proto::v1::event::Kind>> {
    let mut client = Client::connect(endpoint).await?;
    let mut kinds = Vec::new();
    client
        .sync(
            &horde_cli::client::SyncOptions::new(horde_proto::WORKSPACE_HOUSEKEEPING_STREAM),
            |envelope| {
                if let Some(kind) = envelope.event.as_ref().and_then(|event| event.kind.clone()) {
                    kinds.push(kind);
                }
            },
            |_| {},
        )
        .await?;
    Ok(kinds)
}

/// The gc requests on the housekeeping stream, in order.
pub async fn gc_requests(endpoint: &str) -> Setup<Vec<horde_proto::v1::WorkspaceGcRequested>> {
    Ok(housekeeping(endpoint)
        .await?
        .into_iter()
        .filter_map(|kind| match kind {
            horde_proto::v1::event::Kind::WorkspaceGcRequested(requested) => Some(requested),
            _ => None,
        })
        .collect())
}

/// Defines the one-repo workspace `name` and returns its definition.
pub async fn define_workspace(
    endpoint: &str,
    name: &str,
) -> Setup<horde_proto::v1::WorkspaceDefinition> {
    horde_server_testkit::define_atlas_workspace(endpoint, name).await
}

/// A definition's id as text.
#[must_use]
pub fn definition_id(definition: &horde_proto::v1::WorkspaceDefinition) -> String {
    definition
        .workspace_definition_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default()
}
