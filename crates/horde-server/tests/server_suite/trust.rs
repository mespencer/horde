//! The trust boundary over the real mTLS door (ADR-0009).
//!
//! Every identity here is issued through the production enrollment path,
//! and every call crosses the production listener — the door, the stamped
//! method path, the role table, the handlers' identity reads. The unit
//! tables in `horde_server::trust` say what the rules are; these say the
//! listener actually enforces them.

use crate::common::Setup;
use horde_identity::{PrincipalRole, StoredIdentity};
use horde_proto::prost::Message;
use horde_proto::prost_types::FileDescriptorSet;
use horde_proto::prost_types::{Struct, Value, value::Kind as ValueKind};
use horde_proto::v1::{
    AdapterCapability, AgentId, AnswerPermissionRequest, Event, HarnessCapability, JournaledEvent,
    JournaledEventBatch, McpServerConfigChanged, PermissionOutcome, PermissionRequest,
    PermissionRequestId, PermissionRequested, PushEventsRequest, RegisterWorkerRequest, SessionId,
    WorkerCapabilities, WorkerId, control_plane_service_client::ControlPlaneServiceClient,
    event::Kind, push_events_request, push_events_response,
    worker_service_client::WorkerServiceClient,
};
use horde_server::trust::{Admits, GUARDED_SERVICES, admits};
use horde_server_testkit::secure::{SecureHarness, hello, serve_secured, within};

/// Every guarded method path the compiled proto declares.
fn guarded_methods() -> Setup<Vec<String>> {
    let set = FileDescriptorSet::decode(horde_proto::FILE_DESCRIPTOR_SET)?;
    let mut paths = Vec::new();
    for file in &set.file {
        for service in &file.service {
            let full = format!("{}.{}", file.package(), service.name());
            if GUARDED_SERVICES.contains(&full.as_str()) {
                for method in &service.method {
                    paths.push(format!("/{full}/{}", method.name()));
                }
            }
        }
    }
    Ok(paths)
}

/// Calls `path` with an empty body as `identity`, returning the status. The
/// door rules before any body is decoded, so an empty unary message reaches
/// it the same way for every method shape — and a method the door admits
/// may answer however its handler answers an empty request.
async fn call_raw(
    harness: &SecureHarness,
    identity: &StoredIdentity,
    path: &str,
) -> Setup<tonic::Status> {
    let channel = harness.main_endpoint_as(identity)?.connect().await?;
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await?;
    let path = tonic::codegen::http::uri::PathAndQuery::try_from(path.to_owned())?;
    let outcome = within(
        "the raw call",
        grpc.unary(
            tonic::Request::new(()),
            path,
            tonic_prost::ProstCodec::<(), ()>::default(),
        ),
    )
    .await?;
    Ok(outcome.err().unwrap_or_else(|| tonic::Status::ok("")))
}

/// The structural guarantee, end to end: every method on a guarded service
/// refuses the role its row excludes, at the listener, with the refusal
/// naming the method — so a new RPC is gated the moment it has a row, and
/// cannot be served at all until it has one.
#[tokio::test]
async fn every_guarded_method_refuses_the_role_its_policy_excludes() -> Setup<()> {
    let harness = serve_secured().await?;
    let worker = harness.enroll(PrincipalRole::Worker, "mac-studio").await?;
    let client = harness.enroll(PrincipalRole::Client, "laptop").await?;

    let mut refused = 0_u32;
    for path in guarded_methods()? {
        let (wrong, who) = match admits(&path) {
            Some(Admits::Clients) => (&worker, "worker:mac-studio"),
            Some(Admits::Workers) => (&client, "client:laptop"),
            Some(Admits::Anyone) => continue,
            None => return Err(format!("{path} has no policy").into()),
        };
        let status = call_raw(&harness, wrong, &path).await?;
        assert_eq!(
            status.code(),
            tonic::Code::PermissionDenied,
            "{path}: {status:?}"
        );
        assert!(
            status.message().contains(&path) && status.message().contains(who),
            "{path}: the refusal names the method and the caller: {}",
            status.message()
        );
        refused += 1;
    }
    assert!(
        refused >= 35,
        "the walk refused {refused} methods; it must cover the spines"
    );
    Ok(())
}

/// The worker a request speaks for is its certificate's: registering under
/// another worker's name is refused, and so is a client registering at all.
#[tokio::test]
async fn a_worker_cannot_register_as_another_worker() -> Setup<()> {
    let harness = serve_secured().await?;
    let identity = harness.enroll(PrincipalRole::Worker, "mac-studio").await?;
    let mut worker = harness.worker_client(&identity).await?;

    let forged = within(
        "the forged registration",
        worker.register_worker(registration("gpu-box", WorkerCapabilities::default())),
    )
    .await?
    .expect_err("a body naming another worker is refused");
    assert_eq!(forged.code(), tonic::Code::PermissionDenied);
    assert!(forged.message().contains("gpu-box"), "{forged}");

    let honest = within(
        "the honest registration",
        worker.register_worker(registration("mac-studio", WorkerCapabilities::default())),
    )
    .await??
    .into_inner();
    assert_eq!(
        honest.worker_id.map(|id| id.to_string()),
        Some("mac-studio".to_owned())
    );
    Ok(())
}

fn registration(name: &str, capabilities: WorkerCapabilities) -> RegisterWorkerRequest {
    RegisterWorkerRequest {
        hello: Some(hello(PrincipalRole::Worker, name)),
        worker_id: Some(WorkerId::new(name)),
        capabilities: Some(capabilities),
    }
}

/// A worker advertising one harness, with or without input rewriting.
fn capabilities(rewrites: bool) -> WorkerCapabilities {
    WorkerCapabilities {
        harnesses: vec![HarnessCapability {
            provider: "fake".to_owned(),
            installed: true,
            capabilities: if rewrites {
                vec![AdapterCapability::ToolInputRewrite as i32]
            } else {
                Vec::new()
            },
            ..HarnessCapability::default()
        }],
        ..WorkerCapabilities::default()
    }
}

/// Journals `events` as the certified worker, from journal position one,
/// and returns the last ack.
async fn journal(
    worker: &mut WorkerServiceClient<tonic::transport::Channel>,
    events: Vec<(String, Event)>,
) -> Setup<u64> {
    let (to_server, mut acks) = horde_server_testkit::open_journal(worker, "mac-studio", 0).await?;
    within("the start ack", acks.message()).await??;
    to_server
        .send(PushEventsRequest {
            body: Some(push_events_request::Body::Batch(JournaledEventBatch {
                events: events
                    .into_iter()
                    .enumerate()
                    .map(|(index, (session, event))| JournaledEvent {
                        journal_sequence: index as u64 + 1,
                        session_id: Some(SessionId::new(session)),
                        event: Some(event),
                    })
                    .collect(),
                acked_command_sequence: 0,
            })),
        })
        .await?;
    match within("the batch ack", acks.message())
        .await??
        .and_then(|r| r.body)
    {
        Some(push_events_response::Body::Ack(ack)) => Ok(ack.acked_journal_sequence),
        other => Err(format!("expected an ack, got {other:?}").into()),
    }
}

/// A certified worker's server-only event is dead-lettered, not appended,
/// and does not stop the journal: the ack covers it, and the observation
/// after it lands.
#[tokio::test]
async fn a_certified_workers_server_only_event_is_dead_lettered() -> Setup<()> {
    let harness = serve_secured().await?;
    let identity = harness.enroll(PrincipalRole::Worker, "mac-studio").await?;
    let mut worker = harness.worker_client(&identity).await?;
    within(
        "the registration",
        worker.register_worker(registration("mac-studio", capabilities(true))),
    )
    .await??;
    let operator = harness.enroll(PrincipalRole::Client, "laptop").await?;
    let mut client = harness.client(&operator).await?;
    let (summary, root) = within("the session", client.create_session("origin", "")).await??;
    let session = summary
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .ok_or("a session id")?;

    let catalog_change = Event {
        kind: Some(Kind::McpServerConfigChanged(
            McpServerConfigChanged::default(),
        )),
        ..Event::default()
    };
    let observed = Event {
        agent_id: Some(AgentId::new(root.to_string())),
        kind: Some(Kind::TurnInterrupted(
            horde_proto::v1::TurnInterrupted::default(),
        )),
        ..Event::default()
    };
    let acked = journal(
        &mut worker,
        vec![
            (
                horde_server::mcp::MCP_CATALOG_STREAM.to_owned(),
                catalog_change,
            ),
            (session.clone(), observed),
        ],
    )
    .await?;

    assert_eq!(acked, 2, "the refusal does not stop the journal");
    assert!(
        harness
            .log
            .read_all(horde_server::mcp::MCP_CATALOG_STREAM)
            .await?
            .is_empty(),
        "the catalog was not touched"
    );
    let letters = harness
        .log
        .read_all(horde_server::service::ingest::DEAD_LETTER_STREAM)
        .await?;
    assert_eq!(letters.len(), 1, "one refusal, recorded");
    let interrupts = harness
        .log
        .read_all(&session)
        .await?
        .iter()
        .filter_map(|stored| Event::decode(stored.payload.as_slice()).ok())
        .filter(|event| matches!(event.kind, Some(Kind::TurnInterrupted(_))))
        .count();
    assert_eq!(interrupts, 1, "the admissible observation landed");
    Ok(())
}

/// Raises one permission request for `root` from the certified worker, with
/// id `request_id`.
async fn raise(
    worker: &mut WorkerServiceClient<tonic::transport::Channel>,
    session: &str,
    root: &str,
    request_id: &str,
) -> Setup<()> {
    let mut input = Struct::default();
    input.fields.insert(
        "path".to_owned(),
        Value {
            kind: Some(ValueKind::StringValue("/etc/hosts".to_owned())),
        },
    );
    let raised = Event {
        agent_id: Some(AgentId::new(root)),
        idempotency_key: request_id.to_owned(),
        kind: Some(Kind::PermissionRequested(PermissionRequested {
            request: Some(PermissionRequest {
                request_id: Some(PermissionRequestId::new(request_id)),
                agent_id: Some(AgentId::new(root)),
                tool_call: Some(horde_proto::v1::ProposedToolCall {
                    tool_name: "Write".to_owned(),
                    input: Some(input),
                    rendered: String::new(),
                }),
                ..PermissionRequest::default()
            }),
        })),
        ..Event::default()
    };
    journal(worker, vec![(session.to_owned(), raised)]).await?;
    Ok(())
}

/// A narrowing on the wire, as `horde approve --narrow` sends it.
fn narrowing(request_id: &str) -> AnswerPermissionRequest {
    let mut clamp = Struct::default();
    clamp.fields.insert(
        "path".to_owned(),
        Value {
            kind: Some(ValueKind::StringValue("/tmp/hosts".to_owned())),
        },
    );
    AnswerPermissionRequest {
        request_id: Some(PermissionRequestId::new(request_id)),
        outcome: PermissionOutcome::Allow as i32,
        updated_input: Some(clamp),
        ..AnswerPermissionRequest::default()
    }
}

/// A session whose root is placed on a certified worker advertising
/// `rewrites`, with one permission request pending: the arrangement both
/// answer tests open with.
async fn pending_permission(
    harness: &SecureHarness,
    rewrites: bool,
) -> Setup<ControlPlaneServiceClient<tonic::transport::Channel>> {
    let identity = harness.enroll(PrincipalRole::Worker, "mac-studio").await?;
    let mut worker = harness.worker_client(&identity).await?;
    within(
        "the registration",
        worker.register_worker(registration("mac-studio", capabilities(rewrites))),
    )
    .await??;
    let operator = harness.enroll(PrincipalRole::Client, "laptop").await?;
    let mut client = harness.client(&operator).await?;
    let (summary, root) = within("the session", client.create_session("asks", "")).await??;
    let session = summary
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .ok_or("a session id")?;
    raise(&mut worker, &session, &root.to_string(), "call-1").await?;
    let channel = harness.main_endpoint_as(&operator)?.connect().await?;
    Ok(ControlPlaneServiceClient::new(channel))
}

/// The decision a client records names that client — and only a client can
/// record one: the worker channel that raised the request cannot answer it.
#[tokio::test]
async fn a_decision_is_the_answering_clients_and_never_a_workers() -> Setup<()> {
    let harness = serve_secured().await?;
    let mut control = pending_permission(&harness, true).await?;

    let worker_identity = harness.enroll(PrincipalRole::Worker, "gpu-box").await?;
    let mut as_worker = ControlPlaneServiceClient::new(
        harness
            .main_endpoint_as(&worker_identity)?
            .connect()
            .await?,
    );
    let refused = within(
        "the worker's answer",
        as_worker.answer_permission(AnswerPermissionRequest {
            request_id: Some(PermissionRequestId::new("call-1")),
            outcome: PermissionOutcome::Allow as i32,
            ..AnswerPermissionRequest::default()
        }),
    )
    .await?
    .expect_err("a worker cannot answer a permission prompt");
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);

    within(
        "the client's narrowing",
        control.answer_permission(narrowing("call-1")),
    )
    .await??;
    let decisions: Vec<horde_proto::v1::PermissionDecision> = harness_decisions(&harness).await?;
    assert_eq!(decisions.len(), 1, "{decisions:?}");
    assert_eq!(
        decisions[0].decided_by_actor,
        Some(horde_proto::v1::Actor {
            actor: Some(horde_proto::v1::actor::Actor::ClientId(
                horde_proto::v1::ClientId::new("laptop")
            )),
        }),
        "the decision is the laptop's"
    );
    assert!(
        decisions[0].updated_input.is_some(),
        "an adapter that rewrites takes the narrowing"
    );
    Ok(())
}

/// A narrowing for an agent whose adapter cannot rewrite is refused with a
/// precondition the operator can act on, and nothing is decided.
#[tokio::test]
async fn a_narrowing_is_refused_for_an_adapter_that_cannot_rewrite() -> Setup<()> {
    let harness = serve_secured().await?;
    let mut control = pending_permission(&harness, false).await?;

    let refused = within(
        "the narrowing",
        control.answer_permission(narrowing("call-1")),
    )
    .await?
    .expect_err("nobody can apply this narrowing");
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().contains("TOOL_INPUT_REWRITE"),
        "{refused}"
    );
    assert!(harness_decisions(&harness).await?.is_empty());

    // Allowing as proposed still works.
    within(
        "the plain allow",
        control.answer_permission(AnswerPermissionRequest {
            request_id: Some(PermissionRequestId::new("call-1")),
            outcome: PermissionOutcome::Allow as i32,
            ..AnswerPermissionRequest::default()
        }),
    )
    .await??;
    assert_eq!(harness_decisions(&harness).await?.len(), 1);
    Ok(())
}

/// Every permission decision in every session of the harness's log.
async fn harness_decisions(
    harness: &SecureHarness,
) -> Setup<Vec<horde_proto::v1::PermissionDecision>> {
    let mut decisions = Vec::new();
    for session in harness.log.session_ids().await? {
        for stored in harness.log.read_all(&session).await? {
            if let Some(Kind::PermissionDecided(decided)) =
                Event::decode(stored.payload.as_slice())?.kind
                && let Some(decision) = decided.decision
            {
                decisions.push(decision);
            }
        }
    }
    Ok(decisions)
}
