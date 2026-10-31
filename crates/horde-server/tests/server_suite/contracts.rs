//! The contracts the services promise, asserted rather than assumed.
//!
//! Every test here exists because a mutation survived: a stub that fabricated
//! success, a handshake that returned an empty struct, an effect nothing looked
//! at. The pattern in all of them is the same — the previous tests checked that
//! a call *returned* something, never that the something was true.

use horde_proto::v1::{
    AddWorkspaceRepoRequest, AgentId, ArchiveSessionRequest, Assignment,
    CreateWorkspaceDefinitionRequest, DeleteWorkspaceDefinitionRequest,
    GetWorkspaceDefinitionRequest, HelloRequest, ListSessionsRequest, ListWorkersRequest,
    ListWorkspaceDefinitionsRequest, MintEnrollmentCodeRequest, PrewarmWorkspaceRequest,
    QueryUsageRequest, RegisterWorkerRequest, ReportCapabilitiesRequest,
    ReportWorkspaceInstancesRequest, RevokeEnrollmentRequest, SendMessageRequest, SessionId,
    UpdateWorkspaceDefinitionRequest, WorkerCapabilities, WorkerId, send_message_request,
    sync_events_response,
};
use tokio_stream::StreamExt;
use tonic::Code;

use crate::common::{Harness, Setup, connect, open_sync, seed};

/// Every sequence in a session, read back through SyncEvents.
async fn sequences(harness: &mut Harness, session_id: &str) -> Setup<Vec<u64>> {
    drain_sync(harness, session_id, |batch, out| {
        out.extend(batch.events.iter().map(|event| event.sequence));
    })
    .await
}

/// Replays a session to its caught-up marker, keeping whatever `collect` takes.
///
/// One cursor-and-drain loop rather than one per thing a test wants out of the
/// stream: the duplication gate caught the second copy, and a second copy of a
/// stream loop is also a second chance to get the terminator wrong.
async fn drain_sync<T>(
    harness: &mut Harness,
    session_id: &str,
    mut collect: impl FnMut(&horde_proto::v1::EventBatch, &mut Vec<T>),
) -> Setup<Vec<T>> {
    let mut stream = open_sync(&mut harness.control, session_id, 0, false).await?;

    let mut out = Vec::new();
    while let Some(message) = stream.next().await {
        match message?.update {
            Some(sync_events_response::Update::Batch(batch)) => collect(&batch, &mut out),
            Some(sync_events_response::Update::CaughtUp(_)) => break,
            _ => {}
        }
    }
    Ok(out)
}

/// Registers a worker with the given tags.
///
/// Three tests need a registered worker before they can assert anything about
/// listings or capability refreshes.
async fn register(h: &mut Harness, worker: &str, tags: &[&str]) -> Setup<()> {
    h.worker
        .register_worker(RegisterWorkerRequest {
            hello: crate::common::versioned_hello(),
            worker_id: Some(WorkerId::new(worker)),
            capabilities: Some(WorkerCapabilities {
                tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
                ..WorkerCapabilities::default()
            }),
            last_local_journal_sequence: None,
        })
        .await?;
    Ok(())
}

/// The tags the listing reports for a worker.
async fn listed_tags(h: &mut Harness, worker: &str) -> Setup<Vec<String>> {
    let workers = h
        .control
        .list_workers(ListWorkersRequest::default())
        .await?
        .into_inner()
        .workers;
    Ok(workers
        .into_iter()
        .find(|listed| {
            listed
                .worker_id
                .as_ref()
                .map(ToString::to_string)
                .as_deref()
                == Some(worker)
        })
        .and_then(|listed| listed.capabilities)
        .map(|capabilities| capabilities.tags)
        .unwrap_or_default())
}

/// The `recorded_at` stamps a session's events carry.
///
/// Shares the cursor-and-drain shape with [`sequences`]; both walk one sync
/// stream to its caught-up marker and differ only in what they keep.
async fn recorded_stamps(
    harness: &mut Harness,
    session_id: &str,
) -> Setup<Vec<prost_types::Timestamp>> {
    drain_sync(harness, session_id, |batch, out| {
        out.extend(batch.events.iter().filter_map(|event| event.recorded_at));
    })
    .await
}

// -- Honest refusals ---------------------------------------------------------

/// An unbuilt RPC must refuse. Every one of these had a mutant replacing the
/// refusal with `Ok(Default::default())`, which is precisely the fabricated
/// success the design forbids: a client cannot tell an empty answer from an
/// unbuilt feature.
#[tokio::test]
async fn every_unbuilt_control_plane_rpc_refuses_rather_than_fabricating_success() {
    let mut h = connect().await.expect("a harness");

    macro_rules! refuses {
        ($call:expr, $what:literal) => {{
            let status = $call.await.expect_err(concat!($what, " is not built"));
            assert_eq!(
                status.code(),
                Code::Unimplemented,
                concat!($what, " must refuse as Unimplemented, got: {}"),
                status.message()
            );
            assert!(
                !status.message().is_empty(),
                concat!($what, " must say what is not built")
            );
        }};
    }

    refuses!(
        h.control
            .mint_enrollment_code(MintEnrollmentCodeRequest::default()),
        "mint_enrollment_code"
    );
    refuses!(
        h.control
            .revoke_enrollment(RevokeEnrollmentRequest::default()),
        "revoke_enrollment"
    );

    // query_usage graduated from this list (W6c): it is built, so a default
    // request is now refused for what it actually is — a query that never
    // said how to group — rather than as unbuilt.
    let status = h
        .control
        .query_usage(QueryUsageRequest::default())
        .await
        .expect_err("a groupless usage query is refused");
    assert_eq!(status.code(), Code::InvalidArgument);
}

/// The request-level workspace's precedence truth table (fix a12): the spec's
/// own workspace always wins, the request-level id fills in only when the
/// spec names none, and the filled-in workspace is *consumed* — it lands in
/// the root agent's journaled spec, where placement reads it.
#[tokio::test]
async fn the_request_level_workspace_fills_in_without_overriding_the_spec() {
    use horde_proto::v1::CreateSessionRequest;

    let mut h = connect().await.expect("a harness");
    let definition = create_named(&mut h, "precedence-ws")
        .await
        .expect("a definition");
    let ws = definition.workspace_definition_id.clone();

    // Request-level only: the id is consumed into the root agent's spec.
    let created = h
        .control
        .create_session(CreateSessionRequest {
            title: "request-level".to_owned(),
            workspace_definition_id: ws.clone(),
            root_agent: None,
            idempotency_key: "ws-precedence-1".to_owned(),
        })
        .await
        .expect("the session creates")
        .into_inner();
    let session_id = created
        .session
        .and_then(|summary| summary.session_id)
        .map(|id| id.to_string())
        .expect("a session id");
    let spawned_workspaces = drain_sync(&mut h, &session_id, |batch, out| {
        for envelope in &batch.events {
            if let Some(horde_proto::v1::event::Kind::AgentSpawned(spawned)) =
                envelope.event.as_ref().and_then(|event| event.kind.clone())
            {
                out.push(
                    spawned
                        .spec
                        .and_then(|spec| spec.workspace_definition_id)
                        .map(|id| id.to_string()),
                );
            }
        }
    })
    .await
    .expect("the log replays");
    assert_eq!(
        spawned_workspaces,
        vec![ws.as_ref().map(ToString::to_string)],
        "the request-level workspace must land in the root agent's spec"
    );

    // Spec-level beside a bogus request-level id: the spec wins, so the
    // bogus id is never validated and the create succeeds.
    h.control
        .create_session(CreateSessionRequest {
            title: "spec-wins".to_owned(),
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(
                "ws-that-does-not-exist",
            )),
            root_agent: Some(horde_proto::v1::AgentSpec {
                workspace_definition_id: ws.clone(),
                ..horde_proto::v1::AgentSpec::default()
            }),
            idempotency_key: "ws-precedence-2".to_owned(),
        })
        .await
        .expect("the spec's workspace outranks the request-level id");
}

/// An unknown tier is refused at the RPC, naming the catalog's own — never
/// left for the deterministic fold to trip over (fix a12 / W7b f5).
#[tokio::test]
async fn a_spawn_naming_an_unknown_tier_is_refused_with_the_known_ones() {
    let mut h = connect().await.expect("a harness");
    let (session_id, _) = seed(&mut h, "tiers", "tier-key").await.expect("a session");

    let refused = h
        .control
        .spawn_agent(horde_proto::v1::SpawnAgentRequest {
            session_id: Some(SessionId::new(session_id)),
            parent_agent_id: None,
            spec: Some(horde_proto::v1::AgentSpec {
                model: Some(horde_proto::v1::ModelSelection {
                    selection: Some(horde_proto::v1::model_selection::Selection::Tier(
                        "imaginary".to_owned(),
                    )),
                }),
                ..horde_proto::v1::AgentSpec::default()
            }),
            idempotency_key: String::new(),
        })
        .await
        .expect_err("an unknown tier must be refused");
    assert_eq!(refused.code(), Code::InvalidArgument);
    for known in ["imaginary", "fast", "standard", "deep"] {
        assert!(
            refused.message().contains(known),
            "the refusal names the tier and the catalog: {}",
            refused.message()
        );
    }
}

// -- Workspaces --------------------------------------------------------------

fn atlas_repo() -> horde_proto::v1::WorkspaceRepo {
    horde_proto::v1::WorkspaceRepo {
        name: "atlas".to_owned(),
        remote_url: "https://example.com/atlas.git".to_owned(),
        default_branch: "main".to_owned(),
        layout_path: String::new(),
    }
}

/// Fetches a definition by id, returning the whole response — the read every
/// workspace assertion goes through.
async fn get_by_id(
    h: &mut Harness,
    id: Option<&horde_proto::v1::WorkspaceDefinitionId>,
) -> Result<horde_proto::v1::GetWorkspaceDefinitionResponse, tonic::Status> {
    h.control
        .get_workspace_definition(GetWorkspaceDefinitionRequest {
            workspace_definition_id: id.cloned(),
            name: String::new(),
        })
        .await
        .map(tonic::Response::into_inner)
}

async fn create_named(h: &mut Harness, name: &str) -> Setup<horde_proto::v1::WorkspaceDefinition> {
    let created = h
        .control
        .create_workspace_definition(CreateWorkspaceDefinitionRequest {
            name: name.to_owned(),
            repos: vec![atlas_repo()],
            unmanaged_paths: vec!["datasets".to_owned()],
        })
        .await?
        .into_inner()
        .definition
        .ok_or("a created definition")?;
    Ok(created)
}

async fn create_atlas(h: &mut Harness) -> Setup<horde_proto::v1::WorkspaceDefinition> {
    create_named(h, "atlas").await
}

/// [`create_atlas`], returning the id the instance-facing tests key on.
async fn created_atlas_id(h: &mut Harness) -> Setup<horde_proto::v1::WorkspaceDefinitionId> {
    create_atlas(h)
        .await?
        .workspace_definition_id
        .ok_or_else(|| "a definition id".into())
}

#[tokio::test]
async fn a_workspace_definition_round_trips_through_the_catalog() {
    let mut h = connect().await.expect("a harness");
    let created = create_atlas(&mut h).await.expect("create");

    assert!(!created.etag.is_empty(), "an etag is minted at creation");
    assert_eq!(
        created.repos[0].layout_path, "atlas",
        "an unset layout path defaults to the repo's name"
    );

    let fetched = get_by_id(&mut h, created.workspace_definition_id.as_ref())
        .await
        .expect("get by id")
        .definition
        .expect("a definition");
    assert_eq!(fetched, created, "the catalog returns what was created");

    let by_name = h
        .control
        .get_workspace_definition(GetWorkspaceDefinitionRequest {
            workspace_definition_id: None,
            name: "atlas".to_owned(),
        })
        .await
        .expect("get by name")
        .into_inner()
        .definition
        .expect("a definition");
    assert_eq!(by_name, created);

    let listed = h
        .control
        .list_workspace_definitions(ListWorkspaceDefinitionsRequest::default())
        .await
        .expect("list")
        .into_inner()
        .definitions;
    assert_eq!(listed, vec![created]);
}

/// A lookup that names nothing must be refused as the caller's mistake — a
/// NotFound for an empty name would read as "no workspaces exist".
#[tokio::test]
async fn a_lookup_with_neither_id_nor_name_is_invalid_not_missing() {
    let mut h = connect().await.expect("a harness");
    create_atlas(&mut h).await.expect("create");

    let refused = get_by_id(&mut h, None)
        .await
        .expect_err("nothing was asked for");
    assert_eq!(refused.code(), Code::InvalidArgument);
    assert!(
        refused.message().contains("id or a name"),
        "the refusal says what a lookup needs: {}",
        refused.message()
    );
}

/// Renaming may not shadow another live definition, and keeping one's own
/// name is not a collision with oneself.
#[tokio::test]
async fn a_rename_may_keep_its_own_name_but_not_take_a_neighbours() {
    let mut h = connect().await.expect("a harness");
    let atlas = create_atlas(&mut h).await.expect("create atlas");
    let beta = create_named(&mut h, "beta").await.expect("create beta");

    let update = |definition: &horde_proto::v1::WorkspaceDefinition, name: &str| {
        UpdateWorkspaceDefinitionRequest {
            workspace_definition_id: definition.workspace_definition_id.clone(),
            etag: definition.etag.clone(),
            name: name.to_owned(),
            unmanaged_paths: vec!["datasets".to_owned()],
            repos: vec![],
        }
    };

    let taken = h
        .control
        .update_workspace_definition(update(&beta, "atlas"))
        .await
        .expect_err("atlas is a live neighbour's name");
    assert_eq!(taken.code(), Code::AlreadyExists);

    let unchanged = h
        .control
        .update_workspace_definition(update(&atlas, "atlas"))
        .await
        .expect("keeping one's own name is not a collision")
        .into_inner()
        .definition
        .expect("the updated definition");
    assert_eq!(unchanged.name, "atlas");
}

/// An update's changes actually land — the returned definition and the
/// catalog agree on the new layout and unmanaged paths, and the events the
/// catalog stream carries hold no stale etag for a replay to trust.
#[tokio::test]
async fn an_update_lands_its_layout_and_the_log_carries_no_stale_etag() {
    let mut h = connect().await.expect("a harness");
    let created = create_atlas(&mut h).await.expect("create");

    let mut relaid = atlas_repo();
    relaid.layout_path = "code/atlas".to_owned();
    h.control
        .update_workspace_definition(UpdateWorkspaceDefinitionRequest {
            workspace_definition_id: created.workspace_definition_id.clone(),
            etag: created.etag.clone(),
            name: String::new(),
            unmanaged_paths: vec!["weights".to_owned()],
            repos: vec![relaid],
        })
        .await
        .expect("the layout update");

    let fetched = get_by_id(&mut h, created.workspace_definition_id.as_ref())
        .await
        .expect("get")
        .definition
        .expect("a definition");
    assert_eq!(
        fetched.repos[0].layout_path, "code/atlas",
        "the layout change is what update exists for"
    );
    assert_eq!(
        fetched.unmanaged_paths,
        vec!["weights".to_owned()],
        "so is the unmanaged set"
    );

    // The catalog stream's own events never carry an etag: the token is
    // derived from the log position on every fold, and a stored copy would
    // let a replay trust a token that was stale the moment it was written.
    let etags = drain_catalog(&mut h, |kind, out| {
        if let horde_proto::v1::event::Kind::WorkspaceDefinitionChanged(changed) = kind
            && let Some(definition) = &changed.definition
        {
            out.push(definition.etag.clone());
        }
    })
    .await
    .expect("the catalog stream replays");
    assert_eq!(
        etags,
        vec![String::new(), String::new()],
        "two changes, neither storing a token"
    );
}

/// Replays the workspace catalog stream, collecting each event's kind. The
/// catalog is a log stream like any other — it is only session *listings* it
/// stays out of — which is why [`drain_sync`]'s loop can read it too.
async fn drain_catalog<T>(
    h: &mut Harness,
    mut collect: impl FnMut(&horde_proto::v1::event::Kind, &mut Vec<T>),
) -> Setup<Vec<T>> {
    drain_sync(h, horde_server::workspace::CATALOG_STREAM, |batch, out| {
        for envelope in &batch.events {
            if let Some(kind) = envelope
                .event
                .as_ref()
                .and_then(|event| event.kind.as_ref())
            {
                collect(kind, out);
            }
        }
    })
    .await
}

#[tokio::test]
async fn a_second_definition_may_not_take_a_live_name() {
    let mut h = connect().await.expect("a harness");
    create_atlas(&mut h).await.expect("create");

    let status = create_atlas(&mut h)
        .await
        .expect_err("the name is taken")
        .downcast::<tonic::Status>()
        .expect("a status");
    assert_eq!(status.code(), Code::AlreadyExists);
    assert!(status.message().contains("atlas"));
}

/// The etag is the whole optimistic-concurrency story: a stale one must be
/// refused as ABORTED, and every successful mutation mints a fresh one.
#[tokio::test]
async fn workspace_mutations_enforce_the_etag() {
    let mut h = connect().await.expect("a harness");
    let created = create_atlas(&mut h).await.expect("create");

    let stale = h
        .control
        .update_workspace_definition(UpdateWorkspaceDefinitionRequest {
            workspace_definition_id: created.workspace_definition_id.clone(),
            etag: "stale".to_owned(),
            name: String::new(),
            unmanaged_paths: vec![],
            repos: vec![],
        })
        .await
        .expect_err("a stale etag is refused");
    assert_eq!(stale.code(), Code::Aborted);
    assert!(
        stale.message().contains("stale"),
        "the refusal names the supplied token: {}",
        stale.message()
    );

    let renamed = h
        .control
        .update_workspace_definition(UpdateWorkspaceDefinitionRequest {
            workspace_definition_id: created.workspace_definition_id.clone(),
            etag: created.etag.clone(),
            name: "atlas-v2".to_owned(),
            unmanaged_paths: vec!["datasets".to_owned()],
            repos: vec![],
        })
        .await
        .expect("a fresh etag updates")
        .into_inner()
        .definition
        .expect("the updated definition");
    assert_eq!(renamed.name, "atlas-v2");
    assert_ne!(renamed.etag, created.etag, "a mutation mints a fresh etag");
    assert_eq!(
        renamed.repos, created.repos,
        "an update with no repos leaves the repo set alone"
    );

    // Widening the repo set is AddWorkspaceRepo's job, and Update refuses it.
    let widened = h
        .control
        .update_workspace_definition(UpdateWorkspaceDefinitionRequest {
            workspace_definition_id: created.workspace_definition_id.clone(),
            etag: renamed.etag.clone(),
            name: String::new(),
            unmanaged_paths: vec![],
            repos: vec![
                atlas_repo(),
                horde_proto::v1::WorkspaceRepo {
                    name: "extra".to_owned(),
                    remote_url: "https://example.com/extra.git".to_owned(),
                    default_branch: String::new(),
                    layout_path: String::new(),
                },
            ],
        })
        .await
        .expect_err("update may not widen the repo set");
    assert_eq!(widened.code(), Code::InvalidArgument);

    let added = h
        .control
        .add_workspace_repo(AddWorkspaceRepoRequest {
            workspace_definition_id: created.workspace_definition_id.clone(),
            repo: Some(horde_proto::v1::WorkspaceRepo {
                name: "docs".to_owned(),
                remote_url: "https://example.com/docs.git".to_owned(),
                default_branch: "main".to_owned(),
                layout_path: String::new(),
            }),
            etag: renamed.etag.clone(),
        })
        .await
        .expect("add repo")
        .into_inner()
        .definition
        .expect("the widened definition");
    assert_eq!(added.repos.len(), 2, "the deliberate ceremony widens it");

    h.control
        .delete_workspace_definition(DeleteWorkspaceDefinitionRequest {
            workspace_definition_id: created.workspace_definition_id.clone(),
            etag: added.etag.clone(),
        })
        .await
        .expect("delete with the fresh etag");

    let gone = get_by_id(&mut h, created.workspace_definition_id.as_ref())
        .await
        .expect_err("a deleted definition is not found");
    assert_eq!(gone.code(), Code::NotFound);
}

/// The worker-local state file is the truth; a report corrects the server's
/// cache, and the cached view is what `GetWorkspaceDefinition` returns.
#[tokio::test]
async fn reported_instances_appear_on_the_definition_they_materialize() {
    let mut h = connect().await.expect("a harness");
    let definition_id = created_atlas_id(&mut h).await.expect("create");

    let report = horde_proto::v1::WorkspaceInstanceReport {
        workspace_definition_id: Some(definition_id.clone()),
        worker_id: Some(WorkerId::new("worker-a")),
        state: horde_proto::v1::WorkspaceInstanceState::Ready as i32,
        root_path: "/workspaces/atlas".to_owned(),
        repos: vec![],
        detail: String::new(),
        observed_at: None,
    };
    h.worker
        .report_workspace_instances(ReportWorkspaceInstancesRequest {
            worker_id: Some(WorkerId::new("worker-a")),
            reports: vec![report.clone()],
            authoritative: false,
        })
        .await
        .expect("reporting instances is built now");

    let instances = get_by_id(&mut h, Some(&definition_id))
        .await
        .expect("get")
        .instances;
    assert_eq!(instances, vec![report]);

    // An authoritative empty set means the worker no longer has it.
    h.worker
        .report_workspace_instances(ReportWorkspaceInstancesRequest {
            worker_id: Some(WorkerId::new("worker-a")),
            reports: vec![],
            authoritative: true,
        })
        .await
        .expect("authoritative replacement");
    let instances = get_by_id(&mut h, Some(&definition_id))
        .await
        .expect("get")
        .instances;
    assert!(
        instances.is_empty(),
        "the cache dropped what the worker lost"
    );
}

/// Prewarm requires a live definition and a connected worker, and answers
/// with the materializing report placement colocates onto.
#[tokio::test]
async fn prewarm_targets_a_connected_worker_and_reports_materializing() {
    let mut h = connect().await.expect("a harness");
    let definition_id = created_atlas_id(&mut h).await.expect("create");

    let unplaced = h
        .control
        .prewarm_workspace(PrewarmWorkspaceRequest {
            workspace_definition_id: Some(definition_id.clone()),
            worker_id: Some(WorkerId::new("nobody")),
            session_id: None,
        })
        .await
        .expect_err("no such worker is connected");
    assert_eq!(unplaced.code(), Code::FailedPrecondition);

    register(&mut h, "worker-a", &[]).await.expect("register");
    let report = h
        .control
        .prewarm_workspace(PrewarmWorkspaceRequest {
            workspace_definition_id: Some(definition_id.clone()),
            worker_id: Some(WorkerId::new("worker-a")),
            session_id: None,
        })
        .await
        .expect("prewarm")
        .into_inner()
        .report
        .expect("a report at acceptance");
    assert_eq!(
        report.state,
        horde_proto::v1::WorkspaceInstanceState::Materializing as i32,
        "acceptance is materializing; completion arrives as events"
    );

    // A prewarm with an *empty* session id names no session, so its request
    // journals to the catalog stream like a session-less one — an empty
    // stream name is not a stream.
    register(&mut h, "worker-b", &[]).await.expect("register");
    h.control
        .prewarm_workspace(PrewarmWorkspaceRequest {
            workspace_definition_id: Some(definition_id),
            worker_id: Some(WorkerId::new("worker-b")),
            session_id: Some(SessionId::new("")),
        })
        .await
        .expect("prewarm with an empty session id");

    let prewarms = drain_catalog(&mut h, |kind, out| {
        if matches!(
            kind,
            horde_proto::v1::event::Kind::WorkspacePrewarmRequested(_)
        ) {
            out.push(());
        }
    })
    .await
    .expect("the catalog stream replays");
    assert_eq!(
        prewarms.len(),
        2,
        "both session-less prewarms journal to the catalog stream"
    );
}

// -- Handshake ---------------------------------------------------------------

/// A server that will not name its version cannot be version-skewed against,
/// which is the whole point of the handshake (DESIGN.md §2).
#[tokio::test]
async fn the_control_plane_handshake_names_a_real_version() {
    let mut h = connect().await.expect("a harness");

    let server = h
        .control
        .hello(HelloRequest {
            // The version gate (fix a6) reads this; an empty request is now
            // a refusal, proven in `services.rs`.
            hello: crate::common::versioned_hello(),
        })
        .await
        .expect("hello must succeed")
        .into_inner()
        .server
        .expect("a handshake carries a ServerHello");

    assert_eq!(server.semver, env!("CARGO_PKG_VERSION"));
    assert!(!server.min_supported_semver.is_empty());
}

#[tokio::test]
async fn the_worker_handshake_names_the_same_version() {
    let mut h = connect().await.expect("a harness");

    let registered = h
        .worker
        .register_worker(crate::common::plain_registration("worker-a"))
        .await
        .expect("registration must succeed")
        .into_inner();

    let server = registered
        .server
        .expect("registration carries a ServerHello");
    assert_eq!(server.semver, env!("CARGO_PKG_VERSION"));
    assert!(!server.min_supported_semver.is_empty());
    assert_eq!(
        registered.resume_from_journal_sequence, 1,
        "a fresh worker starts at the first journal position"
    );
}

// -- Effects that nothing was looking at -------------------------------------

/// A registered worker must be visible, or a connected worker is
/// indistinguishable from none.
#[tokio::test]
async fn a_registered_worker_appears_in_the_listing_with_its_capabilities() {
    let mut h = connect().await.expect("a harness");

    assert!(
        h.control
            .list_workers(ListWorkersRequest::default())
            .await
            .expect("list must succeed")
            .into_inner()
            .workers
            .is_empty(),
        "nothing has registered yet"
    );

    register(&mut h, "worker-a", &["gpu"])
        .await
        .expect("registration");

    let workers = h
        .control
        .list_workers(ListWorkersRequest::default())
        .await
        .expect("list")
        .into_inner()
        .workers;

    assert_eq!(workers.len(), 1);
    assert_eq!(
        workers[0].worker_id.as_ref().map(ToString::to_string),
        Some("worker-a".to_owned())
    );
    assert_eq!(
        listed_tags(&mut h, "worker-a").await.expect("tags"),
        vec!["gpu".to_owned()]
    );
}

/// Capabilities are replaced wholesale, so a refresh must be observable —
/// otherwise the scheduler matches on facts that stopped being true.
#[tokio::test]
async fn refreshing_capabilities_replaces_what_the_listing_reports() {
    let mut h = connect().await.expect("a harness");

    register(&mut h, "worker-a", &["gpu"])
        .await
        .expect("registration");

    h.worker
        .report_capabilities(ReportCapabilitiesRequest {
            worker_id: Some(WorkerId::new("worker-a")),
            capabilities: Some(WorkerCapabilities {
                tags: vec!["macos".to_owned()],
                ..WorkerCapabilities::default()
            }),
        })
        .await
        .expect("a capability refresh must succeed");

    assert_eq!(
        listed_tags(&mut h, "worker-a").await.expect("tags"),
        vec!["macos".to_owned()],
        "a refresh replaces rather than merges, and must actually take effect"
    );
}

#[tokio::test]
async fn archiving_appends_an_event_and_reports_the_archived_session() {
    let mut h = connect().await.expect("a harness");
    let (session_id, _) = seed(&mut h, "temporary", "key-1").await.expect("seed");

    let before = sequences(&mut h, &session_id).await.expect("replay").len();

    let summary = h
        .control
        .archive_session(ArchiveSessionRequest {
            session_id: Some(SessionId::new(session_id.clone())),
            reason: "done".to_owned(),
        })
        .await
        .expect("archive must succeed")
        .into_inner()
        .session
        .expect("archiving reports the session it archived");

    assert_eq!(
        summary.session_id.as_ref().map(ToString::to_string),
        Some(session_id.clone()),
        "an empty summary would tell the caller nothing about what it archived"
    );
    assert_eq!(
        summary.state,
        horde_proto::v1::SessionLifecycleState::Archived as i32
    );
    assert_eq!(
        sequences(&mut h, &session_id).await.expect("replay").len(),
        before + 1,
        "archiving is an event like everything else"
    );
}

#[tokio::test]
async fn sending_a_message_reports_the_position_it_appended_at() {
    let mut h = connect().await.expect("a harness");
    let (session_id, root) = seed(&mut h, "messages", "key-1").await.expect("seed");

    let response = h
        .control
        .send_message(SendMessageRequest {
            to_agent_id: Some(AgentId::new(root)),
            body: Some(send_message_request::Body::Assignment(Assignment {
                assignment_id: None,
                text: "fix the build".to_owned(),
                payload: None,
            })),
            delivery: 0,
            idempotency_key: String::new(),
        })
        .await
        .expect("send must succeed")
        .into_inner();

    assert!(
        response.assignment_id.is_some(),
        "an assignment owes a result, so it must be given an id"
    );
    assert_eq!(
        response.session_id.as_ref().map(ToString::to_string),
        Some(session_id.clone())
    );

    let seen = sequences(&mut h, &session_id).await.expect("replay");
    assert_eq!(
        seen.last().copied(),
        Some(response.sequence),
        "the reported sequence must be where the event actually landed"
    );
}

/// `include_archived` is a filter, and a negation error would either hide
/// everything or hide nothing.
#[tokio::test]
async fn archived_sessions_are_hidden_by_default_and_shown_on_request() {
    let mut h = connect().await.expect("a harness");
    let (archived, _) = seed(&mut h, "old", "key-1").await.expect("seed");
    let (_kept, _) = seed(&mut h, "current", "key-2").await.expect("seed");

    h.control
        .archive_session(ArchiveSessionRequest {
            session_id: Some(SessionId::new(archived.clone())),
            reason: String::new(),
        })
        .await
        .expect("archive");

    let default = h
        .control
        .list_sessions(ListSessionsRequest::default())
        .await
        .expect("list")
        .into_inner()
        .sessions;
    assert_eq!(default.len(), 1, "the archived one is hidden");
    assert_ne!(
        default[0].session_id.as_ref().map(ToString::to_string),
        Some(archived.clone())
    );

    let all = h
        .control
        .list_sessions(ListSessionsRequest {
            include_archived: true,
            ..ListSessionsRequest::default()
        })
        .await
        .expect("list")
        .into_inner()
        .sessions;
    assert_eq!(all.len(), 2, "asking for archived shows both");
}

/// Server-assigned ids must actually distinguish things. A constant id would
/// make two sessions collide in the log.
#[tokio::test]
async fn server_assigned_ids_are_unique_and_non_empty() {
    let mut h = connect().await.expect("a harness");

    let (first, first_root) = seed(&mut h, "one", "").await.expect("seed");
    let (second, second_root) = seed(&mut h, "two", "").await.expect("seed");

    assert!(!first.is_empty() && !second.is_empty());
    assert_ne!(
        first, second,
        "two sessions created without idempotency keys must not share an id"
    );
    assert_ne!(first_root, second_root);

    assert_eq!(
        h.control
            .list_sessions(ListSessionsRequest::default())
            .await
            .expect("list")
            .into_inner()
            .sessions
            .len(),
        2,
        "colliding ids would silently merge the two sessions"
    );
}

/// `recorded_at` is the server's clock, and a client renders and reconciles
/// skew against it. A constant would make every event look simultaneous.
#[tokio::test]
async fn events_are_stamped_with_a_plausible_server_clock() {
    let mut h = connect().await.expect("a harness");
    let (session_id, _) = seed(&mut h, "clock", "key-1").await.expect("seed");

    let stamps = recorded_stamps(&mut h, &session_id)
        .await
        .expect("sync must succeed");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after the epoch")
        .as_secs() as i64;

    assert!(!stamps.is_empty(), "events must carry a recorded_at");
    for stamp in stamps {
        // A day either side: wide enough not to flake, narrow enough that 0, 1
        // or -1 fail.
        assert!(
            (stamp.seconds - now).abs() < 86_400,
            "recorded_at {} is not a plausible clock reading near {now}",
            stamp.seconds
        );
    }
}
