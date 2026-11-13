//! ADR-0014 at the Server, over the wire: Definition and spawn validation,
//! branch claims, the protocol fence, the gc verbs and what they command,
//! `delete --purge`, and the Instance listing.

use horde_proto::v1::stream_commands_response::Command;
use horde_proto::v1::{
    AgentSpec, CheckoutMode, CollectWorkspaceGarbageRequest, CreateWorkspaceDefinitionRequest,
    DeleteWorkspaceDefinitionRequest, GcLevel, ListWorkspaceInstancesRequest,
    RegisterWorkerRequest, ReportWorkspaceInstancesRequest, SessionId, SpawnAgentRequest,
    StreamCommandsRequest, WorkerCapabilities, WorkerId, WorkspaceDefinitionId,
    WorkspaceInstanceReport, WorkspaceInstanceState, WorkspaceRepo,
};

use crate::common::{Harness, Setup, connect, seed};

fn repo(name: &str, layout: &str) -> WorkspaceRepo {
    WorkspaceRepo {
        name: name.to_owned(),
        remote_url: format!("https://example.com/{name}.git"),
        default_branch: "main".to_owned(),
        layout_path: layout.to_owned(),
        ..WorkspaceRepo::default()
    }
}

async fn define(
    harness: &mut Harness,
    request: CreateWorkspaceDefinitionRequest,
) -> Result<String, tonic::Status> {
    Ok(harness
        .control
        .create_workspace_definition(request)
        .await?
        .into_inner()
        .definition
        .and_then(|definition| definition.workspace_definition_id)
        .map(|id| id.to_string())
        .unwrap_or_default())
}

async fn plain_workspace(harness: &mut Harness) -> Setup<String> {
    Ok(define(
        harness,
        CreateWorkspaceDefinitionRequest {
            name: "suite".to_owned(),
            repos: vec![repo("atlas", ""), repo("proto", "")],
            ..CreateWorkspaceDefinitionRequest::default()
        },
    )
    .await?)
}

async fn register(harness: &mut Harness, worker: &str, protocol: u32) -> Setup<()> {
    harness
        .worker
        .register_worker(RegisterWorkerRequest {
            hello: crate::common::versioned_hello(),
            worker_id: Some(WorkerId::new(worker)),
            capabilities: Some(WorkerCapabilities {
                supports_workspaces: true,
                workspace_protocol: protocol,
                ..WorkerCapabilities::default()
            }),
            last_local_journal_sequence: None,
        })
        .await?;
    Ok(())
}

async fn report_instance(harness: &mut Harness, worker: &str, definition: &str) -> Setup<()> {
    harness
        .worker
        .report_workspace_instances(ReportWorkspaceInstancesRequest {
            worker_id: Some(WorkerId::new(worker)),
            reports: vec![WorkspaceInstanceReport {
                workspace_definition_id: Some(WorkspaceDefinitionId::new(definition)),
                worker_id: Some(WorkerId::new(worker)),
                state: WorkspaceInstanceState::Ready.into(),
                checkouts: vec![horde_proto::v1::CheckoutReport {
                    agent_id: Some(horde_proto::v1::AgentId::new("agent-9")),
                    branch: "horde/agent-9".to_owned(),
                    state: horde_proto::v1::CheckoutState::Orphan.into(),
                    ..horde_proto::v1::CheckoutReport::default()
                }],
                filesystem_free_bytes: 1 << 40,
                filesystem_total_bytes: 1 << 41,
                ..WorkspaceInstanceReport::default()
            }],
            authoritative: true,
        })
        .await?;
    Ok(())
}

/// A harness whose workspace a protocol-2 worker reports holding.
async fn held_workspace() -> Setup<(Harness, String)> {
    let mut harness = connect().await?;
    let workspace = plain_workspace(&mut harness).await?;
    register(&mut harness, "new-worker", 2).await?;
    report_instance(&mut harness, "new-worker", &workspace).await?;
    Ok((harness, workspace))
}

/// Every new Definition rule is enforced at create: unmanaged paths, nested
/// layouts under lazy, a layout under `.horde`, and a reclaim argv that
/// would run a relative program.
#[tokio::test]
async fn definition_rules_are_refused_at_create() {
    let mut harness = connect().await.expect("a harness");
    let refused = |request| async {
        let mut harness = connect().await.expect("a harness");
        define(&mut harness, request)
            .await
            .expect_err("refused")
            .code()
    };
    let base = || CreateWorkspaceDefinitionRequest {
        name: "x".to_owned(),
        repos: vec![repo("atlas", "")],
        ..CreateWorkspaceDefinitionRequest::default()
    };
    assert_eq!(
        refused(CreateWorkspaceDefinitionRequest {
            unmanaged_paths: vec!["atlas/data".to_owned()],
            ..base()
        })
        .await,
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        refused(CreateWorkspaceDefinitionRequest {
            repos: vec![repo("outer", "code"), repo("inner", "code/inner")],
            checkout_mode: CheckoutMode::Lazy.into(),
            ..base()
        })
        .await,
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        refused(CreateWorkspaceDefinitionRequest {
            repos: vec![repo("atlas", ".horde/atlas")],
            ..base()
        })
        .await,
        tonic::Code::InvalidArgument
    );
    let mut relative = repo("atlas", "");
    relative.reclaim = Some(horde_proto::v1::ReclaimPolicy {
        policy: Some(horde_proto::v1::reclaim_policy::Policy::Command(
            horde_proto::v1::reclaim_policy::Command {
                argv: vec!["./clean.sh".to_owned()],
                ..horde_proto::v1::reclaim_policy::Command::default()
            },
        )),
    });
    assert_eq!(
        refused(CreateWorkspaceDefinitionRequest {
            repos: vec![relative],
            ..base()
        })
        .await,
        tonic::Code::InvalidArgument
    );
    // The same nested layout is fine when the workspace checks out everything.
    define(
        &mut harness,
        CreateWorkspaceDefinitionRequest {
            repos: vec![repo("outer", "code"), repo("inner", "code/inner")],
            ..base()
        },
    )
    .await
    .expect("nested layouts are fine under ALL");
}

async fn spawn(
    harness: &mut Harness,
    session: &str,
    spec: AgentSpec,
) -> Result<horde_proto::v1::SpawnAgentResponse, tonic::Status> {
    Ok(harness
        .control
        .spawn_agent(SpawnAgentRequest {
            session_id: Some(SessionId::new(session)),
            parent_agent_id: None,
            spec: Some(spec),
            idempotency_key: String::new(),
        })
        .await?
        .into_inner())
}

/// A branch is held by one live agent per workspace: a second spawn naming
/// it is refused before placement with the holder named, while another
/// workspace's agent may use the same name.
#[tokio::test]
async fn a_held_branch_is_refused_with_its_holder_named() {
    let mut harness = connect().await.expect("a harness");
    let workspace = plain_workspace(&mut harness).await.expect("a workspace");
    let other = define(
        &mut harness,
        CreateWorkspaceDefinitionRequest {
            name: "other".to_owned(),
            repos: vec![repo("atlas", "")],
            ..CreateWorkspaceDefinitionRequest::default()
        },
    )
    .await
    .expect("another workspace");
    let (session, _root) = seed(&mut harness, "claims", "k-claims")
        .await
        .expect("a session");
    let on = |workspace: &str| AgentSpec {
        workspace_definition_id: Some(WorkspaceDefinitionId::new(workspace)),
        branch: "feat/login".to_owned(),
        ..AgentSpec::default()
    };
    let first = spawn(&mut harness, &session, on(&workspace))
        .await
        .expect("the first holds it");
    let holder = first.agent_id.map(|id| id.to_string()).unwrap_or_default();

    let refused = spawn(&mut harness, &session, on(&workspace))
        .await
        .expect_err("held");
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(refused.message().contains(&holder), "{}", refused.message());
    spawn(&mut harness, &session, on(&other))
        .await
        .expect("another workspace's branch is its own");

    let bad = AgentSpec {
        branch: "horde/someone-else".to_owned(),
        ..on(&workspace)
    };
    let refused = spawn(&mut harness, &session, bad)
        .await
        .expect_err("another agent's default name");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    let ghost = AgentSpec {
        workspace_definition_id: Some(WorkspaceDefinitionId::new(&workspace)),
        checkout_repos: vec!["ghost".to_owned()],
        ..AgentSpec::default()
    };
    let refused = spawn(&mut harness, &session, ghost)
        .await
        .expect_err("an unknown repo");
    assert!(
        refused.message().contains("valid repos: atlas, proto"),
        "{}",
        refused.message()
    );
}

/// A spawn naming a branch is placed only on a protocol-2 worker; an older
/// one is refused with the reason recorded.
#[tokio::test]
async fn new_checkout_fields_wait_for_a_protocol_two_worker() {
    let mut harness = connect().await.expect("a harness");
    let workspace = plain_workspace(&mut harness).await.expect("a workspace");
    register(&mut harness, "old-worker", 0)
        .await
        .expect("registers");
    let (session, _root) = seed(&mut harness, "fence", "k-fence")
        .await
        .expect("a session");
    let spawned = spawn(
        &mut harness,
        &session,
        AgentSpec {
            workspace_definition_id: Some(WorkspaceDefinitionId::new(&workspace)),
            branch: "feat/x".to_owned(),
            ..AgentSpec::default()
        },
    )
    .await
    .expect("queued, not refused");
    let placement = spawned.placement.expect("a placement");
    assert_eq!(
        placement.state,
        i32::from(horde_proto::v1::PlacementState::Queued)
    );
    assert!(
        placement.reason.contains("workspace protocol 0"),
        "{}",
        placement.reason
    );
}

/// gc resolves its targets from the Instance cache and the registry, splits
/// them, appends one request, and the targeted worker's command stream
/// carries the matching command; the listing serves the reported checkouts.
#[tokio::test]
async fn gc_targets_holders_and_commands_them() {
    let (mut harness, workspace) = held_workspace().await.expect("a held workspace");
    register(&mut harness, "old-worker", 0)
        .await
        .expect("registers");
    report_instance(&mut harness, "old-worker", &workspace)
        .await
        .expect("reports");

    let listed = harness
        .control
        .list_workspace_instances(ListWorkspaceInstancesRequest {
            worker_id: Some(WorkerId::new("new-worker")),
            workspace_definition_id: None,
        })
        .await
        .expect("lists")
        .into_inner()
        .instances;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].checkouts[0].branch, "horde/agent-9");

    let answer = harness
        .control
        .collect_workspace_garbage(CollectWorkspaceGarbageRequest {
            workspace_definition_id: Some(WorkspaceDefinitionId::new(&workspace)),
            worker_ids: vec![
                WorkerId::new("new-worker"),
                WorkerId::new("old-worker"),
                WorkerId::new("away"),
            ],
            level: GcLevel::Hard.into(),
            dry_run: true,
            ..CollectWorkspaceGarbageRequest::default()
        })
        .await
        .expect("accepted")
        .into_inner();
    let names = |ids: &[WorkerId]| ids.iter().map(ToString::to_string).collect::<Vec<_>>();
    assert_eq!(names(&answer.targeted), vec!["new-worker"]);
    assert_eq!(names(&answer.unsupported), vec!["old-worker"]);
    assert_eq!(names(&answer.offline), vec!["away"]);
    assert!(answer.request_id.starts_with("gc-") && answer.sequence > 0);

    let mut commands = harness
        .worker
        .stream_commands(StreamCommandsRequest {
            worker_id: Some(WorkerId::new("new-worker")),
            resume_from_command_sequence: 1,
            resume_after_log_position: 0,
        })
        .await
        .expect("the stream opens")
        .into_inner();
    let first = tokio::time::timeout(std::time::Duration::from_secs(10), commands.message())
        .await
        .expect("a command in time")
        .expect("the stream")
        .expect("a command");
    let Some(Command::WorkspaceGc(gc)) = first.command else {
        panic!("a gc command, got {first:?}");
    };
    assert_eq!(gc.request_id, answer.request_id);
    assert!(gc.dry_run);
    assert!(gc.policy.is_some(), "the resolved policy rides it");

    let refused = harness
        .control
        .collect_workspace_garbage(CollectWorkspaceGarbageRequest {
            force: true,
            level: GcLevel::Soft.into(),
            ..CollectWorkspaceGarbageRequest::default()
        })
        .await
        .expect_err("force needs hard");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
}

/// `delete --purge` removes the Definition and asks every holder to remove
/// its Instance; `--purge --dry-run` keeps the Definition; `--force`
/// without `--purge` is refused.
#[tokio::test]
async fn delete_purge_requests_instance_removal_and_a_dry_run_keeps_the_definition() {
    let (mut harness, workspace) = held_workspace().await.expect("a held workspace");
    let current = |harness: &mut Harness| {
        let mut control = harness.control.clone();
        let workspace = workspace.clone();
        async move {
            control
                .get_workspace_definition(horde_proto::v1::GetWorkspaceDefinitionRequest {
                    workspace_definition_id: Some(WorkspaceDefinitionId::new(workspace)),
                    name: String::new(),
                })
                .await
                .map(|r| {
                    r.into_inner()
                        .definition
                        .map(|d| d.etag)
                        .unwrap_or_default()
                })
        }
    };
    let delete =
        |etag: String, purge: bool, force: bool, dry_run: bool| DeleteWorkspaceDefinitionRequest {
            workspace_definition_id: Some(WorkspaceDefinitionId::new(&workspace)),
            etag,
            purge,
            force,
            dry_run,
        };

    let tag = current(&mut harness).await.expect("exists");
    let refused = harness
        .control
        .delete_workspace_definition(delete(tag.clone(), false, true, false))
        .await
        .expect_err("--force needs --purge");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);

    let dry = harness
        .control
        .delete_workspace_definition(delete(tag.clone(), true, false, true))
        .await
        .expect("a dry run")
        .into_inner();
    assert!(!dry.purge_request_id.is_empty());
    assert!(
        current(&mut harness).await.is_ok(),
        "the dry run kept the definition"
    );

    let purged = harness
        .control
        .delete_workspace_definition(delete(tag, true, false, false))
        .await
        .expect("purged")
        .into_inner();
    assert!(!purged.purge_request_id.is_empty() && purged.purge_sequence > dry.purge_sequence);
    assert_eq!(
        current(&mut harness).await.expect_err("deleted").code(),
        tonic::Code::NotFound
    );
}

/// Instance removal names its workspace and worker; an older worker is
/// refused outright, an online one is targeted, and an offline one gets the
/// command waiting in its stream.
#[tokio::test]
async fn instance_removal_targets_one_worker_or_says_why_not() {
    let (mut harness, workspace) = held_workspace().await.expect("a held workspace");
    register(&mut harness, "old-worker", 0)
        .await
        .expect("registers");
    let remove = |worker: Option<&str>, workspace: Option<&str>| {
        horde_proto::v1::RemoveWorkspaceInstanceRequest {
            workspace_definition_id: workspace.map(WorkspaceDefinitionId::new),
            worker_id: worker.map(WorkerId::new),
            force: false,
            dry_run: true,
        }
    };
    let online = harness
        .control
        .remove_workspace_instance(remove(Some("new-worker"), Some(&workspace)))
        .await
        .expect("accepted")
        .into_inner();
    assert!(online.targeted && online.sequence > 0 && online.request_id.starts_with("gc-"));

    let offline = harness
        .control
        .remove_workspace_instance(remove(Some("away"), Some(&workspace)))
        .await
        .expect("accepted")
        .into_inner();
    assert!(!offline.targeted, "the command waits for it");

    let old = harness
        .control
        .remove_workspace_instance(remove(Some("old-worker"), Some(&workspace)))
        .await
        .expect_err("too old");
    assert_eq!(old.code(), tonic::Code::FailedPrecondition);
    for (worker, workspace) in [(None, Some(workspace.as_str())), (Some("new-worker"), None)] {
        let refused = harness
            .control
            .remove_workspace_instance(remove(worker, workspace))
            .await
            .expect_err("both are required");
        assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    }
}
