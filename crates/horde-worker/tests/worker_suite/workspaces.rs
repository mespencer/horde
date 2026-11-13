//! Workspace materialization and checkout policies against real git repos.
//!
//! Everything here runs the production [`WorkspaceManager`] against origins
//! created with the real `git` binary in temp directories — the "relay
//! against real git repos" bar from TESTING.md §2, applied to the worker's
//! half of workspaces. No git behavior is simulated: a worktree is proven to
//! be a worktree by the `.git` *file* git leaves there, a clone by the `.git`
//! *directory*.

use horde_proto::v1::{
    CheckoutPolicy, WorkspaceDefinition, WorkspaceDefinitionId, WorkspaceInstanceState,
    WorkspaceRepo,
};
use horde_worker::WorkspaceManager;

#[cfg(test)]
pub(crate) mod harness {
    use std::path::{Path, PathBuf};

    use super::{WorkspaceDefinition, WorkspaceDefinitionId, WorkspaceRepo};

    /// Runs one git command in `cwd`, panicking with git's own words on
    /// failure — a fixture builder, not a reimplementation of the code under
    /// test, which shells out on its own.
    ///
    /// Auto-maintenance is pinned off: otherwise every fixture commit leaves
    /// a detached `git maintenance run --auto` working in that repo's
    /// `objects/` after `git commit` has returned, racing whatever the test
    /// does next. Production must survive that race (see
    /// `a_source_under_maintenance_still_materializes`); the fixture should
    /// not depend on winning it.
    pub(crate) fn git(cwd: &Path, arguments: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(["-c", "maintenance.auto=false", "-c", "gc.auto=0"])
            .args(arguments)
            .current_dir(cwd)
            .output()
            .expect("git runs");
        assert!(
            output.status.success(),
            "git {arguments:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    /// A real origin repo with one commit on `main`, returning its path.
    pub(crate) fn origin(root: &Path, name: &str) -> PathBuf {
        let origin = root.join(format!("{name}-origin"));
        std::fs::create_dir_all(&origin).expect("the origin dir");
        git(&origin, &["init", "-q", "-b", "main"]);
        git(&origin, &["config", "user.email", "horde@test"]);
        git(&origin, &["config", "user.name", "horde"]);
        std::fs::write(origin.join("README.md"), format!("# {name}\n")).expect("a seed file");
        git(&origin, &["add", "-A"]);
        git(&origin, &["commit", "-qm", "seed"]);
        origin
    }

    pub(crate) fn repo(name: &str, remote: &Path) -> WorkspaceRepo {
        WorkspaceRepo {
            name: name.to_owned(),
            remote_url: remote.display().to_string(),
            default_branch: "main".to_owned(),
            layout_path: String::new(),
            ..horde_proto::v1::WorkspaceRepo::default()
        }
    }

    pub(crate) fn definition(id: &str, repos: Vec<WorkspaceRepo>) -> WorkspaceDefinition {
        WorkspaceDefinition {
            workspace_definition_id: Some(WorkspaceDefinitionId::new(id)),
            name: id.to_owned(),
            repos,
            unmanaged_paths: Vec::new(),
            created_at: None,
            etag: String::new(),
            ..WorkspaceDefinition::default()
        }
    }

    /// One agent's checkout, as a spawn with no ADR-0014 fields asks for it:
    /// the root the agent works in.
    pub(crate) async fn checkout(
        manager: &super::WorkspaceManager,
        definition_id: &str,
        agent_id: &str,
        policy: horde_proto::v1::CheckoutPolicy,
    ) -> Result<PathBuf, horde_worker::WorkerError> {
        checkout_request(manager, &request(definition_id, agent_id, policy)).await
    }

    /// One agent's checkout starting each named repo at a parent's commit.
    pub(crate) async fn checkout_from(
        manager: &super::WorkspaceManager,
        definition_id: &str,
        agent_id: &str,
        policy: horde_proto::v1::CheckoutPolicy,
        parent_heads: &std::collections::BTreeMap<String, String>,
    ) -> Result<PathBuf, horde_worker::WorkerError> {
        let mut request = request(definition_id, agent_id, policy);
        request.bases = parent_heads
            .iter()
            .map(|(repo, sha)| horde_proto::v1::RepoBase {
                repo_name: repo.clone(),
                r#ref: sha.clone(),
                source: horde_proto::v1::RepoBaseSource::Parent.into(),
            })
            .collect();
        checkout_request(manager, &request).await
    }

    /// The plain request for one agent.
    pub(crate) fn request(
        definition_id: &str,
        agent_id: &str,
        policy: horde_proto::v1::CheckoutPolicy,
    ) -> horde_worker::workspace::CheckoutRequest {
        horde_worker::workspace::CheckoutRequest {
            definition_id: definition_id.to_owned(),
            agent_id: agent_id.to_owned(),
            session_id: "s1".to_owned(),
            policy,
            ..horde_worker::workspace::CheckoutRequest::default()
        }
    }

    /// A checkout as the request says, returning its root.
    pub(crate) async fn checkout_request(
        manager: &super::WorkspaceManager,
        request: &horde_worker::workspace::CheckoutRequest,
    ) -> Result<PathBuf, horde_worker::WorkerError> {
        manager
            .checkout_agent(request, &horde_worker::workspace::base::NoSource)
            .await
            .map(|prepared| prepared.root)
    }

    /// A manager whose root lives under this test's tempdir.
    pub(crate) fn manager_under(temp: &Path) -> super::WorkspaceManager {
        super::WorkspaceManager::new("worker-a", temp.join("workspaces"))
    }

    /// The `atlas`+`docs` definition most multi-repo tests want, with the
    /// origins it points at.
    pub(crate) fn two_repo_definition(temp: &Path) -> (WorkspaceDefinition, PathBuf, PathBuf) {
        let atlas = origin(temp, "atlas");
        let docs = origin(temp, "docs");
        let definition = definition("ws-1", vec![repo("atlas", &atlas), repo("docs", &docs)]);
        (definition, atlas, docs)
    }

    /// Commits one file in `repo` as "the parent's work" and returns the
    /// resulting HEAD — the arrangement both a8 tests inherit from.
    pub(crate) fn parents_commit(repo: &Path) -> String {
        std::fs::write(repo.join("finding.md"), "found\n").expect("writes");
        git(repo, &["add", "."]);
        git(repo, &["commit", "-qm", "the parent's work"]);
        git(repo, &["rev-parse", "HEAD"]).trim().to_owned()
    }

    /// Leaves `repo`'s object store as a concurrent `git maintenance` run
    /// can: holding an entry that is not an object. Here it is a dangling
    /// `maintenance.lock` symlink — the deterministic stand-in for a lock
    /// file that vanishes mid-copy, since a local-path clone walking
    /// `objects/` refuses it outright, while a clone through git's
    /// transport never lists the directory at all.
    pub(crate) fn plant_stray_lock(repo: &Path) {
        let objects = repo.join(".git").join("objects");
        std::os::unix::fs::symlink(objects.join("gone"), objects.join("maintenance.lock"))
            .expect("plants the stray lock");
    }

    /// One already-materialized single-repo instance: the arrangement every
    /// checkout-policy test starts from.
    pub(crate) async fn materialized_atlas(
        temp: &Path,
    ) -> (
        super::WorkspaceManager,
        WorkspaceDefinition,
        horde_proto::v1::WorkspaceInstanceReport,
    ) {
        let atlas = origin(temp, "atlas");
        let manager = manager_under(temp);
        let definition = definition("ws-1", vec![repo("atlas", &atlas)]);
        let report = manager.materialize(&definition).await;
        assert_eq!(
            report.state,
            super::WorkspaceInstanceState::Ready as i32,
            "setup: {}",
            report.detail
        );
        (manager, definition, report)
    }
}

/// The Runtime's half of "children start from the parent's committed HEAD"
/// (fix a8, ADR-0014): the parent's endpoint names that commit as the
/// child's `PARENT` base, and a spawn carrying it starts the child's
/// checkout there — through the whole apply path, with real git underneath.
/// A base the worker ignored would leave the child at the fresh default,
/// and the head comparison here is exactly what notices.
#[tokio::test]
async fn a_spawned_child_starts_at_its_parents_committed_head() {
    use horde_worker::ApplyCommand as _;

    let temp = tempfile::tempdir().expect("a tempdir");
    let atlas = harness::origin(temp.path(), "atlas");
    let definition = harness::definition("ws-1", vec![harness::repo("atlas", &atlas)]);

    let journal = horde_worker::Journal::in_memory().expect("an in-memory journal");
    let mut runtime = horde_worker::Runtime::new(journal)
        .with_adapter(std::sync::Arc::new(horde_adapter_fake::FakeAdapter::new(
            horde_adapter_fake::Script::echoing(),
        )))
        .with_workspaces(harness::manager_under(temp.path()));

    let command = |sequence: u64, kind| horde_proto::v1::StreamCommandsResponse {
        command_sequence: sequence,
        command_id: format!("a8:{sequence}"),
        log_position: 0,
        command: Some(kind),
    };
    let spawn = |agent: &str, parent: Option<&str>| {
        horde_proto::v1::stream_commands_response::Command::SpawnAgent(
            horde_proto::v1::SpawnAgentCommand {
                session_id: Some(horde_proto::v1::SessionId::new("s1")),
                agent_id: Some(horde_proto::v1::AgentId::new(agent)),
                parent_agent_id: parent.map(horde_proto::v1::AgentId::new),
                spec: Some(horde_proto::v1::AgentSpec {
                    workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(
                        "ws-1",
                    )),
                    checkout_policy: CheckoutPolicy::Worktree as i32,
                    ..horde_proto::v1::AgentSpec::default()
                }),
                ..horde_proto::v1::SpawnAgentCommand::default()
            },
        )
    };

    runtime
        .apply(command(
            1,
            horde_proto::v1::stream_commands_response::Command::MaterializeWorkspace(
                horde_proto::v1::MaterializeWorkspaceCommand {
                    definition: Some(definition),
                    session_id: Some(horde_proto::v1::SessionId::new("s1")),
                    ..horde_proto::v1::MaterializeWorkspaceCommand::default()
                },
            ),
        ))
        .await
        .expect("the materialize applies");
    runtime
        .apply(command(2, spawn("parent-1", None)))
        .await
        .expect("the parent spawns");
    // Accepted is not run: the spawn completes on its own lane.
    crate::support::within("the parent's spawn", runtime.settled()).await;

    // The parent's work: a commit in its worktree, past the instance HEAD.
    let checkout_of = |agent: &str| {
        temp.path()
            .join("workspaces")
            .join("ws-1")
            .join(".horde-agents")
            .join(agent)
            .join("atlas")
    };
    let parent_head = harness::parents_commit(&checkout_of("parent-1"));

    let mut child = spawn("child-1", Some("parent-1"));
    if let horde_proto::v1::stream_commands_response::Command::SpawnAgent(spawn) = &mut child
        && let Some(spec) = spawn.spec.as_mut()
    {
        spec.repo_bases = vec![horde_proto::v1::RepoBase {
            repo_name: "atlas".to_owned(),
            r#ref: parent_head.clone(),
            source: horde_proto::v1::RepoBaseSource::Parent.into(),
        }];
    }
    runtime
        .apply(command(3, child))
        .await
        .expect("the child spawns");
    crate::support::within("the child's spawn", runtime.settled()).await;
    let child_head = harness::git(&checkout_of("child-1"), &["rev-parse", "HEAD"])
        .trim()
        .to_owned();
    assert_eq!(
        child_head, parent_head,
        "the child's checkout starts at the parent's committed HEAD"
    );

    runtime.shutdown().await;
}

/// "Children start from the parent's committed HEAD" (DESIGN §5, fix a8):
/// a checkout carrying a start point begins at that commit, not at the
/// instance's HEAD — for a worktree and for a fresh clone alike — and a
/// checkout carrying none keeps starting at the instance's.
#[tokio::test]
async fn a_start_point_makes_the_child_begin_at_the_parents_commit() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, _report) = harness::materialized_atlas(temp.path()).await;

    // The parent works in its own worktree and commits.
    let parent_root = harness::checkout(&manager, "ws-1", "parent-1", CheckoutPolicy::Worktree)
        .await
        .expect("the parent checks out");
    let parent_head = harness::parents_commit(&parent_root.join("atlas"));

    let inherit: std::collections::BTreeMap<String, String> =
        [("atlas".to_owned(), parent_head.clone())].into();

    for (child, policy) in [
        ("child-wt", CheckoutPolicy::Worktree),
        ("child-fc", CheckoutPolicy::FreshClone),
    ] {
        let child_root = harness::checkout_from(&manager, "ws-1", child, policy, &inherit)
            .await
            .expect("the child checks out");
        let child_head = harness::git(&child_root.join("atlas"), &["rev-parse", "HEAD"])
            .trim()
            .to_owned();
        assert_eq!(
            child_head, parent_head,
            "{child} must start at the parent's committed HEAD"
        );
    }

    // A parentless checkout keeps the old meaning: the instance's HEAD,
    // which does not carry the parent's commit.
    let orphan_root = harness::checkout(&manager, "ws-1", "orphan-1", CheckoutPolicy::Worktree)
        .await
        .expect("the orphan checks out");
    let orphan_head = harness::git(&orphan_root.join("atlas"), &["rev-parse", "HEAD"])
        .trim()
        .to_owned();
    assert_ne!(
        orphan_head, parent_head,
        "no start point means the instance's HEAD, not somebody's worktree"
    );
}

#[tokio::test]
async fn materialize_clones_every_repo_and_reports_ready() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (definition, atlas, _docs) = harness::two_repo_definition(temp.path());
    let manager = harness::manager_under(temp.path());

    let report = manager.materialize(&definition).await;

    assert_eq!(
        report.state,
        WorkspaceInstanceState::Ready as i32,
        "detail: {}",
        report.detail
    );
    assert_eq!(
        report
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("worker-a")
    );
    assert_eq!(report.repos.len(), 2, "one repo report per repo");

    let atlas_head = harness::git(&atlas, &["rev-parse", "HEAD"]);
    let cloned_atlas = &report.repos[0];
    assert_eq!(cloned_atlas.repo_name, "atlas");
    assert_eq!(
        cloned_atlas.head_commit_sha,
        atlas_head.trim(),
        "the clone is at the origin's head"
    );
    assert!(!cloned_atlas.dirty, "a fresh clone is clean");
    assert!(
        std::path::Path::new(&cloned_atlas.path)
            .join(".git")
            .is_dir(),
        "each repo is a real checkout under the instance root"
    );
    assert!(
        std::path::Path::new(&report.root_path)
            .join(horde_worker::workspace::STATE_FILE)
            .is_file(),
        "the worker-local state file is the truth an advertisement reads"
    );
}

/// "Create or repair": a second materialize command finds the checkout and
/// fetches rather than failing on a non-empty directory.
#[tokio::test]
async fn materializing_again_repairs_rather_than_erroring() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, definition, _first) = harness::materialized_atlas(temp.path()).await;

    let second = manager.materialize(&definition).await;
    assert_eq!(
        second.state,
        WorkspaceInstanceState::Ready as i32,
        "repair must not fail on an existing checkout: {}",
        second.detail
    );
    assert_eq!(second.repos.len(), 1);
}

/// A repo that cannot clone degrades the report and names itself; the repos
/// that could still materialize.
#[tokio::test]
async fn a_failing_repo_degrades_the_report_and_names_itself() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let atlas = harness::origin(temp.path(), "atlas");
    let missing = temp.path().join("no-such-origin");
    let manager = harness::manager_under(temp.path());

    let report = manager
        .materialize(&harness::definition(
            "ws-1",
            vec![
                harness::repo("atlas", &atlas),
                harness::repo("ghost", &missing),
            ],
        ))
        .await;

    assert_eq!(
        report.state,
        WorkspaceInstanceState::Degraded as i32,
        "a failed repo is a degraded instance, not a dead worker"
    );
    assert!(
        report.detail.contains("ghost"),
        "the failing repo is named: {}",
        report.detail
    );
    assert_eq!(
        report.repos.len(),
        1,
        "the repo that could materialize still did"
    );
}

#[tokio::test]
async fn a_worktree_checkout_composes_an_agent_private_root() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (definition, _atlas, _docs) = harness::two_repo_definition(temp.path());
    let manager = harness::manager_under(temp.path());
    let instance = manager.materialize(&definition).await;
    assert_eq!(instance.state, WorkspaceInstanceState::Ready as i32);

    // Unspecified is the worktree default (DESIGN §7), so this exercises both.
    let checkout = harness::checkout(&manager, "ws-1", "agent-1", CheckoutPolicy::Unspecified)
        .await
        .expect("a worktree checkout");

    assert_ne!(
        checkout.display().to_string(),
        instance.root_path,
        "the agent works in a private root, not the shared instance"
    );
    for repo in ["atlas", "docs"] {
        let marker = checkout.join(repo).join(".git");
        assert!(
            marker.is_file(),
            "a git worktree leaves a .git *file*; {repo} has {marker:?}"
        );
    }

    // A second agent composes beside the first, not inside it.
    let second = harness::checkout(&manager, "ws-1", "agent-2", CheckoutPolicy::Worktree)
        .await
        .expect("a second checkout");
    assert_ne!(second, checkout, "agents must not share a worktree");

    // A redelivered spawn reuses the checkout instead of stacking another.
    let redelivered = harness::checkout(&manager, "ws-1", "agent-1", CheckoutPolicy::Worktree)
        .await
        .expect("a redelivered checkout");
    assert_eq!(redelivered, checkout);
}

/// The cleanup pass an explicit, non-dry-run removal runs under.
fn explicit_pass(
    policy: &horde_worker::workspace::policy::WorkspacePolicy,
) -> horde_worker::workspace::cleanup::Pass<'_> {
    horde_worker::workspace::cleanup::Pass {
        trigger: horde_proto::v1::CleanupTrigger::Explicit,
        gc_request_id: "gc-test",
        dry_run: false,
        policy,
    }
}

/// One agent's ledger record, archived.
async fn archived(
    manager: &WorkspaceManager,
    agent: &str,
) -> horde_worker::workspace::ledger::LedgerRecord {
    manager
        .mark_archived("ws-1", agent)
        .await
        .expect("the agent has a ledgered checkout")
}

/// A lossless hard removal gives the checkout back: the worktree is
/// unregistered, the branch the Worker created deleted, the directory and
/// ledger record gone — and the agent's last commit stays reachable under
/// `refs/horde/archived/<agent>`. A sibling's checkout is untouched.
#[tokio::test]
async fn a_lossless_hard_removal_removes_the_worktree_and_pins_its_head() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, instance) = harness::materialized_atlas(temp.path()).await;
    let checkout = harness::checkout(&manager, "ws-1", "agent-1", CheckoutPolicy::Worktree)
        .await
        .expect("a worktree checkout");
    let sibling = harness::checkout(&manager, "ws-1", "agent-2", CheckoutPolicy::Worktree)
        .await
        .expect("a sibling checkout");
    let work = harness::parents_commit(&checkout.join("atlas"));
    let instance_repo = std::path::Path::new(&instance.root_path).join("atlas");
    let policy = horde_worker::workspace::policy::WorkspacePolicy::default();

    let record = archived(&manager, "agent-1").await;
    let (event, removed) = manager
        .hard_remove("ws-1", &record, explicit_pass(&policy), false)
        .await;
    assert!(removed, "{event:?}");
    assert_eq!(event.repos[0].pinned_commit_sha, work);

    assert!(!checkout.exists(), "the agent's directory is gone");
    let worktrees = harness::git(&instance_repo, &["worktree", "list"]);
    assert!(
        !worktrees.contains("agent-1") && worktrees.contains("agent-2"),
        "only the archived agent's worktree is unregistered: {worktrees}"
    );
    let branches = harness::git(&instance_repo, &["branch", "--list", "horde/*"]);
    assert!(
        !branches.contains("horde/agent-1") && branches.contains("horde/agent-2"),
        "only the archived agent's branch is deleted: {branches}"
    );
    assert_eq!(
        harness::git(
            &instance_repo,
            &["rev-parse", "refs/horde/archived/agent-1"]
        )
        .trim(),
        work,
        "the agent's last commit stays reachable"
    );
    assert!(
        sibling.join("atlas").join(".git").is_file(),
        "the sibling is untouched"
    );
    assert!(
        manager
            .records("ws-1")
            .await
            .iter()
            .all(|r| r.agent_id != "agent-1"),
        "the ledger record goes last"
    );
}

/// A fresh clone holding only its HEAD's history is removed the same way,
/// its HEAD pinned in the Instance repo (the clone was the only place it
/// lived); the shared policy's root has no ledger and is never removed.
#[tokio::test]
async fn a_fresh_clone_is_pinned_then_removed_and_shared_roots_are_left_alone() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, instance) = harness::materialized_atlas(temp.path()).await;
    let clone = harness::checkout(&manager, "ws-1", "agent-1", CheckoutPolicy::FreshClone)
        .await
        .expect("a fresh clone");
    let work = harness::parents_commit(&clone.join("atlas"));
    let policy = horde_worker::workspace::policy::WorkspacePolicy::default();

    let record = archived(&manager, "agent-1").await;
    let (event, removed) = manager
        .hard_remove("ws-1", &record, explicit_pass(&policy), false)
        .await;
    assert!(removed, "{event:?}");
    assert!(!clone.exists());
    let instance_repo = std::path::Path::new(&instance.root_path).join("atlas");
    assert_eq!(
        harness::git(
            &instance_repo,
            &["rev-parse", "refs/horde/archived/agent-1"]
        )
        .trim(),
        work
    );

    let shared = harness::checkout(&manager, "ws-1", "agent-2", CheckoutPolicy::Shared)
        .await
        .expect("the shared checkout");
    assert!(
        manager.mark_archived("ws-1", "agent-2").await.is_none(),
        "no ledger, nothing to remove"
    );
    assert!(
        shared.join("atlas").join(".git").is_dir(),
        "the instance survives"
    );
}

/// A dirty checkout is retained, its reason recorded in the ledger; only a
/// forced removal takes it, and even then its committed HEAD is pinned.
#[tokio::test]
async fn a_dirty_checkout_is_retained_unless_forced() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, instance) = harness::materialized_atlas(temp.path()).await;
    let checkout = harness::checkout(&manager, "ws-1", "agent-1", CheckoutPolicy::Worktree)
        .await
        .expect("a worktree checkout");
    std::fs::write(checkout.join("atlas").join("scratch.txt"), "wip").expect("writes");
    let policy = horde_worker::workspace::policy::WorkspacePolicy::default();

    let record = archived(&manager, "agent-1").await;
    let (event, removed) = manager
        .hard_remove("ws-1", &record, explicit_pass(&policy), false)
        .await;
    assert!(!removed);
    assert_eq!(
        event.repos[0].outcome,
        i32::from(horde_proto::v1::workspace_cleanup::repo_action::Outcome::Refused)
    );
    assert_eq!(event.repos[0].reason, "untracked files: 1");
    assert!(
        checkout.join("atlas").join("scratch.txt").exists(),
        "nothing was lost"
    );
    let retained = manager.records("ws-1").await;
    assert_eq!(retained[0].retained_reason, "untracked files: 1");

    let head = harness::git(&checkout.join("atlas"), &["rev-parse", "HEAD"])
        .trim()
        .to_owned();
    let (event, removed) = manager
        .hard_remove("ws-1", &retained[0], explicit_pass(&policy), true)
        .await;
    assert!(removed, "{event:?}");
    assert!(event.repos[0].reason.starts_with("forced:"), "{event:?}");
    let instance_repo = std::path::Path::new(&instance.root_path).join("atlas");
    assert_eq!(
        harness::git(
            &instance_repo,
            &["rev-parse", "refs/horde/archived/agent-1"]
        )
        .trim(),
        head
    );
}

/// A remote URL or branch shaped like an option is refused before git ever
/// sees it — `--upload-pack=` on a clone runs a command — and a layout that
/// would escape the instance degrades the report instead of cloning there.
#[tokio::test]
async fn option_shaped_clone_inputs_and_escaping_layouts_are_refused() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let atlas = harness::origin(temp.path(), "atlas");
    let manager = harness::manager_under(temp.path());
    let marker = temp.path().join("pwned");

    let hostile_url = WorkspaceRepo {
        remote_url: format!("--upload-pack=touch {}", marker.display()),
        ..harness::repo("url", &atlas)
    };
    let hostile_branch = WorkspaceRepo {
        default_branch: "--upload-pack=true".to_owned(),
        ..harness::repo("branch", &atlas)
    };
    let escaping = WorkspaceRepo {
        layout_path: "../../escaped".to_owned(),
        ..harness::repo("escape", &atlas)
    };
    let report = manager
        .materialize(&harness::definition(
            "ws-1",
            vec![
                hostile_url,
                hostile_branch,
                escaping,
                harness::repo("atlas", &atlas),
            ],
        ))
        .await;

    assert_eq!(report.state, WorkspaceInstanceState::Degraded as i32);
    assert!(
        report.detail.contains("repo url") && report.detail.contains("git option"),
        "{}",
        report.detail
    );
    assert!(report.detail.contains("repo branch"), "{}", report.detail);
    assert!(report.detail.contains("repo escape"), "{}", report.detail);
    assert!(!marker.exists(), "no injected command ran");
    assert!(
        !temp.path().join("escaped").exists(),
        "nothing landed outside"
    );
    assert_eq!(report.repos.len(), 1, "the honest repo still materialized");
}

#[tokio::test]
async fn a_fresh_clone_checkout_is_a_full_clone() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, _instance) = harness::materialized_atlas(temp.path()).await;

    let checkout = harness::checkout(&manager, "ws-1", "agent-1", CheckoutPolicy::FreshClone)
        .await
        .expect("a fresh clone");

    assert!(
        checkout.join("atlas").join(".git").is_dir(),
        "a fresh clone owns its whole repository, so .git is a directory"
    );
}

/// A remote that is a live local repo — its object store mid-maintenance —
/// still materializes: the clone goes through git's transport rather than
/// copying `objects/` entry by entry, so a lock file appearing or vanishing
/// there is none of its business.
#[tokio::test]
async fn a_source_under_maintenance_still_materializes() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let atlas = harness::origin(temp.path(), "atlas");
    harness::plant_stray_lock(&atlas);
    let manager = harness::manager_under(temp.path());

    let report = manager
        .materialize(&harness::definition(
            "ws-1",
            vec![harness::repo("atlas", &atlas)],
        ))
        .await;

    assert_eq!(
        report.state,
        WorkspaceInstanceState::Ready as i32,
        "a maintained source must not break the clone: {}",
        report.detail
    );
    assert_eq!(
        report.repos[0].head_commit_sha,
        harness::git(&atlas, &["rev-parse", "HEAD"]).trim(),
        "the clone is at the origin's head"
    );
}

/// The Instance's object store is the live one: every worktree agent commits
/// into it, each commit spawning auto-maintenance there. A fresh-clone
/// checkout taken meanwhile must not trip over it.
#[tokio::test]
async fn a_fresh_clone_survives_maintenance_in_the_instance() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, instance) = harness::materialized_atlas(temp.path()).await;
    let instance_atlas = std::path::PathBuf::from(&instance.repos[0].path);
    harness::plant_stray_lock(&instance_atlas);

    let checkout = harness::checkout(&manager, "ws-1", "agent-1", CheckoutPolicy::FreshClone)
        .await
        .expect("a fresh clone despite the maintained instance");

    assert_eq!(
        harness::git(&checkout.join("atlas"), &["rev-parse", "HEAD"]),
        harness::git(&instance_atlas, &["rev-parse", "HEAD"]),
        "the fresh clone starts at the instance's HEAD"
    );
}

#[tokio::test]
async fn a_shared_checkout_is_the_instance_itself() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, instance) = harness::materialized_atlas(temp.path()).await;

    let checkout = harness::checkout(&manager, "ws-1", "agent-1", CheckoutPolicy::Shared)
        .await
        .expect("the shared checkout");
    assert_eq!(checkout.display().to_string(), instance.root_path);
}

/// The definition's declared branch is what materializes — not whatever the
/// origin's HEAD happens to point at.
#[tokio::test]
async fn materialize_checks_out_the_declared_branch_not_the_origins_head() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let atlas = harness::origin(temp.path(), "atlas");
    harness::git(&atlas, &["checkout", "-q", "-b", "feature"]);
    std::fs::write(atlas.join("FEATURE.md"), "on the feature branch\n").expect("a change");
    harness::git(&atlas, &["add", "-A"]);
    harness::git(
        &atlas,
        &[
            "-c",
            "user.email=horde@test",
            "-c",
            "user.name=horde",
            "commit",
            "-qm",
            "feature",
        ],
    );
    let feature_sha = harness::git(&atlas, &["rev-parse", "HEAD"]);
    harness::git(&atlas, &["checkout", "-q", "main"]);

    let manager = harness::manager_under(temp.path());
    let mut repo = harness::repo("atlas", &atlas);
    repo.default_branch = "feature".to_owned();
    let report = manager
        .materialize(&harness::definition("ws-1", vec![repo]))
        .await;

    assert_eq!(
        report.state,
        WorkspaceInstanceState::Ready as i32,
        "detail: {}",
        report.detail
    );
    assert_eq!(
        report.repos[0].head_commit_sha,
        feature_sha.trim(),
        "the instance is on the declared branch, not the origin's HEAD"
    );
}

/// An agent id that is not a plain directory component is refused even when
/// the workspace itself is fine: a cleaned-up or trusted-through id is a path
/// escape wearing an agent's name.
#[tokio::test]
async fn a_checkout_for_a_path_hostile_agent_id_is_refused() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, _instance) = harness::materialized_atlas(temp.path()).await;

    let refused = harness::checkout(
        &manager,
        "ws-1",
        "agent/../escape",
        CheckoutPolicy::Worktree,
    )
    .await
    .expect_err("a separator in an agent id is an escape, not a name");
    assert!(
        refused.to_string().contains("not directory names"),
        "the refusal says why: {refused}"
    );
}

/// An agent cannot run in a checkout that does not exist: instances are never
/// self-initiated, so an unmaterialized workspace is a refusal, not a clone.
#[tokio::test]
async fn a_checkout_without_a_materialized_instance_is_refused() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let manager = harness::manager_under(temp.path());

    let refused = harness::checkout(&manager, "ws-none", "agent-1", CheckoutPolicy::Worktree)
        .await
        .expect_err("nothing was materialized");
    assert!(
        refused.to_string().contains("ws-none"),
        "the refusal names the workspace: {refused}"
    );
}

#[tokio::test]
async fn warm_instances_report_what_is_actually_on_disk() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let root = temp.path().join("workspaces");

    assert!(
        harness::manager_under(temp.path())
            .warm_instances()
            .await
            .is_empty(),
        "an empty root advertises nothing"
    );

    let (manager, _definition, _instance) = harness::materialized_atlas(temp.path()).await;
    let warm = manager.warm_instances().await;
    assert_eq!(warm.len(), 1);
    assert_eq!(warm[0].state, WorkspaceInstanceState::Ready as i32);
    assert_eq!(
        warm[0]
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("ws-1"),
        "warmth is per definition, or placement cannot colocate on it"
    );

    // An out-of-band deletion downgrades the advertisement: the server must
    // stop preferring an instance that is not really there.
    std::fs::remove_dir_all(root.join("ws-1").join("atlas")).expect("delete the repo");
    let degraded = manager.warm_instances().await;
    assert_eq!(degraded.len(), 1);
    assert_eq!(degraded[0].state, WorkspaceInstanceState::Degraded as i32);
    assert!(
        degraded[0].detail.contains("atlas"),
        "the missing repo is named: {}",
        degraded[0].detail
    );
}

/// The commit made in one agent's worktree is reachable from the instance
/// repo (worktrees share the object store), but never appears in another
/// agent's checkout — the isolation the policy promises.
#[tokio::test]
async fn worktree_agents_share_history_but_not_working_trees() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, _instance) = harness::materialized_atlas(temp.path()).await;

    let first = harness::checkout(&manager, "ws-1", "agent-1", CheckoutPolicy::Worktree)
        .await
        .expect("first checkout");
    let second = harness::checkout(&manager, "ws-1", "agent-2", CheckoutPolicy::Worktree)
        .await
        .expect("second checkout");

    let working = first.join("atlas");
    std::fs::write(working.join("CHANGE.md"), "agent-1 was here\n").expect("a change");
    harness::git(&working, &["add", "-A"]);
    harness::git(
        &working,
        &[
            "-c",
            "user.email=horde@test",
            "-c",
            "user.name=horde",
            "commit",
            "-qm",
            "agent-1 change",
        ],
    );

    assert!(
        !second.join("atlas").join("CHANGE.md").exists(),
        "one agent's work must not appear in another's tree"
    );

    // Commit-then-share: the commit is in the shared object store, reachable
    // by hash from the second agent's worktree.
    let sha = harness::git(&working, &["rev-parse", "HEAD"]);
    let seen = harness::git(&second.join("atlas"), &["cat-file", "-t", sha.trim()]);
    assert_eq!(seen.trim(), "commit", "worktrees share one object store");
}
