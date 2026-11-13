//! [`WorkspaceManager`] against real repositories: identity and disk facts,
//! lazy additions, adoption, half-built recovery, synthesized records, the
//! shared policy and branch-name validation.

use std::sync::Mutex;

use super::super::base::{FetchPolicy, NoSource};
use super::super::ledger;
use super::{
    Arc, BaseResolution, CheckoutMode, CheckoutPolicy, CheckoutRequest, FsStats, LedgerRecord,
    LedgerRepo, Path, PathBuf, PreparedRepo, RepoBase, RepoBaseSource, StatvfsStats,
    WorkspaceDefinition, WorkspaceManager, WorkspaceRepo,
};
use crate::workspace::test_git::git as run_git;

/// An origin whose one commit carries its own name, so every origin's
/// commit differs.
fn origin(dir: &Path, name: &str) -> PathBuf {
    let origin = dir.join(format!("{name}-origin"));
    std::fs::create_dir_all(&origin).expect("origin");
    run_git(&origin, &["init", "-q", "-b", "main"]);
    std::fs::write(origin.join("README.md"), name).expect("seed");
    run_git(&origin, &["add", "."]);
    run_git(&origin, &["commit", "-qm", name]);
    origin
}

fn definition(id: &str, dir: &Path, names: &[&str]) -> WorkspaceDefinition {
    WorkspaceDefinition {
        workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(id)),
        name: id.to_owned(),
        repos: names
            .iter()
            .map(|name| WorkspaceRepo {
                name: (*name).to_owned(),
                remote_url: origin(dir, &format!("{id}-{name}")).display().to_string(),
                default_branch: "main".to_owned(),
                ..WorkspaceRepo::default()
            })
            .collect(),
        ..WorkspaceDefinition::default()
    }
}

/// A manager over `<dir>/ws` with `ws-1` materialized from `names`.
async fn materialized(dir: &Path, names: &[&str]) -> (WorkspaceManager, WorkspaceDefinition) {
    let manager = WorkspaceManager::new("w-7", dir.join("ws"));
    let definition = definition("ws-1", dir, names);
    let report = manager.materialize(&definition).await;
    assert_eq!(report.detail, "");
    (manager, definition)
}

fn request(agent: &str) -> CheckoutRequest {
    CheckoutRequest {
        definition_id: "ws-1".to_owned(),
        agent_id: agent.to_owned(),
        policy: CheckoutPolicy::Worktree,
        ..CheckoutRequest::default()
    }
}

fn head(repo: &Path) -> String {
    run_git(repo, &["rev-parse", "HEAD"])
}

/// Free-space facts that are whatever the test says, recording where they
/// were asked about.
#[derive(Debug, Default)]
struct FixedStats {
    asked: Mutex<Vec<PathBuf>>,
}

impl FsStats for FixedStats {
    fn stats(&self, path: &Path) -> Option<(u64, u64)> {
        if let Ok(mut asked) = self.asked.lock() {
            asked.push(path.to_path_buf());
        }
        Some((500, 1000))
    }
}

#[test]
fn statvfs_reports_the_real_filesystem() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (free, total) = StatvfsStats
        .stats(dir.path())
        .expect("a real filesystem answers");
    assert!(total > 1, "{total}");
    assert!(free > 1, "{free}");
    assert!(free <= total, "{free} of {total}");
    assert_eq!(StatvfsStats.stats(&dir.path().join("absent")), None);
}

/// The manager reports as its Worker, has fetched nothing yet, and asks
/// the nearest existing ancestor of a root that is not there yet.
#[test]
fn a_new_manager_knows_its_worker_and_asks_the_nearest_existing_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stats = Arc::new(FixedStats::default());
    let manager = WorkspaceManager::new("w-7", dir.path().join("ws/deeper"))
        .with_fs_stats(Arc::clone(&stats) as Arc<dyn FsStats>);
    assert_eq!(manager.worker_id(), "w-7");
    assert_eq!(manager.fetches_run(), 0);
    assert_eq!(manager.filesystem(), Some((500, 1000)));
    let asked = stats.asked.lock().expect("the record").clone();
    assert_eq!(asked, vec![dir.path().to_path_buf()]);
}

/// A lazy agent adds a second repo next to its first: the ledger keeps
/// both, the new one starts fresh, and asking again reuses it. Commits
/// delivered beforehand are listed pending in the manifest.
#[tokio::test]
async fn a_lazy_agent_adds_repos_alongside_the_first() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas", "beta"]).await;
    let mut lazy = request("agent-1");
    lazy.session_id = "s-1".to_owned();
    lazy.checkout_repos = vec!["atlas".to_owned()];
    let first = manager
        .checkout_agent(&lazy, &NoSource)
        .await
        .expect("the lazy checkout");
    assert_eq!(manager.fetches_run(), 1, "atlas's origin, once");
    let atlas_base = first.prepared.repos[0].base_commit_sha.clone();
    let agents_dir = manager.agents_dir("ws-1");
    let started = ledger::read(&agents_dir, "agent-1")
        .await
        .expect("the ledger");
    assert!(started.created_at_ms > 0 && started.last_active_at_ms > 0);

    manager
        .note_pending(&first.root, "beta", "abc123")
        .await
        .expect("noted");
    let manifest = super::super::manifest::read(&first.root)
        .await
        .expect("the manifest");
    let beta = manifest
        .repos
        .iter()
        .find(|repo| repo.name == "beta")
        .expect("beta is listed");
    assert_eq!(beta.pending_commits, vec!["abc123".to_owned()]);

    let added = manager
        .add_repo(
            "ws-1",
            "agent-1",
            "beta",
            None,
            FetchPolicy::default(),
            &NoSource,
        )
        .await
        .expect("beta is added");
    assert_eq!(added.relative, PathBuf::from("beta"));
    assert_eq!(added.path, first.root.join("beta"));
    assert!(added.path.join(".git").exists());
    assert_eq!(added.branch, "horde/agent-1");
    assert_eq!(added.prepared.repo_name, "beta");
    assert_eq!(added.prepared.resolution, i32::from(BaseResolution::Fresh));
    let names: Vec<String> = ledger::read(&agents_dir, "agent-1")
        .await
        .expect("the ledger")
        .repos
        .into_iter()
        .map(|repo| repo.name)
        .collect();
    assert_eq!(names, ["atlas", "beta"]);
    assert_ne!(added.prepared.base_commit_sha, atlas_base);

    let again = manager
        .add_repo(
            "ws-1",
            "agent-1",
            "atlas",
            None,
            FetchPolicy::default(),
            &NoSource,
        )
        .await
        .expect("idempotent");
    assert_eq!(
        again.prepared,
        PreparedRepo {
            repo_name: "atlas".to_owned(),
            branch: "horde/agent-1".to_owned(),
            base_commit_sha: head(&first.root.join("atlas")),
            resolution: BaseResolution::Reused.into(),
            requested_ref: atlas_base,
            ..PreparedRepo::default()
        }
    );

    let refused = manager
        .add_repo(
            "ws-1",
            "../x",
            "beta",
            None,
            FetchPolicy::default(),
            &NoSource,
        )
        .await
        .expect_err("not a directory name")
        .to_string();
    assert!(refused.contains("not directory names"), "{refused}");

    let reports = manager.warm_instances().await;
    let checkout = &reports[0].checkouts[0];
    assert_eq!(
        checkout.session_id,
        Some(horde_proto::v1::SessionId::new("s-1"))
    );
}

/// The Instance repos are read from the state file, in Definition order.
#[tokio::test]
async fn instance_repos_are_named_and_located_from_the_state_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas", "beta"]).await;
    assert_eq!(
        manager.instance_repos("ws-1").await.expect("materialized"),
        vec![
            ("atlas".to_owned(), PathBuf::from("atlas")),
            ("beta".to_owned(), PathBuf::from("beta")),
        ]
    );
    assert_eq!(
        manager
            .instance_repo_path("ws-1", "beta")
            .await
            .expect("beta"),
        dir.path().join("ws/ws-1/beta")
    );
    assert!(manager.instance_repo_path("ws-1", "ghost").await.is_err());
}

/// A directory whose state file names another Definition is not that
/// Instance; the reports size unmanaged data and carry the disk facts.
#[tokio::test]
async fn only_a_directory_named_for_its_definition_is_an_instance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let manager = WorkspaceManager::new("w-7", dir.path().join("ws"))
        .with_fs_stats(Arc::new(FixedStats::default()));
    let mut first = definition("ws-1", dir.path(), &["atlas"]);
    first.unmanaged_paths = vec!["data".to_owned()];
    manager.materialize(&first).await;
    manager
        .materialize(&definition("ws-2", dir.path(), &["atlas"]))
        .await;
    std::fs::write(dir.path().join("ws/ws-1/data/weights.bin"), [0_u8; 64]).expect("data");
    let reports = manager.warm_instances().await;
    assert_eq!(reports[0].unmanaged_bytes, 64);
    assert_eq!(reports[0].filesystem_free_bytes, 500);
    assert_eq!(reports[0].filesystem_total_bytes, 1000);

    std::fs::rename(dir.path().join("ws/ws-1"), dir.path().join("ws/moved")).expect("moved");
    let ids: Vec<String> = manager
        .instances()
        .await
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(ids, ["ws-2"]);
}

/// A checkout on disk the ledger does not know is adopted as it is.
#[tokio::test]
async fn an_unledgered_checkout_is_adopted_at_its_head() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, definition) = materialized(dir.path(), &["atlas"]).await;
    let destination = manager.agents_dir("ws-1").join("agent-1/atlas");
    std::fs::create_dir_all(destination.parent().expect("a parent")).expect("agents dir");
    run_git(
        dir.path(),
        &[
            "clone",
            "-q",
            "--",
            &definition.repos[0].remote_url,
            &destination.display().to_string(),
        ],
    );
    let sha = head(&destination);
    let prepared = manager
        .checkout_agent(&request("agent-1"), &NoSource)
        .await
        .expect("adopted");
    assert_eq!(
        prepared.prepared.repos,
        vec![PreparedRepo {
            repo_name: "atlas".to_owned(),
            branch: "horde/agent-1".to_owned(),
            base_commit_sha: sha.clone(),
            resolution: BaseResolution::Reused.into(),
            ..PreparedRepo::default()
        }]
    );
    let record = ledger::read(&manager.agents_dir("ws-1"), "agent-1")
        .await
        .expect("the ledger");
    assert_eq!(
        record.repos,
        vec![LedgerRepo {
            name: "atlas".to_owned(),
            base_commit_sha: sha,
            resolution: "reused".to_owned(),
            created_branch: true,
            ..LedgerRepo::default()
        }]
    );
}

fn crashed_record(agent: &str) -> LedgerRecord {
    LedgerRecord {
        agent_id: agent.to_owned(),
        policy: ledger::WORKTREE.to_owned(),
        mode: "all".to_owned(),
        branch: format!("horde/{agent}"),
        repos: vec![LedgerRepo {
            name: "atlas".to_owned(),
            pending: true,
            ..LedgerRepo::default()
        }],
        ..LedgerRecord::default()
    }
}

/// A materialized `ws-1` (`atlas`) whose `agent-1` crashed with its repo
/// still pending in the ledger: the manager and the agents directory.
async fn crashed(dir: &Path) -> (WorkspaceManager, PathBuf) {
    let (manager, _) = materialized(dir, &["atlas"]).await;
    let agents_dir = manager.agents_dir("ws-1");
    ledger::write(&agents_dir, &crashed_record("agent-1"))
        .await
        .expect("the crashed ledger");
    (manager, agents_dir)
}

/// A crash before anything reached disk leaves nothing to clear: the
/// checkout is simply made.
#[tokio::test]
async fn a_pending_repo_with_nothing_on_disk_is_composed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, agents_dir) = crashed(dir.path()).await;
    manager
        .checkout_agent(&request("agent-1"), &NoSource)
        .await
        .expect("composed");
    assert!(agents_dir.join("agent-1/atlas/.git").is_file());
}

/// What cannot be cleared fails the checkout, naming itself.
#[tokio::test]
async fn a_half_built_path_that_cannot_be_cleared_fails_the_checkout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, agents_dir) = crashed(dir.path()).await;
    std::fs::create_dir_all(agents_dir.join("agent-1")).expect("the agent root");
    std::fs::write(agents_dir.join("agent-1/atlas"), "not a directory").expect("a file");
    let error = manager
        .checkout_agent(&request("agent-1"), &NoSource)
        .await
        .expect_err("a file is not cleared as a directory")
        .to_string();
    assert!(error.contains("could not clear"), "{error}");
}

/// An agent directory with no ledger is reported from what is on disk:
/// a worktree's `.git` file reads as the worktree policy, a clone's `.git`
/// directory as fresh-clone.
#[tokio::test]
async fn an_unledgered_agent_directory_is_synthesized_from_disk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas", "beta"]).await;
    let agents_dir = manager.agents_dir("ws-1");
    std::fs::create_dir_all(agents_dir.join("agent-8/atlas/.git")).expect("a clone shape");
    std::fs::create_dir_all(agents_dir.join("agent-9/atlas")).expect("a worktree shape");
    std::fs::write(agents_dir.join("agent-9/atlas/.git"), "gitdir: x").expect("a .git file");

    let records = manager.records("ws-1").await;
    assert_eq!(records.len(), 2);
    let expected_repos = vec![LedgerRepo {
        name: "atlas".to_owned(),
        created_branch: true,
        ..LedgerRepo::default()
    }];
    for (record, agent, policy) in [
        (&records[0], "agent-8", ledger::FRESH_CLONE),
        (&records[1], "agent-9", ledger::WORKTREE),
    ] {
        assert_eq!(record.agent_id, agent);
        assert_eq!(record.policy, policy);
        assert_eq!(record.mode, "all");
        assert_eq!(record.branch, format!("horde/{agent}"));
        assert_eq!(record.repos, expected_repos);
        assert!(record.created_at_ms > 0, "{record:?}");
        assert!(record.last_active_at_ms > 0, "{record:?}");
    }
}

/// A branch git would refuse, or one shaped like an option, is refused up
/// front with the same sentence.
#[tokio::test]
async fn an_invalid_branch_name_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas"]).await;
    for branch in ["bad..name", "--evil"] {
        let mut asked = request("agent-1");
        asked.branch = branch.to_owned();
        let error = manager
            .checkout_agent(&asked, &NoSource)
            .await
            .expect_err("refused")
            .to_string();
        // The worker's own check refuses, before git is asked to make the
        // branch (git's refusal words it with single quotes).
        assert!(
            error.contains(&format!("{branch:?} is not a valid branch name")),
            "{error}"
        );
    }
    assert!(!manager.agents_dir("ws-1").join("agent-1").exists());
}

/// The shared policy works in the Instance at its HEAD, and refuses
/// anything that would change the Instance's tree.
#[tokio::test]
async fn the_shared_policy_is_the_instance_itself() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas"]).await;
    let shared = CheckoutRequest {
        policy: CheckoutPolicy::Shared,
        ..request("agent-1")
    };
    let instance_root = dir.path().join("ws/ws-1");
    let prepared = manager
        .checkout_agent(&shared, &NoSource)
        .await
        .expect("shared");
    assert_eq!(prepared.root, instance_root);
    assert_eq!(
        prepared.repos,
        vec![("atlas".to_owned(), PathBuf::from("atlas"))]
    );
    assert_eq!(
        prepared.prepared.repos,
        vec![PreparedRepo {
            repo_name: "atlas".to_owned(),
            base_commit_sha: head(&instance_root.join("atlas")),
            resolution: BaseResolution::InstanceHead.into(),
            ..PreparedRepo::default()
        }]
    );

    let lazy = CheckoutRequest {
        mode: CheckoutMode::Lazy,
        ..shared.clone()
    };
    let branched = CheckoutRequest {
        branch: "feat".to_owned(),
        ..shared.clone()
    };
    let based = CheckoutRequest {
        bases: vec![RepoBase {
            repo_name: "atlas".to_owned(),
            r#ref: "main".to_owned(),
            source: RepoBaseSource::Requested.into(),
        }],
        ..shared.clone()
    };
    for refused in [lazy, branched, based] {
        let error = manager
            .checkout_agent(&refused, &NoSource)
            .await
            .expect_err("it would change the instance")
            .to_string();
        assert!(error.contains("shared policy"), "{error}");
    }
}
