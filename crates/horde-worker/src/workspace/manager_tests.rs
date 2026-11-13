//! [`WorkspaceManager`] against real repositories: identity and disk facts,
//! lazy additions, adoption, half-built recovery, synthesized records, the
//! shared policy and branch-name validation.

use std::sync::Mutex;

use super::super::base::{FetchPolicy, NoSource};
use super::super::ledger;
use super::{
    Arc, BaseResolution, CheckoutMode, CheckoutPolicy, CheckoutRequest, FsStats, LedgerRecord,
    LedgerRepo, Path, PathBuf, PreparedRepo, RepoBase, RepoBaseSource, StatvfsStats,
    WorkspaceDefinition, WorkspaceManager, WorkspaceRepo, saturating_u32,
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
    // An absent path: Unix `statvfs` fails on it, while Windows'
    // `GetDiskFreeSpaceExW` answers for the volume the path would be on.
    let absent = StatvfsStats.stats(&dir.path().join("absent"));
    #[cfg(unix)]
    assert_eq!(absent, None);
    #[cfg(windows)]
    assert_eq!(
        absent.map(|(_, total)| total),
        Some(total),
        "an absent path reports its volume"
    );
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
    assert_eq!(prepared.overlay, None, "the shared policy has no overlay");

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

/// The bytes `statvfs` reports agree with `df`'s: the filesystem's size
/// exactly, and its free space to within half a block per free block —
/// tight enough that miscounting the block size by one is caught. Other
/// writers can move free space between the two readings, so a reading is
/// retried a few times; a miscount is off by the same amount every time.
#[test]
fn statvfs_agrees_with_df() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut last = None;
    for _ in 0..10 {
        let (free, total) = StatvfsStats.stats(dir.path()).expect("a real filesystem");
        let output = std::process::Command::new("df")
            .args(["-B1", "--output=size,avail"])
            .arg(dir.path())
            .output()
            .expect("df runs");
        assert!(output.status.success(), "df failed");
        let text = String::from_utf8_lossy(&output.stdout).into_owned();
        let numbers: Vec<u64> = text
            .lines()
            .nth(1)
            .expect("df's row")
            .split_whitespace()
            .map(|field| field.parse().expect("a byte count"))
            .collect();
        let (size, avail) = (numbers[0], numbers[1]);
        assert_eq!(total, size, "the filesystem's size");
        if free.abs_diff(avail) <= avail / 8192 {
            return;
        }
        last = Some((free, avail));
    }
    panic!("statvfs's free space never agreed with df's: {last:?}");
}

/// Collects the progress a materialization reports, as `(step, completed,
/// total)`.
async fn progress_of(
    manager: &WorkspaceManager,
    definition: &WorkspaceDefinition,
) -> (
    horde_proto::v1::WorkspaceInstanceReport,
    Vec<(String, u32, u32)>,
) {
    let mut steps = Vec::new();
    let report = manager
        .materialize_with(definition, &mut |progress| {
            steps.push((
                progress.step,
                progress.completed_repos,
                progress.total_repos,
            ));
        })
        .await;
    (report, steps)
}

/// A repo directory with no `.git` in it is cloned into, not fetched in;
/// once cloned it is fetched. Each repo and the unmanaged paths are one
/// step each, counted.
#[tokio::test]
async fn materialization_clones_then_fetches_and_counts_its_steps() {
    let dir = tempfile::tempdir().expect("tempdir");
    let manager = WorkspaceManager::new("w-7", dir.path().join("ws"));
    let mut definition = definition("ws-1", dir.path(), &["atlas", "beta"]);
    definition.unmanaged_paths = vec!["data".to_owned()];
    std::fs::create_dir_all(dir.path().join("ws/ws-1/atlas")).expect("an empty repo directory");
    let (first, steps) = progress_of(&manager, &definition).await;
    assert_eq!(first.detail, "");
    assert_eq!(
        first.state,
        i32::from(horde_proto::v1::WorkspaceInstanceState::Ready)
    );
    assert_eq!(
        steps,
        vec![
            ("cloning atlas".to_owned(), 0, 2),
            ("cloning beta".to_owned(), 1, 2),
            ("creating unmanaged paths".to_owned(), 2, 2),
        ]
    );
    let (_, again) = progress_of(&manager, &definition).await;
    assert_eq!(again[0], ("fetching atlas".to_owned(), 0, 2));
    assert_eq!(again[1], ("fetching beta".to_owned(), 1, 2));
}

/// A repo with no default branch named is cloned at origin's HEAD.
#[tokio::test]
async fn a_repo_with_no_default_branch_clones_origins_head() {
    let dir = tempfile::tempdir().expect("tempdir");
    let manager = WorkspaceManager::new("w-7", dir.path().join("ws"));
    let mut definition = definition("ws-1", dir.path(), &["atlas"]);
    definition.repos[0].default_branch = String::new();
    let report = manager.materialize(&definition).await;
    assert_eq!(report.detail, "");
    let origin_head = head(Path::new(&definition.repos[0].remote_url));
    assert_eq!(report.repos[0].head_commit_sha, origin_head);
}

/// A Definition id that is not a directory name materializes nothing.
#[tokio::test]
async fn an_unsafe_definition_id_materializes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let manager = WorkspaceManager::new("w-7", dir.path().join("ws"));
    let mut definition = definition("ws-1", dir.path(), &["atlas"]);
    definition.workspace_definition_id =
        Some(horde_proto::v1::WorkspaceDefinitionId::new("../escape"));
    let report = manager.materialize(&definition).await;
    assert_eq!(
        report.state,
        i32::from(horde_proto::v1::WorkspaceInstanceState::Degraded)
    );
    assert_eq!(
        report.detail,
        "definition id \"../escape\" is not a directory name"
    );
    assert!(!dir.path().join("escape").exists());
    assert!(!dir.path().join("ws").exists());
}

/// An Instance root that cannot be created, or a state file that cannot
/// be written, degrades the report and says which.
#[tokio::test]
async fn what_cannot_be_written_degrades_the_instance() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("ws"), "a file where the root goes").expect("a file");
    let definition = definition("ws-1", dir.path(), &["atlas"]);
    let blocked = WorkspaceManager::new("w-7", dir.path().join("ws"));
    let report = blocked.materialize(&definition).await;
    assert_eq!(
        report.state,
        i32::from(horde_proto::v1::WorkspaceInstanceState::Degraded)
    );
    assert!(
        report.detail.starts_with("could not create "),
        "{}",
        report.detail
    );

    let manager = WorkspaceManager::new("w-7", dir.path().join("ws2"));
    std::fs::create_dir_all(dir.path().join("ws2/ws-1").join(super::STATE_FILE))
        .expect("a directory where the state file goes");
    let report = manager.materialize(&definition).await;
    assert_eq!(
        report.state,
        i32::from(horde_proto::v1::WorkspaceInstanceState::Degraded)
    );
    assert!(
        report
            .detail
            .starts_with("could not write the instance state file: "),
        "{}",
        report.detail
    );
}

/// Unmanaged paths that would leave the Instance are each refused and
/// named, the rest are still created, and only the contained ones are
/// sized.
#[tokio::test]
async fn escaping_unmanaged_paths_are_refused_one_by_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let manager = WorkspaceManager::new("w-7", dir.path().join("ws"));
    let mut definition = definition("ws-1", dir.path(), &["atlas"]);
    definition.unmanaged_paths = vec![
        "../escape".to_owned(),
        "data".to_owned(),
        "../other".to_owned(),
        "more".to_owned(),
    ];
    let report = manager.materialize(&definition).await;
    assert_eq!(
        report.detail,
        "unmanaged path \"../escape\" is not a relative path inside the instance; \
         unmanaged path \"../other\" is not a relative path inside the instance"
    );
    let ws = dir.path().join("ws");
    assert!(ws.join("ws-1/data").is_dir() && ws.join("ws-1/more").is_dir());
    assert!(!ws.join("escape").exists() && !ws.join("other").exists());

    std::fs::create_dir_all(ws.join("escape")).expect("data outside");
    std::fs::write(ws.join("escape/outside.bin"), [0_u8; 100]).expect("outside");
    std::fs::write(ws.join("ws-1/data/inside.bin"), [0_u8; 64]).expect("inside");
    let reports = manager.warm_instances().await;
    assert_eq!(reports[0].unmanaged_bytes, 64);
}

/// An unmanaged path that cannot be created is named with the reason.
#[tokio::test]
async fn an_unmanaged_path_that_cannot_be_created_is_named() {
    let dir = tempfile::tempdir().expect("tempdir");
    let manager = WorkspaceManager::new("w-7", dir.path().join("ws"));
    let mut definition = definition("ws-1", dir.path(), &["atlas"]);
    manager.materialize(&definition).await;
    std::fs::write(dir.path().join("ws/ws-1/blocked"), "a file").expect("a file in the way");
    definition.unmanaged_paths = vec!["blocked/x".to_owned()];
    let report = manager.materialize(&definition).await;
    assert!(
        report
            .detail
            .starts_with("could not create unmanaged path blocked/x: "),
        "{}",
        report.detail
    );
}

/// An Instance repo whose directory lost its `.git` is reported missing,
/// not probed with git.
#[tokio::test]
async fn an_instance_repo_without_git_is_missing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas"]).await;
    let repo = dir.path().join("ws/ws-1/atlas");
    std::fs::remove_dir_all(&repo).expect("removed");
    std::fs::create_dir_all(&repo).expect("an empty directory");
    let reports = manager.warm_instances().await;
    assert_eq!(reports[0].detail, "repo atlas is missing from disk");
}

/// The warm report of an unarchived checkout no live agent claims: an
/// orphan, named, with its activity, its size, and when it was sized.
#[tokio::test]
async fn a_warm_report_describes_each_checkout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas"]).await;
    manager
        .checkout_agent(&request("agent-1"), &NoSource)
        .await
        .expect("checked out");
    let record = ledger::read(&manager.agents_dir("ws-1"), "agent-1")
        .await
        .expect("the ledger");
    let reports = manager.warm_instances().await;
    let report = &reports[0];
    assert!(report.instance_bytes > 0, "the Instance is sized");
    let sized_at = report.sized_at.expect("sized now");
    assert!(sized_at.seconds > 1_700_000_000, "{sized_at:?}");
    let checkout = &report.checkouts[0];
    assert_eq!(
        checkout.agent_id,
        Some(horde_proto::v1::AgentId::new("agent-1"))
    );
    assert_eq!(
        checkout.state,
        i32::from(horde_proto::v1::CheckoutState::Orphan)
    );
    assert_eq!(
        checkout.last_active_at,
        Some(super::timestamp(record.last_active_at_ms))
    );
}

/// Records come back in agent order, ledgered and synthesized alike.
#[tokio::test]
async fn records_are_in_agent_order_whatever_their_source() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas"]).await;
    let agents_dir = manager.agents_dir("ws-1");
    ledger::write(&agents_dir, &crashed_record("agent-z"))
        .await
        .expect("a ledgered record");
    std::fs::create_dir_all(agents_dir.join("agent-a")).expect("an unledgered directory");
    let agents: Vec<String> = manager
        .records("ws-1")
        .await
        .into_iter()
        .map(|record| record.agent_id)
        .collect();
    assert_eq!(agents, ["agent-a", "agent-z"]);
}

/// A synthesized record takes its activity from the directory's
/// modification time, and is zero when there is no directory to ask.
#[test]
fn a_synthesized_record_dates_from_its_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let agent_root = dir.path().join("agent-1");
    std::fs::create_dir_all(&agent_root).expect("the agent root");
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_234_567_890);
    std::fs::File::open(&agent_root)
        .expect("the directory opens")
        .set_modified(at)
        .expect("its mtime is set");
    let definition = WorkspaceDefinition::default();
    let record = super::synthesized_record(&agent_root, "agent-1", &definition);
    assert_eq!(record.created_at_ms, 1_234_567_890);
    assert_eq!(record.last_active_at_ms, 1_234_567_890);
    let absent = super::synthesized_record(&dir.path().join("gone"), "gone", &definition);
    assert_eq!(absent.created_at_ms, 0);
    assert_eq!(absent.last_active_at_ms, 0);
}

/// A repo whose layout escapes the Instance is never read as part of a
/// synthesized checkout, nor as making it a fresh clone.
#[test]
fn a_synthesized_record_skips_repos_that_cannot_be_laid_out() {
    let dir = tempfile::tempdir().expect("tempdir");
    let agent_root = dir.path().join("agent-1");
    std::fs::create_dir_all(&agent_root).expect("the agent root");
    let definition = WorkspaceDefinition {
        repos: vec![WorkspaceRepo {
            name: "bad".to_owned(),
            layout_path: "../bad".to_owned(),
            ..WorkspaceRepo::default()
        }],
        ..WorkspaceDefinition::default()
    };
    let record = super::synthesized_record(&agent_root, "agent-1", &definition);
    assert_eq!(record.repos, Vec::<LedgerRepo>::new());
    assert_eq!(record.policy, ledger::WORKTREE);
}

/// Adding a repo is activity: it refreshes the checkout's last activity
/// and undoes an earlier soft reclaim.
#[tokio::test]
async fn adding_a_repo_is_activity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas", "beta"]).await;
    let mut lazy = request("agent-1");
    lazy.checkout_repos = vec!["atlas".to_owned()];
    manager
        .checkout_agent(&lazy, &NoSource)
        .await
        .expect("the lazy checkout");
    let agents_dir = manager.agents_dir("ws-1");
    let mut record = ledger::read(&agents_dir, "agent-1")
        .await
        .expect("the ledger");
    record.last_active_at_ms = 1;
    record.soft_reclaimed_at_ms = Some(5);
    ledger::write(&agents_dir, &record).await.expect("aged");
    manager
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
    let after = ledger::read(&agents_dir, "agent-1")
        .await
        .expect("the ledger");
    assert!(after.last_active_at_ms > 1_700_000_000_000, "{after:?}");
    assert_eq!(after.soft_reclaimed_at_ms, None);
}

/// A directory the ledger does not know, holding files but no `.git`, is
/// neither adopted nor cleared: the checkout fails and the files stay.
#[tokio::test]
async fn an_unknown_directory_without_git_is_left_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas"]).await;
    let destination = manager.agents_dir("ws-1").join("agent-1/atlas");
    std::fs::create_dir_all(&destination).expect("a directory");
    std::fs::write(destination.join("notes.txt"), "someone's").expect("a file");
    let failed = manager.checkout_agent(&request("agent-1"), &NoSource).await;
    assert!(failed.is_err(), "{failed:?}");
    assert_eq!(
        std::fs::read_to_string(destination.join("notes.txt")).expect("still there"),
        "someone's"
    );
}

/// A crashed repo, redone, leaves one ledger entry: done, branch created.
#[tokio::test]
async fn a_redone_repo_leaves_one_ledger_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, agents_dir) = crashed(dir.path()).await;
    let prepared = manager
        .checkout_agent(&request("agent-1"), &NoSource)
        .await
        .expect("composed");
    assert_eq!(
        prepared.prepared.mode,
        i32::from(CheckoutMode::All),
        "an ALL checkout says so"
    );
    assert_eq!(prepared.overlay, None);
    let record = ledger::read(&agents_dir, "agent-1")
        .await
        .expect("the ledger");
    assert_eq!(
        record.repos,
        vec![LedgerRepo {
            name: "atlas".to_owned(),
            base_commit_sha: prepared.prepared.repos[0].base_commit_sha.clone(),
            resolution: "fresh".to_owned(),
            created_branch: true,
            ..LedgerRepo::default()
        }]
    );
}

/// A checkout that fails after the ledger marked its repo pending leaves
/// it pending, with no branch claimed as created.
#[tokio::test]
async fn a_failed_placement_stays_pending() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas"]).await;
    let instance_repo = dir.path().join("ws/ws-1/atlas");
    let holder = dir.path().join("elsewhere");
    run_git(
        &instance_repo,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            "horde/agent-1",
            &holder.display().to_string(),
        ],
    );
    let error = manager
        .checkout_agent(&request("agent-1"), &NoSource)
        .await
        .expect_err("the branch is held elsewhere")
        .to_string();
    assert!(error.contains("checked out by"), "{error}");
    let record = ledger::read(&manager.agents_dir("ws-1"), "agent-1")
        .await
        .expect("the ledger");
    assert_eq!(record.repos.len(), 1);
    assert!(record.repos[0].pending, "{record:?}");
    assert!(!record.repos[0].created_branch, "{record:?}");
}

/// A branch already in the Instance, behind its base and held nowhere, is
/// adopted: the ledger says the branch was not created.
#[tokio::test]
async fn an_adopted_branch_is_not_created() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, _) = materialized(dir.path(), &["atlas"]).await;
    let instance_repo = dir.path().join("ws/ws-1/atlas");
    run_git(&instance_repo, &["branch", "horde/agent-1"]);
    manager
        .checkout_agent(&request("agent-1"), &NoSource)
        .await
        .expect("adopted");
    let record = ledger::read(&manager.agents_dir("ws-1"), "agent-1")
        .await
        .expect("the ledger");
    assert!(!record.repos[0].created_branch, "{record:?}");
    assert!(!record.repos[0].pending, "{record:?}");
}

/// A fresh clone starts on its own new branch at a base only the Instance's
/// remote-tracking refs hold, which the clone fetches first.
#[tokio::test]
async fn a_fresh_clone_fetches_a_base_its_clone_lacks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (manager, definition) = materialized(dir.path(), &["atlas"]).await;
    let origin = PathBuf::from(&definition.repos[0].remote_url);
    std::fs::write(origin.join("NEW.md"), "new").expect("a change");
    run_git(&origin, &["add", "."]);
    run_git(&origin, &["commit", "-qm", "new"]);
    let latest = head(&origin);
    let fresh = CheckoutRequest {
        policy: CheckoutPolicy::FreshClone,
        ..request("agent-1")
    };
    let prepared = manager
        .checkout_agent(&fresh, &NoSource)
        .await
        .expect("cloned");
    assert_eq!(prepared.prepared.repos[0].base_commit_sha, latest);
    assert_eq!(head(&prepared.root.join("atlas")), latest);
    let record = ledger::read(&manager.agents_dir("ws-1"), "agent-1")
        .await
        .expect("the ledger");
    assert!(record.repos[0].created_branch, "{record:?}");
}

/// Unmanaged paths are linked into a checkout once; a recomposition
/// leaves the links it finds and restores any that went missing.
#[tokio::test]
async fn unmanaged_links_are_kept_and_restored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let manager = WorkspaceManager::new("w-7", dir.path().join("ws"));
    let mut definition = definition("ws-1", dir.path(), &["atlas"]);
    definition.unmanaged_paths = vec!["a".to_owned(), "b".to_owned()];
    assert_eq!(manager.materialize(&definition).await.detail, "");
    let first = manager
        .checkout_agent(&request("agent-1"), &NoSource)
        .await
        .expect("composed");
    let (a, b) = (first.root.join("a"), first.root.join("b"));
    assert!(a.is_symlink() && b.is_symlink());
    std::fs::remove_file(&b).expect("one link lost");
    manager
        .checkout_agent(&request("agent-1"), &NoSource)
        .await
        .expect("recomposed over the existing link");
    assert!(a.is_symlink() && b.is_symlink(), "both linked again");
}

#[test]
fn a_count_past_u32_saturates() {
    assert_eq!(saturating_u32(7), 7);
    assert_eq!(saturating_u32(usize::MAX), u32::MAX);
}
