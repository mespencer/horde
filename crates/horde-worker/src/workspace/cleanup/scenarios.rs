//! Cleanup against real git: the actions each operation records — repo,
//! outcome, reason, bytes and unrecoverable files — and the decisions
//! behind them (what blocks an Instance's removal, when a pin may go,
//! what activity clears).
//!
//! Every assertion reads what production returned or left on disk.

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use horde_proto::v1::{CheckoutPolicy, WorkspaceRepo};

use super::super::base::NoSource;
use super::super::manager::CheckoutRequest;
use super::{
    AgentId, CleanupTier, CleanupTrigger, LedgerRecord, Outcome, Pass, RepoAction, RetainedFile,
    WorkerId, WorkspaceCleanup, WorkspaceDefinition, WorkspaceDefinitionId,
    WorkspaceInstanceReport, WorkspaceInstanceState, WorkspaceManager, WorkspacePolicy,
    commit_older_than, hard_remove, ledger, note_activity, removal, remove_instance, unpin,
};

const WS: &str = "ws-1";

fn git_in(cwd: &Path, args: &[&str], date: Option<&str>) -> String {
    let mut command = std::process::Command::new("git");
    command
        .args(["-c", "user.name=h", "-c", "user.email=h@t"])
        .args(args)
        .current_dir(cwd);
    if let Some(date) = date {
        command
            .env("GIT_AUTHOR_DATE", date)
            .env("GIT_COMMITTER_DATE", date);
    }
    let output = command.output().expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn git(cwd: &Path, args: &[&str]) -> String {
    git_in(cwd, args, None)
}

/// An origin whose `.gitignore` ignores `*.secret`.
fn origin(root: &Path, name: &str) -> PathBuf {
    let path = root.join(format!("{name}-origin"));
    std::fs::create_dir_all(&path).expect("origin dir");
    git(&path, &["init", "-q", "-b", "main"]);
    std::fs::write(path.join(".gitignore"), "*.secret\n").expect("gitignore");
    std::fs::write(path.join("README.md"), name).expect("readme");
    git(&path, &["add", "."]);
    git(&path, &["commit", "-qm", "seed"]);
    path
}

struct Rig {
    _temp: tempfile::TempDir,
    manager: WorkspaceManager,
    policy: WorkspacePolicy,
}

impl Rig {
    async fn new(repos: &[&str], unmanaged: &[&str]) -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let repos = repos
            .iter()
            .map(|name| WorkspaceRepo {
                name: (*name).to_owned(),
                remote_url: origin(temp.path(), name).display().to_string(),
                default_branch: "main".to_owned(),
                ..WorkspaceRepo::default()
            })
            .collect();
        let definition = WorkspaceDefinition {
            workspace_definition_id: Some(WorkspaceDefinitionId::new(WS)),
            repos,
            unmanaged_paths: unmanaged.iter().map(|path| (*path).to_owned()).collect(),
            ..WorkspaceDefinition::default()
        };
        let manager = WorkspaceManager::new("worker-1", temp.path().join("ws"));
        manager.materialize(&definition).await;
        Self {
            _temp: temp,
            manager,
            policy: WorkspacePolicy::default(),
        }
    }

    fn instance(&self) -> PathBuf {
        self.manager.instance_root(WS)
    }

    fn instance_repo(&self, repo: &str) -> PathBuf {
        self.instance().join(repo)
    }

    fn agent_root(&self, agent: &str) -> PathBuf {
        self.manager.agents_dir(WS).join(agent)
    }

    async fn spawn(&self, agent: &str) -> PathBuf {
        let request = CheckoutRequest {
            definition_id: WS.to_owned(),
            agent_id: agent.to_owned(),
            policy: CheckoutPolicy::Worktree,
            ..CheckoutRequest::default()
        };
        self.manager
            .checkout_agent(&request, &NoSource)
            .await
            .expect("the checkout composes")
            .root
    }

    async fn record(&self, agent: &str) -> Option<LedgerRecord> {
        ledger::read(&self.manager.agents_dir(WS), agent).await
    }

    fn pass(&self, dry_run: bool) -> Pass<'_> {
        Pass {
            trigger: CleanupTrigger::Explicit,
            gc_request_id: "gc-1",
            dry_run,
            policy: &self.policy,
        }
    }

    async fn hard(&self, agent: &str, dry_run: bool, force: bool) -> (WorkspaceCleanup, bool) {
        let record = self.record(agent).await.expect("a ledgered checkout");
        hard_remove(&self.manager, WS, &record, self.pass(dry_run), force).await
    }

    async fn remove(
        &self,
        dry_run: bool,
        force: bool,
    ) -> (Vec<WorkspaceCleanup>, Option<WorkspaceInstanceReport>) {
        remove_instance(&self.manager, WS, &[], &no_relay, self.pass(dry_run), force).await
    }

    async fn unpin(&self, dry_run: bool, force: bool) -> Vec<WorkspaceCleanup> {
        unpin(&self.manager, WS, &no_relay, self.pass(dry_run), force).await
    }
}

fn no_relay(_: &str) -> Option<String> {
    None
}

fn outcome(action: &RepoAction) -> Outcome {
    Outcome::try_from(action.outcome).expect("a known outcome")
}

fn instance_actions(events: &[WorkspaceCleanup]) -> Vec<RepoAction> {
    events
        .iter()
        .filter(|event| event.tier == i32::from(CleanupTier::Instance))
        .flat_map(|event| event.repos.clone())
        .collect()
}

fn refusal_reason(events: &[WorkspaceCleanup]) -> String {
    let actions = instance_actions(events);
    assert_eq!(actions.len(), 1, "one instance action: {actions:?}");
    assert_eq!(outcome(&actions[0]), Outcome::Refused, "{actions:?}");
    actions[0].reason.clone()
}

/// Makes a directory undeletable by its owner, and restores it on drop so
/// the tempdir can go.
struct Stuck(PathBuf);

impl Stuck {
    fn new(dir: PathBuf) -> Self {
        std::fs::create_dir_all(dir.join("inner")).expect("stuck dir");
        std::fs::write(dir.join("inner/file"), "x").expect("stuck file");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).expect("chmod");
        Self(dir)
    }
}

impl Drop for Stuck {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
    }
}

// ---- hard removal of one checkout -----------------------------------------

/// A checkout with one dirty repo and one clean: the dirty one is refused
/// with its reason, the clean one skipped alongside, each by name.
#[tokio::test]
async fn a_refused_checkout_names_each_repo_and_why() {
    let rig = Rig::new(&["atlas", "beta"], &[]).await;
    let root = rig.spawn("agent-1").await;
    std::fs::write(root.join("beta/notes.txt"), "draft").expect("untracked");

    let (event, removed) = rig.hard("agent-1", false, false).await;
    assert!(!removed);
    let atlas = event
        .repos
        .iter()
        .find(|action| action.repo_name == "atlas")
        .expect("atlas is named");
    assert_eq!(outcome(atlas), Outcome::Skipped);
    assert_eq!(
        atlas.reason,
        "another repo of this checkout is not provably lossless"
    );
    let beta = event
        .repos
        .iter()
        .find(|action| action.repo_name == "beta")
        .expect("beta is named");
    assert_eq!(outcome(beta), Outcome::Refused);
    assert_eq!(beta.reason, "untracked files: 1");
    assert!(root.join("beta/notes.txt").exists());
}

/// An ignored file outside every reclaim output is loss: a forced dry run
/// says it would remove the repo anyway, naming the overridden refusal and
/// the file; the real forced run removes it and counts the bytes.
#[tokio::test]
async fn a_forced_removal_names_what_it_destroys() {
    let rig = Rig::new(&["atlas"], &[]).await;
    let root = rig.spawn("agent-1").await;
    std::fs::write(root.join("atlas/key.secret"), "0123456789").expect("ignored");

    let (refused, _) = rig.hard("agent-1", true, false).await;
    assert_eq!(refused.repos.len(), 1);
    assert_eq!(refused.repos[0].repo_name, "atlas");
    assert_eq!(outcome(&refused.repos[0]), Outcome::Refused);
    assert_eq!(refused.repos[0].unrecoverable_files[0].path, "key.secret");

    let (dry, removed) = rig.hard("agent-1", true, true).await;
    assert!(!removed, "a dry run removes nothing");
    let action = &dry.repos[0];
    assert_eq!(action.repo_name, "atlas");
    assert_eq!(outcome(action), Outcome::WouldDo);
    assert!(action.reason.starts_with("forced: "), "{}", action.reason);
    assert_eq!(
        action.unrecoverable_files,
        vec![RetainedFile {
            path: "key.secret".to_owned(),
            bytes: 10
        }]
    );
    assert!(root.join("atlas/key.secret").exists());

    let (done, removed) = rig.hard("agent-1", false, true).await;
    assert!(removed);
    let action = &done.repos[0];
    assert_eq!(action.repo_name, "atlas");
    assert_eq!(outcome(action), Outcome::Done);
    assert!(action.reason.starts_with("forced: "), "{}", action.reason);
    assert!(action.bytes_freed >= 10, "{}", action.bytes_freed);
    assert_eq!(action.unrecoverable_files[0].path, "key.secret");
    assert!(!rig.agent_root("agent-1").exists());
    assert_eq!(rig.record("agent-1").await, None, "the record goes last");
}

/// A repo whose removal fails is recorded as failed, by name and with the
/// error, and the checkout stays ledgered.
#[tokio::test]
async fn a_repo_that_cannot_be_removed_is_recorded_failed() {
    let rig = Rig::new(&["atlas"], &[]).await;
    let root = rig.spawn("agent-1").await;
    std::fs::write(root.join("atlas/.git"), "gitdir: /nonexistent/horde\n").expect("broken");

    let (event, removed) = rig.hard("agent-1", false, true).await;
    assert!(!removed);
    assert_eq!(event.repos.len(), 1);
    let action = &event.repos[0];
    assert_eq!(action.repo_name, "atlas");
    assert_eq!(outcome(action), Outcome::Failed);
    assert!(
        action.reason.contains("HEAD names no commit"),
        "{}",
        action.reason
    );
    assert!(rig.record("agent-1").await.is_some());
}

/// The agent root already gone is fine: the removal completes and the
/// record goes.
#[tokio::test]
async fn an_already_missing_agent_root_still_completes_the_removal() {
    let rig = Rig::new(&["atlas"], &[]).await;
    rig.spawn("agent-1").await;
    std::fs::remove_dir_all(rig.agent_root("agent-1")).expect("gone already");
    git(&rig.instance_repo("atlas"), &["worktree", "prune"]);

    let (event, removed) = rig.hard("agent-1", false, true).await;
    assert!(removed, "{event:?}");
    assert!(
        event
            .repos
            .iter()
            .all(|action| outcome(action) == Outcome::Done),
        "{event:?}"
    );
    assert_eq!(rig.record("agent-1").await, None);
}

/// An agent root that cannot be deleted fails the removal with a reason,
/// and the record stays.
#[tokio::test]
async fn an_undeletable_agent_root_fails_the_removal() {
    let rig = Rig::new(&["atlas"], &[]).await;
    rig.spawn("agent-1").await;
    let _stuck = Stuck::new(rig.agent_root("agent-1").join("stuck"));

    let (event, removed) = rig.hard("agent-1", false, false).await;
    assert!(!removed);
    let last = event.repos.last().expect("an action");
    assert_eq!(outcome(last), Outcome::Failed);
    assert!(
        last.reason.starts_with("could not remove"),
        "{}",
        last.reason
    );
    assert!(rig.record("agent-1").await.is_some());
}

// ---- activity ---------------------------------------------------------------

/// Only newer activity is written through, and it clears a soft reclaim.
#[tokio::test]
async fn only_newer_activity_is_noted_and_it_clears_a_soft_reclaim() {
    let temp = tempfile::tempdir().expect("tempdir");
    let manager = WorkspaceManager::new("worker-1", temp.path());
    let agents_dir = manager.agents_dir(WS);
    let record = LedgerRecord {
        agent_id: "agent-1".to_owned(),
        last_active_at_ms: 1_000,
        soft_reclaimed_at_ms: Some(900),
        ..LedgerRecord::default()
    };
    ledger::write(&agents_dir, &record).await.expect("written");

    for stale in [1_000, 500] {
        note_activity(&manager, WS, "agent-1", stale).await;
        assert_eq!(
            ledger::read(&agents_dir, "agent-1").await,
            Some(record.clone()),
            "activity at {stale} is not newer"
        );
    }
    note_activity(&manager, WS, "agent-1", 2_000).await;
    let after = ledger::read(&agents_dir, "agent-1")
        .await
        .expect("the record");
    assert_eq!(after.last_active_at_ms, 2_000);
    assert_eq!(after.soft_reclaimed_at_ms, None);
}

// ---- archive pins -------------------------------------------------------------

/// Archives `agent` with one unpushed commit dated 2001, leaving only its
/// pin behind.
async fn archived_with_old_work(rig: &Rig, agent: &str) -> String {
    let root = rig.spawn(agent).await;
    let checkout = root.join("atlas");
    std::fs::write(checkout.join("work.txt"), "work").expect("work");
    git(&checkout, &["add", "."]);
    git_in(
        &checkout,
        &["commit", "-qm", "old work"],
        Some("2001-01-01T00:00:00Z"),
    );
    let (_, removed) = rig.hard(agent, false, false).await;
    assert!(removed, "the work is pinned, so the checkout goes");
    removal::pin_ref(agent)
}

fn has_ref(repo: &Path, reference: &str) -> bool {
    std::process::Command::new("git")
        .args(["rev-parse", "--verify", "-q", reference])
        .current_dir(repo)
        .status()
        .expect("git runs")
        .success()
}

/// A one-repo (`atlas`) rig whose `agent` was spawned and then removed,
/// losslessly, leaving its pin.
async fn pinned_rig(agent: &str) -> Rig {
    let rig = Rig::new(&["atlas"], &[]).await;
    rig.spawn(agent).await;
    let (_, removed) = rig.hard(agent, false, false).await;
    assert!(removed, "the removal pins and goes");
    rig
}

/// A pin reachable from origin goes, with no reason; the event names the
/// pin's agent and repo.
#[tokio::test]
async fn a_reachable_pin_is_dropped_and_names_its_agent() {
    let rig = pinned_rig("agent-1").await;

    let events = rig.unpin(false, false).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].agent_id, Some(AgentId::new("agent-1")));
    let action = &events[0].repos[0];
    assert_eq!(action.repo_name, "atlas");
    assert_eq!(outcome(action), Outcome::Done);
    assert_eq!(action.reason, "");
    assert!(!has_ref(
        &rig.instance_repo("atlas"),
        &removal::pin_ref("agent-1")
    ));
}

/// An unreachable pin is refused with the reachability reason — even past
/// its TTL, unless forced.
#[tokio::test]
async fn an_unreachable_pin_is_refused_unless_forced_past_its_ttl() {
    let mut rig = Rig::new(&["atlas"], &[]).await;
    let pin = archived_with_old_work(&rig, "agent-1").await;
    rig.policy.archived_pin_ttl = Some(std::time::Duration::from_secs(3600));

    let events = rig.unpin(false, false).await;
    assert_eq!(events.len(), 1);
    let action = &events[0].repos[0];
    assert_eq!(action.repo_name, "atlas");
    assert_eq!(outcome(action), Outcome::Refused);
    assert!(
        action.reason.contains("is on no origin branch"),
        "{}",
        action.reason
    );
    assert!(has_ref(&rig.instance_repo("atlas"), &pin));

    let dry = rig.unpin(true, true).await;
    let action = &dry[0].repos[0];
    assert_eq!(outcome(action), Outcome::WouldDo);
    assert!(
        action.reason.starts_with("older than the pin TTL: "),
        "{}",
        action.reason
    );
    assert!(has_ref(&rig.instance_repo("atlas"), &pin));

    let done = rig.unpin(false, true).await;
    let action = &done[0].repos[0];
    assert_eq!(outcome(action), Outcome::Done);
    assert!(
        action.reason.starts_with("older than the pin TTL: "),
        "{}",
        action.reason
    );
    assert!(!has_ref(&rig.instance_repo("atlas"), &pin));
}

/// Forced without a TTL, an unreachable pin never expires.
#[tokio::test]
async fn without_a_ttl_a_forced_unpin_keeps_an_unreachable_pin() {
    let rig = Rig::new(&["atlas"], &[]).await;
    let pin = archived_with_old_work(&rig, "agent-1").await;
    let events = rig.unpin(false, true).await;
    assert_eq!(outcome(&events[0].repos[0]), Outcome::Refused);
    assert!(has_ref(&rig.instance_repo("atlas"), &pin));
}

/// A pin whose ref cannot be deleted is recorded failed with git's error.
#[tokio::test]
async fn a_pin_that_cannot_be_deleted_is_recorded_failed() {
    let rig = pinned_rig("agent-1").await;
    let lock = rig
        .instance_repo("atlas")
        .join(".git/refs/horde/archived/agent-1.lock");
    std::fs::create_dir_all(lock.parent().expect("a parent")).expect("refs dir");
    std::fs::write(&lock, "").expect("a held lock");

    let events = rig.unpin(false, false).await;
    let action = &events[0].repos[0];
    assert_eq!(action.repo_name, "atlas");
    assert_eq!(outcome(action), Outcome::Failed);
    assert!(!action.reason.is_empty());
}

/// Commit age reads the commit's own date.
#[test]
fn a_commit_is_older_than_a_ttl_by_its_date() {
    let temp = tempfile::tempdir().expect("tempdir");
    let repo = origin(temp.path(), "atlas");
    std::fs::write(repo.join("old.txt"), "old").expect("file");
    git(&repo, &["add", "."]);
    git_in(
        &repo,
        &["commit", "-qm", "old"],
        Some("2001-01-01T00:00:00Z"),
    );
    let hour = std::time::Duration::from_secs(3600);
    let century = std::time::Duration::from_secs(100 * 365 * 24 * 3600);
    assert!(commit_older_than(&repo, "HEAD", hour));
    assert!(!commit_older_than(&repo, "HEAD", century));
    assert!(!commit_older_than(&repo, "refs/heads/nothing", hour));
}

// ---- Instance removal -----------------------------------------------------------

/// A clean Instance with an empty unmanaged path goes without force, and
/// its report says so.
#[tokio::test]
async fn a_clean_instance_goes_unforced_and_reports_missing() {
    let rig = Rig::new(&["atlas"], &["data"]).await;
    assert!(rig.instance().join("data").is_dir());

    let (events, report) = rig.remove(false, false).await;
    let report = report.expect("removed");
    assert!(!rig.instance().exists());
    assert_eq!(
        report.workspace_definition_id,
        Some(WorkspaceDefinitionId::new(WS))
    );
    assert_eq!(report.worker_id, Some(WorkerId::new("worker-1")));
    assert_eq!(report.state, i32::from(WorkspaceInstanceState::Missing));
    assert_eq!(report.root_path, rig.instance().display().to_string());
    assert_eq!(report.detail, "removed");
    let actions = instance_actions(&events);
    assert_eq!(actions.len(), 1);
    assert_eq!(outcome(&actions[0]), Outcome::Done);
    assert!(actions[0].unrecoverable_files.is_empty());
    assert!(actions[0].bytes_freed > 0);
}

/// A dirty Instance tree blocks an unforced removal and names the repo;
/// forced, it goes.
#[tokio::test]
async fn a_dirty_instance_blocks_unless_forced() {
    let rig = Rig::new(&["atlas"], &[]).await;
    std::fs::write(rig.instance_repo("atlas").join("stray.txt"), "x").expect("dirty");

    let (events, report) = rig.remove(false, false).await;
    assert_eq!(report, None);
    let reason = refusal_reason(&events);
    assert!(
        reason.starts_with("repo atlas: untracked files: 1"),
        "{reason}"
    );
    assert!(rig.instance().exists());

    let (_, report) = rig.remove(false, true).await;
    assert!(report.is_some());
    assert!(!rig.instance().exists());
}

/// A local branch holding commits nobody keeps blocks the removal.
#[tokio::test]
async fn an_unkept_local_branch_blocks_the_instance() {
    let rig = Rig::new(&["atlas"], &[]).await;
    let repo = rig.instance_repo("atlas");
    git(&repo, &["checkout", "-q", "-b", "side"]);
    std::fs::write(repo.join("side.txt"), "x").expect("file");
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "side"]);
    git(&repo, &["checkout", "-q", "main"]);

    let (events, report) = rig.remove(false, false).await;
    assert_eq!(report, None);
    let reason = refusal_reason(&events);
    assert!(
        reason.starts_with("repo atlas refs/heads/side: "),
        "{reason}"
    );
}

/// Unmanaged data is anything there: an empty file in the directory, or a
/// regular file standing in its place, blocks an unforced removal and is
/// named with its size.
#[tokio::test]
async fn any_unmanaged_data_blocks_an_unforced_removal() {
    let rig = Rig::new(&["atlas"], &["data"]).await;
    std::fs::write(rig.instance().join("data/empty"), "").expect("empty file");
    let (events, report) = rig.remove(false, false).await;
    assert_eq!(report, None);
    assert_eq!(
        refusal_reason(&events),
        "non-empty unmanaged data cannot be proven recoverable"
    );

    let data = rig.instance().join("data");
    std::fs::remove_dir_all(&data).expect("dir gone");
    std::fs::write(&data, vec![0_u8; 64]).expect("a file in its place");
    let (events, report) = rig.remove(false, false).await;
    assert_eq!(report, None);
    let actions = instance_actions(&events);
    assert_eq!(
        actions[0].unrecoverable_files,
        vec![RetainedFile {
            path: "data".to_owned(),
            bytes: 64
        }]
    );

    let (events, report) = rig.remove(false, true).await;
    assert!(report.is_some());
    let actions = instance_actions(&events);
    assert_eq!(outcome(&actions[0]), Outcome::Done);
    assert_eq!(actions[0].unrecoverable_files[0].path, "data");
    assert!(actions[0].bytes_freed >= 64);
}

/// An Instance root that cannot be deleted fails with a reason.
#[tokio::test]
async fn an_undeletable_instance_root_fails() {
    let rig = Rig::new(&["atlas"], &[]).await;
    let _stuck = Stuck::new(rig.instance().join("stuck"));
    let (events, report) = rig.remove(false, true).await;
    assert_eq!(report, None);
    let actions = instance_actions(&events);
    assert_eq!(outcome(&actions[0]), Outcome::Failed);
    assert!(
        actions[0].reason.starts_with("could not remove"),
        "{}",
        actions[0].reason
    );
}

/// A dry run over a lossless checkout says what would go — the checkout
/// and the Instance — and removes nothing.
#[tokio::test]
async fn a_dry_run_over_a_lossless_checkout_would_remove_everything() {
    let rig = Rig::new(&["atlas"], &[]).await;
    rig.spawn("agent-1").await;

    let (events, report) = rig.remove(true, false).await;
    assert_eq!(report, None);
    let tiers: Vec<i32> = events.iter().map(|event| event.tier).collect();
    assert_eq!(
        tiers,
        vec![
            i32::from(CleanupTier::Hard),
            i32::from(CleanupTier::Instance)
        ]
    );
    assert_eq!(events[0].agent_id, Some(AgentId::new("agent-1")));
    assert_eq!(outcome(&events[0].repos[0]), Outcome::WouldDo);
    assert_eq!(outcome(&events[1].repos[0]), Outcome::WouldDo);
    assert!(rig.agent_root("agent-1").exists());
    assert!(rig.instance().exists());
}

/// A dry run over a dirty checkout says the checkout blocks the Instance.
#[tokio::test]
async fn a_dry_run_over_a_dirty_checkout_is_blocked_by_it() {
    let rig = Rig::new(&["atlas"], &[]).await;
    let root = rig.spawn("agent-1").await;
    std::fs::write(root.join("atlas/notes.txt"), "draft").expect("untracked");

    let (events, report) = rig.remove(true, false).await;
    assert_eq!(report, None);
    assert_eq!(refusal_reason(&events), "checkout of agent agent-1");
}

/// For real, a lossless checkout goes first and then the Instance.
#[tokio::test]
async fn a_lossless_checkout_goes_with_its_instance() {
    let rig = Rig::new(&["atlas"], &[]).await;
    rig.spawn("agent-1").await;

    let (events, report) = rig.remove(false, false).await;
    assert!(report.is_some(), "{events:?}");
    assert_eq!(events.len(), 2);
    assert_eq!(outcome(&events[0].repos[0]), Outcome::Done);
    assert!(!rig.instance().exists());
}
