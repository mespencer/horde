//! ADR-0014 on the Worker, against real git: fresh bases, the fetch
//! throttle and its fallback, named bases, lazy checkout, shared branch
//! names, soft reclaim, archive, idle and disk-pressure triggers, orphans,
//! gc levels, Instance removal, and the reports that tell the Server.
//!
//! Every assertion reads what production wrote — the git refs, the files,
//! the journaled events, the reports a recording reporter received — and
//! nothing re-derives a predicate to compare against.

use std::path::{Path, PathBuf};
use std::time::Duration;

use horde_proto::v1::checkout_prepared::BaseResolution;
use horde_proto::v1::event::Kind;
use horde_proto::v1::stream_commands_response::Command;
use horde_proto::v1::workspace_cleanup::repo_action::Outcome;
use horde_proto::v1::{
    CheckoutMode, CheckoutPolicy, CheckoutState, CleanupTier, CleanupTrigger, GcLevel, RepoBase,
    RepoBaseSource, WorkspaceCleanup, WorkspaceDefinition,
};
use horde_worker::workspace::base::NoSource;
use horde_worker::workspace::{CheckoutRequest, WorkspaceManager};
use horde_worker::{ApplyCommand as _, Journal};

use crate::workspace_rig::{
    Rig, archive, cleanups, command, fresh_rig, kinds, materialize, observed_within, policy, rig,
    settled_within, spawn_in, three_repo_rig,
};
use crate::workspaces::harness;

/// A policy with no fetch throttle, so every checkout fetches.
fn eager_fetch() -> horde_worker::workspace::base::FetchPolicy {
    horde_worker::workspace::base::FetchPolicy {
        throttle: Duration::ZERO,
        timeout: Duration::from_secs(30),
    }
}

fn head(repo: &Path) -> String {
    harness::git(repo, &["rev-parse", "HEAD"]).trim().to_owned()
}

/// A commit on the origin's main, past whatever the Instance cloned.
fn upstream_commit(origin: &Path, file: &str) -> String {
    std::fs::write(origin.join(file), "upstream\n").expect("writes");
    harness::git(origin, &["add", "."]);
    harness::git(origin, &["commit", "-qm", "upstream work"]);
    head(origin)
}

async fn checkout(
    manager: &WorkspaceManager,
    request: &CheckoutRequest,
) -> horde_worker::workspace::PreparedCheckout {
    manager
        .checkout_agent(request, &NoSource)
        .await
        .expect("the checkout composes")
}

fn request(agent: &str) -> CheckoutRequest {
    harness::request("ws-1", agent, CheckoutPolicy::Worktree)
}

/// Freshness: the origin advances after the Instance was cloned, so a new
/// agent's base is the new tip — and the Instance's own HEAD and working
/// tree are untouched.
#[tokio::test]
async fn a_new_agent_starts_from_the_freshly_fetched_default_branch() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, instance) = harness::materialized_atlas(temp.path()).await;
    let instance_repo = Path::new(&instance.root_path).join("atlas");
    let cloned_at = head(&instance_repo);
    let tip = upstream_commit(&temp.path().join("atlas-origin"), "new.txt");

    let prepared = checkout(&manager, &request("agent-1")).await;
    let repo = &prepared.prepared.repos[0];
    assert_eq!(repo.base_commit_sha, tip);
    assert_eq!(repo.resolution, i32::from(BaseResolution::Fresh));
    assert!(
        repo.fetched_at.is_some() && repo.stale_reason.is_empty(),
        "{repo:?}"
    );
    assert_eq!(head(&prepared.root.join("atlas")), tip);
    assert_eq!(
        head(&instance_repo),
        cloned_at,
        "the instance HEAD never moves"
    );
    assert!(
        !instance_repo.join("new.txt").exists(),
        "nor its working tree"
    );
}

/// The throttle: two spawns inside it share one fetch — the same
/// `fetched_at` on both, one fetch run, and one `upload-pack` on the
/// origin's side, counted by a wrapper the Instance's remote invokes.
#[tokio::test]
async fn spawns_inside_the_throttle_share_one_fetch() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, instance) = harness::materialized_atlas(temp.path()).await;
    let instance_repo = Path::new(&instance.root_path).join("atlas");
    let counter = temp.path().join("upload-packs");
    let wrapper = temp.path().join("count-upload-pack");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\necho x >> {}\nexec git-upload-pack \"$@\"\n",
            counter.display()
        ),
    )
    .expect("the wrapper");
    std::process::Command::new("chmod")
        .args(["+x"])
        .arg(&wrapper)
        .status()
        .expect("chmod");
    harness::git(
        &instance_repo,
        &[
            "config",
            "remote.origin.uploadpack",
            &wrapper.display().to_string(),
        ],
    );

    let first = checkout(&manager, &request("agent-1")).await;
    let second = checkout(&manager, &request("agent-2")).await;
    assert_eq!(
        first.prepared.repos[0].fetched_at,
        second.prepared.repos[0].fetched_at
    );
    assert_eq!(manager.fetches_run(), 1);
    let packs = std::fs::read_to_string(&counter).expect("the wrapper ran");
    assert_eq!(packs.lines().count(), 1, "one upload-pack for two spawns");
}

/// The stale fallback: with the origin gone, a spawn still succeeds — on
/// the cached `origin/main`, with the reason recorded.
#[tokio::test]
async fn an_unreachable_origin_falls_back_to_the_cached_base_with_a_warning() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition, instance) = harness::materialized_atlas(temp.path()).await;
    let cached = head(&Path::new(&instance.root_path).join("atlas"));
    std::fs::rename(temp.path().join("atlas-origin"), temp.path().join("gone")).expect("moved");
    let mut stale = request("agent-1");
    stale.fetch = eager_fetch();

    let prepared = checkout(&manager, &stale).await;
    let repo = &prepared.prepared.repos[0];
    assert_eq!(repo.resolution, i32::from(BaseResolution::Cached));
    assert_eq!(repo.base_commit_sha, cached);
    assert!(repo.stale_reason.contains("fetch failed"), "{repo:?}");
}

fn base(repo: &str, reference: &str) -> RepoBase {
    RepoBase {
        repo_name: repo.to_owned(),
        r#ref: reference.to_owned(),
        source: RepoBaseSource::Requested.into(),
    }
}

/// A named base resolves as a SHA, an origin branch or a tag; one that
/// names nothing fails the spawn with the ref named — it never falls back.
#[tokio::test]
async fn named_bases_resolve_in_order_and_an_unknown_one_fails() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let origin_dir = temp.path().join("atlas-origin");
    let (manager, _definition, _instance) = harness::materialized_atlas(temp.path()).await;
    let seed = head(&origin_dir);
    harness::git(&origin_dir, &["tag", "v1"]);
    harness::git(&origin_dir, &["checkout", "-qb", "release"]);
    let release = upstream_commit(&origin_dir, "release.txt");
    harness::git(&origin_dir, &["checkout", "-q", "main"]);

    for (agent, reference, expected) in [
        ("agent-sha", &seed[..12], seed.as_str()),
        ("agent-branch", "release", release.as_str()),
        ("agent-tag", "v1", seed.as_str()),
    ] {
        let mut named = request(agent);
        named.bases = vec![base("atlas", reference)];
        let prepared = checkout(&manager, &named).await;
        let repo = &prepared.prepared.repos[0];
        assert_eq!(repo.base_commit_sha, expected, "{reference}");
        assert_eq!(repo.resolution, i32::from(BaseResolution::Requested));
        assert_eq!(repo.requested_ref, reference);
    }

    let mut unknown = request("agent-x");
    unknown.bases = vec![base("atlas", "no-such-ref")];
    let error = manager
        .checkout_agent(&unknown, &NoSource)
        .await
        .expect_err("an unknown base fails the spawn");
    assert!(error.to_string().contains("no-such-ref"), "{error}");
}

/// A two-repo Definition in lazy mode, materialized.
async fn lazy_pair(temp: &Path) -> (WorkspaceManager, WorkspaceDefinition) {
    let (mut definition, _atlas, _docs) = harness::two_repo_definition(temp);
    definition.checkout_mode = CheckoutMode::Lazy.into();
    let manager = harness::manager_under(temp);
    let report = manager.materialize(&definition).await;
    assert_eq!(
        report.state,
        horde_proto::v1::WorkspaceInstanceState::Ready as i32,
        "{}",
        report.detail
    );
    (manager, definition)
}

/// Lazy checkout: no worktrees at start, a manifest naming every repo,
/// `add_repo` (what `workspace_checkout` runs) creating exactly one and
/// idempotent on repeat, and a revival recomposing the same subset.
#[tokio::test]
async fn a_lazy_agent_checks_out_only_what_it_asks_for() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (manager, _definition) = lazy_pair(temp.path()).await;
    let prepared = checkout(&manager, &request("agent-1")).await;
    assert!(prepared.repos.is_empty(), "{:?}", prepared.repos);
    assert_eq!(prepared.prepared.mode, i32::from(CheckoutMode::Lazy));
    assert_eq!(prepared.prepared.available_repos, vec!["atlas", "docs"]);
    assert!(!prepared.root.join("atlas").exists() && !prepared.root.join("docs").exists());
    let overlay = prepared
        .overlay
        .expect("a lazy agent is told how to check out");
    assert!(overlay.contains("workspace_checkout"), "{overlay}");
    let manifest = horde_worker::workspace::manifest::read(&prepared.root)
        .await
        .expect("a manifest");
    assert_eq!(manifest.mode, "lazy");
    assert!(manifest.repos.iter().all(|repo| !repo.checked_out));

    let added = manager
        .add_repo("ws-1", "agent-1", "docs", None, eager_fetch(), &NoSource)
        .await
        .expect("docs checks out");
    assert!(added.path.join(".git").is_file(), "a worktree");
    assert_eq!(added.branch, "horde/agent-1");
    assert!(!prepared.root.join("atlas").exists(), "and only docs");
    let again = manager
        .add_repo("ws-1", "agent-1", "docs", None, eager_fetch(), &NoSource)
        .await
        .expect("idempotent");
    assert_eq!(again.prepared.resolution, i32::from(BaseResolution::Reused));
    assert_eq!(again.path, added.path);
    let manifest = horde_worker::workspace::manifest::read(&prepared.root)
        .await
        .expect("a manifest");
    assert!(
        manifest
            .repos
            .iter()
            .any(|repo| repo.name == "docs" && repo.checked_out)
    );

    let unknown = manager
        .add_repo("ws-1", "agent-1", "ghost", None, eager_fetch(), &NoSource)
        .await
        .expect_err("ghost is not a repo");
    assert!(
        unknown.to_string().contains("valid repos: atlas, docs"),
        "{unknown}"
    );

    let revived = checkout(&manager, &request("agent-1")).await;
    assert_eq!(revived.repos.len(), 1, "the same subset");
    assert_eq!(
        revived.prepared.repos[0].resolution,
        i32::from(BaseResolution::Reused)
    );
}

/// Initial repos imply lazy, and stamping covers exactly the checked-out
/// repos: a lazy agent's result can never carry a repo it never touched.
#[tokio::test]
async fn initial_repos_check_out_at_spawn_and_only_they_are_stamped() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (definition, _atlas, _docs) = harness::two_repo_definition(temp.path());
    let manager = harness::manager_under(temp.path());
    manager.materialize(&definition).await;
    let mut named = request("agent-1");
    named.checkout_repos = vec!["docs".to_owned()];
    let prepared = checkout(&manager, &named).await;
    assert_eq!(
        prepared.repos,
        vec![("docs".to_owned(), PathBuf::from("docs"))]
    );

    let broker = horde_worker::ShareBroker::new(None);
    broker.register(
        "agent-1",
        horde_worker::AgentCheckout {
            root: prepared.root.clone(),
            repos: prepared.repos.clone(),
        },
    );
    let payload = broker
        .stamp("agent-1")
        .await
        .expect("stamps")
        .expect("a payload");
    let stamped: Vec<&str> = payload
        .git_refs
        .iter()
        .map(|r| r.repo_name.as_str())
        .collect();
    assert_eq!(stamped, vec!["docs"]);
}

/// One branch name spans every repo; a second agent naming it while the
/// first holds it is refused with the holder named; a branch origin
/// already has is continued from its tip.
#[tokio::test]
async fn a_branch_spans_repos_is_held_once_and_continues_from_origin() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (definition, atlas_origin, _docs) = harness::two_repo_definition(temp.path());
    let manager = harness::manager_under(temp.path());
    manager.materialize(&definition).await;

    let mut feature = request("agent-1");
    feature.branch = "feat/login".to_owned();
    let prepared = checkout(&manager, &feature).await;
    for repo in ["atlas", "docs"] {
        let branch = harness::git(&prepared.root.join(repo), &["branch", "--show-current"]);
        assert_eq!(branch.trim(), "feat/login", "{repo}");
    }
    let mut twin = request("agent-2");
    twin.branch = "feat/login".to_owned();
    let refused = manager
        .checkout_agent(&twin, &NoSource)
        .await
        .expect_err("held");
    assert!(
        refused.to_string().contains("checked out by agent agent-1"),
        "{refused}"
    );

    harness::git(&atlas_origin, &["checkout", "-qb", "feat/existing"]);
    let tip = upstream_commit(&atlas_origin, "existing.txt");
    harness::git(&atlas_origin, &["checkout", "-q", "main"]);
    let mut continuing = request("agent-3");
    continuing.branch = "feat/existing".to_owned();
    continuing.fetch = eager_fetch();
    let prepared = checkout(&manager, &continuing).await;
    let atlas = prepared
        .prepared
        .repos
        .iter()
        .find(|r| r.repo_name == "atlas")
        .expect("atlas");
    assert_eq!(atlas.base_commit_sha, tip);
    assert_eq!(atlas.resolution, i32::from(BaseResolution::Requested));
    assert_eq!(atlas.requested_ref, "origin/feat/existing");
}

/// A Rust repo's origin: a crate the auto rule will `cargo clean`.
fn crate_origin(root: &Path) -> PathBuf {
    let origin = harness::origin(root, "crate");
    std::fs::write(
        origin.join("Cargo.toml"),
        "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[workspace]\n",
    )
    .expect("Cargo.toml");
    std::fs::create_dir_all(origin.join("src")).expect("src");
    std::fs::write(origin.join("src/lib.rs"), "").expect("lib.rs");
    std::fs::write(origin.join(".gitignore"), "/target\n").expect(".gitignore");
    harness::git(&origin, &["add", "-A"]);
    harness::git(&origin, &["commit", "-qm", "a crate"]);
    origin
}

fn explicit(
    policy: &horde_worker::workspace::policy::WorkspacePolicy,
) -> horde_worker::workspace::cleanup::Pass<'_> {
    horde_worker::workspace::cleanup::Pass {
        trigger: CleanupTrigger::Explicit,
        gc_request_id: "gc-1",
        dry_run: false,
        policy,
    }
}

/// Soft reclaim runs a real `cargo clean` in a Rust checkout: `target/` is
/// gone, the worktree, branch and commit stay — and the checkout is then
/// provably lossless, since `target/` was the reclaim's own output.
#[tokio::test]
async fn soft_reclaim_runs_cargo_clean_and_keeps_the_work() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let origin = crate_origin(temp.path());
    let manager = harness::manager_under(temp.path());
    let definition = harness::definition("ws-1", vec![harness::repo("crate", &origin)]);
    manager.materialize(&definition).await;
    let prepared = checkout(&manager, &request("agent-1")).await;
    let repo = prepared.root.join("crate");
    std::fs::create_dir_all(repo.join("target/debug")).expect("target");
    std::fs::write(repo.join("target/debug/big.rlib"), vec![0_u8; 50_000]).expect("an artifact");
    let work = harness::parents_commit(&repo);
    let policy = horde_worker::workspace::policy::WorkspacePolicy::default();
    let record = manager.records("ws-1").await.remove(0);

    let event = soft(&manager, &record, &policy).await;
    let action = &event.repos[0];
    assert_eq!(action.outcome, i32::from(Outcome::Done), "{action:?}");
    assert_eq!(action.argv, vec!["cargo", "clean"]);
    assert!(action.bytes_freed >= 50_000, "{action:?}");
    assert!(!repo.join("target").exists(), "target/ is gone");
    assert_eq!(head(&repo), work, "the commit stays");
    assert!(repo.join("src/lib.rs").exists(), "the worktree stays");
    assert!(
        manager.records("ws-1").await[0]
            .soft_reclaimed_at_ms
            .is_some()
    );

    std::fs::create_dir_all(repo.join("target/debug")).expect("rebuilt");
    let archived = horde_worker::workspace::cleanup::mark_archived(&manager, "ws-1", "agent-1")
        .await
        .expect("archived");
    let (event, removed) = horde_worker::workspace::cleanup::hard_remove(
        &manager,
        "ws-1",
        &archived,
        explicit(&policy),
        false,
    )
    .await;
    assert!(
        removed,
        "target/ is the reclaim's output, not loss: {event:?}"
    );
}

/// A repo whose reclaim is disabled is skipped with the reason, and an
/// ignored file outside every reclaim output makes removal unprovable —
/// listed with its size — until forced (ADR-0014 resolution 2).
#[tokio::test]
async fn an_ignored_file_outside_the_reclaim_outputs_blocks_automatic_removal() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let origin = crate_origin(temp.path());
    std::fs::write(origin.join(".gitignore"), "/target\n.env\n").expect("ignores");
    harness::git(&origin, &["commit", "-qam", "ignore .env"]);
    let manager = harness::manager_under(temp.path());
    let mut definition = harness::definition("ws-1", vec![harness::repo("crate", &origin)]);
    manager.materialize(&definition).await;
    let prepared = checkout(&manager, &request("agent-1")).await;
    std::fs::write(prepared.root.join("crate/.env"), "SECRET=1").expect("an agent's .env");
    let policy = horde_worker::workspace::policy::WorkspacePolicy::default();
    let archived = horde_worker::workspace::cleanup::mark_archived(&manager, "ws-1", "agent-1")
        .await
        .expect("archived");

    let dry = horde_worker::workspace::cleanup::Pass {
        dry_run: true,
        ..explicit(&policy)
    };
    let (event, removed) =
        horde_worker::workspace::cleanup::hard_remove(&manager, "ws-1", &archived, dry, false)
            .await;
    assert!(!removed);
    let action = &event.repos[0];
    assert_eq!(action.outcome, i32::from(Outcome::Refused));
    assert_eq!(action.unrecoverable_files.len(), 1, "{action:?}");
    assert_eq!(action.unrecoverable_files[0].path, ".env");
    assert_eq!(action.unrecoverable_files[0].bytes, 8);
    let (event, removed) = horde_worker::workspace::cleanup::hard_remove(
        &manager,
        "ws-1",
        &archived,
        explicit(&policy),
        true,
    )
    .await;
    assert!(removed, "{event:?}");

    definition.repos[0].reclaim = Some(horde_proto::v1::ReclaimPolicy {
        policy: Some(horde_proto::v1::reclaim_policy::Policy::Disabled(
            horde_proto::v1::reclaim_policy::Disabled {},
        )),
    });
    manager.materialize(&definition).await;
    checkout(&manager, &request("agent-2")).await;
    let record = manager.records("ws-1").await.remove(0);
    let event = soft(&manager, &record, &policy).await;
    assert_eq!(event.repos[0].outcome, i32::from(Outcome::Skipped));
    assert_eq!(event.repos[0].reason, "no reclaim command");
}

/// The one-repo `atlas` Definition over a fresh origin under `temp`.
fn atlas_definition(temp: &Path) -> WorkspaceDefinition {
    let atlas = harness::origin(temp, "atlas");
    harness::definition("ws-1", vec![harness::repo("atlas", &atlas)])
}

/// One explicit soft reclaim of `record`'s checkout in `ws-1`.
async fn soft(
    manager: &WorkspaceManager,
    record: &horde_worker::workspace::ledger::LedgerRecord,
    policy: &horde_worker::workspace::policy::WorkspacePolicy,
) -> WorkspaceCleanup {
    horde_worker::workspace::cleanup::soft_reclaim(manager, "ws-1", record, explicit(policy)).await
}

async fn spawn(rig: &mut Rig, agent: &str) {
    spawn_in(rig, "ws-1", agent).await;
}

async fn gc(
    rig: &mut Rig,
    id: &str,
    level: GcLevel,
    force: bool,
    dry_run: bool,
    include_pins: bool,
) {
    rig.runtime
        .apply(command(
            id,
            Command::WorkspaceGc(horde_proto::v1::WorkspaceGcCommand {
                request_id: id.to_owned(),
                level: level.into(),
                force,
                dry_run,
                include_pins,
                ..horde_proto::v1::WorkspaceGcCommand::default()
            }),
        ))
        .await
        .expect("the gc applies");
    settled_within("the gc", rig.runtime.settled()).await;
}

/// Waits for an archive's release to finish: it runs once the pump has
/// stopped, after the archive command itself completed, and journals the
/// archive's soft reclaim when done.
async fn released(rig: &Rig, agent: &str) {
    observed_within("the archive's release", async {
        while !cleanups(&rig.journal).await.iter().any(|c| {
            c.trigger == i32::from(CleanupTrigger::Archive)
                && c.agent_id
                    .as_ref()
                    .is_some_and(|id| id.to_string() == agent)
        }) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
}

fn agent_root(rig: &Rig, agent: &str) -> PathBuf {
    rig.root.join("ws-1").join(".horde-agents").join(agent)
}

/// Archive with a grace: soft reclaim at once, the checkout kept; the scan
/// after the grace removes it, losslessly, and the Server hears of it.
#[tokio::test]
async fn an_archived_checkout_is_kept_through_its_grace_then_removed() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let definition = atlas_definition(temp.path());
    let mut rig = fresh_rig(temp.path());
    materialize(&mut rig, &definition, policy(1, 0)).await;
    assert!(rig.reporter.count() >= 1, "a materialize reports");
    spawn(&mut rig, "agent-1").await;
    archive(&mut rig, "agent-1").await;
    observed_within("the archive's release", async {
        while cleanups(&rig.journal).await.is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    let events = cleanups(&rig.journal).await;
    assert_eq!(
        events.len(),
        1,
        "a soft reclaim only, inside the grace: {events:?}"
    );
    assert_eq!(events[0].tier, i32::from(CleanupTier::Soft));
    assert_eq!(events[0].trigger, i32::from(CleanupTrigger::Archive));
    assert!(agent_root(&rig, "agent-1").exists());
    // The archive reports after it journals; wait for the report too.
    observed_within("the archive's report", async {
        while !rig.reporter.last().first().is_some_and(|report| {
            report
                .checkouts
                .first()
                .is_some_and(|c| c.state == i32::from(CheckoutState::ArchivedRetained))
        }) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(rig.runtime.scan_workspaces().await);
    assert!(
        !agent_root(&rig, "agent-1").exists(),
        "removed after the grace"
    );
    let events = cleanups(&rig.journal).await;
    let hard = events.last().expect("a removal");
    assert_eq!(hard.tier, i32::from(CleanupTier::Hard));
    assert_eq!(hard.trigger, i32::from(CleanupTrigger::Scan));
    assert!(rig.reporter.last()[0].checkouts.is_empty());
    rig.runtime.shutdown().await;
}

/// An agent idle past its policy is soft-reclaimed once per idle period:
/// a second scan does nothing, and a turn re-arms it.
#[tokio::test]
async fn an_idle_agent_is_reclaimed_once_per_idle_period() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let definition = atlas_definition(temp.path());
    let mut rig = fresh_rig(temp.path());
    materialize(&mut rig, &definition, policy(0, 1)).await;
    spawn(&mut rig, "agent-1").await;
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    assert!(rig.runtime.scan_workspaces().await);
    let idle: Vec<_> = cleanups(&rig.journal)
        .await
        .into_iter()
        .filter(|c| c.trigger == i32::from(CleanupTrigger::Idle))
        .collect();
    assert_eq!(idle.len(), 1);
    assert_eq!(
        rig.reporter.last()[0].checkouts[0].state,
        i32::from(CheckoutState::Idle)
    );
    assert!(
        !rig.runtime.scan_workspaces().await,
        "not again in the same idle period"
    );

    let activity = rig.runtime.activity();
    activity.set_busy("agent-1", true);
    assert!(
        !rig.runtime.scan_workspaces().await,
        "never under a turn in flight"
    );
    activity.set_busy("agent-1", false);
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(rig.runtime.scan_workspaces().await, "a turn re-arms it");
    rig.runtime.shutdown().await;
}

/// Disk pressure: oldest first, archived before live, stopping at the high
/// watermark; and with nothing left to reclaim, one exhausted event.
#[tokio::test]
async fn disk_pressure_reclaims_oldest_first_and_reports_exhaustion_once() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let definition = atlas_definition(temp.path());
    let mut rig = fresh_rig(temp.path());
    // A long grace and no idle reclaim: only pressure acts.
    materialize(&mut rig, &definition, policy(86_400, 0)).await;
    spawn(&mut rig, "agent-old").await;
    archive(&mut rig, "agent-old").await;
    released(&rig, "agent-old").await;
    spawn(&mut rig, "agent-live").await;
    let before = cleanups(&rig.journal).await.len();

    rig.disk.set_free(1);
    rig.runtime.scan_workspaces().await;
    let pressure: Vec<WorkspaceCleanup> = cleanups(&rig.journal)
        .await
        .into_iter()
        .skip(before)
        .filter(|c| c.trigger == i32::from(CleanupTrigger::DiskPressure))
        .collect();
    let order: Vec<String> = pressure
        .iter()
        .filter(|c| c.tier == i32::from(CleanupTier::Soft))
        .filter_map(|c| c.agent_id.as_ref().map(ToString::to_string))
        .collect();
    assert_eq!(
        order,
        vec!["agent-live"],
        "the archived one was reclaimed at archive already"
    );
    let removed: Vec<String> = pressure
        .iter()
        .filter(|c| c.tier == i32::from(CleanupTier::Hard))
        .filter_map(|c| c.agent_id.as_ref().map(ToString::to_string))
        .collect();
    assert_eq!(
        removed,
        vec!["agent-old"],
        "a lossless archived checkout goes, grace or not"
    );
    let exhausted: Vec<_> = kinds(&rig.journal)
        .await
        .into_iter()
        .filter(|kind| matches!(kind, Kind::WorkspaceDiskPressure(p) if p.exhausted))
        .collect();
    assert_eq!(exhausted.len(), 1);
    assert!(!agent_root(&rig, "agent-old").exists());
    assert!(
        agent_root(&rig, "agent-live").exists(),
        "a live checkout is never removed"
    );
    let before = kinds(&rig.journal).await.len();
    rig.runtime.scan_workspaces().await;
    let repeated = kinds(&rig.journal)
        .await
        .into_iter()
        .skip(before)
        .filter(|kind| matches!(kind, Kind::WorkspaceDiskPressure(_)))
        .count();
    assert_eq!(repeated, 0, "one exhausted event per episode");
    rig.runtime.shutdown().await;
}

/// After a worker reset (a fresh journal over the same disk), every old
/// checkout is an orphan: reported as such, soft-reclaimed once, never
/// removed by a scan — and removed by `gc --hard` when lossless.
#[tokio::test]
async fn orphans_after_a_reset_are_reported_soft_at_most_and_removed_by_hard_gc() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let definition = atlas_definition(temp.path());
    let root = temp.path().join("workspaces");
    let mut first = rig(&root, Journal::in_memory().expect("journal"));
    materialize(&mut first, &definition, policy(0, 0)).await;
    spawn(&mut first, "agent-1").await;
    first.runtime.shutdown().await;

    let mut reset = rig(&root, Journal::in_memory().expect("a fresh journal"));
    assert!(reset.runtime.scan_workspaces().await);
    let reported = reset.reporter.last();
    assert_eq!(
        reported[0].checkouts[0].state,
        i32::from(CheckoutState::Orphan)
    );
    let events = cleanups(&reset.journal).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].trigger, i32::from(CleanupTrigger::Scan));
    assert_eq!(
        events[0]
            .session_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("s1")
    );
    assert!(!reset.runtime.scan_workspaces().await, "reported once");
    assert!(
        agent_root(&reset, "agent-1").exists(),
        "a scan never removes an orphan"
    );

    gc(
        &mut reset,
        "gc-auto",
        GcLevel::Automatic,
        false,
        false,
        false,
    )
    .await;
    assert!(
        agent_root(&reset, "agent-1").exists(),
        "nor does an automatic gc"
    );
    gc(&mut reset, "gc-hard", GcLevel::Hard, false, false, false).await;
    assert!(
        !agent_root(&reset, "agent-1").exists(),
        "a hard gc removes a lossless orphan"
    );
    let completed = kinds(&reset.journal)
        .await
        .into_iter()
        .filter(|kind| matches!(kind, Kind::WorkspaceGcCompleted(c) if c.request_id == "gc-hard"))
        .count();
    assert_eq!(completed, 1);
    reset.runtime.shutdown().await;
}

/// gc: a dry run journals what it would do and changes nothing on disk; a
/// hard gc refuses a dirty archived checkout; forced, it removes it; and
/// `--include-pins` drops only the pins whose commits origin holds.
#[tokio::test]
async fn gc_dry_runs_refuses_forces_and_drops_only_safe_pins() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let definition = atlas_definition(temp.path());
    let mut rig = fresh_rig(temp.path());
    materialize(&mut rig, &definition, policy(86_400, 0)).await;
    spawn(&mut rig, "agent-dirty").await;
    std::fs::write(agent_root(&rig, "agent-dirty").join("atlas/wip.txt"), "wip").expect("dirty");
    archive(&mut rig, "agent-dirty").await;
    released(&rig, "agent-dirty").await;

    let listing = |root: &Path| {
        let mut files: Vec<String> = walk(root);
        files.sort();
        files
    };
    let before = listing(&agent_root(&rig, "agent-dirty"));
    gc(&mut rig, "gc-dry", GcLevel::Hard, true, true, false).await;
    assert_eq!(
        listing(&agent_root(&rig, "agent-dirty")),
        before,
        "a dry run changes nothing"
    );
    let dry: Vec<_> = cleanups(&rig.journal)
        .await
        .into_iter()
        .filter(|c| c.gc_request_id == "gc-dry")
        .collect();
    assert!(dry.iter().all(|c| c.dry_run));
    assert!(
        dry.iter()
            .flat_map(|c| &c.repos)
            .any(|a| a.outcome == i32::from(Outcome::WouldDo))
    );

    gc(&mut rig, "gc-hard", GcLevel::Hard, false, false, false).await;
    assert!(
        agent_root(&rig, "agent-dirty").exists(),
        "not provably lossless: refused"
    );
    let refused = cleanups(&rig.journal)
        .await
        .into_iter()
        .filter(|c| c.gc_request_id == "gc-hard");
    assert!(
        refused
            .flat_map(|c| c.repos)
            .any(|a| a.outcome == i32::from(Outcome::Refused) && a.reason.contains("untracked"))
    );

    gc(&mut rig, "gc-force", GcLevel::Hard, true, false, false).await;
    assert!(!agent_root(&rig, "agent-dirty").exists(), "forced");
    let instance_repo = rig.root.join("ws-1").join("atlas");
    let pinned = harness::git(
        &instance_repo,
        &["rev-parse", "refs/horde/archived/agent-dirty"],
    );
    // The pinned commit is the clone point, which origin holds: droppable.
    harness::git(
        &instance_repo,
        &[
            "update-ref",
            "refs/horde/archived/unpushed",
            &make_unpushed(&instance_repo),
        ],
    );
    gc(&mut rig, "gc-pins", GcLevel::Hard, false, false, true).await;
    let pins = harness::git(
        &instance_repo,
        &["for-each-ref", "--format=%(refname)", "refs/horde/archived"],
    );
    assert!(
        !pins.contains("agent-dirty"),
        "a pin origin holds goes: {pins} (was {})",
        pinned.trim()
    );
    assert!(
        pins.contains("unpushed"),
        "a pin nobody else holds stays: {pins}"
    );
    rig.runtime.shutdown().await;
}

/// A commit only the Instance has: a dangling commit object, as a squash
/// merge leaves an archived agent's work.
fn make_unpushed(repo: &Path) -> String {
    let tree = harness::git(repo, &["rev-parse", "HEAD^{tree}"]);
    let output = std::process::Command::new("git")
        .args(["commit-tree", tree.trim(), "-p", "HEAD", "-m", "unpushed"])
        .env("GIT_AUTHOR_NAME", "h")
        .env("GIT_AUTHOR_EMAIL", "h@t")
        .env("GIT_COMMITTER_NAME", "h")
        .env("GIT_COMMITTER_EMAIL", "h@t")
        .current_dir(repo)
        .output()
        .expect("git runs");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn walk(root: &Path) -> Vec<String> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        if let Ok(entries) = std::fs::read_dir(&path) {
            for entry in entries.flatten() {
                let path = entry.path();
                let modified = entry.metadata().ok().and_then(|m| m.modified().ok());
                files.push(format!("{} {modified:?}", path.display()));
                if path.is_dir() && !path.is_symlink() {
                    pending.push(path);
                }
            }
        }
    }
    files
}

/// Instance removal: refused while an agent lives there; refused with
/// non-empty unmanaged data unless forced; then the Instance is gone and
/// reported MISSING.
#[tokio::test]
async fn an_instance_is_removed_only_without_live_agents_and_with_unmanaged_data_forced() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let atlas = harness::origin(temp.path(), "atlas");
    let mut definition = harness::definition("ws-1", vec![harness::repo("atlas", &atlas)]);
    definition.unmanaged_paths = vec!["data".to_owned()];
    let mut rig = fresh_rig(temp.path());
    materialize(&mut rig, &definition, policy(0, 0)).await;
    spawn(&mut rig, "agent-1").await;
    let link = agent_root(&rig, "agent-1").join("data");
    assert!(
        link.is_symlink(),
        "the unmanaged path is linked into the checkout"
    );
    std::fs::write(link.join("weights.bin"), vec![0_u8; 64]).expect("data through the link");
    assert!(
        rig.root.join("ws-1/data/weights.bin").exists(),
        "it lands in the instance"
    );

    let remove = |id: &str, force: bool| {
        command(
            id,
            Command::WorkspaceGc(horde_proto::v1::WorkspaceGcCommand {
                request_id: id.to_owned(),
                workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new("ws-1")),
                level: GcLevel::Hard.into(),
                force,
                remove_instance: true,
                ..horde_proto::v1::WorkspaceGcCommand::default()
            }),
        )
    };
    rig.runtime
        .apply(remove("rm-live", true))
        .await
        .expect("applies");
    settled_within("the removal", rig.runtime.settled()).await;
    assert!(
        rig.root.join("ws-1").exists(),
        "refused with a live agent, even forced"
    );

    archive(&mut rig, "agent-1").await;
    observed_within("the archive's removal", async {
        while agent_root(&rig, "agent-1").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        rig.root.join("ws-1/data/weights.bin").exists(),
        "removing a checkout leaves the data"
    );
    rig.runtime
        .apply(remove("rm-data", false))
        .await
        .expect("applies");
    settled_within("the removal", rig.runtime.settled()).await;
    assert!(
        rig.root.join("ws-1").exists(),
        "non-empty unmanaged data needs --force"
    );
    let refused = cleanups(&rig.journal)
        .await
        .into_iter()
        .filter(|c| c.gc_request_id == "rm-data");
    assert!(refused.flat_map(|c| c.repos).any(|a| {
        a.unrecoverable_files
            .iter()
            .any(|f| f.path == "data" && f.bytes == 64)
    }));

    rig.runtime
        .apply(remove("rm-force", true))
        .await
        .expect("applies");
    settled_within("the removal", rig.runtime.settled()).await;
    assert!(!rig.root.join("ws-1").exists(), "forced");
    let missing = kinds(&rig.journal).await.into_iter().any(|kind| {
        matches!(kind, Kind::WorkspaceInstanceChanged(c)
            if c.report.as_ref().is_some_and(|r| r.state == i32::from(horde_proto::v1::WorkspaceInstanceState::Missing)))
    });
    assert!(missing);
    assert!(
        rig.reporter.last().is_empty(),
        "the authoritative set is empty now"
    );
    rig.runtime.shutdown().await;
}

/// A three-repo materialize journals one progress step per repo before its
/// final report, in order, and reports to the Server afterwards.
#[tokio::test]
async fn materialization_journals_its_progress_and_reports() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = three_repo_rig(temp.path()).await;
    let steps: Vec<(String, u32, u32)> = kinds(&rig.journal)
        .await
        .into_iter()
        .filter_map(|kind| match kind {
            Kind::WorkspaceInstanceChanged(changed) => changed
                .progress
                .map(|p| (p.step, p.completed_repos, p.total_repos)),
            _ => None,
        })
        .collect();
    assert_eq!(
        steps,
        vec![
            ("cloning a".to_owned(), 0, 3),
            ("cloning b".to_owned(), 1, 3),
            ("cloning c".to_owned(), 2, 3),
        ]
    );
    let reported = rig.reporter.last();
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].repos.len(), 3);
    assert!(
        reported[0]
            .repos
            .iter()
            .all(|r| r.head_commit_sha.len() == 40),
        "real HEADs now"
    );
    assert!(
        reported[0].filesystem_total_bytes > 0,
        "disk facts ride the report"
    );
    rig.runtime.shutdown().await;
}
