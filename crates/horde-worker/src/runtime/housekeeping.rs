//! Workspace housekeeping on this Worker: the archive trigger, the gc
//! command, and the periodic scan that reclaims idle and orphaned checkouts,
//! removes archived ones after their grace, reacts to disk pressure, drops
//! pinned commits that are safe elsewhere, and re-reports the Instances
//! (ADR-0014).
//!
//! Cleanup events go to the reserved housekeeping stream, never an agent's
//! Session: a cleanup is the Worker's act on its own disk, and an orphan's
//! Session may no longer exist anywhere.
//!
//! # Never under a running agent
//!
//! Every action on one agent's checkout takes that agent's checkout lock —
//! the same one its spawn holds from checkout to registration and its
//! `workspace_checkout` holds while adding a repo — and a soft reclaim is
//! skipped while the agent has a turn in flight. Hard removal never touches
//! a live agent's checkout at all, not even forced.

use std::sync::Arc;
use std::time::Duration;

use horde_proto::v1::event::Kind;
use horde_proto::v1::{
    CheckoutState, CleanupTier, CleanupTrigger, GcLevel, WorkerId, WorkspaceCleanup,
    WorkspaceDiskPressure, WorkspaceGcCommand, WorkspaceGcCompleted, WorkspaceInstanceChanged,
    WorkspaceInstanceReport,
};

use super::shared::Shared;
use crate::journal::JournalEntry;
use crate::workspace::WorkspaceManager;
use crate::workspace::cleanup::{Pass, tally};
use crate::workspace::ledger::{LedgerRecord, now_ms};
use crate::workspace::policy::WorkspacePolicy;

/// A free-space threshold: a share of the filesystem, an absolute size, or
/// the larger of the two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermark {
    /// Hundredths of a percent of the total (1000 = 10 %).
    pub basis_points: u64,
    pub bytes: u64,
}

impl Watermark {
    /// The threshold in bytes on a filesystem of `total` bytes.
    #[must_use]
    pub fn bytes_of(&self, total: u64) -> u64 {
        let share = u64::try_from(u128::from(total) * u128::from(self.basis_points) / 10_000)
            .unwrap_or(u64::MAX);
        share.max(self.bytes)
    }
}

/// The machine-local half of the workspace configuration (ADR-0014's
/// `worker.workspace.*` keys; flags until W12e's config file).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HousekeepingConfig {
    pub scan_interval: Duration,
    pub pin_scan_interval: Duration,
    pub report_interval: Duration,
    /// Default the larger of 10 % and 20 GiB.
    pub disk_low_watermark: Watermark,
    /// Default 15 %; must exceed the low watermark.
    pub disk_high_watermark: Watermark,
}

impl Default for HousekeepingConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(60),
            pin_scan_interval: Duration::from_secs(6 * 3600),
            report_interval: Duration::from_secs(300),
            disk_low_watermark: Watermark {
                basis_points: 1_000,
                bytes: 20 << 30,
            },
            disk_high_watermark: Watermark {
                basis_points: 1_500,
                bytes: 0,
            },
        }
    }
}

/// The housekeeper over a Runtime's shared state, when it has workspaces.
pub(super) fn of(shared: &Shared) -> Option<Housekeeper> {
    shared
        .workspaces
        .clone()
        .map(|workspaces| Housekeeper::new(shared.clone(), workspaces))
}

/// Gives back what a spawn minted for an agent whose harness never
/// started: the endpoint token, its `local` MCP children, and the checkout
/// — nothing ran in it, so it goes at once when lossless (ADR-0014).
pub(super) async fn abandon_spawn(shared: &Shared, agent_id: &str, root: Option<&std::path::Path>) {
    shared.forget_endpoint(agent_id).await;
    if let (Some(root), Some(housekeeper)) = (root, of(shared)) {
        housekeeper.archived(agent_id, root, true).await;
    }
}

/// An archived agent's checkout: soft reclaim, then hard removal when
/// provably lossless and the grace allows; otherwise retained. Never the
/// old unconditional pin-and-force-remove (ADR-0014).
pub(super) async fn release_checkout(shared: &Shared, agent_id: &str, root: &std::path::Path) {
    if let Some(housekeeper) = of(shared) {
        housekeeper.archived(agent_id, root, false).await;
    }
}

/// How a checkout stands, for the cleanup rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Standing {
    /// Running here: soft reclaim at most, and only between turns.
    Live,
    /// Remembered (revivable) but not running: nothing removes it.
    Remembered,
    Archived,
    /// Neither running, remembered nor archived: left by a reset.
    Orphan,
}

/// The housekeeper: what the scan, archives and gc commands share.
#[derive(Clone)]
pub(super) struct Housekeeper {
    pub(super) shared: Shared,
    pub(super) workspaces: WorkspaceManager,
}

impl Housekeeper {
    pub(super) fn new(shared: Shared, workspaces: WorkspaceManager) -> Self {
        Self { shared, workspaces }
    }

    fn worker_id(&self) -> String {
        self.workspaces.worker_id().to_owned()
    }

    /// The policy last seen for an Instance, or the defaults.
    fn policy(&self, definition_id: &str) -> WorkspacePolicy {
        self.shared.policy_for(definition_id)
    }

    /// Journals housekeeping events, best effort: a cleanup that cannot be
    /// recorded is logged, never a reason to stop cleaning.
    async fn journal(&self, kinds: Vec<(String, Kind)>) {
        for (idempotency_key, kind) in kinds {
            let entry = JournalEntry {
                session_id: horde_proto::WORKSPACE_HOUSEKEEPING_STREAM.to_owned(),
                agent_id: None,
                idempotency_key,
                kind,
            };
            if let Err(error) = self.shared.journal.record(entry).await {
                tracing::warn!(%error, "a workspace cleanup event could not be journaled");
            }
        }
    }

    async fn journal_cleanups(&self, events: Vec<WorkspaceCleanup>) {
        self.journal(
            events
                .into_iter()
                .map(|event| (String::new(), Kind::WorkspaceCleanup(event)))
                .collect(),
        )
        .await;
    }

    /// How one agent's checkout stands.
    pub(super) async fn standing(&self, record: &LedgerRecord) -> Standing {
        if self.shared.broker.checkout_of(&record.agent_id).is_some() {
            return Standing::Live;
        }
        if record.archived_at_ms.is_some() {
            return Standing::Archived;
        }
        match self.shared.journal.revivable_agent(&record.agent_id).await {
            Ok(Some(_)) => Standing::Remembered,
            Ok(None) => Standing::Orphan,
            // Unreadable is not "gone": treat it as remembered, which
            // removes nothing.
            Err(_) => Standing::Remembered,
        }
    }

    /// The archive trigger: soft reclaim, then — with no grace — the
    /// lossless hard removal. Uncommitted work is retained, never discarded.
    /// `abandoned` is a spawn whose harness never started: nothing ran in
    /// its checkout, so the grace does not apply.
    pub(super) async fn archived(&self, agent_id: &str, root: &std::path::Path, abandoned: bool) {
        let Some((definition_id, _)) = self.workspaces.agent_private(root) else {
            return;
        };
        let lock = self.shared.checkout_locks.of(agent_id);
        let _held = lock.lock().await;
        self.shared.activity.forget(agent_id);
        let Some(record) =
            crate::workspace::cleanup::mark_archived(&self.workspaces, &definition_id, agent_id)
                .await
        else {
            return;
        };
        let policy = self.policy(&definition_id);
        let pass = Pass {
            trigger: CleanupTrigger::Archive,
            gc_request_id: "",
            dry_run: false,
            policy: &policy,
        };
        let mut events = Vec::new();
        if !abandoned {
            events.push(
                crate::workspace::cleanup::soft_reclaim(
                    &self.workspaces,
                    &definition_id,
                    &record,
                    pass,
                )
                .await,
            );
        }
        if abandoned || policy.archived_grace.is_zero() {
            let (event, _) = crate::workspace::cleanup::hard_remove(
                &self.workspaces,
                &definition_id,
                &record,
                pass,
                false,
            )
            .await;
            events.push(event);
        }
        self.journal_cleanups(events).await;
        self.report().await;
    }

    /// Carries out one gc command: plan, act (unless a dry run), journal one
    /// event per checkout, pin or Instance, then the completion.
    pub(super) async fn gc(&self, command: &WorkspaceGcCommand) -> Vec<JournalEntry> {
        let policy = WorkspacePolicy::from_wire(command.policy.as_ref());
        let trigger = if command.remove_instance {
            CleanupTrigger::Purge
        } else {
            CleanupTrigger::Explicit
        };
        let pass = Pass {
            trigger,
            gc_request_id: &command.request_id,
            dry_run: command.dry_run,
            policy: &policy,
        };
        let wanted_definition = command
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty());
        let mut events = Vec::new();
        let mut missing = Vec::new();
        for (definition_id, _) in self.workspaces.instances().await {
            if wanted_definition
                .as_ref()
                .is_some_and(|wanted| *wanted != definition_id)
            {
                continue;
            }
            if command.remove_instance {
                let (instance_events, gone) = self
                    .remove_instance(&definition_id, pass, command.force)
                    .await;
                events.extend(instance_events);
                missing.extend(gone);
                continue;
            }
            events.extend(self.gc_instance(&definition_id, command, pass).await);
        }
        let (done, refused, freed) = tally(&events);
        let mut entries: Vec<JournalEntry> = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                housekeeping_entry(
                    format!("gc:{}:{index}", command.request_id),
                    Kind::WorkspaceCleanup(event),
                )
            })
            .collect();
        for report in missing {
            entries.push(housekeeping_entry(
                format!(
                    "gc:{}:missing:{}",
                    command.request_id,
                    report
                        .workspace_definition_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default()
                ),
                Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged {
                    report: Some(report),
                    progress: None,
                }),
            ));
        }
        entries.push(housekeeping_entry(
            format!("gc:{}:completed", command.request_id),
            Kind::WorkspaceGcCompleted(WorkspaceGcCompleted {
                request_id: command.request_id.clone(),
                worker_id: Some(WorkerId::new(self.worker_id())),
                actions_done: done,
                actions_refused: refused,
                bytes_freed: freed,
                error: String::new(),
            }),
        ));
        entries
    }

    /// The gc levels on one Instance's checkouts and pins.
    async fn gc_instance(
        &self,
        definition_id: &str,
        command: &WorkspaceGcCommand,
        pass: Pass<'_>,
    ) -> Vec<WorkspaceCleanup> {
        let level = match GcLevel::try_from(command.level) {
            Ok(GcLevel::Soft) => GcLevel::Soft,
            Ok(GcLevel::Hard) => GcLevel::Hard,
            _ => GcLevel::Automatic,
        };
        let agents: Vec<String> = command.agent_ids.iter().map(ToString::to_string).collect();
        let mut events = Vec::new();
        for record in self.workspaces.records(definition_id).await {
            if !agents.is_empty() && !agents.contains(&record.agent_id) {
                continue;
            }
            let standing = self.standing(&record).await;
            let lock = self.shared.checkout_locks.of(&record.agent_id);
            let _held = lock.lock().await;
            let busy = self.shared.activity.is_busy(&record.agent_id);
            if !busy && record.soft_reclaimed_at_ms.is_none() {
                events.push(
                    crate::workspace::cleanup::soft_reclaim(
                        &self.workspaces,
                        definition_id,
                        &record,
                        pass,
                    )
                    .await,
                );
            }
            let removable = match (level, standing) {
                (GcLevel::Soft, _) | (_, Standing::Live | Standing::Remembered) => false,
                (GcLevel::Automatic, Standing::Archived) | (GcLevel::Hard, _) => true,
                (GcLevel::Automatic, Standing::Orphan) => false,
                _ => false,
            };
            if removable {
                let force = level == GcLevel::Hard && command.force;
                let (event, _) = crate::workspace::cleanup::hard_remove(
                    &self.workspaces,
                    definition_id,
                    &record,
                    pass,
                    force,
                )
                .await;
                events.push(event);
            }
        }
        if level == GcLevel::Hard && command.include_pins {
            let relay = self.shared.relay_repo_url();
            events.extend(
                crate::workspace::cleanup::unpin(
                    &self.workspaces,
                    definition_id,
                    &relay,
                    pass,
                    command.force,
                )
                .await,
            );
        }
        events
    }

    async fn remove_instance(
        &self,
        definition_id: &str,
        pass: Pass<'_>,
        force: bool,
    ) -> (Vec<WorkspaceCleanup>, Option<WorkspaceInstanceReport>) {
        let mut live = Vec::new();
        for record in self.workspaces.records(definition_id).await {
            if matches!(
                self.standing(&record).await,
                Standing::Live | Standing::Remembered
            ) {
                live.push(record.agent_id.clone());
            }
        }
        let relay = self.shared.relay_repo_url();
        crate::workspace::cleanup::remove_instance(
            &self.workspaces,
            definition_id,
            &live,
            &relay,
            pass,
            force,
        )
        .await
    }

    /// One scan pass: idle and orphan reclaim, grace expiry, then disk
    /// pressure. Returns whether anything changed.
    pub(super) async fn scan(&self, config: &HousekeepingConfig, episode: &mut Episode) -> bool {
        let mut changed = false;
        for (definition_id, _) in self.workspaces.instances().await {
            for record in self.workspaces.records(&definition_id).await {
                changed |= self.scan_checkout(&definition_id, &record).await;
            }
        }
        changed |= self.relieve_pressure(config, episode).await;
        changed
    }

    /// The scan's rules for one checkout.
    async fn scan_checkout(&self, definition_id: &str, record: &LedgerRecord) -> bool {
        let lock = self.shared.checkout_locks.of(&record.agent_id);
        // A checkout someone is working on is skipped this pass, not waited
        // for: the scan has every other checkout to get through.
        let Ok(_held) = lock.try_lock() else {
            return false;
        };
        let policy = self.policy(definition_id);
        match self.standing(record).await {
            Standing::Live => self.scan_live(definition_id, record, &policy).await,
            Standing::Archived => self.scan_archived(definition_id, record, &policy).await,
            Standing::Orphan if !record.orphan_reported => {
                self.soft(definition_id, record, CleanupTrigger::Scan).await;
                true
            }
            Standing::Orphan | Standing::Remembered => false,
        }
    }

    /// A live agent: write activity through, and soft-reclaim once per idle
    /// period past the policy's idle time.
    async fn scan_live(
        &self,
        definition_id: &str,
        record: &LedgerRecord,
        policy: &WorkspacePolicy,
    ) -> bool {
        let activity = &self.shared.activity;
        if let Some(at) = activity.due_for_write(&record.agent_id) {
            crate::workspace::cleanup::note_activity(
                &self.workspaces,
                definition_id,
                &record.agent_id,
                at,
            )
            .await;
        }
        let Some(after) = policy.idle_reclaim_after else {
            return false;
        };
        let seen = activity.snapshot(&record.agent_id);
        if seen.is_some_and(|seen| seen.busy || seen.idle_reclaimed) {
            return false;
        }
        let last_active = seen
            .map(|seen| seen.last_active_ms)
            .unwrap_or(record.last_active_at_ms)
            .max(record.last_active_at_ms);
        let idle_for = now_ms().saturating_sub(last_active);
        if u128::try_from(idle_for).unwrap_or(0) < after.as_millis() {
            return false;
        }
        activity.mark_idle_reclaimed(&record.agent_id);
        if seen.is_none() {
            // Seen for the first time already idle (a revival's checkout,
            // a restarted Worker): remember the flag from here on.
            activity.set_busy(&record.agent_id, false);
            activity.mark_idle_reclaimed(&record.agent_id);
        }
        let pass = Pass {
            trigger: CleanupTrigger::Idle,
            gc_request_id: "",
            dry_run: false,
            policy,
        };
        let event =
            crate::workspace::cleanup::soft_reclaim(&self.workspaces, definition_id, record, pass)
                .await;
        self.journal_cleanups(vec![event]).await;
        true
    }

    /// An archived checkout past its grace: the lossless hard removal. A
    /// refusal is journaled only when its reason changed, so a retained
    /// checkout costs one event, not one a minute.
    async fn scan_archived(
        &self,
        definition_id: &str,
        record: &LedgerRecord,
        policy: &WorkspacePolicy,
    ) -> bool {
        let Some(archived_at) = record.archived_at_ms else {
            return false;
        };
        let grace_ms = i64::try_from(policy.archived_grace.as_millis()).unwrap_or(i64::MAX);
        if now_ms().saturating_sub(archived_at) < grace_ms {
            return false;
        }
        let pass = Pass {
            trigger: CleanupTrigger::Scan,
            gc_request_id: "",
            dry_run: false,
            policy,
        };
        let (event, removed) = crate::workspace::cleanup::hard_remove(
            &self.workspaces,
            definition_id,
            record,
            pass,
            false,
        )
        .await;
        let reason_now = self
            .workspaces
            .records(definition_id)
            .await
            .into_iter()
            .find(|current| current.agent_id == record.agent_id)
            .map(|current| current.retained_reason)
            .unwrap_or_default();
        if removed || reason_now != record.retained_reason {
            self.journal_cleanups(vec![event]).await;
            return true;
        }
        false
    }

    /// Disk pressure: oldest-first soft reclaim until the high watermark,
    /// then lossless removal of archived checkouts, then — still below the
    /// low watermark — one exhausted event per episode. Never forced, never
    /// a busy agent.
    async fn relieve_pressure(&self, config: &HousekeepingConfig, episode: &mut Episode) -> bool {
        let Some((free, total)) = self.workspaces.filesystem() else {
            return false;
        };
        let low = config.disk_low_watermark.bytes_of(total);
        let high = config.disk_high_watermark.bytes_of(total).max(low);
        if free >= high {
            episode.end();
        }
        if free >= low && !episode.active {
            return false;
        }
        episode.active = true;
        let mut changed = false;
        let candidates = self.pressure_candidates().await;
        for (definition_id, record) in &candidates {
            if self.relieved(high) {
                return changed;
            }
            let lock = self.shared.checkout_locks.of(&record.agent_id);
            let Ok(_held) = lock.try_lock() else {
                continue;
            };
            if self.shared.activity.is_busy(&record.agent_id) {
                continue;
            }
            self.soft(definition_id, record, CleanupTrigger::DiskPressure)
                .await;
            changed = true;
        }
        for (definition_id, record) in self.archived_oldest_first().await {
            if self.relieved(high) {
                return changed;
            }
            let lock = self.shared.checkout_locks.of(&record.agent_id);
            let Ok(_held) = lock.try_lock() else {
                continue;
            };
            let policy = self.policy(&definition_id);
            let pass = Pass {
                trigger: CleanupTrigger::DiskPressure,
                gc_request_id: "",
                dry_run: false,
                policy: &policy,
            };
            let (event, removed) = crate::workspace::cleanup::hard_remove(
                &self.workspaces,
                &definition_id,
                &record,
                pass,
                false,
            )
            .await;
            if removed {
                self.journal_cleanups(vec![event]).await;
                changed = true;
            }
        }
        let free_now = self.workspaces.filesystem().map_or(free, |(free, _)| free);
        if free_now < low && !episode.reported {
            episode.reported = true;
            self.journal(vec![(
                String::new(),
                Kind::WorkspaceDiskPressure(WorkspaceDiskPressure {
                    worker_id: Some(WorkerId::new(self.worker_id())),
                    free_bytes: free_now,
                    low_watermark_bytes: low,
                    exhausted: true,
                }),
            )])
            .await;
            changed = true;
        }
        changed
    }

    /// The checkouts disk pressure may soft-reclaim, oldest first: archived
    /// by archive time, then orphans, then idle live agents by last
    /// activity. Busy agents and checkouts already reclaimed since their
    /// last activity are not candidates.
    pub(super) async fn pressure_candidates(&self) -> Vec<(String, LedgerRecord)> {
        let mut archived = Vec::new();
        let mut orphans = Vec::new();
        let mut idle = Vec::new();
        for (definition_id, _) in self.workspaces.instances().await {
            for record in self.workspaces.records(&definition_id).await {
                if record.soft_reclaimed_at_ms.is_some()
                    || self.shared.activity.is_busy(&record.agent_id)
                {
                    continue;
                }
                match self.standing(&record).await {
                    Standing::Archived => archived.push((definition_id.clone(), record)),
                    Standing::Orphan => orphans.push((definition_id.clone(), record)),
                    Standing::Live => idle.push((definition_id.clone(), record)),
                    Standing::Remembered => {}
                }
            }
        }
        archived.sort_by_key(|(_, record)| record.archived_at_ms.unwrap_or(0));
        orphans.sort_by_key(|(_, record)| record.last_active_at_ms);
        idle.sort_by_key(|(_, record)| record.last_active_at_ms);
        archived.into_iter().chain(orphans).chain(idle).collect()
    }

    /// Whether free space has reached `high` bytes.
    fn relieved(&self, high: u64) -> bool {
        self.workspaces
            .filesystem()
            .is_some_and(|(free, _)| free >= high)
    }

    /// One soft reclaim under `trigger` and its policy, journaled.
    async fn soft(&self, definition_id: &str, record: &LedgerRecord, trigger: CleanupTrigger) {
        let policy = self.policy(definition_id);
        let pass = Pass {
            trigger,
            gc_request_id: "",
            dry_run: false,
            policy: &policy,
        };
        let event =
            crate::workspace::cleanup::soft_reclaim(&self.workspaces, definition_id, record, pass)
                .await;
        self.journal_cleanups(vec![event]).await;
    }

    /// Every archived checkout, oldest archive first: what disk pressure
    /// removes when lossless, after its soft reclaims.
    async fn archived_oldest_first(&self) -> Vec<(String, LedgerRecord)> {
        let mut archived = Vec::new();
        for (definition_id, _) in self.workspaces.instances().await {
            for record in self.workspaces.records(&definition_id).await {
                if record.archived_at_ms.is_some()
                    && self.shared.broker.checkout_of(&record.agent_id).is_none()
                {
                    archived.push((definition_id.clone(), record));
                }
            }
        }
        archived.sort_by_key(|(_, record)| record.archived_at_ms.unwrap_or(0));
        archived
    }

    /// Drops every pin provably safe elsewhere (lossless only).
    pub(super) async fn unpin_pass(&self) -> bool {
        let relay = self.shared.relay_repo_url();
        let mut events = Vec::new();
        for (definition_id, _) in self.workspaces.instances().await {
            let policy = self.policy(&definition_id);
            let pass = Pass {
                trigger: CleanupTrigger::Scan,
                gc_request_id: "",
                dry_run: false,
                policy: &policy,
            };
            events.extend(
                crate::workspace::cleanup::unpin(
                    &self.workspaces,
                    &definition_id,
                    &relay,
                    pass,
                    false,
                )
                .await
                .into_iter()
                .filter(|event| event.tier == i32::from(CleanupTier::Unpin))
                .filter(|event| {
                    event.repos.iter().any(|action| {
                        action.outcome
                            == i32::from(
                                horde_proto::v1::workspace_cleanup::repo_action::Outcome::Done,
                            )
                    })
                }),
            );
        }
        let changed = !events.is_empty();
        self.journal_cleanups(events).await;
        changed
    }

    /// Sends the whole Instance set to the Server, when a reporter is set.
    pub(super) async fn report(&self) {
        let Some(reporter) = self.shared.reporter.clone() else {
            return;
        };
        let reports = self.reports().await;
        reporter.report(reports).await;
    }

    /// Every Instance with its checkouts classified by what the Runtime
    /// knows now: a live agent is ACTIVE, or IDLE once it was reclaimed for
    /// idleness; anything else the ledger classifies (archived, orphan).
    pub(super) async fn reports(&self) -> Vec<WorkspaceInstanceReport> {
        let live: Vec<String> = self.shared.broker.agents();
        let idle: Vec<String> = live
            .iter()
            .filter(|agent| {
                self.shared
                    .activity
                    .snapshot(agent)
                    .is_some_and(|seen| !seen.busy && seen.idle_reclaimed)
            })
            .cloned()
            .collect();
        self.workspaces
            .instance_reports(&|record: &LedgerRecord| {
                if idle.contains(&record.agent_id) {
                    Some(CheckoutState::Idle)
                } else if live.contains(&record.agent_id) {
                    Some(CheckoutState::Active)
                } else {
                    None
                }
            })
            .await
    }
}

/// One disk-pressure episode's state across scans.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Episode {
    pub(super) active: bool,
    /// The exhausted event was journaled for this episode.
    pub(super) reported: bool,
}

impl Episode {
    fn end(&mut self) {
        *self = Self::default();
    }
}

fn housekeeping_entry(idempotency_key: String, kind: Kind) -> JournalEntry {
    JournalEntry {
        session_id: horde_proto::WORKSPACE_HOUSEKEEPING_STREAM.to_owned(),
        agent_id: None,
        idempotency_key,
        kind,
    }
}

/// Runs the scan loop until the Runtime drops its handle: every
/// `scan_interval` a scan, every `pin_scan_interval` a pin pass, and a
/// report after any change or every `report_interval`.
pub(super) async fn run(
    housekeeper: Housekeeper,
    config: HousekeepingConfig,
    episode: Arc<tokio::sync::Mutex<Episode>>,
) {
    let mut last_pins = tokio::time::Instant::now();
    let mut last_report = tokio::time::Instant::now();
    let mut ticker = tokio::time::interval(config.scan_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let mut changed = {
            let mut episode = episode.lock().await;
            housekeeper.scan(&config, &mut episode).await
        };
        if last_pins.elapsed() >= config.pin_scan_interval {
            last_pins = tokio::time::Instant::now();
            changed |= housekeeper.unpin_pass().await;
        }
        if changed || last_report.elapsed() >= config.report_interval {
            last_report = tokio::time::Instant::now();
            housekeeper.report().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_watermark_is_the_larger_of_its_share_and_its_floor() {
        let mark = Watermark {
            basis_points: 1_000,
            bytes: 20 << 30,
        };
        assert_eq!(
            mark.bytes_of(100 << 30),
            20 << 30,
            "10% of 100 GiB is under the 20 GiB floor"
        );
        assert_eq!(
            mark.bytes_of(1 << 40),
            (1 << 40) / 10,
            "10% of 1 TiB is over it"
        );
        let share_only = Watermark {
            basis_points: 1_500,
            bytes: 0,
        };
        assert_eq!(share_only.bytes_of(1_000), 150);
    }

    #[test]
    fn the_default_config_matches_the_adr() {
        let config = HousekeepingConfig::default();
        assert_eq!(config.scan_interval, Duration::from_secs(60));
        assert_eq!(config.pin_scan_interval, Duration::from_secs(21_600));
        assert_eq!(config.report_interval, Duration::from_secs(300));
        assert_eq!(config.disk_low_watermark.basis_points, 1_000);
        assert_eq!(config.disk_low_watermark.bytes, 20 << 30);
        assert_eq!(config.disk_high_watermark.basis_points, 1_500);
    }

    #[test]
    fn an_episode_ends_whole() {
        let mut episode = Episode {
            active: true,
            reported: true,
        };
        episode.end();
        assert_eq!(episode, Episode::default());
    }
}
