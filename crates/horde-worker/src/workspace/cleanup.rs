//! Cleanup on whole checkouts, pins and Instances, as events (ADR-0014).
//!
//! Each operation returns the `WorkspaceCleanup` that records it — done,
//! would-do (a dry run), skipped, refused or failed, per repo — and leaves
//! the decision of where to journal it to the caller. Soft reclaim keeps
//! the worktree, branch and commits; hard removal runs only when the
//! reachability check proves nothing is lost, or when an operator forces it.

use std::path::{Path, PathBuf};

use horde_proto::v1::workspace_cleanup::repo_action::Outcome;
use horde_proto::v1::workspace_cleanup::{RepoAction, RetainedFile};
use horde_proto::v1::{
    AgentId, CleanupTier, CleanupTrigger, SessionId, WorkerId, WorkspaceCleanup,
    WorkspaceDefinition, WorkspaceDefinitionId, WorkspaceInstanceReport, WorkspaceInstanceState,
};

use super::ledger::{self, LedgerRecord, now_ms};
use super::policy::WorkspacePolicy;
use super::reclaim::{self, Resolved};
use super::removal::{self, Proof, Target};
use super::{WorkspaceManager, layout_of, read_state_file};

/// What one cleanup pass is, for every event it writes.
#[derive(Debug, Clone, Copy)]
pub struct Pass<'a> {
    pub trigger: CleanupTrigger,
    /// The gc request behind an explicit pass; empty for triggers.
    pub gc_request_id: &'a str,
    pub dry_run: bool,
    pub policy: &'a WorkspacePolicy,
}

/// One repo of a checkout, located.
struct Located {
    name: String,
    instance_repo: PathBuf,
    checkout: PathBuf,
    created_branch: bool,
    output_paths: Vec<String>,
}

impl WorkspaceManager {
    fn event(
        &self,
        definition_id: &str,
        record: Option<&LedgerRecord>,
        tier: CleanupTier,
        pass: Pass<'_>,
        repos: Vec<RepoAction>,
    ) -> WorkspaceCleanup {
        WorkspaceCleanup {
            worker_id: Some(WorkerId::new(self.worker_id.clone())),
            workspace_definition_id: Some(WorkspaceDefinitionId::new(definition_id)),
            agent_id: record.map(|record| AgentId::new(record.agent_id.clone())),
            session_id: record
                .filter(|record| !record.session_id.is_empty())
                .map(|record| SessionId::new(record.session_id.clone())),
            tier: tier.into(),
            trigger: pass.trigger.into(),
            gc_request_id: pass.gc_request_id.to_owned(),
            dry_run: pass.dry_run,
            repos,
        }
    }

    /// The checked-out repos of a record, with their reclaim resolved.
    async fn locate(
        &self,
        definition_id: &str,
        definition: &WorkspaceDefinition,
        record: &LedgerRecord,
        policy: &WorkspacePolicy,
    ) -> Vec<(Located, Resolved)> {
        let instance_root = self.instance_root(definition_id);
        let agent_root = self.agents_dir(definition_id).join(&record.agent_id);
        let mut located = Vec::new();
        for entry in record.repos.iter().filter(|repo| !repo.pending) {
            let Some(repo) = definition.repos.iter().find(|repo| repo.name == entry.name) else {
                continue;
            };
            let Ok(layout) = layout_of(repo) else {
                continue;
            };
            let checkout = agent_root.join(layout);
            let resolved = reclaim::resolve(&checkout, repo.reclaim.as_ref(), policy).await;
            let output_paths = match &resolved {
                Resolved::Run(command) => command.output_paths.clone(),
                Resolved::Skip(_) => Vec::new(),
            };
            located.push((
                Located {
                    name: entry.name.clone(),
                    instance_repo: instance_root.join(layout),
                    checkout,
                    created_branch: entry.created_branch,
                    output_paths,
                },
                resolved,
            ));
        }
        located
    }

    /// Soft reclaim of one checkout: each repo's reclaim command, in its
    /// checkout. `soft_reclaimed_at` is set only when every repo was done or
    /// skipped (and never by a dry run).
    pub async fn soft_reclaim(
        &self,
        definition_id: &str,
        record: &LedgerRecord,
        pass: Pass<'_>,
    ) -> WorkspaceCleanup {
        let instance_root = self.instance_root(definition_id);
        let Some(definition) = read_state_file(&instance_root).await else {
            return self.event(
                definition_id,
                Some(record),
                CleanupTier::Soft,
                pass,
                Vec::new(),
            );
        };
        let mut actions = Vec::new();
        for (located, resolved) in self
            .locate(definition_id, &definition, record, pass.policy)
            .await
        {
            actions.push(if pass.dry_run {
                would_reclaim(&located.name, &resolved)
            } else {
                reclaim::run(
                    &located.name,
                    &located.checkout,
                    &resolved,
                    pass.policy.reclaim_timeout,
                )
                .await
            });
        }
        if !pass.dry_run {
            let settled = actions.iter().all(|action| {
                action.outcome == i32::from(Outcome::Done)
                    || action.outcome == i32::from(Outcome::Skipped)
            });
            let mut updated = record.clone();
            if settled {
                updated.soft_reclaimed_at_ms = Some(now_ms());
            }
            if pass.trigger == CleanupTrigger::Scan {
                updated.orphan_reported = true;
            }
            if updated != *record {
                let _ = ledger::write(&self.agents_dir(definition_id), &updated).await;
            }
        }
        self.event(
            definition_id,
            Some(record),
            CleanupTier::Soft,
            pass,
            actions,
        )
    }

    /// Hard removal of one archived or orphaned checkout: every repo must be
    /// provably lossless, or `force` must be set. Returns the event and
    /// whether the checkout is gone. A refusal records its reason in the
    /// ledger (`retained_reason`).
    pub async fn hard_remove(
        &self,
        definition_id: &str,
        record: &LedgerRecord,
        pass: Pass<'_>,
        force: bool,
    ) -> (WorkspaceCleanup, bool) {
        let instance_root = self.instance_root(definition_id);
        let Some(definition) = read_state_file(&instance_root).await else {
            return (
                self.event(
                    definition_id,
                    Some(record),
                    CleanupTier::Hard,
                    pass,
                    Vec::new(),
                ),
                false,
            );
        };
        let located = self
            .locate(definition_id, &definition, record, pass.policy)
            .await;
        let mut proofs = Vec::new();
        for (repo, _) in &located {
            proofs.push(
                removal::removable(Target {
                    instance_repo: &repo.instance_repo,
                    checkout: &repo.checkout,
                    branch: &record.branch,
                    fresh_clone: record.is_fresh_clone(),
                    output_paths: &repo.output_paths,
                })
                .await,
            );
        }
        let lossless = proofs.iter().all(Proof::is_lossless);
        if !lossless && !force {
            let actions = refusals(&located, &proofs);
            if !pass.dry_run {
                self.retain(definition_id, record, &proofs).await;
            }
            return (
                self.event(
                    definition_id,
                    Some(record),
                    CleanupTier::Hard,
                    pass,
                    actions,
                ),
                false,
            );
        }
        if pass.dry_run {
            let actions = located
                .iter()
                .zip(&proofs)
                .map(|((repo, _), proof)| RepoAction {
                    repo_name: repo.name.clone(),
                    outcome: Outcome::WouldDo.into(),
                    reason: forced_reason(proof),
                    unrecoverable_files: unrecoverable_of(proof),
                    ..RepoAction::default()
                })
                .collect();
            return (
                self.event(
                    definition_id,
                    Some(record),
                    CleanupTier::Hard,
                    pass,
                    actions,
                ),
                false,
            );
        }
        let (actions, removed) = self
            .remove_checkout(definition_id, record, &located, &proofs, force)
            .await;
        (
            self.event(
                definition_id,
                Some(record),
                CleanupTier::Hard,
                pass,
                actions,
            ),
            removed,
        )
    }

    /// Carries out a decided removal: each repo, then the agent root, then
    /// the ledger record last.
    async fn remove_checkout(
        &self,
        definition_id: &str,
        record: &LedgerRecord,
        located: &[(Located, Resolved)],
        proofs: &[Proof],
        force: bool,
    ) -> (Vec<RepoAction>, bool) {
        let mut actions = Vec::new();
        let mut all_removed = true;
        for ((repo, _), proof) in located.iter().zip(proofs) {
            let before = reclaim::dir_size(&repo.checkout).await.unwrap_or_default();
            let removed = removal::remove_repo(
                Target {
                    instance_repo: &repo.instance_repo,
                    checkout: &repo.checkout,
                    branch: &record.branch,
                    fresh_clone: record.is_fresh_clone(),
                    output_paths: &repo.output_paths,
                },
                &record.agent_id,
                repo.created_branch,
                force,
            )
            .await;
            actions.push(match removed {
                Ok(removed) => RepoAction {
                    repo_name: repo.name.clone(),
                    outcome: Outcome::Done.into(),
                    reason: forced_reason(proof),
                    bytes_freed: before,
                    pinned_commit_sha: removed.pinned_commit_sha,
                    unrecoverable_files: unrecoverable_of(proof),
                    ..RepoAction::default()
                },
                Err(error) => {
                    all_removed = false;
                    RepoAction {
                        repo_name: repo.name.clone(),
                        outcome: Outcome::Failed.into(),
                        reason: error,
                        ..RepoAction::default()
                    }
                }
            });
        }
        if !all_removed {
            return (actions, false);
        }
        let agent_root = self.agents_dir(definition_id).join(&record.agent_id);
        match tokio::fs::remove_dir_all(&agent_root).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                actions.push(RepoAction {
                    outcome: Outcome::Failed.into(),
                    reason: format!("could not remove {}: {error}", agent_root.display()),
                    ..RepoAction::default()
                });
                return (actions, false);
            }
        }
        let _ = ledger::remove(&self.agents_dir(definition_id), &record.agent_id).await;
        (actions, true)
    }

    /// Writes why a checkout was retained, when that changed.
    async fn retain(&self, definition_id: &str, record: &LedgerRecord, proofs: &[Proof]) {
        let reason: Vec<&str> = proofs
            .iter()
            .map(Proof::reason)
            .filter(|reason| !reason.is_empty())
            .collect();
        let reason = reason.join("; ");
        let agents_dir = self.agents_dir(definition_id);
        if let Some(mut current) = ledger::read(&agents_dir, &record.agent_id).await
            && current.retained_reason != reason
        {
            current.retained_reason = reason;
            let _ = ledger::write(&agents_dir, &current).await;
        }
    }

    /// Marks a checkout archived, returning its record. `None` when the
    /// agent has no ledgered checkout here.
    pub async fn mark_archived(&self, definition_id: &str, agent_id: &str) -> Option<LedgerRecord> {
        let agents_dir = self.agents_dir(definition_id);
        let mut record = ledger::read(&agents_dir, agent_id).await?;
        if record.archived_at_ms.is_none() {
            record.archived_at_ms = Some(now_ms());
            ledger::write(&agents_dir, &record).await.ok()?;
        }
        Some(record)
    }

    /// Writes an agent's last activity through to its ledger, clearing its
    /// soft reclaim (a new turn may rebuild what it removed).
    pub async fn note_activity(&self, definition_id: &str, agent_id: &str, at_ms: i64) {
        let agents_dir = self.agents_dir(definition_id);
        if let Some(mut record) = ledger::read(&agents_dir, agent_id).await
            && at_ms > record.last_active_at_ms
        {
            record.last_active_at_ms = at_ms;
            record.soft_reclaimed_at_ms = None;
            let _ = ledger::write(&agents_dir, &record).await;
        }
    }

    /// Drops every archive pin of one Instance whose commit is reachable
    /// from origin or a Relay share ref (`unpinnable`). With `force` and a
    /// pin TTL, an unreachable pin whose commit is older than the TTL goes
    /// too. One event per pin acted on or refused.
    pub async fn unpin(
        &self,
        definition_id: &str,
        relay_repo_url: &(dyn Fn(&str) -> Option<String> + Sync),
        pass: Pass<'_>,
        force: bool,
    ) -> Vec<WorkspaceCleanup> {
        let instance_root = self.instance_root(definition_id);
        let Some(definition) = read_state_file(&instance_root).await else {
            return Vec::new();
        };
        let live: Vec<String> = ledger::list(&self.agents_dir(definition_id))
            .await
            .into_iter()
            .map(|record| record.agent_id)
            .collect();
        let mut events = Vec::new();
        for repo in &definition.repos {
            let Ok(layout) = layout_of(repo) else {
                continue;
            };
            let instance_repo = instance_root.join(layout);
            let Ok(pins) = removal::pins(&instance_repo).await else {
                continue;
            };
            if pins.is_empty() {
                continue;
            }
            let keepers = removal::keepers(
                &instance_repo,
                relay_repo_url(&repo.name).as_deref(),
                pass.policy.fetch.timeout,
            )
            .await;
            for pin in pins {
                let agent = pin.trim_start_matches("refs/horde/archived/").to_owned();
                if live.contains(&agent) {
                    continue;
                }
                let proof = removal::reachable(&instance_repo, &pin, &keepers).await;
                let expired = force
                    && pass
                        .policy
                        .archived_pin_ttl
                        .is_some_and(|ttl| commit_older_than(&instance_repo, &pin, ttl));
                let action = self
                    .unpin_one(
                        &instance_repo,
                        &repo.name,
                        &pin,
                        &proof,
                        expired,
                        pass.dry_run,
                    )
                    .await;
                let record = LedgerRecord {
                    agent_id: agent,
                    ..LedgerRecord::default()
                };
                events.push(self.event(
                    definition_id,
                    Some(&record),
                    CleanupTier::Unpin,
                    pass,
                    vec![action],
                ));
            }
        }
        events
    }

    async fn unpin_one(
        &self,
        instance_repo: &Path,
        repo_name: &str,
        pin: &str,
        proof: &Proof,
        expired: bool,
        dry_run: bool,
    ) -> RepoAction {
        let base = RepoAction {
            repo_name: repo_name.to_owned(),
            ..RepoAction::default()
        };
        if !proof.is_lossless() && !expired {
            return RepoAction {
                outcome: Outcome::Refused.into(),
                reason: proof.reason().to_owned(),
                ..base
            };
        }
        let reason = if proof.is_lossless() {
            String::new()
        } else {
            format!("older than the pin TTL: {}", proof.reason())
        };
        if dry_run {
            return RepoAction {
                outcome: Outcome::WouldDo.into(),
                reason,
                ..base
            };
        }
        match removal::delete_ref(instance_repo, pin).await {
            Ok(()) => RepoAction {
                outcome: Outcome::Done.into(),
                reason,
                ..base
            },
            Err(error) => RepoAction {
                outcome: Outcome::Failed.into(),
                reason: error,
                ..base
            },
        }
    }

    /// Removes a whole Instance (`instance remove`, `delete --purge`).
    /// Refused outright while any agent lives there. Every checkout must be
    /// removable, the Instance trees clean, every local ref reachable and
    /// every unmanaged path empty — or `force`. Returns the events and, when
    /// the Instance is gone, its `MISSING` report.
    pub async fn remove_instance(
        &self,
        definition_id: &str,
        live_agents: &[String],
        relay_repo_url: &(dyn Fn(&str) -> Option<String> + Sync),
        pass: Pass<'_>,
        force: bool,
    ) -> (Vec<WorkspaceCleanup>, Option<WorkspaceInstanceReport>) {
        let instance_root = self.instance_root(definition_id);
        let Some(definition) = read_state_file(&instance_root).await else {
            return (Vec::new(), None);
        };
        let refuse = |reason: String, unrecoverable: Vec<RetainedFile>| {
            vec![self.event(
                definition_id,
                None,
                CleanupTier::Instance,
                pass,
                vec![RepoAction {
                    outcome: Outcome::Refused.into(),
                    reason,
                    unrecoverable_files: unrecoverable,
                    ..RepoAction::default()
                }],
            )]
        };
        if !live_agents.is_empty() {
            return (
                refuse(
                    format!(
                        "agents still live in this instance: {}",
                        live_agents.join(", ")
                    ),
                    Vec::new(),
                ),
                None,
            );
        }
        let mut events = Vec::new();
        let mut blocked = Vec::new();
        for record in self.records(definition_id).await {
            let (event, removed) = self.hard_remove(definition_id, &record, pass, force).await;
            let refused = event.repos.iter().any(|action| {
                action.outcome != i32::from(Outcome::Done)
                    && action.outcome != i32::from(Outcome::WouldDo)
            });
            if (!removed && !pass.dry_run) || refused {
                blocked.push(format!("checkout of agent {}", record.agent_id));
            }
            events.push(event);
        }
        let mut unrecoverable = Vec::new();
        if !force {
            blocked.extend(
                self.instance_blockers(&definition, relay_repo_url, pass)
                    .await,
            );
        }
        for path in &definition.unmanaged_paths {
            let bytes = reclaim::dir_size(&instance_root.join(path))
                .await
                .unwrap_or_default();
            if bytes > 0 || has_entries(&instance_root.join(path)) {
                unrecoverable.push(RetainedFile {
                    path: path.clone(),
                    bytes,
                });
            }
        }
        if !unrecoverable.is_empty() && !force {
            blocked.push("non-empty unmanaged data cannot be proven recoverable".to_owned());
        }
        if !blocked.is_empty() {
            events.extend(refuse(blocked.join("; "), unrecoverable));
            return (events, None);
        }
        let freed = reclaim::dir_size(&instance_root).await.unwrap_or_default();
        let action = if pass.dry_run {
            RepoAction {
                outcome: Outcome::WouldDo.into(),
                bytes_freed: freed,
                unrecoverable_files: unrecoverable,
                ..RepoAction::default()
            }
        } else {
            match tokio::fs::remove_dir_all(&instance_root).await {
                Ok(()) => RepoAction {
                    outcome: Outcome::Done.into(),
                    bytes_freed: freed,
                    unrecoverable_files: unrecoverable,
                    ..RepoAction::default()
                },
                Err(error) => RepoAction {
                    outcome: Outcome::Failed.into(),
                    reason: format!("could not remove {}: {error}", instance_root.display()),
                    ..RepoAction::default()
                },
            }
        };
        let gone = action.outcome == i32::from(Outcome::Done);
        events.push(self.event(
            definition_id,
            None,
            CleanupTier::Instance,
            pass,
            vec![action],
        ));
        let missing = gone.then(|| WorkspaceInstanceReport {
            workspace_definition_id: Some(WorkspaceDefinitionId::new(definition_id)),
            worker_id: Some(WorkerId::new(self.worker_id.clone())),
            state: WorkspaceInstanceState::Missing.into(),
            root_path: instance_root.display().to_string(),
            detail: "removed".to_owned(),
            ..WorkspaceInstanceReport::default()
        });
        (events, missing)
    }

    /// Why the Instance's own repos cannot go: a dirty tree, or a local ref
    /// no keeper reaches.
    async fn instance_blockers(
        &self,
        definition: &WorkspaceDefinition,
        relay_repo_url: &(dyn Fn(&str) -> Option<String> + Sync),
        pass: Pass<'_>,
    ) -> Vec<String> {
        let instance_root = self.instance_root(&super::definition_id_of(definition));
        let mut blocked = Vec::new();
        for repo in &definition.repos {
            let Ok(layout) = layout_of(repo) else {
                continue;
            };
            let instance_repo = instance_root.join(layout);
            match removal::instance_tree_clean(&instance_repo).await {
                Ok(None) => {}
                Ok(Some(reason)) => blocked.push(format!("repo {}: {reason}", repo.name)),
                Err(error) => blocked.push(format!("repo {}: not provable: {error}", repo.name)),
            }
            let keepers = removal::keepers(
                &instance_repo,
                relay_repo_url(&repo.name).as_deref(),
                pass.policy.fetch.timeout,
            )
            .await;
            match removal::local_refs(&instance_repo).await {
                Ok(refs) => {
                    for reference in refs {
                        let proof = removal::reachable(&instance_repo, &reference, &keepers).await;
                        if !proof.is_lossless() {
                            blocked.push(format!(
                                "repo {} {reference}: {}",
                                repo.name,
                                proof.reason()
                            ));
                        }
                    }
                }
                Err(error) => blocked.push(format!("repo {}: not provable: {error}", repo.name)),
            }
        }
        blocked
    }
}

/// What a dry run says one repo's soft reclaim would do.
fn would_reclaim(name: &str, resolved: &Resolved) -> RepoAction {
    match resolved {
        Resolved::Skip(reason) => RepoAction {
            repo_name: name.to_owned(),
            outcome: Outcome::Skipped.into(),
            reason: reason.clone(),
            ..RepoAction::default()
        },
        Resolved::Run(command) => RepoAction {
            repo_name: name.to_owned(),
            outcome: Outcome::WouldDo.into(),
            argv: command.argv.clone(),
            ..RepoAction::default()
        },
    }
}

/// The refusal actions for a checkout whose removal is not provable: the
/// unprovable repos say why; the lossless ones are skipped with them.
fn refusals(located: &[(Located, Resolved)], proofs: &[Proof]) -> Vec<RepoAction> {
    located
        .iter()
        .zip(proofs)
        .map(|((repo, _), proof)| match proof {
            Proof::Lossless => RepoAction {
                repo_name: repo.name.clone(),
                outcome: Outcome::Skipped.into(),
                reason: "another repo of this checkout is not provably lossless".to_owned(),
                ..RepoAction::default()
            },
            Proof::NotProvable {
                reason,
                unrecoverable,
            } => RepoAction {
                repo_name: repo.name.clone(),
                outcome: Outcome::Refused.into(),
                reason: reason.clone(),
                unrecoverable_files: unrecoverable.clone(),
                ..RepoAction::default()
            },
        })
        .collect()
}

/// The reason a removal records: empty when lossless, the overridden
/// refusal when forced.
fn forced_reason(proof: &Proof) -> String {
    match proof {
        Proof::Lossless => String::new(),
        Proof::NotProvable { reason, .. } => format!("forced: {reason}"),
    }
}

fn unrecoverable_of(proof: &Proof) -> Vec<RetainedFile> {
    match proof {
        Proof::Lossless => Vec::new(),
        Proof::NotProvable { unrecoverable, .. } => unrecoverable.clone(),
    }
}

fn has_entries(path: &Path) -> bool {
    std::fs::read_dir(path).is_ok_and(|mut entries| entries.next().is_some())
}

/// Whether the commit `reference` names was committed longer than `ttl` ago.
fn commit_older_than(repo: &Path, reference: &str, ttl: std::time::Duration) -> bool {
    let Ok(output) = std::process::Command::new("git")
        .args(["log", "-1", "--format=%ct", reference])
        .current_dir(repo)
        .output()
    else {
        return false;
    };
    let Ok(seconds) = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<i64>()
    else {
        return false;
    };
    let age_ms = now_ms().saturating_sub(seconds.saturating_mul(1000));
    u128::try_from(age_ms).is_ok_and(|age| age > ttl.as_millis())
}

/// Counts for a gc completion: actions done (or would-do) and refused,
/// and bytes freed.
#[must_use]
pub fn tally(events: &[WorkspaceCleanup]) -> (u32, u32, u64) {
    let mut done = 0_u32;
    let mut refused = 0_u32;
    let mut freed = 0_u64;
    for action in events.iter().flat_map(|event| &event.repos) {
        match Outcome::try_from(action.outcome) {
            Ok(Outcome::Done | Outcome::WouldDo) => done += 1,
            Ok(Outcome::Refused | Outcome::Failed) => refused += 1,
            _ => {}
        }
        freed = freed.saturating_add(action.bytes_freed);
    }
    (done, refused, freed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn action(outcome: Outcome, bytes: u64) -> RepoAction {
        RepoAction {
            outcome: outcome.into(),
            bytes_freed: bytes,
            ..RepoAction::default()
        }
    }

    #[test]
    fn a_tally_counts_done_and_refused_and_sums_bytes() {
        let events = vec![
            WorkspaceCleanup {
                repos: vec![action(Outcome::Done, 10), action(Outcome::Skipped, 0)],
                ..WorkspaceCleanup::default()
            },
            WorkspaceCleanup {
                repos: vec![
                    action(Outcome::Refused, 0),
                    action(Outcome::Failed, 0),
                    action(Outcome::WouldDo, 5),
                ],
                ..WorkspaceCleanup::default()
            },
        ];
        assert_eq!(tally(&events), (2, 2, 15));
    }

    #[test]
    fn a_dry_run_reclaim_names_the_command_or_the_skip() {
        let skip = would_reclaim("atlas", &Resolved::Skip("no reclaim command".to_owned()));
        assert_eq!(skip.outcome, i32::from(Outcome::Skipped));
        let run = would_reclaim(
            "atlas",
            &Resolved::Run(super::super::policy::cargo_rule().command),
        );
        assert_eq!(run.outcome, i32::from(Outcome::WouldDo));
        assert_eq!(run.argv, vec!["cargo", "clean"]);
    }

    #[test]
    fn a_forced_reason_names_what_was_overridden() {
        assert_eq!(forced_reason(&Proof::Lossless), "");
        let proof = Proof::NotProvable {
            reason: "untracked files: 1".to_owned(),
            unrecoverable: vec![RetainedFile {
                path: ".env".to_owned(),
                bytes: 3,
            }],
        };
        assert_eq!(forced_reason(&proof), "forced: untracked files: 1");
        assert_eq!(unrecoverable_of(&proof).len(), 1);
        assert!(unrecoverable_of(&Proof::Lossless).is_empty());
    }
}
