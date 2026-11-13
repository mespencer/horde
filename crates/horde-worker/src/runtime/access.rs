//! The Worker's half of the endpoint's workspace verbs (ADR-0014):
//! `workspace_checkout` and the parent bases a child spawn carries.

use async_trait::async_trait;
use horde_mcp::workspace::{CheckoutAnswer, WorkspaceAccess};
use horde_proto::v1::{CheckoutMode, CheckoutPrepared, RepoBase, RepoBaseSource};
use horde_relay::{ShareRoute, observe_head, stage_share};

use super::shared::{Shared, checkout_prepared_entry};
use crate::workspace::resolution_name;

/// Serves the workspace verbs from the Runtime's shared state.
pub(super) struct WorkerWorkspaceAccess {
    pub(super) shared: Shared,
}

#[async_trait]
impl WorkspaceAccess for WorkerWorkspaceAccess {
    async fn checkout(
        &self,
        agent_id: &str,
        repo: &str,
        base: Option<&str>,
    ) -> Result<CheckoutAnswer, String> {
        let shared = &self.shared;
        let workspaces = shared
            .workspaces
            .as_ref()
            .ok_or("this worker has no workspaces; there is nothing to check out")?;
        let lock = shared.checkout_locks.of(agent_id);
        let _held = lock.lock().await;
        // Checked under the lock: an archive that got there first wins, and
        // this call loses cleanly rather than adding to a checkout on its
        // way out.
        let checkout = shared.broker.checkout_of(agent_id).ok_or_else(|| {
            format!("agent {agent_id} has no live checkout here (it may have been archived)")
        })?;
        let (definition_id, _) = workspaces.agent_private(&checkout.root).ok_or(
            "this agent works in the shared instance itself; the shared policy cannot check out repos",
        )?;
        let policy = shared.policy_for(&definition_id);
        let added = workspaces
            .add_repo(
                &definition_id,
                agent_id,
                repo,
                base,
                policy.fetch,
                shared.broker.as_ref(),
            )
            .await
            .map_err(|error| error.to_string())?;
        shared.broker.add_repo(agent_id, repo, added.relative.clone());
        let resolution = horde_proto::v1::checkout_prepared::BaseResolution::try_from(
            added.prepared.resolution,
        )
        .unwrap_or_default();
        if let Ok(Some(record)) = shared.journal.revivable_agent(agent_id).await {
            let available = workspaces
                .instance_repos(&definition_id)
                .await
                .map(|repos| repos.into_iter().map(|(name, _)| name).collect())
                .unwrap_or_default();
            let entry = checkout_prepared_entry(
                &record.session_id,
                agent_id,
                format!("{agent_id}:checkout:{repo}"),
                CheckoutPrepared {
                    repos: vec![added.prepared.clone()],
                    mode: CheckoutMode::Lazy.into(),
                    available_repos: available,
                },
            );
            if let Err(error) = shared.journal.record(entry).await {
                tracing::warn!(agent_id, %error, "a workspace checkout could not be journaled");
            }
        }
        Ok(CheckoutAnswer {
            path: added.path.display().to_string(),
            branch: added.branch,
            base_commit_sha: added.prepared.base_commit_sha,
            resolution: resolution_name(resolution).to_owned(),
            stale_reason: added.prepared.stale_reason,
        })
    }

    async fn parent_bases(&self, agent_id: &str) -> Result<Vec<RepoBase>, String> {
        let broker = &self.shared.broker;
        let Some(checkout) = broker.checkout_of(agent_id) else {
            return Ok(Vec::new());
        };
        let mut bases = Vec::new();
        for (repo_name, relative) in &checkout.repos {
            let repo = checkout.root.join(relative);
            let observed = match observe_head(&repo).await {
                Ok(observed) => observed,
                // A repo whose HEAD cannot be observed contributes no base:
                // the child starts fresh there, rather than not at all.
                Err(error) => {
                    tracing::warn!(agent_id, repo = %repo_name, %error, "could not observe the parent's HEAD");
                    continue;
                }
            };
            // Staged so a child on another Worker can obtain the commit.
            if let Some(relay) = broker.relay() {
                let route = ShareRoute::ViaRelay {
                    repo_name: repo_name.clone(),
                };
                stage_share(&repo, &route, Some(relay), &observed.commit_sha)
                    .await
                    .map_err(|error| format!("could not stage {repo_name}'s HEAD: {error}"))?;
            }
            bases.push(RepoBase {
                repo_name: repo_name.clone(),
                r#ref: observed.commit_sha,
                source: RepoBaseSource::Parent.into(),
            });
        }
        Ok(bases)
    }
}
