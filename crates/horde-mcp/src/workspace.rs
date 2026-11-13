//! The seam onto the Worker's workspaces (ADR-0014).
//!
//! Two verbs need the Agent's checkout, which lives on the Worker, not here:
//! `workspace_checkout` adds a repo to a lazy Agent's checkout, and
//! `spawn_agent` names the caller's committed HEAD in each checked-out repo
//! as its child's `PARENT` base. Like [`JournalSink`](crate::journal::JournalSink),
//! this crate states what it needs and the Worker implements it; an endpoint
//! with no implementation answers both honestly (no checkout to add to, no
//! parent bases to give).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// What `workspace_checkout` answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CheckoutAnswer {
    /// The repo's checkout path.
    pub path: String,
    /// The branch it is on.
    pub branch: String,
    /// The commit it started from (its HEAD, when it was already there).
    pub base_commit_sha: String,
    /// How the base was chosen: fresh, cached, parent, requested, reused…
    pub resolution: String,
    /// Non-empty: why the base is not fresh.
    pub stale_reason: String,
}

/// The Worker's half of the workspace verbs.
#[async_trait]
pub trait WorkspaceAccess: Send + Sync {
    /// Checks `repo` out for `agent_id`, starting at `base` when given.
    /// Idempotent: a repo already checked out answers with its path.
    ///
    /// # Errors
    ///
    /// Why not, in words the Agent can act on: an unknown repo (naming the
    /// valid ones), a `shared` policy, no workspace, an agent archived.
    async fn checkout(
        &self,
        agent_id: &str,
        repo: &str,
        base: Option<&str>,
    ) -> Result<CheckoutAnswer, String>;

    /// The `PARENT` bases a child of `agent_id` starts from: the caller's
    /// committed HEAD per checked-out repo, staged to the Relay when one is
    /// configured so a child on another Worker can obtain it.
    ///
    /// # Errors
    ///
    /// A HEAD that could not be observed or staged.
    async fn parent_bases(&self, agent_id: &str) -> Result<Vec<horde_proto::v1::RepoBase>, String>;
}

/// The bases a spawn carries: the caller's explicit `bases` win; every other
/// repo the caller has checked out starts from its committed HEAD.
#[must_use]
pub fn merged_bases(
    requested: &std::collections::BTreeMap<String, String>,
    parent: Vec<horde_proto::v1::RepoBase>,
) -> Vec<horde_proto::v1::RepoBase> {
    let mut bases: Vec<horde_proto::v1::RepoBase> = requested
        .iter()
        .map(|(repo, reference)| horde_proto::v1::RepoBase {
            repo_name: repo.clone(),
            r#ref: reference.clone(),
            source: horde_proto::v1::RepoBaseSource::Requested.into(),
        })
        .collect();
    bases.extend(
        parent
            .into_iter()
            .filter(|base| !requested.contains_key(&base.repo_name)),
    );
    bases
}

#[cfg(test)]
mod tests {
    use super::*;
    use horde_proto::v1::{RepoBase, RepoBaseSource};

    fn parent(repo: &str, sha: &str) -> RepoBase {
        RepoBase {
            repo_name: repo.to_owned(),
            r#ref: sha.to_owned(),
            source: RepoBaseSource::Parent.into(),
        }
    }

    /// An explicit base overrides the parent's for the same repo; the
    /// parent's commit fills every repo the caller did not name.
    #[test]
    fn explicit_bases_override_parent_bases_repo_by_repo() {
        let requested = std::collections::BTreeMap::from([("atlas".to_owned(), "release/1.4".to_owned())]);
        let merged = merged_bases(&requested, vec![parent("atlas", "aaa"), parent("proto", "bbb")]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].repo_name, "atlas");
        assert_eq!(merged[0].r#ref, "release/1.4");
        assert_eq!(merged[0].source, i32::from(RepoBaseSource::Requested));
        assert_eq!(merged[1], parent("proto", "bbb"));
        assert!(merged_bases(&std::collections::BTreeMap::new(), Vec::new()).is_empty());
    }
}
