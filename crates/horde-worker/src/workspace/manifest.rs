//! The workspace manifest an agent reads: every repo it could check out,
//! which ones it has, and the unmanaged paths linked into its root.
//!
//! Lives at `<agent_root>/.horde/workspace.json` — inside the composed root,
//! outside every repo layout (the Server refuses a layout beginning
//! `.horde`) — and is rewritten atomically on every change (ADR-0014).

use std::path::{Path, PathBuf};

use horde_proto::v1::WorkspaceDefinition;
use serde::{Deserialize, Serialize};

use super::ledger::{LedgerRecord, write_atomically};
use super::paths::layout_of;

/// The manifest's path inside an agent root.
pub const MANIFEST_PATH: &str = ".horde/workspace.json";

/// One repo as the manifest lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRepo {
    pub name: String,
    pub path: String,
    pub default_branch: String,
    pub checked_out: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub base: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub resolution: String,
    /// Commits delivered for this repo before it was checked out: present in
    /// the Instance repo, waiting for `workspace_checkout(repo, base=<sha>)`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_commits: Vec<String>,
}

/// The whole manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub workspace: String,
    pub mode: String,
    pub branch: String,
    pub repos: Vec<ManifestRepo>,
    #[serde(default)]
    pub unmanaged: Vec<String>,
}

impl Manifest {
    /// The manifest a ledger record and its Definition describe, keeping the
    /// pending commits a previous manifest recorded.
    #[must_use]
    pub fn describe(
        definition: &WorkspaceDefinition,
        record: &LedgerRecord,
        previous: Option<&Self>,
    ) -> Self {
        let repos = definition
            .repos
            .iter()
            .filter_map(|repo| {
                let path = layout_of(repo).ok()?.to_owned();
                let checked = record
                    .repos
                    .iter()
                    .find(|checked| checked.name == repo.name && !checked.pending);
                let pending_commits = previous
                    .and_then(|previous| previous.repos.iter().find(|r| r.name == repo.name))
                    .filter(|_| checked.is_none())
                    .map(|previous| previous.pending_commits.clone())
                    .unwrap_or_default();
                Some(ManifestRepo {
                    name: repo.name.clone(),
                    path,
                    default_branch: repo.default_branch.clone(),
                    checked_out: checked.is_some(),
                    base: checked
                        .map(|c| c.base_commit_sha.clone())
                        .unwrap_or_default(),
                    resolution: checked.map(|c| c.resolution.clone()).unwrap_or_default(),
                    pending_commits,
                })
            })
            .collect();
        Self {
            workspace: definition.name.clone(),
            mode: record.mode.clone(),
            branch: record.branch.clone(),
            repos,
            unmanaged: definition.unmanaged_paths.clone(),
        }
    }

    /// Records a commit delivered into a repo not checked out yet.
    pub fn add_pending(&mut self, repo_name: &str, sha: &str) {
        if let Some(repo) = self.repos.iter_mut().find(|repo| repo.name == repo_name)
            && !repo.pending_commits.iter().any(|pending| pending == sha)
        {
            repo.pending_commits.push(sha.to_owned());
        }
    }
}

/// Where one agent root's manifest lives.
#[must_use]
pub fn manifest_path(agent_root: &Path) -> PathBuf {
    agent_root.join(MANIFEST_PATH)
}

/// The manifest currently on disk, if one parses.
pub async fn read(agent_root: &Path) -> Option<Manifest> {
    let bytes = tokio::fs::read(manifest_path(agent_root)).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Writes the manifest atomically.
pub(crate) async fn write(agent_root: &Path, manifest: &Manifest) -> Result<(), String> {
    let path = manifest_path(agent_root);
    if let Some(dir) = path.parent() {
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|error| format!("could not create {}: {error}", dir.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| format!("could not encode the manifest: {error}"))?;
    write_atomically(&path, &bytes).await
}

/// The paragraph a lazy agent's prompt overlay gains: what it can check out
/// and how.
#[must_use]
pub fn lazy_overlay(manifest: &Manifest, manifest_file: &Path) -> String {
    let listed: Vec<String> = manifest
        .repos
        .iter()
        .map(|repo| {
            let state = if repo.checked_out {
                "checked out"
            } else {
                "not checked out"
            };
            format!("- {} at ./{} ({state})", repo.name, repo.path)
        })
        .collect();
    format!(
        "Your workspace \"{}\" checks repos out on demand. Before reading or changing a repo that \
         is not checked out, call the horde tool `workspace_checkout` with its name; it creates \
         your worktree on branch {} and returns its path. The full list is in {}.\n{}",
        manifest.workspace,
        manifest.branch,
        manifest_file.display(),
        listed.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{WorkspaceDefinitionId, WorkspaceRepo};

    use super::*;
    use crate::workspace::ledger::LedgerRepo;

    fn definition() -> WorkspaceDefinition {
        WorkspaceDefinition {
            workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-1")),
            name: "suite".to_owned(),
            repos: ["atlas", "proto"]
                .into_iter()
                .map(|name| WorkspaceRepo {
                    name: name.to_owned(),
                    default_branch: "main".to_owned(),
                    ..WorkspaceRepo::default()
                })
                .collect(),
            unmanaged_paths: vec!["data".to_owned()],
            ..WorkspaceDefinition::default()
        }
    }

    fn record() -> LedgerRecord {
        LedgerRecord {
            agent_id: "agent-1".to_owned(),
            mode: "lazy".to_owned(),
            branch: "horde/agent-1".to_owned(),
            repos: vec![LedgerRepo {
                name: "atlas".to_owned(),
                base_commit_sha: "9c1e".to_owned(),
                resolution: "fresh".to_owned(),
                ..LedgerRepo::default()
            }],
            ..LedgerRecord::default()
        }
    }

    #[test]
    fn the_manifest_lists_every_repo_and_marks_the_checked_out_ones() {
        let manifest = Manifest::describe(&definition(), &record(), None);
        assert_eq!(manifest.workspace, "suite");
        assert_eq!(manifest.mode, "lazy");
        assert_eq!(manifest.unmanaged, vec!["data"]);
        let atlas = &manifest.repos[0];
        assert!(atlas.checked_out);
        assert_eq!(atlas.base, "9c1e");
        let proto = &manifest.repos[1];
        assert!(!proto.checked_out);
        assert!(proto.base.is_empty());
    }

    /// A pending commit is kept until its repo is checked out, and recorded
    /// once however often it is delivered.
    #[test]
    fn pending_commits_survive_rewrites_until_checkout() {
        let mut manifest = Manifest::describe(&definition(), &record(), None);
        manifest.add_pending("proto", "41aa");
        manifest.add_pending("proto", "41aa");
        manifest.add_pending("ghost", "41aa");
        let rewritten = Manifest::describe(&definition(), &record(), Some(&manifest));
        assert_eq!(rewritten.repos[1].pending_commits, vec!["41aa"]);

        let mut checked = record();
        checked.repos.push(LedgerRepo {
            name: "proto".to_owned(),
            ..LedgerRepo::default()
        });
        let after = Manifest::describe(&definition(), &checked, Some(&manifest));
        assert!(after.repos[1].pending_commits.is_empty());
    }

    #[test]
    fn a_pending_repo_is_not_yet_checked_out() {
        let mut pending = record();
        pending.repos[0].pending = true;
        let manifest = Manifest::describe(&definition(), &pending, None);
        assert!(!manifest.repos[0].checked_out);
    }

    #[tokio::test]
    async fn the_manifest_round_trips_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = Manifest::describe(&definition(), &record(), None);
        write(dir.path(), &manifest).await.expect("written");
        assert_eq!(read(dir.path()).await, Some(manifest));
    }

    #[test]
    fn the_lazy_overlay_names_the_tool_and_every_repo() {
        let manifest = Manifest::describe(&definition(), &record(), None);
        let overlay = lazy_overlay(&manifest, Path::new("/a/.horde/workspace.json"));
        assert!(overlay.contains("workspace_checkout"), "{overlay}");
        assert!(
            overlay.contains("- atlas at ./atlas (checked out)"),
            "{overlay}"
        );
        assert!(
            overlay.contains("- proto at ./proto (not checked out)"),
            "{overlay}"
        );
        assert!(overlay.contains("/a/.horde/workspace.json"), "{overlay}");
    }
}
