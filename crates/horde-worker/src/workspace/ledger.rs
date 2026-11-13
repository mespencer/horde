//! The checkout ledger: one file per agent-private checkout, beside it.
//!
//! A file and not a journal table because a worker reset (ADR-0011) replaces
//! the journal, and the checkouts that survive a reset must still say whose
//! they were (ADR-0014). The record stores ids, never paths: every path is
//! derived from ids through `is_path_safe` and `layout_of`, so an edited
//! ledger cannot point the Worker at anything outside the Instance.
//!
//! Written before `worktree add` with the repo marked `pending`, finalized
//! after it, so a crash leaves a record the next scan can repair or remove.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::paths::is_path_safe;

/// Where ledger records live, under an Instance's agents directory. The
/// leading dot keeps it from ever reading as an agent.
pub(crate) const LEDGER_DIR: &str = ".ledger";

/// One repo of one checkout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LedgerRepo {
    pub name: String,
    pub base_commit_sha: String,
    /// The `BaseResolution` it started from, by its lower-case name.
    pub resolution: String,
    pub requested_ref: String,
    /// Whether the Worker created the branch (and so may delete it) or
    /// adopted one that existed before the agent.
    pub created_branch: bool,
    /// Written before `worktree add`, cleared after: a crash in between
    /// leaves this set.
    pub pending: bool,
}

/// One agent-private checkout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LedgerRecord {
    pub agent_id: String,
    pub session_id: String,
    /// `worktree` or `fresh-clone`.
    pub policy: String,
    /// `all` or `lazy`.
    pub mode: String,
    pub branch: String,
    pub repos: Vec<LedgerRepo>,
    pub created_at_ms: i64,
    pub last_active_at_ms: i64,
    /// Set when a soft reclaim finished with every repo done or skipped;
    /// cleared by activity.
    pub soft_reclaimed_at_ms: Option<i64>,
    pub archived_at_ms: Option<i64>,
    /// Why hard removal was refused, when it last was.
    pub retained_reason: String,
    /// Whether this checkout was already reported as an orphan, so a scan
    /// reports it once and not every minute.
    #[serde(default)]
    pub orphan_reported: bool,
}

impl LedgerRecord {
    /// Whether a repo of this name is checked out (or being checked out).
    #[must_use]
    pub fn has_repo(&self, name: &str) -> bool {
        self.repos.iter().any(|repo| repo.name == name)
    }

    /// Whether this is a fresh clone rather than a worktree.
    #[must_use]
    pub fn is_fresh_clone(&self) -> bool {
        self.policy == FRESH_CLONE
    }
}

/// The ledger's spelling of the `fresh-clone` policy.
pub const FRESH_CLONE: &str = "fresh-clone";

/// The ledger's spelling of the `worktree` policy.
pub const WORKTREE: &str = "worktree";

/// The ledger directory of one Instance.
pub(crate) fn ledger_dir(agents_dir: &Path) -> PathBuf {
    agents_dir.join(LEDGER_DIR)
}

fn record_path(agents_dir: &Path, agent_id: &str) -> Option<PathBuf> {
    is_path_safe(agent_id).then(|| ledger_dir(agents_dir).join(format!("{agent_id}.json")))
}

/// One agent's record, if it has one that parses.
pub(crate) async fn read(agents_dir: &Path, agent_id: &str) -> Option<LedgerRecord> {
    let bytes = tokio::fs::read(record_path(agents_dir, agent_id)?)
        .await
        .ok()?;
    let record: LedgerRecord = serde_json::from_slice(&bytes).ok()?;
    // A record whose agent id disagrees with its file name is not this
    // agent's, whatever it says.
    (record.agent_id == agent_id).then_some(record)
}

/// Writes one record atomically: write-temp-then-rename, so a reader never
/// sees half a record.
pub(crate) async fn write(agents_dir: &Path, record: &LedgerRecord) -> Result<(), String> {
    let path = record_path(agents_dir, &record.agent_id)
        .ok_or_else(|| format!("{:?} is not a directory name", record.agent_id))?;
    let dir = ledger_dir(agents_dir);
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|error| format!("could not create {}: {error}", dir.display()))?;
    let bytes = serde_json::to_vec_pretty(record)
        .map_err(|error| format!("could not encode the ledger record: {error}"))?;
    write_atomically(&path, &bytes).await
}

/// Removes one record; absent is fine.
pub(crate) async fn remove(agents_dir: &Path, agent_id: &str) -> Result<(), String> {
    let Some(path) = record_path(agents_dir, agent_id) else {
        return Ok(());
    };
    match tokio::fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("could not remove {}: {error}", path.display())),
    }
}

/// Every record in one Instance, in agent-id order. Unparseable files are
/// skipped: one corrupt record must not hide every other checkout.
pub(crate) async fn list(agents_dir: &Path) -> Vec<LedgerRecord> {
    let mut records = Vec::new();
    let Ok(mut entries) = tokio::fs::read_dir(ledger_dir(agents_dir)).await else {
        return records;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let Some(agent_id) = name.to_str().and_then(|name| name.strip_suffix(".json")) else {
            continue;
        };
        if let Some(record) = read(agents_dir, agent_id).await {
            records.push(record);
        }
    }
    records.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
    records
}

/// Writes `bytes` to `path` through a sibling temporary file and a rename.
pub(crate) async fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("{} has no file name", path.display()))?;
    let temporary = path.with_file_name(format!(".{file_name}.tmp"));
    tokio::fs::write(&temporary, bytes)
        .await
        .map_err(|error| format!("could not write {}: {error}", temporary.display()))?;
    tokio::fs::rename(&temporary, path)
        .await
        .map_err(|error| format!("could not replace {}: {error}", path.display()))
}

/// Milliseconds since the Unix epoch, now.
#[must_use]
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(agent_id: &str) -> LedgerRecord {
        LedgerRecord {
            agent_id: agent_id.to_owned(),
            session_id: "s1".to_owned(),
            policy: WORKTREE.to_owned(),
            mode: "all".to_owned(),
            branch: format!("horde/{agent_id}"),
            repos: vec![LedgerRepo {
                name: "atlas".to_owned(),
                created_branch: true,
                ..LedgerRepo::default()
            }],
            ..LedgerRecord::default()
        }
    }

    #[tokio::test]
    async fn a_record_round_trips_and_lists_in_agent_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        for agent in ["agent-b", "agent-a"] {
            write(dir.path(), &record(agent)).await.expect("written");
        }
        assert_eq!(read(dir.path(), "agent-a").await, Some(record("agent-a")));
        let listed: Vec<String> = list(dir.path())
            .await
            .into_iter()
            .map(|record| record.agent_id)
            .collect();
        assert_eq!(listed, vec!["agent-a", "agent-b"]);

        remove(dir.path(), "agent-a").await.expect("removed");
        assert_eq!(read(dir.path(), "agent-a").await, None);
        remove(dir.path(), "agent-a")
            .await
            .expect("removing an absent record is fine");
    }

    /// A record is addressed by its file name: one claiming another agent's
    /// id, or a hostile id, is nobody's.
    #[tokio::test]
    async fn a_record_must_agree_with_its_file_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), &record("agent-a"))
            .await
            .expect("written");
        let liar = ledger_dir(dir.path()).join("agent-z.json");
        tokio::fs::copy(ledger_dir(dir.path()).join("agent-a.json"), &liar)
            .await
            .expect("copied");
        assert_eq!(read(dir.path(), "agent-z").await, None);
        assert_eq!(read(dir.path(), "../agent-a").await, None);
        assert!(write(dir.path(), &record("../escape")).await.is_err());
        assert_eq!(list(dir.path()).await.len(), 1);
    }

    #[test]
    fn a_record_knows_its_repos_and_policy() {
        let mut record = record("agent-a");
        assert!(record.has_repo("atlas"));
        assert!(!record.has_repo("proto"));
        assert!(!record.is_fresh_clone());
        record.policy = FRESH_CLONE.to_owned();
        assert!(record.is_fresh_clone());
    }

    #[test]
    fn now_is_after_the_epoch() {
        assert!(now_ms() > 1_700_000_000_000);
    }
}
