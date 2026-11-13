//! Per-agent harness stderr files (ADR-0015).
//!
//! The worker's stderr drain writes each line a harness prints to
//! `agents/<agent-id>.<n>.jsonl` as `{ts, agent, session, stream: "stderr",
//! line, boot, seq}`, beside the in-memory tail it keeps for the death
//! diagnostic. Each agent gets two 4 MiB files, rotated like the daemon
//! logs; the whole directory has a budget, and past it the files of agents
//! no longer running go first, oldest first, then the oldest files of live
//! agents. A live agent always keeps its current file.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::SystemTime;

use serde_json::{Map, Value};

use crate::files::{FileSet, LogFile, RotatingFiles, list};
use crate::layer::Stamp;
use crate::record::{STDERR_STREAM, format_ts, serialize_bounded};
use crate::settings::AgentLogSettings;
use crate::tap::LogTap;

/// One agent's open family of stderr files.
#[derive(Debug)]
struct AgentFiles {
    files: RotatingFiles,
}

#[derive(Debug, Default)]
struct Live {
    agents: BTreeMap<String, Weak<Mutex<AgentFiles>>>,
}

/// The `agents/` directory of one worker.
#[derive(Debug)]
pub struct AgentLogs {
    dir: PathBuf,
    settings: AgentLogSettings,
    stamp: Arc<Stamp>,
    tap: Option<LogTap>,
    live: Mutex<Live>,
}

/// One agent's stderr writer. Dropping the last clone marks the agent as no
/// longer live for the budget.
#[derive(Debug, Clone)]
pub struct AgentStderr {
    logs: Arc<AgentLogs>,
    agent: String,
    session: String,
    files: Arc<Mutex<AgentFiles>>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

impl AgentLogs {
    /// The harness-stderr directory `dir`, stamped with the worker's boot
    /// and published to `tap` for follow.
    #[must_use]
    pub fn new(
        dir: impl Into<PathBuf>,
        settings: AgentLogSettings,
        boot: &str,
        tap: Option<LogTap>,
    ) -> Arc<Self> {
        Arc::new(Self {
            dir: dir.into(),
            settings,
            stamp: Arc::new(Stamp::new(boot.to_owned())),
            tap,
            live: Mutex::new(Live::default()),
        })
    }

    /// The directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The writer for one agent's harness. A revived agent's writer
    /// continues its files.
    #[must_use]
    pub fn writer(self: &Arc<Self>, agent: &str, session: &str) -> AgentStderr {
        let mut live = lock(&self.live);
        let files = live
            .agents
            .get(agent)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                Arc::new(Mutex::new(AgentFiles {
                    files: RotatingFiles::new(FileSet {
                        dir: self.dir.clone(),
                        prefix: agent.to_owned(),
                        boot: None,
                        max_file_bytes: self.settings.max_file_bytes,
                        max_files: self.settings.max_files,
                        max_age: None,
                    }),
                }))
            });
        live.agents.insert(agent.to_owned(), Arc::downgrade(&files));
        AgentStderr {
            logs: Arc::clone(self),
            agent: agent.to_owned(),
            session: session.to_owned(),
            files,
        }
    }

    /// The agents with a live writer, and each one's current file.
    fn live_files(&self) -> (BTreeSet<String>, BTreeSet<PathBuf>) {
        let mut live = lock(&self.live);
        live.agents.retain(|_, files| files.strong_count() > 0);
        let mut agents = BTreeSet::new();
        let mut current = BTreeSet::new();
        for (agent, files) in &live.agents {
            agents.insert(agent.clone());
            if let Some(files) = files.upgrade()
                && let Ok(guard) = files.try_lock()
                && let Some(path) = guard.files.current_path()
            {
                current.insert(path.to_path_buf());
            }
        }
        (agents, current)
    }

    /// Enforces the directory budget; returns what it deleted. `opened` is
    /// the file just opened, which is current even while its writer holds
    /// the lock that hides it from [`Self::live_files`].
    fn enforce_budget(&self, opened: &Path) -> Vec<PathBuf> {
        let (live, mut current) = self.live_files();
        current.insert(opened.to_path_buf());
        let files = list(&self.dir, None);
        budget_choice(&files, self.settings.budget_bytes, &live, &current)
            .into_iter()
            .filter(|path| std::fs::remove_file(path).is_ok())
            .collect()
    }
}

/// Which files (oldest first in `files`) the budget removes: archived
/// agents' files first, oldest first, then live agents' non-current files,
/// until the total fits. Current files are never chosen.
#[must_use]
pub fn budget_choice(
    files: &[LogFile],
    budget: u64,
    live: &BTreeSet<String>,
    current: &BTreeSet<PathBuf>,
) -> Vec<PathBuf> {
    let mut total: u64 = files.iter().map(|file| file.len).sum();
    let archived = files.iter().filter(|file| !live.contains(&file.prefix));
    let older_live = files
        .iter()
        .filter(|file| live.contains(&file.prefix) && !current.contains(&file.path));
    let mut doomed = Vec::new();
    for file in archived.chain(older_live) {
        if total <= budget {
            break;
        }
        if current.contains(&file.path) {
            continue;
        }
        total = total.saturating_sub(file.len);
        doomed.push(file.path.clone());
    }
    doomed
}

impl AgentStderr {
    /// The agent this writer belongs to.
    #[must_use]
    pub fn agent(&self) -> &str {
        &self.agent
    }

    /// Writes one stderr line. Never fails: a line that cannot be written
    /// is still in the death tail, and logging is never fatal.
    pub fn line(&self, text: &str) {
        self.line_at(text, SystemTime::now());
    }

    /// [`Self::line`] at an injected time.
    pub fn line_at(&self, text: &str, now: SystemTime) {
        let mut record = Map::new();
        record.insert("ts".to_owned(), Value::String(format_ts(now)));
        record.insert("agent".to_owned(), Value::String(self.agent.clone()));
        record.insert("session".to_owned(), Value::String(self.session.clone()));
        record.insert("stream".to_owned(), Value::String(STDERR_STREAM.to_owned()));
        record.insert("line".to_owned(), Value::String(text.to_owned()));
        record.insert(
            "boot".to_owned(),
            Value::String(self.logs.stamp.boot().to_owned()),
        );
        record.insert("seq".to_owned(), Value::from(self.logs.stamp.next()));
        let line = serialize_bounded(record);
        if let Some(tap) = &self.logs.tap {
            tap.publish(Arc::from(line.as_str()));
        }
        let (rotated, opened) = {
            let mut files = lock(&self.files);
            let before = files.files.current_path().map(Path::to_path_buf);
            if files.files.write(&line, now).is_err() {
                files.files.discard();
                return;
            }
            let after = files.files.current_path().map(Path::to_path_buf);
            (before != after, after)
        };
        if rotated && let Some(opened) = opened {
            self.logs.enforce_budget(&opened);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime};

    use super::{AgentLogs, budget_choice};
    use crate::files::{LogFile, list};
    use crate::reader::read_dir;
    use crate::record::Source;
    use crate::settings::AgentLogSettings;
    use crate::tap::LogTap;

    fn settings(budget: u64) -> AgentLogSettings {
        AgentLogSettings {
            max_file_bytes: 400,
            max_files: 2,
            budget_bytes: budget,
        }
    }

    #[tokio::test]
    async fn a_stderr_line_lands_in_the_agents_file_and_on_the_tap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tap = LogTap::default();
        let mut receiver = tap.subscribe();
        let logs = AgentLogs::new(dir.path(), settings(1 << 20), "boot1", Some(tap));
        let writer = logs.writer("agent-1", "session-1");
        writer.line("warning: deprecated flag");
        let records = read_dir(dir.path(), Some("agent-1"));
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.key.source, Source::Stderr);
        assert_eq!(record.message(), "warning: deprecated flag");
        assert_eq!(record.text("agent"), Some("agent-1"));
        assert_eq!(record.text("session"), Some("session-1"));
        assert_eq!(record.key.boot, "boot1");
        assert!(
            receiver
                .try_recv()
                .is_ok_and(|line| line.contains("deprecated")),
            "published as it was written"
        );
        assert_eq!(writer.agent(), "agent-1");
    }

    #[test]
    fn each_agent_keeps_two_files_and_a_revival_continues_them() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = AgentLogs::new(dir.path(), settings(1 << 20), "b", None);
        let writer = logs.writer("a1", "s1");
        for n in 0..12 {
            writer.line(&format!("line {n} {}", "x".repeat(100)));
        }
        let files = list(dir.path(), Some("a1"));
        assert_eq!(files.len(), 2, "{files:?}");
        drop(writer);
        let revived = logs.writer("a1", "s1");
        revived.line("after revival");
        let newest = list(dir.path(), Some("a1"));
        assert!(newest.iter().map(|f| f.n).max() >= files.iter().map(|f| f.n).max());
        assert!(
            read_dir(dir.path(), Some("a1"))
                .iter()
                .any(|r| r.message() == "after revival")
        );
    }

    #[test]
    fn the_budget_prunes_archived_agents_first_and_spares_live_current_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = AgentLogs::new(dir.path(), settings(900), "b", None);
        let archived = logs.writer("old", "s1");
        for _ in 0..3 {
            archived.line(&"o".repeat(300));
        }
        drop(archived);
        let live = logs.writer("live", "s1");
        for _ in 0..3 {
            live.line(&"l".repeat(300));
        }
        let left = list(dir.path(), None);
        let total: u64 = left.iter().map(|f| f.len).sum();
        assert!(total <= 900, "within budget: {left:?}");
        assert!(
            left.iter().any(|f| f.prefix == "live"),
            "the live agent keeps its file"
        );
        assert!(left.iter().filter(|f| f.prefix == "old").count() < 2);
    }

    /// An archived agent's newer file goes before a live agent's older
    /// one: liveness, not age, decides who pays first.
    #[test]
    fn the_budget_spares_a_live_agents_older_files_before_an_archived_agents_newer_ones() {
        let probe = tempfile::tempdir().expect("tempdir");
        let line = "x".repeat(300);
        let measure = AgentLogs::new(probe.path(), settings(1 << 20), "b", None);
        measure.writer("l", "s").line(&line);
        let size = list(probe.path(), None)[0].len;

        let dir = tempfile::tempdir().expect("tempdir");
        let three = AgentLogSettings {
            max_file_bytes: 400,
            max_files: 3,
            budget_bytes: size * 4 + size / 2,
        };
        let logs = AgentLogs::new(dir.path(), three, "b", None);
        let live = logs.writer("l", "s");
        live.line(&line);
        live.line(&line);
        let archived = logs.writer("a", "s");
        archived.line(&line);
        archived.line(&line);
        drop(archived);
        live.line(&line);
        let left: Vec<String> = list(dir.path(), None)
            .iter()
            .map(|file| format!("{}.{}", file.prefix, file.n))
            .collect();
        assert!(
            left.contains(&"l.1".to_owned()),
            "the live agent's oldest file stays: {left:?}"
        );
        assert!(
            !left.contains(&"a.1".to_owned()),
            "the archived agent's file went: {left:?}"
        );
        assert_eq!(left.len(), 4, "{left:?}");
    }

    fn file(prefix: &str, n: u64, len: u64) -> LogFile {
        LogFile {
            path: PathBuf::from(format!("/a/{prefix}.{n}.jsonl")),
            prefix: prefix.to_owned(),
            boot: None,
            n,
            modified: SystemTime::UNIX_EPOCH + Duration::from_secs(n),
            len,
        }
    }

    #[test]
    fn budget_choice_orders_archived_before_live_and_never_takes_current() {
        let files = [
            file("live", 1, 100),
            file("gone", 2, 100),
            file("live", 3, 100),
            file("gone", 4, 100),
        ];
        let live = BTreeSet::from(["live".to_owned()]);
        let current = BTreeSet::from([files[2].path.clone()]);
        assert!(budget_choice(&files, 400, &live, &current).is_empty());
        assert_eq!(
            budget_choice(&files, 250, &live, &current),
            [files[1].path.clone(), files[3].path.clone()]
        );
        assert_eq!(
            budget_choice(&files, 100, &live, &current),
            [
                files[1].path.clone(),
                files[3].path.clone(),
                files[0].path.clone()
            ]
        );
        assert_eq!(
            budget_choice(&files, 0, &live, &current).len(),
            3,
            "current survives"
        );
    }

    #[test]
    fn an_unwritable_directory_costs_lines_never_a_panic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let blocker = dir.path().join("agents");
        std::fs::write(&blocker, "not a directory").expect("write");
        let logs = AgentLogs::new(&blocker, settings(1 << 20), "b", None);
        logs.writer("a", "s").line("dropped quietly");
        assert!(blocker.is_file());
        assert_eq!(logs.dir(), blocker);
    }
}
