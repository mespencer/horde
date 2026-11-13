//! Reading logs back: filters, selection, and the one egress path
//! (ADR-0015).
//!
//! Every reader — `horde logs --dir`, the worker's tunnel, the server's
//! `ReadLogs`, the endpoint's local `logs` — reads through [`LocalLogs`] and
//! [`select`]. Every path that sends records off the machine then goes
//! through [`egress`], which is where redaction happens; `--dir` alone shows
//! files as they are, because the operator already owns them.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use regex::Regex;

use crate::files;
use crate::record::{Cursor, Record, Source, format_ts};
use crate::redact::redact_line;
use crate::tap::LogTap;

/// The compiled size a `--grep` pattern may reach.
pub const GREP_SIZE_LIMIT: usize = 1 << 20;

/// The default number of newest records a read shows.
pub const DEFAULT_TAIL: usize = 200;

/// The subdirectory of a worker's log directory holding harness stderr.
pub const AGENTS_DIR: &str = "agents";

/// Minimum level, in tracing's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    /// Parses a level name, any case.
    ///
    /// # Errors
    ///
    /// A message naming the levels.
    pub fn parse(text: &str) -> Result<Self, String> {
        match text.to_ascii_lowercase().as_str() {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" | "warning" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            _ => Err(format!(
                "{text:?} is not a level; use trace, debug, info, warn or error"
            )),
        }
    }

    /// The lowercase name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// Which file families an agent read covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sources {
    pub horde: bool,
    pub stderr: bool,
}

impl Sources {
    /// Both families.
    pub const ALL: Self = Self {
        horde: true,
        stderr: true,
    };

    /// Parses `all`, `horde` or `stderr`; empty means all.
    ///
    /// # Errors
    ///
    /// A message naming the choices.
    pub fn parse(text: &str) -> Result<Self, String> {
        match text {
            "" | "all" => Ok(Self::ALL),
            "horde" => Ok(Self {
                horde: true,
                stderr: false,
            }),
            "stderr" => Ok(Self {
                horde: false,
                stderr: true,
            }),
            _ => Err(format!(
                "{text:?} is not a source; use all, horde or stderr"
            )),
        }
    }

    fn admits(self, source: Source) -> bool {
        match source {
            Source::Horde => self.horde,
            Source::Stderr => self.stderr,
        }
    }
}

/// What a read keeps.
#[derive(Debug, Clone)]
pub struct Filter {
    /// Records at or after this `ts` (fixed-width spelling).
    pub since: Option<String>,
    pub min_level: Option<Level>,
    pub grep: Option<Regex>,
    /// Records about this agent only.
    pub agent: Option<String>,
    /// For an agent caller: records naming another session are dropped;
    /// records naming none are process-level and kept.
    pub session_scope: Option<String>,
    pub sources: Sources,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            since: None,
            min_level: None,
            grep: None,
            agent: None,
            session_scope: None,
            sources: Sources::ALL,
        }
    }
}

/// Compiles a `--grep` pattern under the size limit.
///
/// # Errors
///
/// A pattern that does not compile, or compiles too large.
pub fn compile_grep(pattern: &str) -> Result<Regex, String> {
    regex::RegexBuilder::new(pattern)
        .size_limit(GREP_SIZE_LIMIT)
        .build()
        .map_err(|error| format!("--grep {pattern:?}: {error}"))
}

/// Resolves `--since`: a duration before `now` (`10m`, `2h`, `30s`, `1d`)
/// or an RFC 3339 timestamp, as the fixed-width `ts` it compares against.
///
/// # Errors
///
/// Neither shape parses.
pub fn resolve_since(text: &str, now: SystemTime) -> Result<String, String> {
    if let Some(duration) = parse_duration(text) {
        // Nothing was logged before the epoch; a longer look-back is all of it.
        let at = now
            .checked_sub(duration)
            .filter(|at| *at >= SystemTime::UNIX_EPOCH)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        return Ok(format_ts(at));
    }
    crate::record::parse_ts(text)
        .map(format_ts)
        .map_err(|_| format!("--since {text:?} is neither a duration (10m) nor a timestamp"))
}

/// `30s`, `10m`, `2h`, `1d`.
fn parse_duration(text: &str) -> Option<Duration> {
    let unit = text.chars().last()?;
    let seconds_per = match unit {
        's' => 1,
        'm' => 60,
        'h' => 3600,
        'd' => 86_400,
        _ => return None,
    };
    let count: u64 = text[..text.len() - 1].parse().ok()?;
    Some(Duration::from_secs(count.checked_mul(seconds_per)?))
}

impl Filter {
    /// Whether `record` passes.
    #[must_use]
    pub fn admits(&self, record: &Record) -> bool {
        if !self.sources.admits(record.key.source) {
            return false;
        }
        if let Some(since) = &self.since
            && record.key.ts.as_str() < since.as_str()
        {
            return false;
        }
        if let Some(min) = self.min_level
            && Level::parse(record.level()).is_ok_and(|level| level < min)
        {
            return false;
        }
        if let Some(agent) = &self.agent
            && record.text("agent") != Some(agent.as_str())
        {
            return false;
        }
        if !scope_admits(record, self.session_scope.as_deref()) {
            return false;
        }
        if let Some(grep) = &self.grep {
            let fields = record
                .value
                .get("fields")
                .map(ToString::to_string)
                .unwrap_or_default();
            if !grep.is_match(record.message()) && !grep.is_match(&fields) {
                return false;
            }
        }
        true
    }
}

/// The session scoping an agent caller gets: a record naming a different
/// session is never served to it; a record naming none is process-level.
#[must_use]
pub fn scope_admits(record: &Record, scope: Option<&str>) -> bool {
    match (scope, record.text("session")) {
        (Some(scope), Some(session)) => scope == session,
        _ => true,
    }
}

/// How much of what matched a read returns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Window {
    /// Resume strictly after this record; reads forward from it.
    pub after: Option<Cursor>,
    /// Without `after`: only the newest this many. `None` is no limit.
    pub tail: Option<usize>,
    /// The bytes of record lines returned may not exceed this.
    pub max_bytes: Option<usize>,
}

/// What a read returned.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Selection {
    pub records: Vec<Record>,
    /// Matching records were left out to respect the byte cap — older ones
    /// for a tail read, newer ones for a forward read.
    pub truncated: bool,
}

impl Selection {
    /// The cursor of the newest record returned.
    #[must_use]
    pub fn last(&self) -> Option<&Cursor> {
        self.records.last().map(|record| &record.key)
    }
}

/// Picks what a read returns from `records` (any order): sorted by key,
/// filtered, then windowed.
///
/// A forward read (`after` set) returns the oldest records past the cursor
/// that fit the byte cap. A tail read returns the newest `tail` records,
/// then drops the oldest of those until they fit the cap.
#[must_use]
pub fn select(mut records: Vec<Record>, filter: &Filter, window: &Window) -> Selection {
    records.sort_by(|a, b| a.key.cmp(&b.key));
    records.dedup_by(|a, b| a.key == b.key);
    let mut matched: Vec<Record> = records
        .into_iter()
        .filter(|record| {
            window
                .after
                .as_ref()
                .is_none_or(|after| record.key > *after)
        })
        .filter(|record| filter.admits(record))
        .collect();
    let mut truncated = false;
    if window.after.is_some() {
        if let Some(cap) = window.max_bytes {
            let mut used = 0;
            let keep = matched
                .iter()
                .take_while(|record| {
                    used += record.line.len();
                    used <= cap
                })
                .count();
            truncated = keep < matched.len();
            matched.truncate(keep);
        }
        return Selection {
            records: matched,
            truncated,
        };
    }
    if let Some(tail) = window.tail
        && matched.len() > tail
    {
        matched.drain(..matched.len() - tail);
    }
    if let Some(cap) = window.max_bytes {
        let mut used = 0;
        let keep = matched
            .iter()
            .rev()
            .take_while(|record| {
                used += record.line.len();
                used <= cap
            })
            .count();
        truncated = keep < matched.len();
        matched.drain(..matched.len() - keep);
    }
    Selection {
        records: matched,
        truncated,
    }
}

/// Every record in one file; torn or foreign lines are skipped.
#[must_use]
pub fn read_file(path: &Path) -> Vec<Record> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(Record::parse)
        .collect()
}

/// Every record in every log file directly in `dir` — what `--dir` reads.
#[must_use]
pub fn read_dir(dir: &Path, prefix: Option<&str>) -> Vec<Record> {
    files::list(dir, prefix)
        .iter()
        .flat_map(|file| read_file(&file.path))
        .collect()
}

/// Which records of a process a read is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// The process's own log.
    Process,
    /// One agent: the process log filtered to it, merged with its stderr.
    Agent(String),
}

/// One machine's log directory, as the readers on that machine see it.
#[derive(Debug, Clone)]
pub struct LocalLogs {
    dir: PathBuf,
    binary: String,
    tap: Option<LogTap>,
}

impl LocalLogs {
    /// The logs `binary` writes in `dir`, followable through `tap` when the
    /// process writing them is this one.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>, binary: impl Into<String>, tap: Option<LogTap>) -> Self {
        Self {
            dir: dir.into(),
            binary: binary.into(),
            tap,
        }
    }

    /// The log directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The tap, when follow is possible.
    #[must_use]
    pub fn tap(&self) -> Option<&LogTap> {
        self.tap.as_ref()
    }

    /// The records a target covers, unfiltered, unsorted.
    #[must_use]
    pub fn records(&self, target: &Target) -> Vec<Record> {
        let mut records = read_dir(&self.dir, Some(&self.binary));
        if let Target::Agent(agent) = target {
            records.retain(|record| record.text("agent") == Some(agent.as_str()));
            records.extend(read_dir(&self.dir.join(AGENTS_DIR), Some(agent)));
        }
        records
    }

    /// Reads a target: its records, the target's own narrowing, the filter,
    /// and the window.
    #[must_use]
    pub fn read(&self, target: &Target, filter: &Filter, window: &Window) -> Selection {
        select(self.records(target), filter, window)
    }
}

/// Records ready to leave the machine.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Egress {
    pub lines: Vec<String>,
    /// How many records redaction touched.
    pub redacted: u32,
}

/// Redacts records for egress — the one place every off-machine path runs
/// through.
#[must_use]
pub fn egress<'a>(records: impl IntoIterator<Item = &'a Record>) -> Egress {
    let mut out = Egress::default();
    for record in records {
        let (line, touched) = redact_line(&record.line);
        out.redacted += u32::from(touched);
        out.lines.push(line);
    }
    out
}

/// What a follow produces past the initial read.
#[derive(Debug, Clone, PartialEq)]
pub enum Followed {
    Record(Record),
    /// The tap outran this reader by this many records.
    Lagged(u64),
}

/// Takes the next followed item off a tap receiver: records the filter
/// admits and the cursor has not passed, or a lag notice. `None` when the
/// tap is closed.
pub async fn next_followed(
    receiver: &mut tokio::sync::broadcast::Receiver<std::sync::Arc<str>>,
    target: &Target,
    filter: &Filter,
    last: &mut Option<Cursor>,
) -> Option<Followed> {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match receiver.recv().await {
            Ok(line) => {
                let Some(record) = Record::parse(&line) else {
                    continue;
                };
                if last.as_ref().is_some_and(|last| record.key <= *last) {
                    continue;
                }
                if let Target::Agent(agent) = target
                    && record.text("agent") != Some(agent.as_str())
                {
                    continue;
                }
                if !filter.admits(&record) {
                    continue;
                }
                *last = Some(record.key.clone());
                return Some(Followed::Record(record));
            }
            Err(RecvError::Lagged(missed)) => return Some(Followed::Lagged(missed)),
            Err(RecvError::Closed) => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use super::{
        Filter, Followed, Level, LocalLogs, Sources, Target, Window, compile_grep, egress,
        next_followed, resolve_since, scope_admits, select,
    };
    use crate::record::{Cursor, Record, Source};
    use crate::tap::LogTap;

    fn rec(ts: u32, level: &str, msg: &str, extra: &str) -> Record {
        let line = format!(
            r#"{{"ts":"2026-01-01T00:00:{ts:02}.000000Z","level":"{level}","msg":"{msg}","boot":"b","seq":{ts}{extra}}}"#
        );
        Record::parse(&line).expect("record")
    }

    fn msgs(selection: &super::Selection) -> Vec<&str> {
        selection.records.iter().map(Record::message).collect()
    }

    #[test]
    fn levels_parse_and_order() {
        assert_eq!(Level::parse("WARN"), Ok(Level::Warn));
        assert_eq!(Level::parse("warning"), Ok(Level::Warn));
        assert!(Level::parse("loud").is_err());
        assert!(Level::Trace < Level::Debug && Level::Warn < Level::Error);
        assert_eq!(Level::Info.as_str(), "info");
    }

    #[test]
    fn the_filter_honors_level_since_agent_grep_and_source() {
        let records = vec![
            rec(1, "DEBUG", "noise", ""),
            rec(2, "INFO", "spawned", r#","agent":"a1","fields":{"model":"opus"}"#),
            rec(3, "WARN", "slow", r#","agent":"a2""#),
            Record::parse(
                r#"{"ts":"2026-01-01T00:00:04.000000Z","stream":"stderr","line":"boom","agent":"a1","boot":"b","seq":1}"#,
            )
            .expect("stderr"),
        ];
        let all = Window::default();
        let level = Filter {
            min_level: Some(Level::Info),
            ..Filter::default()
        };
        assert_eq!(
            msgs(&select(records.clone(), &level, &all)),
            ["spawned", "slow", "boom"]
        );
        let since = Filter {
            since: Some("2026-01-01T00:00:03.000000Z".to_owned()),
            ..Filter::default()
        };
        assert_eq!(
            msgs(&select(records.clone(), &since, &all)),
            ["slow", "boom"]
        );
        let agent = Filter {
            agent: Some("a1".to_owned()),
            ..Filter::default()
        };
        assert_eq!(
            msgs(&select(records.clone(), &agent, &all)),
            ["spawned", "boom"]
        );
        let grep_fields = Filter {
            grep: Some(compile_grep("op.s").expect("grep")),
            ..Filter::default()
        };
        assert_eq!(
            msgs(&select(records.clone(), &grep_fields, &all)),
            ["spawned"]
        );
        let stderr_only = Filter {
            sources: Sources::parse("stderr").expect("source"),
            ..Filter::default()
        };
        assert_eq!(msgs(&select(records.clone(), &stderr_only, &all)), ["boom"]);
        let horde_only = Filter {
            sources: Sources::parse("horde").expect("source"),
            ..Filter::default()
        };
        assert_eq!(select(records, &horde_only, &all).records.len(), 3);
        assert!(Sources::parse("both").is_err());
        assert_eq!(Sources::parse(""), Ok(Sources::ALL));
    }

    #[test]
    fn an_agent_caller_never_sees_another_sessions_records() {
        let mine = rec(1, "INFO", "mine", r#","session":"s1""#);
        let theirs = rec(2, "INFO", "theirs", r#","session":"s2""#);
        let process = rec(3, "INFO", "process", "");
        assert!(scope_admits(&mine, Some("s1")));
        assert!(!scope_admits(&theirs, Some("s1")));
        assert!(scope_admits(&process, Some("s1")));
        assert!(scope_admits(&theirs, None), "a client sees everything");
        let scoped = Filter {
            session_scope: Some("s1".to_owned()),
            ..Filter::default()
        };
        assert_eq!(
            msgs(&select(
                vec![mine, theirs, process],
                &scoped,
                &Window::default()
            )),
            ["mine", "process"]
        );
    }

    #[test]
    fn a_tail_read_keeps_the_newest_and_the_cap_drops_the_oldest() {
        let records: Vec<Record> = (1..=5)
            .map(|n| rec(n, "INFO", &format!("m{n}"), ""))
            .collect();
        let tail = Window {
            tail: Some(2),
            ..Window::default()
        };
        assert_eq!(
            msgs(&select(records.clone(), &Filter::default(), &tail)),
            ["m4", "m5"]
        );
        let line = records[0].line.len();
        let capped = Window {
            max_bytes: Some(line * 2 + 1),
            ..Window::default()
        };
        let selection = select(records.clone(), &Filter::default(), &capped);
        assert_eq!(msgs(&selection), ["m4", "m5"]);
        assert!(selection.truncated);
        let roomy = Window {
            max_bytes: Some(line * 10),
            ..Window::default()
        };
        assert!(!select(records, &Filter::default(), &roomy).truncated);
    }

    #[test]
    fn a_forward_read_pages_past_the_cursor_within_the_cap() {
        let records: Vec<Record> = (1..=5)
            .map(|n| rec(n, "INFO", &format!("m{n}"), ""))
            .collect();
        let line = records[0].line.len();
        let first = Window {
            after: Some(records[0].key.clone()),
            max_bytes: Some(line * 2),
            ..Window::default()
        };
        let page = select(records.clone(), &Filter::default(), &first);
        assert_eq!(msgs(&page), ["m2", "m3"]);
        assert!(page.truncated);
        let next = Window {
            after: page.last().cloned(),
            max_bytes: Some(line * 2),
            tail: Some(1),
        };
        let page = select(records.clone(), &Filter::default(), &next);
        assert_eq!(
            msgs(&page),
            ["m4", "m5"],
            "tail does not apply going forward"
        );
        assert!(!page.truncated);
        let done = Window {
            after: page.last().cloned(),
            ..Window::default()
        };
        assert!(
            select(records, &Filter::default(), &done)
                .records
                .is_empty()
        );
    }

    #[test]
    fn records_are_merged_by_key_and_duplicates_collapse() {
        let a = rec(2, "INFO", "later", "");
        let b = rec(1, "INFO", "earlier", "");
        let selection = select(
            vec![a.clone(), b, a],
            &Filter::default(),
            &Window::default(),
        );
        assert_eq!(msgs(&selection), ["earlier", "later"]);
    }

    #[test]
    fn since_takes_durations_and_timestamps() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(3600);
        assert_eq!(
            resolve_since("10m", now).as_deref(),
            Ok("1970-01-01T00:50:00.000000Z")
        );
        assert_eq!(
            resolve_since("30s", now).as_deref(),
            Ok("1970-01-01T00:59:30.000000Z")
        );
        assert_eq!(
            resolve_since("1h", now).as_deref(),
            Ok("1970-01-01T00:00:00.000000Z")
        );
        assert_eq!(
            resolve_since("2d", now).as_deref(),
            Ok("1970-01-01T00:00:00.000000Z")
        );
        assert_eq!(
            resolve_since("2026-01-01T01:00:00+01:00", now).as_deref(),
            Ok("2026-01-01T00:00:00.000000Z")
        );
        for bad in ["", "m", "10x", "tomorrow", "-5m"] {
            assert!(resolve_since(bad, now).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn grep_patterns_are_bounded() {
        assert!(compile_grep("a+b").is_ok());
        assert!(compile_grep("(").is_err());
        assert!(
            compile_grep(&"a{1000}".repeat(100)).is_err(),
            "too large to compile"
        );
    }

    #[test]
    fn an_agent_read_merges_its_process_records_with_its_stderr() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("horde-worker.b.1.jsonl"),
            [
                rec(1, "INFO", "pumped", r#","agent":"a1""#).line,
                rec(3, "INFO", "other", r#","agent":"a2""#).line,
            ]
            .join("\n"),
        )
        .expect("write");
        std::fs::create_dir(dir.path().join("agents")).expect("mkdir");
        std::fs::write(
            dir.path().join("agents/a1.1.jsonl"),
            r#"{"ts":"2026-01-01T00:00:02.000000Z","stream":"stderr","line":"warned","agent":"a1","boot":"b","seq":1}
torn"#,
        )
        .expect("write");
        let logs = LocalLogs::new(dir.path(), "horde-worker", None);
        let agent = logs.read(
            &Target::Agent("a1".to_owned()),
            &Filter::default(),
            &Window::default(),
        );
        assert_eq!(msgs(&agent), ["pumped", "warned"]);
        let process = logs.read(&Target::Process, &Filter::default(), &Window::default());
        assert_eq!(msgs(&process), ["pumped", "other"]);
        assert_eq!(logs.dir(), dir.path());
        assert!(logs.tap().is_none());
    }

    #[test]
    fn egress_redacts_and_counts() {
        let clean = rec(1, "INFO", "fine", "");
        let dirty = rec(2, "INFO", "join horde://join/h:1/s.f", "");
        let out = egress([&clean, &dirty]);
        assert_eq!(out.redacted, 1);
        assert_eq!(out.lines[0], clean.line);
        assert!(out.lines[1].contains("[REDACTED:join-token]"));
        assert!(!out.lines[1].contains("horde://join"));
    }

    #[tokio::test]
    async fn a_follow_skips_what_the_cursor_passed_and_reports_lag() {
        let tap = LogTap::new(4);
        let mut receiver = tap.subscribe();
        let old = rec(1, "INFO", "old", r#","agent":"a1""#);
        let fresh = rec(2, "INFO", "fresh", r#","agent":"a1""#);
        let elsewhere = rec(3, "INFO", "elsewhere", r#","agent":"a2""#);
        let debug = rec(4, "DEBUG", "quiet", r#","agent":"a1""#);
        for record in [&old, &fresh, &elsewhere, &debug] {
            tap.publish(Arc::from(record.line.as_str()));
        }
        tap.publish(Arc::from("not json"));
        let filter = Filter {
            min_level: Some(Level::Info),
            ..Filter::default()
        };
        let mut last: Option<Cursor> = Some(old.key.clone());
        let target = Target::Agent("a1".to_owned());
        match next_followed(&mut receiver, &target, &filter, &mut last).await {
            Some(Followed::Lagged(1)) => {}
            other => panic!("expected a lag of one, got {other:?}"),
        }
        let followed = next_followed(&mut receiver, &target, &filter, &mut last).await;
        assert_eq!(followed, Some(Followed::Record(fresh.clone())));
        assert_eq!(last.as_ref(), Some(&fresh.key));
        drop(tap);
        assert_eq!(
            next_followed(&mut receiver, &target, &filter, &mut last).await,
            None
        );
        assert_eq!(fresh.key.source, Source::Horde);
    }
}
