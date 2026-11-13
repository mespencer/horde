//! Bounded, rotating JSON-lines files (ADR-0015).
//!
//! A daemon's files are `<binary>.<boot>.<n>.jsonl`, a harness's stderr
//! files `<agent>.<n>.jsonl`. A new file opens when the current one would
//! pass its size; files are never renamed, which keeps rotation safe for a
//! concurrent reader and for Windows, where renaming an open file fails.
//! Every open prunes: files past the count or older than the age limit go,
//! oldest first, so disk use has a hard ceiling of `max_files × max_bytes`.
//!
//! Directories are created `0700` and files `0600` — the same owner-only
//! rule as the rest of a state directory.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// The suffix every log file carries.
pub const SUFFIX: &str = ".jsonl";

/// How one family of files is named and bounded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSet {
    pub dir: PathBuf,
    /// The binary (`horde-worker`) or the agent id.
    pub prefix: String,
    /// The process's boot id, for daemon files; `None` for per-agent files,
    /// whose numbering continues across worker restarts instead.
    pub boot: Option<String>,
    pub max_file_bytes: u64,
    pub max_files: usize,
    pub max_age: Option<Duration>,
}

/// One file on disk that belongs to a family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFile {
    pub path: PathBuf,
    pub prefix: String,
    pub boot: Option<String>,
    pub n: u64,
    pub modified: SystemTime,
    pub len: u64,
}

/// Splits a file name into `(prefix, boot, n)` when it is one of ours.
///
/// The prefix is everything before the first dot (binaries and agent ids
/// carry none); the rest is `n` or `boot.n`.
#[must_use]
pub fn parse_name(name: &str) -> Option<(String, Option<String>, u64)> {
    let stem = name.strip_suffix(SUFFIX)?;
    let (prefix, rest) = stem.split_once('.')?;
    if prefix.is_empty() {
        return None;
    }
    match rest.split_once('.') {
        None => Some((prefix.to_owned(), None, rest.parse().ok()?)),
        Some((boot, n)) if !boot.is_empty() && !n.contains('.') => {
            Some((prefix.to_owned(), Some(boot.to_owned()), n.parse().ok()?))
        }
        Some(_) => None,
    }
}

/// The name of file `n` of a family.
#[must_use]
pub fn file_name(prefix: &str, boot: Option<&str>, n: u64) -> String {
    match boot {
        Some(boot) => format!("{prefix}.{boot}.{n}{SUFFIX}"),
        None => format!("{prefix}.{n}{SUFFIX}"),
    }
}

/// Every log file directly in `dir`, oldest first (by modification time,
/// then number). `prefix` narrows to one family's files; a missing
/// directory is simply empty.
#[must_use]
pub fn list(dir: &Path, prefix: Option<&str>) -> Vec<LogFile> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<LogFile> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let (found, boot, n) = parse_name(&name)?;
            if prefix.is_some_and(|wanted| wanted != found) {
                return None;
            }
            let metadata = entry.metadata().ok()?;
            if !metadata.is_file() {
                return None;
            }
            Some(LogFile {
                path: entry.path(),
                prefix: found,
                boot,
                n,
                modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                len: metadata.len(),
            })
        })
        .collect();
    files.sort_by_key(|file| (file.modified, file.n));
    files
}

/// Which of `files` (oldest first) pruning removes: everything past the
/// newest `max_files`, and everything older than `max_age` at `now`. The
/// file at `keep` — the one just opened — is never chosen.
#[must_use]
pub fn prune_choice(
    files: &[LogFile],
    max_files: usize,
    max_age: Option<Duration>,
    now: SystemTime,
    keep: &Path,
) -> Vec<PathBuf> {
    let excess = files.len().saturating_sub(max_files);
    files
        .iter()
        .enumerate()
        .filter(|(index, file)| {
            let over_count = *index < excess;
            let over_age = max_age.is_some_and(|age| {
                now.duration_since(file.modified)
                    .is_ok_and(|elapsed| elapsed > age)
            });
            (over_count || over_age) && file.path != keep
        })
        .map(|(_, file)| file.path.clone())
        .collect()
}

/// Creates `dir` and makes it owner-only. Ancestors it has to create are
/// owner-only too: the log writer may run before anything else has made the
/// state directory, and must not leave it world-readable.
///
/// # Errors
///
/// The directory could not be created or restricted.
pub fn owner_only_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
        builder.mode(0o700);
        builder.create(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    builder.create(dir)?;
    Ok(())
}

/// Opens (creating) one owner-only file for appending.
fn open_owner_only(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options.open(path)
}

/// The writing end of one family: the current file and its size.
pub struct RotatingFiles {
    set: FileSet,
    n: u64,
    current: Option<(File, PathBuf, u64)>,
    /// A line written first into every new file — the service identity
    /// stamp — built at open time.
    header: Option<Box<dyn Fn(SystemTime) -> String + Send>>,
}

impl std::fmt::Debug for RotatingFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RotatingFiles")
            .field("set", &self.set)
            .field("n", &self.n)
            .field("current", &self.current_path())
            .finish_non_exhaustive()
    }
}

impl RotatingFiles {
    /// A family that opens its first file on the first write. Numbering
    /// continues past whatever this family already left on disk.
    #[must_use]
    pub fn new(set: FileSet) -> Self {
        let n = list(&set.dir, Some(&set.prefix))
            .iter()
            .filter(|file| file.boot == set.boot)
            .map(|file| file.n)
            .max()
            .unwrap_or(0);
        Self {
            set,
            n,
            current: None,
            header: None,
        }
    }

    /// Writes `header(now)` as the first line of every file this family
    /// opens.
    #[must_use]
    pub fn with_header(mut self, header: impl Fn(SystemTime) -> String + Send + 'static) -> Self {
        self.header = Some(Box::new(header));
        self
    }

    /// The file being written, once one is open.
    #[must_use]
    pub fn current_path(&self) -> Option<&Path> {
        self.current.as_ref().map(|(_, path, _)| path.as_path())
    }

    /// The family's bounds.
    #[must_use]
    pub fn set(&self) -> &FileSet {
        &self.set
    }

    /// Appends one line (a newline is added), rotating first when the line
    /// would carry the current file past its size. Returns the files pruned
    /// by a rotation, if one happened.
    ///
    /// # Errors
    ///
    /// The directory or file could not be created or written. The caller
    /// decides what a failure means; [`Self::discard`] drops the handle so
    /// the next write opens afresh.
    pub fn write(&mut self, line: &str, now: SystemTime) -> std::io::Result<Vec<PathBuf>> {
        let needed = line.len() as u64 + 1;
        let full = self
            .current
            .as_ref()
            // A file is never empty here: one opens only to take a line, and
            // a failed write discards it. So an oversized line still lands,
            // alone, in the fresh file it opens.
            .is_none_or(|(_, _, size)| size + needed > self.set.max_file_bytes);
        let pruned = if full {
            self.open_next(now)?
        } else {
            Vec::new()
        };
        if let Some((file, _, size)) = &mut self.current {
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
            *size += needed;
        }
        Ok(pruned)
    }

    /// Forgets the open file, so the next write opens a new one.
    pub fn discard(&mut self) {
        self.current = None;
    }

    fn open_next(&mut self, now: SystemTime) -> std::io::Result<Vec<PathBuf>> {
        owner_only_dir(&self.set.dir)?;
        self.n += 1;
        let path = self.set.dir.join(file_name(
            &self.set.prefix,
            self.set.boot.as_deref(),
            self.n,
        ));
        let mut file = open_owner_only(&path)?;
        let mut size = file.metadata().map(|m| m.len()).unwrap_or(0);
        if let Some(header) = &self.header {
            let line = header(now);
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
            size += line.len() as u64 + 1;
        }
        self.current = Some((file, path.clone(), size));
        let files = list(&self.set.dir, Some(&self.set.prefix));
        let doomed = prune_choice(&files, self.set.max_files, self.set.max_age, now, &path);
        for path in &doomed {
            // A file that vanished already is pruned all the same.
            let _ = std::fs::remove_file(path);
        }
        Ok(doomed)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    use super::{FileSet, LogFile, RotatingFiles, file_name, list, parse_name, prune_choice};

    fn set(dir: &Path, boot: Option<&str>) -> FileSet {
        FileSet {
            dir: dir.to_path_buf(),
            prefix: "horde-worker".to_owned(),
            boot: boot.map(str::to_owned),
            max_file_bytes: 100,
            max_files: 3,
            max_age: None,
        }
    }

    #[test]
    fn names_round_trip_and_strangers_are_ignored() {
        assert_eq!(
            parse_name(&file_name("horde-worker", Some("ab12"), 7)),
            Some(("horde-worker".to_owned(), Some("ab12".to_owned()), 7))
        );
        assert_eq!(
            parse_name(&file_name("agent-1", None, 2)),
            Some(("agent-1".to_owned(), None, 2))
        );
        for stranger in [
            "notes.txt",
            "horde.jsonl",
            ".1.jsonl",
            "a.b.c.1.jsonl",
            "a.x.jsonl",
            "a..1.jsonl",
            "a.b.x.jsonl",
        ] {
            assert_eq!(parse_name(stranger), None, "{stranger}");
        }
    }

    #[test]
    fn a_file_rotates_at_its_size_and_never_renames() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut files = RotatingFiles::new(set(dir.path(), Some("b")));
        let now = SystemTime::now();
        let line = "x".repeat(39); // 40 bytes with the newline
        files.write(&line, now).expect("write");
        files.write(&line, now).expect("write");
        assert_eq!(list(dir.path(), None).len(), 1, "80 bytes fit in 100");
        let first = files.current_path().map(Path::to_path_buf);
        files.write(&line, now).expect("write");
        let listed = list(dir.path(), None);
        assert_eq!(listed.len(), 2, "120 would not: {listed:?}");
        assert_eq!(listed.iter().map(|f| f.len).sum::<u64>(), 120);
        assert!(first.is_some_and(|path| path.exists()), "not renamed");
        assert_eq!(listed.iter().map(|f| f.n).collect::<Vec<_>>(), [1, 2]);
    }

    /// The size counts each line's newline, and a file exactly at its
    /// size is full, not over.
    #[test]
    fn the_size_counts_newlines_and_a_full_file_is_not_over() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut exact = RotatingFiles::new(set(dir.path(), Some("exact")));
        exact
            .write(&"a".repeat(49), SystemTime::now())
            .expect("write");
        exact
            .write(&"a".repeat(49), SystemTime::now())
            .expect("write");
        assert_eq!(list(dir.path(), None).len(), 1, "50 + 50 bytes fit in 100");

        let over = tempfile::tempdir().expect("tempdir");
        let mut files = RotatingFiles::new(set(over.path(), Some("over")));
        files
            .write(&"b".repeat(50), SystemTime::now())
            .expect("write");
        files
            .write(&"b".repeat(50), SystemTime::now())
            .expect("write");
        assert_eq!(list(over.path(), None).len(), 2, "51 + 51 bytes do not");
    }

    /// The header's bytes count toward the file's size.
    #[test]
    fn the_header_counts_toward_the_size() {
        let dir = tempfile::tempdir().expect("tempdir");
        let small = FileSet {
            max_file_bytes: 12,
            ..set(dir.path(), Some("b"))
        };
        let mut files = RotatingFiles::new(small).with_header(|_| "HHHH".to_owned());
        for _ in 0..3 {
            files.write("abc", SystemTime::now()).expect("write");
        }
        let listed = list(dir.path(), None);
        let first = std::fs::read_to_string(&listed[0].path).expect("read");
        assert_eq!(
            first, "HHHH\nabc\n",
            "5 + 4 bytes, and the next 4 would pass 12"
        );
        assert_eq!(listed.len(), 3);
    }

    #[test]
    fn debug_names_the_family_and_its_current_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut files = RotatingFiles::new(set(dir.path(), Some("b")));
        files.write("x", SystemTime::now()).expect("write");
        let debug = format!("{files:?}");
        assert!(debug.starts_with("RotatingFiles"), "{debug}");
        assert!(debug.contains("horde-worker.b.1.jsonl"), "{debug}");
    }

    #[test]
    fn an_oversized_line_still_lands_alone_in_a_fresh_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut files = RotatingFiles::new(set(dir.path(), Some("b")));
        files
            .write(&"y".repeat(500), SystemTime::now())
            .expect("write");
        files.write("z", SystemTime::now()).expect("write");
        let lens: Vec<u64> = list(dir.path(), None).iter().map(|f| f.len).collect();
        assert_eq!(lens, [501, 2]);
    }

    #[test]
    fn pruning_keeps_the_newest_count() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut files = RotatingFiles::new(set(dir.path(), Some("b")));
        let mut pruned = Vec::new();
        for _ in 0..6 {
            pruned.extend(
                files
                    .write(&"x".repeat(99), SystemTime::now())
                    .expect("write"),
            );
        }
        let left: Vec<u64> = list(dir.path(), None).iter().map(|f| f.n).collect();
        assert_eq!(left, [4, 5, 6]);
        assert_eq!(pruned.len(), 3);
    }

    #[test]
    fn numbering_continues_past_what_is_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut first = RotatingFiles::new(set(dir.path(), None));
        first.write("a", SystemTime::now()).expect("write");
        first
            .write(&"a".repeat(120), SystemTime::now())
            .expect("write");
        drop(first);
        let mut again = RotatingFiles::new(set(dir.path(), None));
        again.write("b", SystemTime::now()).expect("write");
        let ns: Vec<u64> = list(dir.path(), None).iter().map(|f| f.n).collect();
        assert_eq!(ns, [1, 2, 3], "a restart must not append to file 1");
    }

    fn file(n: u64, age_secs: u64, now: SystemTime) -> LogFile {
        LogFile {
            path: format!("/logs/w.b.{n}.jsonl").into(),
            prefix: "w".to_owned(),
            boot: Some("b".to_owned()),
            n,
            modified: now - Duration::from_secs(age_secs),
            len: 1,
        }
    }

    #[test]
    fn pruning_by_age_uses_the_injected_clock_and_spares_the_current_file() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let files = [file(1, 500, now), file(2, 100, now), file(3, 10, now)];
        let keep = Path::new("/logs/w.b.3.jsonl");
        assert!(prune_choice(&files, 8, None, now, keep).is_empty());
        assert_eq!(
            prune_choice(&files, 8, Some(Duration::from_secs(200)), now, keep),
            [files[0].path.clone()]
        );
        assert_eq!(
            prune_choice(&files, 8, Some(Duration::from_secs(100)), now, keep),
            [files[0].path.clone()],
            "exactly at the limit is not past it"
        );
        assert_eq!(
            prune_choice(&files, 8, Some(Duration::from_secs(1)), now, keep).len(),
            2,
            "the file just opened is never pruned, however old its clock says it is"
        );
        assert_eq!(
            prune_choice(&files, 2, None, now, keep),
            [files[0].path.clone()]
        );
        assert_eq!(prune_choice(&files, 0, None, now, keep).len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn directories_and_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = dir.path().join("logs");
        let mut files = RotatingFiles::new(set(&logs, Some("b")));
        files.write("x", SystemTime::now()).expect("write");
        let mode =
            |path: &Path| std::fs::metadata(path).expect("meta").permissions().mode() & 0o777;
        assert_eq!(mode(&logs), 0o700);
        let path = files.current_path().expect("open").to_path_buf();
        assert_eq!(mode(&path), 0o600);
    }

    #[test]
    fn every_new_file_starts_with_its_header() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut files =
            RotatingFiles::new(set(dir.path(), Some("b"))).with_header(|_| "H".to_owned());
        files
            .write(&"x".repeat(60), SystemTime::now())
            .expect("write");
        files
            .write(&"x".repeat(60), SystemTime::now())
            .expect("write");
        for file in list(dir.path(), None) {
            let text = std::fs::read_to_string(&file.path).expect("read");
            assert!(text.starts_with("H\n"), "{text:?}");
        }
    }

    #[test]
    fn a_discarded_handle_reopens_a_fresh_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut files = RotatingFiles::new(set(dir.path(), Some("b")));
        files.write("x", SystemTime::now()).expect("write");
        files.discard();
        assert!(files.current_path().is_none());
        files.write("y", SystemTime::now()).expect("write");
        assert_eq!(list(dir.path(), None).len(), 2);
    }
}
