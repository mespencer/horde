//! Reaping the product processes a test leaves behind.
//!
//! [`crate::process::SpawnedProcess`] is kill-on-drop, but that reaches
//! only its own child, and only when a drop runs. Two leaks survive it:
//!
//! * **Grandchildren on drop.** A spawned `horde up` owns a server and a
//!   worker; killing `up` orphans both. The drop now kills the child's whole
//!   process tree first ([`reap_descendants`]).
//! * **A test process that never drops.** cargo-mutants kills a timed-out
//!   test binary outright, so no destructor runs and every daemon it started
//!   is adopted by init. The first spawn in each test process sweeps for
//!   those ([`reap_orphans_once`]): product binaries from *this* target
//!   directory whose parent is init. A live sibling suite's daemons still
//!   have their test process as parent, so they are never touched.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// The product binaries a sweep may reap.
pub const PRODUCTS: &[&str] = &["horde-server", "horde-worker", "horde"];

/// One row of the process table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRow {
    pub pid: u32,
    pub ppid: u32,
    /// The full command line; its first word is the executable as invoked.
    pub command: String,
}

/// Parses `ps -A -o pid=,ppid=,command=` output, skipping rows it cannot
/// read rather than failing: a sweep is best effort by nature.
#[must_use]
pub fn parse_table(text: &str) -> Vec<ProcessRow> {
    text.lines().filter_map(parse_row).collect()
}

fn parse_row(line: &str) -> Option<ProcessRow> {
    let mut words = line.split_whitespace();
    let pid = words.next()?.parse().ok()?;
    let ppid = words.next()?.parse().ok()?;
    let command = words.collect::<Vec<_>>().join(" ");
    Some(ProcessRow { pid, ppid, command })
}

/// Every process below `root` in `table`, deepest first — the order that
/// stops a parent from respawning or reparenting what it supervises.
#[must_use]
pub fn descendants(table: &[ProcessRow], root: u32) -> Vec<u32> {
    let mut found = Vec::new();
    let mut frontier = vec![root];
    while let Some(parent) = frontier.pop() {
        for row in table.iter().filter(|row| row.ppid == parent) {
            // A pid seen twice would loop: tables are snapshots, and a
            // corrupt one must not hang the drop that reads it.
            if row.pid != root && !found.contains(&row.pid) {
                found.push(row.pid);
                frontier.push(row.pid);
            }
        }
    }
    found.reverse();
    found
}

/// The orphans a sweep reaps: adopted by init (`ppid` 1) and running one of
/// `names` from under `root`.
#[must_use]
pub fn orphans_matching(table: &[ProcessRow], root: &Path, names: &[&str]) -> Vec<u32> {
    table
        .iter()
        .filter(|row| row.ppid == 1 && runs_from(&row.command, root, names))
        .map(|row| row.pid)
        .collect()
}

/// Whether `command`'s executable is one of `names` under `root`.
fn runs_from(command: &str, root: &Path, names: &[&str]) -> bool {
    let Some(executable) = command.split_whitespace().next().map(Path::new) else {
        return false;
    };
    let named = executable
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| names.contains(&name));
    named && executable.starts_with(root)
}

/// The live process table, or empty when `ps` cannot run.
#[must_use]
pub fn process_table() -> Vec<ProcessRow> {
    std::process::Command::new("ps")
        .args(["-A", "-o", "pid=,ppid=,command="])
        .output()
        .map(|output| parse_table(&String::from_utf8_lossy(&output.stdout)))
        .unwrap_or_default()
}

/// SIGKILLs every pid, ignoring the already-gone.
fn kill_all(pids: &[u32]) {
    if pids.is_empty() {
        return;
    }
    let _best_effort = std::process::Command::new("kill")
        .arg("-KILL")
        .args(pids.iter().map(ToString::to_string))
        .stderr(std::process::Stdio::null())
        .status();
}

/// Kills every descendant of `root` (not `root` itself), returning them.
pub fn reap_descendants(root: u32) -> Vec<u32> {
    let pids = descendants(&process_table(), root);
    kill_all(&pids);
    pids
}

/// Kills the orphans [`orphans_matching`] finds, returning them.
pub fn reap_orphans(root: &Path, names: &[&str]) -> Vec<u32> {
    let pids = orphans_matching(&process_table(), root, names);
    kill_all(&pids);
    pids
}

/// The target directory a test executable runs from:
/// `target/<profile>/deps/<test>` → `target`.
#[must_use]
pub fn target_root(test_executable: &Path) -> Option<PathBuf> {
    let mut root = test_executable.parent()?.to_path_buf();
    if root.ends_with("deps") {
        root.pop();
    }
    root.parent().map(Path::to_path_buf)
}

/// Sweeps this target directory's orphaned products, once per test process.
pub fn reap_orphans_once() {
    static SWEPT: OnceLock<()> = OnceLock::new();
    SWEPT.get_or_init(|| {
        if let Some(root) = std::env::current_exe()
            .ok()
            .as_deref()
            .and_then(target_root)
        {
            reap_orphans(&root, PRODUCTS);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pid: u32, ppid: u32, command: &str) -> ProcessRow {
        ProcessRow {
            pid,
            ppid,
            command: command.to_owned(),
        }
    }

    #[test]
    fn the_table_parses_and_unreadable_rows_are_skipped() {
        let table = parse_table(
            "    1     0 /sbin/launchd\n  412     1 /t/debug/horde-server --serve --bind x\n\
             garbage\n  9  \n",
        );
        assert_eq!(
            table,
            vec![
                row(1, 0, "/sbin/launchd"),
                row(412, 1, "/t/debug/horde-server --serve --bind x"),
            ]
        );
    }

    #[test]
    fn descendants_are_the_whole_tree_deepest_first() {
        let table = [
            row(10, 1, "up"),
            row(11, 10, "server"),
            row(12, 10, "worker"),
            row(13, 12, "adapter"),
            row(20, 1, "bystander"),
            row(10, 13, "a corrupt cycle back to the root"),
        ];
        let tree = descendants(&table, 10);
        assert_eq!(tree.len(), 3, "{tree:?}");
        for pid in [11, 12, 13] {
            assert!(tree.contains(&pid), "{tree:?}");
        }
        let position = |pid| tree.iter().position(|&p| p == pid);
        assert!(
            position(13) < position(12),
            "a grandchild dies before its parent"
        );
        assert!(descendants(&table, 20).is_empty());
    }

    #[test]
    fn only_init_adopted_products_from_this_target_are_orphans() {
        let root = Path::new("/w/target");
        let table = [
            row(1, 0, "/sbin/launchd"),
            row(30, 1, "/w/target/debug/horde-server --serve"),
            row(
                31,
                1,
                "/w/target/llvm-cov-target/debug/horde-worker --join x",
            ),
            row(32, 77, "/w/target/debug/horde-server --serve"),
            row(33, 1, "/other/target/debug/horde-server --serve"),
            row(34, 1, "/w/target/debug/deps/cli_suite-abc"),
            row(35, 1, "/w/target/debug/horde"),
        ];
        assert_eq!(orphans_matching(&table, root, PRODUCTS), vec![30, 31, 35]);
    }

    #[test]
    fn the_target_root_is_above_the_profile() {
        assert_eq!(
            target_root(Path::new("/w/target/debug/deps/suite-1")),
            Some(PathBuf::from("/w/target"))
        );
        assert_eq!(
            target_root(Path::new("/w/target/debug/suite")),
            Some(PathBuf::from("/w/target"))
        );
        assert_eq!(target_root(Path::new("suite")), None);
    }

    /// Whether `pid` is still a live (non-zombie) process.
    fn alive(pid: u32) -> bool {
        std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .is_ok_and(|out| {
                let stat = String::from_utf8_lossy(&out.stdout);
                !stat.trim().is_empty() && !stat.trim().starts_with('Z')
            })
    }

    async fn gone_within_ten_seconds(pid: u32) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            if !alive(pid) {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        false
    }

    /// A real orphan — a `sleep` its shell abandoned to init — is reaped by
    /// a sweep over the directory it runs from, and nothing else is.
    #[tokio::test]
    async fn a_real_orphan_is_reaped_by_its_directory() {
        let scratch = tempfile::tempdir().expect("scratch");
        let sleeper = scratch.path().join("sleep");
        std::os::unix::fs::symlink("/bin/sleep", &sleeper).expect("a sleep under the root");
        let shell = tokio::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(format!(
                "'{}' 60 >/dev/null 2>&1 & echo $!",
                sleeper.display()
            ))
            .output()
            .await
            .expect("the shell runs");
        let orphan: u32 = String::from_utf8_lossy(&shell.stdout)
            .trim()
            .parse()
            .expect("the orphan's pid");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !process_table()
            .iter()
            .any(|row| row.pid == orphan && row.ppid == 1)
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "init never adopted it"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(reap_orphans(Path::new("/nowhere"), &["sleep"]).is_empty());
        assert_eq!(reap_orphans(scratch.path(), &["sleep"]), vec![orphan]);
        assert!(
            gone_within_ten_seconds(orphan).await,
            "the orphan was reaped"
        );
    }

    /// Dropping a spawned process takes its grandchildren with it.
    #[tokio::test]
    async fn dropping_a_spawned_process_reaps_its_tree() {
        let spawned = crate::process::SpawnedProcess::spawn(
            "tree",
            Path::new("/bin/sh"),
            &["-c", "sleep 60 & echo grandchild=$!; wait"],
            &[],
        )
        .expect("the shell spawns");
        let seen = spawned
            .await_stdout("the grandchild's pid", |out| out.contains("grandchild="))
            .await
            .expect("the pid is announced");
        let grandchild: u32 = crate::process::value_after(&seen, "grandchild=")
            .and_then(|pid| pid.parse().ok())
            .expect("a pid");
        assert!(alive(grandchild));
        drop(spawned);
        assert!(
            gone_within_ten_seconds(grandchild).await,
            "the grandchild outlived the drop"
        );
    }

    #[test]
    fn a_sweep_runs_and_an_empty_kill_is_a_no_op() {
        reap_orphans_once();
        reap_orphans_once();
        kill_all(&[]);
        assert!(reap_descendants(u32::MAX).is_empty());
    }
}
