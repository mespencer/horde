//! Reading the process table — the harmless half of the orphan sweep.
//!
//! This module only *describes* processes; it never chooses or signals one.
//! It is mutated like any other code: a mutant here can at worst hand the
//! sweep a wrong or empty table, or a wrong adopter, and every row the sweep
//! picks from it is re-checked by [`crate::reap`]'s selection (a product
//! binary run from under this test's own target directory, leading its own
//! group) and guard before anything is sent.
//!
//! # Who adopts an orphan
//!
//! An orphan is not always adopted by pid 1. Linux re-parents it to the
//! nearest live ancestor that set `PR_SET_CHILD_SUBREAPER`: `systemd --user`
//! for anything in a desktop or ssh session, a container's `tini` or
//! `dumb-init`, and so on. Nothing in `/proc` says which ancestor that is,
//! and `PR_GET_CHILD_SUBREAPER` only answers for the calling process, so
//! walking the ancestry cannot find it. [`adopter`] asks the kernel instead:
//! it abandons a harmless sentinel and reads who took it in. That is the
//! process an orphaned product of this test run is re-parented to, by the
//! same rule, on every platform — on macOS, which has no subreapers, the
//! answer is launchd, pid 1.

/// One row of the process table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRow {
    pub pid: u32,
    pub ppid: u32,
    /// The process group; equal to `pid` for a group leader.
    pub pgid: u32,
    /// The full command line; its first word is the executable as invoked.
    pub command: String,
}

/// Parses `ps -A -o pid=,ppid=,pgid=,command=` output, skipping rows it
/// cannot read rather than failing: a sweep is best effort by nature.
#[must_use]
pub fn parse_table(text: &str) -> Vec<ProcessRow> {
    text.lines().filter_map(parse_row).collect()
}

fn parse_row(line: &str) -> Option<ProcessRow> {
    let mut words = line.split_whitespace();
    let pid = words.next()?.parse().ok()?;
    let ppid = words.next()?.parse().ok()?;
    let pgid = words.next()?.parse().ok()?;
    let command = words.collect::<Vec<_>>().join(" ");
    Some(ProcessRow {
        pid,
        ppid,
        pgid,
        command,
    })
}

/// The live process table, or empty when `ps` cannot run.
#[must_use]
pub fn process_table() -> Vec<ProcessRow> {
    std::process::Command::new("ps")
        .args(["-A", "-o", "pid=,ppid=,pgid=,command="])
        .output()
        .map(|output| parse_table(&String::from_utf8_lossy(&output.stdout)))
        .unwrap_or_default()
}

/// The process that adopted `sentinel` once `abandoned_by`, its parent,
/// exited — or `None` when the table cannot say.
///
/// Never `me`: a sweep selecting this process's children would choose the
/// live products it is still using. Never `abandoned_by`: a sentinel still
/// listed under it was not orphaned yet. Never 0: no process adopts as 0;
/// that is the kernel, or an adopter outside this pid namespace that the
/// sweep could not name anyway.
#[must_use]
pub fn adopted_by(table: &[ProcessRow], sentinel: u32, abandoned_by: u32, me: u32) -> Option<u32> {
    let parent = table.iter().find(|row| row.pid == sentinel)?.ppid;
    (parent != 0 && parent != abandoned_by && parent != me).then_some(parent)
}

/// The process that adopts this process's orphans: pid 1, or the nearest
/// subreaper above us. `None` when it cannot be learned.
///
/// The sentinel is a `cat` reading this process's pipe, backgrounded by a
/// shell that exits at once. It is read from the table after the shell is
/// waited for — the kernel re-parents an orphan before its parent's exit is
/// reported — and it exits on its own when the pipe closes at return, on
/// every path. Nothing here sends a signal.
#[must_use]
pub fn adopter() -> Option<u32> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    // Without job control a background job's stdin is /dev/null, so the
    // pipe is passed to it on fd 3 explicitly.
    let mut shell = Command::new("/bin/sh")
        .args(["-c", "exec 3<&0; cat <&3 >/dev/null 2>&1 & echo $!"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    // Held apart from `shell`, whose `wait` would close it — ending the
    // sentinel before it can be read.
    let pipe = shell.stdin.take();
    let mut said = String::new();
    let read = shell
        .stdout
        .take()
        .map(|mut out| out.read_to_string(&mut said));
    let waited = shell.wait();
    let sentinel = said.trim().parse().ok();
    let found = match (read, waited, sentinel) {
        (Some(Ok(_)), Ok(_), Some(sentinel)) => {
            adopted_by(&process_table(), sentinel, shell.id(), std::process::id())
        }
        _ => None,
    };
    // Closing the pipe ends the sentinel.
    drop(pipe);
    found
}

#[cfg(test)]
mod tests {
    use super::{ProcessRow, adopted_by, adopter, parse_table, process_table};

    fn row(pid: u32, ppid: u32, pgid: u32, command: &str) -> ProcessRow {
        ProcessRow {
            pid,
            ppid,
            pgid,
            command: command.to_owned(),
        }
    }

    #[test]
    fn the_table_parses_and_unreadable_rows_are_skipped() {
        let table = parse_table(
            "    1     0     1 /sbin/launchd\n  412     1   412 /t/debug/horde-server --serve\n\
             garbage\n  9  \n  10 1 \n",
        );
        assert_eq!(
            table,
            vec![
                row(1, 0, 1, "/sbin/launchd"),
                row(412, 1, 412, "/t/debug/horde-server --serve"),
            ]
        );
    }

    /// The live table holds this very process, with its real parent and
    /// group — the columns are read in the order `ps` was asked for them.
    #[test]
    fn the_live_table_holds_this_process() {
        let me = std::process::id();
        let table = process_table();
        let found = table
            .iter()
            .find(|row| row.pid == me)
            .expect("this process is in the table");
        assert_eq!(found.ppid, std::os::unix::process::parent_id());
        assert_eq!(
            found.pgid,
            u32::try_from(nix::unistd::getpgrp().as_raw()).expect("a positive pgid")
        );
    }

    #[test]
    fn the_adopter_is_the_sentinels_new_parent() {
        let table = [
            row(1, 0, 1, "/sbin/init"),
            row(2009, 1, 2009, "/usr/lib/systemd/systemd --user"),
            row(500, 2009, 400, "cat"),
            row(501, 1, 400, "cat"),
            row(502, 400, 400, "cat"),
            row(503, 77, 400, "cat"),
            row(504, 0, 400, "cat"),
        ];
        // A subreaper took it in.
        assert_eq!(adopted_by(&table, 500, 400, 77), Some(2009));
        // Init took it in.
        assert_eq!(adopted_by(&table, 501, 400, 77), Some(1));
        // Still under the shell: not orphaned yet.
        assert_eq!(adopted_by(&table, 502, 400, 77), None);
        // Under this very process: never an adopter.
        assert_eq!(adopted_by(&table, 503, 400, 77), None);
        // Parent 0 names no process.
        assert_eq!(adopted_by(&table, 504, 400, 77), None);
        // Gone from the table.
        assert_eq!(adopted_by(&table, 999, 400, 77), None);
    }

    /// On this host the sentinel is really adopted, by someone other than
    /// this process. The real orphan in `reap`'s tests checks the answer
    /// against a second, independent abandonment.
    #[test]
    fn the_live_adopter_is_learned() {
        let found = adopter().expect("the sentinel is adopted");
        assert_ne!(found, std::process::id());
        assert!(
            process_table().iter().any(|row| row.pid == found),
            "the adopter {found} is a live process"
        );
    }
}
