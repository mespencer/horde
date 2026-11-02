//! Reading the process table — the harmless half of the orphan sweep.
//!
//! This module only *describes* processes; it never chooses or signals one.
//! It is mutated like any other code: a mutant here can at worst hand the
//! sweep a wrong or empty table, and every row the sweep picks from it is
//! re-checked by [`crate::reap`]'s selection and guard before anything is
//! sent.

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

#[cfg(test)]
mod tests {
    use super::{ProcessRow, parse_table, process_table};

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
}
