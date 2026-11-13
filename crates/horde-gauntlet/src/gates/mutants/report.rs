//! Reading cargo-gamma's `gamma-report.json`.
//!
//! The report is the verdict's source, not gamma's console summary: it names
//! every mutant with its status and the reason for it, and it is versioned, so
//! a format change is refused instead of silently read as zero findings.

use std::collections::BTreeMap;

use serde::Deserialize;

/// The report schema this reader understands.
pub const SCHEMA: &str = "2";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    schema_version: String,
    files: BTreeMap<String, File>,
}

#[derive(Deserialize)]
struct File {
    mutants: Vec<Mutant>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Mutant {
    status: String,
    mutator_name: String,
    replacement: String,
    location: Location,
    #[serde(default)]
    status_reason: String,
}

#[derive(Deserialize)]
struct Location {
    start: Position,
}

#[derive(Deserialize)]
struct Position {
    line: u32,
    column: u32,
}

/// What a gamma run concluded, mutant by mutant, folded into counts.
///
/// `missed` is the gate's number: every mutant no test assertion rejected.
/// That is a survivor, but also a mutant whose tests timed out or ran out of
/// memory, and one no test reached at all. A timeout is not a kill — the same
/// mutant can time out on a loaded machine and survive on an idle one — so it
/// is counted against the change, never for it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tally {
    /// Mutants the report names.
    pub total: usize,
    /// Mutants a test assertion rejected.
    pub caught: usize,
    /// Mutants no test assertion rejected, for any reason.
    pub missed: usize,
    /// Of the missed: the tests ran to completion and passed.
    pub survived: usize,
    /// Of the missed: the tests did not finish in time.
    pub timeout: usize,
    /// Of the missed: the tests breached their memory ceiling.
    pub out_of_memory: usize,
    /// Of the missed: no test reaches the mutated code.
    pub uncovered: usize,
    /// Mutants that did not compile: not a finding about the tests.
    pub unviable: usize,
    /// Mutants gamma set aside — a source suppression, or a flaky kill.
    pub excluded: usize,
    /// Of the excluded: a test failed with the mutant and again without it.
    pub flaky: usize,
    /// One line per missed mutant: where, what, and why it counts.
    pub findings: Vec<String>,
}

impl Tally {
    /// Mutants that produced a verdict about the tests.
    pub fn scored(&self) -> usize {
        self.caught + self.missed
    }
}

/// Folds a gamma report into a [`Tally`].
///
/// # Errors
///
/// Returns a message when the report is not JSON this reader understands, is
/// at another schema version, or names a status this reader does not know. An
/// unknown status is refused rather than skipped: a new way to fail that the
/// reader drops is a missed mutant reported as nothing.
pub fn parse(text: &str) -> Result<Tally, String> {
    let report: Report = serde_json::from_str(text).map_err(|error| {
        format!("gamma-report.json is not a report this gate can read: {error}")
    })?;
    if report.schema_version != SCHEMA {
        return Err(format!(
            "gamma-report.json is schema {}, and this gate reads schema {SCHEMA}",
            report.schema_version
        ));
    }
    let mut tally = Tally::default();
    for (file, entry) in &report.files {
        for mutant in &entry.mutants {
            tally.total += 1;
            if count(&mut tally, mutant).map_err(|status| {
                format!("gamma-report.json: unknown status `{status}` for a mutant in {file}")
            })? {
                tally.missed += 1;
                tally.findings.push(finding(file, mutant));
            }
        }
    }
    Ok(tally)
}

/// Counts one mutant, returning whether it is missed.
fn count(tally: &mut Tally, mutant: &Mutant) -> Result<bool, String> {
    let reason = mutant.status_reason.as_str();
    match mutant.status.as_str() {
        "Killed" => {
            tally.caught += 1;
            Ok(false)
        }
        "Survived" | "Timeout" => {
            if mutant.status == "Timeout" || reason.starts_with("timed out: ") {
                tally.timeout += 1;
            } else if reason.starts_with("out of memory: ") {
                tally.out_of_memory += 1;
            } else {
                tally.survived += 1;
            }
            Ok(true)
        }
        "NoCoverage" => {
            tally.uncovered += 1;
            Ok(true)
        }
        "CompileError" => {
            tally.unviable += 1;
            Ok(false)
        }
        "Ignored" => {
            tally.excluded += 1;
            if reason.starts_with("flaky: ") {
                tally.flaky += 1;
            }
            Ok(false)
        }
        status => Err(status.to_owned()),
    }
}

/// One missed mutant, as a line someone can act on.
fn finding(file: &str, mutant: &Mutant) -> String {
    let reason = match mutant.status_reason.as_str() {
        "" => String::new(),
        reason => format!(" {reason}"),
    };
    let replacement = mutant
        .replacement
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "{file}:{}:{}: {} -> {replacement} [{}]{reason}",
        mutant.location.start.line,
        mutant.location.start.column,
        mutant.mutator_name,
        mutant.status,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real report: `horde-observe` before its survivors were killed.
    const SURVIVORS: &str = include_str!("../../../fixtures/gamma-report-survivors.json");

    fn report(mutants: &str) -> String {
        format!(
            r#"{{"schemaVersion":"2","files":{{"crates/a/src/lib.rs":{{"mutants":[{mutants}]}}}}}}"#
        )
    }

    fn mutant(status: &str, reason: &str) -> String {
        format!(
            r#"{{"status":"{status}","mutatorName":"relational.gt_to_ge","replacement":">=",
                "location":{{"start":{{"line":3,"column":7}}}},"statusReason":"{reason}"}}"#
        )
    }

    #[test]
    fn a_real_report_with_survivors_is_read() {
        let tally = parse(SURVIVORS).expect("the fixture is a real gamma report");
        assert_eq!(tally.total, 46);
        assert_eq!(tally.caught, 34);
        assert_eq!(tally.survived, 3);
        assert_eq!(tally.missed, 3);
        assert_eq!(tally.unviable, 9);
        assert_eq!(tally.findings.len(), 3);
        assert!(
            tally.findings.iter().any(|line| line
                .starts_with("crates/horde-observe/src/pipeline.rs:179:47: literal.str_to_empty")),
            "{:?}",
            tally.findings
        );
    }

    #[test]
    fn a_timeout_is_missed_and_counted_as_a_timeout() {
        let tally = parse(&report(&mutant("Timeout", ""))).expect("valid");
        assert_eq!((tally.missed, tally.timeout, tally.caught), (1, 1, 0));
    }

    #[test]
    fn a_survivor_whose_tests_timed_out_is_a_timeout_not_a_plain_survivor() {
        let tally = parse(&report(&mutant("Survived", "timed out: after 60s"))).expect("valid");
        assert_eq!((tally.missed, tally.timeout, tally.survived), (1, 1, 0));
    }

    #[test]
    fn running_out_of_memory_is_missed() {
        let tally = parse(&report(&mutant("Survived", "out of memory: 4 GiB"))).expect("valid");
        assert_eq!(
            (tally.missed, tally.out_of_memory, tally.survived),
            (1, 1, 0)
        );
    }

    #[test]
    fn an_uncovered_mutant_is_missed() {
        let tally = parse(&report(&mutant("NoCoverage", ""))).expect("valid");
        assert_eq!((tally.missed, tally.uncovered), (1, 1));
    }

    #[test]
    fn unviable_and_ignored_mutants_are_neither_caught_nor_missed() {
        let mutants = [
            mutant("CompileError", "the mutant does not compile"),
            mutant("Ignored", "flaky: failed again without the mutant"),
            mutant("Ignored", "suppressed"),
        ]
        .join(",");
        let tally = parse(&report(&mutants)).expect("valid");
        assert_eq!(tally.scored(), 0);
        assert_eq!((tally.unviable, tally.excluded, tally.flaky), (1, 2, 1));
        assert_eq!(tally.total, 3);
    }

    #[test]
    fn a_kill_is_caught() {
        let tally = parse(&report(&mutant("Killed", "failed `t`"))).expect("valid");
        assert_eq!((tally.caught, tally.missed, tally.scored()), (1, 0, 1));
        assert!(tally.findings.is_empty());
    }

    #[test]
    fn a_finding_names_the_place_the_mutation_and_the_status() {
        let tally = parse(&report(&mutant("Survived", ""))).expect("valid");
        assert_eq!(
            tally.findings,
            vec!["crates/a/src/lib.rs:3:7: relational.gt_to_ge -> >= [Survived]".to_owned()]
        );
    }

    #[test]
    fn an_unknown_status_is_refused_rather_than_dropped() {
        let message = parse(&report(&mutant("Pending", ""))).expect_err("unknown status");
        assert!(message.contains("unknown status `Pending`"), "{message}");
    }

    #[test]
    fn another_schema_version_is_refused() {
        let message = parse(r#"{"schemaVersion":"3","files":{}}"#).expect_err("schema 3");
        assert!(message.contains("schema 3"), "{message}");
    }

    #[test]
    fn text_that_is_not_a_report_is_refused() {
        assert!(parse("not json").is_err());
    }
}
