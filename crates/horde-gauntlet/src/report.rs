//! Rendering a run into a terminal table and into markdown.
//!
//! Both renderers walk the same rows, so the step summary in CI and the table
//! on a laptop cannot disagree about what happened.

use std::fmt::Write as _;

use crate::gate::GateResult;

/// Column headings, in order.
const HEADERS: [&str; 5] = ["gate", "enforcement", "result", "time", "detail"];

/// What a row says when the gate never reported.
const NO_RESULT: &str = "no result";

/// Turns one result into its display cells.
pub fn row_cells(result: &GateResult) -> [String; 5] {
    let (verdict, detail) = match &result.outcome {
        Some(outcome) => (outcome.label().to_owned(), outcome.detail().to_owned()),
        None => (
            NO_RESULT.to_owned(),
            "the gate never reported — it crashed, or the run stopped first".to_owned(),
        ),
    };
    [
        result.name.clone(),
        result.enforcement.label().to_owned(),
        verdict,
        format_duration(result.millis),
        first_line(&detail),
    ]
}

/// Renders a duration for the time column.
pub fn format_duration(millis: Option<u64>) -> String {
    match millis {
        Some(millis) => format!("{}.{:02}s", millis / 1000, (millis % 1000) / 10),
        None => "-".to_owned(),
    }
}

/// Keeps a table cell to one line; the full text lives in the gate's log.
fn first_line(text: &str) -> String {
    match text.split_once('\n') {
        Some((head, _)) => format!("{head} …"),
        None => text.to_owned(),
    }
}

fn column_widths(rows: &[[String; 5]]) -> [usize; 5] {
    let mut widths = [0usize; 5];
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    widths
}

fn all_rows(results: &[GateResult]) -> Vec<[String; 5]> {
    let mut rows = vec![HEADERS.map(str::to_owned)];
    rows.extend(results.iter().map(row_cells));
    rows
}

/// Renders the run as an aligned plain-text table.
pub fn render_table(results: &[GateResult]) -> String {
    let rows = all_rows(results);
    let widths = column_widths(&rows);
    let mut out = String::new();
    for (index, row) in rows.iter().enumerate() {
        out.push_str(&pad_row(row, &widths));
        out.push('\n');
        if index == 0 {
            out.push_str(&separator(&widths));
            out.push('\n');
        }
    }
    out.push_str(&verdict_line(results));
    out.push('\n');
    out
}

fn pad_row(row: &[String; 5], widths: &[usize; 5]) -> String {
    let mut line = String::new();
    let last = row.len().saturating_sub(1);
    for (index, (cell, width)) in row.iter().zip(widths).enumerate() {
        if index > 0 {
            line.push_str("  ");
        }
        line.push_str(cell);
        if index != last {
            for _ in cell.chars().count()..*width {
                line.push(' ');
            }
        }
    }
    line
}

fn separator(widths: &[usize; 5]) -> String {
    let dashes: Vec<String> = widths.iter().map(|width| "-".repeat(*width)).collect();
    dashes.join("  ")
}

/// The one-line summary under the table.
pub fn verdict_line(results: &[GateResult]) -> String {
    let red: Vec<&str> = results
        .iter()
        .filter(|result| result.reds_run())
        .map(|result| result.name.as_str())
        .collect();
    if results.is_empty() {
        return "no gates ran".to_owned();
    }
    if red.is_empty() {
        return format!("{} gates green", results.len());
    }
    format!(
        "{} of {} gates red: {}",
        red.len(),
        results.len(),
        red.join(", ")
    )
}

/// Renders the run as a markdown table, for a CI step summary.
pub fn render_markdown(results: &[GateResult]) -> String {
    let mut out = String::from("| gate | enforcement | result | time | detail |\n");
    out.push_str("| --- | --- | --- | --- | --- |\n");
    for result in results {
        let cells = row_cells(result);
        let escaped: Vec<String> = cells.iter().map(|cell| escape_pipes(cell)).collect();
        let _ = writeln!(out, "| {} |", escaped.join(" | "));
    }
    let _ = writeln!(out, "\n{}", verdict_line(results));
    out
}

fn escape_pipes(text: &str) -> String {
    text.replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::{Enforcement, GateOutcome, GateResult};

    fn result(name: &str, outcome: Option<GateOutcome>, millis: Option<u64>) -> GateResult {
        GateResult {
            name: name.to_owned(),
            enforcement: Enforcement::Blocking,
            outcome,
            millis,
        }
    }

    #[test]
    fn a_missing_outcome_renders_as_a_no_result_row_rather_than_disappearing() {
        let cells = row_cells(&result("mutants", None, None));
        assert_eq!(cells[0], "mutants");
        assert_eq!(cells[2], "no result");
        assert_eq!(cells[3], "-");
        assert!(cells[4].contains("never reported"), "{}", cells[4]);
    }

    #[test]
    fn skipped_renders_differently_from_pass() {
        let skipped = row_cells(&result(
            "mutants",
            Some(GateOutcome::skipped("no diff")),
            None,
        ));
        let passed = row_cells(&result("mutants", Some(GateOutcome::pass("no diff")), None));
        assert_eq!(skipped[2], "skipped");
        assert_eq!(passed[2], "pass");
    }

    #[test]
    fn duration_is_rendered_to_hundredths_of_a_second() {
        assert_eq!(format_duration(Some(0)), "0.00s");
        assert_eq!(format_duration(Some(1234)), "1.23s");
        assert_eq!(format_duration(Some(60_500)), "60.50s");
        assert_eq!(format_duration(None), "-");
    }

    #[test]
    fn a_multi_line_detail_is_truncated_to_its_first_line_in_the_table() {
        let cells = row_cells(&result(
            "crap",
            Some(GateOutcome::fail(
                "2 functions over 15\nfoo: 18.2\nbar: 22.9",
            )),
            None,
        ));
        assert_eq!(cells[4], "2 functions over 15 …");
    }

    #[test]
    fn the_whole_table_renders_exactly_like_this() {
        let table = render_table(&[
            result("fmt", Some(GateOutcome::pass("clean")), Some(80)),
            result(
                "duplication",
                Some(GateOutcome::fail("2 blocks")),
                Some(1500),
            ),
            result("mutants", None, None),
        ]);
        assert_eq!(
            table,
            "\
gate         enforcement  result     time   detail\n\
-----------  -----------  ---------  -----  --------------------------------------------------------------\n\
fmt          blocking     pass       0.08s  clean\n\
duplication  blocking     fail       1.50s  2 blocks\n\
mutants      blocking     no result  -      the gate never reported — it crashed, or the run stopped first\n\
2 of 3 gates red: duplication, mutants\n"
        );
    }

    #[test]
    fn columns_are_padded_to_the_widest_cell() {
        let table = render_table(&[
            result("fmt", Some(GateOutcome::pass("clean")), Some(100)),
            result("duplication", Some(GateOutcome::pass("clean")), Some(100)),
        ]);
        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(lines[0].find("enforcement"), lines[2].find("blocking"));
        assert_eq!(lines[2].find("blocking"), lines[3].find("blocking"));
    }

    #[test]
    fn the_table_has_a_separator_under_the_header() {
        let table = render_table(&[result("fmt", Some(GateOutcome::pass("clean")), Some(10))]);
        let lines: Vec<&str> = table.lines().collect();
        assert!(lines[1].starts_with("---"), "{table}");
    }

    #[test]
    fn a_green_run_says_so() {
        let results = [
            result("fmt", Some(GateOutcome::pass("clean")), Some(10)),
            result("test", Some(GateOutcome::skipped("nothing")), Some(10)),
        ];
        assert_eq!(verdict_line(&results), "2 gates green");
    }

    #[test]
    fn a_red_run_names_every_red_gate() {
        let results = [
            result("fmt", Some(GateOutcome::pass("clean")), Some(10)),
            result("crap", Some(GateOutcome::fail("two over")), Some(10)),
            result("mutants", None, None),
        ];
        assert_eq!(verdict_line(&results), "2 of 3 gates red: crap, mutants");
    }

    #[test]
    fn an_empty_run_is_not_reported_as_green() {
        assert_eq!(verdict_line(&[]), "no gates ran");
    }

    #[test]
    fn markdown_has_a_header_a_divider_and_one_row_per_gate() {
        let markdown = render_markdown(&[
            result("fmt", Some(GateOutcome::pass("clean")), Some(10)),
            result("crap", None, None),
        ]);
        let lines: Vec<&str> = markdown.lines().collect();
        assert_eq!(lines[0], "| gate | enforcement | result | time | detail |");
        assert_eq!(lines[1], "| --- | --- | --- | --- | --- |");
        assert_eq!(lines[2], "| fmt | blocking | pass | 0.01s | clean |");
        assert!(
            lines[3].contains("| crap | blocking | no result | - |"),
            "{markdown}"
        );
        assert!(markdown.contains("1 of 2 gates red: crap"), "{markdown}");
    }

    #[test]
    fn markdown_escapes_pipes_so_a_detail_cannot_forge_a_column() {
        let markdown =
            render_markdown(&[result("clippy", Some(GateOutcome::fail("a | b")), Some(10))]);
        assert!(markdown.contains(r"a \| b"), "{markdown}");
    }

    #[test]
    fn the_table_ends_with_the_verdict_line() {
        let table = render_table(&[result("fmt", Some(GateOutcome::fail("dirty")), Some(10))]);
        assert!(table.ends_with("1 of 1 gates red: fmt\n"), "{table}");
    }
}
