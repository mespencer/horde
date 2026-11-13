//! A transcript laid out as terminal rows at a given width.
//!
//! Shared by the renderer (which draws the rows) and the App (which clamps
//! scrolling to how many rows there are) — one computation, so the scroll
//! ceiling and the picture can never disagree.

use ratatui::style::Style;
use ratatui::text::{Line, Span};

use crate::theme;
use crate::transcript::{Block, Settled, Speaker, Tone, Transcript};
use crate::wrap::{clip, wrap};

/// How many result lines a collapsed tool call shows.
const COLLAPSED_RESULT_LINES: usize = 3;

/// Every row of `transcript` at `width` cells, blocks separated by a blank
/// row. `details` expands tool input and output in full.
#[must_use]
pub fn transcript_lines(transcript: &Transcript, details: bool, width: u16) -> Vec<Line<'static>> {
    let width = usize::from(width).max(8);
    let mut rows = Vec::new();
    for block in &transcript.blocks {
        if !rows.is_empty() {
            rows.push(Line::raw(""));
        }
        block_lines(&mut rows, block, details, width);
    }
    rows
}

/// Rows for one block.
fn block_lines(rows: &mut Vec<Line<'static>>, block: &Block, details: bool, width: usize) {
    match block {
        Block::Message { from, text, note } => message_lines(rows, from, text, *note, width),
        Block::Assistant { text, .. } => {
            marked(rows, ("● ", theme::PLAIN), text.trim(), theme::PLAIN, width);
        }
        Block::Thinking { text, .. } => thinking_lines(rows, text, details, width),
        Block::Tool {
            name,
            input,
            result,
            ..
        } => tool_lines(rows, name, input, result.as_deref(), details, width),
        Block::Prompt {
            summary, settled, ..
        } => prompt_lines(rows, summary, settled.as_ref(), width),
        Block::Child { text } => marked(rows, ("↳ ", theme::focus()), text, theme::dim(), width),
        Block::Reported { status, summary } => {
            let style = if *status == "success" {
                theme::ok()
            } else {
                theme::warn()
            };
            marked(
                rows,
                ("◼ ", style),
                &format!("result {status}: {summary}"),
                theme::PLAIN,
                width,
            );
        }
        Block::Notice { tone, text } => notice_lines(rows, *tone, text, width),
    }
}

fn message_lines(
    rows: &mut Vec<Line<'static>>,
    from: &Speaker,
    text: &str,
    note: bool,
    width: usize,
) {
    let label = match (from, note) {
        (Speaker::Human, false) => String::new(),
        (Speaker::Human, true) => "note: ".to_owned(),
        (Speaker::Agent(name), false) => format!("{name}: "),
        (Speaker::Agent(name), true) => format!("note from {name}: "),
        (Speaker::Server, _) => "server: ".to_owned(),
    };
    marked(
        rows,
        ("› ", theme::dim()),
        &format!("{label}{text}"),
        theme::bold(),
        width,
    );
}

fn thinking_lines(rows: &mut Vec<Line<'static>>, text: &str, details: bool, width: usize) {
    let shown = if details {
        text.trim().to_owned()
    } else {
        format!("Thinking… {}", clip(text.trim(), width.saturating_sub(14)))
    };
    marked(
        rows,
        ("✻ ", theme::thinking()),
        &shown,
        theme::thinking(),
        width,
    );
}

fn tool_lines(
    rows: &mut Vec<Line<'static>>,
    name: &str,
    input: &str,
    result: Option<&str>,
    details: bool,
    width: usize,
) {
    if !name.is_empty() {
        let bullet = if result.is_some() {
            theme::ok()
        } else {
            theme::warn()
        };
        let head = format!("{name}({})", tool_summary(input));
        marked(rows, ("● ", bullet), &head, theme::bold(), width);
        if details && !input.is_empty() {
            indented(rows, &pretty_input(input), theme::dim(), width);
        }
    }
    match result {
        Some(text) => result_lines(rows, text, details, width),
        None => indented(rows, "running…", theme::dim(), width),
    }
}

/// A tool result under its call: the first few lines, then a count of the
/// rest, unless details are on.
fn result_lines(rows: &mut Vec<Line<'static>>, text: &str, details: bool, width: usize) {
    let all: Vec<&str> = text.trim_end().lines().collect();
    let shown = if details {
        all.len()
    } else {
        all.len().min(COLLAPSED_RESULT_LINES)
    };
    let mut body = all[..shown].join("\n");
    if body.is_empty() {
        body = "(no output)".to_owned();
    }
    let hidden = all.len() - shown;
    if hidden > 0 {
        body.push_str(&format!("\n… +{hidden} lines (ctrl-o to expand)"));
    }
    let inner = width.saturating_sub(4);
    for (index, row) in wrap(&body, inner).into_iter().enumerate() {
        let gutter = if index == 0 { "  ⎿ " } else { "    " };
        rows.push(Line::from(vec![
            Span::styled(gutter, theme::dim()),
            Span::styled(row, theme::dim()),
        ]));
    }
}

fn prompt_lines(
    rows: &mut Vec<Line<'static>>,
    summary: &str,
    settled: Option<&Settled>,
    width: usize,
) {
    let (marker, text) = match settled {
        None => (("◆ ", theme::needs_you()), format!("needs you: {summary}")),
        Some(Settled::Allowed) => (("✓ ", theme::ok()), format!("allowed: {summary}")),
        Some(Settled::Denied) => (("✗ ", theme::error()), format!("denied: {summary}")),
        Some(Settled::Answered(answers)) => (("✓ ", theme::ok()), format!("answered: {answers}")),
    };
    marked(rows, marker, &text, theme::PLAIN, width);
}

fn notice_lines(rows: &mut Vec<Line<'static>>, tone: Tone, text: &str, width: usize) {
    let marker = match tone {
        Tone::Info => ("· ", theme::dim()),
        Tone::Warn => ("⚠ ", theme::warn()),
        Tone::Error => ("✗ ", theme::error()),
    };
    let style = if tone == Tone::Info {
        theme::dim()
    } else {
        marker.1
    };
    marked(rows, marker, text, style, width);
}

/// A paragraph behind a two-cell marker, continuation rows indented to match.
fn marked(
    rows: &mut Vec<Line<'static>>,
    marker: (&'static str, Style),
    text: &str,
    style: Style,
    width: usize,
) {
    for (index, row) in wrap(text, width.saturating_sub(2)).into_iter().enumerate() {
        let gutter = if index == 0 { marker.0 } else { "  " };
        rows.push(Line::from(vec![
            Span::styled(gutter, marker.1),
            Span::styled(row, style),
        ]));
    }
}

/// A paragraph indented under its block's marker.
fn indented(rows: &mut Vec<Line<'static>>, text: &str, style: Style, width: usize) {
    for row in wrap(text, width.saturating_sub(4)) {
        rows.push(Line::from(vec![
            Span::raw("    "),
            Span::styled(row, style),
        ]));
    }
}

/// The argument a human recognizes a call by: the command, the path, the
/// pattern — or the arguments themselves, clipped, when none of those exist.
#[must_use]
pub fn tool_summary(input: &str) -> String {
    const KEYS: [&str; 8] = [
        "command",
        "file_path",
        "path",
        "pattern",
        "url",
        "query",
        "description",
        "prompt",
    ];
    let parsed: Option<serde_json::Value> = serde_json::from_str(input).ok();
    let picked = parsed.as_ref().and_then(|value| {
        KEYS.iter()
            .find_map(|key| value.get(key).and_then(serde_json::Value::as_str))
            .or_else(|| first_string(value))
    });
    clip(picked.unwrap_or(input), 60)
}

fn first_string(value: &serde_json::Value) -> Option<&str> {
    value
        .as_object()?
        .values()
        .find_map(serde_json::Value::as_str)
}

/// Tool input pretty-printed when it is JSON, verbatim otherwise.
fn pretty_input(input: &str) -> String {
    serde_json::from_str::<serde_json::Value>(input)
        .ok()
        .and_then(|value| serde_json::to_string_pretty(&value).ok())
        .unwrap_or_else(|| input.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{pretty_input, tool_summary, transcript_lines};
    use crate::transcript::{Block, Settled, Speaker, Tone, Transcript};

    fn text_of(transcript: &Transcript, details: bool, width: u16) -> Vec<String> {
        transcript_lines(transcript, details, width)
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect()
            })
            .collect()
    }

    fn of(blocks: Vec<Block>) -> Transcript {
        Transcript {
            blocks: blocks.into(),
        }
    }

    #[test]
    fn a_tool_call_names_its_argument_and_collapses_long_results() {
        let transcript = of(vec![Block::Tool {
            turn: "t".to_owned(),
            name: "Bash".to_owned(),
            input: r#"{"command":"ls -la","timeout":5}"#.to_owned(),
            result: Some("a\nb\nc\nd\ne".to_owned()),
        }]);
        assert_eq!(
            text_of(&transcript, false, 60),
            vec![
                "● Bash(ls -la)",
                "  ⎿ a",
                "    b",
                "    c",
                "    … +2 lines (ctrl-o to expand)",
            ]
        );
        let expanded = text_of(&transcript, true, 60);
        assert!(
            expanded.contains(&"    \"command\": \"ls -la\",".to_owned()),
            "{expanded:?}"
        );
        assert!(expanded.contains(&"    e".to_owned()));
        assert!(!expanded.iter().any(|row| row.contains("+2 lines")));
    }

    #[test]
    fn a_running_call_and_an_orphan_result_both_render() {
        let transcript = of(vec![
            Block::Tool {
                turn: "t".to_owned(),
                name: "Read".to_owned(),
                input: "not json".to_owned(),
                result: None,
            },
            Block::Tool {
                turn: "t".to_owned(),
                name: String::new(),
                input: String::new(),
                result: Some(String::new()),
            },
        ]);
        assert_eq!(
            text_of(&transcript, false, 40),
            vec!["● Read(not json)", "    running…", "", "  ⎿ (no output)"]
        );
    }

    #[test]
    fn every_block_kind_has_its_marker() {
        let transcript = of(vec![
            Block::Message {
                from: Speaker::Human,
                text: "fix it".to_owned(),
                note: false,
            },
            Block::Message {
                from: Speaker::Human,
                text: "fyi".to_owned(),
                note: true,
            },
            Block::Message {
                from: Speaker::Agent("lead".to_owned()),
                text: "go".to_owned(),
                note: false,
            },
            Block::Message {
                from: Speaker::Agent("lead".to_owned()),
                text: "hm".to_owned(),
                note: true,
            },
            Block::Message {
                from: Speaker::Server,
                text: "x".to_owned(),
                note: false,
            },
            Block::Assistant {
                turn: "t".to_owned(),
                text: " done \n".to_owned(),
            },
            Block::Thinking {
                turn: "t".to_owned(),
                text: "line one\nline two".to_owned(),
            },
            Block::Prompt {
                request_id: "r".to_owned(),
                summary: "rm x".to_owned(),
                settled: None,
            },
            Block::Prompt {
                request_id: "r".to_owned(),
                summary: "rm x".to_owned(),
                settled: Some(Settled::Allowed),
            },
            Block::Prompt {
                request_id: "r".to_owned(),
                summary: "rm x".to_owned(),
                settled: Some(Settled::Denied),
            },
            Block::Prompt {
                request_id: "r".to_owned(),
                summary: "q".to_owned(),
                settled: Some(Settled::Answered("a → b".to_owned())),
            },
            Block::Child {
                text: "spawned kid".to_owned(),
            },
            Block::Reported {
                status: "success",
                summary: "ok".to_owned(),
            },
            Block::Reported {
                status: "blocked",
                summary: "no".to_owned(),
            },
            Block::Notice {
                tone: Tone::Info,
                text: "i".to_owned(),
            },
            Block::Notice {
                tone: Tone::Warn,
                text: "w".to_owned(),
            },
            Block::Notice {
                tone: Tone::Error,
                text: "e".to_owned(),
            },
        ]);
        let rows: Vec<String> = text_of(&transcript, false, 60)
            .into_iter()
            .filter(|row| !row.is_empty())
            .collect();
        assert_eq!(
            rows,
            vec![
                "› fix it",
                "› note: fyi",
                "› lead: go",
                "› note from lead: hm",
                "› server: x",
                "● done",
                "✻ Thinking… line one…",
                "◆ needs you: rm x",
                "✓ allowed: rm x",
                "✗ denied: rm x",
                "✓ answered: a → b",
                "↳ spawned kid",
                "◼ result success: ok",
                "◼ result blocked: no",
                "· i",
                "⚠ w",
                "✗ e",
            ]
        );
        let thinking = text_of(
            &of(vec![Block::Thinking {
                turn: "t".to_owned(),
                text: "a\nb".to_owned(),
            }]),
            true,
            40,
        );
        assert_eq!(
            thinking,
            vec!["✻ a", "  b"],
            "details show the whole thought"
        );
    }

    #[test]
    fn long_text_wraps_under_its_marker() {
        let transcript = of(vec![Block::Assistant {
            turn: "t".to_owned(),
            text: "one two three four".to_owned(),
        }]);
        assert_eq!(
            text_of(&transcript, false, 10),
            vec!["● one two", "  three", "  four"]
        );
    }

    #[test]
    fn summaries_prefer_the_argument_a_human_knows() {
        assert_eq!(tool_summary(r#"{"file_path":"/a/b","x":1}"#), "/a/b");
        assert_eq!(tool_summary(r#"{"zeta":"z","n":2}"#), "z");
        assert_eq!(tool_summary(r#"{"n":2}"#), r#"{"n":2}"#);
        assert_eq!(tool_summary("plain words"), "plain words");
        assert_eq!(tool_summary(&"x".repeat(70)).chars().count(), 60);
        assert_eq!(pretty_input("[1]"), "[\n  1\n]");
        assert_eq!(pretty_input("nope"), "nope");
    }
}
