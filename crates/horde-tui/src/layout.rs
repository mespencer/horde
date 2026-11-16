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
    // The text wears its marker's colour: dim for information, the tone's
    // colour for a warning or an error.
    marked(rows, marker, text, marker.1, width);
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

    fn x(count: usize) -> String {
        "x".repeat(count)
    }

    fn tool(name: &str, input: &str, result: Option<&str>) -> Block {
        Block::Tool {
            turn: "t".to_owned(),
            name: name.to_owned(),
            input: input.to_owned(),
            result: result.map(str::to_owned),
        }
    }

    fn prompt(summary: usize) -> Block {
        Block::Prompt {
            request_id: "r".to_owned(),
            summary: x(summary),
            settled: None,
        }
    }

    fn reported(summary: usize) -> Block {
        Block::Reported {
            status: "success",
            summary: x(summary),
        }
    }

    /// At 20 columns a marked paragraph has 18 cells and an indented one 16.
    /// Each block's text is one cell too long, so it breaks exactly one cell
    /// from the edge: a row a cell narrower or wider would cut it elsewhere.
    #[test]
    fn every_block_breaks_exactly_at_the_edge_of_its_width() {
        let wide = x(19);
        let cases: Vec<(Block, bool, Vec<String>)> = vec![
            (
                Block::Message {
                    from: Speaker::Human,
                    text: wide.clone(),
                    note: false,
                },
                false,
                vec![format!("› {}", x(18)), "  x".to_owned()],
            ),
            (
                Block::Assistant {
                    turn: "t".to_owned(),
                    text: wide.clone(),
                },
                false,
                vec![format!("● {}", x(18)), "  x".to_owned()],
            ),
            (
                Block::Thinking {
                    turn: "t".to_owned(),
                    text: wide.clone(),
                },
                true,
                vec![format!("✻ {}", x(18)), "  x".to_owned()],
            ),
            // Words move whole, so a prompt and a result take two texts:
            // one that exactly fills the row, one a cell too long for it.
            (prompt(7), false, vec![format!("◆ needs you: {}", x(7))]),
            (
                prompt(8),
                false,
                vec!["◆ needs you:".to_owned(), format!("  {}", x(8))],
            ),
            (
                Block::Child { text: wide.clone() },
                false,
                vec![format!("↳ {}", x(18)), "  x".to_owned()],
            ),
            (
                reported(2),
                false,
                vec![format!("◼ result success: {}", x(2))],
            ),
            (
                reported(3),
                false,
                vec!["◼ result success:".to_owned(), format!("  {}", x(3))],
            ),
            (
                Block::Notice {
                    tone: Tone::Warn,
                    text: wide.clone(),
                },
                false,
                vec![format!("⚠ {}", x(18)), "  x".to_owned()],
            ),
            (
                tool(&x(17), "", Some(&x(17))),
                false,
                vec![
                    format!("● {}(", x(17)),
                    "  )".to_owned(),
                    format!("  ⎿ {}", x(16)),
                    "    x".to_owned(),
                ],
            ),
            (
                tool("T", &x(17), Some("ok")),
                true,
                vec![
                    format!("● T({}", x(16)),
                    "  x)".to_owned(),
                    format!("    {}", x(16)),
                    "    x".to_owned(),
                    "  ⎿ ok".to_owned(),
                ],
            ),
        ];
        for (block, details, expected) in cases {
            let label = format!("{block:?}");
            assert_eq!(text_of(&of(vec![block]), details, 20), expected, "{label}");
        }
    }

    /// Eleven columns leave seven cells under a tool call, one short of
    /// `running…`, which therefore breaks before its ellipsis.
    #[test]
    fn a_running_call_breaks_at_its_indented_width() {
        assert_eq!(
            text_of(&of(vec![tool("T", "", None)]), false, 11),
            vec!["● T()", "    running", "    …"]
        );
    }

    /// Below eight columns the transcript lays out as if eight wide: six
    /// cells behind the marker.
    #[test]
    fn a_tiny_terminal_lays_out_at_eight_columns() {
        let transcript = of(vec![Block::Assistant {
            turn: "t".to_owned(),
            text: x(7),
        }]);
        for width in [0, 3, 8] {
            assert_eq!(
                text_of(&transcript, false, width),
                vec![format!("● {}", x(6)), "  x".to_owned()],
                "at {width} columns"
            );
        }
    }

    /// A result's trailing blank lines are dropped, and a single hidden
    /// line is still counted.
    #[test]
    fn a_result_drops_trailing_blanks_and_counts_one_hidden_line() {
        assert_eq!(
            text_of(&of(vec![tool("", "", Some("ok\n\n\n"))]), false, 40),
            vec!["  ⎿ ok"]
        );
        assert_eq!(
            text_of(&of(vec![tool("", "", Some("a\nb\nc\nd"))]), false, 40),
            vec![
                "  ⎿ a",
                "    b",
                "    c",
                "    … +1 lines (ctrl-o to expand)"
            ]
        );
    }

    /// A call still running wears a yellow bullet; one with its result, green.
    #[test]
    fn a_call_is_yellow_while_running_and_green_when_done() {
        let bullet = |result: Option<&str>| {
            transcript_lines(&of(vec![tool("Read", "", result)]), false, 40)[0].spans[0].style
        };
        assert_eq!(bullet(None), crate::theme::warn());
        assert_eq!(bullet(Some("ok")), crate::theme::ok());
    }

    /// A notice's text wears its marker's colour.
    #[test]
    fn a_notice_is_coloured_by_its_tone() {
        for (tone, style) in [
            (Tone::Info, crate::theme::dim()),
            (Tone::Warn, crate::theme::warn()),
            (Tone::Error, crate::theme::error()),
        ] {
            let lines = transcript_lines(
                &of(vec![Block::Notice {
                    tone,
                    text: "n".to_owned(),
                }]),
                false,
                40,
            );
            let styles: Vec<_> = lines[0].spans.iter().map(|span| span.style).collect();
            assert_eq!(styles, vec![style, style], "{tone:?}");
        }
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
