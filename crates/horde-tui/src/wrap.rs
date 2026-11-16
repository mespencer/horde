//! Text layout in terminal cells: word wrapping and width-aware clipping.
//!
//! The transcript scrolls by *rendered rows*, so it must know exactly how
//! many rows a block occupies at the current width before ratatui draws it.
//! Wrapping here — rather than handing ratatui a `Wrap` and hoping — keeps
//! that count and the drawing the same computation.

use unicode_width::UnicodeWidthStr;

/// How many terminal cells `text` occupies.
#[must_use]
pub fn width(text: &str) -> usize {
    UnicodeWidthStr::width(text)
}

/// How many cells one character occupies, counted the way [`width`] counts
/// it inside a string, so rows built a character at a time measure what
/// the whole row measures (a control character takes a cell in both).
fn char_width(c: char) -> usize {
    width(c.encode_utf8(&mut [0; 4]))
}

/// Wraps `text` to rows of at most `limit` cells.
///
/// Explicit newlines always break, and a blank line survives as an empty
/// row. Words break at spaces; a word wider than the whole row is split
/// mid-word rather than overflowing. A zero limit is treated as one cell, so
/// a degenerate terminal still terminates.
#[must_use]
pub fn wrap(text: &str, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    text.split('\n')
        .flat_map(|line| wrap_line(line.trim_end_matches('\r'), limit))
        .collect()
}

fn wrap_line(line: &str, limit: usize) -> Vec<String> {
    let mut rows = Vec::new();
    let mut current = String::new();
    for word in line.split(' ') {
        place_word(&mut rows, &mut current, word, limit);
    }
    rows.push(current);
    rows
}

/// Appends one space-delimited word to the row being built, starting a new
/// row (or several, for an over-wide word) when it does not fit.
fn place_word(rows: &mut Vec<String>, current: &mut String, word: &str, limit: usize) {
    let separator = usize::from(!current.is_empty());
    if width(current) + separator + width(word) <= limit {
        if separator == 1 {
            current.push(' ');
        }
        current.push_str(word);
        return;
    }
    if !current.is_empty() {
        rows.push(std::mem::take(current));
    }
    for c in word.chars() {
        let cell = char_width(c);
        if width(current) + cell > limit && !current.is_empty() {
            rows.push(std::mem::take(current));
        }
        current.push(c);
    }
}

/// `text` cut to at most `limit` cells, with an ellipsis when anything was
/// cut. Single-line by construction: everything from the first newline on
/// counts as cut.
#[must_use]
pub fn clip(text: &str, limit: usize) -> String {
    let first = text.lines().next().unwrap_or("");
    let cut = first.len() < text.trim_end().len();
    if !cut && width(first) <= limit {
        return first.to_owned();
    }
    let mut out = String::new();
    for c in first.chars() {
        if width(&out) + char_width(c) + 1 > limit {
            break;
        }
        out.push(c);
    }
    out.push('…');
    out
}

/// `text` padded with spaces to exactly `limit` cells, or clipped to it.
#[must_use]
pub fn fit(text: &str, limit: usize) -> String {
    let clipped = clip(text, limit);
    let pad = limit.saturating_sub(width(&clipped));
    format!("{clipped}{}", " ".repeat(pad))
}

#[cfg(test)]
mod tests {
    use super::{clip, fit, width, wrap};

    #[test]
    fn words_wrap_at_spaces_and_fit_exactly() {
        assert_eq!(wrap("aa bb cc", 5), vec!["aa bb", "cc"]);
        assert_eq!(wrap("aa bb", 5), vec!["aa bb"], "exactly the limit fits");
        assert_eq!(wrap("aa bb", 4), vec!["aa", "bb"]);
    }

    #[test]
    fn newlines_break_and_blank_lines_survive() {
        assert_eq!(wrap("a\n\nb\r", 10), vec!["a", "", "b"]);
        assert_eq!(wrap("", 10), vec![""]);
    }

    #[test]
    fn an_over_wide_word_splits_mid_word() {
        assert_eq!(wrap("abcdefg", 3), vec!["abc", "def", "g"]);
        assert_eq!(wrap("x abcdef", 3), vec!["x", "abc", "def"]);
        assert_eq!(wrap("ab", 0), vec!["a", "b"], "zero is one cell");
    }

    #[test]
    fn wide_characters_count_two_cells() {
        assert_eq!(width("日本"), 4);
        assert_eq!(wrap("日本語", 4), vec!["日本", "語"]);
    }

    #[test]
    fn clipping_marks_what_it_cut() {
        assert_eq!(clip("hello", 5), "hello");
        assert_eq!(clip("hello world", 6), "hello…");
        assert_eq!(clip("one\ntwo", 20), "one…", "a second line counts as cut");
        assert_eq!(clip("one\n", 20), "one", "a trailing newline cuts nothing");
        assert_eq!(fit("ab", 4), "ab  ");
        assert_eq!(fit("abcdef", 4), "abc…");
    }

    /// A zero limit is one cell, not none: a combining mark stays on its
    /// letter.
    #[test]
    fn a_zero_limit_keeps_a_combining_mark_on_its_letter() {
        assert_eq!(wrap("e\u{301}", 0), vec!["e\u{301}"]);
    }

    /// A control character takes the cell a string's width gives it, so a
    /// split word's rows and a clipped line stay within their limit.
    #[test]
    fn a_control_character_takes_a_cell() {
        assert_eq!(wrap("abcd\u{7}", 2), vec!["ab", "cd", "\u{7}"]);
        assert_eq!(clip("ab\u{7}cdef", 5), "ab\u{7}c…");
        assert_eq!(clip("abc\u{7}def", 5), "abc\u{7}…");
    }

    /// Clipping stops at the first character that does not fit, even when a
    /// narrower one follows.
    #[test]
    fn clipping_stops_at_the_first_character_that_does_not_fit() {
        assert_eq!(clip("a界bcd", 3), "a…");
    }
}
