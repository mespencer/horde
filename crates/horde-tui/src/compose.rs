//! The compose box: a small multi-line editor with a send history.
//!
//! Positions are counted in characters, never bytes, so no edit can split a
//! multi-byte character. The editor knows nothing about sending: it holds
//! text and a cursor, and [`Compose::take`] hands the text over (and files
//! it in the history) when the App decides to send.

/// The editor's whole state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compose {
    lines: Vec<String>,
    row: usize,
    col: usize,
    history: Vec<String>,
    /// Which history entry is showing, counted back from the newest; `None`
    /// while editing fresh text.
    recalled: Option<usize>,
    /// The fresh text that was there before history browsing began, so
    /// walking forward past the newest entry gives it back.
    draft: String,
}

impl Default for Compose {
    fn default() -> Self {
        Self {
            lines: vec![String::new()],
            row: 0,
            col: 0,
            history: Vec::new(),
            recalled: None,
            draft: String::new(),
        }
    }
}

/// How many sent messages the history keeps.
pub const HISTORY_CAPACITY: usize = 100;

impl Compose {
    /// The whole text, lines joined with newlines.
    #[must_use]
    pub fn text(&self) -> String {
        self.lines.join("\n")
    }

    #[must_use]
    pub fn lines(&self) -> &[String] {
        &self.lines
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lines.len() == 1 && self.lines[0].is_empty()
    }

    /// The cursor as `(line, character)`.
    #[must_use]
    pub fn cursor(&self) -> (usize, usize) {
        (self.row, self.col)
    }

    /// Replaces the whole text, cursor at its end.
    pub fn set_text(&mut self, text: &str) {
        self.lines = text.split('\n').map(ToOwned::to_owned).collect();
        self.row = self.lines.len() - 1;
        self.col = char_len(&self.lines[self.row]);
    }

    /// Empties the box without touching the history.
    pub fn clear(&mut self) {
        self.set_text("");
        self.recalled = None;
    }

    /// Hands the text over and files it in the history. Blank text is
    /// returned as-is and not remembered.
    pub fn take(&mut self) -> String {
        let text = self.text();
        if !text.trim().is_empty() {
            if self.history.len() >= HISTORY_CAPACITY {
                self.history.remove(0);
            }
            self.history.push(text.clone());
        }
        self.clear();
        self.draft.clear();
        text
    }

    pub fn insert_char(&mut self, c: char) {
        if c == '\n' {
            self.newline();
            return;
        }
        let line = &mut self.lines[self.row];
        let at = byte_at(line, self.col);
        line.insert(at, c);
        self.col += 1;
    }

    /// Inserts pasted text verbatim, newlines included.
    pub fn insert_str(&mut self, text: &str) {
        for c in text.chars().filter(|c| *c != '\r') {
            self.insert_char(c);
        }
    }

    /// Splits the current line at the cursor.
    pub fn newline(&mut self) {
        let line = &mut self.lines[self.row];
        let at = byte_at(line, self.col);
        let rest = line.split_off(at);
        self.row += 1;
        self.lines.insert(self.row, rest);
        self.col = 0;
    }

    /// Deletes the character before the cursor, joining lines at a line start.
    pub fn backspace(&mut self) {
        if self.col > 0 {
            let line = &mut self.lines[self.row];
            line.remove(byte_at(line, self.col - 1));
            self.col -= 1;
        } else if self.row > 0 {
            let current = self.lines.remove(self.row);
            self.row -= 1;
            self.col = char_len(&self.lines[self.row]);
            self.lines[self.row].push_str(&current);
        }
    }

    /// Deletes the character under the cursor, joining lines at a line end.
    pub fn delete(&mut self) {
        let len = char_len(&self.lines[self.row]);
        if self.col < len {
            let line = &mut self.lines[self.row];
            line.remove(byte_at(line, self.col));
        } else if self.row + 1 < self.lines.len() {
            let next = self.lines.remove(self.row + 1);
            self.lines[self.row].push_str(&next);
        }
    }

    /// Deletes back to the start of the previous word (Ctrl-W).
    pub fn delete_word(&mut self) {
        let chars: Vec<char> = self.lines[self.row].chars().collect();
        let before = &chars[..self.col.min(chars.len())];
        // Past any spaces just behind the cursor to the word's last
        // character, then back to the space before the word.
        let start = before
            .iter()
            .rposition(|c| *c != ' ')
            .and_then(|last| before[..last].iter().rposition(|c| *c == ' '))
            .map_or(0, |space| space + 1);
        let line = &mut self.lines[self.row];
        let (from, to) = (byte_at(line, start), byte_at(line, self.col));
        line.replace_range(from..to, "");
        self.col = start;
    }

    /// Deletes from the line start to the cursor (Ctrl-U).
    pub fn delete_to_line_start(&mut self) {
        let line = &mut self.lines[self.row];
        let to = byte_at(line, self.col);
        line.replace_range(..to, "");
        self.col = 0;
    }

    pub fn left(&mut self) {
        if self.col > 0 {
            self.col -= 1;
        } else if self.row > 0 {
            self.row -= 1;
            self.col = char_len(&self.lines[self.row]);
        }
    }

    pub fn right(&mut self) {
        if self.col < char_len(&self.lines[self.row]) {
            self.col += 1;
        } else if self.row + 1 < self.lines.len() {
            self.row += 1;
            self.col = 0;
        }
    }

    pub fn home(&mut self) {
        self.col = 0;
    }

    pub fn end(&mut self) {
        self.col = char_len(&self.lines[self.row]);
    }

    /// Up a line; on the first line, one step back through the history.
    pub fn up(&mut self) {
        if self.row > 0 {
            self.row -= 1;
            self.col = self.col.min(char_len(&self.lines[self.row]));
        } else {
            self.recall(1);
        }
    }

    /// Down a line; on the last line, one step forward through the history.
    pub fn down(&mut self) {
        if self.row + 1 < self.lines.len() {
            self.row += 1;
            self.col = self.col.min(char_len(&self.lines[self.row]));
        } else {
            self.recall(-1);
        }
    }

    /// Moves the history pointer `step` entries older (negative: newer).
    fn recall(&mut self, step: isize) {
        let current = self.recalled.map_or(0, |back| back + 1);
        let Some(target) = current.checked_add_signed(step) else {
            return;
        };
        if target > self.history.len() {
            return;
        }
        if self.recalled.is_none() {
            self.draft = self.text();
        }
        if target == 0 {
            let draft = std::mem::take(&mut self.draft);
            self.set_text(&draft);
            self.recalled = None;
        } else {
            let entry = self.history[self.history.len() - target].clone();
            self.set_text(&entry);
            self.recalled = Some(target - 1);
        }
    }
}

fn char_len(text: &str) -> usize {
    text.chars().count()
}

/// The byte offset of character `index` in `text` (its length past the end).
fn byte_at(text: &str, index: usize) -> usize {
    text.char_indices()
        .nth(index)
        .map_or(text.len(), |(at, _)| at)
}

#[cfg(test)]
mod tests {
    use super::{Compose, HISTORY_CAPACITY};

    fn typed(text: &str) -> Compose {
        let mut compose = Compose::default();
        compose.insert_str(text);
        compose
    }

    #[test]
    fn typing_inserts_at_the_cursor_and_newlines_split_lines() {
        let mut compose = typed("héllo");
        compose.left();
        compose.left();
        compose.insert_char('X');
        assert_eq!(compose.text(), "hélXlo");
        compose.insert_char('\n');
        assert_eq!(compose.lines(), &["hélX".to_owned(), "lo".to_owned()]);
        assert_eq!(compose.cursor(), (1, 0));
        assert!(!compose.is_empty());
    }

    #[test]
    fn paste_keeps_newlines_and_drops_carriage_returns() {
        let compose = typed("a\r\nb");
        assert_eq!(compose.text(), "a\nb");
        assert_eq!(compose.cursor(), (1, 1));
    }

    #[test]
    fn backspace_and_delete_join_lines_at_the_edges() {
        let mut compose = typed("ab\ncd");
        compose.home();
        compose.backspace();
        assert_eq!(compose.text(), "abcd");
        assert_eq!(compose.cursor(), (0, 2));
        compose.backspace();
        assert_eq!(compose.text(), "acd");
        compose.delete();
        assert_eq!(compose.text(), "ad");

        let mut compose = typed("ab\ncd");
        compose.up();
        compose.end();
        compose.delete();
        assert_eq!(compose.text(), "abcd", "delete at a line end joins");
        compose.end();
        compose.delete();
        assert_eq!(compose.text(), "abcd", "delete at the very end is a no-op");

        let mut empty = Compose::default();
        empty.backspace();
        assert!(empty.is_empty(), "backspace at the very start is a no-op");
    }

    #[test]
    fn word_and_line_deletion() {
        let mut compose = typed("one two  ");
        compose.delete_word();
        assert_eq!(compose.text(), "one ");
        compose.delete_word();
        assert_eq!(compose.text(), "");

        let mut compose = typed("ab cd ef");
        compose.left();
        compose.left();
        compose.left();
        compose.delete_word();
        assert_eq!(
            compose.text(),
            "ab  ef",
            "only the word behind the cursor goes"
        );
        assert_eq!(compose.cursor(), (0, 3));

        let mut compose = typed("a b");
        compose.delete_word();
        assert_eq!(compose.text(), "a ", "a one-letter word goes alone");

        let mut compose = typed("   ");
        compose.delete_word();
        assert_eq!(compose.text(), "", "a line of spaces clears");
        assert_eq!(compose.cursor(), (0, 0));

        let mut compose = typed("keep this");
        compose.left();
        compose.left();
        compose.delete_to_line_start();
        assert_eq!(compose.text(), "is");
        assert_eq!(compose.cursor(), (0, 0));
    }

    #[test]
    fn arrows_cross_line_boundaries_and_clamp_columns() {
        let mut compose = typed("long line\nab");
        compose.home();
        compose.left();
        assert_eq!(compose.cursor(), (0, 9), "left from a line start wraps up");
        compose.right();
        assert_eq!(compose.cursor(), (1, 0), "right from a line end wraps down");
        compose.end();
        compose.right();
        assert_eq!(compose.cursor(), (1, 2), "the very end stays put");
        compose.up();
        assert_eq!(compose.cursor(), (0, 2));
        compose.end();
        compose.down();
        assert_eq!(compose.cursor(), (1, 2), "the column clamps to the line");
        let mut start = Compose::default();
        start.left();
        assert_eq!(start.cursor(), (0, 0));
    }

    #[test]
    fn history_walks_back_and_returns_the_draft() {
        let mut compose = Compose::default();
        for text in ["first", "second"] {
            compose.insert_str(text);
            assert_eq!(compose.take(), text);
        }
        compose.insert_str("draft");
        compose.up();
        assert_eq!(compose.text(), "second");
        compose.up();
        assert_eq!(compose.text(), "first");
        compose.up();
        assert_eq!(compose.text(), "first", "the oldest is the end");
        compose.down();
        assert_eq!(compose.text(), "second");
        compose.down();
        assert_eq!(compose.text(), "draft", "past the newest is the draft");
        compose.down();
        assert_eq!(compose.text(), "draft");
    }

    #[test]
    fn blank_sends_are_not_remembered_and_history_is_bounded() {
        let mut compose = Compose::default();
        compose.insert_str("  ");
        assert_eq!(compose.take(), "  ");
        compose.up();
        assert!(compose.is_empty(), "nothing was filed");

        for index in 0..=HISTORY_CAPACITY {
            compose.insert_str(&format!("m{index}"));
            compose.take();
        }
        for _ in 0..=HISTORY_CAPACITY + 1 {
            compose.up();
        }
        assert_eq!(compose.text(), "m1", "the oldest fell off");
    }

    #[test]
    fn set_text_and_clear_place_the_cursor_at_the_end() {
        let mut compose = Compose::default();
        compose.set_text("/attach x\nmore");
        assert_eq!(compose.cursor(), (1, 4));
        compose.clear();
        assert!(compose.is_empty());
        assert_eq!(compose.cursor(), (0, 0));
    }
}
