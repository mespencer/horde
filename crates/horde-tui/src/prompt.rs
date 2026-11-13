//! Answering a prompt in place: the cursor over a permission request's
//! choices, or over a question request's questions and options.
//!
//! A permission offers three rows — allow, allow and remember for the
//! session, deny. A question request offers, per question, its options and
//! (when the harness accepts one) a free-text row. Highlighting moves with
//! the arrows, digits pick directly, and once every question has an answer
//! the cursor hands back a [`Decision`]; it never talks to the server
//! itself.

use std::collections::{BTreeMap, BTreeSet};

use crate::state::{PendingPermission, PendingQuestion};

/// What the human settled on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Allow {
        remember: bool,
    },
    Deny,
    /// Question id to the chosen labels and/or free text.
    Answer(BTreeMap<String, Vec<String>>),
}

/// One input the cursor understands, already translated from a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptKey {
    Up,
    Down,
    /// Previous question.
    Left,
    /// Next question.
    Right,
    /// Choose (single-select) or advance (multi-select).
    Enter,
    /// Toggle an option of a multi-select question; choose otherwise.
    Space,
    Backspace,
    /// A printable character: a digit picks an option, `y`/`n` allow and
    /// deny, and on a free-text row every character is text.
    Char(char),
}

/// The rows a permission request offers, in order.
pub const PERMISSION_CHOICES: [&str; 3] = ["Yes", "Yes, and don't ask again this session", "No"];

/// The cursor over one pending request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptCursor {
    pub request_id: String,
    /// The question showing (always 0 for a permission).
    pub question: usize,
    /// Per question, the highlighted row.
    pub highlighted: Vec<usize>,
    /// Per question, the chosen option indices.
    pub chosen: Vec<BTreeSet<usize>>,
    /// Per question, the free text typed so far.
    pub free_text: Vec<String>,
}

impl PromptCursor {
    /// A fresh cursor on `request`, first row of the first question.
    #[must_use]
    pub fn new(request: &PendingPermission) -> Self {
        let count = request.questions.len().max(1);
        Self {
            request_id: request.request_id.clone(),
            question: 0,
            highlighted: vec![0; count],
            chosen: vec![BTreeSet::new(); count],
            free_text: vec![String::new(); count],
        }
    }

    /// The highlighted row of the showing question.
    #[must_use]
    pub fn row(&self) -> usize {
        self.highlighted.get(self.question).copied().unwrap_or(0)
    }

    /// Whether the highlighted row is the showing question's free-text row.
    #[must_use]
    pub fn on_free_text(&self, request: &PendingPermission) -> bool {
        request
            .questions
            .get(self.question)
            .is_some_and(|question| has_free_text(question) && self.row() == question.options.len())
    }

    /// Applies one key; `Some` once the request is settled.
    pub fn press(&mut self, request: &PendingPermission, key: PromptKey) -> Option<Decision> {
        if request.is_question() {
            self.press_question(request, key)
        } else {
            self.press_permission(key)
        }
    }

    fn press_permission(&mut self, key: PromptKey) -> Option<Decision> {
        match key {
            PromptKey::Up => self.step(-1, PERMISSION_CHOICES.len()),
            PromptKey::Down => self.step(1, PERMISSION_CHOICES.len()),
            PromptKey::Enter | PromptKey::Space => return permission_choice(self.row()),
            PromptKey::Char('y') => return permission_choice(0),
            PromptKey::Char('n') => return Some(Decision::Deny),
            PromptKey::Char(c) => return digit(c).and_then(permission_choice),
            PromptKey::Left | PromptKey::Right | PromptKey::Backspace => {}
        }
        None
    }

    fn press_question(&mut self, request: &PendingPermission, key: PromptKey) -> Option<Decision> {
        let question = request.questions.get(self.question)?;
        if self.on_free_text(request) && self.types_text(key) {
            return None;
        }
        let rows = row_count(question);
        match key {
            PromptKey::Up => self.step(-1, rows),
            PromptKey::Down => self.step(1, rows),
            PromptKey::Left => self.question = self.question.saturating_sub(1),
            PromptKey::Right => return self.advance(request),
            PromptKey::Enter => return self.enter(request, question),
            PromptKey::Space => return self.space(request, question),
            PromptKey::Char('n') => return Some(Decision::Deny),
            PromptKey::Char(c) => return self.pick(request, question, c),
            PromptKey::Backspace => {}
        }
        None
    }

    /// On a free-text row, characters and backspace edit the text. Returns
    /// whether the key was consumed as editing.
    fn types_text(&mut self, key: PromptKey) -> bool {
        let Some(text) = self.free_text.get_mut(self.question) else {
            return false;
        };
        match key {
            PromptKey::Char(c) => text.push(c),
            PromptKey::Space => text.push(' '),
            PromptKey::Backspace => {
                text.pop();
            }
            _ => return false,
        }
        true
    }

    fn step(&mut self, by: isize, rows: usize) {
        if let Some(row) = self.highlighted.get_mut(self.question) {
            *row = row.saturating_add_signed(by).min(rows.saturating_sub(1));
        }
    }

    fn enter(
        &mut self,
        request: &PendingPermission,
        question: &PendingQuestion,
    ) -> Option<Decision> {
        let row = self.row();
        let multi = question.multi_select && row < question.options.len();
        if row < question.options.len() && (!multi || self.chosen_here().is_empty()) {
            self.choose(row, question.multi_select);
        }
        self.advance(request)
    }

    fn space(
        &mut self,
        request: &PendingPermission,
        question: &PendingQuestion,
    ) -> Option<Decision> {
        let row = self.row();
        if row >= question.options.len() {
            return None;
        }
        self.choose(row, question.multi_select);
        if question.multi_select {
            return None;
        }
        self.advance(request)
    }

    /// A digit picks option `n` the way Enter would; anything else is noise.
    fn pick(
        &mut self,
        request: &PendingPermission,
        question: &PendingQuestion,
        c: char,
    ) -> Option<Decision> {
        let index = digit(c).filter(|index| *index < question.options.len())?;
        if let Some(row) = self.highlighted.get_mut(self.question) {
            *row = index;
        }
        self.choose(index, question.multi_select);
        if question.multi_select {
            return None;
        }
        self.advance(request)
    }

    fn chosen_here(&self) -> &BTreeSet<usize> {
        &self.chosen[self.question]
    }

    /// Single-select replaces the choice; multi-select toggles it.
    fn choose(&mut self, option: usize, multi: bool) {
        let chosen = &mut self.chosen[self.question];
        if !multi {
            chosen.clear();
            chosen.insert(option);
        } else if !chosen.remove(&option) {
            chosen.insert(option);
        }
    }

    /// To the next unanswered question; when there is none, the answers.
    fn advance(&mut self, request: &PendingPermission) -> Option<Decision> {
        let answers = self.answers(request);
        let count = request.questions.len();
        let unanswered = (1..=count)
            .map(|offset| (self.question + offset) % count)
            .find(|index| !answers.contains_key(&request.questions[*index].id));
        match unanswered {
            Some(index) => {
                self.question = index;
                None
            }
            None => Some(Decision::Answer(answers)),
        }
    }

    /// Every question answered so far, keyed by question id.
    #[must_use]
    pub fn answers(&self, request: &PendingPermission) -> BTreeMap<String, Vec<String>> {
        request
            .questions
            .iter()
            .enumerate()
            .filter_map(|(index, question)| {
                let mut values: Vec<String> = self.chosen[index]
                    .iter()
                    .filter_map(|option| question.options.get(*option))
                    .map(|choice| choice.label.clone())
                    .collect();
                let text = self.free_text[index].trim();
                if !text.is_empty() {
                    values.push(text.to_owned());
                }
                (!values.is_empty()).then(|| (question.id.clone(), values))
            })
            .collect()
    }
}

/// Whether a question takes typed text: it says so, or it offers nothing
/// to choose from.
#[must_use]
pub fn has_free_text(question: &PendingQuestion) -> bool {
    question.allows_free_text || question.options.is_empty()
}

/// Options plus the free-text row when there is one.
#[must_use]
pub fn row_count(question: &PendingQuestion) -> usize {
    question.options.len() + usize::from(has_free_text(question))
}

fn permission_choice(row: usize) -> Option<Decision> {
    match row {
        0 => Some(Decision::Allow { remember: false }),
        1 => Some(Decision::Allow { remember: true }),
        2 => Some(Decision::Deny),
        _ => None,
    }
}

/// `'1'` → 0, `'9'` → 8; anything else (including `'0'`) is no option.
fn digit(c: char) -> Option<usize> {
    c.to_digit(10)
        .and_then(|number| usize::try_from(number).ok())
        .and_then(|number| number.checked_sub(1))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Decision, PromptCursor, PromptKey, has_free_text, row_count};
    use crate::state::{PendingPermission, PendingQuestion, QuestionChoice};

    fn question(id: &str, options: &[&str]) -> PendingQuestion {
        PendingQuestion {
            id: id.to_owned(),
            header: String::new(),
            question: format!("{id}?"),
            options: options
                .iter()
                .map(|label| QuestionChoice {
                    label: (*label).to_owned(),
                    description: String::new(),
                })
                .collect(),
            multi_select: false,
            allows_free_text: false,
            secret: false,
        }
    }

    fn request(questions: Vec<PendingQuestion>) -> PendingPermission {
        PendingPermission {
            request_id: "r1".to_owned(),
            questions,
            ..PendingPermission::default()
        }
    }

    fn answer(pairs: &[(&str, &[&str])]) -> Option<Decision> {
        Some(Decision::Answer(
            pairs
                .iter()
                .map(|(id, values)| {
                    (
                        (*id).to_owned(),
                        values.iter().map(ToString::to_string).collect(),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        ))
    }

    fn press_all(
        cursor: &mut PromptCursor,
        request: &PendingPermission,
        keys: &[PromptKey],
    ) -> Option<Decision> {
        let mut last = None;
        for key in keys {
            last = cursor.press(request, *key);
        }
        last
    }

    #[test]
    fn a_permission_offers_yes_remember_and_no() {
        let permission = request(Vec::new());
        let mut cursor = PromptCursor::new(&permission);
        assert_eq!(
            cursor.press(&permission, PromptKey::Enter),
            Some(Decision::Allow { remember: false })
        );
        assert_eq!(cursor.press(&permission, PromptKey::Down), None);
        assert_eq!(
            cursor.press(&permission, PromptKey::Space),
            Some(Decision::Allow { remember: true })
        );
        press_all(
            &mut cursor,
            &permission,
            &[PromptKey::Down, PromptKey::Down],
        );
        assert_eq!(cursor.row(), 2, "the last row is the floor");
        cursor.press(&permission, PromptKey::Up);
        assert_eq!(cursor.row(), 1);
        for (key, decision) in [
            ('y', Some(Decision::Allow { remember: false })),
            ('n', Some(Decision::Deny)),
            ('1', Some(Decision::Allow { remember: false })),
            ('2', Some(Decision::Allow { remember: true })),
            ('3', Some(Decision::Deny)),
            ('4', None),
            ('0', None),
            ('x', None),
        ] {
            assert_eq!(cursor.press(&permission, PromptKey::Char(key)), decision);
        }
        for key in [PromptKey::Left, PromptKey::Right, PromptKey::Backspace] {
            assert_eq!(cursor.press(&permission, key), None);
        }
        assert_eq!(cursor.row(), 1, "noise keys move nothing");
    }

    #[test]
    fn a_single_question_is_answered_by_digit_or_enter() {
        let asking = request(vec![question("colour", &["Red", "Blue"])]);
        let mut cursor = PromptCursor::new(&asking);
        assert_eq!(
            cursor.press(&asking, PromptKey::Char('2')),
            answer(&[("colour", &["Blue"])])
        );

        let mut cursor = PromptCursor::new(&asking);
        assert_eq!(
            cursor.press(&asking, PromptKey::Char('3')),
            None,
            "no option 3"
        );
        assert_eq!(
            cursor.press(&asking, PromptKey::Char('y')),
            None,
            "y cannot allow"
        );
        cursor.press(&asking, PromptKey::Down);
        cursor.press(&asking, PromptKey::Down);
        assert_eq!(cursor.row(), 1, "two rows, no free text");
        assert_eq!(
            cursor.press(&asking, PromptKey::Enter),
            answer(&[("colour", &["Blue"])])
        );
        assert_eq!(
            PromptCursor::new(&asking).press(&asking, PromptKey::Char('n')),
            Some(Decision::Deny)
        );
    }

    #[test]
    fn several_questions_advance_until_all_are_answered() {
        let asking = request(vec![question("a", &["x", "y"]), question("b", &["z"])]);
        let mut cursor = PromptCursor::new(&asking);
        assert_eq!(cursor.press(&asking, PromptKey::Char('2')), None);
        assert_eq!(cursor.question, 1, "on to the next question");
        cursor.press(&asking, PromptKey::Left);
        assert_eq!(cursor.question, 0);
        cursor.press(&asking, PromptKey::Left);
        assert_eq!(cursor.question, 0, "the first is the first");
        assert_eq!(cursor.press(&asking, PromptKey::Right), None);
        assert_eq!(cursor.question, 1, "right skips to the unanswered one");
        assert_eq!(
            cursor.press(&asking, PromptKey::Enter),
            answer(&[("a", &["y"]), ("b", &["z"])])
        );
    }

    #[test]
    fn multi_select_toggles_with_space_and_enter_moves_on() {
        let mut many = question("tags", &["a", "b", "c"]);
        many.multi_select = true;
        let asking = request(vec![many]);
        let mut cursor = PromptCursor::new(&asking);
        assert_eq!(cursor.press(&asking, PromptKey::Space), None);
        cursor.press(&asking, PromptKey::Down);
        cursor.press(&asking, PromptKey::Down);
        assert_eq!(
            cursor.press(&asking, PromptKey::Char('3')),
            None,
            "a digit toggles too"
        );
        assert_eq!(
            cursor.press(&asking, PromptKey::Char('1')),
            None,
            "and untoggles"
        );
        cursor.press(&asking, PromptKey::Char('2'));
        assert_eq!(
            cursor.press(&asking, PromptKey::Enter),
            answer(&[("tags", &["b", "c"])]),
            "enter with choices made submits them as they are"
        );

        let mut cursor = PromptCursor::new(&asking);
        assert_eq!(
            cursor.press(&asking, PromptKey::Enter),
            answer(&[("tags", &["a"])]),
            "enter with nothing chosen takes the highlighted row"
        );
    }

    #[test]
    fn free_text_rows_take_typing_including_digits_and_n() {
        let mut open = question("why", &["because"]);
        open.allows_free_text = true;
        let asking = request(vec![open]);
        assert!(has_free_text(&asking.questions[0]));
        assert_eq!(row_count(&asking.questions[0]), 2);
        let mut cursor = PromptCursor::new(&asking);
        assert!(!cursor.on_free_text(&asking));
        cursor.press(&asking, PromptKey::Down);
        assert!(cursor.on_free_text(&asking));
        for key in [
            PromptKey::Char('n'),
            PromptKey::Char('o'),
            PromptKey::Space,
            PromptKey::Char('1'),
            PromptKey::Char('x'),
            PromptKey::Backspace,
        ] {
            assert_eq!(cursor.press(&asking, key), None);
        }
        assert_eq!(cursor.free_text[0], "no 1");
        assert_eq!(
            cursor.press(&asking, PromptKey::Enter),
            answer(&[("why", &["no 1"])])
        );
        cursor.press(&asking, PromptKey::Up);
        assert!(!cursor.on_free_text(&asking));
        assert_eq!(
            cursor.press(&asking, PromptKey::Space),
            answer(&[("why", &["because", "no 1"])]),
            "space on an option chooses it beside the typed text"
        );
    }

    #[test]
    fn a_question_without_options_is_free_text_only() {
        let asking = request(vec![question("name", &[])]);
        let mut cursor = PromptCursor::new(&asking);
        assert!(cursor.on_free_text(&asking));
        assert_eq!(
            cursor.press(&asking, PromptKey::Enter),
            None,
            "blank is no answer"
        );
        cursor.press(&asking, PromptKey::Char('z'));
        assert_eq!(
            cursor.press(&asking, PromptKey::Enter),
            answer(&[("name", &["z"])])
        );
        assert_eq!(
            cursor.press(&asking, PromptKey::Space),
            None,
            "space on a free-text row is a space"
        );
    }

    #[test]
    fn a_cursor_on_a_vanished_question_does_nothing() {
        let asking = request(vec![question("a", &["x"])]);
        let mut cursor = PromptCursor::new(&asking);
        cursor.question = 5;
        assert_eq!(cursor.press(&asking, PromptKey::Enter), None);
        assert_eq!(cursor.row(), 0);
    }
}
