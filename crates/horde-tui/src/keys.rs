//! What each key means, as lookup tables.
//!
//! A key becomes an [`Intent`] here and nowhere else, so the whole binding
//! map is data: one table per context, read top to bottom, first match
//! wins. The App never inspects a `KeyEvent`; it acts on intents, which is
//! also what lets the help screen print the bindings from the same tables
//! the dispatcher uses.

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::prompt::PromptKey;

/// Which screen is showing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Screen {
    /// The attached agent's transcript and the compose box.
    Session,
    /// Sessions and their agent trees — also the start screen with nothing
    /// attached.
    Overview,
    Inbox,
    Workspaces,
    Usage,
    Logs,
    /// The forward panel: named forwards with backer/unbacked state.
    Forwards,
    Help,
}

/// An editing action on the compose box.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edit {
    Backspace,
    Delete,
    Left,
    Right,
    Up,
    Down,
    Home,
    End,
    DeleteWord,
    DeleteToLineStart,
}

/// What a key asks for, independent of which key it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    /// Ctrl-C: clear the compose box, or quit when there is nothing to clear.
    Cancel,
    Quit,
    Goto(Screen),
    ToggleDetails,
    Interrupt,
    /// Send the compose text with interrupt delivery.
    SendNow,
    Submit,
    Newline,
    /// Esc: close what is open, interrupt a working agent, go back.
    Escape,
    /// Tab: complete a command, or move focus between prompt and compose.
    Tab,
    Edit(Edit),
    Type(char),
    ScrollUp,
    ScrollDown,
    ScrollEnd,
    Prompt(PromptKey),
    /// List screens: move the selection.
    Up,
    Down,
    Activate,
    Refresh,
    NewSession,
    Archive,
    /// Attach to the selected row's session or agent.
    Attach,
}

/// A binding: key code, required modifiers, meaning.
type Binding = (KeyCode, KeyModifiers, Intent);

const NONE: KeyModifiers = KeyModifiers::NONE;
const CTRL: KeyModifiers = KeyModifiers::CONTROL;
const ALT: KeyModifiers = KeyModifiers::ALT;
const SHIFT: KeyModifiers = KeyModifiers::SHIFT;

/// Bindings that hold on every screen, checked first.
pub const GLOBAL: &[Binding] = &[
    (KeyCode::Char('c'), CTRL, Intent::Cancel),
    (KeyCode::Char('t'), CTRL, Intent::Goto(Screen::Overview)),
    (KeyCode::Char('o'), CTRL, Intent::ToggleDetails),
    (KeyCode::Char('1'), ALT, Intent::Goto(Screen::Session)),
    (KeyCode::Char('2'), ALT, Intent::Goto(Screen::Overview)),
    (KeyCode::Char('3'), ALT, Intent::Goto(Screen::Inbox)),
    (KeyCode::Char('4'), ALT, Intent::Goto(Screen::Workspaces)),
    (KeyCode::Char('5'), ALT, Intent::Goto(Screen::Usage)),
    (KeyCode::Char('6'), ALT, Intent::Goto(Screen::Logs)),
    (KeyCode::Char('7'), ALT, Intent::Goto(Screen::Forwards)),
    (KeyCode::F(1), NONE, Intent::Goto(Screen::Help)),
    (KeyCode::PageUp, NONE, Intent::ScrollUp),
    (KeyCode::PageDown, NONE, Intent::ScrollDown),
];

/// The session screen while the compose box has focus.
pub const COMPOSE: &[Binding] = &[
    (KeyCode::Enter, ALT, Intent::Newline),
    (KeyCode::Enter, SHIFT, Intent::Newline),
    (KeyCode::Char('j'), CTRL, Intent::Newline),
    (KeyCode::Enter, NONE, Intent::Submit),
    (KeyCode::Esc, NONE, Intent::Escape),
    (KeyCode::Tab, NONE, Intent::Tab),
    (KeyCode::Char('s'), CTRL, Intent::SendNow),
    (KeyCode::Char('x'), CTRL, Intent::Interrupt),
    (KeyCode::Char('d'), CTRL, Intent::Quit),
    (KeyCode::Char('a'), CTRL, Intent::Edit(Edit::Home)),
    (KeyCode::Char('e'), CTRL, Intent::Edit(Edit::End)),
    (KeyCode::Char('w'), CTRL, Intent::Edit(Edit::DeleteWord)),
    (
        KeyCode::Char('u'),
        CTRL,
        Intent::Edit(Edit::DeleteToLineStart),
    ),
    (KeyCode::Backspace, NONE, Intent::Edit(Edit::Backspace)),
    (KeyCode::Delete, NONE, Intent::Edit(Edit::Delete)),
    (KeyCode::Left, NONE, Intent::Edit(Edit::Left)),
    (KeyCode::Right, NONE, Intent::Edit(Edit::Right)),
    (KeyCode::Up, NONE, Intent::Edit(Edit::Up)),
    (KeyCode::Down, NONE, Intent::Edit(Edit::Down)),
    (KeyCode::Home, NONE, Intent::Edit(Edit::Home)),
    (KeyCode::End, NONE, Intent::Edit(Edit::End)),
    (KeyCode::End, CTRL, Intent::ScrollEnd),
];

/// The session screen (or the inbox) while a prompt card has focus.
pub const PROMPT: &[Binding] = &[
    (KeyCode::Up, NONE, Intent::Prompt(PromptKey::Up)),
    (KeyCode::Down, NONE, Intent::Prompt(PromptKey::Down)),
    (KeyCode::Left, NONE, Intent::Prompt(PromptKey::Left)),
    (KeyCode::Right, NONE, Intent::Prompt(PromptKey::Right)),
    (KeyCode::Enter, NONE, Intent::Prompt(PromptKey::Enter)),
    (KeyCode::Char(' '), NONE, Intent::Prompt(PromptKey::Space)),
    (
        KeyCode::Backspace,
        NONE,
        Intent::Prompt(PromptKey::Backspace),
    ),
    (KeyCode::Tab, NONE, Intent::Tab),
    (KeyCode::Esc, NONE, Intent::Escape),
    (KeyCode::Char('x'), CTRL, Intent::Interrupt),
];

/// The inbox list, checked before [`LIST`]: answer the selected entry in
/// place, open its card, or go to its agent.
pub const INBOX: &[Binding] = &[
    (
        KeyCode::Char('y'),
        NONE,
        Intent::Prompt(PromptKey::Char('y')),
    ),
    (
        KeyCode::Char('n'),
        NONE,
        Intent::Prompt(PromptKey::Char('n')),
    ),
    (
        KeyCode::Char('1'),
        NONE,
        Intent::Prompt(PromptKey::Char('1')),
    ),
    (
        KeyCode::Char('2'),
        NONE,
        Intent::Prompt(PromptKey::Char('2')),
    ),
    (
        KeyCode::Char('3'),
        NONE,
        Intent::Prompt(PromptKey::Char('3')),
    ),
    (
        KeyCode::Char('4'),
        NONE,
        Intent::Prompt(PromptKey::Char('4')),
    ),
    (
        KeyCode::Char('5'),
        NONE,
        Intent::Prompt(PromptKey::Char('5')),
    ),
    (
        KeyCode::Char('6'),
        NONE,
        Intent::Prompt(PromptKey::Char('6')),
    ),
    (
        KeyCode::Char('7'),
        NONE,
        Intent::Prompt(PromptKey::Char('7')),
    ),
    (
        KeyCode::Char('8'),
        NONE,
        Intent::Prompt(PromptKey::Char('8')),
    ),
    (
        KeyCode::Char('9'),
        NONE,
        Intent::Prompt(PromptKey::Char('9')),
    ),
    (KeyCode::Char('g'), NONE, Intent::Attach),
];

/// Every list screen: overview, inbox, workspaces, usage, logs, forwards,
/// help.
pub const LIST: &[Binding] = &[
    (KeyCode::Up, NONE, Intent::Up),
    (KeyCode::Char('k'), NONE, Intent::Up),
    (KeyCode::Down, NONE, Intent::Down),
    (KeyCode::Char('j'), NONE, Intent::Down),
    (KeyCode::Enter, NONE, Intent::Activate),
    (KeyCode::Esc, NONE, Intent::Escape),
    (KeyCode::Char('q'), NONE, Intent::Quit),
    (KeyCode::Char('r'), NONE, Intent::Refresh),
    (KeyCode::Char('n'), NONE, Intent::NewSession),
    (KeyCode::Char('x'), NONE, Intent::Archive),
    (KeyCode::Char('s'), NONE, Intent::Goto(Screen::Session)),
    (KeyCode::Char('o'), NONE, Intent::Goto(Screen::Overview)),
    (KeyCode::Char('a'), NONE, Intent::Goto(Screen::Overview)),
    (KeyCode::Char('i'), NONE, Intent::Goto(Screen::Inbox)),
    (KeyCode::Char('w'), NONE, Intent::Goto(Screen::Workspaces)),
    (KeyCode::Char('u'), NONE, Intent::Goto(Screen::Usage)),
    (KeyCode::Char('l'), NONE, Intent::Goto(Screen::Logs)),
    (KeyCode::Char('f'), NONE, Intent::Goto(Screen::Forwards)),
    (KeyCode::Char('?'), NONE, Intent::Goto(Screen::Help)),
];

/// The first binding in `table` matching `key`. Modifiers must match
/// exactly, except that SHIFT is ignored on a character (terminals differ
/// on whether `?` arrives shifted).
#[must_use]
pub fn lookup(table: &[Binding], key: &KeyEvent) -> Option<Intent> {
    let modifiers = match key.code {
        KeyCode::Char(_) => key.modifiers - SHIFT,
        _ => key.modifiers,
    };
    table
        .iter()
        .find(|(code, wanted, _)| *code == key.code && *wanted == modifiers)
        .map(|(_, _, intent)| *intent)
}

/// A plain character — no control or alt — typed as text.
#[must_use]
pub fn typed(key: &KeyEvent) -> Option<char> {
    let chord = key.modifiers.intersects(CTRL.union(ALT));
    match key.code {
        KeyCode::Char(c) if !chord => Some(c),
        _ => None,
    }
}

/// What `key` means under `tables`, tried in order, with a plain character
/// falling through to `fallback` (text in the compose box, a prompt key on
/// a card, nothing on a list).
#[must_use]
pub fn resolve(
    tables: &[&[Binding]],
    key: &KeyEvent,
    fallback: fn(char) -> Option<Intent>,
) -> Option<Intent> {
    tables
        .iter()
        .find_map(|table| lookup(table, key))
        .or_else(|| typed(key).and_then(fallback))
}

/// The fallback for the compose box: characters are text.
#[must_use]
pub fn as_text(c: char) -> Option<Intent> {
    Some(Intent::Type(c))
}

/// The fallback for a prompt card: characters go to the card.
#[must_use]
pub fn as_prompt(c: char) -> Option<Intent> {
    Some(Intent::Prompt(PromptKey::Char(c)))
}

/// The fallback for lists: unbound characters do nothing.
#[must_use]
pub fn ignored(_: char) -> Option<Intent> {
    None
}

/// A key's name as the help screen prints it: `ctrl-t`, `alt-3`, `enter`.
#[must_use]
pub fn key_name(code: KeyCode, modifiers: KeyModifiers) -> String {
    let base = match code {
        KeyCode::Char(' ') => "space".to_owned(),
        KeyCode::Char(c) => c.to_string(),
        KeyCode::F(n) => format!("f{n}"),
        KeyCode::PageUp => "pgup".to_owned(),
        KeyCode::PageDown => "pgdn".to_owned(),
        other => format!("{other:?}").to_lowercase(),
    };
    let prefix = [(CTRL, "ctrl-"), (ALT, "alt-"), (SHIFT, "shift-")]
        .iter()
        .filter(|(modifier, _)| modifiers.contains(*modifier))
        .map(|(_, name)| *name)
        .collect::<String>();
    format!("{prefix}{base}")
}

#[cfg(test)]
mod tests {
    use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::{
        COMPOSE, Edit, GLOBAL, Intent, LIST, PROMPT, Screen, as_prompt, as_text, ignored, key_name,
        lookup, resolve, typed,
    };
    use crate::prompt::PromptKey;

    fn key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    #[test]
    fn modifiers_must_match_except_shift_on_characters() {
        let enter = key(KeyCode::Enter, KeyModifiers::NONE);
        assert_eq!(lookup(COMPOSE, &enter), Some(Intent::Submit));
        let alt_enter = key(KeyCode::Enter, KeyModifiers::ALT);
        assert_eq!(lookup(COMPOSE, &alt_enter), Some(Intent::Newline));
        let shift_enter = key(KeyCode::Enter, KeyModifiers::SHIFT);
        assert_eq!(lookup(COMPOSE, &shift_enter), Some(Intent::Newline));
        let question = key(KeyCode::Char('?'), KeyModifiers::SHIFT);
        assert_eq!(lookup(LIST, &question), Some(Intent::Goto(Screen::Help)));
        let ctrl_shift_c = key(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        );
        assert_eq!(lookup(GLOBAL, &ctrl_shift_c), Some(Intent::Cancel));
        assert_eq!(
            lookup(GLOBAL, &key(KeyCode::Char('c'), KeyModifiers::NONE)),
            None
        );
    }

    #[test]
    fn plain_characters_type_and_chords_do_not() {
        assert_eq!(
            typed(&key(KeyCode::Char('a'), KeyModifiers::NONE)),
            Some('a')
        );
        assert_eq!(
            typed(&key(KeyCode::Char('A'), KeyModifiers::SHIFT)),
            Some('A')
        );
        assert_eq!(typed(&key(KeyCode::Char('a'), KeyModifiers::CONTROL)), None);
        assert_eq!(typed(&key(KeyCode::Char('a'), KeyModifiers::ALT)), None);
        assert_eq!(typed(&key(KeyCode::Enter, KeyModifiers::NONE)), None);
    }

    #[test]
    fn resolution_tries_tables_in_order_then_the_fallback() {
        let tables: &[&[_]] = &[GLOBAL, COMPOSE];
        let ctrl_t = key(KeyCode::Char('t'), KeyModifiers::CONTROL);
        assert_eq!(
            resolve(tables, &ctrl_t, as_text),
            Some(Intent::Goto(Screen::Overview))
        );
        let left = key(KeyCode::Left, KeyModifiers::NONE);
        assert_eq!(
            resolve(tables, &left, as_text),
            Some(Intent::Edit(Edit::Left))
        );
        let q = key(KeyCode::Char('q'), KeyModifiers::NONE);
        assert_eq!(resolve(tables, &q, as_text), Some(Intent::Type('q')));
        assert_eq!(
            resolve(&[GLOBAL, PROMPT], &q, as_prompt),
            Some(Intent::Prompt(PromptKey::Char('q')))
        );
        assert_eq!(resolve(&[GLOBAL, LIST], &q, ignored), Some(Intent::Quit));
        let z = key(KeyCode::Char('z'), KeyModifiers::NONE);
        assert_eq!(resolve(&[GLOBAL, LIST], &z, ignored), None);
        let ctrl_z = key(KeyCode::Char('z'), KeyModifiers::CONTROL);
        assert_eq!(
            resolve(tables, &ctrl_z, as_text),
            None,
            "an unbound chord is not text"
        );
    }

    #[test]
    fn keys_name_themselves_for_the_help_screen() {
        assert_eq!(
            key_name(KeyCode::Char('t'), KeyModifiers::CONTROL),
            "ctrl-t"
        );
        assert_eq!(key_name(KeyCode::Char('3'), KeyModifiers::ALT), "alt-3");
        assert_eq!(key_name(KeyCode::Enter, KeyModifiers::SHIFT), "shift-enter");
        assert_eq!(key_name(KeyCode::Char(' '), KeyModifiers::NONE), "space");
        assert_eq!(key_name(KeyCode::F(1), KeyModifiers::NONE), "f1");
        assert_eq!(key_name(KeyCode::PageUp, KeyModifiers::NONE), "pgup");
        assert_eq!(key_name(KeyCode::PageDown, KeyModifiers::NONE), "pgdn");
        assert_eq!(key_name(KeyCode::Esc, KeyModifiers::NONE), "esc");
    }
}
