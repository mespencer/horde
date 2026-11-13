//! The palette: a handful of named styles, all in the terminal's own ANSI
//! colours so the user's theme decides the actual hues.
//!
//! Restrained on purpose (an operator's tool, not a poster): text is the
//! default colour, chrome is dim, and colour is spent on *state* — green for
//! done, yellow for in-flight or warned, red for failed or denied, cyan for
//! what has focus, magenta for what needs a human.

use ratatui::style::{Color, Modifier, Style};

#[must_use]
pub fn dim() -> Style {
    Style::new().fg(Color::DarkGray)
}

/// Unstyled text. A constant rather than a function: it is the default
/// style itself, so there is nothing about it a test could tell apart.
pub const PLAIN: Style = Style::new();

#[must_use]
pub fn bold() -> Style {
    Style::new().add_modifier(Modifier::BOLD)
}

#[must_use]
pub fn focus() -> Style {
    Style::new().fg(Color::Cyan)
}

#[must_use]
pub fn selected() -> Style {
    Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD)
}

#[must_use]
pub fn ok() -> Style {
    Style::new().fg(Color::Green)
}

#[must_use]
pub fn warn() -> Style {
    Style::new().fg(Color::Yellow)
}

#[must_use]
pub fn error() -> Style {
    Style::new().fg(Color::Red)
}

#[must_use]
pub fn needs_you() -> Style {
    Style::new().fg(Color::Magenta).add_modifier(Modifier::BOLD)
}

#[must_use]
pub fn thinking() -> Style {
    Style::new()
        .fg(Color::DarkGray)
        .add_modifier(Modifier::ITALIC)
}
