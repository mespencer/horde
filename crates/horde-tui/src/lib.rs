//! The Horde TUI, as a library.
//!
//! DESIGN.md §11: the TUI is optional and the CLI is the parity surface, so
//! everything here is a *view* over the same RPCs `horde <verb>` uses — this
//! crate holds no transport at all. [`App`] is a pure state machine: server
//! [`Update`]s and terminal keys go in, [`Directive`]s (what to ask the
//! server) come out. Keys are first translated to [`keys::Intent`]s by
//! lookup tables, so what a key *means* in each context is data a test can
//! read, and [`view::render`] draws the state on any ratatui backend. The
//! interactive loop that pumps a real terminal and a real client lives in
//! `horde-cli`, thin enough to have no decisions of its own.
//!
//! That split is what makes the TUI testable headlessly: tests drive [`App`]
//! with the same [`Update`]s the runtime feeds it and render onto ratatui's
//! `TestBackend`, and the insta snapshots of those buffers are the UI diff
//! (docs/TESTING.md §2).
//!
//! # Example
//!
//! ```
//! use horde_tui::{App, Screen};
//! use ratatui::crossterm::event::{Event, KeyCode, KeyEvent};
//!
//! let mut app = App::overview();
//! let directives = app.on_input(&Event::Key(KeyEvent::from(KeyCode::Char('i'))));
//! assert!(directives.is_empty(), "navigation never crosses the wire");
//! assert_eq!(app.screen(), Screen::Inbox);
//! ```

pub mod app;
pub mod compose;
#[cfg(test)]
mod fixtures;
pub mod keys;
pub mod layout;
pub mod prompt;
pub mod slash;
pub mod state;
pub mod theme;
pub mod transcript;
pub mod tree;
pub mod view;
pub mod words;
pub mod wrap;

pub use app::{App, Attachment, Directive, InboxEntry, Screen, Update};
pub use state::{FeedLine, PendingPermission, SessionState};
