//! The Horde client.
//!
//! DESIGN.md §11 makes CLI parity a hard rule: every operation a client can
//! perform is an RPC, and nothing is reachable only through a UI. This crate is
//! shaped to keep that true rather than to promise it — the typed calls live
//! here in the library, and the CLI in `bin/horde.rs` is a thin argument parser
//! over them. The TUI, when it lands, is a second frontend over this same
//! surface, so a verb cannot exist in one and not the other.
//!
//! Nothing here formats for a human. Rendering is the frontend's business;
//! [`Client`] returns wire types and domain values, and [`render`] holds the
//! CLI's two presentations of them.

pub mod agent;
pub mod cli;
pub mod client;
pub mod connection;
pub mod doctor;
pub mod forward;
pub mod front_door;
pub mod login;
pub mod names;
pub mod picker;
pub mod render;
pub mod resolve;
pub mod tui;
pub mod up;
pub mod workspace_flags;
pub mod workspace_gc;
pub mod workspace_update;

pub use client::{Client, ClientError, SyncOptions};
pub use render::Format;
