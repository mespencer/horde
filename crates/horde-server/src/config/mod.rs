//! The server's configuration (DESIGN §13, ADR-0016): its key table, read
//! strictly from `~/.horde/server/config.toml` with `horde-config`, layered
//! under its flags, and the per-agent resolution through the workspace and
//! session layers.
//!
//! This module declares and re-exports; it does not define.

pub mod agent;
pub mod permissions;
pub mod settings;
pub mod tiers;
pub mod workspace;

pub use agent::{SessionLayer, agent_entries, agent_mode};
pub use settings::{ServerFlags, ServerSettings};
pub use workspace::WorkspaceFlags;
