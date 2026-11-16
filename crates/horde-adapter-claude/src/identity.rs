//! What it takes to find a Claude conversation again.
//!
//! The rule is the same for every provider (see [`horde_adapter::identity`]).
//! What is Claude-specific is which directory it names:
//! spike W1c established that `CLAUDE_CONFIG_DIR` fully isolates an account,
//! and that the CLI stores its `sessions/` and `projects/` *inside* that
//! directory. A session recorded under one config directory does not exist
//! under another — and because an unauthenticated config directory fails loudly
//! rather than falling back, the failure mode is a hard error rather than a
//! silently fresh conversation.

/// Resolves an account name to the `CLAUDE_CONFIG_DIR` it runs under.
///
/// The resolution is provider-independent — see [`horde_adapter::account_dir`];
/// only the name of the directory is ours.
pub use horde_adapter::account_dir as config_dir_for;
