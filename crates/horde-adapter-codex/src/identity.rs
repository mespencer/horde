//! What it takes to find a Codex thread again.
//!
//! The type is [`horde_adapter::RevivalIdentity`], because the rule is the same
//! for every provider. What is Codex-specific is which directory it names:
//! `CODEX_HOME` locates credentials *and* rollout files. Spike W2c confirmed
//! the CLI's own help says so — `--ignore-user-config` skips `config.toml` but
//! "auth still uses `CODEX_HOME`" — and threads persist as rollout JSONL under
//! `$CODEX_HOME/sessions/`. One Provider Account is one `CODEX_HOME`.

pub use horde_adapter::RevivalIdentity;
/// Resolves an account name to the `CODEX_HOME` it runs under.
///
/// The resolution is provider-independent — see [`horde_adapter::account_dir`];
/// only the name of the directory is ours.
pub use horde_adapter::account_dir as codex_home_for;
