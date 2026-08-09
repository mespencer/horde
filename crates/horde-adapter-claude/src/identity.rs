//! What it takes to find a Claude conversation again.
//!
//! A `session_id` on its own is not enough to revive an agent, and treating it
//! as though it were is the kind of mistake that only shows up once a Worker
//! holds two accounts. Spike W1c established why: `CLAUDE_CONFIG_DIR` fully
//! isolates an account, and the CLI stores its `sessions/` and `projects/`
//! *inside that directory*. A session recorded under one config directory
//! simply does not exist under another — and because an unauthenticated config
//! directory fails loudly rather than falling back, the failure mode is a hard
//! error rather than a silently fresh conversation.
//!
//! So the config directory is part of an agent's revival identity, not a
//! spawn-time detail. [`RevivalIdentity`] makes that a type rather than a
//! convention, so a Worker cannot journal half of it.

use std::path::{Path, PathBuf};

use horde_adapter::{ProviderAccount, ProviderSessionId};

/// Everything needed to resume one Claude conversation.
///
/// Both halves must be journaled together. The account binding is held for the
/// agent's life (DESIGN.md §5); rebalancing across accounts happens by spawning
/// new agents, never by reviving an existing one somewhere else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevivalIdentity {
    pub provider_session_id: ProviderSessionId,
    /// The `CLAUDE_CONFIG_DIR` the session was recorded under.
    ///
    /// `None` means the ambient default — fine for a single-account Worker, and
    /// a latent bug on a Worker that later grows a second account, since the
    /// default is whichever account happens to own `~/.claude`.
    pub config_dir: Option<PathBuf>,
}

impl RevivalIdentity {
    /// An identity bound to an explicit account directory.
    #[must_use]
    pub fn new(provider_session_id: ProviderSessionId, config_dir: impl Into<PathBuf>) -> Self {
        Self {
            provider_session_id,
            config_dir: Some(config_dir.into()),
        }
    }

    /// An identity under whatever config directory the process inherits.
    #[must_use]
    pub fn ambient(provider_session_id: ProviderSessionId) -> Self {
        Self {
            provider_session_id,
            config_dir: None,
        }
    }

    /// Whether this session could be resumed under `config_dir`.
    ///
    /// Refusing a mismatch is the point: resuming under the wrong account does
    /// not find the conversation, and a Worker that checks first can report the
    /// mistake instead of surfacing an authentication error that looks like an
    /// outage.
    #[must_use]
    pub fn is_resumable_under(&self, config_dir: Option<&Path>) -> bool {
        self.config_dir.as_deref() == config_dir
    }
}

/// Resolves an account name to the config directory it runs under.
///
/// A Worker-level resource (DESIGN.md §4): accounts are directories the Worker
/// owns and advertises as a capability.
#[must_use]
pub fn config_dir_for(account: Option<&ProviderAccount>, accounts_root: &Path) -> Option<PathBuf> {
    account.map(|account| accounts_root.join(account.as_str()))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use horde_adapter::{ProviderAccount, ProviderSessionId};

    use super::{RevivalIdentity, config_dir_for};

    #[test]
    fn an_identity_is_resumable_only_under_its_own_config_dir() {
        let identity = RevivalIdentity::new(ProviderSessionId::new("abc"), "/accounts/work");

        assert!(identity.is_resumable_under(Some(Path::new("/accounts/work"))));
        assert!(
            !identity.is_resumable_under(Some(Path::new("/accounts/personal"))),
            "a session recorded under one account does not exist under another"
        );
        assert!(
            !identity.is_resumable_under(None),
            "the ambient directory is a different account, not a wildcard"
        );
    }

    #[test]
    fn an_ambient_identity_matches_only_the_ambient_directory() {
        let identity = RevivalIdentity::ambient(ProviderSessionId::new("abc"));
        assert!(identity.is_resumable_under(None));
        assert!(!identity.is_resumable_under(Some(Path::new("/accounts/work"))));
    }

    #[test]
    fn accounts_resolve_to_directories_beneath_the_workers_root() {
        let account = ProviderAccount::new("work");
        assert_eq!(
            config_dir_for(Some(&account), Path::new("/var/horde/accounts")),
            Some(PathBuf::from("/var/horde/accounts/work"))
        );
        assert_eq!(config_dir_for(None, Path::new("/var/horde/accounts")), None);
    }
}
