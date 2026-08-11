//! What it takes to find a provider's conversation again.
//!
//! A provider's own session identifier is not enough to revive an agent, and
//! treating it as though it were is the kind of mistake that only shows up once
//! a Worker holds two accounts. Both harnesses Horde drives store their sessions
//! *inside* the per-account directory — `CLAUDE_CONFIG_DIR` for one,
//! `CODEX_HOME` for the other — so a session recorded under one account simply
//! does not exist under another.
//!
//! The account directory is therefore part of an agent's revival identity, not a
//! spawn-time detail. [`RevivalIdentity`] makes that a type rather than a
//! convention, so a Worker cannot journal half of it.

use std::path::{Path, PathBuf};

use crate::ids::ProviderSessionId;

/// Everything needed to resume one conversation with one provider.
///
/// Both halves must be journaled together. The account binding is held for the
/// agent's life (DESIGN.md §5); rebalancing across accounts happens by spawning
/// new agents, never by reviving an existing one somewhere else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevivalIdentity {
    /// The provider's own identifier for the conversation.
    pub provider_session_id: ProviderSessionId,
    /// The per-account directory the session was recorded under.
    ///
    /// `None` means the ambient default — fine for a single-account Worker, and
    /// a latent bug on a Worker that later grows a second account, since the
    /// default is whichever account happens to own the harness's home.
    pub account_dir: Option<PathBuf>,
}

impl RevivalIdentity {
    /// An identity bound to an explicit account directory.
    #[must_use]
    pub fn new(provider_session_id: ProviderSessionId, account_dir: impl Into<PathBuf>) -> Self {
        Self {
            provider_session_id,
            account_dir: Some(account_dir.into()),
        }
    }

    /// An identity under whatever account directory the process inherits.
    #[must_use]
    pub fn ambient(provider_session_id: ProviderSessionId) -> Self {
        Self {
            provider_session_id,
            account_dir: None,
        }
    }

    /// Whether this session could be resumed under `account_dir`.
    ///
    /// Refusing a mismatch is the point: resuming under the wrong account does
    /// not find the conversation, and a Worker that checks first can report the
    /// mistake instead of surfacing an authentication error that looks like an
    /// outage.
    #[must_use]
    pub fn is_resumable_under(&self, account_dir: Option<&Path>) -> bool {
        self.account_dir.as_deref() == account_dir
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::RevivalIdentity;
    use crate::ids::{ProviderAccount, ProviderSessionId, account_dir};

    #[test]
    fn an_identity_is_resumable_only_under_its_own_account_dir() {
        let identity = RevivalIdentity::new(ProviderSessionId::new("abc"), "/accounts/work");

        assert!(identity.is_resumable_under(Some(Path::new("/accounts/work"))));
        assert!(
            !identity.is_resumable_under(Some(Path::new("/accounts/personal"))),
            "the same id under another account is a different conversation"
        );
        assert!(
            !identity.is_resumable_under(None),
            "the ambient default is not the account this was recorded under"
        );
    }

    #[test]
    fn an_ambient_identity_is_resumable_only_ambiently() {
        let identity = RevivalIdentity::ambient(ProviderSessionId::new("abc"));

        assert!(identity.is_resumable_under(None));
        assert!(!identity.is_resumable_under(Some(Path::new("/accounts/work"))));
    }

    #[test]
    fn an_account_resolves_under_the_root_that_holds_them() {
        let account = ProviderAccount::new("work");
        assert_eq!(
            account_dir(Some(&account), Path::new("/accounts")),
            Some(Path::new("/accounts/work").to_path_buf())
        );
    }

    #[test]
    fn no_account_means_the_ambient_directory() {
        assert_eq!(
            account_dir(None, Path::new("/accounts")),
            None,
            "no account named is the inherited default, not a directory called none"
        );
    }
}
