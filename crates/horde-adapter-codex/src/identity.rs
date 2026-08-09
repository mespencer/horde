//! What it takes to find a Codex thread again.
//!
//! A `threadId` on its own is not enough, for the same reason a Claude
//! `session_id` is not (see `horde-adapter-claude`'s equivalent): `CODEX_HOME`
//! locates credentials *and* rollout files. Spike W2c confirmed the CLI's own
//! help says so — `--ignore-user-config` skips `config.toml` but "auth still
//! uses `CODEX_HOME`" — and threads persist as rollout JSONL under
//! `$CODEX_HOME/sessions/`. A thread recorded under one home does not exist
//! under another.
//!
//! So the home directory is part of an agent's revival identity, not a
//! spawn-time detail, and one Provider Account is one `CODEX_HOME`.

use std::path::{Path, PathBuf};

use horde_adapter::{ProviderAccount, ProviderSessionId};

/// Everything needed to resume one Codex thread.
///
/// Both halves must be journaled together. The account binding is held for the
/// agent's life (DESIGN.md §5); rebalancing across accounts happens by spawning
/// new agents, never by reviving an existing one somewhere else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevivalIdentity {
    pub provider_session_id: ProviderSessionId,
    /// The `CODEX_HOME` the thread was recorded under.
    ///
    /// `None` means the ambient default — fine for a single-account Worker, and
    /// a latent bug on a Worker that later grows a second account, since the
    /// default is whichever account happens to own `~/.codex`.
    pub codex_home: Option<PathBuf>,
}

impl RevivalIdentity {
    /// An identity bound to an explicit account home.
    #[must_use]
    pub fn new(provider_session_id: ProviderSessionId, codex_home: impl Into<PathBuf>) -> Self {
        Self {
            provider_session_id,
            codex_home: Some(codex_home.into()),
        }
    }

    /// An identity under whatever `CODEX_HOME` the process inherits.
    #[must_use]
    pub fn ambient(provider_session_id: ProviderSessionId) -> Self {
        Self {
            provider_session_id,
            codex_home: None,
        }
    }

    /// Whether this thread could be resumed under `codex_home`.
    ///
    /// Refusing a mismatch is the point: resuming under the wrong account does
    /// not find the thread, and a Worker that checks first can report the
    /// mistake instead of surfacing an auth error that looks like an outage.
    #[must_use]
    pub fn is_resumable_under(&self, codex_home: Option<&Path>) -> bool {
        self.codex_home.as_deref() == codex_home
    }

    /// Where this thread's rollout file lives, given its home.
    ///
    /// Diagnostic only — the Worker resumes by id through the protocol, never
    /// by reading rollouts. It exists so an operator chasing a missing thread
    /// is looking in the right directory.
    #[must_use]
    pub fn sessions_dir(&self) -> Option<PathBuf> {
        self.codex_home.as_ref().map(|home| home.join("sessions"))
    }
}

/// Resolves an account name to the `CODEX_HOME` it runs under.
///
/// A Worker-level resource (DESIGN.md §4): accounts are directories the Worker
/// owns and advertises as a capability.
#[must_use]
pub fn codex_home_for(account: Option<&ProviderAccount>, accounts_root: &Path) -> Option<PathBuf> {
    account.map(|account| accounts_root.join(account.as_str()))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use horde_adapter::{ProviderAccount, ProviderSessionId};

    use super::{RevivalIdentity, codex_home_for};

    fn thread() -> ProviderSessionId {
        ProviderSessionId::new("019fe869-e1f5-7c00-ad44-992a1921eb48")
    }

    #[test]
    fn an_identity_is_resumable_only_under_its_own_home() {
        let identity = RevivalIdentity::new(thread(), "/accounts/work");

        assert!(identity.is_resumable_under(Some(Path::new("/accounts/work"))));
        assert!(
            !identity.is_resumable_under(Some(Path::new("/accounts/personal"))),
            "a thread recorded under one CODEX_HOME does not exist under another"
        );
        assert!(
            !identity.is_resumable_under(None),
            "the ambient home is a different account, not a wildcard"
        );
    }

    #[test]
    fn an_ambient_identity_matches_only_the_ambient_home() {
        let identity = RevivalIdentity::ambient(thread());
        assert!(identity.is_resumable_under(None));
        assert!(!identity.is_resumable_under(Some(Path::new("/accounts/work"))));
    }

    #[test]
    fn rollouts_are_looked_for_beneath_the_bound_home() {
        let identity = RevivalIdentity::new(thread(), "/accounts/work");
        assert_eq!(
            identity.sessions_dir(),
            Some(PathBuf::from("/accounts/work/sessions"))
        );
        assert_eq!(
            RevivalIdentity::ambient(thread()).sessions_dir(),
            None,
            "an ambient identity cannot name a directory it did not bind"
        );
    }

    #[test]
    fn accounts_resolve_to_homes_beneath_the_workers_root() {
        let account = ProviderAccount::new("work");
        assert_eq!(
            codex_home_for(Some(&account), Path::new("/var/horde/codex")),
            Some(PathBuf::from("/var/horde/codex/work"))
        );
        assert_eq!(codex_home_for(None, Path::new("/var/horde/codex")), None);
    }
}
