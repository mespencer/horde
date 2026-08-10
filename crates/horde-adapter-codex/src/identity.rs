//! What it takes to find a Codex thread again.
//!
//! The type is [`horde_adapter::RevivalIdentity`], because the rule is the same
//! for every provider. What is Codex-specific is which directory it names:
//! `CODEX_HOME` locates credentials *and* rollout files. Spike W2c confirmed
//! the CLI's own help says so — `--ignore-user-config` skips `config.toml` but
//! "auth still uses `CODEX_HOME`" — and threads persist as rollout JSONL under
//! `$CODEX_HOME/sessions/`. One Provider Account is one `CODEX_HOME`.

use std::path::{Path, PathBuf};

use horde_adapter::ProviderAccount;

pub use horde_adapter::RevivalIdentity;

/// Resolves an account name to the `CODEX_HOME` it runs under.
#[must_use]
pub fn codex_home_for(account: Option<&ProviderAccount>, accounts_root: &Path) -> Option<PathBuf> {
    horde_adapter::account_dir(account, accounts_root)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use horde_adapter::ProviderAccount;

    use super::codex_home_for;

    #[test]
    fn an_account_names_a_directory_under_the_accounts_root() {
        let account = ProviderAccount::new("work");
        assert_eq!(
            codex_home_for(Some(&account), Path::new("/accounts")),
            Some(Path::new("/accounts/work").to_path_buf()),
            "one account is one CODEX_HOME"
        );
    }

    #[test]
    fn no_account_means_the_inherited_directory() {
        assert_eq!(codex_home_for(None, Path::new("/accounts")), None);
    }
}
