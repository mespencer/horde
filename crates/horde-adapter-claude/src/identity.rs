//! What it takes to find a Claude conversation again.
//!
//! The type is [`horde_adapter::RevivalIdentity`], because the rule is the same
//! for every provider. What is Claude-specific is which directory it names:
//! spike W1c established that `CLAUDE_CONFIG_DIR` fully isolates an account,
//! and that the CLI stores its `sessions/` and `projects/` *inside* that
//! directory. A session recorded under one config directory does not exist
//! under another — and because an unauthenticated config directory fails loudly
//! rather than falling back, the failure mode is a hard error rather than a
//! silently fresh conversation.

use std::path::{Path, PathBuf};

use horde_adapter::ProviderAccount;

pub use horde_adapter::RevivalIdentity;

/// Resolves an account name to the `CLAUDE_CONFIG_DIR` it runs under.
#[must_use]
pub fn config_dir_for(account: Option<&ProviderAccount>, accounts_root: &Path) -> Option<PathBuf> {
    horde_adapter::account_dir(account, accounts_root)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use horde_adapter::ProviderAccount;

    use super::config_dir_for;

    #[test]
    fn an_account_names_a_directory_under_the_accounts_root() {
        let account = ProviderAccount::new("work");
        assert_eq!(
            config_dir_for(Some(&account), Path::new("/accounts")),
            Some(Path::new("/accounts/work").to_path_buf()),
            "one account is one CLAUDE_CONFIG_DIR"
        );
    }

    #[test]
    fn no_account_means_the_inherited_directory() {
        assert_eq!(config_dir_for(None, Path::new("/accounts")), None);
    }
}
