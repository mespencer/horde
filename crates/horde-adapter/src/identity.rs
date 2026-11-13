//! What it takes to find a provider's conversation again.
//!
//! A provider's own session identifier is not enough to revive an agent, and
//! treating it as though it were is the kind of mistake that only shows up once
//! a Worker holds two accounts. Both harnesses Horde drives store their sessions
//! *inside* the per-account directory — `CLAUDE_CONFIG_DIR` for one,
//! `CODEX_HOME` for the other — so a session recorded under one account simply
//! does not exist under another.
//!
//! The revival identity is therefore the pair **(provider session id, account
//! binding)**, and the Worker journals both halves together: the session id
//! when the harness names it, and the account inside the spawn command it
//! keeps whole (`horde_worker::RevivableAgent`). Revival rebuilds the spec from
//! that spawn command, so the account directory is re-derived by
//! [`account_dir`](crate::account_dir) under the same Worker state directory
//! the journal itself lives in — the directory a conversation is resumed under
//! is the one it was recorded under by construction, never by a comparison
//! made afterwards.
//!
//! (A `RevivalIdentity` type with an `is_resumable_under` guard stood here
//! until W12c. Nothing in production called it — the comparison it offered is
//! between a directory and its own recomputation — so it was deleted rather
//! than wired; CONFORMANCE contract disagreement 7.)

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::ids::{ProviderAccount, account_dir};

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
