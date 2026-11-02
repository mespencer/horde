//! The Worker-owned harness profile (ADR-0010).
//!
//! A supervised harness must read configuration this Worker owns, never the
//! operator's. Claude merges permission allow lists from every settings file
//! it can find, so an operator's `~/.claude/settings.json` — or the checked-out
//! repo's `.claude/settings.json` — can pre-approve a tool call before Horde's
//! permission chain ever sees it; an ambient `CODEX_HOME` brings the
//! operator's `config.toml`, MCP servers, and trusted projects. One
//! [`HarnessProfile`] per adapter owns everything that closes those doors:
//!
//! * **The config directory** — `CLAUDE_CONFIG_DIR` / `CODEX_HOME`, always set,
//!   always under the Worker's state directory: `<root>/default` for an agent
//!   bound to no account, `<root>/accounts/<account>` for one that is. Absolute
//!   paths, created owner-only, so an account never resolves relative to the
//!   child's working directory (inside the checkout).
//! * **Secrets off argv** — anything carrying a bearer token is written to an
//!   owner-only (0600) file under `<root>/run`, held by the harness and deleted
//!   with it; `ps` shows the path, never the token.
//! * **Process groups** — the spec it builds carries the ledger and stop grace
//!   [`crate::process`] needs to stop a harness and everything it started.
//!
//! The provider-specific flags (Claude's `--setting-sources ''` and
//! `--strict-mcp-config`; Codex's `env_http_headers`) live with each adapter,
//! built on this profile.
//!
//! A worker-owned config directory starts with no credentials: an agent run
//! there fails loudly with the harness's own "not logged in" until
//! `horde worker login` signs that directory in — which is why the login
//! recipe each adapter publishes carries [`HarnessProfile::login_env`].

use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use horde_adapter::ProviderAccount;

use crate::journal::Journal;
use crate::process::{DEFAULT_STOP_GRACE, GroupLedger, HarnessGroup, HarnessSpec};

/// Where profiles live inside the Worker's state directory.
pub const HARNESS_DIR: &str = "harness";

/// The config directory an account-less agent runs under.
const DEFAULT_DIR: &str = "default";

/// Where per-account config directories live.
const ACCOUNTS_DIR: &str = "accounts";

/// Where per-launch secret files live.
const RUN_DIR: &str = "run";

/// One adapter's Worker-owned harness state.
#[derive(Debug, Clone)]
pub struct HarnessProfile {
    root: PathBuf,
    ledger: Option<Arc<dyn GroupLedger>>,
    stop_grace: std::time::Duration,
}

impl HarnessProfile {
    /// A profile rooted at `root`, made absolute now: a relative root would
    /// resolve against each child's working directory instead.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            root: std::path::absolute(&root).unwrap_or(root),
            ledger: None,
            stop_grace: DEFAULT_STOP_GRACE,
        }
    }

    /// The production layout: `<state_dir>/harness/<provider>`.
    #[must_use]
    pub fn in_state_dir(state_dir: &Path, provider: &str) -> Self {
        Self::new(state_dir.join(HARNESS_DIR).join(provider))
    }

    /// Remembers every spawned harness group in `ledger`, so a crashed run's
    /// leftovers can be swept at the next start.
    #[must_use]
    pub fn with_ledger(mut self, ledger: Arc<dyn GroupLedger>) -> Self {
        self.ledger = Some(ledger);
        self
    }

    /// The SIGTERM-to-SIGKILL grace a stopped harness group gets.
    #[must_use]
    pub fn with_stop_grace(mut self, grace: std::time::Duration) -> Self {
        self.stop_grace = grace;
        self
    }

    /// The profile's root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The config directory an agent bound to `account` runs under, created
    /// owner-only if it is not there yet.
    ///
    /// # Errors
    ///
    /// An account name that is not a single plain directory component (`..`,
    /// a separator, a leading dot) is refused rather than joined: an account
    /// arrives over the wire, and a cleaned-up name would silently point two
    /// accounts at one directory. Also refused when the directory cannot be
    /// created.
    pub fn config_dir(&self, account: Option<&ProviderAccount>) -> Result<PathBuf, String> {
        let dir = match account {
            None => self.root.join(DEFAULT_DIR),
            Some(account) if crate::workspace::is_path_safe(account.as_str()) => {
                self.root.join(ACCOUNTS_DIR).join(account.as_str())
            }
            Some(account) => {
                return Err(format!(
                    "account {:?} is not a plain directory name",
                    account.as_str()
                ));
            }
        };
        owner_only_dir(&dir)?;
        Ok(dir)
    }

    /// The accounts this profile holds a sign-in for, in name order: every
    /// `<root>/accounts/<account>` directory the harness has written into.
    /// A login prepares the directory empty before the provider's command
    /// runs, so an empty one is a login that never got as far as saving
    /// anything — not an account placement may count on.
    #[must_use]
    pub fn signed_in_accounts(&self) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(self.root.join(ACCOUNTS_DIR)) else {
            return Vec::new();
        };
        let mut accounts: Vec<String> = entries
            .filter_map(Result::ok)
            .filter(|entry| {
                std::fs::read_dir(entry.path()).is_ok_and(|mut inside| inside.next().is_some())
            })
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| crate::workspace::is_path_safe(name))
            .collect();
        accounts.sort();
        accounts
    }

    /// Writes `contents` to a fresh owner-only (0600) file under the
    /// profile's run directory. The file is deleted when the last clone of
    /// the returned handle drops — hand it to the [`HarnessSpec`] and it
    /// lives exactly as long as the harness.
    ///
    /// # Errors
    ///
    /// When the directory or the file cannot be created or written.
    pub fn secret_file(&self, prefix: &str, contents: &[u8]) -> Result<SecretFile, String> {
        let run = self.root.join(RUN_DIR);
        owner_only_dir(&run)?;
        let mut file = tempfile::Builder::new()
            .prefix(prefix)
            .suffix(".json")
            .permissions(std::fs::Permissions::from_mode(0o600))
            .tempfile_in(&run)
            .map_err(|error| {
                format!(
                    "could not create a secret file in {}: {error}",
                    run.display()
                )
            })?;
        file.write_all(contents)
            .and_then(|()| file.flush())
            .map_err(|error| format!("could not write a secret file: {error}"))?;
        Ok(SecretFile(Arc::new(file.into_temp_path())))
    }

    /// A launch spec for `program` under this profile: the account's config
    /// directory exported as `config_env_var`, the ledger, and the stop grace.
    ///
    /// # Errors
    ///
    /// As [`Self::config_dir`].
    pub fn spec(
        &self,
        program: &str,
        config_env_var: &'static str,
        account: Option<&ProviderAccount>,
    ) -> Result<HarnessSpec, String> {
        Ok(HarnessSpec {
            config_dir: Some(self.config_dir(account)?),
            config_env_var,
            ledger: self.ledger.clone(),
            stop_grace: self.stop_grace,
            ..HarnessSpec::new(program)
        })
    }

    /// `recipe`, made to sign in this profile: by default the directory an
    /// account-less agent runs under — without it, `claude login` would sign
    /// in the operator's own directory and every agent would stay logged out
    /// — and, for a login naming an account, that account's directory.
    ///
    /// Infallible on purpose: the directory is prepared owner-only when it
    /// can be, and when it cannot the login command still gets the path (and
    /// creates it, or fails loudly in the operator's terminal).
    #[must_use]
    pub fn signing_in(
        &self,
        recipe: horde_adapter::LoginRecipe,
        config_env_var: &str,
    ) -> horde_adapter::LoginRecipe {
        recipe
            .with_env(self.login_env(config_env_var))
            .with_accounts(config_env_var, self.root())
    }

    /// The environment a login command needs to sign in the directory an
    /// account-less agent runs under (see [`Self::signing_in`]).
    #[must_use]
    pub fn login_env(&self, config_env_var: &str) -> Vec<(String, String)> {
        let dir = self.root.join(DEFAULT_DIR);
        if let Err(error) = owner_only_dir(&dir) {
            tracing::warn!(%error, "could not prepare the login's config directory");
        }
        vec![(config_env_var.to_owned(), dir.display().to_string())]
    }
}

/// Creates `dir` (and its parents) and makes the leaf owner-only.
fn owner_only_dir(dir: &Path) -> Result<(), String> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .and_then(|()| std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)))
        .map_err(|error| format!("could not prepare {}: {error}", dir.display()))
}

/// An owner-only file a harness launch references, deleted when the last
/// handle drops.
#[derive(Debug, Clone)]
pub struct SecretFile(Arc<tempfile::TempPath>);

impl SecretFile {
    /// Where the file is.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0
    }
}

/// The journal is the production ledger: a group remembered there survives
/// the Worker, which is the whole point.
#[async_trait::async_trait]
impl GroupLedger for Journal {
    async fn opened(&self, group: &HarnessGroup) -> Result<(), String> {
        self.remember_harness_group(group.pgid, &group.fingerprint)
            .await
            .map_err(|error| error.to_string())
    }

    async fn closed(&self, pgid: u32) {
        if let Err(error) = self.forget_harness_group(pgid).await {
            tracing::warn!(pgid, %error, "could not forget a reaped harness group");
        }
    }

    async fn open_groups(&self) -> Result<Vec<HarnessGroup>, String> {
        self.harness_groups()
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|(pgid, fingerprint)| HarnessGroup { pgid, fingerprint })
                    .collect()
            })
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use horde_adapter::ProviderAccount;

    use super::HarnessProfile;
    use crate::process::{GroupLedger, HarnessGroup};

    fn mode(path: &std::path::Path) -> u32 {
        std::fs::metadata(path)
            .expect("the path exists")
            .permissions()
            .mode()
            & 0o777
    }

    /// Accounts resolve to absolute, owner-only directories under the
    /// profile — never relative to the child's cwd, never shared with the
    /// account-less default.
    #[test]
    fn accounts_live_under_the_profile_as_absolute_owner_only_dirs() {
        let scratch = tempfile::tempdir().expect("scratch");
        let profile = HarnessProfile::new(scratch.path().join("claude"));

        let default = profile.config_dir(None).expect("the default dir");
        let work = profile
            .config_dir(Some(&ProviderAccount::new("work")))
            .expect("an account dir");

        assert!(default.is_absolute() && work.is_absolute());
        assert_eq!(default, scratch.path().join("claude").join("default"));
        assert_eq!(
            work,
            scratch.path().join("claude").join("accounts").join("work")
        );
        assert_eq!(mode(&default), 0o700, "config dirs hold credentials");
        assert_eq!(mode(&work), 0o700);
    }

    /// The accounts a profile holds are the account directories a sign-in
    /// wrote into — in name order — and not the ones merely prepared, not
    /// the account-less default, and not stray files beside them.
    #[test]
    fn the_signed_in_accounts_are_the_account_dirs_holding_a_sign_in() {
        let scratch = tempfile::tempdir().expect("scratch");
        let profile = HarnessProfile::new(scratch.path().join("claude"));
        assert!(
            profile.signed_in_accounts().is_empty(),
            "no accounts directory, no accounts"
        );

        for account in ["work", "personal", "prepared"] {
            profile
                .config_dir(Some(&ProviderAccount::new(account)))
                .expect("an account dir");
        }
        let default = profile.config_dir(None).expect("the default dir");
        for signed_in in [
            profile.root().join("accounts").join("work"),
            profile.root().join("accounts").join("personal"),
            default,
        ] {
            std::fs::write(signed_in.join(".credentials.json"), "{}").expect("a sign-in");
        }
        std::fs::write(profile.root().join("accounts").join("stray"), "x").expect("a stray file");

        assert_eq!(
            profile.signed_in_accounts(),
            vec!["personal".to_owned(), "work".to_owned()]
        );
    }

    /// A relative root is made absolute at construction, so no account can
    /// land inside whatever checkout the child runs in.
    #[test]
    fn a_relative_root_is_made_absolute() {
        let profile = HarnessProfile::new("relative/root");
        assert!(profile.root().is_absolute(), "{:?}", profile.root());
        assert!(profile.root().ends_with("relative/root"));
    }

    /// An account name that would escape the profile is refused, not joined.
    #[test]
    fn an_account_that_would_escape_the_profile_is_refused() {
        let scratch = tempfile::tempdir().expect("scratch");
        let profile = HarnessProfile::new(scratch.path());
        for hostile in ["..", "../../etc", "/etc", "a/b", ".hidden", ""] {
            let refused = profile.config_dir(Some(&ProviderAccount::new(hostile)));
            assert!(
                refused
                    .as_ref()
                    .is_err_and(|e| e.contains("not a plain directory")),
                "{hostile:?} must be refused, got {refused:?}"
            );
        }
    }

    /// Secret files are owner-only, carry exactly what was written, and are
    /// gone once the last handle drops.
    #[test]
    fn a_secret_file_is_owner_only_and_dies_with_its_handle() {
        let scratch = tempfile::tempdir().expect("scratch");
        let profile = HarnessProfile::new(scratch.path());
        let secret = profile
            .secret_file("mcp-", b"{\"token\":\"t\"}")
            .expect("written");
        let path = secret.path().to_path_buf();

        assert_eq!(mode(&path), 0o600, "a bearer token is owner-only");
        assert_eq!(mode(path.parent().expect("a run dir")), 0o700);
        assert_eq!(
            std::fs::read_to_string(&path).expect("readable"),
            "{\"token\":\"t\"}"
        );
        assert!(path.starts_with(scratch.path().join("run")));

        let clone = secret.clone();
        drop(secret);
        assert!(path.exists(), "a clone still holds it");
        drop(clone);
        assert!(!path.exists(), "the secret dies with its last handle");
    }

    /// The spec carries the config dir under the provider's variable, the
    /// ledger, and the grace; the login env names the same default dir.
    #[test]
    fn the_spec_and_the_login_env_name_the_same_directory() {
        let scratch = tempfile::tempdir().expect("scratch");
        let journal = crate::journal::Journal::in_memory().expect("journal");
        let profile = HarnessProfile::new(scratch.path())
            .with_ledger(std::sync::Arc::new(journal))
            .with_stop_grace(std::time::Duration::from_millis(250));

        let spec = profile
            .spec("/bin/claude", "CLAUDE_CONFIG_DIR", None)
            .expect("a spec");
        assert_eq!(spec.program, "/bin/claude");
        assert_eq!(spec.config_env_var, "CLAUDE_CONFIG_DIR");
        assert_eq!(spec.config_dir, Some(scratch.path().join("default")));
        assert!(spec.ledger.is_some());
        assert_eq!(spec.stop_grace, std::time::Duration::from_millis(250));

        let login = profile.login_env("CLAUDE_CONFIG_DIR");
        assert_eq!(
            login,
            vec![(
                "CLAUDE_CONFIG_DIR".to_owned(),
                scratch.path().join("default").display().to_string()
            )]
        );
    }

    /// The journal ledger round-trips groups and forgets them on close.
    #[tokio::test]
    async fn the_journal_ledger_remembers_and_forgets_groups() {
        let journal = crate::journal::Journal::in_memory().expect("journal");
        let group = HarnessGroup {
            pgid: 4242,
            fingerprint: "Fri Oct 30 22:13:34 2026".to_owned(),
        };
        journal.opened(&group).await.expect("remembered");
        journal
            .opened(&HarnessGroup {
                pgid: 77,
                fingerprint: String::new(),
            })
            .await
            .expect("remembered");

        let mut open = journal.open_groups().await.expect("readable");
        open.sort_by_key(|group| group.pgid);
        assert_eq!(
            open,
            vec![
                HarnessGroup {
                    pgid: 77,
                    fingerprint: String::new()
                },
                group.clone()
            ]
        );

        journal.closed(77).await;
        assert_eq!(journal.open_groups().await.expect("readable"), vec![group]);
    }
}
