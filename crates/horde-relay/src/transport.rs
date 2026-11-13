//! Executing a resolved route: the Worker-side halves of a share.
//!
//! A share has two halves that run on (potentially) two machines. The
//! sender's Worker **stages** — makes the commit reachable along the route,
//! which only the Relay route needs work for — and the receiver's Worker
//! **obtains** — makes the commit present in the receiving checkout. Both
//! halves end by proving the commit is where they claim it is, because a
//! transport that reports success on faith is how a Payload names a commit
//! nobody can check out.
//!
//! Also here: what the Worker observes before any share exists —
//! [`observe_head`] for stamping a result's Payload (DESIGN §5: the
//! orchestrator observes what happened; Agents do not declare it), and
//! [`require_committed`], the "commit, then share" rule with the helpful
//! error ADR-0002 promises.

use std::path::Path;

use crate::git::{GitError, check, git, raw};
use crate::resolve::ShareRoute;

/// Where Workers reach the Relay, e.g. `http://127.0.0.1:7420` (the
/// server's loopback door) or `https://horde.lan:7779` (its mTLS door).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayLocation {
    base: String,
    identity: Option<RelayIdentity>,
}

/// The certificate a Worker's git dial presents at the Relay's mTLS door
/// (DESIGN §3, §8): the PEM files of the Worker's own enrolled identity,
/// handed to git as `http.sslCert`/`http.sslKey`, with the server's CA as
/// the only trust root (`http.sslCAInfo`) — a share is never verified
/// against the machine's public roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayIdentity {
    pub certificate: std::path::PathBuf,
    pub key: std::path::PathBuf,
    pub ca: std::path::PathBuf,
}

impl RelayIdentity {
    /// The identity a `horde_identity::StoredIdentity` directory holds.
    #[must_use]
    pub fn in_dir(directory: &Path) -> Self {
        Self {
            certificate: directory.join("identity.pem"),
            key: directory.join("identity.key"),
            ca: directory.join("ca.pem"),
        }
    }

    /// The `-c` options that make one git invocation present this identity.
    fn git_options(&self) -> Vec<String> {
        [
            ("http.sslCert", &self.certificate),
            ("http.sslKey", &self.key),
            ("http.sslCAInfo", &self.ca),
        ]
        .into_iter()
        .flat_map(|(key, path)| ["-c".to_owned(), format!("{key}={}", path.display())])
        .collect()
    }
}

impl RelayLocation {
    #[must_use]
    pub fn new(base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            identity: None,
        }
    }

    /// The same location, dialled presenting `identity`.
    #[must_use]
    pub fn with_identity(mut self, identity: RelayIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// The URL one repo's mirror is fetched from and pushed to.
    #[must_use]
    pub fn repo_url(&self, repo_name: &str) -> String {
        format!("{}/{repo_name}", self.base)
    }

    /// The git argv for `verb` against this Relay: the identity options, if
    /// any, come first — `-c` is a git option, not a subcommand option.
    fn git_args(&self, verb: &[&str]) -> Vec<String> {
        let mut args = self
            .identity
            .as_ref()
            .map(RelayIdentity::git_options)
            .unwrap_or_default();
        args.extend(verb.iter().map(|word| (*word).to_owned()));
        args
    }
}

/// The ref a shared commit is pushed to on the Relay.
///
/// Content-addressed on purpose: pushing the same commit twice lands on the
/// same ref (idempotent, like every retryable step here), two Agents sharing
/// different commits cannot collide, and the mirror keeps the commit
/// reachable so upstream refreshes cannot garbage-collect it away.
#[must_use]
pub fn share_ref(commit_sha: &str) -> String {
    format!("refs/horde/share/{commit_sha}")
}

/// Why a share could not be staged or obtained.
#[derive(Debug, thiserror::Error)]
pub enum ShareError {
    /// A git operation underneath failed; carries git's own words.
    #[error(transparent)]
    Git(#[from] GitError),

    /// The working tree has uncommitted changes. ADR-0002: syncing dirty
    /// trees is permanently out of scope, and the tools say so helpfully.
    #[error(
        "{repo} has uncommitted changes ({paths}): commit, then share — \
         dirty working trees never travel"
    )]
    Uncommitted { repo: String, paths: String },

    /// The commit is not where this half of the share needs it to be.
    #[error("commit {commit_sha} is not present in {repo}: {because}")]
    MissingCommit {
        commit_sha: String,
        repo: String,
        because: String,
    },

    /// The route crosses Workers but no Relay location was given.
    #[error("the share must cross Workers, and no Relay location is configured")]
    NoRelay,
}

/// What the Worker observed at a checkout's HEAD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedHead {
    /// Full commit id. Authoritative (`Payload.git_refs[].commit_sha`).
    pub commit_sha: String,
    /// The branch HEAD was on, absent when detached. Advisory, like the
    /// proto field it feeds.
    pub branch: Option<String>,
}

/// Observes HEAD for stamping into a Payload.
///
/// # Errors
///
/// [`GitError`] when the path is not a checkout or has no commits yet.
pub async fn observe_head(repo: &Path) -> Result<ObservedHead, GitError> {
    let commit_sha = git(repo, &["rev-parse", "HEAD"]).await?;
    // `-q` reserves exit 1 for "detached", which is an answer, not a failure.
    let outcome = raw(repo, &["symbolic-ref", "--short", "-q", "HEAD"]).await?;
    let branch = match outcome.code {
        Some(0) => Some(String::from_utf8_lossy(&outcome.stdout).trim().to_owned()),
        _ => None,
    };
    Ok(ObservedHead { commit_sha, branch })
}

/// The paths `git status --porcelain` says are not committed.
#[must_use]
pub fn uncommitted_paths(porcelain: &str) -> Vec<String> {
    porcelain
        .lines()
        .filter(|line| line.len() > 3)
        .map(|line| line[3..].to_owned())
        .collect()
}

/// Refuses to proceed while the working tree is dirty.
///
/// The error names the offending paths: "commit, then share" is only a
/// helpful rule when the message says what to commit.
///
/// # Errors
///
/// [`ShareError::Uncommitted`] listing the paths; [`ShareError::Git`] when
/// the status itself could not be taken.
pub async fn require_committed(repo: &Path) -> Result<(), ShareError> {
    let porcelain = git(repo, &["status", "--porcelain"]).await?;
    let paths = uncommitted_paths(&porcelain);
    if paths.is_empty() {
        Ok(())
    } else {
        Err(ShareError::Uncommitted {
            repo: repo.display().to_string(),
            paths: paths.join(", "),
        })
    }
}

/// Proves `commit_sha` resolves to a commit present in `repo`.
///
/// `rev-parse -q --verify` with a `^{commit}` peel: exit 1 — the probe's
/// "no" — covers absent objects, present-but-not-a-commit objects, and
/// unparseable ids alike, while a broken repo stays a hard error.
async fn verify_present(repo: &Path, commit_sha: &str, because: &str) -> Result<(), ShareError> {
    let probe = format!("{commit_sha}^{{commit}}");
    if check(repo, &["rev-parse", "-q", "--verify", &probe]).await? {
        Ok(())
    } else {
        Err(ShareError::MissingCommit {
            commit_sha: commit_sha.to_owned(),
            repo: repo.display().to_string(),
            because: because.to_owned(),
        })
    }
}

/// The Relay a cross-Worker route needs, or the honest refusal.
fn required(relay: Option<&RelayLocation>) -> Result<&RelayLocation, ShareError> {
    relay.ok_or(ShareError::NoRelay)
}

/// Runs one git verb against `relay` in `repo`.
async fn relay_git(repo: &Path, relay: &RelayLocation, verb: &[&str]) -> Result<(), ShareError> {
    let args = relay.git_args(verb);
    let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
    git(repo, &borrowed).await?;
    Ok(())
}

/// The sender's half: make `commit_sha` reachable along `route`.
///
/// For the local routes there is nothing to move, but the stage still proves
/// the sender actually has the commit it is advertising — a stamped Payload
/// naming a commit its own checkout lacks is a bug this surfaces at the
/// sender, where it is diagnosable, instead of at the receiver, where it is
/// noise.
///
/// # Errors
///
/// [`ShareError::MissingCommit`] when the sender lacks the commit,
/// [`ShareError::NoRelay`] on a cross-Worker route with no Relay, or the
/// underlying [`ShareError::Git`].
pub async fn stage_share(
    sender_repo: &Path,
    route: &ShareRoute,
    relay: Option<&RelayLocation>,
    commit_sha: &str,
) -> Result<(), ShareError> {
    verify_present(sender_repo, commit_sha, "the sender does not have it").await?;
    if let ShareRoute::ViaRelay { repo_name } = route {
        let relay = required(relay)?;
        let url = relay.repo_url(repo_name);
        let refspec = format!("{commit_sha}:{}", share_ref(commit_sha));
        relay_git(sender_repo, relay, &["push", "--", &url, &refspec]).await?;
    }
    Ok(())
}

/// The receiver's half: make `commit_sha` present in `receiver_repo`.
///
/// Ends by verifying the commit is really there, whatever the route said —
/// notably, a Relay share ref is only a name, and a ref that points at the
/// wrong commit must fail *here*, not when a checkout later comes up wrong.
///
/// # Errors
///
/// [`ShareError::MissingCommit`] when the commit did not arrive,
/// [`ShareError::NoRelay`] on a cross-Worker route with no Relay, or the
/// underlying [`ShareError::Git`].
pub async fn obtain_share(
    receiver_repo: &Path,
    route: &ShareRoute,
    relay: Option<&RelayLocation>,
    commit_sha: &str,
) -> Result<(), ShareError> {
    match route {
        ShareRoute::AlreadyShared => {
            verify_present(
                receiver_repo,
                commit_sha,
                "the checkouts share an object store, but the store lacks it",
            )
            .await
        }
        ShareRoute::PeerFetch { source } => {
            let source = source.display().to_string();
            git(receiver_repo, &["fetch", "--", &source, commit_sha]).await?;
            verify_present(receiver_repo, commit_sha, "the peer fetch did not bring it").await
        }
        ShareRoute::ViaRelay { repo_name } => {
            let relay = required(relay)?;
            let url = relay.repo_url(repo_name);
            let share = share_ref(commit_sha);
            relay_git(receiver_repo, relay, &["fetch", "--", &url, &share]).await?;
            verify_present(
                receiver_repo,
                commit_sha,
                "the Relay share ref did not contain it",
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RelayLocation, ShareError, share_ref, uncommitted_paths};

    #[test]
    fn the_relay_url_joins_base_and_repo_exactly_once() {
        assert_eq!(
            RelayLocation::new("http://127.0.0.1:7420").repo_url("atlas"),
            "http://127.0.0.1:7420/atlas"
        );
        assert_eq!(
            RelayLocation::new("http://relay.local/").repo_url("atlas"),
            "http://relay.local/atlas",
            "a trailing slash must not become a double one"
        );
    }

    /// The ref is content-addressed under the horde namespace — outside
    /// refs/heads and refs/tags, where a mirror refresh could fight it.
    #[test]
    fn the_share_ref_is_content_addressed_under_the_horde_namespace() {
        assert_eq!(share_ref("abc123"), "refs/horde/share/abc123");
    }

    /// Porcelain lines are `XY path`; every kind of dirt counts, including
    /// untracked files — uncommitted work is uncommitted work.
    #[test]
    fn porcelain_output_yields_the_dirty_paths() {
        let porcelain = " M src/lib.rs\n?? notes.txt\nA  new.rs";
        assert_eq!(
            uncommitted_paths(porcelain),
            vec!["src/lib.rs", "notes.txt", "new.rs"]
        );
        assert!(
            uncommitted_paths("").is_empty(),
            "a clean tree lists nothing"
        );
    }

    /// The boundary itself: a status line with no path after the `XY ` prefix
    /// must yield nothing, not an empty path. Porcelain never writes such a
    /// line, which is exactly why only a boundary test can hold the `>`.
    #[test]
    fn a_line_too_short_to_carry_a_path_yields_no_path() {
        assert_eq!(uncommitted_paths(" M "), Vec::<String>::new());
    }

    /// The message is the feature: it must carry the rule and the paths.
    #[test]
    fn the_uncommitted_error_says_commit_then_share_and_names_the_paths() {
        let error = ShareError::Uncommitted {
            repo: "/w1/agent-a/repo".to_owned(),
            paths: "src/lib.rs, notes.txt".to_owned(),
        };
        let rendered = error.to_string();
        for needed in [
            "commit, then share",
            "src/lib.rs",
            "notes.txt",
            "/w1/agent-a/repo",
        ] {
            assert!(rendered.contains(needed), "missing {needed:?}: {rendered}");
        }
    }
}
