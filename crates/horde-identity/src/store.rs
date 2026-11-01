//! The identity directory an enrolled peer keeps.
//!
//! Three PEM files: `identity.pem` (the certificate the CA signed),
//! `identity.key` (the private key that never travelled), and `ca.pem` (the
//! root to verify the Server against). One layout for Workers and Clients,
//! because enrollment is one mechanism (DESIGN §3).

use std::path::Path;

use crate::error::IdentityError;

const CERT_FILE: &str = "identity.pem";
const KEY_FILE: &str = "identity.key";
const CA_FILE: &str = "ca.pem";
const ENDPOINT_FILE: &str = "endpoint";

/// An enrolled peer's identity material, loaded into memory.
#[derive(Clone)]
pub struct StoredIdentity {
    pub certificate_pem: String,
    pub key_pem: String,
    pub ca_pem: String,
}

impl std::fmt::Debug for StoredIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The key is deliberately absent: Debug output lands in logs.
        f.debug_struct("StoredIdentity")
            .field("certificate_pem", &self.certificate_pem)
            .field("ca_pem", &self.ca_pem)
            .finish_non_exhaustive()
    }
}

impl StoredIdentity {
    /// Writes the identity into `directory`, key readable by owner only.
    ///
    /// # Errors
    ///
    /// Any filesystem failure, with the path in the message.
    pub fn save(&self, directory: &Path) -> Result<(), IdentityError> {
        create_private_dir(directory)?;
        let write = |file: &str, contents: &str| {
            let path = directory.join(file);
            std::fs::write(&path, contents)
                .map_err(|e| IdentityError::io(path.display().to_string(), e))
        };
        write(CERT_FILE, &self.certificate_pem)?;
        write(CA_FILE, &self.ca_pem)?;
        write_secret(&directory.join(KEY_FILE), &self.key_pem)
    }

    /// Loads an identity previously [`save`](Self::save)d.
    ///
    /// # Errors
    ///
    /// Any missing or unreadable file, named.
    pub fn load(directory: &Path) -> Result<Self, IdentityError> {
        let read = |file: &str| {
            let path = directory.join(file);
            std::fs::read_to_string(&path)
                .map_err(|e| IdentityError::io(path.display().to_string(), e))
        };
        Ok(Self {
            certificate_pem: read(CERT_FILE)?,
            key_pem: read(KEY_FILE)?,
            ca_pem: read(CA_FILE)?,
        })
    }

    /// Whether `directory` holds a complete identity.
    #[must_use]
    pub fn exists(directory: &Path) -> bool {
        [CERT_FILE, KEY_FILE, CA_FILE]
            .iter()
            .all(|file| directory.join(file).is_file())
    }

    /// Moves an existing identity directory aside as
    /// `<dir>.backup-<unix_secs>` (with `-N` appended when that name is
    /// taken), so a forced re-join never destroys the credential it
    /// replaces. Returns where the old directory now lives.
    ///
    /// # Errors
    ///
    /// The rename failing, or a directory with no usable name.
    pub fn back_up(directory: &Path, unix_secs: u64) -> Result<std::path::PathBuf, IdentityError> {
        let name = directory
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .ok_or_else(|| {
                IdentityError::io(
                    directory.display().to_string(),
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "no directory name"),
                )
            })?;
        let stem = format!("{name}.backup-{unix_secs}");
        let target = (0_u32..)
            .map(|n| match n {
                0 => directory.with_file_name(&stem),
                n => directory.with_file_name(format!("{stem}-{n}")),
            })
            .find(|candidate| !candidate.exists())
            .unwrap_or_else(|| directory.with_file_name(&stem));
        std::fs::rename(directory, &target)
            .map_err(|e| IdentityError::io(directory.display().to_string(), e))?;
        Ok(target)
    }
}

/// Creates `directory` (and any missing parents) owner-only, and tightens
/// it to `0700` when it already existed: identity and state directories
/// hold keys and journals, and a directory listing is itself a disclosure.
///
/// # Errors
///
/// Any filesystem failure, with the path in the message.
pub fn create_private_dir(directory: &Path) -> Result<(), IdentityError> {
    let context = || directory.display().to_string();
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)
            .map_err(|e| IdentityError::io(context(), e))?;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| IdentityError::io(context(), e))
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(directory).map_err(|e| IdentityError::io(context(), e))
    }
}

/// Refuses an endpoint that no later invocation could dial (ADR-0007,
/// 2026-10-31 amendment): it must be `https://host:port`, with a real host
/// that is not the unspecified address and a non-zero port. Checked before
/// anything persists it, so a bad endpoint fails at the join that would
/// have written it rather than at every dial after.
///
/// # Errors
///
/// [`IdentityError::InvalidEndpoint`], naming what was wrong.
pub fn validate_endpoint(endpoint: &str) -> Result<(), IdentityError> {
    let refuse = |reason| {
        Err(IdentityError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            reason,
        })
    };
    let Some(authority) = endpoint.strip_prefix("https://") else {
        return refuse("the control plane is mTLS-only, so it must start with https://");
    };
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    let Some((host, port)) = authority.rsplit_once(':') else {
        return refuse("it names no port");
    };
    if host.is_empty() || authority.contains('/') {
        return refuse("it is not https://host:port");
    }
    if !port.parse::<u16>().is_ok_and(|port| port != 0) {
        return refuse("its port is not a TCP port");
    }
    if crate::token::host_scope(host) == crate::token::HostScope::Unspecified {
        return refuse("it names the unspecified address, which no peer can dial");
    }
    Ok(())
}

/// An exclusive, advisory lock on one state directory, held for as long as
/// this value lives: how a second server, worker, or `horde up` on the same
/// state learns that one is already running, from a fact the OS keeps
/// rather than a port that happens to answer. The holder's pid is written
/// into the file for the refusal to name. Released on drop — and by the OS
/// when the holder dies, so a crash never leaves a stale lock.
///
/// Released *explicitly* (`LOCK_UN`), never by closing the fd alone:
/// `flock` belongs to the open file description, and a child that any
/// thread of this process is forking holds a copy of every fd until it
/// execs (close-on-exec only acts at exec). Closing ours would then leave
/// the state held by this very process until that copy closed, and the
/// next holder would be refused naming our own pid. The probe releases the
/// same way.
#[derive(Debug)]
pub struct StateLock {
    file: std::fs::File,
    path: std::path::PathBuf,
}

impl StateLock {
    /// Takes the lock `<directory>/<name>` without waiting, creating the
    /// directory owner-only if it is missing.
    ///
    /// # Errors
    ///
    /// [`IdentityError::StateHeld`] when another process holds it, naming
    /// its pid; any filesystem failure otherwise.
    pub fn acquire(directory: &Path, name: &str) -> Result<Self, IdentityError> {
        use std::io::Write;

        create_private_dir(directory)?;
        let path = directory.join(name);
        let mut file = open_lock_file(&path)?;
        if let Err(error) = file.try_lock() {
            return Err(held_or_io(&path, error));
        }
        // Rewritten only once held, so the pid in the file is the holder's.
        let stamped = file
            .set_len(0)
            .and_then(|()| write!(file, "{}", std::process::id()));
        stamped.map_err(|e| IdentityError::io(path.display().to_string(), e))?;
        Ok(Self { file, path })
    }

    /// Whether some live process holds `<directory>/<name>` right now. A
    /// missing directory or file is "nobody", never an error: probing must
    /// not create state.
    #[must_use]
    pub fn is_held(directory: &Path, name: &str) -> bool {
        let path = directory.join(name);
        if !path.is_file() {
            return false;
        }
        let Ok(file) = open_lock_file(&path) else {
            return false;
        };
        match file.try_lock() {
            // The probe took it: give it back explicitly (see the type docs).
            Ok(()) => {
                let _released = file.unlock();
                false
            }
            Err(error) => matches!(error, std::fs::TryLockError::WouldBlock),
        }
    }

    /// The lock file this value holds.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for StateLock {
    fn drop(&mut self) {
        // Best effort: a failed unlock still closes the fd, which is the
        // old behaviour, and the OS releases everything at exit anyway.
        let _released = self.file.unlock();
    }
}

fn open_lock_file(path: &Path) -> Result<std::fs::File, IdentityError> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|e| IdentityError::io(path.display().to_string(), e))
}

/// The refusal for a lock that would not take: held (naming the holder's
/// pid when the file carries one), or a plain I/O failure.
fn held_or_io(path: &Path, error: std::fs::TryLockError) -> IdentityError {
    match error {
        std::fs::TryLockError::WouldBlock => {
            let holder = std::fs::read_to_string(path)
                .ok()
                .map(|pid| pid.trim().to_owned())
                .filter(|pid| !pid.is_empty())
                .map(|pid| format!(" (pid {pid})"))
                .unwrap_or_default();
            IdentityError::StateHeld {
                path: path.display().to_string(),
                holder,
            }
        }
        std::fs::TryLockError::Error(e) => IdentityError::io(path.display().to_string(), e),
    }
}

/// Persists the control-plane endpoint beside the credential (ADR-0007):
/// after one join, later invocations read the server from here instead of
/// demanding a flag or an environment variable.
///
/// A fourth file rather than a field of [`StoredIdentity`], because the
/// credential triple is complete without it — a directory holding only the
/// three PEMs is still an identity, and must stay one.
///
/// The endpoint is validated first ([`validate_endpoint`]): a stored
/// endpoint is dialed by every later invocation, so nothing undialable is
/// ever written.
///
/// # Errors
///
/// An undialable endpoint, or any filesystem failure with the path.
pub fn save_endpoint(directory: &Path, endpoint: &str) -> Result<(), IdentityError> {
    validate_endpoint(endpoint)?;
    create_private_dir(directory)?;
    let path = directory.join(ENDPOINT_FILE);
    std::fs::write(&path, format!("{endpoint}\n"))
        .map_err(|e| IdentityError::io(path.display().to_string(), e))
}

/// The endpoint a join stored, if this directory has one.
///
/// Best-effort by design: an identity dir without the stored endpoint still
/// works (the compatibility rule ADR-0007 names), so absence — and any
/// unreadable file — is `None`, never a refusal. The refusal belongs to the
/// caller that ends up with no endpoint from any source.
#[must_use]
pub fn load_endpoint(directory: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(directory.join(ENDPOINT_FILE)).ok()?;
    let endpoint = raw.trim();
    if endpoint.is_empty() {
        return None;
    }
    Some(endpoint.to_owned())
}

/// Writes secret material with owner-only permissions.
///
/// Written before tightening would leave a world-readable window; instead the
/// file is created empty with the right mode first, then filled. A file that
/// already existed keeps the mode it was created with, so the mode is set
/// again on the open handle before any byte lands — a rewrite tightens a
/// loose file rather than filling it. Public
/// because the server's bootstrap-token file (ADR-0007) is the same shape of
/// secret and must not grow a second permission story.
///
/// # Errors
///
/// Any filesystem failure, with the path in the message.
pub fn write_secret(path: &Path, contents: &str) -> Result<(), IdentityError> {
    let context = || path.display().to_string();

    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .map_err(|e| IdentityError::io(context(), e))?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|e| IdentityError::io(context(), e))?;
        file.write_all(contents.as_bytes())
            .map_err(|e| IdentityError::io(context(), e))
    }

    #[cfg(not(unix))]
    {
        std::fs::write(path, contents).map_err(|e| IdentityError::io(context(), e))
    }
}

#[cfg(test)]
mod tests {
    use super::StoredIdentity;

    fn identity() -> StoredIdentity {
        StoredIdentity {
            certificate_pem: "CERT".to_owned(),
            key_pem: "KEY".to_owned(),
            ca_pem: "CA".to_owned(),
        }
    }

    #[test]
    fn an_identity_round_trips_through_disk() {
        let directory = tempfile::tempdir().expect("a temp dir");
        assert!(!StoredIdentity::exists(directory.path()));

        identity().save(directory.path()).expect("saves");
        assert!(StoredIdentity::exists(directory.path()));

        let loaded = StoredIdentity::load(directory.path()).expect("loads");
        assert_eq!(loaded.certificate_pem, "CERT");
        assert_eq!(loaded.key_pem, "KEY");
        assert_eq!(loaded.ca_pem, "CA");
    }

    /// The stored endpoint (ADR-0007): saved, loaded back, and — the
    /// compatibility rule — absent without ceremony from a dir that never
    /// stored one. It is beside the identity, not part of it: a dir holding
    /// only the endpoint is no identity.
    #[test]
    fn the_endpoint_persists_beside_the_identity_and_its_absence_is_none() {
        let directory = tempfile::tempdir().expect("a temp dir");
        assert_eq!(super::load_endpoint(directory.path()), None);

        identity().save(directory.path()).expect("saves");
        assert_eq!(
            super::load_endpoint(directory.path()),
            None,
            "a pre-ADR-0007 identity dir still works, endpointless"
        );

        super::save_endpoint(directory.path(), "https://horde.example:7777").expect("saves");
        assert_eq!(
            super::load_endpoint(directory.path()).as_deref(),
            Some("https://horde.example:7777"),
            "the trailing newline is packaging, not endpoint"
        );
        assert!(
            StoredIdentity::exists(directory.path()),
            "the endpoint file must not change what an identity is"
        );

        // A blank file stores nothing: an empty endpoint dialed nowhere.
        std::fs::write(directory.path().join("endpoint"), "\n").expect("writes");
        assert_eq!(super::load_endpoint(directory.path()), None);

        // Saving into a fresh directory creates it, so a join can store the
        // endpoint before or after the credential lands.
        let fresh = directory.path().join("fresh");
        super::save_endpoint(&fresh, "https://h:1").expect("creates and saves");
        assert_eq!(super::load_endpoint(&fresh).as_deref(), Some("https://h:1"));
        assert!(
            !StoredIdentity::exists(&fresh),
            "an endpoint alone is not an identity"
        );
    }

    #[test]
    fn a_partial_directory_is_not_an_identity() {
        let directory = tempfile::tempdir().expect("a temp dir");
        identity().save(directory.path()).expect("saves");
        std::fs::remove_file(directory.path().join("identity.key")).expect("removes");
        assert!(
            !StoredIdentity::exists(directory.path()),
            "a keyless identity is no identity"
        );
        assert!(StoredIdentity::load(directory.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn the_saved_key_is_owner_only_even_when_overwriting() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("a temp dir");
        identity().save(directory.path()).expect("saves");
        identity().save(directory.path()).expect("saves again");

        let mode = std::fs::metadata(directory.path().join("identity.key"))
            .expect("the key exists")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    /// The rewrite half of the secret writer: a file that already existed
    /// with a loose mode (a hand-copied key, an older build) is tightened
    /// by the write, not filled at its old mode.
    #[cfg(unix)]
    #[test]
    fn a_secret_rewrite_tightens_a_loose_file() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("a temp dir");
        let path = directory.path().join("secret");
        std::fs::write(&path, "old").expect("writes");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("loosens");

        super::write_secret(&path, "new").expect("rewrites");
        let mode = std::fs::metadata(&path)
            .expect("exists")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "the rewrite tightened the file");
        assert_eq!(std::fs::read_to_string(&path).expect("reads"), "new");
    }

    /// Identity directories are owner-only, created that way and tightened
    /// when they pre-existed loose — parents included when created here.
    #[cfg(unix)]
    #[test]
    fn identity_directories_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let mode_of = |path: &std::path::Path| {
            std::fs::metadata(path)
                .expect("exists")
                .permissions()
                .mode()
                & 0o777
        };

        let root = tempfile::tempdir().expect("a temp dir");
        let nested = root.path().join("home/.horde/id");
        identity().save(&nested).expect("saves");
        assert_eq!(mode_of(&nested), 0o700);
        assert_eq!(mode_of(&root.path().join("home/.horde")), 0o700);

        let loose = root.path().join("loose");
        std::fs::create_dir(&loose).expect("creates");
        std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o755)).expect("loosens");
        super::save_endpoint(&loose, "https://h:1").expect("saves");
        assert_eq!(mode_of(&loose), 0o700, "a loose dir is tightened");
    }

    /// What may be persisted as an endpoint, as a table: only a dialable
    /// `https://host:port` survives, and each refusal says why.
    #[test]
    fn only_a_dialable_https_endpoint_is_storable() {
        for good in [
            "https://horde.example:7777",
            "https://10.0.0.5:7777/",
            "https://[::1]:7777",
            "https://127.0.0.1:1",
        ] {
            assert!(super::validate_endpoint(good).is_ok(), "{good}");
        }
        for (bad, why) in [
            ("http://h:7777", "https://"),
            ("h:7777", "https://"),
            ("https://h", "no port"),
            ("https://:7777", "host:port"),
            ("https://h:7777/path", "host:port"),
            ("https://h:0", "TCP port"),
            ("https://h:http", "TCP port"),
            ("https://0.0.0.0:7777", "unspecified"),
            ("https://[::]:7777", "unspecified"),
        ] {
            let refused = super::validate_endpoint(bad).expect_err(bad);
            assert!(refused.to_string().contains(why), "{bad}: {refused}");
        }
        let directory = tempfile::tempdir().expect("a temp dir");
        assert!(super::save_endpoint(directory.path(), "https://0.0.0.0:7777").is_err());
        assert_eq!(
            super::load_endpoint(directory.path()),
            None,
            "a refused endpoint writes nothing"
        );
    }

    /// A forced re-join's safety net: the old directory moves aside under a
    /// timestamped name, a second backup in the same second gets its own
    /// name, and nothing is lost.
    #[test]
    fn a_backup_moves_the_identity_aside_without_clobbering() {
        let root = tempfile::tempdir().expect("a temp dir");
        let dir = root.path().join("id");
        identity().save(&dir).expect("saves");

        let first = StoredIdentity::back_up(&dir, 1_700_000_000).expect("backs up");
        assert_eq!(first, root.path().join("id.backup-1700000000"));
        assert!(!dir.exists(), "the original is moved, not copied");
        assert!(StoredIdentity::exists(&first), "the backup is whole");

        identity().save(&dir).expect("saves again");
        let second = StoredIdentity::back_up(&dir, 1_700_000_000).expect("backs up again");
        assert_eq!(second, root.path().join("id.backup-1700000000-1"));
        assert!(StoredIdentity::exists(&first) && StoredIdentity::exists(&second));

        let missing = StoredIdentity::back_up(&dir, 1).expect_err("nothing to move");
        assert!(missing.to_string().contains("id"), "{missing}");
        let nameless = StoredIdentity::back_up(std::path::Path::new("/"), 1).expect_err("no name");
        assert!(
            nameless.to_string().contains("no directory name"),
            "{nameless}"
        );
    }

    /// The state lock: one holder at a time, the refusal names the holder's
    /// pid, probing never creates state, and dropping releases.
    #[test]
    fn a_state_lock_has_one_holder_until_dropped() {
        let root = tempfile::tempdir().expect("a temp dir");
        let dir = root.path().join("state");
        assert!(!super::StateLock::is_held(&dir, "server.lock"));
        assert!(!dir.exists(), "a probe creates nothing");

        let held = super::StateLock::acquire(&dir, "server.lock").expect("takes");
        assert_eq!(held.path(), dir.join("server.lock"));
        assert!(super::StateLock::is_held(&dir, "server.lock"));
        assert!(
            !super::StateLock::is_held(&dir, "worker.lock"),
            "locks are per name"
        );
        let refused = super::StateLock::acquire(&dir, "server.lock").expect_err("one holder");
        assert!(
            refused
                .to_string()
                .contains(&format!("(pid {})", std::process::id())),
            "the refusal names the holder: {refused}"
        );

        drop(held);
        assert!(!super::StateLock::is_held(&dir, "server.lock"));
        let _again = super::StateLock::acquire(&dir, "server.lock").expect("free again");

        let blocked = root.path().join("file");
        std::fs::write(&blocked, "x").expect("writes");
        assert!(
            super::StateLock::acquire(&blocked, "server.lock").is_err(),
            "a file is no state directory"
        );
    }

    /// The fork window, deterministically: a child forked by any thread
    /// holds a copy of every fd until it execs, and `flock` belongs to the
    /// open file description those copies share. A `try_clone` is exactly
    /// such a copy. Dropping the lock (or ending a probe) must release it
    /// while the copy lives — closing our fd alone would leave the state
    /// "held by" this very process until the copy closed, which is how a
    /// sequence of `horde up` calls in one process refused itself under
    /// plain `cargo test`.
    #[test]
    fn a_dropped_lock_is_released_even_while_a_forked_copy_lives() {
        let root = tempfile::tempdir().expect("a temp dir");
        let held = super::StateLock::acquire(root.path(), "up.lock").expect("takes");
        let forked_copy = held.file.try_clone().expect("the fd duplicates");
        drop(held);
        assert!(
            !super::StateLock::is_held(root.path(), "up.lock"),
            "a drop releases the lock, not just our fd"
        );
        let _again = super::StateLock::acquire(root.path(), "up.lock")
            .expect("the next holder takes it at once");
        drop(forked_copy);
    }

    #[test]
    fn debug_never_prints_the_key() {
        let debugged = format!("{:?}", identity());
        assert!(
            !debugged.contains("KEY"),
            "keys must not reach logs: {debugged}"
        );
        assert!(
            debugged.contains("CERT"),
            "certificates are public: {debugged}"
        );
    }
}
