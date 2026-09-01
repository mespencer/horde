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
        std::fs::create_dir_all(directory)
            .map_err(|e| IdentityError::io(directory.display().to_string(), e))?;
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
}

/// Persists the control-plane endpoint beside the credential (ADR-0007):
/// after one join, later invocations read the server from here instead of
/// demanding a flag or an environment variable.
///
/// A fourth file rather than a field of [`StoredIdentity`], because the
/// credential triple is complete without it — a directory holding only the
/// three PEMs is still an identity, and must stay one.
///
/// # Errors
///
/// Any filesystem failure, with the path in the message.
pub fn save_endpoint(directory: &Path, endpoint: &str) -> Result<(), IdentityError> {
    std::fs::create_dir_all(directory)
        .map_err(|e| IdentityError::io(directory.display().to_string(), e))?;
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
/// file is created empty with the right mode first, then filled. Public
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
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
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
