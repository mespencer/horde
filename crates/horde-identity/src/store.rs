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

/// Writes key material with owner-only permissions.
///
/// Written before tightening would leave a world-readable window; instead the
/// file is created empty with the right mode first, then filled.
pub(crate) fn write_secret(path: &Path, contents: &str) -> Result<(), IdentityError> {
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
