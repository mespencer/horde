//! Finding and parsing a role's config file.

use std::path::{Path, PathBuf};

use crate::origin::Origin;

/// Which binary a file configures — named in every refusal, so an operator
/// with three files open knows which one is wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Server,
    Worker,
    Client,
}

impl Role {
    /// The role as refusals name it.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Worker => "worker",
            Self::Client => "client",
        }
    }

    /// The file's name inside the role's directory under `~/.horde`.
    pub const FILE_NAME: &'static str = "config.toml";
}

/// One parsed file.
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    pub role: Role,
    pub path: PathBuf,
    pub table: toml::Table,
}

impl Document {
    /// The origin of every value this file sets.
    #[must_use]
    pub fn origin(&self) -> Origin {
        Origin::File(self.path.clone())
    }

    /// A refusal about this file, prefixed with the role and the path.
    #[must_use]
    pub fn refusal(&self, message: impl std::fmt::Display) -> String {
        format!(
            "{} config {}: {message}",
            self.role.name(),
            self.path.display()
        )
    }
}

/// Loads the role's file: `explicit` when `--config` named one, else
/// `default`.
///
/// A missing default file is no layer at all (`Ok(None)`); a missing
/// explicit file is a refusal, because the operator asked for a file that
/// is not there.
///
/// # Errors
///
/// The file is unreadable, is not TOML, or `explicit` does not exist —
/// each prefixed with the role and the path.
pub fn load(
    role: Role,
    explicit: Option<&Path>,
    default: &Path,
) -> Result<Option<Document>, String> {
    let path = explicit.unwrap_or(default);
    let refuse = |message: String| format!("{} config {}: {message}", role.name(), path.display());
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && explicit.is_none() => {
            return Ok(None);
        }
        Err(error) => return Err(refuse(format!("could not read it: {error}"))),
    };
    let table = text
        .parse::<toml::Table>()
        .map_err(|error| refuse(format!("not valid TOML: {}", error.to_string().trim_end())))?;
    Ok(Some(Document {
        role,
        path: path.to_path_buf(),
        table,
    }))
}

#[cfg(test)]
mod tests {
    use super::{Role, load};
    use crate::origin::Origin;

    #[test]
    fn a_missing_default_file_is_no_layer_but_a_missing_explicit_one_is_refused() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let absent = dir.path().join("config.toml");
        assert_eq!(load(Role::Server, None, &absent), Ok(None));
        let refused =
            load(Role::Worker, Some(&absent), &dir.path().join("other")).expect_err("asked for");
        assert!(
            refused.starts_with(&format!(
                "worker config {}: could not read it",
                absent.display()
            )),
            "{refused}"
        );
    }

    #[test]
    fn a_file_parses_and_names_its_origin_and_bad_toml_is_refused_with_the_path() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[logs]\nlevel = \"debug\"\n").expect("writes");
        let document = load(Role::Client, None, &path)
            .expect("reads")
            .expect("present");
        assert_eq!(document.role, Role::Client);
        assert_eq!(document.origin(), Origin::File(path.clone()));
        assert!(document.table.contains_key("logs"));
        assert_eq!(
            document.refusal("why"),
            format!("client config {}: why", path.display())
        );

        std::fs::write(&path, "[logs\n").expect("writes");
        let refused = load(Role::Server, Some(&path), &path).expect_err("not TOML");
        assert!(
            refused.starts_with(&format!("server config {}: not valid TOML", path.display())),
            "{refused}"
        );
    }

    #[test]
    fn roles_have_names_and_one_file_name() {
        assert_eq!(
            [Role::Server, Role::Worker, Role::Client].map(Role::name),
            ["server", "worker", "client"]
        );
        assert_eq!(Role::FILE_NAME, "config.toml");
    }
}
