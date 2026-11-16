//! Finding and parsing a role's config file.

use std::path::{Path, PathBuf};

use crate::logs::LEVEL_VAR;
use crate::origin::Origin;
use crate::reader::Section;

/// Which role owns each top-level table; `logs` is every role's.
const TOP_LEVEL: &[(&str, Role)] = &[
    ("permissions", Role::Server),
    ("tiers", Role::Server),
    ("workspace", Role::Server),
    ("placement", Role::Server),
    ("worker", Role::Worker),
    ("codex", Role::Worker),
    ("session", Role::Client),
];

/// The table a role reads when it has no file.
static EMPTY: std::sync::LazyLock<toml::Table> = std::sync::LazyLock::new(toml::Table::new);

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

/// The file's root section for `role` — empty without a file — with every
/// other role's top-level table declared foreign, and the origin its values
/// carry.
#[must_use]
pub fn open(role: Role, document: Option<&Document>) -> (Section<'_>, Origin) {
    let (table, origin) = document.map_or((&*EMPTY, Origin::Default), |document| {
        (&document.table, document.origin())
    });
    let mut root = Section::root(table);
    for (key, owner) in TOP_LEVEL.iter().filter(|(_, owner)| *owner != role) {
        root.foreign(key, owner.name());
    }
    (root, origin)
}

/// Prefixes a refusal with the file it is about — unless there is no file,
/// or the refusal is about `HORDE_LOG`, which the environment set.
pub fn refuser(document: Option<&Document>) -> impl Fn(String) -> String + Copy + '_ {
    move |message| match document {
        Some(document) if !message.starts_with(LEVEL_VAR) => document.refusal(message),
        _ => message,
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
    fn open_declares_the_other_roles_tables_foreign_and_refuser_names_the_file() {
        let document = super::Document {
            role: Role::Worker,
            path: "/w.toml".into(),
            table: "[tiers.t]\n".parse().expect("parses"),
        };
        let (root, origin) = super::open(Role::Worker, Some(&document));
        assert_eq!(origin, Origin::File("/w.toml".into()));
        assert_eq!(
            root.finish(),
            Err("tiers belongs in the server's config file, not this one".to_owned())
        );
        let (mut own, _) = super::open(Role::Server, Some(&document));
        assert!(own.named("tiers").is_ok());
        assert_eq!(own.finish(), Ok(()));
        let (bare, origin) = super::open(Role::Client, None);
        assert_eq!((bare.finish(), origin), (Ok(()), Origin::Default));

        let refuse = super::refuser(Some(&document));
        assert_eq!(refuse("x".to_owned()), "worker config /w.toml: x");
        assert_eq!(refuse("HORDE_LOG=1".to_owned()), "HORDE_LOG=1");
        assert_eq!(super::refuser(None)("x".to_owned()), "x");
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
