//! Where an effective value came from (ADR-0016's provenance).

use std::fmt;
use std::path::PathBuf;

/// The layer that decided one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// The built-in default.
    Default,
    /// The role's config file.
    File(PathBuf),
    /// A Workspace Definition's setting, by the workspace's name.
    Workspace(String),
    /// A session's setting, by its id.
    Session(String),
    /// A command-line flag, spelled with its dashes (`--permission-mode`).
    Flag(String),
    /// An environment variable.
    Env(String),
}

impl fmt::Display for Origin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => formatter.write_str("default"),
            Self::File(path) => write!(formatter, "file {}", path.display()),
            Self::Workspace(name) => write!(formatter, "workspace {name}"),
            Self::Session(id) => write!(formatter, "session {id}"),
            Self::Flag(flag) => write!(formatter, "flag {flag}"),
            Self::Env(var) => write!(formatter, "env {var}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Origin;

    #[test]
    fn each_origin_names_its_layer_and_its_source() {
        let cases = [
            (Origin::Default, "default"),
            (Origin::File("/h/c.toml".into()), "file /h/c.toml"),
            (Origin::Workspace("api".to_owned()), "workspace api"),
            (Origin::Session("s-1".to_owned()), "session s-1"),
            (
                Origin::Flag("--permission-mode".to_owned()),
                "flag --permission-mode",
            ),
            (Origin::Env("HORDE_LOG".to_owned()), "env HORDE_LOG"),
        ];
        for (origin, shown) in cases {
            assert_eq!(origin.to_string(), shown);
        }
    }
}
