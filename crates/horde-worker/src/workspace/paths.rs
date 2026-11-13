//! Where things live inside a workspace root, and the guards every path
//! goes through: a leaf the rest of the workspace module builds on.

use std::path::Path;

use horde_proto::prost::Message;
use horde_proto::v1::{WorkspaceDefinition, WorkspaceRepo};

use crate::error::WorkerError;

/// Where an Instance records the Definition it was materialized from.
pub const STATE_FILE: &str = ".horde-instance.pb";

/// Where per-agent checkouts live inside an Instance root.
pub const AGENTS_DIR: &str = ".horde-agents";

/// The path a repo occupies inside its Instance, defaulted to its name.
///
/// # Errors
///
/// A layout that would land outside the Instance root — absolute, empty, or
/// climbing with `..` — is refused, not joined: `Path::join` with an
/// absolute path *replaces* the root, so an unchecked layout from a confused
/// or compromised Server would clone (and later delete) wherever it pointed.
pub(crate) fn layout_of(repo: &WorkspaceRepo) -> Result<&str, String> {
    let layout = if repo.layout_path.is_empty() {
        &repo.name
    } else {
        &repo.layout_path
    };
    let mut components = Path::new(layout).components().peekable();
    let contained = components.peek().is_some()
        && components.all(|component| matches!(component, std::path::Component::Normal(_)));
    if contained {
        Ok(layout)
    } else {
        Err(format!(
            "repo {:?} lays out at {layout:?}, which is not a path inside the instance",
            repo.name
        ))
    }
}

/// Whether an id may be used as a single directory component.
///
/// Both ids interpolated into paths here are server-minted, so this is a
/// guard against a compromised or confused server, not a validation layer —
/// which is why it refuses rather than sanitizes: a cleaned-up id would
/// silently point two different ids at one directory.
pub(crate) fn is_path_safe(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        && !id.starts_with('.')
}

/// The Definition an Instance was materialized from, if one is recorded.
pub(crate) async fn read_state_file(instance_root: &Path) -> Option<WorkspaceDefinition> {
    let bytes = tokio::fs::read(instance_root.join(STATE_FILE)).await.ok()?;
    WorkspaceDefinition::decode(bytes.as_slice()).ok()
}

/// The state file's definition, or the error naming what is not there.
pub(crate) async fn required_state(
    instance_root: &Path,
    definition_id: &str,
) -> Result<WorkspaceDefinition, WorkerError> {
    read_state_file(instance_root).await.ok_or_else(|| {
        WorkerError::Workspace(format!(
            "no instance of {definition_id} is materialized under {}",
            instance_root.display()
        ))
    })
}

/// A Unix-millisecond instant on the wire.
#[must_use]
pub fn timestamp(unix_ms: i64) -> horde_proto::prost_types::Timestamp {
    horde_proto::prost_types::Timestamp {
        seconds: unix_ms.div_euclid(1000),
        nanos: i32::try_from(unix_ms.rem_euclid(1000) * 1_000_000).unwrap_or(0),
    }
}

pub(crate) fn definition_id_of(definition: &WorkspaceDefinition) -> String {
    definition
        .workspace_definition_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_directory_component_admits_no_traversal_and_no_hidden_names() {
        assert!(is_path_safe("ws-1a2b-3"));
        assert!(is_path_safe("agent_7"));
        assert!(!is_path_safe(""));
        assert!(!is_path_safe(".."));
        assert!(
            !is_path_safe(".hidden"),
            "a leading dot hides the directory"
        );
        assert!(!is_path_safe("a/b"), "a separator escapes the root");
        assert!(!is_path_safe("a b"), "shell-hostile names are refused");
    }
    #[test]
    fn a_repo_lays_out_under_its_name_unless_told_otherwise() {
        let mut repo = WorkspaceRepo {
            name: "atlas".to_owned(),
            ..WorkspaceRepo::default()
        };
        assert_eq!(layout_of(&repo), Ok("atlas"));
        repo.layout_path = "code/atlas".to_owned();
        assert_eq!(layout_of(&repo), Ok("code/atlas"));
    }
    /// A layout that would land outside the Instance is refused: `join` with
    /// an absolute path replaces the root, and `..` climbs out of it.
    #[test]
    fn a_layout_that_escapes_the_instance_is_refused() {
        for hostile in ["../escape", "/etc", "code/../../x", "./atlas", "."] {
            let repo = WorkspaceRepo {
                name: "atlas".to_owned(),
                layout_path: hostile.to_owned(),
                ..WorkspaceRepo::default()
            };
            assert!(
                layout_of(&repo).is_err_and(|e| e.contains("not a path inside")),
                "{hostile:?} must be refused"
            );
        }
        let nameless = WorkspaceRepo::default();
        assert!(layout_of(&nameless).is_err(), "an empty layout is no path");
        let escaping_name = WorkspaceRepo {
            name: "../atlas".to_owned(),
            ..WorkspaceRepo::default()
        };
        assert!(
            layout_of(&escaping_name).is_err(),
            "the name is the layout's default, so it is checked too"
        );
    }
    #[test]
    fn a_timestamp_splits_milliseconds_into_seconds_and_nanos() {
        let stamp = timestamp(1_700_000_000_123);
        assert_eq!(stamp.seconds, 1_700_000_000);
        assert_eq!(stamp.nanos, 123_000_000);
    }
}
