//! `horde workspace update`: rename a definition, re-lay its repos, or
//! replace its unmanaged paths, at the version `--etag` names.
//!
//! The server's `UpdateWorkspaceDefinition` replaces `unmanaged_paths`
//! wholesale and reads an empty name or repo list as "unchanged". The verb
//! keeps that contract honest from a shell: an update that mentions no
//! unmanaged paths keeps the current ones (read back first), and clearing
//! them is its own explicit flag rather than the accident of omitting one.

use clap::Args;
use horde_proto::v1::{UpdateWorkspaceDefinitionRequest, WorkspaceDefinition};

use crate::client::{Client, ClientError};
use crate::render::{self, Format};

/// The flags of `workspace update`.
#[derive(Args, Debug)]
pub struct UpdateArgs {
    /// The definition, by id or name.
    pub reference: String,
    /// The etag `list` or `get` shows, so a concurrent edit aborts.
    #[arg(long)]
    pub etag: String,
    /// A new name for the definition.
    #[arg(long)]
    pub name: Option<String>,
    /// The definition's unmanaged paths, replacing the current set;
    /// repeatable. Omitted, the current set is kept.
    #[arg(long = "unmanaged", conflicts_with = "clear_unmanaged")]
    pub unmanaged_paths: Vec<String>,
    /// Drop every unmanaged path.
    #[arg(long)]
    pub clear_unmanaged: bool,
    /// The repo set re-laid (branch, path), in `create`'s --repo form;
    /// repeatable. Must name exactly the current repos — additions go
    /// through `add-repo`. Omitted, the layout is unchanged.
    #[arg(long = "repo", value_name = "NAME=URL[,branch=B][,path=P]")]
    pub repos: Vec<String>,
}

/// The update request these flags make of `current`.
///
/// # Errors
///
/// A `--repo` value that does not parse.
pub fn request_from(
    args: &UpdateArgs,
    current: &WorkspaceDefinition,
) -> Result<UpdateWorkspaceDefinitionRequest, ClientError> {
    let repos = args
        .repos
        .iter()
        .map(|value| crate::cli::repo_from(value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(UpdateWorkspaceDefinitionRequest {
        workspace_definition_id: current.workspace_definition_id.clone(),
        // The caller's etag, never the one just read: the read is only for
        // the id and the kept paths, and substituting its etag would turn
        // the optimistic check into a blind overwrite.
        etag: args.etag.clone(),
        name: args.name.clone().unwrap_or_default(),
        unmanaged_paths: unmanaged_paths_for(args, current),
        repos,
    })
}

/// The unmanaged paths the update sends: cleared, replaced, or kept.
fn unmanaged_paths_for(args: &UpdateArgs, current: &WorkspaceDefinition) -> Vec<String> {
    if args.clear_unmanaged {
        Vec::new()
    } else if args.unmanaged_paths.is_empty() {
        current.unmanaged_paths.clone()
    } else {
        args.unmanaged_paths.clone()
    }
}

/// Runs `workspace update`: resolve, update, print the journaled result.
///
/// # Errors
///
/// An unknown reference, an unparsable `--repo`, or the server's refusal —
/// a stale etag (ABORTED), a taken name, or a changed repo set.
pub async fn run(
    client: &mut Client,
    args: &UpdateArgs,
    format: Format,
) -> Result<(), ClientError> {
    let current = client
        .get_workspace_definition(&args.reference)
        .await?
        .definition
        .unwrap_or_default();
    let request = request_from(args, &current)?;
    let updated = client.update_workspace_definition(request).await?;
    println!("{}", render::workspace(&updated, format));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct Harness {
        #[command(flatten)]
        args: UpdateArgs,
    }

    fn parsed(argv: &[&str]) -> UpdateArgs {
        Harness::try_parse_from(std::iter::once("update").chain(argv.iter().copied()))
            .expect("the flags parse")
            .args
    }

    fn current() -> WorkspaceDefinition {
        WorkspaceDefinition {
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new("ws-1")),
            name: "atlas".to_owned(),
            unmanaged_paths: vec!["datasets".to_owned()],
            etag: "read-etag".to_owned(),
            ..WorkspaceDefinition::default()
        }
    }

    #[test]
    fn a_bare_rename_keeps_the_paths_and_sends_the_callers_etag() {
        let request = request_from(
            &parsed(&["atlas", "--etag", "mine", "--name", "atlas-2"]),
            &current(),
        )
        .expect("a request");
        assert_eq!(request.etag, "mine");
        assert_eq!(request.name, "atlas-2");
        assert_eq!(request.unmanaged_paths, vec!["datasets".to_owned()]);
        assert!(request.repos.is_empty(), "no --repo is an unchanged layout");
        assert_eq!(
            request.workspace_definition_id,
            current().workspace_definition_id
        );
    }

    #[test]
    fn paths_are_replaced_when_given_and_cleared_only_when_asked() {
        let replaced = request_from(
            &parsed(&["atlas", "--etag", "e", "--unmanaged", "weights"]),
            &current(),
        )
        .expect("a request");
        assert_eq!(replaced.unmanaged_paths, vec!["weights".to_owned()]);
        assert!(replaced.name.is_empty(), "no --name is an unchanged name");

        let cleared = request_from(
            &parsed(&["atlas", "--etag", "e", "--clear-unmanaged"]),
            &current(),
        )
        .expect("a request");
        assert!(cleared.unmanaged_paths.is_empty());
        assert!(
            Harness::try_parse_from([
                "update",
                "atlas",
                "--etag",
                "e",
                "--clear-unmanaged",
                "--unmanaged",
                "x",
            ])
            .is_err(),
            "clearing and replacing at once is refused"
        );
    }

    #[test]
    fn repos_parse_in_creates_form_and_a_bad_one_is_refused() {
        let request = request_from(
            &parsed(&[
                "atlas",
                "--etag",
                "e",
                "--repo",
                "atlas=https://git.example/atlas.git,branch=dev",
            ]),
            &current(),
        )
        .expect("a request");
        assert_eq!(request.repos.len(), 1);
        assert_eq!(request.repos[0].name, "atlas");
        assert!(
            request_from(
                &parsed(&["atlas", "--etag", "e", "--repo", "nonsense"]),
                &current()
            )
            .is_err()
        );
    }
}
