//! Workspace shape, read from `cargo metadata`.
//!
//! The module-structure gate has to ask cargo-modules about each target
//! individually, so it needs the list of libs and bins in the workspace.
//! Parsing the JSON here (rather than depending on a metadata crate) keeps the
//! whole path testable from a captured fixture.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// One workspace member and the targets a structural check applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePackage {
    /// Cargo package name, as passed to `-p`.
    pub name: String,
    /// Whether the package has a library target.
    pub has_lib: bool,
    /// Names of the package's binary targets.
    pub bins: Vec<String>,
    /// Directory holding the package's `Cargo.toml`.
    ///
    /// Tools that take a directory rather than a package name — cargo-crap's
    /// `--path`, for one — are narrowed by pointing them here.
    pub manifest_dir: PathBuf,
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_members: Vec<String>,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    targets: Vec<Target>,
    manifest_path: PathBuf,
}

#[derive(Deserialize)]
struct Target {
    kind: Vec<String>,
    name: String,
}

/// True for target kinds that produce a module tree worth checking.
fn is_library_kind(kind: &str) -> bool {
    matches!(kind, "lib" | "rlib" | "dylib" | "cdylib" | "proc-macro")
}

/// Extracts the workspace members and their lib/bin targets.
///
/// # Errors
///
/// Returns a message when the JSON is not `cargo metadata` output.
pub fn parse_workspace_packages(json: &str) -> Result<Vec<WorkspacePackage>, String> {
    let metadata: Metadata = serde_json::from_str(json)
        .map_err(|err| format!("could not parse cargo metadata: {err}"))?;
    let members: Vec<&String> = metadata.workspace_members.iter().collect();
    let mut packages: Vec<WorkspacePackage> = metadata
        .packages
        .iter()
        .filter(|package| members.contains(&&package.id))
        .map(package_targets)
        .collect();
    packages.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(packages)
}

fn package_targets(package: &Package) -> WorkspacePackage {
    let has_lib = package
        .targets
        .iter()
        .any(|target| target.kind.iter().any(|kind| is_library_kind(kind)));
    let bins = package
        .targets
        .iter()
        .filter(|target| target.kind.iter().any(|kind| kind == "bin"))
        .map(|target| target.name.clone())
        .collect();
    WorkspacePackage {
        name: package.name.clone(),
        has_lib,
        bins,
        manifest_dir: package
            .manifest_path
            .parent()
            .unwrap_or(Path::new("."))
            .to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../fixtures/cargo-metadata.json");

    #[test]
    fn workspace_members_are_listed_with_their_targets() {
        let packages = parse_workspace_packages(FIXTURE).expect("fixture is real cargo output");
        assert_eq!(
            packages,
            vec![WorkspacePackage {
                name: "horde-gauntlet".to_owned(),
                has_lib: true,
                bins: vec!["gauntlet".to_owned()],
                manifest_dir: PathBuf::from("/repo/crates/horde-gauntlet"),
            }]
        );
    }

    #[test]
    fn non_member_packages_are_excluded() {
        let json = r#"{
            "packages": [
                {"id": "mine", "name": "mine", "manifest_path": "/repo/crates/mine/Cargo.toml", "targets": [{"kind": ["lib"], "name": "mine"}]},
                {"id": "theirs", "name": "theirs", "manifest_path": "/repo/crates/theirs/Cargo.toml", "targets": [{"kind": ["lib"], "name": "theirs"}]}
            ],
            "workspace_members": ["mine"]
        }"#;
        let packages = parse_workspace_packages(json).expect("valid");
        assert_eq!(packages.len(), 1);
        assert_eq!(packages[0].name, "mine");
    }

    #[test]
    fn packages_are_sorted_so_gate_output_is_stable() {
        let json = r#"{
            "packages": [
                {"id": "b", "name": "zulu", "manifest_path": "/repo/crates/zulu/Cargo.toml", "targets": []},
                {"id": "a", "name": "alpha", "manifest_path": "/repo/crates/alpha/Cargo.toml", "targets": []}
            ],
            "workspace_members": ["a", "b"]
        }"#;
        let packages = parse_workspace_packages(json).expect("valid");
        assert_eq!(packages[0].name, "alpha");
        assert_eq!(packages[1].name, "zulu");
    }

    #[test]
    fn build_scripts_and_test_targets_are_not_libs_or_bins() {
        let json = r#"{
            "packages": [{"id": "a", "name": "a", "manifest_path": "/repo/crates/a/Cargo.toml", "targets": [
                {"kind": ["custom-build"], "name": "build-script-build"},
                {"kind": ["test"], "name": "integration"},
                {"kind": ["example"], "name": "demo"}
            ]}],
            "workspace_members": ["a"]
        }"#;
        let packages = parse_workspace_packages(json).expect("valid");
        assert!(!packages[0].has_lib);
        assert!(packages[0].bins.is_empty());
    }

    #[test]
    fn proc_macro_and_rlib_crates_count_as_libraries() {
        for kind in ["rlib", "proc-macro", "cdylib", "dylib"] {
            let json = format!(
                r#"{{"packages": [{{"id": "a", "name": "a", "manifest_path": "/repo/crates/a/Cargo.toml", "targets": [{{"kind": ["{kind}"], "name": "a"}}]}}], "workspace_members": ["a"]}}"#
            );
            let packages = parse_workspace_packages(&json).expect("valid");
            assert!(packages[0].has_lib, "{kind} should count as a library");
        }
    }

    #[test]
    fn every_binary_target_is_listed() {
        let json = r#"{
            "packages": [{"id": "a", "name": "a", "manifest_path": "/repo/crates/a/Cargo.toml", "targets": [
                {"kind": ["bin"], "name": "one"},
                {"kind": ["bin"], "name": "two"}
            ]}],
            "workspace_members": ["a"]
        }"#;
        let packages = parse_workspace_packages(json).expect("valid");
        assert_eq!(packages[0].bins, vec!["one".to_owned(), "two".to_owned()]);
    }

    #[test]
    fn malformed_json_is_reported_not_swallowed() {
        let message = parse_workspace_packages("not json").expect_err("invalid");
        assert!(
            message.contains("could not parse cargo metadata"),
            "{message}"
        );
    }
}
