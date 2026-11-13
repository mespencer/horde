//! Codex protocol drift, against codex's own generated schema (W12c).
//!
//! `schema/surface.json` is [`horde_adapter_codex::surface::extract`] run
//! over `codex app-server generate-json-schema` at the codex version the
//! adapter was last measured against: every method and field path the codec
//! depends on, with its type summary. Two checks:
//!
//! * offline (always): the snapshot covers [`SURFACE`] exactly and nothing in
//!   it is `MISSING` — so the declaration and the snapshot cannot drift apart
//!   from each other;
//! * live (`--ignored`): regenerate from the installed codex and diff. Run it
//!   when bumping codex; `HORDE_UPDATE_CODEX_SURFACE=1` rewrites the snapshot
//!   after the diff has been reviewed.
//!
//! ```sh
//! cargo test -p horde-adapter-codex --test schema_drift -- --ignored --nocapture
//! ```

use std::collections::BTreeMap;

use horde_adapter_codex::surface::{Extracted, MISSING, SURFACE, extract};

fn snapshot_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("schema/surface.json")
}

fn committed() -> (String, Extracted) {
    let text = std::fs::read_to_string(snapshot_path()).expect("the snapshot is committed");
    let value: serde_json::Value = serde_json::from_str(&text).expect("the snapshot is JSON");
    let version = value["codex_version"]
        .as_str()
        .expect("the snapshot names its codex")
        .to_owned();
    let surface: Extracted =
        serde_json::from_value(value["surface"].clone()).expect("the surface is a map of maps");
    (version, surface)
}

#[test]
fn the_snapshot_covers_exactly_what_the_codec_declares_and_all_of_it_exists() {
    let (version, surface) = committed();
    assert!(version.starts_with("codex-cli "), "{version}");

    let declared: BTreeMap<String, Vec<&str>> = SURFACE
        .iter()
        .map(|used| (used.source.key(), used.paths.to_vec()))
        .collect();
    assert_eq!(
        surface.keys().collect::<Vec<_>>(),
        declared.keys().collect::<Vec<_>>(),
        "regenerate the snapshot after changing SURFACE"
    );
    for (key, paths) in &declared {
        let recorded = &surface[key];
        let mut expected: Vec<&str> = paths.clone();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(
            recorded.keys().map(String::as_str).collect::<Vec<_>>(),
            expected,
            "{key}: regenerate the snapshot after changing SURFACE"
        );
        for (path, summary) in recorded {
            assert_ne!(
                summary, MISSING,
                "{key} {path}: the codec depends on a field {version}'s schema does not have"
            );
        }
    }
}

/// The vocabularies the codec speaks, pinned in the snapshot: the decision
/// values exec and file-change replies use, and the elicitation actions.
#[test]
fn the_reply_vocabularies_the_codec_sends_are_in_the_schema() {
    let (_, surface) = committed();
    let decision = &surface["CommandExecutionRequestApprovalResponse.json"]["decision"];
    for value in ["accept", "acceptForSession", "decline", "cancel"] {
        assert!(decision.contains(value), "{decision}");
    }
    let file_decision = &surface["FileChangeRequestApprovalResponse.json"]["decision"];
    for value in ["accept", "acceptForSession", "decline", "cancel"] {
        assert!(file_decision.contains(value), "{file_decision}");
    }
    let action = &surface["McpServerElicitationRequestResponse.json"]["action"];
    for value in ["accept", "decline"] {
        assert!(action.contains(value), "{action}");
    }
    let scope = &surface["PermissionsRequestApprovalResponse.json"]["scope"];
    for value in ["turn", "session"] {
        assert!(scope.contains(value), "{scope}");
    }
}

#[test]
#[ignore = "runs the installed codex; run when bumping the supported codex"]
fn the_installed_codex_schema_matches_the_snapshot() {
    let version = std::process::Command::new("codex")
        .arg("--version")
        .output()
        .expect("codex is on PATH");
    let version = String::from_utf8_lossy(&version.stdout).trim().to_owned();
    let out = tempfile::tempdir().expect("a scratch dir");
    let generated = std::process::Command::new("codex")
        .args(["app-server", "generate-json-schema", "--out"])
        .arg(out.path())
        .output()
        .expect("codex generates its schema");
    assert!(
        generated.status.success(),
        "generate-json-schema failed: {}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let fresh = extract(out.path()).expect("every declared method exists in the schema");

    if std::env::var_os("HORDE_UPDATE_CODEX_SURFACE").is_some() {
        let snapshot = serde_json::json!({ "codex_version": version, "surface": fresh });
        let text = serde_json::to_string_pretty(&snapshot).expect("serializes");
        std::fs::write(snapshot_path(), format!("{text}\n")).expect("writes the snapshot");
        eprintln!("rewrote {} from {version}", snapshot_path().display());
        return;
    }

    let (recorded_version, recorded) = committed();
    let mut drift = Vec::new();
    for (key, paths) in &fresh {
        for (path, summary) in paths {
            let before = recorded.get(key).and_then(|paths| paths.get(path));
            if before != Some(summary) {
                drift.push(format!(
                    "{key} {path}: {recorded_version} {before:?} -> {version} {summary:?}"
                ));
            }
        }
    }
    assert!(
        drift.is_empty(),
        "the codec's codex surface drifted:\n{}\nreview each line against wire.rs/codec.rs, \
         then re-run with HORDE_UPDATE_CODEX_SURFACE=1",
        drift.join("\n")
    );
}
