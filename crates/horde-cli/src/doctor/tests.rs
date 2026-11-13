//! `horde doctor`'s checks against real directories, real certificates and
//! a real secured server — every input passed explicitly, nothing read from
//! the process environment.

use std::os::unix::fs::PermissionsExt;

use super::{
    Check, Inputs, Verdict, available_kib, certificate_check, disk_verdict, harness_checks, render,
    report, state_dir_checks, verdict_of,
};
use crate::Format;

const DAY: i64 = 86_400;

fn verdict_named<'checks>(checks: &'checks [Check], name: &str) -> &'checks Check {
    checks
        .iter()
        .find(|check| check.name == name)
        .unwrap_or_else(|| panic!("no {name} check in {checks:?}"))
}

/// A certificate is judged against its own window: valid, near expiry,
/// expired, not yet valid, or unreadable.
#[test]
fn certificates_are_judged_against_their_window() {
    const NOW_MS: i64 = 1_700_000_000_000;
    let ca = horde_identity::CertificateAuthority::generate(NOW_MS).expect("a CA");
    let pem = ca.certificate_pem();
    let (not_before, not_after) = horde_identity::validity_of_pem(pem).expect("parses");

    let fine = certificate_check("ca", pem, NOW_MS / 1_000);
    assert_eq!(fine.verdict, Verdict::Ok, "{fine:?}");
    assert!(fine.detail.contains("more days"), "{fine:?}");

    let soon = certificate_check("ca", pem, not_after - 2 * DAY);
    assert_eq!(soon.verdict, Verdict::Warn, "{soon:?}");
    assert_eq!(soon.detail, "expires in 48h");
    assert!(soon.fix.is_some());

    let edge = certificate_check("ca", pem, not_after - 7 * DAY);
    assert_eq!(
        edge.verdict,
        Verdict::Ok,
        "exactly a week out is not yet a warning"
    );

    let gone = certificate_check("ca", pem, not_after + 3 * DAY);
    assert_eq!(gone.verdict, Verdict::Fail);
    assert_eq!(gone.detail, "expired 3 days ago");
    let at_expiry = certificate_check("ca", pem, not_after);
    assert_eq!(
        at_expiry.verdict,
        Verdict::Fail,
        "not_after itself is expired"
    );

    let early = certificate_check("ca", pem, not_before - 10);
    assert_eq!(early.verdict, Verdict::Fail);
    assert_eq!(early.detail, "not valid for another 10s");
    let at_start = certificate_check("ca", pem, not_before);
    assert_ne!(at_start.detail, "not valid for another 0s");

    let garbled = certificate_check("certificate", "nonsense", 0);
    assert_eq!(garbled.verdict, Verdict::Fail);
    assert!(garbled.detail.starts_with("unreadable"), "{garbled:?}");
}

/// Harnesses are found as executables on the given PATH; a missing one is
/// a warning naming the fix; no list means the default pair.
#[test]
fn harness_binaries_are_looked_for_on_the_given_path() {
    let bin = tempfile::tempdir().expect("a dir");
    let claude = bin.path().join("claude");
    std::fs::write(&claude, "#!/bin/sh\n").expect("writes");
    std::fs::set_permissions(&claude, std::fs::Permissions::from_mode(0o755)).expect("chmods");
    let plain = bin.path().join("codex");
    std::fs::write(&plain, "not executable").expect("writes");
    std::fs::set_permissions(&plain, std::fs::Permissions::from_mode(0o644)).expect("chmods");
    let path = std::env::join_paths([bin.path()]).expect("a path");

    let found_at = harness_checks(&[], Some(&path));
    assert_eq!(
        found_at[0].1,
        Some(("claude".to_owned(), claude.clone())),
        "where a harness was found travels on, for its version check"
    );
    assert_eq!(found_at[1].1, None);
    let checks: Vec<Check> = found_at.into_iter().map(|(check, _)| check).collect();
    assert_eq!(checks.len(), 2);
    let found = verdict_named(&checks, "harness claude");
    assert_eq!(found.verdict, Verdict::Ok);
    assert_eq!(found.detail, claude.display().to_string());
    let missing = verdict_named(&checks, "harness codex");
    assert_eq!(
        missing.verdict,
        Verdict::Warn,
        "a non-executable file is no harness"
    );
    assert!(
        missing
            .fix
            .as_deref()
            .is_some_and(|fix| fix.contains("--adapter"))
    );

    let named = harness_checks(&["fake-harness".to_owned()], None);
    assert_eq!(named.len(), 1);
    assert_eq!(named[0].0.verdict, Verdict::Warn, "no PATH finds nothing");
}

/// Each harness's version is classified against its adapter's measured
/// range: supported is ok, untested warns, refused fails, and a harness no
/// adapter knows is reported without a verdict.
#[test]
fn harness_versions_are_classified_against_each_adapters_range() {
    use super::harness_version_check;

    let claude = harness_version_check("claude", Ok("2.1.323 (Claude Code)\n".to_owned()));
    assert_eq!(claude.verdict, Verdict::Ok);
    assert_eq!(claude.name, "harness claude version");
    assert!(claude.detail.contains("2.1.323 is supported"), "{claude:?}");

    let newer = harness_version_check("codex", Ok("codex-cli 0.170.0".to_owned()));
    assert_eq!(newer.verdict, Verdict::Warn);
    assert!(newer.detail.contains("untested"), "{newer:?}");
    assert!(newer.fix.as_deref().is_some_and(|fix| fix.contains("first turn")));

    let old = harness_version_check("codex", Ok("codex-cli 0.145.0".to_owned()));
    assert_eq!(old.verdict, Verdict::Fail);
    assert!(old.detail.contains("below 0.147.0"), "{old:?}");
    assert!(old.fix.as_deref().is_some_and(|fix| fix.contains("upgrade")));

    let silent = harness_version_check("claude", Err("could not run: denied".to_owned()));
    assert_eq!(silent.verdict, Verdict::Warn);
    assert!(silent.detail.contains("could not run"), "{silent:?}");

    let other = harness_version_check("fake-harness", Ok(" 9.9 \n".to_owned()));
    assert_eq!((other.verdict, other.detail.as_str()), (Verdict::Ok, "9.9"));
    let other = harness_version_check("fake-harness", Err("hung".to_owned()));
    assert_eq!(other.detail, "unknown (hung)");
}

/// An owner-only dir passes; a group-readable one warns with the chmod; an
/// absent one is fine (created owner-only on first use).
#[test]
fn state_dirs_are_checked_for_permissions_and_disk() {
    let root = tempfile::tempdir().expect("a dir");
    let private = root.path().join("private");
    std::fs::create_dir(&private).expect("mkdir");
    std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700)).expect("chmods");
    let checks = state_dir_checks(&private);
    assert_eq!(checks[0].verdict, Verdict::Ok, "{checks:?}");
    assert_eq!(checks[0].detail, "owner-only (700)");
    assert!(checks[1].name.starts_with("disk "), "{checks:?}");
    assert_ne!(
        checks[1].detail, "could not measure free space",
        "df answers on macOS and linux"
    );

    let open = root.path().join("open");
    std::fs::create_dir(&open).expect("mkdir");
    std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o750)).expect("chmods");
    let checks = state_dir_checks(&open);
    assert_eq!(checks[0].verdict, Verdict::Warn);
    assert_eq!(
        checks[0].fix.as_deref(),
        Some(format!("chmod 700 {}", open.display()).as_str())
    );

    let absent = state_dir_checks(&root.path().join("absent"));
    assert_eq!(absent.len(), 1);
    assert_eq!(absent[0].verdict, Verdict::Ok);
}

/// `df -Pk`'s available column, and the thresholds over it.
#[test]
fn free_disk_is_read_from_df_and_judged() {
    let df = "Filesystem 1024-blocks Used Available Capacity Mounted on\n\
              /dev/disk3s1 971350180 12000000 76543210 14% /\n";
    assert_eq!(available_kib(df), Some(76_543_210));
    assert_eq!(available_kib("header only\n"), None);
    assert_eq!(available_kib("h\na b c notanumber\n"), None);

    assert_eq!(disk_verdict("d", Some(76_543_210)).verdict, Verdict::Ok);
    assert_eq!(disk_verdict("d", Some(76_543_210)).detail, "72 GiB free");
    let low = disk_verdict("d", Some(1024 * 1024));
    assert_eq!(
        (low.verdict, low.detail.as_str()),
        (Verdict::Warn, "1024 MiB free")
    );
    assert_eq!(
        disk_verdict("d", Some(2 * 1024 * 1024)).verdict,
        Verdict::Ok
    );
    let critical = disk_verdict("d", Some(1024));
    assert_eq!(
        (critical.verdict, critical.detail.as_str()),
        (Verdict::Fail, "1 MiB free")
    );
    assert_eq!(disk_verdict("d", Some(256 * 1024)).verdict, Verdict::Warn);
    assert_eq!(disk_verdict("d", None).verdict, Verdict::Warn);
}

/// Warnings pass, failures fail with their count; both formats carry
/// every check, the fix included.
#[test]
fn the_report_renders_and_fails_only_on_failures() {
    let checks = vec![
        Check::ok("identity", "fine"),
        Check::not_ok("harness codex", Verdict::Warn, "missing", "install it"),
    ];
    assert!(verdict_of(&checks).is_ok(), "warnings alone pass");
    assert_eq!(
        render(&checks, Format::Human),
        "ok    identity: fine\nwarn  harness codex: missing\n      fix: install it"
    );
    let mut failing = checks.clone();
    failing.push(Check::not_ok("server", Verdict::Fail, "down", "start it"));
    failing.push(Check::not_ok("endpoint", Verdict::Fail, "none", "pass one"));
    let refused = verdict_of(&failing).expect_err("two failures");
    assert!(refused.to_string().contains("2 problem"), "{refused}");
    let json: serde_json::Value =
        serde_json::from_str(&render(&failing, Format::Json)).expect("json");
    assert_eq!(json["healthy"], false);
    assert_eq!(json["checks"][2]["verdict"], "fail");
    assert_eq!(json["checks"][2]["fix"], "start it");
    assert_eq!(json["checks"][0]["fix"], serde_json::Value::Null);
    let json: serde_json::Value =
        serde_json::from_str(&render(&checks, Format::Json)).expect("json");
    assert_eq!(json["healthy"], true);
}

fn inputs(identity: &std::path::Path, endpoint: Option<String>) -> Inputs {
    Inputs {
        endpoint,
        identity: Some(identity.to_path_buf()),
        home: None,
        path: None,
        harnesses: vec!["claude".to_owned()],
        state_dirs: Vec::new(),
        now_unix_secs: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| i64::try_from(elapsed.as_secs()).unwrap_or(0)),
    }
}

/// Against a real secured server with an identity it issued: every check
/// on the connection path passes, and the server line names the principal.
#[tokio::test(flavor = "multi_thread")]
async fn a_joined_machine_passes_the_connection_checks() {
    let harness = horde_server_testkit::secure::serve_secured()
        .await
        .expect("a secured server");
    let dir = tempfile::tempdir().expect("a dir");
    let identity_dir = dir.path().join("id");
    harness
        .enroll(horde_identity::PrincipalRole::Client, "doctor")
        .await
        .expect("enrolls")
        .save(&identity_dir)
        .expect("saves");
    let checks = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        report(&inputs(&identity_dir, Some(harness.main_endpoint()))),
    )
    .await
    .expect("the report is bounded");
    for name in ["identity", "certificate", "ca", "endpoint", "server"] {
        assert_eq!(
            verdict_named(&checks, name).verdict,
            Verdict::Ok,
            "{checks:?}"
        );
    }
    assert!(
        verdict_named(&checks, "identity")
            .detail
            .contains("client:doctor"),
        "{checks:?}"
    );
    assert!(
        verdict_named(&checks, "server")
            .detail
            .contains("client:doctor"),
        "{checks:?}"
    );
    assert_eq!(
        verdict_named(&checks, "harness claude").verdict,
        Verdict::Warn
    );
}

/// A machine with nothing: the identity and endpoint fail with their
/// fixes, and nothing past them is attempted.
#[tokio::test]
async fn an_empty_machine_fails_with_the_fixes() {
    let dir = tempfile::tempdir().expect("a dir");
    let checks = report(&inputs(&dir.path().join("id"), None)).await;
    let identity = verdict_named(&checks, "identity");
    assert_eq!(identity.verdict, Verdict::Fail);
    assert!(
        identity
            .fix
            .as_deref()
            .is_some_and(|fix| fix.contains("horde up"))
    );
    let endpoint = verdict_named(&checks, "endpoint");
    assert_eq!(endpoint.verdict, Verdict::Fail);
    assert!(
        endpoint
            .fix
            .as_deref()
            .is_some_and(|fix| fix.contains("enroll join"))
    );
    assert!(
        checks.iter().all(|check| check.name != "server"),
        "{checks:?}"
    );
    assert!(
        checks.iter().all(|check| check.name != "certificate"),
        "{checks:?}"
    );
    let refused = verdict_of(&checks).expect_err("two failures").to_string();
    assert!(refused.contains("2 problem"), "{refused}");
}

/// A corrupt identity fails to load, named; an unreachable server fails
/// its check with the diagnosis rather than hanging.
#[tokio::test]
async fn a_corrupt_identity_and_a_dead_server_are_named() {
    let dir = tempfile::tempdir().expect("a dir");
    let identity_dir = dir.path().join("id");
    std::fs::create_dir_all(&identity_dir).expect("mkdir");
    for file in ["identity.pem", "identity.key", "ca.pem"] {
        std::fs::write(identity_dir.join(file), "garbage").expect("writes");
    }
    let checks = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        report(&inputs(
            &identity_dir,
            Some("http://127.0.0.1:1".to_owned()),
        )),
    )
    .await
    .expect("bounded");
    let identity = verdict_named(&checks, "identity");
    assert_eq!(identity.verdict, Verdict::Fail);
    assert!(identity.detail.contains("does not load"), "{identity:?}");
    let server = verdict_named(&checks, "server");
    assert_eq!(server.verdict, Verdict::Fail, "{checks:?}");
    assert!(!server.detail.contains('\n'), "one line: {server:?}");
}

/// The thresholds at their exact edges: one KiB under each disk floor
/// takes the stricter verdict, and a certificate inside the last week
/// warns.
#[test]
fn every_threshold_holds_at_its_edge() {
    const NOW_MS: i64 = 1_700_000_000_000;
    assert_eq!(
        disk_verdict("d", Some(2 * 1024 * 1024 - 1)).verdict,
        Verdict::Warn
    );
    assert_eq!(
        disk_verdict("d", Some(256 * 1024 - 1)).verdict,
        Verdict::Fail
    );
    let ca = horde_identity::CertificateAuthority::generate(NOW_MS).expect("a CA");
    let pem = ca.certificate_pem();
    let (_, not_after) = horde_identity::validity_of_pem(pem).expect("parses");
    let inside_the_week = certificate_check("ca", pem, not_after - 7 * DAY + 1);
    assert_eq!(
        inside_the_week.verdict,
        Verdict::Warn,
        "{inside_the_week:?}"
    );
}

/// The doctor's clock is the wall clock, in seconds.
#[test]
fn the_doctor_reads_the_wall_clock_in_seconds() {
    let expected = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| i64::try_from(elapsed.as_secs()).unwrap_or(0));
    let read = super::unix_now_secs();
    assert!((read - expected).abs() <= 5, "{read} vs {expected}");
}

/// The verb itself fails — a non-zero exit — when a check fails: here a
/// machine with no identity and no endpoint.
#[tokio::test]
async fn the_verb_fails_when_a_check_fails() {
    let dir = tempfile::tempdir().expect("a dir");
    let args = super::DoctorArgs {
        harnesses: vec!["claude".to_owned()],
        state_dirs: vec![dir.path().join("absent")],
    };
    let identity = dir.path().join("id");
    let refused = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        super::run(None, Some(&identity), &args, Format::Json),
    )
    .await
    .expect("bounded")
    .expect_err("no identity, no endpoint");
    assert!(
        matches!(refused, crate::ClientError::Unhealthy { failures: 2 }),
        "{refused:?}"
    );
}
