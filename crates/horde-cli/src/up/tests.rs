//! `horde up`'s flow, driven with scripted stand-in daemons and a real
//! secured server: what `stand_up` watches and tears down is stdout,
//! locks, and signals, which is exactly what the scripts perform; the
//! daemons' own behavior has its own suites and the e2e.

use std::sync::{Arc, Mutex};

use super::{
    UpEnvironment, UpPaths, announced_server, forwarded_line, line_value, local_endpoint,
    paths_under, push_capped, registered_name, worker_name_from,
};

#[test]
fn the_layout_under_a_root_is_server_id_worker_unless_an_identity_is_named() {
    let root = std::path::Path::new("/home/me/.horde");
    assert_eq!(
        paths_under(root, None),
        UpPaths {
            root: root.to_path_buf(),
            server_state: root.join("server"),
            server_db: root.join("server/horde.db"),
            client_identity: root.join("id"),
            worker_state: root.join("worker"),
        },
        "id/ must be the same default the rest of the CLI resolves"
    );
    assert_eq!(
        paths_under(root, Some(std::path::Path::new("/elsewhere/id"))).client_identity,
        std::path::PathBuf::from("/elsewhere/id"),
        "--identity / HORDE_IDENTITY is honored"
    );
}

#[test]
fn the_root_is_the_flag_or_home_or_a_refusal() {
    assert_eq!(
        super::resolve_root(Some("/x".into()), Some("/home/me".into())).expect("resolves"),
        std::path::PathBuf::from("/x")
    );
    assert_eq!(
        super::resolve_root(None, Some("/home/me".into())).expect("resolves"),
        std::path::PathBuf::from("/home/me/.horde")
    );
    let refused = super::resolve_root(None, None).expect_err("nowhere honest to default to");
    assert!(refused.to_string().contains("--state-dir"), "{refused}");
}

/// The hostname becomes a principal-safe worker name.
#[test]
fn worker_names_are_principal_safe_hostnames_or_the_fallback() {
    assert_eq!(
        worker_name_from(Some("Myless-MacBook-Pro.local".to_owned())),
        "Myless-MacBook-Pro.local"
    );
    assert_eq!(
        worker_name_from(Some("études box".to_owned())),
        "-tudes-box"
    );
    assert_eq!(worker_name_from(Some("a_b-c.9".to_owned())), "a_b-c.9");
    assert_eq!(worker_name_from(Some("é é".to_owned())), "this-machine");
    assert_eq!(worker_name_from(Some("a".repeat(80))), "a".repeat(64));
    assert_eq!(worker_name_from(None), "this-machine");
    assert_eq!(worker_name_from(Some(String::new())), "this-machine");
}

/// The probe reports the OS's own answer — the machine's name is the only
/// oracle there is.
#[test]
fn the_hostname_probe_reports_the_machines_own_name() {
    let raw = std::process::Command::new("hostname")
        .output()
        .expect("hostname runs");
    let expected = String::from_utf8_lossy(&raw.stdout).trim().to_owned();
    assert!(!expected.is_empty(), "this machine has a name");
    assert_eq!(super::hostname().as_deref(), Some(expected.as_str()));
}

#[test]
fn sibling_binaries_are_found_beside_the_own_binary_or_named_missing() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let own = dir.path().join("horde");
    std::fs::write(&own, b"").expect("writes");
    std::fs::write(dir.path().join("horde-server"), b"").expect("writes");
    assert_eq!(
        super::sibling_binary(&own, "horde-server").expect("found"),
        dir.path().join("horde-server")
    );
    let refused = super::sibling_binary(&own, "horde-worker").expect_err("missing");
    assert!(
        refused.to_string().contains("horde-worker")
            && refused.to_string().contains(&*dir.path().to_string_lossy()),
        "{refused}"
    );
}

/// The server wait's truth table: everything `up` reads, or not yet.
#[test]
fn the_server_wait_needs_both_listeners_and_the_fingerprint() {
    let main = "listening main=127.0.0.1:1\n";
    let enrollment = "listening enrollment=127.0.0.1:2\n";
    let fingerprint = "server-fingerprint ab\n";
    assert!(announced_server(&format!(
        "{main}{enrollment}{fingerprint}"
    )));
    assert!(!announced_server(&format!("{main}{enrollment}")));
    assert!(!announced_server(&format!("{main}{fingerprint}")));
    assert!(!announced_server(&format!("{enrollment}{fingerprint}")));
}

#[test]
fn line_values_and_registered_names_read_the_operator_line_convention() {
    let out = "listening main=127.0.0.1:7777\nregistered worker=w resume=3\n";
    assert_eq!(line_value(out, "listening main="), Some("127.0.0.1:7777"));
    assert_eq!(line_value(out, "bootstrap-client-token "), None);
    assert_eq!(
        line_value("listening main=127.0.0.1:77", "listening main="),
        None,
        "a half-arrived line is a truncated address that parses — refuse it"
    );
    assert_eq!(registered_name(out), Some("w"), "any name, not one string");
    assert_eq!(registered_name("registered worker=x"), None, "incomplete");
}

#[test]
fn local_endpoints_dial_loopback_for_wildcard_binds() {
    let ip = |text: &str| text.parse().expect("an ip");
    assert_eq!(local_endpoint(ip("0.0.0.0"), 1), "https://127.0.0.1:1");
    assert_eq!(local_endpoint(ip("::"), 1), "https://127.0.0.1:1");
    assert_eq!(local_endpoint(ip("127.0.0.1"), 2), "https://127.0.0.1:2");
    assert_eq!(local_endpoint(ip("::1"), 3), "https://[::1]:3");
    assert_eq!(local_endpoint(ip("10.0.0.7"), 4), "https://10.0.0.7:4");
}

/// Forwarding is capped twice: each line, on a character boundary, and
/// the kept stdout as a whole.
#[test]
fn forwarded_lines_and_kept_stdout_are_capped() {
    assert_eq!(forwarded_line("w", "hi"), "[w] hi");
    let long = format!("{}é", "a".repeat(super::LINE_CAP - 1));
    let clipped = forwarded_line("w", &long);
    assert_eq!(
        clipped,
        format!("[w] {}…", "a".repeat(super::LINE_CAP - 1)),
        "clipped at the last boundary under the cap"
    );
    let exact = "b".repeat(super::LINE_CAP);
    assert_eq!(forwarded_line("w", &exact), format!("[w] {exact}"));

    let mut kept = String::new();
    push_capped(&mut kept, &"x".repeat(super::STDOUT_CAP - 1));
    push_capped(&mut kept, "éé");
    assert_eq!(
        kept.len(),
        super::STDOUT_CAP - 1,
        "a split char is not kept"
    );
    push_capped(&mut kept, "y");
    push_capped(&mut kept, "zzz");
    assert_eq!(kept.len(), super::STDOUT_CAP);
}

/// The caps as literal bytes: they are policy, and a policy asserted only
/// through its own symbols survives any rewrite of itself.
#[test]
fn the_forwarding_caps_are_exactly_the_documented_sizes() {
    assert_eq!(super::STDOUT_CAP, 65_536, "64 KiB of kept stdout");
    assert_eq!(super::LINE_CAP, 2_048, "2 KiB per forwarded line");
}

/// Line framing: complete lines leave trimmed; a newline-less remainder
/// leaves only once it is *past* the cap — at exactly the cap it waits.
#[test]
fn complete_lines_take_whole_lines_and_only_an_overgrown_remainder() {
    let mut pending = "one\ntwo\r\npart".to_owned();
    assert_eq!(super::complete_lines(&mut pending), vec!["one", "two"]);
    assert_eq!(pending, "part", "the partial line waits for its newline");

    let mut at_cap = "x".repeat(super::LINE_CAP);
    assert!(
        super::complete_lines(&mut at_cap).is_empty(),
        "exactly the cap waits"
    );
    assert_eq!(at_cap.len(), super::LINE_CAP);

    let mut past_cap = "x".repeat(super::LINE_CAP + 1);
    assert_eq!(
        super::complete_lines(&mut past_cap),
        vec!["x".repeat(super::LINE_CAP + 1)],
        "past the cap the remainder is flushed whole (the forward clips it)"
    );
    assert!(past_cap.is_empty());

    let mut far_past = format!("a\n{}", "y".repeat(super::LINE_CAP + 7));
    assert_eq!(super::complete_lines(&mut far_past).len(), 2);
}

/// The export hint is owed exactly when the identity is not the default
/// `~/.horde/id` a bare `horde` resolves.
#[test]
fn the_export_hint_is_owed_only_off_the_default_identity_dir() {
    let home = Some(std::path::PathBuf::from("/home/me"));
    assert!(!super::needs_export(
        std::path::Path::new("/home/me/.horde/id"),
        home.clone()
    ));
    assert!(super::needs_export(
        std::path::Path::new("/tmp/up/id"),
        home
    ));
}

/// The readiness Hello's give-up table: a certificate cause at once; a
/// transient or unrecognized one only once the ceiling has passed.
#[test]
fn the_hello_gives_up_on_certificates_at_once_and_on_weather_at_the_ceiling() {
    use horde_identity::diagnosis::Cause;
    let started = tokio::time::Instant::now();
    let early = started + std::time::Duration::from_millis(1);
    let ceiling = super::HELLO_CEILING;
    let at_ceiling = started + ceiling;
    assert!(super::hello_gives_up(
        Some(&Cause::UnknownIssuer),
        started,
        early,
        ceiling
    ));
    for transient in [Some(&Cause::NothingListening), Some(&Cause::TimedOut), None] {
        assert!(
            !super::hello_gives_up(transient, started, early, ceiling),
            "{transient:?} is retried before the ceiling"
        );
        assert!(
            super::hello_gives_up(transient, started, at_ceiling, ceiling),
            "{transient:?} gives up at the ceiling"
        );
    }
}

/// The production bounds as literal durations — policy, asserted apart
/// from its own symbols.
#[test]
fn the_production_timings_are_the_documented_ceilings() {
    assert_eq!(
        super::UpTimings::PRODUCTION,
        super::UpTimings {
            startup: std::time::Duration::from_secs(60),
            hello: std::time::Duration::from_secs(20),
            teardown: std::time::Duration::from_secs(10),
        }
    );
}

/// A worker `up` starts is warned about before it starts: no adapter at
/// all, or a harness binary missing from PATH; the built-in fake and a
/// harness that is on PATH earn nothing.
#[test]
fn harness_warnings_name_what_the_local_worker_cannot_run() {
    use std::os::unix::fs::PermissionsExt;
    let none = super::harness_warnings(&[], None);
    assert_eq!(none.len(), 1);
    assert!(none[0].contains("no --adapter"), "{none:?}");

    let bin = tempfile::tempdir().expect("a dir");
    let codex = bin.path().join("codex");
    std::fs::write(&codex, "#!/bin/sh\n").expect("writes");
    std::fs::set_permissions(&codex, std::fs::Permissions::from_mode(0o755)).expect("chmods");
    let path = std::env::join_paths([bin.path()]).expect("a path");
    let adapters = ["fake", "codex", "claude"].map(str::to_owned);
    let warnings = super::harness_warnings(&adapters, Some(&path));
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].contains("--adapter claude") && warnings[0].contains("no claude binary"),
        "{warnings:?}"
    );
}

/// Clipping is to the last character boundary within the cap — exactly,
/// not merely somewhere short of it.
#[test]
fn clipping_keeps_the_longest_whole_character_prefix() {
    assert_eq!(super::clip("abc", 3), "abc");
    assert_eq!(super::clip("abcd", 3), "abc");
    assert_eq!(super::clip("aé", 2), "a", "é straddles the cap");
    assert_eq!(super::clip("a😀b", 4), "a", "a four-byte char straddles");
    assert_eq!(super::clip("a😀b", 5), "a😀");
    assert_eq!(super::clip("😀", 0), "");
}

/// The CA check, without a network: an identity trusting the state's own
/// CA passes, one trusting another CA is refused, and a state with no CA
/// published is not judged here.
#[test]
fn the_ca_check_admits_the_states_own_ca_and_refuses_another() {
    let root = tempfile::tempdir().expect("a temp dir");
    let state = root.path().join("server");
    let id_dir = root.path().join("id");
    let generate =
        || horde_identity::CertificateAuthority::generate(1_754_956_800_000).expect("a CA");
    let ours = generate();
    horde_identity::StoredIdentity {
        certificate_pem: "CERT".to_owned(),
        key_pem: "KEY".to_owned(),
        ca_pem: ours.certificate_pem().to_owned(),
    }
    .save(&id_dir)
    .expect("saves");

    super::check_same_ca(&id_dir, &state).expect("no published CA is not judged");
    ours.persist(&state.join("ca")).expect("persists");
    super::check_same_ca(&id_dir, &state).expect("the same CA is admitted");
    generate().persist(&state.join("ca")).expect("replaces");
    let refused = super::check_same_ca(&id_dir, &state).expect_err("another CA");
    assert!(refused.to_string().contains("different CA"), "{refused}");
}

/// The stop wait ends on a signal and nothing else.
#[tokio::test]
async fn the_stop_wait_outlives_a_ready_future() {
    let stop = super::stop_signals().expect("signals install");
    tokio::select! {
        biased;
        () = stop => panic!("nobody sent a signal, so the wait must not end"),
        () = std::future::ready(()) => {}
    }
}

/// The whole verb from a bare test binary: the missing sibling is the
/// refusal — proof `run` composes its environment.
#[tokio::test]
async fn run_without_sibling_binaries_refuses_naming_them() {
    let root = tempfile::tempdir().expect("a temp dir");
    let refused = super::run(
        &super::UpArgs {
            state_dir: Some(root.path().to_path_buf()),
            adapters: Vec::new(),
            bind: "127.0.0.1:1".to_owned(),
            enrollment_bind: "127.0.0.1:2".to_owned(),
            name: Some("x".to_owned()),
            subject_names: Vec::new(),
        },
        None,
    )
    .await
    .expect_err("a test binary has no product siblings");
    assert!(refused.to_string().contains("horde-server"), "{refused}");
}

/// The port of an `https://host:PORT` endpoint string.
fn port_of(endpoint: &str) -> u16 {
    endpoint
        .rsplit(':')
        .next()
        .and_then(|port| port.parse().ok())
        .expect("an endpoint carries a port")
}

/// A `/bin/sh` stand-in daemon: runs `prelude`, prints `lines`, marks its
/// SIGTERM in `<name>.stopped`, then runs `epilogue` (park by default).
fn fake_daemon(
    dir: &std::path::Path,
    name: &str,
    prelude: &str,
    lines: &[String],
    epilogue: Option<&str>,
) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).expect("the bin dir");
    let mark = bin.join(format!("{name}.stopped"));
    let path = bin.join(name);
    let printed: String = lines
        .iter()
        .map(|line| format!("echo '{line}'\n"))
        .collect();
    let script = format!(
        "#!/bin/sh\ntrap 'echo stopped > {}; exit 0' TERM\n{prelude}\n{printed}{}\n",
        mark.display(),
        epilogue.unwrap_or("while true; do sleep 1; done")
    );
    std::fs::write(&path, script).expect("the script writes");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmods");
    path
}

fn stopped(dir: &std::path::Path, name: &str) -> bool {
    dir.join("bin").join(format!("{name}.stopped")).is_file()
}

fn environment_under(
    root: &std::path::Path,
    server_binary: std::path::PathBuf,
    worker_binary: std::path::PathBuf,
    bind: &str,
    adapters: Vec<String>,
) -> UpEnvironment {
    UpEnvironment {
        server_binary,
        worker_binary,
        paths: paths_under(root, None),
        bind: bind.to_owned(),
        enrollment_bind: "127.0.0.1:2".to_owned(),
        adapters,
        worker_name: "up-test".to_owned(),
        subject_names: Vec::new(),
        export_identity: false,
        harness_path: None,
        timings: TEST_TIMINGS,
    }
}

/// The bounds the flow tests run under: short enough that a flow which
/// should have answered is caught waiting well inside [`TEST_CEILING`],
/// long enough for a loaded machine's real server and scripted daemons.
const TEST_TIMINGS: super::UpTimings = super::UpTimings {
    startup: std::time::Duration::from_secs(20),
    hello: std::time::Duration::from_secs(10),
    teardown: std::time::Duration::from_secs(5),
};

/// The most any one flow test waits for `stand_up` to return. A build
/// that never announces ready, or never gives up a Hello, fails here
/// instead of hanging the suite.
const TEST_CEILING: std::time::Duration = std::time::Duration::from_secs(45);

fn no_forward() -> super::Forward {
    Arc::new(|_| {})
}

/// The environment most refusal tests share: an idle scripted server and
/// worker under `root`, dialing `bind`.
fn idle_rig(root: &std::path::Path, bind: &str) -> UpEnvironment {
    let server_binary = fake_daemon(root, "server", "", &[], None);
    let worker_binary = fake_daemon(root, "worker", "", &[], None);
    environment_under(root, server_binary, worker_binary, bind, Vec::new())
}

/// A scripted server that announces everything `up` waits on, with the
/// given fingerprint, then parks.
fn announcing_server(
    root: &std::path::Path,
    main_port: u16,
    enrollment_port: u16,
    fingerprint: &str,
) -> std::path::PathBuf {
    fake_daemon(
        root,
        "server",
        "",
        &[
            format!("listening main=127.0.0.1:{main_port}"),
            format!("listening enrollment=127.0.0.1:{enrollment_port}"),
            format!("server-fingerprint {fingerprint}"),
        ],
        None,
    )
}

/// The "found server" arrangement: its state lock held as a running
/// server holds it, and a client identity issued by `issuer` in place.
async fn found_server(
    paths: &UpPaths,
    issuer: &horde_server_testkit::secure::SecureHarness,
) -> horde_identity::StateLock {
    let held = horde_identity::StateLock::acquire(&paths.server_state, "server.lock")
        .expect("the stand-in server holds its state");
    issuer
        .enroll(horde_identity::PrincipalRole::Client, "operator")
        .await
        .expect("a client enrolls")
        .save(&paths.client_identity)
        .expect("saves");
    held
}

/// A real secured server, a scratch root, and that server "found running"
/// there (its lock held, a client identity it issued in place).
struct Found {
    harness: horde_server_testkit::secure::SecureHarness,
    root: tempfile::TempDir,
    paths: UpPaths,
    _lock: horde_identity::StateLock,
}

async fn found() -> Found {
    let harness = horde_server_testkit::secure::serve_secured()
        .await
        .expect("a secured server");
    let root = tempfile::tempdir().expect("a temp dir");
    let paths = paths_under(root.path(), None);
    let _lock = found_server(&paths, &harness).await;
    Found {
        harness,
        root,
        paths,
        _lock,
    }
}

/// The refusal from a started server that announces itself (fingerprint
/// `a…`) over whatever the test planted in `root`'s server state.
async fn refusal_after_announcing(root: &std::path::Path) -> String {
    let mut environment = idle_rig(root, "127.0.0.1:1");
    environment.server_binary = announcing_server(root, 4242, 4243, &"a".repeat(64));
    refusal_of(&environment).await
}

/// A worker state that already holds an identity, so `up` mints nothing.
fn seed_worker_identity(paths: &UpPaths) {
    let worker_identity = paths.worker_state.join("identity");
    std::fs::create_dir_all(&worker_identity).expect("the dir");
    for file in ["identity.pem", "identity.key", "ca.pem"] {
        std::fs::write(worker_identity.join(file), "seeded").expect("writes");
    }
}

/// The refusal a flow ends in, as text.
async fn refusal_of(environment: &UpEnvironment) -> String {
    stood_up(environment)
        .await
        .0
        .expect_err("this arrangement must refuse")
        .to_string()
}

/// The `127.0.0.1:<port>` the harness's main listener answers on.
fn harness_bind(harness: &horde_server_testkit::secure::SecureHarness) -> String {
    format!("127.0.0.1:{}", port_of(&harness.main_endpoint()))
}

/// Runs the flow with a shutdown that fires the moment the ready line is
/// announced — the fact every successful flow test stops on.
async fn stood_up(environment: &UpEnvironment) -> (Result<(), crate::ClientError>, Vec<String>) {
    let (ready, is_ready) = tokio::sync::oneshot::channel::<()>();
    let mut ready = Some(ready);
    let mut lines = Vec::new();
    let outcome = tokio::time::timeout(
        TEST_CEILING,
        super::stand_up(
            environment,
            &mut |line| {
                if line.starts_with("ready ")
                    && let Some(ready) = ready.take()
                {
                    let _ = ready.send(());
                }
                lines.push(line);
            },
            no_forward(),
            async move {
                let _ = is_ready.await;
            },
        ),
    )
    .await
    .expect("stand_up returns within the test ceiling: ready, or a refusal");
    (outcome, lines)
}

/// The whole first boot, in process: the bootstrap token riding the
/// filesystem, the client enrolled for real, readiness proven by a Hello,
/// a worker token minted through the real control plane and handed over
/// the environment — and it actually redeems. Teardown is proven by the
/// SIGTERM marks both scripted daemons leave.
#[tokio::test(flavor = "multi_thread")]
async fn stand_up_boots_enrolls_mints_and_tears_down_what_it_started() {
    let harness = horde_server_testkit::secure::serve_secured()
        .await
        .expect("a secured server");
    let main_port = port_of(&harness.main_endpoint());
    let enrollment_port = port_of(&harness.enrollment_endpoint());
    let root = tempfile::tempdir().expect("a temp dir");
    let paths = paths_under(root.path(), None);
    std::fs::create_dir_all(&paths.server_state).expect("the server state dir");

    let operator =
        horde_identity::Principal::new(horde_identity::PrincipalRole::Client, "operator")
            .expect("a valid name");
    let minted = harness
        .authority
        .mint(
            &operator,
            0,
            horde_server::service::enrollment::wall_clock_ms(),
        )
        .await
        .expect("the bootstrap mint");
    std::fs::write(
        paths.server_state.join("bootstrap-token"),
        format!("{}\n", harness.join_token(&minted)),
    )
    .expect("the token files");

    let token_out = root.path().join("worker-token");
    let server_binary = announcing_server(
        root.path(),
        main_port,
        enrollment_port,
        &minted.server_fingerprint,
    );
    let worker_binary = fake_daemon(
        root.path(),
        "worker",
        &format!(
            "printf '%s' \"$HORDE_JOIN_TOKEN\" > {}",
            token_out.display()
        ),
        &["registered worker=up-test resume=0".to_owned()],
        None,
    );
    let mut environment = environment_under(
        root.path(),
        server_binary,
        worker_binary,
        "127.0.0.1:1",
        vec!["fake".to_owned()],
    );
    environment.export_identity = true;
    let (outcome, lines) = stood_up(&environment).await;
    outcome.expect("the machine stands up");

    let endpoint = format!("https://127.0.0.1:{main_port}");
    assert!(
        lines.contains(&format!(
            "ready worker=up-test adapters=fake endpoint={endpoint}"
        )),
        "{lines:?}"
    );
    assert!(
        lines.contains(&format!(
            "export HORDE_IDENTITY={}",
            paths.client_identity.display()
        )),
        "the export hint follows ready: {lines:?}"
    );
    assert!(
        lines.contains(&format!(
            "client enrolled principal=client:operator identity={}",
            paths.client_identity.display()
        )),
        "{lines:?}"
    );
    assert_eq!(
        horde_identity::store::load_endpoint(&paths.client_identity),
        Some(endpoint),
        "the join stores the endpoint `up` dialed"
    );
    assert!(
        !lines.iter().any(|line| line.contains("horde://join/")),
        "no token reaches the operator's screen: {lines:?}"
    );

    let token_text = std::fs::read_to_string(&token_out).expect("the worker saw its token");
    let token = horde_identity::JoinToken::parse(&token_text).expect("the token parses");
    assert_eq!(token.host_port, format!("127.0.0.1:{enrollment_port}"));
    let enrolled = horde_identity::join(
        &token.enrollment_endpoint(),
        &token.code,
        horde_server_testkit::secure::hello(horde_identity::PrincipalRole::Worker, "up-test"),
    )
    .await
    .expect("the minted token enrolls");
    assert_eq!(enrolled.principal.to_string(), "worker:up-test");

    assert!(stopped(root.path(), "server"), "the server got SIGTERM");
    assert!(stopped(root.path(), "worker"), "the worker got SIGTERM");
}

/// A found server (its state lock held) and existing identities are left
/// alone: no server spawned, nothing re-enrolled, readiness still proven by
/// a Hello, and the ready line names whatever name the worker registered.
#[tokio::test(flavor = "multi_thread")]
async fn a_found_server_and_existing_identities_are_left_alone() {
    let Found {
        harness,
        root,
        paths,
        _lock,
    } = found().await;
    let bind = harness_bind(&harness);
    seed_worker_identity(&paths);

    let server_binary = fake_daemon(root.path(), "server", "", &[], None);
    let worker_binary = fake_daemon(
        root.path(),
        "worker",
        "",
        &["registered worker=its-cert-name resume=7".to_owned()],
        None,
    );
    let environment =
        environment_under(root.path(), server_binary, worker_binary, &bind, Vec::new());
    let (outcome, lines) = stood_up(&environment).await;
    outcome.expect("the machine stands up");

    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("server already-running state=")),
        "{lines:?}"
    );
    assert!(
        lines.contains(&format!(
            "ready worker=its-cert-name adapters=none endpoint=https://{bind}"
        )),
        "the registered name, not the flag: {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("warning: the local worker runs no harness")),
        "a worker started with no adapter is warned about: {lines:?}"
    );
    assert!(
        !stopped(root.path(), "server"),
        "never spawned, never stopped"
    );
    assert!(stopped(root.path(), "worker"), "the started worker stops");
}

/// A found server and no client identity: `up` refuses rather than read a
/// bootstrap token it did not cause — even when one is on disk.
#[tokio::test]
async fn a_found_server_without_an_identity_never_reads_the_token_file() {
    let root = tempfile::tempdir().expect("a temp dir");
    let paths = paths_under(root.path(), None);
    let _running =
        horde_identity::StateLock::acquire(&paths.server_state, "server.lock").expect("holds");
    std::fs::write(paths.server_state.join("bootstrap-token"), "planted\n").expect("plants");
    let refusal = refusal_of(&idle_rig(root.path(), "127.0.0.1:1")).await;
    assert!(
        refusal.contains("started itself") && refusal.contains("enroll join"),
        "{refusal}"
    );
}

/// A started server whose state carries no token file is a refusal naming
/// the file and the alternative — and the started server is torn down.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_bootstrap_token_refuses_and_tears_down_the_started_server() {
    let root = tempfile::tempdir().expect("a temp dir");
    let refusal = refusal_after_announcing(root.path()).await;
    assert!(
        refusal.contains("bootstrap-token") && refusal.contains("enroll join"),
        "{refusal}"
    );
    assert!(stopped(root.path(), "server"), "the error path tears down");
    assert!(
        !stopped(root.path(), "worker"),
        "nothing started the worker"
    );
}

/// A token file an earlier server left (its fingerprint is not the one
/// this server announced) is stale, and refused before any redeem.
#[tokio::test]
async fn a_stale_bootstrap_token_is_refused() {
    let root = tempfile::tempdir().expect("a temp dir");
    let paths = paths_under(root.path(), None);
    std::fs::create_dir_all(&paths.server_state).expect("the dir");
    std::fs::write(
        paths.server_state.join("bootstrap-token"),
        format!("horde://join/127.0.0.1:4243/00ff.{}\n", "b".repeat(64)),
    )
    .expect("plants");
    let refusal = refusal_after_announcing(root.path()).await;
    assert!(refusal.contains("earlier run"), "{refusal}");
}

/// A daemon that dies before its line is reported at once, with what it
/// said — not after the startup ceiling.
#[tokio::test]
async fn a_daemon_dying_during_bring_up_fails_with_its_output() {
    let root = tempfile::tempdir().expect("a temp dir");
    let mut environment = idle_rig(root.path(), "127.0.0.1:1");
    environment.server_binary = fake_daemon(
        root.path(),
        "server",
        "",
        &["boom: address in use".to_owned()],
        Some("exit 3"),
    );
    let started = std::time::Instant::now();
    let refusal = refusal_of(&environment).await;
    assert!(
        refusal.contains("exited") && refusal.contains("boom: address in use"),
        "{refusal}"
    );
    assert!(
        started.elapsed() < TEST_TIMINGS.startup,
        "reported on the exit, not the ceiling"
    );
}

/// After ready, a started daemon that dies ends `up` with an error naming
/// it — the machine is not held up half-alive.
#[tokio::test(flavor = "multi_thread")]
async fn a_daemon_dying_after_ready_ends_up_naming_it() {
    let Found {
        harness,
        root,
        paths,
        _lock,
    } = found().await;
    seed_worker_identity(&paths);
    let dies = root.path().join("die-now");
    let mut environment = idle_rig(root.path(), &harness_bind(&harness));
    environment.worker_binary = fake_daemon(
        root.path(),
        "worker",
        "",
        &["registered worker=up-test resume=0".to_owned()],
        Some(&format!(
            "while [ ! -f {} ]; do sleep 0.05; done; exit 4",
            dies.display()
        )),
    );
    let mut lines = Vec::new();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        super::stand_up(
            &environment,
            &mut |line| {
                if line.starts_with("ready ") {
                    std::fs::write(&dies, "").expect("the death cue writes");
                }
                lines.push(line);
            },
            no_forward(),
            std::future::pending(),
        ),
    )
    .await
    .expect("a death ends the hold");
    let refusal = outcome.expect_err("a dead worker").to_string();
    assert!(refusal.contains("horde-worker exited"), "{refusal}");
    assert!(
        lines.contains(&"stopped horde-worker".to_owned()),
        "{lines:?}"
    );
}

/// A second `up` on the same root refuses while the first holds it.
#[tokio::test]
async fn a_second_up_on_the_same_root_refuses() {
    let root = tempfile::tempdir().expect("a temp dir");
    let _first = horde_identity::StateLock::acquire(root.path(), "up.lock").expect("holds");
    let refusal = refusal_of(&idle_rig(root.path(), "127.0.0.1:1")).await;
    assert!(refusal.contains("another `horde up`"), "{refusal}");
    assert!(!stopped(root.path(), "server"));
}

/// `--san` is a promise to other machines: refused with loopback binds, or
/// when the name itself is no dialable name.
#[tokio::test]
async fn san_advertising_is_refused_when_nobody_else_could_dial_it() {
    let root = tempfile::tempdir().expect("a temp dir");
    let mut environment = idle_rig(root.path(), "127.0.0.1:7777");
    environment.subject_names = vec!["box.lan".to_owned()];
    let refusal = refusal_of(&environment).await;
    assert!(refusal.contains("--bind 127.0.0.1:7777"), "{refusal}");

    environment.bind = "0.0.0.0:7777".to_owned();
    let refusal = refusal_of(&environment).await;
    assert!(refusal.contains("--enrollment-bind"), "{refusal}");

    environment.subject_names = vec!["0.0.0.0".to_owned()];
    let refusal = refusal_of(&environment).await;
    assert!(refusal.contains("--san 0.0.0.0"), "{refusal}");

    environment.subject_names = vec!["box.lan".to_owned()];
    environment.bind = "not-an-address".to_owned();
    let refusal = refusal_of(&environment).await;
    assert!(refusal.contains("not an address"), "{refusal}");
    assert!(!stopped(root.path(), "server"), "nothing was started");
}

/// An identity from another CA than the server in `up`'s own state is
/// refused before any dial, naming both directories.
#[tokio::test]
async fn an_identity_from_another_ca_is_refused_against_the_state_ca() {
    // Every field bound: a `..` would drop the harness and the held lock
    // at the end of this statement, and the server would stop being found.
    let Found {
        harness: _harness,
        root,
        paths,
        _lock,
    } = found().await;
    horde_identity::CertificateAuthority::generate(1_754_956_800_000)
        .expect("a CA")
        .persist(&paths.server_state.join("ca"))
        .expect("persists");
    let refusal = refusal_of(&idle_rig(root.path(), "127.0.0.1:1")).await;
    assert!(refusal.contains("different CA"), "{refusal}");
}

/// Readiness is a real Hello: against a server whose CA the identity does
/// not trust, it fails at once with the diagnosed cause — a TCP connect
/// would have called this server ready.
#[tokio::test(flavor = "multi_thread")]
async fn the_hello_probe_diagnoses_a_server_from_another_ca() {
    let serving = horde_server_testkit::secure::serve_secured()
        .await
        .expect("a secured server");
    let other = horde_server_testkit::secure::serve_secured()
        .await
        .expect("another secured server");
    let root = tempfile::tempdir().expect("a temp dir");
    let paths = paths_under(root.path(), None);
    // The identity comes from `other`; the server dialed is `serving`.
    let _running = found_server(&paths, &other).await;
    let started = std::time::Instant::now();
    let refusal = refusal_of(&idle_rig(root.path(), &harness_bind(&serving))).await;
    assert!(
        refusal.contains("did not answer a Hello") && refusal.contains("not issued by the CA"),
        "{refusal}"
    );
    assert!(
        started.elapsed() < TEST_TIMINGS.hello,
        "a certificate failure is not retried"
    );
}

/// A stop that lands during bring-up still tears down what was started.
/// The stop fires on a fact — the scripted server has run — not a timer.
#[tokio::test]
async fn a_stop_during_bring_up_tears_down_what_was_started() {
    let root = tempfile::tempdir().expect("a temp dir");
    let ran = root.path().join("server-ran");
    let mut environment = idle_rig(root.path(), "127.0.0.1:1");
    environment.server_binary = fake_daemon(
        root.path(),
        "server",
        &format!("touch {}", ran.display()),
        &[],
        None,
    );
    let mut lines = Vec::new();
    let watched = ran.clone();
    let outcome = super::stand_up(
        &environment,
        &mut |line| lines.push(line),
        no_forward(),
        async move {
            for _ in 0_u32..1500 {
                if watched.exists() {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            panic!("the scripted server never ran");
        },
    )
    .await;
    outcome.expect("a stop is not an error");
    assert!(
        lines.contains(&"stopped during bring-up".to_owned()),
        "{lines:?}"
    );
    // The trap runs once the shell finishes its current command; the
    // teardown's bounded wait is what this mark proves.
    assert!(
        stopped(root.path(), "server"),
        "the started server got SIGTERM"
    );
}

/// Once forwarding is on, a daemon's lines reach the sink tagged and
/// clipped.
#[tokio::test]
async fn forwarded_daemon_lines_are_tagged_and_clipped() {
    let root = tempfile::tempdir().expect("a temp dir");
    let go = root.path().join("go");
    let long = "x".repeat(super::LINE_CAP + 10);
    let binary = fake_daemon(
        root.path(),
        "chatty",
        &format!("while [ ! -f {} ]; do sleep 0.02; done", go.display()),
        &["hello".to_owned(), long],
        None,
    );
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = Arc::clone(&seen);
    let child = super::WatchedChild::spawn(
        "chatty",
        &binary,
        &[],
        &[],
        Arc::new(move |line| sink.lock().expect("unpoisoned").push(line)),
    )
    .expect("spawns");
    child
        .forwarding
        .store(true, std::sync::atomic::Ordering::SeqCst);
    std::fs::write(&go, "").expect("the cue writes");
    for _ in 0_u32..1500 {
        if seen.lock().expect("unpoisoned").len() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let seen = seen.lock().expect("unpoisoned").clone();
    assert_eq!(seen.first().map(String::as_str), Some("[chatty] hello"));
    assert!(
        seen.get(1).is_some_and(|line| line.ends_with('…')),
        "{seen:?}"
    );
    child.stop(TEST_TIMINGS.teardown).await;
}
