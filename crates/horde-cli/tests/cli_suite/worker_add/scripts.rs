//! `horde worker add`'s remote scripts, run for real under `sh` with a
//! throwaway `$HOME`, and its local command runner — so what is asserted
//! is what a remote shell would do, not what the text looks like.
//!
//! These live in the CLI suite rather than beside the plan, because they
//! fork processes: `cargo test` runs the library's tests as threads of one
//! process, where a fork landing between another test's write and exec of
//! a script makes that exec fail with ETXTBSY.

use std::path::Path;
use std::time::Duration;

use horde_cli::worker_add::plan::*;

type Outcome = Result<(), Box<dyn std::error::Error>>;

fn run_sh(script: &str, home: &Path, path: Option<&str>, stdin: &str) -> std::process::Output {
    use std::io::Write;
    let mut command = std::process::Command::new("sh");
    command
        .arg("-c")
        .arg(script)
        .env("HOME", home)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if let Some(path) = path {
        command.env("PATH", path);
    }
    let mut child = command.spawn().expect("sh starts");
    child
        .stdin
        .take()
        .expect("stdin is piped")
        .write_all(stdin.as_bytes())
        .expect("stdin takes the input");
    child.wait_with_output().expect("sh finishes")
}

fn write_executable(path: &Path, content: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, content).expect("the script writes");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .expect("the script is executable");
}

#[test]
fn shell_words_quote_only_what_needs_it() {
    assert_eq!(sh_word("plain/path-1.2"), "plain/path-1.2");
    assert_eq!(sh_word("two words"), "'two words'");
    assert_eq!(sh_word(""), "''");
    assert_eq!(sh_quote("it's"), r"'it'\''s'");
    let quoted = run_sh(
        &format!("printf %s {}", sh_word("a 'b' $c")),
        Path::new("/"),
        None,
        "",
    );
    assert_eq!(String::from_utf8_lossy(&quoted.stdout), "a 'b' $c");
}

#[test]
fn the_ssh_argument_survives_a_login_shell() {
    let remote = Remote {
        ssh: "ssh".to_owned(),
        scp: "scp".to_owned(),
        config: None,
        target: "box".to_owned(),
    };
    let script = "printf '%s|' \"it's\" '$HOME'";
    let argument = remote.ssh(script).args.pop().expect("a remote command");
    let out = run_sh(&argument, Path::new("/nowhere"), None, "");
    assert_eq!(String::from_utf8_lossy(&out.stdout), "it's|$HOME|");
}

#[test]
fn the_probe_reports_platform_home_path_and_enrollment() -> Outcome {
    let home = tempfile::tempdir()?;
    let out = run_sh(&probe_script(), home.path(), Some("/usr/bin:/bin"), "");
    let probe = parse_probe(&String::from_utf8_lossy(&out.stdout))?;
    assert!(Platform::from_uname(&probe.uname).is_some(), "{probe:?}");
    assert_eq!(probe.home, home.path().display().to_string());
    assert_eq!(probe.path, "/usr/bin:/bin");
    assert!(!probe.enrolled);

    let marker = home.path().join(IDENTITY_MARKER);
    std::fs::create_dir_all(marker.parent().ok_or("a parent")?)?;
    std::fs::write(&marker, "pem")?;
    let out = run_sh(&probe_script(), home.path(), None, "");
    assert!(parse_probe(&String::from_utf8_lossy(&out.stdout))?.enrolled);
    Ok(())
}

#[test]
fn prepare_and_finalize_install_an_owner_only_binary_that_runs() -> Outcome {
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir()?;
    let dir = RemoteDir::Home(".horde/bin".to_owned());
    let prepared = run_sh(&prepare_script(&dir), home.path(), None, "");
    assert!(prepared.status.success());
    let bin = home.path().join(".horde/bin");
    assert_eq!(std::fs::metadata(&bin)?.permissions().mode() & 0o777, 0o700);

    std::fs::write(
        bin.join(staged_name()),
        "#!/bin/sh\necho 'horde-worker 9.9'\n",
    )?;
    let finalized = run_sh(&finalize_script(&dir), home.path(), None, "");
    assert!(finalized.status.success(), "{finalized:?}");
    assert_eq!(
        String::from_utf8_lossy(&finalized.stdout),
        "horde-worker 9.9\n"
    );
    let installed = bin.join(WORKER_BINARY);
    assert_eq!(
        std::fs::metadata(&installed)?.permissions().mode() & 0o777,
        0o700
    );
    assert!(!bin.join(staged_name()).exists(), "the staged copy moved");

    // A binary that cannot run fails the install step.
    std::fs::write(bin.join(staged_name()), "#!/bin/sh\nexit 1\n")?;
    let refused = run_sh(&finalize_script(&dir), home.path(), None, "");
    assert!(!refused.status.success());
    Ok(())
}

fn fake_worker(bin: &Path, record: &Path, then: &str) {
    write_executable(
        &bin.join(WORKER_BINARY),
        &format!(
            "#!/bin/sh\n\
             echo \"$@\" > {record}/args\n\
             if [ \"$1\" = --join-file ]; then\n\
             \x20 cat \"$2\" > {record}/token\n\
             \x20 stat -c %a \"$2\" > {record}/mode 2>/dev/null || stat -f %Lp \"$2\" > {record}/mode\n\
             fi\n\
             echo 'worker says hello'\n\
             {then}\n",
            record = record.display()
        ),
    );
}

fn joins(home: &Path) -> String {
    format!(
        "mkdir -p {dir} && echo pem > {dir}/identity.pem",
        dir = home.join(".horde/worker/identity").display()
    )
}

#[test]
fn the_foreground_start_joins_with_the_token_off_argv_and_removes_it() -> Outcome {
    let home = tempfile::tempdir()?;
    let record = tempfile::tempdir()?;
    let dir = RemoteDir::Home(".horde/bin".to_owned());
    let bin = home.path().join(".horde/bin");
    std::fs::create_dir_all(&bin)?;
    fake_worker(&bin, record.path(), &joins(home.path()));
    let adapters = ["fake".to_owned(), "claude".to_owned()];
    let script = start_script(&dir, &adapters, &StartMode::Foreground, 10);
    let out = run_sh(&script, home.path(), None, "horde://join/h:1/s.f\n");
    let started = parse_started(&String::from_utf8_lossy(&out.stdout));
    assert!(out.status.success(), "{out:?}");
    assert!(started.joined, "{started:?}");
    assert!(started.pid.is_some_and(|pid| pid > 1), "{started:?}");

    let args = std::fs::read_to_string(record.path().join("args"))?;
    let token_path = home.path().join(STATE_ROOT).join(TOKEN_FILE);
    assert_eq!(
        args.trim(),
        format!(
            "--join-file {} --adapter fake --adapter claude",
            token_path.display()
        )
    );
    assert!(!args.contains("s.f"), "the token is never on argv");
    assert_eq!(
        std::fs::read_to_string(record.path().join("token"))?,
        "horde://join/h:1/s.f\n"
    );
    assert_eq!(
        std::fs::read_to_string(record.path().join("mode"))?.trim(),
        "600"
    );
    assert!(!token_path.exists(), "the token file is removed");
    let log = std::fs::read_to_string(home.path().join(STATE_ROOT).join(LOG_FILE))?;
    assert!(log.contains("worker says hello"), "{log}");
    let launcher = home.path().join(".horde/bin").join(LAUNCHER);
    assert_eq!(
        std::fs::read_to_string(&launcher)?,
        launcher_script(&adapters)
    );
    Ok(())
}

fn foreground_start_of(then: &str, wait_secs: u64) -> (tempfile::TempDir, std::process::Output) {
    let home = tempfile::tempdir().expect("a home");
    let record = tempfile::tempdir().expect("a record dir");
    let dir = RemoteDir::Home("b".to_owned());
    std::fs::create_dir_all(home.path().join("b")).expect("the bin dir");
    fake_worker(&home.path().join("b"), record.path(), then);
    let script = start_script(
        &dir,
        &["fake".to_owned()],
        &StartMode::Foreground,
        wait_secs,
    );
    let out = run_sh(&script, home.path(), None, "tok\n");
    (home, out)
}

#[test]
fn a_worker_that_dies_before_joining_ends_the_wait_with_its_log() -> Outcome {
    let (home, out) = foreground_start_of("exit 1", 20);
    assert_eq!(out.status.code(), Some(3), "{out:?}");
    let started = parse_started(&String::from_utf8_lossy(&out.stdout));
    assert!(!started.joined);
    assert_eq!(started.failure.as_deref(), Some("exited"));
    assert!(
        started.log.iter().any(|line| line == "worker says hello"),
        "{started:?}"
    );
    assert!(!home.path().join(STATE_ROOT).join(TOKEN_FILE).exists());
    Ok(())
}

#[test]
fn a_join_that_never_lands_times_out_and_still_removes_the_token() -> Outcome {
    let (home, out) = foreground_start_of("sleep 3", 1);
    assert_eq!(out.status.code(), Some(4), "{out:?}");
    let started = parse_started(&String::from_utf8_lossy(&out.stdout));
    assert_eq!(started.failure.as_deref(), Some("timeout"));
    assert!(!home.path().join(STATE_ROOT).join(TOKEN_FILE).exists());
    Ok(())
}

#[test]
fn the_launcher_joins_once_and_then_only_starts() -> Outcome {
    let home = tempfile::tempdir()?;
    let record = tempfile::tempdir()?;
    let bin = home.path().join("bin");
    std::fs::create_dir_all(&bin)?;
    fake_worker(&bin, record.path(), "true");
    let launcher = bin.join(LAUNCHER);
    write_executable(&launcher, &launcher_script(&["codex".to_owned()]));
    let token = home.path().join(STATE_ROOT).join(TOKEN_FILE);
    std::fs::create_dir_all(home.path().join(STATE_ROOT))?;
    std::fs::write(&token, "tok\n")?;
    let run = || run_sh(&launcher.display().to_string(), home.path(), None, "");

    assert!(run().status.success());
    let args = std::fs::read_to_string(record.path().join("args"))?;
    assert_eq!(
        args.trim(),
        format!("--join-file {} --adapter codex", token.display())
    );

    // Enrolled now: a lingering token is not redeemed again.
    let marker = home.path().join(IDENTITY_MARKER);
    std::fs::create_dir_all(marker.parent().ok_or("a parent")?)?;
    std::fs::write(&marker, "pem")?;
    assert!(run().status.success());
    let args = std::fs::read_to_string(record.path().join("args"))?;
    assert_eq!(args.trim(), "--adapter codex");
    Ok(())
}

#[test]
fn the_harness_report_finds_versions_on_the_workers_path() -> Outcome {
    let home = tempfile::tempdir()?;
    let bin = home.path().join("bin");
    std::fs::create_dir_all(&bin)?;
    write_executable(
        &bin.join("claude"),
        "#!/bin/sh\necho '2.1.322 (Claude Code)'\necho second\n",
    );
    let path = format!("{}:/usr/bin:/bin", bin.display());
    let out = run_sh(&harness_script(), home.path(), Some(&path), "");
    let harnesses = parse_harnesses(&String::from_utf8_lossy(&out.stdout));
    assert_eq!(
        harnesses,
        [
            Harness {
                name: "claude".to_owned(),
                path: Some(bin.join("claude").display().to_string()),
                version: Some("2.1.322 (Claude Code)".to_owned()),
            },
            Harness {
                name: "codex".to_owned(),
                path: None,
                version: None,
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn execute_returns_stdout_and_feeds_stdin() -> Outcome {
    let cat = Invocation {
        program: "sh".to_owned(),
        args: vec!["-c".to_owned(), "cat; echo done".to_owned()],
    };
    let out =
        horde_cli::worker_add::execute("echo", &cat, Some("in\n"), Duration::from_secs(10)).await?;
    assert_eq!(out, "in\ndone\n");
    let quiet = horde_cli::worker_add::execute("echo", &cat, None, Duration::from_secs(10)).await?;
    assert_eq!(quiet, "done\n", "no stdin is an empty stdin");
    Ok(())
}

#[tokio::test]
async fn execute_refuses_a_failing_command_with_what_it_said() {
    let failing = Invocation {
        program: "sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            "echo said; echo oops >&2; exit 255".to_owned(),
        ],
    };
    let error = horde_cli::worker_add::execute("probe", &failing, None, Duration::from_secs(10))
        .await
        .expect_err("a failing command is refused");
    let text = error.to_string();
    assert!(text.contains("probe failed (exit 255)"), "{text}");
    assert!(text.contains("ssh could not connect"), "{text}");
    assert!(text.contains("said\noops"), "{text}");
}

#[tokio::test]
async fn execute_refuses_a_command_that_outlives_its_limit() {
    let hanging = Invocation {
        program: "sleep".to_owned(),
        args: vec!["30".to_owned()],
    };
    let error = horde_cli::worker_add::execute("start", &hanging, None, Duration::from_millis(200))
        .await
        .expect_err("a hang is refused");
    assert!(error.to_string().contains("start: no answer within 200ms"));
}

#[tokio::test]
async fn execute_refuses_a_program_that_does_not_exist() {
    let missing = Invocation {
        program: "/nonexistent/ssh".to_owned(),
        args: Vec::new(),
    };
    let error = horde_cli::worker_add::execute("probe", &missing, None, Duration::from_secs(1))
        .await
        .expect_err("a missing program is refused");
    assert!(error.to_string().contains("could not run /nonexistent/ssh"));
}
