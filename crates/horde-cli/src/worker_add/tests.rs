//! `horde worker add`'s sequencing, refusals and renders. The flow against
//! a real server and a real worker binary lives in the e2e suite
//! (`horde-e2e`, `worker_add.rs`); these pin the decisions it is built on.

use std::time::Duration;

use clap::Parser;

use super::*;
use crate::cli::{Cli, Command, WorkerCommand};

type Outcome = Result<(), Box<dyn std::error::Error>>;

const LINUX_X86: Platform = Platform {
    os: plan::Os::Linux,
    arch: plan::Arch::X86_64,
};
const MAC_ARM: Platform = Platform {
    os: plan::Os::Darwin,
    arch: plan::Arch::Aarch64,
};

/// The `AddArgs` the real parser builds from `words`.
fn parsed(words: &[&str]) -> Result<AddArgs, Box<dyn std::error::Error>> {
    let cli = Cli::try_parse_from(
        ["horde", "worker", "add"]
            .into_iter()
            .chain(words.iter().copied()),
    )?;
    if let Some(Command::Worker(WorkerCommand::Add(args))) = cli.command {
        return Ok(args);
    }
    Err("not worker add".into())
}

fn message(error: &ClientError) -> String {
    error.to_string()
}

fn context(platform: Platform) -> Context {
    Context {
        platform,
        home: "/home/me".to_owned(),
        path: "/usr/bin:/bin".to_owned(),
    }
}

#[test]
fn defaults_are_claude_home_bin_and_a_minute() -> Outcome {
    let args = parsed(&["gpu", "me@box"])?;
    assert_eq!(args.adapters, [AdapterArg::Claude]);
    assert_eq!(args.remote_dir, ".horde/bin");
    assert_eq!(args.timeout_secs, 60);
    assert_eq!(args.ssh, "ssh");
    assert_eq!(args.scp, "scp");
    assert!(!args.service && !args.dry_run && !args.build_locally && !args.from_release);
    let checked = checked(&args)?;
    assert_eq!(checked.adapters, ["claude"]);
    assert_eq!(checked.dir, RemoteDir::Home(".horde/bin".to_owned()));
    assert_eq!(checked.wait_secs, 60);
    assert_eq!(checked.remote.target, "me@box");
    assert_eq!(checked.remote.config, None);
    Ok(())
}

#[test]
fn flags_reach_the_checked_plan() -> Outcome {
    let args = parsed(&[
        "gpu",
        "me@box",
        "--adapter",
        "codex",
        "--adapter",
        "fake",
        "--remote-dir",
        "/opt/h",
        "--service",
        "--timeout-secs",
        "5",
        "--ssh",
        "my-ssh",
        "--scp",
        "my-scp",
        "--ssh-config",
        "/c",
        "--ttl-minutes",
        "3",
    ])?;
    let checked = checked(&args)?;
    assert_eq!(checked.adapters, ["codex", "fake"]);
    assert_eq!(checked.dir, RemoteDir::Absolute("/opt/h".to_owned()));
    assert!(checked.service);
    assert_eq!(checked.wait_secs, 5);
    assert_eq!(checked.remote.ssh, "my-ssh");
    assert_eq!(checked.remote.scp, "my-scp");
    assert_eq!(checked.remote.config, Some(PathBuf::from("/c")));
    assert_eq!(args.ttl_minutes, Some(3));
    assert_eq!(checked.name, "gpu");
    Ok(())
}

#[test]
fn contradictory_or_out_of_range_flags_fail_at_parse() {
    for words in [
        &["gpu", "me@box", "--from-release", "--build-locally"][..],
        &["gpu", "me@box", "--binary-dir", "/b", "--build-locally"],
        &["gpu", "me@box", "--binary-dir", "/b", "--from-release"],
        &["gpu", "me@box", "--timeout-secs", "0"],
        &["gpu", "me@box", "--timeout-secs", "3601"],
        &["gpu", "me@box", "--ttl-minutes", "0"],
        &["gpu", "me@box", "--adapter", "gemini"],
        &["gpu"],
    ] {
        assert!(parsed(words).is_err(), "{words:?}");
    }
}

#[test]
fn bad_names_targets_dirs_and_releases_are_refused_before_any_connection() -> Outcome {
    let refusal_for = |words: &[&str]| -> Result<String, Box<dyn std::error::Error>> {
        let error = checked(&parsed(words)?).err().ok_or("refused")?;
        assert_eq!(error.kind(), "worker_add");
        Ok(message(&error))
    };
    assert!(refusal_for(&["bad name", "me@box"])?.contains("cannot be a worker name"));
    assert!(refusal_for(&["gpu", "me@box;x"])?.contains("not an ssh destination"));
    assert!(refusal_for(&["gpu", "me@box", "--remote-dir", "a b"])?.contains("--remote-dir"));
    assert!(refusal_for(&["gpu", "me@box", "--from-release"])?.contains("no Horde release"));
    Ok(())
}

#[test]
fn the_binary_comes_from_the_flag_the_exe_dir_or_a_build() -> Outcome {
    let here = Some(PathBuf::from("/here"));
    let source = Source::of(&parsed(&["g", "h"])?, here.clone(), None)?;
    assert_eq!(source, Source::Dir(PathBuf::from("/here")));
    assert_eq!(
        source.binary(LINUX_X86),
        PathBuf::from("/here/horde-worker")
    );
    assert_eq!(source.build(LINUX_X86), None);

    let flagged = Source::of(
        &parsed(&["g", "h", "--binary-dir", "/b"])?,
        here.clone(),
        None,
    )?;
    assert_eq!(flagged, Source::Dir(PathBuf::from("/b")));

    let built = Source::of(&parsed(&["g", "h", "--build-locally"])?, here.clone(), None)?;
    assert_eq!(
        built,
        Source::Build {
            target_dir: PathBuf::from("target")
        }
    );
    assert_eq!(
        built.binary(MAC_ARM),
        PathBuf::from("target/aarch64-apple-darwin/release/horde-worker")
    );
    let build = built.build(MAC_ARM).ok_or("a build")?;
    assert_eq!(
        build.display(),
        "cargo build --release -p horde-worker --bin horde-worker --target aarch64-apple-darwin"
    );
    let elsewhere = Source::of(
        &parsed(&["g", "h", "--build-locally"])?,
        here,
        Some(PathBuf::from("/t")),
    )?;
    assert_eq!(
        elsewhere,
        Source::Build {
            target_dir: PathBuf::from("/t")
        }
    );
    assert!(Source::of(&parsed(&["g", "h"])?, None, None).is_err());
    Ok(())
}

#[test]
fn the_steps_run_in_order_and_only_start_reads_stdin() -> Outcome {
    let checked = checked(&parsed(&["gpu", "me@box"])?)?;
    let steps = later_steps(&checked, &context(LINUX_X86), Path::new("/b/horde-worker"));
    let names: Vec<_> = steps.iter().map(|step| step.name).collect();
    assert_eq!(names, ["prepare", "copy", "install", "start", "harnesses"]);
    for step in &steps {
        let expected = (step.name == "start").then_some("join token");
        assert_eq!(step.stdin, expected, "{}", step.name);
    }
    assert_eq!(
        steps[1].invocation.args,
        [
            "-q",
            "/b/horde-worker",
            "me@box:.horde/bin/horde-worker.new"
        ]
    );
    assert_eq!(steps[1].script, None);
    assert_eq!(
        steps[0].script.as_deref(),
        Some(plan::prepare_script(&checked.dir).as_str())
    );
    assert_eq!(
        steps[3].script.as_deref(),
        Some(
            plan::start_script(&checked.dir, &checked.adapters, &StartMode::Foreground, 60)
                .as_str()
        )
    );
    assert_eq!(
        probe_step(&checked).script.as_deref(),
        Some(plan::probe_script().as_str())
    );
    assert_eq!(probe_step(&checked).name, "probe");
    Ok(())
}

#[test]
fn service_mode_picks_the_unit_for_the_remote_os() -> Outcome {
    let foreground = checked(&parsed(&["gpu", "me@box"])?)?;
    assert_eq!(
        start_mode(&foreground, &context(LINUX_X86)),
        StartMode::Foreground
    );
    assert_eq!(mode_word(&StartMode::Foreground), "foreground");

    let service = checked(&parsed(&["gpu", "me@box", "--service"])?)?;
    let linux = start_mode(&service, &context(LINUX_X86));
    assert_eq!(
        linux,
        StartMode::Service(plan::service_unit(
            plan::Os::Linux,
            "/home/me/.horde/bin/horde-worker-start",
            "/usr/bin:/bin",
            "/home/me/.horde/worker.log",
        ))
    );
    assert_eq!(mode_word(&linux), "systemd");
    let mac = start_mode(&service, &context(MAC_ARM));
    assert_eq!(
        mac,
        StartMode::Service(plan::service_unit(
            plan::Os::Darwin,
            "/home/me/.horde/bin/horde-worker-start",
            "/usr/bin:/bin",
            "/home/me/.horde/worker.log",
        ))
    );
    assert_eq!(mode_word(&mac), "launchd");
    Ok(())
}

#[test]
fn the_probe_refuses_unknown_platforms_and_enrolled_machines() -> Outcome {
    let probe = |uname: &str, enrolled| Probe {
        uname: uname.to_owned(),
        home: "/h".to_owned(),
        path: "/p".to_owned(),
        enrolled,
    };
    assert_eq!(
        context_of("box", &probe("Darwin arm64", false))?,
        Context {
            platform: MAC_ARM,
            home: "/h".to_owned(),
            path: "/p".to_owned(),
        }
    );
    let unknown = context_of("box", &probe("SunOS i86pc", false))
        .err()
        .ok_or("refused")?;
    assert!(message(&unknown).contains("reports `SunOS i86pc`"));
    let enrolled = context_of("box", &probe("Linux x86_64", true))
        .err()
        .ok_or("refused")?;
    assert!(message(&enrolled).contains("already holds a worker identity"));
    Ok(())
}

#[test]
fn a_binary_for_another_platform_is_refused_with_how_to_provide_one() -> Outcome {
    let me = std::env::current_exe()?;
    let header = std::fs::read(&me)?;
    let here =
        Platform::from_consts(std::env::consts::OS, std::env::consts::ARCH).ok_or("supported")?;
    check_binary(&me, &header, "box", here)?;
    let elsewhere = if here == LINUX_X86 {
        MAC_ARM
    } else {
        LINUX_X86
    };
    let refused = check_binary(&me, &header, "box", elsewhere)
        .err()
        .ok_or("refused")?;
    let text = message(&refused);
    assert!(text.contains(&format!("box is {elsewhere}")), "{text}");
    assert!(text.contains(elsewhere.triple()), "{text}");
    assert!(text.contains("--binary-dir"), "{text}");
    let script = check_binary(Path::new("/s"), b"#!/bin/sh", "box", here)
        .err()
        .ok_or("refused")?;
    assert!(message(&script).contains("not a 64-bit Linux or macOS executable"));
    Ok(())
}

#[test]
fn header_reads_at_most_the_first_bytes() -> Outcome {
    let dir = tempfile::tempdir()?;
    let short = dir.path().join("short");
    std::fs::write(&short, b"abc")?;
    assert_eq!(header_of(&short)?, b"abc");
    let long = dir.path().join("long");
    std::fs::write(&long, vec![7_u8; 1000])?;
    assert_eq!(header_of(&long)?.len(), 64);
    assert!(matches!(
        header_of(&dir.path().join("absent")),
        Err(ClientError::File { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn execute_returns_stdout_and_feeds_stdin() -> Outcome {
    let cat = Invocation {
        program: "sh".to_owned(),
        args: vec!["-c".to_owned(), "cat; echo done".to_owned()],
    };
    let out = execute("echo", &cat, Some("in\n"), Duration::from_secs(10)).await?;
    assert_eq!(out, "in\ndone\n");
    let quiet = execute("echo", &cat, None, Duration::from_secs(10)).await?;
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
    let error = execute("probe", &failing, None, Duration::from_secs(10))
        .await
        .expect_err("a failing command is refused");
    let text = message(&error);
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
    let error = execute("start", &hanging, None, Duration::from_millis(200))
        .await
        .expect_err("a hang is refused");
    assert!(message(&error).contains("start: no answer within 200ms"));
}

#[tokio::test]
async fn execute_refuses_a_program_that_does_not_exist() {
    let missing = Invocation {
        program: "/nonexistent/ssh".to_owned(),
        args: Vec::new(),
    };
    let error = execute("probe", &missing, None, Duration::from_secs(1))
        .await
        .expect_err("a missing program is refused");
    assert!(message(&error).contains("could not run /nonexistent/ssh"));
}

#[test]
fn step_failures_name_the_step_and_only_blame_ssh_for_255() {
    let other = message(&step_failure("copy", Some(1), "", "no space\n"));
    assert_eq!(other, "worker add: copy failed (exit 1): no space");
    let signalled = message(&step_failure("copy", None, "out", ""));
    assert_eq!(signalled, "worker add: copy failed (a signal): out");
    let ssh = message(&step_failure("probe", Some(255), "", "refused"));
    assert!(
        ssh.contains("check that `ssh <target> true` works"),
        "{ssh}"
    );
}

#[test]
fn join_failures_carry_the_log_tail() {
    assert_eq!(
        join_failure("pid 3\nexited\nlog bad token\nlog bye\n"),
        "the worker exited before its join was redeemed; its log ends:\nbad token\nbye"
    );
    assert_eq!(
        join_failure("timeout\n"),
        "the worker's join was not redeemed in time: timeout"
    );
    assert_eq!(
        join_failure("start failed (exit 1): x"),
        "the worker did not join: start failed (exit 1): x"
    );
}

#[test]
fn missing_harnesses_warn_only_for_requested_adapters() {
    let harnesses = [
        plan::Harness {
            name: "claude".to_owned(),
            path: None,
            version: None,
        },
        plan::Harness {
            name: "codex".to_owned(),
            path: None,
            version: None,
        },
        plan::Harness {
            name: "fake".to_owned(),
            path: Some("/f".to_owned()),
            version: None,
        },
    ];
    let warnings = harness_warnings(&["claude".to_owned(), "fake".to_owned()], &harnesses);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].starts_with("--adapter claude, but no claude"));
}

fn added() -> Added {
    Added {
        name: "gpu".to_owned(),
        target: "me@box".to_owned(),
        platform: LINUX_X86,
        binary: "/home/me/.horde/bin/horde-worker".to_owned(),
        version: "horde-worker 0.1.0".to_owned(),
        mode: "foreground",
        pid: Some(42),
        log: Some("/home/me/.horde/worker.log".to_owned()),
        harnesses: vec![
            plan::Harness {
                name: "claude".to_owned(),
                path: Some("/bin/claude".to_owned()),
                version: Some("2.1.322".to_owned()),
            },
            plan::Harness {
                name: "codex".to_owned(),
                path: Some("/bin/codex".to_owned()),
                version: None,
            },
            plan::Harness {
                name: "x".to_owned(),
                path: None,
                version: None,
            },
        ],
        adapters: vec!["claude".to_owned()],
        warnings: vec!["careful".to_owned()],
    }
}

#[test]
fn the_report_renders_for_a_terminal() {
    assert_eq!(
        render(&added(), Format::Human),
        "added worker=gpu target=me@box platform=linux x86_64\n\
         \x20 binary     /home/me/.horde/bin/horde-worker (horde-worker 0.1.0)\n\
         \x20 running    pid=42 log=/home/me/.horde/worker.log\n\
         \x20 registered gpu\n\
         \x20 harness    claude /bin/claude (2.1.322)\n\
         \x20 harness    codex /bin/codex\n\
         \x20 harness    x missing\n\
         warning: careful"
    );
    let service = Added {
        mode: "systemd",
        pid: None,
        log: None,
        ..added()
    };
    assert!(
        render(&service, Format::Human).contains("\n  running    systemd service\n"),
        "{}",
        render(&service, Format::Human)
    );
}

#[test]
fn the_report_renders_as_json() -> Outcome {
    let value: serde_json::Value = serde_json::from_str(&render(&added(), Format::Json))?;
    assert_eq!(
        value,
        serde_json::json!({
            "worker": "gpu",
            "target": "me@box",
            "platform": "linux x86_64",
            "triple": "x86_64-unknown-linux-gnu",
            "binary": "/home/me/.horde/bin/horde-worker",
            "version": "horde-worker 0.1.0",
            "mode": "foreground",
            "pid": 42,
            "log": "/home/me/.horde/worker.log",
            "registered": true,
            "harnesses": [
                {"name": "claude", "path": "/bin/claude", "version": "2.1.322", "required": true},
                {"name": "codex", "path": "/bin/codex", "version": null, "required": false},
                {"name": "x", "path": null, "version": null, "required": false},
            ],
            "warnings": ["careful"],
        })
    );
    Ok(())
}

#[test]
fn the_dry_run_plan_lists_every_command_in_order() -> Outcome {
    let checked = checked(&parsed(&["gpu", "me@box", "--build-locally"])?)?;
    let source = Source::Build {
        target_dir: PathBuf::from("target"),
    };
    let human = render_plan(&checked, &source, &context(LINUX_X86), Format::Human);
    let order: Vec<&str> = human
        .lines()
        .filter(|line| !line.starts_with("    "))
        .map(|line| line.split(':').next().unwrap_or_default())
        .collect();
    assert_eq!(
        order,
        [
            "dry run",
            "local  build",
            "remote probe",
            "remote prepare",
            "remote copy",
            "remote install",
            "client mint a worker join token for gpu through the server (horde worker enroll)",
            "remote start",
            "client wait up to 60s for gpu to register",
            "remote harnesses",
        ],
        "{human}"
    );
    assert!(human.contains("remote start: ssh -T me@box sh -c <<script   (stdin: the join token)"));
    assert!(
        human.contains("\n    umask 077\n"),
        "scripts print as written"
    );

    let value: serde_json::Value = serde_json::from_str(&render_plan(
        &checked,
        &source,
        &context(LINUX_X86),
        Format::Json,
    ))?;
    assert_eq!(value["dry_run"], true);
    assert_eq!(value["assumed_platform"], "linux x86_64");
    assert_eq!(value["build"][0], "cargo");
    let steps = value["steps"].as_array().ok_or("steps")?;
    assert_eq!(steps.len(), 6);
    assert_eq!(steps[0]["step"], "probe");
    assert_eq!(steps[0]["argv"][0], "ssh");
    assert_eq!(steps[4]["stdin"], "join token");
    assert_eq!(steps[1]["stdin"], serde_json::Value::Null);
    assert_eq!(value["client"].as_array().map(Vec::len), Some(2));
    Ok(())
}

#[test]
fn a_plain_step_displays_as_its_command_line() {
    let step = Step {
        name: "copy",
        invocation: Invocation {
            program: "scp".to_owned(),
            args: vec!["-q".to_owned(), "a".to_owned(), "h:b".to_owned()],
        },
        script: None,
        stdin: Some("x"),
    };
    assert_eq!(step.display(), "scp -q a h:b   (stdin: the x)");
}

#[test]
fn adapter_words_are_the_worker_flag_values() {
    assert_eq!(AdapterArg::Claude.word(), "claude");
    assert_eq!(AdapterArg::Codex.word(), "codex");
    assert_eq!(AdapterArg::Fake.word(), "fake");
}

#[test]
fn only_a_machine_known_not_to_linger_earns_the_linger_warning() {
    let started = |linger| plan::Started {
        linger,
        ..plan::Started::default()
    };
    let warning = linger_warning(&started(Some(false)), "me@box").unwrap_or_default();
    assert!(
        warning.contains("`loginctl enable-linger` on me@box"),
        "{warning}"
    );
    assert_eq!(linger_warning(&started(Some(true)), "me@box"), None);
    assert_eq!(linger_warning(&started(None), "me@box"), None);
}
