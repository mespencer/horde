//! The plan's values, and its scripts run for real: each remote script is
//! executed here under `sh` with a throwaway `$HOME`, so what is asserted
//! is what a remote shell would do, not what the text looks like.

use super::*;

type Outcome = Result<(), Box<dyn std::error::Error>>;

const LINUX_X86: Platform = Platform {
    os: Os::Linux,
    arch: Arch::X86_64,
};
const MAC_ARM: Platform = Platform {
    os: Os::Darwin,
    arch: Arch::Aarch64,
};

/// Runs `script` under `sh -c` with `home` as `$HOME`, `stdin` piped in.
#[test]
fn uname_answers_map_to_platforms_and_triples() {
    let cases = [
        ("Linux x86_64", Some(LINUX_X86), "x86_64-unknown-linux-gnu"),
        (
            "Linux aarch64",
            Some(Platform {
                os: Os::Linux,
                arch: Arch::Aarch64,
            }),
            "aarch64-unknown-linux-gnu",
        ),
        ("Darwin arm64", Some(MAC_ARM), "aarch64-apple-darwin"),
        (
            "Darwin x86_64",
            Some(Platform {
                os: Os::Darwin,
                arch: Arch::X86_64,
            }),
            "x86_64-apple-darwin",
        ),
    ];
    for (uname, platform, triple) in cases {
        assert_eq!(Platform::from_uname(uname), platform, "{uname}");
        assert_eq!(platform.map(Platform::triple), Some(triple), "{uname}");
    }
    for refused in [
        "FreeBSD amd64",
        "Linux armv7l",
        "Linux",
        "",
        "Linux x86_64 extra",
    ] {
        assert_eq!(Platform::from_uname(refused), None, "{refused:?}");
    }
    assert_eq!(Platform::from_uname("Linux amd64"), Some(LINUX_X86));
}

#[test]
fn rust_consts_map_to_platforms() {
    assert_eq!(Platform::from_consts("linux", "x86_64"), Some(LINUX_X86));
    assert_eq!(Platform::from_consts("macos", "aarch64"), Some(MAC_ARM));
    assert_eq!(Platform::from_consts("windows", "x86_64"), None);
    assert_eq!(Platform::from_consts("linux", "riscv64"), None);
    assert_eq!(LINUX_X86.to_string(), "linux x86_64");
    assert_eq!(MAC_ARM.to_string(), "darwin aarch64");
}

/// An ELF header with the given class, data and machine bytes.
fn elf(class: u8, data: u8, machine: u16) -> Vec<u8> {
    let mut header = vec![0_u8; 64];
    header[..4].copy_from_slice(&[0x7f, b'E', b'L', b'F']);
    header[4] = class;
    header[5] = data;
    header[18..20].copy_from_slice(&machine.to_le_bytes());
    header
}

fn macho(cpu: u32) -> Vec<u8> {
    let mut header = vec![0_u8; 32];
    header[..4].copy_from_slice(&[0xcf, 0xfa, 0xed, 0xfe]);
    header[4..8].copy_from_slice(&cpu.to_le_bytes());
    header
}

#[test]
fn binary_headers_name_their_platform() {
    assert_eq!(
        binary_kind(&elf(2, 1, 0x3e)),
        Some(BinaryKind::Native(LINUX_X86))
    );
    assert_eq!(
        binary_kind(&elf(2, 1, 0xb7)),
        Some(BinaryKind::Native(Platform {
            os: Os::Linux,
            arch: Arch::Aarch64
        }))
    );
    assert_eq!(binary_kind(&elf(1, 1, 0x3e)), None, "32-bit is refused");
    assert_eq!(binary_kind(&elf(2, 2, 0x3e)), None, "big-endian is refused");
    assert_eq!(binary_kind(&elf(2, 1, 0x28)), None, "arm32 is refused");
    assert_eq!(
        binary_kind(&macho(0x0100_000c)),
        Some(BinaryKind::Native(MAC_ARM))
    );
    assert_eq!(
        binary_kind(&macho(0x0100_0007)),
        Some(BinaryKind::Native(Platform {
            os: Os::Darwin,
            arch: Arch::X86_64
        }))
    );
    assert_eq!(binary_kind(&macho(0x0000_0007)), None);
    assert_eq!(
        binary_kind(&[0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 2]),
        Some(BinaryKind::UniversalDarwin)
    );
    assert_eq!(binary_kind(b"#!/bin/sh\n"), None);
    assert_eq!(binary_kind(&[0x7f, b'E']), None, "truncated");
    assert_eq!(
        binary_kind(&elf(2, 1, 0x3e)[..19]),
        None,
        "truncated machine"
    );
}

#[test]
fn this_test_binary_is_read_as_this_platform() -> Outcome {
    let me = std::env::current_exe()?;
    let header = std::fs::read(&me)?;
    let kind = binary_kind(&header).ok_or("this test binary has a known header")?;
    let here = Platform::from_consts(std::env::consts::OS, std::env::consts::ARCH)
        .ok_or("a supported platform")?;
    assert!(kind.runs_on(here), "{kind:?} on {here}");
    Ok(())
}

#[test]
fn a_binary_runs_only_where_it_was_built_for() {
    assert!(BinaryKind::Native(LINUX_X86).runs_on(LINUX_X86));
    assert!(!BinaryKind::Native(LINUX_X86).runs_on(MAC_ARM));
    assert!(BinaryKind::UniversalDarwin.runs_on(MAC_ARM));
    assert!(!BinaryKind::UniversalDarwin.runs_on(LINUX_X86));
    assert_eq!(BinaryKind::Native(MAC_ARM).describe(), "darwin aarch64");
    assert_eq!(BinaryKind::UniversalDarwin.describe(), "darwin universal");
}

#[test]
fn targets_are_ssh_destinations_never_options() {
    for good in [
        "me@box.lan",
        "box",
        "gpu-box_2",
        "me@10.0.0.4",
        "me@[fe80::1]",
        "a.b-c@d",
    ] {
        assert!(is_valid_target(good), "{good}");
    }
    for bad in [
        "-oProxyCommand=sh",
        "me@-oProxyCommand=x",
        "",
        "me@",
        "me box",
        "me@box;rm",
        "m$e@box",
        "me@box/x",
    ] {
        assert!(!is_valid_target(bad), "{bad:?}");
    }
}

#[test]
fn remote_dirs_parse_home_relative_or_absolute() {
    assert_eq!(
        RemoteDir::parse(".horde/bin"),
        Ok(RemoteDir::Home(".horde/bin".to_owned()))
    );
    assert_eq!(
        RemoteDir::parse("~/bin/"),
        Ok(RemoteDir::Home("bin".to_owned()))
    );
    assert_eq!(
        RemoteDir::parse("/opt/me/horde"),
        Ok(RemoteDir::Absolute("/opt/me/horde".to_owned()))
    );
    for bad in ["", "/", "a b", "x;y", "$HOME/x", "a~b", "'q'"] {
        assert!(RemoteDir::parse(bad).is_err(), "{bad:?}");
    }
    let home = RemoteDir::Home(".horde/bin".to_owned());
    assert_eq!(home.shell(), "\"$HOME\"/.horde/bin");
    assert_eq!(home.scp_path(), ".horde/bin");
    assert_eq!(home.absolute("/home/me/"), "/home/me/.horde/bin");
    let absolute = RemoteDir::Absolute("/opt/h".to_owned());
    assert_eq!(absolute.shell(), "/opt/h");
    assert_eq!(absolute.scp_path(), "/opt/h");
    assert_eq!(absolute.absolute("/home/me"), "/opt/h");
}

#[test]
fn ssh_and_scp_invocations_carry_config_and_target() {
    let remote = Remote {
        ssh: "ssh".to_owned(),
        scp: "scp".to_owned(),
        config: Some(PathBuf::from("/tmp/cfg")),
        target: "me@box".to_owned(),
    };
    let ssh = remote.ssh("echo 'hi'");
    assert_eq!(ssh.program, "ssh");
    assert_eq!(
        ssh.args,
        ["-F", "/tmp/cfg", "-T", "me@box", r"sh -c 'echo '\''hi'\'''"]
    );
    let scp = remote.scp(Path::new("/b/horde-worker"), ".horde/bin/x");
    assert_eq!(
        scp.args,
        [
            "-F",
            "/tmp/cfg",
            "-q",
            "/b/horde-worker",
            "me@box:.horde/bin/x"
        ]
    );
    let bare = Remote {
        config: None,
        ..remote
    };
    assert_eq!(bare.ssh("true").args, ["-T", "me@box", "sh -c 'true'"]);
    assert_eq!(
        bare.ssh("true").display(),
        "ssh -T me@box 'sh -c '\\''true'\\'''"
    );
}

/// The remote command an ssh step sends is exactly what a remote login
/// shell turns back into the script: the last argument, run by `sh -c`
/// as a shell would parse it, prints the script's output.
#[test]
fn a_probe_missing_a_line_is_refused() {
    let refused = parse_probe("uname Linux x86_64\nhome /h\npath /bin\n");
    assert!(
        refused.is_err_and(|error| error.contains("`enrolled`")),
        "the refusal names the missing line"
    );
    assert!(parse_probe("").is_err());
    assert!(
        parse_probe("uname Linux x86_64\nhome /h\npath /bin\nenrolled no\n")
            .is_ok_and(|probe| !probe.enrolled)
    );
    assert!(
        parse_probe("unamex\nuname L x\nhome /h\npath /p\nenrolled yes\n")
            .is_ok_and(|probe| probe.uname == "L x" && probe.enrolled)
    );
}

#[test]
fn start_output_parses_into_its_report() {
    let started = parse_started("pid 42\nlinger no\njoined\n");
    assert_eq!(
        started,
        Started {
            pid: Some(42),
            linger: Some(false),
            joined: true,
            failure: None,
            log: Vec::new(),
        }
    );
    let failed = parse_started("linger yes\nexited\nlog boom\nlog  two\n");
    assert_eq!(failed.linger, Some(true));
    assert!(!failed.joined);
    assert_eq!(failed.failure.as_deref(), Some("exited"));
    assert_eq!(failed.log, ["boom", " two"]);
    assert_eq!(
        parse_started("timeout\n").failure.as_deref(),
        Some("timeout")
    );
    assert_eq!(parse_started("pid x\n").pid, None);
    assert_eq!(parse_started("noise\n"), Started::default());
}

#[test]
fn the_systemd_unit_runs_the_launcher_on_the_probed_path() {
    let unit = service_unit(
        Os::Linux,
        "/home/me/.horde/bin/horde-worker-start",
        "/home/me/.local/bin:/usr/bin",
        "/home/me/.horde/worker.log",
    );
    assert_eq!(unit.os, Os::Linux);
    assert!(
        unit.content
            .contains("ExecStart=\"/home/me/.horde/bin/horde-worker-start\"\n"),
        "{}",
        unit.content
    );
    assert!(
        unit.content
            .contains("Environment=\"PATH=/home/me/.local/bin:/usr/bin\"\n")
    );
    assert!(unit.content.contains("WantedBy=default.target"));
    assert!(unit.content.contains("Restart=on-failure"));
    assert_eq!(
        systemd_quote(r#"a "b" 50% $X \y"#),
        r#""a \"b\" 50%% $$X \\y""#
    );
}

#[test]
fn the_launchd_agent_runs_the_launcher_on_the_probed_path() {
    let unit = service_unit(
        Os::Darwin,
        "/Users/me/.horde/bin/horde-worker-start",
        "/opt/homebrew/bin:/usr/bin",
        "/Users/me/.horde/worker.log",
    );
    assert_eq!(unit.os, Os::Darwin);
    for expected in [
        "<key>Label</key><string>dev.horde.worker</string>",
        "<array><string>/Users/me/.horde/bin/horde-worker-start</string></array>",
        "<key>PATH</key><string>/opt/homebrew/bin:/usr/bin</string>",
        "<key>StandardOutPath</key><string>/Users/me/.horde/worker.log</string>",
        "<key>RunAtLoad</key><true/>",
    ] {
        assert!(
            unit.content.contains(expected),
            "{expected}\n{}",
            unit.content
        );
    }
    assert_eq!(
        xml_escape(r#"<a & 'b' "c">"#),
        "&lt;a &amp; &apos;b&apos; &quot;c&quot;&gt;"
    );
}

#[test]
fn service_starts_install_the_unit_and_skip_the_pid_wait() {
    let linux = StartMode::Service(service_unit(Os::Linux, "/l", "/p", "/log"));
    let script = start_script(&RemoteDir::Home("b".to_owned()), &[], &linux, 7);
    for expected in [
        "cat > \"$dir\"/horde-worker.service <<'HORDE_WORKER_ADD_EOF'\n[Unit]",
        "systemctl --user enable horde-worker.service\n",
        "systemctl --user restart horde-worker.service\n",
        "Linger=yes) echo 'linger yes'",
        "journalctl --user -u horde-worker.service",
        "-gt 7 ]",
    ] {
        assert!(script.contains(expected), "{expected}\n{script}");
    }
    assert!(!script.contains("kill -0"), "no pid to watch: {script}");
    assert!(!script.contains("nohup"));

    let mac = StartMode::Service(service_unit(Os::Darwin, "/l", "/p", "/log"));
    let script = start_script(&RemoteDir::Home("b".to_owned()), &[], &mac, 7);
    for expected in [
        "plist=\"$dir\"/dev.horde.worker.plist\n",
        "launchctl bootstrap \"gui/$uid\" \"$plist\"\n",
        "<key>Label</key>",
        "tail -n 20 \"$state\"/worker.log",
    ] {
        assert!(script.contains(expected), "{expected}\n{script}");
    }
    assert!(!script.contains("systemctl"));
}

#[test]
fn harness_lines_parse_tolerantly() {
    assert_eq!(
        parse_harnesses("harness\tx\t/p\t\nnoise\nharness\t\t\t\n"),
        [Harness {
            name: "x".to_owned(),
            path: Some("/p".to_owned()),
            version: None,
        }]
    );
}

#[test]
fn display_quotes_only_what_needs_it() {
    let invocation = Invocation {
        program: "scp".to_owned(),
        args: vec!["-q".to_owned(), "/a b".to_owned(), "me@h:x".to_owned()],
    };
    assert_eq!(invocation.display(), "scp -q '/a b' me@h:x");
}
