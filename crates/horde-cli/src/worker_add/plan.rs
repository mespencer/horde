//! The pure half of `horde worker add`: what to run on the remote machine
//! and how to read what it answers. Nothing here spawns a process, touches
//! the network, or reads the environment, so every command the verb runs is
//! a value a test can inspect and `--dry-run` can print.
//!
//! Remote commands go through the remote user's login shell, which may not
//! be POSIX (fish, zsh). Every script is therefore wrapped as
//! `sh -c '<script>'` with POSIX single-quote escaping, the one quoting form
//! every common login shell agrees on, and the script itself is plain sh.

use std::path::{Path, PathBuf};

/// The installed binary's file name inside the remote install dir.
pub const WORKER_BINARY: &str = "horde-worker";

/// The launcher script, beside the binary, that both start modes run.
pub const LAUNCHER: &str = "horde-worker-start";

/// The remote state root, relative to the remote `$HOME` (ADR-0007: all
/// state under `~/.horde`).
pub const STATE_ROOT: &str = ".horde";

/// The join-token file the start step writes and removes, under the state
/// root. Fixed rather than random because the launcher, which a service
/// manager may run long after this invocation ends, must find it.
pub const TOKEN_FILE: &str = "join-token";

/// The worker's log in foreground mode, under the state root.
pub const LOG_FILE: &str = "worker.log";

/// The identity file whose presence proves the join was redeemed, relative
/// to the remote `$HOME` (the worker's default state dir is
/// `~/.horde/worker`, its identity `identity/`).
pub const IDENTITY_MARKER: &str = ".horde/worker/identity/identity.pem";

/// The systemd user unit's name.
pub const SYSTEMD_UNIT: &str = "horde-worker.service";

/// The launchd agent's label (and plist base name).
pub const LAUNCHD_LABEL: &str = "dev.horde.worker";

/// The harnesses whose presence the verb reports, like `horde doctor`'s
/// default.
pub const HARNESSES: &[&str] = &["claude", "codex"];

/// An operating system the worker is built for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Os {
    Linux,
    Darwin,
}

impl Os {
    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Darwin => "darwin",
        }
    }
}

/// A CPU architecture the worker is built for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arch {
    X86_64,
    Aarch64,
}

impl Arch {
    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            Self::X86_64 => "x86_64",
            Self::Aarch64 => "aarch64",
        }
    }
}

/// One machine's os and architecture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Platform {
    pub os: Os,
    pub arch: Arch,
}

impl Platform {
    /// The Rust target triple a `horde-worker` for this platform is built
    /// for.
    #[must_use]
    pub fn triple(self) -> &'static str {
        match (self.os, self.arch) {
            (Os::Linux, Arch::X86_64) => "x86_64-unknown-linux-gnu",
            (Os::Linux, Arch::Aarch64) => "aarch64-unknown-linux-gnu",
            (Os::Darwin, Arch::X86_64) => "x86_64-apple-darwin",
            (Os::Darwin, Arch::Aarch64) => "aarch64-apple-darwin",
        }
    }

    /// The platform from Rust's own `std::env::consts` spelling (`OS`,
    /// `ARCH`): how this `horde` knows what it was built for.
    #[must_use]
    pub fn from_consts(os: &str, arch: &str) -> Option<Self> {
        let os = match os {
            "linux" => Os::Linux,
            "macos" => Os::Darwin,
            _ => return None,
        };
        Some(Self {
            os,
            arch: arch_of(arch)?,
        })
    }

    /// The platform `uname -sm` describes (`Linux x86_64`, `Darwin arm64`).
    #[must_use]
    pub fn from_uname(uname: &str) -> Option<Self> {
        let mut words = uname.split_whitespace();
        let os = match words.next()? {
            "Linux" => Os::Linux,
            "Darwin" => Os::Darwin,
            _ => return None,
        };
        let arch = arch_of(words.next()?)?;
        if words.next().is_some() {
            return None;
        }
        Some(Self { os, arch })
    }
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.os.word(), self.arch.word())
    }
}

/// Both spellings each architecture goes by (`uname -m` on Linux says
/// `aarch64`, on macOS `arm64`; Rust says `aarch64`).
fn arch_of(word: &str) -> Option<Arch> {
    match word {
        "x86_64" | "amd64" => Some(Arch::X86_64),
        "aarch64" | "arm64" => Some(Arch::Aarch64),
        _ => None,
    }
}

/// What a binary file was built for, read off its header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryKind {
    /// A single-architecture executable.
    Native(Platform),
    /// A macOS universal ("fat") binary: runs on either Mac architecture.
    UniversalDarwin,
}

impl BinaryKind {
    /// Whether this binary runs on `platform`.
    #[must_use]
    pub fn runs_on(self, platform: Platform) -> bool {
        match self {
            Self::Native(built) => built == platform,
            Self::UniversalDarwin => platform.os == Os::Darwin,
        }
    }

    #[must_use]
    pub fn describe(self) -> String {
        match self {
            Self::Native(platform) => platform.to_string(),
            Self::UniversalDarwin => "darwin universal".to_owned(),
        }
    }
}

/// The platform an executable's first bytes declare: a 64-bit
/// little-endian ELF (Linux) or Mach-O (macOS) for x86_64 or aarch64, or a
/// Mach-O universal binary. `None` for anything else — a script, a 32-bit
/// build, a truncated file.
#[must_use]
pub fn binary_kind(header: &[u8]) -> Option<BinaryKind> {
    match header.get(..4)? {
        [0x7f, b'E', b'L', b'F'] => elf_kind(header),
        [0xcf, 0xfa, 0xed, 0xfe] => macho_kind(header),
        [0xca, 0xfe, 0xba, 0xbe] => Some(BinaryKind::UniversalDarwin),
        _ => None,
    }
}

/// ELF: class 64 (byte 4 = 2), little-endian (byte 5 = 1), and `e_machine`
/// at bytes 18–19.
fn elf_kind(header: &[u8]) -> Option<BinaryKind> {
    if header.get(4) != Some(&2) || header.get(5) != Some(&1) {
        return None;
    }
    let machine = u16::from_le_bytes([*header.get(18)?, *header.get(19)?]);
    let arch = match machine {
        0x3e => Arch::X86_64,
        0xb7 => Arch::Aarch64,
        _ => return None,
    };
    Some(BinaryKind::Native(Platform {
        os: Os::Linux,
        arch,
    }))
}

/// Mach-O 64: `cputype` at bytes 4–7.
fn macho_kind(header: &[u8]) -> Option<BinaryKind> {
    let cpu = u32::from_le_bytes([
        *header.get(4)?,
        *header.get(5)?,
        *header.get(6)?,
        *header.get(7)?,
    ]);
    let arch = match cpu {
        0x0100_0007 => Arch::X86_64,
        0x0100_000c => Arch::Aarch64,
        _ => return None,
    };
    Some(BinaryKind::Native(Platform {
        os: Os::Darwin,
        arch,
    }))
}

/// Whether `target` is an ssh destination this verb will pass on:
/// `[user@]host`, from a conservative alphabet, and never starting with
/// `-` — ssh would read that as an option (`-oProxyCommand=…`).
#[must_use]
pub fn is_valid_target(target: &str) -> bool {
    let (user, host) = target.rsplit_once('@').unwrap_or(("", target));
    let user_ok = user
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
    let host_ok = !host.is_empty()
        && host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-:[]".contains(&b));
    !target.starts_with('-') && !host.starts_with('-') && user_ok && host_ok
}

/// Where on the remote machine the binaries go: home-relative (the default
/// `.horde/bin`, or `~/…`) or absolute, from a shell- and unit-file-safe
/// alphabet so every place it is interpolated stays unambiguous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteDir {
    Home(String),
    Absolute(String),
}

impl RemoteDir {
    /// Parses `--remote-dir`.
    ///
    /// # Errors
    ///
    /// A refusal naming the allowed alphabet.
    pub fn parse(given: &str) -> Result<Self, String> {
        let trimmed = given.trim_end_matches('/');
        let safe = !trimmed.is_empty()
            && trimmed
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-/~".contains(&b));
        if !safe {
            return Err(format!(
                "--remote-dir {given:?} must be a path of letters, digits, `.`, `_`, `-` and `/`"
            ));
        }
        match trimmed.strip_prefix("~/") {
            Some(rest) => Ok(Self::Home(rest.to_owned())),
            None if trimmed.starts_with('/') => Ok(Self::Absolute(trimmed.to_owned())),
            None if trimmed.contains('~') => Err(format!(
                "--remote-dir {given:?}: `~` is only understood as a leading `~/`"
            )),
            None => Ok(Self::Home(trimmed.to_owned())),
        }
    }

    /// The directory as a POSIX shell expression.
    #[must_use]
    pub fn shell(&self) -> String {
        match self {
            Self::Home(rest) => format!("\"$HOME\"/{}", sh_word(rest)),
            Self::Absolute(path) => sh_word(path),
        }
    }

    /// The directory as scp's remote path: scp resolves a relative path
    /// against the remote home.
    #[must_use]
    pub fn scp_path(&self) -> &str {
        match self {
            Self::Home(rest) => rest,
            Self::Absolute(path) => path,
        }
    }

    /// The absolute directory, once the remote `$HOME` is known — what a
    /// service unit, which has no shell, must name.
    #[must_use]
    pub fn absolute(&self, home: &str) -> String {
        match self {
            Self::Home(rest) => format!("{}/{rest}", home.trim_end_matches('/')),
            Self::Absolute(path) => path.clone(),
        }
    }
}

/// POSIX single-quoting: `'` becomes `'\''`.
#[must_use]
pub fn sh_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', r"'\''"))
}

/// A shell word: left bare when every byte is plainly inert to the shell,
/// single-quoted otherwise.
#[must_use]
pub fn sh_word(text: &str) -> String {
    let bare = !text.is_empty()
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-/:@=,+".contains(&b));
    if bare { text.to_owned() } else { sh_quote(text) }
}

/// One local command line: the program and its arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invocation {
    pub program: String,
    pub args: Vec<String>,
}

impl Invocation {
    /// The command line as one shell-pasteable string, for `--dry-run`.
    #[must_use]
    pub fn display(&self) -> String {
        std::iter::once(&self.program)
            .chain(&self.args)
            .map(|word| sh_word(word))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// How this invocation reaches the remote machine: the system ssh and scp
/// (so `~/.ssh/config`, the agent, `known_hosts` and password prompts all
/// behave as they always do), an optional config file, and the target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Remote {
    pub ssh: String,
    pub scp: String,
    pub config: Option<PathBuf>,
    pub target: String,
}

impl Remote {
    fn config_args(&self) -> Vec<String> {
        self.config
            .iter()
            .flat_map(|config| ["-F".to_owned(), config.display().to_string()])
            .collect()
    }

    /// `ssh [-F config] -T <target> "sh -c '<script>'"`. `-T`: no remote
    /// tty, so stdin is a clean byte channel for the join token.
    #[must_use]
    pub fn ssh(&self, script: &str) -> Invocation {
        let mut args = self.config_args();
        args.extend([
            "-T".to_owned(),
            self.target.clone(),
            format!("sh -c {}", sh_quote(script)),
        ]);
        Invocation {
            program: self.ssh.clone(),
            args,
        }
    }

    /// `scp [-F config] -q <local> <target>:<remote>`.
    #[must_use]
    pub fn scp(&self, local: &Path, remote: &str) -> Invocation {
        let mut args = self.config_args();
        args.extend([
            "-q".to_owned(),
            local.display().to_string(),
            format!("{}:{remote}", self.target),
        ]);
        Invocation {
            program: self.scp.clone(),
            args,
        }
    }
}

/// The first remote command: what the machine is, where home is, the PATH
/// a worker started over ssh inherits, and whether a worker identity is
/// already there.
#[must_use]
pub fn probe_script() -> String {
    format!(
        "printf 'uname %s\\n' \"$(uname -sm)\"\n\
         printf 'home %s\\n' \"$HOME\"\n\
         printf 'path %s\\n' \"$PATH\"\n\
         if [ -f \"$HOME\"/{marker} ]; then echo 'enrolled yes'; else echo 'enrolled no'; fi\n",
        marker = sh_word(IDENTITY_MARKER)
    )
}

/// What the probe found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    /// `uname -sm`, verbatim.
    pub uname: String,
    pub home: String,
    pub path: String,
    pub enrolled: bool,
}

/// Reads [`probe_script`]'s answer.
///
/// # Errors
///
/// A refusal naming the line that is missing.
pub fn parse_probe(output: &str) -> Result<Probe, String> {
    let field = |key: &str| {
        output
            .lines()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix(' '))
            .map(str::to_owned)
            .ok_or_else(|| format!("the remote probe printed no `{key}` line: {output:?}"))
    };
    Ok(Probe {
        uname: field("uname")?,
        home: field("home")?,
        path: field("path")?,
        enrolled: field("enrolled")? == "yes",
    })
}

/// Creates the install dir, owner-only.
#[must_use]
pub fn prepare_script(dir: &RemoteDir) -> String {
    format!("umask 077\nmkdir -p {}\n", dir.shell())
}

/// The name the copy lands under before [`finalize_script`] renames it
/// into place, so a running worker never sees a half-written binary.
#[must_use]
pub fn staged_name() -> String {
    format!("{WORKER_BINARY}.new")
}

/// Moves the staged copy into place and runs it once (`--version`): a
/// binary that cannot run there (a C library too old, the wrong
/// architecture after all) fails here, before any token is spent.
#[must_use]
pub fn finalize_script(dir: &RemoteDir) -> String {
    let dir = dir.shell();
    format!(
        "set -e\n\
         chmod 700 {dir}/{staged}\n\
         mv -f {dir}/{staged} {dir}/{WORKER_BINARY}\n\
         {dir}/{WORKER_BINARY} --version\n",
        staged = staged_name()
    )
}

/// Every adapter as `--adapter <name>` words, in order.
fn adapter_flags(adapters: &[String]) -> String {
    adapters
        .iter()
        .map(|adapter| format!(" --adapter {}", sh_word(adapter)))
        .collect()
}

/// How the started worker is kept alive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartMode {
    /// Detached from the ssh session (`nohup`, `setsid` where it exists),
    /// logging to `~/.horde/worker.log`; lives until the machine reboots.
    Foreground,
    /// A systemd user unit (Linux) or a launchd agent (macOS), carrying
    /// this definition verbatim.
    Service(ServiceUnit),
}

/// A service definition and where it is installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceUnit {
    pub os: Os,
    /// The file's content.
    pub content: String,
}

/// The unit for `os`, running `launcher` with `path` as its `PATH` — the
/// PATH the probe saw over ssh, which is where the harnesses were found;
/// a service manager's own PATH is minimal and would hide them.
#[must_use]
pub fn service_unit(os: Os, launcher: &str, path: &str, log: &str) -> ServiceUnit {
    let content = match os {
        Os::Linux => systemd_unit(launcher, path),
        Os::Darwin => launchd_plist(launcher, path, log),
    };
    ServiceUnit { os, content }
}

/// A systemd unit-file value, double-quoted, with systemd's own
/// specifiers (`%`) and variable expansion (`$`) escaped.
#[must_use]
pub fn systemd_quote(value: &str) -> String {
    let escaped = value
        .replace('\\', r"\\")
        .replace('"', "\\\"")
        .replace('%', "%%")
        .replace('$', "$$");
    format!("\"{escaped}\"")
}

fn systemd_unit(launcher: &str, path: &str) -> String {
    format!(
        "[Unit]\n\
         Description=Horde worker (installed by horde worker add)\n\
         After=network-online.target\n\
         \n\
         [Service]\n\
         ExecStart={launcher}\n\
         Environment={path}\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        launcher = systemd_quote(launcher),
        path = systemd_quote(&format!("PATH={path}")),
    )
}

/// XML text escaping for a plist string.
#[must_use]
pub fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn launchd_plist(launcher: &str, path: &str, log: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
         \x20 <key>Label</key><string>{LAUNCHD_LABEL}</string>\n\
         \x20 <key>ProgramArguments</key><array><string>{launcher}</string></array>\n\
         \x20 <key>EnvironmentVariables</key><dict><key>PATH</key><string>{path}</string></dict>\n\
         \x20 <key>RunAtLoad</key><true/>\n\
         \x20 <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>\n\
         \x20 <key>StandardOutPath</key><string>{log}</string>\n\
         \x20 <key>StandardErrorPath</key><string>{log}</string>\n\
         </dict>\n\
         </plist>\n",
        launcher = xml_escape(launcher),
        path = xml_escape(path),
        log = xml_escape(log),
    )
}

/// The heredoc delimiter embedded files are written under. Neither the
/// launcher nor a unit can contain it: it is not in their alphabet of
/// lines.
const HEREDOC: &str = "HORDE_WORKER_ADD_EOF";

/// `cat > <path> <<'EOF' … EOF`, quoted so the remote shell expands
/// nothing inside.
fn write_file(path_expr: &str, content: &str) -> String {
    format!("cat > {path_expr} <<'{HEREDOC}'\n{content}{HEREDOC}\n")
}

/// The start step. Its stdin is the join token, which it writes (umask
/// 077) to the token file — never argv, never the process list — before
/// starting the worker through the launcher. It then waits, at most
/// `wait_secs`, for the identity the join writes, and removes the token
/// file on every exit path.
///
/// Prints `pid <n>` (foreground), `linger <yes|no>` (systemd), then
/// `joined`, or `exited`/`timeout` followed by `log …` lines.
#[must_use]
pub fn start_script(dir: &RemoteDir, adapters: &[String], mode: &StartMode, wait_secs: u64) -> String {
    let launcher_expr = format!("{}/{LAUNCHER}", dir.shell());
    let mut script = format!(
        "set -eu\n\
         umask 077\n\
         state=\"$HOME\"/{STATE_ROOT}\n\
         mkdir -p \"$state\"\n\
         tok=\"$state\"/{TOKEN_FILE}\n\
         trap 'rm -f \"$tok\"' EXIT\n\
         cat > \"$tok\"\n"
    );
    script.push_str(&write_file(
        &launcher_expr,
        &launcher_script(adapters),
    ));
    script.push_str(&format!("chmod 700 {launcher_expr}\n"));
    script.push_str(&launch_lines(mode, &launcher_expr));
    script.push_str(&wait_lines(mode, wait_secs));
    script
}

/// The launcher both start modes run, beside the binary. It redeems the
/// join token only while no identity exists, so a service manager
/// restarting it later — token long removed — just starts the enrolled
/// worker. It finds the binary through its own directory, so the file
/// names no path a quoted heredoc would have to expand.
#[must_use]
pub fn launcher_script(adapters: &[String]) -> String {
    let adapters = adapter_flags(adapters);
    format!(
        "#!/bin/sh\n\
         # Written by `horde worker add`: starts the horde-worker on this machine.\n\
         bin=$(cd \"$(dirname \"$0\")\" && pwd)\n\
         tok=\"$HOME\"/{STATE_ROOT}/{TOKEN_FILE}\n\
         if [ -f \"$tok\" ] && [ ! -f \"$HOME\"/{marker} ]; then\n\
         \x20 exec \"$bin\"/{WORKER_BINARY} --join-file \"$tok\"{adapters}\n\
         fi\n\
         exec \"$bin\"/{WORKER_BINARY}{adapters}\n",
        marker = sh_word(IDENTITY_MARKER),
    )
}

/// How the start script launches the launcher, per mode.
fn launch_lines(mode: &StartMode, launcher_expr: &str) -> String {
    match mode {
        StartMode::Foreground => format!(
            "if command -v setsid >/dev/null 2>&1; then detach=setsid; else detach=; fi\n\
             nohup $detach {launcher_expr} >\"$state\"/{LOG_FILE} 2>&1 </dev/null &\n\
             pid=$!\n\
             echo \"pid $pid\"\n"
        ),
        StartMode::Service(unit) => match unit.os {
            Os::Linux => format!(
                "dir=\"$HOME\"/.config/systemd/user\n\
                 mkdir -p \"$dir\"\n\
                 {write}\
                 systemctl --user daemon-reload\n\
                 systemctl --user enable {SYSTEMD_UNIT}\n\
                 systemctl --user restart {SYSTEMD_UNIT}\n\
                 if command -v loginctl >/dev/null 2>&1; then\n\
                 \x20 case \"$(loginctl show-user \"$(id -un)\" -p Linger 2>/dev/null)\" in\n\
                 \x20   Linger=yes) echo 'linger yes' ;;\n\
                 \x20   *) echo 'linger no' ;;\n\
                 \x20 esac\n\
                 fi\n",
                write = write_file(&format!("\"$dir\"/{SYSTEMD_UNIT}"), &unit.content),
            ),
            Os::Darwin => format!(
                "dir=\"$HOME\"/Library/LaunchAgents\n\
                 mkdir -p \"$dir\"\n\
                 plist=\"$dir\"/{LAUNCHD_LABEL}.plist\n\
                 {write}\
                 uid=$(id -u)\n\
                 launchctl bootout \"gui/$uid/{LAUNCHD_LABEL}\" 2>/dev/null || true\n\
                 launchctl bootstrap \"gui/$uid\" \"$plist\"\n",
                write = write_file("\"$plist\"", &unit.content),
            ),
        },
    }
}

/// The bounded wait for the identity the join writes. A foreground worker
/// that dies ends the wait at once; either failure prints the log's tail.
fn wait_lines(mode: &StartMode, wait_secs: u64) -> String {
    let tail = match mode {
        StartMode::Service(ServiceUnit { os: Os::Linux, .. }) => format!(
            "journalctl --user -u {SYSTEMD_UNIT} -n 20 --no-pager 2>/dev/null | sed 's/^/log /'"
        ),
        _ => format!("tail -n 20 \"$state\"/{LOG_FILE} 2>/dev/null | sed 's/^/log /'"),
    };
    let liveness = match mode {
        StartMode::Foreground => format!(
            "\x20 if ! kill -0 \"$pid\" 2>/dev/null; then echo exited; {tail} || true; exit 3; fi\n"
        ),
        StartMode::Service(_) => String::new(),
    };
    format!(
        "i=0\n\
         while [ ! -f \"$HOME\"/{marker} ]; do\n\
         {liveness}\
         \x20 i=$((i+1))\n\
         \x20 if [ \"$i\" -gt {wait_secs} ]; then echo timeout; {tail} || true; exit 4; fi\n\
         \x20 sleep 1\n\
         done\n\
         echo joined\n",
        marker = sh_word(IDENTITY_MARKER),
    )
}

/// What the start step reported.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Started {
    /// The detached worker's pid (foreground mode).
    pub pid: Option<u32>,
    /// Whether systemd keeps this user's services running after logout
    /// (`None`: not systemd, or not answerable).
    pub linger: Option<bool>,
    /// Whether the join was redeemed.
    pub joined: bool,
    /// `exited` or `timeout` when it was not.
    pub failure: Option<String>,
    /// The worker log's tail, on failure.
    pub log: Vec<String>,
}

/// Reads [`start_script`]'s output.
#[must_use]
pub fn parse_started(output: &str) -> Started {
    let mut started = Started::default();
    for line in output.lines() {
        if let Some(pid) = line.strip_prefix("pid ") {
            started.pid = pid.trim().parse().ok();
        } else if let Some(linger) = line.strip_prefix("linger ") {
            started.linger = Some(linger.trim() == "yes");
        } else if let Some(log) = line.strip_prefix("log ") {
            started.log.push(log.to_owned());
        } else if line == "joined" {
            started.joined = true;
        } else if line == "exited" || line == "timeout" {
            started.failure = Some(line.to_owned());
        }
    }
    started
}

/// The harness report, run after the worker joined: each harness's path
/// and first `--version` line, on the PATH the worker itself inherited.
#[must_use]
pub fn harness_script() -> String {
    let names = HARNESSES.join(" ");
    format!(
        "for h in {names}; do\n\
         \x20 if p=$(command -v \"$h\" 2>/dev/null); then\n\
         \x20   v=$(\"$p\" --version 2>/dev/null </dev/null | head -n 1) || v=\n\
         \x20   printf 'harness\\t%s\\t%s\\t%s\\n' \"$h\" \"$p\" \"$v\"\n\
         \x20 else\n\
         \x20   printf 'harness\\t%s\\t\\t\\n' \"$h\"\n\
         \x20 fi\n\
         done\n"
    )
}

/// One harness as the remote machine reported it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Harness {
    pub name: String,
    /// Where it is on the worker's PATH; `None`: not there.
    pub path: Option<String>,
    /// Its first `--version` line, when it answered one.
    pub version: Option<String>,
}

/// Reads [`harness_script`]'s output.
#[must_use]
pub fn parse_harnesses(output: &str) -> Vec<Harness> {
    let non_empty = |field: Option<&str>| {
        field
            .map(str::trim)
            .filter(|field| !field.is_empty())
            .map(str::to_owned)
    };
    output
        .lines()
        .filter_map(|line| {
            let mut fields = line.strip_prefix("harness\t")?.split('\t');
            let name = non_empty(fields.next())?;
            Some(Harness {
                name,
                path: non_empty(fields.next()),
                version: non_empty(fields.next()),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;
