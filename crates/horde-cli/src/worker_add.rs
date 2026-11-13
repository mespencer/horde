//! `horde worker add <name> <user@host>`: bootstrap a remote Unix machine as
//! a worker over the system ssh (DESIGN §3, "Worker bootstrap over ssh";
//! ADR-0007, 2026-11-13 amendment).
//!
//! Client-side orchestration of existing primitives, no new protocol:
//! probe the machine (`uname -sm`), copy a `horde-worker` built for it into
//! a user-owned directory, mint a join token through the ordinary `worker
//! enroll` path, hand it over ssh *stdin* to a file written under umask 077,
//! start the worker through a launcher that redeems it, remove the file,
//! and wait — bounded — for the worker to register. Then report which
//! harnesses the worker can see.
//!
//! What runs remotely is decided in [`plan`], as values; this module only
//! sequences them, runs them, and renders the outcome. `--dry-run` prints
//! the same values without connecting to anything.

pub mod plan;

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;

use crate::{Client, ClientError, Format};
use plan::{Invocation, Platform, Probe, Remote, RemoteDir, StartMode};

/// How long one ssh step may take — generous, because a password or
/// host-key prompt waits on a human.
const STEP_CEILING: Duration = Duration::from_secs(180);

/// How long the copy may take: a debug `horde-worker` is large.
const COPY_CEILING: Duration = Duration::from_secs(900);

/// How long `--build-locally`'s cargo build may take.
const BUILD_CEILING: Duration = Duration::from_secs(3600);

/// How often the registration wait asks the server for its roster.
const REGISTRATION_POLL: Duration = Duration::from_millis(500);

/// A harness the worker can run.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterArg {
    Claude,
    Codex,
    Fake,
}

impl AdapterArg {
    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Fake => "fake",
        }
    }
}

/// `horde worker add`'s flags.
#[derive(clap::Args, Debug, Clone)]
pub struct AddArgs {
    /// The worker's principal name, fixed at mint time.
    pub name: String,
    /// The ssh destination: `user@host`, or a Host alias from
    /// `~/.ssh/config`.
    pub target: String,
    /// A harness the worker registers; repeatable, the first is its default
    /// provider.
    #[arg(long = "adapter", value_enum, default_values_t = [AdapterArg::Claude])]
    pub adapters: Vec<AdapterArg>,
    /// The local directory holding a `horde-worker` built for the remote
    /// machine. Default: beside this `horde`.
    #[arg(long, conflicts_with_all = ["from_release", "build_locally"])]
    pub binary_dir: Option<PathBuf>,
    /// Download a published release for the remote platform. No release
    /// is published yet, so this refuses and says what to do instead.
    #[arg(long, conflicts_with = "build_locally")]
    pub from_release: bool,
    /// Build `horde-worker` here for the remote's target triple first
    /// (`cargo build --release --target <triple>`, from this workspace).
    #[arg(long)]
    pub build_locally: bool,
    /// Where the binaries go on the remote machine: relative to its home,
    /// or absolute. Never needs sudo.
    #[arg(long, default_value = ".horde/bin")]
    pub remote_dir: String,
    /// Install a user-level service — a systemd user unit on Linux, a
    /// launchd agent on macOS — so the worker survives reboots. Without
    /// it the worker runs detached until the machine restarts.
    #[arg(long)]
    pub service: bool,
    /// How long, in seconds, to wait for the join and then for the
    /// registration, each.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_secs: u64,
    /// The join token's lifetime in minutes (the server clamps it).
    #[arg(long, value_parser = clap::builder::RangedU64ValueParser::<u32>::new().range(1..))]
    pub ttl_minutes: Option<u32>,
    /// The ssh program.
    #[arg(long, default_value = "ssh")]
    pub ssh: String,
    /// The scp program.
    #[arg(long, default_value = "scp")]
    pub scp: String,
    /// An ssh config file, passed to ssh and scp as `-F`.
    #[arg(long)]
    pub ssh_config: Option<PathBuf>,
    /// Print every command this would run, and run none of them.
    #[arg(long)]
    pub dry_run: bool,
}

/// The arguments, validated.
#[derive(Debug, Clone)]
pub struct Checked {
    pub name: String,
    pub remote: Remote,
    pub dir: RemoteDir,
    pub adapters: Vec<String>,
    pub service: bool,
    pub wait_secs: u64,
}

fn refusal(message: impl Into<String>) -> ClientError {
    ClientError::WorkerAdd(message.into())
}

/// Validates everything that can be judged before connecting anywhere.
///
/// # Errors
///
/// A refusal naming the argument.
pub fn checked(args: &AddArgs) -> Result<Checked, ClientError> {
    if !horde_identity::principal::is_valid_principal_name(&args.name) {
        return Err(refusal(format!(
            "{:?} cannot be a worker name: use up to 64 letters, digits, `.`, `_` or `-`",
            args.name
        )));
    }
    if !plan::is_valid_target(&args.target) {
        return Err(refusal(format!(
            "{:?} is not an ssh destination this verb passes on: use user@host (letters, \
             digits, `.`, `_`, `-`), or a Host alias from ~/.ssh/config",
            args.target
        )));
    }
    if args.from_release {
        return Err(refusal(
            "no Horde release is published yet, so there is nothing to download; pass \
             --binary-dir with a horde-worker built for the remote machine, or --build-locally",
        ));
    }
    let dir = RemoteDir::parse(&args.remote_dir).map_err(refusal)?;
    Ok(Checked {
        name: args.name.clone(),
        remote: Remote {
            ssh: args.ssh.clone(),
            scp: args.scp.clone(),
            config: args.ssh_config.clone(),
            target: args.target.clone(),
        },
        dir,
        adapters: args.adapters.iter().map(|a| a.word().to_owned()).collect(),
        service: args.service,
        wait_secs: args.timeout_secs,
    })
}

/// Where the binary to copy comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// A directory holding a ready `horde-worker`.
    Dir(PathBuf),
    /// Built here, for the remote triple, into `target_dir`.
    Build { target_dir: PathBuf },
}

impl Source {
    /// The source `args` asks for: `--build-locally`, else `--binary-dir`,
    /// else the directory this `horde` runs from.
    ///
    /// # Errors
    ///
    /// A refusal when no directory can be named.
    pub fn of(
        args: &AddArgs,
        exe_dir: Option<PathBuf>,
        cargo_target_dir: Option<PathBuf>,
    ) -> Result<Self, ClientError> {
        if args.build_locally {
            return Ok(Self::Build {
                target_dir: cargo_target_dir.unwrap_or_else(|| PathBuf::from("target")),
            });
        }
        args.binary_dir
            .clone()
            .or(exe_dir)
            .map(Self::Dir)
            .ok_or_else(|| refusal("cannot tell where this horde runs from; pass --binary-dir"))
    }

    /// The `horde-worker` to copy for `platform`.
    #[must_use]
    pub fn binary(&self, platform: Platform) -> PathBuf {
        match self {
            Self::Dir(dir) => dir.join(plan::WORKER_BINARY),
            Self::Build { target_dir } => target_dir
                .join(platform.triple())
                .join("release")
                .join(plan::WORKER_BINARY),
        }
    }

    /// The build to run first, if any.
    #[must_use]
    pub fn build(&self, platform: Platform) -> Option<Invocation> {
        match self {
            Self::Dir(_) => None,
            Self::Build { .. } => Some(Invocation {
                program: "cargo".to_owned(),
                args: [
                    "build",
                    "--release",
                    "-p",
                    "horde-worker",
                    "--bin",
                    "horde-worker",
                    "--target",
                    platform.triple(),
                ]
                .map(str::to_owned)
                .to_vec(),
            }),
        }
    }
}

/// One remote command, and what (if anything) its stdin carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub name: &'static str,
    pub invocation: Invocation,
    /// The script an ssh step runs under `sh -c`, for the human plan.
    pub script: Option<String>,
    /// `Some("join token")`: stdin carries the token.
    pub stdin: Option<&'static str>,
}

impl Step {
    fn ssh(remote: &Remote, name: &'static str, script: String) -> Self {
        Self {
            name,
            invocation: remote.ssh(&script),
            script: Some(script),
            stdin: None,
        }
    }

    /// The step for a reader: the command line, and an ssh step's script
    /// shown as written rather than as the quoted argument it travels in.
    #[must_use]
    pub fn display(&self) -> String {
        let stdin = self
            .stdin
            .map_or_else(String::new, |what| format!("   (stdin: the {what})"));
        match &self.script {
            Some(script) => {
                let mut prefix = self.invocation.clone();
                prefix.args.pop();
                let body = script
                    .lines()
                    .map(|line| format!("    {line}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{} sh -c <<script{stdin}\n{body}", prefix.display())
            }
            None => format!("{}{stdin}", self.invocation.display()),
        }
    }
}

/// What the steps after the probe need from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    pub platform: Platform,
    pub home: String,
    pub path: String,
}

/// The start mode `checked` asks for on `context`'s machine.
#[must_use]
pub fn start_mode(checked: &Checked, context: &Context) -> StartMode {
    if !checked.service {
        return StartMode::Foreground;
    }
    let dir = checked.dir.absolute(&context.home);
    let log = format!(
        "{}/{}/{}",
        context.home.trim_end_matches('/'),
        plan::STATE_ROOT,
        plan::LOG_FILE
    );
    StartMode::Service(plan::service_unit(
        context.platform.os,
        &format!("{dir}/{}", plan::LAUNCHER),
        &context.path,
        &log,
    ))
}

/// The probe: the first remote command.
#[must_use]
pub fn probe_step(checked: &Checked) -> Step {
    Step::ssh(&checked.remote, "probe", plan::probe_script())
}

/// Every remote command after the probe, in order: prepare the dir, copy
/// the binary, move it into place, start the worker, report harnesses.
#[must_use]
pub fn later_steps(checked: &Checked, context: &Context, binary: &Path) -> Vec<Step> {
    let remote = &checked.remote;
    let staged = format!("{}/{}", checked.dir.scp_path(), plan::staged_name());
    let mode = start_mode(checked, context);
    vec![
        Step::ssh(remote, "prepare", plan::prepare_script(&checked.dir)),
        Step {
            name: "copy",
            invocation: remote.scp(binary, &staged),
            script: None,
            stdin: None,
        },
        Step::ssh(remote, "install", plan::finalize_script(&checked.dir)),
        Step {
            stdin: Some("join token"),
            ..Step::ssh(
                remote,
                "start",
                plan::start_script(&checked.dir, &checked.adapters, &mode, checked.wait_secs),
            )
        },
        Step::ssh(remote, "harnesses", plan::harness_script()),
    ]
}

/// `--dry-run`: prints every command, connecting to nothing. The remote
/// platform is not known without the probe, so the plan assumes this
/// machine's, and the remote home and PATH appear as placeholders.
///
/// # Errors
///
/// The same argument refusals a real run gives.
pub fn dry_run(args: &AddArgs, format: Format) -> Result<(), ClientError> {
    let checked = checked(args)?;
    let source = Source::of(args, exe_dir(), cargo_target_dir())?;
    let platform = local_platform()?;
    let context = Context {
        platform,
        home: "<remote-home>".to_owned(),
        path: "<remote-PATH>".to_owned(),
    };
    println!("{}", render_plan(&checked, &source, &context, format));
    Ok(())
}

/// The dry-run plan in either format.
#[must_use]
pub fn render_plan(
    checked: &Checked,
    source: &Source,
    context: &Context,
    format: Format,
) -> String {
    let mut steps = Vec::new();
    let build = source.build(context.platform);
    steps.push(probe_step(checked));
    steps.extend(later_steps(
        checked,
        context,
        &source.binary(context.platform),
    ));
    let mint = format!(
        "mint a worker join token for {} through the server (horde worker enroll)",
        checked.name
    );
    let register = format!(
        "wait up to {}s for {} to register",
        checked.wait_secs, checked.name
    );
    match format {
        Format::Json => json!({
            "dry_run": true,
            "assumed_platform": context.platform.to_string(),
            "build": build.as_ref().map(|b| b.args.iter().fold(vec![b.program.clone()], |mut all, a| { all.push(a.clone()); all })),
            "steps": steps.iter().map(|step| json!({
                "step": step.name,
                "argv": std::iter::once(&step.invocation.program).chain(&step.invocation.args).collect::<Vec<_>>(),
                "stdin": step.stdin,
            })).collect::<Vec<_>>(),
            "client": [mint, register],
        })
        .to_string(),
        Format::Human => {
            let mut lines = vec![format!(
                "dry run: nothing is run; the remote is assumed to be {} until `uname -sm` says",
                context.platform
            )];
            if let Some(build) = &build {
                lines.push(format!("local  build: {}", build.display()));
            }
            for step in &steps {
                if step.name == "start" {
                    lines.push(format!("client {mint}"));
                }
                lines.push(format!("remote {}: {}", step.name, step.display()));
                if step.name == "start" {
                    lines.push(format!("client {register}"));
                }
            }
            lines.join("\n")
        }
    }
}

fn exe_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()?
        .parent()
        .map(Path::to_path_buf)
}

fn cargo_target_dir() -> Option<PathBuf> {
    std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from)
}

fn local_platform() -> Result<Platform, ClientError> {
    Platform::from_consts(std::env::consts::OS, std::env::consts::ARCH).ok_or_else(|| {
        refusal(format!(
            "this horde runs on {} {}, which worker add does not plan for",
            std::env::consts::OS,
            std::env::consts::ARCH
        ))
    })
}

/// Runs one local command to completion within `limit`: stdout on success,
/// a refusal carrying the step and the command's stderr otherwise.
///
/// # Errors
///
/// The command not starting, failing, or outliving `limit`.
pub async fn execute(
    step: &str,
    invocation: &Invocation,
    stdin: Option<&str>,
    limit: Duration,
) -> Result<String, ClientError> {
    use tokio::io::AsyncWriteExt;
    let mut command = tokio::process::Command::new(&invocation.program);
    command
        .args(&invocation.args)
        .stdin(if stdin.is_some() {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        })
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|error| {
        refusal(format!(
            "{step}: could not run {}: {error}",
            invocation.program
        ))
    })?;
    if let (Some(text), Some(mut pipe)) = (stdin, child.stdin.take()) {
        // A write the remote end refused shows up as the command's own
        // failure below, with its stderr; nothing to add here.
        let _ = pipe.write_all(text.as_bytes()).await;
        drop(pipe);
    }
    let output = tokio::time::timeout(limit, child.wait_with_output())
        .await
        .map_err(|_| refusal(format!("{step}: no answer within {limit:?}")))?
        .map_err(|error| refusal(format!("{step}: {error}")))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    if output.status.success() {
        Ok(stdout)
    } else {
        Err(step_failure(
            step,
            output.status.code(),
            &stdout,
            &String::from_utf8_lossy(&output.stderr),
        ))
    }
}

/// A failed step in operator words: ssh's own exit code 255 means the
/// connection itself failed, which `ssh <target> true` reproduces.
#[must_use]
pub fn step_failure(step: &str, code: Option<i32>, stdout: &str, stderr: &str) -> ClientError {
    let said = [stdout.trim(), stderr.trim()]
        .into_iter()
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    let code = code.map_or_else(|| "a signal".to_owned(), |code| format!("exit {code}"));
    let hint = if code == "exit 255" {
        "; ssh could not connect or authenticate — check that `ssh <target> true` works"
    } else {
        ""
    };
    refusal(format!("{step} failed ({code}){hint}: {said}"))
}

/// What one successful run found and did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Added {
    pub name: String,
    pub target: String,
    pub platform: Platform,
    /// The installed binary's absolute path.
    pub binary: String,
    /// The installed binary's `--version` line.
    pub version: String,
    pub mode: &'static str,
    pub pid: Option<u32>,
    /// The worker's log on the remote machine, when it writes one.
    pub log: Option<String>,
    pub harnesses: Vec<plan::Harness>,
    pub adapters: Vec<String>,
    pub warnings: Vec<String>,
}

/// The word the report uses for a start mode.
#[must_use]
pub fn mode_word(mode: &StartMode) -> &'static str {
    match mode {
        StartMode::Foreground => "foreground",
        StartMode::Service(unit) => match unit.os {
            plan::Os::Linux => "systemd",
            plan::Os::Darwin => "launchd",
        },
    }
}

/// Warnings about the remote harnesses: an adapter asked for whose binary
/// the worker cannot see means agents placed there fail to start.
#[must_use]
pub fn harness_warnings(adapters: &[String], harnesses: &[plan::Harness]) -> Vec<String> {
    harnesses
        .iter()
        .filter(|harness| harness.path.is_none() && adapters.contains(&harness.name))
        .map(|harness| {
            format!(
                "--adapter {name}, but no {name} is on the remote PATH the worker inherited; \
                 install it there (then `horde worker login <worker> {name}`) or drop the adapter",
                name = harness.name
            )
        })
        .collect()
}

/// The finished report in either format.
#[must_use]
pub fn render(added: &Added, format: Format) -> String {
    match format {
        Format::Json => json!({
            "worker": added.name,
            "target": added.target,
            "platform": added.platform.to_string(),
            "triple": added.platform.triple(),
            "binary": added.binary,
            "version": added.version,
            "mode": added.mode,
            "pid": added.pid,
            "log": added.log,
            "registered": true,
            "harnesses": added.harnesses.iter().map(|harness| json!({
                "name": harness.name,
                "path": harness.path,
                "version": harness.version,
                "required": added.adapters.contains(&harness.name),
            })).collect::<Vec<_>>(),
            "warnings": added.warnings,
        })
        .to_string(),
        Format::Human => {
            let mut lines = vec![
                format!(
                    "added worker={} target={} platform={}",
                    added.name, added.target, added.platform
                ),
                format!("  binary     {} ({})", added.binary, added.version),
                match (added.pid, &added.log) {
                    (Some(pid), Some(log)) => format!("  running    pid={pid} log={log}"),
                    _ => format!("  running    {} service", added.mode),
                },
                format!("  registered {}", added.name),
            ];
            for harness in &added.harnesses {
                lines.push(match (&harness.path, &harness.version) {
                    (Some(path), Some(version)) => {
                        format!("  harness    {} {path} ({version})", harness.name)
                    }
                    (Some(path), None) => format!("  harness    {} {path}", harness.name),
                    (None, _) => format!("  harness    {} missing", harness.name),
                });
            }
            lines.extend(added.warnings.iter().map(|w| format!("warning: {w}")));
            lines.join("\n")
        }
    }
}

/// A progress line on stderr, for a terminal only: `--json` keeps stderr
/// for refusals.
fn progress(format: Format, line: &str) {
    if format == Format::Human {
        eprintln!("{line}");
    }
}

/// Reads the probe into the platform and context, refusing a machine this
/// verb cannot serve or one that already holds a worker identity.
///
/// # Errors
///
/// The refusal, naming the machine and what to do.
pub fn context_of(target: &str, probe: &Probe) -> Result<Context, ClientError> {
    let platform = Platform::from_uname(&probe.uname).ok_or_else(|| {
        refusal(format!(
            "{target} reports `{}`; worker add serves Linux and macOS on x86_64 or aarch64",
            probe.uname
        ))
    })?;
    if probe.enrolled {
        return Err(refusal(format!(
            "{target} already holds a worker identity (~/.horde/worker/identity); start it there \
             with `~/.horde/bin/horde-worker --adapter …`, or move ~/.horde/worker aside to \
             enroll it afresh (then `horde worker reset <name>` if the server knew it)"
        )));
    }
    Ok(Context {
        platform,
        home: probe.home.clone(),
        path: probe.path.clone(),
    })
}

/// Refuses a local binary that will not run on `platform`.
///
/// # Errors
///
/// A refusal explaining how to provide the right binary.
pub fn check_binary(
    binary: &Path,
    header: &[u8],
    target: &str,
    platform: Platform,
) -> Result<(), ClientError> {
    let provide = format!(
        "build one for {triple} (on such a machine, `cargo build --release -p horde-worker`; \
         here, `rustup target add {triple}` and --build-locally, when a linker for it is \
         installed) and pass its directory with --binary-dir",
        triple = platform.triple()
    );
    match plan::binary_kind(header) {
        Some(kind) if kind.runs_on(platform) => Ok(()),
        Some(kind) => Err(refusal(format!(
            "{} is built for {}, but {target} is {platform}; {provide}",
            binary.display(),
            kind.describe()
        ))),
        None => Err(refusal(format!(
            "{} is not a 64-bit Linux or macOS executable; {provide}",
            binary.display()
        ))),
    }
}

/// The first bytes of `binary`, enough for [`plan::binary_kind`].
fn header_of(binary: &Path) -> Result<Vec<u8>, ClientError> {
    use std::io::Read;
    let mut header = vec![0_u8; 64];
    let mut file = std::fs::File::open(binary).map_err(|source| ClientError::File {
        path: binary.to_path_buf(),
        source,
    })?;
    let read = file.read(&mut header).map_err(|source| ClientError::File {
        path: binary.to_path_buf(),
        source,
    })?;
    header.truncate(read);
    Ok(header)
}

/// Mints the worker's join token through the ordinary enrollment path,
/// warning when it names a loopback host (only a server on the remote
/// machine itself would answer it) and refusing the unspecified address.
///
/// # Errors
///
/// The mint refusing, or a token no machine can dial.
pub async fn mint(
    client: &mut Client,
    checked: &Checked,
    endpoint: &str,
    ttl_minutes: Option<u32>,
    warnings: &mut Vec<String>,
) -> Result<horde_identity::JoinToken, ClientError> {
    let minted = client
        .mint_enrollment_code(horde_proto::v1::Role::Worker, &checked.name, ttl_minutes)
        .await?;
    let token = crate::cli::token_from_mint(endpoint, &minted)
        .ok_or_else(|| refusal("the server's mint answer named no enrollment listener"))?;
    match token.scope() {
        horde_identity::HostScope::Unspecified => Err(refusal(format!(
            "this invocation dialed {endpoint}, so the token would name {}, which no machine \
             can dial; pass --endpoint https://<name {} can reach>:<port>",
            token.host(),
            checked.remote.target
        ))),
        horde_identity::HostScope::Loopback => {
            warnings.push(format!(
                "the join token names {}, which {} resolves to itself: the join works only if \
                 the server runs there; mint through a name it can reach with --endpoint",
                token.host(),
                checked.remote.target
            ));
            Ok(token)
        }
        horde_identity::HostScope::Routable => Ok(token),
    }
}

/// Polls the server's roster until `name` is on it, at most `limit`.
///
/// # Errors
///
/// The roster call failing, or `name` absent when `limit` passes.
pub async fn await_registration(
    client: &mut Client,
    name: &str,
    limit: Duration,
) -> Result<(), ClientError> {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        let workers = client.list_workers().await?;
        if workers.iter().any(|worker| worker.name == name) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(refusal(format!(
                "{name} joined but did not register within {}s; its log on the remote machine \
                 (~/.horde/worker.log, or the service's journal) says why",
                limit.as_secs()
            )));
        }
        tokio::time::sleep(REGISTRATION_POLL).await;
    }
}

/// Runs `horde worker add` for real.
///
/// # Errors
///
/// The first step that fails, in operator words.
pub async fn run(
    client: &mut Client,
    args: &AddArgs,
    endpoint: &str,
    format: Format,
) -> Result<(), ClientError> {
    let checked = checked(args)?;
    let source = Source::of(args, exe_dir(), cargo_target_dir())?;
    progress(format, &format!("probing {}", checked.remote.target));
    let probed = execute(
        "probe",
        &probe_step(&checked).invocation,
        None,
        STEP_CEILING,
    )
    .await?;
    let probe = plan::parse_probe(&probed).map_err(refusal)?;
    let context = context_of(&checked.remote.target, &probe)?;
    let binary = local_binary(&source, &checked, &context, format).await?;
    let steps = later_steps(&checked, &context, &binary);
    let mut warnings = Vec::new();
    let version = install(&steps, &checked, format).await?;
    let token = mint(client, &checked, endpoint, args.ttl_minutes, &mut warnings).await?;
    let started = start(&steps, &token, format).await?;
    progress(format, &format!("waiting for {} to register", checked.name));
    await_registration(
        client,
        &checked.name,
        Duration::from_secs(checked.wait_secs),
    )
    .await?;
    let harnesses = plan::parse_harnesses(&run_step(&steps, "harnesses", None).await?);
    warnings.extend(harness_warnings(&checked.adapters, &harnesses));
    if started.linger == Some(false) {
        warnings.push(format!(
            "systemd stops this user's services at logout; run `loginctl enable-linger` on {} \
             to keep the worker running",
            checked.remote.target
        ));
    }
    let mode = start_mode(&checked, &context);
    let added = Added {
        name: checked.name.clone(),
        target: checked.remote.target.clone(),
        platform: context.platform,
        binary: format!(
            "{}/{}",
            checked.dir.absolute(&context.home),
            plan::WORKER_BINARY
        ),
        version,
        mode: mode_word(&mode),
        pid: started.pid,
        log: started
            .pid
            .map(|_| format!("{}/{}/{}", context.home, plan::STATE_ROOT, plan::LOG_FILE)),
        harnesses,
        adapters: checked.adapters.clone(),
        warnings,
    };
    println!("{}", render(&added, format));
    Ok(())
}

/// The binary to copy: built first under `--build-locally`, then checked
/// against the remote platform.
async fn local_binary(
    source: &Source,
    checked: &Checked,
    context: &Context,
    format: Format,
) -> Result<PathBuf, ClientError> {
    if let Some(build) = source.build(context.platform) {
        progress(format, &format!("building: {}", build.display()));
        execute("build", &build, None, BUILD_CEILING).await?;
    }
    let binary = source.binary(context.platform);
    check_binary(
        &binary,
        &header_of(&binary)?,
        &checked.remote.target,
        context.platform,
    )?;
    Ok(binary)
}

/// Runs the named step from `steps`.
async fn run_step(steps: &[Step], name: &str, stdin: Option<&str>) -> Result<String, ClientError> {
    let step = steps
        .iter()
        .find(|step| step.name == name)
        .ok_or_else(|| refusal(format!("no {name} step was planned")))?;
    let limit = if name == "copy" {
        COPY_CEILING
    } else {
        STEP_CEILING
    };
    execute(name, &step.invocation, stdin, limit).await
}

/// Prepare, copy, install: the binary's `--version` line on success.
async fn install(steps: &[Step], checked: &Checked, format: Format) -> Result<String, ClientError> {
    progress(
        format,
        &format!("installing into {}", checked.dir.scp_path()),
    );
    run_step(steps, "prepare", None).await?;
    run_step(steps, "copy", None).await?;
    let version = run_step(steps, "install", None).await?;
    Ok(version.lines().next().unwrap_or_default().trim().to_owned())
}

/// The start step, with the token on stdin; refuses when the join was not
/// redeemed, carrying the worker log's tail.
async fn start(
    steps: &[Step],
    token: &horde_identity::JoinToken,
    format: Format,
) -> Result<plan::Started, ClientError> {
    progress(format, "starting the worker");
    let stdin = format!("{token}\n");
    let output = match run_step(steps, "start", Some(&stdin)).await {
        Ok(output) => output,
        Err(ClientError::WorkerAdd(message)) => {
            return Err(refusal(join_failure(&message)));
        }
        Err(other) => return Err(other),
    };
    let started = plan::parse_started(&output);
    if started.joined {
        Ok(started)
    } else {
        Err(refusal(join_failure(&output)))
    }
}

/// A join that did not happen, with what the remote side printed.
#[must_use]
pub fn join_failure(said: &str) -> String {
    let started = plan::parse_started(said);
    let why = match started.failure.as_deref() {
        Some("exited") => "the worker exited before its join was redeemed",
        Some("timeout") => "the worker's join was not redeemed in time",
        _ => "the worker did not join",
    };
    if started.log.is_empty() {
        format!("{why}: {}", said.trim())
    } else {
        format!("{why}; its log ends:\n{}", started.log.join("\n"))
    }
}

#[cfg(test)]
mod tests;
