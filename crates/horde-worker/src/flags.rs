//! The daemon's command line (ADR-0007, 2026-10-31 amendment).
//!
//! Parsed by clap, like the `horde` CLI and `horde-server`: `--help` and
//! `--version` exist, refusals name the flag, and no value may start with
//! `-`, so a forgotten value never swallows the next flag as a path. The
//! environment is read through an injected lookup rather than clap's `env`
//! so every precedence row is testable without mutating the process
//! environment.
//!
//! Precedence for where to dial: `--server`, then `HORDE_SERVER`, then the
//! endpoint the identity dir stored at join (resolved at startup, where
//! the identity dir is read). The join token comes from `--join`, then
//! `--join-file`, then `HORDE_JOIN_TOKEN` — the environment one is
//! *ambient*: it is redeemed when there is nothing enrolled yet and
//! ignored over an existing identity, because a variable left set in a
//! supervisor's unit must not fail every restart after the first.

use std::path::PathBuf;

use clap::builder::NonEmptyStringValueParser;

/// The journal's file name inside the state directory.
pub const JOURNAL_FILE: &str = "journal.sqlite3";

/// The identity directory inside the state directory.
pub const IDENTITY_DIR: &str = "identity";

/// A provider harness the daemon can register.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterChoice {
    /// The scripted hermetic provider (TESTING.md §3).
    Fake,
    /// The Claude Code harness.
    Claude,
    /// The Codex app-server harness.
    Codex,
}

impl AdapterChoice {
    /// Parses one `--adapter` value, refusing anything unknown by name.
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "fake" => Ok(Self::Fake),
            "claude" => Ok(Self::Claude),
            "codex" => Ok(Self::Codex),
            other => Err(format!(
                "--adapter {other} is not one of fake, claude, codex"
            )),
        }
    }
}

/// A join token to redeem, and whether it was asked for or merely found in
/// the environment (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinRequest {
    /// The token text, judged at redeem.
    pub token: String,
    /// Found in `HORDE_JOIN_TOKEN` rather than named on the command line:
    /// redeemed only when nothing is enrolled yet.
    pub ambient: bool,
}

impl JoinRequest {
    /// A token the command line named (`--join` or `--join-file`).
    #[must_use]
    pub fn explicit(token: String) -> Self {
        Self {
            token,
            ambient: false,
        }
    }

    /// A token found in the environment.
    #[must_use]
    pub fn ambient(token: String) -> Self {
        Self {
            token,
            ambient: true,
        }
    }
}

/// Everything one command line asks the daemon to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    /// The Server's main (mTLS) endpoint, when `--server` or
    /// `HORDE_SERVER` named one; otherwise startup reads the endpoint the
    /// identity dir stored at join.
    pub server: Option<String>,
    /// Holds `identity/` and the journal. Losing it loses both.
    pub state_dir: PathBuf,
    /// A one-time join token (`horde://join/<host:port>/<secret>.<fingerprint>`,
    /// ADR-0007) to redeem first: the enrollment listener, the secret, and
    /// the pinned server fingerprint in one string.
    pub join: Option<JoinRequest>,
    /// The display name, and the plaintext worker id under `--insecure`.
    /// An enrolled identity's principal always outranks it.
    pub name: Option<String>,
    /// Which harnesses to register, in order; the first is the default
    /// provider for spawns that name none.
    pub adapters: Vec<AdapterChoice>,
    /// A JSON script for the fake adapter; absent means the echo script.
    pub fake_script: Option<PathBuf>,
    /// The fake adapter's login command (DESIGN §4's relay, hermetically):
    /// a program the login PTY spawns instead of a real provider CLI.
    pub fake_login: Option<PathBuf>,
    /// The browser-facing callback port the fake login recipe declares —
    /// what the client binds exactly and the URL the command prints names.
    pub fake_login_callback_port: Option<u16>,
    /// Where the fake login command actually listens on this machine, when
    /// it differs from the callback port. The hermetic e2e needs the split:
    /// on one machine, one port cannot be both the client's listener and
    /// the worker's.
    pub fake_login_listen_port: Option<u16>,
    /// Enables workspace support rooted here.
    pub workspace_root: Option<PathBuf>,
    /// The git Relay payload shares stage to and fetch through.
    pub relay: Option<String>,
    /// The workspace scan's machine-local settings (ADR-0014's
    /// `worker.workspace.*`): defaults, the config file, then flags.
    pub housekeeping: crate::runtime::HousekeepingConfig,
    /// The codex sandbox tier per permission mode (`codex.sandbox.*`).
    pub sandbox: horde_adapter_codex::SandboxPolicy,
    /// ADR-0015's `logs.*` keys, for the logs work (W12h) to consume.
    pub logs: horde_config::LogKeys,
    /// Every configured key with the layer that decided it (ADR-0016).
    pub provenance: horde_config::Provenance,
    /// Placement tags this Worker advertises (`--tag`, repeatable).
    pub tags: Vec<String>,
    /// Remote URLs this machine's credentials can reach
    /// (`--advertise-remote`, repeatable).
    pub advertised_remotes: Vec<String>,
    /// Dial plaintext with no identity — the in-process-test-server shape.
    pub insecure: bool,
    /// The codex `approvalsReviewer`, already validated at parse: only
    /// `"user"` survives [`horde_adapter_codex::approvals_reviewer_from_config`].
    /// `auto_review` is the transcript-23 self-approval hazard and is refused
    /// before any process starts.
    pub codex_approvals_reviewer: Option<String>,
}

impl WorkerConfig {
    /// The rules that make a parse a usable configuration.
    ///
    /// Each is a refusal rather than a repair: an insecure join is a
    /// contradiction (the point of joining is the certificate), a fake
    /// script without the fake adapter configures nothing, and a scheme that
    /// disagrees with the security mode would otherwise fail later with a
    /// TLS error that names neither flag.
    fn check_shape(&self) -> Result<(), String> {
        let explicit_join = self.join.as_ref().is_some_and(|join| !join.ambient);
        if explicit_join && self.insecure {
            return Err("--join enrolls an identity; it cannot combine with --insecure".to_owned());
        }
        self.check_fake_shape()?;
        if self.codex_approvals_reviewer.is_some() && !self.adapters.contains(&AdapterChoice::Codex)
        {
            return Err("--codex-approvals-reviewer needs --adapter codex".to_owned());
        }
        self.check_scheme()
    }

    /// The server's scheme must agree with the security mode: `--insecure`
    /// needs an explicit `http://` server, and an enrolled dial `https://`.
    fn check_scheme(&self) -> Result<(), String> {
        match (&self.server, self.insecure) {
            (Some(server), true) if server.starts_with("http://") => Ok(()),
            (server, true) => Err(format!(
                "--insecure dials plaintext, so --server must be http://…, not {}",
                server.as_deref().unwrap_or("nothing")
            )),
            (Some(server), false) if !server.starts_with("https://") => Err(format!(
                "an enrolled dial is mTLS, so --server must be https://…, not {server} \
                 (or pass --insecure for a plaintext test server)"
            )),
            _ => Ok(()),
        }
    }

    /// The fake adapter's own shape rules: scripts and login recipes
    /// configure nothing without the adapter they belong to, and the login
    /// ports form a dependency chain (listen needs callback needs command).
    fn check_fake_shape(&self) -> Result<(), String> {
        if self.fake_script.is_some() && !self.adapters.contains(&AdapterChoice::Fake) {
            return Err("--fake-script needs --adapter fake".to_owned());
        }
        if self.fake_login.is_some() && !self.adapters.contains(&AdapterChoice::Fake) {
            return Err("--fake-login needs --adapter fake".to_owned());
        }
        if self.fake_login_callback_port.is_some() && self.fake_login.is_none() {
            return Err("--fake-login-callback-port needs --fake-login".to_owned());
        }
        if self.fake_login_listen_port.is_some() && self.fake_login_callback_port.is_none() {
            return Err("--fake-login-listen-port needs --fake-login-callback-port".to_owned());
        }
        Ok(())
    }

    /// Where this configuration keeps its identity.
    #[must_use]
    pub fn identity_dir(&self) -> PathBuf {
        self.state_dir.join(IDENTITY_DIR)
    }

    /// Where this configuration keeps its journal.
    #[must_use]
    pub fn journal_path(&self) -> PathBuf {
        self.state_dir.join(JOURNAL_FILE)
    }
}

/// The environment override for `--server`.
pub const SERVER_VAR: &str = "HORDE_SERVER";

/// The ambient join token (see the module docs).
pub const JOIN_TOKEN_VAR: &str = "HORDE_JOIN_TOKEN";

/// The state directory under `$HOME` when `--state-dir` does not say — the
/// `worker/` that `horde up` lays out under `~/.horde`, and never the
/// working directory.
pub const DEFAULT_STATE_SUBDIR: &str = ".horde/worker";

/// What one command line asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parsed {
    /// Run the daemon with this configuration.
    Run(Box<WorkerConfig>),
    /// `--help` or `--version`: text for stdout, then a successful exit.
    Print(String),
}

#[derive(clap::Parser, Debug)]
#[command(
    name = "horde-worker",
    version,
    about = "The Horde worker daemon: supervises agent harnesses for a server."
)]
struct WorkerArgs {
    /// The server's main mTLS endpoint (`https://host:port`). Overrides
    /// HORDE_SERVER, which overrides the endpoint a join stored.
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    server: Option<String>,

    /// Holds `identity/` and the journal. Defaults to `~/.horde/worker`,
    /// created owner-only.
    #[arg(long)]
    state_dir: Option<PathBuf>,

    /// A one-time join token (`horde://join/<host:port>/<secret>.<fingerprint>`)
    /// to redeem first. Prefer --join-file or HORDE_JOIN_TOKEN: argv is
    /// visible to every user on the machine.
    #[arg(long, value_parser = NonEmptyStringValueParser::new(), conflicts_with = "join_file")]
    join: Option<String>,

    /// A file holding the join token.
    #[arg(long)]
    join_file: Option<PathBuf>,

    /// The display name (and the plaintext worker id under --insecure). An
    /// enrolled identity's principal always outranks it.
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    name: Option<String>,

    /// A harness to register (`fake`, `claude`, `codex`); repeatable, the
    /// first is the default provider.
    #[arg(long = "adapter", value_parser = AdapterChoice::parse)]
    adapters: Vec<AdapterChoice>,

    /// A JSON script for the fake adapter.
    #[arg(long)]
    fake_script: Option<PathBuf>,

    /// The fake adapter's login command.
    #[arg(long)]
    fake_login: Option<PathBuf>,

    /// The browser-facing callback port the fake login declares.
    #[arg(long)]
    fake_login_callback_port: Option<u16>,

    /// Where the fake login actually listens, when it differs.
    #[arg(long)]
    fake_login_listen_port: Option<u16>,

    /// Enables workspace support rooted here; overrides
    /// `worker.workspace.root`.
    #[arg(long)]
    workspace_root: Option<PathBuf>,

    /// The config file. Defaults to `<state-dir>/config.toml`, which may be
    /// absent; a file named here must exist.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Print the settings these flags and the config file resolve to, and
    /// exit (ADR-0016).
    #[arg(long)]
    show_config: bool,

    /// With --show-config, name the layer each value came from.
    #[arg(long, requires = "show_config")]
    origin: bool,

    /// The git Relay payload shares stage to and fetch through.
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    relay: Option<String>,

    /// How often the workspace scan runs (idle and orphan reclaim, grace
    /// expiry, disk pressure), e.g. 60s (default 60s).
    #[arg(long, value_parser = seconds)]
    workspace_scan_interval: Option<std::time::Duration>,

    /// How often archive pins are checked against origin and the Relay (6h).
    #[arg(long, value_parser = seconds)]
    workspace_pin_scan_interval: Option<std::time::Duration>,

    /// How often the whole Instance set is re-reported to the Server (5m).
    #[arg(long, value_parser = seconds)]
    workspace_report_interval: Option<std::time::Duration>,

    /// Free space below which disk-pressure reclaim starts: a percentage,
    /// a size, or both (the larger wins); default "10%,20GiB".
    #[arg(long, value_parser = watermark)]
    workspace_disk_low_watermark: Option<crate::runtime::Watermark>,

    /// Free space disk-pressure reclaim stops at; above the low mark (15%).
    #[arg(long, value_parser = watermark)]
    workspace_disk_high_watermark: Option<crate::runtime::Watermark>,

    /// A placement tag this worker advertises; repeatable.
    #[arg(long = "tag", value_parser = NonEmptyStringValueParser::new())]
    tags: Vec<String>,

    /// A remote URL this machine's credentials can reach; repeatable.
    #[arg(long = "advertise-remote", value_parser = NonEmptyStringValueParser::new())]
    advertised_remotes: Vec<String>,

    /// Dial plaintext with no identity — the in-process-test-server shape.
    #[arg(long)]
    insecure: bool,

    /// The codex `approvalsReviewer`; only `user` is admitted.
    #[arg(long, value_parser = reviewer)]
    codex_approvals_reviewer: Option<String>,
}

/// A duration flag (`60s`, `5m`, `6h`); never zero, since a zero interval
/// would spin.
fn seconds(value: &str) -> Result<std::time::Duration, String> {
    let duration = horde_proto::units::parse_duration(value)?;
    if duration.is_zero() {
        return Err(format!("{value:?} must be longer than zero"));
    }
    Ok(duration)
}

/// A watermark flag: `10%`, `20GiB`, or both comma-separated.
fn watermark(value: &str) -> Result<crate::runtime::Watermark, String> {
    let (basis_points, bytes) = horde_proto::units::parse_threshold(value)?;
    Ok(crate::runtime::Watermark {
        basis_points,
        bytes,
    })
}

/// Rejected at parse, not at spawn: `auto_review` had codex's own subagent
/// approve a sandbox escalation and run the write under a read-only sandbox
/// (transcript 23). The validator admits only "user".
fn reviewer(value: &str) -> Result<String, String> {
    horde_adapter_codex::approvals_reviewer_from_config(Some(value))?;
    Ok(value.to_owned())
}

/// Parses the command line against an environment and a `$HOME`.
///
/// # Errors
///
/// A human-readable refusal naming the flag that was wrong.
pub fn parse(
    arguments: impl IntoIterator<Item = String>,
    env: impl Fn(&str) -> Option<String>,
    home: Option<PathBuf>,
) -> Result<Parsed, String> {
    use clap::Parser;
    let program = std::iter::once("horde-worker".to_owned());
    let args = match WorkerArgs::try_parse_from(program.chain(arguments)) {
        Ok(args) => args,
        Err(error) if !error.use_stderr() => return Ok(Parsed::Print(error.to_string())),
        Err(error) => return Err(error.to_string()),
    };
    let join = join_request(args.join, args.join_file, env(JOIN_TOKEN_VAR))?;
    let state_dir = match (args.state_dir, home) {
        (Some(dir), _) => dir,
        (None, Some(home)) => home.join(DEFAULT_STATE_SUBDIR),
        (None, None) => return Err("HOME is unset, so pass --state-dir".to_owned()),
    };
    let document = horde_config::load(
        horde_config::Role::Worker,
        args.config.as_deref(),
        &state_dir.join(horde_config::Role::FILE_NAME),
    )?;
    let settings = crate::config::WorkerSettings::resolve(
        document.as_ref(),
        &crate::config::WorkerFlags {
            workspace_root: args.workspace_root,
            scan_interval: args.workspace_scan_interval,
            pin_scan_interval: args.workspace_pin_scan_interval,
            report_interval: args.workspace_report_interval,
            disk_low_watermark: args.workspace_disk_low_watermark,
            disk_high_watermark: args.workspace_disk_high_watermark,
        },
        &env,
    )?;
    if args.show_config {
        return Ok(Parsed::Print(settings.provenance.render(args.origin)));
    }
    let config = WorkerConfig {
        server: args.server.or_else(|| env(SERVER_VAR)),
        state_dir,
        join,
        name: args.name,
        adapters: distinct(args.adapters)?,
        fake_script: args.fake_script,
        fake_login: args.fake_login,
        fake_login_callback_port: args.fake_login_callback_port,
        fake_login_listen_port: args.fake_login_listen_port,
        workspace_root: settings.workspace_root,
        relay: args.relay,
        housekeeping: settings.housekeeping,
        sandbox: settings.sandbox,
        logs: settings.logs,
        provenance: settings.provenance,
        tags: args.tags,
        advertised_remotes: args.advertised_remotes,
        insecure: args.insecure,
        codex_approvals_reviewer: args.codex_approvals_reviewer,
    };
    config.check_shape()?;
    Ok(Parsed::Run(Box::new(config)))
}

/// The join token by precedence: the flag, the file, the ambient variable.
fn join_request(
    flag: Option<String>,
    file: Option<PathBuf>,
    ambient: Option<String>,
) -> Result<Option<JoinRequest>, String> {
    if let Some(token) = flag {
        return Ok(Some(JoinRequest::explicit(token)));
    }
    if let Some(path) = file {
        let token = std::fs::read_to_string(&path).map_err(|error| {
            format!(
                "could not read the join token file {}: {error}",
                path.display()
            )
        })?;
        return Ok(Some(JoinRequest::explicit(token.trim().to_owned())));
    }
    Ok(ambient
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
        .map(JoinRequest::ambient))
}

/// The adapters in order, each at most once.
fn distinct(adapters: Vec<AdapterChoice>) -> Result<Vec<AdapterChoice>, String> {
    let mut seen = Vec::with_capacity(adapters.len());
    for adapter in adapters {
        if seen.contains(&adapter) {
            return Err(format!("--adapter {adapter:?} was given twice").to_lowercase());
        }
        seen.push(adapter);
    }
    Ok(seen)
}

#[cfg(test)]
mod tests {
    use super::{JOIN_TOKEN_VAR, JoinRequest, Parsed, SERVER_VAR, WorkerConfig, parse};

    fn parsed_with(arguments: &[&str], env: &[(&str, &str)]) -> Result<WorkerConfig, String> {
        let env: Vec<(String, String)> = env
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        match parse(
            arguments.iter().map(|s| (*s).to_owned()),
            |name| env.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone()),
            Some("/home/me".into()),
        )? {
            Parsed::Run(config) => Ok(*config),
            Parsed::Print(text) => Err(format!("printed: {text}")),
        }
    }

    /// The workspace intervals parse as durations, default to the ADR's
    /// values, and a zero interval — which would spin the scan — is refused.
    #[test]
    fn workspace_intervals_parse_default_and_refuse_zero() {
        let defaults = parsed_with(&[], &[]).expect("parses");
        assert_eq!(
            defaults.housekeeping,
            crate::runtime::HousekeepingConfig::default()
        );
        let set = parsed_with(
            &[
                "--workspace-scan-interval",
                "90s",
                "--workspace-pin-scan-interval",
                "2h",
                "--workspace-report-interval",
                "5m",
            ],
            &[],
        )
        .expect("parses");
        assert_eq!(
            set.housekeeping.scan_interval,
            std::time::Duration::from_secs(90)
        );
        assert_eq!(
            set.housekeeping.pin_scan_interval,
            std::time::Duration::from_secs(7_200)
        );
        assert_eq!(
            set.housekeeping.report_interval,
            std::time::Duration::from_secs(300)
        );
        let zero = parsed_with(&["--workspace-scan-interval", "0s"], &[])
            .expect_err("a zero interval would spin");
        assert!(zero.contains("longer than zero"), "{zero}");
    }

    /// Where to dial, by precedence: the flag over the variable; neither
    /// leaves it to the stored endpoint at startup. The state dir defaults
    /// under HOME, never the working directory.
    #[test]
    fn the_server_is_flag_then_environment_and_state_is_under_home() {
        let both = parsed_with(
            &["--server", "https://flag:1"],
            &[(SERVER_VAR, "https://env:1")],
        )
        .expect("parses");
        assert_eq!(both.server.as_deref(), Some("https://flag:1"));
        let env = parsed_with(&[], &[(SERVER_VAR, "https://env:1")]).expect("parses");
        assert_eq!(env.server.as_deref(), Some("https://env:1"));
        let neither = parsed_with(&[], &[]).expect("parses");
        assert_eq!(neither.server, None, "left to the stored endpoint");
        assert_eq!(
            neither.state_dir,
            std::path::PathBuf::from("/home/me/.horde/worker")
        );
        let refused = parsed_with(&[], &[(SERVER_VAR, "http://env:1")])
            .expect_err("the variable is held to the flag's shape rules");
        assert!(refused.contains("https://"), "{refused}");
    }

    /// `--insecure` dials plaintext, so only an `http://` server takes it:
    /// an `https://` server, a schemeless one, or none at all is refused by
    /// name rather than left to fail later as a TLS error.
    #[test]
    fn an_insecure_dial_takes_only_an_http_server() {
        let plaintext =
            parsed_with(&["--insecure", "--server", "http://lab:1"], &[]).expect("parses");
        assert_eq!(plaintext.server.as_deref(), Some("http://lab:1"));
        assert!(plaintext.insecure);

        for server in ["https://lab:1", "lab:1"] {
            let refused = parsed_with(&["--insecure", "--server", server], &[])
                .expect_err("an insecure dial needs an http:// server");
            assert!(
                refused.contains("--insecure") && refused.contains(server),
                "{server}: {refused}"
            );
        }
        let nowhere =
            parsed_with(&["--insecure"], &[]).expect_err("an insecure dial needs a server");
        assert!(nowhere.contains("not nothing"), "{nowhere}");
    }

    /// The token by precedence — flag, file, ambient variable — and only
    /// the variable is ambient. A blank variable is no token.
    #[test]
    fn the_join_token_is_flag_then_file_then_ambient_environment() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let file = dir.path().join("token");
        std::fs::write(&file, "horde://join/f:1/c.d\n").expect("writes");
        let file = file.to_string_lossy().into_owned();
        let env = [(JOIN_TOKEN_VAR, "horde://join/e:1/c.d")];

        let flag = parsed_with(&["--join", "horde://join/a:1/c.d"], &env).expect("parses");
        assert_eq!(
            flag.join,
            Some(JoinRequest::explicit("horde://join/a:1/c.d".to_owned()))
        );
        let from_file = parsed_with(&["--join-file", &file], &env).expect("parses");
        assert_eq!(
            from_file.join,
            Some(JoinRequest::explicit("horde://join/f:1/c.d".to_owned())),
            "the file's trailing newline is packaging"
        );
        let ambient = parsed_with(&[], &env).expect("parses");
        assert_eq!(
            ambient.join,
            Some(JoinRequest::ambient("horde://join/e:1/c.d".to_owned()))
        );
        let blank = parsed_with(&[], &[(JOIN_TOKEN_VAR, "  ")]).expect("parses");
        assert_eq!(blank.join, None);

        let both = parsed_with(&["--join", "x", "--join-file", &file], &[])
            .expect_err("one source at a time");
        assert!(both.contains("--join-file"), "{both}");
        let missing = parsed_with(&["--join-file", "/nonexistent/token"], &[])
            .expect_err("an unreadable file is a refusal");
        assert!(missing.contains("/nonexistent/token"), "{missing}");
    }

    /// Help and version print; no HOME and no `--state-dir` is a refusal;
    /// a forgotten value never takes the next flag as a path.
    #[test]
    fn help_prints_and_paths_never_start_with_a_dash() {
        for flag in ["--help", "-h", "--version"] {
            let printed = parsed_with(&[flag], &[]).expect_err("prints instead of running");
            assert!(printed.contains("horde-worker"), "{flag}: {printed}");
        }
        let homeless = parse(std::iter::empty(), |_| None, None).expect_err("nowhere to default");
        assert!(homeless.contains("--state-dir"), "{homeless}");
        let swallowed =
            parsed_with(&["--state-dir", "--insecure"], &[]).expect_err("no dash-prefixed path");
        assert!(swallowed.contains("--state-dir"), "{swallowed}");
    }

    /// The config file in the state dir is the layer between the defaults
    /// and the flags (ADR-0016): it sets what no flag does, a flag beats it,
    /// and `--show-config --origin` prints each value with its layer.
    #[test]
    fn the_state_dirs_config_file_sits_between_the_defaults_and_the_flags() {
        let state = tempfile::tempdir().expect("a temp dir");
        let dir = state.path().to_string_lossy().into_owned();
        let file = state.path().join("config.toml");
        std::fs::write(
            &file,
            "[worker.workspace]\nroot = \"/srv/ws\"\nscan_interval = \"2m\"\n\
             [codex.sandbox]\nbuilder = \"workspace-write\"\n",
        )
        .expect("writes");

        let filed = parsed_with(&["--state-dir", &dir], &[]).expect("parses");
        assert_eq!(filed.workspace_root, Some("/srv/ws".into()));
        assert_eq!(filed.housekeeping.scan_interval.as_secs(), 120);
        assert_eq!(
            filed.sandbox.tier_for("builder").tier,
            horde_adapter_codex::SandboxTier::WorkspaceWrite
        );

        let flagged = parsed_with(
            &[
                "--state-dir",
                &dir,
                "--workspace-scan-interval",
                "5s",
                "--workspace-root",
                "/x",
            ],
            &[],
        )
        .expect("parses");
        assert_eq!(flagged.housekeeping.scan_interval.as_secs(), 5);
        assert_eq!(flagged.workspace_root, Some("/x".into()));

        let shown = parsed_with(
            &[
                "--state-dir",
                &dir,
                "--workspace-scan-interval",
                "5s",
                "--show-config",
                "--origin",
            ],
            &[("HORDE_LOG", "warn")],
        )
        .expect_err("prints instead of running");
        let file_origin = format!("# file {}", file.display());
        for line in [
            "worker.workspace.scan_interval = 5s  # flag --workspace-scan-interval".to_owned(),
            format!("worker.workspace.root = \"/srv/ws\"  {file_origin}"),
            "worker.workspace.report_interval = 5m  # default".to_owned(),
            "logs.level = \"warn\"  # env HORDE_LOG".to_owned(),
        ] {
            assert!(shown.contains(&line), "{line} in:\n{shown}");
        }
        let plain = parsed_with(&["--state-dir", &dir, "--show-config"], &[])
            .expect_err("prints instead of running");
        assert!(
            plain.contains("worker.workspace.scan_interval = 2m\n"),
            "{plain}"
        );
        assert!(!plain.contains('#'), "no origins without --origin: {plain}");
    }

    /// A broken or missing config file refuses the start by path.
    #[test]
    fn a_bad_or_missing_config_file_refuses_the_start() {
        let state = tempfile::tempdir().expect("a temp dir");
        let dir = state.path().to_string_lossy().into_owned();
        std::fs::write(
            state.path().join("config.toml"),
            "[worker.workspace]\nscan = 1\n",
        )
        .expect("writes");
        let refused = parsed_with(&["--state-dir", &dir], &[]).expect_err("an unknown key");
        assert!(
            refused.contains("unknown key worker.workspace.scan"),
            "{refused}"
        );
        let missing = parsed_with(&["--config", "/nonexistent/worker.toml"], &[])
            .expect_err("a named file must exist");
        assert!(missing.contains("/nonexistent/worker.toml"), "{missing}");
        let origin_alone = parsed_with(&["--origin"], &[]).expect_err("needs --show-config");
        assert!(origin_alone.contains("--show-config"), "{origin_alone}");
    }

    /// An empty value is no value: every string flag refuses one by name,
    /// rather than dialing, joining, or naming "".
    #[test]
    fn empty_string_values_are_refused_by_name() {
        for flag in [
            "--server",
            "--join",
            "--name",
            "--relay",
            "--tag",
            "--advertise-remote",
        ] {
            let refused = parsed_with(&[flag, ""], &[]).expect_err("an empty value");
            assert!(refused.contains(flag), "{flag}: {refused}");
        }
    }
}
