//! `horde doctor`: this machine's setup, checked end to end, in the order
//! a connection depends on it — identity, endpoint, certificates, the
//! server itself (a real mTLS `Hello`), then the local worker's harness
//! binaries, state-directory permissions and disk.
//!
//! Every check carries a verdict and, when it is not `ok`, the fix in
//! words; `--json` carries the same checks as data. Everything the checks
//! read from the process (HOME, PATH, the clock) is gathered once in
//! [`Inputs`], so the checks themselves are tested without touching the
//! process environment.

use std::path::{Path, PathBuf};

use serde_json::json;

use crate::{ClientError, Format};

/// How long the reachability check may take, connect and `Hello` together.
const REACH_CEILING: std::time::Duration = std::time::Duration::from_secs(5);

/// A certificate this close to expiry is a warning.
const EXPIRY_WARNING_SECS: i64 = 7 * 86_400;

/// Free space below this on a state directory's filesystem is a warning…
const LOW_DISK_KIB: u64 = 2 * 1024 * 1024;

/// …and below this, a failure: journals and checkouts stop fitting.
const CRITICAL_DISK_KIB: u64 = 256 * 1024;

/// The harness binaries looked for when `--harness` names none.
const DEFAULT_HARNESSES: &[&str] = &["claude", "codex"];

/// `horde doctor`'s flags.
#[derive(clap::Args, Debug)]
pub struct DoctorArgs {
    /// A harness binary the local worker runs, looked for on PATH;
    /// repeatable. Default: claude and codex.
    #[arg(long = "harness")]
    pub harnesses: Vec<String>,
    /// A state directory to check for permissions and free disk;
    /// repeatable. Default: `~/.horde/server` and `~/.horde/worker`, where
    /// they exist, beside the identity dir.
    #[arg(long = "state-dir")]
    pub state_dirs: Vec<PathBuf>,
}

/// One check's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Ok,
    Warn,
    Fail,
}

impl Verdict {
    fn word(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Fail => "FAIL",
        }
    }
}

/// One line of the report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    pub name: String,
    pub verdict: Verdict,
    pub detail: String,
    /// What to do, when the verdict is not `ok`.
    pub fix: Option<String>,
}

impl Check {
    fn ok(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_owned(),
            verdict: Verdict::Ok,
            detail: detail.into(),
            fix: None,
        }
    }

    fn not_ok(name: &str, verdict: Verdict, detail: impl Into<String>, fix: &str) -> Self {
        Self {
            name: name.to_owned(),
            verdict,
            detail: detail.into(),
            fix: Some(fix.to_owned()),
        }
    }
}

/// Everything the checks read from outside: gathered once by [`run`],
/// passed explicitly everywhere else.
#[derive(Debug, Clone)]
pub struct Inputs {
    /// `--endpoint` / `HORDE_ENDPOINT`, as clap merged them.
    pub endpoint: Option<String>,
    /// `--identity` / `HORDE_IDENTITY`.
    pub identity: Option<PathBuf>,
    pub home: Option<PathBuf>,
    /// The `PATH` harnesses are looked for on.
    pub path: Option<std::ffi::OsString>,
    pub harnesses: Vec<String>,
    pub state_dirs: Vec<PathBuf>,
    pub now_unix_secs: i64,
}

/// Runs `horde doctor`: prints the report, and fails (non-zero exit) when
/// any check failed.
///
/// # Errors
///
/// [`ClientError::Unhealthy`] naming how many checks failed.
pub async fn run(
    cli: &crate::cli::Cli,
    args: &DoctorArgs,
    format: Format,
) -> Result<(), ClientError> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let state_dirs = if args.state_dirs.is_empty() {
        home.iter()
            .flat_map(|home| ["server", "worker"].map(|dir| home.join(".horde").join(dir)))
            .filter(|dir| dir.is_dir())
            .collect()
    } else {
        args.state_dirs.clone()
    };
    let inputs = Inputs {
        endpoint: cli.endpoint.clone(),
        identity: cli.identity.clone(),
        home,
        path: std::env::var_os("PATH"),
        harnesses: args.harnesses.clone(),
        state_dirs,
        now_unix_secs: unix_now_secs(),
    };
    let checks = report(&inputs).await;
    println!("{}", render(&checks, format));
    verdict_of(&checks)
}

fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
        })
}

/// `Ok` unless a check failed; warnings alone still pass.
///
/// # Errors
///
/// [`ClientError::Unhealthy`] naming how many checks failed.
pub fn verdict_of(checks: &[Check]) -> Result<(), ClientError> {
    let failures = checks
        .iter()
        .filter(|check| check.verdict == Verdict::Fail)
        .count();
    if failures == 0 {
        Ok(())
    } else {
        Err(ClientError::Unhealthy { failures })
    }
}

/// Every check, in dependency order.
pub async fn report(inputs: &Inputs) -> Vec<Check> {
    let mut checks = Vec::new();
    let identity_dir = inputs
        .identity
        .clone()
        .unwrap_or_else(|| crate::cli::default_identity_dir(inputs.home.clone()));
    let identity = identity_check(&identity_dir, &mut checks);
    if let Some(identity) = &identity {
        checks.extend(certificate_checks(identity, inputs.now_unix_secs));
    }
    let resolved = endpoint_check(inputs, &mut checks);
    if let Some(resolved) = &resolved {
        checks.push(server_check(resolved).await);
    }
    checks.extend(harness_checks(&inputs.harnesses, inputs.path.as_deref()));
    let mut dirs = vec![identity_dir];
    dirs.extend(inputs.state_dirs.iter().cloned());
    for dir in &dirs {
        checks.extend(state_dir_checks(dir));
    }
    checks
}

/// The identity dir: present and loadable, naming its principal.
fn identity_check(dir: &Path, checks: &mut Vec<Check>) -> Option<horde_identity::StoredIdentity> {
    if !horde_identity::StoredIdentity::exists(dir) {
        checks.push(Check::not_ok(
            "identity",
            Verdict::Fail,
            format!("{} holds no identity", dir.display()),
            "join a server with `horde enroll join <token>`, stand one up here with `horde up`, \
             or point --identity at where the enrollment landed",
        ));
        return None;
    }
    let loaded = horde_identity::StoredIdentity::load(dir).and_then(|identity| {
        let principal = horde_identity::principal_of_pem(&identity.certificate_pem)?;
        Ok((identity, principal))
    });
    match loaded {
        Ok((identity, principal)) => {
            checks.push(Check::ok(
                "identity",
                format!("{} holds {}", dir.display(), principal.common_name()),
            ));
            Some(identity)
        }
        Err(error) => {
            checks.push(Check::not_ok(
                "identity",
                Verdict::Fail,
                format!("{} does not load: {error}", dir.display()),
                "re-join with `horde enroll join <token> --force` (the old identity is backed up)",
            ));
            None
        }
    }
}

/// Both certificates the identity holds — its own and the CA it trusts.
fn certificate_checks(identity: &horde_identity::StoredIdentity, now: i64) -> Vec<Check> {
    vec![
        certificate_check("certificate", &identity.certificate_pem, now),
        certificate_check("ca", &identity.ca_pem, now),
    ]
}

/// One certificate's validity window against `now`.
#[must_use]
pub fn certificate_check(name: &str, pem: &str, now: i64) -> Check {
    let renew = "re-join with `horde enroll join <token> --force` for a fresh certificate";
    match horde_identity::validity_of_pem(pem) {
        Err(error) => Check::not_ok(name, Verdict::Fail, format!("unreadable: {error}"), renew),
        Ok((not_before, _)) if now < not_before => Check::not_ok(
            name,
            Verdict::Fail,
            format!("not valid for another {}s", not_before - now),
            "check this machine's clock",
        ),
        Ok((_, not_after)) if now >= not_after => Check::not_ok(
            name,
            Verdict::Fail,
            format!("expired {} days ago", (now - not_after) / 86_400),
            renew,
        ),
        Ok((_, not_after)) if not_after - now < EXPIRY_WARNING_SECS => Check::not_ok(
            name,
            Verdict::Warn,
            format!("expires in {}h", (not_after - now) / 3_600),
            renew,
        ),
        Ok((_, not_after)) => Check::ok(
            name,
            format!("valid for {} more days", (not_after - now) / 86_400),
        ),
    }
}

/// Where this invocation would dial, by the same precedence every verb
/// uses.
fn endpoint_check(
    inputs: &Inputs,
    checks: &mut Vec<Check>,
) -> Option<crate::cli::ResolvedConnection> {
    match crate::cli::resolve_connection(
        inputs.endpoint.as_deref(),
        inputs.identity.as_deref(),
        inputs.home.clone(),
    ) {
        Ok(resolved) => {
            checks.push(Check::ok("endpoint", resolved.endpoint.clone()));
            Some(resolved)
        }
        Err(error) => {
            checks.push(Check::not_ok(
                "endpoint",
                Verdict::Fail,
                "no server to dial",
                &error.to_string(),
            ));
            None
        }
    }
}

/// A real connection and `Hello`, as every verb would make it, bounded by
/// [`REACH_CEILING`].
async fn server_check(resolved: &crate::cli::ResolvedConnection) -> Check {
    let attempt = async {
        let mut client = crate::cli::connect(resolved).await?;
        client.hello(env!("CARGO_PKG_VERSION")).await
    };
    let outcome = tokio::time::timeout(REACH_CEILING, attempt)
        .await
        .unwrap_or_else(|_| {
            Err(ClientError::Rpc(tonic::Status::deadline_exceeded(format!(
                "no answer within {}s",
                REACH_CEILING.as_secs()
            ))))
        });
    match outcome {
        Ok(server) => Check::ok(
            "server",
            format!(
                "horde-server {} answered; you are {}",
                server.semver,
                if server.principal.is_empty() {
                    "unauthenticated"
                } else {
                    server.principal.as_str()
                }
            ),
        ),
        Err(error) => Check::not_ok(
            "server",
            Verdict::Fail,
            error.report().replace('\n', "; "),
            "start the server (`horde up` on its machine), or fix --endpoint; a certificate \
             error means this identity was joined to another server",
        ),
    }
}

/// Each harness binary, looked for on `path`.
#[must_use]
pub fn harness_checks(harnesses: &[String], path: Option<&std::ffi::OsStr>) -> Vec<Check> {
    let wanted: Vec<String> = if harnesses.is_empty() {
        DEFAULT_HARNESSES
            .iter()
            .map(|&name| name.to_owned())
            .collect()
    } else {
        harnesses.to_vec()
    };
    wanted
        .iter()
        .map(|name| {
            let label = format!("harness {name}");
            match on_path(name, path) {
                Some(found) => Check::ok(&label, found.display().to_string()),
                None => Check::not_ok(
                    &label,
                    Verdict::Warn,
                    format!("{name} is not on PATH"),
                    "install it (or put it on PATH) on this worker, or run the worker without \
                     that --adapter; placement skips workers lacking a harness",
                ),
            }
        })
        .collect()
}

/// The first executable file named `name` in `path`'s directories.
pub(crate) fn on_path(name: &str, path: Option<&std::ffi::OsStr>) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    std::env::split_paths(path?)
        .map(|dir| dir.join(name))
        .find(|candidate| {
            std::fs::metadata(candidate)
                .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        })
}

/// One state directory's permissions and free disk.
#[must_use]
pub fn state_dir_checks(dir: &Path) -> Vec<Check> {
    let label = format!("dir {}", dir.display());
    let Ok(meta) = std::fs::metadata(dir) else {
        return vec![Check::ok(&label, "absent; created owner-only on first use")];
    };
    vec![permission_check(&label, dir, &meta), disk_check(dir)]
}

fn permission_check(label: &str, dir: &Path, meta: &std::fs::Metadata) -> Check {
    use std::os::unix::fs::PermissionsExt;
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 == 0 {
        Check::ok(label, format!("owner-only ({mode:o})"))
    } else {
        Check::not_ok(
            label,
            Verdict::Warn,
            format!("mode {mode:o} lets other users look inside"),
            &format!("chmod 700 {}", dir.display()),
        )
    }
}

fn disk_check(dir: &Path) -> Check {
    let label = format!("disk {}", dir.display());
    let output = std::process::Command::new("df")
        .arg("-Pk")
        .arg(dir)
        .output();
    let available = output
        .ok()
        .and_then(|out| available_kib(&String::from_utf8_lossy(&out.stdout)));
    disk_verdict(&label, available)
}

/// The verdict on `available` KiB free (`None`: `df` gave no answer).
#[must_use]
pub fn disk_verdict(label: &str, available: Option<u64>) -> Check {
    let free = "free space on that filesystem (old worktrees and checkouts are the usual bulk)";
    match available {
        None => Check::not_ok(
            label,
            Verdict::Warn,
            "could not measure free space",
            "run `df -h` yourself",
        ),
        Some(kib) if kib < CRITICAL_DISK_KIB => Check::not_ok(
            label,
            Verdict::Fail,
            format!("{} MiB free", kib / 1024),
            free,
        ),
        Some(kib) if kib < LOW_DISK_KIB => Check::not_ok(
            label,
            Verdict::Warn,
            format!("{} MiB free", kib / 1024),
            free,
        ),
        Some(kib) => Check::ok(label, format!("{} GiB free", kib / (1024 * 1024))),
    }
}

/// The "Available" column of POSIX `df -Pk` output's data row.
#[must_use]
pub fn available_kib(df_output: &str) -> Option<u64> {
    df_output
        .lines()
        .nth(1)?
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()
}

/// The report in either format.
#[must_use]
pub fn render(checks: &[Check], format: Format) -> String {
    match format {
        Format::Json => json!({
            "healthy": verdict_of(checks).is_ok(),
            "checks": checks.iter().map(|check| json!({
                "name": check.name,
                "verdict": check.verdict.word().to_ascii_lowercase(),
                "detail": check.detail,
                "fix": check.fix,
            })).collect::<Vec<_>>(),
        })
        .to_string(),
        Format::Human => checks
            .iter()
            .map(|check| {
                let line = format!(
                    "{:<4}  {}: {}",
                    check.verdict.word(),
                    check.name,
                    check.detail
                );
                match &check.fix {
                    Some(fix) => format!("{line}\n      fix: {fix}"),
                    None => line,
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

#[cfg(test)]
mod tests;
