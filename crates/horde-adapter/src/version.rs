//! Which harness versions Horde will drive, and how sure it is about each.
//!
//! Both harnesses are moving targets (`claude` ships several releases a week;
//! codex's app-server protocol is generated per release), and every adapter
//! claim is a measurement against particular versions. So each provider
//! states its measurements as a [`SupportTable`], and a launched binary is
//! classified against it before an agent runs on it:
//!
//! * [`SupportVerdict::Supported`] — inside the range the golden transcripts
//!   and live smokes were recorded against. Driven without comment.
//! * [`SupportVerdict::Untested`] — newer than anything measured, between
//!   measurements, or unreadable. Driven, but the Worker journals a warning
//!   on the agent's first turn and `horde doctor` says so, because a protocol
//!   change there would surface as a malformed-stream failure rather than as
//!   a clear refusal.
//! * [`SupportVerdict::Refused`] — older than a version the adapter's claims
//!   depend on. The adapter refuses to spawn, loudly, naming the floor — a
//!   harness that silently lacks the permission route Horde relies on is a
//!   chain that gates nothing.
//!
//! Versions are read from the binary's own `--version` output by scanning for
//! the first dotted `major.minor.patch` token, so a banner change does not
//! break the probe (`claude` prints `2.1.323 (Claude Code)`, codex prints
//! `codex-cli 0.167.0`).

use core::fmt;

/// A harness's `major.minor.patch`, ordered numerically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HarnessVersion {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl HarnessVersion {
    #[must_use]
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// The first `major.minor[.patch]` token in `text`, or `None`.
    ///
    /// Tokens split on whitespace and parentheses; a token with a suffix
    /// (`0.147.0-rc1`) or too many parts is skipped rather than misread, and
    /// the scan moves on.
    #[must_use]
    pub fn parse_first(text: &str) -> Option<Self> {
        text.split(|c: char| c.is_whitespace() || c == '(' || c == ')')
            .find_map(Self::parse_token)
    }

    fn parse_token(token: &str) -> Option<Self> {
        let mut parts = token.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        let patch = match parts.next() {
            None => 0,
            Some(part) => part.parse().ok()?,
        };
        if parts.next().is_some() {
            return None;
        }
        Some(Self::new(major, minor, patch))
    }
}

impl fmt::Display for HarnessVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// How a version stands against a provider's measurements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportVerdict {
    /// Inside the measured range.
    Supported,
    /// Driven, with a warning: newer than, between, or unreadable against
    /// what was measured.
    Untested,
    /// Below the floor the adapter's claims depend on; never driven.
    Refused,
}

impl SupportVerdict {
    /// The one-word name `horde doctor` and the journal use.
    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Untested => "untested",
            Self::Refused => "refused",
        }
    }
}

/// One provider's measured ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportTable {
    /// The provider key, for messages.
    pub provider: &'static str,
    /// Anything below this is refused.
    pub floor: HarnessVersion,
    /// The lowest version the adapter's recordings cover.
    pub tested_from: HarnessVersion,
    /// The highest version the adapter's recordings cover.
    pub tested_through: HarnessVersion,
    /// Why the floor is where it is, for the refusal message.
    pub floor_reason: &'static str,
}

impl SupportTable {
    /// Classifies a version.
    #[must_use]
    pub fn verdict(&self, version: HarnessVersion) -> SupportVerdict {
        if version < self.floor {
            SupportVerdict::Refused
        } else if version >= self.tested_from && version <= self.tested_through {
            SupportVerdict::Supported
        } else {
            SupportVerdict::Untested
        }
    }

    /// Classifies a binary's `--version` output. Output with no readable
    /// version is [`SupportVerdict::Untested`]: it may be a fine harness with
    /// a new banner, and refusing it would strand a Worker on a cosmetic
    /// change — but it is never silently "supported".
    #[must_use]
    pub fn classify(&self, version_output: &str) -> VersionReport {
        let raw = version_output.trim().to_owned();
        let version = HarnessVersion::parse_first(&raw);
        let verdict = version.map_or(SupportVerdict::Untested, |v| self.verdict(v));
        VersionReport {
            table: *self,
            raw,
            version,
            verdict,
        }
    }
}

/// What a probe of one binary found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionReport {
    pub table: SupportTable,
    /// The binary's trimmed `--version` output, kept verbatim for the record.
    pub raw: String,
    pub version: Option<HarnessVersion>,
    pub verdict: SupportVerdict,
}

impl VersionReport {
    /// A report for a binary whose version could not be read at all — the
    /// probe failed to run, or answered nothing in time.
    #[must_use]
    pub fn unreadable(table: SupportTable, why: impl Into<String>) -> Self {
        Self {
            table,
            raw: why.into(),
            version: None,
            verdict: SupportVerdict::Untested,
        }
    }

    /// One sentence for the journal, a log line, or `horde doctor`.
    #[must_use]
    pub fn describe(&self) -> String {
        let table = &self.table;
        let range = format!("{}–{}", table.tested_from, table.tested_through);
        match (self.verdict, self.version) {
            (SupportVerdict::Supported, Some(version)) => {
                format!("{} {version} is supported (tested {range})", table.provider)
            }
            (SupportVerdict::Refused, Some(version)) => format!(
                "{} {version} is refused: below {} ({})",
                table.provider, table.floor, table.floor_reason
            ),
            (_, Some(version)) => format!(
                "{} {version} is untested (tested {range}); a protocol change would surface as \
                 a malformed-stream failure",
                table.provider
            ),
            (_, None) => format!(
                "{} version unreadable ({:?}); treated as untested (tested {range})",
                table.provider, self.raw
            ),
        }
    }

    /// Whether an adapter may drive this binary at all.
    #[must_use]
    pub fn is_refused(&self) -> bool {
        self.verdict == SupportVerdict::Refused
    }

    /// Whether the binary is at or above `version` — `false` when unreadable,
    /// so a version-gated claim fails closed.
    #[must_use]
    pub fn at_least(&self, version: HarnessVersion) -> bool {
        self.version.is_some_and(|found| found >= version)
    }
}

/// Runs `program --version` once, bounded by `ceiling`: what it printed on
/// stdout, or why it printed nothing. The child is killed if the ceiling
/// fires (`kill_on_drop`), so a wedged binary costs the ceiling once.
///
/// # Errors
///
/// A sentence saying the binary could not run, or answered nothing in time.
#[cfg(any(feature = "probe", test))]
pub async fn run_version(
    program: impl AsRef<std::ffi::OsStr>,
    ceiling: std::time::Duration,
) -> Result<String, String> {
    let program = program.as_ref();
    let shown = program.to_string_lossy().into_owned();
    let probe = tokio::process::Command::new(program)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output();
    match tokio::time::timeout(ceiling, probe).await {
        Err(_elapsed) => Err(format!(
            "`{shown} --version` answered nothing within {ceiling:?}"
        )),
        Ok(Err(error)) => Err(format!("`{shown} --version` could not run: {error}")),
        Ok(Ok(output)) => Ok(String::from_utf8_lossy(&output.stdout).into_owned()),
    }
}

/// [`run_version`], classified against `table`. Every failure reads as an
/// unreadable version — untested, never supported and never refused — with
/// the reason kept for the journal and `horde doctor`.
#[cfg(any(feature = "probe", test))]
pub async fn probe_version(
    program: impl AsRef<std::ffi::OsStr>,
    table: SupportTable,
    ceiling: std::time::Duration,
) -> VersionReport {
    match run_version(program, ceiling).await {
        Ok(output) => table.classify(&output),
        Err(why) => VersionReport::unreadable(table, why),
    }
}

#[cfg(test)]
mod tests {
    use super::{HarnessVersion, SupportTable, SupportVerdict, VersionReport};

    const TABLE: SupportTable = SupportTable {
        provider: "toy",
        floor: HarnessVersion::new(1, 2, 0),
        tested_from: HarnessVersion::new(1, 4, 0),
        tested_through: HarnessVersion::new(1, 9, 3),
        floor_reason: "the permission route arrived in 1.2",
    };

    #[test]
    fn versions_order_numerically_not_lexically() {
        assert!(HarnessVersion::new(0, 167, 0) > HarnessVersion::new(0, 99, 9));
        assert!(HarnessVersion::new(2, 1, 323) > HarnessVersion::new(2, 1, 226));
        assert!(HarnessVersion::new(3, 0, 0) > HarnessVersion::new(2, 99, 99));
        assert_eq!(HarnessVersion::new(2, 1, 3).to_string(), "2.1.3");
    }

    /// Both CLIs' real banners, and the scanner's corners.
    #[test]
    fn the_first_dotted_version_is_found_wherever_it_sits() {
        let parse = HarnessVersion::parse_first;
        assert_eq!(
            parse("2.1.323 (Claude Code)"),
            Some(HarnessVersion::new(2, 1, 323))
        );
        assert_eq!(
            parse("codex-cli 0.167.0"),
            Some(HarnessVersion::new(0, 167, 0))
        );
        assert_eq!(parse("0.147"), Some(HarnessVersion::new(0, 147, 0)));
        assert_eq!(
            parse("codex 0.147.0-rc1 0.146.0"),
            Some(HarnessVersion::new(0, 146, 0)),
            "a suffixed token is skipped, not misread"
        );
        assert_eq!(parse("1.2.3.4"), None, "too many parts");
        assert_eq!(
            parse("codex(0.147.0)"),
            Some(HarnessVersion::new(0, 147, 0))
        );
        assert_eq!(parse("(0.148.0"), Some(HarnessVersion::new(0, 148, 0)));
        assert_eq!(parse("0.149.0)"), Some(HarnessVersion::new(0, 149, 0)));
        for nothing in ["", "command not found", "codex-cli", "v.next", "0.x.1"] {
            assert_eq!(parse(nothing), None, "{nothing:?}");
        }
    }

    /// Every boundary of the table, both sides.
    #[test]
    fn the_verdict_follows_the_table_at_every_boundary() {
        let at = |major, minor, patch| TABLE.verdict(HarnessVersion::new(major, minor, patch));
        assert_eq!(at(1, 1, 99), SupportVerdict::Refused);
        assert_eq!(
            at(1, 2, 0),
            SupportVerdict::Untested,
            "the floor itself drives"
        );
        assert_eq!(at(1, 3, 99), SupportVerdict::Untested);
        assert_eq!(at(1, 4, 0), SupportVerdict::Supported);
        assert_eq!(at(1, 9, 3), SupportVerdict::Supported);
        assert_eq!(at(1, 9, 4), SupportVerdict::Untested, "newer than measured");
        assert_eq!(at(2, 0, 0), SupportVerdict::Untested);
    }

    #[test]
    fn unreadable_output_is_untested_never_supported_or_refused() {
        let report = TABLE.classify("  banner changed  ");
        assert_eq!(report.verdict, SupportVerdict::Untested);
        assert_eq!(report.version, None);
        assert_eq!(report.raw, "banner changed");
        assert!(!report.is_refused());
        assert!(
            !report.at_least(HarnessVersion::new(0, 0, 0)),
            "fails closed"
        );

        let failed = VersionReport::unreadable(TABLE, "could not run");
        assert_eq!(failed.verdict, SupportVerdict::Untested);
        assert!(
            failed.describe().contains("could not run"),
            "{}",
            failed.describe()
        );
    }

    #[test]
    fn each_description_names_the_version_and_the_evidence() {
        let supported = TABLE.classify("toy 1.5.0");
        assert_eq!(
            supported.describe(),
            "toy 1.5.0 is supported (tested 1.4.0–1.9.3)"
        );
        assert!(supported.at_least(HarnessVersion::new(1, 5, 0)));
        assert!(!supported.at_least(HarnessVersion::new(1, 5, 1)));

        let refused = TABLE.classify("toy 1.0.0");
        assert!(refused.is_refused());
        assert_eq!(
            refused.describe(),
            "toy 1.0.0 is refused: below 1.2.0 (the permission route arrived in 1.2)"
        );

        let untested = TABLE.classify("toy 2.0.0");
        assert!(
            untested
                .describe()
                .starts_with("toy 2.0.0 is untested (tested 1.4.0–1.9.3)"),
            "{}",
            untested.describe()
        );
    }

    /// An executable shell script in a fresh temp directory, standing in for
    /// a harness binary.
    #[cfg(any(feature = "probe", test))]
    fn script(name: &str, body: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("horde-probe-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a scratch dir");
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("writes");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmods");
        path
    }

    /// The bounded probe: what a binary prints is read and classified, one
    /// that cannot run says why, one that hangs is cut off at the ceiling, and
    /// none is ever refused for being unreadable.
    ///
    /// One test, both scripts written before anything is spawned: a fork
    /// from a parallel test while a script is still open for writing makes
    /// exec fail with ETXTBSY.
    #[cfg(any(feature = "probe", test))]
    #[tokio::test]
    async fn the_probe_reads_what_runs_and_explains_what_does_not() {
        let banner = script("banner", "echo \"toy 1.5.0 ($1)\"");
        let hang = script("hang", "exec sleep 30");
        let ceiling = std::time::Duration::from_secs(10);

        assert_eq!(
            super::run_version(&banner, ceiling).await,
            Ok("toy 1.5.0 (--version)\n".to_owned()),
            "the probe passes --version and reads stdout"
        );
        let report = super::probe_version(&banner, TABLE, ceiling).await;
        assert_eq!(report.verdict, SupportVerdict::Supported);
        assert_eq!(report.version, Some(HarnessVersion::new(1, 5, 0)));

        let missing = super::run_version("/nonexistent/horde-probe", ceiling)
            .await
            .expect_err("a missing binary cannot run");
        assert!(
            missing.contains("/nonexistent/horde-probe --version` could not run"),
            "{missing}"
        );
        let report = super::probe_version("/nonexistent/horde-probe", TABLE, ceiling).await;
        assert_eq!(report.verdict, SupportVerdict::Untested);
        assert_eq!(report.raw, missing);

        let started = std::time::Instant::now();
        let silent = super::run_version(&hang, std::time::Duration::from_millis(300))
            .await
            .expect_err("a hung binary is cut off");
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
        assert!(silent.contains("answered nothing within 300ms"), "{silent}");
    }

    #[test]
    fn verdict_words_are_stable() {
        assert_eq!(SupportVerdict::Supported.word(), "supported");
        assert_eq!(SupportVerdict::Untested.word(), "untested");
        assert_eq!(SupportVerdict::Refused.word(), "refused");
    }
}
