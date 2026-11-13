//! The log settings and their defaults (ADR-0015).
//!
//! Every bound is a code default under the setting name W12e's config
//! layering will expose as a `logs.*` key. Until then only the file level
//! is read from the environment (`HORDE_LOG`); the sizes, counts, ages and
//! the agent-read limits are these constants.

use std::time::Duration;

/// The environment variable controlling the file layer's filter, beside
/// `RUST_LOG` for the console. Becomes `logs.level`.
pub const LEVEL_VAR: &str = "HORDE_LOG";

/// The environment variable that opts the CLI into writing its own file.
pub const CLIENT_FILE_VAR: &str = "HORDE_LOG_FILE";

/// Setting names, for W12e's config layering.
pub const SETTING_LEVEL: &str = "logs.level";
pub const SETTING_MAX_FILE_BYTES: &str = "logs.max_file_bytes";
pub const SETTING_MAX_FILES: &str = "logs.max_files";
pub const SETTING_MAX_AGE: &str = "logs.max_age";
pub const SETTING_AGENT_MAX_FILE_BYTES: &str = "logs.agents.max_file_bytes";
pub const SETTING_AGENT_MAX_FILES: &str = "logs.agents.max_files";
pub const SETTING_AGENT_BUDGET_BYTES: &str = "logs.agents.budget_bytes";
pub const SETTING_AGENT_DEFAULT_BYTES: &str = "logs.agent_reads.default_bytes";
pub const SETTING_AGENT_MAX_BYTES: &str = "logs.agent_reads.max_bytes";
pub const SETTING_AGENT_CALLS_PER_MINUTE: &str = "logs.agent_reads.calls_per_minute";
pub const SETTING_AGENT_BURST: &str = "logs.agent_reads.burst";

/// The file filter when `HORDE_LOG` is unset.
pub const DEFAULT_LEVEL: &str = "info,horde_server=debug,horde_worker=debug";

const MIB: u64 = 1024 * 1024;
const DAY: Duration = Duration::from_secs(24 * 60 * 60);

/// How one daemon's (or the CLI's) files are bounded and filtered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSettings {
    /// An `EnvFilter` directive string.
    pub level: String,
    pub max_file_bytes: u64,
    pub max_files: usize,
    pub max_age: Option<Duration>,
}

impl LogSettings {
    /// `horde-server` and `horde-worker`: 16 MiB × 8, 14 days.
    #[must_use]
    pub fn daemon() -> Self {
        Self {
            level: DEFAULT_LEVEL.to_owned(),
            max_file_bytes: 16 * MIB,
            max_files: 8,
            max_age: Some(14 * DAY),
        }
    }

    /// `horde --log-file`: 4 MiB × 2, 7 days.
    #[must_use]
    pub fn client() -> Self {
        Self {
            level: DEFAULT_LEVEL.to_owned(),
            max_file_bytes: 4 * MIB,
            max_files: 2,
            max_age: Some(7 * DAY),
        }
    }

    /// The hard ceiling on this family's disk use.
    #[must_use]
    pub fn ceiling_bytes(&self) -> u64 {
        self.max_file_bytes.saturating_mul(self.max_files as u64)
    }

    /// Applies `HORDE_LOG` from `lookup`, refusing a filter that does not
    /// parse — a typo is a refusal, never a silent no-op.
    ///
    /// # Errors
    ///
    /// The variable is set and is not a valid filter.
    pub fn with_env(mut self, lookup: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        if let Some(level) = lookup(LEVEL_VAR).filter(|level| !level.trim().is_empty()) {
            tracing_subscriber::EnvFilter::try_new(&level)
                .map_err(|error| format!("{LEVEL_VAR}={level:?} is not a log filter: {error}"))?;
            self.level = level;
        }
        Ok(self)
    }
}

/// How each agent's harness-stderr files are bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentLogSettings {
    pub max_file_bytes: u64,
    pub max_files: usize,
    /// The whole `agents/` directory's budget.
    pub budget_bytes: u64,
}

impl Default for AgentLogSettings {
    /// 4 MiB × 2 per agent, 256 MiB for the directory.
    fn default() -> Self {
        Self {
            max_file_bytes: 4 * MIB,
            max_files: 2,
            budget_bytes: 256 * MIB,
        }
    }
}

/// What one agent's log reads may cost (the `logs` MCP verb, and `ReadLogs`
/// from a worker caller).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentReadLimits {
    /// Bytes per call when the agent names none.
    pub default_bytes: u32,
    /// Bytes per call at most, whatever the agent asks for.
    pub max_bytes: u32,
    /// Sustained calls per minute.
    pub calls_per_minute: u32,
    /// Calls allowed back to back before the rate applies.
    pub burst: u32,
}

impl Default for AgentReadLimits {
    /// 16 KiB per call, 64 KiB at most, 6 calls a minute with a burst of 3.
    fn default() -> Self {
        Self {
            default_bytes: 16 * 1024,
            max_bytes: 64 * 1024,
            calls_per_minute: 6,
            burst: 3,
        }
    }
}

impl AgentReadLimits {
    /// The byte cap one call gets: the default for zero, the maximum for
    /// anything above it.
    #[must_use]
    pub fn clamp(&self, asked: u32) -> u32 {
        match asked {
            0 => self.default_bytes,
            asked => asked.min(self.max_bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AgentLogSettings, AgentReadLimits, DEFAULT_LEVEL, LEVEL_VAR, LogSettings};

    #[test]
    fn the_defaults_are_the_adrs() {
        let daemon = LogSettings::daemon();
        assert_eq!(daemon.ceiling_bytes(), 128 * 1024 * 1024);
        assert_eq!(daemon.max_age.map(|age| age.as_secs()), Some(14 * 86_400));
        let client = LogSettings::client();
        assert_eq!(client.ceiling_bytes(), 8 * 1024 * 1024);
        assert_eq!(client.max_age.map(|age| age.as_secs()), Some(7 * 86_400));
        let agents = AgentLogSettings::default();
        assert_eq!(
            (agents.max_file_bytes, agents.max_files),
            (4 * 1024 * 1024, 2)
        );
        assert_eq!(agents.budget_bytes, 256 * 1024 * 1024);
    }

    #[test]
    fn horde_log_overrides_the_level_and_a_typo_is_refused() {
        let unset = LogSettings::daemon().with_env(|_| None).expect("default");
        assert_eq!(unset.level, DEFAULT_LEVEL);
        let blank = LogSettings::daemon()
            .with_env(|_| Some("  ".to_owned()))
            .expect("blank");
        assert_eq!(blank.level, DEFAULT_LEVEL);
        let set = LogSettings::daemon()
            .with_env(|name| (name == LEVEL_VAR).then(|| "warn".to_owned()))
            .expect("valid");
        assert_eq!(set.level, "warn");
        let typo = LogSettings::daemon().with_env(|_| Some("info,=[".to_owned()));
        assert!(typo.is_err_and(|refusal| refusal.contains(LEVEL_VAR)));
    }

    #[test]
    fn a_read_is_clamped_to_the_limits() {
        let limits = AgentReadLimits::default();
        assert_eq!(limits.clamp(0), 16 * 1024);
        assert_eq!(limits.clamp(100), 100);
        assert_eq!(limits.clamp(64 * 1024), 64 * 1024);
        assert_eq!(limits.clamp(1_000_000), 64 * 1024);
    }
}
