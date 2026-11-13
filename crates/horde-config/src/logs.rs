//! ADR-0015's `logs.*` keys, for every role.
//!
//! Defined here, before the logs implementation (W12h) consumes them, so the
//! names, defaults and refusals are settled once: each field is named after
//! ADR-0015's `SETTING_*` constant. `logs.level` is also read from
//! `HORDE_LOG`, which outranks the file — ADR-0015's variable, kept as the
//! environment layer.

use std::time::Duration;

use crate::file::Role;
use crate::origin::Origin;
use crate::provenance::Provenance;
use crate::reader::Section;
use crate::values::{show_optional_duration, show_size, show_string};

/// The environment variable that sets `logs.level`.
pub const LEVEL_VAR: &str = "HORDE_LOG";

/// The file filter when nothing sets one.
pub const DEFAULT_LEVEL: &str = "info,horde_server=debug,horde_worker=debug";

const MIB: u64 = 1 << 20;
const DAY: Duration = Duration::from_secs(86_400);

/// One process's log files: how they are filtered and bounded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogKeys {
    /// `logs.level`: an `EnvFilter` directive string.
    pub level: String,
    /// `logs.max_file_bytes`.
    pub max_file_bytes: u64,
    /// `logs.max_files`.
    pub max_files: usize,
    /// `logs.max_age`; `None` is no age limit (`"0"` in the file).
    pub max_age: Option<Duration>,
    /// `logs.agents.*` — the worker's per-agent stderr files.
    pub agents: Option<AgentLogKeys>,
    /// `logs.agent_reads.*` — what an agent's log reads may cost (server).
    pub agent_reads: Option<AgentReadKeys>,
}

/// `logs.agents.*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentLogKeys {
    pub max_file_bytes: u64,
    pub max_files: usize,
    /// The whole `agents/` directory's budget.
    pub budget_bytes: u64,
}

/// `logs.agent_reads.*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentReadKeys {
    pub default_bytes: u32,
    pub max_bytes: u32,
    pub calls_per_minute: u32,
    pub burst: u32,
}

/// The file-family defaults per role: daemons 16 MiB × 8 for 14 days, the
/// client 4 MiB × 2 for 7 days (ADR-0015).
fn family_defaults(role: Role) -> (u64, usize, Duration) {
    match role {
        Role::Server | Role::Worker => (16 * MIB, 8, 14 * DAY),
        Role::Client => (4 * MIB, 2, 7 * DAY),
    }
}

/// Shows a count as itself.
fn show_count<T: ToString>(value: &T) -> String {
    value.to_string()
}

/// Where a key's file value came from: the file's origin, when there is one.
struct Layer<'a> {
    origin: Origin,
    section: Option<Section<'a>>,
}

impl<'a> Layer<'a> {
    /// Reads one key with `read`, or nothing when there is no section.
    fn read<T>(
        &mut self,
        read: impl FnOnce(&mut Section<'a>) -> Result<Option<T>, String>,
    ) -> Result<Option<T>, String> {
        self.section.as_mut().map_or(Ok(None), read)
    }

    fn sub(&mut self, key: &str) -> Result<Layer<'a>, String> {
        Ok(Layer {
            origin: self.origin.clone(),
            section: self.read(|section| section.section(key))?,
        })
    }

    fn finish(self) -> Result<(), String> {
        self.section.map_or(Ok(()), Section::finish)
    }
}

impl LogKeys {
    /// Resolves the role's `logs.*` keys: the built-in defaults, then the
    /// file's `[logs]` section (`logs`, finished here), then `HORDE_LOG`
    /// for the level. Every key is recorded in `provenance`.
    ///
    /// # Errors
    ///
    /// A key of the wrong type or value, an unknown key, another role's
    /// key (`logs.agents` outside the worker, `logs.agent_reads` outside the
    /// server), or a level — from the file or the environment — that is not
    /// a filter.
    pub fn resolve(
        role: Role,
        logs: Option<Section<'_>>,
        file_origin: &Origin,
        env: impl Fn(&str) -> Option<String>,
        provenance: &mut Provenance,
    ) -> Result<Self, String> {
        let mut layer = Layer {
            origin: file_origin.clone(),
            section: logs,
        };
        let (bytes, files, age) = family_defaults(role);
        let level = level(&mut layer, env, provenance)?;
        let max_file_bytes = pick(
            provenance,
            &mut layer,
            "logs.max_file_bytes",
            bytes,
            |section| section.positive_size("max_file_bytes"),
            show_size,
        )?;
        let max_files = pick(
            provenance,
            &mut layer,
            "logs.max_files",
            files,
            |section| section.positive_count("max_files"),
            show_count,
        )?;
        let max_age = pick(
            provenance,
            &mut layer,
            "logs.max_age",
            Some(age),
            |section| {
                Ok(section
                    .duration("max_age")?
                    .map(|age| Some(age).filter(|age| !age.is_zero())))
            },
            show_optional_duration,
        )?;
        let agents = match role {
            Role::Worker => Some(AgentLogKeys::resolve(&mut layer, provenance)?),
            Role::Server | Role::Client => {
                layer.read(|section| {
                    section.foreign("agents", "worker");
                    Ok(None::<()>)
                })?;
                None
            }
        };
        let agent_reads = match role {
            Role::Server => Some(AgentReadKeys::resolve(&mut layer, provenance)?),
            Role::Worker | Role::Client => {
                layer.read(|section| {
                    section.foreign("agent_reads", "server");
                    Ok(None::<()>)
                })?;
                None
            }
        };
        layer.finish()?;
        Ok(Self {
            level,
            max_file_bytes,
            max_files,
            max_age,
            agents,
            agent_reads,
        })
    }
}

/// `logs.level`: the default, then the file, then `HORDE_LOG`; each filter
/// is checked.
fn level(
    layer: &mut Layer<'_>,
    env: impl Fn(&str) -> Option<String>,
    provenance: &mut Provenance,
) -> Result<String, String> {
    let file_level = layer.read(|section| {
        section
            .string("level")?
            .map(|level| checked_filter(&level).map_err(|error| section.refuse("level", error)))
            .transpose()
    })?;
    let env_level = env(LEVEL_VAR)
        .filter(|level| !level.trim().is_empty())
        .map(|level| {
            checked_filter(&level).map_err(|error| format!("{LEVEL_VAR}={level:?}: {error}"))
        })
        .transpose()?;
    Ok(provenance.pick(
        "logs.level",
        DEFAULT_LEVEL.to_owned(),
        vec![
            (file_level, layer.origin.clone()),
            (env_level, Origin::Env(LEVEL_VAR.to_owned())),
        ],
        show_string,
    ))
}

/// Picks one key: the default, then the file layer's value.
fn pick<'a, T>(
    provenance: &mut Provenance,
    layer: &mut Layer<'a>,
    key: &str,
    default: T,
    read: impl FnOnce(&mut Section<'a>) -> Result<Option<T>, String>,
    show: impl Fn(&T) -> String,
) -> Result<T, String> {
    let filed = layer.read(read)?;
    Ok(provenance.pick(key, default, vec![(filed, layer.origin.clone())], show))
}

impl AgentLogKeys {
    fn resolve(parent: &mut Layer<'_>, provenance: &mut Provenance) -> Result<Self, String> {
        let mut layer = parent.sub("agents")?;
        let keys = Self {
            max_file_bytes: pick(
                provenance,
                &mut layer,
                "logs.agents.max_file_bytes",
                4 * MIB,
                |section| section.positive_size("max_file_bytes"),
                show_size,
            )?,
            max_files: pick(
                provenance,
                &mut layer,
                "logs.agents.max_files",
                2,
                |section| section.positive_count("max_files"),
                show_count,
            )?,
            budget_bytes: pick(
                provenance,
                &mut layer,
                "logs.agents.budget_bytes",
                256 * MIB,
                |section| section.positive_size("budget_bytes"),
                show_size,
            )?,
        };
        layer.finish()?;
        Ok(keys)
    }
}

impl AgentReadKeys {
    fn resolve(parent: &mut Layer<'_>, provenance: &mut Provenance) -> Result<Self, String> {
        let mut layer = parent.sub("agent_reads")?;
        let byte_count = |section: &mut Section<'_>, key: &str| -> Result<Option<u32>, String> {
            section
                .positive_size(key)?
                .map(|bytes| {
                    u32::try_from(bytes).map_err(|_| section.refuse(key, "must be under 4GiB"))
                })
                .transpose()
        };
        let show_bytes = |bytes: &u32| show_size(&u64::from(*bytes));
        let keys = Self {
            default_bytes: pick(
                provenance,
                &mut layer,
                "logs.agent_reads.default_bytes",
                16 * 1024,
                |section| byte_count(section, "default_bytes"),
                show_bytes,
            )?,
            max_bytes: pick(
                provenance,
                &mut layer,
                "logs.agent_reads.max_bytes",
                64 * 1024,
                |section| byte_count(section, "max_bytes"),
                show_bytes,
            )?,
            calls_per_minute: pick(
                provenance,
                &mut layer,
                "logs.agent_reads.calls_per_minute",
                6,
                |section| section.positive_count("calls_per_minute"),
                show_count,
            )?,
            burst: pick(
                provenance,
                &mut layer,
                "logs.agent_reads.burst",
                3,
                |section| section.positive_count("burst"),
                show_count,
            )?,
        };
        if keys.default_bytes > keys.max_bytes {
            return Err(format!(
                "logs.agent_reads.default_bytes ({}) may not exceed logs.agent_reads.max_bytes ({})",
                show_bytes(&keys.default_bytes),
                show_bytes(&keys.max_bytes)
            ));
        }
        layer.finish()?;
        Ok(keys)
    }
}

/// `level` when it parses as a filter.
fn checked_filter(level: &str) -> Result<String, String> {
    tracing_subscriber::EnvFilter::try_new(level)
        .map(|_| level.to_owned())
        .map_err(|error| format!("{level:?} is not a log filter: {error}"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{AgentLogKeys, AgentReadKeys, DEFAULT_LEVEL, LEVEL_VAR, LogKeys};
    use crate::file::Role;
    use crate::origin::Origin;
    use crate::provenance::Provenance;
    use crate::reader::Section;

    fn file() -> Origin {
        Origin::File("/c.toml".into())
    }

    fn resolve(
        role: Role,
        text: &str,
        env: &[(&str, &str)],
    ) -> Result<(LogKeys, Provenance), String> {
        let table: toml::Table = text.parse().expect("test TOML parses");
        let mut root = Section::root(&table);
        let logs = root.section("logs")?;
        let mut provenance = Provenance::default();
        let keys = LogKeys::resolve(
            role,
            logs,
            &file(),
            |name| {
                env.iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| (*value).to_owned())
            },
            &mut provenance,
        )?;
        root.finish()?;
        Ok((keys, provenance))
    }

    #[test]
    fn the_defaults_are_adr_0015s_per_role() {
        let (server, provenance) = resolve(Role::Server, "", &[]).expect("resolves");
        assert_eq!(server.level, DEFAULT_LEVEL);
        assert_eq!(
            (server.max_file_bytes, server.max_files, server.max_age),
            (16 << 20, 8, Some(Duration::from_secs(14 * 86_400)))
        );
        assert_eq!(server.agents, None);
        assert_eq!(
            server.agent_reads,
            Some(AgentReadKeys {
                default_bytes: 16 * 1024,
                max_bytes: 64 * 1024,
                calls_per_minute: 6,
                burst: 3,
            })
        );
        assert_eq!(
            provenance.render(true),
            "logs.agent_reads.burst = 3  # default\n\
             logs.agent_reads.calls_per_minute = 6  # default\n\
             logs.agent_reads.default_bytes = 16KiB  # default\n\
             logs.agent_reads.max_bytes = 64KiB  # default\n\
             logs.level = \"info,horde_server=debug,horde_worker=debug\"  # default\n\
             logs.max_age = 14d  # default\n\
             logs.max_file_bytes = 16MiB  # default\n\
             logs.max_files = 8  # default\n"
        );

        let (worker, _) = resolve(Role::Worker, "", &[]).expect("resolves");
        assert_eq!(
            worker.agents,
            Some(AgentLogKeys {
                max_file_bytes: 4 << 20,
                max_files: 2,
                budget_bytes: 256 << 20,
            })
        );
        assert_eq!(worker.agent_reads, None);

        let (client, _) = resolve(Role::Client, "", &[]).expect("resolves");
        assert_eq!(
            (client.max_file_bytes, client.max_files, client.max_age),
            (4 << 20, 2, Some(Duration::from_secs(7 * 86_400)))
        );
        assert_eq!((client.agents, client.agent_reads), (None, None));
    }

    #[test]
    fn the_file_overrides_each_default_and_horde_log_overrides_the_files_level() {
        let text = r#"
            [logs]
            level = "warn"
            max_file_bytes = "1MiB"
            max_files = 3
            max_age = "0"
            [logs.agents]
            max_file_bytes = "2MiB"
            max_files = 4
            budget_bytes = "1GiB"
        "#;
        let (worker, provenance) = resolve(Role::Worker, text, &[]).expect("resolves");
        assert_eq!(worker.level, "warn");
        assert_eq!((worker.max_file_bytes, worker.max_files), (1 << 20, 3));
        assert_eq!(worker.max_age, None, "0 is no age limit");
        assert_eq!(
            worker.agents,
            Some(AgentLogKeys {
                max_file_bytes: 2 << 20,
                max_files: 4,
                budget_bytes: 1 << 30,
            })
        );
        let level = provenance.get("logs.level").expect("recorded");
        assert_eq!(level.origin, file());
        assert_eq!(
            provenance
                .get("logs.max_age")
                .map(|entry| entry.value.as_str()),
            Some("unset")
        );

        let (env, provenance) =
            resolve(Role::Worker, text, &[(LEVEL_VAR, "debug")]).expect("resolves");
        assert_eq!(env.level, "debug");
        assert_eq!(
            provenance.get("logs.level").map(|entry| &entry.origin),
            Some(&Origin::Env(LEVEL_VAR.to_owned()))
        );
        let (blank, _) = resolve(Role::Worker, text, &[(LEVEL_VAR, " ")]).expect("resolves");
        assert_eq!(blank.level, "warn", "a blank variable sets nothing");

        let (server, _) = resolve(
            Role::Server,
            "[logs.agent_reads]\ndefault_bytes = \"1KiB\"\nmax_bytes = \"2KiB\"\ncalls_per_minute = 1\nburst = 9\n",
            &[],
        )
        .expect("resolves");
        assert_eq!(
            server.agent_reads,
            Some(AgentReadKeys {
                default_bytes: 1024,
                max_bytes: 2048,
                calls_per_minute: 1,
                burst: 9,
            })
        );
    }

    #[test]
    fn bad_values_unknown_keys_and_other_roles_keys_are_refused() {
        for (role, text, env, wanted) in [
            (
                Role::Server,
                "[logs]\nlevel = \"=[\"",
                &[][..],
                "logs.level: \"=[\" is not a log filter",
            ),
            (
                Role::Server,
                "",
                &[(LEVEL_VAR, "=[")][..],
                "HORDE_LOG=\"=[\": \"=[\" is not a log filter",
            ),
            (
                Role::Server,
                "[logs]\nmax_files = 0",
                &[][..],
                "logs.max_files: must be at least 1",
            ),
            (
                Role::Server,
                "[logs]\nmax_file_bytes = \"0\"",
                &[][..],
                "logs.max_file_bytes: must be at least 1",
            ),
            (
                Role::Server,
                "[logs]\nmax_size = 1",
                &[][..],
                "unknown key logs.max_size",
            ),
            (
                Role::Server,
                "[logs.agents]\nmax_files = 1",
                &[][..],
                "logs.agents belongs in the worker's config file",
            ),
            (
                Role::Client,
                "[logs.agent_reads]\nburst = 1",
                &[][..],
                "logs.agent_reads belongs in the server's config file",
            ),
            (
                Role::Worker,
                "[logs.agents]\nmax = 1",
                &[][..],
                "unknown key logs.agents.max",
            ),
            (
                Role::Worker,
                "[logs.agents]\nbudget_bytes = \"0\"",
                &[][..],
                "logs.agents.budget_bytes: must be at least 1",
            ),
            (
                Role::Server,
                "[logs.agent_reads]\ndefault_bytes = \"8KiB\"\nmax_bytes = \"4KiB\"",
                &[][..],
                "logs.agent_reads.default_bytes (8KiB) may not exceed logs.agent_reads.max_bytes (4KiB)",
            ),
            (
                Role::Server,
                "[logs.agent_reads]\nmax_bytes = \"4GiB\"",
                &[][..],
                "logs.agent_reads.max_bytes: must be under 4GiB",
            ),
            (
                Role::Server,
                "[logs.agent_reads]\nburst = 0",
                &[][..],
                "logs.agent_reads.burst: must be at least 1",
            ),
            (
                Role::Server,
                "[logs.agent_reads]\nx = 0",
                &[][..],
                "unknown key logs.agent_reads.x",
            ),
            (
                Role::Server,
                "logs = 1",
                &[][..],
                "logs: expected a table, found integer 1",
            ),
        ] {
            let refused = resolve(role, text, env).expect_err(text);
            assert!(refused.starts_with(wanted), "{text}: {refused}");
        }
    }
}
