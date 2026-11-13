//! The worker's configuration (ADR-0016): built-in defaults, then
//! `~/.horde/worker/config.toml`, then `horde-worker`'s flags.
//!
//! The worker's keys are the machine's: workspace root, scan cadence and
//! disk watermarks (ADR-0014's `worker.workspace.*`), the codex sandbox
//! tier each permission mode gets on this machine, and ADR-0015's `logs.*`
//! with the per-agent stderr files. What a workspace's agents experience is
//! the server's to say, and travels in commands: the worker never reads the
//! server's file.

use std::path::PathBuf;
use std::time::Duration;

use horde_adapter_codex::{SandboxPolicy, SandboxTier};
use horde_config::logs::LEVEL_VAR;
use horde_config::values::{show_duration, show_string, show_threshold};
use horde_config::{Document, LogKeys, Origin, Provenance, Role, Section};

use crate::runtime::{HousekeepingConfig, Watermark};

/// The permission mode the built-in sandbox map covers.
const BYPASS: &str = "bypass";

/// The `horde-worker` flags that are configuration keys' flag layer; each
/// `None` defers to the file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerFlags {
    pub workspace_root: Option<PathBuf>,
    pub scan_interval: Option<Duration>,
    pub pin_scan_interval: Option<Duration>,
    pub report_interval: Option<Duration>,
    pub disk_low_watermark: Option<Watermark>,
    pub disk_high_watermark: Option<Watermark>,
}

/// Everything the worker runs with that configuration decides, and where
/// each key came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSettings {
    pub workspace_root: Option<PathBuf>,
    pub housekeeping: HousekeepingConfig,
    /// The codex sandbox tier per permission mode (`codex.sandbox.<mode>`).
    pub sandbox: SandboxPolicy,
    /// ADR-0015's keys, for the logs work (W12h) to consume.
    pub logs: LogKeys,
    pub provenance: Provenance,
}

/// What the file's `[worker.workspace]` and `[codex.sandbox]` say.
#[derive(Debug, Default)]
struct WorkerFile {
    keys: WorkerFlags,
    sandbox: Option<Vec<(String, SandboxTier)>>,
}

impl WorkerSettings {
    /// Resolves every key: the defaults, then `document` (when the file
    /// exists), then `flags`; `logs.level` also reads `HORDE_LOG` from `env`.
    ///
    /// # Errors
    ///
    /// The file breaks a rule (unknown key, wrong type, bad value, another
    /// role's key, a zero interval, an unknown sandbox tier, a high
    /// watermark not above the low one) — prefixed with the file's path —
    /// or `HORDE_LOG` is not a filter.
    pub fn resolve(
        document: Option<&Document>,
        flags: &WorkerFlags,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, String> {
        let empty = toml::Table::new();
        let (table, origin) = document.map_or((&empty, Origin::Default), |document| {
            (&document.table, document.origin())
        });
        let file_error = |message: String| match document {
            Some(document) if !message.starts_with(LEVEL_VAR) => document.refusal(message),
            _ => message,
        };
        let mut provenance = Provenance::default();
        let mut root = Section::root(table);
        for key in ["permissions", "tiers", "workspace", "placement"] {
            root.foreign(key, "server");
        }
        root.foreign("session", "client");
        let file = read(&mut root).map_err(file_error)?;
        let logs_section = root.section("logs").map_err(file_error)?;
        let logs = LogKeys::resolve(Role::Worker, logs_section, &origin, env, &mut provenance)
            .map_err(file_error)?;
        root.finish().map_err(file_error)?;

        let housekeeping = housekeeping(&file.keys, flags, &origin, &mut provenance);
        let (low, high) = (
            housekeeping.disk_low_watermark,
            housekeeping.disk_high_watermark,
        );
        if high.basis_points <= low.basis_points && high.bytes <= low.bytes {
            return Err(format!(
                "worker.workspace.disk_high_watermark ({}) must exceed \
                 worker.workspace.disk_low_watermark ({})",
                show_watermark(&high),
                show_watermark(&low)
            ));
        }
        let workspace_root = provenance.pick(
            "worker.workspace.root",
            None,
            vec![
                (file.keys.workspace_root.map(Some), origin.clone()),
                (
                    flags.workspace_root.clone().map(Some),
                    Origin::Flag("--workspace-root".to_owned()),
                ),
            ],
            |root: &Option<PathBuf>| {
                root.as_ref().map_or_else(
                    || "unset".to_owned(),
                    |root| format!("{:?}", root.display().to_string()),
                )
            },
        );
        let sandbox = sandbox(file.sandbox, &origin, &mut provenance);
        Ok(Self {
            workspace_root,
            housekeeping,
            sandbox,
            logs,
            provenance,
        })
    }
}

/// Reads `[worker.workspace]` and `[codex.sandbox]` from the file's root.
fn read(root: &mut Section<'_>) -> Result<WorkerFile, String> {
    let mut file = WorkerFile::default();
    if let Some(mut worker) = root.section("worker")? {
        if let Some(mut workspace) = worker.section("workspace")? {
            let keys = &mut file.keys;
            keys.workspace_root = workspace.string("root")?.map(PathBuf::from);
            keys.scan_interval = interval(&mut workspace, "scan_interval")?;
            keys.pin_scan_interval = interval(&mut workspace, "pin_scan_interval")?;
            keys.report_interval = interval(&mut workspace, "report_interval")?;
            keys.disk_low_watermark = watermark(&mut workspace, "disk_low_watermark")?;
            keys.disk_high_watermark = watermark(&mut workspace, "disk_high_watermark")?;
            workspace.finish()?;
        }
        worker.finish()?;
    }
    if let Some(mut codex) = root.section("codex")? {
        file.sandbox = codex
            .string_map("sandbox")?
            .map(|modes| {
                modes
                    .into_iter()
                    .map(|(mode, tier)| {
                        let tier = tier_named(&tier).ok_or_else(|| {
                            codex.refuse(
                                &format!("sandbox.{mode}"),
                                format!("{tier:?} is not \"read-only\" or \"workspace-write\""),
                            )
                        })?;
                        Ok((mode, tier))
                    })
                    .collect::<Result<Vec<_>, String>>()
            })
            .transpose()?;
        codex.finish()?;
    }
    Ok(file)
}

/// A scan interval: a duration, never zero, since a zero interval would spin.
fn interval(section: &mut Section<'_>, key: &str) -> Result<Option<Duration>, String> {
    match section.duration(key)? {
        Some(duration) if duration.is_zero() => {
            Err(section.refuse(key, "must be longer than zero"))
        }
        duration => Ok(duration),
    }
}

/// A watermark threshold.
fn watermark(section: &mut Section<'_>, key: &str) -> Result<Option<Watermark>, String> {
    Ok(section
        .threshold(key)?
        .map(|(basis_points, bytes)| Watermark {
            basis_points,
            bytes,
        }))
}

fn show_watermark(watermark: &Watermark) -> String {
    show_threshold(&(watermark.basis_points, watermark.bytes))
}

/// A sandbox tier by its wire name.
fn tier_named(name: &str) -> Option<SandboxTier> {
    [SandboxTier::ReadOnly, SandboxTier::WorkspaceWrite]
        .into_iter()
        .find(|tier| tier.wire_name() == name)
}

/// The housekeeping keys: default, then file, then flag, per key.
fn housekeeping(
    file: &WorkerFlags,
    flags: &WorkerFlags,
    origin: &Origin,
    provenance: &mut Provenance,
) -> HousekeepingConfig {
    let defaults = HousekeepingConfig::default();
    let flag = |name: &str| Origin::Flag(format!("--workspace-{name}"));
    let mut duration =
        |name: &str, default: Duration, filed: Option<Duration>, flagged: Option<Duration>| {
            provenance.pick(
                &format!("worker.workspace.{}", name.replace('-', "_")),
                default,
                vec![(filed, origin.clone()), (flagged, flag(name))],
                show_duration,
            )
        };
    let scan_interval = duration(
        "scan-interval",
        defaults.scan_interval,
        file.scan_interval,
        flags.scan_interval,
    );
    let pin_scan_interval = duration(
        "pin-scan-interval",
        defaults.pin_scan_interval,
        file.pin_scan_interval,
        flags.pin_scan_interval,
    );
    let report_interval = duration(
        "report-interval",
        defaults.report_interval,
        file.report_interval,
        flags.report_interval,
    );
    let mut mark =
        |name: &str, default: Watermark, filed: Option<Watermark>, flagged: Option<Watermark>| {
            provenance.pick(
                &format!("worker.workspace.{}", name.replace('-', "_")),
                default,
                vec![(filed, origin.clone()), (flagged, flag(name))],
                show_watermark,
            )
        };
    let disk_low_watermark = mark(
        "disk-low-watermark",
        defaults.disk_low_watermark,
        file.disk_low_watermark,
        flags.disk_low_watermark,
    );
    let disk_high_watermark = mark(
        "disk-high-watermark",
        defaults.disk_high_watermark,
        file.disk_high_watermark,
        flags.disk_high_watermark,
    );
    HousekeepingConfig {
        scan_interval,
        pin_scan_interval,
        report_interval,
        disk_low_watermark,
        disk_high_watermark,
    }
}

/// The codex sandbox map: the file's, wholesale, or the built-in one that
/// lets a bypass-mode agent write its checkout (ADR-0016).
fn sandbox(
    configured: Option<Vec<(String, SandboxTier)>>,
    origin: &Origin,
    provenance: &mut Provenance,
) -> SandboxPolicy {
    let (modes, origin) = match configured {
        Some(modes) => (modes, origin.clone()),
        None => (
            vec![(BYPASS.to_owned(), SandboxTier::WorkspaceWrite)],
            Origin::Default,
        ),
    };
    modes
        .into_iter()
        .fold(SandboxPolicy::default(), |policy, (mode, tier)| {
            provenance.record(
                format!("codex.sandbox.{mode}"),
                show_string(&tier.wire_name().to_owned()),
                origin.clone(),
            );
            policy.with_mode(mode, tier)
        })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use horde_adapter_codex::{SandboxPolicy, SandboxTier};
    use horde_config::{Document, Origin, Role};

    use super::{WorkerFlags, WorkerSettings};
    use crate::runtime::{HousekeepingConfig, Watermark};

    fn document(text: &str) -> Document {
        Document {
            role: Role::Worker,
            path: "/h/.horde/worker/config.toml".into(),
            table: text.parse().expect("test TOML parses"),
        }
    }

    fn file() -> Origin {
        Origin::File("/h/.horde/worker/config.toml".into())
    }

    #[test]
    fn no_file_and_no_flags_is_the_built_in_worker() {
        let settings =
            WorkerSettings::resolve(None, &WorkerFlags::default(), |_| None).expect("resolves");
        assert_eq!(settings.housekeeping, HousekeepingConfig::default());
        assert_eq!(settings.workspace_root, None);
        assert_eq!(
            settings.sandbox,
            SandboxPolicy::default().with_mode("bypass", SandboxTier::WorkspaceWrite)
        );
        assert_eq!(
            settings.sandbox.tier_for("bypass").tier,
            SandboxTier::WorkspaceWrite
        );
        assert_eq!(settings.sandbox.tier_for("ask").tier, SandboxTier::ReadOnly);
        let shown = settings.provenance.render(true);
        for line in [
            "codex.sandbox.bypass = \"workspace-write\"  # default\n",
            "worker.workspace.root = unset  # default\n",
            "worker.workspace.scan_interval = 1m  # default\n",
            "worker.workspace.pin_scan_interval = 6h  # default\n",
            "worker.workspace.report_interval = 5m  # default\n",
            "worker.workspace.disk_low_watermark = 10%,20GiB  # default\n",
            "worker.workspace.disk_high_watermark = 15%  # default\n",
            "logs.agents.budget_bytes = 256MiB  # default\n",
        ] {
            assert!(shown.contains(line), "{line} in:\n{shown}");
        }
    }

    #[test]
    fn the_file_overrides_each_default_and_each_flag_overrides_the_file() {
        let doc = document(
            r#"
            [worker.workspace]
            root = "/srv/ws"
            scan_interval = "2m"
            pin_scan_interval = "1h"
            report_interval = "10m"
            disk_low_watermark = "5%"
            disk_high_watermark = "8%"
            [codex.sandbox]
            builder = "workspace-write"
            "#,
        );
        let filed = WorkerSettings::resolve(Some(&doc), &WorkerFlags::default(), |_| None)
            .expect("resolves");
        assert_eq!(filed.workspace_root, Some("/srv/ws".into()));
        assert_eq!(
            filed.housekeeping,
            HousekeepingConfig {
                scan_interval: Duration::from_secs(120),
                pin_scan_interval: Duration::from_secs(3600),
                report_interval: Duration::from_secs(600),
                disk_low_watermark: Watermark {
                    basis_points: 500,
                    bytes: 0
                },
                disk_high_watermark: Watermark {
                    basis_points: 800,
                    bytes: 0
                },
            }
        );
        assert_eq!(
            filed.sandbox,
            SandboxPolicy::default().with_mode("builder", SandboxTier::WorkspaceWrite),
            "a configured map replaces the built-in one wholesale"
        );
        assert_eq!(filed.sandbox.tier_for("bypass").tier, SandboxTier::ReadOnly);
        assert!(
            filed
                .provenance
                .entries()
                .iter()
                .filter(|entry| !entry.key.starts_with("logs."))
                .all(|entry| entry.origin == file()),
            "{}",
            filed.provenance.render(true)
        );

        let flags = WorkerFlags {
            workspace_root: Some("/flag/ws".into()),
            scan_interval: Some(Duration::from_secs(1)),
            pin_scan_interval: Some(Duration::from_secs(2)),
            report_interval: Some(Duration::from_secs(3)),
            disk_low_watermark: Some(Watermark {
                basis_points: 100,
                bytes: 0,
            }),
            disk_high_watermark: Some(Watermark {
                basis_points: 200,
                bytes: 0,
            }),
        };
        let flagged = WorkerSettings::resolve(Some(&doc), &flags, |_| None).expect("resolves");
        assert_eq!(flagged.workspace_root, Some("/flag/ws".into()));
        assert_eq!(flagged.housekeeping.scan_interval, Duration::from_secs(1));
        assert_eq!(
            flagged.housekeeping.pin_scan_interval,
            Duration::from_secs(2)
        );
        assert_eq!(flagged.housekeeping.report_interval, Duration::from_secs(3));
        assert_eq!(flagged.housekeeping.disk_low_watermark.basis_points, 100);
        assert_eq!(flagged.housekeeping.disk_high_watermark.basis_points, 200);
        for (key, flag) in [
            ("worker.workspace.root", "--workspace-root"),
            (
                "worker.workspace.scan_interval",
                "--workspace-scan-interval",
            ),
            (
                "worker.workspace.pin_scan_interval",
                "--workspace-pin-scan-interval",
            ),
            (
                "worker.workspace.report_interval",
                "--workspace-report-interval",
            ),
            (
                "worker.workspace.disk_low_watermark",
                "--workspace-disk-low-watermark",
            ),
            (
                "worker.workspace.disk_high_watermark",
                "--workspace-disk-high-watermark",
            ),
        ] {
            assert_eq!(
                flagged.provenance.get(key).map(|entry| &entry.origin),
                Some(&Origin::Flag(flag.to_owned())),
                "{key}"
            );
        }
        assert_eq!(
            flagged
                .provenance
                .get("worker.workspace.root")
                .map(|e| e.value.as_str()),
            Some("\"/flag/ws\"")
        );
    }

    #[test]
    fn refusals_name_the_file_and_the_key() {
        let prefix = "worker config /h/.horde/worker/config.toml: ";
        for (text, wanted) in [
            (
                "[permissions]\ndefault_mode = \"ask\"",
                "permissions belongs in the server's config file",
            ),
            (
                "[workspace]\nfetch_throttle = \"1m\"",
                "workspace belongs in the server's config file",
            ),
            ("[tiers.t]\n", "tiers belongs in the server's config file"),
            (
                "[placement]\n",
                "placement belongs in the server's config file",
            ),
            ("[session]\n", "session belongs in the client's config file"),
            (
                "[worker.workspace]\nscan_interval = \"0\"",
                "worker.workspace.scan_interval: must be longer than zero",
            ),
            (
                "[worker.workspace]\nreclaim_concurrency = 2",
                "unknown key worker.workspace.reclaim_concurrency",
            ),
            (
                "[worker]\nname = \"x\"",
                "unknown key worker.name (known here: workspace)",
            ),
            (
                "[codex]\nsandbox = { b = \"full\" }",
                "codex.sandbox.b: \"full\" is not \"read-only\" or \"workspace-write\"",
            ),
            (
                "[codex]\nreviewer = \"user\"",
                "unknown key codex.reviewer (known here: sandbox)",
            ),
            (
                "[worker.workspace]\ndisk_low_watermark = 5",
                "worker.workspace.disk_low_watermark: expected a threshold",
            ),
            (
                "[logs.agent_reads]\nburst = 1",
                "logs.agent_reads belongs in the server's config file",
            ),
            ("logs = 1", "logs: expected a table"),
            ("worker = 1", "worker: expected a table"),
        ] {
            let refused =
                WorkerSettings::resolve(Some(&document(text)), &WorkerFlags::default(), |_| None)
                    .expect_err(text);
            assert!(
                refused.starts_with(&format!("{prefix}{wanted}")),
                "{text}: {refused}"
            );
        }
        let inverted = document(
            "[worker.workspace]\ndisk_low_watermark = \"20%\"\ndisk_high_watermark = \"10%\"",
        );
        assert_eq!(
            WorkerSettings::resolve(Some(&inverted), &WorkerFlags::default(), |_| None),
            Err("worker.workspace.disk_high_watermark (10%) must exceed \
                 worker.workspace.disk_low_watermark (20%)"
                .to_owned())
        );
        let equal = WorkerFlags {
            disk_low_watermark: Some(Watermark {
                basis_points: 100,
                bytes: 5,
            }),
            disk_high_watermark: Some(Watermark {
                basis_points: 100,
                bytes: 5,
            }),
            ..WorkerFlags::default()
        };
        assert!(
            WorkerSettings::resolve(None, &equal, |_| None).is_err(),
            "equal is not above"
        );
        let one_part_above = WorkerFlags {
            disk_low_watermark: Some(Watermark {
                basis_points: 100,
                bytes: 5,
            }),
            disk_high_watermark: Some(Watermark {
                basis_points: 100,
                bytes: 6,
            }),
            ..WorkerFlags::default()
        };
        assert!(WorkerSettings::resolve(None, &one_part_above, |_| None).is_ok());
        assert!(
            WorkerSettings::resolve(None, &WorkerFlags::default(), |_| Some("=[".to_owned()))
                .expect_err("a bad HORDE_LOG")
                .starts_with("HORDE_LOG=")
        );
        assert!(
            WorkerSettings::resolve(Some(&document("")), &WorkerFlags::default(), |_| Some(
                "=[".to_owned()
            ))
            .expect_err("a bad HORDE_LOG beside a file")
            .starts_with("HORDE_LOG="),
            "the environment's error does not blame the file"
        );
    }
}
