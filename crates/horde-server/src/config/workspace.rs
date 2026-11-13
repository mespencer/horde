//! ADR-0014's server keys: `[workspace]` and `[placement]`.
//!
//! Each key layers the built-in default, then the file, then the matching
//! `horde-server` flag. The Definition's `cleanup` overrides two of them
//! per workspace, later, in `workspace_policy::resolve`.

use std::time::Duration;

use horde_config::values::{show_duration, show_optional_duration, show_size, show_threshold};
use horde_config::{Origin, Provenance, Section};
use horde_proto::v1::reclaim_policy;
use horde_proto::v1::resolved_workspace_policy::MarkerRule;

use crate::scheduler::DiskPlacement;
use crate::workspace_policy::{WorkspacePolicyConfig, reclaim_argv_shortfall, wire};

/// The `horde-server` flags for these keys; each `None` defers to the file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkspaceFlags {
    pub fetch_throttle: Option<Duration>,
    pub fetch_timeout: Option<Duration>,
    pub idle_reclaim_after: Option<Duration>,
    pub archived_grace: Option<Duration>,
    pub reclaim_timeout: Option<Duration>,
    pub archived_pin_ttl: Option<Duration>,
    /// `(basis points, bytes)`: below it a Worker is placed last.
    pub disk_pressure: Option<(u64, u64)>,
    /// Below it a Worker takes no workspace spawns.
    pub disk_floor: Option<u64>,
}

/// What the file's `[workspace]` and `[placement]` tables say.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WorkspaceFile {
    pub keys: WorkspaceFlags,
    /// `workspace.reclaim.auto`, when set.
    pub auto_rules: Option<Vec<MarkerRule>>,
}

/// Reads `[workspace]` and `[placement]` from the file's root.
///
/// # Errors
///
/// A key of the wrong type or value, an unknown key, or a reclaim rule
/// without its marker or with an argv the Worker would refuse.
pub fn read(root: &mut Section<'_>) -> Result<WorkspaceFile, String> {
    let mut file = WorkspaceFile::default();
    if let Some(mut workspace) = root.section("workspace")? {
        let keys = &mut file.keys;
        keys.fetch_throttle = workspace.duration("fetch_throttle")?;
        keys.fetch_timeout = workspace.duration("fetch_timeout")?;
        keys.idle_reclaim_after = workspace.duration("idle_reclaim_after")?;
        keys.archived_grace = workspace.duration("archived_grace")?;
        keys.archived_pin_ttl = workspace.duration("archived_pin_ttl")?;
        keys.reclaim_timeout = workspace.duration("reclaim_timeout")?;
        if let Some(mut reclaim) = workspace.section("reclaim")? {
            file.auto_rules = reclaim
                .sections("auto")?
                .map(|rules| rules.into_iter().map(rule_from).collect())
                .transpose()?;
            reclaim.finish()?;
        }
        workspace.finish()?;
    }
    if let Some(mut placement) = root.section("placement")? {
        file.keys.disk_pressure = placement.threshold("disk_pressure")?;
        file.keys.disk_floor = placement.size("disk_floor")?;
        placement.finish()?;
    }
    Ok(file)
}

/// One `{ marker, argv, timeout, output_paths }` rule.
fn rule_from(mut section: Section<'_>) -> Result<MarkerRule, String> {
    let marker_file = section
        .string("marker")?
        .filter(|marker| !marker.is_empty())
        .ok_or_else(|| {
            section.refuse(
                "marker",
                "a rule needs the file that marks it, e.g. \"Cargo.toml\"",
            )
        })?;
    let argv = section.strings("argv")?.unwrap_or_default();
    let timeout = section.duration("timeout")?;
    let output_paths = section.strings("output_paths")?.unwrap_or_default();
    if let Some(why) = reclaim_argv_shortfall(&argv, &output_paths) {
        return Err(section.refuse("argv", why));
    }
    section.finish()?;
    Ok(MarkerRule {
        marker_file,
        command: Some(reclaim_policy::Command {
            argv,
            timeout: timeout.map(wire),
            output_paths,
        }),
    })
}

/// The workspace policy: default, then file, then flag, per key.
pub fn policy(
    file: WorkspaceFile,
    flags: &WorkspaceFlags,
    origin: &Origin,
    provenance: &mut Provenance,
) -> WorkspacePolicyConfig {
    let defaults = WorkspacePolicyConfig::default();
    let mut duration = |key: &str,
                        default: Duration,
                        filed: Option<Duration>,
                        flag: Option<Duration>,
                        name: &str| {
        provenance.pick(
            key,
            default,
            vec![
                (filed, origin.clone()),
                (flag, Origin::Flag(format!("--{name}"))),
            ],
            show_duration,
        )
    };
    let keys = &file.keys;
    let fetch_throttle = duration(
        "workspace.fetch_throttle",
        defaults.fetch_throttle,
        keys.fetch_throttle,
        flags.fetch_throttle,
        "workspace-fetch-throttle",
    );
    let fetch_timeout = duration(
        "workspace.fetch_timeout",
        defaults.fetch_timeout,
        keys.fetch_timeout,
        flags.fetch_timeout,
        "workspace-fetch-timeout",
    );
    let idle_reclaim_after = duration(
        "workspace.idle_reclaim_after",
        defaults.idle_reclaim_after,
        keys.idle_reclaim_after,
        flags.idle_reclaim_after,
        "workspace-idle-reclaim-after",
    );
    let archived_grace = duration(
        "workspace.archived_grace",
        defaults.archived_grace,
        keys.archived_grace,
        flags.archived_grace,
        "workspace-archived-grace",
    );
    let reclaim_timeout = duration(
        "workspace.reclaim_timeout",
        defaults.reclaim_timeout,
        keys.reclaim_timeout,
        flags.reclaim_timeout,
        "workspace-reclaim-timeout",
    );
    let archived_pin_ttl = provenance.pick(
        "workspace.archived_pin_ttl",
        defaults.archived_pin_ttl,
        vec![
            (keys.archived_pin_ttl.map(Some), origin.clone()),
            (
                flags.archived_pin_ttl.map(Some),
                Origin::Flag("--workspace-archived-pin-ttl".to_owned()),
            ),
        ],
        show_optional_duration,
    );
    let auto_rules = provenance.pick(
        "workspace.reclaim.auto",
        defaults.auto_rules,
        vec![(file.auto_rules, origin.clone())],
        |rules| show_rules(rules),
    );
    WorkspacePolicyConfig {
        fetch_throttle,
        fetch_timeout,
        idle_reclaim_after,
        archived_grace,
        reclaim_timeout,
        archived_pin_ttl,
        auto_rules,
    }
}

/// Disk-aware placement: default, then file, then flag, per key.
pub fn placement(
    file: &WorkspaceFile,
    flags: &WorkspaceFlags,
    origin: &Origin,
    provenance: &mut Provenance,
) -> DiskPlacement {
    let defaults = DiskPlacement::default();
    let (pressure_basis_points, pressure_bytes) = provenance.pick(
        "placement.disk_pressure",
        (defaults.pressure_basis_points, defaults.pressure_bytes),
        vec![
            (file.keys.disk_pressure, origin.clone()),
            (
                flags.disk_pressure,
                Origin::Flag("--placement-disk-pressure".to_owned()),
            ),
        ],
        show_threshold,
    );
    let floor_bytes = provenance.pick(
        "placement.disk_floor",
        defaults.floor_bytes,
        vec![
            (file.keys.disk_floor, origin.clone()),
            (
                flags.disk_floor,
                Origin::Flag("--placement-disk-floor".to_owned()),
            ),
        ],
        show_size,
    );
    DiskPlacement {
        pressure_basis_points,
        pressure_bytes,
        floor_bytes,
    }
}

/// The reclaim rules in TOML's inline spelling.
fn show_rules(rules: &[MarkerRule]) -> String {
    let shown: Vec<String> = rules
        .iter()
        .map(|rule| {
            let argv = rule
                .command
                .as_ref()
                .map(|command| format!("{:?}", command.argv))
                .unwrap_or_default();
            format!("{{ marker = {:?}, argv = {argv} }}", rule.marker_file)
        })
        .collect();
    format!("[{}]", shown.join(", "))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use horde_config::{Origin, Provenance, Section};

    use super::{WorkspaceFile, WorkspaceFlags, placement, policy, read};
    use crate::scheduler::DiskPlacement;
    use crate::workspace_policy::WorkspacePolicyConfig;

    fn file_of(text: &str) -> Result<WorkspaceFile, String> {
        let table: toml::Table = text.parse().expect("test TOML parses");
        let mut root = Section::root(&table);
        let file = read(&mut root)?;
        root.finish()?;
        Ok(file)
    }

    fn origin() -> Origin {
        Origin::File("/s.toml".into())
    }

    #[test]
    fn nothing_anywhere_is_every_default() {
        let mut provenance = Provenance::default();
        let file = file_of("").expect("parses");
        assert_eq!(
            policy(
                file.clone(),
                &WorkspaceFlags::default(),
                &origin(),
                &mut provenance
            ),
            WorkspacePolicyConfig::default()
        );
        assert_eq!(
            placement(
                &file,
                &WorkspaceFlags::default(),
                &origin(),
                &mut provenance
            ),
            DiskPlacement::default()
        );
        assert_eq!(
            provenance.render(true),
            "placement.disk_floor = 2GiB  # default\n\
             placement.disk_pressure = 10%,20GiB  # default\n\
             workspace.archived_grace = 1d  # default\n\
             workspace.archived_pin_ttl = unset  # default\n\
             workspace.fetch_throttle = 1m  # default\n\
             workspace.fetch_timeout = 30s  # default\n\
             workspace.idle_reclaim_after = 4h  # default\n\
             workspace.reclaim.auto = [{ marker = \"Cargo.toml\", argv = [\"cargo\", \"clean\"] }]  # default\n\
             workspace.reclaim_timeout = 10m  # default\n"
        );
    }

    #[test]
    fn the_file_overrides_each_default_and_each_flag_overrides_the_file() {
        let file = file_of(
            r#"
            [workspace]
            fetch_throttle = "2m"
            fetch_timeout = "5s"
            idle_reclaim_after = "0"
            archived_grace = "1h"
            archived_pin_ttl = "30d"
            reclaim_timeout = "20m"
            [workspace.reclaim]
            auto = [{ marker = "Makefile", argv = ["make", "clean"], timeout = "1m", output_paths = ["build"] }]
            [placement]
            disk_pressure = "5%"
            disk_floor = "1GiB"
            "#,
        )
        .expect("parses");
        let mut provenance = Provenance::default();
        let filed = policy(
            file.clone(),
            &WorkspaceFlags::default(),
            &origin(),
            &mut provenance,
        );
        assert_eq!(filed.fetch_throttle, Duration::from_secs(120));
        assert_eq!(filed.fetch_timeout, Duration::from_secs(5));
        assert_eq!(filed.idle_reclaim_after, Duration::ZERO);
        assert_eq!(filed.archived_grace, Duration::from_secs(3600));
        assert_eq!(
            filed.archived_pin_ttl,
            Some(Duration::from_secs(30 * 86_400))
        );
        assert_eq!(filed.reclaim_timeout, Duration::from_secs(1200));
        let rule = &filed.auto_rules[0];
        assert_eq!(rule.marker_file, "Makefile");
        let command = rule.command.as_ref().expect("a command");
        assert_eq!(command.argv, ["make", "clean"]);
        assert_eq!(command.timeout.map(|t| t.seconds), Some(60));
        assert_eq!(command.output_paths, ["build"]);
        assert!(
            provenance
                .entries()
                .iter()
                .all(|entry| entry.origin == origin()),
            "every key came from the file: {}",
            provenance.render(true)
        );
        let filed_disk = placement(
            &file,
            &WorkspaceFlags::default(),
            &origin(),
            &mut provenance,
        );
        assert_eq!(
            filed_disk,
            DiskPlacement {
                pressure_basis_points: 500,
                pressure_bytes: 0,
                floor_bytes: 1 << 30,
            }
        );

        let flags = WorkspaceFlags {
            fetch_throttle: Some(Duration::from_secs(1)),
            fetch_timeout: Some(Duration::from_secs(2)),
            idle_reclaim_after: Some(Duration::from_secs(3)),
            archived_grace: Some(Duration::from_secs(4)),
            reclaim_timeout: Some(Duration::from_secs(5)),
            archived_pin_ttl: Some(Duration::from_secs(6)),
            disk_pressure: Some((100, 7)),
            disk_floor: Some(8),
        };
        let mut provenance = Provenance::default();
        let flagged = policy(file.clone(), &flags, &origin(), &mut provenance);
        assert_eq!(
            (
                flagged.fetch_throttle.as_secs(),
                flagged.fetch_timeout.as_secs(),
                flagged.idle_reclaim_after.as_secs(),
                flagged.archived_grace.as_secs(),
                flagged.reclaim_timeout.as_secs(),
                flagged.archived_pin_ttl.map(|ttl| ttl.as_secs()),
            ),
            (1, 2, 3, 4, 5, Some(6))
        );
        let flagged_disk = placement(&file, &flags, &origin(), &mut provenance);
        assert_eq!(
            flagged_disk,
            DiskPlacement {
                pressure_basis_points: 100,
                pressure_bytes: 7,
                floor_bytes: 8,
            }
        );
        for (key, flag) in [
            ("workspace.fetch_throttle", "--workspace-fetch-throttle"),
            ("workspace.fetch_timeout", "--workspace-fetch-timeout"),
            (
                "workspace.idle_reclaim_after",
                "--workspace-idle-reclaim-after",
            ),
            ("workspace.archived_grace", "--workspace-archived-grace"),
            ("workspace.reclaim_timeout", "--workspace-reclaim-timeout"),
            ("workspace.archived_pin_ttl", "--workspace-archived-pin-ttl"),
            ("placement.disk_pressure", "--placement-disk-pressure"),
            ("placement.disk_floor", "--placement-disk-floor"),
        ] {
            assert_eq!(
                provenance.get(key).map(|entry| &entry.origin),
                Some(&Origin::Flag(flag.to_owned())),
                "{key}"
            );
        }
        assert_eq!(
            provenance
                .get("workspace.reclaim.auto")
                .map(|entry| &entry.origin),
            Some(&origin()),
            "no flag sets the reclaim rules"
        );
    }

    #[test]
    fn malformed_workspace_keys_are_refused_by_path() {
        for (text, wanted) in [
            (
                "[workspace]\nfetch_throttle = 60",
                "workspace.fetch_throttle: expected a duration",
            ),
            (
                "[workspace]\nfetch_throtle = \"1s\"",
                "unknown key workspace.fetch_throtle (known here: archived_grace, archived_pin_ttl, fetch_throttle, fetch_timeout, idle_reclaim_after, reclaim, reclaim_timeout)",
            ),
            (
                "[workspace.reclaim]\nauto = [{ argv = [\"make\"] }]",
                "workspace.reclaim.auto[0].marker: a rule needs the file",
            ),
            (
                "[workspace.reclaim]\nauto = [{ marker = \"M\", argv = [\"./clean\"] }]",
                "workspace.reclaim.auto[0].argv:",
            ),
            (
                "[workspace.reclaim]\nauto = [{ marker = \"M\", argv = [] }]",
                "workspace.reclaim.auto[0].argv:",
            ),
            (
                "[workspace.reclaim]\nmanual = []",
                "unknown key workspace.reclaim.manual",
            ),
            (
                "[workspace.reclaim]\nauto = [{ marker = \"M\", argv = [\"make\"], when = 1 }]",
                "unknown key workspace.reclaim.auto[0].when",
            ),
            (
                "[placement]\ndisk_pressure = \"x%\"",
                "placement.disk_pressure: \"x%\" is not a percentage",
            ),
            (
                "[placement]\ndisk_floor = \"lots\"",
                "placement.disk_floor: \"lots\" is not a size",
            ),
            (
                "[placement]\nfloor = \"1GiB\"",
                "unknown key placement.floor (known here: disk_floor, disk_pressure)",
            ),
        ] {
            let refused = file_of(text).expect_err(text);
            assert!(refused.starts_with(wanted), "{text}: {refused}");
        }
    }
}
