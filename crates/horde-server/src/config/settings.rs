//! The server's effective settings: built-in defaults, then
//! `~/.horde/server/config.toml`, then `horde-server`'s flags (ADR-0016).

use horde_config::{Document, LogKeys, Origin, Provenance, Role};

use super::workspace::WorkspaceFlags;
use super::{permissions, tiers, workspace};
use crate::permission::modes::PermissionModes;
use crate::scheduler::DiskPlacement;
use crate::tiers::TierCatalog;
use crate::workspace_policy::WorkspacePolicyConfig;

/// The `horde-server` flags that are configuration keys' flag layer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerFlags {
    /// `--permission-mode`: the default mode, over `permissions.default_mode`.
    pub permission_mode: Option<String>,
    /// The `--workspace-*` and `--placement-*` flags.
    pub workspace: WorkspaceFlags,
}

/// Everything the server runs with that configuration decides, and where
/// each key came from.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerSettings {
    pub modes: PermissionModes,
    pub tiers: TierCatalog,
    pub workspace: WorkspacePolicyConfig,
    pub placement: DiskPlacement,
    /// ADR-0015's keys, for the logs work (W12h) to consume.
    pub logs: LogKeys,
    pub provenance: Provenance,
}

impl ServerSettings {
    /// Loads the config file — `explicit` when `--config` named one, else
    /// `default`, which may be absent — and resolves it under `flags`.
    ///
    /// # Errors
    ///
    /// As [`horde_config::load`] and [`Self::resolve`].
    pub fn load(
        explicit: Option<&std::path::Path>,
        default: &std::path::Path,
        flags: &ServerFlags,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, String> {
        let document = horde_config::load(Role::Server, explicit, default)?;
        Self::resolve(document.as_ref(), flags, env)
    }

    /// Resolves every key: the defaults, then `document` (when the file
    /// exists), then `flags`; `logs.level` also reads `HORDE_LOG` from `env`.
    ///
    /// # Errors
    ///
    /// The file breaks a rule (unknown key, wrong type, bad value, another
    /// role's key, a default mode that names no mode) — prefixed with the
    /// file's path — or `--permission-mode` / `HORDE_LOG` is invalid.
    pub fn resolve(
        document: Option<&Document>,
        flags: &ServerFlags,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, String> {
        let (mut root, origin) = horde_config::open(Role::Server, document);
        let file_error = horde_config::refuser(document);
        let mut provenance = Provenance::default();
        let permission_file = permissions::read(root.section("permissions").map_err(file_error)?)
            .map_err(file_error)?;
        let configured_tiers = tiers::read(&mut root).map_err(file_error)?;
        let workspace_file = workspace::read(&mut root).map_err(file_error)?;
        let logs_section = root.section("logs").map_err(file_error)?;
        let logs = LogKeys::resolve(Role::Server, logs_section, &origin, env, &mut provenance)
            .map_err(file_error)?;
        root.finish().map_err(file_error)?;

        let flag_mode = flags.permission_mode.clone();
        let from_flag = flag_mode.is_some();
        let modes = permissions::registry(permission_file, &origin, flag_mode, &mut provenance)
            .map_err(|message| {
                if from_flag {
                    message
                } else {
                    file_error(message)
                }
            })?;
        let (tiers, tiers_origin) = match configured_tiers {
            Some(catalog) => (catalog, origin.clone()),
            None => (TierCatalog::shipped_defaults(), Origin::Default),
        };
        tiers::record(&tiers, &tiers_origin, &mut provenance);
        let placement =
            workspace::placement(&workspace_file, &flags.workspace, &origin, &mut provenance);
        let workspace =
            workspace::policy(workspace_file, &flags.workspace, &origin, &mut provenance);
        Ok(Self {
            modes,
            tiers,
            workspace,
            placement,
            logs,
            provenance,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use horde_config::{Document, Origin, Role};

    use super::{ServerFlags, ServerSettings};
    use crate::config::workspace::WorkspaceFlags;
    use crate::permission::modes::PermissionModes;
    use crate::tiers::TierCatalog;

    fn document(text: &str) -> Document {
        Document {
            role: Role::Server,
            path: "/h/.horde/server/config.toml".into(),
            table: text.parse().expect("test TOML parses"),
        }
    }

    fn file() -> Origin {
        Origin::File("/h/.horde/server/config.toml".into())
    }

    #[test]
    fn no_file_and_no_flags_is_the_shipped_server() {
        let settings =
            ServerSettings::resolve(None, &ServerFlags::default(), |_| None).expect("resolves");
        assert_eq!(settings.modes, PermissionModes::shipped());
        assert_eq!(settings.tiers, TierCatalog::shipped_defaults());
        assert_eq!(
            settings.workspace,
            crate::workspace_policy::WorkspacePolicyConfig::default()
        );
        assert_eq!(
            settings.placement,
            crate::scheduler::DiskPlacement::default()
        );
        assert!(
            settings
                .provenance
                .entries()
                .iter()
                .all(|entry| entry.origin == Origin::Default),
            "{}",
            settings.provenance.render(true)
        );
        for key in [
            "permissions.default_mode",
            "permissions.modes.ask",
            "permissions.modes.bypass",
            "tiers.fast",
            "workspace.fetch_throttle",
            "placement.disk_floor",
            "logs.level",
            "logs.agent_reads.burst",
        ] {
            assert!(settings.provenance.get(key).is_some(), "{key} is shown");
        }
    }

    #[test]
    fn each_layer_wins_over_the_one_before_it_with_its_origin() {
        let doc = document(
            r#"
            [permissions]
            default_mode = "ask"
            [tiers.only]
            candidates = [{ provider = "claude", model = "opus" }]
            [workspace]
            fetch_throttle = "2m"
            [logs]
            level = "warn"
            "#,
        );
        let settings = ServerSettings::resolve(Some(&doc), &ServerFlags::default(), |_| None)
            .expect("resolves");
        assert_eq!(settings.modes.default_name(), "ask");
        assert_eq!(settings.tiers.names(), ["only"]);
        assert_eq!(settings.workspace.fetch_throttle, Duration::from_secs(120));
        assert_eq!(settings.logs.level, "warn");
        for key in [
            "permissions.default_mode",
            "tiers.only",
            "workspace.fetch_throttle",
            "logs.level",
        ] {
            assert_eq!(
                settings.provenance.get(key).map(|entry| &entry.origin),
                Some(&file()),
                "{key}"
            );
        }
        assert_eq!(
            settings.provenance.get("tiers.fast"),
            None,
            "replaced wholesale"
        );

        let flags = ServerFlags {
            permission_mode: Some("bypass".to_owned()),
            workspace: WorkspaceFlags {
                fetch_throttle: Some(Duration::from_secs(5)),
                ..WorkspaceFlags::default()
            },
        };
        let settings = ServerSettings::resolve(Some(&doc), &flags, |name| {
            (name == "HORDE_LOG").then(|| "error".to_owned())
        })
        .expect("resolves");
        assert_eq!(settings.modes.default_name(), "bypass");
        assert_eq!(settings.workspace.fetch_throttle, Duration::from_secs(5));
        assert_eq!(settings.logs.level, "error");
        for (key, origin) in [
            (
                "permissions.default_mode",
                Origin::Flag("--permission-mode".to_owned()),
            ),
            (
                "workspace.fetch_throttle",
                Origin::Flag("--workspace-fetch-throttle".to_owned()),
            ),
            ("logs.level", Origin::Env("HORDE_LOG".to_owned())),
        ] {
            assert_eq!(
                settings.provenance.get(key).map(|entry| &entry.origin),
                Some(&origin),
                "{key}"
            );
        }
    }

    #[test]
    fn a_refusal_names_the_file_except_for_what_a_flag_or_the_environment_set() {
        let resolve = |text: &str, flags: &ServerFlags, env: Option<&str>| {
            ServerSettings::resolve(Some(&document(text)), flags, |name| {
                env.filter(|_| name == "HORDE_LOG").map(str::to_owned)
            })
            .expect_err(text)
        };
        let none = ServerFlags::default();
        let prefix = "server config /h/.horde/server/config.toml: ";
        for (text, wanted) in [
            (
                "[worker.workspace]\nscan_interval = \"1m\"",
                "worker belongs in the worker's config file",
            ),
            (
                "[codex.sandbox]\nbypass = \"read-only\"",
                "codex belongs in the worker's config file",
            ),
            (
                "[session]\npermission_mode = \"ask\"",
                "session belongs in the client's config file",
            ),
            (
                "stray = 1",
                "unknown key stray (known here: logs, permissions, placement, tiers, workspace)",
            ),
            (
                "[permissions]\ndefault_mode = \"nope\"",
                "permissions.default_mode: no mode named \"nope\"",
            ),
            ("[tiers.t]\n", "tiers.t.candidates"),
            ("[workspace]\nfetch_timeout = 1", "workspace.fetch_timeout"),
            ("[logs]\nlevel = 1", "logs.level"),
            ("permissions = 1", "permissions: expected a table"),
            ("logs = 1", "logs: expected a table"),
        ] {
            let refused = resolve(text, &none, None);
            assert!(
                refused.starts_with(&format!("{prefix}{wanted}")),
                "{text}: {refused}"
            );
        }
        let flagged = ServerFlags {
            permission_mode: Some("nope".to_owned()),
            ..ServerFlags::default()
        };
        assert!(resolve("", &flagged, None).starts_with("--permission-mode: no mode named"));
        assert!(resolve("", &none, Some("=[")).starts_with("HORDE_LOG="));
        assert!(
            ServerSettings::resolve(None, &none, |_| Some("=[".to_owned()))
                .expect_err("a bad HORDE_LOG with no file")
                .starts_with("HORDE_LOG=")
        );
    }
}
