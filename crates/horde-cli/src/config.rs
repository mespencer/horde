//! The client's configuration (ADR-0016): built-in defaults, then
//! `~/.horde/client/config.toml`, then flags.
//!
//! The client's keys are about the person at this terminal: the session
//! layer a new session gets when `horde new --permission-mode` says nothing
//! (`session.permission_mode`), and ADR-0015's `logs.*` for `--log-file`.
//! Everything an agent runs under is the server's to resolve; this file only
//! supplies what the client sends.

use std::path::{Path, PathBuf};

use clap::Args;
use horde_config::values::show_string;
use horde_config::{Document, LogKeys, Provenance, Role};
use horde_proto::v1::SessionSettings;

use crate::client::{Client, ClientError};
use crate::render::Format;

/// The client file's place under `$HOME`.
pub const DEFAULT_FILE: &str = ".horde/client/config.toml";

/// Everything the client runs with that configuration decides.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientSettings {
    /// What a new session's settings are when its flags name none.
    pub session: SessionSettings,
    /// ADR-0015's keys, for the logs work (W12h) to consume.
    pub logs: Option<LogKeys>,
    pub provenance: Provenance,
}

impl ClientSettings {
    /// Resolves every key: the defaults, then `document` (when the file
    /// exists); `logs.level` also reads `HORDE_LOG` from `env`.
    ///
    /// # Errors
    ///
    /// The file breaks a rule — prefixed with its path — or `HORDE_LOG` is
    /// not a filter.
    pub fn resolve(
        document: Option<&Document>,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, String> {
        let (mut root, origin) = horde_config::open(Role::Client, document);
        let file_error = horde_config::refuser(document);
        let mut provenance = Provenance::default();
        let filed_mode = match root.section("session").map_err(file_error)? {
            Some(mut session) => {
                let mode = session.string("permission_mode").map_err(file_error)?;
                session.finish().map_err(file_error)?;
                mode
            }
            None => None,
        };
        let logs_section = root.section("logs").map_err(file_error)?;
        let logs = LogKeys::resolve(Role::Client, logs_section, &origin, env, &mut provenance)
            .map_err(file_error)?;
        root.finish().map_err(file_error)?;
        let permission_mode = provenance.pick(
            "session.permission_mode",
            None,
            vec![(filed_mode.map(Some), origin)],
            |mode: &Option<String>| {
                mode.as_ref()
                    .map_or_else(|| "unset".to_owned(), show_string)
            },
        );
        Ok(Self {
            session: SessionSettings {
                permission_mode: permission_mode.unwrap_or_default(),
            },
            logs: Some(logs),
            provenance,
        })
    }

    /// Loads `explicit` (`--config`), else `~/.horde/client/config.toml`
    /// when `home` is known, and resolves it.
    ///
    /// # Errors
    ///
    /// As [`Self::resolve`], and an unreadable file — or a missing one that
    /// `--config` named.
    pub fn load(
        explicit: Option<&Path>,
        home: Option<PathBuf>,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, String> {
        let default = home.map(|home| home.join(DEFAULT_FILE));
        let document = match (explicit, default) {
            (None, None) => None,
            (explicit, default) => horde_config::load(
                Role::Client,
                explicit,
                default.as_deref().unwrap_or(Path::new(DEFAULT_FILE)),
            )?,
        };
        Self::resolve(document.as_ref(), env)
    }
}

/// The session settings a create sends: what the flag named
/// (`--permission-mode`, carried in the root spec by the shared spec flags
/// and moved out of it here), else this client's defaults.
#[must_use]
pub fn session_settings(
    spec: &mut horde_proto::v1::AgentSpec,
    defaults: &SessionSettings,
) -> SessionSettings {
    match std::mem::take(&mut spec.permission_mode) {
        mode if mode.is_empty() => defaults.clone(),
        permission_mode => SessionSettings { permission_mode },
    }
}

/// `horde config`.
#[derive(clap::Subcommand, Debug)]
pub enum ConfigCommand {
    /// Print the effective configuration, one `key = value` line per key.
    Show(ShowArgs),
}

/// The flags of `horde config show`.
#[derive(Args, Debug, Default)]
pub struct ShowArgs {
    /// Name the layer each value came from.
    #[arg(long)]
    pub origin: bool,
    /// The running server's configuration instead of this client's.
    #[arg(long)]
    pub server: bool,
    /// With the server's, add the per-agent keys for an agent created in
    /// this workspace (by name or id).
    #[arg(long, value_name = "WORKSPACE")]
    pub workspace: Option<String>,
    /// With the server's, add the per-agent keys for an agent created in
    /// this session (by name or id).
    #[arg(long, value_name = "SESSION")]
    pub session: Option<String>,
}

impl ShowArgs {
    /// Whether this asks the server: `--server`, or a workspace or session
    /// to resolve per-agent keys in.
    #[must_use]
    pub fn asks_server(&self) -> bool {
        self.server || self.workspace.is_some() || self.session.is_some()
    }
}

/// Entries as `config show` prints them: lines, or JSON objects.
#[must_use]
pub fn render(entries: &[(String, String, String)], origins: bool, format: Format) -> String {
    match format {
        Format::Json => {
            let objects: Vec<serde_json::Value> = entries
                .iter()
                .map(|(key, value, origin)| {
                    serde_json::json!({"key": key, "value": value, "origin": origin})
                })
                .collect();
            serde_json::Value::Array(objects).to_string()
        }
        Format::Human => entries
            .iter()
            .map(|(key, value, origin)| {
                if origins {
                    format!("{key} = {value}  # {origin}")
                } else {
                    format!("{key} = {value}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

/// This client's entries.
#[must_use]
pub fn local_entries(settings: &ClientSettings) -> Vec<(String, String, String)> {
    settings
        .provenance
        .entries()
        .iter()
        .map(|entry| {
            (
                entry.key.clone(),
                entry.value.clone(),
                entry.origin.to_string(),
            )
        })
        .collect()
}

/// The server's entries, with the per-agent keys for the workspace and
/// session asked about.
///
/// # Errors
///
/// An unknown session reference, or the server's refusal.
pub async fn server_entries(
    client: &mut Client,
    args: &ShowArgs,
) -> Result<Vec<(String, String, String)>, ClientError> {
    let session_id = match &args.session {
        Some(reference) => Some(crate::resolve::session_id(client, reference, true).await?),
        None => None,
    };
    let entries = client
        .effective_config(
            args.workspace.as_deref().unwrap_or_default(),
            session_id.as_deref(),
        )
        .await?;
    Ok(entries
        .into_iter()
        .map(|entry| (entry.key, entry.value, entry.origin))
        .collect())
}

#[cfg(test)]
mod tests {
    use horde_config::{Document, Origin, Role};
    use horde_proto::v1::{AgentSpec, SessionSettings};

    use super::{ClientSettings, ShowArgs, local_entries, render, session_settings};
    use crate::render::Format;

    fn document(text: &str) -> Document {
        Document {
            role: Role::Client,
            path: "/h/.horde/client/config.toml".into(),
            table: text.parse().expect("test TOML parses"),
        }
    }

    #[test]
    fn the_file_sets_the_session_default_and_the_flag_beats_it() {
        let none = ClientSettings::resolve(None, |_| None).expect("resolves");
        assert_eq!(none.session, SessionSettings::default());
        assert_eq!(
            none.provenance
                .get("session.permission_mode")
                .map(|e| e.value.as_str()),
            Some("unset")
        );
        let filed = ClientSettings::resolve(
            Some(&document(
                "[session]\npermission_mode = \"ask\"\n[logs]\nmax_files = 3\n",
            )),
            |_| None,
        )
        .expect("resolves");
        assert_eq!(filed.session.permission_mode, "ask");
        assert_eq!(filed.logs.as_ref().map(|logs| logs.max_files), Some(3));
        assert_eq!(
            filed
                .provenance
                .get("session.permission_mode")
                .map(|e| &e.origin),
            Some(&Origin::File("/h/.horde/client/config.toml".into()))
        );

        let mut flagged = AgentSpec {
            permission_mode: "bypass".to_owned(),
            ..AgentSpec::default()
        };
        assert_eq!(
            session_settings(&mut flagged, &filed.session).permission_mode,
            "bypass",
            "the flag beats the file"
        );
        assert_eq!(flagged.permission_mode, "", "moved to the session layer");
        let mut silent = AgentSpec::default();
        assert_eq!(session_settings(&mut silent, &filed.session), filed.session);
    }

    #[test]
    fn refusals_name_the_file_and_the_key() {
        let prefix = "client config /h/.horde/client/config.toml: ";
        for (text, wanted) in [
            (
                "[permissions]\n",
                "permissions belongs in the server's config file",
            ),
            (
                "[worker.workspace]\n",
                "worker belongs in the worker's config file",
            ),
            ("[codex]\n", "codex belongs in the worker's config file"),
            (
                "[session]\nmode = \"ask\"",
                "unknown key session.mode (known here: permission_mode)",
            ),
            (
                "[session]\npermission_mode = 1",
                "session.permission_mode: expected a string",
            ),
            ("session = 1", "session: expected a table"),
            ("logs = 1", "logs: expected a table"),
            (
                "[logs.agents]\n",
                "logs.agents belongs in the worker's config file",
            ),
            (
                "endpoint = \"x\"",
                "unknown key endpoint (known here: logs, session)",
            ),
        ] {
            let refused = ClientSettings::resolve(Some(&document(text)), |_| None).expect_err(text);
            assert!(
                refused.starts_with(&format!("{prefix}{wanted}")),
                "{text}: {refused}"
            );
        }
        assert!(
            ClientSettings::resolve(Some(&document("")), |_| Some("=[".to_owned()))
                .expect_err("a bad HORDE_LOG")
                .starts_with("HORDE_LOG=")
        );
    }

    #[test]
    fn load_reads_the_home_file_or_the_named_one() {
        let home = tempfile::tempdir().expect("a temp dir");
        let unset = ClientSettings::load(None, Some(home.path().to_path_buf()), |_| None)
            .expect("no file is no layer");
        assert_eq!(unset.session, SessionSettings::default());
        let dir = home.path().join(".horde/client");
        std::fs::create_dir_all(&dir).expect("creates");
        std::fs::write(
            dir.join("config.toml"),
            "[session]\npermission_mode = \"ask\"\n",
        )
        .expect("writes");
        let homed =
            ClientSettings::load(None, Some(home.path().to_path_buf()), |_| None).expect("reads");
        assert_eq!(homed.session.permission_mode, "ask");
        let homeless = ClientSettings::load(None, None, |_| None).expect("nothing to read");
        assert_eq!(homeless.session, SessionSettings::default());
        let named = home.path().join("other.toml");
        std::fs::write(&named, "[session]\npermission_mode = \"loose\"\n").expect("writes");
        let explicit = ClientSettings::load(Some(&named), None, |_| None).expect("reads");
        assert_eq!(explicit.session.permission_mode, "loose");
        let missing = ClientSettings::load(Some(&home.path().join("nope.toml")), None, |_| None)
            .expect_err("a named file must exist");
        assert!(missing.contains("nope.toml"), "{missing}");
    }

    #[test]
    fn entries_render_as_lines_with_optional_origins_or_as_json() {
        let settings = ClientSettings::resolve(None, |_| None).expect("resolves");
        let entries = local_entries(&settings);
        let lines = render(&entries, true, Format::Human);
        assert!(
            lines.contains("session.permission_mode = unset  # default"),
            "{lines}"
        );
        assert!(lines.contains("logs.max_files = 2  # default"), "{lines}");
        let bare = render(&entries, false, Format::Human);
        assert!(
            bare.contains("logs.max_files = 2\n") && !bare.contains('#'),
            "{bare}"
        );
        let json: serde_json::Value =
            serde_json::from_str(&render(&entries, false, Format::Json)).expect("JSON");
        assert_eq!(
            json.as_array().and_then(|all| all.first()).cloned(),
            Some(serde_json::json!({
                "key": "logs.level",
                "value": "\"info,horde_server=debug,horde_worker=debug\"",
                "origin": "default"
            }))
        );
        assert!(!ShowArgs::default().asks_server());
        for args in [
            ShowArgs {
                server: true,
                ..ShowArgs::default()
            },
            ShowArgs {
                workspace: Some("w".to_owned()),
                ..ShowArgs::default()
            },
            ShowArgs {
                session: Some("s".to_owned()),
                ..ShowArgs::default()
            },
        ] {
            assert!(args.asks_server(), "{args:?}");
        }
    }
}
