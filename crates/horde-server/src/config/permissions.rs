//! `[permissions]`: the default mode and the authored modes (ADR-0016).
//!
//! A mode is `bypass`, `own_orchestrator_verbs`, and `deny`/`allow` rule
//! tables in the chain's own literal vocabulary ([`Rule`]): a tool name,
//! string-prefix facts about named arguments (`where`), and an ancestry
//! scope (`under`). No key sets judge instructions: there is no judge.

use horde_config::{Origin, Provenance, Section};

use crate::permission::modes::{BYPASS, PermissionMode, PermissionModes};
use crate::permission::rules::Rule;

/// What the file's `[permissions]` table says.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PermissionFile {
    /// `permissions.default_mode`.
    pub default_mode: Option<String>,
    /// `permissions.modes.<name>`, in name order.
    pub modes: Vec<(String, PermissionMode)>,
}

/// Reads `[permissions]`, finishing the section.
///
/// # Errors
///
/// A key of the wrong type, an unknown key, an empty mode name, or a deny
/// rule without its reason.
pub fn read(section: Option<Section<'_>>) -> Result<PermissionFile, String> {
    let Some(mut section) = section else {
        return Ok(PermissionFile::default());
    };
    let default_mode = section.string("default_mode")?;
    let modes = section
        .named("modes")?
        .unwrap_or_default()
        .into_iter()
        .map(|(name, mode)| {
            if name.is_empty() {
                return Err(format!("{}: a mode needs a name", mode.path()));
            }
            Ok((name, mode_from(mode)?))
        })
        .collect::<Result<_, String>>()?;
    section.finish()?;
    Ok(PermissionFile {
        default_mode,
        modes,
    })
}

/// One `[permissions.modes.<name>]` table.
fn mode_from(mut section: Section<'_>) -> Result<PermissionMode, String> {
    let mut mode = PermissionMode::escalate_everything();
    mode.bypass = section.boolean("bypass")?.unwrap_or(false);
    mode.own_orchestrator_verbs = section.boolean("own_orchestrator_verbs")?.unwrap_or(false);
    for mut deny in section.sections("deny")?.unwrap_or_default() {
        let rule = rule_from(&mut deny)?;
        let reason = deny
            .string("reason")?
            .filter(|reason| !reason.trim().is_empty())
            .ok_or_else(|| {
                deny.refuse(
                    "reason",
                    "a deny rule needs the reason the refused agent will read",
                )
            })?;
        deny.finish()?;
        mode = mode.deny(rule, reason);
    }
    for mut allow in section.sections("allow")?.unwrap_or_default() {
        let rule = rule_from(&mut allow)?;
        allow.finish()?;
        mode = mode.allow(rule);
    }
    section.finish()?;
    Ok(mode)
}

/// The rule half of a deny or allow table: `tool`, `where`, `under`.
fn rule_from(section: &mut Section<'_>) -> Result<Rule, String> {
    let mut rule = match section.string("tool")? {
        Some(tool) if tool.is_empty() => {
            return Err(section.refuse("tool", "omit it to match every tool, rather than \"\""));
        }
        Some(tool) => Rule::tool(tool),
        None => Rule::any_tool(),
    };
    for (field, prefix) in section.string_map("where")?.unwrap_or_default() {
        rule = rule.where_input_starts_with(field, prefix);
    }
    if let Some(under) = section.strings("under")? {
        rule = rule.under(under);
    }
    Ok(rule)
}

/// The registry the server runs: the built-in modes, the file's on top (a
/// same-named file mode replaces the built-in one), and the default chosen
/// by `--permission-mode`, else `permissions.default_mode`, else `bypass`.
/// Records `permissions.default_mode` and one `permissions.modes.<name>`
/// entry per mode.
///
/// # Errors
///
/// The chosen default names no mode, naming the layer that chose it and
/// the modes that exist.
pub fn registry(
    file: PermissionFile,
    file_origin: &Origin,
    flag: Option<String>,
    provenance: &mut Provenance,
) -> Result<PermissionModes, String> {
    let mut modes = PermissionModes::shipped();
    for name in modes.names() {
        let builtin = modes.resolve(name).map(summary).unwrap_or_default();
        provenance.record(
            format!("permissions.modes.{name}"),
            builtin,
            Origin::Default,
        );
    }
    for (name, mode) in file.modes {
        provenance.record(
            format!("permissions.modes.{name}"),
            summary(&mode),
            file_origin.clone(),
        );
        modes = modes.with_mode(name, mode);
    }
    let flag_origin = Origin::Flag("--permission-mode".to_owned());
    let (name, origin) = match (flag, file.default_mode) {
        (Some(name), _) => (name, flag_origin),
        (None, Some(name)) => (name, file_origin.clone()),
        (None, None) => (BYPASS.to_owned(), Origin::Default),
    };
    let known = modes.names().join(", ");
    let modes = modes.defaulting_to(&name).ok_or_else(|| {
        let layer = match &origin {
            Origin::Flag(flag) => flag.clone(),
            _ => "permissions.default_mode".to_owned(),
        };
        format!("{layer}: no mode named {name:?} (defined: {known})")
    })?;
    provenance.record("permissions.default_mode", format!("{name:?}"), origin);
    Ok(modes)
}

/// A mode in one line, as `config show` prints it.
#[must_use]
pub fn summary(mode: &PermissionMode) -> String {
    let plural = |count: usize| if count == 1 { "rule" } else { "rules" };
    format!(
        "{{ bypass = {}, own_orchestrator_verbs = {}, deny = {} {}, allow = {} {} }}",
        mode.bypass,
        mode.own_orchestrator_verbs,
        mode.deny.len(),
        plural(mode.deny.len()),
        mode.allow.len(),
        plural(mode.allow.len()),
    )
}

#[cfg(test)]
mod tests {
    use horde_config::{Origin, Provenance, Section};

    use super::{PermissionFile, read, registry, summary};
    use crate::permission::modes::PermissionMode;
    use crate::permission::rules::Rule;

    fn file_of(text: &str) -> Result<PermissionFile, String> {
        let table: toml::Table = text.parse().expect("test TOML parses");
        let mut root = Section::root(&table);
        let section = root.section("permissions")?;
        read(section)
    }

    fn origin() -> Origin {
        Origin::File("/s.toml".into())
    }

    #[test]
    fn modes_parse_into_the_chains_own_rules() {
        let file = file_of(
            r#"
            [permissions]
            default_mode = "careful"
            [permissions.modes.careful]
            own_orchestrator_verbs = true
            deny = [{ tool = "Bash", where = { command = "rm " }, reason = "no rm" }]
            allow = [{ tool = "Read" }, { under = ["ci", "tests"] }]
            [permissions.modes.open]
            bypass = true
            "#,
        )
        .expect("parses");
        assert_eq!(file.default_mode.as_deref(), Some("careful"));
        let careful = PermissionMode::escalate_everything()
            .allowing_own_orchestrator_verbs()
            .deny(
                Rule::tool("Bash").where_input_starts_with("command", "rm "),
                "no rm",
            )
            .allow(Rule::tool("Read"))
            .allow(Rule::any_tool().under(vec!["ci".to_owned(), "tests".to_owned()]));
        assert_eq!(
            file.modes,
            vec![
                ("careful".to_owned(), careful),
                ("open".to_owned(), PermissionMode::bypassing()),
            ]
        );
        assert_eq!(file_of(""), Ok(PermissionFile::default()));
    }

    #[test]
    fn malformed_modes_are_refused_by_path() {
        for (text, wanted) in [
            (
                "[permissions.modes.m]\ndeny = [{ tool = \"Bash\" }]",
                "permissions.modes.m.deny[0].reason: a deny rule needs the reason",
            ),
            (
                "[permissions.modes.m]\ndeny = [{ tool = \"Bash\", reason = \" \" }]",
                "permissions.modes.m.deny[0].reason: a deny rule needs the reason",
            ),
            (
                "[permissions.modes.m]\nallow = [{ tool = \"\" }]",
                "permissions.modes.m.allow[0].tool: omit it to match every tool",
            ),
            (
                "[permissions.modes.m]\nallow = [{ tool = \"Read\", reason = \"x\" }]",
                "unknown key permissions.modes.m.allow[0].reason (known here: tool, under, where)",
            ),
            (
                "[permissions.modes.m]\njudge = \"allow reads\"",
                "unknown key permissions.modes.m.judge",
            ),
            (
                "[permissions.modes.\"\"]\nbypass = true",
                "permissions.modes.: a mode needs a name",
            ),
            (
                "[permissions]\ndefault = \"x\"",
                "unknown key permissions.default (known here: default_mode, modes)",
            ),
            (
                "[permissions.modes.m]\nbypass = \"yes\"",
                "permissions.modes.m.bypass: expected true or false",
            ),
            (
                "[permissions.modes.m]\ndeny = [{ reason = \"r\", where = { command = 1 } }]",
                "permissions.modes.m.deny[0].where.command: expected a string",
            ),
        ] {
            let refused = file_of(text).expect_err(text);
            assert!(refused.starts_with(wanted), "{text}: {refused}");
        }
    }

    #[test]
    fn the_default_is_the_flag_then_the_file_then_bypass_and_must_name_a_mode() {
        let file = file_of(
            "[permissions]\ndefault_mode = \"ask\"\n[permissions.modes.open]\nbypass = true\n",
        )
        .expect("parses");

        let mut provenance = Provenance::default();
        let builtin = registry(PermissionFile::default(), &origin(), None, &mut provenance)
            .expect("resolves");
        assert_eq!(builtin.default_name(), "bypass");
        let entry = provenance
            .get("permissions.default_mode")
            .expect("recorded");
        assert_eq!(
            (entry.value.as_str(), &entry.origin),
            ("\"bypass\"", &Origin::Default)
        );

        let mut provenance = Provenance::default();
        let filed = registry(file.clone(), &origin(), None, &mut provenance).expect("resolves");
        assert_eq!(filed.default_name(), "ask");
        assert_eq!(
            provenance
                .get("permissions.default_mode")
                .map(|e| &e.origin),
            Some(&origin())
        );
        assert_eq!(
            provenance.get("permissions.modes.open").map(|e| &e.origin),
            Some(&origin())
        );
        assert_eq!(
            provenance.get("permissions.modes.ask").map(|e| &e.origin),
            Some(&Origin::Default)
        );

        let mut provenance = Provenance::default();
        let flagged = registry(
            file.clone(),
            &origin(),
            Some("open".to_owned()),
            &mut provenance,
        )
        .expect("resolves");
        assert_eq!(flagged.default_name(), "open");
        assert_eq!(flagged.resolve(""), Some(&PermissionMode::bypassing()));
        assert_eq!(
            provenance
                .get("permissions.default_mode")
                .map(|e| &e.origin),
            Some(&Origin::Flag("--permission-mode".to_owned()))
        );

        let refused = registry(
            file.clone(),
            &origin(),
            Some("nope".to_owned()),
            &mut provenance,
        )
        .expect_err("an unknown flag mode");
        assert_eq!(
            refused,
            "--permission-mode: no mode named \"nope\" (defined: ask, bypass, open)"
        );
        let typo = file_of("[permissions]\ndefault_mode = \"bypas\"").expect("parses");
        let refused = registry(typo, &origin(), None, &mut provenance).expect_err("a typo");
        assert_eq!(
            refused,
            "permissions.default_mode: no mode named \"bypas\" (defined: ask, bypass)"
        );
    }

    #[test]
    fn a_file_mode_replaces_a_builtin_of_the_same_name() {
        let file =
            file_of("[permissions.modes.bypass]\ndeny = [{ tool = \"Bash\", reason = \"r\" }]\n")
                .expect("parses");
        let mut provenance = Provenance::default();
        let modes = registry(file, &origin(), None, &mut provenance).expect("resolves");
        let bypass = modes.resolve("").expect("the default");
        assert!(!bypass.bypass, "the file's mode, not the built-in");
        assert_eq!(bypass.deny.len(), 1);
        assert_eq!(
            provenance
                .get("permissions.modes.bypass")
                .map(|e| &e.origin),
            Some(&origin())
        );
    }

    #[test]
    fn a_summary_counts_rules_with_their_plural() {
        assert_eq!(
            summary(&PermissionMode::bypassing()),
            "{ bypass = true, own_orchestrator_verbs = false, deny = 0 rules, allow = 0 rules }"
        );
        let one = PermissionMode::escalate_everything()
            .allowing_own_orchestrator_verbs()
            .deny(Rule::tool("Bash"), "r")
            .allow(Rule::tool("Read"));
        assert_eq!(
            summary(&one),
            "{ bypass = false, own_orchestrator_verbs = true, deny = 1 rule, allow = 1 rule }"
        );
    }
}
