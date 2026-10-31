//! The isolation flags every Horde-supervised `claude` runs with.
//!
//! One builder, so the production spawn and the manual live smokes cannot
//! drift apart: a smoke that passes `--setting-sources ''` while production
//! does not is a smoke proving a harness nobody ships (ADR-0010).
//!
//! What each flag closes, measured against claude 2.1.314's `--help` and the
//! W1c / 2026-08-11 findings:
//!
//! * `--setting-sources ''` — no user, project, or local settings file is
//!   read. Without it Claude **merges** permission allow lists across
//!   `~/.claude/settings.json`, the checked-out repo's `.claude/settings*.json`
//!   and `--settings`, so an operator's or a repo's allow rule decides a tool
//!   call before `--permission-prompt-tool` ever sees it (ADR-0003's
//!   re-verification). The same files carry hooks and enabled plugins.
//! * `--strict-mcp-config` — only the servers named by `--mcp-config` load;
//!   ambient `.mcp.json` and user-scope servers do not.
//! * `--settings` with empty allow/deny lists and `--permission-mode manual`
//!   — only when a permission chain is wired, so nothing is pre-decided and
//!   the prompt tool is genuinely consulted. An agent with no chain keeps the
//!   harness's default mode rather than one that refuses every prompt.
//!
//! Pure: argv in, argv out. Where the MCP config file lives and who may read
//! it is the Worker's business; this module only names it.

use std::path::Path;

/// The permission settings a wired chain brings: nothing allowed, nothing
/// denied, so every gated call reaches the prompt tool.
pub const EMPTY_PERMISSION_SETTINGS: &str = r#"{"permissions":{"allow":[],"deny":[]}}"#;

/// The isolation and permission flags for one `claude` launch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IsolationFlags<'a> {
    /// The MCP config file the harness loads — the only MCP servers it gets.
    pub mcp_config: Option<&'a Path>,
    /// The namespaced `--permission-prompt-tool`, when a chain is wired.
    pub permission_prompt_tool: Option<&'a str>,
}

impl IsolationFlags<'_> {
    /// The argv fragment, in a stable order.
    #[must_use]
    pub fn argv(&self) -> Vec<String> {
        let mut args = vec![
            "--setting-sources".to_owned(),
            String::new(),
            "--strict-mcp-config".to_owned(),
        ];
        if let Some(config) = self.mcp_config {
            args.push("--mcp-config".to_owned());
            args.push(config.display().to_string());
        }
        if let Some(tool) = self.permission_prompt_tool {
            args.push("--permission-prompt-tool".to_owned());
            args.push(tool.to_owned());
            args.push("--settings".to_owned());
            args.push(EMPTY_PERMISSION_SETTINGS.to_owned());
            args.push("--permission-mode".to_owned());
            args.push("manual".to_owned());
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{EMPTY_PERMISSION_SETTINGS, IsolationFlags};

    /// Setting sources and strict MCP are unconditional: an agent without a
    /// chain still must not load an operator's hooks or MCP servers.
    #[test]
    fn isolation_is_unconditional_and_the_chain_is_opt_in() {
        let bare = IsolationFlags::default().argv();
        assert_eq!(
            bare,
            vec![
                "--setting-sources".to_owned(),
                String::new(),
                "--strict-mcp-config".to_owned(),
            ]
        );
    }

    #[test]
    fn a_wired_chain_brings_the_config_file_the_tool_and_empty_settings() {
        let argv = IsolationFlags {
            mcp_config: Some(Path::new("/state/run/mcp-1.json")),
            permission_prompt_tool: Some("mcp__horde__approve"),
        }
        .argv();
        assert_eq!(
            argv,
            vec![
                "--setting-sources".to_owned(),
                String::new(),
                "--strict-mcp-config".to_owned(),
                "--mcp-config".to_owned(),
                "/state/run/mcp-1.json".to_owned(),
                "--permission-prompt-tool".to_owned(),
                "mcp__horde__approve".to_owned(),
                "--settings".to_owned(),
                EMPTY_PERMISSION_SETTINGS.to_owned(),
                "--permission-mode".to_owned(),
                "manual".to_owned(),
            ]
        );
    }
}
