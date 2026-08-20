//! The live Claude adapter: the pure codec driven over a real child process.

use std::collections::VecDeque;
use std::path::PathBuf;

use async_trait::async_trait;
use horde_adapter::{
    Adapter, AdapterError, AgentEvent, AgentSession, AgentSpec, Capabilities, Delivery,
    PermissionDecision, ProviderSessionId, Result, ToolCallId, TurnInput,
};
use horde_adapter_claude::codec::{decode_line, encode_interrupt, encode_user_message};
use horde_adapter_claude::turn::TurnAssembler;

use crate::process::{HarnessSpec, SupervisedHarness};

/// How to launch `claude` on this Worker.
#[derive(Debug, Clone)]
pub struct ClaudeAdapter {
    /// Binary to run. A path, so tests can point at a stub and never touch the
    /// operator's real CLI.
    pub program: String,
    /// Root under which each account's `CLAUDE_CONFIG_DIR` lives.
    pub accounts_root: Option<PathBuf>,
    /// The `--permission-prompt-tool` value, when Horde's MCP endpoint is up.
    ///
    /// A constructor option rather than a hardcoded flag because the endpoint is
    /// the next increment: the seam exists now so wiring it later is not a
    /// change to spawn logic.
    pub permission_prompt_tool: Option<String>,
    /// Extra environment for every child this adapter spawns.
    ///
    /// Per-child, never process-wide. A supervisor that configured children by
    /// setting its own variables would race every concurrent spawn — which is
    /// precisely what happened when the tests tried it.
    pub extra_env: Vec<(String, String)>,
}

impl ClaudeAdapter {
    /// An adapter that runs the real `claude` on `PATH`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            program: "claude".to_owned(),
            accounts_root: None,
            permission_prompt_tool: None,
            extra_env: Vec::new(),
        }
    }

    /// An adapter pointed at an explicit binary — how the process tests reach
    /// the stub harness without ever risking the operator's real CLI.
    #[must_use]
    pub fn with_program(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            ..Self::new()
        }
    }

    /// The flags spike W1c established as the minimum for a usable stream.
    ///
    /// `--verbose` is mandatory alongside `-p`; without it the harness collapses
    /// the stream to a final message and the turn cannot be watched.
    fn base_args(&self, spec: &AgentSpec) -> Vec<String> {
        let mut args = vec![
            "-p".to_owned(),
            "--verbose".to_owned(),
            "--input-format".to_owned(),
            "stream-json".to_owned(),
            "--output-format".to_owned(),
            "stream-json".to_owned(),
        ];

        if let Some(model) = &spec.model {
            args.push("--model".to_owned());
            args.push(model.clone());
        }
        if let Some(overlay) = &spec.append_system_prompt {
            // Appended, never replacing: the harness's own prompt is what makes
            // its tools work.
            args.push("--append-system-prompt".to_owned());
            args.push(overlay.clone());
        }
        // The endpoint from the spec is the production wire (the Worker mints
        // one per agent); the constructor option remains for callers that
        // configure the tool without an endpoint (the live smoke).
        if let Some(config) = mcp_config_for(spec.horde_endpoint.as_ref()) {
            args.push("--mcp-config".to_owned());
            args.push(config);
        }
        let prompt_tool = match (&self.permission_prompt_tool, &spec.horde_endpoint) {
            (Some(tool), _) => Some(tool.clone()),
            // The verb is served as `approve` on the endpoint named `horde`;
            // the harness addresses it by its namespaced MCP name.
            (None, Some(_)) => Some("mcp__horde__approve".to_owned()),
            (None, None) => None,
        };
        if let Some(tool) = prompt_tool {
            args.push("--permission-prompt-tool".to_owned());
            args.push(tool);
            // ADR-0003 (re-verified 2026-08-11): ambient settings allowlists
            // decide a tool call BEFORE the permission tool ever sees it — an
            // allowed call executes with no prompt, no denial, and no event.
            // For the §10 chain to be the real authority, the Worker must own
            // the permission settings the harness reads: explicit empty
            // allow/deny lists, and never the operator's ~/.claude/settings.
            args.push("--settings".to_owned());
            args.push(r#"{"permissions":{"allow":[],"deny":[]}}"#.to_owned());
            args.push("--permission-mode".to_owned());
            args.push("manual".to_owned());
        }
        args
    }

    fn harness_spec(&self, spec: &AgentSpec, extra: Vec<String>) -> HarnessSpec {
        let mut args = self.base_args(spec);
        args.extend(extra);

        HarnessSpec {
            program: self.program.clone(),
            args,
            env: self.extra_env.clone(),
            config_dir: horde_adapter_claude::identity::config_dir_for(
                spec.account.as_ref(),
                self.accounts_root
                    .as_deref()
                    .unwrap_or_else(|| std::path::Path::new(".")),
            ),
            config_env_var: "CLAUDE_CONFIG_DIR",
            cwd: spec.cwd.clone(),
        }
    }
}

impl Default for ClaudeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Adapter for ClaudeAdapter {
    fn provider(&self) -> &str {
        horde_adapter_claude::PROVIDER
    }

    fn capabilities(&self) -> Capabilities {
        horde_adapter_claude::capabilities()
    }

    fn login_recipe(&self) -> Option<horde_adapter::LoginRecipe> {
        // The command DESIGN §4 names, against this adapter's own binary so
        // stubs stay reachable in tests. Terminal-only: Claude's flow can
        // finish with a pasted code, so no callback port is promised —
        // PROVISIONAL until verified against the live CLI (see the W9a
        // handoff flags).
        Some(horde_adapter::LoginRecipe::terminal_only(
            self.program.clone(),
            vec!["login".to_owned()],
        ))
    }

    async fn spawn(&self, spec: &AgentSpec) -> Result<Box<dyn AgentSession>> {
        self.start(spec, Vec::new(), TurnAssembler::new())
    }

    async fn resume(
        &self,
        spec: &AgentSpec,
        provider_session_id: &ProviderSessionId,
    ) -> Result<Box<dyn AgentSession>> {
        // Turn numbering continues where the previous processes stopped, or the
        // revived turns would re-mint journal keys and be silently dropped.
        let turns_taken = spec.turns_taken().map_err(|message| AdapterError::Spawn {
            provider: horde_adapter_claude::PROVIDER.to_owned(),
            message,
        })?;
        // Under the SAME config dir: a session recorded under one account does
        // not exist under another, and an unauthenticated directory fails loudly
        // rather than starting a silently fresh conversation.
        self.start(
            spec,
            vec!["--resume".to_owned(), provider_session_id.to_string()],
            TurnAssembler::resuming_after(turns_taken),
        )
    }
}

impl ClaudeAdapter {
    /// Starts a supervised `claude` and wraps it as a session.
    ///
    /// Spawning and reviving differ only in the extra flags and where the turn
    /// numbering starts, so they share the launch: a second copy is a second
    /// place for the provider to go missing from a spawn failure.
    fn start(
        &self,
        spec: &AgentSpec,
        extra: Vec<String>,
        assembler: TurnAssembler,
    ) -> Result<Box<dyn AgentSession>> {
        let harness =
            SupervisedHarness::spawn(&self.harness_spec(spec, extra)).map_err(|error| {
                AdapterError::Spawn {
                    provider: horde_adapter_claude::PROVIDER.to_owned(),
                    message: error.to_string(),
                }
            })?;
        Ok(Box::new(LiveClaudeSession::new(harness, assembler)))
    }
}

/// One live `claude` conversation.
pub struct LiveClaudeSession {
    harness: SupervisedHarness,
    assembler: TurnAssembler,
    pending: VecDeque<AgentEvent>,
    session_id: Option<ProviderSessionId>,
    /// Set once the child's stdout has closed, so later reads report the exit
    /// instead of blocking on a pipe nobody will write to again.
    ended: bool,
}

impl LiveClaudeSession {
    fn new(harness: SupervisedHarness, assembler: TurnAssembler) -> Self {
        Self {
            harness,
            assembler,
            pending: VecDeque::new(),
            session_id: None,
            ended: false,
        }
    }

    /// The child's stdout closed while a conversation was open: the harness is
    /// dead. Reap it for the exit status — the one fact that distinguishes a
    /// crash from a refusal-to-start — and keep its stderr where an operator
    /// can find it, because that is the only explanation a dead harness leaves.
    async fn report_death(&mut self) -> AdapterError {
        self.ended = true;
        let exit_code = self.harness.wait().await.ok().flatten();
        if let Some(stderr) = death_diagnostics(&self.harness.diagnostics().await) {
            tracing::warn!(
                ?exit_code,
                %stderr,
                "the claude harness died; its stderr is the only explanation it left"
            );
        }
        AdapterError::HarnessExited { exit_code }
    }
}

/// The `--mcp-config` JSON handing one agent its Horde endpoint.
///
/// Inline JSON rather than a file: `CLAUDE_CONFIG_DIR` is shared by every
/// agent on the same Provider Account, and a config file written there by one
/// spawn would race every concurrent one — the same reasoning as Codex's
/// per-process `-c` overrides. The identity headers ride the entry because
/// the endpoint's gate refuses any request without them.
fn mcp_config_for(endpoint: Option<&horde_adapter::McpEndpoint>) -> Option<String> {
    let horde_adapter::McpEndpoint::Http { url, headers } = endpoint? else {
        // The stdio shape is Codex vocabulary; the Worker serves Claude over
        // HTTP. Refusing to render it would fail the spawn for a shape nobody
        // produces — leaving it unwired is the honest no-op.
        return None;
    };
    let headers: serde_json::Map<String, serde_json::Value> = headers
        .iter()
        .map(|(name, value)| (name.clone(), serde_json::Value::String(value.clone())))
        .collect();
    Some(
        serde_json::json!({
            "mcpServers": {
                "horde": { "type": "http", "url": url, "headers": headers }
            }
        })
        .to_string(),
    )
}

/// What a dead harness left on stderr, or `None` when it left nothing.
///
/// Pure so the distinction is testable: a death that logs an empty stderr
/// drowns the real explanation in noise, and one that drops a non-empty
/// stderr discards the only explanation there is.
fn death_diagnostics(diagnostics: &[String]) -> Option<String> {
    if diagnostics.is_empty() {
        None
    } else {
        Some(diagnostics.join("\n"))
    }
}

#[async_trait]
impl AgentSession for LiveClaudeSession {
    fn provider_session_id(&self) -> Option<&ProviderSessionId> {
        self.session_id.as_ref()
    }

    async fn send(&mut self, input: TurnInput) -> Result<()> {
        if self.ended {
            return Err(AdapterError::HarnessExited { exit_code: None });
        }
        // An interrupting message aborts any turn in flight first (the trait's
        // contract for Delivery::Interrupt). The control message is the only
        // mechanism that does that and leaves the session usable; the harness
        // then closes the aborted turn with its own `aborted_streaming` result.
        if input.delivery == Delivery::Interrupt {
            self.interrupt().await?;
        }
        self.harness
            .write_line(&encode_user_message(&input.text))
            .await
            .map_err(|e| AdapterError::Protocol {
                message: e.to_string(),
                raw: String::new(),
            })
    }

    async fn next_event(&mut self) -> Result<Option<AgentEvent>> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
            if self.ended {
                return Ok(None);
            }

            let line = self
                .harness
                .read_line()
                .await
                .map_err(|e| AdapterError::Protocol {
                    message: e.to_string(),
                    raw: String::new(),
                })?;

            let Some(line) = line else {
                // EOF: the harness is gone. The Worker never closes stdin on a
                // live agent, so an EOF here is always a death, never a normal
                // end — reported as one, with the exit status attached, and
                // never waited on. (`shutdown` returns before this is reached.)
                return Err(self.report_death().await);
            };

            let envelope = decode_line(&line)?;
            self.pending.extend(self.assembler.feed(&envelope));
            self.session_id = self.assembler.provider_session_id();
        }
    }

    async fn answer_permission(
        &mut self,
        tool_call_id: &ToolCallId,
        _decision: PermissionDecision,
    ) -> Result<()> {
        // Permission prompts arrive on the MCP endpoint, not on stdout, and that
        // endpoint is the next increment. Refusing beats accepting a decision
        // that would go nowhere.
        Err(AdapterError::UnknownPermissionRequest {
            tool_call_id: tool_call_id.clone(),
        })
    }

    async fn interrupt(&mut self) -> Result<()> {
        // The control message is the ONLY mechanism that aborts a turn and
        // leaves the session usable (ADR-0003): SIGINT ends the process, and
        // closing stdin does not interrupt at all — it lets the turn run to
        // completion.
        self.harness
            .write_line(&encode_interrupt("horde-interrupt"))
            .await
            .map_err(|e| AdapterError::Protocol {
                message: e.to_string(),
                raw: String::new(),
            })
    }

    async fn shutdown(&mut self) -> Result<()> {
        // Kill by handle. Never by name or pattern.
        let _ = self.harness.kill().await;
        self.ended = true;
        self.pending.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use horde_adapter::{Adapter, AgentSpec, Capability};

    use super::ClaudeAdapter;

    /// The login recipe rides the adapter's own binary — terminal-only, per
    /// DESIGN §4's example — so a stubbed adapter logs in through the same
    /// stub it spawns with. `None` here would silently unplug
    /// `horde worker login <w> claude`.
    #[test]
    fn the_login_recipe_runs_this_adapters_own_binary() {
        let recipe = ClaudeAdapter::with_program("/stub/claude")
            .login_recipe()
            .expect("claude is loginable");
        assert_eq!(recipe.program, "/stub/claude");
        assert_eq!(recipe.args, vec!["login".to_owned()]);
        assert_eq!(
            recipe.callback_port, None,
            "terminal-only until the live CLI's callback is verified"
        );
    }

    /// The flags W1c established as the minimum for a usable stream.
    ///
    /// `--verbose` is the one that bites: without it alongside `-p`, the harness
    /// collapses the stream to a final message and the turn cannot be watched at
    /// all. Pinned here because losing it degrades silently — output still
    /// arrives, just never incrementally.
    #[test]
    fn the_base_flags_keep_the_stream_watchable() {
        let args = ClaudeAdapter::new().base_args(&AgentSpec::default());

        for required in [
            "-p",
            "--verbose",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
        ] {
            assert!(
                args.iter().any(|a| a == required),
                "{required} is required for a usable stream; got {args:?}"
            );
        }
    }

    #[test]
    fn the_overlay_is_appended_and_the_model_is_named() {
        let args = ClaudeAdapter::new().base_args(&AgentSpec {
            model: Some("haiku".to_owned()),
            append_system_prompt: Some("overlay text".to_owned()),
            ..AgentSpec::default()
        });

        // Appended, never replacing: the harness's own prompt is what makes its
        // tools work, so there is deliberately no --system-prompt path.
        let joined = args.join(" ");
        assert!(joined.contains("--append-system-prompt overlay text"));
        assert!(joined.contains("--model haiku"));
        assert!(
            !joined.contains("--system-prompt "),
            "replacing the harness prompt would break its tools"
        );
    }

    /// The permission seam: absent by default, present when configured.
    #[test]
    fn the_permission_prompt_tool_is_opt_in() {
        let plain = ClaudeAdapter::new().base_args(&AgentSpec::default());
        assert!(
            !plain.iter().any(|a| a == "--permission-prompt-tool"),
            "no endpoint means no flag, not an empty one"
        );

        let mut wired = ClaudeAdapter::new();
        wired.permission_prompt_tool = Some("mcp__horde__approve".to_owned());
        let args = wired.base_args(&AgentSpec::default());
        assert!(args.iter().any(|a| a == "mcp__horde__approve"));
    }

    /// ADR-0003's re-verified finding: ambient settings allowlists decide a
    /// tool call *before* the permission tool sees it — a silent bypass of the
    /// whole §10 chain. A wired permission tool therefore always travels with
    /// explicit empty allow/deny lists and manual mode, so the harness reads
    /// permission settings this Worker owns, never the operator's.
    #[test]
    fn a_wired_permission_tool_brings_its_own_empty_settings() {
        let mut wired = ClaudeAdapter::new();
        wired.permission_prompt_tool = Some("mcp__horde__approve".to_owned());
        let args = wired.base_args(&AgentSpec::default());
        let joined = args.join(" ");

        assert!(
            joined.contains(r#"--settings {"permissions":{"allow":[],"deny":[]}}"#),
            "the worker must own the settings the harness reads: {joined}"
        );
        assert!(
            joined.contains("--permission-mode manual"),
            "anything but manual lets a mode allowlist bypass the chain: {joined}"
        );

        // And without the tool there is no settings override either: an agent
        // with no permission chain runs the harness as configured.
        let plain = ClaudeAdapter::new().base_args(&AgentSpec::default());
        assert!(
            !plain.iter().any(|a| a == "--settings"),
            "no chain means no override, not an empty one"
        );
    }

    /// The production wire: a spec carrying the Worker-minted endpoint gets
    /// the `--mcp-config` entry (identity headers included), the namespaced
    /// permission prompt tool, and the ADR-0003 settings hardening — all
    /// three, because any one missing silently disarms the §10 chain.
    #[test]
    fn a_spec_with_an_endpoint_wires_the_mcp_config_and_the_chain() {
        let spec = AgentSpec {
            horde_endpoint: Some(horde_adapter::McpEndpoint::Http {
                url: "http://127.0.0.1:8942/mcp".to_owned(),
                headers: vec![
                    ("Authorization".to_owned(), "Bearer tok-9".to_owned()),
                    ("X-Horde-Agent".to_owned(), "agent-9".to_owned()),
                ],
            }),
            ..AgentSpec::default()
        };
        let args = ClaudeAdapter::new().base_args(&spec);
        let joined = args.join(" ");

        let config_index = args
            .iter()
            .position(|a| a == "--mcp-config")
            .unwrap_or_else(|| panic!("--mcp-config must be emitted: {joined}"));
        let config: serde_json::Value =
            serde_json::from_str(&args[config_index + 1]).expect("the mcp config is inline JSON");
        assert_eq!(config["mcpServers"]["horde"]["type"], "http");
        assert_eq!(
            config["mcpServers"]["horde"]["url"],
            "http://127.0.0.1:8942/mcp"
        );
        assert_eq!(
            config["mcpServers"]["horde"]["headers"]["Authorization"], "Bearer tok-9",
            "without the identity pair every request is refused 401"
        );
        assert_eq!(
            config["mcpServers"]["horde"]["headers"]["X-Horde-Agent"],
            "agent-9"
        );

        assert!(
            joined.contains("--permission-prompt-tool mcp__horde__approve"),
            "the approve verb is addressed by its namespaced MCP name: {joined}"
        );
        assert!(
            joined.contains(r#"--settings {"permissions":{"allow":[],"deny":[]}}"#),
            "an endpoint without the settings hardening lets ambient allowlists \
             bypass the chain: {joined}"
        );
        assert!(joined.contains("--permission-mode manual"), "{joined}");
    }

    /// A configured prompt tool (the live-smoke shape) outranks the derived
    /// name, and a spec without an endpoint changes nothing.
    #[test]
    fn the_constructor_prompt_tool_outranks_the_derived_name() {
        let mut adapter = ClaudeAdapter::new();
        adapter.permission_prompt_tool = Some("mcp__custom__gate".to_owned());
        let spec = AgentSpec {
            horde_endpoint: Some(horde_adapter::McpEndpoint::Http {
                url: "http://127.0.0.1:8931/mcp".to_owned(),
                headers: Vec::new(),
            }),
            ..AgentSpec::default()
        };
        let joined = adapter.base_args(&spec).join(" ");
        assert!(joined.contains("--permission-prompt-tool mcp__custom__gate"));
        assert!(!joined.contains("mcp__horde__approve"));
    }

    /// Both directions of the stderr distinction: a silent death reports no
    /// diagnostics, a noisy one reports exactly what was said.
    #[test]
    fn death_diagnostics_report_what_was_said_and_only_that() {
        assert_eq!(
            super::death_diagnostics(&[]),
            None,
            "a silent death must not log an empty explanation"
        );
        assert_eq!(
            super::death_diagnostics(&["boom".to_owned(), "trace".to_owned()]).as_deref(),
            Some("boom\ntrace"),
            "the harness's own words are the only explanation it left"
        );
    }

    #[test]
    fn the_adapter_reports_the_provider_key_the_wire_contract_uses() {
        assert_eq!(ClaudeAdapter::new().provider(), "claude");
    }

    /// The adapter must pass the codec's measured capabilities through
    /// unchanged — a supervisor that reported its own guess would let callers
    /// rely on something no spike ever demonstrated.
    #[test]
    fn capabilities_come_from_the_codec_not_from_the_supervisor() {
        let claimed = ClaudeAdapter::new().capabilities();

        assert_eq!(claimed, horde_adapter_claude::capabilities());
        assert!(claimed.supports(Capability::Resumable));
        assert!(claimed.supports(Capability::PreAuthorizeToolCalls));
    }
}
