//! The live Claude adapter: the pure codec driven over a real child process.

use std::collections::VecDeque;
use std::path::Path;

use async_trait::async_trait;
use horde_adapter::{
    Adapter, AdapterError, AgentEvent, AgentSession, AgentSpec, Capabilities, Delivery,
    PermissionDecision, ProviderSessionId, Result, ToolCallId, TurnInput,
};
use horde_adapter_claude::codec::{decode_line, encode_interrupt, encode_user_message};
use horde_adapter_claude::launch::IsolationFlags;
use horde_adapter_claude::turn::TurnAssembler;

use crate::harness::HarnessProfile;
use crate::process::{HarnessSpec, SupervisedHarness, SupervisorError};

/// The variable `claude` reads its config directory from.
const CONFIG_ENV_VAR: &str = "CLAUDE_CONFIG_DIR";

/// How to launch `claude` on this Worker.
#[derive(Debug, Clone)]
pub struct ClaudeAdapter {
    /// Binary to run. A path, so tests can point at a stub and never touch the
    /// operator's real CLI.
    pub program: String,
    /// The Worker-owned config directories, secret files, and process-group
    /// ledger every launch runs under (ADR-0010).
    pub profile: HarnessProfile,
    /// The `--permission-prompt-tool` value for a launch with no endpoint.
    ///
    /// The production wire derives it from the spec's endpoint; this option
    /// exists for callers that configure the tool without one.
    pub permission_prompt_tool: Option<String>,
    /// Extra environment for every child this adapter spawns.
    ///
    /// Per-child, never process-wide. A supervisor that configured children by
    /// setting its own variables would race every concurrent spawn — which is
    /// precisely what happened when the tests tried it.
    pub extra_env: Vec<(String, String)>,
}

impl ClaudeAdapter {
    /// An adapter that runs the real `claude` on `PATH` under `profile`.
    #[must_use]
    pub fn new(profile: HarnessProfile) -> Self {
        Self {
            program: "claude".to_owned(),
            profile,
            permission_prompt_tool: None,
            extra_env: Vec::new(),
        }
    }

    /// Points the adapter at an explicit binary — how the process tests reach
    /// the stub harness without ever risking the operator's real CLI.
    #[must_use]
    pub fn with_program(mut self, program: impl Into<String>) -> Self {
        self.program = program.into();
        self
    }

    /// The `--permission-prompt-tool` this launch carries, if any.
    fn prompt_tool(&self, spec: &AgentSpec) -> Option<String> {
        match (&self.permission_prompt_tool, &spec.horde_endpoint) {
            (Some(tool), _) => Some(tool.clone()),
            // The verb is served as `approve` on the endpoint named `horde`;
            // the harness addresses it by its namespaced MCP name.
            (None, Some(_)) => Some("mcp__horde__approve".to_owned()),
            (None, None) => None,
        }
    }

    /// The argv for one launch: the stream flags spike W1c established as the
    /// minimum, then the isolation flags every supervised launch carries.
    ///
    /// `--verbose` is mandatory alongside `-p`; without it the harness collapses
    /// the stream to a final message and the turn cannot be watched.
    fn base_args(&self, spec: &AgentSpec, mcp_config: Option<&Path>) -> Vec<String> {
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
        // ADR-0010, on top of ADR-0003's re-verification: no settings file
        // but the Worker's own is read (an ambient allowlist decides a tool
        // call before the permission tool ever sees it), no MCP server but
        // the Worker's own loads, and a wired chain brings empty allow/deny
        // lists and manual mode. The same builder the live smokes run.
        let prompt_tool = self.prompt_tool(spec);
        args.extend(
            IsolationFlags {
                mcp_config,
                permission_prompt_tool: prompt_tool.as_deref(),
            }
            .argv(),
        );
        args
    }

    /// The full launch: the account's Worker-owned config directory, the
    /// endpoint's MCP config written owner-only (its bearer token never on
    /// argv), and the argv built around it.
    fn harness_spec(
        &self,
        spec: &AgentSpec,
        extra: Vec<String>,
    ) -> core::result::Result<HarnessSpec, String> {
        let held = mcp_config_for(spec.horde_endpoint.as_ref())
            .map(|config| self.profile.secret_file("mcp-", config.as_bytes()))
            .transpose()?;
        let mut args = self.base_args(spec, held.as_ref().map(|file| file.path()));
        args.extend(extra);

        Ok(HarnessSpec {
            args,
            env: self.extra_env.clone(),
            cwd: spec.cwd.clone(),
            held_files: held.into_iter().collect(),
            ..self
                .profile
                .spec(&self.program, CONFIG_ENV_VAR, spec.account.as_ref())?
        })
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
        // handoff flags). It signs in the Worker-owned config directory the
        // agents run under (ADR-0010), not the operator's own.
        Some(
            horde_adapter::LoginRecipe::terminal_only(
                self.program.clone(),
                vec!["login".to_owned()],
            )
            .with_env(self.profile.login_env(CONFIG_ENV_VAR)),
        )
    }

    async fn spawn(&self, spec: &AgentSpec) -> Result<Box<dyn AgentSession>> {
        self.start(spec, Vec::new(), TurnAssembler::new()).await
    }

    async fn resume(
        &self,
        spec: &AgentSpec,
        provider_session_id: &ProviderSessionId,
    ) -> Result<Box<dyn AgentSession>> {
        // Turn numbering continues where the previous processes stopped, or the
        // revived turns would re-mint journal keys and be silently dropped.
        let turns_taken = spec.turns_taken().map_err(spawn_error)?;
        // Under the SAME config dir: a session recorded under one account does
        // not exist under another, and an unauthenticated directory fails loudly
        // rather than starting a silently fresh conversation.
        self.start(
            spec,
            vec!["--resume".to_owned(), provider_session_id.to_string()],
            TurnAssembler::resuming_after(turns_taken),
        )
        .await
    }
}

/// A launch that never got as far as a running harness.
fn spawn_error(message: String) -> AdapterError {
    AdapterError::Spawn {
        provider: horde_adapter_claude::PROVIDER.to_owned(),
        message,
    }
}

impl ClaudeAdapter {
    /// Starts a supervised `claude` and wraps it as a session.
    ///
    /// Spawning and reviving differ only in the extra flags and where the turn
    /// numbering starts, so they share the launch: a second copy is a second
    /// place for the provider to go missing from a spawn failure.
    async fn start(
        &self,
        spec: &AgentSpec,
        extra: Vec<String>,
        assembler: TurnAssembler,
    ) -> Result<Box<dyn AgentSession>> {
        let launch = self.harness_spec(spec, extra).map_err(spawn_error)?;
        let harness = SupervisedHarness::spawn(&launch)
            .await
            .map_err(|error| spawn_error(error.to_string()))?;
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

    /// Writes one line, or reports the harness dead.
    ///
    /// A hang-up is the write-side face of the death EOF announces on the
    /// read side — which one the session meets first is the scheduler's race
    /// — so it is reported identically, exit status and stderr included.
    async fn write(&mut self, line: &str) -> Result<()> {
        match self.harness.write_line(line).await {
            Ok(()) => Ok(()),
            Err(SupervisorError::HungUp) => Err(self.report_death().await),
            Err(error) => Err(AdapterError::Protocol {
                message: error.to_string(),
                raw: String::new(),
            }),
        }
    }
}

/// The `--mcp-config` JSON handing one agent its Horde endpoint.
///
/// Written to an owner-only file per launch, never passed inline: the
/// identity headers ride the entry (the endpoint's gate refuses any request
/// without them), and an inline config would put the bearer token on argv,
/// where `ps` shows it to every user on the machine (ADR-0010). Per launch
/// rather than a shared file in the config directory, which every agent on
/// the same account shares and a concurrent spawn would race.
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
        self.write(&encode_user_message(&input.text)).await
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
        self.write(&encode_interrupt("horde-interrupt")).await
    }

    async fn shutdown(&mut self) -> Result<()> {
        // Stop by handle — the harness and everything it started, SIGTERM
        // then SIGKILL. Never by name or pattern.
        let _ = self.harness.stop().await;
        self.ended = true;
        self.pending.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use horde_adapter::{Adapter, AgentSpec, Capability, ProviderAccount};

    use super::ClaudeAdapter;
    use crate::harness::HarnessProfile;

    /// An adapter whose profile lives in `scratch` — argv-shaping tests never
    /// spawn, but the spec builder prepares real directories.
    fn adapter_in(scratch: &tempfile::TempDir) -> ClaudeAdapter {
        ClaudeAdapter::new(HarnessProfile::new(scratch.path().join("claude")))
    }

    fn scratch() -> tempfile::TempDir {
        tempfile::tempdir().expect("a scratch dir")
    }

    fn endpoint_spec(token: &str) -> AgentSpec {
        AgentSpec {
            horde_endpoint: Some(horde_adapter::McpEndpoint::Http {
                url: "http://127.0.0.1:8942/mcp".to_owned(),
                headers: vec![
                    ("Authorization".to_owned(), format!("Bearer {token}")),
                    ("X-Horde-Agent".to_owned(), "agent-9".to_owned()),
                ],
            }),
            ..AgentSpec::default()
        }
    }

    /// The login recipe rides the adapter's own binary — terminal-only, per
    /// DESIGN §4's example — so a stubbed adapter logs in through the same
    /// stub it spawns with, and it signs in the profile's default config
    /// directory: the one account-less agents run under. Without the env,
    /// `horde worker login` would sign in the operator's own directory and
    /// every agent would stay logged out.
    #[test]
    fn the_login_recipe_signs_in_the_directory_agents_run_under() {
        let scratch = scratch();
        let adapter = adapter_in(&scratch).with_program("/stub/claude");
        let recipe = adapter.login_recipe().expect("claude is loginable");
        assert_eq!(recipe.program, "/stub/claude");
        assert_eq!(recipe.args, vec!["login".to_owned()]);
        assert_eq!(
            recipe.callback_port, None,
            "terminal-only until the live CLI's callback is verified"
        );

        let launch = adapter
            .harness_spec(&AgentSpec::default(), Vec::new())
            .expect("a launch");
        let agents_dir = launch.config_dir.expect("always a worker-owned dir");
        assert_eq!(
            recipe.env,
            vec![(
                "CLAUDE_CONFIG_DIR".to_owned(),
                agents_dir.display().to_string()
            )]
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
        let scratch = scratch();
        let args = adapter_in(&scratch).base_args(&AgentSpec::default(), None);

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
        let scratch = scratch();
        let args = adapter_in(&scratch).base_args(
            &AgentSpec {
                model: Some("haiku".to_owned()),
                append_system_prompt: Some("overlay text".to_owned()),
                ..AgentSpec::default()
            },
            None,
        );

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

    /// ADR-0010: the production argv carries the isolation flags on every
    /// launch, chain or no chain — `--setting-sources ''` so no operator or
    /// repo settings file (allowlists, hooks, plugins) is read, and
    /// `--strict-mcp-config` so no ambient MCP server loads. Pinned on the
    /// full launch, not just a helper, because that is what production runs.
    #[test]
    fn every_production_launch_carries_the_isolation_flags() {
        let scratch = scratch();
        let adapter = adapter_in(&scratch);
        for spec in [AgentSpec::default(), endpoint_spec("tok-1")] {
            let args = adapter
                .harness_spec(&spec, Vec::new())
                .expect("a launch")
                .args;
            let sources = args
                .iter()
                .position(|a| a == "--setting-sources")
                .unwrap_or_else(|| panic!("--setting-sources must be passed: {args:?}"));
            assert_eq!(
                args.get(sources + 1).map(String::as_str),
                Some(""),
                "an empty source list reads no settings file at all: {args:?}"
            );
            assert!(
                args.iter().any(|a| a == "--strict-mcp-config"),
                "ambient MCP servers must not load: {args:?}"
            );
        }
    }

    /// The permission seam: absent by default, present when configured.
    #[test]
    fn the_permission_prompt_tool_is_opt_in() {
        let scratch = scratch();
        let plain = adapter_in(&scratch).base_args(&AgentSpec::default(), None);
        assert!(
            !plain.iter().any(|a| a == "--permission-prompt-tool"),
            "no endpoint means no flag, not an empty one"
        );

        let mut wired = adapter_in(&scratch);
        wired.permission_prompt_tool = Some("mcp__horde__approve".to_owned());
        let args = wired.base_args(&AgentSpec::default(), None);
        assert!(args.iter().any(|a| a == "mcp__horde__approve"));
    }

    /// ADR-0003's re-verified finding: ambient settings allowlists decide a
    /// tool call *before* the permission tool sees it — a silent bypass of the
    /// whole §10 chain. A wired permission tool therefore always travels with
    /// explicit empty allow/deny lists and manual mode.
    #[test]
    fn a_wired_permission_tool_brings_its_own_empty_settings() {
        let scratch = scratch();
        let mut wired = adapter_in(&scratch);
        wired.permission_prompt_tool = Some("mcp__horde__approve".to_owned());
        let joined = wired.base_args(&AgentSpec::default(), None).join(" ");

        assert!(
            joined.contains(r#"--settings {"permissions":{"allow":[],"deny":[]}}"#),
            "the worker must own the settings the harness reads: {joined}"
        );
        assert!(
            joined.contains("--permission-mode manual"),
            "anything but manual lets a mode allowlist bypass the chain: {joined}"
        );

        // And without the tool there is no settings override either: an agent
        // with no permission chain keeps the harness's default mode.
        let plain = adapter_in(&scratch).base_args(&AgentSpec::default(), None);
        assert!(
            !plain.iter().any(|a| a == "--settings"),
            "no chain means no override, not an empty one"
        );
    }

    /// The production wire: a spec carrying the Worker-minted endpoint gets
    /// its MCP config in an owner-only file the argv names — the bearer token
    /// is in the file and nowhere on the command line, where `ps` would show
    /// it to every user — plus the namespaced prompt tool and the settings
    /// hardening.
    #[test]
    fn a_spec_with_an_endpoint_keeps_its_token_off_argv() {
        let scratch = scratch();
        let launch = adapter_in(&scratch)
            .harness_spec(&endpoint_spec("tok-secret-9"), Vec::new())
            .expect("a launch");
        let joined = launch.args.join(" ");

        assert!(
            !joined.contains("tok-secret-9"),
            "the bearer token must never ride argv: {joined}"
        );
        let config_index = launch
            .args
            .iter()
            .position(|a| a == "--mcp-config")
            .unwrap_or_else(|| panic!("--mcp-config must be emitted: {joined}"));
        let path = std::path::PathBuf::from(&launch.args[config_index + 1]);
        assert_eq!(
            launch
                .held_files
                .first()
                .map(|file| file.path().to_path_buf()),
            Some(path.clone()),
            "the harness holds the file it was pointed at, so it lives as long"
        );
        let mode = std::fs::metadata(&path)
            .expect("the config file exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "a file carrying a bearer token is owner-only");

        let config: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("readable"))
                .expect("the mcp config is JSON");
        assert_eq!(config["mcpServers"]["horde"]["type"], "http");
        assert_eq!(
            config["mcpServers"]["horde"]["url"],
            "http://127.0.0.1:8942/mcp"
        );
        assert_eq!(
            config["mcpServers"]["horde"]["headers"]["Authorization"], "Bearer tok-secret-9",
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
            "{joined}"
        );
        assert!(joined.contains("--permission-mode manual"), "{joined}");

        drop(launch);
        assert!(
            !path.exists(),
            "the secret dies with the launch that held it"
        );
    }

    /// The config directory is always the Worker's: the profile's default for
    /// an account-less agent, the account's own directory otherwise, and a
    /// hostile account name refuses the launch rather than escaping the root.
    #[test]
    fn the_config_dir_is_always_worker_owned() {
        let scratch = scratch();
        let adapter = adapter_in(&scratch);
        let root = scratch.path().join("claude");

        let plain = adapter
            .harness_spec(
                &AgentSpec {
                    cwd: Some(std::path::PathBuf::from("/checkout/agent-1")),
                    ..AgentSpec::default()
                },
                Vec::new(),
            )
            .expect("a launch");
        assert_eq!(plain.config_env_var, "CLAUDE_CONFIG_DIR");
        assert_eq!(plain.config_dir, Some(root.join("default")));
        assert_eq!(
            plain.cwd,
            Some(std::path::PathBuf::from("/checkout/agent-1")),
            "the agent runs in its checkout, not wherever the Worker runs"
        );

        let bound = adapter
            .harness_spec(
                &AgentSpec {
                    account: Some(ProviderAccount::new("work")),
                    ..AgentSpec::default()
                },
                vec!["--resume".to_owned(), "s1".to_owned()],
            )
            .expect("a launch");
        assert_eq!(bound.config_dir, Some(root.join("accounts").join("work")));
        assert!(
            bound
                .args
                .ends_with(&["--resume".to_owned(), "s1".to_owned()])
        );

        let hostile = adapter.harness_spec(
            &AgentSpec {
                account: Some(ProviderAccount::new("../escape")),
                ..AgentSpec::default()
            },
            Vec::new(),
        );
        assert!(
            hostile.is_err(),
            "an escaping account must refuse the launch"
        );
    }

    /// A configured prompt tool (the live-smoke shape) outranks the derived
    /// name, and a spec without an endpoint changes nothing.
    #[test]
    fn the_constructor_prompt_tool_outranks_the_derived_name() {
        let scratch = scratch();
        let mut adapter = adapter_in(&scratch);
        adapter.permission_prompt_tool = Some("mcp__custom__gate".to_owned());
        let joined = adapter
            .harness_spec(&endpoint_spec("t"), Vec::new())
            .expect("a launch")
            .args
            .join(" ");
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
        let scratch = scratch();
        assert_eq!(adapter_in(&scratch).provider(), "claude");
    }

    /// The adapter must pass the codec's measured capabilities through
    /// unchanged — a supervisor that reported its own guess would let callers
    /// rely on something no spike ever demonstrated.
    #[test]
    fn capabilities_come_from_the_codec_not_from_the_supervisor() {
        let scratch = scratch();
        let claimed = adapter_in(&scratch).capabilities();

        assert_eq!(claimed, horde_adapter_claude::capabilities());
        assert!(claimed.supports(Capability::Resumable));
        assert!(claimed.supports(Capability::PreAuthorizeToolCalls));
    }
}
