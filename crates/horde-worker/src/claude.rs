//! The live Claude adapter: the pure codec driven over a real child process.

use std::collections::VecDeque;
use std::path::PathBuf;

use async_trait::async_trait;
use horde_adapter::{
    Adapter, AdapterError, AgentEvent, AgentSession, AgentSpec, Capabilities, PermissionDecision,
    ProviderSessionId, Result, ToolCallId, TurnInput,
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
        if let Some(tool) = &self.permission_prompt_tool {
            args.push("--permission-prompt-tool".to_owned());
            args.push(tool.clone());
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

    async fn spawn(&self, spec: &AgentSpec) -> Result<Box<dyn AgentSession>> {
        let harness =
            SupervisedHarness::spawn(&self.harness_spec(spec, Vec::new())).map_err(|e| {
                AdapterError::Spawn {
                    provider: horde_adapter_claude::PROVIDER.to_owned(),
                    message: e.to_string(),
                }
            })?;
        Ok(Box::new(LiveClaudeSession::new(harness)))
    }

    async fn resume(
        &self,
        spec: &AgentSpec,
        provider_session_id: &ProviderSessionId,
    ) -> Result<Box<dyn AgentSession>> {
        // Under the SAME config dir: a session recorded under one account does
        // not exist under another, and an unauthenticated directory fails loudly
        // rather than starting a silently fresh conversation.
        let extra = vec!["--resume".to_owned(), provider_session_id.to_string()];
        let harness = SupervisedHarness::spawn(&self.harness_spec(spec, extra)).map_err(|e| {
            AdapterError::Spawn {
                provider: horde_adapter_claude::PROVIDER.to_owned(),
                message: e.to_string(),
            }
        })?;
        Ok(Box::new(LiveClaudeSession::new(harness)))
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
    fn new(harness: SupervisedHarness) -> Self {
        Self {
            harness,
            assembler: TurnAssembler::new(),
            pending: VecDeque::new(),
            session_id: None,
            ended: false,
        }
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
                // EOF: the harness is gone. Reported, never waited on.
                self.ended = true;
                return Ok(None);
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

    fn spec() -> AgentSpec {
        AgentSpec {
            model: None,
            append_system_prompt: None,
            cwd: None,
            account: None,
            horde_endpoint: None,
            provider_options: std::collections::BTreeMap::new(),
        }
    }

    /// The flags W1c established as the minimum for a usable stream.
    ///
    /// `--verbose` is the one that bites: without it alongside `-p`, the harness
    /// collapses the stream to a final message and the turn cannot be watched at
    /// all. Pinned here because losing it degrades silently — output still
    /// arrives, just never incrementally.
    #[test]
    fn the_base_flags_keep_the_stream_watchable() {
        let args = ClaudeAdapter::new().base_args(&spec());

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
            ..spec()
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
        let plain = ClaudeAdapter::new().base_args(&spec());
        assert!(
            !plain.iter().any(|a| a == "--permission-prompt-tool"),
            "no endpoint means no flag, not an empty one"
        );

        let mut wired = ClaudeAdapter::new();
        wired.permission_prompt_tool = Some("mcp__horde__approve".to_owned());
        let args = wired.base_args(&spec());
        assert!(args.iter().any(|a| a == "mcp__horde__approve"));
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
