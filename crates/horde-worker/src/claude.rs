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
}

impl ClaudeAdapter {
    /// An adapter that runs the real `claude` on `PATH`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            program: "claude".to_owned(),
            accounts_root: None,
            permission_prompt_tool: None,
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
            env: Vec::new(),
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
