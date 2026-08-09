//! Minimal Rust driver for `claude --input-format stream-json --output-format stream-json`.
//!
//! Spike W1c (ADR-0003): proves a Rust worker can supervise Claude Code as a plain
//! child process — spawn, send a user message, parse the streamed event envelopes,
//! and pull the result plus usage accounting out the far end. No Node sidecar, no
//! Agent SDK.
//!
//! Deliberately thin. Events are kept as raw JSON with typed accessors layered on
//! top, because the wire format carries fields this spike does not model and a
//! worker that silently drops bytes it did not expect cannot be trusted to journal
//! a session faithfully.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// Everything that can go wrong driving the CLI.
#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    #[error("failed to spawn `{binary}`: {source}")]
    Spawn {
        binary: String,
        #[source]
        source: std::io::Error,
    },
    #[error("child process is missing its {stream} pipe")]
    MissingPipe { stream: &'static str },
    #[error("i/o error talking to the child process: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not serialize an outbound message: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("unparseable event on stdout: {source}; raw line was: {line}")]
    Decode {
        line: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("event stream ended before a result message arrived")]
    StreamEndedEarly,
    #[error("turn exceeded the {0:?} timeout")]
    TurnTimeout(Duration),
}

/// How to launch the CLI.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub binary: String,
    pub cwd: Option<PathBuf>,
    pub model: Option<String>,
    pub append_system_prompt: Option<String>,
    /// Built-in tools to expose. `Some(vec![])` disables all tools.
    pub tools: Option<Vec<String>>,
    pub mcp_config: Option<PathBuf>,
    pub permission_prompt_tool: Option<String>,
    pub resume_session_id: Option<String>,
    /// Isolates per-account credential and session state (see spike section 9).
    pub config_dir: Option<PathBuf>,
    pub turn_timeout: Duration,
    pub extra_args: Vec<String>,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            binary: "claude".to_owned(),
            cwd: None,
            model: None,
            append_system_prompt: None,
            tools: None,
            mcp_config: None,
            permission_prompt_tool: None,
            resume_session_id: None,
            config_dir: None,
            turn_timeout: Duration::from_secs(300),
            extra_args: Vec::new(),
        }
    }
}

impl ProbeConfig {
    fn to_args(&self) -> Vec<String> {
        let mut args: Vec<String> = [
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();

        if let Some(model) = &self.model {
            args.push("--model".to_owned());
            args.push(model.clone());
        }
        if let Some(prompt) = &self.append_system_prompt {
            args.push("--append-system-prompt".to_owned());
            args.push(prompt.clone());
        }
        if let Some(tools) = &self.tools {
            args.push("--tools".to_owned());
            args.push(tools.join(","));
        }
        if let Some(path) = &self.mcp_config {
            args.push("--mcp-config".to_owned());
            args.push(path.display().to_string());
            args.push("--strict-mcp-config".to_owned());
        }
        if let Some(tool) = &self.permission_prompt_tool {
            args.push("--permission-prompt-tool".to_owned());
            args.push(tool.clone());
        }
        if let Some(session) = &self.resume_session_id {
            args.push("--resume".to_owned());
            args.push(session.clone());
        }
        args.extend(self.extra_args.iter().cloned());
        args
    }
}

/// One line off the child's stdout: the raw envelope plus typed accessors.
#[derive(Debug, Clone)]
pub struct Event {
    pub raw: Value,
}

impl Event {
    pub fn kind(&self) -> &str {
        self.raw.get("type").and_then(Value::as_str).unwrap_or("")
    }

    pub fn subtype(&self) -> Option<&str> {
        self.raw.get("subtype").and_then(Value::as_str)
    }

    pub fn session_id(&self) -> Option<&str> {
        self.raw.get("session_id").and_then(Value::as_str)
    }

    /// Names of tools the assistant asked to run in this event, if any.
    pub fn tool_uses(&self) -> Vec<String> {
        self.raw
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array)
            .map(|blocks| {
                blocks
                    .iter()
                    .filter(|b| b.get("type").and_then(Value::as_str) == Some("tool_use"))
                    .filter_map(|b| b.get("name").and_then(Value::as_str))
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Assistant-authored plain text in this event.
    pub fn assistant_text(&self) -> Option<String> {
        if self.kind() != "assistant" {
            return None;
        }
        let blocks = self
            .raw
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array)?;
        let text: String = blocks
            .iter()
            .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|b| b.get("text").and_then(Value::as_str))
            .collect();
        (!text.is_empty()).then_some(text)
    }

    pub fn is_result(&self) -> bool {
        self.kind() == "result"
    }

    /// Decode this event as the terminal result message of a turn.
    pub fn as_result(&self) -> Option<Result<ResultMessage, serde_json::Error>> {
        self.is_result()
            .then(|| serde_json::from_value(self.raw.clone()))
    }
}

/// Aggregate token counts for a turn.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
}

/// Per-model breakdown. A turn can bill more than one model: auxiliary work runs
/// on a small model even when `--model` names a large one, so cost accounting
/// must sum this map rather than trusting the requested model name.
/// Note the casing: `usage` is snake_case but `modelUsage` members are camelCase.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelUsage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(rename = "costUSD", default)]
    pub cost_usd: f64,
    #[serde(default)]
    pub context_window: u64,
    #[serde(default)]
    pub canonical_model: String,
}

/// A tool call the permission layer refused.
#[derive(Debug, Clone, Deserialize)]
pub struct PermissionDenial {
    pub tool_name: String,
    #[serde(default)]
    pub tool_use_id: String,
    #[serde(default)]
    pub tool_input: Value,
}

/// The `type: "result"` envelope that closes every turn.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultMessage {
    pub subtype: String,
    /// Authoritative success flag. `subtype` alone is NOT sufficient: an
    /// unauthenticated run reports `subtype: "success"` with `is_error: true`.
    #[serde(default)]
    pub is_error: bool,
    pub session_id: String,
    #[serde(default)]
    pub result: Option<String>,
    /// Cumulative for the lifetime of the child process, NOT per-turn. A worker
    /// metering a long-lived agent must diff consecutive results rather than
    /// summing them, or it will over-bill quadratically.
    #[serde(default)]
    pub total_cost_usd: f64,
    #[serde(default)]
    pub num_turns: u64,
    /// Wall-clock for this turn alone.
    #[serde(default)]
    pub duration_ms: u64,
    /// Cumulative across the process, like [`Self::total_cost_usd`].
    #[serde(default)]
    pub duration_api_ms: u64,
    #[serde(default)]
    pub terminal_reason: Option<String>,
    #[serde(default)]
    pub usage: Usage,
    #[serde(rename = "modelUsage", default)]
    pub model_usage: HashMap<String, ModelUsage>,
    #[serde(default)]
    pub permission_denials: Vec<PermissionDenial>,
}

impl ResultMessage {
    /// True only when the turn both completed and did not carry an error flag.
    pub fn succeeded(&self) -> bool {
        !self.is_error && self.subtype == "success"
    }
}

/// Everything observed during one turn.
#[derive(Debug, Clone)]
pub struct Turn {
    pub result: ResultMessage,
    pub events: Vec<Event>,
}

impl Turn {
    pub fn tool_uses(&self) -> Vec<String> {
        self.events.iter().flat_map(Event::tool_uses).collect()
    }
}

/// A running `claude` child process speaking stream-json in both directions.
pub struct ClaudeProbe {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    turn_timeout: Duration,
    interrupts: u64,
}

impl ClaudeProbe {
    pub fn spawn(config: &ProbeConfig) -> Result<Self, ProbeError> {
        let mut command = Command::new(&config.binary);
        command
            .args(config.to_args())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

        if let Some(cwd) = &config.cwd {
            command.current_dir(cwd);
        }
        if let Some(dir) = &config.config_dir {
            command.env("CLAUDE_CONFIG_DIR", dir);
        }

        let mut child = command.spawn().map_err(|source| ProbeError::Spawn {
            binary: config.binary.clone(),
            source,
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or(ProbeError::MissingPipe { stream: "stdin" })?;
        let stdout = child
            .stdout
            .take()
            .ok_or(ProbeError::MissingPipe { stream: "stdout" })?;

        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            turn_timeout: config.turn_timeout,
            interrupts: 0,
        })
    }

    async fn write_json(&mut self, value: &Value) -> Result<(), ProbeError> {
        let mut line = serde_json::to_string(value).map_err(ProbeError::Encode)?;
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.flush().await?;
        Ok(())
    }

    /// Queue a user message. Multiple messages may be sent to one process; each
    /// yields its own result envelope, with context carried across them.
    pub async fn send_user_message(&mut self, text: &str) -> Result<(), ProbeError> {
        let msg = json!({
            "type": "user",
            "message": { "role": "user", "content": [{ "type": "text", "text": text }] }
        });
        self.write_json(&msg).await
    }

    /// Abort the in-flight turn while keeping the process alive and the session
    /// resumable. Closing stdin does NOT do this — it lets the turn run to
    /// completion — and SIGINT aborts but also kills the process.
    pub async fn interrupt(&mut self) -> Result<(), ProbeError> {
        self.interrupts += 1;
        let msg = json!({
            "type": "control_request",
            "request_id": format!("horde-interrupt-{}", self.interrupts),
            "request": { "subtype": "interrupt" }
        });
        self.write_json(&msg).await
    }

    /// Read the next event, or `None` once the child closes stdout.
    pub async fn next_event(&mut self) -> Result<Option<Event>, ProbeError> {
        match self.stdout.next_line().await? {
            None => Ok(None),
            Some(line) => {
                let raw = serde_json::from_str(&line)
                    .map_err(|source| ProbeError::Decode { line, source })?;
                Ok(Some(Event { raw }))
            }
        }
    }

    /// Drain events until this turn's result envelope arrives.
    pub async fn drain_turn(&mut self) -> Result<Turn, ProbeError> {
        let mut events = Vec::new();
        loop {
            let next = tokio::time::timeout(self.turn_timeout, self.next_event())
                .await
                .map_err(|_| ProbeError::TurnTimeout(self.turn_timeout))??;
            let Some(event) = next else {
                return Err(ProbeError::StreamEndedEarly);
            };
            let decoded = event.as_result();
            events.push(event);
            if let Some(parsed) = decoded {
                let result = parsed.map_err(|source| ProbeError::Decode {
                    line: "<result event>".to_owned(),
                    source,
                })?;
                return Ok(Turn { result, events });
            }
        }
    }

    /// Send a message and collect the whole turn.
    pub async fn run_turn(&mut self, text: &str) -> Result<Turn, ProbeError> {
        self.send_user_message(text).await?;
        self.drain_turn().await
    }

    /// Close stdin and wait for the child to exit.
    pub async fn shutdown(mut self) -> Result<Option<i32>, ProbeError> {
        self.stdin.shutdown().await?;
        drop(self.stdin);
        let status = self.child.wait().await?;
        Ok(status.code())
    }
}
