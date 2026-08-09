//! W2c spike probe: drive `codex app-server --stdio` from Rust.
//!
//! Proves the chosen surface is drivable from a Rust supervisor the way
//! `horde-worker` will drive it — spawn the harness as a child process, speak
//! newline-delimited JSON-RPC over its stdio, and record every frame.
//!
//! This is deliberately not the adapter. It has no trait, no async runtime, and
//! no reconnect logic; it exists to answer "does the wire work from Rust" and
//! to leave a transcript behind. Wave 5 writes the real thing.
//!
//! Usage:
//!   cargo run -- <transcript.jsonl> [prompt]
//!
//! Threads are started read-only, so the model cannot write outside its cwd
//! without an approval this probe is not able to grant.

use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

/// Anything that can go wrong talking to the harness.
#[derive(Debug)]
enum ProbeError {
    Io(std::io::Error),
    Json(serde_json::Error),
    /// The harness closed its stdout before saying what we were waiting for.
    StreamEnded(&'static str),
    Usage(String),
}

impl fmt::Display for ProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Json(e) => write!(f, "json: {e}"),
            Self::StreamEnded(what) => {
                write!(f, "app-server closed the stream while awaiting {what}")
            }
            Self::Usage(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for ProbeError {}

impl From<std::io::Error> for ProbeError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for ProbeError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// A supervised `codex app-server` child, plus the transcript of its wire.
struct AppServer {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    transcript: File,
    next_id: u64,
}

impl AppServer {
    fn spawn(transcript_path: &str) -> Result<Self, ProbeError> {
        let mut child = Command::new("codex")
            .args(["app-server", "--stdio"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;

        // Taking both handles is what makes the borrow checker allow reading and
        // writing concurrently below; `child.stdin` stays None afterwards.
        let stdin = child.stdin.take().ok_or(ProbeError::StreamEnded("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(ProbeError::StreamEnded("stdout"))?;

        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            transcript: File::create(transcript_path)?,
            next_id: 0,
        })
    }

    fn record(&mut self, direction: &str, frame: &serde_json::Value) -> Result<(), ProbeError> {
        let line = serde_json::json!({ "dir": direction, "msg": frame });
        writeln!(self.transcript, "{line}")?;
        Ok(())
    }

    fn send(&mut self, frame: serde_json::Value) -> Result<(), ProbeError> {
        self.record("out", &frame)?;
        writeln!(self.stdin, "{frame}")?;
        self.stdin.flush()?;
        Ok(())
    }

    fn request(&mut self, method: &str, params: serde_json::Value) -> Result<u64, ProbeError> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))?;
        Ok(id)
    }

    fn notify(&mut self, method: &str) -> Result<(), ProbeError> {
        self.send(serde_json::json!({ "jsonrpc": "2.0", "method": method }))
    }

    /// Next frame from the harness, or `None` once its stdout closes.
    fn read(&mut self) -> Result<Option<serde_json::Value>, ProbeError> {
        loop {
            let mut line = String::new();
            if self.stdout.read_line(&mut line)? == 0 {
                return Ok(None);
            }
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let frame: serde_json::Value = serde_json::from_str(line)?;
            self.record("in", &frame)?;
            return Ok(Some(frame));
        }
    }

    /// Reads until the response to `id`, recording notifications on the way.
    fn await_response(&mut self, id: u64) -> Result<serde_json::Value, ProbeError> {
        while let Some(frame) = self.read()? {
            if frame.get("id").and_then(serde_json::Value::as_u64) == Some(id)
                && (frame.get("result").is_some() || frame.get("error").is_some())
            {
                return Ok(frame);
            }
        }
        Err(ProbeError::StreamEnded("a response"))
    }
}

impl Drop for AppServer {
    fn drop(&mut self) {
        // Best effort: the probe is short-lived, but leaving orphaned harness
        // processes behind is exactly the mess a supervisor must not make.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn run() -> Result<(), ProbeError> {
    let mut args = std::env::args().skip(1);
    let transcript = args.next().ok_or_else(|| {
        ProbeError::Usage("usage: codex-probe <transcript.jsonl> [prompt]".to_owned())
    })?;
    let prompt = args.next().unwrap_or_else(|| {
        "Reply with exactly the token HORDE_RUST_OK and nothing else.".to_owned()
    });

    let cwd = std::env::current_dir()?;
    let mut srv = AppServer::spawn(&transcript)?;

    let id = srv.request(
        "initialize",
        serde_json::json!({
            "clientInfo": { "name": "horde-codex-probe", "version": "0.1.0" },
            "capabilities": { "experimentalApi": true },
        }),
    )?;
    let hello = srv.await_response(id)?;
    println!("initialize -> {}", compact(&hello, 200));
    srv.notify("initialized")?;

    let id = srv.request(
        "thread/start",
        serde_json::json!({
            "cwd": cwd.to_string_lossy(),
            "approvalPolicy": "untrusted",
            "sandbox": "read-only",
        }),
    )?;
    let started = srv.await_response(id)?;
    let thread_id = started
        .pointer("/result/thread/id")
        .and_then(serde_json::Value::as_str)
        .ok_or(ProbeError::StreamEnded("a thread id"))?
        .to_owned();
    println!("thread/start -> {thread_id}");

    srv.request(
        "turn/start",
        serde_json::json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": prompt }],
        }),
    )?;

    let mut answer = String::new();
    let mut usage = serde_json::Value::Null;
    while let Some(frame) = srv.read()? {
        match frame.get("method").and_then(serde_json::Value::as_str) {
            Some("item/agentMessage/delta") => {
                if let Some(d) = frame
                    .pointer("/params/delta")
                    .and_then(serde_json::Value::as_str)
                {
                    answer.push_str(d);
                }
            }
            Some("thread/tokenUsage/updated") => {
                if let Some(u) = frame.pointer("/params/tokenUsage/last") {
                    usage = u.clone();
                }
            }
            Some("turn/completed") => break,
            _ => {}
        }
    }

    println!("streamed answer: {answer:?}");
    println!("turn usage: {}", compact(&usage, 300));
    println!("transcript: {transcript}");
    Ok(())
}

fn compact(value: &serde_json::Value, limit: usize) -> String {
    let s = value.to_string();
    match s.char_indices().nth(limit) {
        Some((cut, _)) => format!("{}…", &s[..cut]),
        None => s,
    }
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("codex-probe failed: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
