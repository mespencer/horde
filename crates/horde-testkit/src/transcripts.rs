//! Loading the golden transcripts captured by spike W1c.
//!
//! The transcripts under `docs/spikes/transcripts/` are recordings of real
//! harness sessions. Replaying them is how an adapter is held to what the
//! harness actually did, rather than to what its author believed the harness
//! did. See that directory's README for what each file covers.

use std::path::{Path, PathBuf};

use serde_json::Value;

/// Where a transcript could not be read or understood.
#[derive(Debug, thiserror::Error)]
pub enum TranscriptError {
    #[error("transcript {path} could not be read: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("transcript {path} line {line_number} is not valid JSON: {source}")]
    Malformed {
        path: PathBuf,
        line_number: usize,
        #[source]
        source: serde_json::Error,
    },
}

/// The repository root, resolved from this crate's location at compile time.
///
/// Transcripts are committed fixtures, so locating them must not depend on the
/// working directory a test happens to run in.
#[must_use]
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

/// The directory holding the golden transcripts.
#[must_use]
pub fn transcript_dir() -> PathBuf {
    repo_root().join("docs/spikes/transcripts")
}

/// A recorded harness session.
#[derive(Debug, Clone)]
pub struct Transcript {
    pub name: String,
    /// Lines the harness wrote to stdout, in order, as raw JSON text.
    pub outbound: Vec<String>,
    /// Lines the driver wrote to the harness's stdin, in order.
    pub inbound: Vec<String>,
}

impl Transcript {
    /// Loads a transcript by file name, e.g. `"02-multi-turn.jsonl"`.
    ///
    /// Handles both capture formats: the driver-wrapped
    /// `{"dir":…,"t":…,"msg":…}` envelopes, and the one file that is unwrapped
    /// raw stdout because it was captured with a shell pipe.
    pub fn load(name: &str) -> Result<Self, TranscriptError> {
        let path = transcript_dir().join(name);
        let contents = std::fs::read_to_string(&path).map_err(|source| TranscriptError::Read {
            path: path.clone(),
            source,
        })?;

        let mut outbound = Vec::new();
        let mut inbound = Vec::new();

        for (index, line) in contents.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let value: Value =
                serde_json::from_str(line).map_err(|source| TranscriptError::Malformed {
                    path: path.clone(),
                    line_number: index + 1,
                    source,
                })?;

            match (value.get("dir").and_then(Value::as_str), value.get("msg")) {
                (Some("out"), Some(msg)) => outbound.push(msg.to_string()),
                (Some("in"), Some(msg)) => inbound.push(msg.to_string()),
                // Unwrapped capture: the line is the harness event itself.
                _ => outbound.push(line.to_owned()),
            }
        }

        Ok(Self {
            name: name.to_owned(),
            outbound,
            inbound,
        })
    }

    /// Splits the outbound lines into one group per turn.
    ///
    /// A turn runs up to and including its `result` envelope, which is the only
    /// thing that closes a turn in this protocol.
    #[must_use]
    pub fn turns(&self) -> Vec<Vec<String>> {
        let mut turns = Vec::new();
        let mut current = Vec::new();

        for line in &self.outbound {
            let is_result = serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|value| {
                    value
                        .get("type")
                        .and_then(Value::as_str)
                        .map(|kind| kind == "result")
                })
                .unwrap_or(false);

            current.push(line.clone());
            if is_result {
                turns.push(std::mem::take(&mut current));
            }
        }

        if !current.is_empty() {
            turns.push(current);
        }
        turns
    }
}

/// Every transcript file in the golden directory, sorted by name.
///
/// Used by the conformance suite so a newly captured transcript is exercised
/// automatically instead of waiting for someone to remember to list it.
pub fn all_transcript_names() -> Result<Vec<String>, TranscriptError> {
    let dir = transcript_dir();
    let entries = std::fs::read_dir(&dir).map_err(|source| TranscriptError::Read {
        path: dir.clone(),
        source,
    })?;

    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| TranscriptError::Read {
            path: dir.clone(),
            source,
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        // `perm-*.jsonl` are permission-server logs, not harness output.
        if name.ends_with(".jsonl") && !name.starts_with("perm-") {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::{Transcript, all_transcript_names, transcript_dir};

    #[test]
    fn the_golden_transcripts_are_where_we_think_they_are() {
        let dir = transcript_dir();
        assert!(
            dir.join("README.md").is_file(),
            "expected the transcript README at {}",
            dir.display()
        );
    }

    #[test]
    fn a_wrapped_transcript_separates_the_two_directions() {
        let transcript = Transcript::load("02-multi-turn.jsonl").expect("must load");
        assert_eq!(
            transcript.inbound.len(),
            3,
            "the three-turn capture sent three user messages"
        );
        assert!(transcript.outbound.len() > transcript.inbound.len());
    }

    #[test]
    fn an_unwrapped_transcript_is_read_as_harness_output() {
        let transcript = Transcript::load("01-basic-turn.out.jsonl").expect("must load");
        assert!(transcript.inbound.is_empty());
        assert!(!transcript.outbound.is_empty());
    }

    #[test]
    fn turns_are_split_on_the_result_envelope() {
        let transcript = Transcript::load("02-multi-turn.jsonl").expect("must load");
        assert_eq!(transcript.turns().len(), 3);
    }

    #[test]
    fn every_harness_transcript_is_discoverable_and_permission_logs_are_excluded() {
        let names = all_transcript_names().expect("the directory must be listable");
        assert!(names.contains(&"02-multi-turn.jsonl".to_owned()));
        assert!(
            !names.iter().any(|name| name.starts_with("perm-")),
            "permission-server logs are not harness output: {names:?}"
        );
    }
}
