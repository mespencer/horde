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

/// The directory holding spike W1c's golden transcripts.
///
/// Kept pointing at the flat claude directory it has always meant. A second
/// spike records into its own subdirectory, which is what
/// [`Transcript::load_from`] is for.
#[must_use]
pub fn transcript_dir() -> PathBuf {
    repo_root().join("docs/spikes/transcripts")
}

/// Which `dir` value in a wrapped capture marks *harness output*.
///
/// The two spikes chose opposite conventions for the same key, and the mistake
/// is invisible — a loader that guesses wrong silently swaps what the harness
/// said with what the driver sent, and every assertion still runs.
///
/// * [`Self::Out`] — W1c's claude captures, where `dir` describes the harness's
///   stdout: `"out"` is what came *out of* the harness.
/// * [`Self::In`] — W2c's codex captures, where `dir` describes the driver's
///   socket: `"in"` is what came *in to* the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessOutputKey {
    Out,
    In,
}

impl HarnessOutputKey {
    fn marks_output(self, dir: &str) -> bool {
        match self {
            Self::Out => dir == "out",
            Self::In => dir == "in",
        }
    }
}

/// A recorded harness session.
///
/// ```
/// use horde_testkit::transcripts::Transcript;
///
/// let transcript = Transcript::load("02-multi-turn.jsonl")?;
///
/// // Three user messages went in, and the capture splits into three turns —
/// // a turn runs up to and including its `result` envelope.
/// assert_eq!(transcript.inbound.len(), 3);
/// assert_eq!(transcript.turns().len(), 3);
/// # Ok::<(), horde_testkit::TranscriptError>(())
/// ```
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
        Self::load_from(&transcript_dir(), name, HarnessOutputKey::Out)
    }

    /// Loads a transcript from an explicit directory, naming which `dir` value
    /// marks harness output.
    ///
    /// Additive counterpart to [`Self::load`], for spikes that record into
    /// their own subdirectory or with the opposite direction convention. See
    /// [`HarnessOutputKey`] — getting it wrong is silent, so it is a required
    /// argument rather than a guess.
    pub fn load_from(
        dir: &Path,
        name: &str,
        output_key: HarnessOutputKey,
    ) -> Result<Self, TranscriptError> {
        let path = dir.join(name);
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
                (Some(dir), Some(msg)) if output_key.marks_output(dir) => {
                    outbound.push(msg.to_string());
                }
                (Some(_), Some(msg)) => inbound.push(msg.to_string()),
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
        self.turns_where(|line| {
            serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|value| {
                    value
                        .get("type")
                        .and_then(Value::as_str)
                        .map(|kind| kind == "result")
                })
                .unwrap_or(false)
        })
    }

    /// Splits the outbound lines into turns using a caller-supplied terminator.
    ///
    /// Additive counterpart to [`Self::turns`], whose `result` envelope is
    /// specific to claude's protocol. Codex, for one, has no single terminator
    /// at all: an interrupted turn ends on a status change and never emits a
    /// terminal frame, so its predicate has to accept both.
    #[must_use]
    pub fn turns_where(&self, is_turn_end: impl Fn(&str) -> bool) -> Vec<Vec<String>> {
        let mut turns = Vec::new();
        let mut current = Vec::new();

        for line in &self.outbound {
            let ends_turn = is_turn_end(line);
            current.push(line.clone());
            if ends_turn {
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
    use super::{
        HarnessOutputKey, Transcript, TranscriptError, all_transcript_names, transcript_dir,
    };

    /// The two spikes chose opposite conventions for `dir`, and guessing wrong
    /// silently swaps what the harness said with what the driver sent — every
    /// assertion still runs, against the wrong half of the conversation.
    #[test]
    fn each_convention_marks_only_its_own_direction_as_harness_output() {
        assert!(HarnessOutputKey::Out.marks_output("out"));
        assert!(!HarnessOutputKey::Out.marks_output("in"));

        assert!(HarnessOutputKey::In.marks_output("in"));
        assert!(!HarnessOutputKey::In.marks_output("out"));
    }

    #[test]
    fn an_unrelated_direction_is_not_harness_output_under_either_convention() {
        for key in [HarnessOutputKey::Out, HarnessOutputKey::In] {
            assert!(!key.marks_output("sideways"));
            assert!(!key.marks_output(""));
        }
    }

    /// A malformed line must be reported at its own 1-based position: an
    /// off-by-one sends whoever is debugging a transcript to the wrong line.
    #[test]
    fn a_malformed_line_is_reported_at_its_one_based_position() {
        let directory = tempfile::tempdir().expect("a temp dir");
        std::fs::write(
            directory.path().join("broken.jsonl"),
            "{\"a\":1}\n{\"b\":2}\nnot json at all\n",
        )
        .expect("write the fixture");

        let error = Transcript::load_from(directory.path(), "broken.jsonl", HarnessOutputKey::Out)
            .expect_err("a malformed line must be rejected");

        match error {
            TranscriptError::Malformed { line_number, .. } => assert_eq!(
                line_number, 3,
                "the third line is malformed; 1-based counting is what a reader expects"
            ),
            other => panic!("expected Malformed, got {other}"),
        }
    }

    #[test]
    fn a_malformed_first_line_is_reported_as_line_one() {
        let directory = tempfile::tempdir().expect("a temp dir");
        std::fs::write(directory.path().join("broken.jsonl"), "nope\n").expect("write the fixture");

        match Transcript::load_from(directory.path(), "broken.jsonl", HarnessOutputKey::Out) {
            Err(TranscriptError::Malformed { line_number, .. }) => assert_eq!(line_number, 1),
            other => panic!("expected Malformed at line 1, got {other:?}"),
        }
    }

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
