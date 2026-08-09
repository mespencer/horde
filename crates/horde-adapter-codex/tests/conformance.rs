//! The codec replayed against the recordings it was built from.
//!
//! These are the bytes `codex app-server` actually wrote during spike W2c.
//! Everything above the transport — decoding, turn assembly, usage
//! normalization — is the same code a live session runs, so a verdict here is a
//! verdict about the real thing.
//!
//! Each of the three traps gets a test that would fail against the naive
//! implementation, which is the point: they are regression tests for bugs that
//! were never written, because the spike found them first.

use horde_adapter::usage::Utilization;
use horde_adapter::{AgentEvent, TurnStatus};
use horde_adapter_codex::TurnAssembler;
use horde_adapter_codex::codec::decode_line;

/// Where the W2c recordings live.
///
/// `horde-testkit::transcripts` resolves to `docs/spikes/transcripts`, which is
/// spike W1c's flat claude directory; the codex recordings sit in their own
/// subdirectory. Loading them here rather than widening the testkit keeps this
/// crate from touching a shared file — see the report's finding on that.
fn transcript(name: &str) -> Vec<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("docs/spikes/codex-app-server/transcripts")
        .join(name);

    // assert! rather than panic!: this is a helper, not a #[test] fn, and the
    // workspace bans panic! outside test functions.
    let raw = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        !raw.is_empty(),
        "transcript {} must be readable and non-empty",
        path.display()
    );

    // The recordings wrap each frame as {"dir","t","msg"}; only inbound frames
    // are harness output, and only those are what a live session would decode.
    raw.lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|entry| entry.get("dir").and_then(serde_json::Value::as_str) == Some("in"))
        .filter_map(|entry| entry.get("msg").map(ToString::to_string))
        .collect()
}

/// Feeds a whole recording through a fresh assembler.
fn replay(name: &str) -> (Vec<AgentEvent>, TurnAssembler) {
    let mut assembler = TurnAssembler::new();
    let mut events = Vec::new();
    for line in transcript(name) {
        if let Ok(frame) = decode_line(&line) {
            events.extend(assembler.feed(&frame));
        }
    }
    (events, assembler)
}

fn turn_endings(events: &[AgentEvent]) -> Vec<&horde_adapter::TurnOutcome> {
    events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::TurnEnded(outcome) => Some(outcome),
            _ => None,
        })
        .collect()
}

#[test]
fn a_basic_turn_yields_output_usage_and_headroom() {
    let (events, assembler) = replay("03-basic-turn.jsonl");

    assert_eq!(
        assembler.provider_session_id().map(|id| id.to_string()),
        Some("019fe869-e1f5-7c00-ad44-992a1921eb48".to_owned())
    );

    let streamed: String = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::Output { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(streamed, "HORDE_OK", "the deltas must reassemble in order");

    let endings = turn_endings(&events);
    assert_eq!(endings.len(), 1, "exactly one turn in this recording");
    let outcome = endings[0];
    assert_eq!(outcome.status, TurnStatus::Completed);
    assert_eq!(outcome.text.as_deref(), Some("HORDE_OK"));
    assert_eq!(outcome.usage.tokens.input_tokens, 17_098);
    assert_eq!(outcome.usage.tokens.output_tokens, 7);
    assert_eq!(outcome.usage.tokens.cache_read_input_tokens, 11_008);

    let headroom = events.iter().find_map(|event| match event {
        AgentEvent::HeadroomReported { window } => Some(window),
        _ => None,
    });
    let window = headroom.expect("Codex reports rate limits in-band");
    assert!(
        matches!(window.utilization, Utilization::Fraction(_)),
        "Codex publishes usedPercent, never counts"
    );
}

/// Trap 1 — transcript 11. `turn/interrupt` emits no terminal frame.
#[test]
fn an_interrupted_turn_closes_on_idle_and_tolerates_the_delta_tail() {
    let lines = transcript("11-interrupt.jsonl");

    // The recording contains no turn terminal frame at all. If it ever grows
    // one, this test is the thing that notices the protocol changed.
    assert!(
        !lines.iter().any(|line| line.contains("\"turn/completed\"")),
        "the recording must contain no turn/completed — that is the trap"
    );

    let mut assembler = TurnAssembler::new();
    let mut events = Vec::new();
    let mut interrupt_noted = false;
    let mut deltas_after_interrupt = 0_usize;

    for line in &lines {
        let Ok(frame) = decode_line(line) else {
            continue;
        };
        let produced = assembler.feed(&frame);
        if interrupt_noted {
            deltas_after_interrupt += produced
                .iter()
                .filter(|event| matches!(event, AgentEvent::Output { .. }))
                .count();
        }
        events.extend(produced);

        // Stand in for the Worker having called `interrupt()`: after five
        // deltas, the turn is cancelled.
        if !interrupt_noted
            && events
                .iter()
                .filter(|event| matches!(event, AgentEvent::Output { .. }))
                .count()
                >= 5
        {
            assembler.note_interrupt_requested();
            interrupt_noted = true;
        }
    }

    assert!(
        interrupt_noted,
        "the recording must stream enough to interrupt"
    );
    assert!(
        deltas_after_interrupt > 0,
        "the tail is real — transcript 11 recorded 394 deltas after the interrupt"
    );

    let endings = turn_endings(&events);
    assert_eq!(
        endings.len(),
        1,
        "the turn must close exactly once, on thread/status idle"
    );
    assert_eq!(endings[0].status, TurnStatus::Interrupted);
    assert_eq!(
        endings[0].text, None,
        "a cancelled turn has no answer, even though text was streaming"
    );
}

/// Trap 2 — transcript 15. A steered turn carries two final answers.
#[test]
fn a_steered_turn_reports_the_answer_to_the_steer_not_the_first_one() {
    let lines = transcript("15-steer.jsonl");

    let final_answers = lines
        .iter()
        .filter(|line| line.contains("\"final_answer\""))
        .count();
    assert!(
        final_answers >= 2,
        "the recording must contain the two final answers that make this a trap"
    );

    let (events, _) = replay("15-steer.jsonl");
    let endings = turn_endings(&events);

    assert_eq!(
        endings.len(),
        1,
        "steering amends the active turn — it does not open a second one"
    );
    assert_eq!(
        endings[0].text.as_deref(),
        Some("STEERED"),
        "the turn's answer is the answer to the steer, not the work it replaced"
    );
}

/// Trap 3 — transcript 16. Resume replays the previous turn's usage.
#[test]
fn resume_does_not_bill_the_previous_turn_again() {
    let lines = transcript("16-thread-resume.jsonl");

    let usage_frames = lines
        .iter()
        .filter(|line| line.contains("thread/tokenUsage/updated"))
        .count();
    assert!(
        usage_frames >= 2,
        "the recording must replay a prior reading for this to be a trap"
    );

    let (events, _) = replay("16-thread-resume.jsonl");
    let endings = turn_endings(&events);
    assert_eq!(endings.len(), 1, "one new turn ran after the resume");

    // The replayed reading was 17 098 input tokens (the pre-resume turn); the
    // new turn's own is 19 522. Summing both would report 36 620.
    let billed = endings[0].usage.tokens.input_tokens;
    assert_eq!(
        billed, 19_522,
        "only the new turn's usage may be billed; the replayed reading is a duplicate"
    );
}

/// Every recording must survive the codec without a protocol error.
///
/// Broader than the targeted tests on purpose: it is the check that catches a
/// frame shape nobody thought to assert on.
#[test]
fn every_codex_transcript_decodes() {
    for name in [
        "01-initialize.jsonl",
        "02-free-probes.jsonl",
        "03-basic-turn.jsonl",
        "04-approval-echo-untrusted.jsonl",
        "05-approval-write-untrusted.jsonl",
        "06-approval-write-granular.jsonl",
        "07-approval-network-onrequest.jsonl",
        "08-approval-write-onrequest-workspacewrite.jsonl",
        "09-approval-outside-workspace.jsonl",
        "10-approval-with-experimental-flags.jsonl",
        "11-interrupt.jsonl",
        "14-rust-probe.jsonl",
        "15-steer.jsonl",
        "16-thread-resume.jsonl",
    ] {
        let mut assembler = TurnAssembler::new();
        for line in transcript(name) {
            let frame = decode_line(&line)
                .unwrap_or_else(|e| panic!("{name}: every recorded frame must decode: {e}"));
            let _ = assembler.feed(&frame);
        }
    }
}

/// The approval recordings prove the negative ADR-0005 rests on.
#[test]
fn no_recording_ever_produced_a_permission_request() {
    for name in [
        "04-approval-echo-untrusted.jsonl",
        "06-approval-write-granular.jsonl",
        "10-approval-with-experimental-flags.jsonl",
    ] {
        let (events, _) = replay(name);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentEvent::PermissionRequested { .. })),
            "{name}: Codex 0.145.0 routes no approvals to the client (ADR-0005)"
        );
    }
}
