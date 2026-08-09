//! Replays the golden transcripts through the codec.
//!
//! These are the bytes `claude` 2.1.226 actually produced during spike W1c. If
//! a future CLI changes the protocol, or a refactor changes how we read it,
//! this is where it surfaces — which is the entire reason the transcripts were
//! captured and committed.

use horde_adapter::{AgentEvent, AgentSession, OutputChannel, TurnInput, TurnStatus};
use horde_adapter_claude::codec::decode_line;
use horde_adapter_claude::replay::ReplaySession;
use horde_adapter_claude::wire::Event;
use horde_testkit::conformance::{TurnRecord, check_session};
use horde_testkit::transcripts::{Transcript, all_transcript_names};

/// Loads a transcript, reporting the file name alongside any failure.
///
/// Returns rather than panics: these helpers are not `#[test]` functions, so
/// they are held to the production lint policy that bans `panic!`. Call sites
/// inside tests turn the error into a failure.
fn load(name: &str) -> Result<Transcript, String> {
    Transcript::load(name).map_err(|error| format!("transcript {name}: {error}"))
}

/// Replays a whole transcript and returns one record per recorded turn.
async fn replay(name: &str) -> Result<Vec<TurnRecord>, String> {
    let turns = load(name)?.turns();
    let messages: Vec<&str> = turns.iter().map(|_| "replayed").collect();
    let mut session = ReplaySession::new(turns);

    check_session(&mut session, &messages)
        .await
        .map_err(|error| format!("transcript {name} violated the adapter contract: {error}"))
}

#[test]
fn every_recorded_line_decodes() {
    let names = all_transcript_names().expect("the transcript directory must be listable");
    assert!(
        names.len() >= 15,
        "expected the full W1c capture set, found {names:?}"
    );

    for name in &names {
        let transcript = load(name).expect("fixture must load");
        for (index, line) in transcript.outbound.iter().enumerate() {
            if let Err(error) = decode_line(line) {
                panic!("{name} line {}: {error}", index + 1);
            }
        }
    }
}

/// A `type` this crate does not model is tolerated by design, but it is also a
/// signal that the protocol moved. Naming the tolerated set here means a new
/// one shows up as a failure to look at rather than as silence.
#[test]
fn no_unmodelled_event_types_remain_in_the_captures() {
    let names = all_transcript_names().expect("listable");
    let mut unknown = Vec::new();

    for name in &names {
        for line in &load(name).expect("fixture must load").outbound {
            if let Ok(envelope) = decode_line(line)
                && let Event::Unknown { event_type } = &envelope.event
                && !unknown.contains(event_type)
            {
                unknown.push(event_type.clone());
            }
        }
    }

    assert!(
        unknown.is_empty(),
        "unmodelled event types appeared in the captures: {unknown:?}"
    );
}

#[tokio::test]
async fn every_transcript_satisfies_the_adapter_contract() {
    for name in all_transcript_names().expect("listable") {
        let records = replay(&name).await.expect("must conform");
        assert!(
            !records.is_empty(),
            "{name} produced no turns; a transcript with no result envelope is not a session"
        );
    }
}

#[tokio::test]
async fn multi_turn_keeps_context_and_one_session_across_turns() {
    let records = replay("02-multi-turn.jsonl")
        .await
        .expect("transcript must conform");
    assert_eq!(records.len(), 3);

    let answers: Vec<String> = records
        .iter()
        .map(|record| record.outcome.text.clone().unwrap_or_default())
        .collect();
    assert_eq!(
        answers,
        vec!["OK".to_owned(), "4271".to_owned(), "4272".to_owned()],
        "the recorded session remembered a number across turns of one process"
    );

    for record in &records {
        assert!(record.outcome.status.is_completed());
    }
}

/// The W1c usage finding, enforced: the harness reports running totals, the
/// adapter must hand back per-turn deltas.
#[tokio::test]
async fn usage_is_reported_as_per_turn_deltas() {
    let records = replay("02-multi-turn.jsonl")
        .await
        .expect("transcript must conform");

    let outputs: Vec<u64> = records
        .iter()
        .map(|record| record.outcome.usage.tokens.output_tokens)
        .collect();
    assert_eq!(
        outputs,
        vec![451, 654 - 451, 713 - 654],
        "each turn must report only its own tokens"
    );

    let summed: f64 = records
        .iter()
        .map(|record| record.outcome.usage.cost_usd)
        .sum();
    assert!(
        (summed - 0.0077755).abs() < 1e-9,
        "per-turn costs must sum back to the harness's final cumulative total, got {summed}"
    );
}

#[tokio::test]
async fn a_denied_tool_call_is_surfaced_and_the_turn_is_not_a_clean_success() {
    let records = replay("04a-perm-deny.jsonl")
        .await
        .expect("transcript must conform");
    let outcome = &records[0].outcome;

    assert_eq!(outcome.denials.len(), 1, "the Write was refused");
    assert_eq!(outcome.denials[0].tool_name, "Write");
    assert_eq!(
        outcome.denials[0].attempted_input["content"], "BLOCKED",
        "the audit record keeps what the agent tried to do"
    );
    assert!(outcome.status.is_completed());
    assert!(
        !outcome.is_clean_success(),
        "a turn that was refused work must not read as clean"
    );
}

#[tokio::test]
async fn an_allowed_tool_call_leaves_no_denials() {
    let records = replay("04b-perm-allow.jsonl")
        .await
        .expect("transcript must conform");
    assert!(records[0].outcome.denials.is_empty());
    assert!(records[0].outcome.is_clean_success());
}

#[tokio::test]
async fn an_interrupted_turn_is_classified_as_interrupted_and_the_session_survives() {
    let records = replay("07a-interrupt-control.jsonl")
        .await
        .expect("transcript must conform");

    let interrupted = records
        .iter()
        .position(|record| record.outcome.status == TurnStatus::Interrupted)
        .expect("the capture contains an aborted turn");

    let recovery = records
        .get(interrupted + 1)
        .expect("the same process answered another message after the interrupt");
    assert!(recovery.outcome.status.is_completed());
    assert_eq!(recovery.outcome.text.as_deref(), Some("STILL_ALIVE"));
}

/// Closing stdin does not interrupt: the recorded turn ran to completion. If
/// this ever reads as `Interrupted`, the classifier has started guessing.
#[tokio::test]
async fn closing_stdin_did_not_abort_the_recorded_turn() {
    let records = replay("07c-interrupt-stdinclose.jsonl")
        .await
        .expect("transcript must conform");
    assert!(
        records
            .iter()
            .all(|record| record.outcome.status.is_completed()),
        "stdin close lets the turn finish; only a control message aborts one"
    );
}

#[tokio::test]
async fn an_unauthenticated_session_is_a_failure_despite_claiming_success() {
    let records = replay("09-config-isolation.jsonl")
        .await
        .expect("transcript must conform");

    match &records[0].outcome.status {
        TurnStatus::Failed { message } => assert!(
            message.contains("Not logged in"),
            "expected the harness's own wording, got {message:?}"
        ),
        other => {
            panic!("an isolated config dir has no credentials; expected Failed, got {other:?}")
        }
    }
}

#[tokio::test]
async fn a_tool_using_turn_reports_the_call_and_its_completion() {
    let records = replay("03-tool-use.jsonl")
        .await
        .expect("transcript must conform");
    let events = &records[0].events;

    let call = events.iter().find_map(|event| match event {
        AgentEvent::Output {
            channel: OutputChannel::ToolCall,
            text,
            ..
        } => Some(text.clone()),
        _ => None,
    });
    assert!(
        call.is_some_and(|text| text.starts_with("Write ")),
        "the recorded turn wrote a file"
    );

    assert!(
        events.iter().any(|event| matches!(
            event,
            AgentEvent::ToolCompleted {
                is_error: false,
                ..
            }
        )),
        "the write succeeded and must be reported as completed"
    );
}

#[tokio::test]
async fn the_bound_model_is_reported_from_the_harness_not_from_the_request() {
    let records = replay("08b-model-sonnet.jsonl")
        .await
        .expect("transcript must conform");

    let model = records[0].events.iter().find_map(|event| match event {
        AgentEvent::SessionEstablished { model, .. } => Some(model.clone()),
        _ => None,
    });
    assert_eq!(model.as_deref(), Some("claude-sonnet-5"));
}

#[tokio::test]
async fn headroom_is_reported_when_the_harness_volunteers_it() {
    let records = replay("02-multi-turn.jsonl")
        .await
        .expect("transcript must conform");

    let reported = records.iter().flat_map(|record| &record.events).any(|event| {
        matches!(event, AgentEvent::HeadroomReported { window } if window.window == "five_hour")
    });
    assert!(
        reported,
        "the capture contains a rate_limit_event; headroom drives tier selection"
    );
}

/// Draining past the recorded turns must not invent events.
#[tokio::test]
async fn a_replayed_session_ends_cleanly() {
    let transcript = load("01-basic-turn.out.jsonl").expect("fixture must load");
    let mut session = ReplaySession::new(transcript.turns());

    session
        .send(TurnInput::queued("replayed"))
        .await
        .expect("the recorded turn replays");
    while session
        .next_event()
        .await
        .expect("draining must not fail")
        .is_some()
    {}

    assert_eq!(session.remaining_turns(), 0);
    assert!(
        session.next_event().await.expect("must not fail").is_none(),
        "a drained session yields nothing further"
    );
}
