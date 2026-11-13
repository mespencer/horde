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

use horde_adapter::AgentSession;
use horde_adapter::usage::Utilization;
use horde_adapter::{AgentEvent, TurnStatus};
use horde_adapter_codex::TurnAssembler;
use horde_adapter_codex::codec::decode_line;
use horde_adapter_codex::replay::replay as replay_session;
use horde_adapter_codex::wire::{Frame, Notification};
use horde_testkit::{HarnessOutputKey, Transcript};

/// The W2c recordings, loaded through the shared testkit.
///
/// `HarnessOutputKey::In` is required and not a default: the codex driver
/// recorded `dir` from its own socket's point of view, the opposite of the
/// claude captures. Guessing wrong swaps harness output with driver input and
/// every assertion still runs, which is why the testkit makes it an argument.
fn transcript(name: &str) -> Vec<String> {
    let dir =
        horde_testkit::transcripts::repo_root().join("docs/spikes/codex-app-server/transcripts");
    let transcript = Transcript::load_from(&dir, name, HarnessOutputKey::In);
    // assert! rather than panic!: this is a helper, not a #[test] fn, and the
    // workspace bans panic! outside test functions.
    assert!(
        transcript.is_ok(),
        "transcript {name} must load from {}",
        dir.display()
    );
    transcript.map(|t| t.outbound).unwrap_or_default()
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

/// Codex sends one usage reading per *model call*, not per turn: `last` is
/// one call and `total` is the thread's running sum. Every recording whose
/// turn called a tool has two or more readings in that turn — transcript 33
/// has six, whose `last` figures (18 411 … 19 395) sum to its final `total`
/// of 113 895 — and the turn must bill all of them, not the first.
///
/// Each figure is the recording's final `total.totalTokens` (codex's own
/// `inputTokens + outputTokens`); every one of these is a single-turn thread,
/// so that total is the turn.
#[test]
fn a_multi_reading_turn_bills_every_model_call_it_made() {
    for (name, readings, recorded_total) in [
        ("04-approval-echo-untrusted.jsonl", 2, 34_525),
        (
            "08-approval-write-onrequest-workspacewrite.jsonl",
            2,
            39_610,
        ),
        ("09-approval-outside-workspace.jsonl", 2, 39_554),
        ("15-steer.jsonl", 2, 34_733),
        ("17-mcp-elicitation-accept.jsonl", 3, 56_986),
        ("18-mcp-elicitation-decline.jsonl", 3, 57_055),
        ("20-exec-user-onrequest-ro-vscode-write.jsonl", 2, 37_863),
        ("21-exec-user-onrequest-ro-horde-write.jsonl", 2, 38_036),
        ("22-exec-unset-onrequest-ro-horde-write.jsonl", 2, 38_048),
        (
            "23-exec-autoreview-onrequest-ro-horde-write.jsonl",
            3,
            57_456,
        ),
        ("24-exec-user-untrusted-ro-horde-echo.jsonl", 2, 36_542),
        ("25-exec-user-granular-ro-horde-write.jsonl", 4, 86_982),
        ("26-exec-user-onrequest-ww-horde-escape.jsonl", 3, 58_023),
        ("27-exec-user-onrequest-ww-horde-net.jsonl", 3, 58_152),
        (
            "29-exec-user-onrequest-ro-horde-write-accept.jsonl",
            2,
            38_002,
        ),
        (
            "30-exec-user-onrequest-ro-horde-write-amend.jsonl",
            3,
            57_352,
        ),
        (
            "33-0147-exec-user-granular-ro-horde-write.jsonl",
            6,
            113_895,
        ),
    ] {
        let recorded_readings = transcript(name)
            .iter()
            .filter(|line| {
                matches!(
                    decode_line(line),
                    Ok(Frame::Notification(Notification::TokenUsageUpdated(_)))
                )
            })
            .count();
        assert_eq!(
            recorded_readings, readings,
            "{name}: the recording's reading count is the premise"
        );

        let (mut events, mut assembler) = replay(name);
        // The approval probe stopped reading at `idle` in most of these, one
        // frame short of the `turn/completed` codex writes next (20, 22 and 26
        // kept it). Appending that frame closes the turn the way the live
        // stream would; it carries no usage of its own.
        if let Some(turn_id) = assembler.active_turn_id().map(ToString::to_string) {
            let completed = serde_json::json!({
                "method": "turn/completed",
                "params": {"turn": {"id": turn_id, "status": "completed"}}
            });
            let frame = decode_line(&completed.to_string()).expect("a well-formed terminal frame");
            events.extend(assembler.feed(&frame));
        }
        let billed: u64 = turn_endings(&events)
            .iter()
            .map(|outcome| outcome.usage.tokens.input_tokens + outcome.usage.tokens.output_tokens)
            .sum();
        assert_eq!(
            billed, recorded_total,
            "{name}: every one of the {readings} model calls must be billed"
        );
    }
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
        "17-mcp-elicitation-accept.jsonl",
        "18-mcp-elicitation-decline.jsonl",
        "19-mcp-elicitation-onrequest.jsonl",
    ] {
        let mut assembler = TurnAssembler::new();
        for line in transcript(name) {
            let frame = decode_line(&line)
                .unwrap_or_else(|e| panic!("{name}: every recorded frame must decode: {e}"));
            let _ = assembler.feed(&frame);
        }
    }
}

/// An exec recording that carried no approval request still produces none —
/// the assembler surfaces what the wire routed, never what it wishes had
/// been. Transcript 04 is the only exec recording that is genuinely
/// request-free (a trivially safe `echo` under `untrusted`, auto-approved);
/// even 06 and 10, whose W2c summaries said "no request", turn out to carry
/// late-routed frames in their preserved bytes.
#[test]
fn an_exec_recording_without_an_approval_frame_produces_no_request() {
    let (events, _) = replay("04-approval-echo-untrusted.jsonl");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::PermissionRequested { .. })),
        "04: no approval frame was recorded, so none may be invented"
    );
}

/// The correction the re-probe forced on W2c's own analysis: transcript 06's
/// bytes (codex-cli 0.145.0, granular) contain one `item/commandExecution/
/// requestApproval`, routed 182 seconds after the exec item started — which
/// the spike's summary wrote off as "no request". The assembler surfaces what
/// the bytes say. The claim stays version-gated at 0.147 regardless: a
/// three-minute internal retry loop before the question is not interception
/// worth advertising, and the re-probe's bounded matrix is the measurement.
#[test]
fn the_late_routed_0145_approval_in_transcript_06_surfaces_as_the_bytes_say() {
    let (events, _) = replay("06-approval-write-granular.jsonl");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::PermissionRequested { .. })),
        "the recording carries the frame, so the replay must carry the event"
    );
}

/// The exec interception seam against the recorded bytes — the 2026-08-19
/// re-probe's finding, replayed. Transcript 25 (codex-cli 0.148.0, granular,
/// read-only) routes `item/commandExecution/requestApproval` before the
/// command completes; the surfaced proposal carries the command and cwd the
/// chain's rules match on, and the exec item is still unresolved when the
/// question arrives.
#[test]
fn a_recorded_exec_approval_surfaces_before_the_command_resolves() {
    for name in [
        "25-exec-user-granular-ro-horde-write.jsonl",
        "33-0147-exec-user-granular-ro-horde-write.jsonl",
    ] {
        let (events, assembler) = replay(name);

        let request_at = events
            .iter()
            .position(|event| matches!(event, AgentEvent::PermissionRequested { .. }))
            .unwrap_or_else(|| panic!("{name}: the recorded approval must surface"));
        let request = match &events[request_at] {
            AgentEvent::PermissionRequested { request, .. } => request,
            _ => unreachable!("indexed by position"),
        };
        assert_eq!(request.tool_name, "exec", "{name}");
        let command = request.input["command"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: the command must be rule-matchable text"));
        assert!(
            command.contains("horde-escalation-probe.txt"),
            "{name}: the chain sees what codex will run: {command}"
        );
        assert!(
            request.input["cwd"]
                .as_str()
                .is_some_and(|cwd| cwd.starts_with("/private/tmp/")),
            "{name}: the cwd rides as a rule-matchable argument"
        );

        let completed_before_request = events[..request_at].iter().any(|event| {
            matches!(event, AgentEvent::ToolCompleted { tool_call_id, .. }
                    if tool_call_id == &request.tool_call_id)
        });
        assert!(
            !completed_before_request,
            "{name}: the question precedes the verdict — that is the seam"
        );
        assert!(
            assembler.awaiting_decision().is_none(),
            "{name}: the probe answered on the wire, which a replay cannot see — \
             but codex's own `serverRequest/resolved` after each answer is \
             recorded, and it settles what the replayer would otherwise owe"
        );
    }
}

/// Transcript 17 — the ADR-0005 amendment, in recorded bytes: an MCP tool
/// call surfaces as a permission request carrying the full arguments, and
/// only after the answer does the call complete.
#[test]
fn an_mcp_tool_call_is_offered_before_it_runs_with_its_full_arguments() {
    let (events, _) = replay("17-mcp-elicitation-accept.jsonl");

    let request_at = events
        .iter()
        .position(|event| matches!(event, AgentEvent::PermissionRequested { .. }))
        .expect("the elicitation must surface as a permission request");
    let completed_at = events
        .iter()
        .position(|event| matches!(event, AgentEvent::ToolCompleted { .. }))
        .expect("the allowed call completes");
    assert!(
        request_at < completed_at,
        "the request precedes the call's completion — that ordering is the seam"
    );

    let Some(AgentEvent::PermissionRequested { request, .. }) = events.get(request_at) else {
        unreachable!("position() found it");
    };
    assert_eq!(request.tool_name, "spike/horde_probe");
    assert_eq!(
        request.input,
        serde_json::json!({"token": "HORDE_MCP_OK"}),
        "the chain sees the full arguments before the call runs"
    );

    let Some(AgentEvent::ToolCompleted { is_error, .. }) = events.get(completed_at) else {
        unreachable!("position() found it");
    };
    assert!(!is_error, "the accepted call succeeded");
}

/// Replays a recording through the whole session seam under the shared
/// conformance checks, answering every permission request with `policy`.
async fn checked_replay(
    name: &str,
    policy: horde_testkit::conformance::Policy<'_>,
) -> Result<
    Vec<horde_testkit::conformance::TurnRecord>,
    horde_testkit::conformance::ConformanceFailure,
> {
    let turns = codex_turns(name);
    let messages: Vec<&str> = turns.iter().map(|_| "replayed").collect();
    let mut session = replay_session(turns);
    horde_testkit::conformance::check_session_with(&mut session, &messages, policy).await
}

/// Transcript 18 — the decline path: the call fails without a byte reaching
/// the MCP server, and the denial rides the turn's outcome.
#[tokio::test]
async fn a_declined_mcp_call_fails_the_tool_and_records_the_denial() {
    let records = checked_replay("18-mcp-elicitation-decline.jsonl", &|_| {
        horde_adapter::PermissionDecision::deny("blocked by policy")
    })
    .await
    .expect("the declined recording must still satisfy the contract");

    let outcome = &records.last().expect("the recording has a turn").outcome;
    assert_eq!(outcome.denials.len(), 1, "the refusal is the turn's record");
    assert_eq!(outcome.denials[0].tool_name, "spike/horde_probe");
    assert!(
        !outcome.is_clean_success(),
        "a turn that hit a refusal is not a clean success"
    );

    let rejected = records
        .last()
        .into_iter()
        .flat_map(|r| &r.events)
        .any(|event| matches!(event, AgentEvent::ToolCompleted { is_error: true, .. }));
    assert!(
        rejected,
        "the declined call surfaces to the agent as a failed tool"
    );
}

/// The accept recording, driven through the whole session seam under the
/// shared conformance checks — permission answered where a live worker would
/// answer it.
#[tokio::test]
async fn the_elicitation_recording_satisfies_the_adapter_contract() {
    let records = checked_replay("17-mcp-elicitation-accept.jsonl", &|_| {
        horde_testkit::conformance::allow_all()
    })
    .await;
    assert!(
        records.is_ok(),
        "the recorded session violated the adapter contract: {:?}",
        records.err()
    );
}

// ---------------------------------------------------------------------------
// Trait-level parity: the same seam invariants the FakeAdapter is held to.
// ---------------------------------------------------------------------------

/// Splits a codex recording into turns.
///
/// Codex has no single terminator: a completed turn ends on `turn/completed`,
/// an interrupted one ends on `thread/status → idle` and emits no terminal
/// frame at all. The predicate must accept both or an interrupted recording
/// never closes.
fn codex_turns(name: &str) -> Vec<Vec<String>> {
    let lines = transcript(name);
    let mut turns = Vec::new();
    let mut current = Vec::new();

    for line in lines {
        let completed = line.contains("\"turn/completed\"");
        current.push(line);
        if completed {
            turns.push(std::mem::take(&mut current));
        }
    }

    // Whatever is left is a turn only if a turn actually opened in it — an
    // interrupted turn ends on `thread/status → idle` with no terminal frame,
    // so it lands here. Trailing status chatter after the last completed turn
    // is not a turn and must not be replayed as one, or the contract check
    // sees a message answered by no ending.
    if current.iter().any(|line| line.contains("\"turn/started\"")) {
        turns.push(current);
    } else if let Some(last) = turns.last_mut() {
        last.extend(current);
    }

    turns
}

/// The recorded session must satisfy the same contract the FakeAdapter does.
///
/// This is the parity that keeps the fake honest: one suite, two backends. A
/// codec that assembled plausible-looking events but broke an invariant — a
/// turn id that changes mid-turn, output before the session was announced, a
/// gap in chunk indices — fails here even though every targeted test above
/// still passes.
#[tokio::test]
async fn a_recorded_session_satisfies_the_adapter_contract() {
    let turns = codex_turns("03-basic-turn.jsonl");
    assert!(!turns.is_empty(), "the recording must contain a turn");

    let messages: Vec<&str> = turns.iter().map(|_| "replayed").collect();
    let mut session = replay_session(turns);

    let records = horde_testkit::conformance::check_session(&mut session, &messages).await;
    assert!(
        records.is_ok(),
        "the recorded session violated the adapter contract: {:?}",
        records.err()
    );
}

/// The capability claims must match what the adapter can actually do.
#[tokio::test]
async fn the_capability_claims_are_honest_about_gating() {
    // Codex claims no PreAuthorizeToolCalls, so the seam must never surface a
    // permission request — and answering one that cannot exist must be an
    // error rather than a silent success.
    let mut session = replay_session(codex_turns("04-approval-echo-untrusted.jsonl"));
    let refused = session
        .answer_permission(
            &horde_adapter::ToolCallId::new("exec-1"),
            horde_adapter::PermissionDecision::allow(),
        )
        .await;

    assert!(
        refused.is_err(),
        "a decision that went nowhere must not look like one that was applied"
    );
    assert!(
        !horde_adapter_codex::capabilities()
            .supports(horde_adapter::Capability::PreAuthorizeToolCalls)
    );
}

// ---------------------------------------------------------------------------
// W12c — codex-cli 0.167.0 (transcripts 34–41).
// ---------------------------------------------------------------------------

/// The request a recording surfaces under `tool_name`, if any.
fn requested_as(events: &[AgentEvent], tool_name: &str) -> Option<horde_adapter::ProposedToolCall> {
    events.iter().find_map(|event| match event {
        AgentEvent::PermissionRequested { request, .. } if request.tool_name == tool_name => {
            Some(request.clone())
        }
        _ => None,
    })
}

/// Transcript 36 — the file-change approval that 0.148 never sent (28/31)
/// now arrives before the patch applies, and the surfaced proposal carries
/// the patch itself, joined from the started `fileChange` item, so a rule or
/// a human sees what would be written.
#[test]
fn a_file_change_approval_surfaces_with_its_patch_before_it_applies() {
    let (events, _) = replay("36-0167-filechange-user-granular-ro-horde-patch.jsonl");
    let request = requested_as(&events, "apply_patch").expect("the request surfaces");
    let changes = request.input["changes"]
        .as_array()
        .expect("the patch rides the proposal");
    assert_eq!(changes.len(), 1);
    assert!(
        changes[0]["path"]
            .as_str()
            .is_some_and(|path| path.ends_with("/horde-patch-probe.txt")),
        "{changes:?}"
    );
    assert_eq!(changes[0]["kind"], serde_json::json!({"type": "add"}));
    assert_eq!(changes[0]["diff"], "PATCHED\n");
    assert!(!request.is_question());

    let request_at = events
        .iter()
        .position(|event| matches!(event, AgentEvent::PermissionRequested { .. }))
        .expect("surfaced");
    let completed_at = events
        .iter()
        .position(|event| {
            matches!(event, AgentEvent::ToolCompleted { tool_call_id, is_error: true, .. }
                if tool_call_id == &request.tool_call_id)
        })
        .expect("the declined patch completes as a failed tool");
    assert!(
        request_at < completed_at,
        "the question precedes the verdict"
    );
}

/// Transcripts 36–37 under the conformance driver: declined, the turn still
/// closes and carries the refusal; accepted, it carries none.
#[tokio::test]
async fn file_change_recordings_satisfy_the_contract_both_ways() {
    let declined = checked_replay(
        "36-0167-filechange-user-granular-ro-horde-patch.jsonl",
        &|_| horde_adapter::PermissionDecision::deny("no patches"),
    )
    .await
    .expect("the declined recording satisfies the contract");
    let outcome = &declined.last().expect("a turn").outcome;
    assert_eq!(outcome.denials.len(), 1);
    assert_eq!(outcome.denials[0].tool_name, "apply_patch");

    let accepted = checked_replay(
        "37-0167-filechange-user-granular-ro-horde-patch-accept.jsonl",
        &|_| horde_testkit::conformance::allow_all(),
    )
    .await
    .expect("the accepted recording satisfies the contract");
    assert!(
        accepted
            .iter()
            .all(|record| record.outcome.denials.is_empty())
    );
}

/// Transcript 38 — codex's question surfaces as a question only a human can
/// answer, keyed by codex's own id, and the recorded turn completes once it
/// is answered.
#[tokio::test]
async fn a_codex_question_surfaces_as_a_question_and_takes_answers() {
    let (events, _) = replay("38-0167-userinput-user-granular-ro-horde-ask.jsonl");
    let request = requested_as(&events, "request_user_input").expect("the request surfaces");
    let [question] = request.questions.as_slice() else {
        panic!("one question, got {:?}", request.questions);
    };
    assert_eq!(question.id, "colour");
    assert_eq!(question.header, "Colour");
    assert_eq!(question.question, "Which colour do you prefer?");
    assert_eq!(
        question
            .options
            .iter()
            .map(|option| option.label.as_str())
            .collect::<Vec<_>>(),
        vec!["Red", "Blue"]
    );
    assert!(question.allows_free_text, "isOther: true");
    assert!(!question.secret);

    let records = checked_replay(
        "38-0167-userinput-user-granular-ro-horde-ask.jsonl",
        &|_| {
            horde_adapter::PermissionDecision::answer(horde_adapter::Answers::from([(
                "colour".to_owned(),
                vec!["Red".to_owned()],
            )]))
        },
    )
    .await
    .expect("the question recording satisfies the contract");
    assert_eq!(
        records
            .last()
            .and_then(|record| record.outcome.text.as_deref()),
        Some("Red"),
        "the recorded answer reached the model"
    );
}

/// Transcript 39 — a sandbox-widening request surfaces with what it asks
/// for, and the exec that follows it routes on its own.
#[test]
fn a_permissions_request_surfaces_with_the_profile_it_asks_for() {
    let (events, _) = replay("39-0167-permissions-user-granular-ro-horde-perms.jsonl");
    let request = requested_as(&events, "request_permissions").expect("the request surfaces");
    assert_eq!(
        request.input["permissions"],
        serde_json::json!({"network": {"enabled": true}, "fileSystem": null})
    );
    assert!(
        request.input["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("Network access")),
        "{:?}",
        request.input
    );
    let exec = requested_as(&events, "exec").expect("the request surfaces");
    assert!(
        exec.input["command"]
            .as_str()
            .is_some_and(|command| command.starts_with("curl")),
        "{:?}",
        exec.input
    );
}

/// Every 0.167 recording satisfies the adapter contract when every request
/// in it is allowed (questions answered with their first option) — the
/// drift check for the new version's frame shapes as a whole.
#[tokio::test]
async fn every_0167_recording_satisfies_the_contract() {
    for name in [
        "34-0167-exec-user-granular-ro-horde-write.jsonl",
        "35-0167-exec-user-granular-ro-horde-write-accept.jsonl",
        "37-0167-filechange-user-granular-ro-horde-patch-accept.jsonl",
        "39-0167-permissions-user-granular-ro-horde-perms.jsonl",
        "40-0167-exec-user-onrequest-ro-horde-write.jsonl",
        "41-0167-basic-user-granular-ro-horde-two-turns.jsonl",
    ] {
        let records = checked_replay(name, &|_| horde_testkit::conformance::allow_all())
            .await
            .unwrap_or_else(|error| panic!("{name} violated the contract: {error:?}"));
        assert!(!records.is_empty(), "{name} produced no turns");
    }

    let two = checked_replay(
        "41-0167-basic-user-granular-ro-horde-two-turns.jsonl",
        &|_| horde_testkit::conformance::allow_all(),
    )
    .await
    .expect("conforms");
    let answers: Vec<Option<&str>> = two
        .iter()
        .map(|record| record.outcome.text.as_deref())
        .collect();
    assert_eq!(answers, vec![Some("OK"), Some("AGAIN")]);
    assert!(
        two.iter()
            .all(|record| record.outcome.usage.tokens.input_tokens > 0),
        "each turn bills its own tokens"
    );
}
