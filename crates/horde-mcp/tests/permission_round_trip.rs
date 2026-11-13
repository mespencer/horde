//! The permission chain, end to end, with nothing stubbed between the prompt
//! tool and the log.
//!
//! A real `horde-server` on a real loopback socket; the request journaled
//! through the real `PushEvents` path a Worker uses; the answer given through
//! the real `AnswerPermission` RPC by a client that found the escalation by
//! watching the log, the way a human's client does. The only fake is the
//! harness itself standing in for a Worker process.
//!
//! What each test is really asserting is that a decision made *over there* — by
//! someone who was not in the call — arrives back at the blocked agent intact.

mod common;

use common::{Setup, harness, identity_for, rewriting_harness, within};
use common::{decode_tool_response as decode, prompt_request as prompt};
use horde_adapter::permission::PermissionDecision;
use horde_cli::client::{Client, PermissionAnswer, SyncOptions};
use horde_mcp::permission::PermissionBroker;
use horde_proto::v1::PermissionOutcome;

/// Waits for the escalation to become visible, the way an attached client does:
/// by watching the log, not by being told.
async fn await_escalation(endpoint: &str, session_id: &str, request_id: &str) -> Setup<String> {
    let mut client = Client::connect(endpoint).await?;
    let mut stream = client
        .follow(&SyncOptions::new(session_id.to_owned()))
        .await?;

    // Bounded: a `respond` that never raises the request, or a request
    // journaled without its id, would otherwise leave this watching forever
    // for an event that never matches.
    let wanted = request_id.to_owned();
    let found = within(
        "the escalation appears in the log",
        stream.next_matching(move |envelope| {
            let Some(horde_proto::v1::event::Kind::PermissionRequested(requested)) = envelope
                .event
                .as_ref()
                .and_then(|event| event.kind.as_ref())
            else {
                return None;
            };
            let request = requested.request.as_ref()?;
            let id = request.request_id.as_ref().map(ToString::to_string)?;
            (id == wanted).then(|| {
                request
                    .tool_call
                    .as_ref()
                    .map(|call| call.rendered.clone())
                    .unwrap_or_default()
            })
        }),
    )
    .await?
    .ok_or("the escalation never appeared in the log")?;
    Ok(found)
}

/// Drives one escalation to a decision, returning what the harness would be
/// handed back.
async fn round_trip(
    tool_use_id: &'static str,
    input: serde_json::Value,
    outcome: PermissionOutcome,
    reason: &'static str,
    narrowing: Option<serde_json::Value>,
) -> Setup<serde_json::Value> {
    let harness = rewriting_harness().await?;
    let identity = identity_for(&harness.agent_id)?;
    let broker = PermissionBroker::new(harness.context().await?);
    let prompt = prompt("Write", input, tool_use_id)?;

    // The agent's side: blocks until somebody answers.
    let blocked = tokio::spawn(async move {
        let identity = identity;
        broker.respond(&identity, &prompt).await
    });

    // The human's side: sees the escalation, then answers it.
    let rendered = await_escalation(&harness.endpoint, &harness.session_id, tool_use_id).await?;
    assert!(
        !rendered.is_empty(),
        "an approver reads the rendering; an empty one is an unanswerable prompt"
    );

    let mut answering = Client::connect(&harness.endpoint).await?;
    answering
        .answer_permission(PermissionAnswer {
            request_id: tool_use_id,
            outcome,
            reason,
            remember_for_session: false,
            updated_input: narrowing.as_ref(),
            answers: horde_adapter::Answers::new(),
        })
        .await?;

    Ok(within("the decision reaches the blocked agent", blocked).await??)
}

/// The decision the model sees is the one the human made, carried across two
/// processes' worth of wire.
#[tokio::test]
async fn an_allow_reaches_the_blocked_agent() {
    let response = round_trip(
        "toolu_allow",
        serde_json::json!({"file_path": "/tmp/x", "content": "hi"}),
        PermissionOutcome::Allow,
        "",
        None,
    )
    .await
    .expect("the round trip must complete");

    let decoded = decode(&response).expect("the harness response must decode");
    assert_eq!(decoded["behavior"], "allow");
    assert_eq!(
        decoded["updatedInput"],
        serde_json::json!({"file_path": "/tmp/x", "content": "hi"}),
        "an unmodified allow must echo the proposal back — the harness runs \
         updatedInput, so omitting it would run nothing"
    );
}

/// The case the whole `updated_input` chain exists for: a human clamps an
/// argument, and the harness runs the clamped call rather than the proposed one.
#[tokio::test]
async fn a_narrowing_reaches_the_blocked_agent_as_the_arguments_to_run() {
    let response = round_trip(
        "toolu_narrow",
        serde_json::json!({"file_path": "/etc/hosts", "content": "hi"}),
        PermissionOutcome::Allow,
        "",
        Some(serde_json::json!({"file_path": "/tmp/safe", "content": "hi"})),
    )
    .await
    .expect("the round trip must complete");

    let decoded = decode(&response).expect("the harness response must decode");
    assert_eq!(decoded["behavior"], "allow");
    assert_eq!(
        decoded["updatedInput"]["file_path"], "/tmp/safe",
        "the clamp must survive; running the proposed path would be the \
         silently-widened permission this chain exists to prevent"
    );
}

#[tokio::test]
async fn a_denial_reaches_the_blocked_agent_with_its_reason() {
    let response = round_trip(
        "toolu_deny",
        serde_json::json!({"file_path": "/etc/hosts", "content": "hi"}),
        PermissionOutcome::Deny,
        "outside the workspace",
        None,
    )
    .await
    .expect("the round trip must complete");

    let decoded = decode(&response).expect("the harness response must decode");
    assert_eq!(decoded["behavior"], "deny");
    assert_eq!(
        decoded["message"], "outside the workspace",
        "the agent is told why, or it retries the same call forever"
    );
}

/// A pending approval survives the link to the server being cut: the human
/// answers during the partition, and the blocked agent hears it once the link
/// heals — the wait redials from its cursor rather than failing the prompt.
#[tokio::test]
async fn a_pending_approval_survives_a_severed_link() -> Setup<()> {
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;
    let proxy = horde_testkit::fault::FaultProxy::front(&harness.endpoint).await?;
    let broker = PermissionBroker::new(horde_mcp::context::AgentContext::new(
        harness.journal(),
        Client::connect(&proxy.endpoint()).await?,
        harness.session_id.clone(),
    ));
    let prompt = prompt(
        "Write",
        serde_json::json!({"file_path": "/tmp/x"}),
        "toolu_cut",
    )?;
    let mut blocked = tokio::spawn(async move { broker.respond(&identity, &prompt).await });

    await_escalation(&harness.endpoint, &harness.session_id, "toolu_cut").await?;
    proxy.cut();

    let mut answering = Client::connect(&harness.endpoint).await?;
    answering
        .answer_permission(PermissionAnswer::allow("toolu_cut"))
        .await?;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut blocked)
            .await
            .is_err(),
        "the answer cannot cross a cut link, and the wait must not fail meanwhile"
    );

    proxy.heal();
    let response = within("the decision crosses the healed link", blocked).await??;
    assert_eq!(decode(&response)?["behavior"], "allow");
    Ok(())
}

/// A sanity check that the fake and the real sink agree about the shape of a
/// failure: an unwritable journal is an error to the agent, never a wait.
#[tokio::test]
async fn an_unraisable_request_fails_rather_than_blocking_forever() -> Setup<()> {
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;

    // A journal pointed at nothing: the request cannot be written, so no human
    // will ever see it.
    let broker = PermissionBroker::new(harness.context_with_dead_journal().await?);
    let prompt = prompt("Write", serde_json::json!({}), "toolu_dead")?;

    let error = broker
        .respond(&identity, &prompt)
        .await
        .expect_err("an unwritable journal must surface, not hang");

    assert!(
        matches!(
            error,
            horde_mcp::permission::PermissionError::NotRaised { .. }
        ),
        "the agent must be told the question was never asked: {error}"
    );
    Ok(())
}

/// `PermissionDecision`'s domain type is what the endpoint reasons about, so
/// the allow-with-rewrite distinction has to survive the whole trip rather than
/// only the encoding at the end.
#[test]
fn the_domain_decision_distinguishes_a_narrowing_from_a_bare_allow() {
    assert_eq!(PermissionDecision::allow().updated_input(), None);
    assert_eq!(
        PermissionDecision::allow_with(serde_json::json!({"a": 1})).updated_input(),
        Some(&serde_json::json!({"a": 1}))
    );
}
