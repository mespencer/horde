//! The permission chain from its entry to its exits, with nothing stubbed.
//!
//! Entry: the real prompt tool's broker, journaling through the real
//! `PushEvents` path a Worker uses. Chain: the real server-side driver,
//! configured through `serve_with_permissions`. Exits: the blocked agent
//! unblocked by a chain decision with *no human in the loop* — or, when the
//! chain escalates, a human answering a request that now carries the tree
//! path and mode an approver needs.
//!
//! `permission_round_trip.rs` proves the human layer; this file proves the
//! three layers in front of it, and that adding them changed nothing about
//! the human's.

mod common;

use std::sync::{Arc, Mutex};

use common::{Setup, harness_with_permissions, identity_for, within};
use common::{decode_tool_response as decode, prompt_request as prompt};
use horde_cli::client::{Client, PermissionAnswer, SyncOptions};
use horde_mcp::permission::PermissionBroker;
use horde_server::permission::{
    Consultation, JudgeRuling, PermissionMode, PermissionModes, PolicyJudge, Rule,
};

/// A registry whose default mode allows workspace writes and denies `Bash`.
fn deterministic_modes() -> PermissionModes {
    PermissionModes::default().with_default(
        PermissionMode::escalate_everything()
            .deny(Rule::tool("Bash"), "no shell for this agent")
            .allow(Rule::tool("Write").where_input_starts_with("file_path", "/workspace/")),
    )
}

/// The agent's whole side of one escalation, driven to whatever the chain
/// (or a human elsewhere) decides.
async fn respond(
    harness: &common::Harness,
    broker: &PermissionBroker,
    input: serde_json::Value,
    tool: &str,
    tool_use_id: &str,
) -> Setup<serde_json::Value> {
    let identity = identity_for(&harness.agent_id)?;
    let response = within(
        "the chain must unblock the agent",
        broker.respond(&identity, &prompt(tool, input, tool_use_id)?),
    )
    .await?;
    decode(&response)
}

/// The whole point of the deterministic layers: an allowed call comes back
/// without any human having been asked — nobody is even attached.
#[tokio::test]
async fn a_rule_allowed_call_unblocks_with_no_human_in_the_loop() {
    let harness = harness_with_permissions(deterministic_modes(), None)
        .await
        .expect("the harness must stand up");
    let broker = PermissionBroker::new(harness.context().await.expect("a context"));

    let input = serde_json::json!({"file_path": "/workspace/notes.md", "content": "x"});
    let decided = respond(&harness, &broker, input.clone(), "Write", "toolu_auto")
        .await
        .expect("the chain must decide");

    assert_eq!(decided["behavior"], "allow");
    assert_eq!(
        decided["updatedInput"], input,
        "an unmodified allow echoes the proposal, same as a human's would"
    );
}

#[tokio::test]
async fn a_rule_denied_call_comes_back_with_the_rules_reason() {
    let harness = harness_with_permissions(deterministic_modes(), None)
        .await
        .expect("the harness must stand up");
    let broker = PermissionBroker::new(harness.context().await.expect("a context"));

    let decided = respond(
        &harness,
        &broker,
        serde_json::json!({"command": "rm -rf /"}),
        "Bash",
        "toolu_denied",
    )
    .await
    .expect("the chain must decide");

    assert_eq!(decided["behavior"], "deny");
    assert_eq!(
        decided["message"], "no shell for this agent",
        "the agent reads the deny rule's reason, or it retries forever"
    );
}

/// A scripted judge — a real implementation of the seam, answering from a
/// script and recording what it was consulted about.
#[derive(Debug)]
struct ScriptedJudge {
    ruling: JudgeRuling,
    consulted: Mutex<Vec<(String, String, Vec<String>)>>,
}

#[async_trait::async_trait]
impl PolicyJudge for ScriptedJudge {
    async fn rule(&self, consultation: Consultation<'_>) -> JudgeRuling {
        if let Ok(mut consulted) = self.consulted.lock() {
            consulted.push((
                consultation.instructions.to_owned(),
                consultation.call.tool_name.clone(),
                consultation.ancestry.to_vec(),
            ));
        }
        self.ruling.clone()
    }
}

#[tokio::test]
async fn the_judge_layer_rules_across_the_whole_stack() {
    let judge = Arc::new(ScriptedJudge {
        ruling: JudgeRuling::Deny {
            rationale: "the mode says never the network".to_owned(),
        },
        consulted: Mutex::new(Vec::new()),
    });
    let modes = PermissionModes::default()
        .with_default(PermissionMode::escalate_everything().judged_by("never the network"));
    let harness = harness_with_permissions(modes, Some(judge.clone()))
        .await
        .expect("the harness must stand up");
    let broker = PermissionBroker::new(harness.context().await.expect("a context"));

    let decided = respond(
        &harness,
        &broker,
        serde_json::json!({"url": "https://example.com"}),
        "WebFetch",
        "toolu_judged",
    )
    .await
    .expect("the judge must decide");

    assert_eq!(decided["behavior"], "deny");
    assert_eq!(
        decided["message"], "the mode says never the network",
        "the judge's rationale is what the agent reads"
    );

    let consulted = judge.consulted.lock().expect("lock");
    assert_eq!(
        consulted.as_slice(),
        &[(
            "never the network".to_owned(),
            "WebFetch".to_owned(),
            vec!["endpoint".to_owned()],
        )],
        "the judge read the instructions, the call, and the real tree path"
    );
}

/// The enriched request in this envelope, if it answers `request_id`.
///
/// "Enriched" is `escalated_from` being set — the chain's own annotation, so
/// matching on it proves the chain processed the request rather than the
/// raiser having pre-filled the context.
fn enrichment_for(
    envelope: &horde_proto::v1::EventEnvelope,
    request_id: &str,
) -> Option<horde_proto::v1::PermissionRequest> {
    let horde_proto::v1::event::Kind::PermissionRequested(requested) =
        envelope.event.as_ref()?.kind.as_ref()?
    else {
        return None;
    };
    let request = requested.request.clone()?;
    let id = request.request_id.as_ref().map(ToString::to_string)?;
    (id == request_id
        && request.escalated_from != horde_proto::v1::PermissionLayer::Unspecified as i32)
        .then_some(request)
}

/// Escalation with the chain live: the approver now reads a tree path and the
/// layer that gave up, and the human's answer still lands exactly as before.
#[tokio::test]
async fn an_escalated_request_reaches_the_approver_enriched_and_still_answerable() {
    let harness = harness_with_permissions(PermissionModes::default(), None)
        .await
        .expect("the harness must stand up");
    let broker = PermissionBroker::new(harness.context().await.expect("a context"));
    let identity = identity_for(&harness.agent_id).expect("an identity");

    let blocked = tokio::spawn(async move {
        let identity = identity;
        let prompt = prompt(
            "Write",
            serde_json::json!({"file_path": "/etc/hosts", "content": "x"}),
            "toolu_escalated",
        )?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
            broker.respond(&identity, &prompt).await?,
        )
    });

    // The approver's view: the enriched re-raise, found by watching the log.
    let mut watcher = Client::connect(&harness.endpoint)
        .await
        .expect("a watcher connects");
    let mut stream = watcher
        .follow(&SyncOptions::new(harness.session_id.clone()))
        .await
        .expect("the log is followable");
    let enriched = within(
        "the enriched escalation appears",
        stream.next_matching(|envelope| enrichment_for(envelope, "toolu_escalated")),
    )
    .await
    .expect("the stream must not end")
    .expect("the enrichment must arrive");

    assert_eq!(
        enriched.tree_path,
        vec!["endpoint".to_owned()],
        "the approver reads where in the tree this came from"
    );
    assert_eq!(
        enriched.escalated_from,
        horde_proto::v1::PermissionLayer::DeterministicAllow as i32,
        "and which layer gave up"
    );

    // The human's answer still works exactly as it did before the chain.
    let mut answering = Client::connect(&harness.endpoint)
        .await
        .expect("an answerer connects");
    answering
        .answer_permission(PermissionAnswer::allow("toolu_escalated"))
        .await
        .expect("the answer must land");

    let response = within("the decision reaches the blocked agent", blocked)
        .await
        .expect("the agent task must finish")
        .expect("the round trip must complete");
    let decided = decode(&response).expect("the response must decode");
    assert_eq!(decided["behavior"], "allow");
}

/// Remember-for-session, end to end: the first call needs a human; the
/// identical repeat is settled by the widened allow layer with nobody
/// watching.
#[tokio::test]
async fn a_remembered_answer_settles_the_repeat_without_a_human() {
    let harness = harness_with_permissions(PermissionModes::default(), None)
        .await
        .expect("the harness must stand up");
    // One broker for both calls: it is the same agent asking twice, and its
    // journal keeps one worker's sequence.
    let broker = PermissionBroker::new(harness.context().await.expect("a context"));
    let identity = identity_for(&harness.agent_id).expect("an identity");
    let input = serde_json::json!({"file_path": "/etc/exports", "content": "x"});

    // First ask: blocks until the human allows, remembering for the session.
    let first = {
        let prompt = prompt("Write", input.clone(), "toolu_first").expect("a prompt");
        let respond = broker.respond(&identity, &prompt);

        let mut watcher = Client::connect(&harness.endpoint)
            .await
            .expect("a watcher connects");
        let session = harness.session_id.clone();
        let endpoint = harness.endpoint.clone();
        let answer = async move {
            let mut stream = watcher.follow(&SyncOptions::new(session)).await?;
            stream
                .next_matching(common::escalation_request_id)
                .await?
                .ok_or("the escalation must be raised")?;
            let mut answering = Client::connect(&endpoint).await?;
            answering
                .answer_permission(PermissionAnswer::allow("toolu_first").remembering(true))
                .await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };

        let (response, answered) =
            within("the first ask is answered by the human", async move {
                tokio::join!(respond, answer)
            })
            .await;
        answered.expect("the answering side must succeed");
        response.expect("the agent must be unblocked")
    };
    assert_eq!(
        decode(&first).expect("decodes")["behavior"],
        "allow",
        "the human's own answer, as ever"
    );

    // The identical repeat: nobody is attached, nobody answers, and the chain
    // settles it from what the session remembered.
    let repeat = respond(&harness, &broker, input, "Write", "toolu_repeat")
        .await
        .expect("the remembered allow must settle the repeat");
    assert_eq!(repeat["behavior"], "allow");
}
