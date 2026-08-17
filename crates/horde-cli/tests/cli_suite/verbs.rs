//! Every verb driven through `cli::run` against a real server.
//!
//! The subprocess tests in `cli.rs` prove the binary's plumbing — argument
//! parsing, streams, exit codes — but a subprocess is opaque to coverage, so
//! the dispatch itself would read as untested however thoroughly it was
//! exercised. Calling `run` in-process closes that, and it is the same code the
//! binary runs: `bin/horde.rs` is `Cli::parse()` plus an exit code.
//!
//! Each case builds its `Cli` by parsing real argument vectors, so a flag that
//! stops reaching its verb fails here rather than in someone's shell.

use crate::common::{self, Setup, invoke, serve};

/// Creates a session and returns its id and root agent id.
async fn seed(endpoint: &str) -> Setup<(String, String)> {
    let (_, session_id, root) = common::seeded(endpoint, "verbs").await?;
    Ok((session_id, root))
}

/// The event kinds a session's log contains, read back through the client.
///
/// Verbs are asserted by their effect on the log rather than by returning `Ok`:
/// a verb that quietly did nothing also returns `Ok`, and mutation testing says
/// so — replacing these bodies with `Ok(())` survived until this was checked.
async fn kinds(endpoint: &str, session_id: &str) -> Setup<Vec<String>> {
    let mut client = horde_cli::Client::connect(endpoint).await?;
    let mut seen = Vec::new();
    client
        .sync(
            &horde_cli::client::SyncOptions::new(session_id),
            |envelope| {
                if let Some(event) = &envelope.event
                    && let Some(kind) = &event.kind
                {
                    seen.push(format!("{kind:?}"));
                }
            },
            |_| {},
        )
        .await?;
    Ok(seen)
}

/// Counts how many events of a kind the session holds.
async fn count_of(endpoint: &str, session_id: &str, needle: &str) -> usize {
    kinds(endpoint, session_id)
        .await
        .unwrap_or_default()
        .iter()
        .filter(|kind| kind.starts_with(needle))
        .count()
}

#[tokio::test]
async fn a_send_actually_appends_an_assignment() {
    let endpoint = serve().await.expect("a listening server");
    let (session_id, agent_id) = seed(&endpoint).await.expect("a seeded session");

    let before = count_of(&endpoint, &session_id, "AssignmentSent").await;
    invoke(&endpoint, &["send", &agent_id, "do the thing"])
        .await
        .expect("send must succeed");
    assert_eq!(
        count_of(&endpoint, &session_id, "AssignmentSent").await,
        before + 1,
        "a send that returns Ok without appending is a no-op"
    );
}

#[tokio::test]
async fn a_note_appends_a_note_and_not_an_assignment() {
    let endpoint = serve().await.expect("a listening server");
    let (session_id, agent_id) = seed(&endpoint).await.expect("a seeded session");

    invoke(&endpoint, &["send", &agent_id, "fyi", "--note"])
        .await
        .expect("send must succeed");

    assert_eq!(count_of(&endpoint, &session_id, "NoteSent").await, 1);
    assert_eq!(
        count_of(&endpoint, &session_id, "AssignmentSent").await,
        0,
        "--note must not be silently dropped into an assignment"
    );
}

#[tokio::test]
async fn an_interrupt_actually_appends_a_turn_interrupted() {
    let endpoint = serve().await.expect("a listening server");
    let (session_id, agent_id) = seed(&endpoint).await.expect("a seeded session");

    invoke(&endpoint, &["interrupt", &agent_id, "--reason", "stop"])
        .await
        .expect("interrupt must succeed");
    assert_eq!(
        count_of(&endpoint, &session_id, "TurnInterrupted").await,
        1,
        "an interrupt that returns Ok without appending is a no-op"
    );
}

#[tokio::test]
async fn a_spawn_actually_adds_an_agent_and_records_its_placement() {
    let endpoint = serve().await.expect("a listening server");
    let (session_id, _) = seed(&endpoint).await.expect("a seeded session");

    let before = count_of(&endpoint, &session_id, "AgentSpawned").await;
    invoke(&endpoint, &["spawn", &session_id])
        .await
        .expect("spawn must succeed");

    assert_eq!(
        count_of(&endpoint, &session_id, "AgentSpawned").await,
        before + 1
    );
    assert_eq!(
        count_of(&endpoint, &session_id, "PlacementChanged").await,
        2,
        "a spawn records where it landed, even when that is nowhere — \
         beside the placement the root agent got at session creation"
    );
}

#[tokio::test]
async fn every_verb_runs_against_a_real_server() {
    let endpoint = serve().await.expect("a listening server");
    let (session_id, agent_id) = seed(&endpoint).await.expect("a seeded session");

    // Grouped rather than one test each: the point is that the dispatch reaches
    // every verb, and a per-verb test would repeat this setup seven times.
    let cases: Vec<Vec<&str>> = vec![
        vec!["hello"],
        vec!["session", "list"],
        vec!["session", "list", "--include-archived"],
        vec!["session", "get", &session_id],
        vec!["session", "new", "another", "--idempotency-key", "key-2"],
        vec!["sync", &session_id],
        vec!["sync", &session_id, "--from", "2"],
        vec!["send", &agent_id, "an assignment"],
        vec!["send", &agent_id, "a note", "--note"],
        vec!["send", &agent_id, "steer me", "--interrupt"],
        vec!["interrupt", &agent_id, "--reason", "changed my mind"],
        vec!["spawn", &session_id],
        vec!["spawn", &session_id, "--parent", &agent_id],
        vec!["usage"],
        vec!["usage", "--by", "session"],
        vec!["usage", "--by", "agent", "--session", &session_id],
        vec!["usage", "--by", "tree", "--subtree", &agent_id],
        vec!["usage", "--by", "model"],
        vec!["session", "archive", &session_id, "--reason", "done"],
    ];

    for case in cases {
        invoke(&endpoint, &case)
            .await
            .unwrap_or_else(|error| panic!("{case:?} must succeed: {error}"));
    }
}

#[tokio::test]
async fn every_verb_also_runs_in_json_mode() {
    let endpoint = serve().await.expect("a listening server");
    let (session_id, agent_id) = seed(&endpoint).await.expect("a seeded session");

    for case in [
        vec!["--json", "hello"],
        vec!["--json", "session", "list"],
        vec!["--json", "session", "get", &session_id],
        vec!["--json", "sync", &session_id],
        vec!["--json", "send", &agent_id, "text"],
        vec!["--json", "interrupt", &agent_id],
        vec!["--json", "spawn", &session_id],
        vec!["--json", "usage"],
    ] {
        invoke(&endpoint, &case)
            .await
            .unwrap_or_else(|error| panic!("{case:?} must succeed: {error}"));
    }
}

/// The workspace surface (fix a12), through the real dispatch: create,
/// list, get by id and by name, add-repo at the right etag, delete — plus
/// `worker list` and a `prewarm` refusal for a worker nobody enrolled.
#[tokio::test]
async fn the_workspace_verbs_round_trip_through_the_dispatch() {
    let endpoint = serve().await.expect("a listening server");

    invoke(
        &endpoint,
        &[
            "workspace",
            "create",
            "atlas-ws",
            "--repo",
            "atlas=https://git.example/atlas.git,branch=main,path=src",
            "--unmanaged",
            "datasets",
        ],
    )
    .await
    .expect("the definition creates");

    // The id and etag, read back through the client library — the CLI
    // prints them, but a test scraping stdout would be testing println.
    let mut client = horde_cli::Client::connect(&endpoint)
        .await
        .expect("a client connects");
    let listed = client
        .list_workspace_definitions()
        .await
        .expect("the list answers");
    assert_eq!(listed.len(), 1, "exactly the created definition");
    let id = listed[0]
        .workspace_definition_id
        .as_ref()
        .expect("an id")
        .to_string();
    let etag = listed[0].etag.clone();

    for case in [
        vec!["workspace", "list"],
        vec!["--json", "workspace", "list"],
        vec!["workspace", "get", id.as_str()],
        vec!["--json", "workspace", "get", "atlas-ws"],
        vec!["worker", "list"],
        vec!["--json", "worker", "list"],
    ] {
        invoke(&endpoint, &case)
            .await
            .unwrap_or_else(|error| panic!("{case:?} must succeed: {error}"));
    }

    // Lookup by a name that is neither an id nor a name refuses.
    let missing = invoke(&endpoint, &["workspace", "get", "no-such-workspace"])
        .await
        .expect_err("an unknown reference is refused");
    assert!(
        missing.to_string().contains("no-such-workspace"),
        "{missing}"
    );

    invoke(
        &endpoint,
        &[
            "workspace",
            "add-repo",
            &id,
            "--repo",
            "docs=https://git.example/docs.git",
            "--etag",
            &etag,
        ],
    )
    .await
    .expect("the repo set widens at the right etag");

    // Prewarm refuses honestly for a worker nobody enrolled; the verb still
    // crossed the wire.
    let refused = invoke(&endpoint, &["prewarm", &id, "worker-that-is-not-there"])
        .await
        .expect_err("prewarming an absent worker refuses");
    assert!(!refused.to_string().is_empty());

    let refreshed = client
        .get_workspace_definition(&id)
        .await
        .expect("the widened definition reads back");
    let definition = refreshed.definition.expect("a definition");
    assert_eq!(definition.repos.len(), 2, "the added repo is in the set");

    invoke(
        &endpoint,
        &["workspace", "delete", &id, "--etag", &definition.etag],
    )
    .await
    .expect("the definition deletes at its current etag");
    assert!(
        client
            .list_workspace_definitions()
            .await
            .expect("the list answers")
            .is_empty(),
        "deleted means gone from the listing"
    );
}

/// `approve` needs an outstanding escalation, which nothing creates yet — no
/// worker exists to request one. Both paths are still exercised: the server
/// refuses an unknown request id, which proves the verb reaches it and that a
/// decision going nowhere is reported rather than silently accepted.
#[tokio::test]
async fn approve_reaches_the_server_and_an_unknown_request_is_refused() {
    let endpoint = serve().await.expect("a listening server");

    for case in [
        vec!["approve", "no-such-request"],
        vec!["approve", "no-such-request", "--deny", "--reason", "nope"],
        vec!["approve", "no-such-request", "--remember"],
    ] {
        let error = invoke(&endpoint, &case)
            .await
            .expect_err("no escalation is outstanding");
        assert!(
            error.to_string().contains("no-such-request"),
            "{case:?} must name what was not found: {error}"
        );
    }
}

#[tokio::test]
async fn a_verb_against_a_missing_session_fails_rather_than_printing_nothing() {
    let endpoint = serve().await.expect("a listening server");

    let error = invoke(&endpoint, &["session", "get", "session-that-never-existed"])
        .await
        .expect_err("a missing session must fail");
    assert!(error.to_string().contains("session-that-never-existed"));

    // The usage verb holds the same line: a typo'd session must not read as
    // "no usage recorded".
    let error = invoke(
        &endpoint,
        &["usage", "--session", "session-that-never-existed"],
    )
    .await
    .expect_err("usage against a missing session must fail");
    assert!(error.to_string().contains("session-that-never-existed"));
}
