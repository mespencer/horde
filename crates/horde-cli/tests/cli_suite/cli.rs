//! The real `horde` binary against a real localhost server.
//!
//! Everything else in this crate exercises the library. This file exercises the
//! part only a subprocess can: argument parsing, the `--json` flag actually
//! reaching the renderer, what lands on stdout versus stderr, and the exit
//! code. A library test cannot catch a verb wired to the wrong call or a flag
//! that parses and is then ignored.
//!
//! The server comes from `common::serve`, on a real loopback socket — which is
//! the reason that harness binds one at all: a subprocess cannot be handed an
//! in-process stream.

use crate::common::{self, Setup, serve};
use tokio::process::Command;

/// What a `horde` invocation produced.
struct Run {
    stdout: String,
    stderr: String,
    success: bool,
}

/// Runs the real binary. `CARGO_BIN_EXE_horde` is the one cargo just built, so
/// this can never test a stale copy on `PATH`.
///
/// Async on purpose: the server under test runs as a task on this same runtime,
/// so a blocking `std::process::Command::output` would stop it being polled and
/// the subprocess would wait forever for a server that never accepts.
async fn horde(endpoint: &str, args: &[&str]) -> Setup<Run> {
    let output = Command::new(env!("CARGO_BIN_EXE_horde"))
        .arg("--endpoint")
        .arg(endpoint)
        .args(args)
        .output()
        .await?;

    Ok(Run {
        stdout: String::from_utf8(output.stdout)?,
        stderr: String::from_utf8(output.stderr)?,
        success: output.status.success(),
    })
}

#[tokio::test]
async fn hello_prints_the_server_version() {
    let endpoint = serve().await.expect("a listening server");

    let run = horde(&endpoint, &["hello"])
        .await
        .expect("the binary must run");
    assert!(run.success, "stderr was: {}", run.stderr);
    assert!(
        run.stdout.contains("horde-server"),
        "expected a server banner, got {:?}",
        run.stdout
    );
}

#[tokio::test]
async fn session_list_says_so_when_there_is_nothing() {
    let endpoint = serve().await.expect("a listening server");

    let run = horde(&endpoint, &["session", "list"]).await.expect("run");
    assert!(run.success, "stderr was: {}", run.stderr);
    assert_eq!(
        run.stdout.trim(),
        "no sessions",
        "an empty listing must say so, or it is indistinguishable from a swallowed error"
    );
}

#[tokio::test]
async fn session_list_json_is_machine_readable() {
    let endpoint = serve().await.expect("a listening server");

    // Seed through the library, then read back through the binary — the point
    // is the CLI's output, not another round of the library's write path.
    common::seeded(&endpoint, "ci-fix").await.expect("seed");

    let run = horde(&endpoint, &["--json", "session", "list"])
        .await
        .expect("run");
    assert!(run.success, "stderr was: {}", run.stderr);

    let parsed: serde_json::Value =
        serde_json::from_str(run.stdout.trim()).expect("--json must emit valid JSON on stdout");
    let sessions = parsed["sessions"].as_array().expect("a sessions array");
    assert_eq!(sessions.len(), 1);
    assert_eq!(
        sessions[0]["title"], "ci-fix",
        "the summary carries the title the session was created with"
    );
    assert_eq!(sessions[0]["state"], "active");
}

#[tokio::test]
async fn session_new_prints_the_root_agent_the_next_command_needs() {
    let endpoint = serve().await.expect("a listening server");

    let run = horde(&endpoint, &["session", "new", "ci-fix"])
        .await
        .expect("run");
    assert!(run.success, "stderr was: {}", run.stderr);
    assert!(
        run.stdout.contains("root agent:"),
        "the id a user must copy has to be on screen: {:?}",
        run.stdout
    );
}

#[tokio::test]
async fn sync_replays_a_session_to_stdout_and_exits() {
    let endpoint = serve().await.expect("a listening server");

    let (_, session_id, _) = common::seeded(&endpoint, "sync").await.expect("seed");

    // No `--follow`: the scriptable form replays to head and completes, which
    // is what makes this test terminate at all.
    let run = horde(&endpoint, &["sync", &session_id]).await.expect("run");
    assert!(run.success, "stderr was: {}", run.stderr);

    let lines: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "the root's spawn and its placement answer: {lines:?}"
    );
    assert!(
        lines[0].contains("agent_spawned"),
        "the session's first event is its root agent: {:?}",
        lines[0]
    );
    assert!(
        lines[1].contains("placement_changed"),
        "and the second is where that agent landed (or why nowhere): {:?}",
        lines[1]
    );
}

#[tokio::test]
async fn sync_from_an_offset_skips_what_came_before() {
    let endpoint = serve().await.expect("a listening server");

    let (mut client, session_id, root) = common::seeded(&endpoint, "offsets").await.expect("seed");
    common::send_notes(&mut client, &root, 3)
        .await
        .expect("send");

    let all = horde(&endpoint, &["--json", "sync", &session_id])
        .await
        .expect("run");
    let tail = horde(&endpoint, &["--json", "sync", &session_id, "--from", "3"])
        .await
        .expect("run");
    assert!(all.success && tail.success);

    let sequences = |stdout: &str| -> Vec<u64> {
        stdout
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter_map(|value| value["sequence"].as_u64())
            .collect()
    };

    let full = sequences(&all.stdout);
    let partial = sequences(&tail.stdout);
    assert_eq!(full, vec![1, 2, 3, 4, 5]);
    assert_eq!(
        partial,
        vec![3, 4, 5],
        "--from must reach the server rather than being parsed and ignored"
    );
}

/// An unbuilt verb must be recognisable as such, and must not be mistaken for
/// success by a script checking the exit code.
#[tokio::test]
async fn an_unknown_session_fails_with_a_nonzero_exit_and_a_message_on_stderr() {
    let endpoint = serve().await.expect("a listening server");

    let run = horde(&endpoint, &["session", "get", "session-that-never-existed"])
        .await
        .expect("run");
    assert!(!run.success, "a missing session must not exit zero");
    assert!(
        run.stderr.contains("session-that-never-existed"),
        "the diagnosis belongs on stderr: {:?}",
        run.stderr
    );
    assert!(
        run.stdout.trim().is_empty(),
        "nothing may go to stdout on failure, or a script will parse an error as data"
    );
}

#[tokio::test]
async fn a_server_that_is_not_there_is_reported_as_unreachable() {
    // Port 1 on loopback: nothing listens there, and binding it would need
    // privileges, so this is a reliable refusal rather than a race.
    let run = horde("http://127.0.0.1:1", &["hello"])
        .await
        .expect("the binary must run");

    assert!(!run.success);
    assert!(
        run.stderr.contains("could not reach the server"),
        "the message must point at the connection, not at a verb: {:?}",
        run.stderr
    );
    assert!(
        run.stderr.contains("hint: nothing is listening"),
        "the source chain is walked to the cause, in plain words: {:?}",
        run.stderr
    );
}

#[tokio::test]
async fn a_malformed_endpoint_is_refused_before_anything_is_attempted() {
    let run = horde("not-a-uri", &["hello"])
        .await
        .expect("the binary must run");

    assert!(!run.success);
    assert!(
        run.stderr.contains("valid endpoint URI"),
        "a typo must send the user to the address, not to the server: {:?}",
        run.stderr
    );
}

/// The MCP Hub verbs, through the real binary: add, list (both formats),
/// remove — the catalog round trip a user actually types (ADR-0004).
#[tokio::test]
async fn the_mcp_verbs_round_trip_the_hub_catalog() {
    let endpoint = serve().await.expect("a listening server");

    let added = horde(
        &endpoint,
        &[
            "mcp",
            "add",
            "linear",
            "--command",
            "linear-mcp",
            "--arg",
            "--stdio",
        ],
    )
    .await
    .expect("run");
    assert!(added.success, "stderr was: {}", added.stderr);
    assert!(
        added.stdout.contains("configured linear"),
        "got {:?}",
        added.stdout
    );

    let listed = horde(&endpoint, &["--json", "mcp", "list"])
        .await
        .expect("run");
    assert!(listed.success, "stderr was: {}", listed.stderr);
    let parsed: serde_json::Value = serde_json::from_str(listed.stdout.trim()).expect("JSON out");
    assert_eq!(parsed["servers"][0]["name"], "linear");
    let etag = parsed["servers"][0]["etag"]
        .as_str()
        .expect("an etag rides the listing")
        .to_owned();

    let removed = horde(&endpoint, &["mcp", "remove", "linear", "--etag", &etag])
        .await
        .expect("run");
    assert!(removed.success, "stderr was: {}", removed.stderr);

    let empty = horde(&endpoint, &["mcp", "list"]).await.expect("run");
    assert!(empty.success, "stderr was: {}", empty.stderr);
    assert_eq!(
        empty.stdout.trim(),
        "no MCP servers configured",
        "an empty catalog says so"
    );
}

// -- The session front door (ADR-0008) ---------------------------------------

/// A bare invocation with no terminal fails fast, with the candidate list in
/// the error — never a hang waiting for a picker nobody can see. `output()`
/// wires stdin to the null device, which is exactly the headless shape.
#[tokio::test]
async fn a_headless_bare_horde_fails_fast_with_the_candidates() {
    let endpoint = serve().await.expect("a listening server");

    let empty = horde(&endpoint, &[]).await.expect("run");
    assert!(!empty.success, "a headless bare horde must not exit zero");
    assert!(
        empty.stderr.contains("horde new"),
        "with nothing live, the error names the remedy: {:?}",
        empty.stderr
    );

    let (_, session_id, _) = common::seeded(&endpoint, "ci-fix").await.expect("seed");
    let listed = horde(&endpoint, &[]).await.expect("run");
    assert!(!listed.success);
    assert!(
        listed.stderr.contains("ci-fix") && listed.stderr.contains(&session_id[..12]),
        "the candidates ride in the error so the next invocation can name one: {:?}",
        listed.stderr
    );
    assert!(
        listed.stdout.trim().is_empty(),
        "nothing goes to stdout on the refusal"
    );
}

/// `session new` without a title draws a thematic name from the word list
/// and prints it — and `--json` carries both id and name, so the script that
/// just created the session never re-resolves.
#[tokio::test]
async fn session_new_without_a_title_draws_a_thematic_name() {
    let endpoint = serve().await.expect("a listening server");

    let run = horde(&endpoint, &["--json", "session", "new"])
        .await
        .expect("run");
    assert!(run.success, "stderr was: {}", run.stderr);
    let parsed: serde_json::Value = serde_json::from_str(run.stdout.trim()).expect("JSON out");
    let title = parsed["title"].as_str().expect("a generated title");
    let (adjective, noun) = title.split_once('-').expect("adjective-noun");
    assert!(
        horde_cli::names::ADJECTIVES.contains(&adjective)
            && horde_cli::names::NOUNS.contains(&noun),
        "the name comes from the fixed word list: {title:?}"
    );
    assert_eq!(
        parsed["name"], title,
        "a drawn name is already its own handle"
    );
    assert!(
        parsed["session_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "id and name both ride the JSON"
    );
}

/// Wherever a session id is accepted, a name or a unique id prefix resolves
/// to it, client-side — and ambiguity is an error naming the candidates,
/// never a guess.
#[tokio::test]
async fn session_references_resolve_by_name_and_unique_prefix() {
    let endpoint = serve().await.expect("a listening server");
    let (mut client, session_id, _) = common::seeded(&endpoint, "Fix Auth Timeout")
        .await
        .expect("seed");

    let by_name = horde(&endpoint, &["--json", "session", "get", "fix-auth-timeout"])
        .await
        .expect("run");
    assert!(by_name.success, "stderr was: {}", by_name.stderr);
    let parsed: serde_json::Value = serde_json::from_str(by_name.stdout.trim()).expect("JSON");
    assert_eq!(
        parsed["session_id"], *session_id,
        "the slugified title is the handle"
    );

    let by_prefix = horde(&endpoint, &["--json", "session", "get", "session-key"])
        .await
        .expect("run");
    assert!(by_prefix.success, "a unique prefix resolves git-style");

    let synced = horde(&endpoint, &["sync", "fix-auth-timeout"])
        .await
        .expect("run");
    assert!(
        synced.success,
        "sync resolves the same way: {}",
        synced.stderr
    );

    client
        .create_session("Fix Auth Timeout", "key-2")
        .await
        .expect("a name twin");
    let ambiguous = horde(&endpoint, &["session", "get", "fix-auth-timeout"])
        .await
        .expect("run");
    assert!(!ambiguous.success, "two sessions, one name, no guessing");
    assert!(
        ambiguous.stderr.contains("session-key-1") && ambiguous.stderr.contains("session-key-2"),
        "the refusal names the candidates: {:?}",
        ambiguous.stderr
    );
}

/// A session name where an agent is expected addresses the session's root
/// agent — `send` and `interrupt` need no second id.
#[tokio::test]
async fn a_session_name_addresses_its_root_agent_in_send_and_interrupt() {
    let endpoint = serve().await.expect("a listening server");
    let (mut client, session_id, root) = common::seeded(&endpoint, "chatty").await.expect("seed");

    let sent = horde(&endpoint, &["--json", "send", "chatty", "hello there"])
        .await
        .expect("run");
    assert!(sent.success, "stderr was: {}", sent.stderr);
    let parsed: serde_json::Value = serde_json::from_str(sent.stdout.trim()).expect("JSON");
    assert_eq!(
        parsed["session_id"], *session_id,
        "the message landed in the named session"
    );

    let interrupted = horde(&endpoint, &["interrupt", "chatty"])
        .await
        .expect("run");
    assert!(
        interrupted.success,
        "interrupt aliases the same way: {}",
        interrupted.stderr
    );

    // The alias really meant the root agent: the send is journaled against it.
    let events = client.list_sessions(false).await.expect("list");
    assert_eq!(events.len(), 1);
    let run = horde(&endpoint, &["--json", "sync", &session_id])
        .await
        .expect("run");
    assert!(
        run.stdout.lines().any(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .is_ok_and(|value| value["agent_id"] == *root)
        }),
        "the aliased verbs journaled against the root agent: {}",
        run.stdout
    );
}

/// Archived sessions stay out of name resolution unless `--all` — and a full
/// id always passes through, so nothing that used to work stops working.
#[tokio::test]
async fn archived_sessions_resolve_only_with_all_but_full_ids_always_do() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, _) = common::seeded(&endpoint, "done-work").await.expect("seed");

    let archived = horde(&endpoint, &["session", "archive", "done-work"])
        .await
        .expect("run");
    assert!(
        archived.success,
        "a live session archives by name: {}",
        archived.stderr
    );

    let hidden = horde(&endpoint, &["session", "get", "done-work"])
        .await
        .expect("run");
    assert!(
        !hidden.success,
        "an archived session's name is out of resolution by default"
    );
    assert!(
        hidden.stderr.contains("archived") && hidden.stderr.contains("--all"),
        "the refusal says it is archived and names the flag: {}",
        hidden.stderr
    );

    let unknown = horde(&endpoint, &["--json", "session", "get", "never-was"])
        .await
        .expect("run");
    assert!(!unknown.success, "an unknown name is refused");
    let refusal: serde_json::Value =
        serde_json::from_str(unknown.stderr.trim()).expect("--json refusals are JSON on stderr");
    assert_eq!(refusal["error"]["kind"], "unknown_session");

    let widened = horde(
        &endpoint,
        &["--json", "session", "get", "done-work", "--all"],
    )
    .await
    .expect("run");
    assert!(widened.success, "--all widens: {}", widened.stderr);
    let parsed: serde_json::Value = serde_json::from_str(widened.stdout.trim()).expect("JSON");
    assert_eq!(parsed["state"], "archived");

    let by_id = horde(&endpoint, &["session", "get", &session_id])
        .await
        .expect("run");
    assert!(
        by_id.success,
        "the full id reaches an archived session without --all: {}",
        by_id.stderr
    );
}

/// `agent status` finds an agent by a session name (its root) or by id,
/// searching the live sessions, and refuses an agent no log mentions.
#[tokio::test]
async fn agent_status_reads_the_agent_off_its_sessions_log() {
    let endpoint = serve().await.expect("a listening server");
    let (_, session_id, root) = common::seeded(&endpoint, "status-check")
        .await
        .expect("seed");

    let by_name = horde(&endpoint, &["--json", "agent", "status", "status-check"])
        .await
        .expect("run");
    assert!(by_name.success, "{}", by_name.stderr);
    let status: serde_json::Value = serde_json::from_str(by_name.stdout.trim()).expect("JSON");
    assert_eq!(status["agent_id"], root.as_str());
    assert_eq!(status["session_id"], session_id.as_str());

    let by_id = horde(
        &endpoint,
        &["agent", "status", &root, "--session", &session_id],
    )
    .await
    .expect("run");
    assert!(by_id.success, "{}", by_id.stderr);
    assert!(
        by_id.stdout.starts_with(&format!("agent {root}\n")),
        "{}",
        by_id.stdout
    );

    let unknown = horde(&endpoint, &["agent", "status", "agent-never"])
        .await
        .expect("run");
    assert!(!unknown.success);
    assert!(
        unknown.stderr.contains("no agent \"agent-never\""),
        "{}",
        unknown.stderr
    );
}

/// `horde help` is `--help`; a usage mistake under `--json` is a JSON
/// object on stderr with a non-zero exit.
#[tokio::test]
async fn help_is_a_verb_and_usage_errors_honor_json() {
    let endpoint = serve().await.expect("a listening server");
    let help = horde(&endpoint, &["help"]).await.expect("run");
    assert!(help.success, "{}", help.stderr);
    assert!(help.stdout.contains("Usage: horde"), "{}", help.stdout);

    let refused = horde(
        &endpoint,
        &["--json", "worker", "enroll", "w", "--ttl-minutes", "0"],
    )
    .await
    .expect("run");
    assert!(!refused.success);
    let error: serde_json::Value =
        serde_json::from_str(refused.stderr.trim()).expect("a JSON refusal");
    assert_eq!(error["error"]["kind"], "usage");

    let prose = horde(&endpoint, &["worker", "enroll", "w", "--ttl-minutes", "0"])
        .await
        .expect("run");
    assert!(!prose.success);
    assert!(prose.stderr.contains("--ttl-minutes"), "{}", prose.stderr);
}

/// `horde new --detach` is the scripting form: create, print, no attach —
/// and a headless `horde new` without it refuses *before* creating anything.
#[tokio::test]
async fn horde_new_detach_creates_headlessly_and_plain_new_refuses_first() {
    let endpoint = serve().await.expect("a listening server");

    let refused = horde(&endpoint, &["new", "never-born"]).await.expect("run");
    assert!(!refused.success, "no terminal, no attach");
    assert!(
        refused.stderr.contains("--detach"),
        "the refusal names the scripting form: {:?}",
        refused.stderr
    );
    let still_empty = horde(&endpoint, &["session", "list"]).await.expect("run");
    assert_eq!(
        still_empty.stdout.trim(),
        "no sessions",
        "the refusal happened before anything was created"
    );

    let detached = horde(
        &endpoint,
        &[
            "--json",
            "new",
            "night watch",
            "--type",
            "reviewer",
            "--detach",
        ],
    )
    .await
    .expect("run");
    assert!(detached.success, "stderr was: {}", detached.stderr);
    let parsed: serde_json::Value = serde_json::from_str(detached.stdout.trim()).expect("JSON");
    assert_eq!(parsed["title"], "night watch");
    assert_eq!(parsed["name"], "night-watch", "the handle is the slug");
    assert!(
        parsed["root_agent_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "the root agent rides along, same as session new"
    );
}
