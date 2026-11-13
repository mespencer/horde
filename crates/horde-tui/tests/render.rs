//! Every screen rendered against fixed session fixtures (docs/TESTING.md §2).
//!
//! The snapshots under `tests/snapshots/` are the UI diff: a rendering change
//! shows up as a reviewable text diff of the actual 80×24 buffer, exactly what
//! an attached terminal would show. The fixtures walk the production fold —
//! `App::apply` over real envelopes — never hand-built view state, so a
//! snapshot can only be green if the fold and the renderer agree.

use horde_proto::v1::{
    AgentId, AgentOutputChannel, AgentOutputChunk, AgentResult, AgentSpawned, Assignment,
    AssignmentSent, Event, EventEnvelope, PermissionRequest, PermissionRequestId,
    PermissionRequested, ProposedToolCall, ResultReported, ResultStatus, SessionId, SessionSummary,
    event::Kind,
};
use horde_tui::view::render;
use horde_tui::{App, Update};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{Event as TermEvent, KeyCode, KeyEvent};

/// Draws the app once at 80×24 and hands back the buffer as text.
fn drawn(app: &App) -> String {
    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).expect("a test terminal always constructs");
    terminal
        .draw(|frame| render(app, frame))
        .expect("drawing on a TestBackend cannot fail");
    terminal.backend().to_string()
}

fn press(app: &mut App, code: KeyCode) {
    app.on_input(&TermEvent::Key(KeyEvent::from(code)));
}

fn summary(session_id: &str, title: &str, root: &str, head: u64) -> SessionSummary {
    SessionSummary {
        session_id: Some(SessionId::new(session_id)),
        title: title.to_owned(),
        root_agent_id: Some(AgentId::new(root)),
        state: horde_proto::v1::SessionLifecycleState::Active as i32,
        head_sequence: head,
        agent_count: 1,
        ..SessionSummary::default()
    }
}

fn event_in(session_id: &str, sequence: u64, agent: &str, kind: Kind) -> Update {
    Update::Event(Box::new(EventEnvelope {
        session_id: Some(SessionId::new(session_id)),
        sequence,
        recorded_at: None,
        event: Some(Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(kind),
        }),
    }))
}

fn output(text: &str) -> Kind {
    Kind::AgentOutputChunk(AgentOutputChunk {
        channel: AgentOutputChannel::Assistant as i32,
        text: text.to_owned(),
        turn_id: "t1".to_owned(),
        chunk_index: 0,
    })
}

fn spawn(parent: Option<&str>, tree_path: &[&str]) -> Kind {
    Kind::AgentSpawned(AgentSpawned {
        parent_agent_id: parent.map(AgentId::new),
        spawned_by: None,
        spec: None,
        tree_path: tree_path.iter().map(ToString::to_string).collect(),
    })
}

fn escalation(request_id: &str, rendered: &str, tree_path: &[&str]) -> Kind {
    Kind::PermissionRequested(PermissionRequested {
        request: Some(PermissionRequest {
            request_id: Some(PermissionRequestId::new(request_id)),
            agent_id: None,
            tool_call: Some(ProposedToolCall {
                tool_name: String::new(),
                input: None,
                rendered: rendered.to_owned(),
                questions: Vec::new(),
            }),
            tree_path: tree_path.iter().map(ToString::to_string).collect(),
            permission_mode: "cautious".to_owned(),
            escalated_from: 0,
            expires_at: None,
            forward: None,
        }),
    })
}

/// The caught-up marker, so fixture sessions do not all render "(syncing)".
fn caught_up(app: &mut App, session_id: &str, head: u64) {
    app.apply(Update::CaughtUp {
        session_id: session_id.to_owned(),
        head,
    });
}

/// A session with a small tree, some conversation, and one escalation from a
/// grandchild — the shared fixture the attach-view snapshots look at.
fn busy_session(app: &mut App) {
    app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1", 0)]));
    app.apply(event_in("s1", 1, "root-1", spawn(None, &["ci-fix"])));
    app.apply(event_in(
        "s1",
        2,
        "root-1",
        Kind::AssignmentSent(AssignmentSent {
            from: None,
            assignment: Some(Assignment {
                assignment_id: None,
                text: "make the tests green".to_owned(),
                payload: None,
            }),
            delivery: 0,
        }),
    ));
    app.apply(event_in(
        "s1",
        3,
        "root-1",
        output("Looking at the failures now."),
    ));
    app.apply(event_in(
        "s1",
        4,
        "reviewer-1",
        spawn(Some("root-1"), &["ci-fix", "reviewer"]),
    ));
    app.apply(event_in(
        "s1",
        5,
        "test-runner-1",
        spawn(Some("reviewer-1"), &["ci-fix", "reviewer", "test-runner"]),
    ));
    app.apply(event_in(
        "s1",
        6,
        "test-runner-1",
        escalation(
            "req-1",
            "docker rm horde-test",
            &["ci-fix", "reviewer", "test-runner"],
        ),
    ));
    app.apply(event_in(
        "s1",
        7,
        "reviewer-1",
        Kind::ResultReported(ResultReported {
            result: Some(AgentResult {
                assignment_id: None,
                status: ResultStatus::Success as i32,
                summary: "review posted".to_owned(),
                payload: None,
            }),
        }),
    ));
    caught_up(app, "s1", 7);
}

/// A second, quieter session beside the busy one, listed at `head`.
fn second_session(app: &mut App, head: u64) {
    app.apply(Update::Sessions(vec![
        summary("s1", "ci-fix", "root-1", 7),
        summary("s2", "write the docs", "root-2", head),
    ]));
}

#[test]
fn the_overview_with_nothing_says_so() {
    let app = App::overview();
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn the_overview_lists_sessions_and_counts_what_waits() {
    let mut app = App::overview();
    busy_session(&mut app);
    second_session(&mut app, 3);
    caught_up(&mut app, "s2", 3);
    press(&mut app, KeyCode::Down);
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_session_still_replaying_says_syncing() {
    let mut app = App::overview();
    app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1", 2)]));
    press(&mut app, KeyCode::Enter);
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn the_attached_session_shows_feed_prompt_and_compose_line() {
    let mut app = App::overview();
    busy_session(&mut app);
    press(&mut app, KeyCode::Enter);
    for c in "also check clippy".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn attaching_to_a_subagent_scopes_the_prompt_and_badges_the_rest() {
    let mut app = App::attached("s1", Some("reviewer-1".to_owned()));
    busy_session(&mut app);
    // A second escalation outside the reviewer's subtree: only the badge may
    // show it.
    app.apply(event_in(
        "s1",
        8,
        "root-1",
        escalation("req-2", "git push --force", &["ci-fix"]),
    ));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn scrolling_back_freezes_the_view_on_older_lines() {
    let mut app = App::overview();
    busy_session(&mut app);
    press(&mut app, KeyCode::Enter);
    for _ in 0..3 {
        press(&mut app, KeyCode::Up);
    }
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn the_inbox_lists_escalations_across_sessions() {
    let mut app = App::overview();
    busy_session(&mut app);
    second_session(&mut app, 1);
    app.apply(event_in(
        "s2",
        1,
        "root-2",
        escalation("req-9", "rm -rf ./stale-docs", &["write the docs"]),
    ));
    caught_up(&mut app, "s2", 1);
    press(&mut app, KeyCode::Char('i'));
    press(&mut app, KeyCode::Down);
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn an_empty_inbox_says_nothing_needs_you() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('i'));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_refusal_shows_in_the_header_without_costing_the_screen() {
    let mut app = App::overview();
    busy_session(&mut app);
    press(&mut app, KeyCode::Enter);
    app.report_error("no permission request req-1 is awaiting a decision");
    insta::assert_snapshot!(drawn(&app));
}

/// The server closing the follow stream says so on the attached view, with
/// the reason when there is one — a frozen feed must not pass for a quiet one.
#[test]
fn a_closed_stream_says_it_ended_and_why() {
    let mut app = App::overview();
    busy_session(&mut app);
    press(&mut app, KeyCode::Enter);
    app.apply(Update::StreamEnded {
        reason: "server unavailable".to_owned(),
    });
    insta::assert_snapshot!(drawn(&app));
}

/// A clean close ends the stream with no reason, and the ended badge
/// outranks the stale one: once the server has hung up, "no heartbeat" is a
/// guess about something already known.
#[test]
fn a_cleanly_closed_stream_outranks_the_stale_badge() {
    let mut app = App::overview();
    busy_session(&mut app);
    app.apply(Update::Tick {
        now: std::time::Instant::now() + horde_tui::app::STALE_AFTER * 2,
    });
    assert!(
        app.stream_stale(),
        "the fixture must be stale to be outranked"
    );
    app.apply(Update::StreamEnded {
        reason: String::new(),
    });
    insta::assert_snapshot!(drawn(&app));
}

/// One forward of each shape (ADR-0006): a reachable expose, a backed
/// inject, and an unbacked inject whose row names the detached backer —
/// because the unbacked state's remedy is that principal's return.
#[test]
fn the_forward_panel_shows_names_reachability_and_backing() {
    use horde_proto::v1::{
        ClientId, ForwardDirection, ForwardId, ForwardIntent, ForwardSpec, ForwardStatus,
    };

    let forward = |name: &str, direction: ForwardDirection, backer: Option<&str>| ForwardIntent {
        forward_id: Some(ForwardId::new(format!("fwd-{name}"))),
        session_id: Some(SessionId::new("s1")),
        agent_id: Some(AgentId::new("root-1")),
        spec: Some(ForwardSpec {
            direction: direction as i32,
            name: name.to_owned(),
            worker_port: match direction {
                ForwardDirection::Expose => 5173,
                _ => 15432,
            },
            client_port: 5432,
        }),
        backer_client_id: backer.map(ClientId::new),
    };

    let mut app = App::overview();
    busy_session(&mut app);
    press(&mut app, KeyCode::Char('f'));
    app.apply(Update::Forwards(vec![
        ForwardStatus {
            intent: Some(forward("vite", ForwardDirection::Expose, None)),
            worker_reachable: true,
            backed: false,
        },
        ForwardStatus {
            intent: Some(forward("db", ForwardDirection::Inject, Some("laptop"))),
            worker_reachable: true,
            backed: true,
        },
        ForwardStatus {
            intent: Some(forward("cache", ForwardDirection::Inject, Some("desk"))),
            worker_reachable: false,
            backed: false,
        },
    ]));
    press(&mut app, KeyCode::Down);
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn an_empty_forward_panel_says_so() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('f'));
    insta::assert_snapshot!(drawn(&app));
}
