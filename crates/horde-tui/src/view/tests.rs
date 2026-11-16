//! Every screen rendered against fixed fixtures (docs/TESTING.md §2).
//!
//! The snapshots under `snapshots/` are the UI diff: a rendering change
//! shows up as a reviewable text diff of the actual 80×24 buffer, exactly
//! what an attached terminal would show. The fixtures walk the production
//! fold — `App::apply` over real envelopes — and the keys go through
//! `App::on_input`, never hand-built view state, so a snapshot can only be
//! green if the fold, the keys and the renderer agree.

use horde_proto::v1::{
    AgentId, AgentOutputChannel, ForwardDirection, ForwardId, ForwardIntent, ForwardSpec,
    ForwardStatus, PlacementState, RepoInstanceReport, SessionId, SessionSummary, TokenUsage,
    UsageAggregate, UsageGroupKey, WorkerId, WorkerSummary, WorkspaceDefinition,
    WorkspaceInstanceReport, WorkspaceInstanceState, WorkspaceRepo, event::Kind,
};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{Event as TermEvent, KeyCode, KeyEvent, KeyModifiers};

use super::render;
use crate::app::{App, Update, UsageReport, WorkspaceEntry};
use crate::fixtures::{
    asked, assignment, bound, chunk, envelope_in, placement, requested, spawned,
};

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

fn ctrl(app: &mut App, c: char) {
    app.on_input(&TermEvent::Key(KeyEvent::new(
        KeyCode::Char(c),
        KeyModifiers::CONTROL,
    )));
}

fn typed(app: &mut App, text: &str) {
    for c in text.chars() {
        press(app, KeyCode::Char(c));
    }
}

fn summary(session_id: &str, title: &str, root: &str) -> SessionSummary {
    SessionSummary {
        session_id: Some(SessionId::new(session_id)),
        title: title.to_owned(),
        root_agent_id: Some(AgentId::new(root)),
        state: horde_proto::v1::SessionLifecycleState::Active as i32,
        ..SessionSummary::default()
    }
}

/// Folds `kinds` into a session, each about `agent`, at the next positions.
fn fold(app: &mut App, session: &str, agent: &str, kinds: Vec<Kind>) {
    for kind in kinds {
        let next = app.session(session).map_or(0, |state| state.stream_head) + 1;
        app.apply(Update::Event(Box::new(envelope_in(
            session, next, agent, kind,
        ))));
    }
}

fn caught_up(app: &mut App, session_id: &str) {
    app.apply(Update::CaughtUp {
        session_id: session_id.to_owned(),
        head: 0,
    });
}

fn workers(app: &mut App) {
    app.apply(Update::Workers(vec![WorkerSummary {
        worker_id: Some(WorkerId::new("worker-1")),
        name: "gpu-box".to_owned(),
        ..WorkerSummary::default()
    }]));
}

/// `ci-fix`: a root on gpu-box running claude/opus that took an assignment,
/// read a file, thought, and replied; a reviewer child it spawned.
fn conversation(app: &mut App) {
    app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
    workers(app);
    fold(
        app,
        "s1",
        "root-1",
        vec![
            spawned(None, &["ci-fix"]),
            placement(PlacementState::Placed, Some("worker-1")),
            assignment("make the tests green"),
            chunk(
                AgentOutputChannel::Reasoning,
                "t1",
                "The failure is probably in the parser.",
            ),
            chunk(
                AgentOutputChannel::ToolCall,
                "t1",
                r#"Bash {"command":"cargo test -p parser"}"#,
            ),
            chunk(
                AgentOutputChannel::ToolResult,
                "t1",
                "running 12 tests\ntest a ... ok\ntest b ... FAILED\ntest c ... ok\nfailures: b",
            ),
            chunk(
                AgentOutputChannel::Assistant,
                "t1",
                "One test fails: `b` expects a trailing ",
            ),
            chunk(
                AgentOutputChannel::Assistant,
                "t1",
                "newline. I'll ask a reviewer to check the fix.",
            ),
        ],
    );
    fold(
        app,
        "s1",
        "kid",
        vec![spawned(Some("root-1"), &["ci-fix", "reviewer"])],
    );
    fold(app, "s1", "root-1", vec![bound("t1", "claude", "opus")]);
    caught_up(app, "s1");
}

fn attached_conversation() -> App {
    let mut app = App::attached("s1", None);
    conversation(&mut app);
    app.apply(Update::Heartbeat);
    app
}

#[test]
fn the_session_shows_the_transcript_compose_box_and_status() {
    let mut app = attached_conversation();
    typed(&mut app, "looks good, go ahead");
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn ctrl_o_expands_tool_input_and_output() {
    let mut app = attached_conversation();
    ctrl(&mut app, 'o');
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_working_agent_shows_the_spinner_line_and_queue_hints() {
    let mut app = attached_conversation();
    fold(
        &mut app,
        "s1",
        "root-1",
        vec![
            assignment("also fix the lint"),
            chunk(
                AgentOutputChannel::ToolCall,
                "t2",
                r#"Read {"file_path":"src/lib.rs"}"#,
            ),
        ],
    );
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_permission_waits_inline_with_its_choices() {
    let mut app = attached_conversation();
    fold(
        &mut app,
        "s1",
        "kid",
        vec![requested("req-1", "git push --force origin main")],
    );
    press(&mut app, KeyCode::Down);
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_multi_question_request_is_answered_in_place() {
    let mut app = attached_conversation();
    fold(
        &mut app,
        "s1",
        "root-1",
        vec![asked(
            "q-1",
            &[
                ("Which branch?", &["main", "release"]),
                ("Run the slow tests?", &["yes", "no"]),
            ],
        )],
    );
    typed(&mut app, "2");
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn an_answered_prompt_says_it_is_waiting_for_the_server() {
    let mut app = attached_conversation();
    fold(
        &mut app,
        "s1",
        "root-1",
        vec![requested("req-1", "rm -rf target")],
    );
    typed(&mut app, "y");
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn slash_opens_the_command_menu() {
    let mut app = attached_conversation();
    typed(&mut app, "/a");
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_session_still_replaying_says_syncing() {
    let mut app = App::attached("s1", None);
    app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_new_session_invites_the_first_message() {
    let mut app = App::attached("s1", None);
    app.apply(Update::Sessions(vec![SessionSummary {
        root_agent_id: None,
        ..summary("s1", "brisk-warband", "")
    }]));
    caught_up(&mut app, "s1");
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_subagent_attach_scopes_prompts_and_badges_the_rest() {
    let mut app = App::attached("s1", Some("kid".to_owned()));
    conversation(&mut app);
    fold(
        &mut app,
        "s1",
        "kid",
        vec![chunk(
            AgentOutputChannel::Assistant,
            "k1",
            "Reviewing the diff.",
        )],
    );
    fold(&mut app, "s1", "root-1", vec![requested("req-root", "ls")]);
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn scrolling_back_shows_older_rows() {
    let mut app = attached_conversation();
    press(&mut app, KeyCode::PageUp);
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_closed_stream_says_it_ended_and_why() {
    let mut app = attached_conversation();
    app.apply(Update::StreamEnded {
        reason: "server unavailable".to_owned(),
    });
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_stale_stream_is_badged() {
    let mut app = attached_conversation();
    app.apply(Update::Tick {
        now: std::time::Instant::now() + crate::app::STALE_AFTER * 2,
    });
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn a_refusal_shows_in_the_hint_line() {
    let mut app = attached_conversation();
    app.report_error("permission denied: you are not the session owner");
    insta::assert_snapshot!(drawn(&app));
}

/// The conversation plus a second session, `docs`, whose root agent was
/// spawned and then went through `extra`.
fn with_docs_session(app: &mut App, extra: Vec<Kind>) {
    conversation(app);
    app.apply(Update::Sessions(vec![summary("s2", "docs", "root-2")]));
    let mut kinds = vec![spawned(None, &["docs"])];
    kinds.extend(extra);
    fold(app, "s2", "root-2", kinds);
}

/// Two sessions, one with a lost agent and one waiting prompt.
fn tree() -> App {
    let mut app = App::overview();
    with_docs_session(
        &mut app,
        vec![placement(PlacementState::Lost, Some("worker-9"))],
    );
    fold(
        &mut app,
        "s1",
        "kid",
        vec![
            placement(PlacementState::Placed, Some("worker-1")),
            requested("req-1", "git push"),
        ],
    );
    app
}

#[test]
fn the_tree_shows_sessions_agents_states_and_workers() {
    let mut app = tree();
    press(&mut app, KeyCode::Down);
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn an_empty_tree_says_how_to_start() {
    insta::assert_snapshot!(drawn(&App::overview()));
}

#[test]
fn archive_asks_in_the_hint_line() {
    let mut app = tree();
    press(&mut app, KeyCode::Char('x'));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn the_inbox_lists_prompts_with_the_selected_card() {
    let mut app = tree();
    fold(
        &mut app,
        "s2",
        "root-2",
        vec![asked("q-9", &[("Publish the docs?", &["now", "later"])])],
    );
    press(&mut app, KeyCode::Char('i'));
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Enter);
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn an_empty_inbox_says_nothing_needs_you() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('i'));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn workspaces_show_definitions_instances_and_checkouts() {
    let mut app = App::overview();
    workers(&mut app);
    let repo = |name: &str, branch: &str| WorkspaceRepo {
        name: name.to_owned(),
        default_branch: branch.to_owned(),
        ..WorkspaceRepo::default()
    };
    app.apply(Update::Workspaces(vec![
        WorkspaceEntry {
            definition: WorkspaceDefinition {
                name: "horde".to_owned(),
                repos: vec![repo("horde", "main"), repo("atlas", "")],
                ..WorkspaceDefinition::default()
            },
            instances: vec![
                WorkspaceInstanceReport {
                    worker_id: Some(WorkerId::new("worker-1")),
                    state: WorkspaceInstanceState::Ready as i32,
                    root_path: "/srv/horde/ws/horde".to_owned(),
                    repos: vec![RepoInstanceReport {
                        repo_name: "horde".to_owned(),
                        path: "/srv/horde/ws/horde/horde".to_owned(),
                        head_commit_sha: "2df0ef0123456789".to_owned(),
                        dirty: true,
                    }],
                    ..WorkspaceInstanceReport::default()
                },
                WorkspaceInstanceReport {
                    worker_id: None,
                    state: WorkspaceInstanceState::Degraded as i32,
                    detail: "atlas failed to fetch".to_owned(),
                    ..WorkspaceInstanceReport::default()
                },
            ],
        },
        WorkspaceEntry {
            definition: WorkspaceDefinition {
                name: "scratch".to_owned(),
                ..WorkspaceDefinition::default()
            },
            instances: Vec::new(),
        },
    ]));
    press(&mut app, KeyCode::Char('w'));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn workspaces_loading_and_empty_say_which() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('w'));
    let loading = drawn(&app);
    app.apply(Update::Workspaces(Vec::new()));
    insta::assert_snapshot!(format!("{loading}\n{}", drawn(&app)));
}

#[test]
fn usage_shows_agents_trees_and_accounts() {
    let mut app = App::attached("s1", None);
    conversation(&mut app);
    let tokens = |input, output| {
        Some(TokenUsage {
            input_tokens: input,
            output_tokens: output,
            cache_read_input_tokens: 1_000,
            ..TokenUsage::default()
        })
    };
    app.apply(Update::Usage(UsageReport {
        scope: "session ci-fix".to_owned(),
        by_agent: vec![UsageAggregate {
            key: Some(UsageGroupKey {
                agent_id: Some(AgentId::new("agent-root-1")),
                ..UsageGroupKey::default()
            }),
            tokens: tokens(12_345, 678),
            agent_count: 1,
        }],
        by_tree: vec![UsageAggregate {
            key: Some(UsageGroupKey {
                tree_path: vec!["ci-fix".to_owned(), "reviewer".to_owned()],
                ..UsageGroupKey::default()
            }),
            tokens: tokens(5, 6),
            agent_count: 1,
        }],
        accounts: vec![horde_proto::v1::AccountHeadroom {
            provider: "claude".to_owned(),
            account: "work".to_owned(),
            tokens: tokens(12_350, 684),
            remaining_fraction: Some(0.42),
            exhausted: true,
            ..horde_proto::v1::AccountHeadroom::default()
        }],
    }));
    ctrl(&mut app, 't');
    press(&mut app, KeyCode::Char('u'));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn usage_loading_and_empty_say_which() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('u'));
    let loading = drawn(&app);
    app.apply(Update::Usage(UsageReport {
        scope: "every session".to_owned(),
        ..UsageReport::default()
    }));
    insta::assert_snapshot!(format!("{loading}\n{}", drawn(&app)));
}

#[test]
fn the_logs_pane_is_a_labelled_placeholder() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('l'));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn help_lists_keys_from_the_binding_tables() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('?'));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn the_forward_panel_shows_names_reachability_and_backing() {
    let mut app = App::overview();
    conversation(&mut app);
    let forward =
        |name: &str, direction: ForwardDirection, port, backer: Option<&str>, backed, reachable| {
            ForwardStatus {
                intent: Some(ForwardIntent {
                    forward_id: Some(ForwardId::new(format!("fwd-{name}"))),
                    session_id: Some(SessionId::new("s1")),
                    spec: Some(ForwardSpec {
                        name: name.to_owned(),
                        direction: direction as i32,
                        worker_port: port,
                        ..ForwardSpec::default()
                    }),
                    backer_client_id: backer.map(horde_proto::v1::ClientId::new),
                    ..ForwardIntent::default()
                }),
                worker_reachable: reachable,
                backed,
            }
        };
    app.apply(Update::Forwards(vec![
        forward("vite", ForwardDirection::Expose, 5173, None, false, true),
        forward(
            "db",
            ForwardDirection::Inject,
            5432,
            Some("laptop"),
            true,
            true,
        ),
        forward(
            "redis",
            ForwardDirection::Inject,
            6379,
            Some("laptop"),
            false,
            false,
        ),
    ]));
    press(&mut app, KeyCode::Char('f'));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn an_empty_forward_panel_says_so() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('f'));
    insta::assert_snapshot!(drawn(&app));
}

/// The first question of a question request, to adjust before folding it.
fn first_question(request: &mut Kind) -> Option<&mut horde_proto::v1::Question> {
    let Kind::PermissionRequested(requested) = request else {
        return None;
    };
    requested
        .request
        .as_mut()
        .and_then(|request| request.tool_call.as_mut())
        .and_then(|call| call.questions.first_mut())
}

#[test]
fn a_secret_free_text_answer_is_masked() {
    let mut app = attached_conversation();
    let mut request = asked("q-s", &[("Token?", &[])]);
    if let Some(question) = first_question(&mut request) {
        question.secret = true;
        question.header = "Credentials".to_owned();
    }
    fold(&mut app, "s1", "root-1", vec![request]);
    typed(&mut app, "hunter2");
    insta::assert_snapshot!(drawn(&app));
}

/// The masked case's twin: an ordinary free-text answer shows as typed.
#[test]
fn an_ordinary_free_text_answer_shows_as_typed() {
    let mut app = attached_conversation();
    fold(
        &mut app,
        "s1",
        "root-1",
        vec![asked("q-o", &[("Branch", &[])])],
    );
    typed(&mut app, "hunter2");
    let screen = drawn(&app);
    assert!(screen.contains("Answer: hunter2"), "{screen}");
}

/// A permission card names the tool when the request carries one, and
/// shows a mode line only for a request that has a mode.
#[test]
fn a_permission_card_names_its_tool_and_omits_an_empty_mode() {
    let mut app = attached_conversation();
    let mut request = requested("req-t", "cargo test");
    if let Kind::PermissionRequested(requested) = &mut request
        && let Some(inner) = requested.request.as_mut()
    {
        inner.permission_mode = String::new();
        if let Some(call) = inner.tool_call.as_mut() {
            call.tool_name = "Bash".to_owned();
        }
    }
    fold(&mut app, "s1", "root-1", vec![request]);
    let screen = drawn(&app);
    assert!(screen.contains("Bash wants to run:"), "{screen}");
    assert!(
        !screen
            .lines()
            .any(|line| line.trim_matches(|c: char| !c.is_alphanumeric()) == "mode"),
        "{screen}"
    );
}

#[test]
fn a_multi_select_question_shows_checkboxes() {
    let mut app = attached_conversation();
    let mut request = asked("q-m", &[("Which checks?", &["fmt", "clippy", "tests"])]);
    if let Some(question) = first_question(&mut request) {
        question.multi_select = true;
        question.allows_free_text = true;
        if let Some(option) = question.options.first_mut() {
            option.description = "rustfmt, no config".to_owned();
        }
    }
    fold(&mut app, "s1", "root-1", vec![request]);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char(' '));
    insta::assert_snapshot!(drawn(&app));
}

#[test]
fn an_unfocused_card_steps_aside_for_the_compose_box() {
    let mut app = attached_conversation();
    fold(&mut app, "s1", "root-1", vec![requested("req-1", "rm x")]);
    press(&mut app, KeyCode::Tab);
    typed(&mut app, "wait, why?");
    insta::assert_snapshot!(drawn(&app));
}

/// Draws the app once at `width`×`height`, handing back the terminal so a
/// test can read the cursor or the cells' styles.
fn terminal_at(app: &App, width: u16, height: u16) -> Terminal<TestBackend> {
    let mut terminal =
        Terminal::new(TestBackend::new(width, height)).expect("a test terminal always constructs");
    terminal
        .draw(|frame| render(app, frame))
        .expect("drawing on a TestBackend cannot fail");
    terminal
}

/// The buffer as text with each change of style marked inline as
/// `‹colour+modifiers›` — what a plain snapshot cannot see, since the
/// palette spends colour on state. The markers describe the cells; they
/// encode nothing about which style the renderer was meant to pick.
fn styled_at(app: &App, width: u16, height: u16) -> String {
    let terminal = terminal_at(app, width, height);
    let buffer = terminal.backend().buffer();
    let mut out = String::new();
    for y in 0..height {
        out.push_str(styled_row(buffer, width, y).trim_end());
        out.push('\n');
    }
    out
}

/// One row of [`styled_at`], a marker opening each run of one style.
fn styled_row(buffer: &ratatui::buffer::Buffer, width: u16, y: u16) -> String {
    let mut current = String::new();
    let mut row = String::new();
    for x in 0..width {
        let cell = &buffer[(x, y)];
        let tag = style_tag(cell);
        if tag != current {
            row.push_str(&format!("‹{tag}›"));
            current = tag;
        }
        row.push_str(cell.symbol());
    }
    row
}

/// A cell's foreground colour and the modifiers the palette uses.
fn style_tag(cell: &ratatui::buffer::Cell) -> String {
    use ratatui::style::Modifier;
    let mut tag = format!("{:?}", cell.fg);
    for (flag, name) in [(Modifier::BOLD, "+b"), (Modifier::ITALIC, "+i")] {
        if cell.modifier.contains(flag) {
            tag.push_str(name);
        }
    }
    tag
}

fn alt_enter(app: &mut App) {
    app.on_input(&TermEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::ALT,
    )));
}

fn cursor_after(app: &App) -> (u16, u16) {
    let mut terminal = terminal_at(app, 80, 24);
    let position = terminal
        .get_cursor_position()
        .expect("a TestBackend always reports its cursor");
    (position.x, position.y)
}

/// The terminal cursor follows the compose cursor: three cells in (border,
/// prompt mark, space), one row under the box's top border, across hard
/// wraps at the box's inner width (76 at 80 columns), and pinned to the
/// last of the six rows the box grows to.
#[test]
fn the_terminal_cursor_follows_the_compose_cursor() {
    let mut app = attached_conversation();
    assert_eq!(cursor_after(&app), (3, 20), "an empty box");
    typed(&mut app, "abc");
    assert_eq!(cursor_after(&app), (6, 20), "after three characters");

    let mut app = attached_conversation();
    typed(&mut app, "ab");
    alt_enter(&mut app);
    typed(&mut app, "cd");
    assert_eq!(cursor_after(&app), (5, 20), "second line of a two-row box");
    press(&mut app, KeyCode::Up);
    assert_eq!(cursor_after(&app), (5, 19), "up a line");

    let mut app = attached_conversation();
    typed(&mut app, &"x".repeat(80));
    assert_eq!(cursor_after(&app), (7, 20), "wrapped onto the second row");
    alt_enter(&mut app);
    typed(&mut app, "y");
    assert_eq!(
        cursor_after(&app),
        (4, 20),
        "below a wrapped line, which counts two rows"
    );

    let mut app = attached_conversation();
    for _ in 0..7 {
        typed(&mut app, "a");
        alt_enter(&mut app);
    }
    typed(&mut app, "b");
    assert_eq!(
        cursor_after(&app),
        (4, 20),
        "the box stops growing at six rows"
    );
}

/// Each state wears its colour: LOST and failed red, working and queued
/// yellow, running and idle green, the rest dim; the current tab and the
/// selected row highlighted, the waiting count magenta.
#[test]
fn the_tree_colours_states_and_selection() {
    use horde_proto::v1::{AgentLifecycleChanged, AgentLifecycleState};
    let lifecycle = |state: AgentLifecycleState| {
        Kind::AgentLifecycleChanged(AgentLifecycleChanged {
            previous_state: 0,
            state: state as i32,
            reason: String::new(),
        })
    };
    let mut app = tree();
    app.apply(Update::Sessions(vec![summary("s3", "states", "a-run")]));
    let agents: [(&str, Vec<Kind>); 5] = [
        ("a-run", vec![lifecycle(AgentLifecycleState::Running)]),
        ("a-idle", vec![lifecycle(AgentLifecycleState::Idle)]),
        (
            "a-busy",
            vec![chunk(AgentOutputChannel::Assistant, "t", "hi")],
        ),
        ("a-wait", vec![placement(PlacementState::Queued, None)]),
        ("a-dead", vec![lifecycle(AgentLifecycleState::Failed)]),
    ];
    for (agent, kinds) in agents {
        let parent = (agent != "a-run").then_some("a-run");
        fold(
            &mut app,
            "s3",
            agent,
            vec![spawned(parent, &["states", agent])],
        );
        fold(&mut app, "s3", agent, kinds);
    }
    insta::assert_snapshot!(styled_at(&app, 80, 16));
}

/// The transcript's colours: thinking dim italic, a child's spawn cyan,
/// success green and any other result yellow, an interrupt yellow, a
/// failure red, an archive dim.
#[test]
fn the_transcript_colours_its_blocks() {
    use horde_proto::v1::{
        AgentLifecycleChanged, AgentLifecycleState, AgentResult, ResultReported, ResultStatus,
        TurnInterrupted,
    };
    let reported = |status: ResultStatus, summary: &str| {
        Kind::ResultReported(ResultReported {
            result: Some(AgentResult {
                status: status as i32,
                summary: summary.to_owned(),
                ..AgentResult::default()
            }),
        })
    };
    let lifecycle = |state: AgentLifecycleState| {
        Kind::AgentLifecycleChanged(AgentLifecycleChanged {
            previous_state: 0,
            state: state as i32,
            reason: String::new(),
        })
    };
    let mut app = attached_conversation();
    fold(
        &mut app,
        "s1",
        "root-1",
        vec![
            Kind::TurnInterrupted(TurnInterrupted {
                reason: "stop".to_owned(),
                ..TurnInterrupted::default()
            }),
            reported(ResultStatus::Success, "all green"),
            reported(ResultStatus::Blocked, "still red"),
            lifecycle(AgentLifecycleState::Failed),
            lifecycle(AgentLifecycleState::Archived),
        ],
    );
    insta::assert_snapshot!(styled_at(&app, 80, 36));
}

/// The command menu highlights the row enter would run and leaves the rest
/// plain.
#[test]
fn the_command_menu_highlights_one_row() {
    let mut app = attached_conversation();
    typed(&mut app, "/a");
    press(&mut app, KeyCode::Down);
    insta::assert_snapshot!(styled_at(&app, 80, 24));
}

/// Instance states by name and colour: ready green, degraded and
/// materializing yellow, missing red, anything unknown dim.
#[test]
fn workspace_instances_show_every_state() {
    let mut app = App::overview();
    workers(&mut app);
    let instance = |state: WorkspaceInstanceState, path: &str| WorkspaceInstanceReport {
        worker_id: Some(WorkerId::new("worker-1")),
        state: state as i32,
        root_path: path.to_owned(),
        ..WorkspaceInstanceReport::default()
    };
    app.apply(Update::Workspaces(vec![WorkspaceEntry {
        definition: WorkspaceDefinition {
            name: "horde".to_owned(),
            ..WorkspaceDefinition::default()
        },
        instances: vec![
            instance(WorkspaceInstanceState::Ready, "/a"),
            instance(WorkspaceInstanceState::Degraded, "/b"),
            instance(WorkspaceInstanceState::Materializing, "/c"),
            instance(WorkspaceInstanceState::Missing, "/d"),
            instance(WorkspaceInstanceState::Unspecified, "/e"),
        ],
    }]));
    press(&mut app, KeyCode::Char('w'));
    insta::assert_snapshot!(styled_at(&app, 80, 12));
}

/// The whole help screen, every table, on a screen tall enough to hold it.
#[test]
fn help_lists_every_binding_on_a_tall_screen() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('?'));
    insta::assert_snapshot!(terminal_at(&app, 80, 120).backend().to_string());
}

/// On a narrow screen agent names take a third of the width.
#[test]
fn the_tree_fits_names_to_a_narrow_screen() {
    let app = tree();
    insta::assert_snapshot!(terminal_at(&app, 60, 12).backend().to_string());
}

/// When the inbox shrinks under its selection, the last entry takes the
/// highlight and the card.
#[test]
fn a_shrinking_inbox_keeps_a_selected_entry() {
    let mut app = tree();
    fold(
        &mut app,
        "s2",
        "root-2",
        vec![requested("req-2", "rm -rf docs")],
    );
    press(&mut app, KeyCode::Char('i'));
    press(&mut app, KeyCode::Down);
    assert_eq!(app.selected(crate::keys::Screen::Inbox), 1);
    fold(
        &mut app,
        "s2",
        "root-2",
        vec![crate::fixtures::decided(
            "req-2",
            horde_proto::v1::PermissionOutcome::Allow,
            "",
        )],
    );
    let screen = terminal_at(&app, 80, 24).backend().to_string();
    assert!(
        screen.contains("❯ ci-fix"),
        "the remaining entry is selected: {screen}"
    );
}

/// The tree marks the attached session, and only that one.
#[test]
fn the_tree_marks_the_attached_session() {
    let mut app = App::attached("s2", None);
    with_docs_session(&mut app, Vec::new());
    ctrl(&mut app, 't');
    let screen = drawn(&app);
    let marked: Vec<&str> = screen
        .lines()
        .filter(|line| line.contains("(attached)"))
        .collect();
    assert_eq!(marked.len(), 1, "one session is attached: {screen}");
    assert!(marked[0].contains("docs"), "and it is docs: {screen}");
}

/// The front prompt's card as text, at `width` columns.
fn card_at(app: &App, focused: bool, width: u16) -> Vec<String> {
    let Some(request) = app.front_prompt() else {
        return Vec::new();
    };
    super::card::lines(app, &request, &app.cursor_for(&request), focused, width)
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect()
        })
        .collect()
}

fn rows(texts: &[&str]) -> Vec<String> {
    texts.iter().map(|text| (*text).to_owned()).collect()
}

/// At 30 columns a card's text has 26 cells: the command 24 behind its
/// indent, the title 25 before its gap. Each text here is one cell too long
/// for its place, so it breaks or clips exactly there; and a permission's
/// long choice is clipped instead of breaking the border.
#[test]
fn a_permission_card_breaks_and_clips_at_its_inner_width() {
    let mut app = attached_conversation();
    fold(
        &mut app,
        "s1",
        "root-1",
        vec![requested("req-w", &"r".repeat(25))],
    );
    assert_eq!(
        card_at(&app, true, 30),
        rows(&[
            "╭─ Permission · ci-fix > re… ╮",
            "│ The agent wants to run:    │",
            "│   rrrrrrrrrrrrrrrrrrrrrrrr │",
            "│   r                        │",
            "│ mode manual                │",
            "│ ❯ 1. Yes                   │",
            "│   2. Yes, and don't ask a… │",
            "│   3. No                    │",
            "╰─ ↑↓ enter · y yes · n no … ╯",
        ])
    );
}

/// The question's twin: the question breaks at 26 cells, an option's label
/// clips at 20 (room for a checkbox), the free-text row at 24.
#[test]
fn a_question_card_breaks_and_clips_at_its_inner_width() {
    let mut app = attached_conversation();
    let mut request = asked("q-w", &[(&"q".repeat(26), &[&"a".repeat(18), "b"])]);
    if let Some(question) = first_question(&mut request) {
        question.allows_free_text = true;
    }
    fold(&mut app, "s1", "root-1", vec![request]);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Down);
    typed(&mut app, &"t".repeat(18));
    assert_eq!(
        card_at(&app, true, 30),
        rows(&[
            "╭─ Question 1/1 · root-1 ────╮",
            "│ qqqqqqqqqqqqqqqqqqqqqqqqqq │",
            "│ ?                          │",
            "│   1. aaaaaaaaaaaaaaaa…     │",
            "│   2. b                     │",
            "│ ❯ Other: tttttttttttttttt… │",
            "╰─ ↑↓ enter · space toggle … ╯",
        ])
    );
}

/// Below twelve columns a card keeps eight cells of text, as at twelve.
#[test]
fn a_tiny_terminal_keeps_eight_cells_of_card() {
    let mut app = attached_conversation();
    fold(&mut app, "s1", "root-1", vec![requested("req-1", "rm x")]);
    let at_twelve = card_at(&app, true, 12);
    assert_eq!(at_twelve[0], "╭─ Permis… ╮");
    for width in [0, 4, 11] {
        assert_eq!(card_at(&app, true, width), at_twelve, "at {width} columns");
    }
}

/// A focused card's border is magenta; one that stepped aside is dim. The
/// highlighted choice is cyan, the rest plain.
#[test]
fn a_card_colours_its_border_by_focus_and_its_highlighted_choice() {
    use crate::theme;
    let mut app = attached_conversation();
    fold(&mut app, "s1", "root-1", vec![requested("req-1", "rm x")]);
    let Some(request) = app.front_prompt() else {
        panic!("the request is waiting");
    };
    let cursor = app.cursor_for(&request);
    let styles = |focused: bool| {
        super::card::lines(&app, &request, &cursor, focused, 40)
            .iter()
            .map(|line| line.spans.iter().map(|span| span.style).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    let focused = styles(true);
    assert_eq!(focused[0][0], theme::needs_you());
    assert_eq!(styles(false)[0][0], theme::dim());
    // Rows 4 and 5 are the first two choices: border, marker, label.
    assert_eq!(focused[4][2], theme::selected(), "the highlighted choice");
    assert_eq!(focused[5][2], theme::PLAIN, "the next choice");
}

/// The screen's rows at `width`×`height`, trailing blanks trimmed.
fn rows_at(app: &App, width: u16, height: u16) -> Vec<String> {
    let terminal = terminal_at(app, width, height);
    let buffer = terminal.backend().buffer();
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .to_owned()
        })
        .collect()
}

/// The compose box breaks its text at its inner width (76 cells at 80
/// columns, two of them the gutter's); continuation rows and later lines
/// sit behind a blank gutter.
#[test]
fn the_compose_box_breaks_at_its_inner_width() {
    let mut app = attached_conversation();
    typed(&mut app, &"x".repeat(77));
    alt_enter(&mut app);
    typed(&mut app, "y");
    let screen = rows_at(&app, 80, 24);
    let boxed = |text: &str| format!("│{text:<78}│");
    assert_eq!(
        screen[17..22],
        [
            format!("╭{}╮", "─".repeat(78)),
            boxed(&format!("› {}", "x".repeat(76))),
            boxed("  x"),
            boxed("  y"),
            format!("╰{}╯", "─".repeat(78)),
        ],
        "{screen:#?}"
    );
}

/// On a five-column terminal the box has one cell of text per row.
#[test]
fn a_tiny_compose_box_holds_one_character_a_row() {
    let mut app = attached_conversation();
    typed(&mut app, "abc");
    let screen = rows_at(&app, 5, 12);
    assert_eq!(screen[5..10], ["╭───╮", "│› a│", "│  b│", "│  c│", "╰───╯"]);
    assert_eq!(
        cursor_at(&app, 4, 12),
        (3, 9),
        "with no room for text, the cursor steps down a row a character"
    );
}

/// A transcript row breaks exactly at the terminal's width.
#[test]
fn the_transcript_breaks_at_the_terminal_width() {
    let mut app = attached_conversation();
    fold(
        &mut app,
        "s1",
        "root-1",
        vec![chunk(AgentOutputChannel::Assistant, "t9", &"z".repeat(79))],
    );
    let screen = rows_at(&app, 80, 24);
    assert_eq!(
        screen[16..18],
        [format!("● {}", "z".repeat(78)), "  z".to_owned()]
    );
}

/// A short terminal gives the bottom rows first: six rows hold the status,
/// the hint, the three-row compose box and one row of transcript; three
/// hold the status, the hint and the compose box's top border.
#[test]
fn a_short_terminal_keeps_the_bottom_rows_first() {
    let app = attached_conversation();
    assert_eq!(
        rows_at(&app, 30, 6),
        [
            "↳ spawned reviewer",
            "╭────────────────────────────╮",
            "│› message the agent, or / fo│",
            "╰────────────────────────────╯",
            " enter send · alt-enter newlin",
            " ci-fix · ci-fix · claude · op",
        ]
    );
    assert_eq!(
        rows_at(&app, 30, 3),
        [
            "╭────────────────────────────╮",
            " enter send · alt-enter newlin",
            " ci-fix · ci-fix · claude · op",
        ]
    );
}

/// Rows are granted bottom-up, then top-down, the body taking the rest,
/// each part placed under the one before it.
#[test]
fn split_grants_the_fixed_rows_and_the_body_the_rest() {
    use ratatui::layout::Rect;
    let rect = |y, height| Rect::new(2, y, 10, height);
    let at = |height| super::split(rect(3, height), &[1, 2], &[2, 1]);
    assert_eq!(
        at(10),
        (
            vec![rect(3, 1), rect(4, 2)],
            rect(6, 4),
            vec![rect(10, 2), rect(12, 1)]
        )
    );
    assert_eq!(
        at(5),
        (
            vec![rect(3, 1), rect(4, 1)],
            rect(5, 0),
            vec![rect(5, 2), rect(7, 1)]
        )
    );
    assert_eq!(
        at(2),
        (
            vec![rect(3, 0), rect(3, 0)],
            rect(3, 0),
            vec![rect(3, 1), rect(4, 1)]
        )
    );
}

/// With the card focused the compose box is dim and the terminal cursor is
/// left where it was, outside the box.
#[test]
fn a_focused_card_dims_the_compose_box_and_takes_the_cursor_out() {
    use ratatui::style::Color;
    let mut app = attached_conversation();
    fold(&mut app, "s1", "root-1", vec![requested("req-1", "rm x")]);
    let mut terminal = terminal_at(&app, 80, 24);
    // The compose box is the last box on the screen, under the card.
    let corner = |terminal: &Terminal<TestBackend>| {
        let buffer = terminal.backend().buffer();
        (0..24)
            .rev()
            .map(|y| &buffer[(0, y)])
            .find(|cell| cell.symbol() == "╭")
            .map(|cell| cell.fg)
    };
    assert_eq!(corner(&terminal), Some(Color::DarkGray));
    let position = terminal
        .get_cursor_position()
        .expect("a TestBackend always reports its cursor");
    assert_eq!((position.x, position.y), (0, 0), "never moved");
    press(&mut app, KeyCode::Tab);
    terminal = terminal_at(&app, 80, 24);
    assert_eq!(
        corner(&terminal),
        Some(Color::Reset),
        "focused again: plain"
    );
}

/// An agent with nothing in its transcript yet invites the first message.
#[test]
fn an_agent_with_nothing_said_invites_a_message() {
    let mut app = App::attached("s1", Some("kid".to_owned()));
    conversation(&mut app);
    let screen = rows_at(&app, 80, 24);
    assert!(
        screen.contains(&" nothing said yet — type a message below".to_owned()),
        "{screen:#?}"
    );
}

/// The command menu shows eight commands at most, and a highlight moved
/// past its end stays on its last row.
#[test]
fn the_command_menu_shows_eight_and_clamps_its_highlight() {
    let mut app = attached_conversation();
    typed(&mut app, "/");
    assert!(crate::slash::completions("/").len() > 8);
    let menu = |app: &App| {
        rows_at(app, 80, 40)
            .into_iter()
            .filter(|row| row.starts_with("  /"))
            .count()
    };
    assert_eq!(menu(&app), 8);

    let mut app = attached_conversation();
    typed(&mut app, "/a");
    let shown = crate::slash::completions("/a").len();
    for _ in 0..shown + 2 {
        press(&mut app, KeyCode::Down);
    }
    let styled = styled_at(&app, 80, 24);
    let highlighted: Vec<&str> = styled
        .lines()
        .filter(|row| row.contains("‹Cyan+b›  /"))
        .collect();
    assert_eq!(highlighted.len(), 1, "{styled}");
    let last = crate::slash::completions("/a")[shown - 1].name;
    assert!(highlighted[0].contains(&format!("/{last}")), "{styled}");
}

fn cursor_at(app: &App, width: u16, height: u16) -> (u16, u16) {
    let mut terminal = terminal_at(app, width, height);
    let position = terminal
        .get_cursor_position()
        .expect("a TestBackend always reports its cursor");
    (position.x, position.y)
}

/// A list screen on five rows keeps one row of its body; on four, only
/// its frame.
#[test]
fn a_short_list_screen_keeps_its_frame_first() {
    let tabs = " horde  1:session  2:agents  3:inbox 1  4:workspaces  5:usage";
    let hint = " enter attach · n new session · x archive · r refresh · esc ba";
    let status = " not attached · ● live";
    assert_eq!(
        rows_at(&tree(), 62, 5),
        [tabs, "", "❯ ci-fix  s1  active  1 waiting", hint, status]
    );
    assert_eq!(rows_at(&tree(), 62, 4), [tabs, "", hint, status]);
}

/// The list scrolls once the selection passes the body's second-to-last
/// row, keeping it in view; and names take a third of the width, so at 62
/// columns a name column is 20 cells.
#[test]
fn a_list_scrolls_to_keep_its_selection_in_view() {
    let mut app = tree();
    for _ in 0..4 {
        press(&mut app, KeyCode::Down);
    }
    assert_eq!(
        rows_at(&app, 62, 8)[2..6],
        [
            "      reviewer         unspecified      gpu-box          1 wai",
            "  docs  s2  active",
            "❯   docs               LOST             worker-9",
            "",
        ]
    );
}

/// The inbox list, card closed, hints at answering from the list.
#[test]
fn the_inbox_list_hints_at_answering_from_the_row() {
    let mut app = tree();
    press(&mut app, KeyCode::Char('i'));
    let screen = rows_at(&app, 80, 24);
    assert_eq!(
        screen[22],
        " y allow · n deny · 1-9 answer · enter open · g go to agent · esc back"
    );
}

/// A stream that ended without a reason says only that it ended.
#[test]
fn a_stream_ended_without_a_reason_says_so() {
    let mut app = attached_conversation();
    app.apply(Update::StreamEnded {
        reason: String::new(),
    });
    let screen = rows_at(&app, 80, 24);
    assert!(
        screen[23].ends_with(" · ⚠ stream ended"),
        "{:?}",
        screen[23]
    );
}

/// An account the provider says is exhausted is marked so; one with no
/// window statement shows its counts alone. An aggregate with no agent
/// reads as a dash.
#[test]
fn usage_marks_an_exhausted_account_and_a_keyless_row() {
    let mut app = App::overview();
    let account = |name: &str, exhausted: bool| horde_proto::v1::AccountHeadroom {
        provider: "claude".to_owned(),
        account: name.to_owned(),
        remaining_fraction: exhausted.then_some(0.0),
        exhausted,
        ..horde_proto::v1::AccountHeadroom::default()
    };
    app.apply(Update::Usage(UsageReport {
        scope: "every session".to_owned(),
        by_agent: vec![UsageAggregate::default()],
        accounts: vec![account("spent", true), account("fresh", false)],
        ..UsageReport::default()
    }));
    press(&mut app, KeyCode::Char('u'));
    let screen = rows_at(&app, 120, 24);
    let counts = "in         0  out         0  cache         0";
    assert_eq!(
        screen[5],
        format!("   —                            {counts}")
    );
    assert_eq!(
        screen[11],
        format!("   claude/spent                 {counts}  0% left  EXHAUSTED")
    );
    assert_eq!(
        screen[12],
        format!("   claude/fresh                 {counts}")
    );
}

/// A clean checkout carries no mark; a dirty one says so.
#[test]
fn a_checkout_is_marked_only_when_dirty() {
    let mut app = App::overview();
    let checkout = |name: &str, dirty: bool| RepoInstanceReport {
        repo_name: name.to_owned(),
        path: format!("/w/{name}"),
        head_commit_sha: "0123456789".to_owned(),
        dirty,
    };
    app.apply(Update::Workspaces(vec![WorkspaceEntry {
        definition: WorkspaceDefinition {
            name: "w".to_owned(),
            ..WorkspaceDefinition::default()
        },
        instances: vec![WorkspaceInstanceReport {
            state: WorkspaceInstanceState::Ready as i32,
            repos: vec![checkout("clean", false), checkout("messy", true)],
            ..WorkspaceInstanceReport::default()
        }],
    }]));
    press(&mut app, KeyCode::Char('w'));
    let screen = rows_at(&app, 80, 24);
    assert_eq!(screen[4], "        clean          01234567  /w/clean");
    assert_eq!(
        screen[5],
        "        messy          01234567  dirty  /w/messy"
    );
}

/// At 60 columns an inbox row has 38 cells for its summary: one that fills
/// them shows whole, one a cell longer is clipped.
#[test]
fn an_inbox_summary_clips_at_the_row_width() {
    let mut app = App::overview();
    app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
    fold(
        &mut app,
        "s1",
        "root-1",
        vec![
            spawned(None, &["ci-fix"]),
            requested("req-1", &"a".repeat(21)),
            requested("req-2", &"b".repeat(22)),
        ],
    );
    press(&mut app, KeyCode::Char('i'));
    let screen = rows_at(&app, 60, 12);
    assert_eq!(
        screen[2..4],
        [
            format!("❯ ci-fix             reviewer wants `{}`", "a".repeat(21)),
            format!("  ci-fix             reviewer wants `{}…", "b".repeat(21)),
        ]
    );
}
