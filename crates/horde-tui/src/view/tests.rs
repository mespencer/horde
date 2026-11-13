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
