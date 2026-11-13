//! The App's keys and updates, driven exactly as the runtime drives them:
//! terminal events into `on_input`, server updates into `apply`, and the
//! directives that come back asserted whole.

use std::collections::BTreeMap;

use horde_proto::v1::{
    AgentId, AgentOutputChannel, PermissionOutcome, SessionId, SessionSummary, WorkerId,
    WorkerSummary, event::Kind,
};
use ratatui::crossterm::event::{
    Event as TermEvent, KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers,
};

use super::{App, Confirm, Directive, Screen, TreeRow, Update};
use crate::fixtures::{asked, chunk, decided, envelope_in, requested, spawned};

fn key(code: KeyCode) -> TermEvent {
    TermEvent::Key(KeyEvent::from(code))
}

fn chord(code: KeyCode, modifiers: KeyModifiers) -> TermEvent {
    TermEvent::Key(KeyEvent::new(code, modifiers))
}

fn ctrl(c: char) -> TermEvent {
    chord(KeyCode::Char(c), KeyModifiers::CONTROL)
}

fn press(app: &mut App, code: KeyCode) -> Vec<Directive> {
    app.on_input(&key(code))
}

fn chars(app: &mut App, text: &str) -> Vec<Directive> {
    text.chars()
        .flat_map(|c| app.on_input(&key(KeyCode::Char(c))))
        .collect()
}

fn summary(session_id: &str, title: &str, root: &str) -> SessionSummary {
    SessionSummary {
        root_agent_id: Some(AgentId::new(root)),
        title: title.to_owned(),
        session_id: Some(SessionId::new(session_id)),
        ..SessionSummary::default()
    }
}

fn event(app: &mut App, session: &str, sequence: u64, agent: &str, kind: Kind) {
    app.apply(Update::Event(Box::new(envelope_in(
        session, sequence, agent, kind,
    ))));
}

/// Attached to `s1`, whose root `root-1` ("ci-fix") has a child `kid`
/// ("reviewer").
fn attached() -> App {
    let mut app = App::attached("s1", None);
    app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
    event(&mut app, "s1", 1, "root-1", spawned(None, &["ci-fix"]));
    event(
        &mut app,
        "s1",
        2,
        "kid",
        spawned(Some("root-1"), &["ci-fix", "reviewer"]),
    );
    app
}

/// An overview over two sessions; `s2` holds one escalation.
fn two_sessions() -> App {
    let mut app = App::overview();
    app.apply(Update::Sessions(vec![
        summary("s1", "ci-fix", "root-1"),
        summary("s2", "docs", "root-2"),
    ]));
    event(&mut app, "s1", 1, "root-1", spawned(None, &["ci-fix"]));
    event(&mut app, "s2", 1, "root-2", spawned(None, &["docs"]));
    event(&mut app, "s2", 2, "root-2", requested("req-1", "rm x"));
    app
}

fn send(agent: &str, text: &str) -> Vec<Directive> {
    vec![Directive::SendAssignment {
        agent_id: agent.to_owned(),
        text: text.to_owned(),
    }]
}

fn approve(request: &str, remember: bool) -> Vec<Directive> {
    vec![Directive::Approve {
        request_id: request.to_owned(),
        remember,
    }]
}

fn deny(request: &str) -> Vec<Directive> {
    vec![Directive::Deny {
        request_id: request.to_owned(),
    }]
}

fn answer(request: &str, question: &str, value: &str) -> Vec<Directive> {
    vec![Directive::Answer {
        request_id: request.to_owned(),
        answers: BTreeMap::from([(question.to_owned(), vec![value.to_owned()])]),
    }]
}

// -- Composing and sending -----------------------------------------------------

#[test]
fn typing_composes_and_enter_sends_to_the_session_root() {
    let mut app = attached();
    assert!(chars(&mut app, "fix it").is_empty());
    assert_eq!(app.input(), "fix it");
    press(&mut app, KeyCode::Backspace);
    assert_eq!(press(&mut app, KeyCode::Enter), send("root-1", "fix i"));
    assert_eq!(app.input(), "", "a sent message leaves the box empty");
    assert!(
        press(&mut app, KeyCode::Enter).is_empty(),
        "nothing typed, nothing sent"
    );
    chars(&mut app, "  ");
    assert!(
        press(&mut app, KeyCode::Enter).is_empty(),
        "blank is nothing"
    );
}

#[test]
fn newline_keys_make_multi_line_messages() {
    let mut app = attached();
    chars(&mut app, "a");
    app.on_input(&chord(KeyCode::Enter, KeyModifiers::ALT));
    chars(&mut app, "b");
    app.on_input(&ctrl('j'));
    chars(&mut app, "c");
    assert_eq!(press(&mut app, KeyCode::Enter), send("root-1", "a\nb\nc"));
}

#[test]
fn editing_keys_reach_the_compose_box() {
    let mut app = attached();
    chars(&mut app, "one two");
    app.on_input(&ctrl('w'));
    assert_eq!(app.input(), "one ");
    app.on_input(&ctrl('a'));
    chars(&mut app, ">");
    app.on_input(&ctrl('e'));
    chars(&mut app, "<");
    assert_eq!(app.input(), ">one <");
    for code in [
        KeyCode::Home,
        KeyCode::Right,
        KeyCode::Delete,
        KeyCode::End,
        KeyCode::Left,
    ] {
        press(&mut app, code);
    }
    assert_eq!(app.input(), ">ne <");
    app.on_input(&ctrl('u'));
    assert_eq!(app.input(), "<");
    press(&mut app, KeyCode::Enter);
    press(&mut app, KeyCode::Up);
    assert_eq!(app.input(), "<", "up recalls the last message");
    press(&mut app, KeyCode::Down);
    assert_eq!(app.input(), "");
}

#[test]
fn a_send_with_no_known_root_is_refused_with_a_status_line() {
    let mut app = App::attached("s1", None);
    chars(&mut app, "hello");
    assert!(press(&mut app, KeyCode::Enter).is_empty());
    assert!(app.status().contains("no root agent"), "{}", app.status());
    assert!(app.on_input(&ctrl('x')).is_empty());
    assert!(app.status().contains("no root agent to interrupt"));
    assert!(app.on_input(&ctrl('s')).is_empty());
    assert!(
        app.run(crate::slash::Slash::Note("x".to_owned()))
            .is_empty()
    );
}

#[test]
fn attachment_to_an_agent_steers_that_agent() {
    let mut app = App::attached("s1", Some("kid".to_owned()));
    app.apply(Update::Sessions(vec![summary("s1", "ci-fix", "root-1")]));
    chars(&mut app, "go");
    assert_eq!(press(&mut app, KeyCode::Enter), send("kid", "go"));
    assert_eq!(
        app.on_input(&ctrl('x')),
        vec![Directive::Interrupt {
            agent_id: "kid".to_owned()
        }]
    );
}

#[test]
fn ctrl_s_sends_now_and_ctrl_x_interrupts_without_touching_the_text() {
    let mut app = attached();
    chars(&mut app, "dra");
    assert_eq!(
        app.on_input(&ctrl('x')),
        vec![Directive::Interrupt {
            agent_id: "root-1".to_owned()
        }]
    );
    assert_eq!(app.input(), "dra", "an interrupt is not a send");
    assert_eq!(
        app.on_input(&ctrl('s')),
        vec![Directive::SendNow {
            agent_id: "root-1".to_owned(),
            text: "dra".to_owned()
        }]
    );
    assert!(app.on_input(&ctrl('s')).is_empty(), "nothing left to send");
    chars(&mut app, "/help");
    assert!(
        app.on_input(&ctrl('s')).is_empty(),
        "a command is not a message"
    );
}

#[test]
fn escape_interrupts_a_working_agent_and_otherwise_clears() {
    let mut app = attached();
    chars(&mut app, "oops");
    assert!(press(&mut app, KeyCode::Esc).is_empty());
    assert_eq!(app.input(), "", "idle: esc clears the box");
    assert_eq!(app.screen(), Screen::Session, "esc never detaches");

    event(
        &mut app,
        "s1",
        3,
        "root-1",
        chunk(AgentOutputChannel::Assistant, "t1", "working"),
    );
    assert!(app.agent_working());
    chars(&mut app, "queued");
    assert_eq!(
        press(&mut app, KeyCode::Esc),
        vec![Directive::Interrupt {
            agent_id: "root-1".to_owned()
        }]
    );
    assert_eq!(app.input(), "queued", "interrupting keeps the draft");

    chars(&mut app, " /x");
    app.on_input(&ctrl('u'));
    chars(&mut app, "/he");
    assert!(
        press(&mut app, KeyCode::Esc).is_empty(),
        "esc closes a command before interrupting"
    );
    assert_eq!(app.input(), "");
}

#[test]
fn ctrl_c_clears_then_quits_and_ctrl_d_quits() {
    let mut app = attached();
    chars(&mut app, "draft");
    app.on_input(&ctrl('c'));
    assert_eq!(app.input(), "");
    assert!(!app.should_quit(), "the first ctrl-c only clears");
    app.on_input(&ctrl('c'));
    assert!(app.should_quit());

    let mut app = attached();
    app.on_input(&ctrl('d'));
    assert!(app.should_quit());

    let mut app = two_sessions();
    app.on_input(&ctrl('c'));
    assert!(app.should_quit(), "a list has nothing to clear");
}

#[test]
fn an_unbound_chord_and_a_key_release_do_nothing() {
    let mut app = attached();
    assert!(app.on_input(&ctrl('z')).is_empty());
    assert_eq!(app.input(), "", "ctrl-z must not type a z");
    let release = TermEvent::Key(KeyEvent {
        code: KeyCode::Char('q'),
        modifiers: KeyModifiers::NONE,
        kind: KeyEventKind::Release,
        state: KeyEventState::NONE,
    });
    app.on_input(&release);
    assert_eq!(app.input(), "", "a release must not act");
    app.on_input(&TermEvent::FocusGained);
    assert_eq!(app.input(), "");
}

#[test]
fn paste_and_resize_are_absorbed() {
    let mut app = attached();
    app.on_input(&TermEvent::Paste("line one\nline two".to_owned()));
    assert_eq!(app.input(), "line one\nline two");
    app.on_input(&TermEvent::Resize(100, 40));
    app.apply(Update::Resize {
        width: 90,
        height: 30,
    });

    let mut list = two_sessions();
    list.on_input(&TermEvent::Paste("ignored".to_owned()));
    assert_eq!(list.input(), "", "a list has no compose box");
}

// -- Slash commands ------------------------------------------------------------

#[test]
fn the_command_menu_completes_and_runs_the_highlighted_command() {
    let mut app = attached();
    chars(&mut app, "/a");
    assert_eq!(app.menu(), 0);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Up);
    assert_eq!(app.menu(), 1, "/agents is second");
    press(&mut app, KeyCode::Tab);
    assert_eq!(app.input(), "/agents", "no argument, no trailing space");
    app.on_input(&ctrl('u'));
    chars(&mut app, "/ne");
    press(&mut app, KeyCode::Tab);
    assert_eq!(app.input(), "/new ", "an argument gets its space");

    app.on_input(&ctrl('u'));
    chars(&mut app, "/in");
    let directives = press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.screen(),
        Screen::Inbox,
        "enter on a partial word runs the match"
    );
    assert_eq!(
        directives,
        vec![Directive::RefreshSessions, Directive::RefreshWorkers]
    );
    assert_eq!(app.input(), "");
}

#[test]
fn a_command_needing_an_argument_waits_for_it() {
    let mut app = attached();
    chars(&mut app, "/not");
    assert!(press(&mut app, KeyCode::Enter).is_empty());
    assert_eq!(
        app.input(),
        "/note ",
        "a partial word completes and waits for its argument"
    );
    chars(&mut app, "fyi");
    assert_eq!(
        press(&mut app, KeyCode::Enter),
        vec![Directive::SendNote {
            agent_id: "root-1".to_owned(),
            text: "fyi".to_owned()
        }]
    );
    chars(&mut app, "/now");
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.status(), "/now needs a message");
    assert_eq!(app.input(), "/now", "a refused command keeps its text");
    chars(&mut app, " stop");
    assert_eq!(
        press(&mut app, KeyCode::Enter),
        vec![Directive::SendNow {
            agent_id: "root-1".to_owned(),
            text: "stop".to_owned()
        }]
    );
    chars(&mut app, "/bogus");
    press(&mut app, KeyCode::Enter);
    assert!(app.status().starts_with("no command /bogus"));
    assert_eq!(chars(&mut app, ""), Vec::new());
}

#[test]
fn commands_open_screens_and_toggle_state() {
    use crate::slash::Slash;
    let mut app = attached();
    for (command, screen) in [
        (Slash::Workspaces, Screen::Workspaces),
        (Slash::Usage, Screen::Usage),
        (Slash::Logs, Screen::Logs),
        (Slash::Forwards, Screen::Forwards),
        (Slash::Help, Screen::Help),
        (Slash::Agents, Screen::Overview),
        (Slash::Inbox, Screen::Inbox),
        (Slash::Attach(None), Screen::Overview),
    ] {
        app.run(command);
        assert_eq!(app.screen(), screen);
    }
    assert_eq!(
        app.run(Slash::New(Some("t".to_owned()))),
        vec![Directive::NewSession {
            title: Some("t".to_owned())
        }]
    );
    assert_eq!(
        app.run(Slash::Interrupt),
        vec![Directive::Interrupt {
            agent_id: "root-1".to_owned()
        }]
    );
    app.run(Slash::Details);
    assert!(app.details());
    app.on_input(&ctrl('o'));
    assert!(!app.details(), "ctrl-o toggles the same switch");
    app.run(Slash::Quit);
    assert!(app.should_quit());
}

#[test]
fn attach_by_name_finds_agents_then_sessions_and_refuses_ambiguity() {
    use crate::slash::Slash;
    let mut app = attached();
    app.apply(Update::Sessions(vec![
        summary("s2-abc", "docs work", "r2"),
        summary("s3-abd", "docs", "r3"),
    ]));
    app.run(Slash::Attach(Some("reviewer".to_owned())));
    assert_eq!(
        app.attachment().and_then(|a| a.agent_id.clone()).as_deref(),
        Some("kid")
    );
    app.run(Slash::Attach(Some("ci-fix".to_owned())));
    assert_eq!(
        app.attachment().and_then(|a| a.agent_id.clone()),
        None,
        "the root's name is the session root"
    );
    app.run(Slash::Attach(Some("kid".to_owned())));
    assert_eq!(
        app.attachment().and_then(|a| a.agent_id.clone()).as_deref(),
        Some("kid"),
        "an id works too"
    );

    app.run(Slash::Attach(Some("docs-work".to_owned())));
    assert_eq!(
        app.attachment().map(|a| a.session_id.as_str()),
        Some("s2-abc"),
        "a slug names a session"
    );
    assert!(
        app.take_resubscribe(),
        "a newly attached session must be replayed"
    );
    app.run(Slash::Attach(Some("s3".to_owned())));
    assert_eq!(
        app.attachment().map(|a| a.session_id.as_str()),
        Some("s3-abd"),
        "an id prefix names one"
    );
    app.run(Slash::Attach(Some("s".to_owned())));
    assert!(app.status().contains("ambiguous"), "{}", app.status());
    app.run(Slash::Attach(Some("nope".to_owned())));
    assert_eq!(app.status(), "no session or agent is named nope");
}

#[test]
fn archive_asks_first_and_only_y_confirms() {
    use crate::slash::Slash;
    let mut app = attached();
    app.run(Slash::Archive(None));
    assert_eq!(
        app.confirm(),
        Some(&Confirm::Archive {
            session_id: "s1".to_owned()
        })
    );
    assert_eq!(
        chars(&mut app, "y"),
        vec![Directive::Archive {
            session_id: "s1".to_owned()
        }]
    );
    assert_eq!(app.confirm(), None);

    app.run(Slash::Archive(Some("ci-fix".to_owned())));
    assert!(chars(&mut app, "n").is_empty(), "anything but y cancels");
    assert_eq!(app.confirm(), None);
    assert_eq!(app.input(), "", "the cancelling key is swallowed");

    app.run(Slash::Archive(Some("nope".to_owned())));
    assert!(app.status().starts_with("archive which session?"));
    let mut detached = App::overview();
    detached.run(Slash::Archive(None));
    assert_eq!(detached.confirm(), None);
}

// -- Prompts in place ----------------------------------------------------------

#[test]
fn a_waiting_permission_takes_focus_and_y_approves_it() {
    let mut app = attached();
    event(&mut app, "s1", 3, "kid", requested("req-1", "rm x"));
    assert!(app.prompt_focused(), "a prompt in scope takes the keys");
    assert_eq!(chars(&mut app, "y"), approve("req-1", false));
    assert!(app.is_submitted("req-1"));
    assert!(chars(&mut app, "y").is_empty(), "answered once is enough");

    app.directive_failed(&approve("req-1", false)[0], "someone else answered");
    assert!(
        !app.is_submitted("req-1"),
        "a refusal lets it be answered again"
    );
    assert_eq!(app.status(), "someone else answered");
    press(&mut app, KeyCode::Down);
    assert_eq!(
        press(&mut app, KeyCode::Enter),
        approve("req-1", true),
        "row two remembers"
    );

    event(
        &mut app,
        "s1",
        4,
        "kid",
        decided("req-1", PermissionOutcome::Allow, ""),
    );
    assert!(!app.is_submitted("req-1"), "the decision settles it");
    assert!(
        !app.prompt_focused(),
        "nothing waits; keys go back to compose"
    );
}

#[test]
fn tab_and_escape_move_focus_between_prompt_and_compose() {
    let mut app = attached();
    event(&mut app, "s1", 3, "root-1", requested("req-1", "rm x"));
    press(&mut app, KeyCode::Tab);
    assert!(!app.prompt_focused());
    chars(&mut app, "yes but");
    assert_eq!(
        app.input(),
        "yes but",
        "with compose focused, y is a letter"
    );
    press(&mut app, KeyCode::Tab);
    assert!(app.prompt_focused());
    press(&mut app, KeyCode::Esc);
    assert!(
        !app.prompt_focused(),
        "esc leaves the card for the compose box"
    );

    event(&mut app, "s1", 4, "root-1", requested("req-2", "rm y"));
    event(
        &mut app,
        "s1",
        5,
        "root-1",
        decided("req-1", PermissionOutcome::Deny, ""),
    );
    assert!(app.prompt_focused(), "the next prompt takes focus again");
    assert_eq!(chars(&mut app, "n"), deny("req-2"));
}

#[test]
fn a_question_is_answered_in_place() {
    let mut app = attached();
    event(
        &mut app,
        "s1",
        3,
        "root-1",
        asked("q-1", &[("colour", &["Red", "Blue"])]),
    );
    press(&mut app, KeyCode::Down);
    assert_eq!(app.cursor().map(|cursor| cursor.row()), Some(1));
    assert_eq!(
        press(&mut app, KeyCode::Enter),
        answer("q-1", "colour", "Blue")
    );
    assert_eq!(app.cursor(), None, "a settled cursor is dropped");

    let mut several = attached();
    event(
        &mut several,
        "s1",
        3,
        "root-1",
        asked("q-2", &[("a", &["x"]), ("b", &["y", "z"])]),
    );
    assert!(
        chars(&mut several, "1").is_empty(),
        "one answered, one to go"
    );
    assert_eq!(
        chars(&mut several, "2"),
        vec![Directive::Answer {
            request_id: "q-2".to_owned(),
            answers: BTreeMap::from([
                ("a".to_owned(), vec!["x".to_owned()]),
                ("b".to_owned(), vec!["z".to_owned()])
            ]),
        }]
    );
}

#[test]
fn prompts_scope_to_the_attached_subtree() {
    let mut app = App::attached("s1", Some("kid".to_owned()));
    event(&mut app, "s1", 1, "root-1", spawned(None, &["ci-fix"]));
    event(
        &mut app,
        "s1",
        2,
        "kid",
        spawned(Some("root-1"), &["ci-fix", "a"]),
    );
    event(
        &mut app,
        "s1",
        3,
        "other",
        spawned(Some("root-1"), &["ci-fix", "b"]),
    );
    event(&mut app, "s1", 4, "kid", requested("req-mine", "ls"));
    event(&mut app, "s1", 5, "other", requested("req-elsewhere", "ls"));
    let ids: Vec<String> = app
        .scoped_pending()
        .into_iter()
        .map(|p| p.request_id)
        .collect();
    assert_eq!(ids, vec!["req-mine"]);
    assert_eq!(app.waiting_elsewhere(), 1);
    assert_eq!(app.waiting_total(), 2);
    assert_eq!(
        app.front_prompt().map(|p| p.request_id).as_deref(),
        Some("req-mine")
    );
    assert!(
        App::overview().scoped_pending().is_empty(),
        "nothing attached, nothing scoped"
    );
}

// -- The inbox -----------------------------------------------------------------

#[test]
fn the_inbox_answers_the_selected_row_in_place() {
    let mut app = two_sessions();
    event(&mut app, "s1", 2, "root-1", requested("req-0", "ls"));
    event(
        &mut app,
        "s1",
        3,
        "root-1",
        asked("q-1", &[("colour", &["Red"])]),
    );
    press(&mut app, KeyCode::Char('i'));
    assert_eq!(app.screen(), Screen::Inbox);
    let ids: Vec<String> = app
        .inbox_entries()
        .into_iter()
        .map(|e| e.permission.request_id)
        .collect();
    assert_eq!(
        ids,
        vec!["req-0", "q-1", "req-1"],
        "session order, then arrival order"
    );
    assert_eq!(chars(&mut app, "y"), approve("req-0", false));
    press(&mut app, KeyCode::Down);
    assert_eq!(chars(&mut app, "1"), answer("q-1", "colour", "Red"));
    press(&mut app, KeyCode::Char('j'));
    assert_eq!(chars(&mut app, "n"), deny("req-1"));
    press(&mut app, KeyCode::Char('j'));
    assert_eq!(app.selected(Screen::Inbox), 2, "the last row is the floor");
    press(&mut app, KeyCode::Char('k'));
    assert_eq!(app.selected(Screen::Inbox), 1);
}

#[test]
fn enter_opens_the_inbox_card_and_g_goes_to_the_agent() {
    let mut app = two_sessions();
    press(&mut app, KeyCode::Char('i'));
    press(&mut app, KeyCode::Enter);
    assert!(app.inbox_card());
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Down);
    assert_eq!(
        press(&mut app, KeyCode::Enter),
        deny("req-1"),
        "the card's third row is No"
    );
    assert!(!app.inbox_card(), "answering closes the card");
    press(&mut app, KeyCode::Enter);
    press(&mut app, KeyCode::Esc);
    assert!(!app.inbox_card());
    assert_eq!(
        app.screen(),
        Screen::Inbox,
        "esc closes the card before leaving"
    );
    press(&mut app, KeyCode::Char('g'));
    assert_eq!(app.screen(), Screen::Session);
    assert_eq!(
        app.attachment()
            .map(|a| (a.session_id.as_str(), a.agent_id.clone())),
        Some(("s2", None))
    );
}

#[test]
fn an_empty_inbox_answers_and_opens_nothing() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('i'));
    assert!(chars(&mut app, "y").is_empty());
    press(&mut app, KeyCode::Enter);
    assert!(!app.inbox_card());
    press(&mut app, KeyCode::Char('g'));
    assert_eq!(app.screen(), Screen::Inbox);
}

// -- The tree and navigation ---------------------------------------------------

#[test]
fn the_tree_lists_sessions_then_agents_depth_first() {
    let app = attached();
    let mut app = app;
    event(
        &mut app,
        "s1",
        3,
        "grandkid",
        spawned(Some("kid"), &["ci-fix", "reviewer", "runner"]),
    );
    event(
        &mut app,
        "s1",
        4,
        "kid-b",
        spawned(Some("root-1"), &["ci-fix", "b"]),
    );
    let agent = |id: &str, depth| TreeRow::Agent {
        session_id: "s1".to_owned(),
        agent_id: id.to_owned(),
        depth,
    };
    assert_eq!(
        app.tree_rows(),
        vec![
            TreeRow::Session {
                session_id: "s1".to_owned()
            },
            agent("root-1", 0),
            agent("kid", 1),
            agent("grandkid", 2),
            agent("kid-b", 1),
        ]
    );
}

#[test]
fn a_parent_cycle_cannot_hang_the_tree() {
    let mut app = attached();
    if let Some(session) = app.sessions.get_mut("s1") {
        if let Some(root) = session.agents.get_mut("root-1") {
            root.parent = None;
        }
        session.agents.insert(
            "x".to_owned(),
            crate::tree::AgentNode {
                parent: Some("x".to_owned()),
                ..Default::default()
            },
        );
    }
    assert!(
        app.tree_rows().len() < 10,
        "a self-parented agent is not a root and not walked forever"
    );
}

#[test]
fn enter_in_the_tree_attaches_to_the_row() {
    let mut app = attached();
    app.on_input(&ctrl('t'));
    assert_eq!(app.screen(), Screen::Overview);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.attachment().and_then(|a| a.agent_id.clone()).as_deref(),
        Some("kid")
    );
    app.on_input(&ctrl('t'));
    press(&mut app, KeyCode::Up);
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.attachment().and_then(|a| a.agent_id.clone()),
        None,
        "the root row is the session root"
    );
    app.on_input(&ctrl('t'));
    press(&mut app, KeyCode::Up);
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.screen(), Screen::Session, "a session row attaches too");

    let mut empty = App::overview();
    press(&mut empty, KeyCode::Enter);
    assert_eq!(empty.screen(), Screen::Overview);
    assert!(empty.attachment().is_none());
}

#[test]
fn the_tree_starts_and_archives_sessions() {
    let mut app = two_sessions();
    assert_eq!(
        press(&mut app, KeyCode::Char('n')),
        vec![Directive::NewSession { title: None }]
    );
    press(&mut app, KeyCode::Char('x'));
    assert_eq!(
        app.confirm(),
        Some(&Confirm::Archive {
            session_id: "s1".to_owned()
        })
    );
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char('x'));
    assert_eq!(
        app.confirm(),
        Some(&Confirm::Archive {
            session_id: "s1".to_owned()
        }),
        "an agent row archives its session"
    );
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Char('w'));
    press(&mut app, KeyCode::Char('x'));
    assert_eq!(app.confirm(), None, "x archives only from the tree");
}

#[test]
fn list_letters_alt_digits_and_f1_navigate() {
    let mut app = attached();
    let cases = [
        (
            chord(KeyCode::Char('2'), KeyModifiers::ALT),
            Screen::Overview,
        ),
        (key(KeyCode::Char('w')), Screen::Workspaces),
        (key(KeyCode::Char('u')), Screen::Usage),
        (key(KeyCode::Char('l')), Screen::Logs),
        (key(KeyCode::Char('f')), Screen::Forwards),
        (key(KeyCode::Char('?')), Screen::Help),
        (key(KeyCode::Char('a')), Screen::Overview),
        (key(KeyCode::Char('o')), Screen::Overview),
        (key(KeyCode::Char('s')), Screen::Session),
        (chord(KeyCode::Char('3'), KeyModifiers::ALT), Screen::Inbox),
        (
            chord(KeyCode::Char('4'), KeyModifiers::ALT),
            Screen::Workspaces,
        ),
        (chord(KeyCode::Char('5'), KeyModifiers::ALT), Screen::Usage),
        (chord(KeyCode::Char('6'), KeyModifiers::ALT), Screen::Logs),
        (
            chord(KeyCode::Char('7'), KeyModifiers::ALT),
            Screen::Forwards,
        ),
        (
            chord(KeyCode::Char('1'), KeyModifiers::ALT),
            Screen::Session,
        ),
        (key(KeyCode::F(1)), Screen::Help),
    ];
    for (input, screen) in cases {
        app.on_input(&input);
        assert_eq!(app.screen(), screen, "after {input:?}");
    }
}

#[test]
fn entering_a_screen_asks_for_its_data() {
    let mut app = attached();
    let asks = |app: &mut App, screen| app.act(crate::keys::Intent::Goto(screen));
    assert_eq!(
        asks(&mut app, Screen::Workspaces),
        vec![Directive::RefreshWorkspaces, Directive::RefreshWorkers]
    );
    assert_eq!(
        asks(&mut app, Screen::Usage),
        vec![Directive::RefreshUsage {
            session_id: Some("s1".to_owned())
        }]
    );
    assert_eq!(
        asks(&mut app, Screen::Forwards),
        vec![Directive::RefreshForwards]
    );
    assert_eq!(
        press(&mut app, KeyCode::Char('r')),
        vec![Directive::RefreshForwards],
        "r re-asks"
    );
    assert!(
        asks(&mut app, Screen::Logs).is_empty(),
        "logs have no RPC to ask"
    );
    assert!(asks(&mut app, Screen::Help).is_empty());
    assert_eq!(
        App::overview().act(crate::keys::Intent::Goto(Screen::Usage)),
        vec![Directive::RefreshUsage { session_id: None }]
    );
}

#[test]
fn the_session_screen_needs_an_attachment() {
    let mut app = App::overview();
    press(&mut app, KeyCode::Char('s'));
    assert_eq!(app.screen(), Screen::Overview);
    assert!(app.status().starts_with("not attached"));
}

#[test]
fn escape_on_a_list_goes_back_where_it_came_from() {
    let mut app = attached();
    app.on_input(&ctrl('t'));
    press(&mut app, KeyCode::Char('w'));
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.screen(), Screen::Overview, "back to the tree");
    press(&mut app, KeyCode::Esc);
    assert_eq!(
        app.screen(),
        Screen::Session,
        "esc from where it came lands on the session"
    );

    let mut detached = App::overview();
    press(&mut detached, KeyCode::Char('u'));
    press(&mut detached, KeyCode::Esc);
    assert_eq!(detached.screen(), Screen::Overview);
    press(&mut detached, KeyCode::Esc);
    assert_eq!(detached.screen(), Screen::Overview, "nowhere further to go");
    press(&mut detached, KeyCode::Char('q'));
    assert!(detached.should_quit());
}

#[test]
fn question_mark_on_an_empty_box_opens_help() {
    let mut app = attached();
    chars(&mut app, "?");
    assert_eq!(app.screen(), Screen::Help);
    press(&mut app, KeyCode::Esc);
    chars(&mut app, "why?");
    assert_eq!(app.input(), "why?", "mid-message it is a question mark");
}

// -- Replay, scroll, stream health ----------------------------------------------

#[test]
fn replay_grows_only_when_asked_and_resumes_past_what_was_folded() {
    let mut app = attached();
    assert!(
        !app.take_resubscribe(),
        "the launch subscription already covers s1"
    );
    assert_eq!(app.replay_cursors(), vec![("s1".to_owned(), 3)]);
    app.apply(Update::Sessions(vec![summary("s2", "docs", "r2")]));
    assert!(
        !app.take_resubscribe(),
        "an attach does not replay the others"
    );
    app.on_input(&ctrl('t'));
    assert!(app.take_resubscribe(), "the tree needs every session");
    assert!(!app.take_resubscribe(), "reading clears it");
    assert_eq!(
        app.replay_cursors(),
        vec![("s1".to_owned(), 3), ("s2".to_owned(), 1)]
    );
    app.on_input(&ctrl('t'));
    assert!(!app.take_resubscribe(), "nothing new to replay");
    app.apply(Update::Sessions(vec![summary("s3", "new", "r3")]));
    assert!(
        app.take_resubscribe(),
        "once replaying everything, new sessions join"
    );

    let mut overview = App::overview();
    overview.apply(Update::Sessions(vec![summary("s1", "a", "r")]));
    assert!(overview.take_resubscribe());
    overview.apply(Update::SessionCreated {
        session_id: "s9".to_owned(),
    });
    assert_eq!(overview.screen(), Screen::Session);
    assert_eq!(
        overview.attachment().map(|a| a.session_id.as_str()),
        Some("s9")
    );
    assert!(overview.take_resubscribe());
}

#[test]
fn re_attaching_restores_the_launched_agent_scope() {
    let mut app = App::attached("s1", Some("kid".to_owned()));
    app.apply(Update::Sessions(vec![
        summary("s1", "ci-fix", "root-1"),
        summary("s2", "docs", "root-2"),
    ]));
    app.on_input(&ctrl('t'));
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.attachment().and_then(|a| a.agent_id.clone()).as_deref(),
        Some("kid")
    );
    app.on_input(&ctrl('t'));
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.attachment()
            .map(|a| (a.session_id.as_str(), a.agent_id.clone())),
        Some(("s2", None))
    );
}

#[test]
fn scrolling_pages_back_and_clamps_to_the_transcript() {
    let mut app = attached();
    app.apply(Update::Resize {
        width: 80,
        height: 10,
    });
    for sequence in 3..40 {
        event(
            &mut app,
            "s1",
            sequence,
            "root-1",
            chunk(
                AgentOutputChannel::Assistant,
                &format!("t{sequence}"),
                "line",
            ),
        );
    }
    press(&mut app, KeyCode::PageUp);
    assert_eq!(app.scroll(), 10);
    for _ in 0..20 {
        press(&mut app, KeyCode::PageUp);
    }
    assert_eq!(
        app.scroll(),
        38 * 2 - 1 - 5,
        "rows (the spawn notice and 37 replies) less half a screen"
    );
    press(&mut app, KeyCode::PageDown);
    assert_eq!(app.scroll(), 60);
    app.on_input(&chord(KeyCode::End, KeyModifiers::CONTROL));
    assert_eq!(app.scroll(), 0);
    let mut empty = App::overview();
    empty.act(crate::keys::Intent::ScrollUp);
    assert_eq!(empty.scroll(), 0, "nothing to scroll");
}

#[test]
fn staleness_begins_strictly_past_the_ceiling() {
    let last = std::time::Instant::now();
    assert!(!super::is_stale(None, last + super::STALE_AFTER * 2));
    assert!(!super::is_stale(Some(last), last + super::STALE_AFTER));
    assert!(super::is_stale(
        Some(last),
        last + super::STALE_AFTER + std::time::Duration::from_millis(1)
    ));
}

#[test]
fn the_connection_word_follows_the_stream() {
    let mut app = App::attached("s1", None);
    assert_eq!(app.connection_word(), "connecting");
    app.apply(Update::Heartbeat);
    assert_eq!(app.connection_word(), "syncing", "alive but not caught up");
    app.apply(Update::CaughtUp {
        session_id: "s1".to_owned(),
        head: 4,
    });
    assert_eq!(app.connection_word(), "live");
    assert_eq!(
        app.session("s1").map(|s| (s.caught_up, s.head)),
        Some((true, 4))
    );
    app.apply(Update::Tick {
        now: std::time::Instant::now() + super::STALE_AFTER * 2,
    });
    assert!(app.stream_stale());
    assert_eq!(app.connection_word(), "stale");
    app.apply(Update::Heartbeat);
    assert!(!app.stream_stale(), "life observed clears the badge");
    app.apply(Update::StreamEnded {
        reason: "gone".to_owned(),
    });
    app.apply(Update::Heartbeat);
    assert_eq!(app.connection_word(), "ended");
    assert_eq!(
        app.stream_ended(),
        Some("gone"),
        "an ended stream stays ended"
    );
}

#[test]
fn rpc_answers_land_where_the_views_read_them() {
    let mut app = App::overview();
    assert!(
        app.workspaces().is_none() && app.usage().is_none(),
        "unasked is not empty"
    );
    app.apply(Update::Workspaces(Vec::new()));
    app.apply(Update::Usage(super::UsageReport::default()));
    assert_eq!(app.workspaces().map(<[_]>::len), Some(0));
    assert!(app.usage().is_some());
    app.apply(Update::Workers(vec![
        WorkerSummary {
            worker_id: Some(WorkerId::new("w-1")),
            name: "gpu-box".to_owned(),
            ..WorkerSummary::default()
        },
        WorkerSummary {
            worker_id: Some(WorkerId::new("w-2")),
            ..WorkerSummary::default()
        },
    ]));
    assert_eq!(app.workers().len(), 2);
    assert_eq!(app.worker_name("w-1"), "gpu-box");
    assert_eq!(
        app.worker_name("w-2"),
        "w-2",
        "a nameless worker is its short id"
    );
    assert_eq!(app.worker_name("worker-unknown-123"), "worker-u");

    let forwards = |count: usize| {
        (0..count)
            .map(|_| horde_proto::v1::ForwardStatus::default())
            .collect::<Vec<_>>()
    };
    app.apply(Update::Forwards(forwards(3)));
    press(&mut app, KeyCode::Char('f'));
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Down);
    assert_eq!(app.selected(Screen::Forwards), 2);
    app.apply(Update::Forwards(forwards(1)));
    assert_eq!(
        app.selected(Screen::Forwards),
        0,
        "a shrinking list clamps the selection"
    );
    assert_eq!(app.forwards().len(), 1);
}

#[test]
fn sessions_are_named_and_summaries_without_ids_are_dropped() {
    let mut app = App::attached("0123456789-untitled", None);
    app.apply(Update::Sessions(vec![
        summary("s1", "ci-fix", "root-1"),
        SessionSummary::default(),
    ]));
    assert_eq!(app.session_name("s1"), "ci-fix");
    assert_eq!(app.session_name("0123456789-untitled"), "01234567");
    assert_eq!(app.session_name("abcdefghij-unknown"), "abcdefgh");
    assert_eq!(app.sessions_in_order().len(), 2);
    app.report_error("the server said no");
    assert_eq!(app.status(), "the server said no");
    app.directive_failed(&Directive::RefreshSessions, "later");
    assert_eq!(app.status(), "later");
}
