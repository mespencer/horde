//! Slash commands typed into the compose box.
//!
//! One table holds every command: its name, aliases, argument shape, the
//! help line the completion menu and `/help` show, and how its argument
//! becomes a [`Slash`]. Parsing is a lookup in that table, so adding a
//! command is one row and the menu, the help screen and the parser can
//! never disagree about what exists.

/// A parsed command, ready for the App to act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Slash {
    /// Attach to a session or agent by name; `None` opens the agent tree.
    Attach(Option<String>),
    /// Create a session (optionally titled) and attach to it.
    New(Option<String>),
    Agents,
    Inbox,
    Workspaces,
    Usage,
    Logs,
    Forwards,
    Interrupt,
    /// Interrupt and deliver this message immediately.
    Now(String),
    /// Send a note rather than an assignment.
    Note(String),
    /// Toggle full tool detail in the transcript.
    Details,
    /// Archive a session (the attached one when `None`), after confirming.
    Archive(Option<String>),
    Help,
    Quit,
}

/// One row of the command table.
#[derive(Debug, Clone, Copy)]
pub struct Command {
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    /// The argument shape for help: `""`, `"[title]"`, `"<message>"`.
    pub args: &'static str,
    pub help: &'static str,
    build: fn(Option<String>) -> Result<Slash, String>,
}

impl Command {
    fn answers_to(&self, word: &str) -> bool {
        self.name == word || self.aliases.contains(&word)
    }
}

/// Every command, in the order the menu lists them.
pub const COMMANDS: &[Command] = &[
    Command {
        name: "attach",
        aliases: &[],
        args: "[session|agent]",
        help: "attach to a session or agent; alone, pick from the tree",
        build: |arg| Ok(Slash::Attach(arg)),
    },
    Command {
        name: "new",
        aliases: &[],
        args: "[title]",
        help: "start a new session and attach to it",
        build: |arg| Ok(Slash::New(arg)),
    },
    Command {
        name: "agents",
        aliases: &["tree", "sessions"],
        args: "",
        help: "sessions and their agent trees",
        build: |_| Ok(Slash::Agents),
    },
    Command {
        name: "inbox",
        aliases: &[],
        args: "",
        help: "everything waiting on you, across sessions",
        build: |_| Ok(Slash::Inbox),
    },
    Command {
        name: "workspaces",
        aliases: &[],
        args: "",
        help: "workspace definitions, instances and checkouts",
        build: |_| Ok(Slash::Workspaces),
    },
    Command {
        name: "usage",
        aliases: &[],
        args: "",
        help: "token usage by agent, tree and account",
        build: |_| Ok(Slash::Usage),
    },
    Command {
        name: "logs",
        aliases: &[],
        args: "",
        help: "worker and server logs",
        build: |_| Ok(Slash::Logs),
    },
    Command {
        name: "forwards",
        aliases: &[],
        args: "",
        help: "port forwards and who backs them",
        build: |_| Ok(Slash::Forwards),
    },
    Command {
        name: "interrupt",
        aliases: &[],
        args: "",
        help: "cancel the agent's current turn",
        build: |_| Ok(Slash::Interrupt),
    },
    Command {
        name: "now",
        aliases: &[],
        args: "<message>",
        help: "interrupt and deliver a message immediately",
        build: |arg| required(arg, "/now needs a message").map(Slash::Now),
    },
    Command {
        name: "note",
        aliases: &[],
        args: "<message>",
        help: "send a note the agent reads without replying",
        build: |arg| required(arg, "/note needs a message").map(Slash::Note),
    },
    Command {
        name: "details",
        aliases: &["expand"],
        args: "",
        help: "show or hide full tool input and output",
        build: |_| Ok(Slash::Details),
    },
    Command {
        name: "archive",
        aliases: &[],
        args: "[session]",
        help: "archive the attached session (asks first)",
        build: |arg| Ok(Slash::Archive(arg)),
    },
    Command {
        name: "help",
        aliases: &["?"],
        args: "",
        help: "keys and commands",
        build: |_| Ok(Slash::Help),
    },
    Command {
        name: "quit",
        aliases: &["exit", "q"],
        args: "",
        help: "leave the TUI (the session keeps running)",
        build: |_| Ok(Slash::Quit),
    },
];

fn required(arg: Option<String>, refusal: &str) -> Result<String, String> {
    arg.ok_or_else(|| refusal.to_owned())
}

/// Whether compose text is a command rather than a message: it starts with
/// `/` and the first word names something (a lone `/` counts, so the menu
/// opens as soon as it is typed). `/tmp/x is broken` is a message.
#[must_use]
pub fn is_command(text: &str) -> bool {
    let Some(rest) = text.strip_prefix('/') else {
        return false;
    };
    let word = rest.split_whitespace().next().unwrap_or("");
    !word.contains('/')
}

/// Parses `/word [argument…]`.
///
/// # Errors
///
/// A refusal in words: an unknown command (naming `/help`), or a missing
/// required argument.
pub fn parse(text: &str) -> Result<Slash, String> {
    let rest = text.trim().strip_prefix('/').unwrap_or(text.trim());
    let (word, arg) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));
    let arg = Some(arg.trim().to_owned()).filter(|arg| !arg.is_empty());
    let command = COMMANDS
        .iter()
        .find(|command| command.answers_to(word))
        .ok_or_else(|| format!("no command /{word} — /help lists them"))?;
    (command.build)(arg)
}

/// The commands whose name starts with what has been typed after the `/`,
/// in table order. Empty once the command word is finished (a space typed).
#[must_use]
pub fn completions(text: &str) -> Vec<&'static Command> {
    let Some(word) = text.strip_prefix('/') else {
        return Vec::new();
    };
    if word.contains(char::is_whitespace) {
        return Vec::new();
    }
    COMMANDS
        .iter()
        .filter(|command| command.name.starts_with(word))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{COMMANDS, Slash, completions, is_command, parse};

    #[test]
    fn every_command_parses_by_name_and_alias() {
        let cases = [
            ("/attach", Slash::Attach(None)),
            ("/attach  brisk-warband ", Slash::Attach(Some("brisk-warband".to_owned()))),
            ("/new", Slash::New(None)),
            ("/new fix the build", Slash::New(Some("fix the build".to_owned()))),
            ("/agents", Slash::Agents),
            ("/tree", Slash::Agents),
            ("/sessions", Slash::Agents),
            ("/inbox", Slash::Inbox),
            ("/workspaces", Slash::Workspaces),
            ("/usage", Slash::Usage),
            ("/logs", Slash::Logs),
            ("/forwards", Slash::Forwards),
            ("/interrupt", Slash::Interrupt),
            ("/now stop that", Slash::Now("stop that".to_owned())),
            ("/note fyi", Slash::Note("fyi".to_owned())),
            ("/details", Slash::Details),
            ("/expand", Slash::Details),
            ("/archive", Slash::Archive(None)),
            ("/archive old-one", Slash::Archive(Some("old-one".to_owned()))),
            ("/help", Slash::Help),
            ("/?", Slash::Help),
            ("/quit", Slash::Quit),
            ("/exit", Slash::Quit),
            ("/q", Slash::Quit),
        ];
        for (text, expected) in cases {
            assert_eq!(parse(text), Ok(expected), "for {text}");
        }
    }

    #[test]
    fn refusals_say_what_is_wrong() {
        assert_eq!(
            parse("/frobnicate"),
            Err("no command /frobnicate — /help lists them".to_owned())
        );
        assert_eq!(parse("/now"), Err("/now needs a message".to_owned()));
        assert_eq!(parse("/note   "), Err("/note needs a message".to_owned()));
    }

    #[test]
    fn a_path_is_a_message_not_a_command() {
        assert!(is_command("/"));
        assert!(is_command("/att"));
        assert!(is_command("/new a/b"), "only the command word is checked");
        assert!(!is_command("/tmp/x is broken"));
        assert!(!is_command("hello /attach"));
    }

    #[test]
    fn completion_matches_prefixes_until_the_word_ends() {
        let names = |text: &str| -> Vec<&str> {
            completions(text).iter().map(|command| command.name).collect()
        };
        assert_eq!(names("/a"), vec!["attach", "agents", "archive"]);
        assert_eq!(names("/").len(), COMMANDS.len());
        assert!(names("/attach ").is_empty(), "a space ends the word");
        assert!(names("attach").is_empty(), "no slash, no menu");
        assert!(names("/zz").is_empty());
    }
}
