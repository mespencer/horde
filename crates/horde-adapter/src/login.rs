//! How an operator signs a provider's CLI into an account (DESIGN §4).
//!
//! The interactive-auth relay is generic — a remote PTY plus an ephemeral
//! callback expose cover nearly any CLI login flow — so the only thing a
//! provider contributes is this recipe: what to run, and where its OAuth
//! callback listens. Zero per-provider relay code is the point; the recipe
//! is data.

/// One provider's interactive login command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginRecipe {
    /// Binary to run — a name on `PATH` or an explicit path, exactly like
    /// the harness spawn's program.
    pub program: String,

    /// Arguments, fixed by the provider's CLI.
    pub args: Vec<String>,

    /// The browser-facing OAuth-callback port — the port in the flow's
    /// redirect URI, when the flow uses one. The relay exposes exactly this
    /// port on the client machine for the login's duration — an OAuth
    /// redirect URI is port-exact, so there is no remapping.
    ///
    /// `None` for flows that need no callback (paste-a-code flows work over
    /// the PTY alone).
    pub callback_port: Option<u16>,

    /// The worker-loopback port the flow's local server actually listens
    /// on, when it differs from [`Self::callback_port`]. `None` means the
    /// same port — every stock provider CLI, which derives its redirect URI
    /// from its own listener.
    ///
    /// Separable at all because the relay itself is a port-forward: the
    /// browser's port and the listener's port only have to agree when
    /// nothing sits between them. The hermetic e2e provider leans on this —
    /// on one machine, one port cannot be both the client's listener and
    /// the worker's.
    pub listen_port: Option<u16>,

    /// Environment the command runs with — the config directory a login
    /// must sign in (`CLAUDE_CONFIG_DIR`, `CODEX_HOME`). Without it the
    /// command signs in the operator's own directory, and the Worker-owned
    /// one its agents run under stays logged out (ADR-0010).
    pub env: Vec<(String, String)>,
}

impl LoginRecipe {
    /// A recipe with no callback: the whole flow lives in the terminal.
    #[must_use]
    pub fn terminal_only(program: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            program: program.into(),
            args,
            callback_port: None,
            listen_port: None,
            env: Vec::new(),
        }
    }

    /// A recipe whose flow opens a browser that must land on this loopback
    /// port — the stock shape, where the flow also listens there.
    #[must_use]
    pub fn with_callback(program: impl Into<String>, args: Vec<String>, port: u16) -> Self {
        Self {
            program: program.into(),
            args,
            callback_port: Some(port),
            listen_port: None,
            env: Vec::new(),
        }
    }

    /// Splits the listener's port from the redirect's — for flows that can
    /// be told to listen somewhere other than the port their redirect URI
    /// names.
    #[must_use]
    pub fn listening_on(mut self, port: u16) -> Self {
        self.listen_port = Some(port);
        self
    }

    /// Runs the command with `env` — the directory the login signs in.
    #[must_use]
    pub fn with_env(mut self, env: Vec<(String, String)>) -> Self {
        self.env = env;
        self
    }

    /// The worker-loopback port the flow's server listens on: the split
    /// listener when one was named, else the callback port itself. One
    /// derivation, because the worker's attach and the relay's dial must
    /// agree on it.
    #[must_use]
    pub fn effective_listen_port(&self) -> Option<u16> {
        self.listen_port.or(self.callback_port)
    }
}

#[cfg(test)]
mod tests {
    use super::LoginRecipe;

    #[test]
    fn the_constructors_differ_exactly_in_the_callback() {
        let terminal = LoginRecipe::terminal_only("claude", vec!["login".to_owned()]);
        assert_eq!(terminal.program, "claude");
        assert_eq!(terminal.args, vec!["login".to_owned()]);
        assert_eq!(terminal.callback_port, None);
        assert_eq!(terminal.listen_port, None);

        let callback = LoginRecipe::with_callback("codex", vec!["login".to_owned()], 1455);
        assert_eq!(callback.callback_port, Some(1455));
        assert_eq!(
            LoginRecipe {
                callback_port: None,
                ..callback.clone()
            },
            LoginRecipe::terminal_only("codex", vec!["login".to_owned()]),
        );
    }

    /// The listener derivation table: split when named, the callback when
    /// not, nothing for terminal-only flows.
    #[test]
    fn the_effective_listener_prefers_the_split_port() {
        let stock = LoginRecipe::with_callback("codex", vec![], 1455);
        assert_eq!(stock.effective_listen_port(), Some(1455));

        let split = LoginRecipe::with_callback("fake-login", vec![], 1455).listening_on(14556);
        assert_eq!(split.effective_listen_port(), Some(14556));
        assert_eq!(split.callback_port, Some(1455), "the redirect is untouched");

        let terminal = LoginRecipe::terminal_only("claude", vec![]);
        assert_eq!(terminal.effective_listen_port(), None);
    }

    /// The env rides the recipe unchanged; the constructors start with none.
    #[test]
    fn the_env_is_empty_until_given_and_kept_as_given() {
        let bare = LoginRecipe::terminal_only("claude", vec![]);
        assert!(bare.env.is_empty());
        let targeted = bare.with_env(vec![(
            "CLAUDE_CONFIG_DIR".to_owned(),
            "/state/c".to_owned(),
        )]);
        assert_eq!(
            targeted.env,
            vec![("CLAUDE_CONFIG_DIR".to_owned(), "/state/c".to_owned())]
        );
        assert_eq!(targeted.program, "claude", "nothing else moves");
    }
}
