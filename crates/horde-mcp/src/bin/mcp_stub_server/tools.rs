//! The stub toolset, shared by its two hosts.
//!
//! One definition, two compilations: this fixture binary serves it over
//! stdio (the child-process path a `local` config takes on a Worker), and
//! `tests/common` serves the same handler over an in-memory duplex (the
//! protocol path with no process). A copy in each host is exactly the drift
//! the duplication gate exists to stop, so the tests include this file by
//! `#[path]` instead.

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{ProtocolVersion, ServerCapabilities, ServerInfo};
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct EchoParams {
    pub(crate) message: String,
}

/// Three tools, each earning its place in a suite:
///
/// * `echo` — the round-trip probe, answering with a marker prefix so a
///   response fabricated anywhere between client and server reads wrong.
/// * `blocked` — exists to be denied: proving a deny stopped a call needs a
///   tool that would have answered, or the assertion is on silence.
/// * `whoami` — answers `HORDE_STUB_IDENTITY`, which is how a test proves a
///   config's `env` map actually reached the spawned child.
#[derive(Clone)]
pub(crate) struct StubTools {
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl StubTools {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    #[tool(name = "echo", description = "Answer the message back, marked.")]
    async fn echo(&self, Parameters(params): Parameters<EchoParams>) -> Result<String, ErrorData> {
        Ok(format!("echo:{}", params.message))
    }

    #[tool(
        name = "blocked",
        description = "Answers only if permission let it through."
    )]
    async fn blocked(&self) -> Result<String, ErrorData> {
        Ok("the chain failed to stop this".to_owned())
    }

    #[tool(
        name = "whoami",
        description = "Answer the HORDE_STUB_IDENTITY environment variable."
    )]
    async fn whoami(&self) -> Result<String, ErrorData> {
        Ok(std::env::var("HORDE_STUB_IDENTITY").unwrap_or_else(|_| "unset".to_owned()))
    }
}

impl Default for StubTools {
    fn default() -> Self {
        Self::new()
    }
}

// Dispatching through the instance's own router keeps the field live; the
// macro's default would rebuild a static router per call and leave the field
// dead.
#[tool_handler(router = self.tool_router)]
impl ServerHandler for StubTools {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.protocol_version = ProtocolVersion::LATEST;
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info.name = "horde-mcp-stub".to_owned();
        info
    }
}
