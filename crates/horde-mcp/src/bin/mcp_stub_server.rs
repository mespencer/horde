//! A stub MCP server the hub upstream tests spawn as a child process.
//!
//! Test fixture wearing a `[[bin]]`'s clothes (the `stub_harness` criterion:
//! the suite *runs* it, it never *tests* it — excluded from mutation for that
//! reason). It serves two tools over stdio:
//!
//! * `echo` — answers its `message` back, the round-trip probe.
//! * `whoami` — answers the `HORDE_STUB_IDENTITY` environment variable, which
//!   is how a test proves the config's `env` map actually reached the child.

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{ProtocolVersion, ServerCapabilities, ServerInfo};
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct EchoParams {
    message: String,
}

#[derive(Clone)]
struct StubServer {
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl StubServer {
    fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    #[tool(name = "echo", description = "Answer the message back verbatim.")]
    async fn echo(&self, Parameters(params): Parameters<EchoParams>) -> Result<String, ErrorData> {
        Ok(params.message)
    }

    #[tool(
        name = "whoami",
        description = "Answer the HORDE_STUB_IDENTITY environment variable."
    )]
    async fn whoami(&self) -> Result<String, ErrorData> {
        Ok(std::env::var("HORDE_STUB_IDENTITY").unwrap_or_else(|_| "unset".to_owned()))
    }
}

// Dispatching through the instance's own router keeps the field live; the
// macro's default would rebuild a static router per call and leave the field
// dead.
#[tool_handler(router = self.tool_router)]
impl ServerHandler for StubServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.protocol_version = ProtocolVersion::LATEST;
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info.name = "horde-mcp-stub".to_owned();
        info
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    use rmcp::ServiceExt;

    let served = match StubServer::new().serve(rmcp::transport::io::stdio()).await {
        Ok(served) => served,
        Err(error) => {
            eprintln!("mcp_stub_server: could not serve stdio: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    // Runs until the parent closes the pipes or kills the child — the suite
    // owns this process's lifetime.
    let _ = served.waiting().await;
    std::process::ExitCode::SUCCESS
}
