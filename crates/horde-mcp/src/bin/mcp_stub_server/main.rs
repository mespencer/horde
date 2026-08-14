//! A stub MCP server the hub upstream tests spawn as a child process.
//!
//! Test fixture wearing a `[[bin]]`'s clothes (the `stub_harness` criterion:
//! the suite *runs* it, it never *tests* it — excluded from mutation for that
//! reason). The toolset lives in [`tools`], shared with the in-memory duplex
//! tests; this file is only the stdio serving.

mod tools;

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    use rmcp::ServiceExt;

    let served = match tools::StubTools::new()
        .serve(rmcp::transport::io::stdio())
        .await
    {
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
