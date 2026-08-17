//! The Gherkin behavioral suite (docs/TESTING.md §2, "Behavioral").
//!
//! Feature files live in the workspace-root `features/` directory and speak
//! CONTEXT.md's language; the step definitions here are thin glue that drives
//! real production surfaces — the `horde` binary, the client library, a real
//! `horde-worker` supervising the FakeAdapter, the secured enrollment path —
//! against the real server harness. Nothing in the steps reimplements product
//! logic; assertions observe what a user observes.
//!
//! `harness = false`: cucumber owns `main`, and `run_and_exit` panics on the
//! first red scenario, which is what makes the binary's exit code honest under
//! nextest and plain `cargo test` alike.

mod steps;
mod world;

use clap::Parser as _;
use cucumber::World as _;

/// Answers the libtest discovery protocol nextest speaks to every test
/// binary, harness or not: `--list --format terse` must enumerate the tests.
/// This binary is one test. The `--ignored` probe gets an empty listing —
/// nothing here is ignored, and echoing the test to both probes would count
/// it twice.
fn answer_libtest_list(args: &[String]) -> bool {
    if !args.iter().any(|argument| argument == "--list") {
        return false;
    }
    if !args.iter().any(|argument| argument == "--ignored") {
        println!("cucumber: test");
    }
    true
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if answer_libtest_list(&args) {
        return;
    }

    // The runner's own CLI is deliberately *not* wired to argv: the test
    // runner invoking this binary speaks libtest flags (`--exact`,
    // `--nocapture`), which cucumber's clap parser would refuse.
    let opts = cucumber::cli::Opts::<_, _, _, cucumber::cli::Empty>::parse_from(["features"]);

    world::HordeWorld::cucumber()
        .with_cli(opts)
        // A step phrase with no definition must red the run, not skip it: a
        // skipped promise reads exactly like a kept one in a green summary.
        .fail_on_skipped()
        .run_and_exit(concat!(env!("CARGO_MANIFEST_DIR"), "/../../features"))
        .await;
}
