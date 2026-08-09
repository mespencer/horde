//! Demonstrates the W1c claims end-to-end against the real `claude` binary.
//!
//! Run from anywhere: `cargo run` inside `spikes/claude-probe/`.

use std::process::ExitCode;
use std::time::Duration;

use claude_probe::{ClaudeProbe, ProbeConfig, ProbeError, Turn};

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn report(turn: &Turn) {
    let r = &turn.result;
    println!(
        "  result       : {:?}",
        r.result.as_deref().unwrap_or("<none>")
    );
    println!(
        "  status       : subtype={} is_error={} terminal_reason={}",
        r.subtype,
        r.is_error,
        r.terminal_reason.as_deref().unwrap_or("-")
    );
    println!("  session_id   : {}", r.session_id);
    println!(
        "  tokens       : in={} out={} cache_read={} cache_write={}",
        r.usage.input_tokens,
        r.usage.output_tokens,
        r.usage.cache_read_input_tokens,
        r.usage.cache_creation_input_tokens
    );
    println!(
        "  cost/timing  : ${:.6} over {} ms ({} ms api), {} turn(s)",
        r.total_cost_usd, r.duration_ms, r.duration_api_ms, r.num_turns
    );
    for (model, usage) in &r.model_usage {
        println!(
            "  billed model : {model} -> in={} out={} cost=${:.6}",
            usage.input_tokens, usage.output_tokens, usage.cost_usd
        );
    }
    let tools = turn.tool_uses();
    if !tools.is_empty() {
        println!("  tool calls   : {}", tools.join(", "));
    }
    for denial in &r.permission_denials {
        println!("  DENIED       : {} ({})", denial.tool_name, denial.tool_use_id);
    }
    println!("  events       : {}", turn.events.len());
}

async fn run() -> Result<(), ProbeError> {
    let config = ProbeConfig {
        model: Some("haiku".to_owned()),
        tools: Some(Vec::new()),
        turn_timeout: Duration::from_secs(180),
        extra_args: vec!["--setting-sources".to_owned(), String::new()],
        ..ProbeConfig::default()
    };

    banner("1. spawn + single turn");
    let mut probe = ClaudeProbe::spawn(&config)?;
    let first = probe.run_turn("Remember the number 8613. Reply with only: OK").await?;
    report(&first);
    let session_id = first.result.session_id.clone();

    banner("2. second turn on the SAME process (context retained)");
    let second = probe
        .run_turn("What number did I ask you to remember? Reply with only the digits.")
        .await?;
    report(&second);

    banner("3. interrupt mid-turn, then keep using the process");
    probe
        .send_user_message("Count from 1 to 300, one number per line. Do not stop early.")
        .await?;
    // Let the turn actually start before aborting it.
    tokio::time::sleep(Duration::from_secs(5)).await;
    probe.interrupt().await?;
    let aborted = probe.drain_turn().await?;
    report(&aborted);

    let revived = probe.run_turn("Reply with exactly: STILL_ALIVE").await?;
    report(&revived);

    let code = probe.shutdown().await?;
    println!("\nchild exited with code {code:?}");

    banner("4. resume the dead session in a NEW process");
    let resume_config = ProbeConfig {
        resume_session_id: Some(session_id.clone()),
        ..config
    };
    let mut resumed = ClaudeProbe::spawn(&resume_config)?;
    let after = resumed
        .run_turn("What number did I ask you to remember? Reply with only the digits.")
        .await?;
    report(&after);
    let code = resumed.shutdown().await?;
    println!("\nchild exited with code {code:?}");

    banner("verdict");
    let context_survived = after.result.result.as_deref().unwrap_or("").contains("8613");
    println!("  same session id across processes : {}", after.result.session_id == session_id);
    println!("  context survived process death   : {context_survived}");
    println!("  interrupt kept process usable    : {}", revived.result.succeeded());

    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("probe failed: {err}");
            let mut source = std::error::Error::source(&err);
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}
