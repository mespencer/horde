//! Dual-era rmcp spike harness.
//!
//! Stands up one rmcp streamable-HTTP endpoint on loopback and puts a
//! byte-level TCP tee in front of it, so a real harness client (Claude Code,
//! codex) and a hand-rolled modern-era probe can be pointed at the *same*
//! server instance and every byte recorded.
//!
//! ```text
//! client ──▶ tee (--listen) ──▶ hyper + rmcp StreamableHttpService (ephemeral)
//!              │
//!              └─▶ transcript file
//! ```

mod server;
mod tee;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use rmcp::{
    model::ProtocolVersion,
    transport::streamable_http_server::{
        session::local::LocalSessionManager, tower::StreamableHttpServerConfig,
        StreamableHttpService,
    },
};
use tokio::net::TcpListener;

use crate::{
    server::{ObserveHeaders, SpikeServer},
    tee::Transcript,
};

#[derive(Debug, thiserror::Error)]
enum SpikeError {
    #[error("usage: rmcp-dual-era --transcript <path> [--listen <addr>] [--legacy-session-mode] [--json-response] [--stateless-metadata-required]")]
    Usage,
    #[error("missing value for {0}")]
    MissingValue(&'static str),
    #[error("unrecognised argument: {0}")]
    Unknown(String),
    #[error("bad listen address: {0}")]
    BadAddr(#[from] std::net::AddrParseError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

struct Options {
    listen: SocketAddr,
    transcript: PathBuf,
    legacy_session_mode: bool,
    json_response: bool,
    stateless_metadata_required: bool,
}

fn parse_options() -> Result<Options, SpikeError> {
    let mut listen: SocketAddr = "127.0.0.1:8931".parse()?;
    let mut transcript = None;
    let mut legacy_session_mode = false;
    let mut json_response = false;
    let mut stateless_metadata_required = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                listen = args
                    .next()
                    .ok_or(SpikeError::MissingValue("--listen"))?
                    .parse()?;
            }
            "--transcript" => {
                transcript = Some(PathBuf::from(
                    args.next().ok_or(SpikeError::MissingValue("--transcript"))?,
                ));
            }
            "--legacy-session-mode" => legacy_session_mode = true,
            "--json-response" => json_response = true,
            "--stateless-metadata-required" => stateless_metadata_required = true,
            "--help" | "-h" => return Err(SpikeError::Usage),
            other => return Err(SpikeError::Unknown(other.to_owned())),
        }
    }

    Ok(Options {
        listen,
        transcript: transcript.ok_or(SpikeError::Usage)?,
        legacy_session_mode,
        json_response,
        stateless_metadata_required,
    })
}

#[tokio::main]
async fn main() -> Result<(), SpikeError> {
    let options = parse_options()?;

    // `StreamableHttpServerConfig` is `#[non_exhaustive]`; build by mutation.
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = options.legacy_session_mode;
    config.json_response = options.json_response;
    config.stateless_protocol_metadata_required = options.stateless_metadata_required;

    // Behavioural proof of which rmcp is linked in: these constants come from
    // the dependency, not from us. Cargo.lock pins `=3.1.2`.
    eprintln!(
        "RMCP-LATEST {}\nRMCP-KNOWN-VERSIONS {}",
        ProtocolVersion::LATEST,
        ProtocolVersion::KNOWN_VERSIONS
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    eprintln!(
        "CONFIG legacy_session_mode={} json_response={} stateless_protocol_metadata_required={} allowed_hosts={:?} allowed_origins={:?} max_request_body_bytes={}",
        config.legacy_session_mode,
        config.json_response,
        config.stateless_protocol_metadata_required,
        config.allowed_hosts,
        config.allowed_origins,
        config.max_request_body_bytes,
    );

    let service = StreamableHttpService::new(
        || Ok(SpikeServer::new()),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let observed = ObserveHeaders::new(service);

    let upstream_listener = TcpListener::bind("127.0.0.1:0").await?;
    let upstream = upstream_listener.local_addr()?;

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = upstream_listener.accept().await else {
                break;
            };
            let observed = observed.clone();
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), TowerToHyperService::new(observed))
                    .with_upgrades()
                    .await;
            });
        }
    });

    let transcript = Arc::new(Transcript::create(&options.transcript).await?);
    transcript
        .note(&format!(
            "spike start listen={} upstream={} legacy_session_mode={} json_response={} stateless_metadata_required={}",
            options.listen,
            upstream,
            options.legacy_session_mode,
            options.json_response,
            options.stateless_metadata_required,
        ))
        .await?;

    let tee_listener = TcpListener::bind(options.listen).await?;
    let actual = tee_listener.local_addr()?;
    eprintln!("LISTENING http://{actual}/mcp");
    eprintln!("UPSTREAM  http://{upstream}/mcp");

    tokio::select! {
        result = tee::run(tee_listener, upstream, transcript.clone()) => { result?; }
        _ = tokio::signal::ctrl_c() => {
            transcript.note("spike shutdown (SIGINT)").await?;
        }
    }
    Ok(())
}
