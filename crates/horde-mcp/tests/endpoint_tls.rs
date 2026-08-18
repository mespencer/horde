//! The MCP endpoint behind mTLS (DESIGN §3).
//!
//! The channel authenticates the *machine*: the peer presents a certificate
//! off the production issuance path, and no certificate, a foreign CA, or a
//! revoked serial is refused at the handshake. The per-agent headers keep
//! carrying the *agent* identity within the channel — a good certificate
//! with bad headers is still 401 — so both doors are proven here, separately
//! and in order.

mod common;

use common::{Setup, harness};
use horde_identity::{
    CertificateAuthority, Principal, PrincipalRole, RevocationList, StoredIdentity,
};
use horde_mcp::endpoint::HordeEndpoint;
use horde_mcp::identity::AgentRegistry;
use horde_mcp::server::{EndpointConfig, MutualTls, serve};
// Through tokio-rustls's re-export, so the client half of these handshakes
// is pinned to exactly the rustls the acceptor was built against.
use tokio_rustls::rustls;

const TOKEN: &str = "endpoint-tls-token";

use horde_identity::clock::unix_now_ms as now_ms;

/// Everything a TLS round-trip needs: the CA, a served endpoint behind it,
/// and one enrolled machine identity.
struct Tls {
    address: std::net::SocketAddr,
    agent_id: String,
    identity: StoredIdentity,
    serial_hex: String,
    deny: RevocationList,
    ca: CertificateAuthority,
    _serving: horde_mcp::server::ServingEndpoint,
}

async fn served_tls() -> Setup<Tls> {
    let horde = harness().await?;
    let ca = CertificateAuthority::generate(now_ms())?;
    let credential = ca.issue_localhost_server(now_ms())?;

    // The peer machine, enrolled the production way: keypair here, CSR
    // signed by the CA, subject written by the CA.
    let (key_pem, csr_pem) = horde_identity::enroll::generate_csr()?;
    let worker = Principal::new(PrincipalRole::Worker, "mac-studio")?;
    let issued = ca.sign_join(&csr_pem, &worker, now_ms())?;
    let identity = StoredIdentity {
        certificate_pem: issued.certificate_pem,
        key_pem,
        ca_pem: ca.certificate_pem().to_owned(),
    };

    let registry = AgentRegistry::new();
    registry.insert(&horde.agent_id, TOKEN);
    let deny = RevocationList::new();

    let config = EndpointConfig::secured(
        std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        "localhost",
        MutualTls {
            credential,
            ca_pem: ca.certificate_pem().to_owned(),
            deny: deny.clone(),
        },
    );
    let (address, serving) = serve(
        &config,
        std::sync::Arc::new(registry),
        HordeEndpoint::new(horde.context().await?),
    )
    .await?;

    Ok(Tls {
        address,
        agent_id: horde.agent_id,
        identity,
        serial_hex: issued.serial_hex,
        deny,
        ca,
        _serving: serving,
    })
}

/// One legacy-era `initialize` POST over TLS, with or without the agent
/// header pair; answers the HTTP status.
///
/// Errors are connection-shaped refusals (the handshake never yielding a
/// channel), which is exactly the outcome half these tests assert.
async fn initialize_over(
    address: std::net::SocketAddr,
    client: rustls::ClientConfig,
    agent: Option<(&str, &str)>,
) -> Setup<http::StatusCode> {
    let tcp = tokio::net::TcpStream::connect(address).await?;
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
    let name = rustls::pki_types::ServerName::try_from("localhost".to_owned())?;
    let tls = connector.connect(name, tcp).await?;

    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tls)).await?;
    tokio::spawn(connection);

    let mut request = http::Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some((agent_id, token)) = agent {
        request = request
            .header("authorization", format!("Bearer {token}"))
            .header("x-horde-agent", agent_id);
    }
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": { "name": "tls-test", "version": "0" },
        },
    });
    let response = sender
        .send_request(
            request.body(http_body_util::Full::new(hyper::body::Bytes::from(
                body.to_string(),
            )))?,
        )
        .await?;
    Ok(response.status())
}

/// A client config trusting the CA but presenting nothing.
fn bare_client(ca: &CertificateAuthority) -> Setup<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls::pki_types::pem::PemObject::pem_slice_iter(ca.certificate_pem().as_bytes()) {
        roots.add(cert?).map_err(|e| e.to_string())?;
    }
    let mut config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| e.to_string())?
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

#[tokio::test]
async fn an_enrolled_machine_reaches_rmcp_and_the_agent_headers_still_rule_within() {
    let tls = served_tls().await.expect("the TLS endpoint serves");
    let client = horde_identity::tls::client_mtls_config(&tls.identity)
        .expect("the enrolled identity builds a client config");

    // Machine door open, agent door open: the request reaches rmcp.
    let status = initialize_over(tls.address, client.clone(), Some((&tls.agent_id, TOKEN)))
        .await
        .expect("an enrolled machine completes the round-trip");
    assert_eq!(status, http::StatusCode::OK);

    // Machine door open, agent door shut: 401 from the identity layer —
    // the certificate authenticates the worker, never any agent on it.
    let status = initialize_over(tls.address, client.clone(), None)
        .await
        .expect("the channel itself still answers");
    assert_eq!(status, http::StatusCode::UNAUTHORIZED);

    let status = initialize_over(tls.address, client, Some((&tls.agent_id, "wrong-token")))
        .await
        .expect("the channel itself still answers");
    assert_eq!(status, http::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_machine_without_a_certificate_is_refused_at_the_handshake() {
    let tls = served_tls().await.expect("the TLS endpoint serves");
    let bare = bare_client(&tls.ca).expect("a certificate-less client config");

    let refused = initialize_over(tls.address, bare, Some((&tls.agent_id, TOKEN))).await;
    assert!(
        refused.is_err(),
        "no client certificate must mean no channel, headers notwithstanding: {refused:?}"
    );
}

#[tokio::test]
async fn a_revoked_machine_is_refused_at_the_handshake() {
    let tls = served_tls().await.expect("the TLS endpoint serves");
    let client = horde_identity::tls::client_mtls_config(&tls.identity)
        .expect("the enrolled identity builds a client config");

    // Sanity: admitted before revocation…
    let status = initialize_over(tls.address, client.clone(), Some((&tls.agent_id, TOKEN)))
        .await
        .expect("admitted before revocation");
    assert_eq!(status, http::StatusCode::OK);

    // …and refused at the very next handshake after.
    tls.deny.revoke(tls.serial_hex.clone());
    let refused = initialize_over(tls.address, client, Some((&tls.agent_id, TOKEN))).await;
    assert!(
        refused.is_err(),
        "a revoked serial must be refused by the acceptor: {refused:?}"
    );
}
