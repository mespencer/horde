//! The Relay behind mTLS (DESIGN §3, DESIGN §8: "served over git smart-HTTP
//! behind the existing mTLS channel").
//!
//! The client here speaks the advertisement half of smart-HTTP itself over a
//! rustls connection, because that is the honest transport question — does
//! the door open for an enrolled certificate and stay shut for no
//! certificate and for a revoked one — while everything behind the door
//! (`git http-backend`, pack negotiation) is the same code the plaintext
//! suite already drives with a real `git`. The certificates come off the
//! production issuance path; nothing is fixture.

mod common;

#[cfg(test)]
mod suite {
    use crate::common::harness::{commit_file, init_repo, within};
    use horde_identity::{
        CertificateAuthority, Principal, PrincipalRole, RevocationList, StoredIdentity,
    };
    use tokio_rustls::rustls;

    fn now_ms() -> i64 {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_millis())
                .unwrap_or_default(),
        )
        .unwrap_or_default()
    }

    struct TlsRelay {
        server: horde_relay::RelayServer,
        _mirrors: tempfile::TempDir,
        _scratch: tempfile::TempDir,
        ca: CertificateAuthority,
        identity: StoredIdentity,
        serial_hex: String,
        deny: RevocationList,
    }

    /// A registered repo with one commit, served over mTLS, and one enrolled
    /// worker identity to fetch it with.
    async fn tls_relay() -> TlsRelay {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = within("origin init", init_repo(scratch.path(), "origin")).await;
        within(
            "a commit lands upstream",
            commit_file(&origin, "src.rs", "fn main() {}", "add src"),
        )
        .await;

        let mirrors = tempfile::tempdir().expect("a mirror dir");
        let store =
            std::sync::Arc::new(horde_relay::MirrorStore::new(mirrors.path().join("relay")));
        store
            .register("atlas", &origin.display().to_string())
            .expect("registers");

        let ca = CertificateAuthority::generate(now_ms()).expect("a CA generates");
        let credential = ca
            .issue_server(
                now_ms(),
                &["localhost".to_owned()],
                &[std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
            )
            .expect("a credential issues");

        let (key_pem, csr_pem) = horde_identity::enroll::generate_csr().expect("a CSR");
        let worker = Principal::new(PrincipalRole::Worker, "mac-studio").expect("a valid name");
        let issued = ca.sign_join(&csr_pem, &worker, now_ms()).expect("signs");
        let identity = StoredIdentity {
            certificate_pem: issued.certificate_pem,
            key_pem,
            ca_pem: ca.certificate_pem().to_owned(),
        };

        let deny = RevocationList::new();
        let server = horde_relay::RelayServer::bind_mtls(
            "127.0.0.1:0".parse().expect("an addr"),
            store,
            &credential,
            ca.certificate_pem(),
            deny.clone(),
        )
        .await
        .expect("binds mTLS");

        TlsRelay {
            server,
            _mirrors: mirrors,
            _scratch: scratch,
            ca,
            identity,
            serial_hex: issued.serial_hex,
            deny,
        }
    }

    /// One `GET /atlas/info/refs?service=git-upload-pack` over TLS — the
    /// advertisement request every clone opens with, which runs the whole
    /// bridge: route parse, lazy mirror, `git http-backend`, CGI translation.
    async fn advertise(
        relay: &TlsRelay,
        client: rustls::ClientConfig,
    ) -> Result<(http::StatusCode, String), String> {
        let tcp = tokio::net::TcpStream::connect(relay.server.addr())
            .await
            .map_err(|e| e.to_string())?;
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
        let name = rustls::pki_types::ServerName::try_from("localhost".to_owned())
            .map_err(|e| e.to_string())?;
        let tls = connector
            .connect(name, tcp)
            .await
            .map_err(|e| e.to_string())?;

        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tls))
                .await
                .map_err(|e| e.to_string())?;
        tokio::spawn(connection);

        let request = http::Request::builder()
            .method("GET")
            .uri("/atlas/info/refs?service=git-upload-pack")
            .header("host", "localhost")
            .body(http_body_util::Full::new(bytes::Bytes::new()))
            .map_err(|e| e.to_string())?;
        let response = sender
            .send_request(request)
            .await
            .map_err(|e| e.to_string())?;
        let (parts, body) = response.into_parts();
        let bytes = http_body_util::BodyExt::collect(body)
            .await
            .map_err(|e| e.to_string())?
            .to_bytes();
        Ok((parts.status, String::from_utf8_lossy(&bytes).into_owned()))
    }

    #[tokio::test]
    async fn an_enrolled_peer_is_served_the_advertisement() {
        let relay = tls_relay().await;
        let client = horde_identity::tls::client_mtls_config(&relay.identity)
            .expect("the enrolled identity builds a client config");

        let (status, body) = within("the advertisement", advertise(&relay, client))
            .await
            .expect("an enrolled peer is served");
        assert_eq!(status, http::StatusCode::OK);
        assert!(
            body.contains("service=git-upload-pack"),
            "the answer is a real smart-HTTP advertisement: {body:?}"
        );
    }

    #[tokio::test]
    async fn a_peer_without_a_certificate_is_refused_at_the_handshake() {
        let relay = tls_relay().await;

        // Trusts the CA, presents nothing.
        let mut roots = rustls::RootCertStore::empty();
        for cert in
            rustls::pki_types::pem::PemObject::pem_slice_iter(relay.ca.certificate_pem().as_bytes())
        {
            roots.add(cert.expect("the CA parses")).expect("adds");
        }
        let mut bare = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
        bare.alpn_protocols = vec![b"http/1.1".to_vec()];

        let refused = within("the refusal", advertise(&relay, bare)).await;
        assert!(
            refused.is_err(),
            "no certificate must mean no advertisement: {refused:?}"
        );
    }

    #[tokio::test]
    async fn a_revoked_peer_is_refused_at_the_handshake() {
        let relay = tls_relay().await;
        let client = horde_identity::tls::client_mtls_config(&relay.identity)
            .expect("the enrolled identity builds a client config");

        // In before the revocation…
        within(
            "the pre-revocation advertisement",
            advertise(&relay, client.clone()),
        )
        .await
        .expect("admitted before revocation");

        // …and out at the very next handshake after it.
        relay.deny.revoke(relay.serial_hex.clone());
        let refused = within("the refusal", advertise(&relay, client)).await;
        assert!(
            refused.is_err(),
            "a revoked serial must be refused by the Relay's door: {refused:?}"
        );
    }
}
