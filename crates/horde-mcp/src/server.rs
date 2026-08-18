//! Serving the endpoint: rmcp's configuration, and the identity gate in front
//! of it.
//!
//! # The configuration is a set of deliberate corrections
//!
//! Three of the settings here differ from rmcp 3.1.2's defaults, and each one
//! was measured by the dual-era spike rather than read from documentation.
//! They are written as explicit values — including the one that happens to
//! match the default — because a default that moves silently is exactly how
//! this endpoint would lose a security property between two releases.

use std::net::SocketAddr;

use rmcp::transport::streamable_http_server::tower::StreamableHttpServerConfig;

use crate::identity::AgentRegistry;

/// The mTLS termination for an endpoint bound beyond loopback (DESIGN §3).
///
/// The presented certificate is the Server credential; the peer must present
/// a certificate chaining to the CA, and revoked serials are refused *at the
/// handshake* — this channel has no per-RPC interceptor to do it later. The
/// per-agent headers keep carrying the agent identity within the channel:
/// mTLS answers "which enrolled machine", the headers answer "which agent on
/// it", and both checks run.
#[derive(Clone)]
pub struct MutualTls {
    pub credential: horde_identity::ServerCredential,
    /// The trust root the peer's certificate must chain to.
    pub ca_pem: String,
    /// The live deny-list; shared with the rest of the server's doors.
    pub deny: horde_identity::RevocationList,
}

impl std::fmt::Debug for MutualTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The credential's own Debug already withholds the key.
        f.debug_struct("MutualTls")
            .field("credential", &self.credential)
            .field("deny", &self.deny)
            .finish_non_exhaustive()
    }
}

/// How to serve one endpoint.
#[derive(Debug, Clone)]
pub struct EndpointConfig {
    /// Loopback without [`tls`](Self::tls); binding wider requires it —
    /// DESIGN §3 forbids an unauthenticated networked listener, and this
    /// endpoint hands out orchestrator verbs.
    pub bind: SocketAddr,

    /// Origins allowed to reach the endpoint from a browser.
    ///
    /// **Must be populated.** rmcp's default is an empty list, which does not
    /// mean "deny all" — it *disables* the check the MCP spec makes a MUST. The
    /// spike sent `Origin: https://evil.example.com` at a default-configured
    /// server and was served `200 OK`. The DNS-rebinding attack that clause
    /// defends against — a page in a browser on this machine reaching a
    /// loopback MCP endpoint — is precisely Horde's deployment shape.
    pub allowed_origins: Vec<String>,

    /// mTLS termination, for an endpoint reached over the network.
    pub tls: Option<MutualTls>,

    /// Host names beyond rmcp's loopback defaults that requests may carry in
    /// `Host:` — the names remote peers dial a TLS endpoint by. Empty keeps
    /// rmcp's default loopback-only list, which is what loopback serving
    /// wants.
    pub extra_hosts: Vec<String>,
}

impl EndpointConfig {
    /// A loopback endpoint on `port`, with origin checking on.
    #[must_use]
    pub fn loopback(port: u16) -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], port)),
            // The endpoint is reached by local harness processes, not by pages.
            // A single self-origin entry turns the check on without admitting
            // anything; adding a real browser origin is a deliberate act.
            allowed_origins: vec![format!("http://127.0.0.1:{port}")],
            tls: None,
            extra_hosts: Vec::new(),
        }
    }

    /// An mTLS endpoint on `bind`, reachable as `host` (DESIGN §3: the
    /// localhost-only rule lifts exactly when the channel authenticates).
    #[must_use]
    pub fn secured(bind: SocketAddr, host: &str, tls: MutualTls) -> Self {
        Self {
            bind,
            allowed_origins: vec![format!("https://{host}:{}", bind.port())],
            tls: Some(tls),
            extra_hosts: vec![host.to_owned(), format!("{host}:{}", bind.port())],
        }
    }

    /// rmcp's transport configuration for this endpoint.
    ///
    /// `StreamableHttpServerConfig` is `#[non_exhaustive]`, so it is built by
    /// mutation rather than as a struct expression — which also means a new
    /// field in a later rmcp arrives at its default and unremarked. That is the
    /// reason this crate pins `=3.1.2`.
    #[must_use]
    pub fn transport(&self) -> StreamableHttpServerConfig {
        let mut config = StreamableHttpServerConfig::default();

        // NOT the rmcp default (`false`), and the difference is not cosmetic.
        // Measured with one client, one binary, one flag apart:
        //
        //   * `GET /mcp`, the SSE stream Claude Code opens, is answered `405`
        //     under the default — so there is no channel to push
        //     `notifications/tools/list_changed` down. This endpoint changes an
        //     Agent's toolset as the Agent's state changes, and under the
        //     default it could not say so.
        //   * `clientInfo` reaching the handler is rmcp's own placeholder
        //     rather than the client's.
        //   * A legacy request is served with no `initialize` at all, so there
        //     is no handshake to hang authentication on.
        //
        // The flag is era-scoped: 2026-07-28 requests are still served
        // statelessly, and a modern client still correctly gets `405` on GET.
        config.legacy_session_mode = true;

        // Off by default, and the default fails *open*. See the field docs.
        config.allowed_origins = self.allowed_origins.clone();

        // The `Host` default (rmcp's literal `localhost`, `127.0.0.1`,
        // `::1` list) is already correct for loopback serving; the spike
        // measured a forged `Host` refused `403`, so it is left alone. When
        // this endpoint is dialed by other names, `with_allowed_hosts`
        // *replaces* the list rather than extending it, so the loopback
        // names are restated alongside the extras — dropping them would
        // break every local harness the moment the endpoint also served the
        // network.
        if !self.extra_hosts.is_empty() {
            let mut hosts = vec![
                "localhost".to_owned(),
                "127.0.0.1".to_owned(),
                "::1".to_owned(),
            ];
            hosts.extend(self.extra_hosts.iter().cloned());
            config = config.with_allowed_hosts(hosts);
        }

        config
    }
}

/// One serving endpoint: its accept loop and every connection it admitted.
///
/// Shutdown is a reap, not just an abort: the loop is told to stop, drops
/// its listener, then aborts and awaits each connection task it spawned —
/// so when [`ServingEndpoint::shutdown`] returns, every socket this
/// endpoint ever held (the listener *and* the accepted connections) is
/// provably closed. Merely aborting an accept loop leaves the connections
/// it admitted alive in detached tasks, still serving verbs and still
/// holding the port after "shutdown" claimed otherwise.
///
/// Dropping the handle instead stops the endpoint the same way, but in the
/// background, with nothing to await.
#[derive(Debug)]
pub struct ServingEndpoint {
    /// Closing this channel is the stop signal; a dropped handle closes it
    /// too, which is what makes drop a background stop.
    stop: tokio::sync::oneshot::Sender<()>,
    serving: tokio::task::JoinHandle<()>,
}

impl ServingEndpoint {
    /// Stops serving, returning only after the listener and every admitted
    /// connection have actually been dropped. Bounded: the reap is an abort
    /// plus a join, and an aborted connection resolves at its next await.
    pub async fn shutdown(self) {
        drop(self.stop);
        // The JoinError here is only ever this endpoint's own completion
        // being reported; the reap having happened is the point.
        let _ = self.serving.await;
    }
}

/// Serves one endpoint until the returned handle is shut down or dropped.
///
/// Returns the address actually bound, which is what a caller passes to the
/// harness — a config with port 0 is resolved by the OS, and guessing at it
/// afterwards is how two agents end up pointed at one endpoint.
///
/// Generic over the handler because two surfaces ride this machinery: the
/// per-Agent endpoint ([`crate::endpoint::HordeEndpoint`]) and the hub's own
/// server ([`crate::hub::HubEndpoint`]). Both get the same dual-era
/// transport, the same identity gate, and the same TLS door.
///
/// # Errors
///
/// The bind failing, which is almost always the port already being held.
pub async fn serve<H>(
    config: &EndpointConfig,
    registry: std::sync::Arc<AgentRegistry>,
    endpoint: H,
) -> std::io::Result<(std::net::SocketAddr, ServingEndpoint)>
where
    H: rmcp::ServerHandler + Clone + Sync,
{
    use rmcp::transport::streamable_http_server::{
        StreamableHttpService, session::local::LocalSessionManager,
    };

    let service = StreamableHttpService::new(
        move || Ok(endpoint.clone()),
        std::sync::Arc::new(LocalSessionManager::default()),
        config.transport(),
    );
    // The identity gate goes in front of rmcp, not inside a tool: under
    // `legacy_session_mode` a client issues `initialize`, `tools/list`, and
    // `GET /mcp` before any tool runs, and every one of those must be
    // attributable.
    let guarded = RequireIdentity::new(service, registry);

    // Built once, before accepting: an acceptor that cannot be built is a
    // startup failure, never a per-connection surprise.
    let acceptor = match &config.tls {
        Some(tls) => Some(
            horde_identity::tls::mtls_acceptor(&tls.credential, &tls.ca_pem, tls.deny.clone())
                .map_err(|error| std::io::Error::other(error.to_string()))?,
        ),
        None => None,
    };

    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    let bound = listener.local_addr()?;

    let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(async move {
        // Owned by the loop, not detached: a connection this endpoint
        // admitted is this endpoint's to reap.
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                // Resolves when the ServingEndpoint closes (or drops) the
                // channel — the stop signal either way.
                _ = &mut stopped => break,
                // Reap connections as they finish, so a long-lived endpoint
                // does not accumulate one dead handle per request ever made.
                finished = connections.join_next(), if !connections.is_empty() => {
                    // A connection ending (even by panic) is that
                    // connection's business; the reap was the point.
                    let _ = finished;
                }
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else {
                        break;
                    };
                    let guarded = guarded.clone();
                    let acceptor = acceptor.clone();
                    connections.spawn(async move {
                        match acceptor {
                            // mTLS: a peer with no certificate, a foreign
                            // one, or a revoked serial fails here and the
                            // connection ends — the refused-connection
                            // outcome for this channel.
                            Some(acceptor) => {
                                let Ok(tls) = acceptor.accept(stream).await else {
                                    return;
                                };
                                serve_connection(tls, guarded).await;
                            }
                            None => serve_connection(stream, guarded).await,
                        }
                    });
                }
            }
        }
        // The door closes before the rooms are cleared: no new connection
        // can arrive while the admitted ones are aborted and awaited.
        drop(listener);
        connections.shutdown().await;
    });

    Ok((bound, ServingEndpoint { stop, serving }))
}

/// One HTTP/1.1 connection over whichever transport the endpoint accepted.
async fn serve_connection<IO, H>(io: IO, service: RequireIdentity<RmcpService<H>>)
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    H: rmcp::ServerHandler + Clone + Sync,
{
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(
            hyper_util::rt::TokioIo::new(io),
            hyper_util::service::TowerToHyperService::new(service),
        )
        // Legacy clients open a GET/SSE stream on this connection.
        .with_upgrades()
        .await;
}

/// The concrete rmcp service the gate wraps, named so the connection helper
/// can be a function rather than a second copy of its body per transport.
type RmcpService<H> = rmcp::transport::streamable_http_server::StreamableHttpService<
    H,
    rmcp::transport::streamable_http_server::session::local::LocalSessionManager,
>;

/// Rejects any request that does not carry a valid Agent identity.
///
/// A tower layer rather than a check inside each tool, because the thing being
/// defended is not any one tool: under `legacy_session_mode` a client may issue
/// `initialize`, `notifications/initialized`, `tools/list`, and `GET /mcp`
/// before ever calling a tool, and every one of those must be attributable. The
/// spike confirmed both harnesses send the header pair on all of them.
///
/// On success the [`AgentIdentity`](crate::identity::AgentIdentity) is inserted
/// into the request's extensions, which is how it reaches a tool handler: rmcp
/// injects `http::request::Parts` into the request context, and extensions ride
/// along with it.
#[derive(Debug, Clone)]
pub struct RequireIdentity<S> {
    inner: S,
    registry: std::sync::Arc<AgentRegistry>,
}

impl<S> RequireIdentity<S> {
    pub fn new(inner: S, registry: std::sync::Arc<AgentRegistry>) -> Self {
        Self { inner, registry }
    }
}

impl<S, ReqBody, ResBody> tower_service::Service<http::Request<ReqBody>> for RequireIdentity<S>
where
    S: tower_service::Service<http::Request<ReqBody>, Response = http::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<ReqBody>) -> Self::Future {
        let identity = self.registry.authenticate(request.headers());

        let identity = match identity {
            Ok(identity) => identity,
            Err(refusal) => {
                // 401 rather than 403: the caller may retry with credentials,
                // and `WWW-Authenticate` is what says how. The reason is
                // deliberately not echoed into the body — it would tell an
                // unauthenticated caller which half of the pair it got wrong.
                tracing::warn!(%refusal, "refused an MCP request with no valid agent identity");
                // Built here rather than inside the future: `http::Error` is
                // not `Send`, so carrying the builder's `Result` across an
                // await point would infect the whole service with it.
                let response = http::Response::builder()
                    .status(http::StatusCode::UNAUTHORIZED)
                    .header(http::header::WWW_AUTHENTICATE, "Bearer")
                    .body(ResBody::default())
                    .unwrap_or_else(|_| {
                        // The builder only fails on an invalid status or
                        // header, and both of these are constants — but
                        // constructing a refusal must never be the thing that
                        // panics a request.
                        let mut fallback = http::Response::new(ResBody::default());
                        *fallback.status_mut() = http::StatusCode::UNAUTHORIZED;
                        fallback
                    });
                return Box::pin(async move { Ok(response) });
            }
        };

        request.extensions_mut().insert(identity);

        // Standard tower clone-and-swap: the clone that was polled to readiness
        // is the one that must serve the request.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move { inner.call(request).await })
    }
}

#[cfg(test)]
mod tests {
    use super::{EndpointConfig, RequireIdentity};
    use crate::identity::{AGENT_HEADER, AgentIdentity, AgentRegistry};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower_service::Service;

    /// A leaf service that records whether it was reached, and what identity
    /// arrived with the request.
    #[derive(Debug, Clone, Default)]
    struct Spy {
        reached: Arc<AtomicUsize>,
        saw: Arc<std::sync::Mutex<Option<AgentIdentity>>>,
    }

    impl Service<http::Request<String>> for Spy {
        type Response = http::Response<String>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<String>) -> Self::Future {
            self.reached.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut slot) = self.saw.lock() {
                *slot = request.extensions().get::<AgentIdentity>().cloned();
            }
            std::future::ready(Ok(http::Response::new(String::new())))
        }
    }

    fn registry() -> Arc<AgentRegistry> {
        let registry = AgentRegistry::new();
        registry.insert("agent-42", "horde-agent-42-token");
        Arc::new(registry)
    }

    fn request(headers: &[(&str, &str)]) -> http::Request<String> {
        let mut builder = http::Request::builder().uri("/mcp").method("POST");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder
            .body(String::new())
            .expect("the test request must build")
    }

    /// Puts one request through the layer, answering with what the layer
    /// returned and what the inner service saw.
    ///
    /// Every test here differs only in the headers it sends and the verdict it
    /// expects, so the arrangement is shared and the assertions are not.
    async fn call_with(headers: &[(&str, &str)]) -> (Spy, http::Response<String>) {
        let spy = Spy::default();
        let mut layer = RequireIdentity::new(spy.clone(), registry());
        let response = layer
            .call(request(headers))
            .await
            .expect("the layer answers every request, refusal included");
        (spy, response)
    }

    #[tokio::test]
    async fn an_authenticated_request_reaches_the_service_carrying_its_identity() {
        let (spy, response) = call_with(&[
            ("authorization", "Bearer horde-agent-42-token"),
            (AGENT_HEADER, "agent-42"),
        ])
        .await;

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(spy.reached.load(Ordering::SeqCst), 1);

        let seen = spy.saw.lock().ok().and_then(|slot| slot.clone());
        assert_eq!(
            seen.as_ref().map(AgentIdentity::agent_id),
            Some("agent-42"),
            "the handler reads identity from the request extensions, so the \
             layer must put it there"
        );
    }

    /// The property the whole layer exists for: an unidentified request must
    /// not reach rmcp at all. Under `legacy_session_mode` there is no handshake
    /// to stop it later.
    #[tokio::test]
    async fn an_unauthenticated_request_never_reaches_the_service() {
        let (spy, response) = call_with(&[]).await;

        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(http::header::WWW_AUTHENTICATE),
            Some(&http::HeaderValue::from_static("Bearer"))
        );
        assert_eq!(
            spy.reached.load(Ordering::SeqCst),
            0,
            "the inner service must never see an unidentified request"
        );
    }

    #[tokio::test]
    async fn a_forged_agent_header_never_reaches_the_service() {
        let (spy, response) = call_with(&[
            ("authorization", "Bearer horde-agent-42-token"),
            (AGENT_HEADER, "agent-99"),
        ])
        .await;

        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
        assert_eq!(spy.reached.load(Ordering::SeqCst), 0);
    }

    /// Identity is re-derived per request. There is no session to carry it, and
    /// the spike proved `clientInfo` cannot be trusted to.
    #[tokio::test]
    async fn identity_is_checked_on_every_request_not_just_the_first() {
        let spy = Spy::default();
        let mut layer = RequireIdentity::new(spy.clone(), registry());

        let good = request(&[
            ("authorization", "Bearer horde-agent-42-token"),
            (AGENT_HEADER, "agent-42"),
        ]);
        assert_eq!(
            layer.call(good).await.map(|r| r.status()),
            Ok(http::StatusCode::OK)
        );

        // Same connection, same layer, no credentials this time.
        let bad = request(&[]);
        assert_eq!(
            layer.call(bad).await.map(|r| r.status()),
            Ok(http::StatusCode::UNAUTHORIZED),
            "an earlier authenticated request must not authorize a later one"
        );

        assert_eq!(spy.reached.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn the_transport_config_carries_the_spikes_corrections() {
        let config = EndpointConfig::loopback(8931);
        let transport = config.transport();

        assert!(
            transport.legacy_session_mode,
            "the rmcp default costs legacy clients their GET/SSE push channel \
             and their clientInfo"
        );
        assert!(
            !transport.allowed_origins.is_empty(),
            "an empty allowed_origins DISABLES origin checking rather than \
             denying everything — the default fails open"
        );

        assert_eq!(
            transport.allowed_hosts,
            vec!["localhost", "127.0.0.1", "::1"],
            "loopback serving keeps exactly rmcp's loopback defaults"
        );
    }

    /// Extra hosts extend the loopback names rather than replacing them —
    /// `with_allowed_hosts` replaces, so getting this wrong breaks either
    /// the local harnesses or the remote peers.
    #[test]
    fn extra_hosts_join_the_loopback_names() {
        let mut config = EndpointConfig::loopback(8931);
        config.extra_hosts = vec!["horde.example".to_owned()];
        let hosts = config.transport().allowed_hosts;
        for needed in ["localhost", "127.0.0.1", "::1", "horde.example"] {
            assert!(
                hosts.contains(&needed.to_owned()),
                "{needed} missing: {hosts:?}"
            );
        }
    }

    /// `serve` stands the endpoint up behind the identity gate. The property
    /// worth asserting is not that it binds — it is that the gate is actually
    /// in front of rmcp on a real socket, because wiring the layer in the wrong
    /// order would still compile and still serve.
    #[tokio::test]
    async fn a_served_endpoint_refuses_an_unidentified_request() {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let endpoint = crate::endpoint::HordeEndpoint::new(crate::context::AgentContext::new(
            std::sync::Arc::new(crate::journal::testing::RecordingJournal::new()),
            horde_cli::client::Client::with_channel(channel),
            "s1",
        ));

        let registry = AgentRegistry::new();
        registry.insert("agent-42", "token");

        let (bound, _serving) = super::serve(
            &EndpointConfig::loopback(0),
            std::sync::Arc::new(registry),
            endpoint,
        )
        .await
        .expect("the endpoint must bind on an OS-chosen port");

        assert!(bound.ip().is_loopback());
        assert_ne!(bound.port(), 0, "the bound port must be reported back");

        // A raw request with no credentials, over the real socket.
        let stream = tokio::net::TcpStream::connect(bound)
            .await
            .expect("the endpoint must accept a connection");
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .expect("http handshake");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let request = http::Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("host", "127.0.0.1")
            .body(String::new())
            .expect("the probe request must build");

        let response = sender.send_request(request).await.expect("a response");
        assert_eq!(
            response.status(),
            http::StatusCode::UNAUTHORIZED,
            "an unidentified request must be refused before it reaches rmcp"
        );
    }

    /// `shutdown` must not *return* before the endpoint's sockets are gone —
    /// the listener and the admitted connections both. The probe is a
    /// synchronous re-bind of the bound address: between `shutdown().await`
    /// resolving and the bind there is no await point, so on this test's
    /// current-thread runtime a shutdown that merely signalled and returned
    /// (leaving the reap to run in the background) cannot slip through on a
    /// lucky poll — the serving task provably has not run in between, and
    /// only the completed reap can have freed the address.
    #[tokio::test]
    async fn shutdown_returns_only_after_the_sockets_are_reaped() {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let endpoint = crate::endpoint::HordeEndpoint::new(crate::context::AgentContext::new(
            std::sync::Arc::new(crate::journal::testing::RecordingJournal::new()),
            horde_cli::client::Client::with_channel(channel),
            "s1",
        ));
        let (bound, serving) = super::serve(
            &EndpointConfig::loopback(0),
            std::sync::Arc::new(AgentRegistry::new()),
            endpoint,
        )
        .await
        .expect("the endpoint must bind on an OS-chosen port");

        // An admitted connection, held open across the shutdown: reaping it
        // is part of what `shutdown` owes before returning.
        let admitted = tokio::net::TcpStream::connect(bound)
            .await
            .expect("the endpoint must accept a connection");

        serving.shutdown().await;

        let probe = tokio::net::TcpSocket::new_v4().expect("a probe socket");
        probe.set_reuseaddr(true).expect("reuseaddr on the probe");
        assert!(
            probe.bind(bound).is_ok(),
            "shutdown returned while the endpoint still held its socket"
        );
        drop(admitted);
    }

    /// The TLS half's Debug reaches logs; it must say what it is and hold
    /// the key back (the credential's own Debug already withholds it, and
    /// this pins that the wrapper keeps doing so).
    #[test]
    fn mutual_tls_debug_names_itself_and_never_the_key() {
        let now = horde_identity::clock::unix_now_ms();
        let ca = horde_identity::CertificateAuthority::generate(now).expect("a CA generates");
        let tls = super::MutualTls {
            credential: ca.issue_localhost_server(now).expect("issues"),
            ca_pem: ca.certificate_pem().to_owned(),
            deny: horde_identity::RevocationList::new(),
        };
        let debugged = format!("{tls:?}");
        assert!(debugged.contains("MutualTls"), "{debugged}");
        assert!(!debugged.contains("PRIVATE KEY"), "{debugged}");
    }

    #[test]
    fn the_endpoint_binds_loopback_only() {
        let config = EndpointConfig::loopback(8931);
        assert!(
            config.bind.ip().is_loopback(),
            "DESIGN §3 forbids binding beyond localhost before enrollment exists"
        );
    }
}
