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

/// How to serve one endpoint.
#[derive(Debug, Clone)]
pub struct EndpointConfig {
    /// Loopback only. DESIGN §3 forbids binding beyond localhost until
    /// enrollment and mTLS exist, and this endpoint hands out orchestrator
    /// verbs, so it is the last thing that should reach the network early.
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

        // The `Host` default (`localhost`, `127.0.0.1`, `::1`) is already
        // correct — the spike measured a forged `Host` refused `403` — so it is
        // deliberately left alone rather than restated and allowed to drift out
        // of step with rmcp's own list.

        config
    }
}

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
        let mut registry = AgentRegistry::new();
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

    #[tokio::test]
    async fn an_authenticated_request_reaches_the_service_carrying_its_identity() {
        let spy = Spy::default();
        let mut layer = RequireIdentity::new(spy.clone(), registry());

        let response = layer
            .call(request(&[
                ("authorization", "Bearer horde-agent-42-token"),
                (AGENT_HEADER, "agent-42"),
            ]))
            .await
            .expect("the layer must not fail an authenticated request");

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
        let spy = Spy::default();
        let mut layer = RequireIdentity::new(spy.clone(), registry());

        let response = layer
            .call(request(&[]))
            .await
            .expect("a refusal is a response, not a transport failure");

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
        let spy = Spy::default();
        let mut layer = RequireIdentity::new(spy.clone(), registry());

        let response = layer
            .call(request(&[
                ("authorization", "Bearer horde-agent-42-token"),
                (AGENT_HEADER, "agent-99"),
            ]))
            .await
            .expect("a refusal is a response");

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
