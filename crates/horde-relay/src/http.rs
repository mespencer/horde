//! The Relay served: git smart-HTTP over `git http-backend`.
//!
//! ADR-0002 rules that cross-Worker transport is the Server's mirrors served
//! over git smart-HTTP behind the existing authenticated channel, and names
//! `git http-backend` as the mechanism. This module is that bridge: parse the
//! two smart-HTTP routes, hand the request to `http-backend` as CGI, and
//! translate its CGI answer back into HTTP. Everything the protocol makes
//! hard — pack negotiation, gzipped request bodies, protocol v2 — stays
//! git's problem, which is the point of using the tool the ruling names.
//!
//! # Authentication
//!
//! The Relay rides the Server's channel (DESIGN §8). [`RelayServer::bind`]
//! is the bootstrap plaintext listener and stays loopback-only;
//! [`RelayServer::bind_mtls`] is the wave-5 door — the peer presents a
//! certificate from the Server's CA, revoked serials are refused at the
//! handshake, and the certificate's principal lands in `REMOTE_USER`, the
//! seam W4b left ready. Bodies are buffered whole rather than streamed —
//! honest for the repos Horde relays today, and a contained upgrade when it
//! stops being — and buffering is bounded: a body past
//! [`MAX_REQUEST_BODY_BYTES`] is refused `413` before it can exhaust memory.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::io::AsyncWriteExt;

use crate::mirror::{MirrorError, MirrorStore, is_valid_repo_name};

/// The two smart-HTTP services.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitService {
    /// Serves fetches and clones.
    UploadPack,
    /// Receives pushes.
    ReceivePack,
}

impl GitService {
    /// The wire name, exactly as it appears in paths and `service=` queries.
    #[must_use]
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::UploadPack => "git-upload-pack",
            Self::ReceivePack => "git-receive-pack",
        }
    }

    fn from_wire(name: &str) -> Option<Self> {
        match name {
            "git-upload-pack" => Some(Self::UploadPack),
            "git-receive-pack" => Some(Self::ReceivePack),
            _ => None,
        }
    }
}

/// A parsed smart-HTTP request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayRoute {
    /// `GET /{repo}/info/refs?service=…` — ref advertisement.
    Advertise { repo: String, service: GitService },
    /// `POST /{repo}/{service}` — the pack exchange itself.
    Exchange { repo: String, service: GitService },
}

impl RelayRoute {
    fn repo(&self) -> &str {
        match self {
            Self::Advertise { repo, .. } | Self::Exchange { repo, .. } => repo,
        }
    }
}

/// The repo segment, with the optional `.git` clients like to append.
fn repo_segment(segment: &str) -> Option<&str> {
    let name = segment.strip_suffix(".git").unwrap_or(segment);
    is_valid_repo_name(name).then_some(name)
}

/// The `service=` value in a query string, undecoded.
fn service_query(query: Option<&str>) -> Option<&str> {
    query?
        .split('&')
        .find_map(|pair| pair.strip_prefix("service="))
}

/// Parses the two routes git smart-HTTP defines; anything else is `None`.
///
/// Pure and strict on purpose: the path segments become filesystem-adjacent
/// `PATH_INFO`, so unknown shapes, traversal attempts, and percent-escapes
/// (no valid repo name needs one) all fall out here, before anything touches
/// disk.
#[must_use]
pub fn parse_route(method: &Method, path: &str, query: Option<&str>) -> Option<RelayRoute> {
    let mut segments = path.split('/').filter(|segment| !segment.is_empty());
    let repo = repo_segment(segments.next()?)?.to_owned();
    let second = segments.next()?;
    let third = segments.next();
    if segments.next().is_some() {
        return None;
    }

    match (method, second, third) {
        (&Method::GET, "info", Some("refs")) => Some(RelayRoute::Advertise {
            repo,
            service: GitService::from_wire(service_query(query)?)?,
        }),
        (&Method::POST, service, None) => Some(RelayRoute::Exchange {
            repo,
            service: GitService::from_wire(service)?,
        }),
        _ => None,
    }
}

/// What `git http-backend` answered, still in CGI clothes.
#[derive(Debug, PartialEq, Eq)]
pub struct CgiResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

/// The status code in a CGI `Status:` value, e.g. `404 Not Found`.
fn cgi_status(value: &str) -> Option<u16> {
    value.split_whitespace().next()?.parse().ok()
}

/// Splits raw CGI output into headers and body.
///
/// CGI is headers, a blank line, then the body; `git http-backend` writes
/// CRLF line endings. `None` means the output is not CGI at all — the bridge
/// reports that as a bad gateway rather than serving garbage as a pack.
#[must_use]
pub fn parse_cgi_response(raw: &[u8]) -> Option<CgiResponse> {
    let split = raw.windows(4).position(|window| window == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&raw[..split]).ok()?;
    let body = Bytes::copy_from_slice(&raw[split + 4..]);

    let mut status = 200;
    let mut headers = Vec::new();
    for line in head.split("\r\n") {
        let (name, value) = line.split_once(':')?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("Status") {
            status = cgi_status(value)?;
        } else {
            headers.push((name.to_owned(), value.to_owned()));
        }
    }
    Some(CgiResponse {
        status,
        headers,
        body,
    })
}

/// Whether a CGI header may be copied onto the HTTP response.
///
/// Framing is hyper's job: the body is re-framed by this bridge, so a stale
/// `Content-Length` or `Transfer-Encoding` from the CGI side would describe
/// bytes hyper is not sending. Connection management is likewise not the
/// backend's to dictate.
fn is_forwardable_header(name: &str) -> bool {
    !(name.eq_ignore_ascii_case("content-length")
        || name.eq_ignore_ascii_case("transfer-encoding")
        || name.eq_ignore_ascii_case("connection"))
}

/// The CGI answer as the HTTP response the git client will read.
#[must_use]
pub fn cgi_to_http(cgi: CgiResponse) -> Response<Full<Bytes>> {
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(cgi.status).unwrap_or(StatusCode::BAD_GATEWAY));
    for (name, value) in &cgi.headers {
        if is_forwardable_header(name) {
            builder = builder.header(name, value);
        }
    }
    builder
        .body(Full::new(cgi.body))
        .unwrap_or_else(|_| plain_response(StatusCode::BAD_GATEWAY, "unrepresentable CGI answer"))
}

/// A plain-text response, for refusals and bridge failures.
fn plain_response(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(format!("{message}\n"))));
    *response.status_mut() = status;
    response
}

/// Why the bridge could not answer, and the HTTP shape of that.
#[derive(Debug, thiserror::Error)]
enum BridgeError {
    #[error(transparent)]
    Mirror(#[from] MirrorError),

    #[error("git http-backend failed: {0}")]
    Backend(#[from] crate::git::GitError),

    #[error("git http-backend answered something that is not CGI")]
    NotCgi,
}

impl BridgeError {
    fn into_response(self) -> Response<Full<Bytes>> {
        let status = match &self {
            Self::Mirror(MirrorError::UnknownRepo { .. }) => StatusCode::NOT_FOUND,
            _ => StatusCode::BAD_GATEWAY,
        };
        plain_response(status, &self.to_string())
    }
}

/// The transport-level facts about one request, as CGI wants them.
///
/// A struct rather than six more parameters: every field rides together from
/// the connection into [`cgi_environment`], and six positionals of mostly
/// `Option<&str>` are indistinguishable at a call site.
#[derive(Debug, Clone, Copy)]
pub struct ExchangeContext<'a> {
    pub content_type: Option<&'a str>,
    pub content_encoding: Option<&'a str>,
    pub git_protocol: Option<&'a str>,
    pub content_length: usize,
    pub peer: SocketAddr,
    /// The authenticated principal (W4b's seam, filled by wave 5): the mTLS
    /// door maps the peer certificate's principal into `REMOTE_USER`; the
    /// bootstrap plaintext listener still says `horde`.
    pub remote_user: &'a str,
}

/// The CGI environment for one routed request.
///
/// A pure builder so the variables — each one load-bearing for
/// `http-backend` — are assertable without a process.
#[must_use]
pub fn cgi_environment(
    route: &RelayRoute,
    project_root: &Path,
    context: &ExchangeContext<'_>,
) -> Vec<(String, String)> {
    let dir = MirrorStore::dir_name(route.repo());
    let mut env = vec![
        (
            "GIT_PROJECT_ROOT".to_owned(),
            project_root.display().to_string(),
        ),
        // The store serves exactly the mirrors it materialized; the
        // per-repo `git-daemon-export-ok` opt-in guards nothing here.
        ("GIT_HTTP_EXPORT_ALL".to_owned(), "1".to_owned()),
        // Where the authenticated principal lands. Present unconditionally
        // because http-backend enables receive-pack for authenticated users,
        // and the Relay's authentication lives on the channel, not in HTTP.
        ("REMOTE_USER".to_owned(), context.remote_user.to_owned()),
        ("REMOTE_ADDR".to_owned(), context.peer.ip().to_string()),
    ];
    match route {
        RelayRoute::Advertise { service, .. } => {
            env.push(("REQUEST_METHOD".to_owned(), "GET".to_owned()));
            env.push(("PATH_INFO".to_owned(), format!("/{dir}/info/refs")));
            env.push((
                "QUERY_STRING".to_owned(),
                format!("service={}", service.wire_name()),
            ));
        }
        RelayRoute::Exchange { service, .. } => {
            env.push(("REQUEST_METHOD".to_owned(), "POST".to_owned()));
            env.push((
                "PATH_INFO".to_owned(),
                format!("/{dir}/{}", service.wire_name()),
            ));
            env.push(("QUERY_STRING".to_owned(), String::new()));
            env.push((
                "CONTENT_LENGTH".to_owned(),
                context.content_length.to_string(),
            ));
        }
    }
    if let Some(content_type) = context.content_type {
        env.push(("CONTENT_TYPE".to_owned(), content_type.to_owned()));
    }
    if let Some(encoding) = context.content_encoding {
        // http-backend inflates gzipped request bodies itself, but only if
        // told they are gzipped.
        env.push(("HTTP_CONTENT_ENCODING".to_owned(), encoding.to_owned()));
    }
    if let Some(protocol) = context.git_protocol {
        // The `Git-Protocol` header is how a client asks for protocol v2;
        // http-backend reads it from this variable.
        env.push(("HTTP_GIT_PROTOCOL".to_owned(), protocol.to_owned()));
    }
    env
}

/// The largest request body the Relay buffers: 1 GiB. A push's pack is the
/// only large body git sends, and one bigger than this is far past what the
/// whole-body buffering above was sized for — refusing it with a named
/// `413` beats letting one client exhaust the Server's memory.
pub const MAX_REQUEST_BODY_BYTES: usize = 1 << 30;

/// One Relay: a mirror store plus the bridge that serves it.
#[derive(Debug, Clone)]
pub struct Relay {
    store: Arc<MirrorStore>,
    body_limit: usize,
}

impl Relay {
    #[must_use]
    pub fn new(store: Arc<MirrorStore>) -> Self {
        Self::with_body_limit(store, MAX_REQUEST_BODY_BYTES)
    }

    /// A Relay that refuses request bodies longer than `body_limit` bytes.
    #[must_use]
    pub fn with_body_limit(store: Arc<MirrorStore>, body_limit: usize) -> Self {
        Self { store, body_limit }
    }

    /// Answers one HTTP request. Infallible by design: every failure has an
    /// HTTP shape, and a bridge that errors the connection instead of the
    /// request tells the git client nothing.
    pub async fn respond<B>(
        &self,
        request: Request<B>,
        peer: SocketAddr,
        remote_user: &str,
    ) -> Response<Full<Bytes>>
    where
        B: hyper::body::Body,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let (parts, body) = request.into_parts();
        let Some(route) = parse_route(&parts.method, parts.uri.path(), parts.uri.query()) else {
            return plain_response(
                StatusCode::NOT_FOUND,
                "not a git smart-HTTP route this Relay serves",
            );
        };
        let body = match Limited::new(body, self.body_limit).collect().await {
            Ok(body) => body,
            Err(error) => return body_refusal(&*error, self.body_limit),
        };

        let header = |name: &str| {
            parts
                .headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        let content_type = header("content-type");
        let content_encoding = header("content-encoding");
        let git_protocol = header("git-protocol");
        let body = body.to_bytes();
        let context = ExchangeContext {
            content_type: content_type.as_deref(),
            content_encoding: content_encoding.as_deref(),
            git_protocol: git_protocol.as_deref(),
            content_length: body.len(),
            peer,
            remote_user,
        };
        match self.exchange(&route, &context, body).await {
            Ok(cgi) => cgi_to_http(cgi),
            Err(error) => error.into_response(),
        }
    }

    /// Materializes the mirror if needed, then runs the CGI exchange.
    async fn exchange(
        &self,
        route: &RelayRoute,
        context: &ExchangeContext<'_>,
        body: Bytes,
    ) -> Result<CgiResponse, BridgeError> {
        // Lazy mirroring happens here: the first request for a registered
        // repo is what clones it.
        self.store.ensure(route.repo()).await?;
        let env = cgi_environment(route, self.store.root(), context);
        run_http_backend(&env, &body).await
    }
}

/// The answer to a body that could not be buffered: `413` naming the limit
/// when it was too long, `400` when it could not be read at all.
fn body_refusal(
    error: &(dyn std::error::Error + Send + Sync + 'static),
    limit: usize,
) -> Response<Full<Bytes>> {
    if error.is::<http_body_util::LengthLimitError>() {
        plain_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            &format!("the request body exceeds the Relay's {limit}-byte limit"),
        )
    } else {
        plain_response(
            StatusCode::BAD_REQUEST,
            "the request body could not be read",
        )
    }
}

/// Runs `git http-backend` once and parses its CGI answer.
async fn run_http_backend(
    env: &[(String, String)],
    body: &Bytes,
) -> Result<CgiResponse, BridgeError> {
    let mut command = tokio::process::Command::new("git");
    command
        .arg("http-backend")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (name, value) in env {
        command.env(name, value);
    }
    let mut child = command
        .spawn()
        .map_err(|source| crate::git::GitError::Spawn {
            command: "http-backend".to_owned(),
            source,
        })?;

    let mut stdin = child.stdin.take();
    let body = body.clone();
    // Written concurrently with the read: receive-pack streams progress while
    // still consuming input, and a full pipe in either direction would
    // deadlock a write-then-read sequence.
    let write = async move {
        if let Some(stdin) = stdin.as_mut() {
            // A write error here means the backend stopped reading — its
            // answer (or its absence) is the verdict, so the error itself
            // carries no extra information.
            let _ = stdin.write_all(&body).await;
            let _ = stdin.shutdown().await;
        }
        drop(stdin);
    };
    let (_, output) = tokio::join!(write, child.wait_with_output());
    let output = output.map_err(|source| crate::git::GitError::Spawn {
        command: "http-backend".to_owned(),
        source,
    })?;

    if !output.status.success() {
        return Err(BridgeError::Backend(crate::git::GitError::Failed {
            command: "http-backend".to_owned(),
            dir: String::new(),
            status: output
                .status
                .code()
                .map(|code| format!(" (exit {code})"))
                .unwrap_or_default(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        }));
    }
    parse_cgi_response(&output.stdout).ok_or(BridgeError::NotCgi)
}

/// A Relay bound to a socket and serving.
///
/// Dropping it stops the listener. Serving stops with the task; in-flight
/// exchanges are cut, which is safe because git's protocols are transactional
/// — an interrupted push updates nothing.
#[derive(Debug)]
pub struct RelayServer {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl RelayServer {
    /// Binds `addr` and serves `store` until dropped.
    ///
    /// The caller chooses the address deliberately: the Relay's
    /// authentication is the channel it rides (DESIGN §8), so binding beyond
    /// localhost is only correct behind the mTLS listener that wave 5 adds.
    ///
    /// # Errors
    ///
    /// The bind failure, verbatim.
    pub async fn bind(addr: SocketAddr, store: Arc<MirrorStore>) -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let relay = Relay::new(store);
        let task = tokio::spawn(accept_loop(listener, relay));
        Ok(Self { addr, task })
    }

    /// Binds `addr` and serves `store` over mTLS until dropped (DESIGN §3).
    ///
    /// Every connection must present a certificate chaining to the CA;
    /// revoked serials are refused at the handshake (this channel has no
    /// per-request interceptor); and the certificate's principal becomes
    /// `REMOTE_USER` for `git http-backend` — the credential seam W4b left.
    ///
    /// # Errors
    ///
    /// The bind failure, or unusable TLS material, as `io::Error` verbatim.
    pub async fn bind_mtls(
        addr: SocketAddr,
        store: Arc<MirrorStore>,
        credential: &horde_identity::ServerCredential,
        ca_pem: &str,
        deny: horde_identity::RevocationList,
    ) -> std::io::Result<Self> {
        let acceptor = horde_identity::tls::mtls_acceptor(credential, ca_pem, deny)
            .map_err(std::io::Error::other)?;
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let relay = Relay::new(store);
        let task = tokio::spawn(accept_mtls_loop(listener, acceptor, relay));
        Ok(Self { addr, task })
    }

    /// Where the Relay is listening.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The base URL Workers reach this Relay at.
    #[must_use]
    pub fn location(&self) -> crate::transport::RelayLocation {
        crate::transport::RelayLocation::new(&format!("http://{}", self.addr))
    }
}

impl Drop for RelayServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Accepts connections until the listener errors.
///
/// A failed accept ends the loop rather than retrying: the plausible errors
/// (fd exhaustion, a closed listener) do not fix themselves, and a retry loop
/// here would be exactly the unbounded hot spin the quality gates forbid. A
/// dead Relay is observable — the next fetch through it fails loudly.
async fn accept_loop(listener: tokio::net::TcpListener, relay: Relay) {
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            return;
        };
        let relay = relay.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let relay = relay.clone();
                async move {
                    // The plaintext bootstrap listener has no principal to
                    // map; `horde` is its one ambient user (DESIGN §8).
                    Ok::<_, Infallible>(relay.respond(request, peer, "horde").await)
                }
            });
            // A connection-level error is the client hanging up; the
            // request-level answer already happened or never will.
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

/// Accepts mTLS connections until the listener errors.
///
/// A failed *handshake* only ends that connection — an unenrolled prober
/// must not be able to stop the Relay — while a failed accept ends the loop
/// for the same reasons as [`accept_loop`].
async fn accept_mtls_loop(
    listener: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    relay: Relay,
) {
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            return;
        };
        let acceptor = acceptor.clone();
        let relay = relay.clone();
        tokio::spawn(async move {
            // The refused-connection outcome: no certificate, a foreign CA,
            // or a revoked serial dies here, before any route is parsed.
            let Ok(tls) = acceptor.accept(stream).await else {
                return;
            };
            let principal = match horde_identity::tls::peer_of_stream(&tls) {
                Ok(identity) => identity.principal.to_string(),
                // The acceptor requires a client certificate, so this is
                // unreachable in practice; refusing the connection is still
                // righter than inventing a principal.
                Err(_) => return,
            };
            let service = service_fn(move |request| {
                let relay = relay.clone();
                let principal = principal.clone();
                async move { Ok::<_, Infallible>(relay.respond(request, peer, &principal).await) }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(tls), service)
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CgiResponse, GitService, RelayRoute, cgi_environment, cgi_to_http, parse_cgi_response,
        parse_route,
    };
    use http::Method;

    async fn answer_to_body(limit: usize, body: &'static [u8]) -> http::StatusCode {
        let scratch = tempfile::tempdir().expect("scratch dir");
        let store = std::sync::Arc::new(super::MirrorStore::new(scratch.path().to_path_buf()));
        let relay = super::Relay::with_body_limit(store, limit);
        let request = http::Request::post("/atlas/git-upload-pack")
            .body(http_body_util::Full::new(bytes::Bytes::from_static(body)))
            .expect("request");
        let peer = "127.0.0.1:9".parse().expect("peer");
        relay.respond(request, peer, "horde").await.status()
    }

    /// A body past the limit is refused `413` before any mirror is touched;
    /// one exactly at it reaches the store (which knows no `atlas`: `404`).
    #[tokio::test]
    async fn a_request_body_past_the_limit_is_refused_payload_too_large() {
        assert_eq!(
            answer_to_body(4, b"12345").await,
            http::StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            answer_to_body(5, b"12345").await,
            http::StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn the_body_refusal_tells_a_length_error_from_an_unreadable_body() {
        use http_body_util::BodyExt;
        let too_long = http_body_util::Limited::new(
            http_body_util::Full::new(bytes::Bytes::from_static(b"12345")),
            4,
        )
        .collect()
        .await
        .expect_err("past the limit");
        assert_eq!(
            super::body_refusal(&*too_long, 7).status(),
            http::StatusCode::PAYLOAD_TOO_LARGE
        );
        let unreadable = std::io::Error::other("reset");
        assert_eq!(
            super::body_refusal(&unreadable, 7).status(),
            http::StatusCode::BAD_REQUEST
        );
    }

    /// The four requests a real git client sends, and nothing else.
    #[test]
    fn the_smart_http_routes_parse_and_the_rest_do_not() {
        assert_eq!(
            parse_route(
                &Method::GET,
                "/atlas/info/refs",
                Some("service=git-upload-pack"),
            ),
            Some(RelayRoute::Advertise {
                repo: "atlas".to_owned(),
                service: GitService::UploadPack,
            })
        );
        assert_eq!(
            parse_route(
                &Method::GET,
                "/atlas.git/info/refs",
                Some("service=git-receive-pack"),
            ),
            Some(RelayRoute::Advertise {
                repo: "atlas".to_owned(),
                service: GitService::ReceivePack,
            }),
            "clients append .git; the route must shed it"
        );
        assert_eq!(
            parse_route(&Method::POST, "/atlas/git-upload-pack", None),
            Some(RelayRoute::Exchange {
                repo: "atlas".to_owned(),
                service: GitService::UploadPack,
            })
        );
        assert_eq!(
            parse_route(&Method::POST, "/atlas/git-receive-pack", None),
            Some(RelayRoute::Exchange {
                repo: "atlas".to_owned(),
                service: GitService::ReceivePack,
            })
        );
    }

    #[test]
    fn hostile_and_malformed_requests_fall_out_of_the_route_parser() {
        let cases: &[(Method, &str, Option<&str>)] = &[
            // Traversal, in both spellings.
            (
                Method::GET,
                "/../etc/info/refs",
                Some("service=git-upload-pack"),
            ),
            (
                Method::GET,
                "/%2e%2e/info/refs",
                Some("service=git-upload-pack"),
            ),
            // Wrong methods for each route.
            (
                Method::POST,
                "/atlas/info/refs",
                Some("service=git-upload-pack"),
            ),
            (Method::GET, "/atlas/git-upload-pack", None),
            // Missing or unknown service.
            (Method::GET, "/atlas/info/refs", None),
            (
                Method::GET,
                "/atlas/info/refs",
                Some("service=git-evil-pack"),
            ),
            (Method::POST, "/atlas/git-evil-pack", None),
            // Shapes that are not routes at all.
            (Method::GET, "/", None),
            (Method::GET, "/atlas", None),
            (
                Method::GET,
                "/atlas/info/refs/extra",
                Some("service=git-upload-pack"),
            ),
            (Method::POST, "/a/b/git-upload-pack", None),
        ];
        for (method, path, query) in cases {
            assert_eq!(
                parse_route(method, path, *query),
                None,
                "{method} {path} {query:?} must not route"
            );
        }
    }

    #[test]
    fn the_service_query_is_found_among_other_pairs() {
        assert_eq!(
            parse_route(
                &Method::GET,
                "/atlas/info/refs",
                Some("a=b&service=git-upload-pack"),
            ),
            Some(RelayRoute::Advertise {
                repo: "atlas".to_owned(),
                service: GitService::UploadPack,
            })
        );
    }

    /// A real http-backend advertisement head, verbatim shape.
    #[test]
    fn cgi_output_parses_into_status_headers_and_body() {
        let raw = b"Content-Type: application/x-git-upload-pack-advertisement\r\n\
                    Cache-Control: no-cache\r\n\r\n001e# service=git-upload-pack\n";
        let parsed = parse_cgi_response(raw).expect("well-formed CGI parses");
        assert_eq!(parsed.status, 200, "no Status header means 200");
        assert_eq!(
            parsed.headers,
            vec![
                (
                    "Content-Type".to_owned(),
                    "application/x-git-upload-pack-advertisement".to_owned(),
                ),
                ("Cache-Control".to_owned(), "no-cache".to_owned()),
            ]
        );
        assert_eq!(&parsed.body[..], b"001e# service=git-upload-pack\n");
    }

    #[test]
    fn a_cgi_status_header_sets_the_http_status() {
        let raw = b"Status: 404 Not Found\r\nContent-Type: text/plain\r\n\r\ngone";
        let parsed = parse_cgi_response(raw).expect("parses");
        assert_eq!(parsed.status, 404);
        assert_eq!(&parsed.body[..], b"gone");
    }

    #[test]
    fn non_cgi_output_is_refused_rather_than_served() {
        assert_eq!(parse_cgi_response(b"fatal: something broke\n"), None);
        assert_eq!(parse_cgi_response(b""), None);
        assert_eq!(
            parse_cgi_response(b"NoColonHere\r\n\r\nbody"),
            None,
            "a headerless blob is not CGI"
        );
    }

    /// Framing headers must not survive the bridge: hyper re-frames the body,
    /// and a stale Content-Length would describe bytes it is not sending.
    #[test]
    fn framing_headers_are_dropped_and_the_rest_forwarded() {
        let response = cgi_to_http(CgiResponse {
            status: 200,
            headers: vec![
                (
                    "Content-Type".to_owned(),
                    "application/x-git-upload-pack-result".to_owned(),
                ),
                ("Content-Length".to_owned(), "999999".to_owned()),
                ("Transfer-Encoding".to_owned(), "chunked".to_owned()),
                ("Connection".to_owned(), "close".to_owned()),
            ],
            body: bytes::Bytes::from_static(b"pack"),
        });
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .map(|value| value.as_bytes()),
            Some(b"application/x-git-upload-pack-result".as_slice())
        );
        for absent in ["content-length", "transfer-encoding", "connection"] {
            assert!(
                response.headers().get(absent).is_none(),
                "{absent} must not be forwarded"
            );
        }
    }

    #[test]
    fn an_unrepresentable_cgi_status_becomes_a_bad_gateway() {
        let response = cgi_to_http(CgiResponse {
            status: 42,
            headers: Vec::new(),
            body: bytes::Bytes::new(),
        });
        assert_eq!(response.status(), http::StatusCode::BAD_GATEWAY);
    }

    fn env_value<'a>(env: &'a [(String, String)], name: &str) -> Option<&'a str> {
        env.iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// Every variable http-backend reads, for both routes. Each one is
    /// load-bearing: a wrong PATH_INFO serves the wrong repo, a missing
    /// REMOTE_USER refuses every push, a missing CONTENT_LENGTH truncates
    /// every request body.
    #[test]
    fn the_cgi_environment_carries_what_http_backend_reads() {
        let peer: std::net::SocketAddr = "127.0.0.1:40000".parse().expect("an addr");
        let advertise = cgi_environment(
            &RelayRoute::Advertise {
                repo: "atlas".to_owned(),
                service: GitService::UploadPack,
            },
            std::path::Path::new("/relay"),
            &super::ExchangeContext {
                content_type: None,
                content_encoding: None,
                git_protocol: Some("version=2"),
                content_length: 0,
                peer,
                remote_user: "horde",
            },
        );
        assert_eq!(env_value(&advertise, "GIT_PROJECT_ROOT"), Some("/relay"));
        assert_eq!(env_value(&advertise, "GIT_HTTP_EXPORT_ALL"), Some("1"));
        assert_eq!(env_value(&advertise, "REQUEST_METHOD"), Some("GET"));
        assert_eq!(
            env_value(&advertise, "PATH_INFO"),
            Some("/atlas.git/info/refs")
        );
        assert_eq!(
            env_value(&advertise, "QUERY_STRING"),
            Some("service=git-upload-pack")
        );
        assert_eq!(env_value(&advertise, "REMOTE_USER"), Some("horde"));
        assert_eq!(env_value(&advertise, "REMOTE_ADDR"), Some("127.0.0.1"));
        assert_eq!(
            env_value(&advertise, "HTTP_GIT_PROTOCOL"),
            Some("version=2")
        );
        assert_eq!(
            env_value(&advertise, "CONTENT_LENGTH"),
            None,
            "a GET carries no body to describe"
        );

        let exchange = cgi_environment(
            &RelayRoute::Exchange {
                repo: "atlas".to_owned(),
                service: GitService::ReceivePack,
            },
            std::path::Path::new("/relay"),
            &super::ExchangeContext {
                content_type: Some("application/x-git-receive-pack-request"),
                content_encoding: Some("gzip"),
                git_protocol: None,
                content_length: 17,
                peer,
                remote_user: "worker:mac-studio",
            },
        );
        assert_eq!(env_value(&exchange, "REQUEST_METHOD"), Some("POST"));
        assert_eq!(
            env_value(&exchange, "PATH_INFO"),
            Some("/atlas.git/git-receive-pack")
        );
        assert_eq!(env_value(&exchange, "QUERY_STRING"), Some(""));
        assert_eq!(env_value(&exchange, "CONTENT_LENGTH"), Some("17"));
        assert_eq!(
            env_value(&exchange, "REMOTE_USER"),
            Some("worker:mac-studio"),
            "the authenticated principal, not a constant, lands in REMOTE_USER"
        );
        assert_eq!(
            env_value(&exchange, "CONTENT_TYPE"),
            Some("application/x-git-receive-pack-request")
        );
        assert_eq!(env_value(&exchange, "HTTP_CONTENT_ENCODING"), Some("gzip"));
    }
}
