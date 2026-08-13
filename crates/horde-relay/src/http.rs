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
//! # What is deliberately not here
//!
//! Authentication. The Relay rides the Server's channel (mTLS at the end
//! state, localhost in the bootstrap), so this listener must be bound
//! accordingly by whoever starts it; `REMOTE_USER` is where the
//! authenticated principal will land once enrollment (wave 5) supplies one.
//! Bodies are buffered whole rather than streamed — honest for the repos
//! Horde relays today, and a contained upgrade when it stops being.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
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

/// The CGI environment for one routed request.
///
/// A pure builder so the variables — each one load-bearing for
/// `http-backend` — are assertable without a process.
#[must_use]
pub fn cgi_environment(
    route: &RelayRoute,
    project_root: &Path,
    content_type: Option<&str>,
    content_encoding: Option<&str>,
    git_protocol: Option<&str>,
    content_length: usize,
    peer: SocketAddr,
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
        ("REMOTE_USER".to_owned(), "horde".to_owned()),
        ("REMOTE_ADDR".to_owned(), peer.ip().to_string()),
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
            env.push(("CONTENT_LENGTH".to_owned(), content_length.to_string()));
        }
    }
    if let Some(content_type) = content_type {
        env.push(("CONTENT_TYPE".to_owned(), content_type.to_owned()));
    }
    if let Some(encoding) = content_encoding {
        // http-backend inflates gzipped request bodies itself, but only if
        // told they are gzipped.
        env.push(("HTTP_CONTENT_ENCODING".to_owned(), encoding.to_owned()));
    }
    if let Some(protocol) = git_protocol {
        // The `Git-Protocol` header is how a client asks for protocol v2;
        // http-backend reads it from this variable.
        env.push(("HTTP_GIT_PROTOCOL".to_owned(), protocol.to_owned()));
    }
    env
}

/// One Relay: a mirror store plus the bridge that serves it.
#[derive(Debug, Clone)]
pub struct Relay {
    store: Arc<MirrorStore>,
}

impl Relay {
    #[must_use]
    pub fn new(store: Arc<MirrorStore>) -> Self {
        Self { store }
    }

    /// Answers one HTTP request. Infallible by design: every failure has an
    /// HTTP shape, and a bridge that errors the connection instead of the
    /// request tells the git client nothing.
    pub async fn respond(
        &self,
        request: Request<Incoming>,
        peer: SocketAddr,
    ) -> Response<Full<Bytes>> {
        let (parts, body) = request.into_parts();
        let Some(route) = parse_route(&parts.method, parts.uri.path(), parts.uri.query()) else {
            return plain_response(
                StatusCode::NOT_FOUND,
                "not a git smart-HTTP route this Relay serves",
            );
        };
        let Ok(body) = body.collect().await else {
            return plain_response(
                StatusCode::BAD_REQUEST,
                "the request body could not be read",
            );
        };

        let header = |name: &str| {
            parts
                .headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        match self
            .exchange(
                &route,
                header("content-type").as_deref(),
                header("content-encoding").as_deref(),
                header("git-protocol").as_deref(),
                body.to_bytes(),
                peer,
            )
            .await
        {
            Ok(cgi) => cgi_to_http(cgi),
            Err(error) => error.into_response(),
        }
    }

    /// Materializes the mirror if needed, then runs the CGI exchange.
    async fn exchange(
        &self,
        route: &RelayRoute,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        git_protocol: Option<&str>,
        body: Bytes,
        peer: SocketAddr,
    ) -> Result<CgiResponse, BridgeError> {
        // Lazy mirroring happens here: the first request for a registered
        // repo is what clones it.
        self.store.ensure(route.repo()).await?;
        let env = cgi_environment(
            route,
            self.store.root(),
            content_type,
            content_encoding,
            git_protocol,
            body.len(),
            peer,
        );
        run_http_backend(&env, &body).await
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
                async move { Ok::<_, Infallible>(relay.respond(request, peer).await) }
            });
            // A connection-level error is the client hanging up; the
            // request-level answer already happened or never will.
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
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
            None,
            None,
            Some("version=2"),
            0,
            peer,
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
            Some("application/x-git-receive-pack-request"),
            Some("gzip"),
            None,
            17,
            peer,
        );
        assert_eq!(env_value(&exchange, "REQUEST_METHOD"), Some("POST"));
        assert_eq!(
            env_value(&exchange, "PATH_INFO"),
            Some("/atlas.git/git-receive-pack")
        );
        assert_eq!(env_value(&exchange, "QUERY_STRING"), Some(""));
        assert_eq!(env_value(&exchange, "CONTENT_LENGTH"), Some("17"));
        assert_eq!(
            env_value(&exchange, "CONTENT_TYPE"),
            Some("application/x-git-receive-pack-request")
        );
        assert_eq!(env_value(&exchange, "HTTP_CONTENT_ENCODING"), Some("gzip"));
    }
}
