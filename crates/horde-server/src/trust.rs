//! The trust boundary: which principal may call which RPC, and who a worker
//! request speaks for (ADR-0009).
//!
//! The mTLS door (`horde_identity::require_enrolled`) answers "is this an
//! enrolled, unrevoked principal?" — and nothing more. Before this module,
//! that was the whole check on both spines: a worker certificate could answer
//! permission prompts and edit the MCP catalog, and a client certificate
//! could register as any worker by naming it in a request body. Enrollment
//! is not authorization; this module is where the second question lives.
//!
//! # Why a table, keyed by method path
//!
//! A per-handler check is one forgotten line away from an open RPC, and the
//! next RPC added is exactly the one that forgets it. So the role gate does
//! not live in the handlers at all: the door reads the gRPC method path
//! (stamped into the request by [`stamp_rpc_path`] on its way in) and looks
//! it up in [`admits`]. A method the table does not name is refused — fail
//! closed — and a test enumerates every method the compiled proto declares
//! on the served spines and requires the table to name each one, so adding
//! an RPC without deciding who may call it fails the build's tests rather
//! than shipping open.
//!
//! # Who a worker request speaks for
//!
//! The role gate says a *worker* is calling; [`caller_worker_id`] says
//! *which* one. The certificate is the answer. Request bodies on the worker
//! spine still carry a `worker_id` (wire compatibility, and the plaintext
//! test shape, where no certificate exists), but a body that names a
//! different worker than its certificate is refused rather than believed.

use horde_identity::{PeerIdentity, PrincipalRole, RevocationList};
use horde_proto::v1::WorkerId;
use tonic::codegen::http;
use tonic::{Request, Status};

/// The gRPC method a request is calling — `/horde.v1.WorkerService/PushEvents`
/// — stamped into its extensions where the door can read it.
///
/// tonic hands an interceptor the request's metadata and extensions but not
/// its URI, so the path has to be carried across by the one layer that sees
/// both: [`stamp_rpc_path`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcPath(pub String);

/// Copies a request's method path into its extensions as an [`RpcPath`].
///
/// Applied as a router layer in front of every service the door guards. A
/// request reaching the door without one is refused, so forgetting this
/// layer closes the listener rather than opening it.
#[must_use]
pub fn stamp_rpc_path<B>(mut request: http::Request<B>) -> http::Request<B> {
    let path = RpcPath(request.uri().path().to_owned());
    request.extensions_mut().insert(path);
    request
}

/// Which principals an RPC admits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admits {
    /// Operator verbs: only a human's client may call them.
    Clients,
    /// The worker spine: only a worker may call it.
    Workers,
    /// Either role. Reserved for the reads and the agent-conduit verbs a
    /// worker makes on its agents' behalf, each of which scopes a worker
    /// caller to its own placed agents in its handler.
    Anyone,
}

impl Admits {
    /// Whether a principal of `role` may call an RPC with this policy.
    #[must_use]
    pub fn role(self, role: PrincipalRole) -> bool {
        match self {
            Self::Clients => role == PrincipalRole::Client,
            Self::Workers => role == PrincipalRole::Worker,
            Self::Anyone => true,
        }
    }
}

/// The services the door guards, by their proto names. The enrollment
/// service is absent on purpose: it is served on its own listener to peers
/// that have no certificate yet, so no role can be asked of them.
pub const GUARDED_SERVICES: [&str; 3] = [
    "horde.v1.ControlPlaneService",
    "horde.v1.WorkerService",
    "horde.v1.BlobService",
];

/// Every `/package.Service/Method` path the compiled proto declares on the
/// [`GUARDED_SERVICES`] — the list a completeness check walks, read from the
/// schema itself so a new RPC appears in it without anyone adding it.
/// Empty only if the compiled descriptor cannot be read, which every check
/// over it then fails on.
#[must_use]
pub fn guarded_method_paths() -> Vec<String> {
    use prost::Message;
    let Ok(set) =
        horde_proto::prost_types::FileDescriptorSet::decode(horde_proto::FILE_DESCRIPTOR_SET)
    else {
        return Vec::new();
    };
    let mut paths = Vec::new();
    for file in &set.file {
        for service in &file.service {
            let full = format!("{}.{}", file.package(), service.name());
            if GUARDED_SERVICES.contains(&full.as_str()) {
                paths.extend(
                    service
                        .method
                        .iter()
                        .map(|method| format!("/{full}/{}", method.name())),
                );
            }
        }
    }
    paths
}

/// The policy for one gRPC method path; `None` for a path this build does
/// not know, which the door refuses.
///
/// Every row is a decision someone made, so the table is spelled out rather
/// than defaulted by service: "the control plane is for clients" is almost
/// true, and the exceptions — the verbs an agent drives through its worker's
/// channel — are exactly the rows worth reading.
#[must_use]
pub fn admits(path: &str) -> Option<Admits> {
    let (service, method) = path.strip_prefix('/')?.split_once('/')?;
    match service {
        "horde.v1.ControlPlaneService" => control_plane(method),
        "horde.v1.WorkerService" => worker_spine(method),
        // Attachments move both ways: a worker uploads what its agents
        // attach, a client downloads it, and the reverse.
        "horde.v1.BlobService" => {
            matches!(method, "UploadBlob" | "DownloadBlob").then_some(Admits::Anyone)
        }
        _ => None,
    }
}

/// The control plane's rows.
fn control_plane(method: &str) -> Option<Admits> {
    match method {
        // The agent conduit: an agent's endpoint lives on its worker, so its
        // spawns, messages, and waits ride the worker's certificate. The
        // handlers scope a worker caller to the agents placed on it.
        "Hello" | "SyncEvents" | "SpawnAgent" | "SendMessage" => Some(Admits::Anyone),
        "CreateSession"
        | "ListSessions"
        | "GetSession"
        | "ArchiveSession"
        | "InterruptAgent"
        | "AnswerPermission"
        | "CreateWorkspaceDefinition"
        | "GetWorkspaceDefinition"
        | "ListWorkspaceDefinitions"
        | "UpdateWorkspaceDefinition"
        | "AddWorkspaceRepo"
        | "DeleteWorkspaceDefinition"
        | "PrewarmWorkspace"
        | "CollectWorkspaceGarbage"
        | "RemoveWorkspaceInstance"
        | "ListWorkspaceInstances"
        | "ConfigureMcpServer"
        | "ListMcpServers"
        | "DeleteMcpServer"
        | "ListWorkers"
        | "GetEffectiveConfig"
        | "MintEnrollmentCode"
        | "RevokeEnrollment"
        | "ResetWorkerJournal"
        | "QueryUsage"
        | "OpenForward"
        | "CloseForward"
        | "ListForwards"
        | "ServeForwards"
        | "ForwardTunnel"
        | "WorkerLogin" => Some(Admits::Clients),
        _ => None,
    }
}

/// The worker spine's rows: every method, and only workers.
fn worker_spine(method: &str) -> Option<Admits> {
    matches!(
        method,
        "RegisterWorker"
            | "StreamCommands"
            | "PushEvents"
            | "ReportCapabilities"
            | "ReportWorkspaceInstances"
            | "StreamForwards"
            | "WorkerForwardTunnel"
            | "StreamLogins"
            | "WorkerLoginTunnel"
            | "ListHubTools"
            | "CallHubTool"
    )
    .then_some(Admits::Workers)
}

/// Rules on one call: the method path the request named, and the principal
/// the door admitted.
///
/// # Errors
///
/// `PermissionDenied` naming the method and why: unknown to this build, or
/// not open to this principal's role.
pub fn rule(path: Option<&RpcPath>, identity: &PeerIdentity) -> Result<(), Status> {
    let path = path.map_or("", |path| path.0.as_str());
    let Some(policy) = admits(path) else {
        return Err(Status::permission_denied(format!(
            "{path:?} is not a method this server admits anyone to"
        )));
    };
    let role = identity.principal.role();
    if !policy.role(role) {
        let wanted = match policy {
            Admits::Workers => "a worker",
            _ => "an enrolled client",
        };
        return Err(Status::permission_denied(format!(
            "only {wanted} may call {path}; {} is a {role}",
            identity.principal
        )));
    }
    Ok(())
}

/// The door every guarded service is served behind: an enrolled, unrevoked
/// principal (`horde_identity::admit_request`) whose role the method admits
/// ([`rule`]). On success the [`PeerIdentity`] rides the request's
/// extensions, exactly as `require_enrolled` left it, for the handlers'
/// identity reads.
pub fn door(
    deny: RevocationList,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |mut request: Request<()>| {
        let identity = horde_identity::admit_request(&request, &deny)?;
        rule(request.extensions().get::<RpcPath>(), &identity)?;
        request.extensions_mut().insert(identity);
        Ok(request)
    }
}

/// The worker a worker-spine request speaks for.
///
/// The certificate, when the channel carries one: a body naming a different
/// worker is refused — a worker that can claim another's id can drain its
/// command stream and journal into its sessions. A plaintext channel has no
/// certificate, and there the body's claim is all there is: that shape exists
/// only on loopback test servers, the same bootstrap trust the forward and
/// login planes extend to an ambient caller.
///
/// # Errors
///
/// `PermissionDenied` for a certificate that names a client, or a body that
/// contradicts the certificate.
pub fn caller_worker_id<T>(
    request: &Request<T>,
    claimed: Option<&WorkerId>,
) -> Result<String, Status> {
    let claimed = claimed.map(ToString::to_string).unwrap_or_default();
    worker_identity(request.extensions().get::<PeerIdentity>(), claimed)
}

/// [`caller_worker_id`]'s decision, over the identity and the claim alone.
///
/// # Errors
///
/// As [`caller_worker_id`].
pub fn worker_identity(identity: Option<&PeerIdentity>, claimed: String) -> Result<String, Status> {
    let Some(identity) = identity else {
        return Ok(claimed);
    };
    if identity.principal.role() != PrincipalRole::Worker {
        return Err(Status::permission_denied(format!(
            "{} is not a worker and cannot speak for one",
            identity.principal
        )));
    }
    let name = identity.principal.name();
    if !claimed.is_empty() && claimed != name {
        return Err(Status::permission_denied(format!(
            "certificate names {} but the request claims worker {claimed:?}",
            identity.principal
        )));
    }
    Ok(name.to_owned())
}

/// The worker a request's certificate names, when it names one — the caller
/// a conduit verb (`SpawnAgent`, `SendMessage`) must scope to its own
/// placed agents. `None` for a client, and for a plaintext channel.
#[must_use]
pub fn conduit_worker<T>(request: &Request<T>) -> Option<String> {
    let identity = request.extensions().get::<PeerIdentity>()?;
    (identity.principal.role() == PrincipalRole::Worker)
        .then(|| identity.principal.name().to_owned())
}

/// Whether the worker an agent is placed on is `worker_id`. An unplaced
/// agent (`None`) matches no worker — fail closed — and an empty worker id
/// matches nothing either, because placement never records an empty worker.
#[must_use]
pub fn holds_agent(placed_on: Option<&str>, worker_id: &str) -> bool {
    !worker_id.is_empty() && placed_on == Some(worker_id)
}

#[cfg(test)]
mod tests {
    use super::{
        Admits, RpcPath, admits, guarded_method_paths as declared_methods, holds_agent, rule,
        stamp_rpc_path, worker_identity,
    };
    use horde_identity::{PeerIdentity, Principal, PrincipalRole};

    fn identity(role: PrincipalRole, name: &str) -> PeerIdentity {
        PeerIdentity {
            principal: Principal::new(role, name).expect("a valid name"),
            serial_hex: "aa".to_owned(),
        }
    }

    /// The walk reads every guarded service and nothing else: the enrollment
    /// service is served to certificate-less peers and has no row.
    #[test]
    fn the_method_walk_covers_the_guarded_services_only() {
        let paths = declared_methods();
        for service in super::GUARDED_SERVICES {
            assert!(
                paths
                    .iter()
                    .any(|path| path.starts_with(&format!("/{service}/"))),
                "{service} must be walked"
            );
        }
        assert!(!paths.iter().any(|path| path.contains("EnrollmentService")));
    }

    /// The structural half of ADR-0009: an RPC added to a guarded service
    /// without a row in the table fails here, not in production — where the
    /// door would refuse it outright, which is safe but is a bug report the
    /// table's author should get first.
    #[test]
    fn every_declared_method_on_a_guarded_service_has_a_policy() {
        let declared = declared_methods();
        assert!(
            declared.len() >= 40,
            "the descriptor walk must actually find the spines: {declared:?}"
        );
        let unruled: Vec<&String> = declared
            .iter()
            .filter(|path| admits(path).is_none())
            .collect();
        assert!(unruled.is_empty(), "methods with no policy: {unruled:?}");
    }

    /// The worker spine is for workers, every method of it: the table may
    /// not quietly widen one.
    #[test]
    fn the_whole_worker_spine_admits_only_workers() {
        for path in declared_methods()
            .iter()
            .filter(|path| path.starts_with("/horde.v1.WorkerService/"))
        {
            assert_eq!(admits(path), Some(Admits::Workers), "{path}");
        }
    }

    /// The control plane's operator verbs refuse a worker, and the agent
    /// conduit admits one — the rows the review named, spelled out.
    #[test]
    fn operator_verbs_are_client_only_and_the_conduit_is_open() {
        for method in [
            "AnswerPermission",
            "ConfigureMcpServer",
            "DeleteMcpServer",
            "ArchiveSession",
            "InterruptAgent",
            "CreateWorkspaceDefinition",
            "UpdateWorkspaceDefinition",
            "AddWorkspaceRepo",
            "DeleteWorkspaceDefinition",
            "PrewarmWorkspace",
            "CollectWorkspaceGarbage",
            "RemoveWorkspaceInstance",
            "MintEnrollmentCode",
            "RevokeEnrollment",
            "ResetWorkerJournal",
            "CreateSession",
            "WorkerLogin",
        ] {
            assert_eq!(
                admits(&format!("/horde.v1.ControlPlaneService/{method}")),
                Some(Admits::Clients),
                "{method}"
            );
        }
        for method in ["Hello", "SyncEvents", "SpawnAgent", "SendMessage"] {
            assert_eq!(
                admits(&format!("/horde.v1.ControlPlaneService/{method}")),
                Some(Admits::Anyone),
                "{method}"
            );
        }
        for method in ["UploadBlob", "DownloadBlob"] {
            assert_eq!(
                admits(&format!("/horde.v1.BlobService/{method}")),
                Some(Admits::Anyone),
                "{method}"
            );
        }
    }

    #[test]
    fn unknown_and_malformed_paths_have_no_policy() {
        for path in [
            "",
            "/",
            "no-leading-slash/Method",
            "/horde.v1.ControlPlaneService",
            "/horde.v1.ControlPlaneService/NoSuchMethod",
            "/horde.v1.WorkerService/NoSuchMethod",
            "/horde.v1.BlobService/DeleteBlob",
            "/horde.v1.EnrollmentService/Join",
            "/other.Service/Hello",
        ] {
            assert_eq!(admits(path), None, "{path:?}");
        }
    }

    #[test]
    fn a_policy_admits_exactly_its_roles() {
        assert!(Admits::Clients.role(PrincipalRole::Client));
        assert!(!Admits::Clients.role(PrincipalRole::Worker));
        assert!(Admits::Workers.role(PrincipalRole::Worker));
        assert!(!Admits::Workers.role(PrincipalRole::Client));
        assert!(Admits::Anyone.role(PrincipalRole::Client));
        assert!(Admits::Anyone.role(PrincipalRole::Worker));
    }

    /// The door's ruling: refused when the path is missing or unknown, or
    /// the role is wrong — with a message naming who and what — and
    /// admitted otherwise.
    #[test]
    fn the_ruling_names_the_method_and_the_principal() {
        let worker = identity(PrincipalRole::Worker, "mac-studio");
        let client = identity(PrincipalRole::Client, "laptop");
        let answer = RpcPath("/horde.v1.ControlPlaneService/AnswerPermission".to_owned());
        let push = RpcPath("/horde.v1.WorkerService/PushEvents".to_owned());

        assert!(rule(Some(&answer), &client).is_ok());
        assert!(rule(Some(&push), &worker).is_ok());

        let refused = rule(Some(&answer), &worker).expect_err("a worker may not answer");
        assert_eq!(refused.code(), tonic::Code::PermissionDenied);
        assert!(
            refused
                .message()
                .contains("only an enrolled client may call")
                && refused.message().contains("AnswerPermission")
                && refused.message().contains("worker:mac-studio"),
            "{}",
            refused.message()
        );

        let refused = rule(Some(&push), &client).expect_err("a client may not push a journal");
        assert!(
            refused.message().contains("only a worker may call")
                && refused.message().contains("client:laptop"),
            "{}",
            refused.message()
        );

        let refused = rule(None, &client).expect_err("no stamped path is refused");
        assert_eq!(refused.code(), tonic::Code::PermissionDenied);
        let unknown = RpcPath("/horde.v1.ControlPlaneService/Nope".to_owned());
        let refused = rule(Some(&unknown), &client).expect_err("an unknown method is refused");
        assert!(refused.message().contains("Nope"), "{}", refused.message());
    }

    #[test]
    fn the_stamp_carries_the_path_and_nothing_else() {
        let request = tonic::codegen::http::Request::builder()
            .uri("https://localhost:1/horde.v1.WorkerService/PushEvents?x=1")
            .body(())
            .expect("a request builds");
        let stamped = stamp_rpc_path(request);
        assert_eq!(
            stamped.extensions().get::<RpcPath>(),
            Some(&RpcPath("/horde.v1.WorkerService/PushEvents".to_owned()))
        );
    }

    /// Who a worker request speaks for: the certificate, a matching or empty
    /// claim deferring to it, a contradicting claim refused, a client
    /// refused, and the plaintext shape taking the claim as all there is.
    #[test]
    fn the_worker_identity_truth_table() {
        let worker = identity(PrincipalRole::Worker, "mac-studio");
        assert_eq!(
            worker_identity(Some(&worker), "mac-studio".to_owned()).ok(),
            Some("mac-studio".to_owned())
        );
        assert_eq!(
            worker_identity(Some(&worker), String::new()).ok(),
            Some("mac-studio".to_owned()),
            "an empty claim defers to the certificate"
        );
        let forged = worker_identity(Some(&worker), "gpu-box".to_owned())
            .expect_err("a body naming another worker is refused");
        assert_eq!(forged.code(), tonic::Code::PermissionDenied);
        assert!(forged.message().contains("gpu-box"), "{}", forged.message());

        let client = identity(PrincipalRole::Client, "laptop");
        let refused = worker_identity(Some(&client), "laptop".to_owned())
            .expect_err("a client cannot speak for a worker");
        assert_eq!(refused.code(), tonic::Code::PermissionDenied);

        assert_eq!(
            worker_identity(None, "plain".to_owned()).ok(),
            Some("plain".to_owned()),
            "a plaintext channel's claim is all there is"
        );
    }

    #[test]
    fn a_conduit_worker_is_named_only_by_a_worker_certificate() {
        let mut request = tonic::Request::new(());
        assert_eq!(super::conduit_worker(&request), None);
        request
            .extensions_mut()
            .insert(identity(PrincipalRole::Client, "laptop"));
        assert_eq!(super::conduit_worker(&request), None);
        request
            .extensions_mut()
            .insert(identity(PrincipalRole::Worker, "mac-studio"));
        assert_eq!(
            super::conduit_worker(&request),
            Some("mac-studio".to_owned())
        );
    }

    #[test]
    fn only_the_placed_worker_holds_an_agent() {
        assert!(holds_agent(Some("w-1"), "w-1"));
        assert!(!holds_agent(Some("w-1"), "w-2"));
        assert!(
            !holds_agent(None, "w-1"),
            "an unplaced agent is held by nobody"
        );
        assert!(!holds_agent(Some(""), ""), "an empty worker holds nothing");
        assert!(!holds_agent(None, ""));
    }
}
