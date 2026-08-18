//! Worker-side forward realization (ADR-0006).
//!
//! The Server pushes whole intent sets down the forward stream; this module
//! reconciles reality against them. An expose is realized *on demand*: the
//! Worker dials the local port once per relayed connection request. An
//! inject is realized as a loopback listener per intent — loopback-wide on
//! the Worker in v1, stated honestly, because scoping it to one agent's
//! processes needs namespaces the Workers don't have.
//!
//! Realization is tied to the stream's liveness: when it ends — partition,
//! server restart, shutdown — every listener is dropped and every in-flight
//! tunnel dies with its `JoinSet`. Reconnection re-subscribes and the
//! re-sent set re-realizes, which is the revival path too: an agent revived
//! from `Failed` re-enters the set without re-declaration.

use std::collections::BTreeMap;

use horde_cli::forward::{
    ConfirmEnd, Tunnel, confirm_opened, connection_id, dial_and_pump, pump_tcp,
};
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::{
    ForwardConnectionRequest, ForwardDirection, ForwardIntent, ForwardRefusalReason,
    StreamForwardsRequest, WorkerId, stream_forwards_response::Update,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use crate::error::WorkerError;

/// Runs the forward plane over one connection until the Server ends it.
///
/// # Errors
///
/// The stream's failure. Ending cleanly is `Ok` — reconnection is the
/// daemon's policy, exactly as for the other two streams.
pub async fn serve(
    mut client: WorkerServiceClient<Channel>,
    worker_id: &str,
) -> Result<(), WorkerError> {
    let mut stream = client
        .stream_forwards(StreamForwardsRequest {
            worker_id: Some(WorkerId::new(worker_id)),
        })
        .await?
        .into_inner();

    let mut plane = ForwardPlane::new(client);
    loop {
        tokio::select! {
            message = stream.message() => {
                let Some(update) = message? else {
                    // Stream over: dropping `plane` drops every listener and
                    // aborts every in-flight tunnel — realization dies with
                    // the connection, exactly as the ADR requires.
                    return Ok(());
                };
                match update.update {
                    Some(Update::Intents(set)) => plane.reconcile(set.intents).await,
                    Some(Update::Connection(request)) => plane.answer_expose(request),
                    None => {}
                }
            }
            // Reaped as they finish, so a long-lived plane does not hoard a
            // handle per connection it ever carried.
            Some(_) = plane.connections.join_next() => {}
        }
    }
}

/// The realized state: what is bound, what is dialable, what is in flight.
struct ForwardPlane {
    client: WorkerServiceClient<Channel>,
    /// EXPOSE intents by id → the loopback port dialed per connection.
    dialable: BTreeMap<String, u32>,
    /// INJECT intents by id → the bound listener realizing them.
    injects: BTreeMap<String, InjectListener>,
    /// In-flight tunnels, all of them dying with this set.
    connections: tokio::task::JoinSet<()>,
}

/// One bound inject listener.
struct InjectListener {
    worker_port: u32,
    accept_task: tokio::task::JoinHandle<()>,
}

impl Drop for InjectListener {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

impl ForwardPlane {
    fn new(client: WorkerServiceClient<Channel>) -> Self {
        Self {
            client,
            dialable: BTreeMap::new(),
            injects: BTreeMap::new(),
            connections: tokio::task::JoinSet::new(),
        }
    }

    /// Makes reality match one pushed intent set, whole.
    async fn reconcile(&mut self, intents: Vec<ForwardIntent>) {
        let mut exposes = BTreeMap::new();
        let mut wanted_injects = BTreeMap::new();
        for intent in intents {
            let Some(id) = intent.forward_id.as_ref().map(ToString::to_string) else {
                continue;
            };
            let Some(spec) = intent.spec else { continue };
            match ForwardDirection::try_from(spec.direction) {
                Ok(ForwardDirection::Expose) => {
                    exposes.insert(id, spec.worker_port);
                }
                Ok(ForwardDirection::Inject) => {
                    wanted_injects.insert(id, spec.worker_port);
                }
                _ => {}
            }
        }
        self.dialable = exposes;

        // Drop listeners whose intent is gone or whose port moved; keep the
        // rest bound — reconciliation must not flap live listeners.
        self.injects
            .retain(|id, listener| still_wanted(&wanted_injects, id, listener.worker_port));

        for (id, worker_port) in wanted_injects {
            if self.injects.contains_key(&id) {
                continue;
            }
            match self.bind_inject(&id, worker_port).await {
                Ok(listener) => {
                    self.injects.insert(id, listener);
                }
                Err(error) => {
                    // Realization failed; the intent survives, and the next
                    // pushed set retries. Journaling this is a diagnostic
                    // the log does not need — the listener is ephemeral.
                    tracing::warn!(forward_id = id, worker_port, %error, "could not bind inject listener");
                }
            }
        }
    }

    /// Binds one inject's loopback listener and starts its accept loop.
    async fn bind_inject(
        &self,
        forward_id: &str,
        worker_port: u32,
    ) -> std::io::Result<InjectListener> {
        let port = u16::try_from(worker_port)
            .map_err(|_| std::io::Error::other("worker_port does not fit a TCP port"))?;
        let listener = TcpListener::bind(("127.0.0.1", port)).await?;
        let accept_task = tokio::spawn(inject_accept_loop(
            listener,
            self.client.clone(),
            forward_id.to_owned(),
        ));
        Ok(InjectListener {
            worker_port,
            accept_task,
        })
    }

    /// EXPOSE: answer one relayed connection request by dialing the target.
    fn answer_expose(&mut self, request: ForwardConnectionRequest) {
        let forward_id = request
            .forward_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let port = self.dialable.get(&forward_id).copied();
        let client = self.client.clone();
        self.connections.spawn(dial_and_carry(
            client,
            forward_id,
            request.connection_id,
            port,
        ));
    }
}

/// Whether a bound listener still realizes a wanted intent. Same id *and*
/// same port: an intent whose port moved is a different listener, and `||`
/// here would leave the old port bound forever while the new one never
/// binds.
fn still_wanted(wanted: &BTreeMap<String, u32>, id: &str, bound_port: u32) -> bool {
    wanted.get(id) == Some(&bound_port)
}

/// One tunnel leg on the worker spine, stripped to bare frames.
async fn open_tunnel(client: &mut WorkerServiceClient<Channel>) -> Result<Tunnel, WorkerError> {
    let (to_relay, outbound) = tokio::sync::mpsc::channel(16);
    let requests = tokio_stream::wrappers::ReceiverStream::new(outbound)
        .map(|frame| horde_proto::v1::WorkerForwardTunnelRequest { frame: Some(frame) });
    let inbound = client.worker_forward_tunnel(requests).await?.into_inner();
    Ok(Tunnel {
        to_relay,
        from_relay: Box::pin(
            inbound.map(|message| message.map(|reply| reply.frame.unwrap_or_default())),
        ),
    })
}

/// The passive half of an expose: dial the local target and carry the bytes,
/// or refuse with the coded reason.
async fn dial_and_carry(
    mut client: WorkerServiceClient<Channel>,
    forward_id: String,
    connection_id: String,
    port: Option<u32>,
) {
    let Ok(mut tunnel) = open_tunnel(&mut client).await else {
        return; // No relay leg: the originator's pairing deadline refuses it.
    };
    if tunnel.open(&forward_id, &connection_id).await.is_err() {
        return;
    }

    let Some(port) = port else {
        tunnel
            .refuse(
                ForwardRefusalReason::UnknownForward,
                format!("this worker holds no expose {forward_id}"),
            )
            .await;
        return;
    };
    dial_and_pump(tunnel, u16::try_from(port).unwrap_or(0)).await;
}

/// An inject listener's life: accept, originate, pump — refusing fast when
/// the relay does. Failure-bounded by count, not clock.
async fn inject_accept_loop(
    listener: TcpListener,
    client: WorkerServiceClient<Channel>,
    forward_id: String,
) {
    let mut carried = tokio::task::JoinSet::new();
    let mut consecutive_failures = 0_u32;
    while consecutive_failures < horde_cli::forward::ACCEPT_FAILURE_BOUND {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((tcp, _peer)) => {
                    consecutive_failures = 0;
                    carried.spawn(originate_inject_connection(
                        client.clone(),
                        forward_id.clone(),
                        tcp,
                    ));
                }
                Err(_) => consecutive_failures += 1,
            },
            Some(_) = carried.join_next() => {}
        }
    }
}

/// One accepted agent-side connection, originated toward the backer.
///
/// An unbacked inject refuses here, distinguishably: the relay answers
/// `UNBACKED` naming the detached backer, and the accepted socket is closed
/// with nothing written.
async fn originate_inject_connection(
    mut client: WorkerServiceClient<Channel>,
    forward_id: String,
    tcp: TcpStream,
) {
    let Ok(mut tunnel) = open_tunnel(&mut client).await else {
        return; // Server unreachable: dropping the socket is the refusal.
    };
    if tunnel.open(&forward_id, &connection_id()).await.is_err() {
        return;
    }
    match confirm_opened(&mut tunnel).await {
        Ok(()) => {
            let _ = pump_tcp(tcp, tunnel).await;
        }
        Err(ConfirmEnd::Refused(refusal)) => {
            tracing::info!(
                forward_id,
                reason = refusal.reason,
                detail = refusal.detail,
                "inject connection refused"
            );
            drop(tcp);
        }
        Err(ConfirmEnd::NoAnswer) => {
            tracing::info!(forward_id, "inject connection got no relay answer");
            drop(tcp);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::still_wanted;
    use std::collections::BTreeMap;

    /// The reconciliation predicate's table: same id and same port keeps a
    /// listener; a vanished intent or a moved port drops it. The moved-port
    /// row is the one an `||` (or an id-only check) would get wrong, and it
    /// is unreachable end-to-end because nothing in v1 rewrites a spec —
    /// which is exactly why it is a named predicate.
    #[test]
    fn a_listener_survives_only_while_its_intent_and_port_hold() {
        let mut wanted = BTreeMap::new();
        wanted.insert("fwd-1".to_owned(), 5432_u32);

        assert!(still_wanted(&wanted, "fwd-1", 5432));
        assert!(!still_wanted(&wanted, "fwd-1", 5433), "the port moved");
        assert!(!still_wanted(&wanted, "fwd-2", 5432), "the intent is gone");
        assert!(!still_wanted(&BTreeMap::new(), "fwd-1", 5432));
    }
}
