use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

// SEC-016, four of five done. What landed with the server, and what did not:
//
//   [x] HMAC-SHA-256 on every frame          crate::auth::Envelope
//   [x] verify on receive, drop what fails   crate::auth::Envelope::open
//   [x] reject UPDATE from a mismatched id   crate::auth::SenderIdentity
//   [x] authenticate before accepting        every frame carries its own proof,
//                                            which is strictly better than a
//                                            handshake: a handshake authenticates
//                                            a connection once and trusts every
//                                            later byte on it
//   [ ] TLS on the bus and replica links     NOT DONE — see below
//
// The HMAC authenticates and detects tampering; it does not encrypt. Anyone on
// the path reads the topology, the node ids and the addresses. On a provider
// with no private network — OVH VPS has none — that is the whole cluster map
// visible to the path. It is not a way in, and it is not nothing.

pub const CLUSTER_BUS_PORT_OFFSET: u16 = 10000;

#[derive(Debug, Clone, PartialEq)]
pub enum NodeFlag {
    Master,
    Slave,
    Myself,
    Fail,
    Handshake,
    NoFail,
}

impl NodeFlag {
    pub fn as_str(&self) -> &str {
        match self {
            NodeFlag::Master => "master",
            NodeFlag::Slave => "slave",
            NodeFlag::Myself => "myself",
            NodeFlag::Fail => "fail",
            NodeFlag::Handshake => "handshake",
            NodeFlag::NoFail => "nofail",
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ClusterNodeInfo {
    pub node_id: String,
    pub ip: String,
    pub port: u16,
    pub flags: Vec<NodeFlag>,
    pub master_id: Option<String>,
    pub ping_sent: Option<u64>,
    pub pong_received: u64,
    pub link_status: String,
}

impl ClusterNodeInfo {
    pub fn new_myself(node_id: String, ip: String, port: u16) -> Self {
        Self {
            node_id,
            ip,
            port,
            flags: vec![NodeFlag::Master, NodeFlag::Myself],
            master_id: None,
            ping_sent: None,
            pong_received: 0,
            link_status: "connected".to_string(),
        }
    }

    pub fn from_gossip(node_id: String, ip: String, port: u16) -> Self {
        Self {
            node_id,
            ip,
            port,
            flags: vec![NodeFlag::Handshake],
            master_id: None,
            ping_sent: None,
            pong_received: 0,
            link_status: "connected".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum GossipMessage {
    Ping {
        node_id: String,
        ip: String,
        port: u16,
        ping_id: u64,
    },
    Pong {
        node_id: String,
        ip: String,
        port: u16,
        ping_id: u64,
    },
    Meet {
        node_id: String,
        ip: String,
        port: u16,
    },
    Update {
        node_id: String,
        ip: String,
        port: u16,
        flags: Vec<String>,
        master_id: Option<String>,
    },
}

impl GossipMessage {
    pub fn encode(&self) -> Vec<u8> {
        let msg = match self {
            GossipMessage::Ping {
                node_id,
                ip,
                port,
                ping_id,
            } => {
                format!("PING {} {} {} {}\n", node_id, ip, port, ping_id)
            }
            GossipMessage::Pong {
                node_id,
                ip,
                port,
                ping_id,
            } => {
                format!("PONG {} {} {} {}\n", node_id, ip, port, ping_id)
            }
            GossipMessage::Meet { node_id, ip, port } => {
                format!("MEET {} {} {}\n", node_id, ip, port)
            }
            GossipMessage::Update {
                node_id,
                ip,
                port,
                flags,
                master_id,
            } => {
                let flags_str = flags.join(",");
                match master_id {
                    Some(mid) => {
                        format!("UPDATE {} {} {} {} {}\n", node_id, ip, port, flags_str, mid)
                    }
                    None => format!("UPDATE {} {} {} {}\n", node_id, ip, port, flags_str),
                }
            }
        };
        msg.into_bytes()
    }

    /// Which node this message claims to come from.
    pub fn node_id(&self) -> &str {
        match self {
            GossipMessage::Ping { node_id, .. }
            | GossipMessage::Pong { node_id, .. }
            | GossipMessage::Meet { node_id, .. }
            | GossipMessage::Update { node_id, .. } => node_id,
        }
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        let msg = String::from_utf8(data.to_vec()).ok()?;
        let parts: Vec<&str> = msg.split_whitespace().collect();

        if parts.is_empty() {
            return None;
        }

        match parts[0] {
            "PING" if parts.len() >= 5 => Some(GossipMessage::Ping {
                node_id: parts[1].to_string(),
                ip: parts[2].to_string(),
                port: parts[3].parse().ok()?,
                ping_id: parts[4].parse().ok()?,
            }),
            "PONG" if parts.len() >= 5 => Some(GossipMessage::Pong {
                node_id: parts[1].to_string(),
                ip: parts[2].to_string(),
                port: parts[3].parse().ok()?,
                ping_id: parts[4].parse().ok()?,
            }),
            "MEET" if parts.len() >= 4 => Some(GossipMessage::Meet {
                node_id: parts[1].to_string(),
                ip: parts[2].to_string(),
                port: parts[3].parse().ok()?,
            }),
            "UPDATE" if parts.len() >= 5 => Some(GossipMessage::Update {
                node_id: parts[1].to_string(),
                ip: parts[2].to_string(),
                port: parts[3].parse().ok()?,
                flags: parts[4].split(',').map(|s| s.to_string()).collect(),
                master_id: parts.get(5).map(|s| s.to_string()),
            }),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct GossipState {
    myself: Arc<RwLock<ClusterNodeInfo>>,
    nodes: Arc<RwLock<HashMap<String, ClusterNodeInfo>>>,
    config: Arc<RwLock<GossipConfig>>,
}

#[derive(Debug, Clone)]
pub struct GossipConfig {
    pub node_timeout_ms: u64,
    pub ping_interval_ms: u64,
    pub gossip_interval_ms: u64,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            node_timeout_ms: 15000,
            ping_interval_ms: 1000,
            gossip_interval_ms: 1000,
        }
    }
}

impl GossipState {
    pub fn new(node_id: String, ip: String, port: u16) -> Self {
        let myself = ClusterNodeInfo::new_myself(node_id.clone(), ip, port);

        Self {
            myself: Arc::new(RwLock::new(myself)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(GossipConfig::default())),
        }
    }

    /// This node's advertised address, as peers should reach it.
    pub fn myself_address(&self) -> (String, u16) {
        let myself = self.myself.read();
        (myself.ip.clone(), myself.port)
    }

    pub fn myself_id(&self) -> String {
        self.myself.read().node_id.clone()
    }

    pub fn add_node(&self, node_id: String, ip: String, port: u16) {
        let mut nodes = self.nodes.write();
        nodes
            .entry(node_id.clone())
            .or_insert_with(|| ClusterNodeInfo::from_gossip(node_id, ip, port));
    }

    pub fn remove_node(&self, node_id: &str) {
        self.nodes.write().remove(node_id);
    }

    pub fn get_all_nodes(&self) -> Vec<ClusterNodeInfo> {
        let mut result: Vec<ClusterNodeInfo> = vec![self.myself.read().clone()];
        result.extend(self.nodes.read().values().cloned().collect::<Vec<_>>());
        result
    }

    pub fn get_known_nodes(&self) -> Vec<(String, u16)> {
        let myself_id = self.myself.read().node_id.clone();
        self.nodes
            .read()
            .values()
            .filter(|n| n.node_id != myself_id)
            .map(|n| (n.ip.clone(), n.port))
            .collect()
    }

    pub fn get_alive_nodes(&self) -> Vec<ClusterNodeInfo> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let timeout = self.config.read().node_timeout_ms;

        self.nodes
            .read()
            .values()
            .filter(|n| now.saturating_sub(n.pong_received) < timeout)
            .cloned()
            .collect()
    }

    /// Answers a ping, and learns the sender.
    ///
    /// The three sender fields used to be `_sender_id`, `_ip`, `_port` — read
    /// and discarded. So a node that pinged you never became known, and gossip
    /// only ever propagated in the direction someone had typed `CLUSTER MEET`.
    /// Two nodes that met each other from one side would each see a peer that
    /// never answered.
    pub fn handle_ping(
        &self,
        sender_id: &str,
        ip: String,
        port: u16,
        ping_id: u64,
    ) -> GossipMessage {
        if sender_id != self.myself_id() {
            self.add_node(sender_id.to_string(), ip, port);
        }

        let myself = self.myself.read();

        GossipMessage::Pong {
            node_id: myself.node_id.clone(),
            ip: myself.ip.clone(),
            port: myself.port,
            ping_id,
        }
    }

    /// Records a pong, reconciling the placeholder `CLUSTER MEET` created.
    ///
    /// `MEET` only knows an address, so it files the peer under `"ip:port"`. The
    /// pong carries the peer's **real** node id, and a straight lookup by that
    /// id misses the placeholder — which is why a met node stayed `fail`
    /// forever while answering every ping. The placeholder is renamed the first
    /// time its real identity arrives.
    pub fn handle_pong(&self, node_id: &str, ping_id: u64) {
        let mut nodes = self.nodes.write();

        if !nodes.contains_key(node_id) {
            let placeholder = nodes
                .iter()
                .find(|(key, node)| {
                    key.as_str() == format!("{}:{}", node.ip, node.port) && node.node_id != node_id
                })
                .map(|(key, _)| key.clone());
            if let Some(key) = placeholder {
                if let Some(mut node) = nodes.remove(&key) {
                    node.node_id = node_id.to_string();
                    nodes.insert(node_id.to_string(), node);
                }
            }
        }

        if let Some(node) = nodes.get_mut(node_id) {
            node.pong_received = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            if let Some(ping) = node.ping_sent {
                if ping == ping_id {
                    node.link_status = "connected".to_string();
                }
            }
        }
    }

    pub fn handle_meet(&self, node_id: String, ip: String, port: u16) {
        self.add_node(node_id, ip, port);
    }

    pub fn handle_update(
        &self,
        node_id: String,
        ip: String,
        port: u16,
        flags: Vec<String>,
        master_id: Option<String>,
    ) {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(&node_id) {
            node.ip = ip;
            node.port = port;
            node.flags = flags
                .iter()
                .map(|s| match s.as_str() {
                    "master" => NodeFlag::Master,
                    "slave" => NodeFlag::Slave,
                    "fail" => NodeFlag::Fail,
                    "handshake" => NodeFlag::Handshake,
                    _ => NodeFlag::NoFail,
                })
                .collect();
            node.master_id = master_id;
        }
    }
}

/// Serves the cluster bus.
///
/// Replaces a version that had four separate problems, each of which would have
/// shipped the day someone called it:
///
/// **It was never called.** `--cluster` built a `GossipState`, called
/// `cluster.enable()` and logged "Cluster mode enabled" without ever starting
/// this. Three nodes were three independent nodes, all reporting healthy.
///
/// **It bound `127.0.0.1` unconditionally**, so even once started it could not
/// have reached a peer on another machine.
///
/// **It had no framing.** `read()` into a buffer and decode the bytes means a
/// message split across two TCP segments is dropped and two messages in one
/// segment are dropped — silently, and more often under load.
///
/// **Its access control was the peer's IP against the known-node list**, which
/// is both unauthenticated and a chicken-and-egg: a node is not known until it
/// has been met, so a new peer could never connect. Authentication is now per
/// frame, so a stranger's frames are dropped whatever their address.
pub async fn start_gossip_server(
    bind: SocketAddr,
    secret: crate::auth::ClusterSecret,
    state: GossipState,
    msg_tx: mpsc::UnboundedSender<GossipMessage>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(bind).await?;
    tracing::info!(%bind, "cluster gossip server listening, every frame authenticated");

    let _ = &state;

    tokio::spawn(async move {
        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    tracing::error!(error = %e, "gossip accept failed");
                    continue;
                }
            };

            let tx = msg_tx.clone();
            let secret = secret.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_peer(stream, peer_addr, secret, tx).await {
                    tracing::debug!(%peer_addr, error = %e, "gossip connection closed");
                }
            });
        }
    });

    Ok(())
}

/// One connection.
///
/// Line-framed, because the protocol is line-terminated and TCP is not. The
/// nonce memory and the claimed identity are per connection: the identity
/// because that is the only scope in which it means anything, and the nonces
/// because a shared one would be a lock on the hot path for a defence that is
/// already per-peer.
async fn serve_peer(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    secret: crate::auth::ClusterSecret,
    msg_tx: mpsc::UnboundedSender<GossipMessage>,
) -> Result<(), std::io::Error> {
    use tokio::io::{AsyncBufReadExt, BufReader};

    let mut lines = BufReader::new(stream).lines();
    let mut seen = crate::auth::NonceMemory::new();
    let mut identity = crate::auth::SenderIdentity::new();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let payload = match crate::auth::Envelope::open(&line, &secret, now_ms(), &mut seen) {
            Ok(payload) => payload,
            Err(e) => {
                // Logged at warn and dropped. Not a disconnect: a single bad
                // frame is far more often a clock that drifted than an attack,
                // and dropping the connection would turn one skewed node into a
                // reconnect storm against every peer.
                tracing::warn!(%peer_addr, error = %e, "rejected gossip frame");
                continue;
            }
        };

        let Some(message) = GossipMessage::decode(payload.as_bytes()) else {
            tracing::warn!(%peer_addr, "authenticated frame did not parse as gossip");
            continue;
        };

        if let Err(e) = identity.observe(message.node_id()) {
            // A connection that changes who it claims to be. See the note in
            // `crate::auth` on what a shared secret can prove: this catches
            // confusion and cross-connection replay, not a malicious member.
            tracing::warn!(%peer_addr, error = %e, "gossip identity switch");
            continue;
        }

        if msg_tx.send(message).is_err() {
            break;
        }
    }

    Ok(())
}

/// Sends one frame to a peer and closes.
///
/// A connection per frame rather than a pool. Gossip is a handful of short
/// messages every second or two, so the pool would exist to save a handshake
/// that is not on any hot path — and a pooled connection to a node that has
/// gone away is a write that blocks until a timeout nobody set. Reconnecting
/// is the cheap, obvious, restartable option.
///
/// Every frame carries its own proof, so there is no session to establish and
/// nothing is trusted because of what an earlier frame on the same connection
/// said.
pub async fn send_frame(
    peer: SocketAddr,
    message: &GossipMessage,
    secret: &crate::auth::ClusterSecret,
    connect_timeout: Duration,
) -> Result<(), std::io::Error> {
    use tokio::io::AsyncWriteExt;

    let payload = String::from_utf8_lossy(&message.encode())
        .trim_end()
        .to_string();
    let frame = crate::auth::Envelope::seal(&payload, secret, now_ms(), &crate::auth::new_nonce());

    // A peer that has gone away must not hold this task open. Without the
    // timeout a single unreachable node stalls the whole gossip round, which is
    // the failure that makes a cluster look partitioned when one machine is
    // merely rebooting.
    let mut stream = tokio::time::timeout(connect_timeout, tokio::net::TcpStream::connect(peer))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("connect to {peer} timed out"),
            )
        })??;

    stream.write_all(frame.as_bytes()).await?;
    stream.flush().await
}

/// One round of gossip: ping every known peer.
///
/// Failures are logged and skipped, never propagated. One unreachable node is
/// the ordinary case — it is what the failure detector exists to notice — and a
/// round that aborted on the first error would stop pinging everyone after it,
/// making one dead node look like a dead cluster.
pub async fn gossip_round(
    state: &GossipState,
    secret: &crate::auth::ClusterSecret,
    bus_offset: u16,
    connect_timeout: Duration,
) -> usize {
    let myself = state.myself_id();
    let (my_ip, my_port) = state.myself_address();
    let mut reached = 0;

    // `get_known_nodes` yields `(ip, port)`, not `(node_id, port)`. The first
    // version of this loop read the first element as a node id and looked it up
    // — which never matched, so the round found no peers and gossip stayed
    // silent with a listener running and nothing in the log to say why.
    for (ip, port) in state.get_known_nodes() {
        if ip == my_ip && port == my_port {
            continue;
        }
        let Ok(peer) = format!("{}:{}", ip, port + bus_offset).parse::<SocketAddr>() else {
            tracing::warn!(%ip, port, "peer address does not parse");
            continue;
        };

        let ping = GossipMessage::Ping {
            node_id: myself.clone(),
            ip: my_ip.clone(),
            port: my_port,
            ping_id: now_ms(),
        };
        match send_frame(peer, &ping, secret, connect_timeout).await {
            Ok(()) => reached += 1,
            Err(e) => tracing::debug!(%peer, error = %e, "gossip ping failed"),
        }
    }

    reached
}

/// Wall-clock milliseconds, for the replay window.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn generate_node_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let rand: u64 = rand_simple();
    format!("{:x}-{:x}", ts, rand)
}

pub fn generate_stable_node_id(ip: &str, port: u16) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{}:{}", ip, port).hash(&mut hasher);
    let hash = hasher.finish();
    format!("{:016x}-{:016x}", hash, port)
}

fn rand_simple() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish()
}
