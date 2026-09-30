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
//   [x] frames bound to their receiver and   crate::auth::Opened::receiver_bound,
//       one replay memory for the bus        start_gossip_server
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

// `PartialEq` so a wire round trip can be asserted as an equation. Without it a
// test has to destructure and compare fields, which is how a test comes to check
// four of six and pass for a message that lost the other two.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        /// The slot ranges this node claims to own, inclusive.
        ///
        /// The missing half of cluster mode. Membership propagated and ownership
        /// did not, so every node kept claiming 0-16383 after meeting its peers:
        /// members agreed and sharding did not, which looks correct from
        /// `CLUSTER NODES` and sends every key to the wrong place.
        slots: Vec<(u16, u16)>,
    },
}

/// `0-5460,10923-16383`, or `-` for a node that owns nothing.
///
/// A separate encoding rather than one number per slot: 16384 numbers is 80 KiB
/// per gossip frame, and this rides on the failure detector's traffic.
pub fn encode_slots(ranges: &[(u16, u16)]) -> String {
    if ranges.is_empty() {
        return "-".to_string();
    }
    ranges
        .iter()
        .map(|(start, end)| {
            if start == end {
                start.to_string()
            } else {
                format!("{start}-{end}")
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Reads what [`encode_slots`] wrote, dropping anything malformed.
///
/// A garbled range is dropped rather than failing the whole frame: the sender's
/// membership is still worth learning, and losing it because one range did not
/// parse would turn a cosmetic problem into an unreachable node. What is *not*
/// done is inventing a range — an unparseable claim means no claim.
pub fn decode_slots(text: &str) -> Vec<(u16, u16)> {
    if text == "-" {
        return Vec::new();
    }
    text.split(',')
        .filter_map(|part| match part.split_once('-') {
            Some((start, end)) => Some((start.parse().ok()?, end.parse().ok()?)),
            None => {
                let only: u16 = part.parse().ok()?;
                Some((only, only))
            }
        })
        // Backwards is not a range. Accepting one would have every reader
        // iterating an empty span and silently recording no owner. Past the
        // last slot is not one either, and indexing with it panicked the
        // gossip drain task.
        .filter(|(start, end)| start <= end && *end < crate::consistent_hash::SLOT_COUNT)
        .collect()
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
                slots,
            } => {
                let flags_str = flags.join(",");
                // Fixed positions, with `-` for an absent field.
                //
                // `master_id` used to be an optional trailing word, so a reader
                // could not tell "no master, here are slots" from "this master,
                // no slots" — the two differ by nothing but position. Adding a
                // second optional tail would have made that ambiguity a
                // misparse rather than a possibility.
                let master = master_id.as_deref().unwrap_or("-");
                let slots_str = encode_slots(slots);
                format!(
                    "UPDATE {} {} {} {} {} {}\n",
                    node_id, ip, port, flags_str, master, slots_str
                )
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
                // `-` is the placeholder, not a node id. Reading it as one would
                // make every masterless node claim a master called "-".
                master_id: parts.get(5).filter(|v| **v != "-").map(|s| s.to_string()),
                slots: parts.get(6).map(|v| decode_slots(v)).unwrap_or_default(),
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
    /// Per peer: hash of the slot claim last announced to it, and when.
    announced: Arc<RwLock<HashMap<String, (u64, std::time::Instant)>>>,
}

/// A peer that already has this node's current claim is sent it again only
/// this often — to repair a peer that missed it (a restart, a lost frame),
/// not every round.
const REANNOUNCE_EVERY: Duration = Duration::from_secs(10);

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
            announced: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Whether `peer` should be sent the claim hashed `hash` this round.
    fn should_announce(&self, peer: &str, hash: u64) -> bool {
        match self.announced.read().get(peer) {
            Some((h, at)) => *h != hash || at.elapsed() >= REANNOUNCE_EVERY,
            None => true,
        }
    }

    fn mark_announced(&self, peer: String, hash: u64) {
        self.announced
            .write()
            .insert(peer, (hash, std::time::Instant::now()));
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

    /// Address of the node `node_id`, without copying the node table (a
    /// `MOVED` redirect looks one up).
    pub fn node_address(&self, node_id: &str) -> Option<(String, u16)> {
        {
            let me = self.myself.read();
            if me.node_id == node_id {
                return Some((me.ip.clone(), me.port));
            }
        }
        let nodes = self.nodes.read();
        if let Some(n) = nodes.get(node_id) {
            if n.node_id == node_id {
                return Some((n.ip.clone(), n.port));
            }
        }
        nodes
            .values()
            .find(|n| n.node_id == node_id)
            .map(|n| (n.ip.clone(), n.port))
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

    /// Peers as `(node_id, ip, port)`. A node id equal to `"ip:port"` is the
    /// `CLUSTER MEET` placeholder: the real identity is not known yet.
    pub fn get_known_peers(&self) -> Vec<(String, String, u16)> {
        let myself_id = self.myself.read().node_id.clone();
        self.nodes
            .read()
            .values()
            .filter(|n| n.node_id != myself_id)
            .map(|n| (n.node_id.clone(), n.ip.clone(), n.port))
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
            let mut nodes = self.nodes.write();
            nodes.entry(sender_id.to_string()).or_insert_with(|| {
                ClusterNodeInfo::from_gossip(sender_id.to_string(), ip.clone(), port)
            });
            absorb_placeholder(&mut nodes, sender_id, &ip, port);
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
    pub fn handle_pong(&self, node_id: &str, ip: &str, port: u16, ping_id: u64) {
        let mut nodes = self.nodes.write();
        absorb_placeholder(&mut nodes, node_id, ip, port);

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

    // One replay memory for the whole bus. Senders open a connection per
    // frame, so a per-connection memory never saw the same nonce twice.
    let seen = Arc::new(parking_lot::Mutex::new(crate::auth::NonceMemory::new()));
    let me: Arc<str> = state.myself_id().into();
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_BUS_CONNECTIONS));
    let rejected_log = Arc::new(RateLimitedLog::default());

    tokio::spawn(async move {
        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    tracing::error!(error = %e, "gossip accept failed");
                    continue;
                }
            };

            // Bounded, because the bus is reachable before anything is
            // authenticated: without a cap, idle sockets are free memory.
            let Ok(permit) = permits.clone().try_acquire_owned() else {
                rejected_log.warn(|suppressed| {
                    tracing::warn!(%peer_addr, suppressed, "gossip connection limit reached");
                });
                continue;
            };

            let ctx = PeerContext {
                secret: secret.clone(),
                seen: seen.clone(),
                me: me.clone(),
                msg_tx: msg_tx.clone(),
                rejected_log: rejected_log.clone(),
            };
            tokio::spawn(async move {
                if let Err(e) = serve_peer(stream, peer_addr, ctx).await {
                    tracing::debug!(%peer_addr, error = %e, "gossip connection closed");
                }
                drop(permit);
            });
        }
    });

    Ok(())
}

/// Most concurrent inbound bus connections. A peer holds one for the length
/// of a frame, so this is far above what any real cluster needs.
pub const MAX_BUS_CONNECTIONS: usize = 256;

/// How long a bus connection may sit without completing a frame. Senders
/// write one line and close, so anything slower is not a peer.
pub const BUS_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Everything a connection needs from the server.
struct PeerContext {
    secret: crate::auth::ClusterSecret,
    seen: Arc<parking_lot::Mutex<crate::auth::NonceMemory>>,
    me: Arc<str>,
    msg_tx: mpsc::UnboundedSender<GossipMessage>,
    rejected_log: Arc<RateLimitedLog>,
}

/// A warn that fires at most once a second, counting what it swallowed.
///
/// Rejected frames come from the network; logging each one at `warn` let
/// anyone who can reach the port fill the disk.
#[derive(Default)]
pub struct RateLimitedLog {
    last_ms: std::sync::atomic::AtomicU64,
    suppressed: std::sync::atomic::AtomicU64,
}

impl RateLimitedLog {
    pub fn warn(&self, emit: impl FnOnce(u64)) {
        use std::sync::atomic::Ordering;
        let now = now_ms();
        let last = self.last_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) >= 1_000
            && self
                .last_ms
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            emit(self.suppressed.swap(0, Ordering::Relaxed));
        } else {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Whether a message may arrive unaddressed (receiver `*`).
///
/// Only the handshake: a `PING` or `MEET` is sent before the peer's node id is
/// known. `UPDATE` moves slot ownership and `PONG` vouches for liveness, so
/// both must name this node — otherwise one capture replays at every node.
fn may_be_unbound(message: &GossipMessage) -> bool {
    matches!(
        message,
        GossipMessage::Ping { .. } | GossipMessage::Meet { .. }
    )
}

/// Reads one `\n`-terminated line of at most `MAX_FRAME_BYTES`.
///
/// `Ok(None)` at a clean end of stream. A longer line is an error and ends
/// the connection: it is refused *before* being buffered, which the previous
/// `BufReader::lines()` could not do — it read until `\n` however far away.
async fn read_frame<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
) -> Result<Option<()>, std::io::Error> {
    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncReadExt;

    buf.clear();
    let limit = (crate::auth::MAX_FRAME_BYTES + 2) as u64;
    let n = reader.take(limit).read_until(b'\n', buf).await?;
    if n == 0 {
        return Ok(None);
    }
    if buf.last() != Some(&b'\n') {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            if n as u64 >= limit {
                "gossip frame over the size limit"
            } else {
                "gossip frame truncated"
            },
        ));
    }
    Ok(Some(()))
}

/// One connection.
///
/// Line-framed, because the protocol is line-terminated and TCP is not. The
/// claimed identity is per connection, because that is the only scope in
/// which it means anything; the nonce memory is shared (see
/// `start_gossip_server`).
async fn serve_peer(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    ctx: PeerContext,
) -> Result<(), std::io::Error> {
    let mut reader = tokio::io::BufReader::new(stream);
    let mut identity = crate::auth::SenderIdentity::new();
    let mut buf = Vec::with_capacity(512);

    loop {
        let read = tokio::time::timeout(BUS_READ_TIMEOUT, read_frame(&mut reader, &mut buf))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "gossip read timed out")
            })??;
        if read.is_none() {
            break;
        }
        let Ok(line) = std::str::from_utf8(&buf) else {
            ctx.rejected_log.warn(|suppressed| {
                tracing::warn!(%peer_addr, suppressed, "rejected gossip frame: not UTF-8");
            });
            continue;
        };
        if line.trim().is_empty() {
            continue;
        }

        let opened = {
            let mut seen = ctx.seen.lock();
            crate::auth::Envelope::open(line, &ctx.secret, now_ms(), &mut seen, &ctx.me)
        };
        let opened = match opened {
            Ok(opened) => opened,
            Err(e) => {
                // Dropped, not a disconnect: a single bad frame is far more
                // often a clock that drifted than an attack, and dropping the
                // connection would turn one skewed node into a reconnect storm.
                // Rate-limited, because the network decides how often this runs.
                ctx.rejected_log.warn(|suppressed| {
                    tracing::warn!(%peer_addr, error = %e, suppressed, "rejected gossip frame");
                });
                continue;
            }
        };

        let Some(message) = GossipMessage::decode(opened.payload.as_bytes()) else {
            ctx.rejected_log.warn(|suppressed| {
                tracing::warn!(%peer_addr, suppressed, "authenticated frame did not parse as gossip");
            });
            continue;
        };

        if !opened.receiver_bound && !may_be_unbound(&message) {
            ctx.rejected_log.warn(|suppressed| {
                tracing::warn!(
                    %peer_addr,
                    suppressed,
                    "rejected unaddressed gossip frame that carries authority"
                );
            });
            continue;
        }

        if let Err(e) = identity.observe(message.node_id()) {
            // A connection that changes who it claims to be. See the note in
            // `crate::auth` on what a shared secret can prove: this catches
            // confusion, not a malicious member.
            ctx.rejected_log.warn(|suppressed| {
                tracing::warn!(%peer_addr, error = %e, suppressed, "gossip identity switch");
            });
            continue;
        }

        if ctx.msg_tx.send(message).is_err() {
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
///
/// `receiver` is the peer's node id, or [`crate::auth::UNBOUND_RECEIVER`]
/// while it is not known — which only a `PING` or `MEET` may use.
pub async fn send_frame(
    peer: SocketAddr,
    message: &GossipMessage,
    secret: &crate::auth::ClusterSecret,
    connect_timeout: Duration,
    receiver: &str,
) -> Result<(), std::io::Error> {
    send_frames(
        peer,
        std::slice::from_ref(message),
        secret,
        connect_timeout,
        receiver,
    )
    .await
}

/// [`send_frame`] for several messages over one connection, each sealed on
/// its own (own nonce, own MAC): one connect for a round's ping and slot
/// announcement instead of one each.
pub async fn send_frames(
    peer: SocketAddr,
    messages: &[GossipMessage],
    secret: &crate::auth::ClusterSecret,
    connect_timeout: Duration,
    receiver: &str,
) -> Result<(), std::io::Error> {
    use tokio::io::AsyncWriteExt;

    let mut out = String::new();
    for message in messages {
        let payload = String::from_utf8_lossy(&message.encode())
            .trim_end()
            .to_string();
        out.push_str(&crate::auth::Envelope::seal(
            &payload,
            secret,
            now_ms(),
            &crate::auth::new_nonce(),
            receiver,
        ));
    }

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

    // The write is bounded too: a peer that accepts and never reads would
    // otherwise hold this task once the socket buffer fills.
    tokio::time::timeout(connect_timeout, async {
        stream.write_all(out.as_bytes()).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("write to {peer} timed out"),
        )
    })?
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
    gossip_round_with_slots(state, secret, bus_offset, connect_timeout, &[]).await
}

/// A round that also announces which slots this node claims.
///
/// Separate entry point rather than a changed signature, because the caller that
/// knows the slots is the server and the ones that do not are the tests — and a
/// test passing an empty claim is saying something true about a node that owns
/// nothing, not filling in a parameter it does not care about.
///
/// The `UPDATE` frame used to be decodable and never built: nothing sent one, so
/// ownership could not propagate however correct the receiving side was.
pub async fn gossip_round_with_slots(
    state: &GossipState,
    secret: &crate::auth::ClusterSecret,
    bus_offset: u16,
    connect_timeout: Duration,
    my_slots: &[(u16, u16)],
) -> usize {
    let myself = state.myself_id();
    let (my_ip, my_port) = state.myself_address();
    let claim_hash = {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        my_slots.hash(&mut h);
        h.finish()
    };

    // Peers are dialled concurrently: a slow or unreachable one costs the
    // round its timeout once, not once per peer after it.
    let mut sends = tokio::task::JoinSet::new();

    // `get_known_nodes` yields `(ip, port)`, not `(node_id, port)`. The first
    // version of this loop read the first element as a node id and looked it up
    // — which never matched, so the round found no peers and gossip stayed
    // silent with a listener running and nothing in the log to say why.
    for (node_id, ip, port) in state.get_known_peers() {
        if ip == my_ip && port == my_port {
            continue;
        }
        // Until the peer's pong reveals its id, all this node has is the
        // `CLUSTER MEET` placeholder: ping unaddressed, and hold the slot
        // announcement back — it must be addressed, or it replays anywhere.
        let known_id = node_id != format!("{ip}:{port}");
        let receiver = if known_id {
            node_id.clone()
        } else {
            crate::auth::UNBOUND_RECEIVER.to_string()
        };
        let Ok(peer) = format!("{}:{}", ip, port + bus_offset).parse::<SocketAddr>() else {
            tracing::warn!(%ip, port, "peer address does not parse");
            continue;
        };

        let mut frames = vec![GossipMessage::Ping {
            node_id: myself.clone(),
            ip: my_ip.clone(),
            port: my_port,
            ping_id: now_ms(),
        }];
        // The claim goes out when it changed, and otherwise every
        // `REANNOUNCE_EVERY`: a peer that missed one — a restart, a dropped
        // frame — still converges, without the full list every second.
        let announce = known_id && state.should_announce(&node_id, claim_hash);
        if announce {
            frames.push(GossipMessage::Update {
                node_id: myself.clone(),
                ip: my_ip.clone(),
                port: my_port,
                flags: vec!["master".to_string()],
                master_id: None,
                slots: my_slots.to_vec(),
            });
        }
        let secret = secret.clone();
        sends.spawn(async move {
            let res = send_frames(peer, &frames, &secret, connect_timeout, &receiver).await;
            (peer, node_id, announce, res)
        });
    }

    let mut reached = 0;
    while let Some(done) = sends.join_next().await {
        let Ok((peer, node_id, announced, res)) = done else {
            continue;
        };
        match res {
            Ok(()) => {
                reached += 1;
                if announced {
                    state.mark_announced(node_id, claim_hash);
                }
            }
            Err(e) => tracing::debug!(%peer, error = %e, "gossip ping failed"),
        }
    }

    reached
}

/// Drops the `CLUSTER MEET` placeholder for an address once the real node id
/// for that address is known.
///
/// `MEET` files a peer under `"ip:port"` because its identity is not known
/// yet. Two things then reveal it — the peer's pong, and the peer's own ping —
/// and **either** can arrive first.
///
/// The first version only reconciled when the real id was still absent, so a
/// peer whose ping beat its pong left the placeholder behind for good: five
/// rows for three nodes, two of them permanently `fail` because nothing ever
/// answers for an address that is already represented. Found on three real
/// machines; two would not have raced.
fn absorb_placeholder(
    nodes: &mut HashMap<String, ClusterNodeInfo>,
    node_id: &str,
    ip: &str,
    port: u16,
) {
    let placeholder = format!("{ip}:{port}");
    if placeholder == node_id {
        return;
    }
    let Some(stale) = nodes.remove(&placeholder) else {
        return;
    };

    // Keep whatever the real entry already learned; the placeholder only ever
    // held an address, which is the one thing both agree on.
    nodes.entry(node_id.to_string()).or_insert_with(|| {
        let mut node = stale;
        node.node_id = node_id.to_string();
        node
    });
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

#[cfg(test)]
mod placeholder_tests {
    use super::*;

    /// Everyone but this node. `get_all_nodes` always puts self first, which is
    /// right for `CLUSTER NODES` and wrong for counting peers.
    fn peers(state: &GossipState) -> Vec<ClusterNodeInfo> {
        let me = state.myself_id();
        state
            .get_all_nodes()
            .into_iter()
            .filter(|n| n.node_id != me)
            .collect()
    }

    /// Two outstanding `MEET`s, two pongs. Each peer must end up under its own
    /// address.
    ///
    /// The regression this exists for was found on three machines and could not
    /// have been found on two: with one placeholder there is nothing to confuse
    /// it with. Node 1 ended up believing peer A lived at peer B's address,
    /// because the reconciliation matched *any* entry that looked like a
    /// placeholder and `HashMap` order decided which.
    #[test]
    fn two_pending_meets_do_not_swap_their_peers() {
        let state = GossipState::new("me".into(), "10.0.0.1".into(), 6379);

        // What `CLUSTER MEET` leaves behind: an entry keyed by address,
        // because the real node id is not known until the peer answers.
        state.add_node("10.0.0.2:6379".into(), "10.0.0.2".into(), 6379);
        state.add_node("10.0.0.3:6379".into(), "10.0.0.3".into(), 6379);

        state.handle_pong("real-id-of-two", "10.0.0.2", 6379, 1);
        state.handle_pong("real-id-of-three", "10.0.0.3", 6379, 2);

        // `get_all_nodes` puts this node first, always. Peers are the rest.
        let by_id: std::collections::HashMap<String, String> = state
            .get_all_nodes()
            .into_iter()
            .filter(|n| n.node_id != "me")
            .map(|n| (n.node_id, n.ip))
            .collect();
        assert_eq!(
            by_id.get("real-id-of-two").map(String::as_str),
            Some("10.0.0.2")
        );
        assert_eq!(
            by_id.get("real-id-of-three").map(String::as_str),
            Some("10.0.0.3")
        );
        assert!(!by_id.contains_key("10.0.0.2:6379"), "placeholder survived");
        assert!(!by_id.contains_key("10.0.0.3:6379"), "placeholder survived");
    }

    #[test]
    fn a_pong_from_an_address_nobody_met_creates_nothing() {
        // A stranger's pong must not invent a member. It is authenticated, so
        // it is a cluster peer — but membership comes from MEET and from being
        // pinged, not from answering a ping nobody sent.
        let state = GossipState::new("me".into(), "10.0.0.1".into(), 6379);
        state.handle_pong("someone-else", "10.9.9.9", 6379, 1);
        assert_eq!(peers(&state).len(), 0);
    }

    #[test]
    fn a_ping_adds_its_sender_so_gossip_flows_both_ways() {
        // Without this, membership only ever propagated in the direction
        // someone had typed MEET.
        let state = GossipState::new("me".into(), "10.0.0.1".into(), 6379);
        state.handle_ping("peer-a", "10.0.0.2".to_string(), 6379, 7);
        assert_eq!(peers(&state).len(), 1);
        assert_eq!(peers(&state)[0].node_id, "peer-a");
    }

    #[test]
    fn a_node_does_not_add_itself_from_its_own_ping() {
        let state = GossipState::new("me".into(), "10.0.0.1".into(), 6379);
        state.handle_ping("me", "10.0.0.1".to_string(), 6379, 7);
        assert_eq!(peers(&state).len(), 0);
    }
}

#[cfg(test)]
mod placeholder_race_tests {
    use super::*;

    fn peers(state: &GossipState) -> Vec<ClusterNodeInfo> {
        let me = state.myself_id();
        state
            .get_all_nodes()
            .into_iter()
            .filter(|n| n.node_id != me)
            .collect()
    }

    /// The peer's ping arrives before its pong.
    ///
    /// Both reveal the same identity, and either may be first. The version that
    /// only reconciled when the id was still unknown left the placeholder
    /// behind on this ordering — five rows for three nodes on real machines,
    /// two of them permanently `fail`.
    #[test]
    fn a_ping_that_beats_the_pong_still_clears_the_placeholder() {
        let state = GossipState::new("me".into(), "10.0.0.1".into(), 6379);
        state.add_node("10.0.0.2:6379".into(), "10.0.0.2".into(), 6379);

        state.handle_ping("real-two", "10.0.0.2".to_string(), 6379, 1);
        state.handle_pong("real-two", "10.0.0.2", 6379, 1);

        let found = peers(&state);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].node_id, "real-two");
    }

    /// And the other ordering, which already worked.
    #[test]
    fn a_pong_that_arrives_first_also_clears_it() {
        let state = GossipState::new("me".into(), "10.0.0.1".into(), 6379);
        state.add_node("10.0.0.3:6379".into(), "10.0.0.3".into(), 6379);

        state.handle_pong("real-three", "10.0.0.3", 6379, 1);
        state.handle_ping("real-three", "10.0.0.3".to_string(), 6379, 1);

        assert_eq!(peers(&state).len(), 1);
    }

    /// Three nodes, both orderings at once — the shape that failed in Paris.
    #[test]
    fn three_nodes_converge_to_exactly_two_peers() {
        let state = GossipState::new("seed".into(), "10.0.0.1".into(), 6379);
        state.add_node("10.0.0.2:6379".into(), "10.0.0.2".into(), 6379);
        state.add_node("10.0.0.3:6379".into(), "10.0.0.3".into(), 6379);

        state.handle_ping("id-two", "10.0.0.2".to_string(), 6379, 1);
        state.handle_pong("id-three", "10.0.0.3", 6379, 2);
        state.handle_pong("id-two", "10.0.0.2", 6379, 1);
        state.handle_ping("id-three", "10.0.0.3".to_string(), 6379, 2);

        let found = peers(&state);
        assert_eq!(found.len(), 2, "{found:?}");
        let mut addresses: Vec<(String, String)> =
            found.into_iter().map(|n| (n.node_id, n.ip)).collect();
        addresses.sort();
        assert_eq!(
            addresses,
            vec![
                ("id-three".to_string(), "10.0.0.3".to_string()),
                ("id-two".to_string(), "10.0.0.2".to_string())
            ],
            "peers must keep their own addresses"
        );
    }
}

#[cfg(test)]
mod slot_propagation_tests {
    use super::*;

    #[test]
    fn a_slot_claim_survives_the_wire() {
        let ranges = vec![(0u16, 5460u16), (10923, 16383)];
        assert_eq!(encode_slots(&ranges), "0-5460,10923-16383");
        assert_eq!(decode_slots("0-5460,10923-16383"), ranges);
    }

    #[test]
    fn owning_nothing_is_distinguishable_from_owning_slot_zero() {
        // `-` and `0` are one character apart and mean opposite things: a node
        // that owns nothing, and a node that owns the slot every empty-string
        // key hashes to.
        assert_eq!(encode_slots(&[]), "-");
        assert!(decode_slots("-").is_empty());
        assert_eq!(decode_slots("0"), vec![(0, 0)]);
    }

    #[test]
    fn a_single_slot_is_written_as_one_number() {
        assert_eq!(encode_slots(&[(42, 42)]), "42");
        assert_eq!(decode_slots("42"), vec![(42, 42)]);
    }

    #[test]
    fn a_backwards_range_is_dropped_rather_than_iterated() {
        // `100-50` would make every reader walk an empty span and record no
        // owner, which reads as "that peer owns nothing" — a claim it never made.
        assert!(decode_slots("100-50").is_empty());
        assert_eq!(decode_slots("100-50,7-9"), vec![(7, 9)]);
    }

    #[test]
    fn a_garbled_range_does_not_discard_the_readable_ones() {
        // The sender's membership is still worth learning. Losing the whole frame
        // over one bad range turns a cosmetic fault into an unreachable node.
        assert_eq!(decode_slots("abc,7-9,x-y"), vec![(7, 9)]);
    }

    #[test]
    fn an_update_round_trips_with_a_master_and_with_slots() {
        let msg = GossipMessage::Update {
            node_id: "a".into(),
            ip: "10.0.0.1".into(),
            port: 6379,
            flags: vec!["master".into()],
            master_id: Some("b".into()),
            slots: vec![(0, 100)],
        };
        let wire = msg.encode();
        assert_eq!(GossipMessage::decode(&wire).unwrap(), msg);
    }

    #[test]
    fn no_master_and_some_slots_is_not_read_as_a_master_called_dash() {
        // The bug the fixed positions exist to prevent. `master_id` was an
        // optional trailing word, so "no master, here are slots" and "this
        // master, no slots" differed by position alone — and appending a second
        // optional tail would have made that a misparse instead of a hazard.
        let msg = GossipMessage::Update {
            node_id: "a".into(),
            ip: "10.0.0.1".into(),
            port: 6379,
            flags: vec!["master".into()],
            master_id: None,
            slots: vec![(0, 100)],
        };
        let decoded = GossipMessage::decode(&msg.encode()).unwrap();
        assert_eq!(decoded, msg);
        match decoded {
            GossipMessage::Update {
                master_id, slots, ..
            } => {
                assert!(master_id.is_none(), "the placeholder was read as a node id");
                assert_eq!(slots, vec![(0, 100)]);
            }
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn a_master_and_no_slots_round_trips_too() {
        let msg = GossipMessage::Update {
            node_id: "a".into(),
            ip: "10.0.0.1".into(),
            port: 6379,
            flags: vec!["slave".into()],
            master_id: Some("b".into()),
            slots: Vec::new(),
        };
        assert_eq!(GossipMessage::decode(&msg.encode()).unwrap(), msg);
    }

    #[test]
    fn a_full_claim_does_not_grow_the_frame_beyond_a_line() {
        // 16384 numbers is about 80 KiB, and this rides on the failure
        // detector's traffic. Coalescing is what keeps it a line.
        let one_range = encode_slots(&[(0, 16383)]);
        assert_eq!(one_range, "0-16383");
        assert!(one_range.len() < 16, "{one_range}");
    }
}

#[cfg(test)]
mod announce_tests {
    use super::*;

    #[test]
    fn a_claim_is_announced_on_change_and_periodically_not_every_round() {
        let state = GossipState::new("me".into(), "127.0.0.1".into(), 7000);
        assert!(state.should_announce("peer", 1));
        state.mark_announced("peer".into(), 1);
        assert!(!state.should_announce("peer", 1));
        assert!(state.should_announce("peer", 2));
        assert!(state.should_announce("other", 1));
        // Sent long enough ago to repeat.
        state.announced.write().insert(
            "peer".into(),
            (1, std::time::Instant::now() - REANNOUNCE_EVERY),
        );
        assert!(state.should_announce("peer", 1));
    }
}
