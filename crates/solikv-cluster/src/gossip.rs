//! The cluster bus: what nodes tell each other, and how it travels.
//!
//! # Messages
//!
//! JSON on one line, inside the authenticated envelope of `crate::auth`
//! (HMAC over timestamp, nonce, receiver and payload). Every message carries
//! the sender's [`Header`] — its identity and address, its role, its epochs,
//! its replication offset and the slots it claims — so any message refreshes
//! the receiver's view of the sender, as Redis Cluster's bus does. `PING`,
//! `PONG` and `MEET` add the sender's view of the other nodes ([`Seen`]),
//! which is how membership and failure reports spread.
//!
//! Redis' binary bus format is not used: clients never speak it (they use
//! `CLUSTER SLOTS`/`SHARDS`/`NODES` and redirections, which are Redis'), and
//! a SoliKV node only ever talks to SoliKV nodes on it.
//!
//! # Transport
//!
//! One persistent outbound connection per peer ([`Bus`]), reconnected when it
//! breaks; a peer's frames to us arrive on its own outbound connection. Gossip
//! is lossy by design — a message that cannot be sent is dropped, and the
//! next round repeats what mattered.
//!
//! # Authentication
//!
//! Every frame is sealed for one receiver, named by node id. Only `PING` and
//! `MEET` may be unaddressed (`*`): they are sent before the peer's id is
//! known, and carry no authority. One replay memory covers the whole bus.
//! The bus is not encrypted (see SECURITY.md).

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

pub const CLUSTER_BUS_PORT_OFFSET: u16 = 10000;

/// Most concurrent inbound bus connections. A peer holds one, so this is far
/// above what any real cluster needs — and bounds what strangers can hold.
pub const MAX_BUS_CONNECTIONS: usize = 256;

/// An inbound connection that delivers nothing for this long is dropped.
/// Peers ping at least every second, so a live one never comes close.
pub const BUS_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Connect and write timeout for outbound frames.
pub const BUS_IO_TIMEOUT: Duration = Duration::from_secs(3);

/// Frames queued for one peer while its connection is (re)established.
const LINK_QUEUE: usize = 1024;

/// A node, as it describes itself on every message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub cport: u16,
    #[serde(default)]
    pub hostname: String,
    /// The master this node replicates, or `None` for a master.
    pub master: Option<String>,
    pub current_epoch: u64,
    /// A master's config epoch; a replica sends its master's.
    pub config_epoch: u64,
    /// Replication offset (`master_repl_offset`, or what a replica applied).
    pub offset: u64,
    /// Slots claimed (a master's own; empty for a replica).
    pub slots: Vec<(u16, u16)>,
    /// A master pausing its clients for a manual failover.
    #[serde(default)]
    pub paused: bool,
    /// The sender considers the cluster up.
    #[serde(default)]
    pub state_ok: bool,
}

/// One node as the sender sees it (the gossip section).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seen {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub cport: u16,
    #[serde(default)]
    pub pfail: bool,
    #[serde(default)]
    pub fail: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    /// `ping_id` is echoed in the `PONG`: a node met by address only (in
    /// handshake, filed under a temporary id) is recognised by it.
    Ping {
        h: Header,
        ping_id: String,
        gossip: Vec<Seen>,
    },
    Pong {
        h: Header,
        ping_id: String,
        gossip: Vec<Seen>,
    },
    /// `CLUSTER MEET`: like a ping, but the receiver adds the sender.
    Meet {
        h: Header,
        ping_id: String,
        gossip: Vec<Seen>,
    },
    /// The sender marked `node` as failed (majority of masters agreed).
    Fail {
        h: Header,
        node: String,
    },
    /// `node`'s configuration is newer than what the receiver advertised.
    Update {
        h: Header,
        node: String,
        config_epoch: u64,
        slots: Vec<(u16, u16)>,
    },
    /// A replica asks the masters to vote for its promotion.
    AuthRequest {
        h: Header,
        /// Manual failover: vote even though the master is not failed.
        force: bool,
        /// The failed master's config epoch and slots.
        master_epoch: u64,
        master_slots: Vec<(u16, u16)>,
    },
    AuthAck {
        h: Header,
    },
    /// A replica asks its master to start a manual failover.
    MfStart {
        h: Header,
    },
    /// `PUBLISH` relayed to every node (`shard`: `SPUBLISH` to a shard's
    /// replicas). Hex, because channels and messages are binary.
    Publish {
        h: Header,
        channel: String,
        message: String,
        shard: bool,
    },
}

impl Message {
    pub fn header(&self) -> &Header {
        match self {
            Message::Ping { h, .. }
            | Message::Pong { h, .. }
            | Message::Meet { h, .. }
            | Message::Fail { h, .. }
            | Message::Update { h, .. }
            | Message::AuthRequest { h, .. }
            | Message::AuthAck { h }
            | Message::MfStart { h }
            | Message::Publish { h, .. } => h,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Message::Ping { .. } => "ping",
            Message::Pong { .. } => "pong",
            Message::Meet { .. } => "meet",
            Message::Fail { .. } => "fail",
            Message::Update { .. } => "update",
            Message::AuthRequest { .. } => "auth-req",
            Message::AuthAck { .. } => "auth-ack",
            Message::MfStart { .. } => "mfstart",
            Message::Publish { .. } => "publish",
        }
    }

    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("gossip messages serialize")
    }

    pub fn decode(text: &str) -> Option<Self> {
        let m: Message = serde_json::from_str(text).ok()?;
        // Slot ranges past the table are refused whole, not clamped: indexing
        // with one panicked the drain task once.
        let bad = |r: &[(u16, u16)]| r.iter().any(|(s, e)| s > e || *e >= 16384);
        let valid = !bad(&m.header().slots)
            && match &m {
                Message::Update { slots, .. } => !bad(slots),
                Message::AuthRequest { master_slots, .. } => !bad(master_slots),
                _ => true,
            };
        valid.then_some(m)
    }

    /// May arrive unaddressed: only the handshake, which carries no
    /// authority (see the module doc).
    pub fn may_be_unbound(&self) -> bool {
        matches!(self, Message::Ping { .. } | Message::Meet { .. })
    }
}

/// Contiguous ranges of the slots in `slots` (sorted or not).
pub fn ranges_of(slots: impl IntoIterator<Item = u16>) -> Vec<(u16, u16)> {
    let mut v: Vec<u16> = slots.into_iter().collect();
    v.sort_unstable();
    v.dedup();
    let mut out: Vec<(u16, u16)> = Vec::new();
    for s in v {
        match out.last_mut() {
            Some((_, e)) if *e + 1 == s => *e = s,
            _ => out.push((s, s)),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

/// Outbound links to the other nodes.
pub struct Bus {
    secret: crate::auth::ClusterSecret,
    links: Mutex<HashMap<String, Link>>,
    pub sent: AtomicU64,
    pub received: AtomicU64,
}

struct Link {
    addr: SocketAddr,
    tx: mpsc::Sender<(String, String)>,
    created: u64,
}

/// One row of `CLUSTER LINKS`.
pub struct LinkInfo {
    pub node: String,
    pub created_ms: u64,
}

impl Bus {
    pub fn new(secret: crate::auth::ClusterSecret) -> Arc<Self> {
        Arc::new(Self {
            secret,
            links: Mutex::new(HashMap::new()),
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
        })
    }

    /// Queue `msg` for the node filed under `key`, at `addr` (its bus port),
    /// sealed for `receiver` (its id, or `*` during the handshake). Never
    /// waits: a full queue drops the message.
    pub fn send(&self, key: &str, addr: SocketAddr, receiver: &str, msg: &Message) {
        let line = crate::auth::Envelope::seal(
            &msg.encode(),
            &self.secret,
            now_ms(),
            &crate::auth::new_nonce(),
            receiver,
        );
        let mut links = self.links.lock();
        let stale = links
            .get(key)
            .is_some_and(|l| l.addr != addr || l.tx.is_closed());
        if stale {
            links.remove(key);
        }
        let link = links.entry(key.to_string()).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(LINK_QUEUE);
            tokio::spawn(run_link(addr, rx));
            Link {
                addr,
                tx,
                created: now_ms(),
            }
        });
        if link.tx.try_send((receiver.to_string(), line)).is_ok() {
            self.sent.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Close the link filed under `key` (a forgotten node, a renamed one).
    pub fn forget(&self, key: &str) {
        self.links.lock().remove(key);
    }

    pub fn links(&self) -> Vec<LinkInfo> {
        self.links
            .lock()
            .iter()
            .map(|(k, l)| LinkInfo {
                node: k.clone(),
                created_ms: l.created,
            })
            .collect()
    }
}

/// One peer's outbound connection: connect, write queued frames, reconnect.
async fn run_link(addr: SocketAddr, mut rx: mpsc::Receiver<(String, String)>) {
    use tokio::io::AsyncWriteExt;
    loop {
        let stream = match tokio::time::timeout(
            BUS_IO_TIMEOUT,
            tokio::net::TcpStream::connect(addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            _ => {
                // Unreachable: what was queued is stale by the time it
                // could go. Drop it; the next round repeats what matters.
                loop {
                    match rx.try_recv() {
                        Ok(_) => continue,
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return,
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
                if rx.is_closed() && rx.is_empty() {
                    return;
                }
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let mut stream = stream;
        loop {
            let Some((_, line)) = rx.recv().await else {
                return;
            };
            let mut batch = line;
            while batch.len() < 256 * 1024 {
                match rx.try_recv() {
                    Ok((_, more)) => batch.push_str(&more),
                    Err(_) => break,
                }
            }
            let wrote =
                tokio::time::timeout(BUS_IO_TIMEOUT, stream.write_all(batch.as_bytes())).await;
            if !matches!(wrote, Ok(Ok(()))) {
                break;
            }
        }
    }
}

/// Serve the cluster bus: authenticate each frame, decode it, and hand it to
/// `msg_tx`. `me` is this node's id (frames must be addressed to it).
pub async fn start_bus_server(
    bind: SocketAddr,
    secret: crate::auth::ClusterSecret,
    me: Arc<parking_lot::RwLock<String>>,
    msg_tx: mpsc::UnboundedSender<Message>,
    bus: Arc<Bus>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(bind).await?;
    tracing::info!(%bind, "cluster bus listening, every frame authenticated");

    let seen = Arc::new(parking_lot::Mutex::new(crate::auth::NonceMemory::new()));
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_BUS_CONNECTIONS));
    let rejected_log = Arc::new(RateLimitedLog::default());

    tokio::spawn(async move {
        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    tracing::error!(error = %e, "cluster bus accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let Ok(permit) = permits.clone().try_acquire_owned() else {
                rejected_log.warn(|suppressed| {
                    tracing::warn!(%peer_addr, suppressed, "cluster bus connection limit reached");
                });
                continue;
            };
            let ctx = PeerContext {
                secret: secret.clone(),
                seen: seen.clone(),
                me: me.clone(),
                msg_tx: msg_tx.clone(),
                rejected_log: rejected_log.clone(),
                bus: bus.clone(),
            };
            tokio::spawn(async move {
                if let Err(e) = serve_peer(stream, peer_addr, ctx).await {
                    tracing::debug!(%peer_addr, error = %e, "cluster bus connection closed");
                }
                drop(permit);
            });
        }
    });
    Ok(())
}

struct PeerContext {
    secret: crate::auth::ClusterSecret,
    seen: Arc<parking_lot::Mutex<crate::auth::NonceMemory>>,
    me: Arc<parking_lot::RwLock<String>>,
    msg_tx: mpsc::UnboundedSender<Message>,
    rejected_log: Arc<RateLimitedLog>,
    bus: Arc<Bus>,
}

/// A warn that fires at most once a second, counting what it swallowed:
/// rejected frames come from the network, and one log line each would let
/// anyone who reaches the port fill the disk.
#[derive(Default)]
pub struct RateLimitedLog {
    last_ms: AtomicU64,
    suppressed: AtomicU64,
}

impl RateLimitedLog {
    pub fn warn(&self, emit: impl FnOnce(u64)) {
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

/// Reads one `\n`-terminated line of at most `MAX_FRAME_BYTES`, refused
/// before it is buffered whole. `Ok(None)` at a clean end of stream.
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
                "cluster bus frame over the size limit"
            } else {
                "cluster bus frame truncated"
            },
        ));
    }
    Ok(Some(()))
}

/// One inbound connection: a peer's outbound link to us.
async fn serve_peer(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    ctx: PeerContext,
) -> Result<(), std::io::Error> {
    let mut reader = tokio::io::BufReader::new(stream);
    let mut identity = crate::auth::SenderIdentity::new();
    let mut buf = Vec::with_capacity(4096);

    loop {
        let read = tokio::time::timeout(BUS_READ_TIMEOUT, read_frame(&mut reader, &mut buf))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "cluster bus read timed out")
            })??;
        if read.is_none() {
            break;
        }
        let Ok(line) = std::str::from_utf8(&buf) else {
            ctx.rejected_log.warn(|suppressed| {
                tracing::warn!(%peer_addr, suppressed, "rejected cluster bus frame: not UTF-8");
            });
            continue;
        };
        if line.trim().is_empty() {
            continue;
        }
        let me = ctx.me.read().clone();
        let opened = {
            let mut seen = ctx.seen.lock();
            crate::auth::Envelope::open(line, &ctx.secret, now_ms(), &mut seen, &me)
        };
        let opened = match opened {
            Ok(opened) => opened,
            Err(e) => {
                // Dropped, not a disconnect: far more often a drifted clock
                // than an attack.
                ctx.rejected_log.warn(|suppressed| {
                    tracing::warn!(%peer_addr, error = %e, suppressed, "rejected cluster bus frame");
                });
                continue;
            }
        };
        let Some(message) = Message::decode(&opened.payload) else {
            ctx.rejected_log.warn(|suppressed| {
                tracing::warn!(%peer_addr, suppressed, "authenticated frame is not a bus message");
            });
            continue;
        };
        if !opened.receiver_bound && !message.may_be_unbound() {
            ctx.rejected_log.warn(|suppressed| {
                tracing::warn!(
                    %peer_addr,
                    suppressed,
                    "rejected unaddressed bus frame that carries authority"
                );
            });
            continue;
        }
        if let Err(e) = identity.observe(&message.header().id) {
            ctx.rejected_log.warn(|suppressed| {
                tracing::warn!(%peer_addr, error = %e, suppressed, "cluster bus identity switch");
            });
            continue;
        }
        ctx.bus.received.fetch_add(1, Ordering::Relaxed);
        if ctx.msg_tx.send(message).is_err() {
            break;
        }
    }
    Ok(())
}

/// Wall-clock milliseconds.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A new node id: 40 hex characters, as Redis Cluster's.
pub fn generate_node_id() -> String {
    use rand::RngCore;
    let mut b = [0u8; 20];
    rand::thread_rng().fill_bytes(&mut b);
    hex::encode(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> Header {
        Header {
            id: "a".repeat(40),
            ip: "10.0.0.1".into(),
            port: 7000,
            cport: 17000,
            hostname: String::new(),
            master: None,
            current_epoch: 3,
            config_epoch: 2,
            offset: 99,
            slots: vec![(0, 5460), (16000, 16383)],
            paused: false,
            state_ok: true,
        }
    }

    #[test]
    fn messages_round_trip() {
        let msgs = vec![
            Message::Ping {
                h: header(),
                ping_id: "x".into(),
                gossip: vec![Seen {
                    id: "b".repeat(40),
                    ip: "10.0.0.2".into(),
                    port: 7001,
                    cport: 17001,
                    pfail: true,
                    fail: false,
                }],
            },
            Message::Fail {
                h: header(),
                node: "b".repeat(40),
            },
            Message::AuthRequest {
                h: header(),
                force: true,
                master_epoch: 1,
                master_slots: vec![(1, 2)],
            },
            Message::Publish {
                h: header(),
                channel: hex::encode(b"ch\n"),
                message: hex::encode([0u8, 255]),
                shard: false,
            },
        ];
        for m in msgs {
            let line = m.encode();
            assert!(!line.contains('\n'), "a message must fit on one line");
            assert_eq!(Message::decode(&line), Some(m));
        }
    }

    #[test]
    fn a_slot_past_the_table_refuses_the_message() {
        let mut h = header();
        h.slots = vec![(0, 16384)];
        let m = Message::AuthAck { h };
        assert_eq!(Message::decode(&m.encode()), None);
        let mut h = header();
        h.slots = vec![(10, 5)];
        assert_eq!(Message::decode(&Message::AuthAck { h }.encode()), None);
    }

    #[test]
    fn only_the_handshake_may_be_unaddressed() {
        let h = header();
        assert!(Message::Ping {
            h: h.clone(),
            ping_id: String::new(),
            gossip: vec![]
        }
        .may_be_unbound());
        assert!(!Message::Pong {
            h: h.clone(),
            ping_id: String::new(),
            gossip: vec![]
        }
        .may_be_unbound());
        assert!(!Message::Update {
            h,
            node: String::new(),
            config_epoch: 0,
            slots: vec![]
        }
        .may_be_unbound());
    }

    #[test]
    fn ranges_coalesce() {
        assert_eq!(
            ranges_of([5, 1, 2, 3, 9, 10]),
            vec![(1, 3), (5, 5), (9, 10)]
        );
        assert!(ranges_of([]).is_empty());
    }

    #[test]
    fn node_ids_look_like_redis_ones() {
        let id = generate_node_id();
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(id, generate_node_id());
    }
}
