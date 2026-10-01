//! Cluster state: who is in the cluster, who serves which slot, who has
//! failed, and who takes over.
//!
//! This follows the Redis Cluster specification closely enough that Redis
//! cluster clients and `redis-cli --cluster` work against it:
//!
//! - **Slots and epochs.** Each slot has one owner. A master claims its slots
//!   on every bus message, with its *config epoch*; a claim with a higher
//!   epoch than the current owner's wins, which is how a failover's new
//!   master — or the target of a migrated slot — takes over everywhere. Two
//!   masters with the same config epoch resolve it: the one with the smaller
//!   id takes a new epoch.
//! - **Failure detection.** A node that has not answered a ping for
//!   `node_timeout` is `PFAIL` here. Masters report what they see in their
//!   gossip; once a majority of the masters serving slots report a node, it
//!   is `FAIL`, and a `FAIL` message tells everyone at once.
//! - **Failover.** The replicas of a failed master wait a delay that grows
//!   with how far behind they are, then ask the masters for votes for a new
//!   epoch. Each master votes once per epoch; the replica with a majority
//!   takes the slots with that epoch. `CLUSTER FAILOVER` does the same on
//!   demand: by default after the master paused its clients and the replica
//!   caught up, `FORCE` without the catch-up, `TAKEOVER` without a vote.
//! - **Migration.** `CLUSTER SETSLOT … MIGRATING/IMPORTING` mark a slot moving
//!   between two nodes: the source answers `-ASK` for keys it no longer has,
//!   the target accepts commands preceded by `ASKING`, and `SETSLOT … NODE`
//!   ends it (the target takes the slot with a new epoch).
//!
//! The command path consults three bitsets (slots served here, slots in
//! migration, slots this replica's master serves) and one flag (cluster up):
//! atomic loads, no lock. Everything else is under one mutex, taken by the bus
//! handler, the once-every-100-ms cron, and `CLUSTER` commands.

use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use solikv_core::CommandResponse;

use crate::gossip::{generate_node_id, now_ms, ranges_of, Bus, Header, Message, Seen};

pub const SLOTS: usize = 16384;
const SLOT_WORDS: usize = SLOTS / 64;

/// What the cluster asks of the server it runs in.
pub trait ClusterHost: Send + Sync {
    /// Replication offset (master: bytes produced; replica: bytes applied).
    fn repl_offset(&self) -> u64;
    /// Become a replica of the node at `ip:port`.
    fn replicate(&self, ip: &str, port: u16);
    /// Stop replicating: become a master.
    fn promote(&self);
    /// Hold clients' writes for `ms` (0: release them now).
    fn pause_writes(&self, ms: u64);
    /// Deliver a `PUBLISH` relayed by another node to local subscribers.
    fn deliver_publish(&self, channel: &[u8], message: &[u8], shard: bool);
    /// Keys in database 0.
    fn dbsize(&self) -> u64;
    fn count_keys_in_slot(&self, slot: u16) -> u64;
    fn keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes>;
}

/// Where the command path should send a key's slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRoute {
    /// Served here, nothing special.
    Local,
    /// Served here, but migrating away (or this node is importing it): the
    /// caller checks the keys (`-ASK`, `-TRYAGAIN`, `ASKING`).
    Special,
    /// Not served here: `-MOVED` (or `READONLY` on a replica of its owner).
    Moved,
    /// The cluster is down: `-CLUSTERDOWN`.
    Down,
}

/// Compatibility name kept for callers that only need local/remote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Local,
    Remote(u16),
}

/// A node in the table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub cport: u16,
    #[serde(default)]
    pub hostname: String,
    /// The master it replicates; `None` for a master.
    pub master: Option<String>,
    pub config_epoch: u64,
    #[serde(default)]
    pub fail: bool,
    /// Met by address only: `id` is temporary until its first `PONG`.
    #[serde(default)]
    pub handshake: bool,
    #[serde(skip)]
    pub pfail: bool,
    /// When the oldest unanswered ping went out (0: none outstanding).
    #[serde(skip)]
    pub ping_sent: u64,
    #[serde(skip)]
    pub pong_recv: u64,
    /// When this node last sent it a ping (scheduling).
    #[serde(skip)]
    pub last_ping: u64,
    #[serde(skip)]
    pub fail_time: u64,
    /// Masters reporting it as failing, with when.
    #[serde(skip)]
    pub fail_reports: HashMap<String, u64>,
    #[serde(skip)]
    pub offset: u64,
    /// When this master last voted for one of its replicas.
    #[serde(skip)]
    pub voted_time: u64,
    /// Pausing for a manual failover (its header said so).
    #[serde(skip)]
    pub paused: bool,
    #[serde(skip)]
    pub created: u64,
}

impl Node {
    fn new(id: String, ip: String, port: u16, cport: u16) -> Self {
        Self {
            id,
            ip,
            port,
            cport,
            hostname: String::new(),
            master: None,
            config_epoch: 0,
            fail: false,
            handshake: false,
            pfail: false,
            ping_sent: 0,
            pong_recv: 0,
            last_ping: 0,
            fail_time: 0,
            fail_reports: HashMap::new(),
            offset: 0,
            voted_time: 0,
            paused: false,
            created: now_ms(),
        }
    }

    fn bus_addr(&self) -> Option<SocketAddr> {
        format!("{}:{}", self.ip, self.cport)
            .parse()
            .ok()
            .or_else(|| {
                use std::net::ToSocketAddrs;
                (self.ip.as_str(), self.cport)
                    .to_socket_addrs()
                    .ok()?
                    .next()
            })
    }

    pub fn is_master(&self) -> bool {
        self.master.is_none()
    }
}

/// A replica's election (see the module doc).
#[derive(Debug, Default, Clone)]
struct Election {
    /// When to ask for votes (0: not scheduled).
    auth_time: u64,
    sent: bool,
    epoch: u64,
    votes: HashSet<String>,
    /// `CLUSTER FAILOVER` (not `TAKEOVER`): votes are asked with `force`.
    manual: Option<Manual>,
}

#[derive(Debug, Clone)]
struct Manual {
    deadline: u64,
    /// Default mode: waiting for the master's paused offset.
    master_offset: Option<u64>,
    ready: bool,
}

/// Everything under the cluster lock.
pub struct State {
    pub myself: String,
    pub nodes: HashMap<String, Node>,
    pub current_epoch: u64,
    pub last_vote_epoch: u64,
    owners: Vec<Option<String>>,
    migrating: BTreeMap<u16, String>,
    importing: BTreeMap<u16, String>,
    /// Forgotten nodes, not re-added from gossip until the time given.
    blacklist: HashMap<String, u64>,
    ok: bool,
    election: Election,
    /// Master side of a manual failover: the replica, and until when.
    mf: Option<(String, u64)>,
    dirty: bool,
}

/// Bus messages and server actions a state change produced, carried out
/// once the lock is released.
#[derive(Default)]
struct Effects {
    sends: Vec<(String, SocketAddr, String, Message)>,
    replicate: Option<(String, u16)>,
    promote: bool,
    pause: Option<u64>,
    publish: Vec<(Vec<u8>, Vec<u8>, bool)>,
}

/// Settings fixed at startup.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_timeout_ms: u64,
    pub require_full_coverage: bool,
    /// `cluster.json`, or `None` to keep nothing on disk (tests).
    pub config_path: Option<PathBuf>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_timeout_ms: 15_000,
            require_full_coverage: true,
            config_path: None,
        }
    }
}

struct Bits(Box<[AtomicU64]>);

impl Bits {
    fn new() -> Self {
        Self((0..SLOT_WORDS).map(|_| AtomicU64::new(0)).collect())
    }
    #[inline]
    fn get(&self, slot: u16) -> bool {
        self.0[slot as usize / 64].load(Ordering::Acquire) & (1 << (slot % 64)) != 0
    }
    fn set_all(&self, f: impl Fn(usize) -> bool) {
        for w in 0..SLOT_WORDS {
            let mut v = 0u64;
            for b in 0..64 {
                if f(w * 64 + b) {
                    v |= 1 << b;
                }
            }
            self.0[w].store(v, Ordering::Release);
        }
    }
}

pub struct ClusterManager {
    state: Mutex<State>,
    cfg: ClusterConfig,
    enabled: AtomicBool,
    ok: AtomicBool,
    /// Slots this node serves.
    mine: Bits,
    /// Slots migrating from or importing to this node.
    special: Bits,
    /// Slots this node's master serves (a replica answers them under
    /// `READONLY`).
    masters: Bits,
    /// This node's id, readable by the bus server without the state lock.
    my_id: Arc<parking_lot::RwLock<String>>,
    bus: OnceLock<Arc<Bus>>,
    host: OnceLock<Arc<dyn ClusterHost>>,
}

/// Persisted form of the state (`cluster.json`).
#[derive(Serialize, Deserialize)]
struct Saved {
    version: u32,
    myself: String,
    current_epoch: u64,
    last_vote_epoch: u64,
    nodes: Vec<Node>,
    /// `(start, end, owner)`.
    slots: Vec<(u16, u16, String)>,
    #[serde(default)]
    migrating: Vec<(u16, String)>,
    #[serde(default)]
    importing: Vec<(u16, String)>,
}

/// The format before version 2, still read.
#[derive(Deserialize)]
struct SavedV1 {
    node_id: String,
    #[serde(default)]
    slot_owners: Vec<(u16, u16, String)>,
    #[serde(default)]
    known_nodes: Vec<(String, u16)>,
}

/// Kept for `--cluster-dump` callers: the old export type.
pub type ClusterStateSnapshot = serde_json::Value;

impl ClusterManager {
    /// A node with this address. Reads `cfg.config_path` if it exists (its
    /// id, epochs, nodes and slots survive restarts, as `nodes.conf` does);
    /// otherwise starts as a fresh master owning nothing.
    pub fn new(ip: String, port: u16, cport: u16, cfg: ClusterConfig) -> Self {
        let mut state = State {
            myself: String::new(),
            nodes: HashMap::new(),
            current_epoch: 0,
            last_vote_epoch: 0,
            owners: vec![None; SLOTS],
            migrating: BTreeMap::new(),
            importing: BTreeMap::new(),
            blacklist: HashMap::new(),
            ok: false,
            election: Election::default(),
            mf: None,
            dirty: false,
        };
        let loaded = cfg
            .config_path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|text| load_saved(&text, &mut state));
        if loaded.is_none() {
            state.myself = generate_node_id();
            state.dirty = true;
        }
        let myself = state.myself.clone();
        let me = state
            .nodes
            .entry(myself.clone())
            .or_insert_with(|| Node::new(myself.clone(), ip.clone(), port, cport));
        // The address is the one this run was started with.
        me.ip = ip;
        me.port = port;
        me.cport = cport;
        me.handshake = false;
        me.fail = false;
        let m = Self {
            state: Mutex::new(state),
            cfg,
            enabled: AtomicBool::new(true),
            ok: AtomicBool::new(false),
            mine: Bits::new(),
            special: Bits::new(),
            masters: Bits::new(),
            my_id: Arc::new(parking_lot::RwLock::new(myself)),
            bus: OnceLock::new(),
            host: OnceLock::new(),
        };
        {
            let mut st = m.state.lock();
            m.refresh(&mut st);
        }
        m
    }

    /// Attach the bus and the server (once, before the cron runs).
    pub fn attach(&self, bus: Arc<Bus>, host: Arc<dyn ClusterHost>) {
        let _ = self.bus.set(bus);
        let _ = self.host.set(host);
    }

    /// This node's id, shared with the bus server.
    pub fn my_id_handle(&self) -> Arc<parking_lot::RwLock<String>> {
        self.my_id.clone()
    }

    pub fn node_id(&self) -> String {
        self.my_id.read().clone()
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn node_timeout(&self) -> u64 {
        self.cfg.node_timeout_ms
    }

    // -----------------------------------------------------------------------
    // The command path (lock-free)
    // -----------------------------------------------------------------------

    #[inline]
    pub fn key_slot(&self, key: &[u8]) -> u16 {
        solikv_core::keyspace::hash_slot(key)
    }

    #[inline]
    pub fn slot_route(&self, slot: u16) -> SlotRoute {
        if !self.ok.load(Ordering::Relaxed) {
            return SlotRoute::Down;
        }
        if self.mine.get(slot) {
            if self.special.get(slot) {
                SlotRoute::Special
            } else {
                SlotRoute::Local
            }
        } else if self.special.get(slot) {
            SlotRoute::Special
        } else {
            SlotRoute::Moved
        }
    }

    /// Local or not, ignoring cluster health (old callers, tests).
    #[inline]
    pub fn route(&self, key: &[u8]) -> Route {
        let slot = self.key_slot(key);
        if self.mine.get(slot) {
            Route::Local
        } else {
            Route::Remote(slot)
        }
    }

    pub fn is_my_slot(&self, key: &[u8]) -> bool {
        self.route(key) == Route::Local
    }

    /// The slot is served by this replica's master (`READONLY` reads).
    #[inline]
    pub fn master_serves(&self, slot: u16) -> bool {
        self.masters.get(slot)
    }

    pub fn is_up(&self) -> bool {
        self.ok.load(Ordering::Relaxed)
    }

    /// Address of the slot's owner, for `-MOVED`.
    pub fn slot_owner_addr(&self, slot: u16) -> Option<(String, u16)> {
        let st = self.state.lock();
        let owner = st.owners.get(slot as usize)?.as_ref()?;
        let n = st.nodes.get(owner)?;
        Some((n.ip.clone(), n.port))
    }

    /// Where a migrating slot is going, for `-ASK`.
    pub fn migrating_to(&self, slot: u16) -> Option<(String, u16)> {
        let st = self.state.lock();
        let to = st.migrating.get(&slot)?;
        let n = st.nodes.get(to)?;
        Some((n.ip.clone(), n.port))
    }

    pub fn is_importing(&self, slot: u16) -> bool {
        self.state.lock().importing.contains_key(&slot)
    }

    pub fn is_replica(&self) -> bool {
        let st = self.state.lock();
        st.nodes.get(&st.myself).is_some_and(|n| n.master.is_some())
    }

    /// Owner of a slot (node id).
    pub fn slot_owner(&self, slot: u16) -> Option<String> {
        self.state.lock().owners.get(slot as usize)?.clone()
    }

    // -----------------------------------------------------------------------
    // Derived state
    // -----------------------------------------------------------------------

    /// Recompute what the command path reads, after any change.
    fn refresh(&self, st: &mut State) {
        let me = st.myself.clone();
        let my_master = st.nodes.get(&me).and_then(|n| n.master.clone());
        self.mine
            .set_all(|s| st.owners[s].as_deref() == Some(me.as_str()));
        self.special.set_all(|s| {
            let s = s as u16;
            st.migrating.contains_key(&s) || st.importing.contains_key(&s)
        });
        self.masters
            .set_all(|s| my_master.is_some() && st.owners[s].as_deref() == my_master.as_deref());
        let ok = self.compute_ok(st);
        if ok != st.ok {
            tracing::info!("cluster state changed: {}", if ok { "ok" } else { "fail" });
        }
        st.ok = ok;
        self.ok.store(ok, Ordering::Release);
        *self.my_id.write() = me;
    }

    fn compute_ok(&self, st: &State) -> bool {
        if self.cfg.require_full_coverage {
            for o in &st.owners {
                match o {
                    None => return false,
                    Some(id) => {
                        if st.nodes.get(id).is_none_or(|n| n.fail) {
                            return false;
                        }
                    }
                }
            }
        }
        // A master in a minority partition stops answering: it cannot reach
        // a majority of the masters serving slots.
        let me = &st.myself;
        let masters = serving_masters(st);
        if masters.contains(me) && masters.len() > 1 {
            let reachable = masters
                .iter()
                .filter(|id| *id == me || st.nodes.get(*id).is_some_and(|n| !n.pfail && !n.fail))
                .count();
            if reachable < masters.len() / 2 + 1 {
                return false;
            }
        }
        true
    }

    fn header(&self, st: &State) -> Header {
        let me = st.nodes.get(&st.myself).expect("myself is in the table");
        let offset = self.host.get().map_or(0, |h| h.repl_offset());
        let (config_epoch, slots) = match &me.master {
            None => (me.config_epoch, slots_of(st, &st.myself)),
            Some(m) => (st.nodes.get(m).map_or(0, |n| n.config_epoch), Vec::new()),
        };
        Header {
            id: me.id.clone(),
            ip: me.ip.clone(),
            port: me.port,
            cport: me.cport,
            hostname: me.hostname.clone(),
            master: me.master.clone(),
            current_epoch: st.current_epoch,
            config_epoch,
            offset,
            slots,
            paused: st.mf.is_some(),
            state_ok: st.ok,
        }
    }

    fn gossip_section(st: &State, exclude: &str) -> Vec<Seen> {
        st.nodes
            .values()
            .filter(|n| n.id != exclude && !n.handshake)
            .map(|n| Seen {
                id: n.id.clone(),
                ip: n.ip.clone(),
                port: n.port,
                cport: n.cport,
                pfail: n.pfail,
                fail: n.fail,
            })
            .collect()
    }

    fn send_to(&self, st: &State, fx: &mut Effects, id: &str, msg: Message) {
        let Some(n) = st.nodes.get(id) else { return };
        if n.id == st.myself {
            return;
        }
        let Some(addr) = n.bus_addr() else { return };
        let receiver = if n.handshake {
            crate::auth::UNBOUND_RECEIVER.to_string()
        } else {
            n.id.clone()
        };
        fx.sends.push((n.id.clone(), addr, receiver, msg));
    }

    fn ping_msg(&self, st: &State, to: &str, kind: &str, ping_id: String) -> Message {
        let h = self.header(st);
        let gossip = Self::gossip_section(st, to);
        match kind {
            "meet" => Message::Meet { h, ping_id, gossip },
            "pong" => Message::Pong { h, ping_id, gossip },
            _ => Message::Ping { h, ping_id, gossip },
        }
    }

    fn broadcast(&self, st: &State, fx: &mut Effects, make: impl Fn(&State) -> Message) {
        let ids: Vec<String> = st
            .nodes
            .values()
            .filter(|n| n.id != st.myself && !n.handshake)
            .map(|n| n.id.clone())
            .collect();
        for id in ids {
            let m = make(st);
            self.send_to(st, fx, &id, m);
        }
    }

    fn broadcast_pong(&self, st: &State, fx: &mut Effects) {
        let ids: Vec<String> = st
            .nodes
            .values()
            .filter(|n| n.id != st.myself && !n.handshake)
            .map(|n| n.id.clone())
            .collect();
        for id in ids {
            let m = self.ping_msg(st, &id, "pong", String::new());
            self.send_to(st, fx, &id, m);
        }
    }

    /// Run the effects, outside the lock.
    fn run(&self, fx: Effects) {
        if let Some(bus) = self.bus.get() {
            for (key, addr, receiver, msg) in &fx.sends {
                bus.send(key, *addr, receiver, msg);
            }
        }
        let Some(host) = self.host.get() else { return };
        if let Some(ms) = fx.pause {
            host.pause_writes(ms);
        }
        if fx.promote {
            host.promote();
        }
        if let Some((ip, port)) = fx.replicate {
            host.replicate(&ip, port);
        }
        for (ch, msg, shard) in fx.publish {
            host.deliver_publish(&ch, &msg, shard);
        }
    }

    fn finish(&self, mut st: parking_lot::MutexGuard<'_, State>, fx: Effects) {
        self.refresh(&mut st);
        if st.dirty {
            st.dirty = false;
            self.save(&st);
        }
        drop(st);
        self.run(fx);
    }

    /// Write `cluster.json` (tmp + rename).
    fn save(&self, st: &State) {
        let Some(path) = &self.cfg.config_path else {
            return;
        };
        let mut slots = Vec::new();
        let mut i = 0usize;
        while i < SLOTS {
            let Some(owner) = &st.owners[i] else {
                i += 1;
                continue;
            };
            let start = i;
            while i + 1 < SLOTS && st.owners[i + 1].as_ref() == Some(owner) {
                i += 1;
            }
            slots.push((start as u16, i as u16, owner.clone()));
            i += 1;
        }
        let saved = Saved {
            version: 2,
            myself: st.myself.clone(),
            current_epoch: st.current_epoch,
            last_vote_epoch: st.last_vote_epoch,
            nodes: st
                .nodes
                .values()
                .filter(|n| !n.handshake)
                .cloned()
                .collect(),
            slots,
            migrating: st.migrating.iter().map(|(s, n)| (*s, n.clone())).collect(),
            importing: st.importing.iter().map(|(s, n)| (*s, n.clone())).collect(),
        };
        let Ok(text) = serde_json::to_string_pretty(&saved) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        let write = || -> std::io::Result<()> {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new();
            f.write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                f.mode(0o600);
            }
            let mut f = f.open(&tmp)?;
            f.write_all(text.as_bytes())?;
            f.sync_all()?;
            std::fs::rename(&tmp, path)
        };
        if let Err(e) = write() {
            tracing::warn!(
                "cannot save cluster configuration to {}: {}",
                path.display(),
                e
            );
        }
    }

    // -----------------------------------------------------------------------
    // The bus
    // -----------------------------------------------------------------------

    /// Apply one authenticated bus message.
    pub fn handle(&self, msg: Message) {
        let mut st = self.state.lock();
        let mut fx = Effects::default();
        self.handle_locked(&mut st, &mut fx, msg, now_ms());
        self.finish(st, fx);
    }

    fn handle_locked(&self, st: &mut State, fx: &mut Effects, msg: Message, now: u64) {
        let h = msg.header().clone();
        if h.id == st.myself {
            return;
        }
        if h.current_epoch > st.current_epoch {
            st.current_epoch = h.current_epoch;
            st.dirty = true;
        }
        let known = st.nodes.contains_key(&h.id);

        // Who it is: an unknown sender is admitted on MEET or PING (the bus
        // is authenticated: only a member holds the secret), and a PONG
        // completes the handshake of a node met by address.
        if !known {
            match &msg {
                Message::Meet { .. } | Message::Ping { .. } => {
                    if st.blacklist.get(&h.id).is_some_and(|&t| t > now) {
                        return;
                    }
                    let mut n = Node::new(h.id.clone(), h.ip.clone(), h.port, h.cport);
                    n.master = h.master.clone();
                    n.config_epoch = if h.master.is_none() {
                        h.config_epoch
                    } else {
                        0
                    };
                    st.nodes.insert(h.id.clone(), n);
                    st.dirty = true;
                }
                Message::Pong { ping_id, .. } => {
                    let temp = st
                        .nodes
                        .values()
                        .find(|n| n.handshake && n.id == *ping_id)
                        .map(|n| n.id.clone());
                    let Some(temp) = temp else { return };
                    st.nodes.remove(&temp);
                    if let Some(bus) = self.bus.get() {
                        bus.forget(&temp);
                    }
                    let mut n = Node::new(h.id.clone(), h.ip.clone(), h.port, h.cport);
                    n.master = h.master.clone();
                    st.nodes.insert(h.id.clone(), n);
                    st.dirty = true;
                }
                _ => return,
            }
        }

        // Its address and role, as it states them.
        let mut role_changed_to_replica = false;
        if let Some(n) = st.nodes.get_mut(&h.id) {
            if n.ip != h.ip || n.port != h.port || n.cport != h.cport {
                n.ip = h.ip.clone();
                n.port = h.port;
                n.cport = h.cport;
                st.dirty = true;
            }
            n.hostname = h.hostname.clone();
            n.offset = h.offset;
            n.paused = h.paused;
            if n.master != h.master {
                role_changed_to_replica = h.master.is_some() && n.master.is_none();
                n.master = h.master.clone();
                st.dirty = true;
            }
            if h.master.is_none() && h.config_epoch > n.config_epoch {
                n.config_epoch = h.config_epoch;
                st.dirty = true;
            }
        }
        if role_changed_to_replica {
            // A master that became a replica serves nothing any more.
            for o in st.owners.iter_mut() {
                if o.as_deref() == Some(h.id.as_str()) {
                    *o = None;
                }
            }
        }

        match &msg {
            Message::Ping {
                ping_id, gossip, ..
            }
            | Message::Meet {
                ping_id, gossip, ..
            } => {
                self.process_gossip(st, fx, &h.id, gossip, now);
                let pong = self.ping_msg(st, &h.id, "pong", ping_id.clone());
                self.send_to(st, fx, &h.id, pong);
            }
            Message::Pong { gossip, .. } => {
                if let Some(n) = st.nodes.get_mut(&h.id) {
                    n.pong_recv = now;
                    n.ping_sent = 0;
                    n.pfail = false;
                    if n.fail {
                        let serves = st
                            .owners
                            .iter()
                            .any(|o| o.as_deref() == Some(n.id.as_str()));
                        if n.master.is_some()
                            || !serves
                            || now.saturating_sub(n.fail_time) > 2 * self.cfg.node_timeout_ms
                        {
                            n.fail = false;
                            st.dirty = true;
                            tracing::info!(node = %h.id, "node reachable again: FAIL cleared");
                        }
                    }
                }
                self.process_gossip(st, fx, &h.id, gossip, now);
            }
            Message::Fail { node, .. } => {
                if *node != st.myself {
                    if let Some(n) = st.nodes.get_mut(node) {
                        if !n.fail {
                            n.fail = true;
                            n.fail_time = now;
                            n.pfail = false;
                            st.dirty = true;
                            tracing::info!(%node, reporter = %h.id, "node marked FAIL by another node");
                        }
                    }
                }
            }
            Message::Update {
                node,
                config_epoch,
                slots,
                ..
            } => {
                let newer = st
                    .nodes
                    .get(node)
                    .is_some_and(|n| *config_epoch > n.config_epoch);
                if newer {
                    if let Some(n) = st.nodes.get_mut(node) {
                        n.config_epoch = *config_epoch;
                    }
                    let node = node.clone();
                    self.claim_slots(st, fx, &node, *config_epoch, slots);
                }
            }
            Message::AuthRequest {
                force,
                master_epoch,
                master_slots,
                ..
            } => self.vote(st, fx, &h, *force, *master_epoch, master_slots, now),
            Message::AuthAck { .. } => {
                let serves = st
                    .owners
                    .iter()
                    .any(|o| o.as_deref() == Some(h.id.as_str()));
                if serves
                    && h.master.is_none()
                    && st.election.sent
                    && h.current_epoch >= st.election.epoch
                {
                    st.election.votes.insert(h.id.clone());
                    let quorum = serving_masters(st).len() / 2 + 1;
                    if st.election.votes.len() >= quorum {
                        let epoch = st.election.epoch;
                        self.take_over(st, fx, epoch, "won the election");
                    }
                }
            }
            Message::MfStart { .. } => {
                let mine = st
                    .nodes
                    .get(&h.id)
                    .is_some_and(|n| n.master.as_deref() == Some(st.myself.as_str()));
                if mine && st.nodes.get(&st.myself).is_some_and(|n| n.master.is_none()) {
                    let timeout = 2 * MANUAL_FAILOVER_MS;
                    st.mf = Some((h.id.clone(), now + timeout));
                    fx.pause = Some(timeout);
                    tracing::info!(replica = %h.id, "manual failover requested: pausing clients");
                    // Tell it the offset to reach, now.
                    let ping = self.ping_msg(st, &h.id, "ping", String::new());
                    self.send_to(st, fx, &h.id, ping);
                }
            }
            Message::Publish {
                channel,
                message,
                shard,
                ..
            } => {
                if let (Ok(c), Ok(m)) = (hex::decode(channel), hex::decode(message)) {
                    fx.publish.push((c, m, *shard));
                }
            }
        }

        // Slots, last: a master's claim, with its config epoch.
        if h.master.is_none() && !h.slots.is_empty() {
            let claim = h.slots.clone();
            self.claim_slots(st, fx, &h.id, h.config_epoch, &claim);
        }
        // Equal config epochs between two masters: the smaller id moves on.
        let me_master = st.nodes.get(&st.myself).is_some_and(|n| n.master.is_none());
        let my_epoch = st.nodes.get(&st.myself).map_or(0, |n| n.config_epoch);
        if me_master && h.master.is_none() && h.config_epoch == my_epoch && st.myself < h.id {
            st.current_epoch += 1;
            let e = st.current_epoch;
            if let Some(me) = st.nodes.get_mut(&st.myself.clone()) {
                me.config_epoch = e;
            }
            st.dirty = true;
            tracing::info!(
                epoch = e,
                "config epoch collision resolved: took a new epoch"
            );
        }

        // A replica following its master's manual failover.
        if let Some(m) = &mut st.election.manual {
            let my_master = st.nodes.get(&st.myself).and_then(|n| n.master.clone());
            if my_master.as_deref() == Some(h.id.as_str()) && h.paused {
                m.master_offset = Some(h.offset);
            }
        }
    }

    /// Take `slots` for `sender` where its `epoch` beats the owner's.
    fn claim_slots(
        &self,
        st: &mut State,
        fx: &mut Effects,
        sender: &str,
        epoch: u64,
        slots: &[(u16, u16)],
    ) {
        let me = st.myself.clone();
        let mine_before = st
            .owners
            .iter()
            .filter(|o| o.as_deref() == Some(me.as_str()))
            .count();
        let my_master = st.nodes.get(&me).and_then(|n| n.master.clone());
        let master_before = my_master
            .as_ref()
            .map(|m| {
                st.owners
                    .iter()
                    .filter(|o| o.as_deref() == Some(m.as_str()))
                    .count()
            })
            .unwrap_or(0);
        let mut stale_owner: Option<String> = None;
        for &(s, e) in slots {
            for slot in s..=e {
                let i = slot as usize;
                let owner = st.owners[i].clone();
                if owner.as_deref() == Some(sender) {
                    continue;
                }
                // Importing it ourselves: the claim settles when SETSLOT
                // NODE does, not before.
                if st.importing.contains_key(&slot) {
                    continue;
                }
                let owner_epoch = owner
                    .as_ref()
                    .and_then(|o| st.nodes.get(o))
                    .map(|n| n.config_epoch);
                match owner_epoch {
                    Some(oe) if oe > epoch => stale_owner = owner,
                    Some(oe) if oe == epoch && owner.is_some() => {
                        // A tie is the collision rule's to break; meanwhile
                        // keep the current owner.
                    }
                    _ => {
                        st.owners[i] = Some(sender.to_string());
                        if owner.as_deref() == Some(me.as_str()) {
                            st.migrating.remove(&slot);
                        }
                        st.dirty = true;
                    }
                }
            }
        }
        // The sender is behind: tell it who owns what it claims.
        if let Some(owner) = stale_owner {
            if let Some(o) = st.nodes.get(&owner) {
                let msg = Message::Update {
                    h: self.header(st),
                    node: owner.clone(),
                    config_epoch: o.config_epoch,
                    slots: slots_of(st, &owner),
                };
                self.send_to(st, fx, sender, msg);
            }
        }
        // Lost every slot to the sender: a master whose replica was promoted
        // (or whose slots all moved) follows the new owner, as in Redis.
        let mine_after = st
            .owners
            .iter()
            .filter(|o| o.as_deref() == Some(me.as_str()))
            .count();
        let sender_is_master = st.nodes.get(sender).is_some_and(|n| n.master.is_none());
        if mine_before > 0 && mine_after == 0 && sender_is_master && my_master.is_none() {
            self.become_replica_of(st, fx, sender);
        }
        // A replica whose master lost its last slot to the sender follows it.
        if let Some(m) = &my_master {
            let master_after = st
                .owners
                .iter()
                .filter(|o| o.as_deref() == Some(m.as_str()))
                .count();
            if master_before > 0 && master_after == 0 && sender_is_master && sender != m {
                self.become_replica_of(st, fx, sender);
            }
        }
    }

    fn become_replica_of(&self, st: &mut State, fx: &mut Effects, master: &str) {
        let me = st.myself.clone();
        let Some((ip, port)) = st.nodes.get(master).map(|n| (n.ip.clone(), n.port)) else {
            return;
        };
        for o in st.owners.iter_mut() {
            if o.as_deref() == Some(me.as_str()) {
                *o = None;
            }
        }
        st.migrating.clear();
        st.importing.clear();
        if let Some(n) = st.nodes.get_mut(&me) {
            n.master = Some(master.to_string());
        }
        st.election = Election::default();
        if st.mf.take().is_some() {
            fx.pause = Some(0);
        }
        st.dirty = true;
        fx.replicate = Some((ip, port));
        tracing::info!(%master, "now a replica");
        self.broadcast_pong(st, fx);
    }

    /// Reports in `from`'s gossip about the other nodes.
    fn process_gossip(
        &self,
        st: &mut State,
        fx: &mut Effects,
        from: &str,
        gossip: &[Seen],
        now: u64,
    ) {
        let from_master_serving = st.owners.iter().any(|o| o.as_deref() == Some(from))
            && st.nodes.get(from).is_some_and(|n| n.master.is_none());
        for g in gossip {
            if g.id == st.myself {
                continue;
            }
            match st.nodes.get_mut(&g.id) {
                Some(n) => {
                    if from_master_serving {
                        if g.pfail || g.fail {
                            n.fail_reports.insert(from.to_string(), now);
                        } else {
                            n.fail_reports.remove(from);
                        }
                    }
                }
                None => {
                    if st.blacklist.get(&g.id).is_some_and(|&t| t > now) || g.fail {
                        continue;
                    }
                    // Learned from a member: start talking to it.
                    let n = Node::new(g.id.clone(), g.ip.clone(), g.port, g.cport);
                    st.nodes.insert(g.id.clone(), n);
                    st.dirty = true;
                }
            }
            self.check_fail(st, fx, &g.id, now);
        }
    }

    /// Mark `id` FAIL when a majority of serving masters agree it is down.
    fn check_fail(&self, st: &mut State, fx: &mut Effects, id: &str, now: u64) {
        let Some(n) = st.nodes.get(id) else { return };
        if !n.pfail || n.fail {
            return;
        }
        let validity = 2 * self.cfg.node_timeout_ms;
        let masters = serving_masters(st);
        let quorum = masters.len() / 2 + 1;
        let mut votes = n
            .fail_reports
            .iter()
            .filter(|(r, t)| now.saturating_sub(**t) <= validity && masters.contains(*r))
            .count();
        if masters.contains(&st.myself) {
            votes += 1;
        }
        if votes < quorum {
            return;
        }
        if let Some(n) = st.nodes.get_mut(id) {
            n.fail = true;
            n.pfail = false;
            n.fail_time = now;
        }
        st.dirty = true;
        tracing::warn!(node = %id, votes, quorum, "node marked FAIL");
        let node = id.to_string();
        self.broadcast(st, fx, |st| Message::Fail {
            h: self.header(st),
            node: node.clone(),
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn vote(
        &self,
        st: &mut State,
        fx: &mut Effects,
        h: &Header,
        force: bool,
        master_epoch: u64,
        master_slots: &[(u16, u16)],
        now: u64,
    ) {
        let me = st.myself.clone();
        let i_serve = st.owners.iter().any(|o| o.as_deref() == Some(me.as_str()));
        if st.nodes.get(&me).is_none_or(|n| n.master.is_some()) || !i_serve {
            return;
        }
        if h.current_epoch < st.current_epoch || st.last_vote_epoch == st.current_epoch {
            return;
        }
        let Some(master) = h.master.clone() else {
            return;
        };
        let Some(m) = st.nodes.get(&master) else {
            return;
        };
        if !force && !m.fail {
            return;
        }
        if now.saturating_sub(m.voted_time) < 2 * self.cfg.node_timeout_ms {
            return;
        }
        for &(s, e) in master_slots {
            for slot in s..=e {
                if let Some(o) = &st.owners[slot as usize] {
                    if st
                        .nodes
                        .get(o)
                        .is_some_and(|n| n.config_epoch > master_epoch)
                    {
                        return;
                    }
                }
            }
        }
        st.last_vote_epoch = st.current_epoch;
        if let Some(m) = st.nodes.get_mut(&master) {
            m.voted_time = now;
        }
        st.dirty = true;
        tracing::info!(replica = %h.id, epoch = st.current_epoch, "voted for a failover");
        let ack = Message::AuthAck { h: self.header(st) };
        self.send_to(st, fx, &h.id, ack);
    }

    /// This replica becomes the master of its master's slots, with `epoch`.
    fn take_over(&self, st: &mut State, fx: &mut Effects, epoch: u64, why: &str) {
        let me = st.myself.clone();
        let Some(old) = st.nodes.get(&me).and_then(|n| n.master.clone()) else {
            return;
        };
        for o in st.owners.iter_mut() {
            if o.as_deref() == Some(old.as_str()) {
                *o = Some(me.clone());
            }
        }
        if let Some(n) = st.nodes.get_mut(&me) {
            n.master = None;
            n.config_epoch = n.config_epoch.max(epoch);
        }
        if epoch > st.current_epoch {
            st.current_epoch = epoch;
        }
        st.election = Election::default();
        st.dirty = true;
        fx.promote = true;
        tracing::warn!(old_master = %old, epoch, "failover: {}", why);
        self.broadcast_pong(st, fx);
    }

    // -----------------------------------------------------------------------
    // Cron
    // -----------------------------------------------------------------------

    /// Ten times a second: pings, failure detection, elections, timeouts.
    pub fn cron(&self) {
        let mut st = self.state.lock();
        let mut fx = Effects::default();
        self.cron_locked(&mut st, &mut fx, now_ms());
        self.finish(st, fx);
    }

    fn cron_locked(&self, st: &mut State, fx: &mut Effects, now: u64) {
        let timeout = self.cfg.node_timeout_ms;
        st.blacklist.retain(|_, t| *t > now);
        let ping_every = (timeout / 2).clamp(100, 1000);

        let ids: Vec<String> = st
            .nodes
            .keys()
            .filter(|k| **k != st.myself)
            .cloned()
            .collect();
        for id in ids {
            let Some(n) = st.nodes.get(&id) else { continue };
            if n.handshake && now.saturating_sub(n.created) > timeout.max(1000) {
                tracing::info!(addr = %format!("{}:{}", n.ip, n.port), "handshake timed out");
                st.nodes.remove(&id);
                if let Some(bus) = self.bus.get() {
                    bus.forget(&id);
                }
                continue;
            }
            if now.saturating_sub(n.last_ping) >= ping_every {
                let kind = if n.handshake { "meet" } else { "ping" };
                let msg = self.ping_msg(st, &id, kind, id.clone());
                self.send_to(st, fx, &id, msg);
                if let Some(n) = st.nodes.get_mut(&id) {
                    n.last_ping = now;
                    if n.ping_sent == 0 {
                        n.ping_sent = now;
                    }
                }
            }
            if let Some(n) = st.nodes.get_mut(&id) {
                if !n.handshake
                    && n.ping_sent != 0
                    && now.saturating_sub(n.ping_sent) > timeout
                    && !n.pfail
                    && !n.fail
                {
                    n.pfail = true;
                    tracing::info!(node = %id, "node not answering: PFAIL");
                }
                let validity = 2 * timeout;
                n.fail_reports
                    .retain(|_, t| now.saturating_sub(*t) <= validity);
            }
            self.check_fail(st, fx, &id, now);
        }

        // Master side of a manual failover: give up after the deadline.
        if let Some((_, until)) = st.mf {
            if now > until {
                st.mf = None;
                fx.pause = Some(0);
                tracing::info!("manual failover timed out: clients released");
            }
        }

        self.election_cron(st, fx, now);
    }

    fn election_cron(&self, st: &mut State, fx: &mut Effects, now: u64) {
        let me = st.myself.clone();
        let Some(master) = st.nodes.get(&me).and_then(|n| n.master.clone()) else {
            st.election = Election::default();
            return;
        };
        let Some(m) = st.nodes.get(&master) else {
            return;
        };
        let master_slots = slots_of(st, &master);
        if master_slots.is_empty() {
            return;
        }
        let auth_timeout = (2 * self.cfg.node_timeout_ms).max(2000);

        // Manual failover progress.
        let mut forced = false;
        if let Some(manual) = st.election.manual.clone() {
            if now > manual.deadline {
                tracing::warn!("manual failover timed out");
                st.election = Election::default();
                return;
            }
            let mut ready = manual.ready;
            if !ready {
                if let Some(target) = manual.master_offset {
                    let mine = self.host.get().map_or(0, |h| h.repl_offset());
                    if mine >= target {
                        ready = true;
                        tracing::info!(offset = mine, "caught up with the paused master");
                    }
                }
            }
            if !ready {
                return;
            }
            if let Some(m) = &mut st.election.manual {
                m.ready = true;
            }
            forced = true;
            if st.election.auth_time == 0 {
                st.election.auth_time = now;
            }
        } else if !m.fail {
            st.election.auth_time = 0;
            st.election.sent = false;
            return;
        }

        if st.election.auth_time == 0
            || (st.election.sent && now > st.election.auth_time + 2 * auth_timeout)
        {
            // Schedule (or reschedule) the election: replicas further behind
            // wait longer, so the best one usually wins.
            let my_offset = self.host.get().map_or(0, |h| h.repl_offset());
            let rank = st
                .nodes
                .values()
                .filter(|n| {
                    n.id != me
                        && n.master.as_deref() == Some(master.as_str())
                        && n.offset > my_offset
                })
                .count() as u64;
            let jitter = rand::random::<u64>() % 500;
            st.election.auth_time = now
                + if forced {
                    0
                } else {
                    500 + jitter + rank * 1000
                };
            st.election.sent = false;
            st.election.votes.clear();
            return;
        }
        if now < st.election.auth_time || st.election.sent {
            return;
        }
        if now > st.election.auth_time + auth_timeout {
            return;
        }
        st.current_epoch += 1;
        st.election.epoch = st.current_epoch;
        st.election.sent = true;
        st.election.votes.clear();
        st.dirty = true;
        let master_epoch = st.nodes.get(&master).map_or(0, |n| n.config_epoch);
        tracing::info!(
            epoch = st.current_epoch,
            "asking the masters for a failover vote"
        );
        self.broadcast(st, fx, |st| Message::AuthRequest {
            h: self.header(st),
            force: forced,
            master_epoch,
            master_slots: master_slots.clone(),
        });
    }

    // -----------------------------------------------------------------------
    // Publishing
    // -----------------------------------------------------------------------

    /// Relay a `PUBLISH` to every other node (Redis Cluster broadcasts it),
    /// or an `SPUBLISH` to this node's replicas.
    pub fn publish(&self, channel: &[u8], message: &[u8], shard: bool) {
        let st = self.state.lock();
        let mut fx = Effects::default();
        let me = st.myself.clone();
        let targets: Vec<String> = st
            .nodes
            .values()
            .filter(|n| {
                n.id != me && !n.handshake && (!shard || n.master.as_deref() == Some(me.as_str()))
            })
            .map(|n| n.id.clone())
            .collect();
        if targets.is_empty() {
            return;
        }
        let ch = hex::encode(channel);
        let msg = hex::encode(message);
        for id in targets {
            let m = Message::Publish {
                h: self.header(&st),
                channel: ch.clone(),
                message: msg.clone(),
                shard,
            };
            self.send_to(&st, &mut fx, &id, m);
        }
        drop(st);
        self.run(fx);
    }

    // -----------------------------------------------------------------------
    // CLUSTER commands
    // -----------------------------------------------------------------------

    /// `CLUSTER <subcommand> ...`.
    pub fn command(&self, args: &[Bytes]) -> CommandResponse {
        if args.is_empty() {
            return CommandResponse::wrong_arity("cluster");
        }
        let sub = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        let rest = &args[1..];
        let arg = |i: usize| -> String { String::from_utf8_lossy(&rest[i]).to_string() };
        match sub.as_str() {
            "INFO" => CommandResponse::bulk(Bytes::from(self.info())),
            "NODES" => CommandResponse::bulk(Bytes::from(self.nodes_text())),
            "SLOTS" => self.slots_reply(),
            "SHARDS" => self.shards_reply(),
            "MYID" => CommandResponse::bulk(Bytes::from(self.node_id())),
            "MYSHARDID" => {
                let st = self.state.lock();
                CommandResponse::bulk(Bytes::from(shard_id_of(&st, &st.myself)))
            }
            "KEYSLOT" => {
                if rest.len() != 1 {
                    return CommandResponse::wrong_arity("cluster|keyslot");
                }
                CommandResponse::integer(self.key_slot(&rest[0]) as i64)
            }
            "MEET" => {
                if rest.len() < 2 || rest.len() > 3 {
                    return CommandResponse::wrong_arity("cluster|meet");
                }
                let Ok(port) = arg(1).parse::<u16>() else {
                    return CommandResponse::error(format!("ERR Invalid base port specified: {}", arg(1)));
                };
                let cport = match rest.get(2) {
                    Some(c) => match String::from_utf8_lossy(c).parse::<u16>() {
                        Ok(c) => c,
                        Err(_) => return CommandResponse::error("ERR Invalid bus port specified"),
                    },
                    None => match port.checked_add(crate::gossip::CLUSTER_BUS_PORT_OFFSET) {
                        Some(c) => c,
                        None => return CommandResponse::error("ERR Invalid base port specified"),
                    },
                };
                match self.meet(arg(0), port, cport) {
                    Ok(()) => CommandResponse::ok(),
                    Err(e) => CommandResponse::error(e),
                }
            }
            "ADDSLOTS" | "DELSLOTS" | "ADDSLOTSRANGE" | "DELSLOTSRANGE" => {
                let ranged = sub.ends_with("RANGE");
                if rest.is_empty() || (ranged && !rest.len().is_multiple_of(2)) {
                    return CommandResponse::wrong_arity(&format!("cluster|{}", sub.to_lowercase()));
                }
                let mut slots = Vec::new();
                if ranged {
                    for pair in rest.chunks(2) {
                        let (Some(a), Some(b)) = (parse_slot(&pair[0]), parse_slot(&pair[1])) else {
                            return CommandResponse::error("ERR Invalid or out of range slot");
                        };
                        if a > b {
                            return CommandResponse::error(format!(
                                "ERR start slot number {} is greater than end slot number {}",
                                a, b
                            ));
                        }
                        slots.extend(a..=b);
                    }
                } else {
                    for a in rest {
                        // `N-M` is accepted too (older SoliKV tooling).
                        let text = String::from_utf8_lossy(a);
                        if let Some((x, y)) = text.split_once('-') {
                            match (x.parse::<u16>(), y.parse::<u16>()) {
                                (Ok(x), Ok(y)) if x <= y && (y as usize) < SLOTS => slots.extend(x..=y),
                                _ => return CommandResponse::error("ERR Invalid or out of range slot"),
                            }
                            continue;
                        }
                        let Some(s) = parse_slot(a) else {
                            return CommandResponse::error("ERR Invalid or out of range slot");
                        };
                        slots.push(s);
                    }
                }
                let r = if sub.starts_with("ADD") {
                    self.add_slots(&slots)
                } else {
                    self.del_slots(&slots)
                };
                match r {
                    Ok(()) => CommandResponse::ok(),
                    Err(e) => CommandResponse::error(e),
                }
            }
            "FLUSHSLOTS" => {
                if self.host.get().is_some_and(|h| h.dbsize() > 0) {
                    return CommandResponse::error("ERR DB must be empty to perform CLUSTER FLUSHSLOTS.");
                }
                let mut st = self.state.lock();
                let me = st.myself.clone();
                for o in st.owners.iter_mut() {
                    if o.as_deref() == Some(me.as_str()) {
                        *o = None;
                    }
                }
                st.dirty = true;
                self.finish(st, Effects::default());
                CommandResponse::ok()
            }
            "SETSLOT" => self.setslot(rest),
            "COUNTKEYSINSLOT" => {
                if rest.len() != 1 {
                    return CommandResponse::wrong_arity("cluster|countkeysinslot");
                }
                let Some(slot) = parse_slot(&rest[0]) else {
                    return CommandResponse::error("ERR Invalid slot");
                };
                CommandResponse::integer(self.host.get().map_or(0, |h| h.count_keys_in_slot(slot)) as i64)
            }
            "GETKEYSINSLOT" => {
                if rest.len() != 2 {
                    return CommandResponse::wrong_arity("cluster|getkeysinslot");
                }
                let Some(slot) = parse_slot(&rest[0]) else {
                    return CommandResponse::error("ERR Invalid slot");
                };
                let Ok(count) = arg(1).parse::<i64>() else {
                    return CommandResponse::error("ERR Invalid number of keys");
                };
                if count < 0 {
                    return CommandResponse::error("ERR Invalid number of keys");
                }
                let keys = self
                    .host
                    .get()
                    .map(|h| h.keys_in_slot(slot, count as usize))
                    .unwrap_or_default();
                CommandResponse::array(keys.into_iter().map(CommandResponse::bulk).collect())
            }
            "COUNT-FAILURE-REPORTS" => {
                if rest.len() != 1 {
                    return CommandResponse::wrong_arity("cluster|count-failure-reports");
                }
                let st = self.state.lock();
                match st.nodes.get(&arg(0)) {
                    Some(n) => CommandResponse::integer(n.fail_reports.len() as i64),
                    None => CommandResponse::error(format!("ERR Unknown node {}", arg(0))),
                }
            }
            "FORGET" => {
                if rest.len() != 1 {
                    return CommandResponse::wrong_arity("cluster|forget");
                }
                self.forget(&arg(0))
            }
            "REPLICATE" => {
                if rest.len() != 1 {
                    return CommandResponse::wrong_arity("cluster|replicate");
                }
                self.replicate(&arg(0))
            }
            "SLAVES" | "REPLICAS" => {
                if rest.len() != 1 {
                    return CommandResponse::wrong_arity("cluster|replicas");
                }
                let st = self.state.lock();
                let id = arg(0);
                if !st.nodes.contains_key(&id) {
                    return CommandResponse::error(format!("ERR Unknown node {}", id));
                }
                let lines = st
                    .nodes
                    .values()
                    .filter(|n| n.master.as_deref() == Some(id.as_str()))
                    .map(|n| CommandResponse::bulk(Bytes::from(node_line(&st, n).trim_end().to_string())))
                    .collect();
                CommandResponse::array(lines)
            }
            "FAILOVER" => {
                let mode = rest.first().map(|a| String::from_utf8_lossy(a).to_ascii_uppercase());
                if rest.len() > 1 {
                    return CommandResponse::syntax_error();
                }
                self.failover(mode.as_deref())
            }
            "RESET" => {
                let hard = match rest.first().map(|a| String::from_utf8_lossy(a).to_ascii_uppercase()) {
                    None => false,
                    Some(m) if m == "SOFT" => false,
                    Some(m) if m == "HARD" => true,
                    Some(_) => return CommandResponse::syntax_error(),
                };
                self.reset(hard)
            }
            "SAVECONFIG" => {
                let st = self.state.lock();
                self.save(&st);
                CommandResponse::ok()
            }
            "BUMPEPOCH" => {
                let mut st = self.state.lock();
                let me = st.myself.clone();
                let max = st.nodes.values().map(|n| n.config_epoch).max().unwrap_or(0);
                let mine = st.nodes.get(&me).map_or(0, |n| n.config_epoch);
                let reply = if mine == 0 || mine != max || mine < st.current_epoch {
                    st.current_epoch += 1;
                    let e = st.current_epoch;
                    if let Some(n) = st.nodes.get_mut(&me) {
                        n.config_epoch = e;
                    }
                    st.dirty = true;
                    format!("BUMPED {}", e)
                } else {
                    format!("STILL {}", mine)
                };
                self.finish(st, Effects::default());
                CommandResponse::SimpleString(Bytes::from(reply))
            }
            "SET-CONFIG-EPOCH" => {
                if rest.len() != 1 {
                    return CommandResponse::wrong_arity("cluster|set-config-epoch");
                }
                let Ok(e) = arg(0).parse::<u64>() else {
                    return CommandResponse::error("ERR Invalid config epoch specified");
                };
                let mut st = self.state.lock();
                let me = st.myself.clone();
                if st.nodes.len() > 1 {
                    return CommandResponse::error(
                        "ERR The user can assign a config epoch only when the node does not know any other node.",
                    );
                }
                if st.nodes.get(&me).is_some_and(|n| n.config_epoch != 0) {
                    return CommandResponse::error("ERR Node config epoch is already non-zero");
                }
                if let Some(n) = st.nodes.get_mut(&me) {
                    n.config_epoch = e;
                }
                st.current_epoch = st.current_epoch.max(e);
                st.dirty = true;
                self.finish(st, Effects::default());
                CommandResponse::ok()
            }
            "LINKS" => {
                let st = self.state.lock();
                let links = self.bus.get().map(|b| b.links()).unwrap_or_default();
                let rows = links
                    .into_iter()
                    .filter(|l| st.nodes.contains_key(&l.node))
                    .map(|l| {
                        CommandResponse::Map(vec![
                            (CommandResponse::bulk_string("direction"), CommandResponse::bulk_string("to")),
                            (CommandResponse::bulk_string("node"), CommandResponse::bulk(Bytes::from(l.node))),
                            (CommandResponse::bulk_string("create-time"), CommandResponse::integer(l.created_ms as i64)),
                            (CommandResponse::bulk_string("events"), CommandResponse::bulk_string("rw")),
                            (CommandResponse::bulk_string("send-buffer-allocated"), CommandResponse::integer(0)),
                            (CommandResponse::bulk_string("send-buffer-used"), CommandResponse::integer(0)),
                        ])
                    })
                    .collect();
                CommandResponse::array(rows)
            }
            "HELP" => CommandResponse::array(
                [
                    "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "ADDSLOTS <slot> [<slot> ...]",
                    "ADDSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
                    "BUMPEPOCH",
                    "COUNT-FAILURE-REPORTS <node-id>",
                    "COUNTKEYSINSLOT <slot>",
                    "DELSLOTS <slot> [<slot> ...]",
                    "DELSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
                    "FAILOVER [FORCE|TAKEOVER]",
                    "FORGET <node-id>",
                    "FLUSHSLOTS",
                    "GETKEYSINSLOT <slot> <count>",
                    "INFO",
                    "KEYSLOT <key>",
                    "LINKS",
                    "MEET <ip> <port> [<bus-port>]",
                    "MYID",
                    "MYSHARDID",
                    "NODES",
                    "REPLICATE <node-id>",
                    "RESET [HARD|SOFT]",
                    "SET-CONFIG-EPOCH <epoch>",
                    "SETSLOT <slot> (IMPORTING <node-id>|MIGRATING <node-id>|STABLE|NODE <node-id>)",
                    "REPLICAS <node-id>",
                    "SAVECONFIG",
                    "SHARDS",
                    "SLOTS",
                ]
                .iter()
                .map(|s| CommandResponse::SimpleString(Bytes::from(s.to_string())))
                .collect(),
            ),
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try CLUSTER HELP.",
                String::from_utf8_lossy(&args[0])
            )),
        }
    }

    pub fn meet(&self, ip: String, port: u16, cport: u16) -> Result<(), String> {
        let mut st = self.state.lock();
        let already = st
            .nodes
            .values()
            .any(|n| n.ip == ip && n.port == port && n.id != st.myself);
        if already {
            return Ok(());
        }
        let me = st.nodes.get(&st.myself).cloned();
        if me.is_some_and(|m| m.ip == ip && m.port == port) {
            return Ok(());
        }
        let temp = generate_node_id();
        let mut n = Node::new(temp.clone(), ip, port, cport);
        n.handshake = true;
        st.nodes.insert(temp, n);
        self.finish(st, Effects::default());
        Ok(())
    }

    pub fn add_slots(&self, slots: &[u16]) -> Result<(), String> {
        let mut st = self.state.lock();
        let me = st.myself.clone();
        if st.nodes.get(&me).is_some_and(|n| n.master.is_some()) {
            return Err("ERR Please use SETSLOT only with masters.".into());
        }
        let mut seen = HashSet::new();
        for &s in slots {
            if !seen.insert(s) {
                return Err(format!("ERR Slot {} specified multiple times", s));
            }
            if st.owners[s as usize].is_some() {
                return Err(format!("ERR Slot {} is already busy", s));
            }
        }
        for &s in slots {
            st.owners[s as usize] = Some(me.clone());
            st.importing.remove(&s);
        }
        st.dirty = true;
        self.finish(st, Effects::default());
        Ok(())
    }

    pub fn del_slots(&self, slots: &[u16]) -> Result<(), String> {
        let mut st = self.state.lock();
        let mut seen = HashSet::new();
        for &s in slots {
            if !seen.insert(s) {
                return Err(format!("ERR Slot {} specified multiple times", s));
            }
            if st.owners[s as usize].is_none() {
                return Err(format!("ERR Slot {} is already unassigned", s));
            }
        }
        for &s in slots {
            st.owners[s as usize] = None;
            st.migrating.remove(&s);
            st.importing.remove(&s);
        }
        st.dirty = true;
        self.finish(st, Effects::default());
        Ok(())
    }

    fn setslot(&self, rest: &[Bytes]) -> CommandResponse {
        if rest.len() < 2 {
            return CommandResponse::wrong_arity("cluster|setslot");
        }
        let Some(slot) = parse_slot(&rest[0]) else {
            return CommandResponse::error("ERR Invalid or out of range slot");
        };
        let action = String::from_utf8_lossy(&rest[1]).to_ascii_uppercase();
        let node = rest.get(2).map(|n| String::from_utf8_lossy(n).to_string());
        let mut st = self.state.lock();
        let me = st.myself.clone();
        if st.nodes.get(&me).is_some_and(|n| n.master.is_some()) {
            return CommandResponse::error("ERR Please use SETSLOT only with masters.");
        }
        let mut fx = Effects::default();
        match action.as_str() {
            "MIGRATING" => {
                let Some(node) = node else {
                    return CommandResponse::syntax_error();
                };
                if st.owners[slot as usize].as_deref() != Some(me.as_str()) {
                    return CommandResponse::error(format!(
                        "ERR I'm not the owner of hash slot {}",
                        slot
                    ));
                }
                match st.nodes.get(&node) {
                    None => {
                        return CommandResponse::error(format!(
                            "ERR I don't know about node {}",
                            node
                        ))
                    }
                    Some(n) if n.master.is_some() => {
                        return CommandResponse::error("ERR Target node is not a master")
                    }
                    _ => {}
                }
                st.migrating.insert(slot, node);
            }
            "IMPORTING" => {
                let Some(node) = node else {
                    return CommandResponse::syntax_error();
                };
                if st.owners[slot as usize].as_deref() == Some(me.as_str()) {
                    return CommandResponse::error(format!(
                        "ERR I'm already the owner of hash slot {}",
                        slot
                    ));
                }
                match st.nodes.get(&node) {
                    None => {
                        return CommandResponse::error(format!(
                            "ERR I don't know about node {}",
                            node
                        ))
                    }
                    Some(n) if n.master.is_some() => {
                        return CommandResponse::error("ERR Target node is not a master")
                    }
                    _ => {}
                }
                st.importing.insert(slot, node);
            }
            "STABLE" => {
                st.migrating.remove(&slot);
                st.importing.remove(&slot);
            }
            "NODE" => {
                let Some(node) = node else {
                    return CommandResponse::syntax_error();
                };
                match st.nodes.get(&node) {
                    None => return CommandResponse::error(format!("ERR Unknown node {}", node)),
                    Some(n) if n.master.is_some() => {
                        return CommandResponse::error("ERR Target node is not a master")
                    }
                    _ => {}
                }
                // Giving away a slot that still has keys here would lose them.
                if st.owners[slot as usize].as_deref() == Some(me.as_str())
                    && node != me
                    && self
                        .host
                        .get()
                        .is_some_and(|h| h.count_keys_in_slot(slot) > 0)
                {
                    return CommandResponse::error(format!(
                        "ERR Can't assign hashslot {} to a different node while I still hold keys for this hash slot.",
                        slot
                    ));
                }
                if node != me {
                    st.migrating.remove(&slot);
                }
                let was_importing = st.importing.remove(&slot).is_some();
                st.owners[slot as usize] = Some(node.clone());
                // The importing node ends the migration with a new epoch, so
                // its claim wins everywhere without an election (Redis'
                // "bump without consensus").
                if node == me && was_importing {
                    let max = st.nodes.values().map(|n| n.config_epoch).max().unwrap_or(0);
                    let mine = st.nodes.get(&me).map_or(0, |n| n.config_epoch);
                    if mine == 0 || mine != max {
                        st.current_epoch += 1;
                        let e = st.current_epoch;
                        if let Some(n) = st.nodes.get_mut(&me) {
                            n.config_epoch = e;
                        }
                    }
                    self.broadcast_pong(&st, &mut fx);
                }
            }
            _ => {
                return CommandResponse::error(
                    "ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP",
                )
            }
        }
        st.dirty = true;
        self.finish(st, fx);
        CommandResponse::ok()
    }

    fn forget(&self, id: &str) -> CommandResponse {
        let mut st = self.state.lock();
        let me = st.myself.clone();
        if id == me {
            return CommandResponse::error("ERR I tried hard but I can't forget myself...");
        }
        if !st.nodes.contains_key(id) {
            return CommandResponse::error(format!("ERR Unknown node {}", id));
        }
        if st.nodes.get(&me).and_then(|n| n.master.as_deref()) == Some(id) {
            return CommandResponse::error("ERR Can't forget my master!");
        }
        st.nodes.remove(id);
        for o in st.owners.iter_mut() {
            if o.as_deref() == Some(id) {
                *o = None;
            }
        }
        st.blacklist.insert(id.to_string(), now_ms() + 60_000);
        st.dirty = true;
        if let Some(bus) = self.bus.get() {
            bus.forget(id);
        }
        self.finish(st, Effects::default());
        CommandResponse::ok()
    }

    fn replicate(&self, id: &str) -> CommandResponse {
        let mut st = self.state.lock();
        let me = st.myself.clone();
        let Some(target) = st.nodes.get(id).cloned() else {
            return CommandResponse::error(format!("ERR Unknown node {}", id));
        };
        if id == me {
            return CommandResponse::error("ERR Can't replicate myself");
        }
        if target.master.is_some() {
            return CommandResponse::error("ERR I can only replicate a master, not a replica.");
        }
        let i_am_master = st.nodes.get(&me).is_some_and(|n| n.master.is_none());
        if i_am_master {
            let serves = st.owners.iter().any(|o| o.as_deref() == Some(me.as_str()));
            if serves || self.host.get().is_some_and(|h| h.dbsize() > 0) {
                return CommandResponse::error(
                    "ERR To set a master the node must be empty and without assigned slots.",
                );
            }
        }
        let mut fx = Effects::default();
        self.become_replica_of(&mut st, &mut fx, id);
        self.finish(st, fx);
        CommandResponse::ok()
    }

    fn failover(&self, mode: Option<&str>) -> CommandResponse {
        let mut st = self.state.lock();
        let me = st.myself.clone();
        let Some(master) = st.nodes.get(&me).and_then(|n| n.master.clone()) else {
            return CommandResponse::error("ERR You should send CLUSTER FAILOVER to a replica");
        };
        let now = now_ms();
        let mut fx = Effects::default();
        match mode {
            Some("TAKEOVER") => {
                st.current_epoch += 1;
                let e = st.current_epoch;
                self.take_over(&mut st, &mut fx, e, "takeover (no election)");
            }
            Some("FORCE") => {
                st.election = Election {
                    manual: Some(Manual {
                        deadline: now + MANUAL_FAILOVER_MS,
                        master_offset: None,
                        ready: true,
                    }),
                    ..Default::default()
                };
            }
            None => {
                if st.nodes.get(&master).is_none_or(|m| m.fail) {
                    return CommandResponse::error(
                        "ERR Master is down or failed, please use CLUSTER FAILOVER FORCE",
                    );
                }
                st.election = Election {
                    manual: Some(Manual {
                        deadline: now + MANUAL_FAILOVER_MS,
                        master_offset: None,
                        ready: false,
                    }),
                    ..Default::default()
                };
                let m = Message::MfStart {
                    h: self.header(&st),
                };
                self.send_to(&st, &mut fx, &master, m);
            }
            Some(_) => return CommandResponse::syntax_error(),
        }
        self.finish(st, fx);
        CommandResponse::ok()
    }

    fn reset(&self, hard: bool) -> CommandResponse {
        let mut st = self.state.lock();
        let me = st.myself.clone();
        let i_am_master = st.nodes.get(&me).is_some_and(|n| n.master.is_none());
        if i_am_master && self.host.get().is_some_and(|h| h.dbsize() > 0) {
            return CommandResponse::error(
                "ERR CLUSTER RESET can't be called with master nodes containing keys",
            );
        }
        let was_replica = !i_am_master;
        let mut me_node = st.nodes.remove(&me).expect("myself");
        if let Some(bus) = self.bus.get() {
            for id in st.nodes.keys() {
                bus.forget(id);
            }
        }
        for id in st.nodes.keys().cloned().collect::<Vec<_>>() {
            st.blacklist.insert(id, now_ms() + 60_000);
        }
        st.nodes.clear();
        st.owners = vec![None; SLOTS];
        st.migrating.clear();
        st.importing.clear();
        st.election = Election::default();
        me_node.master = None;
        if hard {
            me_node.id = generate_node_id();
            me_node.config_epoch = 0;
            st.current_epoch = 0;
            st.last_vote_epoch = 0;
            st.myself = me_node.id.clone();
        }
        st.nodes.insert(me_node.id.clone(), me_node);
        st.dirty = true;
        let mut fx = Effects::default();
        if was_replica {
            fx.promote = true;
        }
        self.finish(st, fx);
        CommandResponse::ok()
    }

    // -----------------------------------------------------------------------
    // Introspection
    // -----------------------------------------------------------------------

    pub fn info(&self) -> String {
        let st = self.state.lock();
        let assigned = st.owners.iter().filter(|o| o.is_some()).count();
        let pfail = st
            .owners
            .iter()
            .flatten()
            .filter(|o| st.nodes.get(*o).is_some_and(|n| n.pfail))
            .count();
        let fail = st
            .owners
            .iter()
            .flatten()
            .filter(|o| st.nodes.get(*o).is_some_and(|n| n.fail))
            .count();
        let my_epoch = st.nodes.get(&st.myself).map_or(0, |n| match &n.master {
            None => n.config_epoch,
            Some(m) => st.nodes.get(m).map_or(0, |m| m.config_epoch),
        });
        let (sent, recv) = self.bus.get().map_or((0, 0), |b| {
            (
                b.sent.load(Ordering::Relaxed),
                b.received.load(Ordering::Relaxed),
            )
        });
        format!(
            "cluster_state:{}\r\ncluster_slots_assigned:{}\r\ncluster_slots_ok:{}\r\n\
             cluster_slots_pfail:{}\r\ncluster_slots_fail:{}\r\ncluster_known_nodes:{}\r\n\
             cluster_size:{}\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{}\r\n\
             cluster_stats_messages_sent:{}\r\ncluster_stats_messages_received:{}\r\n\
             total_cluster_links_buffer_limit_exceeded:0\r\n",
            if st.ok { "ok" } else { "fail" },
            assigned,
            assigned - pfail - fail,
            pfail,
            fail,
            st.nodes.values().filter(|n| !n.handshake).count(),
            serving_masters(&st).len(),
            st.current_epoch,
            my_epoch,
            sent,
            recv,
        )
    }

    pub fn nodes_text(&self) -> String {
        let st = self.state.lock();
        let mut ids: Vec<&String> = st.nodes.keys().collect();
        ids.sort();
        let mut out = String::new();
        for id in ids {
            out.push_str(&node_line(&st, &st.nodes[id]));
        }
        out
    }

    /// `CLUSTER SLOTS`: `[start, end, [ip, port, id, {}], replicas...]` for
    /// every range of every node, as Redis 7 answers.
    pub fn slots_reply(&self) -> CommandResponse {
        let st = self.state.lock();
        let mut out = Vec::new();
        for (start, end, owner) in owner_ranges(&st) {
            let Some(m) = st.nodes.get(&owner) else {
                continue;
            };
            let mut row = vec![
                CommandResponse::integer(start as i64),
                CommandResponse::integer(end as i64),
                endpoint(m),
            ];
            let mut replicas: Vec<&Node> = st
                .nodes
                .values()
                .filter(|n| n.master.as_deref() == Some(owner.as_str()) && !n.fail)
                .collect();
            replicas.sort_by(|a, b| a.id.cmp(&b.id));
            row.extend(replicas.into_iter().map(endpoint));
            out.push(CommandResponse::array(row));
        }
        CommandResponse::array(out)
    }

    /// `CLUSTER SHARDS` (Redis 7).
    pub fn shards_reply(&self) -> CommandResponse {
        let st = self.state.lock();
        let mut masters: Vec<&Node> = st
            .nodes
            .values()
            .filter(|n| n.master.is_none() && !n.handshake)
            .collect();
        masters.sort_by(|a, b| a.id.cmp(&b.id));
        let mut out = Vec::new();
        for m in masters {
            let ranges = slots_of(&st, &m.id);
            let mut slots = Vec::new();
            for (s, e) in &ranges {
                slots.push(CommandResponse::integer(*s as i64));
                slots.push(CommandResponse::integer(*e as i64));
            }
            let mut members = vec![m];
            let mut replicas: Vec<&Node> = st
                .nodes
                .values()
                .filter(|n| n.master.as_deref() == Some(m.id.as_str()))
                .collect();
            replicas.sort_by(|a, b| a.id.cmp(&b.id));
            members.extend(replicas);
            let nodes = members
                .into_iter()
                .map(|n| {
                    let health = if n.fail || n.pfail {
                        "fail"
                    } else if n.master.is_some() && n.offset == 0 {
                        "loading"
                    } else {
                        "online"
                    };
                    CommandResponse::Map(vec![
                        (
                            CommandResponse::bulk_string("id"),
                            CommandResponse::bulk(Bytes::from(n.id.clone())),
                        ),
                        (
                            CommandResponse::bulk_string("port"),
                            CommandResponse::integer(n.port as i64),
                        ),
                        (
                            CommandResponse::bulk_string("ip"),
                            CommandResponse::bulk(Bytes::from(n.ip.clone())),
                        ),
                        (
                            CommandResponse::bulk_string("endpoint"),
                            CommandResponse::bulk(Bytes::from(n.ip.clone())),
                        ),
                        (
                            CommandResponse::bulk_string("hostname"),
                            CommandResponse::bulk(Bytes::from(n.hostname.clone())),
                        ),
                        (
                            CommandResponse::bulk_string("role"),
                            CommandResponse::bulk_string(if n.master.is_none() {
                                "master"
                            } else {
                                "replica"
                            }),
                        ),
                        (
                            CommandResponse::bulk_string("replication-offset"),
                            CommandResponse::integer(n.offset as i64),
                        ),
                        (
                            CommandResponse::bulk_string("health"),
                            CommandResponse::bulk_string(health),
                        ),
                    ])
                })
                .collect();
            out.push(CommandResponse::Map(vec![
                (
                    CommandResponse::bulk_string("slots"),
                    CommandResponse::array(slots),
                ),
                (
                    CommandResponse::bulk_string("nodes"),
                    CommandResponse::array(nodes),
                ),
            ]));
        }
        CommandResponse::array(out)
    }

    /// Write `cluster.json` now.
    pub fn save_now(&self) {
        let st = self.state.lock();
        self.save(&st);
    }

    /// The address of the master this node replicates, if it is a replica.
    pub fn my_master_addr(&self) -> Option<(String, u16)> {
        let st = self.state.lock();
        let m = st.nodes.get(&st.myself)?.master.clone()?;
        let n = st.nodes.get(&m)?;
        Some((n.ip.clone(), n.port))
    }

    /// Every master's address (dump/restore tooling).
    pub fn masters(&self) -> Vec<(String, String, u16)> {
        let st = self.state.lock();
        st.nodes
            .values()
            .filter(|n| n.master.is_none() && !n.handshake)
            .map(|n| (n.id.clone(), n.ip.clone(), n.port))
            .collect()
    }

    /// Ranges this node serves.
    pub fn my_slot_ranges(&self) -> Vec<(u16, u16)> {
        let st = self.state.lock();
        slots_of(&st, &st.myself)
    }
}

/// How long a manual failover may take (`CLUSTER_MF_TIMEOUT`).
const MANUAL_FAILOVER_MS: u64 = 5_000;

fn parse_slot(b: &[u8]) -> Option<u16> {
    let s: u32 = std::str::from_utf8(b).ok()?.parse().ok()?;
    (s < SLOTS as u32).then_some(s as u16)
}

fn slots_of(st: &State, id: &str) -> Vec<(u16, u16)> {
    ranges_of(
        st.owners
            .iter()
            .enumerate()
            .filter(|(_, o)| o.as_deref() == Some(id))
            .map(|(i, _)| i as u16),
    )
}

/// `(start, end, owner)` for every contiguous run of one owner — a range ends
/// where the owner changes, even straight into another owner's range (the
/// export used to drop the boundary slot there).
fn owner_ranges(st: &State) -> Vec<(u16, u16, String)> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < SLOTS {
        let Some(owner) = &st.owners[i] else {
            i += 1;
            continue;
        };
        let start = i;
        while i + 1 < SLOTS && st.owners[i + 1].as_ref() == Some(owner) {
            i += 1;
        }
        out.push((start as u16, i as u16, owner.clone()));
        i += 1;
    }
    out
}

/// Masters serving at least one slot (the voters; their majority is the
/// quorum).
fn serving_masters(st: &State) -> HashSet<String> {
    st.owners
        .iter()
        .flatten()
        .filter(|o| st.nodes.get(*o).is_some_and(|n| n.master.is_none()))
        .cloned()
        .collect()
}

/// A shard's id: its master's id (stable, 40 hex characters).
fn shard_id_of(st: &State, id: &str) -> String {
    match st.nodes.get(id).and_then(|n| n.master.clone()) {
        Some(m) => m,
        None => id.to_string(),
    }
}

fn endpoint(n: &Node) -> CommandResponse {
    CommandResponse::array(vec![
        CommandResponse::bulk(Bytes::from(n.ip.clone())),
        CommandResponse::integer(n.port as i64),
        CommandResponse::bulk(Bytes::from(n.id.clone())),
        CommandResponse::Map(Vec::new()),
    ])
}

/// One `CLUSTER NODES` line.
fn node_line(st: &State, n: &Node) -> String {
    let mut flags = Vec::new();
    if n.id == st.myself {
        flags.push("myself");
    }
    flags.push(if n.master.is_none() {
        "master"
    } else {
        "slave"
    });
    if n.pfail {
        flags.push("fail?");
    }
    if n.fail {
        flags.push("fail");
    }
    if n.handshake {
        flags.push("handshake");
    }
    let addr = if n.hostname.is_empty() {
        format!("{}:{}@{}", n.ip, n.port, n.cport)
    } else {
        format!("{}:{}@{},{}", n.ip, n.port, n.cport, n.hostname)
    };
    let epoch = match &n.master {
        None => n.config_epoch,
        Some(m) => st.nodes.get(m).map_or(0, |m| m.config_epoch),
    };
    let link = if n.id == st.myself || (!n.pfail && !n.fail && n.pong_recv > 0) {
        "connected"
    } else {
        "disconnected"
    };
    let mut line = format!(
        "{} {} {} {} {} {} {} {}",
        n.id,
        addr,
        flags.join(","),
        n.master.as_deref().unwrap_or("-"),
        n.ping_sent,
        n.pong_recv,
        epoch,
        link
    );
    for (s, e) in slots_of(st, &n.id) {
        if s == e {
            line.push_str(&format!(" {}", s));
        } else {
            line.push_str(&format!(" {}-{}", s, e));
        }
    }
    if n.id == st.myself {
        for (slot, to) in &st.migrating {
            line.push_str(&format!(" [{}->-{}]", slot, to));
        }
        for (slot, from) in &st.importing {
            line.push_str(&format!(" [{}-<-{}]", slot, from));
        }
    }
    line.push('\n');
    line
}

/// Read a saved state; `Some(())` when `st` was filled from it.
fn load_saved(text: &str, st: &mut State) -> Option<()> {
    if let Ok(saved) = serde_json::from_str::<Saved>(text) {
        st.myself = saved.myself;
        st.current_epoch = saved.current_epoch;
        st.last_vote_epoch = saved.last_vote_epoch;
        for mut n in saved.nodes {
            n.created = now_ms();
            st.nodes.insert(n.id.clone(), n);
        }
        for (s, e, owner) in saved.slots {
            if s > e || e as usize >= SLOTS {
                continue;
            }
            for slot in s..=e {
                st.owners[slot as usize] = Some(owner.clone());
            }
        }
        st.migrating = saved
            .migrating
            .into_iter()
            .filter(|(s, _)| (*s as usize) < SLOTS)
            .collect();
        st.importing = saved
            .importing
            .into_iter()
            .filter(|(s, _)| (*s as usize) < SLOTS)
            .collect();
        return Some(());
    }
    // Version 1: this node's id and slots, and the other nodes' addresses,
    // which are met again.
    let v1: SavedV1 = serde_json::from_str(text).ok()?;
    st.myself = v1.node_id.clone();
    for (s, e, owner) in v1.slot_owners {
        if owner != v1.node_id || s > e || e as usize >= SLOTS {
            continue;
        }
        for slot in s..=e {
            st.owners[slot as usize] = Some(owner.clone());
        }
    }
    for (ip, port) in v1.known_nodes {
        let temp = generate_node_id();
        let mut n = Node::new(
            temp.clone(),
            ip,
            port,
            port.saturating_add(crate::gossip::CLUSTER_BUS_PORT_OFFSET),
        );
        n.handshake = true;
        st.nodes.insert(temp, n);
    }
    st.dirty = true;
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dbg(r: &CommandResponse) -> String {
        format!("{:?}", r)
    }

    fn mgr() -> ClusterManager {
        ClusterManager::new("127.0.0.1".into(), 7000, 17000, ClusterConfig::default())
    }

    fn header_of(m: &ClusterManager) -> Header {
        let st = m.state.lock();
        m.header(&st)
    }

    fn ping(h: Header) -> Message {
        Message::Ping {
            h,
            ping_id: String::new(),
            gossip: vec![],
        }
    }

    #[test]
    fn a_fresh_node_serves_nothing_and_is_down() {
        let m = mgr();
        assert_eq!(m.node_id().len(), 40);
        assert_eq!(m.slot_route(0), SlotRoute::Down);
        m.add_slots(&(0..16384).collect::<Vec<_>>()).unwrap();
        assert!(m.is_up());
        assert_eq!(m.slot_route(123), SlotRoute::Local);
    }

    #[test]
    fn slot_commands_validate() {
        let m = mgr();
        assert!(m.add_slots(&[1, 1]).is_err());
        m.add_slots(&[1]).unwrap();
        assert!(m.add_slots(&[1]).unwrap_err().contains("busy"));
        assert!(m.del_slots(&[2]).unwrap_err().contains("unassigned"));
        assert_eq!(
            dbg(&m.command(&[Bytes::from("ADDSLOTS"), Bytes::from("16384")])),
            dbg(&CommandResponse::error("ERR Invalid or out of range slot"))
        );
        assert_eq!(
            dbg(&m.command(&[
                Bytes::from("ADDSLOTSRANGE"),
                Bytes::from("10"),
                Bytes::from("20")
            ])),
            dbg(&CommandResponse::ok())
        );
        assert_eq!(m.my_slot_ranges(), vec![(1, 1), (10, 20)]);
    }

    #[test]
    fn a_higher_epoch_claim_takes_slots_and_a_lower_one_is_answered_with_update() {
        let a = mgr();
        let b = ClusterManager::new("127.0.0.1".into(), 7001, 17001, ClusterConfig::default());
        a.add_slots(&(0..100).collect::<Vec<_>>()).unwrap();
        a.state
            .lock()
            .nodes
            .get_mut(&a.node_id())
            .unwrap()
            .config_epoch = 5;
        // b claims 0..50 with epoch 3: stale, a keeps them.
        let mut hb = header_of(&b);
        hb.config_epoch = 3;
        hb.slots = vec![(0, 50)];
        a.handle(ping(hb.clone()));
        assert_eq!(a.slot_owner(10), Some(a.node_id()));
        // With epoch 9 it wins.
        hb.config_epoch = 9;
        a.handle(ping(hb));
        assert_eq!(a.slot_owner(10), Some(b.node_id()));
        assert_eq!(a.slot_owner(60), Some(a.node_id()));
    }

    #[test]
    fn a_master_that_loses_every_slot_follows_the_new_owner() {
        let a = mgr();
        let b = ClusterManager::new("127.0.0.1".into(), 7001, 17001, ClusterConfig::default());
        a.add_slots(&[5, 6]).unwrap();
        let mut hb = header_of(&b);
        hb.config_epoch = 1;
        hb.slots = vec![(5, 6)];
        a.handle(ping(hb));
        assert!(a.is_replica());
        let st = a.state.lock();
        assert_eq!(
            st.nodes[&st.myself].master.as_deref(),
            Some(b.node_id().as_str())
        );
    }

    #[test]
    fn failure_needs_a_majority_of_serving_masters() {
        // a, b, c masters with slots; d is down. a sees d PFAIL; a's own
        // vote is not enough, b's report makes a majority (2 of 3).
        let a = mgr();
        let now = now_ms();
        let (b, c, d) = ("b".repeat(40), "c".repeat(40), "d".repeat(40));
        {
            let mut st = a.state.lock();
            let me = st.myself.clone();
            for (i, id) in [&b, &c, &d].iter().enumerate() {
                let n = Node::new(
                    id.to_string(),
                    "127.0.0.1".into(),
                    7001 + i as u16,
                    17001 + i as u16,
                );
                st.nodes.insert(id.to_string(), n);
            }
            for s in 0..4000 {
                st.owners[s] = Some(me.clone());
            }
            for s in 4000..8000 {
                st.owners[s] = Some(b.clone());
            }
            for s in 8000..12000 {
                st.owners[s] = Some(c.clone());
            }
            for s in 12000..SLOTS {
                st.owners[s] = Some(d.clone());
            }
            st.nodes.get_mut(&d).unwrap().pfail = true;
            let mut fx = Effects::default();
            a.check_fail(&mut st, &mut fx, &d, now);
            assert!(!st.nodes[&d].fail, "one vote of four is no majority");
            st.nodes
                .get_mut(&d)
                .unwrap()
                .fail_reports
                .insert(b.clone(), now);
            a.check_fail(&mut st, &mut fx, &d, now);
            assert!(!st.nodes[&d].fail, "two of four is no majority");
            st.nodes
                .get_mut(&d)
                .unwrap()
                .fail_reports
                .insert(c.clone(), now);
            a.check_fail(&mut st, &mut fx, &d, now);
            assert!(st.nodes[&d].fail);
            assert!(fx
                .sends
                .iter()
                .any(|(_, _, _, m)| matches!(m, Message::Fail { .. })));
        }
    }

    #[test]
    fn a_master_votes_once_per_epoch() {
        let a = mgr();
        let now = now_ms();
        let (m, r1, r2) = ("m".repeat(40), "r".repeat(40), "s".repeat(40));
        let mut st = a.state.lock();
        let me = st.myself.clone();
        let mut mn = Node::new(m.clone(), "127.0.0.1".into(), 7001, 17001);
        mn.fail = true;
        mn.config_epoch = 1;
        st.nodes.insert(m.clone(), mn);
        for r in [&r1, &r2] {
            let mut rn = Node::new(r.to_string(), "127.0.0.1".into(), 7002, 17002);
            rn.master = Some(m.clone());
            st.nodes.insert(r.to_string(), rn);
        }
        st.owners[0] = Some(me.clone());
        st.owners[1] = Some(m.clone());
        st.current_epoch = 7;
        let h = |id: &str| Header {
            id: id.to_string(),
            ip: "127.0.0.1".into(),
            port: 7002,
            cport: 17002,
            hostname: String::new(),
            master: Some(m.clone()),
            current_epoch: 7,
            config_epoch: 1,
            offset: 0,
            slots: vec![],
            paused: false,
            state_ok: true,
        };
        let mut fx = Effects::default();
        a.vote(&mut st, &mut fx, &h(&r1), false, 1, &[(1, 1)], now);
        assert_eq!(
            fx.sends.len(),
            1,
            "first request in the epoch gets the vote"
        );
        a.vote(&mut st, &mut fx, &h(&r2), false, 1, &[(1, 1)], now);
        assert_eq!(fx.sends.len(), 1, "a second vote in one epoch");
        assert_eq!(st.last_vote_epoch, 7);
    }

    #[test]
    fn nodes_and_slots_output_have_redis_shapes() {
        let a = mgr();
        a.add_slots(&(0..10).collect::<Vec<_>>()).unwrap();
        let line = a.nodes_text();
        let fields: Vec<&str> = line.trim_end().split(' ').collect();
        assert_eq!(fields[0].len(), 40);
        assert_eq!(fields[1], "127.0.0.1:7000@17000");
        assert_eq!(fields[2], "myself,master");
        assert_eq!(fields[3], "-");
        assert_eq!(fields[7], "connected");
        assert_eq!(fields[8], "0-9");
        let CommandResponse::Array(rows) = a.slots_reply() else {
            panic!()
        };
        let CommandResponse::Array(row) = &rows[0] else {
            panic!()
        };
        assert_eq!(dbg(&row[0]), dbg(&CommandResponse::Integer(0)));
        assert_eq!(dbg(&row[1]), dbg(&CommandResponse::Integer(9)));
        let CommandResponse::Array(ep) = &row[2] else {
            panic!()
        };
        assert_eq!(dbg(&ep[1]), dbg(&CommandResponse::Integer(7000)));
    }

    #[test]
    fn the_saved_state_round_trips_including_adjacent_owners() {
        let dir = std::env::temp_dir().join(format!("solikv-cluster-{}", generate_node_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cluster.json");
        let cfg = ClusterConfig {
            config_path: Some(path.clone()),
            ..Default::default()
        };
        let a = ClusterManager::new("127.0.0.1".into(), 7000, 17000, cfg.clone());
        let other = "e".repeat(40);
        {
            let mut st = a.state.lock();
            st.nodes.insert(
                other.clone(),
                Node::new(other.clone(), "127.0.0.1".into(), 7001, 17001),
            );
            let me = st.myself.clone();
            for s in 0..100 {
                st.owners[s] = Some(me.clone());
            }
            // Directly adjacent: the old export dropped slot 100 here.
            for s in 100..200 {
                st.owners[s] = Some(other.clone());
            }
            st.current_epoch = 4;
            st.dirty = true;
            let fx = Effects::default();
            a.finish(st, fx);
        }
        let b = ClusterManager::new("127.0.0.1".into(), 7000, 17000, cfg);
        assert_eq!(b.node_id(), a.node_id());
        assert_eq!(b.slot_owner(99), Some(a.node_id()));
        assert_eq!(b.slot_owner(100), Some(other.clone()));
        assert_eq!(b.slot_owner(199), Some(other));
        assert_eq!(b.state.lock().current_epoch, 4);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_handshake_completes_on_the_pong_that_echoes_its_temporary_id() {
        let a = mgr();
        let b = ClusterManager::new("127.0.0.1".into(), 7001, 17001, ClusterConfig::default());
        a.meet("127.0.0.1".into(), 7001, 17001).unwrap();
        let temp = {
            let st = a.state.lock();
            st.nodes.values().find(|n| n.handshake).unwrap().id.clone()
        };
        a.handle(Message::Pong {
            h: header_of(&b),
            ping_id: temp.clone(),
            gossip: vec![],
        });
        let st = a.state.lock();
        assert!(!st.nodes.contains_key(&temp));
        assert!(st.nodes.contains_key(&b.node_id()));
        assert!(!st.nodes[&b.node_id()].handshake);
    }

    #[test]
    fn setslot_node_ends_an_import_with_a_new_epoch() {
        let a = mgr();
        let b = "f".repeat(40);
        {
            let mut st = a.state.lock();
            let mut n = Node::new(b.clone(), "127.0.0.1".into(), 7001, 17001);
            n.config_epoch = 3;
            st.nodes.insert(b.clone(), n);
            st.owners[42] = Some(b.clone());
            st.current_epoch = 3;
        }
        let me = a.node_id();
        let ok = |r: CommandResponse| assert_eq!(dbg(&r), dbg(&CommandResponse::ok()));
        ok(a.command(&[
            Bytes::from("SETSLOT"),
            Bytes::from("42"),
            Bytes::from("IMPORTING"),
            Bytes::from(b.clone()),
        ]));
        assert!(a.is_importing(42));
        assert_eq!(a.slot_route(42), SlotRoute::Down, "cluster not covered yet");
        ok(a.command(&[
            Bytes::from("SETSLOT"),
            Bytes::from("42"),
            Bytes::from("NODE"),
            Bytes::from(me.clone()),
        ]));
        assert!(!a.is_importing(42));
        assert_eq!(a.slot_owner(42), Some(me.clone()));
        let st = a.state.lock();
        assert!(
            st.nodes[&me].config_epoch > 3,
            "no new epoch: the import would not win elsewhere"
        );
    }
}
