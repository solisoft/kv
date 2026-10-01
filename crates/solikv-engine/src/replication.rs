//! The replication stream: what a master sends its replicas, and the backlog
//! a reconnecting replica resumes from.
//!
//! # One ordered stream out of per-shard effects
//!
//! Writes produce *effects* — the records `aof_log` builds, one per shard a
//! command changed, appended while that shard's lock is held. The append-only
//! log keeps one file per shard; replication needs a single byte stream with
//! one offset, as Redis' protocol does (`PSYNC <replid> <offset>`). The feed
//! is that stream:
//!
//! - **Per key, it is in execution order.** A record is appended to the feed
//!   while the shard that produced it is still locked, so two writes to one
//!   key — same shard, same lock — reach the feed in the order they ran.
//!   Writes to different shards interleave in some order, and any order of
//!   independent writes replays to the same state.
//! - **An atomic operation stays atomic.** An operation that holds several
//!   shards at once (`ShardManager::lock_shards`: `EXEC`, scripts, cross-shard
//!   commands) buffers its records on its thread and appends them as one
//!   `MULTI … EXEC` block when the outermost hold ends — before the locks are
//!   released, so a conflicting write still lands after it. The replica
//!   applies a block under all its shard locks: its readers never see half.
//! - **Whole-keyspace commands appear once.** `FLUSHALL`, `FLUSHDB` and
//!   `SWAPDB` are logged per shard for the per-shard log, where each copy
//!   applies to that shard's part; replayed on a replica each would act on
//!   everything, and an even number of `SWAPDB`s swaps nothing. Only shard
//!   0's copy is sent (they run with every shard held, so it is in the same
//!   block as the others would have been).
//!
//! A `SELECT` is inserted whenever a record belongs to a different database
//! than the one before it, as in Redis' stream.
//!
//! # Offsets, ids, backlog
//!
//! Redis semantics, so the numbers mean what tools expect:
//! `master_repl_offset` is the count of bytes ever appended; a replica that
//! has applied up to offset `o` asks for `PSYNC <replid> <o+1>`; the backlog
//! keeps the last `repl-backlog-size` bytes and a request inside it continues
//! from there instead of resynchronizing the whole dataset. A replica that is
//! promoted keeps its master's id as `replid2` (with `second_repl_offset`),
//! so the other replicas of the old master can continue from it.
//!
//! # A replica proxies its master's bytes
//!
//! On a replica the engine does not produce records: it applies the master's
//! stream and appends those same bytes here, unchanged, so its own replicas
//! (and, after a promotion, the old master's replicas) see the master's
//! offsets. Writes a writable replica takes from its own clients are local
//! only, as in Redis.
//!
//! # Cost when nobody replicates
//!
//! Nothing is produced until a replica first connects (Redis creates the
//! backlog at the same moment): a write pays one relaxed load of `producing`.

use bytes::Bytes;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Default `repl-backlog-size`.
pub const DEFAULT_BACKLOG: usize = 1024 * 1024;

/// Default hard limit on what one replica may have waiting to be sent
/// (Redis' `client-output-buffer-limit replica 256mb`).
pub const DEFAULT_REPLICA_BUFFER_LIMIT: usize = 256 * 1024 * 1024;

const PING_RECORD: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const GETACK_RECORD: &[u8] = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";

/// A new 40-hex-character replication id.
pub fn new_replid() -> String {
    use rand_like::next;
    let mut s = String::with_capacity(40);
    while s.len() < 40 {
        s.push_str(&format!("{:016x}", next()));
    }
    s.truncate(40);
    s
}

/// What the server's replication link reports, for `ROLE` and `INFO`.
#[derive(Debug, Clone, Default)]
pub struct ReplicaStatus {
    pub master_host: String,
    pub master_port: u16,
    /// `connect`, `connecting`, `sync` or `connected` (`ROLE`'s words).
    pub state: &'static str,
    /// Seconds since the last byte from the master (-1: never).
    pub last_io_secs: i64,
    /// A full synchronization is being received.
    pub sync_in_progress: bool,
}

/// The server half of replication, which the engine's commands drive
/// (`REPLICAOF`, `ROLE`, `INFO replication`). Implemented by the server,
/// which owns the network link.
pub trait ReplController: Send + Sync {
    /// Start (or switch) following `host:port`.
    fn replicaof(&self, host: String, port: u16) -> Result<(), String>;
    /// Stop following; become a master.
    fn no_one(&self) -> Result<(), String>;
    /// `None` when this node is a master.
    fn status(&self) -> Option<ReplicaStatus>;
}

/// A tiny PRNG (no dependency): ids only need to be unlikely to collide.
mod rand_like {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0);

    pub fn next() -> u64 {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
            ^ (std::process::id() as u64).rotate_left(32);
        let mut z = STATE
            .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
            .wrapping_add(seed);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// One connected replica, as the feed sees it: bytes waiting to be sent.
pub struct Sink {
    pub id: u64,
    /// Woken whenever bytes are added.
    pub notify: Arc<tokio::sync::Notify>,
    /// Offset the replica last acknowledged (`REPLCONF ACK`).
    pub ack: Arc<AtomicU64>,
    /// Address and listening port the replica announced (`INFO`, `ROLE`).
    pub addr: String,
    pub listening_port: u16,
    /// Set once the initial synchronization has been sent; before that the
    /// replica is `wait_bgsave` and not counted by `WAIT`.
    pub online: Arc<AtomicBool>,
    /// Unix ms of the replica's last `REPLCONF ACK` (its lag, for `INFO`
    /// and `min-replicas-max-lag`).
    pub last_ack_ms: Arc<AtomicU64>,
}

impl Sink {
    /// Seconds since the replica last acknowledged.
    pub fn lag_secs(&self) -> u64 {
        solikv_core::now_millis().saturating_sub(self.last_ack_ms.load(Ordering::Relaxed)) / 1000
    }
}

struct SinkSlot {
    sink: Arc<Sink>,
    buf: Vec<u8>,
    /// Over the limit: the link is closed and the replica resynchronizes.
    overflow: bool,
}

struct Inner {
    replid: String,
    replid2: String,
    /// First offset `replid2` is valid for (`second_repl_offset`), or -1.
    second_offset: i64,
    /// Bytes ever appended (`master_repl_offset`).
    offset: u64,
    backlog: VecDeque<u8>,
    backlog_cap: usize,
    /// Database of the last record in the stream; `None` forces a `SELECT`.
    db: Option<u32>,
    sinks: Vec<SinkSlot>,
    next_sink: u64,
    buffer_limit: usize,
    /// Ever had a backlog (a replica connected, or this node is a replica).
    backlog_active: bool,
}

/// The replication stream of this node (see the module doc).
pub struct ReplFeed {
    /// The engine's writes are recorded (master with a backlog).
    producing: AtomicBool,
    /// This node is a replica (its writes are not recorded; it proxies).
    replica: AtomicBool,
    inner: Mutex<Inner>,
    /// Woken when a replica acknowledges (for `WAIT`).
    pub acked: tokio::sync::Notify,
    connected: AtomicUsize,
    /// Identity of this feed for the thread-local transaction buffer.
    id: usize,
    /// `sync_full`, `sync_partial_ok`, `sync_partial_err` (`INFO`).
    pub sync_full: AtomicU64,
    pub sync_partial_ok: AtomicU64,
    pub sync_partial_err: AtomicU64,
}

static FEED_IDS: AtomicUsize = AtomicUsize::new(1);

impl Default for ReplFeed {
    fn default() -> Self {
        Self::new()
    }
}

/// Where a `PSYNC` lands.
pub enum Psync {
    /// `+CONTINUE`: these bytes, then the live stream.
    Continue { replid: String },
    /// `+FULLRESYNC`: a snapshot is needed.
    Full,
}

impl ReplFeed {
    pub fn new() -> Self {
        Self {
            producing: AtomicBool::new(false),
            replica: AtomicBool::new(false),
            inner: Mutex::new(Inner {
                replid: new_replid(),
                replid2: "0".repeat(40),
                second_offset: -1,
                offset: 0,
                backlog: VecDeque::new(),
                backlog_cap: DEFAULT_BACKLOG,
                db: None,
                sinks: Vec::new(),
                next_sink: 1,
                buffer_limit: DEFAULT_REPLICA_BUFFER_LIMIT,
                backlog_active: false,
            }),
            acked: tokio::sync::Notify::new(),
            connected: AtomicUsize::new(0),
            id: FEED_IDS.fetch_add(1, Ordering::Relaxed),
            sync_full: AtomicU64::new(0),
            sync_partial_ok: AtomicU64::new(0),
            sync_partial_err: AtomicU64::new(0),
        }
    }

    /// Whether writes must produce records for this feed (one relaxed load:
    /// the cost a write pays when nobody replicates).
    #[inline]
    pub fn producing(&self) -> bool {
        self.producing.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_replica(&self) -> bool {
        self.replica.load(Ordering::Relaxed)
    }

    pub fn set_backlog_size(&self, bytes: usize) {
        let mut g = self.inner.lock();
        g.backlog_cap = bytes.max(16 * 1024);
        while g.backlog.len() > g.backlog_cap {
            g.backlog.pop_front();
        }
    }

    pub fn backlog_size(&self) -> usize {
        self.inner.lock().backlog_cap
    }

    pub fn set_buffer_limit(&self, bytes: usize) {
        self.inner.lock().buffer_limit = bytes.max(1024 * 1024);
    }

    /// `(replid, replid2, master_repl_offset, second_repl_offset)`.
    pub fn ids(&self) -> (String, String, u64, i64) {
        let g = self.inner.lock();
        (
            g.replid.clone(),
            g.replid2.clone(),
            g.offset,
            g.second_offset,
        )
    }

    pub fn offset(&self) -> u64 {
        self.inner.lock().offset
    }

    /// `(active, first byte offset, histlen)` of the backlog.
    pub fn backlog_info(&self) -> (bool, u64, u64) {
        let g = self.inner.lock();
        let len = g.backlog.len() as u64;
        (g.backlog_active, g.offset - len + 1, len)
    }

    // -----------------------------------------------------------------------
    // Producing (master)
    // -----------------------------------------------------------------------

    /// Append a record of database `db`, as produced under a shard lock.
    /// `held`: the shard is part of this thread's multi-shard hold, so the
    /// record joins its block (see the module doc).
    pub fn record(&self, shard: u32, db: u32, rec: &[u8], held: bool) {
        if shard != 0 && is_whole_keyspace(rec) {
            return;
        }
        if held {
            let buffered = TX.with(|tx| {
                let mut tx = tx.borrow_mut();
                match tx.as_mut() {
                    Some(b) if b.feed == self.id => {
                        b.records.push((db, rec.to_vec()));
                        true
                    }
                    _ => false,
                }
            });
            if buffered {
                return;
            }
        }
        let mut g = self.inner.lock();
        g.select(db);
        g.write(rec);
    }

    /// Append a command not tied to a database or a shard (`PUBLISH`,
    /// `FUNCTION LOAD`).
    pub fn command(&self, name: &[u8], args: &[&[u8]]) {
        if !self.producing() {
            return;
        }
        let rec = solikv_persist::aof::encode_command(name, args);
        self.inner.lock().write(&rec);
    }

    /// Open this thread's block (outermost multi-shard hold).
    pub(crate) fn begin_tx(&self) {
        TX.with(|tx| {
            let mut tx = tx.borrow_mut();
            if tx.is_none() {
                *tx = Some(TxBuf {
                    feed: self.id,
                    records: Vec::new(),
                });
            }
        });
    }

    /// Close it: its records go out as one `MULTI … EXEC` (a lone record
    /// needs no bracket). Called before the hold releases its locks.
    pub(crate) fn end_tx(&self) {
        let Some(buf) = TX.with(|tx| tx.borrow_mut().take()) else {
            return;
        };
        if buf.feed != self.id || buf.records.is_empty() {
            return;
        }
        let mut g = self.inner.lock();
        let bracket = buf.records.len() > 1;
        if bracket {
            g.write(b"*1\r\n$5\r\nMULTI\r\n");
        }
        for (db, rec) in &buf.records {
            g.select(*db);
            g.write(rec);
        }
        if bracket {
            g.write(b"*1\r\n$4\r\nEXEC\r\n");
        }
    }

    /// The periodic `PING` a master sends (`repl-ping-replica-period`).
    pub fn ping(&self) {
        if self.producing() && self.connected.load(Ordering::Relaxed) > 0 {
            self.inner.lock().write(PING_RECORD);
        }
    }

    /// Ask every replica for its offset now (`WAIT`).
    pub fn getack(&self) {
        if self.producing() {
            self.inner.lock().write(GETACK_RECORD);
        }
    }

    // -----------------------------------------------------------------------
    // Replicas of this node
    // -----------------------------------------------------------------------

    /// Decide a `PSYNC <replid> <offset>`. A `Continue` registers `sink`
    /// with the backlog from `offset` already queued; a `Full` registers
    /// nothing (see [`register_full`](Self::register_full)).
    pub fn try_continue(&self, replid: &str, offset: i64, sink: &Arc<Sink>) -> Psync {
        let r = self.try_continue_inner(replid, offset, sink);
        match (&r, replid) {
            (Psync::Continue { .. }, _) => self.sync_partial_ok.fetch_add(1, Ordering::Relaxed),
            (Psync::Full, "?") => 0,
            (Psync::Full, _) => self.sync_partial_err.fetch_add(1, Ordering::Relaxed),
        };
        r
    }

    fn try_continue_inner(&self, replid: &str, offset: i64, sink: &Arc<Sink>) -> Psync {
        let mut g = self.inner.lock();
        if !g.backlog_active || offset < 1 {
            return Psync::Full;
        }
        let offset = offset as u64;
        let same = replid == g.replid
            || (replid == g.replid2 && g.second_offset >= 0 && offset <= g.second_offset as u64);
        let first = g.offset - g.backlog.len() as u64 + 1;
        if !same || offset < first || offset > g.offset + 1 {
            return Psync::Full;
        }
        let skip = (offset - first) as usize;
        let buf: Vec<u8> = g.backlog.iter().skip(skip).copied().collect();
        sink.online.store(true, Ordering::Relaxed);
        g.sinks.push(SinkSlot {
            sink: sink.clone(),
            buf,
            overflow: false,
        });
        self.connected.fetch_add(1, Ordering::Relaxed);
        Psync::Continue {
            replid: g.replid.clone(),
        }
    }

    /// Register a replica for a full synchronization: from now on it is
    /// sent every byte appended. Call while every shard is held, at the
    /// instant the snapshot begins, so the snapshot and the stream meet
    /// exactly. Returns `(replid, offset)` for `+FULLRESYNC`.
    pub fn register_full(&self, sink: &Arc<Sink>) -> (String, u64) {
        self.sync_full.fetch_add(1, Ordering::Relaxed);
        let mut g = self.inner.lock();
        if !g.backlog_active {
            g.backlog_active = true;
        }
        if !self.is_replica() {
            self.producing.store(true, Ordering::Release);
        }
        // The replica starts from a snapshot of every database: the first
        // record after it must say which one it is in.
        g.db = None;
        g.sinks.push(SinkSlot {
            sink: sink.clone(),
            buf: Vec::new(),
            overflow: false,
        });
        self.connected.fetch_add(1, Ordering::Relaxed);
        (g.replid.clone(), g.offset)
    }

    /// New sink id.
    pub fn next_sink_id(&self) -> u64 {
        let mut g = self.inner.lock();
        let id = g.next_sink;
        g.next_sink += 1;
        id
    }

    /// Bytes waiting for `sink`; `None` when it went over its limit (or was
    /// dropped) and must be disconnected.
    pub fn take(&self, sink: &Sink) -> Option<Vec<u8>> {
        let mut g = self.inner.lock();
        let slot = g.sinks.iter_mut().find(|s| s.sink.id == sink.id)?;
        if slot.overflow {
            return None;
        }
        Some(std::mem::take(&mut slot.buf))
    }

    pub fn unregister(&self, sink: &Sink) {
        let mut g = self.inner.lock();
        let before = g.sinks.len();
        g.sinks.retain(|s| s.sink.id != sink.id);
        if g.sinks.len() < before {
            self.connected.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Replicas registered (any state).
    pub fn connected(&self) -> usize {
        self.connected.load(Ordering::Relaxed)
    }

    /// Online replicas that acknowledged within `max_lag_secs`
    /// (`min-replicas-to-write`'s "good" replicas).
    pub fn good_replicas(&self, max_lag_secs: u64) -> usize {
        self.inner
            .lock()
            .sinks
            .iter()
            .filter(|s| s.sink.online.load(Ordering::Relaxed) && s.sink.lag_secs() <= max_lag_secs)
            .count()
    }

    /// The registered replicas.
    pub fn sinks(&self) -> Vec<Arc<Sink>> {
        self.inner
            .lock()
            .sinks
            .iter()
            .map(|s| s.sink.clone())
            .collect()
    }

    /// Online replicas whose acknowledged offset is at least `offset`.
    pub fn acked_at_least(&self, offset: u64) -> usize {
        self.inner
            .lock()
            .sinks
            .iter()
            .filter(|s| {
                s.sink.online.load(Ordering::Relaxed)
                    && s.sink.ack.load(Ordering::Relaxed) >= offset
            })
            .count()
    }

    /// Disconnect every replica (role change).
    pub fn drop_sinks(&self) {
        let mut g = self.inner.lock();
        for s in g.sinks.iter_mut() {
            s.overflow = true;
            s.sink.notify.notify_one();
        }
        g.sinks.clear();
        self.connected.store(0, Ordering::Relaxed);
    }

    // -----------------------------------------------------------------------
    // Role changes
    // -----------------------------------------------------------------------

    /// Become a replica: stop producing; the master's stream will be proxied.
    pub fn become_replica(&self) {
        self.replica.store(true, Ordering::Release);
        self.producing.store(false, Ordering::Release);
    }

    /// A full synchronization from a master loaded: adopt its id and
    /// offset; the backlog restarts there.
    pub fn reset_to(&self, replid: &str, offset: u64) {
        let mut g = self.inner.lock();
        g.replid = replid.to_string();
        g.replid2 = "0".repeat(40);
        g.second_offset = -1;
        g.offset = offset;
        g.backlog.clear();
        g.backlog_active = true;
        g.db = None;
    }

    /// The master answered `+CONTINUE <newid>` after its own promotion:
    /// keep serving the old id up to here.
    pub fn switch_replid(&self, new: &str) {
        let mut g = self.inner.lock();
        if g.replid != new {
            g.replid2 = std::mem::replace(&mut g.replid, new.to_string());
            g.second_offset = g.offset as i64 + 1;
        }
    }

    /// Append bytes received from the master, verbatim (replica).
    pub fn proxy(&self, bytes: &[u8]) {
        let mut g = self.inner.lock();
        g.write(bytes);
        // What follows in the master's stream sets its own database.
        g.db = None;
    }

    /// Promotion (`REPLICAOF NO ONE`, failover): a new history starts here;
    /// the old one stays continuable as `replid2`.
    pub fn promote(&self) {
        let mut g = self.inner.lock();
        g.replid2 = std::mem::replace(&mut g.replid, new_replid());
        g.second_offset = g.offset as i64 + 1;
        g.db = None;
        self.replica.store(false, Ordering::Release);
        if g.backlog_active {
            self.producing.store(true, Ordering::Release);
        }
    }

    /// A write was applied that the feed must also see (replica proxy of
    /// a raw command already counted elsewhere) — kept for symmetry.
    pub fn is_active(&self) -> bool {
        self.inner.lock().backlog_active
    }
}

impl Inner {
    fn select(&mut self, db: u32) {
        if self.db != Some(db) {
            let n = db.to_string();
            let rec = solikv_persist::aof::encode_command(b"SELECT", &[n.as_bytes()]);
            self.write(&rec);
            self.db = Some(db);
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        if !self.backlog_active {
            return;
        }
        self.offset += bytes.len() as u64;
        self.backlog.extend(bytes);
        let over = self.backlog.len().saturating_sub(self.backlog_cap);
        if over > 0 {
            self.backlog.drain(..over);
        }
        let limit = self.buffer_limit;
        for s in self.sinks.iter_mut() {
            if s.overflow {
                continue;
            }
            s.buf.extend_from_slice(bytes);
            if s.buf.len() > limit {
                s.overflow = true;
                s.buf = Vec::new();
            }
            s.sink.notify.notify_one();
        }
    }
}

/// `FLUSHALL`, `FLUSHDB` or `SWAPDB` (a record that acts on the whole
/// keyspace when applied anywhere but the per-shard log).
fn is_whole_keyspace(rec: &[u8]) -> bool {
    // `*N\r\n$L\r\nNAME\r\n`: find the name.
    let Some(p) = rec.iter().position(|&b| b == b'\n') else {
        return false;
    };
    let rest = &rec[p + 1..];
    let Some(q) = rest.iter().position(|&b| b == b'\n') else {
        return false;
    };
    let name = &rest[q + 1..];
    let end = name.iter().position(|&b| b == b'\r').unwrap_or(name.len());
    let name = &name[..end];
    name.eq_ignore_ascii_case(b"FLUSHALL")
        || name.eq_ignore_ascii_case(b"FLUSHDB")
        || name.eq_ignore_ascii_case(b"SWAPDB")
}

struct TxBuf {
    feed: usize,
    records: Vec<(u32, Vec<u8>)>,
}

thread_local! {
    static TX: RefCell<Option<TxBuf>> = const { RefCell::new(None) };
    /// Set while this thread applies its master's stream (the writes are
    /// the master's, not a client's: allowed on a read-only replica).
    static APPLYING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether this thread is applying the master's stream.
#[inline]
pub fn applying_master_stream() -> bool {
    APPLYING.with(|a| a.get())
}

/// Marks this thread as applying the master's stream until dropped.
pub struct ApplyingGuard {
    prev: bool,
}

impl ApplyingGuard {
    pub fn new() -> Self {
        Self {
            prev: APPLYING.with(|a| a.replace(true)),
        }
    }
}

impl Default for ApplyingGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ApplyingGuard {
    fn drop(&mut self) {
        let prev = self.prev;
        APPLYING.with(|a| a.set(prev));
    }
}

/// Parses whole RESP commands out of a byte stream (the replication link),
/// returning each with the bytes it occupied — offsets are counted in bytes
/// of the stream, and a replica proxies them verbatim.
/// One command of the stream: its arguments, and the bytes it took.
pub type StreamCommand = (Vec<Bytes>, Vec<u8>);

#[derive(Default)]
pub struct StreamParser {
    buf: Vec<u8>,
    pos: usize,
}

/// Most bytes one command of the stream may take (a master's `#PUT` of a
/// large value included).
const MAX_STREAM_COMMAND: usize = 1024 * 1024 * 1024;

impl StreamParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, data: &[u8]) {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        } else if self.pos > 64 * 1024 && self.pos * 2 > self.buf.len() {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(data);
    }

    pub fn buffered(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// The next whole command: its arguments and its raw bytes. `Ok(None)`
    /// when more input is needed.
    pub fn next_command(&mut self) -> Result<Option<StreamCommand>, String> {
        let src = &self.buf[self.pos..];
        if src.is_empty() {
            return Ok(None);
        }
        if src[0] != b'*' {
            // An inline line (Redis sends `\n` keepalives while it prepares
            // a snapshot; a stream never carries inline commands otherwise).
            let Some(nl) = src.iter().position(|&b| b == b'\n') else {
                return Ok(None);
            };
            let raw = src[..=nl].to_vec();
            self.pos += nl + 1;
            let line = String::from_utf8_lossy(&raw).trim().to_string();
            if line.is_empty() {
                return Ok(Some((Vec::new(), raw)));
            }
            let args = line
                .split_whitespace()
                .map(|s| Bytes::from(s.to_string()))
                .collect();
            return Ok(Some((args, raw)));
        }
        let mut p = 0;
        let Some((n, used)) = read_header(src, p, b'*')? else {
            return Ok(None);
        };
        p += used;
        if n > 1024 * 1024 {
            return Err(format!("replication stream: {n} arguments in one command"));
        }
        let mut args = Vec::with_capacity(n);
        for _ in 0..n {
            let Some((len, used)) = read_header(src, p, b'$')? else {
                return Ok(None);
            };
            if len > MAX_STREAM_COMMAND {
                return Err(format!("replication stream: argument of {len} bytes"));
            }
            p += used;
            if src.len() < p + len + 2 {
                return Ok(None);
            }
            args.push(Bytes::copy_from_slice(&src[p..p + len]));
            p += len + 2;
        }
        let raw = src[..p].to_vec();
        self.pos += p;
        Ok(Some((args, raw)))
    }
}

/// `<kind><digits>\r\n` at `at`: the number and the bytes it took.
fn read_header(src: &[u8], at: usize, kind: u8) -> Result<Option<(usize, usize)>, String> {
    let s = &src[at..];
    if s.is_empty() {
        return Ok(None);
    }
    if s[0] != kind {
        return Err(format!(
            "replication stream: expected {:?}, got {:?}",
            kind as char, s[0] as char
        ));
    }
    let Some(cr) = s.iter().position(|&b| b == b'\r') else {
        if s.len() > 32 {
            return Err("replication stream: header too long".into());
        }
        return Ok(None);
    };
    if s.len() < cr + 2 {
        return Ok(None);
    }
    let n: usize = std::str::from_utf8(&s[1..cr])
        .ok()
        .and_then(|t| t.parse().ok())
        .ok_or_else(|| "replication stream: bad length".to_string())?;
    Ok(Some((n, cr + 2)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink(feed: &ReplFeed) -> Arc<Sink> {
        Arc::new(Sink {
            id: feed.next_sink_id(),
            notify: Arc::new(tokio::sync::Notify::new()),
            ack: Arc::new(AtomicU64::new(0)),
            addr: "127.0.0.1".into(),
            listening_port: 0,
            online: Arc::new(AtomicBool::new(false)),
            last_ack_ms: Arc::new(AtomicU64::new(solikv_core::now_millis())),
        })
    }

    fn rec(args: &[&str]) -> Vec<u8> {
        let name = args[0].as_bytes();
        let rest: Vec<&[u8]> = args[1..].iter().map(|a| a.as_bytes()).collect();
        solikv_persist::aof::encode_command(name, &rest)
    }

    #[test]
    fn nothing_is_recorded_before_a_replica_connects() {
        let f = ReplFeed::new();
        assert!(!f.producing());
        f.record(0, 0, &rec(&["SET", "a", "1"]), false);
        assert_eq!(f.offset(), 0);
        let s = sink(&f);
        let (_, off) = f.register_full(&s);
        assert_eq!(off, 0);
        assert!(f.producing());
        f.record(0, 0, &rec(&["SET", "a", "1"]), false);
        let got = f.take(&s).unwrap();
        // SELECT first: the replica starts from a snapshot of every db.
        let mut want = rec(&["SELECT", "0"]);
        want.extend(rec(&["SET", "a", "1"]));
        assert_eq!(got, want);
        assert_eq!(f.offset(), want.len() as u64);
    }

    #[test]
    fn a_reconnect_inside_the_backlog_continues_from_its_offset() {
        let f = ReplFeed::new();
        let a = sink(&f);
        f.register_full(&a);
        f.record(0, 0, &rec(&["SET", "a", "1"]), false);
        let mid = f.offset();
        f.record(1, 0, &rec(&["SET", "b", "2"]), false);
        let (replid, _, _, _) = f.ids();
        let b = sink(&f);
        match f.try_continue(&replid, mid as i64 + 1, &b) {
            Psync::Continue { .. } => {}
            Psync::Full => panic!("expected a partial resync"),
        }
        assert_eq!(f.take(&b).unwrap(), rec(&["SET", "b", "2"]));
        // A wrong id, or an offset already dropped, is a full resync.
        assert!(matches!(
            f.try_continue("nope", mid as i64 + 1, &sink(&f)),
            Psync::Full
        ));
        f.set_backlog_size(16 * 1024);
        for i in 0..2000 {
            f.record(0, 0, &rec(&["SET", "k", &i.to_string()]), false);
        }
        assert!(matches!(f.try_continue(&replid, 1, &sink(&f)), Psync::Full));
    }

    #[test]
    fn a_promoted_replica_continues_its_old_masters_history() {
        let f = ReplFeed::new();
        f.become_replica();
        f.reset_to("a".repeat(40).as_str(), 100);
        f.proxy(&rec(&["SET", "a", "1"]));
        let at = f.offset();
        f.promote();
        let (id, id2, _, second) = f.ids();
        assert_ne!(id, "a".repeat(40));
        assert_eq!(id2, "a".repeat(40));
        assert_eq!(second, at as i64 + 1);
        let s = sink(&f);
        assert!(matches!(
            f.try_continue(&"a".repeat(40), at as i64 + 1, &s),
            Psync::Continue { .. }
        ));
    }

    #[test]
    fn a_multi_shard_hold_goes_out_as_one_block() {
        let f = ReplFeed::new();
        let s = sink(&f);
        f.register_full(&s);
        f.take(&s).unwrap();
        f.begin_tx();
        f.record(0, 0, &rec(&["SET", "a", "1"]), true);
        f.record(1, 0, &rec(&["SET", "b", "1"]), true);
        assert!(
            f.take(&s).unwrap().is_empty(),
            "sent before the block ended"
        );
        f.end_tx();
        let mut want = rec(&["MULTI"]);
        want.extend(rec(&["SELECT", "0"]));
        want.extend(rec(&["SET", "a", "1"]));
        want.extend(rec(&["SET", "b", "1"]));
        want.extend(rec(&["EXEC"]));
        assert_eq!(f.take(&s).unwrap(), want);
    }

    #[test]
    fn whole_keyspace_commands_are_sent_once() {
        let f = ReplFeed::new();
        let s = sink(&f);
        f.register_full(&s);
        f.take(&s).unwrap();
        f.record(0, 0, &rec(&["SWAPDB", "0", "1"]), false);
        f.record(1, 0, &rec(&["SWAPDB", "0", "1"]), false);
        f.record(2, 0, &rec(&["flushall"]), false);
        let mut want = rec(&["SELECT", "0"]);
        want.extend(rec(&["SWAPDB", "0", "1"]));
        assert_eq!(f.take(&s).unwrap(), want);
    }

    #[test]
    fn a_replica_over_its_limit_is_cut_off() {
        let f = ReplFeed::new();
        f.set_buffer_limit(1);
        let s = sink(&f);
        f.register_full(&s);
        let big = "x".repeat(2 * 1024 * 1024);
        f.record(0, 0, &rec(&["SET", "a", &big]), false);
        assert!(f.take(&s).is_none());
    }

    #[test]
    fn the_stream_parser_returns_raw_bytes_per_command() {
        let mut p = StreamParser::new();
        let a = rec(&["SET", "a", "1"]);
        let b = rec(&["PING"]);
        let mut all = a.clone();
        all.extend(&b);
        p.push(&all[..5]);
        assert!(p.next_command().unwrap().is_none());
        p.push(&all[5..]);
        let (args, raw) = p.next_command().unwrap().unwrap();
        assert_eq!(args.len(), 3);
        assert_eq!(raw, a);
        let (args, raw) = p.next_command().unwrap().unwrap();
        assert_eq!(args[0].as_ref(), b"PING");
        assert_eq!(raw, b);
        assert!(p.next_command().unwrap().is_none());
        p.push(b"\n");
        let (args, raw) = p.next_command().unwrap().unwrap();
        assert!(args.is_empty());
        assert_eq!(raw, b"\n");
    }
}
