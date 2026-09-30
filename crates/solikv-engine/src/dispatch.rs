use bytes::Bytes;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Instant;

use solikv_core::store::EvictionPolicy;
use solikv_core::types::*;
use solikv_core::zset_ops::{Aggregate, LexBound, ScoreBound, ZRangeBy, ZRangeQuery};

// ── Keyspace notification flags ──
// Manual bitflags (no extra crate).
pub const NOTIFY_KEYSPACE: u16 = 0x001; // K
pub const NOTIFY_KEYEVENT: u16 = 0x002; // E
pub const NOTIFY_GENERIC: u16 = 0x004; // g
pub const NOTIFY_STRING: u16 = 0x008; // $
pub const NOTIFY_LIST: u16 = 0x010; // l
pub const NOTIFY_SET: u16 = 0x020; // s
pub const NOTIFY_HASH: u16 = 0x040; // h
pub const NOTIFY_ZSET: u16 = 0x080; // z
pub const NOTIFY_STREAM: u16 = 0x100; // t
pub const NOTIFY_EXPIRED: u16 = 0x200; // x
pub const NOTIFY_EVICTED: u16 = 0x400; // e
pub const NOTIFY_ALL: u16 = NOTIFY_GENERIC
    | NOTIFY_STRING
    | NOTIFY_LIST
    | NOTIFY_SET
    | NOTIFY_HASH
    | NOTIFY_ZSET
    | NOTIFY_STREAM
    | NOTIFY_EXPIRED
    | NOTIFY_EVICTED; // A

pub fn parse_notify_flags(s: &str) -> u16 {
    let mut flags: u16 = 0;
    for c in s.chars() {
        match c {
            'K' => flags |= NOTIFY_KEYSPACE,
            'E' => flags |= NOTIFY_KEYEVENT,
            'g' => flags |= NOTIFY_GENERIC,
            '$' => flags |= NOTIFY_STRING,
            'l' => flags |= NOTIFY_LIST,
            's' => flags |= NOTIFY_SET,
            'h' => flags |= NOTIFY_HASH,
            'z' => flags |= NOTIFY_ZSET,
            't' => flags |= NOTIFY_STREAM,
            'x' => flags |= NOTIFY_EXPIRED,
            'e' => flags |= NOTIFY_EVICTED,
            'A' => flags |= NOTIFY_ALL,
            _ => {}
        }
    }
    // If neither K nor E is set, disable entirely
    if flags & (NOTIFY_KEYSPACE | NOTIFY_KEYEVENT) == 0 {
        return 0;
    }
    flags
}

pub fn flags_to_string(flags: u16) -> String {
    if flags == 0 {
        return String::new();
    }
    let mut s = String::new();
    if flags & NOTIFY_KEYSPACE != 0 {
        s.push('K');
    }
    if flags & NOTIFY_KEYEVENT != 0 {
        s.push('E');
    }
    if flags & NOTIFY_GENERIC != 0 {
        s.push('g');
    }
    if flags & NOTIFY_STRING != 0 {
        s.push('$');
    }
    if flags & NOTIFY_LIST != 0 {
        s.push('l');
    }
    if flags & NOTIFY_SET != 0 {
        s.push('s');
    }
    if flags & NOTIFY_HASH != 0 {
        s.push('h');
    }
    if flags & NOTIFY_ZSET != 0 {
        s.push('z');
    }
    if flags & NOTIFY_STREAM != 0 {
        s.push('t');
    }
    if flags & NOTIFY_EXPIRED != 0 {
        s.push('x');
    }
    if flags & NOTIFY_EVICTED != 0 {
        s.push('e');
    }
    s
}

fn parse_stream_id(arg: &Bytes) -> Option<StreamIdInput> {
    let s = std::str::from_utf8(arg).ok()?;
    StreamId::parse(s)
}

fn parse_stream_trim(
    args: &[Bytes],
    start: &mut usize,
) -> Result<Option<StreamTrim>, CommandResponse> {
    let i = *start;
    if i >= args.len() {
        return Ok(None);
    }
    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
    match opt.as_str() {
        "MAXLEN" | "MINID" => {
            *start = i + 1;
            let mut exact = true;
            if *start < args.len() {
                let next = std::str::from_utf8(&args[*start]).unwrap_or("");
                if next == "~" || next == "=" {
                    exact = next == "=";
                    *start += 1;
                }
            }
            if *start >= args.len() {
                return Err(CommandResponse::syntax_error());
            }
            if opt == "MAXLEN" {
                let threshold = std::str::from_utf8(&args[*start])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .ok_or_else(CommandResponse::syntax_error)?;
                *start += 1;
                Ok(Some(StreamTrim::MaxLen { exact, threshold }))
            } else {
                let id_input =
                    parse_stream_id(&args[*start]).ok_or_else(CommandResponse::syntax_error)?;
                let threshold = match id_input {
                    StreamIdInput::Explicit(id) => id,
                    StreamIdInput::Partial(ms) => StreamId::new(ms, 0),
                    _ => return Err(CommandResponse::syntax_error()),
                };
                *start += 1;
                Ok(Some(StreamTrim::MinId { exact, threshold }))
            }
        }
        _ => Ok(None),
    }
}
use solikv_persist::AofWriter;
use solikv_pubsub::PubSubBroker;

use crate::commands::Cmd;
use crate::lua::ScriptCache;
use crate::persistence::{PersistConfig, Persistence};
use crate::session::{ClientRegistry, ReplyMode, Session};
use crate::shard::{current_db, DbGuard, ShardManager};
use crate::watch::{Touch, TouchGuard, WatchFlag};

thread_local! {
    /// How many `execute` calls are running on this thread (scripts nest
    /// them). `SELECT` outside a connection is only meaningful nested: in a
    /// script it switches the database of the script's later calls.
    static DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Default number of logical databases (`--databases`).
pub const DEFAULT_DATABASES: u32 = 16;

/// The central command engine that routes commands to appropriate shards.
pub struct CommandEngine {
    pub shards: Arc<ShardManager>,
    pub pubsub: Arc<PubSubBroker>,
    start_time: Instant,
    pub(crate) script_cache: ScriptCache,
    weak_self: OnceLock<Weak<CommandEngine>>,
    notify_flags: Arc<AtomicU16>,
    pub replication_offset: Arc<AtomicU64>,
    /// Snapshots, the append-only log and shutdown.
    ///
    /// `SAVE`/`BGSAVE` used to write to a hardcoded `data`/`dump` in the
    /// command handler, so a server started with `--dir /var/lib/solikv`
    /// wrote its background and shutdown snapshots there but an explicit
    /// `SAVE` wrote to `./data` relative to the process CWD — a snapshot that
    /// the next startup never loads. Everything now goes through the one
    /// configuration the server started with.
    persistence: Arc<Persistence>,
    /// Woken by `SHUTDOWN` once the final save succeeded; the server exits.
    shutdown_requested: Arc<tokio::sync::Notify>,
    /// Most memory one Lua VM may allocate. Without a cap a script such as
    /// `string.rep("x", 2^31)` asks the allocator for whatever it likes, and
    /// the instruction hook cannot interrupt a C function to stop it.
    pub(crate) lua_memory_limit: usize,
    /// Logical databases (`SELECT 0` … `SELECT databases-1`).
    databases: u32,
    /// Connected clients (`CLIENT LIST`, `CLIENT KILL`, `INFO clients`).
    clients: Arc<ClientRegistry>,
    /// Port the RESP server listens on, for `INFO`.
    tcp_port: std::sync::atomic::AtomicU32,
    /// `maxmemory` in bytes (0 = no limit), `maxmemory-policy` and
    /// `maxmemory-samples` (see [`CommandEngine::free_memory`]).
    maxmemory: AtomicU64,
    maxmemory_policy: std::sync::atomic::AtomicU8,
    maxmemory_samples: std::sync::atomic::AtomicUsize,
}

/// The `maxmemory-policy` values, in the order `maxmemory_policy` stores
/// them.
const POLICIES: [EvictionPolicy; 8] = [
    EvictionPolicy::NoEviction,
    EvictionPolicy::AllKeysLru,
    EvictionPolicy::VolatileLru,
    EvictionPolicy::AllKeysLfu,
    EvictionPolicy::VolatileLfu,
    EvictionPolicy::AllKeysRandom,
    EvictionPolicy::VolatileRandom,
    EvictionPolicy::VolatileTtl,
];

/// Reply to a write refused for memory (Redis' wording).
pub const OOM_ERROR: &str = "OOM command not allowed when used memory > 'maxmemory'.";

/// Default for [`CommandEngine::with_lua_memory_limit`]: 64 MB.
pub const DEFAULT_LUA_MEMORY_LIMIT: usize = 64 * 1024 * 1024;

impl CommandEngine {
    pub fn new(shards: Arc<ShardManager>, pubsub: Arc<PubSubBroker>) -> Self {
        Self {
            shards,
            pubsub,
            start_time: Instant::now(),
            script_cache: ScriptCache::new(),
            weak_self: OnceLock::new(),
            notify_flags: Arc::new(AtomicU16::new(0)),
            replication_offset: Arc::new(AtomicU64::new(0)),
            persistence: Arc::new(Persistence::new(PersistConfig::default())),
            shutdown_requested: Arc::new(tokio::sync::Notify::new()),
            lua_memory_limit: DEFAULT_LUA_MEMORY_LIMIT,
            databases: DEFAULT_DATABASES,
            clients: Arc::new(ClientRegistry::default()),
            tcp_port: std::sync::atomic::AtomicU32::new(6379),
            maxmemory: AtomicU64::new(0),
            maxmemory_policy: std::sync::atomic::AtomicU8::new(0),
            maxmemory_samples: std::sync::atomic::AtomicUsize::new(5),
        }
    }

    /// Cap memory at `bytes` (0 = none), evicting by `policy`.
    pub fn with_maxmemory(self, bytes: u64, policy: EvictionPolicy, samples: usize) -> Self {
        self.set_maxmemory(bytes);
        self.set_maxmemory_policy(policy);
        self.maxmemory_samples
            .store(samples.max(1), Ordering::Relaxed);
        self
    }

    pub fn set_maxmemory(&self, bytes: u64) {
        self.maxmemory.store(bytes, Ordering::Relaxed);
    }

    pub fn set_maxmemory_policy(&self, policy: EvictionPolicy) {
        let i = POLICIES.iter().position(|p| *p == policy).unwrap_or(0);
        self.maxmemory_policy.store(i as u8, Ordering::Relaxed);
        solikv_core::access::set_lfu(policy.is_lfu());
    }

    pub fn maxmemory_policy(&self) -> EvictionPolicy {
        POLICIES[self.maxmemory_policy.load(Ordering::Relaxed) as usize % POLICIES.len()]
    }

    /// Before a command that can grow the dataset: if memory is over
    /// `maxmemory`, evict keys by the policy until it is not (Redis'
    /// `performEvictions`). Err(OOM) when the policy forbids evicting or
    /// nothing evictable is left. Shards are visited in turn, each sampling
    /// `maxmemory-samples` keys and dropping the best candidate.
    pub fn free_memory(&self) -> Result<(), CommandResponse> {
        let limit = self.maxmemory.load(Ordering::Relaxed);
        if limit == 0 || !solikv_core::memory::is_enabled() {
            return Ok(());
        }
        if (solikv_core::memory::used() as u64) <= limit {
            return Ok(());
        }
        let policy = self.maxmemory_policy();
        if policy == EvictionPolicy::NoEviction {
            return Err(CommandResponse::error(OOM_ERROR));
        }
        let samples = self.maxmemory_samples.load(Ordering::Relaxed);
        let shards = self.shards.all_shards();
        let n = shards.len();
        let start = (now_millis() as usize) % n;
        // Bounded: a command never pays for more than this many evictions.
        const MAX_EVICTIONS: usize = 4096;
        let mut empty_in_a_row = 0;
        for i in 0..MAX_EVICTIONS {
            match shards[(start + i) % n].evict_one(policy, samples) {
                Some(_) => {
                    empty_in_a_row = 0;
                    self.shards.note_evicted(1);
                    if (solikv_core::memory::used() as u64) <= limit {
                        return Ok(());
                    }
                }
                None => {
                    empty_in_a_row += 1;
                    if empty_in_a_row >= n {
                        break;
                    }
                }
            }
        }
        if (solikv_core::memory::used() as u64) <= limit {
            Ok(())
        } else {
            Err(CommandResponse::error(OOM_ERROR))
        }
    }

    /// Number of logical databases (at least 1).
    pub fn with_databases(mut self, n: u32) -> Self {
        self.databases = n.max(1);
        self
    }

    pub fn databases(&self) -> u32 {
        self.databases
    }

    /// The RESP port, for `INFO server`.
    pub fn set_tcp_port(&self, port: u16) {
        self.tcp_port.store(port as u32, Ordering::Relaxed);
    }

    /// Connected clients.
    pub fn clients(&self) -> &Arc<ClientRegistry> {
        &self.clients
    }

    /// State for a new connection from `addr` to `laddr`.
    pub fn new_session(&self, addr: String, laddr: String) -> Session {
        let info = self.clients.register(addr, laddr);
        Session::new(info, self.clients.clone())
    }

    /// Cap the memory each Lua VM may use (bytes).
    pub fn with_lua_memory_limit(mut self, bytes: usize) -> Self {
        self.lua_memory_limit = bytes;
        self
    }

    pub fn with_notify_flags(mut self, flags: Arc<AtomicU16>) -> Self {
        self.notify_flags = flags;
        self
    }

    pub fn notify_flags(&self) -> &Arc<AtomicU16> {
        &self.notify_flags
    }

    /// Store a weak self-reference so dispatch() can obtain Arc<Self> for Lua closures.
    pub fn init_self_ref(&self, weak: Weak<CommandEngine>) {
        let _ = self.weak_self.set(weak);
    }

    fn get_self_arc(&self) -> Option<Arc<CommandEngine>> {
        self.weak_self.get().and_then(|w| w.upgrade())
    }

    /// Log every change to `aof` from now on (tests; the server attaches
    /// its writer through [`load_persisted`](Self::load_persisted)).
    pub fn with_aof(self, aof: AofWriter) -> Self {
        self.shards.attach_aof(aof);
        self
    }

    /// Point `SAVE`/`BGSAVE` at `dir` and `basename` (`{dir}/{basename}.rdb`)
    /// instead of the `data`/`dump` defaults, with no append-only log.
    pub fn with_rdb_path(self, dir: impl Into<PathBuf>, basename: impl Into<String>) -> Self {
        self.with_persistence(Arc::new(Persistence::new(PersistConfig {
            dir: dir.into(),
            dbfilename: basename.into(),
            ..PersistConfig::default()
        })))
    }

    /// Use the server's persistence configuration.
    pub fn with_persistence(mut self, persistence: Arc<Persistence>) -> Self {
        self.persistence = persistence;
        self
    }

    pub fn persistence(&self) -> &Arc<Persistence> {
        &self.persistence
    }

    /// Resolves once a `SHUTDOWN` command has saved and the server should exit.
    pub fn shutdown_requested(&self) -> Arc<tokio::sync::Notify> {
        self.shutdown_requested.clone()
    }

    /// Writes counted since startup (for `rdb_changes_since_last_save`).
    pub fn dirty(&self) -> u64 {
        self.replication_offset.load(Ordering::Relaxed)
    }

    /// Stop accepting changes, make them durable, and snapshot if configured
    /// (see [`Persistence::shutdown`]).
    pub fn shutdown(&self, save: Option<bool>) -> std::io::Result<()> {
        self.persistence.shutdown(&self.shards, self.dirty(), save)
    }

    /// Bracket a `MULTI`/`EXEC` batch so the log applies it whole or not at
    /// all. Keep it alive across the batch's `execute` calls, with no
    /// `.await` in between (the bracket is per thread).
    pub fn aof_transaction(&self) -> crate::aof_log::TxGuard {
        crate::aof_log::TxGuard::begin(&self.shards)
    }

    /// Execute a command in database `db` (no connection state: replay,
    /// tools, tests).
    pub fn execute_in_db(&self, db: u32, name: &str, args: &[Bytes]) -> CommandResponse {
        let _db = DbGuard::enter(db);
        self.execute(name, args)
    }

    /// Execute a parsed command. Returns the response.
    ///
    /// Runs in the current thread's database (0, unless an enclosing
    /// command — a connection's `execute_session`, a script — set another).
    pub fn execute(&self, name: &str, args: &[Bytes]) -> CommandResponse {
        DEPTH.with(|d| d.set(d.get() + 1));
        struct Depth;
        impl Drop for Depth {
            fn drop(&mut self) {
                DEPTH.with(|d| d.set(d.get() - 1));
            }
        }
        let _depth = Depth;
        // One hash lookup resolves the name: its arm, and its flags.
        let spec = crate::commands::lookup(name);
        let id = spec.map_or(Cmd::Unknown, |s| s.id);
        let write = spec.is_some_and(|s| s.is_write()) || matches!(id, Cmd::EVAL | Cmd::EVALSHA);

        if self.maxmemory.load(Ordering::Relaxed) != 0
            && spec.is_some_and(|c| c.has(crate::commands::F_DENYOOM))
        {
            if let Err(oom) = self.free_memory() {
                return oom;
            }
        }

        // With someone watching keys, tell the shards what this write
        // changes; they mark it under their lock (see `watch`).
        let _touch = (write && self.shards.watch().is_active())
            .then(|| TouchGuard::set(touch_for(name, args)));

        // With the log on, tell the shards what this command logs as; they
        // log it under their lock (see `aof_log`). The frame is set for every
        // command so a read nested in a script never inherits a write's.
        let response = if self.shards.aof().is_some() {
            let mode = crate::aof_log::mode(name, write);
            let cmd = crate::aof_log::AutoCmd::new(name, args, mode);
            let _frame = crate::aof_log::FrameGuard::enter(mode, &cmd);
            self.dispatch(id, name, args)
        } else {
            self.dispatch(id, name, args)
        };

        if response.is_error() {
            return response;
        }

        if write {
            self.replication_offset.fetch_add(1, Ordering::Relaxed);
        }

        // Keyspace notifications (zero-cost when flags == 0).
        self.emit_notifications(name, args, &response);

        response
    }

    /// Emit a single keyspace event notification.
    fn notify_keyspace_event(&self, event_flag: u16, event: &str, key: &Bytes) {
        let flags = self.notify_flags.load(Ordering::Relaxed);
        if flags == 0 {
            return; // zero-cost bail when disabled
        }
        if flags & event_flag == 0 {
            return; // this event type not enabled
        }
        let key_str = String::from_utf8_lossy(key);
        let db = current_db();

        // __keyspace@<db>__:<key> → event name as message
        if flags & NOTIFY_KEYSPACE != 0 {
            let channel = Bytes::from(format!("__keyspace@{}__:{}", db, key_str));
            self.pubsub.publish(channel, Bytes::from(event.to_string()));
        }

        // __keyevent@<db>__:<event> → key name as message
        if flags & NOTIFY_KEYEVENT != 0 {
            let channel = Bytes::from(format!("__keyevent@{}__:{}", db, event));
            self.pubsub.publish(channel, key.clone());
        }
    }

    /// Map a completed command to its notification event(s) and emit them.
    fn emit_notifications(&self, name: &str, args: &[Bytes], _response: &CommandResponse) {
        let flags = self.notify_flags.load(Ordering::Relaxed);
        if flags == 0 {
            return;
        }

        match name {
            // String commands → $ flag
            "SET" | "SETNX" | "SETEX" | "PSETEX" | "GETSET" | "GETDEL" | "GETEX" | "SETRANGE"
            | "APPEND" => {
                if let Some(key) = args.first() {
                    let event = match name {
                        "GETDEL" => "getdel",
                        "GETEX" => "getex",
                        _ => "set",
                    };
                    self.notify_keyspace_event(NOTIFY_STRING, event, key);
                }
            }
            "MSET" | "MSETNX" => {
                // Emit "set" for each key
                for pair in args.chunks(2) {
                    if let Some(key) = pair.first() {
                        self.notify_keyspace_event(NOTIFY_STRING, "set", key);
                    }
                }
            }
            "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" => {
                if let Some(key) = args.first() {
                    let event = match name {
                        "INCR" | "INCRBY" => "incrby",
                        "DECR" | "DECRBY" => "decrby",
                        "INCRBYFLOAT" => "incrbyfloat",
                        _ => "set",
                    };
                    self.notify_keyspace_event(NOTIFY_STRING, event, key);
                }
            }

            // Generic key commands → g flag
            "DEL" | "UNLINK" => {
                for key in args {
                    self.notify_keyspace_event(NOTIFY_GENERIC, "del", key);
                }
            }
            "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
                if !matches!(_response, CommandResponse::Integer(1)) {
                    return;
                }
                if let Some(key) = args.first() {
                    // A deadline already passed deleted the key.
                    let amount = args.get(1).and_then(|a| parse_i64_arg(a)).unwrap_or(0);
                    let past = match name {
                        "EXPIRE" | "PEXPIRE" => amount <= 0,
                        "EXPIREAT" => amount.saturating_mul(1000) <= now_millis() as i64,
                        _ => amount <= now_millis() as i64,
                    };
                    let event = if past { "del" } else { "expire" };
                    self.notify_keyspace_event(NOTIFY_GENERIC, event, key);
                }
            }
            "PERSIST" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_GENERIC, "persist", key);
                }
            }
            "RENAME" | "RENAMENX" if args.len() >= 2 => {
                self.notify_keyspace_event(NOTIFY_GENERIC, "rename_from", &args[0]);
                self.notify_keyspace_event(NOTIFY_GENERIC, "rename_to", &args[1]);
            }

            // List commands → l flag
            "LPUSH" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_LIST, "lpush", key);
                }
            }
            "RPUSH" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_LIST, "rpush", key);
                }
            }
            "LPOP" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_LIST, "lpop", key);
                }
            }
            "RPOP" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_LIST, "rpop", key);
                }
            }
            "LINSERT" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_LIST, "linsert", key);
                }
            }
            "LSET" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_LIST, "lset", key);
                }
            }
            "LTRIM" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_LIST, "ltrim", key);
                }
            }
            "LREM" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_LIST, "lrem", key);
                }
            }

            // Set commands → s flag
            "SADD" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_SET, "sadd", key);
                }
            }
            "SREM" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_SET, "srem", key);
                }
            }
            "SPOP" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_SET, "spop", key);
                }
            }
            "SMOVE" if args.len() >= 2 => {
                self.notify_keyspace_event(NOTIFY_SET, "srem", &args[0]);
                self.notify_keyspace_event(NOTIFY_SET, "sadd", &args[1]);
            }

            // Hash commands → h flag
            "HSET" | "HMSET" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_HASH, "hset", key);
                }
            }
            "HDEL" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_HASH, "hdel", key);
                }
            }
            "HINCRBY" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_HASH, "hincrby", key);
                }
            }
            "HINCRBYFLOAT" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_HASH, "hincrbyfloat", key);
                }
            }
            "HSETNX" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_HASH, "hsetnx", key);
                }
            }

            // Sorted set commands → z flag
            "ZADD" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_ZSET, "zadd", key);
                }
            }
            "ZREM" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_ZSET, "zrem", key);
                }
            }
            "ZINCRBY" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_ZSET, "zincrby", key);
                }
            }
            "ZPOPMIN" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_ZSET, "zpopmin", key);
                }
            }
            "ZPOPMAX" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_ZSET, "zpopmax", key);
                }
            }
            "ZUNIONSTORE" | "ZINTERSTORE" => {
                if let Some(key) = args.first() {
                    let event = if name == "ZUNIONSTORE" {
                        "zunionstore"
                    } else {
                        "zinterstore"
                    };
                    self.notify_keyspace_event(NOTIFY_ZSET, event, key);
                }
            }
            "ZREMRANGEBYRANK" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_ZSET, "zremrangebyrank", key);
                }
            }
            "ZREMRANGEBYSCORE" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_ZSET, "zremrangebyscore", key);
                }
            }

            // Stream commands → t flag
            "XADD" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_STREAM, "xadd", key);
                }
            }
            "XDEL" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_STREAM, "xdel", key);
                }
            }
            "XTRIM" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_STREAM, "xtrim", key);
                }
            }
            // The key is the second argument (after CREATE/DESTROY/etc.)
            "XGROUP" if args.len() >= 2 => {
                self.notify_keyspace_event(NOTIFY_STREAM, "xgroup-create", &args[1]);
            }

            // Bitmap commands → $ flag
            "SETBIT" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_STRING, "setbit", key);
                }
            }
            "BITOP" if args.len() >= 2 => {
                self.notify_keyspace_event(NOTIFY_STRING, "bitop", &args[1]);
            }

            // HyperLogLog commands → $ flag
            "PFADD" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_STRING, "pfadd", key);
                }
            }
            "PFMERGE" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_STRING, "pfmerge", key);
                }
            }

            // Bloom filter commands → $ flag
            "BF.ADD" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_STRING, "bf.add", key);
                }
            }
            "BF.MADD" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_STRING, "bf.madd", key);
                }
            }
            "BF.RESERVE" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_STRING, "bf.reserve", key);
                }
            }

            // Geospatial commands → z flag (stored as zsets)
            "GEOADD" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_ZSET, "geoadd", key);
                }
            }
            "GEOSEARCHSTORE" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_ZSET, "geosearchstore", key);
                }
            }

            // FLUSHDB/FLUSHALL → g flag (no specific key)
            // Redis does not emit per-key notifications for FLUSH
            _ => {}
        }
    }

    fn dispatch(&self, id: Cmd, name: &str, args: &[Bytes]) -> CommandResponse {
        match id {
            // ---- Server commands ----
            Cmd::PING => {
                if args.is_empty() {
                    CommandResponse::simple("PONG")
                } else {
                    CommandResponse::bulk(args[0].clone())
                }
            }
            Cmd::ECHO => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("echo");
                }
                CommandResponse::bulk(args[0].clone())
            }
            Cmd::COMMAND => self.command_command(args),
            Cmd::CONFIG => self.config_command(args),
            Cmd::INFO => self.info(args),
            Cmd::DBSIZE => {
                if !args.is_empty() {
                    return CommandResponse::wrong_arity("dbsize");
                }
                // Sum across all shards (each counts the current database).
                let mut total = 0i64;
                for shard in self.shards.all_shards() {
                    let resp =
                        shard.execute(|store| CommandResponse::integer(store.dbsize() as i64));
                    if let CommandResponse::Integer(n) = resp {
                        total += n;
                    }
                }
                CommandResponse::integer(total)
            }
            Cmd::FLUSHDB | Cmd::FLUSHALL => {
                match args {
                    [] => {}
                    [mode]
                        if mode.eq_ignore_ascii_case(b"ASYNC")
                            || mode.eq_ignore_ascii_case(b"SYNC") => {}
                    _ => return CommandResponse::syntax_error(),
                }
                let all = name == "FLUSHALL";
                // Heavy: release the async worker while we lock every shard,
                // and hold them all so no client sees a half-flushed server.
                self.maybe_block_in_place(|| {
                    let _held = self.shards.lock_all();
                    for shard in self.shards.all_shards() {
                        shard.execute_with_effects(|store, fx| {
                            // Replays per shard: each old shard is replayed
                            // into its own scratch store.
                            if all {
                                store.flush_all();
                                fx.push("FLUSHALL", &[]);
                            } else {
                                store.flush();
                                fx.push("FLUSHDB", &[]);
                            }
                            CommandResponse::ok()
                        });
                    }
                    CommandResponse::ok()
                })
            }
            Cmd::SWAPDB => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("swapdb");
                }
                let (a, b) = match (self.parse_db(&args[0]), self.parse_db(&args[1])) {
                    (Ok(a), Ok(b)) => (a, b),
                    _ => return CommandResponse::error("ERR invalid first/second DB index"),
                };
                let _held = self.shards.lock_all();
                let (sa, sb) = (a.to_string(), b.to_string());
                for shard in self.shards.all_shards() {
                    shard.execute_with_effects(|store, fx| {
                        store.swap_db(a, b);
                        fx.push("SWAPDB", &[sa.as_bytes(), sb.as_bytes()]);
                        CommandResponse::ok()
                    });
                }
                // Every key of both databases changed, as far as WATCH goes.
                let watch = self.shards.watch();
                if watch.is_active() {
                    watch.touch_db(Some(a));
                    watch.touch_db(Some(b));
                }
                CommandResponse::ok()
            }
            Cmd::SELECT => {
                // A connection's SELECT is handled by `execute_session`. Here
                // only a script's: it switches the database of the script's
                // later calls, and the caller's is restored after it.
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("select");
                }
                let db = match self.parse_db(&args[0]) {
                    Ok(db) => db,
                    Err(e) => return e,
                };
                if DEPTH.with(|d| d.get()) <= 1 {
                    return CommandResponse::error(
                        "ERR SELECT is only supported on a RESP connection or inside a script",
                    );
                }
                crate::shard::set_current_db(db);
                CommandResponse::ok()
            }
            Cmd::QUIT => CommandResponse::ok(),
            Cmd::RESET => CommandResponse::simple("RESET"),
            Cmd::CLIENT | Cmd::MULTI | Cmd::EXEC | Cmd::DISCARD | Cmd::WATCH | Cmd::UNWATCH => {
                CommandResponse::error(format!(
                    "ERR {} is only available on a RESP connection",
                    name
                ))
            }

            // ---- Replication commands ----
            Cmd::REPLICAOF | Cmd::SLAVEOF => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity(&name.to_lowercase());
                }
                // Already a master: `NO ONE` is true as it stands. Anything
                // else used to answer OK and replicate nothing.
                if args[0].eq_ignore_ascii_case(b"NO") && args[1].eq_ignore_ascii_case(b"ONE") {
                    return CommandResponse::ok();
                }
                CommandResponse::error("ERR replication is not supported by this server yet")
            }
            Cmd::ROLE => CommandResponse::array(vec![
                CommandResponse::bulk_string("master"),
                CommandResponse::integer(self.replication_offset.load(Ordering::Relaxed) as i64),
                CommandResponse::array(vec![]),
            ]),

            // ---- String commands ----
            Cmd::GET => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("get");
                }
                // No key clone: borrow into the shard lock.
                let key = &args[0];
                self.shards
                    .shard_for_key(key)
                    .execute(|store| store.string_get(key))
            }
            Cmd::SET => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("set");
                }
                // Hot path: plain SET key value (no options) — skip option scan.
                if args.len() == 2 {
                    let key = args[0].clone();
                    let value = args[1].clone();
                    return self.shards.shard_for_key(&key).execute(move |store| {
                        store.string_set(key, value, None, false, false, false)
                    });
                }
                let key = args[0].clone();
                let value = args[1].clone();
                let opts = match parse_set_options(&args[2..]) {
                    Ok(o) => o,
                    Err(e) => return e,
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_set_opts(key, value, opts))
            }
            Cmd::SETNX => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("setnx");
                }
                let key = args[0].clone();
                let value = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_setnx(key, value))
            }
            Cmd::SETEX => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("setex");
                }
                let key = args[0].clone();
                let seconds = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(s) => s,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                if seconds == 0 || ttl_ms(seconds, true).is_none() {
                    return invalid_expire("setex");
                }
                let value = args[2].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_setex(key, seconds, value))
            }
            Cmd::PSETEX => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("psetex");
                }
                let key = args[0].clone();
                let millis = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(m) => m,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                if millis == 0 || ttl_ms(millis, false).is_none() {
                    return invalid_expire("psetex");
                }
                let value = args[2].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_psetex(key, millis, value))
            }
            Cmd::GETSET => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("getset");
                }
                let key = args[0].clone();
                let value = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_getset(key, value))
            }
            Cmd::GETDEL => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("getdel");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_getdel(&key))
            }
            Cmd::GETEX => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("getex");
                }
                let key = args[0].clone();
                let mut expire_at = None;
                let mut persist = false;
                let mut i = 1;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
                    match opt.as_str() {
                        "EX" | "PX" | "EXAT" | "PXAT" if expire_at.is_none() && !persist => {
                            i += 1;
                            let Some(amount) = args.get(i).and_then(|b| parse_i64_arg(b)) else {
                                return if i >= args.len() {
                                    CommandResponse::syntax_error()
                                } else {
                                    CommandResponse::error(
                                        "ERR value is not an integer or out of range",
                                    )
                                };
                            };
                            match absolute_expiry(&opt, amount) {
                                Some(at) => expire_at = Some(at),
                                None => return invalid_expire("getex"),
                            }
                        }
                        "PERSIST" if expire_at.is_none() && !persist => persist = true,
                        _ => return CommandResponse::syntax_error(),
                    }
                    i += 1;
                }
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_getex_at(&key, expire_at, persist))
            }
            Cmd::MGET => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("mget");
                }
                multi_shard_mget(&self.shards, args)
            }
            Cmd::MSET => {
                if args.len() < 2 || !args.len().is_multiple_of(2) {
                    return CommandResponse::wrong_arity("mset");
                }
                let pairs: Vec<(Bytes, Bytes)> = args
                    .chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                multi_shard_mset(&self.shards, pairs)
            }
            Cmd::MSETNX => {
                if args.len() < 2 || !args.len().is_multiple_of(2) {
                    return CommandResponse::wrong_arity("msetnx");
                }
                let pairs: Vec<(Bytes, Bytes)> = args
                    .chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                multi_shard_msetnx(&self.shards, pairs)
            }
            Cmd::INCR => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("incr");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_incr(&key))
            }
            Cmd::DECR => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("decr");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_decr(&key))
            }
            Cmd::INCRBY => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("incrby");
                }
                let key = args[0].clone();
                let delta = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(d) => d,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_incrby(&key, delta))
            }
            Cmd::DECRBY => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("decrby");
                }
                let key = args[0].clone();
                let delta = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(d) => d,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_decrby(&key, delta))
            }
            Cmd::INCRBYFLOAT => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("incrbyfloat");
                }
                let key = args[0].clone();
                let delta = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    Some(d) => d,
                    None => return CommandResponse::error("ERR value is not a valid float"),
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_incrbyfloat(&key, delta))
            }
            Cmd::APPEND => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("append");
                }
                let key = args[0].clone();
                let value = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_append(&key, &value))
            }
            Cmd::STRLEN => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("strlen");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_strlen(&key))
            }
            Cmd::GETRANGE | Cmd::SUBSTR => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("getrange");
                }
                let key = args[0].clone();
                let start = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(s) => s,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                let end = match std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(e) => e,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_getrange(&key, start, end))
            }
            Cmd::SETRANGE => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("setrange");
                }
                let key = args[0].clone();
                let offset = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    Some(o) => o,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                let value = args[2].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_setrange(&key, offset, &value))
            }

            // ---- Generic key commands ----
            Cmd::DEL | Cmd::UNLINK => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity(&name.to_lowercase());
                }
                let unlink = name == "UNLINK";
                let groups = group_by_shard(&self.shards, args);
                // Several shards: hold them all, so no client sees some keys
                // deleted and others not.
                let _held = (groups.len() > 1)
                    .then(|| self.shards.lock_shards(groups.iter().map(|(i, _)| *i)));
                let mut total = 0i64;
                let mut freed: Vec<solikv_core::KeyEntry> = Vec::new();
                for (idx, keys) in &groups {
                    let resp = self.shards.shard(*idx).execute_with_effects(|store, fx| {
                        let mut n = 0i64;
                        let mut gone: Vec<&[u8]> = Vec::new();
                        for k in keys {
                            if let Some(entry) = store.remove_live(k) {
                                n += 1;
                                gone.push(k);
                                if unlink {
                                    freed.push(entry);
                                }
                            }
                        }
                        if !gone.is_empty() {
                            fx.push("DEL", &gone);
                        }
                        CommandResponse::integer(n)
                    });
                    if let CommandResponse::Integer(n) = resp {
                        total += n;
                    }
                }
                drop(_held);
                // UNLINK: values are freed after the locks are released, and
                // big ones on another thread, so a huge collection does not
                // stall the shard (or this client) while it is dropped.
                lazy_free(freed);
                CommandResponse::integer(total)
            }
            Cmd::EXISTS | Cmd::TOUCH => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity(&name.to_lowercase());
                }
                let mut total = 0i64;
                for (idx, keys) in group_by_shard(&self.shards, args) {
                    let resp = self.shards.shard(idx).execute(|store| {
                        CommandResponse::integer(
                            keys.iter().filter(|k| store.exists(k)).count() as i64
                        )
                    });
                    if let CommandResponse::Integer(n) = resp {
                        total += n;
                    }
                }
                CommandResponse::integer(total)
            }
            Cmd::MOVE => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("move");
                }
                let to = match self.parse_db(&args[1]) {
                    Ok(db) => db,
                    Err(e) => return e,
                };
                let from = current_db();
                if to == from {
                    return CommandResponse::error(
                        "ERR source and destination objects are the same",
                    );
                }
                let key = args[0].clone();
                let watch = self.shards.watch();
                self.shards.shard_for_key(&key).execute(|store| {
                    let moved = store.move_key(&key, to);
                    if moved && watch.is_active() {
                        watch.touch(from, &key);
                        watch.touch(to, &key);
                    }
                    CommandResponse::integer(moved as i64)
                })
            }
            Cmd::EXPIRE | Cmd::PEXPIRE | Cmd::EXPIREAT | Cmd::PEXPIREAT => {
                let lname = name.to_lowercase();
                if args.len() < 2 {
                    return CommandResponse::wrong_arity(&lname);
                }
                let key = args[0].clone();
                let Some(amount) = parse_i64_arg(&args[1]) else {
                    return CommandResponse::error("ERR value is not an integer or out of range");
                };
                let mut cond = solikv_core::store::ExpireCond::default();
                for opt in &args[2..] {
                    match String::from_utf8_lossy(opt).to_ascii_uppercase().as_str() {
                        "NX" => cond.nx = true,
                        "XX" => cond.xx = true,
                        "GT" => cond.gt = true,
                        "LT" => cond.lt = true,
                        other => {
                            return CommandResponse::error(format!(
                                "ERR Unsupported option {}",
                                other
                            ))
                        }
                    }
                }
                if cond.nx && (cond.xx || cond.gt || cond.lt) {
                    return CommandResponse::error(
                        "ERR NX and XX, GT or LT options at the same time are not compatible",
                    );
                }
                if cond.gt && cond.lt {
                    return CommandResponse::error(
                        "ERR GT and LT options at the same time are not compatible",
                    );
                }
                // Absolute deadline in ms; anything at or before now deletes.
                let at: i128 = match name {
                    "EXPIRE" => now_millis() as i128 + amount as i128 * 1000,
                    "PEXPIRE" => now_millis() as i128 + amount as i128,
                    "EXPIREAT" => amount as i128 * 1000,
                    _ => amount as i128,
                };
                if at > (i64::MAX / 2) as i128 {
                    return invalid_expire(&lname);
                }
                let at = at.max(0) as u64;
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(store.expire_cond(&key, at, cond) as i64)
                })
            }
            Cmd::EXPIRETIME | Cmd::PEXPIRETIME => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity(&name.to_lowercase());
                }
                let key = args[0].clone();
                let ms = name == "PEXPIRETIME";
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(match store.get(&key) {
                        None => -2,
                        Some(e) => match e.expires_at {
                            None => -1,
                            Some(at) if ms => at as i64,
                            Some(at) => (at / 1000) as i64,
                        },
                    })
                })
            }
            Cmd::TTL => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("ttl");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| CommandResponse::integer(store.ttl(&key)))
            }
            Cmd::PTTL => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("pttl");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| CommandResponse::integer(store.pttl(&key)))
            }
            Cmd::PERSIST => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("persist");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(if store.persist(&key) { 1 } else { 0 })
                })
            }
            Cmd::TYPE => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("type");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| CommandResponse::simple(store.key_type(&key)))
            }
            Cmd::RENAME | Cmd::RENAMENX => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity(&name.to_lowercase());
                }
                let nx = name == "RENAMENX";
                let from = args[0].clone();
                let to = args[1].clone();
                let fs = self.shards.shard_index_for_key(&from);
                let ts = self.shards.shard_index_for_key(&to);
                if fs == ts {
                    // One shard: logged as itself.
                    return self.shards.shard(fs).execute(move |store| {
                        if store.get(&from).is_none() {
                            return CommandResponse::error("ERR no such key");
                        }
                        // (With `from == to`, the destination exists.)
                        if nx && store.exists(&to) {
                            return CommandResponse::integer(0);
                        }
                        match store.rename(&from, to) {
                            Ok(()) if nx => CommandResponse::integer(1),
                            Ok(()) => CommandResponse::ok(),
                            Err(e) => CommandResponse::error(e),
                        }
                    });
                }
                // Two shards, held together: the key leaves one and arrives
                // in the other in one step. Logged as the effect on each:
                // a DEL on the source's log, the value (TTL included) on the
                // destination's. The checks push no record, so log nothing.
                let _held = self.shards.lock_shards([fs, ts]);
                let src_live = matches!(
                    self.shards.shard(fs).execute_with_effects(|s, _| {
                        CommandResponse::integer(s.get(&from).is_some() as i64)
                    }),
                    CommandResponse::Integer(1)
                );
                if !src_live {
                    return CommandResponse::error("ERR no such key");
                }
                if nx {
                    let dest = self.shards.shard(ts).execute_with_effects(|s, _| {
                        CommandResponse::integer(s.get(&to).is_some() as i64)
                    });
                    if matches!(dest, CommandResponse::Integer(1)) {
                        return CommandResponse::integer(0);
                    }
                }
                let mut taken = None;
                self.shards.shard(fs).execute_with_effects(|s, fx| {
                    taken = s.remove_live(&from);
                    if taken.is_some() {
                        fx.push("DEL", &[&from]);
                    }
                    CommandResponse::ok()
                });
                let Some(entry) = taken else {
                    return CommandResponse::error("ERR no such key");
                };
                self.shards.shard(ts).execute_with_effects(|s, fx| {
                    s.insert_entry(to.clone(), entry);
                    fx.put_key(s, &to);
                    CommandResponse::ok()
                });
                if nx {
                    CommandResponse::integer(1)
                } else {
                    CommandResponse::ok()
                }
            }
            Cmd::KEYS => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("keys");
                }
                let pattern = std::str::from_utf8(&args[0]).unwrap_or("*").to_string();
                // Heavy full-space walk: don't pin the Tokio worker.
                self.maybe_block_in_place(|| {
                    let shards = self.shards.clone();
                    let num = shards.num_shards();
                    let mut all_keys = Vec::new();
                    for i in 0..num {
                        let shard = shards.shard(i).clone();
                        let p = pattern.clone();
                        let resp = shard.execute(move |store| {
                            let keys = store.keys(&p);
                            CommandResponse::array(
                                keys.into_iter().map(CommandResponse::bulk).collect(),
                            )
                        });
                        if let CommandResponse::Array(items) = resp {
                            all_keys.extend(items);
                        }
                    }
                    CommandResponse::array(all_keys)
                })
            }
            Cmd::SCAN => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("scan");
                }
                let Some(cursor) = std::str::from_utf8(&args[0])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                else {
                    return CommandResponse::error("ERR invalid cursor");
                };
                let (pattern, count, type_filter) = match parse_scan_options(&args[1..], true) {
                    Ok(o) => o,
                    Err(e) => return e,
                };
                multi_shard_scan(&self.shards, cursor, pattern, count, type_filter)
            }
            Cmd::RANDOMKEY => multi_shard_randomkey(&self.shards),
            Cmd::OBJECT => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("object");
                }
                let sub = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                match sub.as_str() {
                    "ENCODING" => {
                        if args.len() < 2 {
                            return CommandResponse::wrong_arity("object|encoding");
                        }
                        let key = args[1].clone();
                        self.shards.shard_for_key(&key).execute(move |store| {
                            match store.key_type(&key) {
                                "string" => CommandResponse::bulk_string("embstr"),
                                "list" => CommandResponse::bulk_string("listpack"),
                                "hash" => CommandResponse::bulk_string("listpack"),
                                "set" => CommandResponse::bulk_string("listpack"),
                                "zset" => CommandResponse::bulk_string("skiplist"),
                                "stream" => CommandResponse::bulk_string("stream"),
                                _ => CommandResponse::nil(),
                            }
                        })
                    }
                    "IDLETIME" | "FREQ" | "REFCOUNT" => {
                        if args.len() != 2 {
                            return CommandResponse::wrong_arity(&format!(
                                "object|{}",
                                sub.to_lowercase()
                            ));
                        }
                        let lfu = self.maxmemory_policy().is_lfu();
                        if sub == "IDLETIME" && lfu {
                            return CommandResponse::error(
                                "ERR An LFU maxmemory policy is selected, idle time not tracked. \
                                 Please note that when switching between policies at runtime LRU \
                                 and LFU data will take some time to adjust.",
                            );
                        }
                        if sub == "FREQ" && !lfu {
                            return CommandResponse::error(
                                "ERR An LFU maxmemory policy is not selected, access frequency \
                                 not tracked. Please note that when switching between policies at \
                                 runtime LRU and LFU data will take some time to adjust.",
                            );
                        }
                        let key = args[1].clone();
                        let sub2 = sub.clone();
                        self.shards.shard_for_key(&key).execute(move |store| {
                            match store.access_of(&key) {
                                None => CommandResponse::nil(),
                                Some(a) => CommandResponse::integer(match sub2.as_str() {
                                    "IDLETIME" => solikv_core::access::idle_secs(a) as i64,
                                    "FREQ" => solikv_core::access::decayed(a) as i64,
                                    _ => 1,
                                }),
                            }
                        })
                    }
                    "HELP" => CommandResponse::array(
                        [
                            "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                            "ENCODING <key>",
                            "FREQ <key>",
                            "IDLETIME <key>",
                            "REFCOUNT <key>",
                        ]
                        .iter()
                        .map(|l| CommandResponse::simple(l))
                        .collect(),
                    ),
                    _ => CommandResponse::error("ERR Unknown subcommand"),
                }
            }
            Cmd::DUMP | Cmd::RESTORE => CommandResponse::error("ERR not implemented"),
            Cmd::WAIT => CommandResponse::integer(0),

            // ---- List commands ----
            Cmd::LPUSH => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("lpush");
                }
                let key = args[0].clone();
                let values: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_lpush(&key, values))
            }
            Cmd::RPUSH => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("rpush");
                }
                let key = args[0].clone();
                let values: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_rpush(&key, values))
            }
            Cmd::LPOP => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("lpop");
                }
                let key = args[0].clone();
                let count = args
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok())
                    .unwrap_or(1);
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_lpop(&key, count))
            }
            Cmd::RPOP => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("rpop");
                }
                let key = args[0].clone();
                let count = args
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok())
                    .unwrap_or(1);
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_rpop(&key, count))
            }
            Cmd::LLEN => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("llen");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_llen(&key))
            }
            Cmd::LINDEX => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("lindex");
                }
                let key = args[0].clone();
                let index = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(i) => i,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_lindex(&key, index))
            }
            Cmd::LSET => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("lset");
                }
                let key = args[0].clone();
                let index = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(i) => i,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                let value = args[2].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_lset(&key, index, value))
            }
            Cmd::LRANGE => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("lrange");
                }
                let key = args[0].clone();
                let start = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(s) => s,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                let stop = match std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(s) => s,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_lrange(&key, start, stop))
            }
            Cmd::LINSERT => {
                if args.len() != 4 {
                    return CommandResponse::wrong_arity("linsert");
                }
                let key = args[0].clone();
                let pos = std::str::from_utf8(&args[1]).unwrap_or("").to_uppercase();
                let before = match pos.as_str() {
                    "BEFORE" => true,
                    "AFTER" => false,
                    _ => return CommandResponse::syntax_error(),
                };
                let pivot = args[2].clone();
                let value = args[3].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_linsert(&key, before, &pivot, value))
            }
            Cmd::LREM => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("lrem");
                }
                let key = args[0].clone();
                let count = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(c) => c,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                let value = args[2].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_lrem(&key, count, &value))
            }
            Cmd::LTRIM => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("ltrim");
                }
                let key = args[0].clone();
                let start = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(s) => s,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                let stop = match std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(s) => s,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_ltrim(&key, start, stop))
            }

            // ---- Hash commands ----
            Cmd::HSET => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return CommandResponse::wrong_arity("hset");
                }
                let key = args[0].clone();
                let pairs: Vec<(Bytes, Bytes)> = args[1..]
                    .chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hset(&key, pairs))
            }
            Cmd::HMSET => {
                // HMSET is deprecated but still supported, same logic as HSET
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return CommandResponse::wrong_arity("hmset");
                }
                let key = args[0].clone();
                let pairs: Vec<(Bytes, Bytes)> = args[1..]
                    .chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hset(&key, pairs);
                    CommandResponse::ok()
                })
            }
            Cmd::HGET => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("hget");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hget(&key, &field))
            }
            Cmd::HDEL => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("hdel");
                }
                let key = args[0].clone();
                let fields: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hdel(&key, &fields))
            }
            Cmd::HGETALL => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hgetall");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hgetall(&key))
            }
            Cmd::HMGET => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("hmget");
                }
                let key = args[0].clone();
                let fields: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hmget(&key, &fields))
            }
            Cmd::HINCRBY => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("hincrby");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                let delta = match std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(d) => d,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hincrby(&key, field, delta))
            }
            Cmd::HINCRBYFLOAT => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("hincrbyfloat");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                let delta = match std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    Some(d) => d,
                    None => return CommandResponse::error("ERR value is not a valid float"),
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hincrbyfloat(&key, field, delta))
            }
            Cmd::HKEYS => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hkeys");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hkeys(&key))
            }
            Cmd::HVALS => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hvals");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hvals(&key))
            }
            Cmd::HLEN => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hlen");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hlen(&key))
            }
            Cmd::HEXISTS => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("hexists");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hexists(&key, &field))
            }
            Cmd::HSETNX => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("hsetnx");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                let value = args[2].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hsetnx(&key, field, value))
            }
            Cmd::HSCAN => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("hscan");
                }
                let key = args[0].clone();
                let Some(cursor) = std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                else {
                    return CommandResponse::error("ERR invalid cursor");
                };
                let (pattern, count, _) = match parse_scan_options(&args[2..], false) {
                    Ok(o) => o,
                    Err(e) => return e,
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hscan(&key, cursor, pattern.as_deref(), count))
            }

            // ---- Set commands ----
            Cmd::SSCAN => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("sscan");
                }
                let key = args[0].clone();
                let Some(cursor) = std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                else {
                    return CommandResponse::error("ERR invalid cursor");
                };
                let (pattern, count, _) = match parse_scan_options(&args[2..], false) {
                    Ok(o) => o,
                    Err(e) => return e,
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_sscan(&key, cursor, pattern.as_deref(), count))
            }
            Cmd::SADD => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("sadd");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_sadd(&key, members))
            }
            Cmd::SREM => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("srem");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_srem(&key, &members))
            }
            Cmd::SMEMBERS => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("smembers");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_smembers(&key))
            }
            Cmd::SISMEMBER => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("sismember");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_sismember(&key, &member))
            }
            Cmd::SCARD => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("scard");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_scard(&key))
            }
            Cmd::SINTER | Cmd::SUNION | Cmd::SDIFF => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity(&name.to_lowercase());
                }
                self.set_algebra(name, args, None)
            }
            Cmd::SINTERSTORE | Cmd::SUNIONSTORE | Cmd::SDIFFSTORE => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity(&name.to_lowercase());
                }
                let op = &name[..name.len() - "STORE".len()];
                self.set_algebra(op, &args[1..], Some(&args[0]))
            }
            Cmd::SPOP => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("spop");
                }
                let key = args[0].clone();
                let count = args
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok())
                    .unwrap_or(1);
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_spop(&key, count))
            }
            Cmd::SRANDMEMBER => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("srandmember");
                }
                let key = args[0].clone();
                let count = args
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok()?.parse::<i64>().ok());
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_srandmember(&key, count))
            }
            Cmd::SMOVE => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("smove");
                }
                let src = args[0].clone();
                let dst = args[1].clone();
                let member = args[2].clone();
                let ss = self.shards.shard_index_for_key(&src);
                let ds = self.shards.shard_index_for_key(&dst);
                if ss == ds {
                    return self
                        .shards
                        .shard(ss)
                        .execute(move |store| store.set_smove(&src, dst, member));
                }
                // Across shards: both held; logged as the SREM and the SADD.
                let _held = self.shards.lock_shards([ss, ds]);
                let dst_ok =
                    self.shards
                        .shard(ds)
                        .execute_with_effects(|s, _| match s.set_check(&dst) {
                            Err(e) => e,
                            Ok(()) => CommandResponse::ok(),
                        });
                if dst_ok.is_error() {
                    return dst_ok;
                }
                let removed = self.shards.shard(ss).execute_with_effects(|s, fx| {
                    let r = s.set_srem(&src, std::slice::from_ref(&member));
                    if matches!(r, CommandResponse::Integer(1)) {
                        fx.push("SREM", &[&src, &member]);
                    }
                    r
                });
                if !matches!(removed, CommandResponse::Integer(1)) {
                    return removed;
                }
                self.shards.shard(ds).execute_with_effects(|s, fx| {
                    s.set_sadd(&dst, vec![member.clone()]);
                    fx.push("SADD", &[&dst, &member]);
                    CommandResponse::ok()
                });
                CommandResponse::integer(1)
            }

            // ---- Sorted Set commands ----
            Cmd::ZADD => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zadd");
                }
                let key = args[0].clone();
                let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) =
                    (false, false, false, false, false, false);
                let mut i = 1;
                while i < args.len() {
                    match String::from_utf8_lossy(&args[i])
                        .to_ascii_uppercase()
                        .as_str()
                    {
                        "NX" => nx = true,
                        "XX" => xx = true,
                        "GT" => gt = true,
                        "LT" => lt = true,
                        "CH" => ch = true,
                        "INCR" => incr = true,
                        _ => break, // Start of score-member pairs
                    }
                    i += 1;
                }
                let remaining = &args[i..];
                if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
                    return CommandResponse::syntax_error();
                }
                if nx && xx {
                    return CommandResponse::error(
                        "ERR XX and NX options at the same time are not compatible",
                    );
                }
                if (gt && lt) || (nx && (gt || lt)) {
                    return CommandResponse::error(
                        "ERR GT, LT, and/or NX options at the same time are not compatible",
                    );
                }
                if incr && remaining.len() != 2 {
                    return CommandResponse::error(
                        "ERR INCR option supports a single increment-element pair",
                    );
                }
                let mut pairs = Vec::with_capacity(remaining.len() / 2);
                for c in remaining.chunks(2) {
                    let Some(score) = std::str::from_utf8(&c[0])
                        .ok()
                        .and_then(solikv_core::zset_ops::parse_float)
                    else {
                        return CommandResponse::error("ERR value is not a valid float");
                    };
                    pairs.push((score, c[1].clone()));
                }
                if incr {
                    let (delta, member) = pairs.pop().expect("one pair");
                    return self.shards.shard_for_key(&key).execute(move |store| {
                        store.zset_zadd_incr(&key, delta, member, (nx, xx, gt, lt))
                    });
                }
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zadd(&key, pairs, nx, xx, gt, lt, ch))
            }
            Cmd::ZREM => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("zrem");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zrem(&key, &members))
            }
            Cmd::ZSCORE => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("zscore");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zscore(&key, &member))
            }
            Cmd::ZCARD => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("zcard");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zcard(&key))
            }
            Cmd::ZRANK => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("zrank");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zrank(&key, &member))
            }
            Cmd::ZREVRANK => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("zrevrank");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zrevrank(&key, &member))
            }
            Cmd::ZINCRBY => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("zincrby");
                }
                let key = args[0].clone();
                let Some(delta) = std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(solikv_core::zset_ops::parse_float)
                else {
                    return CommandResponse::error("ERR value is not a valid float");
                };
                let member = args[2].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zincrby(&key, delta, member))
            }
            Cmd::ZCOUNT | Cmd::ZLEXCOUNT => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity(&name.to_lowercase());
                }
                let by = match if name == "ZCOUNT" {
                    parse_score_range(&args[1], &args[2])
                } else {
                    parse_lex_range(&args[1], &args[2])
                } {
                    Ok(by) => by,
                    Err(e) => return e,
                };
                let key = args[0].clone();
                let q = ZRangeQuery {
                    by,
                    rev: false,
                    limit: None,
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    match store.zset_range_query(&key, &q) {
                        Ok(v) => CommandResponse::integer(v.len() as i64),
                        Err(e) => e,
                    }
                })
            }
            Cmd::ZRANGE
            | Cmd::ZREVRANGE
            | Cmd::ZRANGEBYSCORE
            | Cmd::ZREVRANGEBYSCORE
            | Cmd::ZRANGEBYLEX
            | Cmd::ZREVRANGEBYLEX => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity(&name.to_lowercase());
                }
                let (q, withscores) = match parse_zrange(name, args) {
                    Ok(q) => q,
                    Err(e) => return e,
                };
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    match store.zset_range_query(&key, &q) {
                        Ok(items) => solikv_core::zset_ops::zrange_reply(items, withscores),
                        Err(e) => e,
                    }
                })
            }
            Cmd::ZREMRANGEBYSCORE | Cmd::ZREMRANGEBYRANK | Cmd::ZREMRANGEBYLEX => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity(&name.to_lowercase());
                }
                let by = match name {
                    "ZREMRANGEBYSCORE" => parse_score_range(&args[1], &args[2]),
                    "ZREMRANGEBYLEX" => parse_lex_range(&args[1], &args[2]),
                    _ => match (parse_i64_arg(&args[1]), parse_i64_arg(&args[2])) {
                        (Some(a), Some(b)) => Ok(ZRangeBy::Rank(a, b)),
                        _ => Err(CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )),
                    },
                };
                let by = match by {
                    Ok(by) => by,
                    Err(e) => return e,
                };
                let key = args[0].clone();
                let q = ZRangeQuery {
                    by,
                    rev: false,
                    limit: None,
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_remove_range(&key, &q))
            }
            Cmd::ZPOPMIN => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("zpopmin");
                }
                let key = args[0].clone();
                let count = args
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok())
                    .unwrap_or(1);
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zpopmin(&key, count))
            }
            Cmd::ZPOPMAX => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("zpopmax");
                }
                let key = args[0].clone();
                let count = args
                    .get(1)
                    .and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok())
                    .unwrap_or(1);
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zpopmax(&key, count))
            }
            Cmd::ZUNIONSTORE | Cmd::ZINTERSTORE => self.zstore(name, args),
            Cmd::ZSCAN => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("zscan");
                }
                let key = args[0].clone();
                let Some(cursor) = std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                else {
                    return CommandResponse::error("ERR invalid cursor");
                };
                let (pattern, count, _) = match parse_scan_options(&args[2..], false) {
                    Ok(o) => o,
                    Err(e) => return e,
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zscan(&key, cursor, pattern.as_deref(), count))
            }

            // ---- Geospatial commands ----
            Cmd::GEOADD => {
                // GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
                if args.len() < 4 {
                    return CommandResponse::wrong_arity("geoadd");
                }
                let key = args[0].clone();
                let mut i = 1usize;
                let mut nx = false;
                let mut xx = false;
                let mut ch = false;

                // Parse optional flags
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "NX" => {
                            nx = true;
                            i += 1;
                        }
                        "XX" => {
                            xx = true;
                            i += 1;
                        }
                        "CH" => {
                            ch = true;
                            i += 1;
                        }
                        _ => break,
                    }
                }

                // Remaining args must be triplets: longitude latitude member
                if (args.len() - i) < 3 || !(args.len() - i).is_multiple_of(3) {
                    return CommandResponse::wrong_arity("geoadd");
                }

                let mut items = Vec::new();
                while i + 2 < args.len() {
                    let lon = match std::str::from_utf8(&args[i])
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        Some(v) => v,
                        None => return CommandResponse::error("ERR value is not a valid float"),
                    };
                    let lat = match std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        Some(v) => v,
                        None => return CommandResponse::error("ERR value is not a valid float"),
                    };
                    let member = args[i + 2].clone();
                    items.push((lon, lat, member));
                    i += 3;
                }

                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.geo_add(&key, nx, xx, ch, &items))
            }
            Cmd::GEOPOS => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("geopos");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.geo_pos(&key, &members))
            }
            Cmd::GEOHASH => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("geohash");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.geo_hash(&key, &members))
            }
            Cmd::GEODIST => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("geodist");
                }
                let key = args[0].clone();
                let member1 = args[1].clone();
                let member2 = args[2].clone();
                let unit = args
                    .get(3)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("m")
                    .to_lowercase();
                match unit.as_str() {
                    "m" | "km" | "ft" | "mi" => {}
                    _ => {
                        return CommandResponse::error(
                            "ERR unsupported unit provided. please use M, KM, FT, MI",
                        )
                    }
                }
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.geo_dist(&key, &member1, &member2, &unit))
            }
            Cmd::GEOSEARCH => {
                // GEOSEARCH key FROMMEMBER member | FROMLONLAT lon lat
                //   BYRADIUS radius unit | BYBOX width height unit
                //   [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("geosearch");
                }
                let key = args[0].clone();
                let mut i = 1usize;
                let mut from = None;
                let mut by = None;
                let mut sort_asc: Option<bool> = None;
                let mut count: Option<usize> = None;
                let mut withcoord = false;
                let mut withdist = false;
                let mut withhash = false;

                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "FROMMEMBER" => {
                            i += 1;
                            if i >= args.len() {
                                return CommandResponse::syntax_error();
                            }
                            from = Some(solikv_core::geo_ops::GeoFrom::Member(args[i].clone()));
                        }
                        "FROMLONLAT" => {
                            if i + 2 >= args.len() {
                                return CommandResponse::syntax_error();
                            }
                            let lon = match std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                Some(v) => v,
                                None => {
                                    return CommandResponse::error("ERR value is not a valid float")
                                }
                            };
                            let lat = match std::str::from_utf8(&args[i + 2])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                Some(v) => v,
                                None => {
                                    return CommandResponse::error("ERR value is not a valid float")
                                }
                            };
                            from = Some(solikv_core::geo_ops::GeoFrom::LonLat(lon, lat));
                            i += 2;
                        }
                        "BYRADIUS" => {
                            if i + 2 >= args.len() {
                                return CommandResponse::syntax_error();
                            }
                            let radius = match std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                Some(v) => v,
                                None => {
                                    return CommandResponse::error("ERR value is not a valid float")
                                }
                            };
                            let unit = std::str::from_utf8(&args[i + 2])
                                .unwrap_or("m")
                                .to_lowercase();
                            by = Some(solikv_core::geo_ops::GeoBy::Radius(radius, unit));
                            i += 2;
                        }
                        "BYBOX" => {
                            if i + 3 >= args.len() {
                                return CommandResponse::syntax_error();
                            }
                            let width = match std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                Some(v) => v,
                                None => {
                                    return CommandResponse::error("ERR value is not a valid float")
                                }
                            };
                            let height = match std::str::from_utf8(&args[i + 2])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                Some(v) => v,
                                None => {
                                    return CommandResponse::error("ERR value is not a valid float")
                                }
                            };
                            let unit = std::str::from_utf8(&args[i + 3])
                                .unwrap_or("m")
                                .to_lowercase();
                            by = Some(solikv_core::geo_ops::GeoBy::Box(width, height, unit));
                            i += 3;
                        }
                        "ASC" => sort_asc = Some(true),
                        "DESC" => sort_asc = Some(false),
                        "COUNT" => {
                            i += 1;
                            count = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok());
                            // Skip optional ANY
                            if i + 1 < args.len() {
                                let next = std::str::from_utf8(&args[i + 1])
                                    .unwrap_or("")
                                    .to_uppercase();
                                if next == "ANY" {
                                    i += 1;
                                }
                            }
                        }
                        "WITHCOORD" => withcoord = true,
                        "WITHDIST" => withdist = true,
                        "WITHHASH" => withhash = true,
                        _ => return CommandResponse::syntax_error(),
                    }
                    i += 1;
                }

                let from = match from {
                    Some(f) => f,
                    None => return CommandResponse::syntax_error(),
                };
                let by = match by {
                    Some(b) => b,
                    None => return CommandResponse::syntax_error(),
                };

                self.shards.shard_for_key(&key).execute(move |store| {
                    store.geo_search(
                        &key, &from, &by, sort_asc, count, withcoord, withdist, withhash,
                    )
                })
            }
            Cmd::GEOSEARCHSTORE => {
                // GEOSEARCHSTORE dest src [FROMMEMBER member | FROMLONLAT lon lat]
                //   [BYRADIUS radius unit | BYBOX width height unit]
                //   [ASC|DESC] [COUNT count [ANY]] [STOREDIST]
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("geosearchstore");
                }
                // Source and destination held together: read and store in
                // one step.
                let _held = self.shards.lock_keys([args[0].as_ref(), args[1].as_ref()]);
                let dest = args[0].clone();
                let src = args[1].clone();
                let mut i = 2usize;
                let mut from = None;
                let mut by = None;
                let mut sort_asc: Option<bool> = None;
                let mut count: Option<usize> = None;
                let mut store_dist = false;

                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "FROMMEMBER" => {
                            i += 1;
                            if i >= args.len() {
                                return CommandResponse::syntax_error();
                            }
                            from = Some(solikv_core::geo_ops::GeoFrom::Member(args[i].clone()));
                        }
                        "FROMLONLAT" => {
                            if i + 2 >= args.len() {
                                return CommandResponse::syntax_error();
                            }
                            let lon = match std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                Some(v) => v,
                                None => {
                                    return CommandResponse::error("ERR value is not a valid float")
                                }
                            };
                            let lat = match std::str::from_utf8(&args[i + 2])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                Some(v) => v,
                                None => {
                                    return CommandResponse::error("ERR value is not a valid float")
                                }
                            };
                            from = Some(solikv_core::geo_ops::GeoFrom::LonLat(lon, lat));
                            i += 2;
                        }
                        "BYRADIUS" => {
                            if i + 2 >= args.len() {
                                return CommandResponse::syntax_error();
                            }
                            let radius = match std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                Some(v) => v,
                                None => {
                                    return CommandResponse::error("ERR value is not a valid float")
                                }
                            };
                            let unit = std::str::from_utf8(&args[i + 2])
                                .unwrap_or("m")
                                .to_lowercase();
                            by = Some(solikv_core::geo_ops::GeoBy::Radius(radius, unit));
                            i += 2;
                        }
                        "BYBOX" => {
                            if i + 3 >= args.len() {
                                return CommandResponse::syntax_error();
                            }
                            let width = match std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                Some(v) => v,
                                None => {
                                    return CommandResponse::error("ERR value is not a valid float")
                                }
                            };
                            let height = match std::str::from_utf8(&args[i + 2])
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                Some(v) => v,
                                None => {
                                    return CommandResponse::error("ERR value is not a valid float")
                                }
                            };
                            let unit = std::str::from_utf8(&args[i + 3])
                                .unwrap_or("m")
                                .to_lowercase();
                            by = Some(solikv_core::geo_ops::GeoBy::Box(width, height, unit));
                            i += 3;
                        }
                        "ASC" => sort_asc = Some(true),
                        "DESC" => sort_asc = Some(false),
                        "COUNT" => {
                            i += 1;
                            count = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok());
                            if i + 1 < args.len() {
                                let next = std::str::from_utf8(&args[i + 1])
                                    .unwrap_or("")
                                    .to_uppercase();
                                if next == "ANY" {
                                    i += 1;
                                }
                            }
                        }
                        "STOREDIST" => store_dist = true,
                        _ => return CommandResponse::syntax_error(),
                    }
                    i += 1;
                }

                let from = match from {
                    Some(f) => f,
                    None => return CommandResponse::syntax_error(),
                };
                let by = match by {
                    Some(b) => b,
                    None => return CommandResponse::syntax_error(),
                };

                // GEOSEARCHSTORE operates on two keys (src + dest), route to dest shard
                let shard = self.shards.shard_for_key(&dest).clone();
                shard.execute_with_effects(move |store, fx| {
                    let r = store
                        .geo_search_store(&dest, &src, &from, &by, sort_asc, count, store_dist);
                    fx.put_key(store, &dest);
                    r
                })
            }

            // ---- Stream commands ----
            Cmd::XADD => {
                // XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] N] *|id field value ...
                if args.len() < 4 {
                    return CommandResponse::wrong_arity("xadd");
                }
                let key = args[0].clone();
                let mut i = 1usize;
                let mut nomkstream = false;

                // Parse optional NOMKSTREAM
                if i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    if opt == "NOMKSTREAM" {
                        nomkstream = true;
                        i += 1;
                    }
                }

                // Parse optional trim
                let trim = match parse_stream_trim(args, &mut i) {
                    Ok(t) => t,
                    Err(e) => return e,
                };

                // Parse ID
                if i >= args.len() {
                    return CommandResponse::wrong_arity("xadd");
                }
                let id_input = match parse_stream_id(&args[i]) {
                    Some(id) => id,
                    None => {
                        return CommandResponse::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                };
                i += 1;

                // Parse field-value pairs
                if (args.len() - i) < 2 || !(args.len() - i).is_multiple_of(2) {
                    return CommandResponse::wrong_arity("xadd");
                }
                let fields: Vec<(Bytes, Bytes)> = args[i..]
                    .chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();

                self.shards.shard_for_key(&key).execute(move |store| {
                    store.stream_xadd(&key, id_input, fields, nomkstream, trim)
                })
            }
            Cmd::XLEN => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("xlen");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.stream_xlen(&key))
            }
            Cmd::XRANGE => {
                // XRANGE key start end [COUNT n]
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("xrange");
                }
                let key = args[0].clone();
                let start = match parse_stream_id(&args[1]) {
                    Some(StreamIdInput::Min) => StreamId::MIN,
                    Some(StreamIdInput::Explicit(id)) => id,
                    Some(StreamIdInput::Partial(ms)) => StreamId::new(ms, 0),
                    _ => {
                        return CommandResponse::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                };
                let end = match parse_stream_id(&args[2]) {
                    Some(StreamIdInput::Max) => StreamId::MAX,
                    Some(StreamIdInput::Explicit(id)) => id,
                    Some(StreamIdInput::Partial(ms)) => StreamId::new(ms, u64::MAX),
                    _ => {
                        return CommandResponse::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                };
                let mut count = None;
                if args.len() >= 5 {
                    let opt = std::str::from_utf8(&args[3]).unwrap_or("").to_uppercase();
                    if opt == "COUNT" {
                        count = args
                            .get(4)
                            .and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok());
                    }
                }
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.stream_xrange(&key, start, end, count))
            }
            Cmd::XREVRANGE => {
                // XREVRANGE key end start [COUNT n]
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("xrevrange");
                }
                let key = args[0].clone();
                let end = match parse_stream_id(&args[1]) {
                    Some(StreamIdInput::Max) => StreamId::MAX,
                    Some(StreamIdInput::Explicit(id)) => id,
                    Some(StreamIdInput::Partial(ms)) => StreamId::new(ms, u64::MAX),
                    _ => {
                        return CommandResponse::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                };
                let start = match parse_stream_id(&args[2]) {
                    Some(StreamIdInput::Min) => StreamId::MIN,
                    Some(StreamIdInput::Explicit(id)) => id,
                    Some(StreamIdInput::Partial(ms)) => StreamId::new(ms, 0),
                    _ => {
                        return CommandResponse::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                };
                let mut count = None;
                if args.len() >= 5 {
                    let opt = std::str::from_utf8(&args[3]).unwrap_or("").to_uppercase();
                    if opt == "COUNT" {
                        count = args
                            .get(4)
                            .and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok());
                    }
                }
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.stream_xrevrange(&key, end, start, count))
            }
            Cmd::XREAD => {
                // XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("xread");
                }
                let mut i = 0usize;
                let mut count = None;

                // Parse options
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "COUNT" => {
                            i += 1;
                            count = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok());
                            i += 1;
                        }
                        "BLOCK" => {
                            // Parse but ignore for v1
                            i += 2;
                        }
                        "STREAMS" => {
                            i += 1;
                            break;
                        }
                        _ => {
                            return CommandResponse::syntax_error();
                        }
                    }
                }

                // Remaining args: keys... ids... (equal count)
                let remaining = &args[i..];
                if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
                    return CommandResponse::syntax_error();
                }
                let half = remaining.len() / 2;
                let keys = &remaining[..half];
                let ids = &remaining[half..];

                // Query each key on its shard, merge results
                let mut results = Vec::new();
                for (idx, key) in keys.iter().enumerate() {
                    let id_arg = &ids[idx];
                    let last_id =
                        if std::str::from_utf8(id_arg).unwrap_or("") == "$" {
                            // "$" means: only new entries from now; since we don't block, return nil
                            continue;
                        } else {
                            match parse_stream_id(id_arg) {
                                Some(StreamIdInput::Explicit(id)) => id,
                                Some(StreamIdInput::Partial(ms)) => StreamId::new(ms, 0),
                                Some(StreamIdInput::Min) => StreamId::MIN,
                                _ => return CommandResponse::error(
                                    "ERR Invalid stream ID specified as stream command argument",
                                ),
                            }
                        };

                    let k = key.clone();
                    let resp = self
                        .shards
                        .shard_for_key(&k)
                        .execute(move |store| store.stream_xread_single(&k, last_id, count));

                    if !matches!(resp, CommandResponse::Nil) {
                        results.push(CommandResponse::array(vec![
                            CommandResponse::bulk(key.clone()),
                            resp,
                        ]));
                    }
                }

                if results.is_empty() {
                    CommandResponse::nil()
                } else {
                    CommandResponse::array(results)
                }
            }
            Cmd::XDEL => {
                // XDEL key id [id ...]
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("xdel");
                }
                let key = args[0].clone();
                let mut ids = Vec::new();
                for arg in &args[1..] {
                    match parse_stream_id(arg) {
                        Some(StreamIdInput::Explicit(id)) => ids.push(id),
                        Some(StreamIdInput::Partial(ms)) => ids.push(StreamId::new(ms, 0)),
                        _ => {
                            return CommandResponse::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            )
                        }
                    }
                }
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.stream_xdel(&key, &ids))
            }
            Cmd::XTRIM => {
                // XTRIM key MAXLEN|MINID [=|~] N
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("xtrim");
                }
                let key = args[0].clone();
                let mut i = 1;
                let trim = match parse_stream_trim(args, &mut i) {
                    Ok(Some(t)) => t,
                    Ok(None) => return CommandResponse::syntax_error(),
                    Err(e) => return e,
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.stream_xtrim(&key, trim))
            }
            Cmd::XINFO => {
                // XINFO STREAM key
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("xinfo");
                }
                let sub = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                match sub.as_str() {
                    "STREAM" => {
                        let key = args[1].clone();
                        self.shards
                            .shard_for_key(&key)
                            .execute(move |store| store.stream_xinfo(&key))
                    }
                    _ => CommandResponse::error(format!("ERR Unknown XINFO subcommand '{}'", sub)),
                }
            }
            Cmd::XGROUP => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("xgroup");
                }
                let sub = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                match sub.as_str() {
                    "CREATE" => {
                        // XGROUP CREATE key group id|$ [MKSTREAM]
                        if args.len() < 4 {
                            return CommandResponse::wrong_arity("xgroup|create");
                        }
                        let key = args[1].clone();
                        let group = args[2].clone();
                        let id_str = std::str::from_utf8(&args[3]).unwrap_or("0");
                        let id = if id_str == "$" {
                            StreamIdInput::Max
                        } else {
                            match StreamId::parse(id_str) {
                                Some(id) => id,
                                None => return CommandResponse::error(
                                    "ERR Invalid stream ID specified as stream command argument",
                                ),
                            }
                        };
                        let mkstream = args
                            .get(4)
                            .map(|a| {
                                std::str::from_utf8(a).unwrap_or("").to_uppercase() == "MKSTREAM"
                            })
                            .unwrap_or(false);
                        self.shards.shard_for_key(&key).execute(move |store| {
                            store.stream_xgroup_create(&key, group, id, mkstream)
                        })
                    }
                    "DESTROY" => {
                        if args.len() < 3 {
                            return CommandResponse::wrong_arity("xgroup|destroy");
                        }
                        let key = args[1].clone();
                        let group = args[2].clone();
                        self.shards
                            .shard_for_key(&key)
                            .execute(move |store| store.stream_xgroup_destroy(&key, &group))
                    }
                    "DELCONSUMER" => {
                        if args.len() < 4 {
                            return CommandResponse::wrong_arity("xgroup|delconsumer");
                        }
                        let key = args[1].clone();
                        let group = args[2].clone();
                        let consumer = args[3].clone();
                        self.shards.shard_for_key(&key).execute(move |store| {
                            store.stream_xgroup_delconsumer(&key, &group, &consumer)
                        })
                    }
                    _ => CommandResponse::error(format!("ERR Unknown XGROUP subcommand '{}'", sub)),
                }
            }
            Cmd::XREADGROUP => {
                // XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]
                // Six arguments at least: GROUP g c STREAMS key id (the
                // one-stream form is what the log records per shard).
                if args.len() < 6 {
                    return CommandResponse::wrong_arity("xreadgroup");
                }
                let mut i = 0;
                // Expect GROUP keyword
                let kw = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                if kw != "GROUP" {
                    return CommandResponse::syntax_error();
                }
                i += 1;
                let group = args[i].clone();
                i += 1;
                let consumer = args[i].clone();
                i += 1;

                let mut count = None;
                let mut noack = false;

                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "COUNT" => {
                            i += 1;
                            count = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok());
                            i += 1;
                        }
                        "BLOCK" => {
                            i += 2; // parse and ignore for v1
                        }
                        "NOACK" => {
                            noack = true;
                            i += 1;
                        }
                        "STREAMS" => {
                            i += 1;
                            break;
                        }
                        _ => return CommandResponse::syntax_error(),
                    }
                }

                let remaining = &args[i..];
                if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
                    return CommandResponse::syntax_error();
                }
                let half = remaining.len() / 2;
                let keys = &remaining[..half];
                let ids = &remaining[half..];

                let mut results = Vec::new();
                for (idx, key) in keys.iter().enumerate() {
                    let id_arg = &ids[idx];
                    let id_str = std::str::from_utf8(id_arg).unwrap_or(">");
                    let id_input =
                        if id_str == ">" {
                            StreamIdInput::Max
                        } else {
                            match parse_stream_id(id_arg) {
                                Some(id) => id,
                                None => return CommandResponse::error(
                                    "ERR Invalid stream ID specified as stream command argument",
                                ),
                            }
                        };

                    let k = key.clone();
                    let g = group.clone();
                    let c = consumer.clone();
                    let count_arg = count.map(|n| n.to_string());
                    let id_arg = id_arg.clone();
                    let resp =
                        self.shards
                            .shard_for_key(&k)
                            .execute_with_effects(move |store, fx| {
                                let r = store
                                    .stream_xreadgroup_opts(&k, &g, &c, id_input, count, noack);
                                if !r.is_error() && fx.enabled() {
                                    // This stream's part only: the others are on
                                    // their own shards' logs.
                                    let mut a: Vec<&[u8]> = vec![b"GROUP", &g, &c];
                                    if let Some(n) = &count_arg {
                                        a.push(b"COUNT");
                                        a.push(n.as_bytes());
                                    }
                                    if noack {
                                        a.push(b"NOACK");
                                    }
                                    a.extend_from_slice(&[b"STREAMS", &k, &id_arg]);
                                    fx.push("XREADGROUP", &a);
                                }
                                r
                            });

                    if !matches!(resp, CommandResponse::Nil) {
                        results.push(CommandResponse::array(vec![
                            CommandResponse::bulk(key.clone()),
                            resp,
                        ]));
                    }
                }

                if results.is_empty() {
                    CommandResponse::nil()
                } else {
                    CommandResponse::array(results)
                }
            }
            Cmd::XACK => {
                // XACK key group id [id ...]
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("xack");
                }
                let key = args[0].clone();
                let group = args[1].clone();
                let mut ids = Vec::new();
                for arg in &args[2..] {
                    match parse_stream_id(arg) {
                        Some(StreamIdInput::Explicit(id)) => ids.push(id),
                        Some(StreamIdInput::Partial(ms)) => ids.push(StreamId::new(ms, 0)),
                        _ => {
                            return CommandResponse::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            )
                        }
                    }
                }
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.stream_xack(&key, &group, &ids))
            }
            Cmd::XPENDING => {
                // XPENDING key group [start end count [consumer]]
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("xpending");
                }
                let key = args[0].clone();
                let group = args[1].clone();

                if args.len() == 2 {
                    // Summary form
                    self.shards.shard_for_key(&key).execute(move |store| {
                        store.stream_xpending(&key, &group, None, None, None, None)
                    })
                } else if args.len() >= 5 {
                    // Detail form: start end count [consumer]
                    let start = match parse_stream_id(&args[2]) {
                        Some(StreamIdInput::Min) => StreamId::MIN,
                        Some(StreamIdInput::Explicit(id)) => id,
                        Some(StreamIdInput::Partial(ms)) => StreamId::new(ms, 0),
                        _ => {
                            return CommandResponse::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            )
                        }
                    };
                    let end = match parse_stream_id(&args[3]) {
                        Some(StreamIdInput::Max) => StreamId::MAX,
                        Some(StreamIdInput::Explicit(id)) => id,
                        Some(StreamIdInput::Partial(ms)) => StreamId::new(ms, u64::MAX),
                        _ => {
                            return CommandResponse::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            )
                        }
                    };
                    let count = std::str::from_utf8(&args[4])
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(10);
                    let consumer_filter = args.get(5).cloned();
                    self.shards.shard_for_key(&key).execute(move |store| {
                        store.stream_xpending(
                            &key,
                            &group,
                            Some(start),
                            Some(end),
                            Some(count),
                            consumer_filter.as_ref(),
                        )
                    })
                } else {
                    CommandResponse::syntax_error()
                }
            }

            // ---- Pub/Sub commands ----
            Cmd::PUBLISH => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("publish");
                }
                let channel = args[0].clone();
                let message = args[1].clone();
                let count = self.pubsub.publish(channel, message);
                CommandResponse::integer(count as i64)
            }
            Cmd::PUBSUB => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("pubsub");
                }
                let sub = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                match sub.as_str() {
                    "CHANNELS" => {
                        if args.len() > 2 {
                            return CommandResponse::wrong_arity("pubsub|channels");
                        }
                        let pattern = args.get(1).map(|p| String::from_utf8_lossy(p).to_string());
                        // Only channels someone is subscribed to, as Redis
                        // lists them (the broker keeps a sender per channel
                        // that ever had one).
                        let channels = self
                            .pubsub
                            .numsub()
                            .into_iter()
                            .filter(|(ch, n)| {
                                *n > 0
                                    && pattern.as_ref().is_none_or(|p| {
                                        solikv_core::store::glob_match_pub(
                                            p,
                                            &String::from_utf8_lossy(ch),
                                        )
                                    })
                            })
                            .map(|(ch, _)| CommandResponse::bulk(ch))
                            .collect();
                        CommandResponse::array(channels)
                    }
                    "NUMSUB" => {
                        // Exactly the channels asked for, in order, 0 when
                        // nobody listens.
                        let mut result = Vec::with_capacity((args.len() - 1) * 2);
                        for ch in &args[1..] {
                            result.push(CommandResponse::bulk(ch.clone()));
                            result.push(CommandResponse::integer(
                                self.pubsub.subscriber_count(ch) as i64
                            ));
                        }
                        CommandResponse::array(result)
                    }
                    "NUMPAT" => {
                        if args.len() != 1 {
                            return CommandResponse::wrong_arity("pubsub|numpat");
                        }
                        let n = self
                            .pubsub
                            .pattern_numsub()
                            .into_iter()
                            .filter(|(_, n)| *n > 0)
                            .count();
                        CommandResponse::integer(n as i64)
                    }
                    _ => CommandResponse::error(format!(
                        "ERR unknown subcommand '{}'. Try PUBSUB HELP.",
                        sub.to_lowercase()
                    )),
                }
            }

            // ---- Persistence commands ----
            Cmd::SAVE => {
                if !args.is_empty() {
                    return CommandResponse::wrong_arity("save");
                }
                // Disk I/O and a lock per shard — release the async worker.
                self.maybe_block_in_place(|| {
                    match self.persistence.save(&self.shards, self.dirty()) {
                        Ok(()) => {
                            tracing::info!("DB saved on disk");
                            CommandResponse::ok()
                        }
                        Err(e) => CommandResponse::error(format!("ERR RDB save failed: {}", e)),
                    }
                })
            }
            Cmd::BGSAVE => {
                // `BGSAVE SCHEDULE` is accepted for compatibility; there is
                // no conflicting child process to wait for.
                if args.len() > 1 {
                    return CommandResponse::syntax_error();
                }
                match self.persistence.bgsave(&self.shards, self.dirty()) {
                    Ok(()) => CommandResponse::simple("Background saving started"),
                    Err(e) => CommandResponse::error(e),
                }
            }
            Cmd::BGREWRITEAOF => match self.persistence.bgrewrite(&self.shards) {
                Ok(()) => CommandResponse::simple("Background append only file rewriting started"),
                Err(e) => CommandResponse::error(e),
            },
            Cmd::LASTSAVE => CommandResponse::integer(
                self.persistence
                    .stats
                    .rdb_last_save_time
                    .load(Ordering::Relaxed) as i64,
            ),
            Cmd::SHUTDOWN => {
                // SHUTDOWN [NOSAVE|SAVE] [NOW] [FORCE] [ABORT]
                let mut save = None;
                for a in args {
                    match std::str::from_utf8(a).unwrap_or("").to_uppercase().as_str() {
                        "NOSAVE" => save = Some(false),
                        "SAVE" => save = Some(true),
                        "NOW" | "FORCE" => {}
                        "ABORT" => return CommandResponse::error("ERR No shutdown in progress."),
                        _ => return CommandResponse::syntax_error(),
                    }
                }
                let res = self.maybe_block_in_place(|| self.shutdown(save));
                match res {
                    Ok(()) => {
                        self.shutdown_requested.notify_one();
                        CommandResponse::ok()
                    }
                    Err(e) => CommandResponse::error(format!(
                        "ERR Errors trying to SHUTDOWN. Check logs. ({})",
                        e
                    )),
                }
            }

            // ---- Scripting commands ----
            Cmd::EVAL => {
                // EVAL script numkeys [key ...] [arg ...]
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("eval");
                }
                let script = match std::str::from_utf8(&args[0]) {
                    Ok(s) => s,
                    Err(_) => return CommandResponse::error("ERR invalid script encoding"),
                };
                let numkeys: usize = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(n) => n,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                let keys_start = match numkeys.checked_add(2) {
                    Some(start) => start,
                    None => {
                        return CommandResponse::error(
                            "ERR Number of keys can't be greater than number of args",
                        )
                    }
                };
                if args.len() < keys_start {
                    return CommandResponse::error(
                        "ERR Number of keys can't be greater than number of args",
                    );
                }
                let keys: Vec<Bytes> = args[2..keys_start].to_vec();
                let argv: Vec<Bytes> = args[keys_start..].to_vec();

                // Cache the script
                self.script_cache.load(script);
                self.run_script(script, keys, argv)
            }
            Cmd::EVALSHA => {
                // EVALSHA sha1 numkeys [key ...] [arg ...]
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("evalsha");
                }
                let sha = match std::str::from_utf8(&args[0]) {
                    Ok(s) => s.to_lowercase(),
                    Err(_) => return CommandResponse::error("ERR invalid SHA encoding"),
                };
                let script = match self.script_cache.get(&sha) {
                    Some(s) => s,
                    None => {
                        return CommandResponse::error("NOSCRIPT No matching script. Use EVAL.")
                    }
                };
                let numkeys: usize = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(n) => n,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                let keys_start = match numkeys.checked_add(2) {
                    Some(start) => start,
                    None => {
                        return CommandResponse::error(
                            "ERR Number of keys can't be greater than number of args",
                        )
                    }
                };
                if args.len() < keys_start {
                    return CommandResponse::error(
                        "ERR Number of keys can't be greater than number of args",
                    );
                }
                let keys: Vec<Bytes> = args[2..keys_start].to_vec();
                let argv: Vec<Bytes> = args[keys_start..].to_vec();
                self.run_script(&script, keys, argv)
            }
            Cmd::SCRIPT => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("script");
                }
                let sub = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                match sub.as_str() {
                    "LOAD" => {
                        if args.len() != 2 {
                            return CommandResponse::wrong_arity("script|load");
                        }
                        match std::str::from_utf8(&args[1]) {
                            Ok(script) => {
                                let sha = self.script_cache.load(script);
                                CommandResponse::BulkString(Bytes::from(sha))
                            }
                            Err(_) => CommandResponse::error("ERR invalid script encoding"),
                        }
                    }
                    "EXISTS" => {
                        if args.len() < 2 {
                            return CommandResponse::wrong_arity("script|exists");
                        }
                        let shas: Vec<&str> = args[1..]
                            .iter()
                            .map(|a| std::str::from_utf8(a).unwrap_or(""))
                            .collect();
                        let results = self.script_cache.exists(&shas);
                        CommandResponse::Array(
                            results
                                .into_iter()
                                .map(|b| CommandResponse::Integer(if b { 1 } else { 0 }))
                                .collect(),
                        )
                    }
                    "FLUSH" => {
                        self.script_cache.flush();
                        CommandResponse::ok()
                    }
                    _ => CommandResponse::error(format!("ERR Unknown SCRIPT subcommand '{}'", sub)),
                }
            }

            // ---- HyperLogLog commands ----
            Cmd::PFADD => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("pfadd");
                }
                let key = args[0].clone();
                let elements: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.pfadd(&key, &elements))
            }
            Cmd::PFCOUNT => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("pfcount");
                }
                if args.len() == 1 {
                    let key = args[0].clone();
                    self.shards
                        .shard_for_key(&key)
                        .execute(move |store| store.pfcount(&[key]))
                } else {
                    // Multi-key: collect registers from each shard, merge, count
                    let mut merged_registers = vec![0u8; 16384];
                    for key in args {
                        let k = key.clone();
                        let resp = self
                            .shards
                            .shard_for_key(key)
                            .execute(move |store| store.pfget_registers(&k));
                        match resp {
                            CommandResponse::BulkString(data) => {
                                for (i, &r) in data.iter().enumerate() {
                                    merged_registers[i] = merged_registers[i].max(r);
                                }
                            }
                            CommandResponse::Nil => {}
                            CommandResponse::Error(_) => return resp,
                            _ => {}
                        }
                    }
                    let hll = solikv_core::types::HyperLogLogValue {
                        registers: merged_registers,
                    };
                    CommandResponse::integer(hll.count() as i64)
                }
            }
            Cmd::PFMERGE => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("pfmerge");
                }
                let _held = self.shards.lock_keys(args.iter().map(|k| k.as_ref()));
                // Cross-shard merge: collect registers from dest + all sources
                let mut merged_registers = vec![0u8; 16384];
                let dest = args[0].clone();

                // Include dest's existing HLL if any
                let dest_resp = self.shards.shard_for_key(&dest).execute({
                    let d = dest.clone();
                    move |store| store.pfget_registers(&d)
                });
                match dest_resp {
                    CommandResponse::BulkString(data) => {
                        for (i, &r) in data.iter().enumerate() {
                            merged_registers[i] = merged_registers[i].max(r);
                        }
                    }
                    CommandResponse::Nil => {}
                    CommandResponse::Error(_) => return dest_resp,
                    _ => {}
                }

                // Merge all source HLLs
                for src in &args[1..] {
                    let src_key = src.clone();
                    let resp = self
                        .shards
                        .shard_for_key(src)
                        .execute(move |store| store.pfget_registers(&src_key));
                    match resp {
                        CommandResponse::BulkString(data) => {
                            for (i, &r) in data.iter().enumerate() {
                                merged_registers[i] = merged_registers[i].max(r);
                            }
                        }
                        CommandResponse::Nil => {}
                        CommandResponse::Error(_) => return resp,
                        _ => {}
                    }
                }

                // Store merged result in dest's shard; the sources may live on
                // other shards, so the log gets the resulting value.
                self.shards
                    .shard_for_key(&dest)
                    .execute_with_effects(move |store, fx| {
                        let r = store.pfset_registers(&dest, merged_registers);
                        fx.put_key(store, &dest);
                        r
                    })
            }

            // ---- Bloom Filter commands ----
            Cmd::BF_RESERVE => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("bf.reserve");
                }
                let key = args[0].clone();
                let error_rate = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    Some(r) if r > 0.0 && r < 1.0 => r,
                    _ => return CommandResponse::error("ERR bad error rate value"),
                };
                let capacity = match std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(c) if c > 0 => c,
                    _ => return CommandResponse::error("ERR bad capacity value"),
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_reserve(&key, error_rate, capacity))
            }
            Cmd::BF_ADD => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("bf.add");
                }
                let key = args[0].clone();
                let item = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_add(&key, &item))
            }
            Cmd::BF_MADD => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("bf.madd");
                }
                let key = args[0].clone();
                let items: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_madd(&key, &items))
            }
            Cmd::BF_EXISTS => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("bf.exists");
                }
                let key = args[0].clone();
                let item = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_exists(&key, &item))
            }
            Cmd::BF_MEXISTS => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("bf.mexists");
                }
                let key = args[0].clone();
                let items: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_mexists(&key, &items))
            }
            Cmd::BF_INFO => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("bf.info");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_info(&key))
            }

            // ---- Bitmap commands ----
            Cmd::SETBIT => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("setbit");
                }
                let key = args[0].clone();
                let offset = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    // 2^32 bits is a 512 MB string, Redis' limit. Unbounded, one
                    // SETBIT could make the server allocate 2^61 bytes.
                    Some(o) if o < solikv_core::types::MAX_BIT_OFFSET => o,
                    _ => {
                        return CommandResponse::error(
                            "ERR bit offset is not an integer or out of range",
                        )
                    }
                };
                let value = match std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<u8>().ok())
                {
                    Some(v) if v <= 1 => v,
                    _ => {
                        return CommandResponse::error("ERR bit is not an integer or out of range")
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_setbit(&key, offset, value))
            }
            Cmd::GETBIT => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("getbit");
                }
                let key = args[0].clone();
                let offset = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    // 2^32 bits is a 512 MB string, Redis' limit. Unbounded, one
                    // SETBIT could make the server allocate 2^61 bytes.
                    Some(o) if o < solikv_core::types::MAX_BIT_OFFSET => o,
                    _ => {
                        return CommandResponse::error(
                            "ERR bit offset is not an integer or out of range",
                        )
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_getbit(&key, offset))
            }
            Cmd::BITCOUNT => {
                if args.is_empty() || args.len() == 2 || args.len() > 3 {
                    return CommandResponse::wrong_arity("bitcount");
                }
                let key = args[0].clone();
                let (start, end) = if args.len() == 3 {
                    let s = match std::str::from_utf8(&args[1])
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(v) => v,
                        None => {
                            return CommandResponse::error(
                                "ERR value is not an integer or out of range",
                            )
                        }
                    };
                    let e = match std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(v) => v,
                        None => {
                            return CommandResponse::error(
                                "ERR value is not an integer or out of range",
                            )
                        }
                    };
                    (Some(s), Some(e))
                } else {
                    (None, None)
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_bitcount(&key, start, end))
            }
            Cmd::BITOP => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("bitop");
                }
                let _held = self.shards.lock_keys(args[1..].iter().map(|k| k.as_ref()));
                let op = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                let dest = args[1].clone();
                let src_keys = &args[2..];

                match op.as_str() {
                    "NOT" => {
                        if src_keys.len() != 1 {
                            return CommandResponse::error(
                                "ERR BITOP NOT requires one and only one key",
                            );
                        }
                    }
                    "AND" | "OR" | "XOR" => {}
                    _ => return CommandResponse::syntax_error(),
                }

                // Fetch raw bytes from each source key (cross-shard)
                let mut sources: Vec<Vec<u8>> = Vec::with_capacity(src_keys.len());
                for src_key in src_keys {
                    let k = src_key.clone();
                    let resp = self
                        .shards
                        .shard_for_key(src_key)
                        .execute(move |store| store.string_get_raw(&k));
                    match resp {
                        CommandResponse::BulkString(data) => sources.push(data.to_vec()),
                        CommandResponse::Nil => sources.push(Vec::new()),
                        CommandResponse::Error(_) => return resp,
                        _ => sources.push(Vec::new()),
                    }
                }

                // Store result in dest shard; the log gets the resulting value,
                // computed from sources that may live on other shards.
                self.shards
                    .shard_for_key(&dest)
                    .execute_with_effects(move |store, fx| {
                        let r = store.string_bitop(dest.clone(), &op, &sources);
                        fx.put_key(store, &dest);
                        r
                    })
            }

            // ---- Catch-all ----
            _ => CommandResponse::error(format!("ERR unknown command '{}'", name)),
        }
    }
}

// ---------------------------------------------------------------------------
// Connections: databases, transactions, watches, CLIENT
// ---------------------------------------------------------------------------

/// Commands that name no key and touch no key: a transaction made only of
/// these (and keyed commands) needs no shard it does not name.
fn keyless_and_keyspace_free(name: &str) -> bool {
    matches!(
        name,
        "PING" | "ECHO" | "PUBLISH" | "COMMAND" | "LASTSAVE" | "SELECT" | "TIME" | "ROLE"
    )
}

/// What a write changes, for watch marking.
fn touch_for(name: &str, args: &[Bytes]) -> Option<Touch> {
    match name {
        // Scripts: each `redis.call` marks for itself.
        "EVAL" | "EVALSHA" => None,
        "FLUSHDB" => Some(Touch::Db(current_db())),
        "FLUSHALL" => Some(Touch::All),
        // Marked by the arm, which knows both databases.
        "SWAPDB" | "MOVE" => None,
        _ => crate::commands::keys_of(name, args).map(|k| Touch::Keys(current_db(), k)),
    }
}

impl CommandEngine {
    /// Execute a command for a connection: connection-level commands
    /// (`SELECT`, `MULTI`/`EXEC`, `WATCH`, `CLIENT`, `RESET`) here, the rest
    /// in the session's database.
    pub fn execute_session(
        &self,
        session: &mut Session,
        name: &str,
        args: &[Bytes],
    ) -> CommandResponse {
        // Inside MULTI, everything but the transaction's own commands queues.
        if session.multi.is_some()
            && !matches!(
                name,
                "EXEC" | "DISCARD" | "MULTI" | "WATCH" | "UNWATCH" | "QUIT" | "RESET"
            )
        {
            return self.queue(session, name, args);
        }
        match name {
            "SELECT" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("select");
                }
                match self.parse_db(&args[0]) {
                    Ok(db) => {
                        session.set_db(db);
                        CommandResponse::ok()
                    }
                    Err(e) => e,
                }
            }
            "MULTI" => {
                if !args.is_empty() {
                    return CommandResponse::wrong_arity("multi");
                }
                if session.multi.is_some() {
                    return CommandResponse::error("ERR MULTI calls can not be nested");
                }
                session.multi = Some(Vec::new());
                session.multi_failed = false;
                session.set_multi_len(Some(0));
                CommandResponse::ok()
            }
            "DISCARD" => {
                if session.multi.take().is_none() {
                    return CommandResponse::error("ERR DISCARD without MULTI");
                }
                session.set_multi_len(None);
                session.unwatch_all();
                CommandResponse::ok()
            }
            "EXEC" => self.exec(session),
            "WATCH" => {
                if session.multi.is_some() {
                    return CommandResponse::error("ERR WATCH inside MULTI is not allowed");
                }
                if args.is_empty() {
                    return CommandResponse::wrong_arity("watch");
                }
                let flag = session
                    .watch_flag
                    .get_or_insert_with(|| Arc::new(WatchFlag::default()))
                    .clone();
                session.watch_owner = Some(self.shards.clone());
                for k in args {
                    let entry = (session.db, k.clone());
                    if !session.watched.contains(&entry) {
                        self.shards.watch().watch(session.db, k.clone(), &flag);
                        session.watched.push(entry);
                    }
                }
                CommandResponse::ok()
            }
            "UNWATCH" => {
                session.unwatch_all();
                CommandResponse::ok()
            }
            "RESET" => {
                session.multi = None;
                session.set_multi_len(None);
                session.unwatch_all();
                session.set_db(0);
                session.set_name(None);
                session.reply = ReplyMode::On;
                CommandResponse::simple("RESET")
            }
            "CLIENT" => self.client_command(session, args),
            _ => {
                let _db = DbGuard::enter(session.db);
                self.execute(name, args)
            }
        }
    }

    fn parse_db(&self, arg: &Bytes) -> Result<u32, CommandResponse> {
        let n = std::str::from_utf8(arg)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| CommandResponse::error("ERR value is not an integer or out of range"))?;
        if n < 0 || n >= self.databases as i64 {
            return Err(CommandResponse::error("ERR DB index is out of range"));
        }
        Ok(n as u32)
    }

    /// Queue a command inside `MULTI`, validating what Redis validates at
    /// queue time (a bad one aborts the whole transaction at `EXEC`).
    fn queue(&self, session: &mut Session, name: &str, args: &[Bytes]) -> CommandResponse {
        let Some(spec) = crate::commands::lookup(name) else {
            session.multi_failed = true;
            return CommandResponse::error(format!(
                "ERR unknown command '{}', with args beginning with: {}",
                name.to_lowercase(),
                args_preview(args)
            ));
        };
        if !spec.arity_ok(args.len()) {
            session.multi_failed = true;
            return CommandResponse::wrong_arity(&name.to_lowercase());
        }
        if spec.flags.contains(&"pubsub") && name != "PUBLISH" && name != "PUBSUB" {
            session.multi_failed = true;
            return CommandResponse::error(format!(
                "ERR Command not allowed inside a transaction: '{}'",
                name.to_lowercase()
            ));
        }
        let q = session.multi.as_mut().expect("in MULTI");
        q.push((name.to_string(), args.to_vec()));
        let n = q.len();
        session.set_multi_len(Some(n));
        crate::session::queued()
    }

    /// `EXEC`: run the queue as one step. Every shard its commands (and
    /// watched keys) live on is locked first, so no other client's command
    /// runs in between — and a watched key changed before this point has
    /// flagged the session by then (see `watch`).
    fn exec(&self, session: &mut Session) -> CommandResponse {
        let Some(queue) = session.multi.take() else {
            return CommandResponse::error("ERR EXEC without MULTI");
        };
        session.set_multi_len(None);
        if std::mem::take(&mut session.multi_failed) {
            session.unwatch_all();
            return CommandResponse::error(
                "EXECABORT Transaction discarded because of previous errors.",
            );
        }

        // Which shards to hold: those of every key named, or all of them
        // when a command's keys are not known up front (scripts, KEYS,
        // FLUSHDB, …).
        let mut all = false;
        let mut idx: Vec<usize> = Vec::new();
        for (name, args) in &queue {
            if name == "EVAL" || name == "EVALSHA" {
                all = true;
                break;
            }
            let keys = crate::commands::keys_of(name, args).unwrap_or_default();
            if keys.is_empty() && !keyless_and_keyspace_free(name) {
                all = true;
                break;
            }
            idx.extend(keys.iter().map(|k| self.shards.shard_index_for_key(k)));
        }
        idx.extend(
            session
                .watched
                .iter()
                .map(|(_, k)| self.shards.shard_index_for_key(k)),
        );
        let _held = if all {
            self.shards.lock_all()
        } else {
            self.shards.lock_shards(idx)
        };

        if session.watch_flag.as_ref().is_some_and(|f| f.is_dirty()) {
            session.unwatch_all();
            return CommandResponse::Nil;
        }
        // Watches end with the EXEC; unregister before running so the
        // transaction's own writes do not bother marking them.
        session.unwatch_all();

        let tx = self.aof_transaction();
        let mut results = Vec::with_capacity(queue.len());
        for (name, args) in queue {
            let resp = if name == "SELECT" {
                match self.parse_db(&args[0]) {
                    Ok(db) => {
                        session.set_db(db);
                        CommandResponse::ok()
                    }
                    Err(e) => e,
                }
            } else if name == "CLIENT" {
                self.client_command(session, &args)
            } else {
                let _db = DbGuard::enter(session.db);
                self.execute(&name, &args)
            };
            results.push(resp);
        }
        drop(tx);
        CommandResponse::Array(results)
    }

    fn client_command(&self, session: &mut Session, args: &[Bytes]) -> CommandResponse {
        let Some(sub) = args.first() else {
            return CommandResponse::wrong_arity("client");
        };
        let sub = String::from_utf8_lossy(sub).to_ascii_uppercase();
        let rest = &args[1..];
        match sub.as_str() {
            "ID" => CommandResponse::integer(session.info.id as i64),
            "SETNAME" => {
                let [name] = rest else {
                    return CommandResponse::wrong_arity("client|setname");
                };
                if name.iter().any(|&b| b <= b' ' || b > b'~') {
                    return CommandResponse::error(
                        "ERR Client names cannot contain spaces, newlines or special characters.",
                    );
                }
                session.set_name((!name.is_empty()).then(|| name.clone()));
                CommandResponse::ok()
            }
            "GETNAME" => match session.info.name() {
                Some(n) => CommandResponse::bulk(n),
                None => CommandResponse::nil(),
            },
            "SETINFO" => {
                let [attr, value] = rest else {
                    return CommandResponse::wrong_arity("client|setinfo");
                };
                match String::from_utf8_lossy(attr).to_ascii_uppercase().as_str() {
                    "LIB-NAME" => session.set_lib(Some(value.clone()), None),
                    "LIB-VER" => session.set_lib(None, Some(value.clone())),
                    other => {
                        return CommandResponse::error(format!(
                            "ERR Unrecognized option '{}'",
                            other
                        ))
                    }
                }
                CommandResponse::ok()
            }
            "INFO" => {
                let mut line = session.info.describe();
                line.push('\n');
                CommandResponse::bulk(Bytes::from(line))
            }
            "LIST" => {
                let mut ids: Option<Vec<u64>> = None;
                let mut i = 0;
                while i < rest.len() {
                    let opt = String::from_utf8_lossy(&rest[i]).to_ascii_uppercase();
                    match opt.as_str() {
                        "TYPE" if i + 1 < rest.len() => i += 2,
                        "ID" => {
                            let mut v = Vec::new();
                            for a in &rest[i + 1..] {
                                match std::str::from_utf8(a).ok().and_then(|s| s.parse().ok()) {
                                    Some(id) => v.push(id),
                                    None => return CommandResponse::error("ERR Invalid client ID"),
                                }
                            }
                            ids = Some(v);
                            i = rest.len();
                        }
                        _ => return CommandResponse::syntax_error(),
                    }
                }
                let mut out = String::new();
                for c in session.registry().list() {
                    if ids.as_ref().is_some_and(|ids| !ids.contains(&c.id)) {
                        continue;
                    }
                    out.push_str(&c.describe());
                    out.push('\n');
                }
                CommandResponse::bulk(Bytes::from(out))
            }
            "KILL" => self.client_kill(session, rest),
            "REPLY" => {
                let [mode] = rest else {
                    return CommandResponse::wrong_arity("client|reply");
                };
                match String::from_utf8_lossy(mode).to_ascii_uppercase().as_str() {
                    "ON" => {
                        session.reply = ReplyMode::On;
                        CommandResponse::ok()
                    }
                    "OFF" => {
                        session.reply = ReplyMode::Off;
                        session.suppress_current = true;
                        CommandResponse::ok()
                    }
                    "SKIP" => {
                        session.reply = ReplyMode::Skip;
                        session.suppress_current = true;
                        CommandResponse::ok()
                    }
                    _ => CommandResponse::syntax_error(),
                }
            }
            // Accepted, meaningless here (no eviction, no LRU clock yet).
            "NO-EVICT" | "NO-TOUCH" => match rest {
                [v] if v.eq_ignore_ascii_case(b"ON") || v.eq_ignore_ascii_case(b"OFF") => {
                    CommandResponse::ok()
                }
                _ => CommandResponse::syntax_error(),
            },
            // Client-side caching is not implemented. Saying OK here made
            // clients serve stale cached values forever.
            "TRACKING" | "CACHING" | "GETREDIR" | "TRACKINGINFO" => CommandResponse::error(
                "ERR client-side caching (CLIENT TRACKING) is not supported by this server",
            ),
            "PAUSE" | "UNPAUSE" | "UNBLOCK" => CommandResponse::error(format!(
                "ERR CLIENT {} is not supported by this server",
                sub
            )),
            "HELP" => CommandResponse::array(
                [
                    "CLIENT <subcommand> [<arg> ...]. Subcommands are:",
                    "ID, GETNAME, SETNAME <name>, SETINFO <attr> <value>, INFO,",
                    "LIST [TYPE type] [ID id ...], KILL <filter> <value> ...,",
                    "REPLY ON|OFF|SKIP, NO-EVICT ON|OFF, NO-TOUCH ON|OFF",
                ]
                .iter()
                .map(|l| CommandResponse::simple(l))
                .collect(),
            ),
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                sub.to_lowercase()
            )),
        }
    }

    fn client_kill(&self, session: &Session, rest: &[Bytes]) -> CommandResponse {
        // Old form: CLIENT KILL addr:port
        if rest.len() == 1 {
            let addr = String::from_utf8_lossy(&rest[0]).to_string();
            let n = session.kill_matching(|c| c.addr == addr, false);
            return if n == 0 {
                CommandResponse::error("ERR No such client")
            } else {
                CommandResponse::ok()
            };
        }
        if rest.is_empty() || !rest.len().is_multiple_of(2) {
            return CommandResponse::syntax_error();
        }
        let mut id: Option<u64> = None;
        let mut addr: Option<String> = None;
        let mut laddr: Option<String> = None;
        let mut skip_me = true;
        for pair in rest.chunks(2) {
            let opt = String::from_utf8_lossy(&pair[0]).to_ascii_uppercase();
            let val = String::from_utf8_lossy(&pair[1]).to_string();
            match opt.as_str() {
                "ID" => match val.parse() {
                    Ok(v) => id = Some(v),
                    Err(_) => {
                        return CommandResponse::error("ERR client-id should be greater than 0")
                    }
                },
                "ADDR" => addr = Some(val),
                "LADDR" => laddr = Some(val),
                "SKIPME" => match val.to_ascii_lowercase().as_str() {
                    "yes" => skip_me = true,
                    "no" => skip_me = false,
                    _ => return CommandResponse::syntax_error(),
                },
                // Every client is a normal client of the default user here.
                "TYPE" | "USER" | "MAXAGE" => {}
                _ => return CommandResponse::syntax_error(),
            }
        }
        let n = session.kill_matching(
            |c| {
                id.is_none_or(|id| c.id == id)
                    && addr.as_ref().is_none_or(|a| &c.addr == a)
                    && laddr.as_ref().is_none_or(|a| &c.laddr == a)
            },
            skip_me,
        );
        CommandResponse::integer(n)
    }
}

// ---------------------------------------------------------------------------
// Commands whose keys may span shards
// ---------------------------------------------------------------------------

impl CommandEngine {
    /// `SINTER`/`SUNION`/`SDIFF` over `keys`, stored into `dest` when given.
    /// Keys on one shard (hash tags) are computed in place; otherwise the
    /// shards involved are held together and the sets combined here.
    fn set_algebra(&self, op: &str, keys: &[Bytes], dest: Option<&Bytes>) -> CommandResponse {
        let Some(op) = solikv_core::set_ops::SetOp::parse(op) else {
            return CommandResponse::syntax_error();
        };
        let mut idx: Vec<usize> = keys
            .iter()
            .map(|k| self.shards.shard_index_for_key(k))
            .collect();
        if let Some(d) = dest {
            idx.push(self.shards.shard_index_for_key(d));
        }
        // One shard: computed where the sets are, borrowing them.
        if idx.iter().all(|&i| i == idx[0]) {
            return self.shards.shard(idx[0]).execute_with_effects(|store, fx| {
                match store.set_op(op, keys) {
                    Err(e) => e,
                    Ok(result) => match dest {
                        None => CommandResponse::array(
                            result.into_iter().map(CommandResponse::bulk).collect(),
                        ),
                        Some(d) => {
                            let n = store.set_store(d, result);
                            fx.put_key(store, d);
                            CommandResponse::integer(n)
                        }
                    },
                }
            });
        }
        // Several shards, held together so the result is one consistent
        // view; the sources are borrowed in place, not copied.
        let held = self.shards.lock_shards(idx.iter().copied());
        let result = {
            let mut sets = Vec::with_capacity(keys.len());
            for k in keys {
                let si = self.shards.shard_index_for_key(k);
                // SAFETY: the borrows end with this block, before the
                // destination is written below.
                let Some(store) = (unsafe { self.shards.held_store(&held, si) }) else {
                    return CommandResponse::error("ERR internal: shard not held");
                };
                match store.peek(k) {
                    None => sets.push(None),
                    Some(e) => match &e.value {
                        RedisValue::Set(s) => sets.push(Some(&**s)),
                        _ => return CommandResponse::wrong_type(),
                    },
                }
            }
            solikv_core::set_ops::set_op_members(op, &sets)
        };
        match dest {
            None => CommandResponse::array(result.into_iter().map(CommandResponse::bulk).collect()),
            Some(d) => self.shards.shard_for_key(d).execute_with_effects(|s, fx| {
                let n = s.set_store(d, result);
                fx.put_key(s, d);
                CommandResponse::integer(n)
            }),
        }
    }
}

impl CommandEngine {
    /// `ZUNIONSTORE`/`ZINTERSTORE dest numkeys key… [WEIGHTS w…]
    /// [AGGREGATE SUM|MIN|MAX]`, over sources on any shards (sets count as
    /// sorted sets scored 1, as in Redis). The shards involved are held
    /// together; the result is logged as the destination's value.
    fn zstore(&self, name: &str, args: &[Bytes]) -> CommandResponse {
        let lname = name.to_lowercase();
        if args.len() < 3 {
            return CommandResponse::wrong_arity(&lname);
        }
        let dest = args[0].clone();
        let numkeys = match std::str::from_utf8(&args[1]).ok().map(|s| s.parse::<u64>()) {
            Some(Ok(n)) => n,
            _ => return CommandResponse::error("ERR value is not an integer or out of range"),
        };
        if numkeys < 1 {
            return CommandResponse::error(format!(
                "ERR at least 1 input key is needed for '{}' command",
                lname
            ));
        }
        let Some(keys_end) = usize::try_from(numkeys)
            .ok()
            .and_then(|n| n.checked_add(2))
            .filter(|&e| e <= args.len())
        else {
            return CommandResponse::error(
                "ERR Number of keys can't be greater than number of args",
            );
        };
        let numkeys = keys_end - 2;
        let keys = &args[2..keys_end];
        let mut weights = vec![1.0f64; numkeys];
        let mut aggregate = Aggregate::Sum;
        let mut i = keys_end;
        while i < args.len() {
            match String::from_utf8_lossy(&args[i])
                .to_ascii_uppercase()
                .as_str()
            {
                "WEIGHTS" if i + numkeys < args.len() => {
                    for (w, a) in weights.iter_mut().zip(&args[i + 1..=i + numkeys]) {
                        match std::str::from_utf8(a)
                            .ok()
                            .and_then(solikv_core::zset_ops::parse_float)
                        {
                            Some(v) => *w = v,
                            None => {
                                return CommandResponse::error("ERR weight value is not a float")
                            }
                        }
                    }
                    i += numkeys + 1;
                }
                "AGGREGATE" if i + 1 < args.len() => {
                    aggregate = match String::from_utf8_lossy(&args[i + 1])
                        .to_ascii_uppercase()
                        .as_str()
                    {
                        "SUM" => Aggregate::Sum,
                        "MIN" => Aggregate::Min,
                        "MAX" => Aggregate::Max,
                        _ => return CommandResponse::syntax_error(),
                    };
                    i += 2;
                }
                _ => return CommandResponse::syntax_error(),
            }
        }

        let idx: Vec<usize> = std::iter::once(&dest)
            .chain(keys)
            .map(|k| self.shards.shard_index_for_key(k))
            .collect();
        let _held = self.shards.lock_shards(idx);
        let mut sources = Vec::with_capacity(numkeys);
        for k in keys {
            let mut got = None;
            let r =
                self.shards
                    .shard_for_key(k)
                    .execute_with_effects(|s, _| match s.zset_source(k) {
                        Ok(v) => {
                            got = v;
                            CommandResponse::ok()
                        }
                        Err(e) => e,
                    });
            if r.is_error() {
                return r;
            }
            sources.push(got);
        }
        let combine = |a: f64, b: f64| match aggregate {
            // +inf + -inf is NaN; Redis makes it 0.
            Aggregate::Sum => {
                let s = a + b;
                if s.is_nan() {
                    0.0
                } else {
                    s
                }
            }
            Aggregate::Min => a.min(b),
            Aggregate::Max => a.max(b),
        };
        let weighted = |score: f64, w: f64| {
            let v = score * w;
            if v.is_nan() {
                0.0
            } else {
                v
            }
        };
        let mut acc: std::collections::HashMap<Bytes, f64> = std::collections::HashMap::new();
        if name == "ZUNIONSTORE" {
            for (src, w) in sources.into_iter().zip(&weights) {
                for (m, sc) in src.unwrap_or_default() {
                    let v = weighted(sc, *w);
                    acc.entry(m)
                        .and_modify(|cur| *cur = combine(*cur, v))
                        .or_insert(v);
                }
            }
        } else if sources.iter().all(|s| s.is_some()) {
            let mut srcs: Vec<(std::collections::HashMap<Bytes, f64>, f64)> = sources
                .into_iter()
                .zip(&weights)
                .map(|(s, w)| (s.unwrap_or_default().into_iter().collect(), *w))
                .collect();
            let (first, w0) = srcs.remove(0);
            'member: for (m, sc) in first {
                let mut v = weighted(sc, w0);
                for (other, w) in &srcs {
                    match other.get(&m) {
                        Some(o) => v = combine(v, weighted(*o, *w)),
                        None => continue 'member,
                    }
                }
                acc.insert(m, v);
            }
        }
        self.shards
            .shard_for_key(&dest)
            .execute_with_effects(|s, fx| {
                let n = s.zset_store(&dest, acc.into_iter().collect());
                fx.put_key(s, &dest);
                CommandResponse::integer(n)
            })
    }
}

impl CommandEngine {
    /// Run a script as one step: every shard is held for its duration (a
    /// script may touch keys it did not declare, as Redis allows outside
    /// cluster mode), and its writes are bracketed in the log so replay
    /// applies all of them or none.
    fn run_script(&self, script: &str, keys: Vec<Bytes>, argv: Vec<Bytes>) -> CommandResponse {
        let Some(arc) = self.get_self_arc() else {
            return CommandResponse::error("ERR scripting not initialized (call init_self_ref)");
        };
        let _held = self.shards.lock_all();
        let _tx = self.aof_transaction();
        crate::lua::execute_script(&arc, script, keys, argv)
    }
}

// ---------------------------------------------------------------------------
// COMMAND, CONFIG, INFO
// ---------------------------------------------------------------------------

fn command_info(spec: &crate::commands::CommandSpec) -> CommandResponse {
    let (first, last, step) = spec.key_range();
    CommandResponse::array(vec![
        CommandResponse::bulk(Bytes::from(spec.name.to_lowercase())),
        CommandResponse::integer(spec.arity as i64),
        CommandResponse::array(
            spec.flags
                .iter()
                .map(|f| CommandResponse::simple(f))
                .collect(),
        ),
        CommandResponse::integer(first as i64),
        CommandResponse::integer(last as i64),
        CommandResponse::integer(step as i64),
        CommandResponse::array(vec![]), // ACL categories
        CommandResponse::array(vec![]), // tips
        CommandResponse::array(vec![]), // key specifications
        CommandResponse::array(vec![]), // subcommands
    ])
}

/// Redis-style glob match on a config parameter name.
fn config_glob(pattern: &str, name: &str) -> bool {
    solikv_core::store::glob_match_pub(&pattern.to_ascii_lowercase(), name)
}

impl CommandEngine {
    fn command_command(&self, args: &[Bytes]) -> CommandResponse {
        let Some(sub) = args.first() else {
            return CommandResponse::array(
                crate::commands::all().iter().map(command_info).collect(),
            );
        };
        match String::from_utf8_lossy(sub).to_ascii_uppercase().as_str() {
            "COUNT" => CommandResponse::integer(crate::commands::all().len() as i64),
            "INFO" => {
                if args.len() == 1 {
                    return CommandResponse::array(
                        crate::commands::all().iter().map(command_info).collect(),
                    );
                }
                CommandResponse::array(
                    args[1..]
                        .iter()
                        .map(|n| {
                            let n = String::from_utf8_lossy(n).to_ascii_uppercase();
                            crate::commands::lookup(&n)
                                .map(command_info)
                                .unwrap_or(CommandResponse::Nil)
                        })
                        .collect(),
                )
            }
            // Documentation is not shipped; an empty map is a valid answer.
            "DOCS" => CommandResponse::array(vec![]),
            "LIST" => CommandResponse::array(
                crate::commands::all()
                    .iter()
                    .map(|s| CommandResponse::bulk(Bytes::from(s.name.to_lowercase())))
                    .collect(),
            ),
            "GETKEYS" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("command|getkeys");
                }
                let name = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
                let Some(spec) = crate::commands::lookup(&name) else {
                    return CommandResponse::error("ERR Invalid command specified");
                };
                if !spec.arity_ok(args.len() - 2) {
                    return CommandResponse::error(
                        "ERR Invalid number of arguments specified for command",
                    );
                }
                let keys = spec.keys_of(&args[2..]);
                if keys.is_empty() {
                    return CommandResponse::error("ERR The command has no key arguments");
                }
                CommandResponse::array(keys.into_iter().map(CommandResponse::bulk).collect())
            }
            other => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try COMMAND HELP.",
                other.to_lowercase()
            )),
        }
    }

    /// Every parameter `CONFIG GET` reports, with its current value.
    fn config_params(&self) -> Vec<(&'static str, String)> {
        let cfg = self.persistence.config();
        let yes_no = |b: bool| if b { "yes" } else { "no" }.to_string();
        vec![
            ("databases", self.databases.to_string()),
            (
                "save",
                if cfg.save_interval > 0 {
                    format!("{} 1", cfg.save_interval)
                } else {
                    String::new()
                },
            ),
            ("appendonly", yes_no(cfg.appendonly)),
            (
                "appendfsync",
                match cfg.fsync {
                    solikv_persist::FsyncPolicy::Always => "always",
                    solikv_persist::FsyncPolicy::Everysec => "everysec",
                    solikv_persist::FsyncPolicy::No => "no",
                }
                .to_string(),
            ),
            ("dir", cfg.dir.display().to_string()),
            ("dbfilename", format!("{}.rdb", cfg.dbfilename)),
            (
                "appenddirname",
                solikv_persist::aof::AOF_DIRNAME.to_string(),
            ),
            (
                "auto-aof-rewrite-percentage",
                cfg.auto_rewrite_percentage.to_string(),
            ),
            (
                "auto-aof-rewrite-min-size",
                cfg.auto_rewrite_min_size.to_string(),
            ),
            (
                "notify-keyspace-events",
                flags_to_string(self.notify_flags.load(Ordering::Relaxed)),
            ),
            ("port", self.tcp_port.load(Ordering::Relaxed).to_string()),
            (
                "maxmemory",
                self.maxmemory.load(Ordering::Relaxed).to_string(),
            ),
            (
                "maxmemory-policy",
                self.maxmemory_policy().name().to_string(),
            ),
            (
                "maxmemory-samples",
                self.maxmemory_samples.load(Ordering::Relaxed).to_string(),
            ),
        ]
    }

    fn config_command(&self, args: &[Bytes]) -> CommandResponse {
        let Some(sub) = args.first() else {
            return CommandResponse::wrong_arity("config");
        };
        let sub = String::from_utf8_lossy(sub).to_ascii_uppercase();
        match sub.as_str() {
            "GET" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("config|get");
                }
                let params = self.config_params();
                let mut out = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for pat in &args[1..] {
                    let pat = String::from_utf8_lossy(pat);
                    for (name, value) in &params {
                        if config_glob(&pat, name) && seen.insert(*name) {
                            out.push(CommandResponse::bulk_string(name));
                            out.push(CommandResponse::bulk(Bytes::from(value.clone())));
                        }
                    }
                }
                CommandResponse::array(out)
            }
            "SET" => {
                if args.len() < 3 || args.len().is_multiple_of(2) {
                    return CommandResponse::wrong_arity("config|set");
                }
                let known: Vec<&str> = self.config_params().iter().map(|(n, _)| *n).collect();
                // Validate all first: Redis applies all or none.
                for pair in args[1..].chunks(2) {
                    let param = String::from_utf8_lossy(&pair[0]).to_ascii_lowercase();
                    let value = String::from_utf8_lossy(&pair[1]);
                    let bad_value = match param.as_str() {
                        "notify-keyspace-events" => false,
                        "maxmemory" => solikv_core::memory::parse_size(&value).is_none(),
                        "maxmemory-policy" => EvictionPolicy::parse(&value).is_none(),
                        "maxmemory-samples" => {
                            !matches!(value.parse::<usize>(), Ok(n) if (1..=64).contains(&n))
                        }
                        p if known.contains(&p) => {
                            return CommandResponse::error(format!(
                                "ERR CONFIG SET failed (possibly related to argument '{}') - \
                                 can't set immutable config",
                                param
                            ));
                        }
                        _ => {
                            return CommandResponse::error(format!(
                                "ERR Unknown option or number of arguments for CONFIG SET - '{}'",
                                param
                            ));
                        }
                    };
                    if bad_value {
                        return CommandResponse::error(format!(
                            "ERR CONFIG SET failed (possibly related to argument '{}') - \
                             argument couldn't be parsed into an integer or is invalid",
                            param
                        ));
                    }
                }
                for pair in args[1..].chunks(2) {
                    let param = String::from_utf8_lossy(&pair[0]).to_ascii_lowercase();
                    let value = String::from_utf8_lossy(&pair[1]);
                    match param.as_str() {
                        "notify-keyspace-events" => self
                            .notify_flags
                            .store(parse_notify_flags(&value), Ordering::Relaxed),
                        "maxmemory" => {
                            self.set_maxmemory(solikv_core::memory::parse_size(&value).unwrap_or(0))
                        }
                        "maxmemory-policy" => {
                            if let Some(p) = EvictionPolicy::parse(&value) {
                                self.set_maxmemory_policy(p);
                            }
                        }
                        "maxmemory-samples" => self
                            .maxmemory_samples
                            .store(value.parse().unwrap_or(5), Ordering::Relaxed),
                        _ => {}
                    }
                }
                CommandResponse::ok()
            }
            "RESETSTAT" => {
                self.clients.reset_stats();
                CommandResponse::ok()
            }
            "REWRITE" => CommandResponse::error("ERR The server is running without a config file"),
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try CONFIG HELP.",
                sub.to_lowercase()
            )),
        }
    }

    fn info(&self, args: &[Bytes]) -> CommandResponse {
        const DEFAULT: &[&str] = &[
            "server",
            "clients",
            "memory",
            "persistence",
            "stats",
            "replication",
            "cpu",
            "keyspace",
        ];
        let mut wanted: Vec<String> = args
            .iter()
            .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase())
            .collect();
        if wanted.is_empty()
            || wanted
                .iter()
                .any(|w| w == "all" || w == "everything" || w == "default")
        {
            wanted = DEFAULT.iter().map(|s| s.to_string()).collect();
        }
        let mut out = String::new();
        for section in DEFAULT {
            if !wanted.iter().any(|w| w == section) {
                continue;
            }
            if !out.is_empty() {
                out.push_str("\r\n");
            }
            out.push_str(&self.info_section(section));
        }
        CommandResponse::bulk(Bytes::from(out))
    }

    fn info_section(&self, section: &str) -> String {
        let uptime = self.start_time.elapsed().as_secs();
        match section {
            "server" => format!(
                "# Server\r\n\
                 redis_version:7.2.0\r\n\
                 solikv_version:{}\r\n\
                 redis_mode:standalone\r\n\
                 os:{} {}\r\n\
                 arch_bits:{}\r\n\
                 process_id:{}\r\n\
                 tcp_port:{}\r\n\
                 uptime_in_seconds:{}\r\n\
                 uptime_in_days:{}\r\n\
                 executable:solikv\r\n",
                env!("CARGO_PKG_VERSION"),
                std::env::consts::OS,
                std::env::consts::ARCH,
                usize::BITS,
                std::process::id(),
                self.tcp_port.load(Ordering::Relaxed),
                uptime,
                uptime / 86400,
            ),
            "clients" => format!(
                "# Clients\r\nconnected_clients:{}\r\nblocked_clients:0\r\n",
                self.clients.count()
            ),
            "memory" => {
                let used = solikv_core::memory::used() as u64;
                let max = self.maxmemory.load(Ordering::Relaxed);
                format!(
                    "# Memory\r\n\
                     used_memory:{}\r\n\
                     used_memory_human:{}\r\n\
                     maxmemory:{}\r\n\
                     maxmemory_human:{}\r\n\
                     maxmemory_policy:{}\r\n\
                     mem_allocator:mimalloc\r\n",
                    used,
                    solikv_core::memory::human(used),
                    max,
                    solikv_core::memory::human(max),
                    self.maxmemory_policy().name(),
                )
            }
            "persistence" => self.persistence.stats.info_section(self.dirty()),
            "stats" => format!(
                "# Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\n\
                 expired_keys:{}\r\nevicted_keys:{}\r\n",
                self.clients.connections_received(),
                self.clients.commands_processed(),
                self.shards.expired_keys(),
                self.shards.evicted_keys(),
            ),
            "replication" => format!(
                "# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_repl_offset:{}\r\n",
                self.replication_offset.load(Ordering::Relaxed)
            ),
            "cpu" => "# CPU\r\n".to_string(),
            "keyspace" => {
                let mut out = String::from("# Keyspace\r\n");
                for db in 0..self.databases {
                    let (mut keys, mut expires) = (0usize, 0usize);
                    for shard in self.shards.all_shards() {
                        let (k, e) = shard.with_store(|s| (s.dbsize_of(db), s.expires_of(db)));
                        keys += k;
                        expires += e;
                    }
                    if keys > 0 {
                        out.push_str(&format!(
                            "db{}:keys={},expires={},avg_ttl=0\r\n",
                            db, keys, expires
                        ));
                    }
                }
                out
            }
            _ => String::new(),
        }
    }
}

fn args_preview(args: &[Bytes]) -> String {
    args.iter()
        .take(3)
        .map(|a| format!("'{}'", String::from_utf8_lossy(a)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_i64_arg(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

/// Absolute expiry (ms) for `EX|PX|EXAT|PXAT amount`, or `None` when it is
/// not a positive, representable time (Redis: "invalid expire time").
fn absolute_expiry(opt: &str, amount: i64) -> Option<u64> {
    if amount <= 0 {
        return None;
    }
    let amount = amount as u64;
    match opt {
        "EX" => now_millis().checked_add(ttl_ms(amount, true)?),
        "PX" => now_millis().checked_add(ttl_ms(amount, false)?),
        "EXAT" => amount
            .checked_mul(1000)
            .filter(|&ms| ms <= (i64::MAX as u64) / 2),
        "PXAT" => Some(amount).filter(|&ms| ms <= (i64::MAX as u64) / 2),
        _ => None,
    }
}

/// `SET` options after `key value`, with Redis 7's validation.
fn parse_set_options(
    opts: &[Bytes],
) -> Result<solikv_core::string_ops::SetOptions, CommandResponse> {
    let mut o = solikv_core::string_ops::SetOptions::default();
    let mut expiry_given = false;
    let mut i = 0;
    while i < opts.len() {
        let opt = String::from_utf8_lossy(&opts[i]).to_ascii_uppercase();
        match opt.as_str() {
            "NX" if !o.xx => o.nx = true,
            "XX" if !o.nx => o.xx = true,
            "GET" => o.get = true,
            "KEEPTTL" if !expiry_given => {
                o.keep_ttl = true;
                expiry_given = true;
            }
            "EX" | "PX" | "EXAT" | "PXAT" if !expiry_given => {
                i += 1;
                let Some(arg) = opts.get(i) else {
                    return Err(CommandResponse::syntax_error());
                };
                let Some(amount) = parse_i64_arg(arg) else {
                    return Err(CommandResponse::error(
                        "ERR value is not an integer or out of range",
                    ));
                };
                match absolute_expiry(&opt, amount) {
                    Some(at) => o.expire_at = Some(at),
                    None => return Err(invalid_expire("set")),
                }
                expiry_given = true;
            }
            _ => return Err(CommandResponse::syntax_error()),
        }
        i += 1;
    }
    Ok(o)
}

/// `keys` grouped by owning shard, in first-seen shard order (duplicates
/// kept: `EXISTS k k` counts twice).
fn group_by_shard(shards: &ShardManager, keys: &[Bytes]) -> Vec<(usize, Vec<Bytes>)> {
    let mut groups: Vec<(usize, Vec<Bytes>)> = Vec::new();
    for k in keys {
        let i = shards.shard_index_for_key(k);
        match groups.iter_mut().find(|(g, _)| *g == i) {
            Some((_, v)) => v.push(k.clone()),
            None => groups.push((i, vec![k.clone()])),
        }
    }
    groups
}

/// Elements past which dropping a value is worth a thread of its own.
const LAZY_FREE_THRESHOLD: usize = 64 * 1024;

/// Drop values removed by `UNLINK`: big ones on a background thread.
fn lazy_free(entries: Vec<solikv_core::KeyEntry>) {
    let weight: usize = entries
        .iter()
        .map(|e| match &e.value {
            RedisValue::List(l) => l.len(),
            RedisValue::Hash(h) => h.len(),
            RedisValue::Set(s) => s.len(),
            RedisValue::ZSet(z) => z.len(),
            RedisValue::Stream(s) => s.len(),
            _ => 1,
        })
        .sum();
    if weight >= LAZY_FREE_THRESHOLD {
        let _ = std::thread::Builder::new()
            .name("solikv-lazyfree".into())
            .spawn(move || drop(entries));
    }
}

/// Relative TTL in milliseconds from a client-supplied amount, or `None` when
/// it cannot be represented: `secs * 1000` used to wrap in release builds, so
/// a huge `EX` produced a tiny TTL. The bound keeps `now + ttl` inside an
/// `i64`, which is what TTL/PTTL report.
fn ttl_ms(amount: u64, seconds: bool) -> Option<u64> {
    const MAX_TTL_MS: u64 = (i64::MAX as u64) / 2;
    let ms = if seconds {
        amount.checked_mul(1000)?
    } else {
        amount
    };
    (ms <= MAX_TTL_MS).then_some(ms)
}

fn invalid_expire(cmd: &str) -> CommandResponse {
    CommandResponse::error(format!("ERR invalid expire time in '{}' command", cmd))
}

impl CommandEngine {
    /// Run `f` via `block_in_place` on the multi-thread runtime so other async
    /// tasks can progress while we hold shard locks / do disk I/O.
    ///
    /// Never in solo mode. `block_in_place` hands this worker's queue to a
    /// new thread, and with the solo store unlocked that meant two threads in
    /// it at once — KEYS or FLUSHALL walking the store while another
    /// connection wrote to it. Solo runs `f` inline, as Redis would: the
    /// single thread is busy, and that is the model. On current-thread
    /// runtimes (e.g. some tests) `block_in_place` panics, so those run
    /// inline too.
    fn maybe_block_in_place<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        if self.shards.is_solo() {
            return f();
        }
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(f)
            }
            _ => f(),
        }
    }
}

/// Encode multi-shard SCAN cursor: high 16 bits = shard index, low 48 = local index.
const SCAN_SHARD_SHIFT: u32 = 48;
const SCAN_LOCAL_MASK: u64 = (1u64 << SCAN_SHARD_SHIFT) - 1;

fn pack_scan_cursor(shard_idx: usize, local: usize) -> u64 {
    ((shard_idx as u64) << SCAN_SHARD_SHIFT) | (local as u64 & SCAN_LOCAL_MASK)
}

fn unpack_scan_cursor(cursor: u64) -> (usize, usize) {
    let shard = (cursor >> SCAN_SHARD_SHIFT) as usize;
    let local = (cursor & SCAN_LOCAL_MASK) as usize;
    (shard, local)
}

fn multi_shard_mget(shards: &crate::shard::ShardManager, keys: &[Bytes]) -> CommandResponse {
    let num = shards.num_shards();
    // Group (original_index, key) by shard so we lock each shard once.
    let mut by_shard: Vec<Vec<(usize, Bytes)>> = vec![Vec::new(); num];
    for (i, key) in keys.iter().enumerate() {
        let shard_idx = shards.shard_index_for_key(key);
        by_shard[shard_idx].push((i, key.clone()));
    }

    let mut results = vec![CommandResponse::Nil; keys.len()];
    // One consistent read across shards (an MSET is all-or-nothing too).
    let involved: Vec<usize> = (0..num).filter(|&i| !by_shard[i].is_empty()).collect();
    let _held = (involved.len() > 1).then(|| shards.lock_shards(involved));
    for (shard_idx, group) in by_shard.into_iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let indices: Vec<usize> = group.iter().map(|(i, _)| *i).collect();
        let group_keys: Vec<Bytes> = group.into_iter().map(|(_, k)| k).collect();
        let resp = shards.shard(shard_idx).execute(move |store| {
            CommandResponse::Array(group_keys.iter().map(|k| store.string_get(k)).collect())
        });
        if let CommandResponse::Array(items) = resp {
            for (idx, item) in indices.into_iter().zip(items) {
                results[idx] = item;
            }
        }
    }
    CommandResponse::Array(results)
}

/// This shard's pairs of an `MSET`/`MSETNX`, as an `MSET` record for its
/// log — encoded before the shard is locked (`None` with the log off).
fn mset_record(shards: &ShardManager, pairs: &[(Bytes, Bytes)]) -> Option<Vec<u8>> {
    shards.aof()?;
    let flat: Vec<&[u8]> = pairs
        .iter()
        .flat_map(|(k, v)| [k.as_ref(), v.as_ref()])
        .collect();
    Some(solikv_persist::aof::encode_command(b"MSET", &flat))
}

fn multi_shard_mset(
    shards: &crate::shard::ShardManager,
    pairs: Vec<(Bytes, Bytes)>,
) -> CommandResponse {
    let num = shards.num_shards();
    let mut by_shard: Vec<Vec<(Bytes, Bytes)>> = vec![Vec::new(); num];
    for (key, value) in pairs {
        let slot = shards.shard_index_for_key(&key);
        by_shard[slot].push((key, value));
    }
    // Atomic like Redis' MSET: nobody reads some keys new and others old.
    let involved: Vec<usize> = (0..num).filter(|&i| !by_shard[i].is_empty()).collect();
    let _held = (involved.len() > 1).then(|| shards.lock_shards(involved));
    for (shard_idx, group) in by_shard.into_iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let record = mset_record(shards, &group);
        shards
            .shard(shard_idx)
            .execute_with_effects(move |store, fx| {
                if let Some(r) = record {
                    fx.push_encoded(r);
                }
                store.string_mset(group)
            });
    }
    CommandResponse::ok()
}

fn multi_shard_msetnx(
    shards: &crate::shard::ShardManager,
    pairs: Vec<(Bytes, Bytes)>,
) -> CommandResponse {
    let num = shards.num_shards();
    let mut by_shard: Vec<Vec<(Bytes, Bytes)>> = vec![Vec::new(); num];
    for (key, value) in pairs {
        let slot = shards.shard_index_for_key(&key);
        by_shard[slot].push((key, value));
    }
    // All-or-nothing: every shard involved is held from the check to the
    // last write, so no key can appear in between.
    let _held = shards.lock_shards(
        by_shard
            .iter()
            .enumerate()
            .filter(|(_, g)| !g.is_empty())
            .map(|(i, _)| i),
    );

    // Phase 1: all keys must be absent.
    for (shard_idx, group) in by_shard.iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let keys: Vec<Bytes> = group.iter().map(|(k, _)| k.clone()).collect();
        let any = shards.shard(shard_idx).execute(move |store| {
            for k in &keys {
                if store.exists(k) {
                    return CommandResponse::integer(1);
                }
            }
            CommandResponse::integer(0)
        });
        if matches!(any, CommandResponse::Integer(1)) {
            return CommandResponse::integer(0);
        }
    }

    // Phase 2: set on every shard.
    for (shard_idx, group) in by_shard.into_iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let record = mset_record(shards, &group);
        shards
            .shard(shard_idx)
            .execute_with_effects(move |store, fx| {
                if let Some(r) = record {
                    fx.push_encoded(r);
                }
                store.string_mset(group)
            });
    }
    CommandResponse::integer(1)
}

/// `[MATCH pattern] [COUNT n] [TYPE t]` of the SCAN family (`TYPE` only for
/// `SCAN` itself).
#[allow(clippy::type_complexity)]
fn parse_scan_options(
    opts: &[Bytes],
    allow_type: bool,
) -> Result<(Option<String>, usize, Option<String>), CommandResponse> {
    let mut pattern = None;
    let mut count = 10usize;
    let mut type_filter = None;
    let mut i = 0;
    while i < opts.len() {
        let opt = String::from_utf8_lossy(&opts[i]).to_ascii_uppercase();
        let Some(val) = opts.get(i + 1) else {
            return Err(CommandResponse::syntax_error());
        };
        match opt.as_str() {
            "MATCH" => pattern = Some(String::from_utf8_lossy(val).to_string()),
            "COUNT" => {
                // Only a hint: a huge one is clamped, never allocated.
                count = match std::str::from_utf8(val).ok().map(|v| v.parse::<i128>()) {
                    Some(Ok(n)) if n >= 1 => usize::try_from(n).unwrap_or(usize::MAX),
                    Some(Ok(_)) => return Err(CommandResponse::syntax_error()),
                    _ => {
                        return Err(CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        ))
                    }
                }
            }
            "TYPE" if allow_type => {
                type_filter = Some(String::from_utf8_lossy(val).to_ascii_lowercase())
            }
            _ => return Err(CommandResponse::syntax_error()),
        }
        i += 2;
    }
    Ok((pattern, count, type_filter))
}

fn multi_shard_scan(
    shards: &crate::shard::ShardManager,
    cursor: u64,
    pattern: Option<String>,
    count: usize,
    type_filter: Option<String>,
) -> CommandResponse {
    let num = shards.num_shards();
    if num == 0 {
        return CommandResponse::array(vec![
            CommandResponse::bulk(Bytes::from_static(b"0")),
            CommandResponse::array(vec![]),
        ]);
    }

    let (mut shard_idx, local) = if cursor == 0 {
        (0usize, 0usize)
    } else {
        unpack_scan_cursor(cursor)
    };

    if shard_idx >= num {
        return CommandResponse::array(vec![
            CommandResponse::bulk(Bytes::from_static(b"0")),
            CommandResponse::array(vec![]),
        ]);
    }

    // Scan the current shard; if it finishes, advance to the next shard (or 0 = done).
    let pat = pattern.clone();
    let resp = shards.shard(shard_idx).execute(move |store| {
        let (next_local, keys) =
            store.scan_typed(local, pat.as_deref(), type_filter.as_deref(), count);
        // Pack next_local into a temporary Integer so we can unwrap outside.
        // We return Array [next_local_as_int, ...keys as bulk]
        let mut items = Vec::with_capacity(1 + keys.len());
        items.push(CommandResponse::integer(next_local as i64));
        items.extend(keys.into_iter().map(CommandResponse::bulk));
        CommandResponse::Array(items)
    });

    let (next_local, keys) = match resp {
        CommandResponse::Array(mut items) if !items.is_empty() => {
            let next = match items.remove(0) {
                CommandResponse::Integer(n) => n.max(0) as usize,
                _ => 0,
            };
            (next, items)
        }
        _ => (0, Vec::new()),
    };

    let next_cursor = if next_local != 0 {
        pack_scan_cursor(shard_idx, next_local)
    } else {
        // Finished this shard — move to the next one, or complete.
        shard_idx += 1;
        if shard_idx >= num {
            0
        } else {
            pack_scan_cursor(shard_idx, 0)
        }
    };

    CommandResponse::array(vec![
        CommandResponse::bulk(Bytes::from(next_cursor.to_string())),
        CommandResponse::Array(keys),
    ])
}

fn multi_shard_randomkey(shards: &crate::shard::ShardManager) -> CommandResponse {
    let num = shards.num_shards();
    if num == 0 {
        return CommandResponse::nil();
    }
    // Sample a few random shards so we don't always hit shard 0.
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as usize;
    for attempt in 0..num {
        let idx = (seed + attempt) % num;
        let resp = shards.shard(idx).execute(|store| match store.random_key() {
            Some(k) => CommandResponse::bulk(k),
            None => CommandResponse::nil(),
        });
        if !matches!(resp, CommandResponse::Nil) {
            return resp;
        }
    }
    CommandResponse::nil()
}

fn parse_score_range(min: &Bytes, max: &Bytes) -> Result<ZRangeBy, CommandResponse> {
    match (ScoreBound::parse(min), ScoreBound::parse(max)) {
        (Some(a), Some(b)) => Ok(ZRangeBy::Score(a, b)),
        _ => Err(CommandResponse::error("ERR min or max is not a float")),
    }
}

fn parse_lex_range(min: &Bytes, max: &Bytes) -> Result<ZRangeBy, CommandResponse> {
    match (LexBound::parse(min), LexBound::parse(max)) {
        (Some(a), Some(b)) => Ok(ZRangeBy::Lex(a, b)),
        _ => Err(CommandResponse::error(
            "ERR min or max not valid string range item",
        )),
    }
}

/// Every `ZRANGE`-family form, as one query:
/// `ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count]
/// [WITHSCORES]` (Redis 6.2), and the older `ZREVRANGE`,
/// `Z[REV]RANGEBYSCORE key min max [WITHSCORES] [LIMIT o c]` and
/// `Z[REV]RANGEBYLEX key min max [LIMIT o c]`.
fn parse_zrange(name: &str, args: &[Bytes]) -> Result<(ZRangeQuery, bool), CommandResponse> {
    #[derive(PartialEq)]
    enum By {
        Rank,
        Score,
        Lex,
    }
    let (mut by, mut rev) = match name {
        "ZRANGE" => (By::Rank, false),
        "ZREVRANGE" => (By::Rank, true),
        "ZRANGEBYSCORE" => (By::Score, false),
        "ZREVRANGEBYSCORE" => (By::Score, true),
        "ZRANGEBYLEX" => (By::Lex, false),
        _ => (By::Lex, true),
    };
    let unified = name == "ZRANGE";
    let mut withscores = false;
    let mut limit = None;
    let mut i = 3;
    while i < args.len() {
        match String::from_utf8_lossy(&args[i])
            .to_ascii_uppercase()
            .as_str()
        {
            "WITHSCORES" if name != "ZRANGEBYLEX" && name != "ZREVRANGEBYLEX" => withscores = true,
            "BYSCORE" if unified && by == By::Rank => by = By::Score,
            "BYLEX" if unified && by == By::Rank => by = By::Lex,
            "REV" if unified => rev = true,
            "LIMIT" if name != "ZREVRANGE" => {
                let (Some(o), Some(c)) = (
                    args.get(i + 1).and_then(|a| parse_i64_arg(a)),
                    args.get(i + 2).and_then(|a| parse_i64_arg(a)),
                ) else {
                    return Err(if i + 2 >= args.len() {
                        CommandResponse::syntax_error()
                    } else {
                        CommandResponse::error("ERR value is not an integer or out of range")
                    });
                };
                limit = Some((o, c));
                i += 2;
            }
            _ => return Err(CommandResponse::syntax_error()),
        }
        i += 1;
    }
    if limit.is_some() && by == By::Rank {
        return Err(CommandResponse::error(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }
    if withscores && by == By::Lex {
        return Err(CommandResponse::error(
            "ERR syntax error, WITHSCORES not supported in combination with BYLEX",
        ));
    }
    // Reverse score/lex forms take the range as `max min`.
    let (lo, hi) = if rev && by != By::Rank {
        (&args[2], &args[1])
    } else {
        (&args[1], &args[2])
    };
    let by = match by {
        By::Rank => match (parse_i64_arg(&args[1]), parse_i64_arg(&args[2])) {
            (Some(a), Some(b)) => ZRangeBy::Rank(a, b),
            _ => {
                return Err(CommandResponse::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        },
        By::Score => parse_score_range(lo, hi)?,
        By::Lex => parse_lex_range(lo, hi)?,
    };
    Ok((ZRangeQuery { by, rev, limit }, withscores))
}

#[cfg(test)]
mod sec_tests {
    use super::*;
    use crate::ShardManager;
    use solikv_pubsub::PubSubBroker;
    use std::sync::Arc;

    fn engine() -> Arc<CommandEngine> {
        let shards = Arc::new(ShardManager::new(4));
        let pubsub = Arc::new(PubSubBroker::new());
        let engine = Arc::new(CommandEngine::new(shards, pubsub));
        engine.init_self_ref(Arc::downgrade(&engine));
        engine
    }

    fn err_string(resp: &CommandResponse) -> String {
        match resp {
            CommandResponse::Error(s) => s.clone(),
            other => panic!("expected error, got {:?}", other),
        }
    }

    /// `SAVE` used to hardcode `data`/`dump`, ignoring `--dir` and
    /// `--dbfilename`. A server started with a custom data directory therefore
    /// wrote its background and shutdown snapshots there, while an explicit
    /// `SAVE` wrote to `./data` relative to the process CWD — a snapshot the
    /// next startup never loads, from a command that reports success.
    #[test]
    fn save_writes_to_the_configured_dir_and_basename() {
        let dir = std::env::temp_dir().join(format!("solikv-save-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let shards = Arc::new(ShardManager::new(2));
        let pubsub = Arc::new(PubSubBroker::new());
        let engine =
            Arc::new(CommandEngine::new(shards, pubsub).with_rdb_path(dir.clone(), "snapshot"));
        engine.init_self_ref(Arc::downgrade(&engine));

        engine.execute("SET", &[Bytes::from("k"), Bytes::from("v")]);
        let resp = engine.execute("SAVE", &[]);
        assert!(!resp.is_error(), "SAVE failed: {:?}", resp);

        let written: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            written.iter().any(|name| name == "snapshot.rdb"),
            "expected snapshot.rdb in the configured dir, found {:?}",
            written
        );
        assert!(
            !written.iter().any(|name| name.starts_with("dump-")),
            "SAVE still used the hardcoded basename: {:?}",
            written
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    // SEC-012: numkeys = usize::MAX must not panic the connection via
    // wrapping `numkeys + 2`; the dispatcher returns a clean error.
    #[test]
    fn test_eval_numkeys_overflow_returns_error() {
        let e = engine();
        let resp = e.execute(
            "EVAL",
            &[Bytes::from("return 1"), Bytes::from(usize::MAX.to_string())],
        );
        let msg = err_string(&resp);
        assert!(msg.contains("Number of keys"), "unexpected error: {}", msg);
    }

    #[test]
    fn test_evalsha_numkeys_overflow_returns_error() {
        let e = engine();
        let resp = e.execute(
            "EVALSHA",
            &[
                // SHA never matches anyway; the numkeys parse happens first
                // only on EVAL. EVALSHA short-circuits on NOSCRIPT before the
                // numkeys parse, so test EVAL specifically — but verify
                // EVALSHA at least never panics on usize::MAX.
                Bytes::from("0000000000000000000000000000000000000000"),
                Bytes::from(usize::MAX.to_string()),
            ],
        );
        // Either NOSCRIPT or "Number of keys" is acceptable; a panic is not.
        let msg = err_string(&resp);
        assert!(msg.contains("NOSCRIPT") || msg.contains("Number of keys"));
    }

    #[test]
    fn test_zunionstore_numkeys_overflow_returns_error() {
        let e = engine();
        let resp = e.execute(
            "ZUNIONSTORE",
            &[
                Bytes::from("dest"),
                Bytes::from(usize::MAX.to_string()),
                Bytes::from("src1"),
            ],
        );
        let msg = err_string(&resp);
        assert!(
            msg.contains("Number of keys") || msg.to_lowercase().contains("arity"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn test_zinterstore_numkeys_overflow_returns_error() {
        let e = engine();
        let resp = e.execute(
            "ZINTERSTORE",
            &[
                Bytes::from("dest"),
                Bytes::from(usize::MAX.to_string()),
                Bytes::from("src1"),
            ],
        );
        let msg = err_string(&resp);
        assert!(
            msg.contains("Number of keys") || msg.to_lowercase().contains("arity"),
            "unexpected error: {}",
            msg
        );
    }

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    /// SETRANGE with an offset near usize::MAX wrapped `offset + len` and
    /// then zero-padded a byte at a time while holding the shard lock.
    #[test]
    fn setrange_huge_offset_is_refused() {
        let e = engine();
        for off in ["18446744073709551615", "536870912"] {
            let r = e.execute("SETRANGE", &[b("k"), b(off), b("x")]);
            assert!(err_string(&r).contains("maximum allowed size"), "{off}");
        }
        assert!(matches!(
            e.execute("SETRANGE", &[b("k"), b("3"), b("x")]),
            CommandResponse::Integer(4)
        ));
    }

    #[test]
    fn bit_offsets_are_capped_at_2_pow_32() {
        let e = engine();
        for cmd in ["SETBIT", "GETBIT"] {
            let mut args = vec![b("bm"), b("4294967296")];
            if cmd == "SETBIT" {
                args.push(b("1"));
            }
            assert!(err_string(&e.execute(cmd, &args)).contains("bit offset"));
        }
        assert!(matches!(
            e.execute("SETBIT", &[b("bm"), b("7"), b("1")]),
            CommandResponse::Integer(0)
        ));
    }

    #[test]
    fn bf_reserve_refuses_filters_it_cannot_afford() {
        let e = engine();
        let r = e.execute(
            "BF.RESERVE",
            &[b("bf"), b("0.0000001"), b("1152921504606846976")],
        );
        assert!(err_string(&r).contains("maximum size"));
        assert!(e
            .execute("BF.RESERVE", &[b("bf"), b("1.5"), b("100")])
            .is_error());
        assert!(e
            .execute("BF.RESERVE", &[b("bf"), b("0.01"), b("0")])
            .is_error());
        assert!(matches!(
            e.execute("BF.RESERVE", &[b("bf"), b("0.01"), b("1000")]),
            CommandResponse::Ok
        ));
    }

    /// `secs * 1000` wrapped in release builds, so a huge EX set a tiny TTL.
    #[test]
    fn overflowing_ttls_are_invalid_expire_times() {
        let e = engine();
        let huge = "18446744073709551615";
        let secs = "9223372036854775"; // * 1000 overflows the i64 range
        let cases: Vec<(&str, Vec<Bytes>)> = vec![
            ("SET", vec![b("k"), b("v"), b("EX"), b(huge)]),
            ("SET", vec![b("k"), b("v"), b("EX"), b(secs)]),
            ("SET", vec![b("k"), b("v"), b("PX"), b(huge)]),
            ("SET", vec![b("k"), b("v"), b("EXAT"), b(huge)]),
            ("SETEX", vec![b("k"), b(huge), b("v")]),
            ("PSETEX", vec![b("k"), b(huge), b("v")]),
            ("GETEX", vec![b("k"), b("EX"), b(huge)]),
        ];
        // Beyond i64 the argument is not an integer (Redis' wording); within
        // it but past the representable range, an invalid expire time.
        let refused = |r: &CommandResponse| {
            let m = err_string(r);
            m.contains("invalid expire time") || m.contains("not an integer")
        };
        for (cmd, args) in cases {
            let r = e.execute(cmd, &args);
            assert!(refused(&r), "{cmd} {args:?}");
        }
        assert!(
            err_string(&e.execute("SET", &[b("k"), b("v"), b("EX"), b(secs)]))
                .contains("invalid expire time")
        );
        e.execute("SET", &[b("k"), b("v")]);
        for (cmd, arg) in [("EXPIRE", huge), ("PEXPIRE", huge), ("EXPIREAT", huge)] {
            let r = e.execute(cmd, &[b("k"), b(arg)]);
            assert!(refused(&r), "{cmd}");
        }
        for (cmd, arg) in [("EXPIRE", secs), ("EXPIREAT", secs)] {
            let r = e.execute(cmd, &[b("k"), b(arg)]);
            assert!(err_string(&r).contains("invalid expire time"), "{cmd}");
        }
        // The key is untouched by the refused commands.
        assert!(matches!(
            e.execute("TTL", &[b("k")]),
            CommandResponse::Integer(-1)
        ));
    }

    #[test]
    fn huge_counts_do_not_preallocate() {
        let e = engine();
        e.execute("RPUSH", &[b("l"), b("a"), b("b")]);
        let r = e.execute("LPOP", &[b("l"), b("18446744073709551615")]);
        assert!(
            matches!(r, CommandResponse::Array(ref v) if v.len() == 2),
            "{r:?}"
        );
        e.execute("SET", &[b("k"), b("v")]);
        let r = e.execute("SCAN", &[b("0"), b("COUNT"), b("18446744073709551615")]);
        assert!(!r.is_error(), "{r:?}");
    }

    /// KEYS / FLUSHALL / SAVE used `block_in_place`, which on the solo
    /// runtime hands the worker to a second thread and lets it into the
    /// unlocked store. The debug guard in `SoloStore` turns any such overlap
    /// into a panic, so hammering the store from other tasks while KEYS runs
    /// must stay quiet.
    #[test]
    fn solo_heavy_commands_stay_on_the_worker() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let shards = Arc::new(ShardManager::solo_plain());
        let pubsub = Arc::new(PubSubBroker::new());
        let e = Arc::new(CommandEngine::new(shards, pubsub));
        e.init_self_ref(Arc::downgrade(&e));
        for i in 0..2000 {
            e.execute("SET", &[b(&format!("k{i}")), b("v")]);
        }
        rt.block_on(async {
            let mut tasks = Vec::new();
            for t in 0..8 {
                let e = e.clone();
                tasks.push(tokio::spawn(async move {
                    for i in 0..200 {
                        e.execute("SET", &[b(&format!("w{t}-{i}")), b("v")]);
                        if i % 50 == 0 {
                            e.execute("KEYS", &[b("*")]);
                            e.execute("FLUSHALL", &[]);
                        }
                        tokio::task::yield_now().await;
                    }
                }));
            }
            for t in tasks {
                t.await
                    .expect("a task panicked: the solo store was entered concurrently");
            }
        });
    }

    #[test]
    fn lua_memory_is_limited() {
        let shards = Arc::new(ShardManager::new(1));
        let pubsub = Arc::new(PubSubBroker::new());
        let e = Arc::new(CommandEngine::new(shards, pubsub).with_lua_memory_limit(8 * 1024 * 1024));
        e.init_self_ref(Arc::downgrade(&e));
        let r = e.execute(
            "EVAL",
            &[
                b("return string.len(string.rep('x', 64 * 1024 * 1024))"),
                b("0"),
            ],
        );
        assert!(r.is_error(), "{r:?}");
        // The VM is still usable afterwards.
        let r = e.execute("EVAL", &[b("return 1 + 1"), b("0")]);
        assert!(matches!(r, CommandResponse::Integer(2)), "{r:?}");
    }

    #[test]
    fn lua_refuses_binary_chunks() {
        let e = engine();
        let r = e.execute("EVAL", &[b("\x1bLuaT\x00"), b("0")]);
        assert!(r.is_error(), "{r:?}");
    }

    /// Solo mode: unlocked store still serves SET/GET correctly.
    #[test]
    fn test_solo_set_get() {
        let shards = Arc::new(ShardManager::solo_plain());
        assert!(shards.is_solo());
        let pubsub = Arc::new(PubSubBroker::new());
        let e = Arc::new(CommandEngine::new(shards, pubsub));
        e.init_self_ref(Arc::downgrade(&e));
        assert!(matches!(
            e.execute("SET", &[Bytes::from("k"), Bytes::from("v")]),
            CommandResponse::Ok
        ));
        match e.execute("GET", &[Bytes::from("k")]) {
            CommandResponse::BulkString(b) => assert_eq!(b.as_ref(), b"v"),
            other => panic!("expected bulk, got {:?}", other),
        }
    }

    /// Multi-shard MSET/MGET must route each key to its own shard (not only the first).
    #[test]
    fn test_multi_shard_mset_mget() {
        let e = engine();
        // Use many distinct keys so they land on different shards with high probability.
        let mut args = Vec::new();
        for i in 0..32 {
            args.push(Bytes::from(format!("mk{i}")));
            args.push(Bytes::from(format!("v{i}")));
        }
        assert!(matches!(e.execute("MSET", &args), CommandResponse::Ok));

        let keys: Vec<Bytes> = (0..32).map(|i| Bytes::from(format!("mk{i}"))).collect();
        let resp = e.execute("MGET", &keys);
        match resp {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 32);
                for (i, item) in items.iter().enumerate() {
                    match item {
                        CommandResponse::BulkString(b) => {
                            assert_eq!(b.as_ref(), format!("v{i}").as_bytes());
                        }
                        other => panic!("key mk{i} missing or wrong: {:?}", other),
                    }
                }
            }
            other => panic!("expected array, got {:?}", other),
        }
    }

    #[test]
    fn test_multi_shard_scan_visits_all_shards() {
        let e = engine();
        for i in 0..40 {
            e.execute("SET", &[Bytes::from(format!("sk{i}")), Bytes::from("1")]);
        }

        let mut all = std::collections::HashSet::new();
        let mut cursor = 0u64;
        let mut steps = 0;
        loop {
            let resp = e.execute(
                "SCAN",
                &[
                    Bytes::from(cursor.to_string()),
                    Bytes::from("COUNT"),
                    Bytes::from("10"),
                ],
            );
            match resp {
                CommandResponse::Array(items) if items.len() == 2 => {
                    cursor = match &items[0] {
                        CommandResponse::BulkString(b) => {
                            std::str::from_utf8(b).unwrap().parse().unwrap()
                        }
                        _ => panic!("bad cursor"),
                    };
                    if let CommandResponse::Array(keys) = &items[1] {
                        for k in keys {
                            if let CommandResponse::BulkString(b) = k {
                                all.insert(b.clone());
                            }
                        }
                    }
                }
                other => panic!("unexpected SCAN response: {:?}", other),
            }
            steps += 1;
            if cursor == 0 || steps > 100 {
                break;
            }
        }
        assert_eq!(all.len(), 40, "SCAN must return keys from all shards");
    }

    #[test]
    fn test_scan_cursor_pack_unpack() {
        assert_eq!(unpack_scan_cursor(0), (0, 0));
        let c = pack_scan_cursor(3, 42);
        assert_eq!(unpack_scan_cursor(c), (3, 42));
        // shard boundary start
        let c = pack_scan_cursor(1, 0);
        assert_eq!(unpack_scan_cursor(c), (1, 0));
    }
}
