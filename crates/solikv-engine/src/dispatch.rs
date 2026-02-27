use bytes::Bytes;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Instant;

use solikv_core::types::*;
use solikv_core::zset_ops::Aggregate;

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

use crate::lua::ScriptCache;
use crate::shard::ShardManager;

/// The central command engine that routes commands to appropriate shards.
pub struct CommandEngine {
    pub shards: Arc<ShardManager>,
    pub pubsub: Arc<PubSubBroker>,
    aof: Option<AofWriter>,
    start_time: Instant,
    pub(crate) script_cache: ScriptCache,
    weak_self: OnceLock<Weak<CommandEngine>>,
    notify_flags: Arc<AtomicU16>,
}

impl CommandEngine {
    pub fn new(shards: Arc<ShardManager>, pubsub: Arc<PubSubBroker>) -> Self {
        Self {
            shards,
            pubsub,
            aof: None,
            start_time: Instant::now(),
            script_cache: ScriptCache::new(),
            weak_self: OnceLock::new(),
            notify_flags: Arc::new(AtomicU16::new(0)),
        }
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

    pub fn with_aof(mut self, aof: AofWriter) -> Self {
        self.aof = Some(aof);
        self
    }

    /// Execute a parsed command. Returns the response.
    pub fn execute(&self, name: &str, args: &[Bytes]) -> CommandResponse {
        let response = self.dispatch(name, args);

        // AOF: log write commands that succeeded (lock-free channel send)
        if let Some(aof) = &self.aof {
            if !response.is_error() {
                if name == "EVALSHA" {
                    // Convert EVALSHA to EVAL in AOF so replay works without script cache
                    if let Some(sha) = args.first() {
                        let sha_str = std::str::from_utf8(sha).unwrap_or("");
                        if let Some(source) = self.script_cache.get(sha_str) {
                            let mut eval_args = vec![Bytes::from(source)];
                            eval_args.extend_from_slice(&args[1..]);
                            aof.log("EVAL", &eval_args);
                        }
                    }
                } else if is_write_command(name) {
                    aof.log(name, args);
                }
            }
        }

        // Keyspace notifications: emit after successful write commands
        if !response.is_error() {
            self.emit_notifications(name, args, &response);
        }

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
        let key_str = std::str::from_utf8(key).unwrap_or("<binary>");

        // __keyspace@0__:<key> → event name as message
        if flags & NOTIFY_KEYSPACE != 0 {
            let channel = Bytes::from(format!("__keyspace@0__:{}", key_str));
            self.pubsub.publish(channel, Bytes::from(event.to_string()));
        }

        // __keyevent@0__:<event> → key name as message
        if flags & NOTIFY_KEYEVENT != 0 {
            let channel = Bytes::from(format!("__keyevent@0__:{}", event));
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
            "EXPIRE" | "PEXPIRE" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_GENERIC, "expire", key);
                }
            }
            "EXPIREAT" | "PEXPIREAT" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_GENERIC, "expire", key);
                }
            }
            "PERSIST" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_GENERIC, "persist", key);
                }
            }
            "RENAME" | "RENAMENX" => {
                if args.len() >= 2 {
                    self.notify_keyspace_event(NOTIFY_GENERIC, "rename_from", &args[0]);
                    self.notify_keyspace_event(NOTIFY_GENERIC, "rename_to", &args[1]);
                }
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
            "SMOVE" => {
                if args.len() >= 2 {
                    self.notify_keyspace_event(NOTIFY_SET, "srem", &args[0]);
                    self.notify_keyspace_event(NOTIFY_SET, "sadd", &args[1]);
                }
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
            "XGROUP" => {
                // The key is the second argument (after CREATE/DESTROY/etc.)
                if args.len() >= 2 {
                    self.notify_keyspace_event(NOTIFY_STREAM, "xgroup-create", &args[1]);
                }
            }

            // Bitmap commands → $ flag
            "SETBIT" => {
                if let Some(key) = args.first() {
                    self.notify_keyspace_event(NOTIFY_STRING, "setbit", key);
                }
            }
            "BITOP" => {
                if args.len() >= 2 {
                    self.notify_keyspace_event(NOTIFY_STRING, "bitop", &args[1]);
                }
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

    fn dispatch(&self, name: &str, args: &[Bytes]) -> CommandResponse {
        match name {
            // ---- Server commands ----
            "PING" => {
                if args.is_empty() {
                    CommandResponse::simple("PONG")
                } else {
                    CommandResponse::bulk(args[0].clone())
                }
            }
            "ECHO" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("echo");
                }
                CommandResponse::bulk(args[0].clone())
            }
            "COMMAND" => {
                // Simplified: just return OK for compatibility
                CommandResponse::ok()
            }
            "CONFIG" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("config");
                }
                let sub = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                match sub.as_str() {
                    "GET" => {
                        // Return empty array for unknown configs
                        if args.len() < 2 {
                            return CommandResponse::wrong_arity("config|get");
                        }
                        let param = std::str::from_utf8(&args[1]).unwrap_or("");
                        match param {
                            "save" => CommandResponse::array(vec![
                                CommandResponse::bulk_string("save"),
                                CommandResponse::bulk_string(""),
                            ]),
                            "databases" => CommandResponse::array(vec![
                                CommandResponse::bulk_string("databases"),
                                CommandResponse::bulk_string("16"),
                            ]),
                            "notify-keyspace-events" => {
                                let flags = self.notify_flags.load(Ordering::Relaxed);
                                CommandResponse::array(vec![
                                    CommandResponse::bulk_string("notify-keyspace-events"),
                                    CommandResponse::bulk(Bytes::from(flags_to_string(flags))),
                                ])
                            }
                            _ => CommandResponse::array(vec![]),
                        }
                    }
                    "SET" => {
                        if args.len() >= 3 {
                            let param = std::str::from_utf8(&args[1]).unwrap_or("");
                            let value = std::str::from_utf8(&args[2]).unwrap_or("");
                            if param == "notify-keyspace-events" {
                                let flags = parse_notify_flags(value);
                                self.notify_flags.store(flags, Ordering::Relaxed);
                            }
                        }
                        CommandResponse::ok()
                    }
                    "RESETSTAT" => CommandResponse::ok(),
                    _ => CommandResponse::error(format!(
                        "ERR Unknown subcommand or wrong number of arguments for 'config|{}'",
                        sub.to_lowercase()
                    )),
                }
            }
            "INFO" => {
                let uptime = self.start_time.elapsed().as_secs();
                let info = format!(
                    "# Server\r\nsolikv_version:0.1.0\r\nuptime_in_seconds:{}\r\ntcp_port:6379\r\n\r\n# Keyspace\r\n",
                    uptime
                );
                CommandResponse::bulk(Bytes::from(info))
            }
            "DBSIZE" => {
                // Sum across all shards
                let shards = self.shards.clone();
                let num = shards.num_shards();
                let mut total = 0i64;
                for i in 0..num {
                    let shard = shards.shard(i).clone();
                    let resp =
                        shard.execute(|store| CommandResponse::integer(store.dbsize() as i64));
                    if let CommandResponse::Integer(n) = resp {
                        total += n;
                    }
                }
                CommandResponse::integer(total)
            }
            "FLUSHDB" | "FLUSHALL" => {
                let shards = self.shards.clone();
                let num = shards.num_shards();
                for i in 0..num {
                    let shard = shards.shard(i).clone();
                    shard.execute(|store| {
                        store.flush();
                        CommandResponse::ok()
                    });
                }
                CommandResponse::ok()
            }
            "SELECT" => {
                // We don't support multiple databases, but accept SELECT 0
                CommandResponse::ok()
            }
            "QUIT" => CommandResponse::ok(),
            "RESET" => CommandResponse::simple("RESET"),
            "CLIENT" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("client");
                }
                let sub = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                match sub.as_str() {
                    "SETNAME" => CommandResponse::ok(),
                    "GETNAME" => CommandResponse::nil(),
                    "ID" => CommandResponse::integer(1),
                    "LIST" => CommandResponse::bulk_string(""),
                    "INFO" => CommandResponse::bulk_string(""),
                    _ => CommandResponse::ok(),
                }
            }

            // ---- String commands ----
            "GET" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("get");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_get(&key))
            }
            "SET" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("set");
                }
                let key = args[0].clone();
                let value = args[1].clone();
                let mut expire_ms = None;
                let mut nx = false;
                let mut xx = false;
                let mut get = false;

                let mut i = 2;
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "EX" => {
                            i += 1;
                            if let Some(secs) = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok())
                            {
                                expire_ms = Some(secs * 1000);
                            } else {
                                return CommandResponse::syntax_error();
                            }
                        }
                        "PX" => {
                            i += 1;
                            if let Some(ms) = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok())
                            {
                                expire_ms = Some(ms);
                            } else {
                                return CommandResponse::syntax_error();
                            }
                        }
                        "EXAT" => {
                            i += 1;
                            if let Some(ts) = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok())
                            {
                                let now = now_millis();
                                let target = ts * 1000;
                                expire_ms = Some(target.saturating_sub(now));
                            } else {
                                return CommandResponse::syntax_error();
                            }
                        }
                        "PXAT" => {
                            i += 1;
                            if let Some(ts) = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok())
                            {
                                let now = now_millis();
                                expire_ms = Some(ts.saturating_sub(now));
                            } else {
                                return CommandResponse::syntax_error();
                            }
                        }
                        "NX" => nx = true,
                        "XX" => xx = true,
                        "GET" => get = true,
                        "KEEPTTL" => {} // TODO: implement
                        _ => return CommandResponse::syntax_error(),
                    }
                    i += 1;
                }

                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_set(key, value, expire_ms, nx, xx, get))
            }
            "SETNX" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("setnx");
                }
                let key = args[0].clone();
                let value = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_setnx(key, value))
            }
            "SETEX" => {
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
                let value = args[2].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_setex(key, seconds, value))
            }
            "PSETEX" => {
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
                let value = args[2].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_psetex(key, millis, value))
            }
            "GETSET" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("getset");
                }
                let key = args[0].clone();
                let value = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_getset(key, value))
            }
            "GETDEL" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("getdel");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_getdel(&key))
            }
            "GETEX" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("getex");
                }
                let key = args[0].clone();
                let mut expire_ms = None;
                let mut persist = false;
                let mut i = 1;
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "EX" => {
                            i += 1;
                            expire_ms = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok())
                                .map(|s| s * 1000);
                        }
                        "PX" => {
                            i += 1;
                            expire_ms = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok());
                        }
                        "PERSIST" => persist = true,
                        _ => return CommandResponse::syntax_error(),
                    }
                    i += 1;
                }
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_getex(&key, expire_ms, persist))
            }
            "MGET" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("mget");
                }
                // For single-shard simplicity, route all to shard of first key
                // TODO: proper multi-shard gather
                let keys: Vec<Bytes> = args.to_vec();
                let shard = self.shards.shard_for_key(&keys[0]).clone();
                shard.execute(move |store| store.string_mget(&keys))
            }
            "MSET" => {
                if args.len() < 2 || !args.len().is_multiple_of(2) {
                    return CommandResponse::wrong_arity("mset");
                }
                let pairs: Vec<(Bytes, Bytes)> = args
                    .chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                // Route to first key's shard for simplicity
                let shard = self.shards.shard_for_key(&pairs[0].0).clone();
                shard.execute(move |store| store.string_mset(pairs))
            }
            "MSETNX" => {
                if args.len() < 2 || !args.len().is_multiple_of(2) {
                    return CommandResponse::wrong_arity("msetnx");
                }
                let pairs: Vec<(Bytes, Bytes)> = args
                    .chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                let shard = self.shards.shard_for_key(&pairs[0].0).clone();
                shard.execute(move |store| store.string_msetnx(pairs))
            }
            "INCR" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("incr");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_incr(&key))
            }
            "DECR" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("decr");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_decr(&key))
            }
            "INCRBY" => {
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
            "DECRBY" => {
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
            "INCRBYFLOAT" => {
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
            "APPEND" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("append");
                }
                let key = args[0].clone();
                let value = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_append(&key, &value))
            }
            "STRLEN" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("strlen");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_strlen(&key))
            }
            "GETRANGE" | "SUBSTR" => {
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
            "SETRANGE" => {
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
            "DEL" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("del");
                }
                let mut total = 0i64;
                for key in args {
                    let k = key.clone();
                    let resp = self.shards.shard_for_key(&k).execute(move |store| {
                        CommandResponse::integer(if store.del(&k) { 1 } else { 0 })
                    });
                    if let CommandResponse::Integer(n) = resp {
                        total += n;
                    }
                }
                CommandResponse::integer(total)
            }
            "UNLINK" => {
                // Same as DEL for now (async deletion could be optimized later)
                if args.is_empty() {
                    return CommandResponse::wrong_arity("unlink");
                }
                let mut total = 0i64;
                for key in args {
                    let k = key.clone();
                    let resp = self.shards.shard_for_key(&k).execute(move |store| {
                        CommandResponse::integer(if store.del(&k) { 1 } else { 0 })
                    });
                    if let CommandResponse::Integer(n) = resp {
                        total += n;
                    }
                }
                CommandResponse::integer(total)
            }
            "EXISTS" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("exists");
                }
                let mut total = 0i64;
                for key in args {
                    let k = key.clone();
                    let resp = self.shards.shard_for_key(&k).execute(move |store| {
                        CommandResponse::integer(if store.exists(&k) { 1 } else { 0 })
                    });
                    if let CommandResponse::Integer(n) = resp {
                        total += n;
                    }
                }
                CommandResponse::integer(total)
            }
            "EXPIRE" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("expire");
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
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(if store.expire(&key, seconds * 1000) {
                        1
                    } else {
                        0
                    })
                })
            }
            "PEXPIRE" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("pexpire");
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
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(if store.expire(&key, millis) { 1 } else { 0 })
                })
            }
            "EXPIREAT" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("expireat");
                }
                let key = args[0].clone();
                let ts = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(t) => t,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(if store.expire_at(&key, ts * 1000) {
                        1
                    } else {
                        0
                    })
                })
            }
            "PEXPIREAT" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("pexpireat");
                }
                let key = args[0].clone();
                let ts = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(t) => t,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(if store.expire_at(&key, ts) { 1 } else { 0 })
                })
            }
            "TTL" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("ttl");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| CommandResponse::integer(store.ttl(&key)))
            }
            "PTTL" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("pttl");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| CommandResponse::integer(store.pttl(&key)))
            }
            "PERSIST" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("persist");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(if store.persist(&key) { 1 } else { 0 })
                })
            }
            "TYPE" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("type");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| CommandResponse::simple(store.key_type(&key)))
            }
            "RENAME" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("rename");
                }
                let from = args[0].clone();
                let to = args[1].clone();
                // Simple case: same shard
                self.shards.shard_for_key(&from).execute(move |store| {
                    match store.rename(&from, to) {
                        Ok(()) => CommandResponse::ok(),
                        Err(e) => CommandResponse::error(e),
                    }
                })
            }
            "RENAMENX" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("renamenx");
                }
                let from = args[0].clone();
                let to = args[1].clone();
                self.shards.shard_for_key(&from).execute(move |store| {
                    if store.exists(&to) {
                        return CommandResponse::integer(0);
                    }
                    match store.rename(&from, to) {
                        Ok(()) => CommandResponse::integer(1),
                        Err(e) => CommandResponse::error(e),
                    }
                })
            }
            "KEYS" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("keys");
                }
                let pattern = std::str::from_utf8(&args[0]).unwrap_or("*").to_string();
                // Gather from all shards
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
            }
            "SCAN" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("scan");
                }
                let cursor = std::str::from_utf8(&args[0])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let mut pattern = None;
                let mut count = 10usize;
                let mut i = 1;
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "MATCH" => {
                            i += 1;
                            pattern = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok())
                                .map(|s| s.to_string());
                        }
                        "COUNT" => {
                            i += 1;
                            count = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok())
                                .unwrap_or(10);
                        }
                        _ => {}
                    }
                    i += 1;
                }
                // Simple single-shard scan
                let shard = self.shards.shard(0).clone();
                let pat = pattern.clone();
                shard.execute(move |store| {
                    let (next, keys) = store.scan(cursor, pat.as_deref(), count);
                    CommandResponse::array(vec![
                        CommandResponse::bulk(Bytes::from(next.to_string())),
                        CommandResponse::array(
                            keys.into_iter().map(CommandResponse::bulk).collect(),
                        ),
                    ])
                })
            }
            "RANDOMKEY" => {
                let shard = self.shards.shard(0).clone();
                shard.execute(|store| match store.random_key() {
                    Some(k) => CommandResponse::bulk(k),
                    None => CommandResponse::nil(),
                })
            }
            "OBJECT" => {
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
                    "REFCOUNT" | "IDLETIME" | "FREQ" | "HELP" => CommandResponse::integer(0),
                    _ => CommandResponse::error("ERR Unknown subcommand"),
                }
            }
            "DUMP" | "RESTORE" => CommandResponse::error("ERR not implemented"),
            "WAIT" => CommandResponse::integer(0),

            // ---- List commands ----
            "LPUSH" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("lpush");
                }
                let key = args[0].clone();
                let values: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_lpush(&key, values))
            }
            "RPUSH" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("rpush");
                }
                let key = args[0].clone();
                let values: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_rpush(&key, values))
            }
            "LPOP" => {
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
            "RPOP" => {
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
            "LLEN" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("llen");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_llen(&key))
            }
            "LINDEX" => {
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
            "LSET" => {
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
            "LRANGE" => {
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
            "LINSERT" => {
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
            "LREM" => {
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
            "LTRIM" => {
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
            "HSET" => {
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
            "HMSET" => {
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
            "HGET" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("hget");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hget(&key, &field))
            }
            "HDEL" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("hdel");
                }
                let key = args[0].clone();
                let fields: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hdel(&key, &fields))
            }
            "HGETALL" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hgetall");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hgetall(&key))
            }
            "HMGET" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("hmget");
                }
                let key = args[0].clone();
                let fields: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hmget(&key, &fields))
            }
            "HINCRBY" => {
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
            "HINCRBYFLOAT" => {
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
            "HKEYS" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hkeys");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hkeys(&key))
            }
            "HVALS" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hvals");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hvals(&key))
            }
            "HLEN" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hlen");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hlen(&key))
            }
            "HEXISTS" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("hexists");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hexists(&key, &field))
            }
            "HSETNX" => {
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
            "HSCAN" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("hscan");
                }
                let key = args[0].clone();
                let cursor = std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let mut pattern = None;
                let mut count = 10usize;
                let mut i = 2;
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "MATCH" => {
                            i += 1;
                            pattern = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok())
                                .map(|s| s.to_string());
                        }
                        "COUNT" => {
                            i += 1;
                            count = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok())
                                .unwrap_or(10);
                        }
                        _ => {}
                    }
                    i += 1;
                }
                let pat = pattern.clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.hash_hscan(&key, cursor, pat.as_deref(), count))
            }

            // ---- Set commands ----
            "SADD" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("sadd");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_sadd(&key, members))
            }
            "SREM" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("srem");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_srem(&key, &members))
            }
            "SMEMBERS" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("smembers");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_smembers(&key))
            }
            "SISMEMBER" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("sismember");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_sismember(&key, &member))
            }
            "SCARD" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("scard");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.set_scard(&key))
            }
            "SINTER" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("sinter");
                }
                let keys: Vec<Bytes> = args.to_vec();
                let shard = self.shards.shard_for_key(&keys[0]).clone();
                shard.execute(move |store| store.set_sinter(&keys))
            }
            "SUNION" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("sunion");
                }
                let keys: Vec<Bytes> = args.to_vec();
                let shard = self.shards.shard_for_key(&keys[0]).clone();
                shard.execute(move |store| store.set_sunion(&keys))
            }
            "SDIFF" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("sdiff");
                }
                let keys: Vec<Bytes> = args.to_vec();
                let shard = self.shards.shard_for_key(&keys[0]).clone();
                shard.execute(move |store| store.set_sdiff(&keys))
            }
            "SPOP" => {
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
            "SRANDMEMBER" => {
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
            "SMOVE" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("smove");
                }
                let src = args[0].clone();
                let dst = args[1].clone();
                let member = args[2].clone();
                self.shards
                    .shard_for_key(&src)
                    .execute(move |store| store.set_smove(&src, dst, member))
            }

            // ---- Sorted Set commands ----
            "ZADD" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zadd");
                }
                let key = args[0].clone();
                let mut nx = false;
                let mut xx = false;
                let mut gt = false;
                let mut lt = false;
                let mut ch = false;
                let mut i = 1;

                // Parse flags
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
                        "GT" => {
                            gt = true;
                            i += 1;
                        }
                        "LT" => {
                            lt = true;
                            i += 1;
                        }
                        "CH" => {
                            ch = true;
                            i += 1;
                        }
                        _ => break, // Start of score-member pairs
                    }
                }

                let remaining = &args[i..];
                if remaining.len() < 2 || !remaining.len().is_multiple_of(2) {
                    return CommandResponse::wrong_arity("zadd");
                }

                let pairs: Vec<(f64, Bytes)> = remaining
                    .chunks(2)
                    .filter_map(|c| {
                        let score = std::str::from_utf8(&c[0]).ok()?.parse::<f64>().ok()?;
                        Some((score, c[1].clone()))
                    })
                    .collect();

                if pairs.is_empty() {
                    return CommandResponse::error("ERR value is not a valid float");
                }

                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zadd(&key, pairs, nx, xx, gt, lt, ch))
            }
            "ZREM" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("zrem");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zrem(&key, &members))
            }
            "ZSCORE" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("zscore");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zscore(&key, &member))
            }
            "ZCARD" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("zcard");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zcard(&key))
            }
            "ZRANK" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("zrank");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zrank(&key, &member))
            }
            "ZREVRANK" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("zrevrank");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zrevrank(&key, &member))
            }
            "ZINCRBY" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("zincrby");
                }
                let key = args[0].clone();
                let delta = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    Some(d) => d,
                    None => return CommandResponse::error("ERR value is not a valid float"),
                };
                let member = args[2].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zincrby(&key, delta, member))
            }
            "ZCOUNT" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("zcount");
                }
                let key = args[0].clone();
                let min = parse_score_bound(&args[1], f64::NEG_INFINITY);
                let max = parse_score_bound(&args[2], f64::INFINITY);
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zcount(&key, min, max))
            }
            "ZRANGE" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zrange");
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
                let withscores = args
                    .get(3)
                    .map(|b| std::str::from_utf8(b).unwrap_or("").to_uppercase() == "WITHSCORES")
                    .unwrap_or(false);
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zrange(&key, start, stop, withscores))
            }
            "ZREVRANGE" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zrevrange");
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
                let withscores = args
                    .get(3)
                    .map(|b| std::str::from_utf8(b).unwrap_or("").to_uppercase() == "WITHSCORES")
                    .unwrap_or(false);
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zrevrange(&key, start, stop, withscores))
            }
            "ZRANGEBYSCORE" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zrangebyscore");
                }
                let key = args[0].clone();
                let min = parse_score_bound(&args[1], f64::NEG_INFINITY);
                let max = parse_score_bound(&args[2], f64::INFINITY);
                let mut withscores = false;
                let mut offset = 0;
                let mut count = None;
                let mut i = 3;
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "WITHSCORES" => withscores = true,
                        "LIMIT" => {
                            i += 1;
                            offset = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok())
                                .unwrap_or(0);
                            i += 1;
                            count = args
                                .get(i)
                                .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok());
                        }
                        _ => {}
                    }
                    i += 1;
                }
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zrangebyscore(&key, min, max, withscores, offset, count)
                })
            }
            "ZPOPMIN" => {
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
            "ZPOPMAX" => {
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
            "ZUNIONSTORE" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zunionstore");
                }
                let dest = args[0].clone();
                let numkeys = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    Some(n) => n,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                if args.len() < 2 + numkeys {
                    return CommandResponse::wrong_arity("zunionstore");
                }
                let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
                let mut weights = Vec::new();
                let mut aggregate = Aggregate::Sum;
                let mut i = 2 + numkeys;
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "WEIGHTS" => {
                            i += 1;
                            while i < args.len() {
                                if let Some(w) = std::str::from_utf8(&args[i])
                                    .ok()
                                    .and_then(|s| s.parse::<f64>().ok())
                                {
                                    weights.push(w);
                                    i += 1;
                                } else {
                                    break;
                                }
                            }
                            continue;
                        }
                        "AGGREGATE" => {
                            i += 1;
                            if let Some(agg) = args.get(i).and_then(|b| std::str::from_utf8(b).ok())
                            {
                                aggregate = match agg.to_uppercase().as_str() {
                                    "MIN" => Aggregate::Min,
                                    "MAX" => Aggregate::Max,
                                    _ => Aggregate::Sum,
                                };
                            }
                        }
                        _ => {}
                    }
                    i += 1;
                }
                let shard = self.shards.shard_for_key(&dest).clone();
                shard
                    .execute(move |store| store.zset_zunionstore(&dest, &keys, &weights, aggregate))
            }
            "ZINTERSTORE" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zinterstore");
                }
                let dest = args[0].clone();
                let numkeys = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    Some(n) => n,
                    None => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                };
                if args.len() < 2 + numkeys {
                    return CommandResponse::wrong_arity("zinterstore");
                }
                let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
                let mut weights = Vec::new();
                let mut aggregate = Aggregate::Sum;
                let mut i = 2 + numkeys;
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "WEIGHTS" => {
                            i += 1;
                            while i < args.len() {
                                if let Some(w) = std::str::from_utf8(&args[i])
                                    .ok()
                                    .and_then(|s| s.parse::<f64>().ok())
                                {
                                    weights.push(w);
                                    i += 1;
                                } else {
                                    break;
                                }
                            }
                            continue;
                        }
                        "AGGREGATE" => {
                            i += 1;
                            if let Some(agg) = args.get(i).and_then(|b| std::str::from_utf8(b).ok())
                            {
                                aggregate = match agg.to_uppercase().as_str() {
                                    "MIN" => Aggregate::Min,
                                    "MAX" => Aggregate::Max,
                                    _ => Aggregate::Sum,
                                };
                            }
                        }
                        _ => {}
                    }
                    i += 1;
                }
                let shard = self.shards.shard_for_key(&dest).clone();
                shard
                    .execute(move |store| store.zset_zinterstore(&dest, &keys, &weights, aggregate))
            }
            "ZSCAN" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("zscan");
                }
                let key = args[0].clone();
                let cursor = std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let count = 10;
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.zset_zscan(&key, cursor, None, count))
            }

            // ---- Geospatial commands ----
            "GEOADD" => {
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
            "GEOPOS" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("geopos");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.geo_pos(&key, &members))
            }
            "GEOHASH" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("geohash");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.geo_hash(&key, &members))
            }
            "GEODIST" => {
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
            "GEOSEARCH" => {
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
            "GEOSEARCHSTORE" => {
                // GEOSEARCHSTORE dest src [FROMMEMBER member | FROMLONLAT lon lat]
                //   [BYRADIUS radius unit | BYBOX width height unit]
                //   [ASC|DESC] [COUNT count [ANY]] [STOREDIST]
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("geosearchstore");
                }
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
                shard.execute(move |store| {
                    store.geo_search_store(&dest, &src, &from, &by, sort_asc, count, store_dist)
                })
            }

            // ---- Stream commands ----
            "XADD" => {
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
            "XLEN" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("xlen");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.stream_xlen(&key))
            }
            "XRANGE" => {
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
            "XREVRANGE" => {
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
            "XREAD" => {
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
            "XDEL" => {
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
            "XTRIM" => {
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
            "XINFO" => {
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
            "XGROUP" => {
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
            "XREADGROUP" => {
                // XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]
                if args.len() < 7 {
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
                    let resp = self.shards.shard_for_key(&k).execute(move |store| {
                        store.stream_xreadgroup_single(&k, &g, &c, id_input, count)
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
            "XACK" => {
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
            "XPENDING" => {
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
            "PUBLISH" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("publish");
                }
                let channel = args[0].clone();
                let message = args[1].clone();
                let count = self.pubsub.publish(channel, message);
                CommandResponse::integer(count as i64)
            }
            "PUBSUB" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("pubsub");
                }
                let sub = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                match sub.as_str() {
                    "CHANNELS" => {
                        let channels = self.pubsub.channels();
                        CommandResponse::array(
                            channels.into_iter().map(CommandResponse::bulk).collect(),
                        )
                    }
                    "NUMSUB" => {
                        let subs = self.pubsub.numsub();
                        let mut result = Vec::new();
                        for (ch, count) in subs {
                            result.push(CommandResponse::bulk(ch));
                            result.push(CommandResponse::integer(count as i64));
                        }
                        CommandResponse::array(result)
                    }
                    _ => CommandResponse::error("ERR Unknown PUBSUB subcommand"),
                }
            }

            // ---- Persistence commands ----
            "SAVE" | "BGSAVE" => {
                let dir = std::path::Path::new("data");
                let shards = self.shards.clone();
                let num = shards.num_shards();
                if let Err(e) = solikv_persist::save_all_shards(dir, "dump", num, |idx, f| {
                    shards.shard(idx).with_store(|store| f(store))
                }) {
                    return CommandResponse::error(format!("ERR RDB save failed: {}", e));
                }
                tracing::info!("RDB saved {} shards to {:?}", num, dir);
                CommandResponse::ok()
            }
            "BGREWRITEAOF" => {
                // Rewrite AOF: not implemented yet, return OK for compatibility
                CommandResponse::ok()
            }

            // ---- Scripting commands ----
            "EVAL" => {
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
                if args.len() < 2 + numkeys {
                    return CommandResponse::error(
                        "ERR Number of keys can't be greater than number of args",
                    );
                }
                let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
                let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

                // Cache the script
                self.script_cache.load(script);

                match self.get_self_arc() {
                    Some(arc) => crate::lua::execute_script(&arc, script, keys, argv),
                    None => {
                        CommandResponse::error("ERR scripting not initialized (call init_self_ref)")
                    }
                }
            }
            "EVALSHA" => {
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
                if args.len() < 2 + numkeys {
                    return CommandResponse::error(
                        "ERR Number of keys can't be greater than number of args",
                    );
                }
                let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
                let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

                match self.get_self_arc() {
                    Some(arc) => crate::lua::execute_script(&arc, &script, keys, argv),
                    None => {
                        CommandResponse::error("ERR scripting not initialized (call init_self_ref)")
                    }
                }
            }
            "SCRIPT" => {
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

            // ---- Transaction commands (basic support) ----
            "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" => {
                // These are handled at the connection level in the server
                CommandResponse::error("ERR transactions are handled at connection level")
            }

            // ---- HyperLogLog commands ----
            "PFADD" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("pfadd");
                }
                let key = args[0].clone();
                let elements: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.pfadd(&key, &elements))
            }
            "PFCOUNT" => {
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
            "PFMERGE" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("pfmerge");
                }
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

                // Store merged result in dest's shard
                self.shards
                    .shard_for_key(&dest)
                    .execute(move |store| store.pfset_registers(&dest, merged_registers))
            }

            // ---- Bloom Filter commands ----
            "BF.RESERVE" => {
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
            "BF.ADD" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("bf.add");
                }
                let key = args[0].clone();
                let item = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_add(&key, &item))
            }
            "BF.MADD" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("bf.madd");
                }
                let key = args[0].clone();
                let items: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_madd(&key, &items))
            }
            "BF.EXISTS" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("bf.exists");
                }
                let key = args[0].clone();
                let item = args[1].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_exists(&key, &item))
            }
            "BF.MEXISTS" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("bf.mexists");
                }
                let key = args[0].clone();
                let items: Vec<Bytes> = args[1..].to_vec();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_mexists(&key, &items))
            }
            "BF.INFO" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("bf.info");
                }
                let key = args[0].clone();
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.bf_info(&key))
            }

            // ---- Bitmap commands ----
            "SETBIT" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("setbit");
                }
                let key = args[0].clone();
                let offset = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(o) => o,
                    None => {
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
            "GETBIT" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("getbit");
                }
                let key = args[0].clone();
                let offset = match std::str::from_utf8(&args[1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(o) => o,
                    None => {
                        return CommandResponse::error(
                            "ERR bit offset is not an integer or out of range",
                        )
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.string_getbit(&key, offset))
            }
            "BITCOUNT" => {
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
            "BITOP" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("bitop");
                }
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

                // Store result in dest shard
                self.shards
                    .shard_for_key(&dest)
                    .execute(move |store| store.string_bitop(dest, &op, &sources))
            }

            // ---- Catch-all ----
            _ => CommandResponse::error(format!("ERR unknown command '{}'", name)),
        }
    }
}

fn is_write_command(name: &str) -> bool {
    matches!(
        name,
        "SET"
            | "SETNX"
            | "SETEX"
            | "PSETEX"
            | "GETSET"
            | "GETDEL"
            | "GETEX"
            | "MSET"
            | "MSETNX"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "INCRBYFLOAT"
            | "APPEND"
            | "SETRANGE"
            | "DEL"
            | "UNLINK"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "PERSIST"
            | "RENAME"
            | "RENAMENX"
            | "FLUSHDB"
            | "FLUSHALL"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "LSET"
            | "LINSERT"
            | "LREM"
            | "LTRIM"
            | "HSET"
            | "HMSET"
            | "HDEL"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "HSETNX"
            | "SADD"
            | "SREM"
            | "SPOP"
            | "SMOVE"
            | "ZADD"
            | "ZREM"
            | "ZINCRBY"
            | "ZPOPMIN"
            | "ZPOPMAX"
            | "ZUNIONSTORE"
            | "ZINTERSTORE"
            | "XADD"
            | "XDEL"
            | "XTRIM"
            | "XGROUP"
            | "XREADGROUP"
            | "XACK"
            | "EVAL"
            | "PFADD"
            | "PFMERGE"
            | "BF.RESERVE"
            | "BF.ADD"
            | "BF.MADD"
            | "SETBIT"
            | "BITOP"
            | "GEOADD"
            | "GEOSEARCHSTORE"
    )
}

fn parse_score_bound(arg: &Bytes, default: f64) -> f64 {
    let s = std::str::from_utf8(arg).unwrap_or("");
    if s == "-inf" {
        f64::NEG_INFINITY
    } else if s == "+inf" || s == "inf" {
        f64::INFINITY
    } else if let Some(stripped) = s.strip_prefix('(') {
        // Exclusive bound: for simplicity, we treat as inclusive with tiny offset
        stripped.parse::<f64>().unwrap_or(default)
    } else {
        s.parse::<f64>().unwrap_or(default)
    }
}
