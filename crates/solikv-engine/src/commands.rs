//! The command table: arity, flags and key positions of every command.
//!
//! Three things read it: `COMMAND` (clients such as go-redis and Lettuce ask
//! for it to route and validate), `MULTI` (a queued command with an unknown
//! name or a wrong argument count aborts the transaction at `EXEC`, like
//! Redis' `EXECABORT`), and the engine, which needs a command's keys to know
//! which shards a transaction locks and which watched keys a write touches.
//!
//! Positions follow Redis: index 0 is the command name, so `GET key` has
//! its key at 1. `last = -1` means "up to the last argument", `-2` "up to
//! the one before", and so on.

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::OnceLock;

/// Where a command's keys are.
#[derive(Debug, Clone, Copy)]
pub enum Keys {
    None,
    /// Keys at `first`, `first + step`, … up to `last`.
    Range {
        first: i32,
        last: i32,
        step: i32,
    },
    /// Found by parsing the arguments (`numkeys`, `STREAMS`, …).
    Movable(fn(&[Bytes]) -> Vec<usize>),
}

#[derive(Debug)]
pub struct CommandSpec {
    pub id: Cmd,
    pub name: &'static str,
    /// Exact argument count including the name when positive; at least
    /// `-arity` when negative.
    pub arity: i32,
    pub flags: &'static [&'static str],
    /// `flags` as bits ([`F_WRITE`] …), for tests on the hot path.
    pub bits: u32,
    pub keys: Keys,
}

pub const F_WRITE: u32 = 1 << 0;
pub const F_READONLY: u32 = 1 << 1;
pub const F_DENYOOM: u32 = 1 << 2;
pub const F_ADMIN: u32 = 1 << 3;
pub const F_PUBSUB: u32 = 1 << 4;
pub const F_NOSCRIPT: u32 = 1 << 5;
pub const F_FAST: u32 = 1 << 6;
pub const F_LOADING: u32 = 1 << 7;
pub const F_STALE: u32 = 1 << 8;
pub const F_MOVABLEKEYS: u32 = 1 << 9;
pub const F_BLOCKING: u32 = 1 << 10;

const fn str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// The bits of a flag list, computed when the table is built.
const fn flag_bits(flags: &[&str]) -> u32 {
    const NAMES: [(&str, u32); 11] = [
        ("write", F_WRITE),
        ("readonly", F_READONLY),
        ("denyoom", F_DENYOOM),
        ("admin", F_ADMIN),
        ("pubsub", F_PUBSUB),
        ("noscript", F_NOSCRIPT),
        ("fast", F_FAST),
        ("loading", F_LOADING),
        ("stale", F_STALE),
        ("movablekeys", F_MOVABLEKEYS),
        ("blocking", F_BLOCKING),
    ];
    let mut bits = 0;
    let mut i = 0;
    while i < flags.len() {
        let mut j = 0;
        while j < NAMES.len() {
            if str_eq(flags[i], NAMES[j].0) {
                bits |= NAMES[j].1;
            }
            j += 1;
        }
        i += 1;
    }
    bits
}

impl CommandSpec {
    #[inline]
    pub fn is_write(&self) -> bool {
        self.bits & F_WRITE != 0
    }

    #[inline]
    pub fn has(&self, flag: u32) -> bool {
        self.bits & flag != 0
    }

    /// Whether `argc` arguments (name excluded) satisfy the arity.
    pub fn arity_ok(&self, argc: usize) -> bool {
        let n = argc as i32 + 1;
        if self.arity >= 0 {
            n == self.arity
        } else {
            n >= -self.arity
        }
    }

    /// Positions of the keys in `args` (name excluded).
    pub fn key_positions(&self, args: &[Bytes]) -> Vec<usize> {
        match self.keys {
            Keys::None => Vec::new(),
            Keys::Movable(f) => f(args),
            Keys::Range { first, last, step } => {
                let argc = args.len() as i32 + 1;
                let last = if last < 0 { argc + last } else { last };
                let mut out = Vec::new();
                let mut i = first;
                while i <= last && i < argc && step > 0 {
                    out.push((i - 1) as usize);
                    i += step;
                }
                out
            }
        }
    }

    /// The keys in `args` (name excluded).
    pub fn keys_of(&self, args: &[Bytes]) -> Vec<Bytes> {
        self.key_positions(args)
            .into_iter()
            .filter_map(|i| args.get(i).cloned())
            .collect()
    }

    /// `(first, last, step)` for `COMMAND` (movable commands report
    /// `0, 0, 0`, as Redis does for its movable-key commands).
    pub fn key_range(&self) -> (i32, i32, i32) {
        match self.keys {
            Keys::Range { first, last, step } => (first, last, step),
            _ => (0, 0, 0),
        }
    }
}

const fn r(first: i32, last: i32, step: i32) -> Keys {
    Keys::Range { first, last, step }
}

const K1: Keys = r(1, 1, 1);
const ALL: Keys = r(1, -1, 1);
const PAIRS: Keys = r(1, -1, 2);
const NONE: Keys = Keys::None;

const W: &[&str] = &["write"];
const WD: &[&str] = &["write", "denyoom"];
const WF: &[&str] = &["write", "fast"];
const WDF: &[&str] = &["write", "denyoom", "fast"];
const RO: &[&str] = &["readonly"];
const ROF: &[&str] = &["readonly", "fast"];
const ADM: &[&str] = &["admin", "noscript"];
const FAST: &[&str] = &["fast"];
const CONN: &[&str] = &["noscript", "loading", "stale", "fast"];
const PUBSUB: &[&str] = &["pubsub", "noscript", "loading", "stale"];
const SCRIPT: &[&str] = &["noscript", "movablekeys"];
const BLK: &[&str] = &["write", "blocking"];
const BLKD: &[&str] = &["write", "denyoom", "blocking"];
const BLKM: &[&str] = &["write", "blocking", "movablekeys"];
const WM: &[&str] = &["write", "movablekeys"];
/// All keys but the last argument (`BLPOP k1 k2 timeout`).
const ALL_BUT_LAST: Keys = r(1, -2, 1);

fn numkeys_at(args: &[Bytes], at: usize, first: usize) -> Vec<usize> {
    let n = args
        .get(at)
        .and_then(|a| std::str::from_utf8(a).ok()?.parse::<usize>().ok())
        .unwrap_or(0);
    (first..first.saturating_add(n))
        .filter(|&i| i < args.len())
        .collect()
}

/// `EVAL script numkeys key…`
fn eval_keys(args: &[Bytes]) -> Vec<usize> {
    numkeys_at(args, 1, 2)
}

/// `ZUNIONSTORE dest numkeys key…`
fn zstore_keys(args: &[Bytes]) -> Vec<usize> {
    let mut v = vec![0];
    v.extend(numkeys_at(args, 1, 2));
    v
}

/// `ZUNION numkeys key…` / `ZINTERCARD`, `SINTERCARD`, `LMPOP`, `ZMPOP`.
fn numkeys_first(args: &[Bytes]) -> Vec<usize> {
    numkeys_at(args, 0, 1)
}

/// `BLMPOP timeout numkeys key…` / `BZMPOP`.
fn numkeys_second(args: &[Bytes]) -> Vec<usize> {
    numkeys_at(args, 1, 2)
}

/// `XREAD … STREAMS k1 k2 id1 id2` / `XREADGROUP GROUP g c … STREAMS …`.
fn streams_keys(args: &[Bytes]) -> Vec<usize> {
    let Some(pos) = args.iter().position(|a| a.eq_ignore_ascii_case(b"STREAMS")) else {
        return Vec::new();
    };
    let rest = args.len() - pos - 1;
    (pos + 1..pos + 1 + rest / 2).collect()
}

/// `SORT key … [STORE dest]`
fn sort_keys(args: &[Bytes]) -> Vec<usize> {
    let mut v = vec![0];
    if let Some(p) = args.iter().position(|a| a.eq_ignore_ascii_case(b"STORE")) {
        if p + 1 < args.len() {
            v.push(p + 1);
        }
    }
    v
}

/// `GEORADIUS key … [STORE dest] [STOREDIST dest]`.
fn georadius_keys(args: &[Bytes]) -> Vec<usize> {
    let mut v = vec![0];
    for (i, a) in args.iter().enumerate() {
        if (a.eq_ignore_ascii_case(b"STORE") || a.eq_ignore_ascii_case(b"STOREDIST"))
            && i + 1 < args.len()
        {
            v.push(i + 1);
        }
    }
    v
}

/// `MEMORY USAGE key`: a key only for that subcommand.
fn memory_keys(args: &[Bytes]) -> Vec<usize> {
    if args.len() >= 2 && args[0].eq_ignore_ascii_case(b"USAGE") {
        vec![1]
    } else {
        Vec::new()
    }
}

/// `OBJECT sub key` / `MEMORY USAGE key`: the key is the second argument.
/// `MIGRATE host port key|"" db timeout [COPY] [REPLACE] [AUTH pw | AUTH2 user pw] [KEYS key…]`
fn migrate_keys(args: &[Bytes]) -> Vec<usize> {
    if args.len() < 5 {
        return Vec::new();
    }
    if !args[2].is_empty() {
        return vec![2];
    }
    match args.iter().position(|a| a.eq_ignore_ascii_case(b"KEYS")) {
        Some(i) if i >= 5 => (i + 1..args.len()).collect(),
        _ => Vec::new(),
    }
}

fn second_arg_key(args: &[Bytes]) -> Vec<usize> {
    if args.len() >= 2 {
        vec![1]
    } else {
        Vec::new()
    }
}

macro_rules! commands {
    ($( $(#[$m:meta])* $id:ident = $name:expr, $arity:expr, $flags:expr, $keys:expr; )*) => {
        /// Every command the table knows, in table order: [`CommandSpec::id`].
        /// `dispatch` matches on this rather than on the name, so finding a
        /// command's arm is a jump, not a walk down a chain of string
        /// compares.
        #[allow(non_camel_case_types, clippy::upper_case_acronyms)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(u16)]
        pub enum Cmd {
            $( $id, )*
            /// Not in the table.
            Unknown,
        }

        static SPECS: &[CommandSpec] = &[
            $( CommandSpec {
                id: Cmd::$id,
                name: $name,
                arity: $arity,
                flags: $flags,
                bits: flag_bits($flags),
                keys: $keys,
            }, )*
        ];
    };
}

commands! {
    // Connection / server
    PING = "PING", -1, FAST, NONE;
    ECHO = "ECHO", 2, FAST, NONE;
    AUTH = "AUTH", -2, CONN, NONE;
    HELLO = "HELLO", -1, CONN, NONE;
    QUIT = "QUIT", -1, CONN, NONE;
    RESET = "RESET", 1, CONN, NONE;
    SELECT = "SELECT", 2, &["loading", "stale", "fast"], NONE;
    CLIENT = "CLIENT", -2, &["admin", "noscript", "loading", "stale"], NONE;
    COMMAND = "COMMAND", -1, &["loading", "stale"], NONE;
    CONFIG = "CONFIG", -2, &["admin", "noscript", "loading", "stale"], NONE;
    INFO = "INFO", -1, &["loading", "stale"], NONE;
    DBSIZE = "DBSIZE", 1, ROF, NONE;
    FLUSHDB = "FLUSHDB", -1, W, NONE;
    FLUSHALL = "FLUSHALL", -1, W, NONE;
    SWAPDB = "SWAPDB", 3, WF, NONE;
    REPLICAOF = "REPLICAOF", 3, ADM, NONE;
    SLAVEOF = "SLAVEOF", 3, ADM, NONE;
    ROLE = "ROLE", 1, &["noscript", "loading", "stale", "fast"], NONE;
    SAVE = "SAVE", 1, ADM, NONE;
    BGSAVE = "BGSAVE", -1, ADM, NONE;
    BGREWRITEAOF = "BGREWRITEAOF", 1, ADM, NONE;
    LASTSAVE = "LASTSAVE", 1, &["loading", "stale", "fast"], NONE;
    SHUTDOWN = "SHUTDOWN", -1, &["admin", "noscript", "loading", "stale"], NONE;
    WAIT = "WAIT", 3, &["noscript"], NONE;
    REPLCONF = "REPLCONF", -1, &["admin", "noscript", "loading", "stale"], NONE;
    PSYNC = "PSYNC", -3, &["admin", "noscript"], NONE;
    SYNC = "SYNC", 1, &["admin", "noscript"], NONE;
    CLUSTER = "CLUSTER", -2, &["loading", "stale"], NONE;
    ASKING = "ASKING", 1, FAST, NONE;
    READONLY = "READONLY", 1, &["loading", "stale", "fast"], NONE;
    READWRITE = "READWRITE", 1, &["loading", "stale", "fast"], NONE;
    MIGRATE = "MIGRATE", -6, &["write", "movablekeys"], Keys::Movable(migrate_keys);
    RESTORE_ASKING = "RESTORE-ASKING", -4, WD, K1;
    ACL = "ACL", -2, &["admin", "noscript", "loading", "stale"], NONE;
    SLOWLOG = "SLOWLOG", -2, &["admin", "loading", "stale"], NONE;
    LATENCY = "LATENCY", -2, &["admin", "noscript", "loading", "stale"], NONE;
    MEMORY = "MEMORY", -2, &["readonly"], Keys::Movable(memory_keys);
    DEBUG = "DEBUG", -2, &["admin", "noscript", "loading", "stale"], NONE;
    MONITOR = "MONITOR", 1, &["admin", "noscript", "loading", "stale"], NONE;
    // Transactions
    MULTI = "MULTI", 1, CONN, NONE;
    EXEC = "EXEC", 1, &["noscript", "loading", "stale"], NONE;
    DISCARD = "DISCARD", 1, CONN, NONE;
    WATCH = "WATCH", -2, CONN, ALL;
    UNWATCH = "UNWATCH", 1, CONN, NONE;
    // Scripting
    EVAL = "EVAL", -3, SCRIPT, Keys::Movable(eval_keys);
    EVALSHA = "EVALSHA", -3, SCRIPT, Keys::Movable(eval_keys);
    EVAL_RO = "EVAL_RO", -3, &["readonly", "noscript", "movablekeys"], Keys::Movable(eval_keys);
    EVALSHA_RO = "EVALSHA_RO", -3, &["readonly", "noscript", "movablekeys"], Keys::Movable(eval_keys);
    SCRIPT = "SCRIPT", -2, &["noscript"], NONE;
    FCALL = "FCALL", -3, SCRIPT, Keys::Movable(eval_keys);
    FCALL_RO = "FCALL_RO", -3, &["readonly", "noscript", "movablekeys"], Keys::Movable(eval_keys);
    FUNCTION = "FUNCTION", -2, &["noscript"], NONE;
    // Pub/sub
    PUBLISH = "PUBLISH", 3, &["pubsub", "loading", "stale", "fast"], NONE;
    PUBSUB = "PUBSUB", -2, &["pubsub", "loading", "stale"], NONE;
    SUBSCRIBE = "SUBSCRIBE", -2, PUBSUB, NONE;
    UNSUBSCRIBE = "UNSUBSCRIBE", -1, PUBSUB, NONE;
    PSUBSCRIBE = "PSUBSCRIBE", -2, PUBSUB, NONE;
    PUNSUBSCRIBE = "PUNSUBSCRIBE", -1, PUBSUB, NONE;
    SSUBSCRIBE = "SSUBSCRIBE", -2, PUBSUB, ALL;
    SUNSUBSCRIBE = "SUNSUBSCRIBE", -1, PUBSUB, ALL;
    SPUBLISH = "SPUBLISH", 3, &["pubsub", "loading", "stale", "fast"], K1;
    // Generic keys
    DEL = "DEL", -2, W, ALL;
    UNLINK = "UNLINK", -2, WF, ALL;
    EXISTS = "EXISTS", -2, ROF, ALL;
    TOUCH = "TOUCH", -2, ROF, ALL;
    TYPE = "TYPE", 2, ROF, K1;
    EXPIRE = "EXPIRE", -3, WF, K1;
    PEXPIRE = "PEXPIRE", -3, WF, K1;
    EXPIREAT = "EXPIREAT", -3, WF, K1;
    PEXPIREAT = "PEXPIREAT", -3, WF, K1;
    EXPIRETIME = "EXPIRETIME", 2, ROF, K1;
    PEXPIRETIME = "PEXPIRETIME", 2, ROF, K1;
    TTL = "TTL", 2, ROF, K1;
    PTTL = "PTTL", 2, ROF, K1;
    PERSIST = "PERSIST", 2, WF, K1;
    RENAME = "RENAME", 3, W, r(1, 2, 1);
    RENAMENX = "RENAMENX", 3, WF, r(1, 2, 1);
    MOVE = "MOVE", 3, WF, K1;
    KEYS = "KEYS", 2, RO, NONE;
    SCAN = "SCAN", -2, RO, NONE;
    RANDOMKEY = "RANDOMKEY", 1, RO, NONE;
    OBJECT = "OBJECT", -2, RO, Keys::Movable(second_arg_key);
    DUMP = "DUMP", 2, RO, K1;
    RESTORE = "RESTORE", -4, WD, K1;
    SORT = "SORT", -2, &["write", "denyoom", "movablekeys"], Keys::Movable(sort_keys);
    SORT_RO = "SORT_RO", -2, RO, K1;
    COPY = "COPY", -3, WD, r(1, 2, 1);
    TIME = "TIME", 1, &["loading", "stale", "fast"], NONE;
    // Strings
    GET = "GET", 2, ROF, K1;
    SET = "SET", -3, WD, K1;
    SETNX = "SETNX", 3, WDF, K1;
    SETEX = "SETEX", 4, WD, K1;
    PSETEX = "PSETEX", 4, WD, K1;
    GETSET = "GETSET", 3, WDF, K1;
    GETDEL = "GETDEL", 2, WF, K1;
    GETEX = "GETEX", -2, WF, K1;
    MGET = "MGET", -2, ROF, ALL;
    MSET = "MSET", -3, WD, PAIRS;
    MSETNX = "MSETNX", -3, WD, PAIRS;
    INCR = "INCR", 2, WDF, K1;
    DECR = "DECR", 2, WDF, K1;
    INCRBY = "INCRBY", 3, WDF, K1;
    DECRBY = "DECRBY", 3, WDF, K1;
    INCRBYFLOAT = "INCRBYFLOAT", 3, WDF, K1;
    APPEND = "APPEND", 3, WDF, K1;
    STRLEN = "STRLEN", 2, ROF, K1;
    GETRANGE = "GETRANGE", 4, RO, K1;
    SUBSTR = "SUBSTR", 4, RO, K1;
    SETRANGE = "SETRANGE", 4, WD, K1;
    SETBIT = "SETBIT", 4, WD, K1;
    GETBIT = "GETBIT", 3, ROF, K1;
    BITCOUNT = "BITCOUNT", -2, RO, K1;
    BITOP = "BITOP", -4, WD, r(2, -1, 1);
    BITPOS = "BITPOS", -3, RO, K1;
    BITFIELD = "BITFIELD", -2, WD, K1;
    BITFIELD_RO = "BITFIELD_RO", -2, ROF, K1;
    LCS = "LCS", -3, RO, r(1, 2, 1);
    // Lists
    LPUSH = "LPUSH", -3, WDF, K1;
    RPUSH = "RPUSH", -3, WDF, K1;
    LPOP = "LPOP", -2, WF, K1;
    RPOP = "RPOP", -2, WF, K1;
    LLEN = "LLEN", 2, ROF, K1;
    LINDEX = "LINDEX", 3, RO, K1;
    LSET = "LSET", 4, WD, K1;
    LRANGE = "LRANGE", 4, RO, K1;
    LINSERT = "LINSERT", 5, WD, K1;
    LREM = "LREM", 4, W, K1;
    LTRIM = "LTRIM", 4, W, K1;
    LPUSHX = "LPUSHX", -3, WDF, K1;
    RPUSHX = "RPUSHX", -3, WDF, K1;
    LPOS = "LPOS", -3, RO, K1;
    LMOVE = "LMOVE", 5, WD, r(1, 2, 1);
    RPOPLPUSH = "RPOPLPUSH", 3, WD, r(1, 2, 1);
    LMPOP = "LMPOP", -4, WM, Keys::Movable(numkeys_first);
    BLPOP = "BLPOP", -3, BLK, ALL_BUT_LAST;
    BRPOP = "BRPOP", -3, BLK, ALL_BUT_LAST;
    BLMOVE = "BLMOVE", 6, BLKD, r(1, 2, 1);
    BRPOPLPUSH = "BRPOPLPUSH", 4, BLKD, r(1, 2, 1);
    BLMPOP = "BLMPOP", -5, BLKM, Keys::Movable(numkeys_second);
    // Hashes
    HSET = "HSET", -4, WDF, K1;
    HMSET = "HMSET", -4, WDF, K1;
    HSETNX = "HSETNX", 4, WDF, K1;
    HGET = "HGET", 3, ROF, K1;
    HMGET = "HMGET", -3, ROF, K1;
    HDEL = "HDEL", -3, WF, K1;
    HGETALL = "HGETALL", 2, RO, K1;
    HINCRBY = "HINCRBY", 4, WDF, K1;
    HINCRBYFLOAT = "HINCRBYFLOAT", 4, WDF, K1;
    HKEYS = "HKEYS", 2, RO, K1;
    HVALS = "HVALS", 2, RO, K1;
    HLEN = "HLEN", 2, ROF, K1;
    HEXISTS = "HEXISTS", 3, ROF, K1;
    HSCAN = "HSCAN", -3, RO, K1;
    HSTRLEN = "HSTRLEN", 3, ROF, K1;
    HRANDFIELD = "HRANDFIELD", -2, RO, K1;
    HEXPIRE = "HEXPIRE", -6, WF, K1;
    HPEXPIRE = "HPEXPIRE", -6, WF, K1;
    HEXPIREAT = "HEXPIREAT", -6, WF, K1;
    HPEXPIREAT = "HPEXPIREAT", -6, WF, K1;
    HTTL = "HTTL", -5, ROF, K1;
    HPTTL = "HPTTL", -5, ROF, K1;
    HEXPIRETIME = "HEXPIRETIME", -5, ROF, K1;
    HPEXPIRETIME = "HPEXPIRETIME", -5, ROF, K1;
    HPERSIST = "HPERSIST", -5, WF, K1;
    HGETDEL = "HGETDEL", -5, WF, K1;
    HGETEX = "HGETEX", -5, WF, K1;
    HSETEX = "HSETEX", -6, WDF, K1;
    SSCAN = "SSCAN", -3, RO, K1;
    // Sets
    SADD = "SADD", -3, WDF, K1;
    SREM = "SREM", -3, WF, K1;
    SMEMBERS = "SMEMBERS", 2, RO, K1;
    SISMEMBER = "SISMEMBER", 3, ROF, K1;
    SCARD = "SCARD", 2, ROF, K1;
    SPOP = "SPOP", -2, WF, K1;
    SRANDMEMBER = "SRANDMEMBER", -2, RO, K1;
    SMOVE = "SMOVE", 4, WF, r(1, 2, 1);
    SINTER = "SINTER", -2, RO, ALL;
    SUNION = "SUNION", -2, RO, ALL;
    SDIFF = "SDIFF", -2, RO, ALL;
    SINTERSTORE = "SINTERSTORE", -3, WD, ALL;
    SUNIONSTORE = "SUNIONSTORE", -3, WD, ALL;
    SDIFFSTORE = "SDIFFSTORE", -3, WD, ALL;
    SMISMEMBER = "SMISMEMBER", -3, ROF, K1;
    SINTERCARD = "SINTERCARD", -3, &["readonly", "movablekeys"], Keys::Movable(numkeys_first);
    // Sorted sets
    ZADD = "ZADD", -4, WDF, K1;
    ZREM = "ZREM", -3, WF, K1;
    ZSCORE = "ZSCORE", 3, ROF, K1;
    ZCARD = "ZCARD", 2, ROF, K1;
    ZRANK = "ZRANK", -3, ROF, K1;
    ZREVRANK = "ZREVRANK", -3, ROF, K1;
    ZINCRBY = "ZINCRBY", 4, WDF, K1;
    ZCOUNT = "ZCOUNT", 4, ROF, K1;
    ZRANGE = "ZRANGE", -4, RO, K1;
    ZREVRANGE = "ZREVRANGE", -4, RO, K1;
    ZRANGEBYSCORE = "ZRANGEBYSCORE", -4, RO, K1;
    ZREVRANGEBYSCORE = "ZREVRANGEBYSCORE", -4, RO, K1;
    ZRANGEBYLEX = "ZRANGEBYLEX", -4, RO, K1;
    ZREVRANGEBYLEX = "ZREVRANGEBYLEX", -4, RO, K1;
    ZLEXCOUNT = "ZLEXCOUNT", 4, ROF, K1;
    ZREMRANGEBYLEX = "ZREMRANGEBYLEX", 4, W, K1;
    ZREMRANGEBYSCORE = "ZREMRANGEBYSCORE", 4, W, K1;
    ZREMRANGEBYRANK = "ZREMRANGEBYRANK", 4, W, K1;
    ZPOPMIN = "ZPOPMIN", -2, WF, K1;
    ZPOPMAX = "ZPOPMAX", -2, WF, K1;
    ZMPOP = "ZMPOP", -4, WM, Keys::Movable(numkeys_first);
    BZPOPMIN = "BZPOPMIN", -3, &["write", "blocking", "fast"], ALL_BUT_LAST;
    BZPOPMAX = "BZPOPMAX", -3, &["write", "blocking", "fast"], ALL_BUT_LAST;
    BZMPOP = "BZMPOP", -5, BLKM, Keys::Movable(numkeys_second);
    ZUNIONSTORE = "ZUNIONSTORE", -4, &["write", "denyoom", "movablekeys"], Keys::Movable(zstore_keys);
    ZINTERSTORE = "ZINTERSTORE", -4, &["write", "denyoom", "movablekeys"], Keys::Movable(zstore_keys);
    ZSCAN = "ZSCAN", -3, RO, K1;
    ZUNION = "ZUNION", -3, &["readonly", "movablekeys"], Keys::Movable(numkeys_first);
    ZINTER = "ZINTER", -3, &["readonly", "movablekeys"], Keys::Movable(numkeys_first);
    ZDIFF = "ZDIFF", -3, &["readonly", "movablekeys"], Keys::Movable(numkeys_first);
    ZINTERCARD = "ZINTERCARD", -3, &["readonly", "movablekeys"], Keys::Movable(numkeys_first);
    ZDIFFSTORE = "ZDIFFSTORE", -4, &["write", "denyoom", "movablekeys"], Keys::Movable(zstore_keys);
    ZRANGESTORE = "ZRANGESTORE", -5, WD, r(1, 2, 1);
    ZMSCORE = "ZMSCORE", -3, ROF, K1;
    ZRANDMEMBER = "ZRANDMEMBER", -2, RO, K1;
    // Geo
    GEOADD = "GEOADD", -5, WD, K1;
    GEOPOS = "GEOPOS", -2, RO, K1;
    GEOHASH = "GEOHASH", -2, RO, K1;
    GEODIST = "GEODIST", -4, RO, K1;
    GEOSEARCH = "GEOSEARCH", -7, RO, K1;
    GEOSEARCHSTORE = "GEOSEARCHSTORE", -8, WD, r(1, 2, 1);
    GEORADIUS = "GEORADIUS", -6, &["write", "denyoom", "movablekeys"], Keys::Movable(georadius_keys);
    GEORADIUSBYMEMBER = "GEORADIUSBYMEMBER", -5, &["write", "denyoom", "movablekeys"], Keys::Movable(georadius_keys);
    GEORADIUS_RO = "GEORADIUS_RO", -6, RO, K1;
    GEORADIUSBYMEMBER_RO = "GEORADIUSBYMEMBER_RO", -5, RO, K1;
    // Streams
    XADD = "XADD", -5, WDF, K1;
    XLEN = "XLEN", 2, ROF, K1;
    XRANGE = "XRANGE", -4, RO, K1;
    XREVRANGE = "XREVRANGE", -4, RO, K1;
    XREAD = "XREAD", -4, &["readonly", "movablekeys"], Keys::Movable(streams_keys);
    XREADGROUP = "XREADGROUP", -7, &["write", "movablekeys"], Keys::Movable(streams_keys);
    XDEL = "XDEL", -3, WF, K1;
    XTRIM = "XTRIM", -4, W, K1;
    XINFO = "XINFO", -2, RO, Keys::Movable(second_arg_key);
    XGROUP = "XGROUP", -2, W, Keys::Movable(second_arg_key);
    XACK = "XACK", -4, WF, K1;
    XPENDING = "XPENDING", -3, RO, K1;
    XCLAIM = "XCLAIM", -6, WF, K1;
    XAUTOCLAIM = "XAUTOCLAIM", -6, WF, K1;
    XSETID = "XSETID", -3, WDF, K1;
    // HyperLogLog
    PFADD = "PFADD", -2, WDF, K1;
    PFCOUNT = "PFCOUNT", -2, RO, ALL;
    PFMERGE = "PFMERGE", -2, WD, ALL;
    // Bloom filters
    BF_RESERVE = "BF.RESERVE", -4, WD, K1;
    BF_ADD = "BF.ADD", 3, WDF, K1;
    BF_MADD = "BF.MADD", -3, WDF, K1;
    BF_EXISTS = "BF.EXISTS", 3, ROF, K1;
    BF_MEXISTS = "BF.MEXISTS", -3, ROF, K1;
    BF_INFO = "BF.INFO", -2, ROF, K1;
}

type Table = HashMap<&'static [u8], &'static CommandSpec, solikv_core::FastHasher>;

fn table() -> &'static Table {
    static T: OnceLock<Table> = OnceLock::new();
    T.get_or_init(|| SPECS.iter().map(|s| (s.name.as_bytes(), s)).collect())
}

thread_local! {
    /// The spec this thread looked up last: a pipeline repeats one command,
    /// and each command is looked up several times on its way (batching,
    /// the session, the engine) — under a batch hold, inside the shard lock.
    static LAST: std::cell::Cell<Option<&'static CommandSpec>> =
        const { std::cell::Cell::new(None) };
}

/// The spec of `name` (upper case).
///
/// Names the decoder resolved are the table's own `&'static str`s, so a
/// name with the same address and length as the last spec's name *is* that
/// name (static memory never holds anything else) — no hashing needed.
#[inline]
pub fn lookup(name: &str) -> Option<&'static CommandSpec> {
    if let Some(last) = LAST.with(|l| l.get()) {
        if std::ptr::eq(last.name.as_ptr(), name.as_ptr()) && last.name.len() == name.len() {
            return Some(last);
        }
    }
    let spec = table().get(name.as_bytes()).copied();
    if let Some(s) = spec {
        if std::ptr::eq(s.name.as_ptr(), name.as_ptr()) {
            LAST.with(|l| l.set(Some(s)));
        }
    }
    spec
}

/// The spec of a command name as a client sent it, in any case — no
/// allocation: short names are upper-cased on the stack.
#[inline]
pub fn lookup_ci(name: &[u8]) -> Option<&'static CommandSpec> {
    let mut buf = [0u8; 32];
    if name.len() > buf.len() {
        return None;
    }
    let up = &mut buf[..name.len()];
    for (d, s) in up.iter_mut().zip(name) {
        *d = s.to_ascii_uppercase();
    }
    table().get(&*up).copied()
}

/// The spec of command `id`.
#[inline]
pub fn spec(id: Cmd) -> Option<&'static CommandSpec> {
    SPECS.get(id as usize)
}

/// Every command, in table order.
pub fn all() -> &'static [CommandSpec] {
    SPECS
}

/// The keys `name args…` names, when the table knows the command.
pub fn keys_of(name: &str, args: &[Bytes]) -> Option<Vec<Bytes>> {
    lookup(name).map(|s| s.keys_of(args))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The last-spec cache answers only for the table's own static name;
    /// any other string — same text on the heap, or other text — is looked
    /// up, so a cached spec is never returned for the wrong name.
    #[test]
    fn lookup_cache_matches_only_the_static_name() {
        let get = lookup("GET").unwrap();
        let set = lookup_ci(b"set").unwrap();
        assert_eq!(lookup(get.name).unwrap().id, Cmd::GET);
        assert_eq!(lookup(get.name).unwrap().id, Cmd::GET);
        // Same text, different memory: still GET, by hashing.
        let heap = String::from("GET");
        assert_eq!(lookup(&heap).unwrap().id, Cmd::GET);
        // A heap string after a cached hit is not mistaken for it.
        assert_eq!(lookup(get.name).unwrap().id, Cmd::GET);
        let other = String::from("SET");
        assert_eq!(lookup(&other).unwrap().id, set.id);
        assert!(lookup("NOSUCHCOMMAND").is_none());
        assert_eq!(lookup(set.name).unwrap().id, Cmd::SET);
        assert_eq!(lookup(get.name).unwrap().id, Cmd::GET);
    }

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn key_positions() {
        let mset = lookup("MSET").unwrap();
        assert_eq!(
            mset.keys_of(&[b("a"), b("1"), b("b"), b("2")]),
            vec![b("a"), b("b")]
        );
        let eval = lookup("EVAL").unwrap();
        assert_eq!(
            eval.keys_of(&[b("return 1"), b("2"), b("k1"), b("k2"), b("arg")]),
            vec![b("k1"), b("k2")]
        );
        let z = lookup("ZUNIONSTORE").unwrap();
        assert_eq!(
            z.keys_of(&[b("d"), b("2"), b("a"), b("b"), b("WEIGHTS"), b("1"), b("2")]),
            vec![b("d"), b("a"), b("b")]
        );
        let x = lookup("XREAD").unwrap();
        assert_eq!(
            x.keys_of(&[
                b("COUNT"),
                b("1"),
                b("STREAMS"),
                b("s1"),
                b("s2"),
                b("0"),
                b("0")
            ]),
            vec![b("s1"), b("s2")]
        );
        let bitop = lookup("BITOP").unwrap();
        assert_eq!(
            bitop.keys_of(&[b("AND"), b("d"), b("a"), b("b")]),
            vec![b("d"), b("a"), b("b")]
        );
        assert_eq!(numkeys_first(&[b("2"), b("a"), b("b")]), vec![1, 2]);
        assert_eq!(numkeys_second(&[b("0"), b("1"), b("a")]), vec![2]);
    }

    #[test]
    fn arity() {
        let get = lookup("GET").unwrap();
        assert!(get.arity_ok(1));
        assert!(!get.arity_ok(2));
        let set = lookup("SET").unwrap();
        assert!(!set.arity_ok(1));
        assert!(set.arity_ok(5));
    }

    #[test]
    fn ids_index_the_table_and_bits_match_flags() {
        for (i, s) in all().iter().enumerate() {
            assert_eq!(s.id as usize, i);
            assert_eq!(spec(s.id).unwrap().name, s.name);
            assert_eq!(s.is_write(), s.flags.contains(&"write"), "{}", s.name);
            assert_eq!(s.has(F_DENYOOM), s.flags.contains(&"denyoom"), "{}", s.name);
        }
        assert_eq!(lookup_ci(b"gEt").unwrap().id, Cmd::GET);
        assert_eq!(lookup_ci(b"bf.add").unwrap().id, Cmd::BF_ADD);
        assert!(lookup_ci(b"nosuchcommand").is_none());
        assert!(lookup_ci(&[b'x'; 100]).is_none());
    }

    #[test]
    fn blocking_commands_start_with_b_or_x() {
        // `CommandEngine::is_blocking_form` relies on it to skip the lookup.
        for s in all() {
            if s.has(F_BLOCKING) {
                assert!(
                    s.name.starts_with('B') || s.name.starts_with('X'),
                    "{}",
                    s.name
                );
            }
        }
    }

    #[test]
    fn names_are_unique_and_upper_case() {
        let mut seen = std::collections::HashSet::new();
        for s in all() {
            assert_eq!(s.name, s.name.to_ascii_uppercase());
            assert!(seen.insert(s.name), "duplicate {}", s.name);
        }
    }
}
