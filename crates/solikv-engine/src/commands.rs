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
    pub name: &'static str,
    /// Exact argument count including the name when positive; at least
    /// `-arity` when negative.
    pub arity: i32,
    pub flags: &'static [&'static str],
    pub keys: Keys,
}

impl CommandSpec {
    pub fn is_write(&self) -> bool {
        self.flags.contains(&"write")
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

/// `ZUNION numkeys key…` / `ZINTERCARD`, `SINTERCARD`, `LMPOP`, `ZMPOP`
/// (commands still to come; tested here so they are ready).
#[cfg_attr(not(test), allow(dead_code))]
fn numkeys_first(args: &[Bytes]) -> Vec<usize> {
    numkeys_at(args, 0, 1)
}

/// `BLMPOP timeout numkeys key…` / `BZMPOP`.
#[cfg_attr(not(test), allow(dead_code))]
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

/// `OBJECT sub key` / `MEMORY USAGE key`: the key is the second argument.
fn second_arg_key(args: &[Bytes]) -> Vec<usize> {
    if args.len() >= 2 {
        vec![1]
    } else {
        Vec::new()
    }
}

macro_rules! spec {
    ($name:expr, $arity:expr, $flags:expr, $keys:expr) => {
        CommandSpec {
            name: $name,
            arity: $arity,
            flags: $flags,
            keys: $keys,
        }
    };
}

static SPECS: &[CommandSpec] = &[
    // Connection / server
    spec!("PING", -1, FAST, NONE),
    spec!("ECHO", 2, FAST, NONE),
    spec!("AUTH", -2, CONN, NONE),
    spec!("HELLO", -1, CONN, NONE),
    spec!("QUIT", -1, CONN, NONE),
    spec!("RESET", 1, CONN, NONE),
    spec!("SELECT", 2, CONN, NONE),
    spec!(
        "CLIENT",
        -2,
        &["admin", "noscript", "loading", "stale"],
        NONE
    ),
    spec!("COMMAND", -1, &["loading", "stale"], NONE),
    spec!(
        "CONFIG",
        -2,
        &["admin", "noscript", "loading", "stale"],
        NONE
    ),
    spec!("INFO", -1, &["loading", "stale"], NONE),
    spec!("DBSIZE", 1, ROF, NONE),
    spec!("FLUSHDB", -1, W, NONE),
    spec!("FLUSHALL", -1, W, NONE),
    spec!("SWAPDB", 3, WF, NONE),
    spec!("REPLICAOF", 3, ADM, NONE),
    spec!("SLAVEOF", 3, ADM, NONE),
    spec!("ROLE", 1, &["noscript", "loading", "stale", "fast"], NONE),
    spec!("SAVE", 1, ADM, NONE),
    spec!("BGSAVE", -1, ADM, NONE),
    spec!("BGREWRITEAOF", 1, ADM, NONE),
    spec!("LASTSAVE", 1, &["loading", "stale", "fast"], NONE),
    spec!(
        "SHUTDOWN",
        -1,
        &["admin", "noscript", "loading", "stale"],
        NONE
    ),
    spec!("WAIT", 3, &["noscript"], NONE),
    spec!("CLUSTER", -2, &["loading", "stale"], NONE),
    // Transactions
    spec!("MULTI", 1, CONN, NONE),
    spec!("EXEC", 1, &["noscript", "loading", "stale"], NONE),
    spec!("DISCARD", 1, CONN, NONE),
    spec!("WATCH", -2, CONN, ALL),
    spec!("UNWATCH", 1, CONN, NONE),
    // Scripting
    spec!("EVAL", -3, SCRIPT, Keys::Movable(eval_keys)),
    spec!("EVALSHA", -3, SCRIPT, Keys::Movable(eval_keys)),
    spec!("SCRIPT", -2, &["noscript"], NONE),
    // Pub/sub
    spec!("PUBLISH", 3, &["pubsub", "loading", "stale", "fast"], NONE),
    spec!("PUBSUB", -2, &["pubsub", "loading", "stale"], NONE),
    spec!("SUBSCRIBE", -2, PUBSUB, NONE),
    spec!("UNSUBSCRIBE", -1, PUBSUB, NONE),
    spec!("PSUBSCRIBE", -2, PUBSUB, NONE),
    spec!("PUNSUBSCRIBE", -1, PUBSUB, NONE),
    // Generic keys
    spec!("DEL", -2, W, ALL),
    spec!("UNLINK", -2, WF, ALL),
    spec!("EXISTS", -2, ROF, ALL),
    spec!("TOUCH", -2, ROF, ALL),
    spec!("TYPE", 2, ROF, K1),
    spec!("EXPIRE", -3, WF, K1),
    spec!("PEXPIRE", -3, WF, K1),
    spec!("EXPIREAT", -3, WF, K1),
    spec!("PEXPIREAT", -3, WF, K1),
    spec!("EXPIRETIME", 2, ROF, K1),
    spec!("PEXPIRETIME", 2, ROF, K1),
    spec!("TTL", 2, ROF, K1),
    spec!("PTTL", 2, ROF, K1),
    spec!("PERSIST", 2, WF, K1),
    spec!("RENAME", 3, W, r(1, 2, 1)),
    spec!("RENAMENX", 3, WF, r(1, 2, 1)),
    spec!("MOVE", 3, WF, K1),
    spec!("KEYS", 2, RO, NONE),
    spec!("SCAN", -2, RO, NONE),
    spec!("RANDOMKEY", 1, RO, NONE),
    spec!("OBJECT", -2, RO, Keys::Movable(second_arg_key)),
    spec!("DUMP", 2, RO, K1),
    spec!("RESTORE", -4, WD, K1),
    spec!("SORT", -2, WD, Keys::Movable(sort_keys)),
    // Strings
    spec!("GET", 2, ROF, K1),
    spec!("SET", -3, WD, K1),
    spec!("SETNX", 3, WDF, K1),
    spec!("SETEX", 4, WD, K1),
    spec!("PSETEX", 4, WD, K1),
    spec!("GETSET", 3, WDF, K1),
    spec!("GETDEL", 2, WF, K1),
    spec!("GETEX", -2, WF, K1),
    spec!("MGET", -2, ROF, ALL),
    spec!("MSET", -3, WD, PAIRS),
    spec!("MSETNX", -3, WD, PAIRS),
    spec!("INCR", 2, WDF, K1),
    spec!("DECR", 2, WDF, K1),
    spec!("INCRBY", 3, WDF, K1),
    spec!("DECRBY", 3, WDF, K1),
    spec!("INCRBYFLOAT", 3, WDF, K1),
    spec!("APPEND", 3, WDF, K1),
    spec!("STRLEN", 2, ROF, K1),
    spec!("GETRANGE", 4, RO, K1),
    spec!("SUBSTR", 4, RO, K1),
    spec!("SETRANGE", 4, WD, K1),
    spec!("SETBIT", 4, WD, K1),
    spec!("GETBIT", 3, ROF, K1),
    spec!("BITCOUNT", -2, RO, K1),
    spec!("BITOP", -4, WD, r(2, -1, 1)),
    // Lists
    spec!("LPUSH", -3, WDF, K1),
    spec!("RPUSH", -3, WDF, K1),
    spec!("LPOP", -2, WF, K1),
    spec!("RPOP", -2, WF, K1),
    spec!("LLEN", 2, ROF, K1),
    spec!("LINDEX", 3, RO, K1),
    spec!("LSET", 4, WD, K1),
    spec!("LRANGE", 4, RO, K1),
    spec!("LINSERT", 5, WD, K1),
    spec!("LREM", 4, W, K1),
    spec!("LTRIM", 4, W, K1),
    // Hashes
    spec!("HSET", -4, WDF, K1),
    spec!("HMSET", -4, WDF, K1),
    spec!("HSETNX", 4, WDF, K1),
    spec!("HGET", 3, ROF, K1),
    spec!("HMGET", -3, ROF, K1),
    spec!("HDEL", -3, WF, K1),
    spec!("HGETALL", 2, RO, K1),
    spec!("HINCRBY", 4, WDF, K1),
    spec!("HINCRBYFLOAT", 4, WDF, K1),
    spec!("HKEYS", 2, RO, K1),
    spec!("HVALS", 2, RO, K1),
    spec!("HLEN", 2, ROF, K1),
    spec!("HEXISTS", 3, ROF, K1),
    spec!("HSCAN", -3, RO, K1),
    // Sets
    spec!("SADD", -3, WDF, K1),
    spec!("SREM", -3, WF, K1),
    spec!("SMEMBERS", 2, RO, K1),
    spec!("SISMEMBER", 3, ROF, K1),
    spec!("SCARD", 2, ROF, K1),
    spec!("SPOP", -2, WF, K1),
    spec!("SRANDMEMBER", -2, RO, K1),
    spec!("SMOVE", 4, WF, r(1, 2, 1)),
    spec!("SINTER", -2, RO, ALL),
    spec!("SUNION", -2, RO, ALL),
    spec!("SDIFF", -2, RO, ALL),
    spec!("SINTERSTORE", -3, WD, ALL),
    spec!("SUNIONSTORE", -3, WD, ALL),
    spec!("SDIFFSTORE", -3, WD, ALL),
    // Sorted sets
    spec!("ZADD", -4, WDF, K1),
    spec!("ZREM", -3, WF, K1),
    spec!("ZSCORE", 3, ROF, K1),
    spec!("ZCARD", 2, ROF, K1),
    spec!("ZRANK", -3, ROF, K1),
    spec!("ZREVRANK", -3, ROF, K1),
    spec!("ZINCRBY", 4, WDF, K1),
    spec!("ZCOUNT", 4, ROF, K1),
    spec!("ZRANGE", -4, RO, K1),
    spec!("ZREVRANGE", -4, RO, K1),
    spec!("ZRANGEBYSCORE", -4, RO, K1),
    spec!("ZREVRANGEBYSCORE", -4, RO, K1),
    spec!("ZRANGEBYLEX", -4, RO, K1),
    spec!("ZREVRANGEBYLEX", -4, RO, K1),
    spec!("ZLEXCOUNT", 4, ROF, K1),
    spec!("ZREMRANGEBYLEX", 4, W, K1),
    spec!("ZREMRANGEBYSCORE", 4, W, K1),
    spec!("ZREMRANGEBYRANK", 4, W, K1),
    spec!("ZPOPMIN", -2, WF, K1),
    spec!("ZPOPMAX", -2, WF, K1),
    spec!("ZUNIONSTORE", -4, WD, Keys::Movable(zstore_keys)),
    spec!("ZINTERSTORE", -4, WD, Keys::Movable(zstore_keys)),
    spec!("ZSCAN", -3, RO, K1),
    // Geo
    spec!("GEOADD", -5, WD, K1),
    spec!("GEOPOS", -2, RO, K1),
    spec!("GEOHASH", -2, RO, K1),
    spec!("GEODIST", -4, RO, K1),
    spec!("GEOSEARCH", -7, RO, K1),
    spec!("GEOSEARCHSTORE", -8, WD, r(1, 2, 1)),
    // Streams
    spec!("XADD", -5, WDF, K1),
    spec!("XLEN", 2, ROF, K1),
    spec!("XRANGE", -4, RO, K1),
    spec!("XREVRANGE", -4, RO, K1),
    spec!(
        "XREAD",
        -4,
        &["readonly", "movablekeys"],
        Keys::Movable(streams_keys)
    ),
    spec!(
        "XREADGROUP",
        -7,
        &["write", "movablekeys"],
        Keys::Movable(streams_keys)
    ),
    spec!("XDEL", -3, WF, K1),
    spec!("XTRIM", -4, W, K1),
    spec!("XINFO", -2, RO, Keys::Movable(second_arg_key)),
    spec!("XGROUP", -2, W, Keys::Movable(second_arg_key)),
    spec!("XACK", -4, WF, K1),
    spec!("XPENDING", -3, RO, K1),
    // HyperLogLog
    spec!("PFADD", -2, WDF, K1),
    spec!("PFCOUNT", -2, RO, ALL),
    spec!("PFMERGE", -2, WD, ALL),
    // Bloom filters
    spec!("BF.RESERVE", -4, WD, K1),
    spec!("BF.ADD", 3, WDF, K1),
    spec!("BF.MADD", -3, WDF, K1),
    spec!("BF.EXISTS", 3, ROF, K1),
    spec!("BF.MEXISTS", -3, ROF, K1),
    spec!("BF.INFO", -2, ROF, K1),
];

fn table() -> &'static HashMap<&'static str, &'static CommandSpec> {
    static T: OnceLock<HashMap<&'static str, &'static CommandSpec>> = OnceLock::new();
    T.get_or_init(|| SPECS.iter().map(|s| (s.name, s)).collect())
}

/// The spec of `name` (upper case).
pub fn lookup(name: &str) -> Option<&'static CommandSpec> {
    table().get(name).copied()
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
    fn names_are_unique_and_upper_case() {
        let mut seen = std::collections::HashSet::new();
        for s in all() {
            assert_eq!(s.name, s.name.to_ascii_uppercase());
            assert!(seen.insert(s.name), "duplicate {}", s.name);
        }
    }
}
