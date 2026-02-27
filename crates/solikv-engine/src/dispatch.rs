use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;

use solikv_core::types::*;
use solikv_core::zset_ops::Aggregate;
use solikv_persist::AofWriter;
use solikv_pubsub::PubSubBroker;

use crate::shard::ShardManager;

/// The central command engine that routes commands to appropriate shards.
pub struct CommandEngine {
    pub shards: Arc<ShardManager>,
    pub pubsub: Arc<PubSubBroker>,
    aof: Option<AofWriter>,
    start_time: Instant,
}

impl CommandEngine {
    pub fn new(shards: Arc<ShardManager>, pubsub: Arc<PubSubBroker>) -> Self {
        Self {
            shards,
            pubsub,
            aof: None,
            start_time: Instant::now(),
        }
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
            if is_write_command(name) && !response.is_error() {
                aof.log(name, args);
            }
        }

        response
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
                            _ => CommandResponse::array(vec![]),
                        }
                    }
                    "SET" => CommandResponse::ok(),
                    "RESETSTAT" => CommandResponse::ok(),
                    _ => CommandResponse::error(format!("ERR Unknown subcommand or wrong number of arguments for 'config|{}'", sub.to_lowercase())),
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
                    let resp = shard.execute(|store| {
                        CommandResponse::integer(store.dbsize() as i64)
                    });
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
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_get(&key)
                })
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
                            if let Some(secs) = args.get(i).and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok()) {
                                expire_ms = Some(secs * 1000);
                            } else {
                                return CommandResponse::syntax_error();
                            }
                        }
                        "PX" => {
                            i += 1;
                            if let Some(ms) = args.get(i).and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok()) {
                                expire_ms = Some(ms);
                            } else {
                                return CommandResponse::syntax_error();
                            }
                        }
                        "EXAT" => {
                            i += 1;
                            if let Some(ts) = args.get(i).and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok()) {
                                let now = now_millis();
                                let target = ts * 1000;
                                expire_ms = Some(target.saturating_sub(now));
                            } else {
                                return CommandResponse::syntax_error();
                            }
                        }
                        "PXAT" => {
                            i += 1;
                            if let Some(ts) = args.get(i).and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok()) {
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

                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_set(key, value, expire_ms, nx, xx, get)
                })
            }
            "SETNX" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("setnx");
                }
                let key = args[0].clone();
                let value = args[1].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_setnx(key, value)
                })
            }
            "SETEX" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("setex");
                }
                let key = args[0].clone();
                let seconds = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<u64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let value = args[2].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_setex(key, seconds, value)
                })
            }
            "PSETEX" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("psetex");
                }
                let key = args[0].clone();
                let millis = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<u64>().ok()) {
                    Some(m) => m,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let value = args[2].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_psetex(key, millis, value)
                })
            }
            "GETSET" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("getset");
                }
                let key = args[0].clone();
                let value = args[1].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_getset(key, value)
                })
            }
            "GETDEL" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("getdel");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_getdel(&key)
                })
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
                            expire_ms = args.get(i).and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok()).map(|s| s * 1000);
                        }
                        "PX" => {
                            i += 1;
                            expire_ms = args.get(i).and_then(|b| std::str::from_utf8(b).ok()?.parse::<u64>().ok());
                        }
                        "PERSIST" => persist = true,
                        _ => return CommandResponse::syntax_error(),
                    }
                    i += 1;
                }
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_getex(&key, expire_ms, persist)
                })
            }
            "MGET" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("mget");
                }
                // For single-shard simplicity, route all to shard of first key
                // TODO: proper multi-shard gather
                let keys: Vec<Bytes> = args.to_vec();
                let shard = self.shards.shard_for_key(&keys[0]).clone();
                shard.execute(move |store| {
                    store.string_mget(&keys)
                })
            }
            "MSET" => {
                if args.len() < 2 || args.len() % 2 != 0 {
                    return CommandResponse::wrong_arity("mset");
                }
                let pairs: Vec<(Bytes, Bytes)> = args.chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                // Route to first key's shard for simplicity
                let shard = self.shards.shard_for_key(&pairs[0].0).clone();
                shard.execute(move |store| {
                    store.string_mset(pairs)
                })
            }
            "MSETNX" => {
                if args.len() < 2 || args.len() % 2 != 0 {
                    return CommandResponse::wrong_arity("msetnx");
                }
                let pairs: Vec<(Bytes, Bytes)> = args.chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                let shard = self.shards.shard_for_key(&pairs[0].0).clone();
                shard.execute(move |store| {
                    store.string_msetnx(pairs)
                })
            }
            "INCR" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("incr");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_incr(&key)
                })
            }
            "DECR" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("decr");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_decr(&key)
                })
            }
            "INCRBY" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("incrby");
                }
                let key = args[0].clone();
                let delta = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(d) => d,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_incrby(&key, delta)
                })
            }
            "DECRBY" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("decrby");
                }
                let key = args[0].clone();
                let delta = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(d) => d,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_decrby(&key, delta)
                })
            }
            "INCRBYFLOAT" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("incrbyfloat");
                }
                let key = args[0].clone();
                let delta = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<f64>().ok()) {
                    Some(d) => d,
                    None => return CommandResponse::error("ERR value is not a valid float"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_incrbyfloat(&key, delta)
                })
            }
            "APPEND" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("append");
                }
                let key = args[0].clone();
                let value = args[1].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_append(&key, &value)
                })
            }
            "STRLEN" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("strlen");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_strlen(&key)
                })
            }
            "GETRANGE" | "SUBSTR" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("getrange");
                }
                let key = args[0].clone();
                let start = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let end = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(e) => e,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_getrange(&key, start, end)
                })
            }
            "SETRANGE" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("setrange");
                }
                let key = args[0].clone();
                let offset = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<usize>().ok()) {
                    Some(o) => o,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let value = args[2].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.string_setrange(&key, offset, &value)
                })
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
                let seconds = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<u64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(if store.expire(&key, seconds * 1000) { 1 } else { 0 })
                })
            }
            "PEXPIRE" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("pexpire");
                }
                let key = args[0].clone();
                let millis = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<u64>().ok()) {
                    Some(m) => m,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
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
                let ts = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<u64>().ok()) {
                    Some(t) => t,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(if store.expire_at(&key, ts * 1000) { 1 } else { 0 })
                })
            }
            "PEXPIREAT" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("pexpireat");
                }
                let key = args[0].clone();
                let ts = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<u64>().ok()) {
                    Some(t) => t,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
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
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(store.ttl(&key))
                })
            }
            "PTTL" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("pttl");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::integer(store.pttl(&key))
                })
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
                self.shards.shard_for_key(&key).execute(move |store| {
                    CommandResponse::simple(store.key_type(&key))
                })
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
                        CommandResponse::array(keys.into_iter().map(CommandResponse::bulk).collect())
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
                let cursor = std::str::from_utf8(&args[0]).ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
                let mut pattern = None;
                let mut count = 10usize;
                let mut i = 1;
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "MATCH" => {
                            i += 1;
                            pattern = args.get(i).and_then(|b| std::str::from_utf8(b).ok()).map(|s| s.to_string());
                        }
                        "COUNT" => {
                            i += 1;
                            count = args.get(i).and_then(|b| std::str::from_utf8(b).ok()?.parse().ok()).unwrap_or(10);
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
                        CommandResponse::array(keys.into_iter().map(CommandResponse::bulk).collect()),
                    ])
                })
            }
            "RANDOMKEY" => {
                let shard = self.shards.shard(0).clone();
                shard.execute(|store| {
                    match store.random_key() {
                        Some(k) => CommandResponse::bulk(k),
                        None => CommandResponse::nil(),
                    }
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
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_lpush(&key, values)
                })
            }
            "RPUSH" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("rpush");
                }
                let key = args[0].clone();
                let values: Vec<Bytes> = args[1..].to_vec();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_rpush(&key, values)
                })
            }
            "LPOP" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("lpop");
                }
                let key = args[0].clone();
                let count = args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(1);
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_lpop(&key, count)
                })
            }
            "RPOP" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("rpop");
                }
                let key = args[0].clone();
                let count = args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(1);
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_rpop(&key, count)
                })
            }
            "LLEN" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("llen");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_llen(&key)
                })
            }
            "LINDEX" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("lindex");
                }
                let key = args[0].clone();
                let index = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(i) => i,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_lindex(&key, index)
                })
            }
            "LSET" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("lset");
                }
                let key = args[0].clone();
                let index = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(i) => i,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let value = args[2].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_lset(&key, index, value)
                })
            }
            "LRANGE" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("lrange");
                }
                let key = args[0].clone();
                let start = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let stop = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_lrange(&key, start, stop)
                })
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
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_linsert(&key, before, &pivot, value)
                })
            }
            "LREM" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("lrem");
                }
                let key = args[0].clone();
                let count = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(c) => c,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let value = args[2].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_lrem(&key, count, &value)
                })
            }
            "LTRIM" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("ltrim");
                }
                let key = args[0].clone();
                let start = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let stop = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.list_ltrim(&key, start, stop)
                })
            }

            // ---- Hash commands ----
            "HSET" => {
                if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                    return CommandResponse::wrong_arity("hset");
                }
                let key = args[0].clone();
                let pairs: Vec<(Bytes, Bytes)> = args[1..].chunks(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hset(&key, pairs)
                })
            }
            "HMSET" => {
                // HMSET is deprecated but still supported, same logic as HSET
                if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                    return CommandResponse::wrong_arity("hmset");
                }
                let key = args[0].clone();
                let pairs: Vec<(Bytes, Bytes)> = args[1..].chunks(2)
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
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hget(&key, &field)
                })
            }
            "HDEL" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("hdel");
                }
                let key = args[0].clone();
                let fields: Vec<Bytes> = args[1..].to_vec();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hdel(&key, &fields)
                })
            }
            "HGETALL" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hgetall");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hgetall(&key)
                })
            }
            "HMGET" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("hmget");
                }
                let key = args[0].clone();
                let fields: Vec<Bytes> = args[1..].to_vec();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hmget(&key, &fields)
                })
            }
            "HINCRBY" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("hincrby");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                let delta = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(d) => d,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hincrby(&key, field, delta)
                })
            }
            "HINCRBYFLOAT" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("hincrbyfloat");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                let delta = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<f64>().ok()) {
                    Some(d) => d,
                    None => return CommandResponse::error("ERR value is not a valid float"),
                };
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hincrbyfloat(&key, field, delta)
                })
            }
            "HKEYS" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hkeys");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hkeys(&key)
                })
            }
            "HVALS" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hvals");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hvals(&key)
                })
            }
            "HLEN" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("hlen");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hlen(&key)
                })
            }
            "HEXISTS" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("hexists");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hexists(&key, &field)
                })
            }
            "HSETNX" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("hsetnx");
                }
                let key = args[0].clone();
                let field = args[1].clone();
                let value = args[2].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hsetnx(&key, field, value)
                })
            }
            "HSCAN" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("hscan");
                }
                let key = args[0].clone();
                let cursor = std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
                let mut pattern = None;
                let mut count = 10usize;
                let mut i = 2;
                while i < args.len() {
                    let opt = std::str::from_utf8(&args[i]).unwrap_or("").to_uppercase();
                    match opt.as_str() {
                        "MATCH" => { i += 1; pattern = args.get(i).and_then(|b| std::str::from_utf8(b).ok()).map(|s| s.to_string()); }
                        "COUNT" => { i += 1; count = args.get(i).and_then(|b| std::str::from_utf8(b).ok()?.parse().ok()).unwrap_or(10); }
                        _ => {}
                    }
                    i += 1;
                }
                let pat = pattern.clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.hash_hscan(&key, cursor, pat.as_deref(), count)
                })
            }

            // ---- Set commands ----
            "SADD" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("sadd");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.set_sadd(&key, members)
                })
            }
            "SREM" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("srem");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.set_srem(&key, &members)
                })
            }
            "SMEMBERS" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("smembers");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.set_smembers(&key)
                })
            }
            "SISMEMBER" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("sismember");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.set_sismember(&key, &member)
                })
            }
            "SCARD" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("scard");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.set_scard(&key)
                })
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
                let count = args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(1);
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.set_spop(&key, count)
                })
            }
            "SRANDMEMBER" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("srandmember");
                }
                let key = args[0].clone();
                let count = args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<i64>().ok());
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.set_srandmember(&key, count)
                })
            }
            "SMOVE" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("smove");
                }
                let src = args[0].clone();
                let dst = args[1].clone();
                let member = args[2].clone();
                self.shards.shard_for_key(&src).execute(move |store| {
                    store.set_smove(&src, dst, member)
                })
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
                        "NX" => { nx = true; i += 1; }
                        "XX" => { xx = true; i += 1; }
                        "GT" => { gt = true; i += 1; }
                        "LT" => { lt = true; i += 1; }
                        "CH" => { ch = true; i += 1; }
                        _ => break, // Start of score-member pairs
                    }
                }

                let remaining = &args[i..];
                if remaining.len() < 2 || remaining.len() % 2 != 0 {
                    return CommandResponse::wrong_arity("zadd");
                }

                let pairs: Vec<(f64, Bytes)> = remaining.chunks(2)
                    .filter_map(|c| {
                        let score = std::str::from_utf8(&c[0]).ok()?.parse::<f64>().ok()?;
                        Some((score, c[1].clone()))
                    })
                    .collect();

                if pairs.is_empty() {
                    return CommandResponse::error("ERR value is not a valid float");
                }

                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zadd(&key, pairs, nx, xx, gt, lt, ch)
                })
            }
            "ZREM" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("zrem");
                }
                let key = args[0].clone();
                let members: Vec<Bytes> = args[1..].to_vec();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zrem(&key, &members)
                })
            }
            "ZSCORE" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("zscore");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zscore(&key, &member)
                })
            }
            "ZCARD" => {
                if args.len() != 1 {
                    return CommandResponse::wrong_arity("zcard");
                }
                let key = args[0].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zcard(&key)
                })
            }
            "ZRANK" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("zrank");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zrank(&key, &member)
                })
            }
            "ZREVRANK" => {
                if args.len() != 2 {
                    return CommandResponse::wrong_arity("zrevrank");
                }
                let key = args[0].clone();
                let member = args[1].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zrevrank(&key, &member)
                })
            }
            "ZINCRBY" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("zincrby");
                }
                let key = args[0].clone();
                let delta = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<f64>().ok()) {
                    Some(d) => d,
                    None => return CommandResponse::error("ERR value is not a valid float"),
                };
                let member = args[2].clone();
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zincrby(&key, delta, member)
                })
            }
            "ZCOUNT" => {
                if args.len() != 3 {
                    return CommandResponse::wrong_arity("zcount");
                }
                let key = args[0].clone();
                let min = parse_score_bound(&args[1], f64::NEG_INFINITY);
                let max = parse_score_bound(&args[2], f64::INFINITY);
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zcount(&key, min, max)
                })
            }
            "ZRANGE" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zrange");
                }
                let key = args[0].clone();
                let start = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let stop = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let withscores = args.get(3).map(|b| std::str::from_utf8(b).unwrap_or("").to_uppercase() == "WITHSCORES").unwrap_or(false);
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zrange(&key, start, stop, withscores)
                })
            }
            "ZREVRANGE" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zrevrange");
                }
                let key = args[0].clone();
                let start = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let stop = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(s) => s,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
                };
                let withscores = args.get(3).map(|b| std::str::from_utf8(b).unwrap_or("").to_uppercase() == "WITHSCORES").unwrap_or(false);
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zrevrange(&key, start, stop, withscores)
                })
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
                            offset = args.get(i).and_then(|b| std::str::from_utf8(b).ok()?.parse().ok()).unwrap_or(0);
                            i += 1;
                            count = args.get(i).and_then(|b| std::str::from_utf8(b).ok()?.parse().ok());
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
                let count = args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(1);
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zpopmin(&key, count)
                })
            }
            "ZPOPMAX" => {
                if args.is_empty() {
                    return CommandResponse::wrong_arity("zpopmax");
                }
                let key = args[0].clone();
                let count = args.get(1).and_then(|b| std::str::from_utf8(b).ok()?.parse::<usize>().ok()).unwrap_or(1);
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zpopmax(&key, count)
                })
            }
            "ZUNIONSTORE" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zunionstore");
                }
                let dest = args[0].clone();
                let numkeys = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<usize>().ok()) {
                    Some(n) => n,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
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
                                if let Some(w) = std::str::from_utf8(&args[i]).ok().and_then(|s| s.parse::<f64>().ok()) {
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
                            if let Some(agg) = args.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
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
                shard.execute(move |store| {
                    store.zset_zunionstore(&dest, &keys, &weights, aggregate)
                })
            }
            "ZINTERSTORE" => {
                if args.len() < 3 {
                    return CommandResponse::wrong_arity("zinterstore");
                }
                let dest = args[0].clone();
                let numkeys = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<usize>().ok()) {
                    Some(n) => n,
                    None => return CommandResponse::error("ERR value is not an integer or out of range"),
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
                                if let Some(w) = std::str::from_utf8(&args[i]).ok().and_then(|s| s.parse::<f64>().ok()) {
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
                            if let Some(agg) = args.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
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
                shard.execute(move |store| {
                    store.zset_zinterstore(&dest, &keys, &weights, aggregate)
                })
            }
            "ZSCAN" => {
                if args.len() < 2 {
                    return CommandResponse::wrong_arity("zscan");
                }
                let key = args[0].clone();
                let cursor = std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
                let count = 10;
                self.shards.shard_for_key(&key).execute(move |store| {
                    store.zset_zscan(&key, cursor, None, count)
                })
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
                        CommandResponse::array(channels.into_iter().map(CommandResponse::bulk).collect())
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

            // ---- Transaction commands (basic support) ----
            "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" => {
                // These are handled at the connection level in the server
                CommandResponse::error("ERR transactions are handled at connection level")
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
