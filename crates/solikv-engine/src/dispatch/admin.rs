//! Server administration: `INFO`, `CONFIG`, `MEMORY`, `DEBUG`, and client
//! tracking's `CLIENT` subcommands.

use bytes::Bytes;
use std::sync::atomic::Ordering;

use solikv_core::types::{KeyEntry, RedisValue};
use solikv_core::CommandResponse;

use super::{flags_to_string, parse_notify_flags, CommandEngine, Upper};
use crate::session::Session;
use crate::shard::current_db;
use solikv_core::store::EvictionPolicy;

// ---------------------------------------------------------------------------
// MEMORY
// ---------------------------------------------------------------------------

/// Bytes an entry takes, estimated as Redis does (`MEMORY USAGE`): the key,
/// the entry's overhead, and the value — sampled for large collections.
fn entry_bytes(key_len: usize, e: &KeyEntry, samples: usize) -> usize {
    const ENTRY: usize = 64;
    const ELEM: usize = 32;
    let sampled = |n: usize, sizes: &mut dyn Iterator<Item = usize>| -> usize {
        if n == 0 {
            return 0;
        }
        let take = if samples == 0 { n } else { samples.min(n) };
        let sum: usize = sizes.take(take).sum();
        sum * n / take.max(1)
    };
    let value = match &e.value {
        RedisValue::String(b) => b.len(),
        RedisValue::List(l) => sampled(l.len(), &mut l.iter().map(|x| x.len() + ELEM)),
        RedisValue::Hash(h) => sampled(
            h.len(),
            &mut h.iter().map(|(k, v)| k.len() + v.len() + ELEM),
        ),
        RedisValue::Set(s) => sampled(s.len(), &mut s.iter().map(|m| m.len() + ELEM)),
        RedisValue::ZSet(z) => {
            sampled(z.len(), &mut z.members.keys().map(|m| 2 * (m.len() + ELEM)))
        }
        RedisValue::Stream(s) => s.len() * 96,
        RedisValue::HyperLogLog(h) => h.registers.len(),
        RedisValue::BloomFilter(b) => b.bits.len(),
    };
    key_len + ENTRY + value
}

impl CommandEngine {
    /// `ROLE`.
    pub(crate) fn role_reply(&self) -> CommandResponse {
        let feed = self.repl();
        if let Some(st) = self.repl_ctl.get().and_then(|c| c.status()) {
            let offset = if st.state == "connected" {
                feed.offset() as i64
            } else {
                -1
            };
            return CommandResponse::array(vec![
                CommandResponse::bulk_string("slave"),
                CommandResponse::bulk(Bytes::from(st.master_host)),
                CommandResponse::integer(st.master_port as i64),
                CommandResponse::bulk_string(st.state),
                CommandResponse::integer(offset),
            ]);
        }
        let replicas = feed
            .sinks()
            .into_iter()
            .filter(|s| s.online.load(Ordering::Relaxed))
            .map(|s| {
                CommandResponse::array(vec![
                    CommandResponse::bulk(Bytes::from(s.addr.clone())),
                    CommandResponse::bulk(Bytes::from(s.listening_port.to_string())),
                    CommandResponse::bulk(Bytes::from(s.ack.load(Ordering::Relaxed).to_string())),
                ])
            })
            .collect();
        CommandResponse::array(vec![
            CommandResponse::bulk_string("master"),
            CommandResponse::integer(feed.offset() as i64),
            CommandResponse::array(replicas),
        ])
    }

    /// `INFO replication`.
    pub(crate) fn info_replication(&self) -> String {
        let feed = self.repl();
        let (replid, replid2, offset, second) = feed.ids();
        let (active, first, histlen) = feed.backlog_info();
        let mut out = String::from("# Replication\r\n");
        match self.repl_ctl.get().and_then(|c| c.status()) {
            Some(st) => {
                out.push_str(&format!(
                    "role:slave\r\nmaster_host:{}\r\nmaster_port:{}\r\nmaster_link_status:{}\r\n\
                     master_last_io_seconds_ago:{}\r\nmaster_sync_in_progress:{}\r\n\
                     slave_read_repl_offset:{}\r\nslave_repl_offset:{}\r\nslave_priority:100\r\n\
                     slave_read_only:{}\r\nreplica_announced:1\r\n",
                    st.master_host,
                    st.master_port,
                    if st.state == "connected" {
                        "up"
                    } else {
                        "down"
                    },
                    st.last_io_secs,
                    st.sync_in_progress as u8,
                    offset,
                    offset,
                    self.replica_read_only() as u8,
                ));
            }
            None => out.push_str("role:master\r\n"),
        }
        let sinks = feed.sinks();
        out.push_str(&format!("connected_slaves:{}\r\n", sinks.len()));
        for (i, s) in sinks.iter().enumerate() {
            let online = s.online.load(Ordering::Relaxed);
            let ack = s.ack.load(Ordering::Relaxed);
            out.push_str(&format!(
                "slave{}:ip={},port={},state={},offset={},lag={}\r\n",
                i,
                s.addr,
                s.listening_port,
                if online { "online" } else { "wait_bgsave" },
                ack,
                0
            ));
        }
        out.push_str(&format!(
            "sync_full:{}\r\nsync_partial_ok:{}\r\nsync_partial_err:{}\r\n",
            feed.sync_full.load(Ordering::Relaxed),
            feed.sync_partial_ok.load(Ordering::Relaxed),
            feed.sync_partial_err.load(Ordering::Relaxed),
        ));
        out.push_str(&format!(
            "master_failover_state:no-failover\r\nmaster_replid:{}\r\nmaster_replid2:{}\r\n\
             master_repl_offset:{}\r\nsecond_repl_offset:{}\r\nrepl_backlog_active:{}\r\n\
             repl_backlog_size:{}\r\nrepl_backlog_first_byte_offset:{}\r\nrepl_backlog_histlen:{}\r\n",
            replid,
            replid2,
            offset,
            second,
            active as u8,
            feed.backlog_size(),
            if active { first } else { 0 },
            histlen,
        ));
        out
    }

    pub(crate) fn memory_command(&self, args: &[Bytes]) -> CommandResponse {
        let sub = Upper::new(&args[0]);
        match sub.as_str() {
            "USAGE" => {
                let Some(key) = args.get(1).cloned() else {
                    return CommandResponse::wrong_arity("memory|usage");
                };
                let mut samples = 5usize;
                let mut i = 2;
                while i < args.len() {
                    if args[i].eq_ignore_ascii_case(b"SAMPLES") && i + 1 < args.len() {
                        match std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                        {
                            Some(n) => samples = n,
                            None => {
                                return CommandResponse::error(
                                    "ERR value is out of range, must be positive",
                                )
                            }
                        }
                        i += 2;
                    } else {
                        return CommandResponse::syntax_error();
                    }
                }
                self.shards.shard_for_key(&key).with_store(|s| {
                    let _ = current_db();
                    match s.peek(&key) {
                        Some(e) if !e.is_expired() => {
                            CommandResponse::integer(entry_bytes(key.len(), e, samples) as i64)
                        }
                        _ => CommandResponse::Nil,
                    }
                })
            }
            "STATS" => {
                let used = solikv_core::memory::used() as u64;
                let keys: usize = self
                    .shards
                    .all_shards()
                    .iter()
                    .map(|s| s.with_store(|st| (0..self.databases).map(|d| st.dbsize_of(d)).sum::<usize>()))
                    .sum();
                let rss = rss_bytes();
                let b = |s: &str| CommandResponse::bulk_string(s);
                let i = |n: u64| CommandResponse::integer(n as i64);
                CommandResponse::array(vec![
                    b("peak.allocated"),
                    i(self.stats.peak_memory().max(used)),
                    b("total.allocated"),
                    i(used),
                    b("startup.allocated"),
                    i(0),
                    b("replication.backlog"),
                    i(0),
                    b("clients.slaves"),
                    i(0),
                    b("clients.normal"),
                    i(0),
                    b("keys.count"),
                    i(keys as u64),
                    b("keys.bytes-per-key"),
                    i(used.checked_div(keys as u64).unwrap_or(0)),
                    b("dataset.bytes"),
                    i(used),
                    b("allocator.allocated"),
                    i(used),
                    b("allocator.resident"),
                    i(rss),
                    b("fragmentation"),
                    CommandResponse::bulk(Bytes::from(format!(
                        "{:.2}",
                        if used > 0 { rss as f64 / used as f64 } else { 1.0 }
                    ))),
                ])
            }
            "DOCTOR" => CommandResponse::bulk(Bytes::from_static(
                b"Hi Sam, I can't find any memory issue in your instance. I can only account for what occurs on this base.",
            )),
            "MALLOC-STATS" => CommandResponse::bulk(Bytes::from_static(
                b"Stats not supported for the current allocator",
            )),
            "PURGE" => CommandResponse::ok(),
            "HELP" => CommandResponse::array(
                [
                    "MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "DOCTOR",
                    "MALLOC-STATS",
                    "PURGE",
                    "STATS",
                    "USAGE <key> [SAMPLES <count>]",
                ]
                .iter()
                .map(|l| CommandResponse::simple(l))
                .collect(),
            ),
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try MEMORY HELP.",
                sub.to_lowercase()
            )),
        }
    }

    // -----------------------------------------------------------------------
    // DEBUG
    // -----------------------------------------------------------------------

    pub(crate) fn debug_command(&self, session: &Session, args: &[Bytes]) -> CommandResponse {
        let local = session.info.addr.starts_with("127.")
            || session.info.addr.starts_with("[::1]")
            || session.info.addr.starts_with("localhost");
        match self.debug_mode.load(Ordering::Relaxed) {
            2 => {}
            1 if local => {}
            _ => {
                return CommandResponse::error(
                    "ERR DEBUG command not allowed. If the enable-debug-command option is set to \
                     \"local\", you can run it from a local connection, otherwise you need to set \
                     this option in the configuration file, and then restart the server.",
                )
            }
        }
        let sub = Upper::new(&args[0]);
        match sub.as_str() {
            "SLEEP" => {
                let secs = args
                    .get(1)
                    .and_then(|a| std::str::from_utf8(a).ok()?.parse::<f64>().ok())
                    .unwrap_or(0.0);
                if secs > 0.0 {
                    std::thread::sleep(std::time::Duration::from_secs_f64(secs.min(3600.0)));
                }
                CommandResponse::ok()
            }
            "SET-ACTIVE-EXPIRE" => match args.get(1).map(|a| a.as_ref()) {
                Some(b"0") => {
                    self.shards.set_active_expire(false);
                    CommandResponse::ok()
                }
                Some(b"1") => {
                    self.shards.set_active_expire(true);
                    CommandResponse::ok()
                }
                _ => CommandResponse::syntax_error(),
            },
            "OBJECT" => {
                let Some(key) = args.get(1).cloned() else {
                    return CommandResponse::wrong_arity("debug|object");
                };
                let _db = crate::shard::DbGuard::enter(session.db);
                let enc = self.execute("OBJECT", &[Bytes::from_static(b"ENCODING"), key.clone()]);
                let CommandResponse::BulkString(enc) = enc else {
                    return CommandResponse::error("ERR no such key");
                };
                let len = self.shards.shard_for_key(&key).with_store(|s| {
                    s.peek(&key)
                        .map(|e| entry_bytes(key.len(), e, 0))
                        .unwrap_or(0)
                });
                CommandResponse::simple(&format!(
                    "Value at:0x0 refcount:1 encoding:{} serializedlength:{} lru:0 lru_seconds_idle:0",
                    String::from_utf8_lossy(&enc),
                    len
                ))
            }
            "CHANGE-REPL-ID" => CommandResponse::ok(),
            "JMAP" | "RELOAD" | "LOADAOF" | "SEGFAULT" | "PANIC" | "RESTART"
            | "CRASH-AND-RECOVER" => {
                CommandResponse::error(format!("ERR DEBUG {} is not supported by this server", sub))
            }
            "HELP" => CommandResponse::array(
                [
                    "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "OBJECT <key>",
                    "SET-ACTIVE-EXPIRE <0|1>",
                    "SLEEP <seconds>",
                ]
                .iter()
                .map(|l| CommandResponse::simple(l))
                .collect(),
            ),
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try DEBUG HELP.",
                sub.to_lowercase()
            )),
        }
    }

    // -----------------------------------------------------------------------
    // CLIENT TRACKING / CACHING / GETREDIR / TRACKINGINFO
    // -----------------------------------------------------------------------

    pub(crate) fn client_tracking(&self, session: &mut Session, rest: &[Bytes]) -> CommandResponse {
        let Some(mode) = rest.first() else {
            return CommandResponse::wrong_arity("client|tracking");
        };
        let on = if mode.eq_ignore_ascii_case(b"ON") {
            true
        } else if mode.eq_ignore_ascii_case(b"OFF") {
            false
        } else {
            return CommandResponse::syntax_error();
        };
        let mut opts = crate::tracking::TrackingOpts::default();
        let mut i = 1;
        while i < rest.len() {
            let opt = Upper::new(&rest[i]);
            match opt.as_str() {
                "REDIRECT" if i + 1 < rest.len() => {
                    let Some(id) = std::str::from_utf8(&rest[i + 1])
                        .ok()
                        .and_then(|s| s.parse::<u64>().ok())
                    else {
                        return CommandResponse::error("ERR Invalid client ID");
                    };
                    opts.redirect = Some(id);
                    i += 2;
                }
                "PREFIX" if i + 1 < rest.len() => {
                    opts.prefixes.push(rest[i + 1].clone());
                    i += 2;
                }
                "BCAST" => {
                    opts.bcast = true;
                    i += 1;
                }
                "OPTIN" => {
                    opts.optin = true;
                    i += 1;
                }
                "OPTOUT" => {
                    opts.optout = true;
                    i += 1;
                }
                "NOLOOP" => {
                    opts.noloop = true;
                    i += 1;
                }
                _ => return CommandResponse::syntax_error(),
            }
        }
        if !on {
            if session.tracking.take().is_some() {
                self.tracking.disable(session.info.id);
            }
            session.set_redir(None);
            session.caching = None;
            return CommandResponse::ok();
        }
        if !opts.prefixes.is_empty() && !opts.bcast {
            return CommandResponse::error("ERR PREFIX option requires BCAST mode to be enabled");
        }
        if opts.optin && opts.optout {
            return CommandResponse::error("ERR You can't use both OPTIN and OPTOUT");
        }
        if opts.bcast && (opts.optin || opts.optout) {
            return CommandResponse::error("ERR OPTIN and OPTOUT are not compatible with BCAST");
        }
        if let Some(r) = opts.redirect {
            if r != session.info.id && session.registry().get(r).is_none() {
                return CommandResponse::error(
                    "ERR The client ID you want redirect to does not exist",
                );
            }
        }
        if let Some(prev) = &session.tracking {
            if prev.bcast != opts.bcast {
                return CommandResponse::error(
                    "ERR You can't switch BCAST mode on/off before disabling tracking for this \
                     client, and then re-enabling it with a different mode.",
                );
            }
            self.tracking.disable(session.info.id);
        }
        self.tracking.enable(session.info.id, &opts);
        session.set_redir(Some(opts.redirect.unwrap_or(0)));
        session.tracking = Some(opts);
        CommandResponse::ok()
    }

    pub(crate) fn client_caching(&self, session: &mut Session, rest: &[Bytes]) -> CommandResponse {
        let [v] = rest else {
            return CommandResponse::wrong_arity("client|caching");
        };
        let Some(t) = &session.tracking else {
            return CommandResponse::error(
                "ERR CLIENT CACHING can be called only when the client is in tracking mode with \
                 OPTIN or OPTOUT mode enabled",
            );
        };
        if v.eq_ignore_ascii_case(b"YES") {
            if !t.optin {
                return CommandResponse::error(
                    "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.",
                );
            }
            session.caching = Some(true);
        } else if v.eq_ignore_ascii_case(b"NO") {
            if !t.optout {
                return CommandResponse::error(
                    "ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.",
                );
            }
            session.caching = Some(false);
        } else {
            return CommandResponse::syntax_error();
        }
        CommandResponse::ok()
    }

    pub(crate) fn client_getredir(&self, session: &Session) -> CommandResponse {
        match &session.tracking {
            None => CommandResponse::integer(-1),
            Some(t) => CommandResponse::integer(t.redirect.map_or(0, |r| r as i64)),
        }
    }

    pub(crate) fn client_trackinginfo(&self, session: &Session) -> CommandResponse {
        let b = |s: &str| CommandResponse::bulk_string(s);
        let (flags, redirect, prefixes) = match &session.tracking {
            None => (vec![b("off")], -1, vec![]),
            Some(t) => {
                let mut f = vec![b("on")];
                if t.bcast {
                    f.push(b("bcast"));
                }
                if t.optin {
                    f.push(b("optin"));
                }
                if t.optout {
                    f.push(b("optout"));
                }
                match session.caching {
                    Some(true) => f.push(b("caching-yes")),
                    Some(false) => f.push(b("caching-no")),
                    None => {}
                }
                if t.noloop {
                    f.push(b("noloop"));
                }
                (
                    f,
                    t.redirect.map_or(0, |r| r as i64),
                    t.prefixes
                        .iter()
                        .cloned()
                        .map(CommandResponse::bulk)
                        .collect(),
                )
            }
        };
        CommandResponse::array(vec![
            b("flags"),
            CommandResponse::Set(flags),
            b("redirect"),
            CommandResponse::integer(redirect),
            b("prefixes"),
            CommandResponse::array(prefixes),
        ])
    }

    // -----------------------------------------------------------------------
    // CONFIG
    // -----------------------------------------------------------------------

    /// Every parameter `CONFIG GET` reports, with its current value.
    pub(crate) fn config_params(&self) -> Vec<(String, String)> {
        let cfg = self.persistence.config();
        let yes_no = |b: bool| if b { "yes" } else { "no" }.to_string();
        let mut v: Vec<(String, String)> = vec![
            ("databases".into(), self.databases.to_string()),
            (
                "save".into(),
                if cfg.save_interval > 0 {
                    format!("{} 1", cfg.save_interval)
                } else {
                    String::new()
                },
            ),
            ("appendonly".into(), yes_no(cfg.appendonly)),
            (
                "appendfsync".into(),
                match cfg.fsync {
                    solikv_persist::FsyncPolicy::Always => "always",
                    solikv_persist::FsyncPolicy::Everysec => "everysec",
                    solikv_persist::FsyncPolicy::No => "no",
                }
                .into(),
            ),
            ("dir".into(), cfg.dir.display().to_string()),
            ("dbfilename".into(), format!("{}.rdb", cfg.dbfilename)),
            (
                "appenddirname".into(),
                solikv_persist::aof::AOF_DIRNAME.to_string(),
            ),
            (
                "auto-aof-rewrite-percentage".into(),
                cfg.auto_rewrite_percentage.to_string(),
            ),
            (
                "auto-aof-rewrite-min-size".into(),
                cfg.auto_rewrite_min_size.to_string(),
            ),
            (
                "notify-keyspace-events".into(),
                flags_to_string(self.notify_flags.load(Ordering::Relaxed)),
            ),
            (
                "port".into(),
                self.tcp_port.load(Ordering::Relaxed).to_string(),
            ),
            (
                "maxmemory".into(),
                self.maxmemory.load(Ordering::Relaxed).to_string(),
            ),
            (
                "maxmemory-policy".into(),
                self.maxmemory_policy().name().to_string(),
            ),
            (
                "maxmemory-samples".into(),
                self.maxmemory_samples.load(Ordering::Relaxed).to_string(),
            ),
            ("timeout".into(), self.idle_timeout().to_string()),
            ("maxclients".into(), self.maxclients().to_string()),
            (
                "slowlog-log-slower-than".into(),
                self.stats
                    .slowlog_threshold_us
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "slowlog-max-len".into(),
                self.stats
                    .slowlog_max_len
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "latency-monitor-threshold".into(),
                self.stats
                    .latency_threshold_ms
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "busy-reply-threshold".into(),
                self.busy_threshold_ms().to_string(),
            ),
            (
                "lua-time-limit".into(),
                self.busy_threshold_ms().to_string(),
            ),
            (
                "tracking-table-max-keys".into(),
                self.tracking.max_keys.load(Ordering::Relaxed).to_string(),
            ),
            ("hz".into(), self.shards.hz().to_string()),
            (
                "requirepass".into(),
                // Redis shows the password; here only whether one is set.
                if self.acl.has_requirepass() {
                    "********"
                } else {
                    ""
                }
                .into(),
            ),
            (
                "enable-debug-command".into(),
                ["no", "local", "yes"][self.debug_mode.load(Ordering::Relaxed) as usize % 3].into(),
            ),
            ("lua-memory-limit".into(), self.lua_memory_limit.to_string()),
        ];
        for (k, val) in self.static_config.read().iter() {
            if !v.iter().any(|(n, _)| n == k) {
                v.push((k.clone(), val.clone()));
            }
        }
        v
    }

    /// Parameters `CONFIG SET` changes at runtime.
    const MUTABLE: &'static [&'static str] = &[
        "notify-keyspace-events",
        "maxmemory",
        "maxmemory-policy",
        "maxmemory-samples",
        "timeout",
        "maxclients",
        "slowlog-log-slower-than",
        "slowlog-max-len",
        "latency-monitor-threshold",
        "busy-reply-threshold",
        "lua-time-limit",
        "tracking-table-max-keys",
        "hz",
        "requirepass",
    ];

    fn config_validate(&self, param: &str, value: &str) -> Result<(), String> {
        let int = |min: i64, max: i64| -> Result<(), String> {
            match value.parse::<i64>() {
                Ok(n) if (min..=max).contains(&n) => Ok(()),
                _ => Err(format!(
                    "ERR CONFIG SET failed (possibly related to argument '{}') - argument couldn't be \
                     parsed into an integer or is out of range",
                    param
                )),
            }
        };
        match param {
            "notify-keyspace-events" | "requirepass" => Ok(()),
            "maxmemory" => solikv_core::memory::parse_size(value)
                .map(|_| ())
                .ok_or_else(|| {
                    format!(
                        "ERR CONFIG SET failed (possibly related to argument '{}') - argument \
                         couldn't be parsed into an integer",
                        param
                    )
                }),
            "maxmemory-policy" => EvictionPolicy::parse(value).map(|_| ()).ok_or_else(|| {
                format!(
                    "ERR CONFIG SET failed (possibly related to argument '{}') - argument(s) must \
                     be one of the following: volatile-lru, allkeys-lru, volatile-lfu, \
                     allkeys-lfu, volatile-random, allkeys-random, volatile-ttl, noeviction",
                    param
                )
            }),
            "maxmemory-samples" => int(1, 64),
            "timeout" => int(0, i32::MAX as i64),
            "maxclients" => int(1, i32::MAX as i64),
            "slowlog-log-slower-than" => int(-1, i64::MAX),
            "slowlog-max-len" => int(0, i32::MAX as i64),
            "latency-monitor-threshold" => int(0, i64::MAX),
            "busy-reply-threshold" | "lua-time-limit" => int(0, i64::MAX),
            "tracking-table-max-keys" => int(0, i64::MAX),
            "hz" => int(1, 500),
            _ => Ok(()),
        }
    }

    fn config_apply(&self, param: &str, value: &str) {
        match param {
            "notify-keyspace-events" => self
                .notify_flags
                .store(parse_notify_flags(value), Ordering::Relaxed),
            "maxmemory" => self.set_maxmemory(solikv_core::memory::parse_size(value).unwrap_or(0)),
            "maxmemory-policy" => {
                if let Some(p) = EvictionPolicy::parse(value) {
                    self.set_maxmemory_policy(p);
                }
            }
            "maxmemory-samples" => self
                .maxmemory_samples
                .store(value.parse().unwrap_or(5), Ordering::Relaxed),
            "timeout" => self.set_idle_timeout(value.parse().unwrap_or(0)),
            "maxclients" => self.set_maxclients(value.parse().unwrap_or(10_000)),
            "slowlog-log-slower-than" => self
                .stats
                .slowlog_threshold_us
                .store(value.parse().unwrap_or(10_000), Ordering::Relaxed),
            "slowlog-max-len" => self
                .stats
                .slowlog_max_len
                .store(value.parse().unwrap_or(128), Ordering::Relaxed),
            "latency-monitor-threshold" => self
                .stats
                .latency_threshold_ms
                .store(value.parse().unwrap_or(0), Ordering::Relaxed),
            "busy-reply-threshold" | "lua-time-limit" => {
                self.set_busy_threshold_ms(value.parse().unwrap_or(5000))
            }
            "tracking-table-max-keys" => self
                .tracking
                .max_keys
                .store(value.parse().unwrap_or(1_000_000), Ordering::Relaxed),
            "hz" => self.shards.set_hz(value.parse().unwrap_or(10)),
            "requirepass" => self.acl.set_requirepass(Some(value)),
            _ => {}
        }
    }

    pub(crate) fn config_command(&self, args: &[Bytes]) -> CommandResponse {
        let sub = Upper::new(&args[0]);
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
                        if super::config_glob(&pat, name) && seen.insert(name.clone()) {
                            out.push(CommandResponse::bulk(Bytes::from(name.clone())));
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
                let known: Vec<String> = self.config_params().into_iter().map(|(n, _)| n).collect();
                // Validate everything first: Redis applies all or none.
                for pair in args[1..].chunks(2) {
                    let param = String::from_utf8_lossy(&pair[0]).to_ascii_lowercase();
                    let value = String::from_utf8_lossy(&pair[1]);
                    if !Self::MUTABLE.contains(&param.as_str()) {
                        return CommandResponse::error(if known.contains(&param) {
                            format!(
                                "ERR CONFIG SET failed (possibly related to argument '{}') - \
                                 can't set immutable config",
                                param
                            )
                        } else {
                            format!(
                                "ERR Unknown option or number of arguments for CONFIG SET - '{}'",
                                param
                            )
                        });
                    }
                    if let Err(e) = self.config_validate(&param, &value) {
                        return CommandResponse::error(e);
                    }
                }
                for pair in args[1..].chunks(2) {
                    let param = String::from_utf8_lossy(&pair[0]).to_ascii_lowercase();
                    self.config_apply(&param, &String::from_utf8_lossy(&pair[1]));
                }
                CommandResponse::ok()
            }
            "RESETSTAT" => {
                self.clients.reset_stats();
                self.stats.reset();
                CommandResponse::ok()
            }
            "REWRITE" => match self.config_rewrite() {
                Ok(()) => CommandResponse::ok(),
                Err(e) => CommandResponse::error(e),
            },
            "HELP" => CommandResponse::array(
                [
                    "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "GET <pattern>",
                    "SET <directive> <value>",
                    "RESETSTAT",
                    "REWRITE",
                ]
                .iter()
                .map(|l| CommandResponse::simple(l))
                .collect(),
            ),
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try CONFIG HELP.",
                sub.to_lowercase()
            )),
        }
    }

    /// `CONFIG REWRITE`: the configuration file with the runtime values of
    /// the mutable parameters, comments and other lines kept.
    fn config_rewrite(&self) -> Result<(), String> {
        let Some(path) = self.config_file() else {
            return Err("ERR The server is running without a config file".into());
        };
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        let params = self.config_params();
        let value_of = |n: &str| params.iter().find(|(k, _)| k == n).map(|(_, v)| v.clone());
        let quote = |v: &str| {
            if v.is_empty() || v.contains(char::is_whitespace) || v.contains('"') {
                format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\""))
            } else {
                v.to_string()
            }
        };
        let mut written = std::collections::HashSet::new();
        let mut out = String::new();
        for line in text.lines() {
            let directive = line
                .split_ascii_whitespace()
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            if !line.trim_start().starts_with('#')
                && Self::MUTABLE.contains(&directive.as_str())
                && directive != "requirepass"
            {
                if written.insert(directive.clone()) {
                    if let Some(v) = value_of(&directive) {
                        out.push_str(&format!("{} {}\n", directive, quote(&v)));
                    }
                }
                continue;
            }
            out.push_str(line);
            out.push('\n');
        }
        let defaults: &[(&str, &str)] = &[
            ("notify-keyspace-events", ""),
            ("maxmemory", "0"),
            ("maxmemory-policy", "noeviction"),
            ("maxmemory-samples", "5"),
            ("timeout", "0"),
            ("maxclients", "10000"),
            ("slowlog-log-slower-than", "10000"),
            ("slowlog-max-len", "128"),
            ("latency-monitor-threshold", "0"),
            ("busy-reply-threshold", "5000"),
            ("tracking-table-max-keys", "1000000"),
            ("hz", "10"),
        ];
        let mut header = false;
        for (name, default) in defaults {
            if written.contains(*name) {
                continue;
            }
            if let Some(v) = value_of(name) {
                if v != *default {
                    if !header {
                        out.push_str("# Generated by CONFIG REWRITE\n");
                        header = true;
                    }
                    out.push_str(&format!("{} {}\n", name, quote(&v)));
                }
            }
        }
        solikv_persist::fsutil::write_atomic(&path, out)
            .map_err(|e| format!("ERR Rewriting config file: {}", e))
    }

    // -----------------------------------------------------------------------
    // INFO
    // -----------------------------------------------------------------------

    pub(crate) fn info(&self, args: &[Bytes]) -> CommandResponse {
        const DEFAULT: &[&str] = &[
            "server",
            "clients",
            "memory",
            "persistence",
            "stats",
            "replication",
            "cpu",
            "modules",
            "errorstats",
            "cluster",
            "keyspace",
        ];
        const ALL_EXTRA: &[&str] = &["commandstats", "latencystats"];
        let mut wanted: Vec<String> = args
            .iter()
            .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase())
            .collect();
        let mut order: Vec<&str> = DEFAULT.to_vec();
        order.extend_from_slice(ALL_EXTRA);
        if wanted.is_empty() || wanted.iter().any(|w| w == "default") {
            wanted.extend(DEFAULT.iter().map(|s| s.to_string()));
        }
        if wanted.iter().any(|w| w == "all" || w == "everything") {
            wanted.extend(order.iter().map(|s| s.to_string()));
        }
        let mut out = String::new();
        for section in order {
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
                 redis_git_sha1:00000000\r\n\
                 redis_git_dirty:0\r\n\
                 redis_mode:{}\r\n\
                 os:{} {}\r\n\
                 arch_bits:{}\r\n\
                 multiplexing_api:epoll\r\n\
                 process_id:{}\r\n\
                 run_id:{}\r\n\
                 tcp_port:{}\r\n\
                 server_time_usec:{}\r\n\
                 uptime_in_seconds:{}\r\n\
                 uptime_in_days:{}\r\n\
                 hz:{}\r\n\
                 configured_hz:{}\r\n\
                 executable:{}\r\n\
                 config_file:{}\r\n",
                env!("CARGO_PKG_VERSION"),
                if self
                    .static_config
                    .read()
                    .iter()
                    .any(|(k, v)| k == "cluster-enabled" && v == "yes")
                {
                    "cluster"
                } else {
                    "standalone"
                },
                std::env::consts::OS,
                std::env::consts::ARCH,
                usize::BITS,
                std::process::id(),
                run_id(),
                self.tcp_port.load(Ordering::Relaxed),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros())
                    .unwrap_or(0),
                uptime,
                uptime / 86400,
                self.shards.hz(),
                self.shards.hz(),
                std::env::current_exe()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
                self.config_file()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
            ),
            "clients" => format!(
                "# Clients\r\n\
                 connected_clients:{}\r\n\
                 cluster_connections:0\r\n\
                 maxclients:{}\r\n\
                 client_recent_max_input_buffer:0\r\n\
                 client_recent_max_output_buffer:0\r\n\
                 blocked_clients:{}\r\n\
                 tracking_clients:{}\r\n\
                 clients_in_timeout_table:{}\r\n\
                 total_blocking_keys:{}\r\n\
                 total_blocking_keys_on_nokey:0\r\n",
                self.clients.count(),
                self.maxclients(),
                self.blocking.blocked_clients(),
                self.tracking.tracking_clients(),
                self.blocking.blocked_clients(),
                self.blocking.blocking_keys(),
            ),
            "memory" => {
                let used = solikv_core::memory::used() as u64;
                let max = self.maxmemory.load(Ordering::Relaxed);
                let rss = rss_bytes();
                let peak = self.stats.peak_memory().max(used);
                format!(
                    "# Memory\r\n\
                     used_memory:{}\r\n\
                     used_memory_human:{}\r\n\
                     used_memory_rss:{}\r\n\
                     used_memory_rss_human:{}\r\n\
                     used_memory_peak:{}\r\n\
                     used_memory_peak_human:{}\r\n\
                     used_memory_lua:{}\r\n\
                     used_memory_scripts:0\r\n\
                     number_of_cached_scripts:{}\r\n\
                     number_of_functions:{}\r\n\
                     number_of_libraries:{}\r\n\
                     maxmemory:{}\r\n\
                     maxmemory_human:{}\r\n\
                     maxmemory_policy:{}\r\n\
                     mem_fragmentation_ratio:{:.2}\r\n\
                     mem_allocator:mimalloc\r\n",
                    used,
                    solikv_core::memory::human(used),
                    rss,
                    solikv_core::memory::human(rss),
                    peak,
                    solikv_core::memory::human(peak),
                    0,
                    self.script_cache.len(),
                    self.functions.counts().1,
                    self.functions.counts().0,
                    max,
                    solikv_core::memory::human(max),
                    self.maxmemory_policy().name(),
                    if used > 0 {
                        rss as f64 / used as f64
                    } else {
                        1.0
                    },
                )
            }
            "persistence" => self.persistence.stats.info_section(self.dirty()),
            "stats" => {
                let (hits, misses) = self.stats.keyspace_hits_misses();
                let pubsub_channels = self
                    .pubsub
                    .numsub()
                    .into_iter()
                    .filter(|(ch, n)| {
                        *n > 0 && !ch.starts_with(solikv_pubsub::broker::SHARD_PREFIX)
                    })
                    .count();
                let shard_channels = self
                    .pubsub
                    .numsub()
                    .into_iter()
                    .filter(|(ch, n)| *n > 0 && ch.starts_with(solikv_pubsub::broker::SHARD_PREFIX))
                    .count();
                let pubsub_patterns = self
                    .pubsub
                    .pattern_numsub()
                    .into_iter()
                    .filter(|(_, n)| *n > 0)
                    .count();
                format!(
                    "# Stats\r\n\
                     total_connections_received:{}\r\n\
                     total_commands_processed:{}\r\n\
                     instantaneous_ops_per_sec:{}\r\n\
                     rejected_connections:{}\r\n\
                     expired_keys:{}\r\n\
                     evicted_keys:{}\r\n\
                     keyspace_hits:{}\r\n\
                     keyspace_misses:{}\r\n\
                     pubsub_channels:{}\r\n\
                     pubsub_patterns:{}\r\n\
                     pubsubshard_channels:{}\r\n\
                     tracking_total_keys:{}\r\n\
                     total_error_replies:{}\r\n\
                     total_blocking_keys:{}\r\n",
                    self.clients.connections_received(),
                    self.clients.commands_processed(),
                    self.stats.ops_per_sec(),
                    self.stats.rejected_connections.load(Ordering::Relaxed),
                    self.shards.expired_keys(),
                    self.shards.evicted_keys(),
                    hits,
                    misses,
                    pubsub_channels,
                    pubsub_patterns,
                    shard_channels,
                    self.tracking.tracked_keys(),
                    self.stats.total_errors(),
                    self.blocking.blocking_keys(),
                )
            }
            "replication" => self.info_replication(),
            "cpu" => {
                let (sys, user) = cpu_times();
                format!(
                    "# CPU\r\nused_cpu_sys:{:.6}\r\nused_cpu_user:{:.6}\r\n\
                     used_cpu_sys_children:0.000000\r\nused_cpu_user_children:0.000000\r\n",
                    sys, user
                )
            }
            "modules" => "# Modules\r\n".to_string(),
            "cluster" => format!(
                "# Cluster\r\ncluster_enabled:{}\r\n",
                self.static_config
                    .read()
                    .iter()
                    .any(|(k, v)| k == "cluster-enabled" && v == "yes") as u8
            ),
            "errorstats" => self.stats.errorstats(),
            "commandstats" => self.stats.commandstats(),
            "latencystats" => "# Latencystats\r\n".to_string(),
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

/// A 40-character id for this process (`INFO server`'s `run_id`).
fn run_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| {
        let seed = format!("{:?}{}", std::time::SystemTime::now(), std::process::id());
        crate::lua::sha1_hex(&seed)
    })
}

/// Resident set size, from `/proc/self/statm`.
fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}

/// `(system, user)` CPU seconds used by the process.
fn cpu_times() -> (f64, f64) {
    // SAFETY: getrusage fills the struct it is given.
    unsafe {
        let mut r: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut r) != 0 {
            return (0.0, 0.0);
        }
        let t = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 / 1e6;
        (t(r.ru_stime), t(r.ru_utime))
    }
}
