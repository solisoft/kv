//! Observability: per-command statistics (`INFO commandstats`), error
//! counts (`INFO errorstats`), keyspace hits and misses, `SLOWLOG`,
//! `LATENCY`, and the `MONITOR` feed.
//!
//! Everything on the command path is per thread or behind a check that is
//! false when the feature is unused:
//!
//! - counters live in a block per thread (each thread writes only its own,
//!   so a counter update is a plain load and store, no shared cache line);
//!   `INFO` sums the blocks;
//! - timing uses the CPU's time-stamp counter where there is one (a few
//!   nanoseconds), calibrated once against the system clock;
//! - the slow log and latency monitor take their lock only for a command
//!   over their threshold; `MONITOR` formats nothing unless a client
//!   monitors.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use solikv_core::CommandResponse;

use crate::commands::{self, Cmd};

// ---------------------------------------------------------------------------
// A cheap clock
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[inline]
fn raw_ticks() -> u64 {
    // SAFETY: RDTSC has no preconditions on x86_64.
    unsafe { core::arch::x86_64::_rdtsc() }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn raw_ticks() -> u64 {
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_nanos() as u64
}

/// Ticks per microsecond, measured once.
fn ticks_per_us() -> f64 {
    static RATE: OnceLock<f64> = OnceLock::new();
    *RATE.get_or_init(|| {
        let t0 = std::time::Instant::now();
        let c0 = raw_ticks();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let c1 = raw_ticks();
        let us = t0.elapsed().as_secs_f64() * 1e6;
        let rate = (c1.wrapping_sub(c0)) as f64 / us;
        if rate.is_finite() && rate > 0.0 {
            rate
        } else {
            1000.0
        }
    })
}

/// A point in time for [`elapsed_us`].
#[derive(Clone, Copy)]
pub struct Stamp(u64);

#[inline]
pub fn stamp() -> Stamp {
    Stamp(raw_ticks())
}

#[inline]
pub fn elapsed_us(s: Stamp) -> u64 {
    (raw_ticks().wrapping_sub(s.0) as f64 / ticks_per_us()) as u64
}

/// Calibrate the clock now (at startup) rather than on the first command.
pub fn calibrate() {
    let _ = ticks_per_us();
}

// ---------------------------------------------------------------------------
// Per-thread counters
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CmdCounters {
    calls: AtomicU64,
    usec: AtomicU64,
    rejected: AtomicU64,
    failed: AtomicU64,
}

struct ThreadBlock {
    cmds: Box<[CmdCounters]>,
    hits: AtomicU64,
    misses: AtomicU64,
}

/// Single-writer increment: only the owning thread writes its block.
#[inline]
fn bump(c: &AtomicU64, n: u64) {
    c.store(c.load(Ordering::Relaxed).wrapping_add(n), Ordering::Relaxed);
}

thread_local! {
    /// This thread's block for each `Stats` (by id): usually one.
    static MINE: std::cell::RefCell<Vec<(u64, Arc<ThreadBlock>)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn new_block() -> Arc<ThreadBlock> {
    let n = Cmd::Unknown as usize + 1;
    Arc::new(ThreadBlock {
        cmds: (0..n).map(|_| CmdCounters::default()).collect(),
        hits: AtomicU64::new(0),
        misses: AtomicU64::new(0),
    })
}

static NEXT_STATS_ID: AtomicU64 = AtomicU64::new(1);

// ---------------------------------------------------------------------------
// Slow log, latency, errors
// ---------------------------------------------------------------------------

pub struct SlowEntry {
    id: u64,
    time: u64,
    duration_us: u64,
    args: Vec<Bytes>,
    addr: String,
    name: String,
}

#[derive(Default)]
struct LatencyEvent {
    latest_time: u64,
    latest_ms: u64,
    max_ms: u64,
    history: VecDeque<(u64, u64)>,
}

/// Engine-wide statistics and the observability features' settings.
pub struct Stats {
    /// Identifies this instance's blocks in each thread's `MINE`.
    id: u64,
    /// Every thread's block for this instance.
    blocks: Mutex<Vec<Arc<ThreadBlock>>>,
    /// `slowlog-log-slower-than` (µs; negative disables, 0 logs all).
    pub slowlog_threshold_us: AtomicI64,
    pub slowlog_max_len: AtomicUsize,
    slowlog: Mutex<VecDeque<SlowEntry>>,
    slowlog_id: AtomicU64,
    /// `latency-monitor-threshold` (ms; 0 disables).
    pub latency_threshold_ms: AtomicU64,
    latency: Mutex<HashMap<&'static str, LatencyEvent>>,
    errors: Mutex<HashMap<String, u64>>,
    /// `CONFIG RESETSTAT` baselines, per command.
    base: Mutex<Vec<(u64, u64, u64, u64)>>,
    base_hits: AtomicU64,
    base_misses: AtomicU64,
    monitor: tokio::sync::broadcast::Sender<Bytes>,
    monitors: AtomicUsize,
    /// `instantaneous_ops_per_sec` samples: (unix ms, total commands).
    samples: Mutex<VecDeque<(u64, u64)>>,
    peak_memory: AtomicU64,
    pub rejected_connections: AtomicU64,
}

impl Default for Stats {
    fn default() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(4096);
        Self {
            id: NEXT_STATS_ID.fetch_add(1, Ordering::Relaxed),
            blocks: Mutex::new(Vec::new()),
            slowlog_threshold_us: AtomicI64::new(10_000),
            slowlog_max_len: AtomicUsize::new(128),
            slowlog: Mutex::new(VecDeque::new()),
            slowlog_id: AtomicU64::new(0),
            latency_threshold_ms: AtomicU64::new(0),
            latency: Mutex::new(HashMap::new()),
            errors: Mutex::new(HashMap::new()),
            base: Mutex::new(Vec::new()),
            base_hits: AtomicU64::new(0),
            base_misses: AtomicU64::new(0),
            monitor: tx,
            monitors: AtomicUsize::new(0),
            samples: Mutex::new(VecDeque::new()),
            peak_memory: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
        }
    }
}

const SLOWLOG_ARGS: usize = 32;
const SLOWLOG_ARG_LEN: usize = 128;
const LATENCY_HISTORY: usize = 160;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Stats {
    /// Run `f` on this thread's counter block.
    #[inline]
    fn with_block<R>(&self, f: impl FnOnce(&ThreadBlock) -> R) -> R {
        MINE.with(|m| {
            let mut m = m.borrow_mut();
            if let Some((_, b)) = m.iter().find(|(id, _)| *id == self.id) {
                return f(b);
            }
            let b = new_block();
            self.blocks.lock().push(b.clone());
            let r = f(&b);
            m.push((self.id, b));
            r
        })
    }

    /// Record one executed command.
    #[inline]
    pub fn record(&self, id: Cmd, us: u64, failed: bool) {
        self.with_block(|m| {
            if let Some(c) = m.cmds.get(id as usize) {
                bump(&c.calls, 1);
                bump(&c.usec, us);
                if failed {
                    bump(&c.failed, 1);
                }
            }
        });
    }

    /// A command refused before it ran (ACL, OOM, arity, BUSY).
    pub fn record_rejected(&self, id: Cmd) {
        self.with_block(|m| {
            if let Some(c) = m.cmds.get(id as usize) {
                bump(&c.rejected, 1);
            }
        });
    }

    #[inline]
    pub fn keyspace_hit(&self, hit: bool) {
        self.with_block(|m| bump(if hit { &m.hits } else { &m.misses }, 1));
    }

    pub fn record_error(&self, msg: &str) {
        let code = msg.split(' ').next().unwrap_or("ERR");
        let code = if code.is_empty() { "ERR" } else { code };
        *self.errors.lock().entry(code.to_string()).or_insert(0) += 1;
    }

    fn totals(&self) -> Vec<(u64, u64, u64, u64)> {
        let n = Cmd::Unknown as usize + 1;
        let mut out = vec![(0u64, 0u64, 0u64, 0u64); n];
        for b in self.blocks.lock().iter() {
            for (i, c) in b.cmds.iter().enumerate() {
                out[i].0 += c.calls.load(Ordering::Relaxed);
                out[i].1 += c.usec.load(Ordering::Relaxed);
                out[i].2 += c.rejected.load(Ordering::Relaxed);
                out[i].3 += c.failed.load(Ordering::Relaxed);
            }
        }
        let base = self.base.lock();
        if base.len() == n {
            for (o, b) in out.iter_mut().zip(base.iter()) {
                o.0 = o.0.saturating_sub(b.0);
                o.1 = o.1.saturating_sub(b.1);
                o.2 = o.2.saturating_sub(b.2);
                o.3 = o.3.saturating_sub(b.3);
            }
        }
        out
    }

    fn raw_hits(&self) -> (u64, u64) {
        let mut h = (0, 0);
        for b in self.blocks.lock().iter() {
            h.0 += b.hits.load(Ordering::Relaxed);
            h.1 += b.misses.load(Ordering::Relaxed);
        }
        h
    }

    pub fn keyspace_hits_misses(&self) -> (u64, u64) {
        let (h, m) = self.raw_hits();
        (
            h.saturating_sub(self.base_hits.load(Ordering::Relaxed)),
            m.saturating_sub(self.base_misses.load(Ordering::Relaxed)),
        )
    }

    /// `CONFIG RESETSTAT`.
    pub fn reset(&self) {
        let raw = {
            *self.base.lock() = Vec::new();
            self.totals()
        };
        *self.base.lock() = raw;
        let (h, m) = self.raw_hits();
        self.base_hits.store(h, Ordering::Relaxed);
        self.base_misses.store(m, Ordering::Relaxed);
        self.errors.lock().clear();
        self.rejected_connections.store(0, Ordering::Relaxed);
    }

    /// `INFO commandstats`.
    pub fn commandstats(&self) -> String {
        let mut out = String::from("# Commandstats\r\n");
        for (i, (calls, usec, rejected, failed)) in self.totals().into_iter().enumerate() {
            if calls == 0 && rejected == 0 {
                continue;
            }
            let Some(spec) = commands::all().get(i) else {
                continue;
            };
            out.push_str(&format!(
                "cmdstat_{}:calls={},usec={},usec_per_call={:.2},rejected_calls={},failed_calls={}\r\n",
                spec.name.to_lowercase(),
                calls,
                usec,
                if calls > 0 { usec as f64 / calls as f64 } else { 0.0 },
                rejected,
                failed
            ));
        }
        out
    }

    /// Per-command totals `(name, calls, usec)` (`/metrics`).
    pub fn command_totals(&self) -> Vec<(&'static str, u64, u64, u64, u64)> {
        self.totals()
            .into_iter()
            .enumerate()
            .filter(|(_, t)| t.0 > 0 || t.2 > 0)
            .filter_map(|(i, t)| commands::all().get(i).map(|s| (s.name, t.0, t.1, t.2, t.3)))
            .collect()
    }

    /// `INFO errorstats`.
    pub fn errorstats(&self) -> String {
        let mut out = String::from("# Errorstats\r\n");
        let errors = self.errors.lock();
        let mut v: Vec<_> = errors.iter().collect();
        v.sort();
        for (code, n) in v {
            out.push_str(&format!("errorstat_{}:count={}\r\n", code, n));
        }
        out
    }

    pub fn total_errors(&self) -> u64 {
        self.errors.lock().values().sum()
    }

    // ---- slow log ----

    /// Whether a command that took `us` goes to the slow log.
    #[inline]
    pub fn is_slow(&self, us: u64) -> bool {
        let threshold = self.slowlog_threshold_us.load(Ordering::Relaxed);
        threshold >= 0 && us as i64 >= threshold
    }

    /// Log a command slower than `slowlog-log-slower-than`.
    pub fn maybe_slowlog(&self, us: u64, name: &str, args: &[Bytes], addr: &str, client: &str) {
        let threshold = self.slowlog_threshold_us.load(Ordering::Relaxed);
        if threshold < 0 || (us as i64) < threshold {
            return;
        }
        let mut argv = Vec::with_capacity((args.len() + 1).min(SLOWLOG_ARGS));
        argv.push(Bytes::from(name.to_lowercase()));
        let max_args = SLOWLOG_ARGS - 1;
        for (i, a) in args.iter().enumerate() {
            if i + 1 >= max_args && args.len() > max_args {
                argv.push(Bytes::from(format!(
                    "... ({} more arguments)",
                    args.len() - i
                )));
                break;
            }
            if a.len() > SLOWLOG_ARG_LEN {
                let mut v = a[..SLOWLOG_ARG_LEN].to_vec();
                v.extend_from_slice(
                    format!("... ({} more bytes)", a.len() - SLOWLOG_ARG_LEN).as_bytes(),
                );
                argv.push(Bytes::from(v));
            } else {
                argv.push(a.clone());
            }
        }
        let entry = SlowEntry {
            id: self.slowlog_id.fetch_add(1, Ordering::Relaxed),
            time: now_ms() / 1000,
            duration_us: us,
            args: argv,
            addr: addr.to_string(),
            name: client.to_string(),
        };
        let max = self.slowlog_max_len.load(Ordering::Relaxed);
        let mut log = self.slowlog.lock();
        log.push_front(entry);
        log.truncate(max);
    }

    /// `SLOWLOG <sub>`.
    pub fn slowlog_command(&self, args: &[Bytes]) -> CommandResponse {
        let sub = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        match sub.as_str() {
            "GET" => {
                let count = match args.get(1) {
                    None => 10,
                    Some(c) => match std::str::from_utf8(c)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(-1) => usize::MAX,
                        Some(n) if n >= 0 => n as usize,
                        _ => {
                            return CommandResponse::error(
                                "ERR count should be greater than or equal to -1",
                            )
                        }
                    },
                };
                let log = self.slowlog.lock();
                CommandResponse::array(
                    log.iter()
                        .take(count)
                        .map(|e| {
                            CommandResponse::array(vec![
                                CommandResponse::integer(e.id as i64),
                                CommandResponse::integer(e.time as i64),
                                CommandResponse::integer(e.duration_us as i64),
                                CommandResponse::array(
                                    e.args.iter().cloned().map(CommandResponse::bulk).collect(),
                                ),
                                CommandResponse::bulk(Bytes::from(e.addr.clone())),
                                CommandResponse::bulk(Bytes::from(e.name.clone())),
                            ])
                        })
                        .collect(),
                )
            }
            "LEN" => CommandResponse::integer(self.slowlog.lock().len() as i64),
            "RESET" => {
                self.slowlog.lock().clear();
                CommandResponse::ok()
            }
            "HELP" => CommandResponse::array(
                [
                    "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "GET [<count>]",
                    "LEN",
                    "RESET",
                ]
                .iter()
                .map(|l| CommandResponse::simple(l))
                .collect(),
            ),
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try SLOWLOG HELP.",
                sub.to_lowercase()
            )),
        }
    }

    // ---- latency monitor ----

    pub fn maybe_latency(&self, event: &'static str, ms: u64) {
        let threshold = self.latency_threshold_ms.load(Ordering::Relaxed);
        if threshold == 0 || ms < threshold {
            return;
        }
        let now = now_ms() / 1000;
        let mut l = self.latency.lock();
        let e = l.entry(event).or_default();
        e.latest_time = now;
        e.latest_ms = ms;
        e.max_ms = e.max_ms.max(ms);
        if e.history.back().is_some_and(|(t, _)| *t == now) {
            if let Some(last) = e.history.back_mut() {
                last.1 = last.1.max(ms);
            }
        } else {
            e.history.push_back((now, ms));
            if e.history.len() > LATENCY_HISTORY {
                e.history.pop_front();
            }
        }
    }

    pub fn latency_command(&self, args: &[Bytes]) -> CommandResponse {
        let sub = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        match sub.as_str() {
            "LATEST" => {
                let l = self.latency.lock();
                CommandResponse::array(
                    l.iter()
                        .map(|(name, e)| {
                            CommandResponse::array(vec![
                                CommandResponse::bulk_string(name),
                                CommandResponse::integer(e.latest_time as i64),
                                CommandResponse::integer(e.latest_ms as i64),
                                CommandResponse::integer(e.max_ms as i64),
                            ])
                        })
                        .collect(),
                )
            }
            "HISTORY" => {
                let Some(ev) = args.get(1) else {
                    return CommandResponse::wrong_arity("latency|history");
                };
                let l = self.latency.lock();
                match l.get(String::from_utf8_lossy(ev).as_ref()) {
                    None => CommandResponse::array(vec![]),
                    Some(e) => CommandResponse::array(
                        e.history
                            .iter()
                            .map(|(t, ms)| {
                                CommandResponse::array(vec![
                                    CommandResponse::integer(*t as i64),
                                    CommandResponse::integer(*ms as i64),
                                ])
                            })
                            .collect(),
                    ),
                }
            }
            "RESET" => {
                let mut l = self.latency.lock();
                if args.len() == 1 {
                    let n = l.len();
                    l.clear();
                    return CommandResponse::integer(n as i64);
                }
                let mut n = 0;
                for ev in &args[1..] {
                    if l.remove(String::from_utf8_lossy(ev).as_ref()).is_some() {
                        n += 1;
                    }
                }
                CommandResponse::integer(n)
            }
            "DOCTOR" => {
                let l = self.latency.lock();
                let text = if self.latency_threshold_ms.load(Ordering::Relaxed) == 0 {
                    "I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this \
                     Redis instance. You may use \"CONFIG SET latency-monitor-threshold \
                     <milliseconds>.\" in order to enable it."
                        .to_string()
                } else if l.is_empty() {
                    "Dave, no latency spike was observed during the lifetime of this Redis \
                     instance, not in the slightest bit. I honestly think you ought to sleep \
                     tonight."
                        .to_string()
                } else {
                    let mut s = String::from(
                        "Dave, I have observed latency spikes in this Redis instance.\n\n",
                    );
                    for (i, (name, e)) in l.iter().enumerate() {
                        s.push_str(&format!(
                            "{}. {}: {} latency spikes (average {}ms, max {}ms).\n",
                            i + 1,
                            name,
                            e.history.len(),
                            e.history.iter().map(|x| x.1).sum::<u64>()
                                / e.history.len().max(1) as u64,
                            e.max_ms
                        ));
                    }
                    s
                };
                CommandResponse::bulk(Bytes::from(text))
            }
            "HELP" => CommandResponse::array(
                [
                    "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "DOCTOR",
                    "LATEST",
                    "HISTORY <event>",
                    "RESET [<event> ...]",
                ]
                .iter()
                .map(|l| CommandResponse::simple(l))
                .collect(),
            ),
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try LATENCY HELP.",
                sub.to_lowercase()
            )),
        }
    }

    // ---- MONITOR ----

    #[inline]
    pub fn monitoring(&self) -> bool {
        self.monitors.load(Ordering::Relaxed) > 0
    }

    /// A new `MONITOR` client's feed; it stops counting when dropped.
    pub fn subscribe_monitor(self: &Arc<Self>) -> MonitorFeed {
        self.monitors.fetch_add(1, Ordering::Relaxed);
        MonitorFeed {
            rx: self.monitor.subscribe(),
            stats: self.clone(),
        }
    }

    /// Feed `name args…`, run by `addr` in database `db`, to monitors.
    pub fn feed_monitor(&self, db: u32, addr: &str, name: &str, args: &[Bytes]) {
        if !self.monitoring() {
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let mut line = format!(
            "+{}.{:06} [{} {}]",
            now.as_secs(),
            now.subsec_micros(),
            db,
            addr
        );
        line.push(' ');
        repr(&mut line, name.to_lowercase().as_bytes());
        for a in args {
            line.push(' ');
            repr(&mut line, a);
        }
        line.push_str("\r\n");
        let _ = self.monitor.send(Bytes::from(line));
    }

    // ---- ops/sec, peak memory ----

    /// Take a throughput sample (called ~10 times a second).
    pub fn sample(&self, total_commands: u64, used_memory: u64) {
        let mut s = self.samples.lock();
        s.push_back((now_ms(), total_commands));
        if s.len() > 16 {
            s.pop_front();
        }
        self.peak_memory.fetch_max(used_memory, Ordering::Relaxed);
    }

    pub fn ops_per_sec(&self) -> u64 {
        let s = self.samples.lock();
        match (s.front(), s.back()) {
            (Some(a), Some(b)) if b.0 > a.0 => {
                (b.1.saturating_sub(a.1) as f64 * 1000.0 / (b.0 - a.0) as f64) as u64
            }
            _ => 0,
        }
    }

    pub fn peak_memory(&self) -> u64 {
        self.peak_memory.load(Ordering::Relaxed)
    }
}

/// Redis' `sdscatrepr`: a quoted, escaped rendering of `bytes`.
fn repr(out: &mut String, bytes: &[u8]) {
    out.push('"');
    for &b in bytes {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x07 => out.push_str("\\a"),
            0x08 => out.push_str("\\b"),
            b if b.is_ascii_graphic() || b == b' ' => out.push(b as char),
            b => out.push_str(&format!("\\x{:02x}", b)),
        }
    }
    out.push('"');
}

/// A `MONITOR` client's subscription.
pub struct MonitorFeed {
    pub rx: tokio::sync::broadcast::Receiver<Bytes>,
    stats: Arc<Stats>,
}

impl Drop for MonitorFeed {
    fn drop(&mut self) {
        self.stats.monitors.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_sum_across_threads_and_reset() {
        let s = Arc::new(Stats::default());
        s.record(Cmd::GET, 5, false);
        let s2 = s.clone();
        std::thread::spawn(move || s2.record(Cmd::GET, 7, true))
            .join()
            .unwrap();
        let text = s.commandstats();
        assert!(text.contains("cmdstat_get:calls=2,usec=12,"), "{text}");
        assert!(text.contains("failed_calls=1"), "{text}");
        s.reset();
        assert!(!s.commandstats().contains("cmdstat_get"));
    }

    #[test]
    fn slowlog_keeps_the_newest_and_truncates_arguments() {
        let s = Stats::default();
        s.slowlog_threshold_us.store(0, Ordering::Relaxed);
        s.slowlog_max_len.store(2, Ordering::Relaxed);
        let big = Bytes::from(vec![b'x'; 200]);
        for i in 0..3 {
            s.maybe_slowlog(i, "SET", &[Bytes::from("k"), big.clone()], "1.2.3.4:5", "");
        }
        match s.slowlog_command(&[Bytes::from("GET")]) {
            CommandResponse::Array(v) => {
                assert_eq!(v.len(), 2);
                let CommandResponse::Array(e) = &v[0] else {
                    panic!()
                };
                assert!(matches!(e[0], CommandResponse::Integer(2)));
                let CommandResponse::Array(args) = &e[3] else {
                    panic!()
                };
                let CommandResponse::BulkString(a) = &args[2] else {
                    panic!()
                };
                assert!(a.ends_with(b"... (72 more bytes)"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn monitor_lines_are_escaped_like_redis() {
        let mut out = String::new();
        repr(&mut out, b"a\"b\n\x01");
        assert_eq!(out, r#""a\"b\n\x01""#);
    }

    #[test]
    fn the_clock_measures_microseconds() {
        calibrate();
        let s = stamp();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let us = elapsed_us(s);
        assert!((4000..200_000).contains(&us), "{us}");
    }
}
