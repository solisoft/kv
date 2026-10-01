//! What reaches the append-only log, and when.
//!
//! # The rule
//!
//! **Every change to a shard's store is logged to that shard's log before its
//! lock is released, as an effect that replays correctly on that shard
//! alone.** `ShardHandle::execute` enforces the first half; this module
//! decides what the record is. (See `solikv_persist::aof` for why the log is
//! per shard.)
//!
//! Commands fall in three groups ([`mode`]):
//!
//! - **Auto** — one key, one shard, one `execute` call: the command itself is
//!   the record, logged by that `execute` once the closure returns a
//!   non-error. Forms that would not replay the same are rewritten first
//!   ([`aof_form`]): relative TTLs become absolute (`EXPIRE k 10` logged
//!   verbatim restarted the TTL at every restart), `SPOP` becomes the `SREM`
//!   of what it popped, `XADD *` gets the ID it generated.
//! - **Explicit** — commands that touch several shards or compute from other
//!   shards' data. Their arms use `ShardHandle::execute_with_effects` and push
//!   one record per shard: the subset of `MSET` pairs that shard took, its
//!   `DEL`s, or — for a result computed elsewhere, like `ZUNIONSTORE`'s — the
//!   value itself (`#PUT key <encoded entry>`, understood only by replay).
//!   A new multi-key or cross-shard write must be added here and do the same.
//! - **Idle** — everything else, reads included. Scripts are Idle too: each
//!   `redis.call` inside them goes through `execute` and logs itself.
//!
//! Independently of the command, every key a shard expires — lazily while
//! serving a command, or by the active cycle — is logged as a `DEL` at that
//! point. Replay runs with expiry off (`solikv_core::LoadingGuard`) and
//! relies on these to reproduce the exact state each later record ran
//! against.

use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::sync::Arc;

use solikv_core::{now_millis, CommandResponse, ShardStore};
use solikv_persist::aof::{encode_command, encode_into};
use solikv_persist::AofWriter;

use crate::shard::ShardManager;

/// Replay-only record: set a key to an encoded entry
/// (`solikv_persist::rdb::encode_entry`).
pub const PUT_RECORD: &str = "#PUT";

/// How a command reaches the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Auto,
    Explicit,
    Idle,
}

/// Commands whose arms log their own per-shard effects.
const EXPLICIT: &[&str] = &[
    "MSET",
    "MSETNX",
    "DEL",
    "UNLINK",
    "FLUSHDB",
    "FLUSHALL",
    "ZUNIONSTORE",
    "ZINTERSTORE",
    "SINTERSTORE",
    "SUNIONSTORE",
    "SDIFFSTORE",
    "SWAPDB",
    "GEOSEARCHSTORE",
    "PFMERGE",
    "BITOP",
    "XREADGROUP",
    "LMOVE",
    "RPOPLPUSH",
    "ZDIFFSTORE",
    "ZRANGESTORE",
    "XCLAIM",
    "XAUTOCLAIM",
    "COPY",
    "RESTORE",
    "RESTORE-ASKING",
    "SORT",
    "HGETDEL",
    "HGETEX",
    "HSETEX",
];

/// Write commands that are never records themselves: each runs the plain
/// commands that are its effect (`BLPOP` → `LPOP`, `LMPOP` → `LPOP key n`)
/// through `execute`, and those log.
const DELEGATING: &[&str] = &[
    "BLPOP",
    "BRPOP",
    "BLMOVE",
    "BRPOPLPUSH",
    "BLMPOP",
    "BZPOPMIN",
    "BZPOPMAX",
    "BZMPOP",
    "LMPOP",
    "ZMPOP",
    "GEORADIUS",
    "GEORADIUSBYMEMBER",
];

/// How command `id` reaches the log: a table lookup, built once from the
/// lists above, rather than walking them on every write.
pub(crate) fn mode(id: crate::commands::Cmd, is_write: bool) -> Mode {
    use std::sync::OnceLock;
    static MODES: OnceLock<Vec<Mode>> = OnceLock::new();
    if !is_write {
        return Mode::Idle;
    }
    let modes = MODES.get_or_init(|| {
        crate::commands::all()
            .iter()
            .map(|spec| mode_by_name(spec.name))
            .collect()
    });
    modes.get(id as usize).copied().unwrap_or(Mode::Idle)
}

fn mode_by_name(name: &str) -> Mode {
    if matches!(name, "EVAL" | "EVALSHA" | "FCALL") || DELEGATING.contains(&name) {
        Mode::Idle
    } else if EXPLICIT.contains(&name) {
        Mode::Explicit
    } else {
        Mode::Auto
    }
}

/// The command `execute` is running, for Auto logging.
pub(crate) struct AutoCmd<'a> {
    name: &'a str,
    args: &'a [Bytes],
    logged: Cell<bool>,
    /// The record, encoded before the shard lock is taken when it does not
    /// depend on the outcome — the lock is what a hot key's clients queue
    /// on, so the less done inside it the better.
    pre: Option<Vec<u8>>,
    /// `pre` came from [`prepare`]: it goes back to that pool.
    prepared: bool,
}

impl<'a> AutoCmd<'a> {
    pub(crate) fn new(name: &'a str, args: &'a [Bytes], mode: Mode) -> Self {
        let mut prepared = false;
        let pre = (mode == Mode::Auto && logged_verbatim(name, args)).then(|| {
            if let Some(buf) = take_prepared(args) {
                prepared = true;
                return buf;
            }
            let mut buf = PRE_POOL.with(|p| p.take());
            buf.clear();
            Record::Verbatim(name, args).write(&mut buf);
            buf
        });
        Self {
            name,
            args,
            logged: Cell::new(false),
            pre,
            prepared,
        }
    }
}

impl Drop for AutoCmd<'_> {
    fn drop(&mut self) {
        // Keep the buffer for the next command on this thread, unless a big
        // value made it big. (Never freed here: this runs under the lock.)
        if let Some(buf) = self.pre.take() {
            if buf.capacity() <= 64 * 1024 {
                if self.prepared {
                    SPARE.with(|s| s.borrow_mut().push(buf));
                } else {
                    let old = PRE_POOL.with(|p| p.replace(buf));
                    drop(old);
                }
            }
        }
    }
}

thread_local! {
    /// Records [`prepare`]d for the batch this thread is about to run, keyed
    /// by the address of each command's argument slice, in run order.
    static PREPARED: RefCell<std::collections::VecDeque<(usize, Vec<u8>)>> =
        const { RefCell::new(std::collections::VecDeque::new()) };
    /// Buffers of prepared records already logged, for the next batch.
    static SPARE: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
}

/// Encode, before a batch hold is taken, the record command `name args`
/// will log as — when it logs as it came — for [`AutoCmd::new`] to find
/// ready inside the hold: under a hot key, every client of the shard waits
/// while the holder encodes.
///
/// Records are matched to their command by the address of its argument
/// slice, which the batch keeps alive (and unmoved) until
/// [`clear_prepared`]; a command that never reaches the log (refused by
/// ACL, say) leaves its record to be skipped.
pub(crate) fn prepare(name: &str, args: &[Bytes], mode: Mode) {
    if args.is_empty() || mode != Mode::Auto || !logged_verbatim(name, args) {
        return;
    }
    let mut buf = SPARE.with(|s| s.borrow_mut().pop()).unwrap_or_default();
    buf.clear();
    Record::Verbatim(name, args).write(&mut buf);
    PREPARED.with(|p| p.borrow_mut().push_back((args.as_ptr() as usize, buf)));
}

fn take_prepared(args: &[Bytes]) -> Option<Vec<u8>> {
    if args.is_empty() {
        return None;
    }
    let key = args.as_ptr() as usize;
    PREPARED.with(|p| {
        let mut p = p.borrow_mut();
        let i = p.iter().position(|(k, _)| *k == key)?;
        // Entries before it belong to commands that logged nothing.
        for (_, buf) in p.drain(..i) {
            SPARE.with(|s| s.borrow_mut().push(buf));
        }
        p.pop_front().map(|(_, buf)| buf)
    })
}

/// Forget records [`prepare`]d for a batch that has ended.
pub(crate) fn clear_prepared() {
    PREPARED.with(|p| {
        let mut p = p.borrow_mut();
        if !p.is_empty() {
            for (_, buf) in p.drain(..) {
                SPARE.with(|s| s.borrow_mut().push(buf));
            }
        }
    });
    SPARE.with(|s| {
        let mut s = s.borrow_mut();
        // A batch's worth is plenty; a big one's buffers are not kept.
        s.truncate(crate::dispatch::BATCH_MAX);
    });
}

/// Whether [`aof_form`] logs this command as it came.
fn logged_verbatim(name: &str, args: &[Bytes]) -> bool {
    !matches!(
        name,
        "SETEX"
            | "PSETEX"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "PERSIST"
            | "GETEX"
            | "SPOP"
            | "XADD"
            | "HEXPIRE"
            | "HPEXPIRE"
            | "HEXPIREAT"
            | "HPEXPIREAT"
            | "HPERSIST"
    ) && !(name == "SET" && args.len() > 2)
}

#[derive(Clone, Copy)]
enum Frame {
    Idle,
    Explicit,
    /// Points at an `AutoCmd` on the stack of the `execute` call that set it;
    /// the guard restoring the previous frame is dropped before it.
    Auto(*const AutoCmd<'static>),
}

thread_local! {
    static FRAME: Cell<Frame> = const { Cell::new(Frame::Idle) };
    /// A spare record buffer, reused across commands on this thread.
    static PRE_POOL: Cell<Vec<u8>> = const { Cell::new(Vec::new()) };
    /// Shards that got a `MULTI` marker in the transaction this thread is
    /// running, if any.
    static TX: RefCell<Option<Vec<u32>>> = const { RefCell::new(None) };
}

/// Sets the logging frame for one command; restores the outer one on drop
/// (scripts nest commands).
pub(crate) struct FrameGuard {
    prev: Frame,
}

impl FrameGuard {
    pub(crate) fn enter(mode: Mode, cmd: &AutoCmd<'_>) -> Self {
        let frame = match mode {
            Mode::Idle => Frame::Idle,
            Mode::Explicit => Frame::Explicit,
            // Lifetime erased: the pointer is only read while this guard is
            // alive, and the guard is declared after `cmd`, so it is dropped
            // first.
            Mode::Auto => Frame::Auto((cmd as *const AutoCmd<'_>).cast()),
        };
        Self {
            prev: FRAME.with(|f| f.replace(frame)),
        }
    }
}

impl Drop for FrameGuard {
    fn drop(&mut self) {
        let prev = self.prev;
        FRAME.with(|f| f.set(prev));
    }
}

/// Records an explicit-mode arm produces for one shard.
pub struct Effects {
    enabled: bool,
    records: Vec<Vec<u8>>,
}

impl Effects {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            enabled,
            records: Vec::new(),
        }
    }

    /// Whether anything is logged (skip building records when not).
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Log a command that replays this shard's part of the effect.
    pub fn push(&mut self, name: &str, args: &[&[u8]]) {
        if self.enabled {
            self.records.push(encode_command(name.as_bytes(), args));
        }
    }

    /// Log a record encoded beforehand (outside the shard lock).
    pub fn push_encoded(&mut self, record: Vec<u8>) {
        if self.enabled {
            self.records.push(record);
        }
    }

    /// Log `key`'s current state as a value: `#PUT` of the entry, or `DEL`
    /// when the command left no key.
    pub fn put_key(&mut self, store: &ShardStore, key: &[u8]) {
        if !self.enabled {
            return;
        }
        let rec = match store.peek(key) {
            Some(entry) => {
                let payload = solikv_persist::rdb::encode_entry(entry);
                encode_command(PUT_RECORD.as_bytes(), &[key, &payload])
            }
            None => encode_command(b"DEL", &[key]),
        };
        self.records.push(rec);
    }
}

/// The replication stream as one shard command sees it.
#[derive(Clone, Copy)]
pub(crate) struct ReplSink<'a> {
    pub feed: &'a crate::replication::ReplFeed,
    /// The shard is in this thread's multi-shard hold.
    pub held: bool,
}

/// Called by `ShardHandle` with the shard lock held, after a command ran in
/// database `db`, when the log is on or replicas are fed. Expired keys carry
/// the database each was in.
#[allow(clippy::too_many_arguments)]
pub(crate) fn log_under_lock(
    w: Option<&AofWriter>,
    repl: Option<ReplSink<'_>>,
    shard: u32,
    seq: &mut u64,
    db: u32,
    resp: &CommandResponse,
    lazy_expired: &[(u32, Bytes)],
    expired_fields: &[(u32, Bytes, Vec<Bytes>)],
    explicit: Option<&mut Effects>,
    active_expired: &[(u32, Bytes)],
) {
    let mut out = Out {
        w,
        repl,
        shard,
        seq,
    };
    for (d, k) in lazy_expired {
        out.emit(*d, |buf| encode_into(buf, b"DEL", &[k]));
    }
    // Hash fields that expired (on access or in the active cycle): before
    // the command that found them, as `HDEL`s.
    for (d, k, fields) in expired_fields {
        out.emit(*d, |buf| {
            let mut args: Vec<&[u8]> = Vec::with_capacity(1 + fields.len());
            args.push(k);
            args.extend(fields.iter().map(|f| f.as_ref()));
            encode_into(buf, b"HDEL", &args)
        });
    }
    match explicit {
        Some(fx) => {
            for rec in fx.records.drain(..) {
                out.emit(db, |buf| buf.extend_from_slice(&rec));
            }
        }
        None => {
            if let Frame::Auto(ptr) = FRAME.with(|f| f.get()) {
                // SAFETY: see `Frame::Auto` — the command outlives its frame.
                let cmd = unsafe { &*ptr };
                if !cmd.logged.get() && !resp.is_error() {
                    cmd.logged.set(true);
                    if let Some(pre) = &cmd.pre {
                        out.emit(db, |buf| buf.extend_from_slice(pre));
                    } else if let Some(rec) = aof_form(cmd.name, cmd.args, resp) {
                        out.emit(db, |buf| rec.write(buf));
                    }
                }
            }
        }
    }
    for (d, k) in active_expired {
        out.emit(*d, |buf| encode_into(buf, b"DEL", &[k]));
    }
}

/// Where one shard's records go: its log, the replication stream, or both.
struct Out<'a> {
    w: Option<&'a AofWriter>,
    repl: Option<ReplSink<'a>>,
    shard: u32,
    /// The shard's record counter (`ShardStore::log_seq`).
    seq: &'a mut u64,
}

thread_local! {
    /// A record encoded once for both destinations.
    static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

impl Out<'_> {
    fn emit(&mut self, db: u32, write: impl FnOnce(&mut Vec<u8>)) {
        match (self.w, self.repl) {
            (Some(w), None) => emit_aof(w, self.shard, self.seq, db, write),
            (w, Some(r)) => SCRATCH.with(|s| {
                let mut buf = s.borrow_mut();
                buf.clear();
                write(&mut buf);
                if let Some(w) = w {
                    emit_aof(w, self.shard, self.seq, db, |b| b.extend_from_slice(&buf));
                }
                r.feed.record(self.shard, db, &buf, r.held);
                if buf.capacity() > 64 * 1024 {
                    *buf = Vec::new();
                }
            }),
            (None, None) => {}
        }
    }
}

fn emit_aof(w: &AofWriter, shard: u32, seq: &mut u64, db: u32, write: impl FnOnce(&mut Vec<u8>)) {
    TX.with(|tx| {
        if let Some(opened) = tx.borrow_mut().as_mut() {
            if !opened.contains(&shard) {
                opened.push(shard);
                w.append(shard, seq, |buf| encode_into(buf, b"MULTI", &[]));
            }
        }
    });
    w.append_db(shard, seq, db, write);
}

/// Brackets a transaction's records in each shard log with `MULTI`/`EXEC`,
/// so replay applies all of a shard's part or none of it.
pub struct TxGuard {
    shards: Option<Arc<ShardManager>>,
}

impl TxGuard {
    pub(crate) fn begin(shards: &Arc<ShardManager>) -> Self {
        // Nested (a script inside EXEC): the outer bracket covers it.
        if shards.aof_on().is_none() || TX.with(|tx| tx.borrow().is_some()) {
            return Self { shards: None };
        }
        TX.with(|tx| *tx.borrow_mut() = Some(Vec::new()));
        Self {
            shards: Some(shards.clone()),
        }
    }
}

impl Drop for TxGuard {
    fn drop(&mut self) {
        let Some(shards) = self.shards.take() else {
            return;
        };
        let opened = TX.with(|tx| tx.borrow_mut().take()).unwrap_or_default();
        for s in opened {
            shards
                .shard(s as usize)
                .log_marker(&encode_command(b"EXEC", &[]));
        }
    }
}

fn upper(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

fn parse_u64(b: &[u8]) -> Option<u64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

fn parse_i64(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// Absolute expiry in ms for a relative amount, as the command computed it.
fn abs_ms(amount: &[u8], seconds: bool) -> Option<Vec<u8>> {
    let n = parse_i64(amount)?;
    let ms = if seconds { n.checked_mul(1000)? } else { n };
    let at = (now_millis() as i64).checked_add(ms)?;
    Some(at.max(0).to_string().into_bytes())
}

fn owned(args: &[Bytes]) -> Vec<Vec<u8>> {
    args.iter().map(|a| a.to_vec()).collect()
}

/// A record to log: the command as it came (no copy until it is encoded into
/// the shard's buffer), or a rewritten form.
pub(crate) enum Record<'a> {
    Verbatim(&'a str, &'a [Bytes]),
    Rewritten(&'static str, Vec<Vec<u8>>),
}

impl Record<'_> {
    pub(crate) fn write(&self, buf: &mut Vec<u8>) {
        match self {
            Record::Verbatim(name, args) => {
                let refs: smallvec_like::Refs = args.iter().map(|a| a.as_ref()).collect();
                encode_into(buf, name.as_bytes(), refs.as_slice());
            }
            Record::Rewritten(name, args) => {
                let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
                encode_into(buf, name.as_bytes(), &refs);
            }
        }
    }

    #[cfg(test)]
    fn bytes(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.write(&mut v);
        v
    }
}

/// Argument slices without a heap allocation for the common short command.
mod smallvec_like {
    pub(crate) enum Refs<'a> {
        Inline([&'a [u8]; 8], usize),
        Heap(Vec<&'a [u8]>),
    }

    impl<'a> FromIterator<&'a [u8]> for Refs<'a> {
        fn from_iter<I: IntoIterator<Item = &'a [u8]>>(iter: I) -> Self {
            let mut inline: [&[u8]; 8] = [&[]; 8];
            let mut n = 0;
            let mut iter = iter.into_iter();
            for item in iter.by_ref() {
                if n == inline.len() {
                    let mut v: Vec<&[u8]> = inline.to_vec();
                    v.push(item);
                    v.extend(iter);
                    return Refs::Heap(v);
                }
                inline[n] = item;
                n += 1;
            }
            Refs::Inline(inline, n)
        }
    }

    impl<'a> Refs<'a> {
        pub(crate) fn as_slice(&self) -> &[&'a [u8]] {
            match self {
                Refs::Inline(a, n) => &a[..*n],
                Refs::Heap(v) => v,
            }
        }
    }
}

fn enc(name: &'static str, args: &[Vec<u8>]) -> Record<'static> {
    Record::Rewritten(name, args.to_vec())
}

/// The record for an Auto-mode command that succeeded, or `None` when it
/// changed nothing worth logging.
pub(crate) fn aof_form<'a>(
    name: &'a str,
    args: &'a [Bytes],
    resp: &CommandResponse,
) -> Option<Record<'a>> {
    let is_one = matches!(resp, CommandResponse::Integer(1));
    match name {
        "SET" if args.len() > 2 => {
            let mut out: Vec<Vec<u8>> = owned(&args[..2]);
            let mut i = 2;
            while i < args.len() {
                let opt = upper(&args[i]);
                if (opt == "EX" || opt == "PX") && i + 1 < args.len() {
                    out.push(b"PXAT".to_vec());
                    out.push(abs_ms(&args[i + 1], opt == "EX")?);
                    i += 2;
                } else {
                    out.push(args[i].to_vec());
                    i += 1;
                }
            }
            Some(enc("SET", &out))
        }
        "SETEX" | "PSETEX" if args.len() == 3 => {
            let at = abs_ms(&args[1], name == "SETEX")?;
            Some(enc(
                "SET",
                &[args[0].to_vec(), args[2].to_vec(), b"PXAT".to_vec(), at],
            ))
        }
        "EXPIRE" | "PEXPIRE" if args.len() >= 2 => {
            if !is_one {
                return None;
            }
            let at = abs_ms(&args[1], name == "EXPIRE")?;
            Some(expire_record(args[0].to_vec(), at))
        }
        "EXPIREAT" | "PEXPIREAT" if args.len() >= 2 => {
            if !is_one {
                return None;
            }
            let at = if name == "EXPIREAT" {
                parse_i64(&args[1])?.checked_mul(1000)?.max(0)
            } else {
                parse_i64(&args[1])?.max(0)
            };
            Some(expire_record(args[0].to_vec(), at.to_string().into_bytes()))
        }
        "PERSIST" if !is_one => None,
        "GETEX" => {
            if matches!(resp, CommandResponse::Nil) || args.len() < 2 {
                return None;
            }
            let key = args[0].to_vec();
            let opt = upper(&args[1]);
            match opt.as_str() {
                "EX" | "PX" => {
                    let at = abs_ms(args.get(2)?, opt == "EX")?;
                    Some(expire_record(key, at))
                }
                "EXAT" => {
                    let at = parse_u64(args.get(2)?)?.checked_mul(1000)?;
                    Some(expire_record(key, at.to_string().into_bytes()))
                }
                "PXAT" => Some(expire_record(key, args.get(2)?.to_vec())),
                "PERSIST" => Some(enc("PERSIST", &[key])),
                _ => None,
            }
        }
        "SPOP" => {
            let mut out = vec![args.first()?.to_vec()];
            match resp {
                CommandResponse::BulkString(m) => out.push(m.to_vec()),
                CommandResponse::Array(items) => {
                    for it in items {
                        if let CommandResponse::BulkString(m) = it {
                            out.push(m.to_vec());
                        }
                    }
                }
                _ => {}
            }
            (out.len() > 1).then(|| enc("SREM", &out))
        }
        "XADD" => {
            let CommandResponse::BulkString(id) = resp else {
                return None;
            };
            let mut out = owned(args);
            let idx = xadd_id_index(args)?;
            out[idx] = id.to_vec();
            Some(enc("XADD", &out))
        }
        "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT" => {
            // Per-field codes: 1 set, 2 deleted (deadline already past); the
            // others changed nothing. All fields share one deadline, so a
            // command sets or deletes, never both.
            let CommandResponse::Array(codes) = resp else {
                return None;
            };
            let fields = &args[args.len() - codes.len()..];
            let pick = |want: i64| -> Vec<Vec<u8>> {
                fields
                    .iter()
                    .zip(codes)
                    .filter(|(_, c)| matches!(c, CommandResponse::Integer(n) if *n == want))
                    .map(|(f, _)| f.to_vec())
                    .collect()
            };
            let deleted = pick(2);
            if !deleted.is_empty() {
                let mut out = vec![args[0].to_vec()];
                out.extend(deleted);
                return Some(enc("HDEL", &out));
            }
            let set = pick(1);
            if set.is_empty() {
                return None;
            }
            let at = match name {
                "HEXPIRE" | "HPEXPIRE" => abs_ms(&args[1], name == "HEXPIRE")?,
                "HEXPIREAT" => parse_i64(&args[1])?
                    .checked_mul(1000)?
                    .max(0)
                    .to_string()
                    .into_bytes(),
                _ => args[1].to_vec(),
            };
            let mut out = vec![
                args[0].to_vec(),
                at,
                b"FIELDS".to_vec(),
                set.len().to_string().into_bytes(),
            ];
            out.extend(set);
            Some(enc("HPEXPIREAT", &out))
        }
        "HPERSIST" => {
            let CommandResponse::Array(codes) = resp else {
                return None;
            };
            let fields = &args[args.len() - codes.len()..];
            let done: Vec<Vec<u8>> = fields
                .iter()
                .zip(codes)
                .filter(|(_, c)| matches!(c, CommandResponse::Integer(1)))
                .map(|(f, _)| f.to_vec())
                .collect();
            if done.is_empty() {
                return None;
            }
            let mut out = vec![
                args[0].to_vec(),
                b"FIELDS".to_vec(),
                done.len().to_string().into_bytes(),
            ];
            out.extend(done);
            Some(enc("HPERSIST", &out))
        }
        _ => Some(Record::Verbatim(name, args)),
    }
}

/// `PEXPIREAT key at`, or `DEL key` when `at` has passed: an expiry set in
/// the past deletes the key there and then (Redis does the same).
fn expire_record(key: Vec<u8>, at: Vec<u8>) -> Record<'static> {
    let past = std::str::from_utf8(&at)
        .ok()
        .and_then(|a| a.parse::<u64>().ok())
        .is_some_and(|a| a <= now_millis());
    if past {
        enc("DEL", &[key])
    } else {
        enc("PEXPIREAT", &[key, at])
    }
}

/// Position of the entry ID in `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~]
/// threshold [LIMIT n]] id field value ...`.
fn xadd_id_index(args: &[Bytes]) -> Option<usize> {
    let mut i = 1;
    loop {
        let tok = upper(args.get(i)?);
        match tok.as_str() {
            "NOMKSTREAM" => i += 1,
            "MAXLEN" | "MINID" => {
                i += 1;
                if matches!(args.get(i).map(|a| a.as_ref()), Some(b"~") | Some(b"=")) {
                    i += 1;
                }
                i += 1;
            }
            "LIMIT" => i += 2,
            _ => return Some(i),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn form(name: &str, args: &[Bytes], resp: &CommandResponse) -> Option<Vec<u8>> {
        aof_form(name, args, resp).map(|r| r.bytes())
    }

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn decoded(rec: &[u8]) -> Vec<String> {
        // Tiny RESP array reader for assertions.
        let text = String::from_utf8_lossy(rec).to_string();
        let parts: Vec<&str> = text.split("\r\n").collect();
        let mut out = Vec::new();
        let mut i = 1;
        while i + 1 < parts.len() {
            out.push(parts[i + 1].to_string());
            i += 2;
        }
        out
    }

    #[test]
    fn relative_ttls_are_logged_absolute() {
        let now = now_millis();
        let rec = form(
            "SET",
            &[b("k"), b("v"), b("EX"), b("10"), b("NX")],
            &CommandResponse::ok(),
        )
        .unwrap();
        let d = decoded(&rec);
        assert_eq!(&d[..4], &["SET", "k", "v", "PXAT"]);
        let at: u64 = d[4].parse().unwrap();
        assert!(at >= now + 9_000 && at <= now + 11_000, "{at} vs {now}");
        assert_eq!(d[5], "NX");

        let rec = form("EXPIRE", &[b("k"), b("5")], &CommandResponse::Integer(1)).unwrap();
        assert_eq!(decoded(&rec)[0], "PEXPIREAT");
        assert!(form("EXPIRE", &[b("k"), b("5")], &CommandResponse::Integer(0)).is_none());

        let rec = form("SETEX", &[b("k"), b("5"), b("v")], &CommandResponse::ok()).unwrap();
        assert_eq!(&decoded(&rec)[..4], &["SET", "k", "v", "PXAT"]);
    }

    #[test]
    fn spop_and_xadd_log_what_happened() {
        let rec = form(
            "SPOP",
            &[b("s"), b("2")],
            &CommandResponse::Array(vec![
                CommandResponse::BulkString(b("a")),
                CommandResponse::BulkString(b("b")),
            ]),
        )
        .unwrap();
        assert_eq!(decoded(&rec), vec!["SREM", "s", "a", "b"]);
        assert!(form("SPOP", &[b("s")], &CommandResponse::Nil).is_none());

        let rec = form(
            "XADD",
            &[
                b("st"),
                b("MAXLEN"),
                b("~"),
                b("10"),
                b("*"),
                b("f"),
                b("v"),
            ],
            &CommandResponse::BulkString(b("5-0")),
        )
        .unwrap();
        assert_eq!(
            decoded(&rec),
            vec!["XADD", "st", "MAXLEN", "~", "10", "5-0", "f", "v"]
        );
    }
}
