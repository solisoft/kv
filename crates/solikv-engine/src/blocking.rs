//! Blocking commands: `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BLMPOP`,
//! `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, and `XREAD`/`XREADGROUP` with `BLOCK`.
//!
//! # How a client blocks
//!
//! The connection asks [`CommandEngine::block_start`]: the command is tried
//! once, and if nothing can be served the client is registered as a
//! [`Waiter`] under every key it names, in the order it blocked. The
//! connection then awaits [`Blocked::wait`] — a result, its deadline, or the
//! client going away.
//!
//! # How it is woken
//!
//! Every write that succeeds while someone is blocked notes the keys it
//! touched (thread-local, so a transaction or a script collects them all
//! first). Once the outermost command is done — after `EXEC`, after the
//! script, after the lone command — the writing thread serves: for each key,
//! the waiters are tried **in the order they blocked**, each by running the
//! plain command that is its effect (`LPOP`, `LMOVE`, `ZPOPMIN`, `XREAD`, …)
//! in its database, and a waiter that got something is handed the reply and
//! unregistered. That is Redis' `handleClientsBlockedOnKeys`: blocked
//! clients are served in FIFO order, after the command that made the data
//! available and never in the middle of a transaction.
//!
//! Serving runs the effect through the engine like any command, so the
//! append-only log gets `LPOP key` (or `LMOVE …`, …), never the blocking
//! command, and keyspace events and `WATCH` see the pop. A pop that pushes
//! (`BLMOVE`) notes its destination in turn, so a chain of blocked movers is
//! served in the same pass.
//!
//! The registry's lock is taken before any shard lock (serving, trying at
//! block time) and never while holding one, so it cannot deadlock with
//! commands. With nobody blocked the only cost to a write is one atomic load.

use bytes::Bytes;
use parking_lot::Mutex;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use solikv_core::types::StreamId;
use solikv_core::CommandResponse;

/// What a blocked client waits to do once a key has data.
#[derive(Debug, Clone)]
pub enum BlockOp {
    /// `BLPOP`/`BRPOP`: pop one element from the first key with data.
    ListPop { left: bool },
    /// `BLMOVE`/`BRPOPLPUSH`: move one element from `keys[0]` to `dst`.
    ListMove {
        dst: Bytes,
        from_left: bool,
        to_left: bool,
    },
    /// `BLMPOP`: pop up to `count` elements from the first key with data.
    ListMPop { left: bool, count: usize },
    /// `BZPOPMIN`/`BZPOPMAX`.
    ZPop { max: bool },
    /// `BZMPOP`.
    ZMPop { max: bool, count: usize },
    /// `XREAD BLOCK`: entries after `ids[i]` in `keys[i]`.
    XRead {
        count: Option<usize>,
        ids: Vec<StreamId>,
    },
    /// `XREADGROUP … BLOCK … STREAMS keys > …`.
    XReadGroup {
        group: Bytes,
        consumer: Bytes,
        count: Option<usize>,
        noack: bool,
    },
}

impl BlockOp {
    /// Whether serving it takes the data away (a pop), so that when the
    /// first waiter gets nothing the ones behind it cannot either.
    fn consumes(&self) -> bool {
        !matches!(self, BlockOp::XRead { .. })
    }
}

/// A client blocked on some keys.
pub struct Waiter {
    pub client_id: u64,
    pub db: u32,
    pub op: BlockOp,
    /// The keys it waits on, in the order it named them.
    pub keys: Vec<Bytes>,
    /// Reply once the deadline passes (or `CLIENT UNBLOCK … TIMEOUT`).
    pub timeout_reply: CommandResponse,
    result: Mutex<Option<CommandResponse>>,
    notify: tokio::sync::Notify,
}

impl Waiter {
    pub(crate) fn new(
        client_id: u64,
        db: u32,
        op: BlockOp,
        keys: Vec<Bytes>,
        timeout_reply: CommandResponse,
    ) -> Self {
        Self {
            client_id,
            db,
            op,
            keys,
            timeout_reply,
            result: Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn deliver(&self, resp: CommandResponse) {
        *self.result.lock() = Some(resp);
        self.notify.notify_one();
    }

    fn take_result(&self) -> Option<CommandResponse> {
        self.result.lock().take()
    }
}

#[derive(Default)]
pub(crate) struct Inner {
    by_key: HashMap<Bytes, VecDeque<Arc<Waiter>>, solikv_core::FastHasher>,
    by_client: HashMap<u64, Arc<Waiter>>,
}

impl Inner {
    fn remove(&mut self, w: &Arc<Waiter>) -> bool {
        let present = self
            .by_client
            .get(&w.client_id)
            .is_some_and(|x| Arc::ptr_eq(x, w));
        if !present {
            return false;
        }
        self.by_client.remove(&w.client_id);
        for k in &w.keys {
            if let Some(q) = self.by_key.get_mut(k) {
                q.retain(|x| !Arc::ptr_eq(x, w));
                if q.is_empty() {
                    self.by_key.remove(k);
                }
            }
        }
        true
    }
}

/// Blocked clients, by key and by client id.
#[derive(Default)]
pub struct BlockingRegistry {
    /// Clients blocked or about to (see `CommandEngine::block_start`): zero
    /// means writes skip everything here.
    active: AtomicUsize,
    inner: Mutex<Inner>,
}

impl BlockingRegistry {
    /// Whether any client is blocked (or blocking).
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst) != 0
    }

    /// Clients blocked right now (`INFO clients`).
    pub fn blocked_clients(&self) -> usize {
        self.inner.lock().by_client.len()
    }

    /// Keys with at least one blocked client (`INFO keyspace`-adjacent
    /// `blocking_keys`).
    pub fn blocking_keys(&self) -> usize {
        self.inner.lock().by_key.len()
    }

    pub(crate) fn begin(&self) {
        self.active.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn end(&self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<'_, Inner> {
        self.inner.lock()
    }

    /// `CLIENT UNBLOCK id [TIMEOUT|ERROR]`: whether `id` was blocked.
    pub fn unblock(&self, client_id: u64, error: bool) -> bool {
        let mut inner = self.inner.lock();
        let Some(w) = inner.by_client.get(&client_id).cloned() else {
            return false;
        };
        inner.remove(&w);
        self.end();
        drop(inner);
        let reply = if error {
            CommandResponse::error("UNBLOCKED client unblocked via CLIENT UNBLOCK")
        } else {
            w.timeout_reply.clone()
        };
        w.deliver(reply);
        true
    }
}

// ---------------------------------------------------------------------------
// Keys written, pending service (per thread)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct Pending {
    /// A write whose keys are unknown (`FLUSHDB`, `SWAPDB`, a script …):
    /// every key with a waiter may have changed.
    all: bool,
    keys: Vec<Bytes>,
}

thread_local! {
    static PENDING: RefCell<Pending> = RefCell::new(Pending::default());
    /// Nonzero while this thread is inside `EXEC` or serving/trying blocked
    /// clients: writes then only note their keys, and whoever set it serves
    /// at the end.
    static DEFER: Cell<u32> = const { Cell::new(0) };
}

/// Note that a write touched `keys` (`None`: unknown keys).
pub(crate) fn note(keys: Option<Vec<Bytes>>) {
    PENDING.with(|p| {
        let mut p = p.borrow_mut();
        match keys {
            Some(k) if !k.is_empty() => p.keys.extend(k),
            _ => p.all = true,
        }
    });
}

pub(crate) fn take_pending() -> Option<Pending> {
    PENDING.with(|p| {
        let mut p = p.borrow_mut();
        if !p.all && p.keys.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut *p))
        }
    })
}

/// Whether serving must wait for an enclosing `EXEC` or serve pass.
pub(crate) fn deferred() -> bool {
    DEFER.with(|d| d.get() != 0)
}

/// Defers serving while alive (see [`DEFER`]).
pub(crate) struct DeferGuard;

impl DeferGuard {
    pub(crate) fn enter() -> Self {
        DEFER.with(|d| d.set(d.get() + 1));
        DeferGuard
    }
}

impl Drop for DeferGuard {
    fn drop(&mut self) {
        DEFER.with(|d| d.set(d.get() - 1));
    }
}

impl Pending {
    pub(crate) fn merge(&mut self, other: Pending) {
        self.all |= other.all;
        self.keys.extend(other.keys);
    }

    pub(crate) fn into_option(self) -> Option<Pending> {
        (self.all || !self.keys.is_empty()).then_some(self)
    }

    /// The keys to try, deduplicated, in the order they were written.
    pub(crate) fn keys_in(self, inner: &Inner) -> Vec<Bytes> {
        let mut out: Vec<Bytes> = Vec::new();
        if self.all {
            out.extend(inner.by_key.keys().cloned());
        } else {
            for k in self.keys {
                if inner.by_key.contains_key(&k) && !out.contains(&k) {
                    out.push(k);
                }
            }
        }
        out
    }
}

/// The waiters on `key`, in the order they blocked.
pub(crate) fn waiters_on(inner: &Inner, key: &Bytes) -> Vec<Arc<Waiter>> {
    inner
        .by_key
        .get(key)
        .map(|q| q.iter().cloned().collect())
        .unwrap_or_default()
}

/// Hand `resp` to `w` and forget it.
pub(crate) fn serve(
    reg: &BlockingRegistry,
    inner: &mut Inner,
    w: &Arc<Waiter>,
    resp: CommandResponse,
) {
    if inner.remove(w) {
        reg.end();
        w.deliver(resp);
    }
}

pub(crate) fn is_consuming(w: &Waiter) -> bool {
    w.op.consumes()
}

// ---------------------------------------------------------------------------
// The client side
// ---------------------------------------------------------------------------

/// Outcome of trying a blocking command.
pub enum BlockStart {
    /// Served (or refused) at once.
    Done(CommandResponse),
    /// Registered: await [`Blocked::wait`].
    Wait(Blocked),
}

/// A registered blocked client. Dropping it (the connection went away)
/// unregisters it.
pub struct Blocked {
    reg: Arc<BlockingRegistry>,
    waiter: Arc<Waiter>,
    deadline: Option<tokio::time::Instant>,
}

impl Blocked {
    pub(crate) fn register(
        reg: &Arc<BlockingRegistry>,
        inner: &mut Inner,
        waiter: Waiter,
        timeout: Option<Duration>,
    ) -> Self {
        let waiter = Arc::new(waiter);
        // A client blocks once at a time; a stale entry (should not exist)
        // is replaced.
        if let Some(old) = inner.by_client.get(&waiter.client_id).cloned() {
            if inner.remove(&old) {
                reg.end();
            }
        }
        inner.by_client.insert(waiter.client_id, waiter.clone());
        let mut seen: Vec<&Bytes> = Vec::new();
        for k in &waiter.keys {
            if seen.contains(&k) {
                continue;
            }
            seen.push(k);
            inner
                .by_key
                .entry(k.clone())
                .or_default()
                .push_back(waiter.clone());
        }
        Blocked {
            reg: reg.clone(),
            waiter,
            deadline: timeout.map(|t| tokio::time::Instant::now() + t),
        }
    }

    /// The reply: served data, the timeout reply, or `CLIENT UNBLOCK`'s.
    /// Cancel-safe: dropping the future loses nothing, and calling it again
    /// picks up a result delivered meanwhile.
    pub async fn wait(&self) -> CommandResponse {
        loop {
            let notified = self.waiter.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(r) = self.waiter.take_result() {
                return r;
            }
            match self.deadline {
                Some(d) => {
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = tokio::time::sleep_until(d) => {
                            // Under the registry lock: either a server
                            // delivered first (take it), or nobody can now.
                            let mut inner = self.reg.lock();
                            if let Some(r) = self.waiter.take_result() {
                                return r;
                            }
                            if inner.remove(&self.waiter) {
                                self.reg.end();
                            }
                            return self.waiter.timeout_reply.clone();
                        }
                    }
                }
                None => notified.await,
            }
        }
    }
}

impl Drop for Blocked {
    fn drop(&mut self) {
        let mut inner = self.reg.lock();
        if inner.remove(&self.waiter) {
            self.reg.end();
        }
    }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// A blocking command, parsed: what to do, on which keys, for how long.
pub(crate) struct Plan {
    pub op: BlockOp,
    pub keys: Vec<Bytes>,
    /// `None` blocks forever.
    pub timeout: Option<Duration>,
    pub timeout_reply: CommandResponse,
    /// `XREAD … $`: ids resolved against the streams when blocking.
    pub resolve_last: Vec<bool>,
}

fn upper(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

/// Seconds as Redis parses a blocking timeout (float, `0` = forever).
pub(crate) fn parse_timeout_secs(arg: &[u8]) -> Result<Option<Duration>, CommandResponse> {
    let s = std::str::from_utf8(arg).unwrap_or("");
    let secs: f64 = s
        .parse()
        .ok()
        .filter(|v: &f64| v.is_finite())
        .ok_or_else(|| CommandResponse::error("ERR timeout is not a float or out of range"))?;
    if secs < 0.0 {
        return Err(CommandResponse::error("ERR timeout is negative"));
    }
    if secs > 1e12 {
        return Err(CommandResponse::error("ERR timeout is out of range"));
    }
    Ok((secs > 0.0).then(|| Duration::from_secs_f64(secs)))
}

/// Milliseconds (`XREAD BLOCK`, integer, `0` = forever).
pub(crate) fn parse_timeout_ms(arg: &[u8]) -> Result<Option<Duration>, CommandResponse> {
    let ms: i64 = std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| CommandResponse::error("ERR timeout is not an integer or out of range"))?;
    if ms < 0 {
        return Err(CommandResponse::error("ERR timeout is negative"));
    }
    Ok((ms > 0).then(|| Duration::from_millis(ms as u64)))
}

fn parse_count(arg: Option<&Bytes>) -> Result<usize, CommandResponse> {
    let n: i64 = arg
        .and_then(|a| std::str::from_utf8(a).ok()?.parse().ok())
        .ok_or_else(|| CommandResponse::error("ERR count should be greater than 0"))?;
    if n <= 0 {
        return Err(CommandResponse::error("ERR count should be greater than 0"));
    }
    Ok(n as usize)
}

fn parse_numkeys(arg: &[u8]) -> Result<usize, CommandResponse> {
    let n: i64 = std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| CommandResponse::error("ERR numkeys should be greater than 0"))?;
    if n <= 0 {
        return Err(CommandResponse::error(
            "ERR numkeys should be greater than 0",
        ));
    }
    Ok(n as usize)
}

/// `numkeys key… WHERE [COUNT n]` (shared by `LMPOP`, `ZMPOP` and their
/// blocking forms). `where_ok` maps the direction word to `left`/`max`.
pub(crate) fn parse_mpop(
    args: &[Bytes],
    cmd: &str,
    where_ok: fn(&str) -> Option<bool>,
) -> Result<(Vec<Bytes>, bool, usize), CommandResponse> {
    let Some(first) = args.first() else {
        return Err(CommandResponse::wrong_arity(cmd));
    };
    let numkeys = parse_numkeys(first)?;
    if args.len() < 1 + numkeys + 1 {
        return Err(CommandResponse::syntax_error());
    }
    let keys = args[1..1 + numkeys].to_vec();
    let dir = where_ok(&upper(&args[1 + numkeys])).ok_or_else(CommandResponse::syntax_error)?;
    let rest = &args[2 + numkeys..];
    let count = match rest {
        [] => 1,
        [c, n] if upper(c) == "COUNT" => parse_count(Some(n))?,
        _ => return Err(CommandResponse::syntax_error()),
    };
    Ok((keys, dir, count))
}

pub(crate) fn list_where(s: &str) -> Option<bool> {
    match s {
        "LEFT" => Some(true),
        "RIGHT" => Some(false),
        _ => None,
    }
}

pub(crate) fn zset_where(s: &str) -> Option<bool> {
    match s {
        "MIN" => Some(false),
        "MAX" => Some(true),
        _ => None,
    }
}

/// Whether `XREAD`/`XREADGROUP` asks to block.
pub fn xread_blocks(args: &[Bytes]) -> bool {
    for a in args {
        if a.eq_ignore_ascii_case(b"STREAMS") {
            return false;
        }
        if a.eq_ignore_ascii_case(b"BLOCK") {
            return true;
        }
    }
    false
}

/// Parse a blocking command. `Ok(None)`: this form does not block (an
/// `XREADGROUP` reading history), run it as usual.
pub(crate) fn plan(name: &str, args: &[Bytes]) -> Result<Option<Plan>, CommandResponse> {
    let nil_array = CommandResponse::NilArray;
    let lname = name.to_ascii_lowercase();
    let arity = |ok: bool| {
        if ok {
            Ok(())
        } else {
            Err(CommandResponse::wrong_arity(&lname))
        }
    };
    let plan = match name {
        "BLPOP" | "BRPOP" | "BZPOPMIN" | "BZPOPMAX" => {
            arity(args.len() >= 2)?;
            let timeout = parse_timeout_secs(&args[args.len() - 1])?;
            let keys = args[..args.len() - 1].to_vec();
            let op = match name {
                "BLPOP" => BlockOp::ListPop { left: true },
                "BRPOP" => BlockOp::ListPop { left: false },
                "BZPOPMIN" => BlockOp::ZPop { max: false },
                _ => BlockOp::ZPop { max: true },
            };
            Plan {
                op,
                resolve_last: vec![false; keys.len()],
                keys,
                timeout,
                timeout_reply: nil_array,
            }
        }
        "BLMOVE" => {
            arity(args.len() == 5)?;
            let from = list_where(&upper(&args[2])).ok_or_else(CommandResponse::syntax_error)?;
            let to = list_where(&upper(&args[3])).ok_or_else(CommandResponse::syntax_error)?;
            let timeout = parse_timeout_secs(&args[4])?;
            Plan {
                op: BlockOp::ListMove {
                    dst: args[1].clone(),
                    from_left: from,
                    to_left: to,
                },
                keys: vec![args[0].clone()],
                resolve_last: vec![false],
                timeout,
                timeout_reply: CommandResponse::Nil,
            }
        }
        "BRPOPLPUSH" => {
            arity(args.len() == 3)?;
            let timeout = parse_timeout_secs(&args[2])?;
            Plan {
                op: BlockOp::ListMove {
                    dst: args[1].clone(),
                    from_left: false,
                    to_left: true,
                },
                keys: vec![args[0].clone()],
                resolve_last: vec![false],
                timeout,
                timeout_reply: CommandResponse::Nil,
            }
        }
        "BLMPOP" | "BZMPOP" => {
            arity(args.len() >= 4)?;
            let timeout = parse_timeout_secs(&args[0])?;
            let (keys, dir, count) = if name == "BLMPOP" {
                parse_mpop(&args[1..], &lname, list_where)?
            } else {
                parse_mpop(&args[1..], &lname, zset_where)?
            };
            let op = if name == "BLMPOP" {
                BlockOp::ListMPop { left: dir, count }
            } else {
                BlockOp::ZMPop { max: dir, count }
            };
            Plan {
                op,
                resolve_last: vec![false; keys.len()],
                keys,
                timeout,
                timeout_reply: nil_array,
            }
        }
        "XREAD" => {
            let mut i = 0;
            let mut count = None;
            let mut timeout = None;
            let mut blocks = false;
            while i < args.len() {
                match upper(&args[i]).as_str() {
                    "COUNT" => {
                        let n = args
                            .get(i + 1)
                            .and_then(|a| std::str::from_utf8(a).ok()?.parse::<i64>().ok())
                            .ok_or_else(|| {
                                CommandResponse::error(
                                    "ERR value is not an integer or out of range",
                                )
                            })?;
                        count = (n > 0).then_some(n as usize);
                        i += 2;
                    }
                    "BLOCK" => {
                        let t = args.get(i + 1).ok_or_else(CommandResponse::syntax_error)?;
                        timeout = parse_timeout_ms(t)?;
                        blocks = true;
                        i += 2;
                    }
                    "STREAMS" => {
                        i += 1;
                        break;
                    }
                    _ => return Err(CommandResponse::syntax_error()),
                }
            }
            let rest = &args[i.min(args.len())..];
            if rest.is_empty() || !rest.len().is_multiple_of(2) {
                return Err(CommandResponse::error(
                    "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
                ));
            }
            if !blocks {
                return Ok(None);
            }
            let half = rest.len() / 2;
            let keys = rest[..half].to_vec();
            let mut ids = Vec::with_capacity(half);
            let mut resolve_last = Vec::with_capacity(half);
            for id in &rest[half..] {
                if id.as_ref() == b"$" {
                    ids.push(StreamId::MIN);
                    resolve_last.push(true);
                } else {
                    let s = std::str::from_utf8(id).unwrap_or("");
                    let parsed = match StreamId::parse(s) {
                        Some(solikv_core::types::StreamIdInput::Explicit(x)) => x,
                        Some(solikv_core::types::StreamIdInput::Partial(ms)) => {
                            StreamId::new(ms, 0)
                        }
                        Some(solikv_core::types::StreamIdInput::Min) => StreamId::MIN,
                        _ => {
                            return Err(CommandResponse::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            ))
                        }
                    };
                    ids.push(parsed);
                    resolve_last.push(false);
                }
            }
            Plan {
                op: BlockOp::XRead { count, ids },
                keys,
                timeout,
                timeout_reply: nil_array,
                resolve_last,
            }
        }
        "XREADGROUP" => {
            if args.len() < 6 || upper(&args[0]) != "GROUP" {
                return Err(CommandResponse::syntax_error());
            }
            let group = args[1].clone();
            let consumer = args[2].clone();
            let mut i = 3;
            let mut count = None;
            let mut timeout = None;
            let mut blocks = false;
            let mut noack = false;
            while i < args.len() {
                match upper(&args[i]).as_str() {
                    "COUNT" => {
                        let n = args
                            .get(i + 1)
                            .and_then(|a| std::str::from_utf8(a).ok()?.parse::<i64>().ok())
                            .ok_or_else(|| {
                                CommandResponse::error(
                                    "ERR value is not an integer or out of range",
                                )
                            })?;
                        count = (n > 0).then_some(n as usize);
                        i += 2;
                    }
                    "BLOCK" => {
                        let t = args.get(i + 1).ok_or_else(CommandResponse::syntax_error)?;
                        timeout = parse_timeout_ms(t)?;
                        blocks = true;
                        i += 2;
                    }
                    "NOACK" => {
                        noack = true;
                        i += 1;
                    }
                    "STREAMS" => {
                        i += 1;
                        break;
                    }
                    _ => return Err(CommandResponse::syntax_error()),
                }
            }
            let rest = &args[i.min(args.len())..];
            if rest.is_empty() || !rest.len().is_multiple_of(2) {
                return Err(CommandResponse::error(
                    "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.",
                ));
            }
            let half = rest.len() / 2;
            // Only new messages (`>`) block; reading a consumer's history
            // answers at once.
            if !blocks || rest[half..].iter().any(|id| id.as_ref() != b">") {
                return Ok(None);
            }
            Plan {
                op: BlockOp::XReadGroup {
                    group,
                    consumer,
                    count,
                    noack,
                },
                keys: rest[..half].to_vec(),
                resolve_last: vec![false; half],
                timeout,
                timeout_reply: nil_array,
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(plan))
}

/// The plain command that serves `op` on `key` (name, args), and how to
/// shape its reply for the blocked client.
pub(crate) fn effect(op: &BlockOp, key: &Bytes, keys: &[Bytes]) -> (&'static str, Vec<Bytes>) {
    let b = |s: &'static str| Bytes::from_static(s.as_bytes());
    match op {
        BlockOp::ListPop { left } => (if *left { "LPOP" } else { "RPOP" }, vec![key.clone()]),
        BlockOp::ListMove {
            dst,
            from_left,
            to_left,
        } => (
            "LMOVE",
            vec![
                key.clone(),
                dst.clone(),
                b(if *from_left { "LEFT" } else { "RIGHT" }),
                b(if *to_left { "LEFT" } else { "RIGHT" }),
            ],
        ),
        BlockOp::ListMPop { left, count } => (
            "LMPOP",
            vec![
                b("1"),
                key.clone(),
                b(if *left { "LEFT" } else { "RIGHT" }),
                b("COUNT"),
                Bytes::from(count.to_string()),
            ],
        ),
        BlockOp::ZPop { max } => (if *max { "ZPOPMAX" } else { "ZPOPMIN" }, vec![key.clone()]),
        BlockOp::ZMPop { max, count } => (
            "ZMPOP",
            vec![
                b("1"),
                key.clone(),
                b(if *max { "MAX" } else { "MIN" }),
                b("COUNT"),
                Bytes::from(count.to_string()),
            ],
        ),
        BlockOp::XRead { count, ids } => {
            let mut a = Vec::with_capacity(3 + 2 * keys.len());
            if let Some(n) = count {
                a.push(b("COUNT"));
                a.push(Bytes::from(n.to_string()));
            }
            a.push(b("STREAMS"));
            a.extend(keys.iter().cloned());
            a.extend(ids.iter().map(|id| Bytes::from(id.to_string())));
            ("XREAD", a)
        }
        BlockOp::XReadGroup {
            group,
            consumer,
            count,
            noack,
        } => {
            let mut a = vec![b("GROUP"), group.clone(), consumer.clone()];
            if let Some(n) = count {
                a.push(b("COUNT"));
                a.push(Bytes::from(n.to_string()));
            }
            if *noack {
                a.push(b("NOACK"));
            }
            a.push(b("STREAMS"));
            a.extend(keys.iter().cloned());
            a.extend(keys.iter().map(|_| b(">")));
            ("XREADGROUP", a)
        }
    }
}

/// Shape the effect's reply for the blocked client; `None` when it served
/// nothing (the client stays blocked).
pub(crate) fn shape(op: &BlockOp, key: &Bytes, resp: CommandResponse) -> Option<CommandResponse> {
    match (op, resp) {
        (_, CommandResponse::Nil | CommandResponse::NilArray) => None,
        // Streams deleted under an XREADGROUP: Redis unblocks with an error.
        (BlockOp::XReadGroup { .. }, CommandResponse::Error(e)) => {
            Some(CommandResponse::Error(if e.starts_with("NOGROUP") {
                "UNBLOCKED the stream key no longer exists".to_string()
            } else {
                e
            }))
        }
        // A key that changed type does not serve a blocked client: it keeps
        // waiting (Redis only signals keys of the type it waits for).
        (_, CommandResponse::Error(_)) => None,
        (BlockOp::ListPop { .. }, CommandResponse::BulkString(v)) => {
            Some(CommandResponse::Array(vec![
                CommandResponse::BulkString(key.clone()),
                CommandResponse::BulkString(v),
            ]))
        }
        (BlockOp::ZPop { .. }, CommandResponse::Array(items)) => {
            if items.is_empty() {
                return None;
            }
            let mut out = Vec::with_capacity(3);
            out.push(CommandResponse::BulkString(key.clone()));
            out.extend(items);
            Some(CommandResponse::Array(out))
        }
        (_, CommandResponse::Array(items)) if items.is_empty() => None,
        (_, r) => Some(r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn timeouts_parse_like_redis() {
        assert_eq!(parse_timeout_secs(b"0").unwrap(), None);
        assert_eq!(
            parse_timeout_secs(b"0.25").unwrap(),
            Some(Duration::from_millis(250))
        );
        assert!(parse_timeout_secs(b"-1").is_err());
        assert!(parse_timeout_secs(b"x").is_err());
        assert!(parse_timeout_secs(b"inf").is_err());
        assert_eq!(parse_timeout_ms(b"0").unwrap(), None);
        assert!(parse_timeout_ms(b"1.5").is_err());
    }

    #[test]
    fn plans() {
        let p = plan("BLPOP", &[b("a"), b("b"), b("1")]).unwrap().unwrap();
        assert_eq!(p.keys, vec![b("a"), b("b")]);
        assert!(matches!(p.op, BlockOp::ListPop { left: true }));
        let p = plan(
            "BLMPOP",
            &[
                b("0"),
                b("2"),
                b("a"),
                b("b"),
                b("RIGHT"),
                b("COUNT"),
                b("3"),
            ],
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            p.op,
            BlockOp::ListMPop {
                left: false,
                count: 3
            }
        ));
        assert!(p.timeout.is_none());
        assert!(plan("BLMPOP", &[b("0"), b("0"), b("LEFT")]).is_err());
        // Non-blocking XREAD / history XREADGROUP: run as usual.
        assert!(plan("XREAD", &[b("STREAMS"), b("s"), b("0")])
            .unwrap()
            .is_none());
        assert!(plan(
            "XREADGROUP",
            &[
                b("GROUP"),
                b("g"),
                b("c"),
                b("BLOCK"),
                b("0"),
                b("STREAMS"),
                b("s"),
                b("0")
            ]
        )
        .unwrap()
        .is_none());
        let p = plan(
            "XREAD",
            &[
                b("BLOCK"),
                b("100"),
                b("STREAMS"),
                b("s1"),
                b("s2"),
                b("$"),
                b("5-1"),
            ],
        )
        .unwrap()
        .unwrap();
        assert_eq!(p.resolve_last, vec![true, false]);
        assert_eq!(p.timeout, Some(Duration::from_millis(100)));
    }
}
