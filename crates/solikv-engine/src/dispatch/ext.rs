//! Commands added for Redis 7 parity: blocking commands and the list and
//! multi-pop families. The engine glue for blocking lives here too (see
//! `crate::blocking` for the design).

use bytes::Bytes;

use solikv_core::types::{RedisValue, StreamId};
use solikv_core::CommandResponse;

use super::{CommandEngine, Upper};
use crate::blocking::{
    self, effect, is_consuming, serve, shape, take_pending, waiters_on, BlockOp, BlockStart,
    Blocked, DeferGuard, Pending, Plan, Waiter,
};
use crate::session::Session;
use crate::shard::DbGuard;

fn int_arg(arg: &[u8]) -> Result<i64, CommandResponse> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| CommandResponse::error("ERR value is not an integer or out of range"))
}

impl CommandEngine {
    // -----------------------------------------------------------------
    // Blocking
    // -----------------------------------------------------------------

    /// Whether `name args` may block (a blocking command, or `XREAD`/
    /// `XREADGROUP` with `BLOCK`): the connection then runs it through
    /// [`block_start`](Self::block_start) instead of `execute_session`.
    pub fn is_blocking_form(&self, name: &str, args: &[Bytes]) -> bool {
        match name {
            "XREAD" | "XREADGROUP" => blocking::xread_blocks(args),
            _ => crate::commands::lookup(name).is_some_and(|s| s.has(crate::commands::F_BLOCKING)),
        }
    }

    /// Run a blocking command for a connection (not inside `MULTI`): served
    /// now if it can be, or registered as blocked.
    pub fn block_start(&self, session: &mut Session, name: &str, args: &[Bytes]) -> BlockStart {
        let mut plan = match blocking::plan(name, args) {
            Err(e) => return BlockStart::Done(e),
            Ok(None) => return BlockStart::Done(self.execute_session(session, name, args)),
            Ok(Some(p)) => p,
        };
        let _db = DbGuard::enter(session.db);
        // Counted before trying: a write racing with the try then sees
        // someone may be blocked and queues on the registry lock, which it
        // gets only once this client is registered.
        self.blocking.begin();
        let outcome = {
            let _defer = DeferGuard::enter();
            let mut inner = self.blocking.lock();
            self.resolve_last_ids(&mut plan);
            match self.try_plan(&plan) {
                Some(r) => {
                    self.blocking.end();
                    Ok(r)
                }
                None => {
                    // Nothing moved: whatever the tries noted is moot.
                    let _ = take_pending();
                    let waiter = Waiter::new(
                        session.info.id,
                        session.db,
                        plan.op,
                        plan.keys,
                        plan.timeout_reply,
                    );
                    Err(Blocked::register(
                        &self.blocking,
                        &mut inner,
                        waiter,
                        plan.timeout,
                    ))
                }
            }
        };
        match outcome {
            Ok(r) => {
                // A `BLMOVE` served at once pushed onto its destination,
                // which may unblock someone else.
                self.serve_blocked();
                BlockStart::Done(r)
            }
            Err(b) => BlockStart::Wait(b),
        }
    }

    /// `XREAD … $`: the IDs the streams end at now.
    fn resolve_last_ids(&self, plan: &mut Plan) {
        let BlockOp::XRead { ids, .. } = &mut plan.op else {
            return;
        };
        for (i, key) in plan.keys.iter().enumerate() {
            if !plan.resolve_last[i] {
                continue;
            }
            let k = key.clone();
            let r = self.shards.shard_for_key(key).execute(move |store| {
                match store.get(&k).map(|e| &e.value) {
                    Some(RedisValue::Stream(s)) => {
                        CommandResponse::bulk(Bytes::from(s.last_id.to_string()))
                    }
                    _ => CommandResponse::Nil,
                }
            });
            ids[i] = match r {
                CommandResponse::BulkString(b) => std::str::from_utf8(&b)
                    .ok()
                    .and_then(|s| s.split_once('-'))
                    .and_then(|(a, b)| Some(StreamId::new(a.parse().ok()?, b.parse().ok()?)))
                    .unwrap_or(StreamId::MIN),
                _ => StreamId::MIN,
            };
        }
    }

    /// Try a blocking command once. `Some` when it has an answer now (data,
    /// or an error such as `WRONGTYPE`), `None` when it would block.
    fn try_plan(&self, plan: &Plan) -> Option<CommandResponse> {
        match &plan.op {
            BlockOp::XRead { .. } | BlockOp::XReadGroup { .. } => {
                let (c, a) = effect(&plan.op, &plan.keys[0], &plan.keys);
                match self.execute(c, &a) {
                    CommandResponse::Error(e) => Some(CommandResponse::Error(e)),
                    r => shape(&plan.op, &plan.keys[0], r),
                }
            }
            _ => {
                // The keys are looked at in one step, as Redis does.
                let _held = self.shards.lock_keys(plan.keys.iter().map(|k| k.as_ref()));
                for key in &plan.keys {
                    let (c, a) = effect(&plan.op, key, &plan.keys);
                    let r = self.execute(c, &a);
                    if r.is_error() {
                        return Some(r);
                    }
                    if let Some(x) = shape(&plan.op, key, r) {
                        return Some(x);
                    }
                }
                None
            }
        }
    }

    /// A blocking command where it cannot block (inside `MULTI`, a script,
    /// REST): answer from the data there is, or as if it timed out.
    pub(super) fn blocking_once(&self, name: &str, args: &[Bytes]) -> CommandResponse {
        match blocking::plan(name, args) {
            Err(e) => e,
            Ok(None) => CommandResponse::NilArray,
            Ok(Some(plan)) => {
                let timeout_reply = plan.timeout_reply.clone();
                self.try_plan(&plan).unwrap_or(timeout_reply)
            }
        }
    }

    /// Serve clients blocked on the keys writes noted since the last call,
    /// in the order they blocked (see `crate::blocking`).
    pub(crate) fn serve_blocked(&self) {
        if !self.blocking.is_active() {
            let _ = take_pending();
            return;
        }
        let _defer = DeferGuard::enter();
        let mut next = take_pending();
        while let Some(pending) = next.take() {
            let mut inner = self.blocking.lock();
            let mut produced = Pending::default();
            for key in pending.keys_in(&inner) {
                // Databases found empty for this key this round.
                let mut exhausted: Vec<u32> = Vec::new();
                for w in waiters_on(&inner, &key) {
                    if exhausted.contains(&w.db) {
                        continue;
                    }
                    let (c, a) = effect(&w.op, &key, &w.keys);
                    let r = {
                        let _db = DbGuard::enter(w.db);
                        self.execute(c, &a)
                    };
                    let notes = take_pending();
                    match shape(&w.op, &key, r) {
                        Some(reply) => {
                            serve(&self.blocking, &mut inner, &w, reply);
                            if let Some(n) = notes {
                                produced.merge(n);
                            }
                        }
                        None => {
                            if is_consuming(&w) {
                                exhausted.push(w.db);
                            }
                        }
                    }
                }
            }
            drop(inner);
            next = produced.into_option();
        }
    }

    // -----------------------------------------------------------------
    // Lists
    // -----------------------------------------------------------------

    /// `LPOP key [count]` / `RPOP key [count]`.
    pub(super) fn list_pop(&self, left: bool, args: &[Bytes]) -> CommandResponse {
        let cmd = if left { "lpop" } else { "rpop" };
        if args.is_empty() || args.len() > 2 {
            return CommandResponse::wrong_arity(cmd);
        }
        let key = args[0].clone();
        match args.get(1) {
            None => self.shards.shard_for_key(&key).execute(move |store| {
                if left {
                    store.list_lpop(&key, 1)
                } else {
                    store.list_rpop(&key, 1)
                }
            }),
            Some(c) => {
                let n = match std::str::from_utf8(c)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(n) if n >= 0 => n as usize,
                    _ => {
                        return CommandResponse::error(
                            "ERR value is out of range, must be positive",
                        )
                    }
                };
                self.shards
                    .shard_for_key(&key)
                    .execute(move |store| store.list_pop_n(&key, n, left))
            }
        }
    }

    /// `LPUSHX` / `RPUSHX`.
    pub(super) fn list_pushx(&self, left: bool, args: &[Bytes]) -> CommandResponse {
        if args.len() < 2 {
            return CommandResponse::wrong_arity(if left { "lpushx" } else { "rpushx" });
        }
        let key = args[0].clone();
        let values = args[1..].to_vec();
        self.shards
            .shard_for_key(&key)
            .execute(move |store| store.list_pushx(&key, values, left))
    }

    /// `LMOVE src dst LEFT|RIGHT LEFT|RIGHT` / `RPOPLPUSH src dst`. Logged
    /// as itself when both keys share a shard, else as the pop and the push.
    pub(super) fn lmove(&self, name: &'static str, args: &[Bytes]) -> CommandResponse {
        let (from_left, to_left) = if name == "RPOPLPUSH" {
            if args.len() != 2 {
                return CommandResponse::wrong_arity("rpoplpush");
            }
            (false, true)
        } else {
            if args.len() != 4 {
                return CommandResponse::wrong_arity("lmove");
            }
            let w = |a: &Bytes| blocking::list_where(Upper::new(a).as_str());
            match (w(&args[2]), w(&args[3])) {
                (Some(f), Some(t)) => (f, t),
                _ => return CommandResponse::syntax_error(),
            }
        };
        let src = args[0].clone();
        let dst = args[1].clone();
        let si = self.shards.shard_index_for_key(&src);
        let di = self.shards.shard_index_for_key(&dst);
        if si == di {
            let record = args.to_vec();
            return self
                .shards
                .shard(si)
                .execute_with_effects(move |store, fx| {
                    let r = store.list_lmove(&src, &dst, from_left, to_left);
                    if matches!(r, CommandResponse::BulkString(_)) {
                        let refs: Vec<&[u8]> = record.iter().map(|a| a.as_ref()).collect();
                        fx.push(name, &refs);
                    }
                    r
                });
        }

        let _held = self.shards.lock_shards([si, di]);
        let s = src.clone();
        match self
            .shards
            .shard(si)
            .execute(move |store| match store.list_check(&s) {
                Err(e) => e,
                Ok(true) => CommandResponse::ok(),
                Ok(false) => CommandResponse::Nil,
            }) {
            CommandResponse::Ok => {}
            other => return other,
        }
        let d = dst.clone();
        if let CommandResponse::Error(e) =
            self.shards
                .shard(di)
                .execute(move |store| match store.list_check(&d) {
                    Err(e) => e,
                    Ok(_) => CommandResponse::ok(),
                })
        {
            return CommandResponse::Error(e);
        }
        let s = src.clone();
        let popped = self
            .shards
            .shard(si)
            .execute_with_effects(move |store, fx| match store.list_pop_one(&s, from_left) {
                Err(e) => e,
                Ok(None) => CommandResponse::Nil,
                Ok(Some(v)) => {
                    fx.push(if from_left { "LPOP" } else { "RPOP" }, &[&s]);
                    CommandResponse::bulk(v)
                }
            });
        let CommandResponse::BulkString(v) = popped else {
            return popped;
        };
        let value = v.clone();
        self.shards
            .shard(di)
            .execute_with_effects(move |store, fx| {
                let r = store.list_push_one(&dst, value.clone(), to_left);
                if !r.is_error() {
                    fx.push(if to_left { "LPUSH" } else { "RPUSH" }, &[&dst, &value]);
                }
                r
            });
        CommandResponse::bulk(v)
    }

    /// `LPOS key element [RANK rank] [COUNT num] [MAXLEN len]`.
    pub(super) fn lpos(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 2 {
            return CommandResponse::wrong_arity("lpos");
        }
        let mut rank = 1i64;
        let mut count = None;
        let mut maxlen = 0usize;
        let mut i = 2;
        while i < args.len() {
            let opt = Upper::new(&args[i]);
            let Some(v) = args.get(i + 1) else {
                return CommandResponse::syntax_error();
            };
            let n = match int_arg(v) {
                Ok(n) => n,
                Err(e) => return e,
            };
            match opt.as_str() {
                "RANK" => {
                    if n == 0 {
                        return CommandResponse::error(
                            "ERR RANK can't be zero: use 1 to start from the first match, 2 from \
                             the second ... or use negative to start from the end of the list",
                        );
                    }
                    if n == i64::MIN {
                        return CommandResponse::error(
                            "ERR value is out of range, value must between -9223372036854775807 \
                             and 9223372036854775807",
                        );
                    }
                    rank = n;
                }
                "COUNT" => {
                    if n < 0 {
                        return CommandResponse::error("ERR COUNT can't be negative");
                    }
                    count = Some(n as usize);
                }
                "MAXLEN" => {
                    if n < 0 {
                        return CommandResponse::error("ERR MAXLEN can't be negative");
                    }
                    maxlen = n as usize;
                }
                _ => return CommandResponse::syntax_error(),
            }
            i += 2;
        }
        let key = args[0].clone();
        let element = args[1].clone();
        self.shards
            .shard_for_key(&key)
            .execute(move |store| store.list_lpos(&key, &element, rank, count, maxlen))
    }

    /// `LMPOP numkeys key… LEFT|RIGHT [COUNT n]`: pops from the first
    /// non-empty key, logged as that `LPOP`/`RPOP`.
    pub(super) fn lmpop(&self, args: &[Bytes]) -> CommandResponse {
        let (keys, left, count) = match blocking::parse_mpop(args, "lmpop", blocking::list_where) {
            Ok(x) => x,
            Err(e) => return e,
        };
        let _held = self.shards.lock_keys(keys.iter().map(|k| k.as_ref()));
        let n = Bytes::from(count.to_string());
        for key in keys {
            let r = self.execute(
                if left { "LPOP" } else { "RPOP" },
                &[key.clone(), n.clone()],
            );
            match r {
                CommandResponse::Array(items) if !items.is_empty() => {
                    return CommandResponse::Array(vec![
                        CommandResponse::BulkString(key),
                        CommandResponse::Array(items),
                    ])
                }
                CommandResponse::Error(_) => return r,
                _ => {}
            }
        }
        CommandResponse::NilArray
    }

    /// `ZMPOP numkeys key… MIN|MAX [COUNT n]`.
    pub(super) fn zmpop(&self, args: &[Bytes]) -> CommandResponse {
        let (keys, max, count) = match blocking::parse_mpop(args, "zmpop", blocking::zset_where) {
            Ok(x) => x,
            Err(e) => return e,
        };
        let _held = self.shards.lock_keys(keys.iter().map(|k| k.as_ref()));
        let n = Bytes::from(count.to_string());
        for key in keys {
            let r = self.execute(
                if max { "ZPOPMAX" } else { "ZPOPMIN" },
                &[key.clone(), n.clone()],
            );
            match r {
                CommandResponse::Array(items) if !items.is_empty() => {
                    let pairs = items
                        .chunks(2)
                        .map(|p| CommandResponse::Array(p.to_vec()))
                        .collect();
                    return CommandResponse::Array(vec![
                        CommandResponse::BulkString(key),
                        CommandResponse::Array(pairs),
                    ]);
                }
                CommandResponse::Error(_) => return r,
                _ => {}
            }
        }
        CommandResponse::NilArray
    }
}

// ---------------------------------------------------------------------------
// Sorted sets, sets, hashes
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum ZAlg {
    Union,
    Inter,
    Diff,
    InterCard,
}

/// Most elements a negative `*RANDMEMBER` count may ask for: the reply is
/// built in memory, so an unbounded count is an allocation the client picks.
const MAX_RANDOM_COUNT: u64 = 1 << 24;

fn parse_random_count(arg: &[u8]) -> Result<i64, CommandResponse> {
    let n = int_arg(arg)?;
    if n.unsigned_abs() > MAX_RANDOM_COUNT && n < 0 {
        return Err(CommandResponse::error("ERR value is out of range"));
    }
    Ok(n)
}

impl CommandEngine {
    /// `ZUNION`/`ZINTER`/`ZDIFF` (+`STORE`) and `ZINTERCARD`, over sources on
    /// any shards (sets count as sorted sets scored 1). The shards involved
    /// are held together; a stored result is logged as the destination's
    /// value.
    pub(super) fn zalgebra(&self, name: &str, args: &[Bytes]) -> CommandResponse {
        let lname = name.to_ascii_lowercase();
        let alg = match name {
            "ZUNION" | "ZUNIONSTORE" => ZAlg::Union,
            "ZINTER" | "ZINTERSTORE" => ZAlg::Inter,
            "ZDIFF" | "ZDIFFSTORE" => ZAlg::Diff,
            _ => ZAlg::InterCard,
        };
        let store = name.ends_with("STORE");
        let (dest, rest) = if store {
            match args.split_first() {
                Some((d, r)) => (Some(d.clone()), r),
                None => return CommandResponse::wrong_arity(&lname),
            }
        } else {
            (None, args)
        };
        let Some(first) = rest.first() else {
            return CommandResponse::wrong_arity(&lname);
        };
        // Wider than i64 so that a count beyond any argument list is still
        // reported as such.
        let numkeys = match std::str::from_utf8(first)
            .ok()
            .and_then(|s| s.parse::<i128>().ok())
        {
            Some(n) => n.clamp(i64::MIN as i128, i64::MAX as i128) as i64,
            None => return CommandResponse::error("ERR value is not an integer or out of range"),
        };
        if numkeys < 1 {
            return CommandResponse::error(if alg == ZAlg::InterCard {
                "ERR numkeys should be greater than 0".to_string()
            } else {
                format!("ERR at least 1 input key is needed for '{}' command", lname)
            });
        }
        let numkeys = numkeys as usize;
        if numkeys > rest.len() - 1 {
            return CommandResponse::error(
                "ERR Number of keys can't be greater than number of args",
            );
        }
        let keys = &rest[1..1 + numkeys];
        let mut weights = vec![1.0f64; numkeys];
        let mut aggregate = solikv_core::zset_ops::Aggregate::Sum;
        let mut withscores = false;
        let mut limit = 0usize;
        let mut i = 1 + numkeys;
        let weighted_ok = matches!(alg, ZAlg::Union | ZAlg::Inter);
        while i < rest.len() {
            let opt = Upper::new(&rest[i]);
            match opt.as_str() {
                "WEIGHTS" if weighted_ok && i + numkeys < rest.len() => {
                    for (w, a) in weights.iter_mut().zip(&rest[i + 1..=i + numkeys]) {
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
                "AGGREGATE" if weighted_ok && i + 1 < rest.len() => {
                    use solikv_core::zset_ops::Aggregate;
                    aggregate = match Upper::new(&rest[i + 1]).as_str() {
                        "SUM" => Aggregate::Sum,
                        "MIN" => Aggregate::Min,
                        "MAX" => Aggregate::Max,
                        _ => return CommandResponse::syntax_error(),
                    };
                    i += 2;
                }
                "WITHSCORES" if !store && alg != ZAlg::InterCard => {
                    withscores = true;
                    i += 1;
                }
                "LIMIT" if alg == ZAlg::InterCard && i + 1 < rest.len() => {
                    match int_arg(&rest[i + 1]) {
                        Ok(n) if n >= 0 => limit = n as usize,
                        Ok(_) => return CommandResponse::error("ERR LIMIT can't be negative"),
                        Err(e) => return e,
                    }
                    i += 2;
                }
                _ => return CommandResponse::syntax_error(),
            }
        }

        let idx: Vec<usize> = dest
            .iter()
            .chain(keys)
            .map(|k| self.shards.shard_index_for_key(k))
            .collect();
        let _held = self.shards.lock_shards(idx);
        let mut sources = Vec::with_capacity(numkeys);
        for k in keys {
            let mut got = None;
            let r = self
                .shards
                .shard_for_key(k)
                .execute(|s| match s.zset_source(k) {
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
            solikv_core::zset_ops::Aggregate::Sum => {
                let s = a + b;
                if s.is_nan() {
                    0.0
                } else {
                    s
                }
            }
            solikv_core::zset_ops::Aggregate::Min => a.min(b),
            solikv_core::zset_ops::Aggregate::Max => a.max(b),
        };
        let weighted = |score: f64, w: f64| {
            let v = score * w;
            if v.is_nan() {
                0.0
            } else {
                v
            }
        };
        type Map = std::collections::HashMap<Bytes, f64, solikv_core::FastHasher>;
        let mut acc: Map = Map::default();
        match alg {
            ZAlg::Union => {
                for (src, w) in sources.into_iter().zip(&weights) {
                    for (m, sc) in src.unwrap_or_default() {
                        let v = weighted(sc, *w);
                        acc.entry(m)
                            .and_modify(|cur| *cur = combine(*cur, v))
                            .or_insert(v);
                    }
                }
            }
            ZAlg::Inter | ZAlg::InterCard => {
                if sources.iter().all(|s| s.is_some()) {
                    // Walk the smallest source, probe the others.
                    let mut srcs: Vec<(Map, f64)> = sources
                        .into_iter()
                        .zip(&weights)
                        .map(|(s, w)| (s.unwrap_or_default().into_iter().collect(), *w))
                        .collect();
                    let smallest = (0..srcs.len())
                        .min_by_key(|&i| srcs[i].0.len())
                        .unwrap_or(0);
                    let (first, w0) = srcs.swap_remove(smallest);
                    // Keep the others in their order for the aggregate.
                    'member: for (m, sc) in first {
                        let mut v = weighted(sc, w0);
                        for (other, w) in &srcs {
                            match other.get(&m) {
                                Some(o) => v = combine(v, weighted(*o, *w)),
                                None => continue 'member,
                            }
                        }
                        acc.insert(m, v);
                        if alg == ZAlg::InterCard && limit != 0 && acc.len() >= limit {
                            break;
                        }
                    }
                }
            }
            ZAlg::Diff => {
                let mut it = sources.into_iter();
                if let Some(Some(first)) = it.next() {
                    let others: Vec<Map> = it
                        .map(|s| s.unwrap_or_default().into_iter().collect())
                        .collect();
                    for (m, sc) in first {
                        if !others.iter().any(|o| o.contains_key(&m)) {
                            acc.insert(m, sc);
                        }
                    }
                }
            }
        }
        if alg == ZAlg::InterCard {
            return CommandResponse::integer(acc.len() as i64);
        }
        match dest {
            Some(dest) => self
                .shards
                .shard_for_key(&dest)
                .execute_with_effects(|s, fx| {
                    let n = s.zset_store(&dest, acc.into_iter().collect());
                    fx.put_key(s, &dest);
                    CommandResponse::integer(n)
                }),
            None => {
                let mut items: Vec<(Bytes, f64)> = acc.into_iter().collect();
                items.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
                solikv_core::zset_ops::zrange_reply(items, withscores)
            }
        }
    }

    /// `ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT o c]`.
    pub(super) fn zrangestore(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 4 {
            return CommandResponse::wrong_arity("zrangestore");
        }
        let (q, withscores) = match super::parse_zrange("ZRANGE", &args[1..]) {
            Ok(x) => x,
            Err(e) => return e,
        };
        if withscores {
            return CommandResponse::syntax_error();
        }
        let dst = args[0].clone();
        let src = args[1].clone();
        let _held = self.shards.lock_keys([dst.as_ref(), src.as_ref()]);
        let mut items = Vec::new();
        let r = self
            .shards
            .shard_for_key(&src)
            .execute(|s| match s.zset_range_query(&src, &q) {
                Ok(v) => {
                    items = v;
                    CommandResponse::ok()
                }
                Err(e) => e,
            });
        if r.is_error() {
            return r;
        }
        self.shards
            .shard_for_key(&dst)
            .execute_with_effects(|s, fx| {
                let n = s.zset_store(&dst, items);
                fx.put_key(s, &dst);
                CommandResponse::integer(n)
            })
    }

    pub(super) fn zmscore(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 2 {
            return CommandResponse::wrong_arity("zmscore");
        }
        let key = args[0].clone();
        let members = args[1..].to_vec();
        self.shards
            .shard_for_key(&key)
            .execute(move |s| s.zset_zmscore(&key, &members))
    }

    /// `ZRANDMEMBER key [count [WITHSCORES]]` / `HRANDFIELD key [count
    /// [WITHVALUES]]`.
    pub(super) fn randmember(&self, hash: bool, args: &[Bytes]) -> CommandResponse {
        let (cmd, with) = if hash {
            ("hrandfield", "WITHVALUES")
        } else {
            ("zrandmember", "WITHSCORES")
        };
        if args.is_empty() || args.len() > 3 {
            return CommandResponse::wrong_arity(cmd);
        }
        let count = match args.get(1).map(|a| parse_random_count(a)) {
            None => None,
            Some(Ok(n)) => Some(n),
            Some(Err(e)) => return e,
        };
        let withx = match args.get(2) {
            None => false,
            Some(a) if Upper::new(a).as_str() == with => true,
            Some(_) => return CommandResponse::syntax_error(),
        };
        let key = args[0].clone();
        self.shards.shard_for_key(&key).execute(move |s| {
            if hash {
                s.hash_hrandfield(&key, count, withx)
            } else {
                s.zset_zrandmember(&key, count, withx)
            }
        })
    }

    pub(super) fn smismember(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 2 {
            return CommandResponse::wrong_arity("smismember");
        }
        let key = args[0].clone();
        let members = args[1..].to_vec();
        self.shards
            .shard_for_key(&key)
            .execute(move |s| s.set_smismember(&key, &members))
    }

    /// `SINTERCARD numkeys key… [LIMIT n]`, over sets on any shards.
    pub(super) fn sintercard(&self, args: &[Bytes]) -> CommandResponse {
        let Some(first) = args.first() else {
            return CommandResponse::wrong_arity("sintercard");
        };
        let numkeys = match int_arg(first) {
            Ok(n) if n > 0 => n as usize,
            Ok(_) => return CommandResponse::error("ERR numkeys should be greater than 0"),
            Err(e) => return e,
        };
        if numkeys > args.len() - 1 {
            return CommandResponse::error(
                "ERR Number of keys can't be greater than number of args",
            );
        }
        let keys = &args[1..1 + numkeys];
        let mut limit = 0usize;
        match &args[1 + numkeys..] {
            [] => {}
            [l, n] if Upper::new(l).as_str() == "LIMIT" => match int_arg(n) {
                Ok(n) if n >= 0 => limit = n as usize,
                Ok(_) => return CommandResponse::error("ERR LIMIT can't be negative"),
                Err(e) => return e,
            },
            _ => return CommandResponse::syntax_error(),
        }
        let held = self.shards.lock_keys(keys.iter().map(|k| k.as_ref()));
        let mut sets = Vec::with_capacity(keys.len());
        for k in keys {
            // Expired keys must not count: let a lookup drop them first.
            let kk = k.clone();
            let r = self
                .shards
                .shard_for_key(k)
                .execute(move |s| match s.set_check(&kk) {
                    Err(e) => e,
                    Ok(()) => CommandResponse::ok(),
                });
            if r.is_error() {
                return r;
            }
        }
        let si: Vec<usize> = keys
            .iter()
            .map(|k| self.shards.shard_index_for_key(k))
            .collect();
        for (k, &i) in keys.iter().zip(&si) {
            // SAFETY: nothing runs against the held shards while `sets`
            // borrows them (this block only reads).
            let Some(store) = (unsafe { self.shards.held_store(&held, i) }) else {
                // Solo mode holds nothing: fall back to a copy.
                return self.sintercard_solo(keys, limit);
            };
            match store.peek(k).map(|e| &e.value) {
                None => sets.push(None),
                Some(RedisValue::Set(s)) => sets.push(Some(&**s)),
                Some(_) => return CommandResponse::wrong_type(),
            }
        }
        let n = if sets.iter().any(|s| s.is_none()) {
            0
        } else {
            let inter = solikv_core::set_ops::intersect(&sets);
            if limit == 0 {
                inter.len()
            } else {
                inter.len().min(limit)
            }
        };
        CommandResponse::integer(n as i64)
    }

    fn sintercard_solo(&self, keys: &[Bytes], limit: usize) -> CommandResponse {
        let keys = keys.to_vec();
        self.shards.shard(0).execute(move |s| {
            match s.set_op(
                solikv_core::set_ops::SetOp::parse("SINTER").expect("SINTER"),
                &keys,
            ) {
                Err(e) => e,
                Ok(r) => {
                    let n = if limit == 0 {
                        r.len()
                    } else {
                        r.len().min(limit)
                    };
                    CommandResponse::integer(n as i64)
                }
            }
        })
    }

    pub(super) fn hstrlen(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() != 2 {
            return CommandResponse::wrong_arity("hstrlen");
        }
        let key = args[0].clone();
        let field = args[1].clone();
        self.shards
            .shard_for_key(&key)
            .execute(move |s| s.hash_hstrlen(&key, &field))
    }
}
