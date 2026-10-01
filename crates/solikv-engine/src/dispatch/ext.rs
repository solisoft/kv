//! Commands added for Redis 7 parity: blocking commands and the list and
//! multi-pop families. The engine glue for blocking lives here too (see
//! `crate::blocking` for the design).

use bytes::Bytes;

use solikv_core::types::{FieldExpiry, RedisValue, SetExpiry, StreamId};
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
    #[inline]
    pub fn is_blocking_form(&self, name: &str, args: &[Bytes]) -> bool {
        // Every blocking command starts with B or X: the rest of the hot
        // path pays one byte compare, not a table lookup.
        if !matches!(name.as_bytes().first(), Some(b'B' | b'X')) {
            return false;
        }
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

    /// `HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field…` and its
    /// `HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT` forms (Redis 7.4).
    pub(super) fn hexpire(&self, id: crate::commands::Cmd, args: &[Bytes]) -> CommandResponse {
        use crate::commands::Cmd;
        let name = match id {
            Cmd::HEXPIRE => "hexpire",
            Cmd::HPEXPIRE => "hpexpire",
            Cmd::HEXPIREAT => "hexpireat",
            _ => "hpexpireat",
        };
        if args.len() < 5 {
            return CommandResponse::wrong_arity(name);
        }
        let key = args[0].clone();
        let Some(amount) = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        else {
            return CommandResponse::error("ERR value is not an integer or out of range");
        };
        if amount < 0 {
            return CommandResponse::error(format!(
                "ERR invalid expire time in '{}' command",
                name
            ));
        }
        let mut cond = solikv_core::store::ExpireCond::default();
        let mut at = 2;
        match Upper::new(&args[2]).as_str() {
            "NX" => cond.nx = true,
            "XX" => cond.xx = true,
            "GT" => cond.gt = true,
            "LT" => cond.lt = true,
            _ => at = 1,
        }
        let fields = match parse_fields(&args[at + 1..]) {
            Ok(f) => f,
            Err(e) => return e,
        };
        let now = solikv_core::now_millis() as i128;
        let deadline: i128 = match id {
            Cmd::HEXPIRE => now + amount as i128 * 1000,
            Cmd::HPEXPIRE => now + amount as i128,
            Cmd::HEXPIREAT => amount as i128 * 1000,
            _ => amount as i128,
        };
        if deadline > (i64::MAX / 2) as i128 {
            return CommandResponse::error(format!(
                "ERR invalid expire time in '{}' command",
                name
            ));
        }
        let deadline = deadline as u64;
        self.shards
            .shard_for_key(&key)
            .execute(move |s| s.hash_field_expire(&key, deadline, cond, &fields))
    }

    /// `HTTL`/`HPTTL`/`HEXPIRETIME`/`HPEXPIRETIME key FIELDS numfields field…`.
    pub(super) fn httl(&self, id: crate::commands::Cmd, args: &[Bytes]) -> CommandResponse {
        use crate::commands::Cmd;
        if args.len() < 4 {
            let name = crate::commands::spec(id).map_or("httl", |s| s.name);
            return CommandResponse::wrong_arity(&name.to_lowercase());
        }
        let key = args[0].clone();
        let fields = match parse_fields(&args[1..]) {
            Ok(f) => f,
            Err(e) => return e,
        };
        let millis = matches!(id, Cmd::HPTTL | Cmd::HPEXPIRETIME);
        let absolute = matches!(id, Cmd::HEXPIRETIME | Cmd::HPEXPIRETIME);
        self.shards
            .shard_for_key(&key)
            .execute(move |s| s.hash_field_ttl(&key, &fields, millis, absolute))
    }

    /// `HPERSIST key FIELDS numfields field…`.
    pub(super) fn hpersist(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 4 {
            return CommandResponse::wrong_arity("hpersist");
        }
        let key = args[0].clone();
        let fields = match parse_fields(&args[1..]) {
            Ok(f) => f,
            Err(e) => return e,
        };
        self.shards
            .shard_for_key(&key)
            .execute(move |s| s.hash_field_persist(&key, &fields))
    }

    /// `HGETDEL key FIELDS numfields field…` (Redis 8).
    pub(super) fn hgetdel(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 4 {
            return CommandResponse::wrong_arity("hgetdel");
        }
        let key = args[0].clone();
        let fields = match parse_fields(&args[1..]) {
            Ok(f) => f,
            Err(e) => return e,
        };
        self.shards
            .shard_for_key(&key)
            .execute_with_effects(move |s, fx| {
                let (resp, deleted) = s.hash_hgetdel(&key, &fields);
                log_hdel(fx, &key, &deleted);
                resp
            })
    }

    /// `HGETEX key [EX s | PX ms | EXAT s | PXAT ms | PERSIST] FIELDS
    /// numfields field…` (Redis 8).
    pub(super) fn hgetex(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 4 {
            return CommandResponse::wrong_arity("hgetex");
        }
        let key = args[0].clone();
        let mut op = FieldExpiry::Keep;
        let mut i = 1;
        while i < args.len() && !args[i].eq_ignore_ascii_case(b"FIELDS") {
            if op != FieldExpiry::Keep {
                return CommandResponse::syntax_error();
            }
            let o = Upper::new(&args[i]);
            if o.as_str() == "PERSIST" {
                op = FieldExpiry::Persist;
                i += 1;
                continue;
            }
            match deadline_arg(o.as_str(), args.get(i + 1), "hgetex") {
                Ok(at) => op = FieldExpiry::At(at),
                Err(e) => return e,
            }
            i += 2;
        }
        let fields = match parse_fields(&args[i..]) {
            Ok(f) => f,
            Err(e) => return e,
        };
        self.shards
            .shard_for_key(&key)
            .execute_with_effects(move |s, fx| {
                let (resp, ch) = s.hash_hgetex(&key, &fields, op);
                log_field_changes(fx, &key, &ch);
                resp
            })
    }

    /// `HSETEX key [FNX | FXX] [EX s | PX ms | EXAT s | PXAT ms | KEEPTTL]
    /// FIELDS numfields field value…` (Redis 8).
    pub(super) fn hsetex(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 5 {
            return CommandResponse::wrong_arity("hsetex");
        }
        let key = args[0].clone();
        let mut only: Option<bool> = None;
        let mut ttl: Option<SetExpiry> = None;
        let mut i = 1;
        while i < args.len() && !args[i].eq_ignore_ascii_case(b"FIELDS") {
            let o = Upper::new(&args[i]);
            match o.as_str() {
                "FNX" | "FXX" if only.is_none() => {
                    only = Some(o.as_str() == "FNX");
                    i += 1;
                }
                "KEEPTTL" if ttl.is_none() => {
                    ttl = Some(SetExpiry::Keep);
                    i += 1;
                }
                "EX" | "PX" | "EXAT" | "PXAT" if ttl.is_none() => {
                    match deadline_arg(o.as_str(), args.get(i + 1), "hsetex") {
                        Ok(at) => ttl = Some(SetExpiry::At(at)),
                        Err(e) => return e,
                    }
                    i += 2;
                }
                _ => return CommandResponse::syntax_error(),
            }
        }
        // `FIELDS numfields` then the pairs.
        let rest = &args[i..];
        if rest.len() < 2 || !rest[0].eq_ignore_ascii_case(b"FIELDS") {
            return CommandResponse::error(
                "ERR Mandatory argument FIELDS is missing or not at the right position",
            );
        }
        let n = match std::str::from_utf8(&rest[1])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) if n > 0 => n as usize,
            _ => return CommandResponse::error("ERR invalid number of fields"),
        };
        let kv = &rest[2..];
        if kv.len() != n * 2 {
            return CommandResponse::wrong_arity("hsetex");
        }
        let pairs: Vec<(Bytes, Bytes)> =
            kv.chunks(2).map(|p| (p[0].clone(), p[1].clone())).collect();
        let ttl = ttl.unwrap_or(SetExpiry::Discard);
        self.shards
            .shard_for_key(&key)
            .execute_with_effects(move |s, fx| {
                let (resp, ch) = s.hash_hsetex(&key, pairs, only, ttl);
                log_field_changes(fx, &key, &ch);
                resp
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

/// `EX s | PX ms | EXAT s | PXAT ms` (`opt` upper-cased, its value next)
/// as a deadline in unix ms, for `HGETEX`/`HSETEX` (`name`, for errors).
fn deadline_arg(opt: &str, value: Option<&Bytes>, name: &str) -> Result<u64, CommandResponse> {
    let invalid =
        || CommandResponse::error(format!("ERR invalid expire time in '{}' command", name));
    let Some(n) = value
        .and_then(|v| std::str::from_utf8(v).ok())
        .and_then(|s| s.parse::<i64>().ok())
    else {
        return Err(if value.is_none() {
            CommandResponse::syntax_error()
        } else {
            CommandResponse::error("ERR value is not an integer or out of range")
        });
    };
    let now = solikv_core::now_millis() as i128;
    let at: i128 = match opt {
        // A relative time must be positive; an absolute one may be past
        // (the fields then go at once).
        "EX" | "PX" if n <= 0 => return Err(invalid()),
        "EX" => now + n as i128 * 1000,
        "PX" => now + n as i128,
        "EXAT" | "PXAT" if n < 0 => return Err(invalid()),
        "EXAT" => n as i128 * 1000,
        "PXAT" => n as i128,
        _ => return Err(CommandResponse::syntax_error()),
    };
    if at > (i64::MAX / 2) as i128 {
        return Err(invalid());
    }
    Ok(at as u64)
}

/// `HDEL key field…` into the log, when any field went.
fn log_hdel(fx: &mut crate::aof_log::Effects, key: &Bytes, fields: &[Bytes]) {
    if fields.is_empty() {
        return;
    }
    let mut a: Vec<&[u8]> = Vec::with_capacity(1 + fields.len());
    a.push(key);
    a.extend(fields.iter().map(|f| f.as_ref()));
    fx.push("HDEL", &a);
}

/// What an `HGETEX`/`HSETEX` did, as the plain commands that redo it:
/// `HSET`, then the expiries as absolute `HPEXPIREAT`s (one per kept
/// deadline), `HPERSIST`, `HDEL`.
fn log_field_changes(
    fx: &mut crate::aof_log::Effects,
    key: &Bytes,
    ch: &solikv_core::FieldChanges,
) {
    if !ch.set.is_empty() {
        let mut a: Vec<&[u8]> = Vec::with_capacity(1 + 2 * ch.set.len());
        a.push(key);
        for (f, v) in &ch.set {
            a.push(f);
            a.push(v);
        }
        fx.push("HSET", &a);
    }
    let expire = |fx: &mut crate::aof_log::Effects, at: u64, fields: &[&Bytes]| {
        let at = at.to_string();
        let n = fields.len().to_string();
        let mut a: Vec<&[u8]> = vec![key, at.as_bytes(), b"FIELDS", n.as_bytes()];
        a.extend(fields.iter().map(|f| f.as_ref()));
        fx.push("HPEXPIREAT", &a);
    };
    if let Some((at, fields)) = &ch.expire_at {
        expire(fx, *at, &fields.iter().collect::<Vec<_>>());
    }
    for (f, at) in &ch.kept {
        expire(fx, *at, &[f]);
    }
    if !ch.persisted.is_empty() {
        let n = ch.persisted.len().to_string();
        let mut a: Vec<&[u8]> = vec![key, b"FIELDS", n.as_bytes()];
        a.extend(ch.persisted.iter().map(|f| f.as_ref()));
        fx.push("HPERSIST", &a);
    }
    log_hdel(fx, key, &ch.deleted);
}

/// `FIELDS numfields field…`, as the hash field expiry commands take it.
fn parse_fields(args: &[Bytes]) -> Result<Vec<Bytes>, CommandResponse> {
    if args.is_empty() || !Upper::new(&args[0]).as_str().eq("FIELDS") {
        return Err(CommandResponse::error(
            "ERR Mandatory argument FIELDS is missing or not at the right position",
        ));
    }
    let n = args
        .get(1)
        .and_then(|a| std::str::from_utf8(a).ok())
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| CommandResponse::error("ERR value is not an integer or out of range"))?;
    if n <= 0 {
        return Err(CommandResponse::error(
            "ERR Parameter `numFields` should be greater than 0",
        ));
    }
    let fields = &args[2..];
    if fields.len() as i64 != n {
        return Err(CommandResponse::error(
            "ERR The `numfields` parameter must match the number of arguments",
        ));
    }
    Ok(fields.to_vec())
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

/// An exact ID argument (`ms-seq` or `ms`), `-`/`+` allowed as the bounds.
fn stream_id_arg(arg: &Bytes, high: bool) -> Result<StreamId, CommandResponse> {
    use solikv_core::types::StreamIdInput;
    match super::parse_stream_id(arg) {
        Some(StreamIdInput::Explicit(id)) => Ok(id),
        Some(StreamIdInput::Partial(ms)) => Ok(StreamId::new(ms, if high { u64::MAX } else { 0 })),
        Some(StreamIdInput::Min) => Ok(StreamId::MIN),
        Some(StreamIdInput::Max) => Ok(StreamId::MAX),
        _ => Err(CommandResponse::error(
            "ERR Invalid stream ID specified as stream command argument",
        )),
    }
}

fn u64_arg(arg: &[u8], err: &str) -> Result<u64, CommandResponse> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n >= 0)
        .map(|n| n as u64)
        .ok_or_else(|| CommandResponse::error(err.to_string()))
}

/// The entry IDs in a stream-entries reply (`[[id, fields], …]` or, with
/// JUSTID, `[id, …]`).
fn reply_ids(r: &CommandResponse) -> Vec<StreamId> {
    let CommandResponse::Array(items) = r else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|it| {
            let id = match it {
                CommandResponse::Array(e) => e.first()?,
                other => other,
            };
            let CommandResponse::BulkString(b) = id else {
                return None;
            };
            let (a, s) = std::str::from_utf8(b).ok()?.split_once('-')?;
            Some(StreamId::new(a.parse().ok()?, s.parse().ok()?))
        })
        .collect()
}

/// Log what a claim did, as records that replay exactly: the consumer's
/// creation, each claimed entry with its delivery time and count, and the
/// entries that left the PEL because the stream no longer had them.
fn push_claim_effects(
    fx: &mut crate::aof_log::Effects,
    key: &Bytes,
    group: &Bytes,
    consumer: &Bytes,
    claimed: &[solikv_core::stream_ext::Claimed],
) {
    if !fx.enabled() {
        return;
    }
    fx.push("XGROUP", &[b"CREATECONSUMER", key, group, consumer]);
    for c in claimed {
        let id = c.id.to_string();
        match c.delivery {
            Some((t, n)) => {
                let (t, n) = (t.to_string(), n.to_string());
                fx.push(
                    "XCLAIM",
                    &[
                        key,
                        group,
                        consumer,
                        b"0",
                        id.as_bytes(),
                        b"TIME",
                        t.as_bytes(),
                        b"RETRYCOUNT",
                        n.as_bytes(),
                        b"FORCE",
                        b"JUSTID",
                    ],
                );
            }
            None => fx.push("XACK", &[key, group, id.as_bytes()]),
        }
    }
}

/// `XGROUP SETID` for a group's current position, logged after reads.
fn push_group_position(
    fx: &mut crate::aof_log::Effects,
    store: &mut solikv_core::ShardStore,
    key: &Bytes,
    group: &Bytes,
) {
    if !fx.enabled() {
        return;
    }
    if let Some((last, read)) = store.stream_group_state(key, group) {
        let last = last.to_string();
        match read {
            Some(n) => {
                let n = n.to_string();
                fx.push(
                    "XGROUP",
                    &[
                        b"SETID",
                        key,
                        group,
                        last.as_bytes(),
                        b"ENTRIESREAD",
                        n.as_bytes(),
                    ],
                );
            }
            None => fx.push("XGROUP", &[b"SETID", key, group, last.as_bytes()]),
        }
    }
}

impl CommandEngine {
    /// The per-shard `XREADGROUP` of one stream, logged as its effects (see
    /// `push_claim_effects`): replaying the read itself would redeliver at
    /// replay time and reset every idle time.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn xreadgroup_one(
        &self,
        key: &Bytes,
        group: &Bytes,
        consumer: &Bytes,
        id_input: solikv_core::types::StreamIdInput,
        count: Option<usize>,
        noack: bool,
    ) -> CommandResponse {
        let (k, g, c) = (key.clone(), group.clone(), consumer.clone());
        self.shards
            .shard_for_key(key)
            .execute_with_effects(move |store, fx| {
                let r = store.stream_xreadgroup_opts(&k, &g, &c, id_input, count, noack);
                if !r.is_error() && fx.enabled() {
                    let ids = reply_ids(&r);
                    let claimed: Vec<_> = store
                        .stream_pel_of(&k, &g, &ids)
                        .into_iter()
                        .filter(|x| x.delivery.is_some())
                        .collect();
                    push_claim_effects(fx, &k, &g, &c, &claimed);
                    if matches!(id_input, solikv_core::types::StreamIdInput::Max) {
                        push_group_position(fx, store, &k, &g);
                    }
                }
                r
            })
    }

    /// `XCLAIM key group consumer min-idle-time id… [IDLE ms] [TIME ms]
    /// [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]`.
    pub(super) fn xclaim(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 5 {
            return CommandResponse::wrong_arity("xclaim");
        }
        let min_idle = match u64_arg(&args[3], "ERR Invalid min-idle-time argument for XCLAIM") {
            Ok(n) => n,
            Err(e) => return e,
        };
        let mut ids = Vec::new();
        let mut i = 4;
        while i < args.len() {
            match super::parse_stream_id(&args[i]) {
                Some(solikv_core::types::StreamIdInput::Explicit(id)) => ids.push(id),
                Some(solikv_core::types::StreamIdInput::Partial(ms)) => {
                    ids.push(StreamId::new(ms, 0))
                }
                _ => break,
            }
            i += 1;
        }
        if ids.is_empty() {
            return CommandResponse::error(
                "ERR Invalid stream ID specified as stream command argument",
            );
        }
        let mut opts = solikv_core::stream_ext::XClaimOpts::default();
        while i < args.len() {
            let opt = Upper::new(&args[i]);
            let val = args.get(i + 1);
            match opt.as_str() {
                "FORCE" => opts.force = true,
                "JUSTID" => opts.justid = true,
                "IDLE" | "TIME" | "RETRYCOUNT" | "LASTID" => {
                    let Some(v) = val else {
                        return CommandResponse::syntax_error();
                    };
                    let err = format!("ERR Invalid {} option argument for XCLAIM", opt.as_str());
                    match opt.as_str() {
                        "IDLE" => match u64_arg(v, &err) {
                            Ok(n) => opts.idle = Some(n),
                            Err(e) => return e,
                        },
                        "TIME" => match u64_arg(v, &err) {
                            Ok(n) => opts.time = Some(n),
                            Err(e) => return e,
                        },
                        "RETRYCOUNT" => match u64_arg(v, &err) {
                            Ok(n) => opts.retrycount = Some(n),
                            Err(e) => return e,
                        },
                        _ => match stream_id_arg(v, false) {
                            Ok(id) => opts.lastid = Some(id),
                            Err(e) => return e,
                        },
                    }
                    i += 1;
                }
                _ => {
                    return CommandResponse::error(format!(
                        "ERR Unrecognized XCLAIM option '{}'",
                        String::from_utf8_lossy(&args[i])
                    ))
                }
            }
            i += 1;
        }
        let (k, g, c) = (args[0].clone(), args[1].clone(), args[2].clone());
        let lastid = opts.lastid.is_some();
        self.shards
            .shard_for_key(&k)
            .execute_with_effects(move |store, fx| {
                let mut claimed = Vec::new();
                let r = store.stream_xclaim(&k, &g, &c, min_idle, &ids, &opts, &mut claimed);
                if !r.is_error() {
                    push_claim_effects(fx, &k, &g, &c, &claimed);
                    if lastid {
                        push_group_position(fx, store, &k, &g);
                    }
                }
                r
            })
    }

    /// `XAUTOCLAIM key group consumer min-idle-time start [COUNT n]
    /// [JUSTID]`.
    pub(super) fn xautoclaim(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 5 {
            return CommandResponse::wrong_arity("xautoclaim");
        }
        let min_idle = match u64_arg(
            &args[3],
            "ERR Invalid min-idle-time argument for XAUTOCLAIM",
        ) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let start = match stream_id_arg(&args[4], false) {
            Ok(id) => id,
            Err(e) => return e,
        };
        let mut count = 100usize;
        let mut justid = false;
        let mut i = 5;
        while i < args.len() {
            match Upper::new(&args[i]).as_str() {
                "COUNT" => {
                    match args.get(i + 1).map(|v| int_arg(v)) {
                        Some(Ok(n)) if n > 0 && n <= (i64::MAX / 10) => count = n as usize,
                        Some(Ok(_)) => return CommandResponse::error("ERR COUNT must be > 0"),
                        Some(Err(e)) => return e,
                        None => return CommandResponse::syntax_error(),
                    }
                    i += 2;
                }
                "JUSTID" => {
                    justid = true;
                    i += 1;
                }
                _ => return CommandResponse::syntax_error(),
            }
        }
        let (k, g, c) = (args[0].clone(), args[1].clone(), args[2].clone());
        self.shards
            .shard_for_key(&k)
            .execute_with_effects(move |store, fx| {
                let mut claimed = Vec::new();
                let r = store.stream_xautoclaim(
                    &k,
                    &g,
                    &c,
                    min_idle,
                    start,
                    count,
                    justid,
                    &mut claimed,
                );
                if !r.is_error() {
                    push_claim_effects(fx, &k, &g, &c, &claimed);
                }
                r
            })
    }

    /// `XSETID key last-id [ENTRIESADDED n] [MAXDELETEDID id]`.
    pub(super) fn xsetid(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 2 {
            return CommandResponse::wrong_arity("xsetid");
        }
        let last = match stream_id_arg(&args[1], false) {
            Ok(id) => id,
            Err(e) => return e,
        };
        let mut added = None;
        let mut maxdel = None;
        let mut i = 2;
        while i < args.len() {
            let Some(v) = args.get(i + 1) else {
                return CommandResponse::syntax_error();
            };
            match Upper::new(&args[i]).as_str() {
                "ENTRIESADDED" => match u64_arg(v, "ERR entries_added must be positive") {
                    Ok(n) => added = Some(n),
                    Err(e) => return e,
                },
                "MAXDELETEDID" => match stream_id_arg(v, false) {
                    Ok(id) => maxdel = Some(id),
                    Err(e) => return e,
                },
                _ => return CommandResponse::syntax_error(),
            }
            i += 2;
        }
        let k = args[0].clone();
        self.shards
            .shard_for_key(&k)
            .execute(move |s| s.stream_xsetid(&k, last, added, maxdel))
    }

    /// `XINFO STREAM key [FULL [COUNT n]] | GROUPS key | CONSUMERS key
    /// group | HELP`.
    pub(super) fn xinfo(&self, args: &[Bytes]) -> CommandResponse {
        let Some(sub) = args.first() else {
            return CommandResponse::wrong_arity("xinfo");
        };
        let sub = Upper::new(sub);
        let key = args.get(1).cloned();
        match (sub.as_str(), key) {
            ("HELP", _) => CommandResponse::array(
                [
                    "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "CONSUMERS <key> <groupname>",
                    "GROUPS <key>",
                    "STREAM <key> [FULL [COUNT <count>]]",
                ]
                .iter()
                .map(|s| CommandResponse::bulk_string(s))
                .collect(),
            ),
            ("STREAM", Some(k)) => {
                let full = match &args[2..] {
                    [] => None,
                    [f] if Upper::new(f).as_str() == "FULL" => Some(10),
                    [f, c, n]
                        if Upper::new(f).as_str() == "FULL"
                            && Upper::new(c).as_str() == "COUNT" =>
                    {
                        match int_arg(n) {
                            Ok(n) if n >= 0 => Some(n as usize),
                            Ok(_) => Some(0),
                            Err(e) => return e,
                        }
                    }
                    _ => return CommandResponse::syntax_error(),
                };
                self.shards
                    .shard_for_key(&k)
                    .execute(move |s| s.stream_xinfo_full(&k, full))
            }
            ("GROUPS", Some(k)) if args.len() == 2 => self
                .shards
                .shard_for_key(&k)
                .execute(move |s| s.stream_xinfo_groups(&k)),
            ("CONSUMERS", Some(k)) if args.len() == 3 => {
                let g = args[2].clone();
                self.shards
                    .shard_for_key(&k)
                    .execute(move |s| s.stream_xinfo_consumers(&k, &g))
            }
            ("STREAM" | "GROUPS" | "CONSUMERS", _) => {
                CommandResponse::wrong_arity(&format!("xinfo|{}", sub.to_ascii_lowercase()))
            }
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try XINFO HELP.",
                String::from_utf8_lossy(&args[0])
            )),
        }
    }

    /// `XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD n] | SETID key
    /// group id|$ [ENTRIESREAD n] | CREATECONSUMER key group consumer |
    /// DELCONSUMER key group consumer | DESTROY key group | HELP`.
    pub(super) fn xgroup(&self, args: &[Bytes]) -> CommandResponse {
        let Some(sub) = args.first() else {
            return CommandResponse::wrong_arity("xgroup");
        };
        let sub = Upper::new(sub);
        let arity = |n: usize, exact: bool| {
            let ok = if exact {
                args.len() == n
            } else {
                args.len() >= n
            };
            (!ok).then(|| {
                CommandResponse::wrong_arity(&format!("xgroup|{}", sub.to_ascii_lowercase()))
            })
        };
        match sub.as_str() {
            "HELP" => CommandResponse::array(
                [
                    "XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "CREATE <key> <groupname> <id|$> [option]",
                    "CREATECONSUMER <key> <groupname> <consumer>",
                    "DELCONSUMER <key> <groupname> <consumername>",
                    "DESTROY <key> <groupname>",
                    "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
                ]
                .iter()
                .map(|s| CommandResponse::bulk_string(s))
                .collect(),
            ),
            "CREATE" | "SETID" => {
                if let Some(e) = arity(4, false) {
                    return e;
                }
                let (k, g) = (args[1].clone(), args[2].clone());
                let id = if args[3].as_ref() == b"$" {
                    None
                } else {
                    match stream_id_arg(&args[3], false) {
                        Ok(id) => Some(id),
                        Err(e) => return e,
                    }
                };
                let mut mkstream = false;
                let mut entries_read = None;
                let mut i = 4;
                while i < args.len() {
                    match Upper::new(&args[i]).as_str() {
                        "MKSTREAM" if sub.as_str() == "CREATE" => {
                            mkstream = true;
                            i += 1;
                        }
                        "ENTRIESREAD" if i + 1 < args.len() => {
                            match int_arg(&args[i + 1]) {
                                Ok(n) if n >= 0 => entries_read = Some(n as u64),
                                Ok(-1) => entries_read = None,
                                Ok(_) => {
                                    return CommandResponse::error(
                                        "ERR value for ENTRIESREAD must be positive or -1",
                                    )
                                }
                                Err(e) => return e,
                            }
                            i += 2;
                        }
                        _ => return CommandResponse::syntax_error(),
                    }
                }
                if sub.as_str() == "SETID" {
                    return self
                        .shards
                        .shard_for_key(&k)
                        .execute(move |s| s.stream_xgroup_setid(&k, &g, id, entries_read));
                }
                let input = match id {
                    None => solikv_core::types::StreamIdInput::Max,
                    Some(id) => solikv_core::types::StreamIdInput::Explicit(id),
                };
                self.shards.shard_for_key(&k).execute(move |s| {
                    s.stream_xgroup_create_opts(&k, g, input, mkstream, entries_read)
                })
            }
            "CREATECONSUMER" | "DELCONSUMER" => {
                if let Some(e) = arity(4, true) {
                    return e;
                }
                let (k, g, c) = (args[1].clone(), args[2].clone(), args[3].clone());
                let create = sub.as_str() == "CREATECONSUMER";
                self.shards.shard_for_key(&k).execute(move |s| {
                    if create {
                        s.stream_xgroup_createconsumer(&k, &g, &c)
                    } else {
                        s.stream_xgroup_delconsumer(&k, &g, &c)
                    }
                })
            }
            "DESTROY" => {
                if let Some(e) = arity(3, true) {
                    return e;
                }
                let (k, g) = (args[1].clone(), args[2].clone());
                self.shards
                    .shard_for_key(&k)
                    .execute(move |s| s.stream_xgroup_destroy(&k, &g))
            }
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try XGROUP HELP.",
                String::from_utf8_lossy(&args[0])
            )),
        }
    }

    /// `XPENDING key group [[IDLE min-idle] start end count [consumer]]`.
    pub(super) fn xpending(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 2 {
            return CommandResponse::wrong_arity("xpending");
        }
        let (k, g) = (args[0].clone(), args[1].clone());
        if args.len() == 2 {
            return self
                .shards
                .shard_for_key(&k)
                .execute(move |s| s.stream_xpending(&k, &g, None, None, None, None));
        }
        let mut i = 2;
        let mut min_idle = None;
        if Upper::new(&args[2]).as_str() == "IDLE" {
            match args.get(3).map(|v| int_arg(v)) {
                Some(Ok(n)) => min_idle = Some(n.max(0) as u64),
                Some(Err(e)) => return e,
                None => return CommandResponse::syntax_error(),
            }
            i = 4;
        }
        let rest = &args[i..];
        if rest.len() < 3 || rest.len() > 4 {
            return CommandResponse::syntax_error();
        }
        let start = match stream_id_arg(&rest[0], false) {
            Ok(id) => id,
            Err(e) => return e,
        };
        let end = match stream_id_arg(&rest[1], true) {
            Ok(id) => id,
            Err(e) => return e,
        };
        let count = match int_arg(&rest[2]) {
            Ok(n) => n.max(0) as usize,
            Err(e) => return e,
        };
        let consumer = rest.get(3).cloned();
        self.shards.shard_for_key(&k).execute(move |s| {
            s.stream_xpending_range(&k, &g, min_idle, start, end, count, consumer.as_ref())
        })
    }
}

// ---------------------------------------------------------------------------
// Keys, strings, geo
// ---------------------------------------------------------------------------

/// Parse `i8`/`u16`… for `BITFIELD`.
fn bit_type(arg: &[u8]) -> Result<solikv_core::string_ops::BitType, CommandResponse> {
    let err = || {
        CommandResponse::error(
            "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not \
             supported but i64 is.",
        )
    };
    let (&sign, rest) = arg.split_first().ok_or_else(err)?;
    let signed = match sign {
        b'i' | b'I' => true,
        b'u' | b'U' => false,
        _ => return Err(err()),
    };
    let bits: u32 = std::str::from_utf8(rest)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(err)?;
    if bits == 0 || (signed && bits > 64) || (!signed && bits > 63) {
        return Err(err());
    }
    Ok(solikv_core::string_ops::BitType { signed, bits })
}

/// A `BITFIELD` offset: bits, or `#n` fields of the type's width.
fn bit_offset(arg: &[u8], bits: u32) -> Result<u64, CommandResponse> {
    let err = || CommandResponse::error("ERR bit offset is not an integer or out of range");
    let (mul, digits) = match arg.split_first() {
        Some((b'#', rest)) => (bits as u64, rest),
        _ => (1, arg),
    };
    let n: u64 = std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(err)?;
    let off = n.checked_mul(mul).ok_or_else(err)?;
    if off.saturating_add(bits as u64) > solikv_core::types::MAX_BIT_OFFSET {
        return Err(err());
    }
    Ok(off)
}

/// `GEORADIUS*` → `GEOSEARCH`/`GEOSEARCHSTORE` arguments.
fn georadius_translate(
    by_member: bool,
    ro: bool,
    args: &[Bytes],
) -> Result<(&'static str, Vec<Bytes>), CommandResponse> {
    let b = |s: &'static str| Bytes::from_static(s.as_bytes());
    let fixed = if by_member { 4 } else { 5 };
    if args.len() < fixed {
        return Err(CommandResponse::wrong_arity(match (by_member, ro) {
            (false, false) => "georadius",
            (false, true) => "georadius_ro",
            (true, false) => "georadiusbymember",
            (true, true) => "georadiusbymember_ro",
        }));
    }
    let key = args[0].clone();
    let mut from = if by_member {
        vec![b("FROMMEMBER"), args[1].clone()]
    } else {
        vec![b("FROMLONLAT"), args[1].clone(), args[2].clone()]
    };
    from.extend([
        b("BYRADIUS"),
        args[fixed - 2].clone(),
        args[fixed - 1].clone(),
    ]);
    let mut opts = Vec::new();
    let mut store: Option<(Bytes, bool)> = None;
    let mut with = false;
    let mut i = fixed;
    while i < args.len() {
        let opt = Upper::new(&args[i]);
        match opt.as_str() {
            "WITHCOORD" | "WITHDIST" | "WITHHASH" => {
                with = true;
                opts.push(args[i].clone());
            }
            "ASC" | "DESC" | "ANY" => opts.push(args[i].clone()),
            "COUNT" => {
                let Some(n) = args.get(i + 1) else {
                    return Err(CommandResponse::syntax_error());
                };
                opts.push(args[i].clone());
                opts.push(n.clone());
                i += 1;
            }
            "STORE" | "STOREDIST" if !ro => {
                let Some(d) = args.get(i + 1) else {
                    return Err(CommandResponse::syntax_error());
                };
                store = Some((d.clone(), opt.as_str() == "STOREDIST"));
                i += 1;
            }
            _ => return Err(CommandResponse::syntax_error()),
        }
        i += 1;
    }
    match store {
        None => {
            let mut a = vec![key];
            a.extend(from);
            a.extend(opts);
            Ok(("GEOSEARCH", a))
        }
        Some(_) if with => Err(CommandResponse::error(
            "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and \
             WITHCOORD options",
        )),
        Some((dest, dist)) => {
            let mut a = vec![dest, key];
            a.extend(from);
            a.extend(opts);
            if dist {
                a.push(b("STOREDIST"));
            }
            Ok(("GEOSEARCHSTORE", a))
        }
    }
}

impl CommandEngine {
    /// `COPY source destination [DB db] [REPLACE]`: the value and its TTL,
    /// across shards and databases; logged as the destination's value.
    pub(super) fn copy(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 2 {
            return CommandResponse::wrong_arity("copy");
        }
        let (src, dst) = (args[0].clone(), args[1].clone());
        let here = crate::shard::current_db();
        let mut db = here;
        let mut replace = false;
        let mut i = 2;
        while i < args.len() {
            match Upper::new(&args[i]).as_str() {
                "REPLACE" => replace = true,
                "DB" if i + 1 < args.len() => {
                    match self.parse_db(&args[i + 1]) {
                        Ok(d) => db = d,
                        Err(e) => return e,
                    }
                    i += 1;
                }
                _ => return CommandResponse::syntax_error(),
            }
            i += 1;
        }
        if src == dst && db == here {
            return CommandResponse::error("ERR source and destination objects are the same");
        }
        let _held = self.shards.lock_keys([src.as_ref(), dst.as_ref()]);
        let mut got = None;
        self.shards.shard_for_key(&src).execute(|s| {
            if let Some(e) = s.get(&src) {
                got = Some((e.value.clone(), e.expires_at));
            }
            CommandResponse::ok()
        });
        let Some((value, expires_at)) = got else {
            return CommandResponse::integer(0);
        };
        let _db = DbGuard::enter(db);
        self.shards
            .shard_for_key(&dst)
            .execute_with_effects(move |s, fx| {
                if !replace && s.exists(&dst) {
                    return CommandResponse::integer(0);
                }
                let entry = match expires_at {
                    Some(at) => solikv_core::types::KeyEntry::with_expiry(value, at),
                    None => solikv_core::types::KeyEntry::new(value),
                };
                s.insert_entry(dst.clone(), entry);
                fx.put_key(s, &dst);
                CommandResponse::integer(1)
            })
    }

    /// `TIME`: seconds and microseconds.
    pub(super) fn time(&self) -> CommandResponse {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        CommandResponse::array(vec![
            CommandResponse::bulk(Bytes::from(now.as_secs().to_string())),
            CommandResponse::bulk(Bytes::from(now.subsec_micros().to_string())),
        ])
    }

    /// `DUMP key`: a Redis-format payload (see `solikv_persist::dump`).
    pub(super) fn dump(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() != 1 {
            return CommandResponse::wrong_arity("dump");
        }
        let key = args[0].clone();
        self.shards
            .shard_for_key(&key)
            .execute(move |s| match s.get(&key) {
                None => CommandResponse::Nil,
                Some(e) => CommandResponse::bulk(Bytes::from(solikv_persist::dump::dump(&e.value))),
            })
    }

    /// `RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f]`;
    /// logged as the value with its absolute expiry.
    pub(super) fn restore(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 3 {
            return CommandResponse::wrong_arity("restore");
        }
        let ttl = match int_arg(&args[1]) {
            Ok(t) if t >= 0 => t as u64,
            Ok(_) => return CommandResponse::error("ERR Invalid TTL value, must be >= 0"),
            Err(e) => return e,
        };
        let (mut replace, mut absttl) = (false, false);
        let (mut idle, mut freq) = (None, None);
        let mut i = 3;
        while i < args.len() {
            match Upper::new(&args[i]).as_str() {
                "REPLACE" => replace = true,
                "ABSTTL" => absttl = true,
                "IDLETIME" if i + 1 < args.len() && freq.is_none() => {
                    match int_arg(&args[i + 1]) {
                        Ok(n) if n >= 0 => idle = Some(n as u64),
                        Ok(_) => {
                            return CommandResponse::error(
                                "ERR Invalid IDLETIME value, must be >= 0",
                            )
                        }
                        Err(e) => return e,
                    }
                    i += 1;
                }
                "FREQ" if i + 1 < args.len() && idle.is_none() => {
                    match int_arg(&args[i + 1]) {
                        Ok(n) if (0..=255).contains(&n) => freq = Some(n as u8),
                        Ok(_) => {
                            return CommandResponse::error(
                                "ERR Invalid FREQ value, must be >= 0 and <= 255",
                            )
                        }
                        Err(e) => return e,
                    }
                    i += 1;
                }
                _ => return CommandResponse::syntax_error(),
            }
            i += 1;
        }
        let value = match solikv_persist::dump::restore(&args[2]) {
            Ok(v) => v,
            Err(solikv_persist::dump::RestoreError::VersionOrChecksum) => {
                return CommandResponse::error("ERR DUMP payload version or checksum are wrong")
            }
            Err(solikv_persist::dump::RestoreError::BadData) => {
                return CommandResponse::error("ERR Bad data format")
            }
        };
        let now = solikv_core::now_millis();
        let expires_at = match (ttl, absttl) {
            (0, _) => None,
            (t, true) => Some(t),
            (t, false) => Some(now.saturating_add(t)),
        };
        let key = args[0].clone();
        // As Redis' `objectSetLRUOrLFU`: IDLETIME counts under an LRU
        // policy, FREQ under an LFU one; the other is ignored.
        let lfu = solikv_core::access::is_lfu();
        let access = match (idle, freq) {
            (Some(secs), _) if !lfu => Some(solikv_core::access::with_idle(secs)),
            (_, Some(f)) if lfu => Some(solikv_core::access::with_freq(f as u32)),
            _ => None,
        };
        self.shards
            .shard_for_key(&key)
            .execute_with_effects(move |s, fx| {
                if !replace && s.exists(&key) {
                    return CommandResponse::error("BUSYKEY Target key name already exists.");
                }
                if expires_at.is_some_and(|at| at <= now) {
                    // Already expired: nothing is created (a replaced key
                    // is gone all the same).
                    if s.del(&key) {
                        fx.put_key(s, &key);
                    }
                    return CommandResponse::ok();
                }
                let mut entry = match expires_at {
                    Some(at) => solikv_core::types::KeyEntry::with_expiry(value, at),
                    None => solikv_core::types::KeyEntry::new(value),
                };
                if let Some(a) = access {
                    entry.access = a;
                }
                s.insert_entry(key.clone(), entry);
                fx.put_key(s, &key);
                CommandResponse::ok()
            })
    }

    /// `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN]`.
    pub(super) fn lcs(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 2 {
            return CommandResponse::wrong_arity("lcs");
        }
        let (mut len_only, mut idx, mut withlen, mut minlen) = (false, false, false, 0usize);
        let mut i = 2;
        while i < args.len() {
            match Upper::new(&args[i]).as_str() {
                "LEN" => len_only = true,
                "IDX" => idx = true,
                "WITHMATCHLEN" => withlen = true,
                "MINMATCHLEN" if i + 1 < args.len() => {
                    match int_arg(&args[i + 1]) {
                        Ok(n) => minlen = n.max(0) as usize,
                        Err(e) => return e,
                    }
                    i += 1;
                }
                _ => return CommandResponse::syntax_error(),
            }
            i += 1;
        }
        if len_only && idx {
            return CommandResponse::error(
                "ERR If you want both the length and indexes, please just use IDX.",
            );
        }
        let _held = self.shards.lock_keys([args[0].as_ref(), args[1].as_ref()]);
        let mut vals = Vec::with_capacity(2);
        for k in &args[..2] {
            let kk = k.clone();
            match self
                .shards
                .shard_for_key(k)
                .execute(move |s| s.string_get_raw(&kk))
            {
                CommandResponse::BulkString(b) => vals.push(b),
                CommandResponse::Nil => vals.push(Bytes::new()),
                _ => {
                    return CommandResponse::error(
                        "ERR The specified keys must contain string values",
                    )
                }
            }
        }
        let (a, b) = (&vals[0], &vals[1]);
        let (al, bl) = (a.len(), b.len());
        // The DP table is (al+1)*(bl+1) u32s: bounded like Redis' own.
        let cells = (al as u64 + 1).saturating_mul(bl as u64 + 1);
        if cells > (512 * 1024 * 1024) / 4 {
            return CommandResponse::error(
                "ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len",
            );
        }
        let w = bl + 1;
        let mut t = vec![0u32; (al + 1) * w];
        for i in 1..=al {
            for j in 1..=bl {
                t[i * w + j] = if a[i - 1] == b[j - 1] {
                    t[(i - 1) * w + j - 1] + 1
                } else {
                    t[(i - 1) * w + j].max(t[i * w + j - 1])
                };
            }
        }
        let total = t[al * w + bl] as usize;
        if len_only {
            return CommandResponse::integer(total as i64);
        }
        // Walk back, collecting the LCS and (for IDX) the matching ranges
        // exactly as Redis does.
        let mut out = vec![0u8; total];
        let mut matches = Vec::new();
        let (mut i, mut j, mut k) = (al, bl, total);
        let (mut a_start, mut a_end, mut b_start, mut b_end) = (al, 0usize, 0usize, 0usize);
        while i > 0 && j > 0 {
            let mut emit = false;
            if a[i - 1] == b[j - 1] {
                out[k - 1] = a[i - 1];
                if a_start == al {
                    a_start = i - 1;
                    a_end = i - 1;
                    b_start = j - 1;
                    b_end = j - 1;
                } else if a_start == i && b_start == j {
                    a_start -= 1;
                    b_start -= 1;
                } else {
                    emit = true;
                }
                if a_start == 0 || b_start == 0 {
                    emit = true;
                }
                k -= 1;
                i -= 1;
                j -= 1;
            } else {
                if t[(i - 1) * w + j] > t[i * w + j - 1] {
                    i -= 1;
                } else {
                    j -= 1;
                }
                if a_start != al {
                    emit = true;
                }
            }
            if emit {
                let mlen = a_end - a_start + 1;
                if idx && (minlen == 0 || mlen >= minlen) {
                    let mut m = vec![
                        CommandResponse::array(vec![
                            CommandResponse::integer(a_start as i64),
                            CommandResponse::integer(a_end as i64),
                        ]),
                        CommandResponse::array(vec![
                            CommandResponse::integer(b_start as i64),
                            CommandResponse::integer(b_end as i64),
                        ]),
                    ];
                    if withlen {
                        m.push(CommandResponse::integer(mlen as i64));
                    }
                    matches.push(CommandResponse::array(m));
                }
                a_start = al;
            }
        }
        if idx {
            CommandResponse::array(vec![
                CommandResponse::bulk_string("matches"),
                CommandResponse::array(matches),
                CommandResponse::bulk_string("len"),
                CommandResponse::integer(total as i64),
            ])
        } else {
            CommandResponse::bulk(Bytes::from(out))
        }
    }

    /// `BITCOUNT key [start end [BYTE|BIT]]`.
    pub(super) fn bitcount(&self, args: &[Bytes]) -> CommandResponse {
        let (range, bitmode) = match args.len() {
            1 => (None, false),
            3 | 4 => {
                let (s, e) = match (int_arg(&args[1]), int_arg(&args[2])) {
                    (Ok(s), Ok(e)) => (s, e),
                    (Err(x), _) | (_, Err(x)) => return x,
                };
                let bit = match args.get(3).map(|m| Upper::new(m)) {
                    None => false,
                    Some(m) if m.as_str() == "BYTE" => false,
                    Some(m) if m.as_str() == "BIT" => true,
                    Some(_) => return CommandResponse::syntax_error(),
                };
                (Some((s, e)), bit)
            }
            0 => return CommandResponse::wrong_arity("bitcount"),
            _ => return CommandResponse::syntax_error(),
        };
        let key = args[0].clone();
        self.shards
            .shard_for_key(&key)
            .execute(move |s| s.string_bitcount_mode(&key, range, bitmode))
    }

    /// `BITPOS key bit [start [end [BYTE|BIT]]]`.
    pub(super) fn bitpos(&self, args: &[Bytes]) -> CommandResponse {
        if args.len() < 2 || args.len() > 5 {
            return CommandResponse::wrong_arity("bitpos");
        }
        let bit = match args[1].as_ref() {
            b"0" => 0u8,
            b"1" => 1u8,
            _ => return CommandResponse::error("ERR The bit argument must be 1 or 0."),
        };
        let int = |i: usize| args.get(i).map(|a| int_arg(a)).transpose();
        let (start, end) = match (int(2), int(3)) {
            (Ok(s), Ok(e)) => (s, e),
            (Err(x), _) | (_, Err(x)) => return x,
        };
        let bitmode = match args.get(4).map(|m| Upper::new(m)) {
            None => false,
            Some(m) if m.as_str() == "BYTE" => false,
            Some(m) if m.as_str() == "BIT" => true,
            Some(_) => return CommandResponse::syntax_error(),
        };
        let key = args[0].clone();
        self.shards
            .shard_for_key(&key)
            .execute(move |s| s.string_bitpos(&key, bit, start, end, bitmode))
    }

    /// `BITFIELD key [GET t o] [SET t o v] [INCRBY t o n] [OVERFLOW
    /// WRAP|SAT|FAIL]…` and `BITFIELD_RO key [GET t o]…`.
    pub(super) fn bitfield(&self, ro: bool, args: &[Bytes]) -> CommandResponse {
        use solikv_core::string_ops::{BitOverflow, BitfieldOp};
        if args.is_empty() {
            return CommandResponse::wrong_arity(if ro { "bitfield_ro" } else { "bitfield" });
        }
        let mut ops = Vec::new();
        let mut of = BitOverflow::Wrap;
        let mut i = 1;
        while i < args.len() {
            let sub = Upper::new(&args[i]);
            let need = match sub.as_str() {
                "GET" => 2,
                "SET" | "INCRBY" => 3,
                "OVERFLOW" => 1,
                _ => return CommandResponse::syntax_error(),
            };
            if i + need >= args.len() {
                return CommandResponse::syntax_error();
            }
            if ro && sub.as_str() != "GET" {
                return CommandResponse::error("ERR BITFIELD_RO only supports the GET subcommand");
            }
            if sub.as_str() == "OVERFLOW" {
                of = match Upper::new(&args[i + 1]).as_str() {
                    "WRAP" => BitOverflow::Wrap,
                    "SAT" => BitOverflow::Sat,
                    "FAIL" => BitOverflow::Fail,
                    _ => return CommandResponse::error("ERR Invalid OVERFLOW type specified"),
                };
                i += 2;
                continue;
            }
            let t = match bit_type(&args[i + 1]) {
                Ok(t) => t,
                Err(e) => return e,
            };
            let o = match bit_offset(&args[i + 2], t.bits) {
                Ok(o) => o,
                Err(e) => return e,
            };
            ops.push(match sub.as_str() {
                "GET" => BitfieldOp::Get(t, o),
                _ => {
                    let v = match int_arg(&args[i + 3]) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    if sub.as_str() == "SET" {
                        BitfieldOp::Set(t, o, v, of)
                    } else {
                        BitfieldOp::Incr(t, o, v, of)
                    }
                }
            });
            i += need + 1;
        }
        let key = args[0].clone();
        self.shards
            .shard_for_key(&key)
            .execute(move |s| s.string_bitfield(&key, &ops))
    }

    /// `GEORADIUS[BYMEMBER][_RO]`: the deprecated forms, run as the
    /// `GEOSEARCH`/`GEOSEARCHSTORE` they are equivalent to.
    pub(super) fn georadius(&self, by_member: bool, ro: bool, args: &[Bytes]) -> CommandResponse {
        match georadius_translate(by_member, ro, args) {
            Ok((cmd, a)) => self.execute(cmd, &a),
            Err(e) => e,
        }
    }
}

// ---------------------------------------------------------------------------
// SORT
// ---------------------------------------------------------------------------

/// A `BY`/`GET` pattern applied to one element: the key (and hash field)
/// it names, `None` for a pattern without `*`.
fn pattern_target(pattern: &[u8], elem: &[u8]) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
    let star = pattern.iter().position(|&c| c == b'*')?;
    let (key_part, field) = match pattern.windows(2).rposition(|w| w == b"->") {
        Some(p) if p > star && p + 2 < pattern.len() => {
            (&pattern[..p], Some(pattern[p + 2..].to_vec()))
        }
        _ => (pattern, None),
    };
    let mut key = Vec::with_capacity(key_part.len() + elem.len());
    key.extend_from_slice(&key_part[..star]);
    key.extend_from_slice(elem);
    key.extend_from_slice(&key_part[star + 1..]);
    Some((key, field))
}

impl CommandEngine {
    /// What a `BY`/`GET` pattern finds for `elem` (`#` is the element).
    fn sort_lookup(&self, pattern: &[u8], elem: &Bytes) -> Option<Bytes> {
        if pattern == b"#" {
            return Some(elem.clone());
        }
        let (key, field) = pattern_target(pattern, elem)?;
        let r = match field {
            Some(f) => self.execute("HGET", &[Bytes::from(key), Bytes::from(f)]),
            None => self.execute("GET", &[Bytes::from(key)]),
        };
        match r {
            CommandResponse::BulkString(b) => Some(b),
            _ => None,
        }
    }

    /// `SORT key [BY pattern] [LIMIT offset count] [GET pattern…] [ASC|DESC]
    /// [ALPHA] [STORE destination]` and `SORT_RO` (no STORE). With patterns
    /// the keys they name can be anywhere, so every shard is held; a stored
    /// result is logged as the destination's value.
    pub(super) fn sort(&self, ro: bool, args: &[Bytes]) -> CommandResponse {
        if args.is_empty() {
            return CommandResponse::wrong_arity(if ro { "sort_ro" } else { "sort" });
        }
        let key = args[0].clone();
        let mut by: Option<Bytes> = None;
        let mut gets: Vec<Bytes> = Vec::new();
        let (mut desc, mut alpha) = (false, false);
        let mut limit: Option<(i64, i64)> = None;
        let mut store: Option<Bytes> = None;
        let mut i = 1;
        while i < args.len() {
            let opt = Upper::new(&args[i]);
            let next = args.get(i + 1);
            match (opt.as_str(), next) {
                ("ASC", _) => desc = false,
                ("DESC", _) => desc = true,
                ("ALPHA", _) => alpha = true,
                ("BY", Some(p)) => {
                    by = Some(p.clone());
                    i += 1;
                }
                ("GET", Some(p)) => {
                    gets.push(p.clone());
                    i += 1;
                }
                ("STORE", Some(d)) if !ro => {
                    store = Some(d.clone());
                    i += 1;
                }
                ("LIMIT", Some(o)) => {
                    let Some(c) = args.get(i + 2) else {
                        return CommandResponse::syntax_error();
                    };
                    match (int_arg(o), int_arg(c)) {
                        (Ok(o), Ok(c)) => limit = Some((o, c)),
                        (Err(e), _) | (_, Err(e)) => return e,
                    }
                    i += 2;
                }
                _ => return CommandResponse::syntax_error(),
            }
            i += 1;
        }
        let patterns = by.as_ref().is_some_and(|b| b.contains(&b'*'))
            || gets.iter().any(|g| g.as_ref() != b"#");
        let _held = if patterns {
            self.shards.lock_all()
        } else {
            self.shards
                .lock_keys(std::iter::once(key.as_ref()).chain(store.as_ref().map(|d| d.as_ref())))
        };
        // The elements, in the value's own order.
        let mut elems: Vec<Bytes> = Vec::new();
        let mut is_zset = false;
        let mut is_set = false;
        let k = key.clone();
        let r = self.shards.shard_for_key(&key).execute(|s| {
            match s.get(&k).map(|e| &e.value) {
                None => {}
                Some(RedisValue::List(l)) => elems = l.iter().cloned().collect(),
                Some(RedisValue::Set(st)) => {
                    is_set = true;
                    elems = st.iter().cloned().collect()
                }
                Some(RedisValue::ZSet(z)) => {
                    is_zset = true;
                    elems = z.scores.iter().map(|(_, m)| m.clone()).collect();
                }
                Some(_) => return CommandResponse::wrong_type(),
            }
            CommandResponse::ok()
        });
        if r.is_error() {
            return r;
        }
        // BY without `*` means "don't sort" (a zset keeps its score order).
        // Except a set's order is its own: where the result must be the
        // same everywhere — stored, or inside a script, whose effects are
        // replayed — Redis sorts it lexicographically, and so does this.
        let mut dontsort = by.as_ref().is_some_and(|b| !b.contains(&b'*'));
        if dontsort && is_set && (store.is_some() || crate::lua::in_script()) {
            dontsort = false;
            alpha = true;
            by = None;
        }
        if !dontsort {
            enum SortKey {
                Num(f64),
                Str(Option<Bytes>),
            }
            let mut keyed: Vec<(SortKey, Bytes)> = Vec::with_capacity(elems.len());
            for e in elems.drain(..) {
                let v = match &by {
                    Some(p) => self.sort_lookup(p, &e),
                    None => Some(e.clone()),
                };
                let sk = if alpha {
                    SortKey::Str(v)
                } else {
                    match v {
                        None => SortKey::Num(0.0),
                        Some(b) => match std::str::from_utf8(&b)
                            .ok()
                            .and_then(|s| s.trim().parse::<f64>().ok())
                            .filter(|f| !f.is_nan())
                        {
                            Some(f) => SortKey::Num(f),
                            None => {
                                return CommandResponse::error(
                                    "ERR One or more scores can't be converted into double",
                                )
                            }
                        },
                    }
                };
                keyed.push((sk, e));
            }
            keyed.sort_by(|(a, ea), (b, eb)| {
                let c = match (a, b) {
                    (SortKey::Num(x), SortKey::Num(y)) => x.total_cmp(y),
                    (SortKey::Str(x), SortKey::Str(y)) => x.cmp(y),
                    _ => std::cmp::Ordering::Equal,
                };
                let c = c.then_with(|| ea.cmp(eb));
                if desc {
                    c.reverse()
                } else {
                    c
                }
            });
            elems = keyed.into_iter().map(|(_, e)| e).collect();
        } else if desc && is_zset {
            elems.reverse();
        }
        if let Some((off, cnt)) = limit {
            let off = off.max(0) as usize;
            let cnt = if cnt < 0 { usize::MAX } else { cnt as usize };
            elems = elems.into_iter().skip(off).take(cnt).collect();
        }
        let out: Vec<Option<Bytes>> = if gets.is_empty() {
            elems.into_iter().map(Some).collect()
        } else {
            let mut v = Vec::with_capacity(elems.len() * gets.len());
            for e in &elems {
                for g in &gets {
                    v.push(self.sort_lookup(g, e));
                }
            }
            v
        };
        match store {
            None => CommandResponse::array(
                out.into_iter()
                    .map(|v| v.map_or(CommandResponse::Nil, CommandResponse::bulk))
                    .collect(),
            ),
            Some(dest) => {
                let list: std::collections::VecDeque<Bytes> =
                    out.into_iter().map(|v| v.unwrap_or_default()).collect();
                let n = list.len() as i64;
                self.shards
                    .shard_for_key(&dest)
                    .execute_with_effects(move |s, fx| {
                        if list.is_empty() {
                            s.del(&dest);
                        } else {
                            s.set(dest.clone(), RedisValue::List(list), None);
                        }
                        fx.put_key(s, &dest);
                        CommandResponse::integer(n)
                    })
            }
        }
    }
}
