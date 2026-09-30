use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::set_ops::{scan_reply, scan_window};
use crate::store::ShardStore;
use crate::types::*;

impl ShardStore {
    fn get_or_create_zset(&mut self, key: &Bytes) -> Result<&mut ZSetValue, CommandResponse> {
        let exists = match self.get(key) {
            Some(entry) if !matches!(entry.value, RedisValue::ZSet(_)) => {
                return Err(CommandResponse::wrong_type())
            }
            Some(_) => true,
            None => false,
        };
        if !exists {
            self.set(key.clone(), RedisValue::new_zset(), None);
        }
        match self.get_mut(key) {
            Some(KeyEntry {
                value: RedisValue::ZSet(zs),
                ..
            }) => Ok(zs),
            _ => Err(CommandResponse::wrong_type()),
        }
    }

    fn get_zset(&mut self, key: &Bytes) -> Result<Option<&ZSetValue>, CommandResponse> {
        match self.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::ZSet(zs) => Ok(Some(zs)),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    /// ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
    #[allow(clippy::too_many_arguments)]
    pub fn zset_zadd(
        &mut self,
        key: &Bytes,
        pairs: Vec<(f64, Bytes)>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
    ) -> CommandResponse {
        let zset = match self.get_or_create_zset(key) {
            Ok(z) => z,
            Err(e) => return e,
        };
        let mut added = 0i64;
        let mut changed = 0i64;

        for (score, member) in pairs {
            let (old, did) = zset.upsert(member, |existing| match existing {
                Some(_) if nx => None,
                None if xx => None,
                None => Some(score),
                Some(old) => {
                    let update = if gt && lt {
                        score != old
                    } else if gt {
                        score > old
                    } else if lt {
                        score < old
                    } else {
                        true
                    };
                    (update && score != old).then_some(score)
                }
            });
            if did {
                changed += 1;
                if old.is_none() {
                    added += 1;
                }
            }
        }

        if zset.is_empty() {
            // `XX` on a missing key created nothing.
            self.del(key);
        }
        CommandResponse::integer(if ch { changed } else { added })
    }

    pub fn zset_zrem(&mut self, key: &Bytes, members: &[Bytes]) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::integer(0),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::ZSet(zset) => {
                let mut removed = 0i64;
                for member in members {
                    if zset.remove(member) {
                        removed += 1;
                    }
                }
                if zset.is_empty() {
                    self.del(key);
                }
                CommandResponse::integer(removed)
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    pub fn zset_zscore(&mut self, key: &Bytes, member: &Bytes) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::nil(),
            Ok(Some(zset)) => match zset.score(member) {
                Some(score) => CommandResponse::bulk(Bytes::from(format_score(score))),
                None => CommandResponse::nil(),
            },
        }
    }

    pub fn zset_zcard(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(zset)) => CommandResponse::integer(zset.len() as i64),
        }
    }

    /// `ZRANK`: O(log n).
    pub fn zset_zrank(&mut self, key: &Bytes, member: &Bytes) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::nil(),
            Ok(Some(zset)) => match zset.rank(member) {
                Some(r) => CommandResponse::integer(r as i64),
                None => CommandResponse::nil(),
            },
        }
    }

    /// `ZREVRANK`: O(log n).
    pub fn zset_zrevrank(&mut self, key: &Bytes, member: &Bytes) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::nil(),
            Ok(Some(zset)) => match zset.rank(member) {
                Some(r) => CommandResponse::integer((zset.len() - 1 - r) as i64),
                None => CommandResponse::nil(),
            },
        }
    }

    pub fn zset_zincrby(&mut self, key: &Bytes, delta: f64, member: Bytes) -> CommandResponse {
        let zset = match self.get_or_create_zset(key) {
            Ok(z) => z,
            Err(e) => return e,
        };
        let old = zset.score(&member).unwrap_or(0.0);
        if (old + delta).is_nan() {
            return CommandResponse::error("ERR resulting score is not a number (NaN)");
        }
        let new_score = zset.incr(member, delta);
        CommandResponse::bulk(Bytes::from(format_score(new_score)))
    }

    /// `ZCOUNT key min max` (inclusive bounds): O(log n).
    pub fn zset_zcount(&mut self, key: &Bytes, min: f64, max: f64) -> CommandResponse {
        let q = ZRangeQuery {
            by: ZRangeBy::Score(ScoreBound::Incl(min), ScoreBound::Incl(max)),
            rev: false,
            limit: None,
        };
        self.zset_range_count(key, &q)
    }

    /// How many members `q` selects (`ZCOUNT`, `ZLEXCOUNT`): two binary
    /// searches, nothing collected.
    pub fn zset_range_count(&mut self, key: &Bytes, q: &ZRangeQuery) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(z)) => {
                let (lo, hi) = q.span(z);
                CommandResponse::integer((hi - lo) as i64)
            }
        }
    }

    /// ZRANGE key start stop [WITHSCORES]
    pub fn zset_zrange(
        &mut self,
        key: &Bytes,
        start: i64,
        stop: i64,
        withscores: bool,
    ) -> CommandResponse {
        self.rank_range(key, start, stop, false, withscores)
    }

    /// ZREVRANGE key start stop [WITHSCORES]
    pub fn zset_zrevrange(
        &mut self,
        key: &Bytes,
        start: i64,
        stop: i64,
        withscores: bool,
    ) -> CommandResponse {
        self.rank_range(key, start, stop, true, withscores)
    }

    fn rank_range(
        &mut self,
        key: &Bytes,
        start: i64,
        stop: i64,
        rev: bool,
        withscores: bool,
    ) -> CommandResponse {
        let q = ZRangeQuery {
            by: ZRangeBy::Rank(start, stop),
            rev,
            limit: None,
        };
        match self.zset_range_query(key, &q) {
            Ok(items) => zrange_reply(items, withscores),
            Err(e) => e,
        }
    }

    /// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    pub fn zset_zrangebyscore(
        &mut self,
        key: &Bytes,
        min: f64,
        max: f64,
        withscores: bool,
        offset: usize,
        count: Option<usize>,
    ) -> CommandResponse {
        let q = ZRangeQuery {
            by: ZRangeBy::Score(ScoreBound::Incl(min), ScoreBound::Incl(max)),
            rev: false,
            limit: Some((
                offset.min(i64::MAX as usize) as i64,
                count.map_or(-1, |c| c.min(i64::MAX as usize) as i64),
            )),
        };
        match self.zset_range_query(key, &q) {
            Ok(items) => zrange_reply(items, withscores),
            Err(e) => e,
        }
    }

    fn zset_pop(&mut self, key: &Bytes, count: usize, max: bool) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::array(vec![]),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::ZSet(zset) => {
                let mut items = Vec::with_capacity(2 * count.min(zset.len()));
                for _ in 0..count {
                    let Some((member, score)) = zset.pop(max) else {
                        break;
                    };
                    items.push(CommandResponse::bulk(member));
                    items.push(CommandResponse::bulk(Bytes::from(format_score(score))));
                }
                if zset.is_empty() {
                    self.del(key);
                }
                CommandResponse::array(items)
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    /// ZPOPMIN key [count]
    pub fn zset_zpopmin(&mut self, key: &Bytes, count: usize) -> CommandResponse {
        self.zset_pop(key, count, false)
    }

    /// ZPOPMAX key [count]
    pub fn zset_zpopmax(&mut self, key: &Bytes, count: usize) -> CommandResponse {
        self.zset_pop(key, count, true)
    }

    /// ZSCAN key cursor [MATCH pattern] [COUNT count]: visits `count`
    /// members by position, no copy of the set.
    pub fn zset_zscan(
        &mut self,
        key: &Bytes,
        cursor: usize,
        pattern: Option<&str>,
        count: usize,
    ) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => scan_reply(0, vec![]),
            Ok(Some(zset)) => {
                let (window, next) = scan_window(zset.members.len(), cursor, count);
                let mut items = Vec::with_capacity(window.len() * 2);
                for i in window.rev() {
                    let Some((member, score)) = zset.members.get_index(i) else {
                        continue;
                    };
                    let matches = pattern.is_none_or(|p| {
                        crate::store::glob_match_pub(p, &String::from_utf8_lossy(member))
                    });
                    if matches {
                        items.push(CommandResponse::bulk(member.clone()));
                        items.push(CommandResponse::bulk(Bytes::from(format_score(
                            score.into_inner(),
                        ))));
                    }
                }
                scan_reply(next, items)
            }
        }
    }

    /// ZUNIONSTORE dest numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    pub fn zset_zunionstore(
        &mut self,
        dest: &Bytes,
        keys: &[Bytes],
        weights: &[f64],
        aggregate: Aggregate,
    ) -> CommandResponse {
        let mut acc: indexmap::IndexMap<Bytes, f64, FastHasher> = Default::default();
        for (i, key) in keys.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            if let Some(KeyEntry {
                value: RedisValue::ZSet(zset),
                ..
            }) = self.get(key)
            {
                for (member, &score) in &zset.members {
                    let weighted = score.into_inner() * weight;
                    acc.entry(member.clone())
                        .and_modify(|s| *s = aggregate.apply(*s, weighted))
                        .or_insert(weighted);
                }
            }
        }
        let n = self.zset_store(dest, acc.into_iter().collect());
        CommandResponse::integer(n)
    }

    /// ZINTERSTORE dest numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    pub fn zset_zinterstore(
        &mut self,
        dest: &Bytes,
        keys: &[Bytes],
        weights: &[f64],
        aggregate: Aggregate,
    ) -> CommandResponse {
        for k in keys {
            if let Err(e) = self.get_zset(k) {
                return e;
            }
        }
        let sets: Vec<Option<&ZSetValue>> = keys
            .iter()
            .map(|k| match self.peek(k) {
                Some(KeyEntry {
                    value: RedisValue::ZSet(z),
                    ..
                }) => Some(&**z),
                _ => None,
            })
            .collect();
        let mut result: Vec<(Bytes, f64)> = Vec::new();
        if !sets.is_empty() && sets.iter().all(|s| s.is_some()) {
            let sets: Vec<&ZSetValue> = sets.into_iter().flatten().collect();
            let w = |i: usize| weights.get(i).copied().unwrap_or(1.0);
            // Walk the smallest set; probe the others.
            let (si, smallest) = sets
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| s.len())
                .map(|(i, s)| (i, *s))
                .expect("non-empty");
            'members: for (member, &score) in &smallest.members {
                let mut total: Option<f64> = None;
                for (i, set) in sets.iter().enumerate() {
                    let s = if i == si {
                        score.into_inner()
                    } else {
                        match set.members.get(member) {
                            Some(s) => s.into_inner(),
                            None => continue 'members,
                        }
                    } * w(i);
                    total = Some(match total {
                        None => s,
                        Some(t) => aggregate.apply(t, s),
                    });
                }
                if let Some(t) = total {
                    result.push((member.clone(), t));
                }
            }
        }
        let n = self.zset_store(dest, result);
        CommandResponse::integer(n)
    }
}

/// One end of a score range: `(5` is exclusive, `5`/`-inf`/`+inf` inclusive.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScoreBound {
    Incl(f64),
    Excl(f64),
}

impl ScoreBound {
    /// Parse `-inf`, `+inf`, `(1.5`, `1.5` … `None` for anything else.
    pub fn parse(arg: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(arg).ok()?;
        let (excl, num) = match s.strip_prefix('(') {
            Some(rest) => (true, rest),
            None => (false, s),
        };
        let v = parse_float(num)?;
        Some(if excl {
            ScoreBound::Excl(v)
        } else {
            ScoreBound::Incl(v)
        })
    }

    fn above_min(&self, v: f64) -> bool {
        match *self {
            ScoreBound::Incl(m) => v >= m,
            ScoreBound::Excl(m) => v > m,
        }
    }

    fn below_max(&self, v: f64) -> bool {
        match *self {
            ScoreBound::Incl(m) => v <= m,
            ScoreBound::Excl(m) => v < m,
        }
    }
}

/// One end of a lexicographic range: `-`, `+`, `[a` (inclusive), `(a`.
#[derive(Debug, Clone, PartialEq)]
pub enum LexBound {
    Min,
    Max,
    Incl(Bytes),
    Excl(Bytes),
}

impl LexBound {
    pub fn parse(arg: &Bytes) -> Option<Self> {
        match arg.first()? {
            b'-' if arg.len() == 1 => Some(LexBound::Min),
            b'+' if arg.len() == 1 => Some(LexBound::Max),
            b'[' => Some(LexBound::Incl(arg.slice(1..))),
            b'(' => Some(LexBound::Excl(arg.slice(1..))),
            _ => None,
        }
    }

    fn above_min(&self, m: &[u8]) -> bool {
        match self {
            LexBound::Min => true,
            LexBound::Max => false,
            LexBound::Incl(b) => m >= b.as_ref(),
            LexBound::Excl(b) => m > b.as_ref(),
        }
    }

    fn below_max(&self, m: &[u8]) -> bool {
        match self {
            LexBound::Min => false,
            LexBound::Max => true,
            LexBound::Incl(b) => m <= b.as_ref(),
            LexBound::Excl(b) => m < b.as_ref(),
        }
    }
}

/// Parse a score argument (`inf`, `-inf`, `+inf` included, NaN refused).
pub fn parse_float(s: &str) -> Option<f64> {
    let v = match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" => f64::INFINITY,
        "-inf" => f64::NEG_INFINITY,
        other => other.parse::<f64>().ok()?,
    };
    (!v.is_nan()).then_some(v)
}

/// Which members a range query selects.
#[derive(Debug, Clone)]
pub enum ZRangeBy {
    /// Ranks `start..=stop` (negative from the end), in the query's order.
    Rank(i64, i64),
    Score(ScoreBound, ScoreBound),
    Lex(LexBound, LexBound),
}

/// A `ZRANGE`-family query. For `Score`/`Lex` the bounds are always
/// `(min, max)`, whatever order the command took them in.
#[derive(Debug, Clone)]
pub struct ZRangeQuery {
    pub by: ZRangeBy,
    pub rev: bool,
    /// `LIMIT offset count` (negative count = all).
    pub limit: Option<(i64, i64)>,
}

impl ZRangeQuery {
    /// Positions `[lo, hi)` (ascending score order) the bounds select,
    /// before `LIMIT`. Two binary searches; ranks are direct.
    pub fn span(&self, zset: &ZSetValue) -> (usize, usize) {
        let len = zset.len();
        let sl = &zset.scores;
        match &self.by {
            ZRangeBy::Rank(start, stop) => {
                let s = normalize_index(*start, len as i64);
                let e = normalize_index(*stop, len as i64);
                if len == 0 || s > e || s >= len {
                    return (0, 0);
                }
                let e = e.min(len - 1);
                if self.rev {
                    // Ranks count from the highest score.
                    (len - 1 - e, len - s)
                } else {
                    (s, e + 1)
                }
            }
            ZRangeBy::Score(min, max) => {
                let lo = sl.partition_point(|(s, _)| !min.above_min(s.into_inner()));
                let hi = sl.partition_point(|(s, _)| max.below_max(s.into_inner()));
                (lo, hi.max(lo))
            }
            ZRangeBy::Lex(min, max) => {
                // Lexicographic ranges assume every member has the same
                // score, as Redis does; then member order is list order.
                let lo = sl.partition_point(|(_, m)| !min.above_min(m));
                let hi = sl.partition_point(|(_, m)| max.below_max(m));
                (lo, hi.max(lo))
            }
        }
    }

    /// The selected `(member, score)` pairs, in order.
    pub fn collect(&self, zset: &ZSetValue) -> Vec<(Bytes, f64)> {
        let (offset, count) = match self.limit {
            Some((o, _)) if o < 0 => return Vec::new(),
            Some((o, c)) => (o as usize, if c < 0 { usize::MAX } else { c as usize }),
            None => (0, usize::MAX),
        };
        let (lo, hi) = self.span(zset);
        let n = hi - lo;
        if offset >= n {
            return Vec::new();
        }
        let take = count.min(n - offset);
        let pair = |(s, m): &(OrderedFloat<f64>, Bytes)| (m.clone(), s.into_inner());
        if self.rev {
            zset.scores
                .range(hi - offset - take, hi - offset)
                .rev()
                .map(pair)
                .collect()
        } else {
            zset.scores
                .range(lo + offset, lo + offset + take)
                .map(pair)
                .collect()
        }
    }
}

/// Reply for a range: members, or member/score pairs `WITHSCORES`.
pub fn zrange_reply(items: Vec<(Bytes, f64)>, withscores: bool) -> CommandResponse {
    let mut result = Vec::with_capacity(items.len() * if withscores { 2 } else { 1 });
    for (member, score) in items {
        result.push(CommandResponse::bulk(member));
        if withscores {
            result.push(CommandResponse::bulk(Bytes::from(format_score(score))));
        }
    }
    CommandResponse::array(result)
}

impl ShardStore {
    /// Run a range query against the sorted set at `key`.
    pub fn zset_range_query(
        &mut self,
        key: &Bytes,
        q: &ZRangeQuery,
    ) -> Result<Vec<(Bytes, f64)>, CommandResponse> {
        Ok(match self.get_zset(key)? {
            None => Vec::new(),
            Some(z) => q.collect(z),
        })
    }

    /// Remove what `q` selects (`ZREMRANGEBY*`); returns how many.
    pub fn zset_remove_range(&mut self, key: &Bytes, q: &ZRangeQuery) -> CommandResponse {
        let victims = match self.zset_range_query(key, q) {
            Ok(v) => v,
            Err(e) => return e,
        };
        if victims.is_empty() {
            return CommandResponse::integer(0);
        }
        let members: Vec<Bytes> = victims.into_iter().map(|(m, _)| m).collect();
        self.zset_zrem(key, &members)
    }

    /// Members and scores of a sorted set — or of a set, each scored 1 —
    /// for `ZUNIONSTORE`/`ZINTERSTORE` over several shards.
    pub fn zset_source(
        &mut self,
        key: &Bytes,
    ) -> Result<Option<Vec<(Bytes, f64)>>, CommandResponse> {
        match self.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::ZSet(z) => Ok(Some(
                    z.members
                        .iter()
                        .map(|(m, s)| (m.clone(), s.into_inner()))
                        .collect(),
                )),
                RedisValue::Set(s) => Ok(Some(s.iter().map(|m| (m.clone(), 1.0)).collect())),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    /// Replace `dest` with these members (deleting it when empty); returns
    /// the cardinality.
    pub fn zset_store(&mut self, dest: &Bytes, members: Vec<(Bytes, f64)>) -> i64 {
        if members.is_empty() {
            self.del(dest);
            return 0;
        }
        let mut z = ZSetValue::new();
        z.members.reserve(members.len());
        for (m, s) in members {
            // A repeated member: the later score wins.
            z.members.insert(m, OrderedFloat(s));
        }
        let mut pairs: Vec<(OrderedFloat<f64>, Bytes)> =
            z.members.iter().map(|(m, s)| (*s, m.clone())).collect();
        pairs.sort();
        z.scores = crate::sorted_list::SortedList::from_sorted(pairs);
        let n = z.len() as i64;
        self.set(dest.clone(), RedisValue::zset(z), None);
        n
    }

    /// `ZADD … INCR delta member` with NX/XX/GT/LT: the new score, or nil
    /// when a condition stopped it.
    pub fn zset_zadd_incr(
        &mut self,
        key: &Bytes,
        delta: f64,
        member: Bytes,
        cond: (bool, bool, bool, bool),
    ) -> CommandResponse {
        let (nx, xx, gt, lt) = cond;
        let existing = match self.get_zset(key) {
            Err(e) => return e,
            Ok(z) => z.and_then(|z| z.score(&member)),
        };
        if (nx && existing.is_some()) || (xx && existing.is_none()) {
            return CommandResponse::nil();
        }
        let new = existing.unwrap_or(0.0) + delta;
        if new.is_nan() {
            return CommandResponse::error("ERR resulting score is not a number (NaN)");
        }
        if let Some(old) = existing {
            if (gt && new <= old) || (lt && new >= old) {
                return CommandResponse::nil();
            }
        }
        let zset = match self.get_or_create_zset(key) {
            Ok(z) => z,
            Err(e) => return e,
        };
        zset.insert(new, member);
        CommandResponse::bulk(Bytes::from(format_score(new)))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Aggregate {
    Sum,
    Min,
    Max,
}

impl Aggregate {
    pub fn apply(&self, a: f64, b: f64) -> f64 {
        match self {
            Aggregate::Sum => a + b,
            Aggregate::Min => a.min(b),
            Aggregate::Max => a.max(b),
        }
    }
}

fn normalize_index(idx: i64, len: i64) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx as usize
    }
}

pub fn format_score(score: f64) -> String {
    if score == score.trunc() && score.abs() < 1e15 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zadd_zcard() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        let r = store.zset_zadd(
            &key,
            vec![(1.0, Bytes::from("a")), (2.0, Bytes::from("b"))],
            false,
            false,
            false,
            false,
            false,
        );
        assert!(matches!(r, CommandResponse::Integer(2)));
        assert!(matches!(
            store.zset_zcard(&key),
            CommandResponse::Integer(2)
        ));
    }

    #[test]
    fn test_zadd_nx() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        store.zset_zadd(
            &key,
            vec![(1.0, Bytes::from("a"))],
            false,
            false,
            false,
            false,
            false,
        );
        store.zset_zadd(
            &key,
            vec![(2.0, Bytes::from("a"))],
            true,
            false,
            false,
            false,
            false,
        );
        match store.zset_zscore(&key, &Bytes::from("a")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("1")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_zrem() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        store.zset_zadd(
            &key,
            vec![(1.0, Bytes::from("a")), (2.0, Bytes::from("b"))],
            false,
            false,
            false,
            false,
            false,
        );
        let r = store.zset_zrem(&key, &[Bytes::from("a")]);
        assert!(matches!(r, CommandResponse::Integer(1)));
        assert!(matches!(
            store.zset_zcard(&key),
            CommandResponse::Integer(1)
        ));
    }

    #[test]
    fn test_zscore() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        store.zset_zadd(
            &key,
            vec![(1.5, Bytes::from("a"))],
            false,
            false,
            false,
            false,
            false,
        );
        match store.zset_zscore(&key, &Bytes::from("a")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("1.5")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_zrank() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        store.zset_zadd(
            &key,
            vec![
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
            false,
            false,
            false,
            false,
            false,
        );
        assert!(matches!(
            store.zset_zrank(&key, &Bytes::from("a")),
            CommandResponse::Integer(0)
        ));
        assert!(matches!(
            store.zset_zrank(&key, &Bytes::from("c")),
            CommandResponse::Integer(2)
        ));
    }

    #[test]
    fn test_zincrby() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        store.zset_zadd(
            &key,
            vec![(1.0, Bytes::from("a"))],
            false,
            false,
            false,
            false,
            false,
        );
        let r = store.zset_zincrby(&key, 2.5, Bytes::from("a"));
        match r {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("3.5")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_zcount() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        store.zset_zadd(
            &key,
            vec![
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
            false,
            false,
            false,
            false,
            false,
        );
        assert!(matches!(
            store.zset_zcount(&key, 1.0, 2.0),
            CommandResponse::Integer(2)
        ));
    }

    #[test]
    fn test_zrange() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        store.zset_zadd(
            &key,
            vec![
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
            false,
            false,
            false,
            false,
            false,
        );
        match store.zset_zrange(&key, 0, -1, false) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 3);
                assert!(
                    matches!(&items[0], CommandResponse::BulkString(v) if v == &Bytes::from("a"))
                );
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_zrange_withscores() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        store.zset_zadd(
            &key,
            vec![(1.0, Bytes::from("a")), (2.0, Bytes::from("b"))],
            false,
            false,
            false,
            false,
            false,
        );
        match store.zset_zrange(&key, 0, -1, true) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 4), // 2 pairs
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_zrevrange() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        store.zset_zadd(
            &key,
            vec![
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
            false,
            false,
            false,
            false,
            false,
        );
        match store.zset_zrevrange(&key, 0, 0, false) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 1);
                assert!(
                    matches!(&items[0], CommandResponse::BulkString(v) if v == &Bytes::from("c"))
                );
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_zpopmin_zpopmax() {
        let mut store = ShardStore::new();
        let key = Bytes::from("zset");
        store.zset_zadd(
            &key,
            vec![
                (1.0, Bytes::from("a")),
                (2.0, Bytes::from("b")),
                (3.0, Bytes::from("c")),
            ],
            false,
            false,
            false,
            false,
            false,
        );

        match store.zset_zpopmin(&key, 1) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(
                    matches!(&items[0], CommandResponse::BulkString(v) if v == &Bytes::from("a"))
                );
            }
            _ => panic!("expected array"),
        }

        match store.zset_zpopmax(&key, 1) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(
                    matches!(&items[0], CommandResponse::BulkString(v) if v == &Bytes::from("c"))
                );
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_zunionstore() {
        let mut store = ShardStore::new();
        store.zset_zadd(
            &Bytes::from("z1"),
            vec![(1.0, Bytes::from("a")), (2.0, Bytes::from("b"))],
            false,
            false,
            false,
            false,
            false,
        );
        store.zset_zadd(
            &Bytes::from("z2"),
            vec![(3.0, Bytes::from("b")), (4.0, Bytes::from("c"))],
            false,
            false,
            false,
            false,
            false,
        );

        let r = store.zset_zunionstore(
            &Bytes::from("out"),
            &[Bytes::from("z1"), Bytes::from("z2")],
            &[],
            Aggregate::Sum,
        );
        assert!(matches!(r, CommandResponse::Integer(3)));
    }

    #[test]
    fn test_zinterstore() {
        let mut store = ShardStore::new();
        store.zset_zadd(
            &Bytes::from("z1"),
            vec![(1.0, Bytes::from("a")), (2.0, Bytes::from("b"))],
            false,
            false,
            false,
            false,
            false,
        );
        store.zset_zadd(
            &Bytes::from("z2"),
            vec![(3.0, Bytes::from("b")), (4.0, Bytes::from("c"))],
            false,
            false,
            false,
            false,
            false,
        );

        let r = store.zset_zinterstore(
            &Bytes::from("out"),
            &[Bytes::from("z1"), Bytes::from("z2")],
            &[],
            Aggregate::Sum,
        );
        assert!(matches!(r, CommandResponse::Integer(1)));
    }
}
