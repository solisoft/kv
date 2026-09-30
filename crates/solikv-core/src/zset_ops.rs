use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::store::ShardStore;
use crate::types::*;

impl ShardStore {
    fn get_or_create_zset(&mut self, key: &Bytes) -> Result<&mut ZSetValue, CommandResponse> {
        if let Some(entry) = self.get(key) {
            if !matches!(entry.value, RedisValue::ZSet(_)) {
                return Err(CommandResponse::wrong_type());
            }
        }

        if self.get(key).is_none() {
            self.set(key.clone(), RedisValue::ZSet(ZSetValue::new()), None);
        }

        match self.get_mut(key).unwrap() {
            KeyEntry {
                value: RedisValue::ZSet(ref mut zs),
                ..
            } => Ok(zs),
            _ => unreachable!(),
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
            let existing_score = zset.members.get(&member).copied();

            if nx && existing_score.is_some() {
                continue;
            }
            if xx && existing_score.is_none() {
                continue;
            }

            if let Some(old) = existing_score {
                let old_f = old.into_inner();
                let update = if gt && lt {
                    score != old_f
                } else if gt {
                    score > old_f
                } else if lt {
                    score < old_f
                } else {
                    true
                };
                if update && score != old_f {
                    zset.insert(score, member);
                    changed += 1;
                }
            } else {
                zset.insert(score, member);
                added += 1;
                changed += 1;
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

    pub fn zset_zrank(&mut self, key: &Bytes, member: &Bytes) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::nil(),
            Ok(Some(zset)) => {
                let score = match zset.members.get(member) {
                    Some(s) => *s,
                    None => return CommandResponse::nil(),
                };
                let rank = zset.scores.range(..(score, member.clone())).count();
                CommandResponse::integer(rank as i64)
            }
        }
    }

    pub fn zset_zrevrank(&mut self, key: &Bytes, member: &Bytes) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::nil(),
            Ok(Some(zset)) => {
                let score = match zset.members.get(member) {
                    Some(s) => *s,
                    None => return CommandResponse::nil(),
                };
                let rank = zset.scores.range(..(score, member.clone())).count();
                CommandResponse::integer((zset.len() - 1 - rank) as i64)
            }
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

    pub fn zset_zcount(&mut self, key: &Bytes, min: f64, max: f64) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(zset)) => {
                let count = zset
                    .scores
                    .range((OrderedFloat(min), Bytes::new())..)
                    .take_while(|((s, _), _)| s.into_inner() <= max)
                    .count();
                CommandResponse::integer(count as i64)
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
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(zset)) => {
                let len = zset.len() as i64;
                let s = normalize_index(start, len);
                let e = normalize_index(stop, len);
                if s > e || s >= len as usize {
                    return CommandResponse::array(vec![]);
                }
                let e = e.min(len as usize - 1);

                let items: Vec<(f64, Bytes)> = zset
                    .scores
                    .iter()
                    .skip(s)
                    .take(e - s + 1)
                    .map(|((score, member), _)| (score.into_inner(), member.clone()))
                    .collect();

                format_zrange_result(items, withscores)
            }
        }
    }

    /// ZREVRANGE key start stop [WITHSCORES]
    pub fn zset_zrevrange(
        &mut self,
        key: &Bytes,
        start: i64,
        stop: i64,
        withscores: bool,
    ) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(zset)) => {
                let len = zset.len() as i64;
                let s = normalize_index(start, len);
                let e = normalize_index(stop, len);
                if s > e || s >= len as usize {
                    return CommandResponse::array(vec![]);
                }
                let e = e.min(len as usize - 1);

                let items: Vec<(f64, Bytes)> = zset
                    .scores
                    .iter()
                    .rev()
                    .skip(s)
                    .take(e - s + 1)
                    .map(|((score, member), _)| (score.into_inner(), member.clone()))
                    .collect();

                format_zrange_result(items, withscores)
            }
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
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(zset)) => {
                let iter = zset
                    .scores
                    .range((OrderedFloat(min), Bytes::new())..)
                    .take_while(|((s, _), _)| s.into_inner() <= max)
                    .skip(offset);

                let items: Vec<(f64, Bytes)> = if let Some(c) = count {
                    iter.take(c)
                        .map(|((s, m), _)| (s.into_inner(), m.clone()))
                        .collect()
                } else {
                    iter.map(|((s, m), _)| (s.into_inner(), m.clone()))
                        .collect()
                };

                format_zrange_result(items, withscores)
            }
        }
    }

    /// ZPOPMIN key [count]
    pub fn zset_zpopmin(&mut self, key: &Bytes, count: usize) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::array(vec![]),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::ZSet(zset) => {
                let mut items = Vec::new();
                for _ in 0..count {
                    let first = zset.scores.keys().next().cloned();
                    match first {
                        Some((score, member)) => {
                            zset.scores.remove(&(score, member.clone()));
                            zset.members.remove(&member);
                            items.push(CommandResponse::bulk(member));
                            items.push(CommandResponse::bulk(Bytes::from(format_score(
                                score.into_inner(),
                            ))));
                        }
                        None => break,
                    }
                }
                if zset.is_empty() {
                    self.del(key);
                }
                CommandResponse::array(items)
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    /// ZPOPMAX key [count]
    pub fn zset_zpopmax(&mut self, key: &Bytes, count: usize) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::array(vec![]),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::ZSet(zset) => {
                let mut items = Vec::new();
                for _ in 0..count {
                    let last = zset.scores.keys().next_back().cloned();
                    match last {
                        Some((score, member)) => {
                            zset.scores.remove(&(score, member.clone()));
                            zset.members.remove(&member);
                            items.push(CommandResponse::bulk(member));
                            items.push(CommandResponse::bulk(Bytes::from(format_score(
                                score.into_inner(),
                            ))));
                        }
                        None => break,
                    }
                }
                if zset.is_empty() {
                    self.del(key);
                }
                CommandResponse::array(items)
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    /// ZSCAN key cursor [MATCH pattern] [COUNT count]
    pub fn zset_zscan(
        &mut self,
        key: &Bytes,
        cursor: usize,
        pattern: Option<&str>,
        count: usize,
    ) -> CommandResponse {
        match self.get_zset(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![
                CommandResponse::bulk(Bytes::from("0")),
                CommandResponse::array(vec![]),
            ]),
            Ok(Some(zset)) => {
                let pairs: Vec<(Bytes, f64)> = zset
                    .scores
                    .iter()
                    .map(|((score, member), _)| (member.clone(), score.into_inner()))
                    .collect();
                let total = pairs.len();
                let start = cursor;
                let mut result = Vec::new();
                let mut i = start;
                let mut checked = 0;

                while checked < count && i < total {
                    let (ref member, score) = pairs[i];
                    let matches = pattern.is_none_or(|p| {
                        crate::store::glob_match_pub(p, &String::from_utf8_lossy(member))
                    });
                    if matches {
                        result.push(CommandResponse::bulk(member.clone()));
                        result.push(CommandResponse::bulk(Bytes::from(format_score(score))));
                    }
                    i += 1;
                    checked += 1;
                }

                let next_cursor = if i >= total { 0 } else { i };
                CommandResponse::array(vec![
                    CommandResponse::bulk(Bytes::from(next_cursor.to_string())),
                    CommandResponse::array(result),
                ])
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
        let mut result = ZSetValue::new();

        for (i, key) in keys.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            if let Some(entry) = self.get(key) {
                if let RedisValue::ZSet(zset) = &entry.value {
                    for (member, &score) in &zset.members {
                        let weighted = score.into_inner() * weight;
                        match result.members.get(member) {
                            Some(&existing) => {
                                let new_score = match aggregate {
                                    Aggregate::Sum => existing.into_inner() + weighted,
                                    Aggregate::Min => existing.into_inner().min(weighted),
                                    Aggregate::Max => existing.into_inner().max(weighted),
                                };
                                result.insert(new_score, member.clone());
                            }
                            None => {
                                result.insert(weighted, member.clone());
                            }
                        }
                    }
                }
            }
        }

        let len = result.len() as i64;
        self.set(dest.clone(), RedisValue::ZSet(result), None);
        CommandResponse::integer(len)
    }

    /// ZINTERSTORE dest numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
    pub fn zset_zinterstore(
        &mut self,
        dest: &Bytes,
        keys: &[Bytes],
        weights: &[f64],
        aggregate: Aggregate,
    ) -> CommandResponse {
        if keys.is_empty() {
            self.set(dest.clone(), RedisValue::ZSet(ZSetValue::new()), None);
            return CommandResponse::integer(0);
        }

        // Get all zsets as cloned data
        let mut sets: Vec<Option<ZSetValue>> = Vec::new();
        for key in keys {
            match self.get(key) {
                Some(entry) => match &entry.value {
                    RedisValue::ZSet(zs) => sets.push(Some(zs.clone())),
                    _ => sets.push(None),
                },
                None => sets.push(None),
            }
        }

        // If any set is missing, result is empty
        if sets.iter().any(|s| s.is_none()) {
            self.set(dest.clone(), RedisValue::ZSet(ZSetValue::new()), None);
            return CommandResponse::integer(0);
        }

        let first_weight = weights.first().copied().unwrap_or(1.0);
        let first_set = sets[0].as_ref().unwrap();
        let mut result = ZSetValue::new();

        for (member, &score) in &first_set.members {
            let base_score = score.into_inner() * first_weight;
            let mut final_score = base_score;
            let mut in_all = true;

            for (i, set_opt) in sets.iter().enumerate().skip(1) {
                let set = set_opt.as_ref().unwrap();
                let weight = weights.get(i).copied().unwrap_or(1.0);
                match set.members.get(member) {
                    Some(&s) => {
                        let weighted = s.into_inner() * weight;
                        final_score = match aggregate {
                            Aggregate::Sum => final_score + weighted,
                            Aggregate::Min => final_score.min(weighted),
                            Aggregate::Max => final_score.max(weighted),
                        };
                    }
                    None => {
                        in_all = false;
                        break;
                    }
                }
            }

            if in_all {
                result.insert(final_score, member.clone());
            }
        }

        let len = result.len() as i64;
        self.set(dest.clone(), RedisValue::ZSet(result), None);
        CommandResponse::integer(len)
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

    fn value(&self) -> f64 {
        match *self {
            ScoreBound::Incl(v) | ScoreBound::Excl(v) => v,
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
    /// The selected `(member, score)` pairs, in order.
    pub fn collect(&self, zset: &ZSetValue) -> Vec<(Bytes, f64)> {
        let (offset, count) = match self.limit {
            Some((o, _)) if o < 0 => return Vec::new(),
            Some((o, c)) => (o as usize, if c < 0 { usize::MAX } else { c as usize }),
            None => (0, usize::MAX),
        };
        let pair = |((s, m), _): (&(OrderedFloat<f64>, Bytes), &())| (m.clone(), s.into_inner());
        match &self.by {
            ZRangeBy::Rank(start, stop) => {
                let len = zset.len() as i64;
                let s = normalize_index(*start, len);
                let e = normalize_index(*stop, len);
                if len == 0 || s > e || s >= len as usize {
                    return Vec::new();
                }
                let e = e.min(len as usize - 1);
                let take = e - s + 1;
                if self.rev {
                    zset.scores
                        .iter()
                        .rev()
                        .skip(s)
                        .take(take)
                        .map(pair)
                        .collect()
                } else {
                    zset.scores.iter().skip(s).take(take).map(pair).collect()
                }
            }
            ZRangeBy::Score(min, max) => {
                let lo = (OrderedFloat(min.value()), Bytes::new());
                let range = zset.scores.range(lo..);
                if self.rev {
                    // Start at the top of the range: one-sided, so no
                    // start > end panic whatever the bounds.
                    use std::ops::Bound;
                    let hi = match *max {
                        ScoreBound::Incl(v) if v == f64::INFINITY => Bound::Unbounded,
                        ScoreBound::Incl(v) => {
                            Bound::Excluded((OrderedFloat(v.next_up()), Bytes::new()))
                        }
                        ScoreBound::Excl(v) => Bound::Excluded((OrderedFloat(v), Bytes::new())),
                    };
                    zset.scores
                        .range((Bound::Unbounded, hi))
                        .rev()
                        .skip_while(|((s, _), _)| !max.below_max(s.into_inner()))
                        .take_while(|((s, _), _)| min.above_min(s.into_inner()))
                        .skip(offset)
                        .take(count)
                        .map(pair)
                        .collect()
                } else {
                    range
                        .skip_while(|((s, _), _)| !min.above_min(s.into_inner()))
                        .take_while(|((s, _), _)| max.below_max(s.into_inner()))
                        .skip(offset)
                        .take(count)
                        .map(pair)
                        .collect()
                }
            }
            ZRangeBy::Lex(min, max) => {
                let inside = |((_, m), _): &(&(OrderedFloat<f64>, Bytes), &())| {
                    min.above_min(m) && max.below_max(m)
                };
                if self.rev {
                    zset.scores
                        .iter()
                        .rev()
                        .filter(inside)
                        .skip(offset)
                        .take(count)
                        .map(pair)
                        .collect()
                } else {
                    zset.scores
                        .iter()
                        .filter(inside)
                        .skip(offset)
                        .take(count)
                        .map(pair)
                        .collect()
                }
            }
        }
    }
}

/// Reply for a range: members, or member/score pairs `WITHSCORES`.
pub fn zrange_reply(items: Vec<(Bytes, f64)>, withscores: bool) -> CommandResponse {
    format_zrange_result(items.into_iter().map(|(m, s)| (s, m)).collect(), withscores)
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
        for (m, s) in members {
            z.insert(s, m);
        }
        let n = z.len() as i64;
        self.set(dest.clone(), RedisValue::ZSet(z), None);
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

fn normalize_index(idx: i64, len: i64) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx as usize
    }
}

fn format_score(score: f64) -> String {
    if score == score.trunc() && score.abs() < 1e15 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}

fn format_zrange_result(items: Vec<(f64, Bytes)>, withscores: bool) -> CommandResponse {
    let mut result = Vec::new();
    for (score, member) in items {
        result.push(CommandResponse::bulk(member));
        if withscores {
            result.push(CommandResponse::bulk(Bytes::from(format_score(score))));
        }
    }
    CommandResponse::array(result)
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
