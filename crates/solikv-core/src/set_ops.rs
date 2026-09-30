use bytes::Bytes;

use crate::store::ShardStore;
use crate::types::*;

/// Positions a SCAN-family call visits, walking down from `cursor` (`0` =
/// from the end): the range, and the cursor to return (`0` = done). Walking
/// down means a removal — which moves the last element into the hole —
/// never carries an unvisited element past the walk (see
/// `ShardStore::scan`).
pub(crate) fn scan_window(
    len: usize,
    cursor: usize,
    count: usize,
) -> (std::ops::Range<usize>, usize) {
    let hi = if cursor == 0 { len } else { cursor.min(len) };
    let lo = hi.saturating_sub(count.max(1));
    (lo..hi, lo)
}

pub(crate) fn scan_reply(next: usize, items: Vec<CommandResponse>) -> CommandResponse {
    CommandResponse::array(vec![
        CommandResponse::bulk(Bytes::from(next.to_string())),
        CommandResponse::array(items),
    ])
}

impl ShardStore {
    fn get_or_create_set(&mut self, key: &Bytes) -> Result<&mut SetValue, CommandResponse> {
        let exists = match self.get(key) {
            Some(entry) if !matches!(entry.value, RedisValue::Set(_)) => {
                return Err(CommandResponse::wrong_type())
            }
            Some(_) => true,
            None => false,
        };
        if !exists {
            self.set(key.clone(), RedisValue::new_set(), None);
        }
        match self.get_mut(key) {
            Some(KeyEntry {
                value: RedisValue::Set(s),
                ..
            }) => Ok(s),
            _ => Err(CommandResponse::wrong_type()),
        }
    }

    fn get_set(&mut self, key: &Bytes) -> Result<Option<&SetValue>, CommandResponse> {
        match self.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::Set(s) => Ok(Some(s)),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    /// The sets at `keys`, borrowed together (`None` for a missing key):
    /// lazy expiry and type checks first, then shared borrows — no copies.
    fn sets_of(&mut self, keys: &[Bytes]) -> Result<Vec<Option<&SetValue>>, CommandResponse> {
        for k in keys {
            self.get_set(k)?;
        }
        Ok(keys
            .iter()
            .map(|k| match self.peek(k) {
                Some(KeyEntry {
                    value: RedisValue::Set(s),
                    ..
                }) => Some(&**s),
                _ => None,
            })
            .collect())
    }

    /// WRONGTYPE unless `key` is a set or missing.
    pub fn set_check(&mut self, key: &Bytes) -> Result<(), CommandResponse> {
        self.get_set(key).map(|_| ())
    }

    /// `SINTER`/`SUNION`/`SDIFF` of sets on this shard, as an owned set
    /// (only the result's members are copied — reference-counted `Bytes`).
    pub fn set_op(&mut self, op: SetOp, keys: &[Bytes]) -> Result<SetValue, CommandResponse> {
        let sets = self.sets_of(keys)?;
        Ok(set_op_members(op, &sets))
    }

    /// A copy of the set at `key` (`None` when missing), for operations that
    /// combine sets living on several shards.
    pub fn set_clone(&mut self, key: &Bytes) -> Result<Option<SetValue>, CommandResponse> {
        self.get_set(key).map(|s| s.cloned())
    }

    /// Replace `dest` with `members` (deleting it when empty); returns the
    /// cardinality (`S*STORE`).
    pub fn set_store(&mut self, dest: &Bytes, members: SetValue) -> i64 {
        let n = members.len() as i64;
        if members.is_empty() {
            self.del(dest);
        } else {
            self.set(dest.clone(), RedisValue::set(members), None);
        }
        n
    }

    pub fn set_sadd(&mut self, key: &Bytes, members: Vec<Bytes>) -> CommandResponse {
        let set = match self.get_or_create_set(key) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let mut added = 0i64;
        for member in members {
            if set.insert(member) {
                added += 1;
            }
        }
        CommandResponse::integer(added)
    }

    pub fn set_srem(&mut self, key: &Bytes, members: &[Bytes]) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::integer(0),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::Set(set) => {
                let mut removed = 0i64;
                for member in members {
                    if set.swap_remove(member) {
                        removed += 1;
                    }
                }
                if set.is_empty() {
                    self.del(key);
                }
                CommandResponse::integer(removed)
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    pub fn set_smembers(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_set(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(set)) => {
                let items = set
                    .iter()
                    .map(|m| CommandResponse::bulk(m.clone()))
                    .collect();
                CommandResponse::array(items)
            }
        }
    }

    pub fn set_sismember(&mut self, key: &Bytes, member: &Bytes) -> CommandResponse {
        match self.get_set(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(set)) => CommandResponse::integer(if set.contains(member) { 1 } else { 0 }),
        }
    }

    pub fn set_scard(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_set(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(set)) => CommandResponse::integer(set.len() as i64),
        }
    }

    /// `SINTER`: walks the smallest set and probes the others; nothing is
    /// copied but the members returned.
    pub fn set_sinter(&mut self, keys: &[Bytes]) -> CommandResponse {
        if keys.is_empty() {
            return CommandResponse::error("ERR wrong number of arguments for 'sinter' command");
        }
        let sets = match self.sets_of(keys) {
            Ok(s) => s,
            Err(e) => return e,
        };
        CommandResponse::array(
            intersect(&sets)
                .into_iter()
                .map(|m| CommandResponse::bulk(m.clone()))
                .collect(),
        )
    }

    pub fn set_sunion(&mut self, keys: &[Bytes]) -> CommandResponse {
        if keys.is_empty() {
            return CommandResponse::error("ERR wrong number of arguments for 'sunion' command");
        }
        let sets = match self.sets_of(keys) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let mut result: indexmap::IndexSet<&Bytes, FastHasher> = Default::default();
        for s in sets.iter().flatten() {
            result.extend(s.iter());
        }
        CommandResponse::array(
            result
                .into_iter()
                .map(|m| CommandResponse::bulk(m.clone()))
                .collect(),
        )
    }

    pub fn set_sdiff(&mut self, keys: &[Bytes]) -> CommandResponse {
        if keys.is_empty() {
            return CommandResponse::error("ERR wrong number of arguments for 'sdiff' command");
        }
        let sets = match self.sets_of(keys) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let Some(first) = sets[0] else {
            return CommandResponse::array(vec![]);
        };
        let rest: Vec<&SetValue> = sets[1..].iter().flatten().copied().collect();
        CommandResponse::array(
            first
                .iter()
                .filter(|m| !rest.iter().any(|s| s.contains(*m)))
                .map(|m| CommandResponse::bulk(m.clone()))
                .collect(),
        )
    }

    /// `SPOP key [count]`: uniformly random members, each in O(1).
    pub fn set_spop(&mut self, key: &Bytes, count: usize) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => {
                return if count == 1 {
                    CommandResponse::nil()
                } else {
                    CommandResponse::array(vec![])
                }
            }
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::Set(set) => {
                let mut result = Vec::with_capacity(count.min(set.len()));
                for _ in 0..count {
                    if set.is_empty() {
                        break;
                    }
                    let i = rand_below(set.len());
                    if let Some(m) = set.swap_remove_index(i) {
                        result.push(CommandResponse::bulk(m));
                    }
                }
                if set.is_empty() {
                    self.del(key);
                }
                if count == 1 {
                    result.into_iter().next().unwrap_or(CommandResponse::nil())
                } else {
                    CommandResponse::array(result)
                }
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    /// `SRANDMEMBER key [count]`: a positive count returns distinct members,
    /// a negative one may repeat them (Redis semantics).
    pub fn set_srandmember(&mut self, key: &Bytes, count: Option<i64>) -> CommandResponse {
        match self.get_set(key) {
            Err(e) => e,
            Ok(None) => match count {
                None => CommandResponse::nil(),
                Some(_) => CommandResponse::array(vec![]),
            },
            Ok(Some(set)) => match count {
                None => match set.get_index(rand_below(set.len())) {
                    Some(m) => CommandResponse::bulk(m.clone()),
                    None => CommandResponse::nil(),
                },
                Some(c) if c < 0 => {
                    let n = c.unsigned_abs() as usize;
                    CommandResponse::array(
                        (0..n)
                            .filter_map(|_| set.get_index(rand_below(set.len())))
                            .map(|m| CommandResponse::bulk(m.clone()))
                            .collect(),
                    )
                }
                Some(c) => {
                    let n = (c as usize).min(set.len());
                    let picks = distinct_indices(set.len(), n);
                    CommandResponse::array(
                        picks
                            .into_iter()
                            .filter_map(|i| set.get_index(i))
                            .map(|m| CommandResponse::bulk(m.clone()))
                            .collect(),
                    )
                }
            },
        }
    }

    /// `SSCAN key cursor [MATCH pattern] [COUNT count]`.
    pub fn set_sscan(
        &mut self,
        key: &Bytes,
        cursor: usize,
        pattern: Option<&str>,
        count: usize,
    ) -> CommandResponse {
        match self.get_set(key) {
            Err(e) => e,
            Ok(None) => scan_reply(0, vec![]),
            Ok(Some(set)) => {
                let (window, next) = scan_window(set.len(), cursor, count);
                let items = window
                    .rev()
                    .filter_map(|i| set.get_index(i))
                    .filter(|m| {
                        pattern.is_none_or(|p| {
                            crate::store::glob_match_pub(p, &String::from_utf8_lossy(m))
                        })
                    })
                    .map(|m| CommandResponse::bulk(m.clone()))
                    .collect();
                scan_reply(next, items)
            }
        }
    }

    pub fn set_smove(&mut self, src: &Bytes, dst: Bytes, member: Bytes) -> CommandResponse {
        // Both types first: a wrong-typed destination must not cost the
        // source its member.
        if let Err(e) = self.get_set(&dst) {
            return e;
        }
        if src == &dst {
            return match self.get_set(src) {
                Err(e) => e,
                Ok(s) => CommandResponse::integer(s.is_some_and(|s| s.contains(&member)) as i64),
            };
        }
        // Remove from source
        let entry = match self.get_mut(src) {
            None => return CommandResponse::integer(0),
            Some(e) => e,
        };
        let removed = match &mut entry.value {
            RedisValue::Set(set) => set.swap_remove(&member),
            _ => return CommandResponse::wrong_type(),
        };
        if !removed {
            return CommandResponse::integer(0);
        }
        // Clean up empty source
        if let Some(entry) = self.get(src) {
            if matches!(&entry.value, RedisValue::Set(s) if s.is_empty()) {
                self.del(src);
            }
        }
        // Add to destination
        let dest_set = match self.get_or_create_set(&dst) {
            Ok(s) => s,
            Err(e) => return e,
        };
        dest_set.insert(member);
        CommandResponse::integer(1)
    }
}

/// `SINTER`/`SUNION`/`SDIFF`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Inter,
    Union,
    Diff,
}

impl SetOp {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "SINTER" => Some(SetOp::Inter),
            "SUNION" => Some(SetOp::Union),
            "SDIFF" => Some(SetOp::Diff),
            _ => None,
        }
    }
}

/// The result of `op` over `sets` (`None` = missing key = empty set).
pub fn set_op_members(op: SetOp, sets: &[Option<&SetValue>]) -> SetValue {
    match op {
        SetOp::Inter => intersect(sets).into_iter().cloned().collect(),
        SetOp::Union => {
            let mut out = SetValue::default();
            for s in sets.iter().flatten() {
                out.extend(s.iter().cloned());
            }
            out
        }
        SetOp::Diff => match sets.first().copied().flatten() {
            None => SetValue::default(),
            Some(first) => {
                let rest: Vec<&SetValue> = sets[1..].iter().flatten().copied().collect();
                first
                    .iter()
                    .filter(|m| !rest.iter().any(|s| s.contains(*m)))
                    .cloned()
                    .collect()
            }
        },
    }
}

/// Members in every set (`None` = a missing key = the empty set), walking
/// the smallest.
pub fn intersect<'a>(sets: &[Option<&'a SetValue>]) -> Vec<&'a Bytes> {
    if sets.iter().any(|s| s.is_none()) {
        return Vec::new();
    }
    let mut sets: Vec<&SetValue> = sets.iter().flatten().copied().collect();
    sets.sort_by_key(|s| s.len());
    let Some((first, rest)) = sets.split_first() else {
        return Vec::new();
    };
    first
        .iter()
        .filter(|m| rest.iter().all(|s| s.contains(*m)))
        .collect()
}

thread_local! {
    static RNG: std::cell::Cell<u64> = std::cell::Cell::new(seed_rng());
}

fn seed_rng() -> u64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9);
    let local = 0u8;
    (t ^ (&local as *const u8 as u64).rotate_left(32)) | 1
}

/// Uniform in `0..n` (`n > 0`), from a per-thread xorshift.
pub(crate) fn rand_below(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let x = RNG.with(|c| {
        let mut x = c.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        c.set(x);
        x
    });
    ((x as u128 * n as u128) >> 64) as usize
}

/// `k` distinct positions in `0..n` (`k <= n`), in random order.
pub(crate) fn distinct_indices(n: usize, k: usize) -> Vec<usize> {
    if k * 2 >= n {
        // Most of them: shuffle all, take k.
        let mut all: Vec<usize> = (0..n).collect();
        for i in (1..n).rev() {
            all.swap(i, rand_below(i + 1));
        }
        all.truncate(k);
        return all;
    }
    let mut picked = std::collections::HashSet::with_capacity(k);
    let mut out = Vec::with_capacity(k);
    while out.len() < k {
        let i = rand_below(n);
        if picked.insert(i) {
            out.push(i);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sadd_scard() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        let r = store.set_sadd(
            &key,
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("a")],
        );
        assert!(matches!(r, CommandResponse::Integer(2)));
        assert!(matches!(store.set_scard(&key), CommandResponse::Integer(2)));
    }

    #[test]
    fn test_srem() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(&key, vec![Bytes::from("a"), Bytes::from("b")]);
        let r = store.set_srem(&key, &[Bytes::from("a")]);
        assert!(matches!(r, CommandResponse::Integer(1)));
        assert!(matches!(store.set_scard(&key), CommandResponse::Integer(1)));
    }

    #[test]
    fn test_sismember() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(&key, vec![Bytes::from("a")]);
        assert!(matches!(
            store.set_sismember(&key, &Bytes::from("a")),
            CommandResponse::Integer(1)
        ));
        assert!(matches!(
            store.set_sismember(&key, &Bytes::from("b")),
            CommandResponse::Integer(0)
        ));
    }

    #[test]
    fn test_smembers() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(&key, vec![Bytes::from("a"), Bytes::from("b")]);
        match store.set_smembers(&key) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sinter() {
        let mut store = ShardStore::new();
        store.set_sadd(
            &Bytes::from("s1"),
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        );
        store.set_sadd(
            &Bytes::from("s2"),
            vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("d")],
        );
        match store.set_sinter(&[Bytes::from("s1"), Bytes::from("s2")]) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sunion() {
        let mut store = ShardStore::new();
        store.set_sadd(&Bytes::from("s1"), vec![Bytes::from("a"), Bytes::from("b")]);
        store.set_sadd(&Bytes::from("s2"), vec![Bytes::from("b"), Bytes::from("c")]);
        match store.set_sunion(&[Bytes::from("s1"), Bytes::from("s2")]) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 3),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sdiff() {
        let mut store = ShardStore::new();
        store.set_sadd(
            &Bytes::from("s1"),
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        );
        store.set_sadd(&Bytes::from("s2"), vec![Bytes::from("b"), Bytes::from("c")]);
        match store.set_sdiff(&[Bytes::from("s1"), Bytes::from("s2")]) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 1);
                assert!(
                    matches!(&items[0], CommandResponse::BulkString(v) if v == &Bytes::from("a"))
                );
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_spop() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(&key, vec![Bytes::from("a"), Bytes::from("b")]);
        let r = store.set_spop(&key, 1);
        assert!(matches!(r, CommandResponse::BulkString(_)));
        assert!(matches!(store.set_scard(&key), CommandResponse::Integer(1)));
    }

    #[test]
    fn test_smove() {
        let mut store = ShardStore::new();
        store.set_sadd(&Bytes::from("s1"), vec![Bytes::from("a"), Bytes::from("b")]);
        store.set_sadd(&Bytes::from("s2"), vec![Bytes::from("c")]);
        let r = store.set_smove(&Bytes::from("s1"), Bytes::from("s2"), Bytes::from("a"));
        assert!(matches!(r, CommandResponse::Integer(1)));
        assert!(matches!(
            store.set_scard(&Bytes::from("s1")),
            CommandResponse::Integer(1)
        ));
        assert!(matches!(
            store.set_scard(&Bytes::from("s2")),
            CommandResponse::Integer(2)
        ));
    }

    #[test]
    fn test_auto_delete_empty_set() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(&key, vec![Bytes::from("a")]);
        store.set_srem(&key, &[Bytes::from("a")]);
        assert!(!store.exists(&key));
    }

    #[test]
    fn spop_drains_every_member_once() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        let members: Vec<Bytes> = (0..1000).map(|i| Bytes::from(format!("m{i}"))).collect();
        store.set_sadd(&key, members.clone());
        let mut got = std::collections::HashSet::new();
        let mut first_pops = Vec::new();
        loop {
            match store.set_spop(&key, 1) {
                CommandResponse::BulkString(m) => {
                    if first_pops.len() < 10 {
                        first_pops.push(m.clone());
                    }
                    assert!(got.insert(m));
                }
                CommandResponse::Nil => break,
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(got.len(), 1000);
        assert!(!store.exists(&key));
        // Not simply insertion order.
        assert_ne!(first_pops, members[..10].to_vec());
    }

    #[test]
    fn srandmember_counts() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(
            &key,
            (0..10).map(|i| Bytes::from(format!("m{i}"))).collect(),
        );
        match store.set_srandmember(&key, Some(5)) {
            CommandResponse::Array(items) => {
                let distinct: std::collections::HashSet<_> =
                    items.iter().map(|i| format!("{i:?}")).collect();
                assert_eq!(distinct.len(), 5);
            }
            other => panic!("{other:?}"),
        }
        match store.set_srandmember(&key, Some(50)) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 10),
            other => panic!("{other:?}"),
        }
        match store.set_srandmember(&key, Some(-30)) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 30),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn sscan_visits_every_member() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(
            &key,
            (0..333).map(|i| Bytes::from(format!("m{i}"))).collect(),
        );
        let mut cursor = 0;
        let mut seen = std::collections::HashSet::new();
        loop {
            let CommandResponse::Array(parts) = store.set_sscan(&key, cursor, None, 10) else {
                panic!()
            };
            let CommandResponse::BulkString(c) = &parts[0] else {
                panic!()
            };
            cursor = std::str::from_utf8(c).unwrap().parse().unwrap();
            if let CommandResponse::Array(items) = &parts[1] {
                for i in items {
                    seen.insert(format!("{i:?}"));
                }
            }
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen.len(), 333);
    }
}
