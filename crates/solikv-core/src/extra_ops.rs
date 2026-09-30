//! Read-mostly commands added for Redis 7 parity that need nothing but a
//! look at one value: `ZMSCORE`, `ZRANDMEMBER`, `SMISMEMBER`, `HSTRLEN`,
//! `HRANDFIELD`.

use bytes::Bytes;

use crate::set_ops::{distinct_indices, rand_below};
use crate::store::ShardStore;
use crate::types::*;
use crate::zset_ops::format_score;

/// Random picks from a collection of `len` items for the `*RANDMEMBER`
/// family: `count > 0` gives distinct positions (at most `len`), `count < 0`
/// gives `|count|` positions that may repeat.
fn random_positions(len: usize, count: i64) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    if count < 0 {
        let n = count.unsigned_abs() as usize;
        (0..n).map(|_| rand_below(len)).collect()
    } else {
        distinct_indices(len, (count as usize).min(len))
    }
}

impl ShardStore {
    fn peek_value(&mut self, key: &Bytes) -> Option<&RedisValue> {
        self.get(key).map(|e| &e.value)
    }

    /// `ZMSCORE key member…`
    pub fn zset_zmscore(&mut self, key: &Bytes, members: &[Bytes]) -> CommandResponse {
        let z = match self.peek_value(key) {
            None => None,
            Some(RedisValue::ZSet(z)) => Some(z),
            Some(_) => return CommandResponse::wrong_type(),
        };
        CommandResponse::array(
            members
                .iter()
                .map(|m| match z.and_then(|z| z.score(m)) {
                    Some(s) => CommandResponse::bulk(Bytes::from(format_score(s))),
                    None => CommandResponse::Nil,
                })
                .collect(),
        )
    }

    /// `ZRANDMEMBER key [count [WITHSCORES]]`
    pub fn zset_zrandmember(
        &mut self,
        key: &Bytes,
        count: Option<i64>,
        withscores: bool,
    ) -> CommandResponse {
        let z = match self.peek_value(key) {
            None => {
                return match count {
                    None => CommandResponse::Nil,
                    Some(_) => CommandResponse::array(vec![]),
                }
            }
            Some(RedisValue::ZSet(z)) => z,
            Some(_) => return CommandResponse::wrong_type(),
        };
        let Some(count) = count else {
            return match z.members.get_index(rand_below(z.len())) {
                Some((m, _)) => CommandResponse::bulk(m.clone()),
                None => CommandResponse::Nil,
            };
        };
        let mut out = Vec::new();
        for i in random_positions(z.len(), count) {
            if let Some((m, s)) = z.members.get_index(i) {
                out.push(CommandResponse::bulk(m.clone()));
                if withscores {
                    out.push(CommandResponse::bulk(Bytes::from(format_score(
                        s.into_inner(),
                    ))));
                }
            }
        }
        CommandResponse::array(out)
    }

    /// `SMISMEMBER key member…`
    pub fn set_smismember(&mut self, key: &Bytes, members: &[Bytes]) -> CommandResponse {
        let s = match self.peek_value(key) {
            None => None,
            Some(RedisValue::Set(s)) => Some(s),
            Some(_) => return CommandResponse::wrong_type(),
        };
        CommandResponse::array(
            members
                .iter()
                .map(|m| CommandResponse::integer(s.is_some_and(|s| s.contains(m)) as i64))
                .collect(),
        )
    }

    /// `HSTRLEN key field`
    pub fn hash_hstrlen(&mut self, key: &Bytes, field: &Bytes) -> CommandResponse {
        match self.peek_value(key) {
            None => CommandResponse::integer(0),
            Some(RedisValue::Hash(h)) => {
                CommandResponse::integer(h.get(field).map_or(0, |v| v.len()) as i64)
            }
            Some(_) => CommandResponse::wrong_type(),
        }
    }

    /// `HRANDFIELD key [count [WITHVALUES]]`
    pub fn hash_hrandfield(
        &mut self,
        key: &Bytes,
        count: Option<i64>,
        withvalues: bool,
    ) -> CommandResponse {
        let h = match self.peek_value(key) {
            None => {
                return match count {
                    None => CommandResponse::Nil,
                    Some(_) => CommandResponse::array(vec![]),
                }
            }
            Some(RedisValue::Hash(h)) => h,
            Some(_) => return CommandResponse::wrong_type(),
        };
        let Some(count) = count else {
            return match h.get_index(rand_below(h.len())) {
                Some((f, _)) => CommandResponse::bulk(f.clone()),
                None => CommandResponse::Nil,
            };
        };
        let mut out = Vec::new();
        for i in random_positions(h.len(), count) {
            if let Some((f, v)) = h.get_index(i) {
                out.push(CommandResponse::bulk(f.clone()));
                if withvalues {
                    out.push(CommandResponse::bulk(v.clone()));
                }
            }
        }
        CommandResponse::array(out)
    }
}

/// What `OBJECT ENCODING` reports for a value, by Redis 7's thresholds
/// (`*-max-listpack-entries` 128, `-value` 64, `set-max-intset-entries`
/// 512, embstr up to 44 bytes). Values are stored one way here; this is the
/// encoding Redis would pick, which is what clients inspecting it expect.
pub fn object_encoding(value: &RedisValue) -> &'static str {
    let small = |n: usize, longest: usize| n <= 128 && longest <= 64;
    match value {
        RedisValue::String(s) => {
            let is_int = std::str::from_utf8(s)
                .ok()
                .and_then(|t| t.parse::<i64>().ok())
                .is_some_and(|n| n.to_string().as_bytes() == &s[..]);
            if is_int {
                "int"
            } else if s.len() <= 44 {
                "embstr"
            } else {
                "raw"
            }
        }
        RedisValue::List(l) => {
            if small(l.len(), l.iter().map(|e| e.len()).max().unwrap_or(0)) {
                "listpack"
            } else {
                "quicklist"
            }
        }
        RedisValue::Set(s) => {
            let ints = s.iter().all(|m| {
                std::str::from_utf8(m)
                    .ok()
                    .and_then(|t| t.parse::<i64>().ok())
                    .is_some_and(|n| n.to_string().as_bytes() == &m[..])
            });
            if ints && s.len() <= 512 {
                "intset"
            } else if small(s.len(), s.iter().map(|e| e.len()).max().unwrap_or(0)) {
                "listpack"
            } else {
                "hashtable"
            }
        }
        RedisValue::Hash(h) => {
            let longest = h
                .iter()
                .map(|(f, v)| f.len().max(v.len()))
                .max()
                .unwrap_or(0);
            if small(h.len(), longest) {
                "listpack"
            } else {
                "hashtable"
            }
        }
        RedisValue::ZSet(z) => {
            if small(
                z.len(),
                z.members.keys().map(|m| m.len()).max().unwrap_or(0),
            ) {
                "listpack"
            } else {
                "skiplist"
            }
        }
        RedisValue::Stream(_) => "stream",
        // A HyperLogLog is a string in Redis.
        RedisValue::HyperLogLog(_) => "raw",
        RedisValue::BloomFilter(_) => "raw",
    }
}

impl ShardStore {
    /// `OBJECT ENCODING key`.
    pub fn object_encoding_of(&mut self, key: &Bytes) -> CommandResponse {
        match self.peek_value(key) {
            None => CommandResponse::Nil,
            Some(v) => CommandResponse::bulk_string(object_encoding(v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn random_members_follow_the_count_rules() {
        let mut st = ShardStore::new();
        st.zset_zadd(
            &b("z"),
            vec![(1.0, b("a")), (2.0, b("b")), (3.0, b("c"))],
            false,
            false,
            false,
            false,
            false,
        );
        let CommandResponse::Array(v) = st.zset_zrandmember(&b("z"), Some(10), false) else {
            panic!()
        };
        assert_eq!(v.len(), 3);
        let CommandResponse::Array(v) = st.zset_zrandmember(&b("z"), Some(-10), true) else {
            panic!()
        };
        assert_eq!(v.len(), 20);
        assert!(matches!(
            st.zset_zrandmember(&b("none"), None, false),
            CommandResponse::Nil
        ));
        let CommandResponse::Array(v) = st.zset_zmscore(&b("z"), &[b("b"), b("x")]) else {
            panic!()
        };
        assert!(matches!(&v[0], CommandResponse::BulkString(s) if s == "2"));
        assert!(matches!(v[1], CommandResponse::Nil));
    }
}
