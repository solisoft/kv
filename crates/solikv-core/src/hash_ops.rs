use bytes::Bytes;

use crate::set_ops::{scan_reply, scan_window};
use crate::store::ShardStore;
use crate::types::*;

impl ShardStore {
    fn get_or_create_hash(&mut self, key: &Bytes) -> Result<&mut HashValue, CommandResponse> {
        self.purge_hash_fields_in(self.db(), key);
        let exists = match self.get(key) {
            Some(entry) if !matches!(entry.value, RedisValue::Hash(_)) => {
                return Err(CommandResponse::wrong_type())
            }
            Some(_) => true,
            None => false,
        };
        if !exists {
            self.set(key.clone(), RedisValue::new_hash(), None);
        }
        match self.get_mut(key) {
            Some(KeyEntry {
                value: RedisValue::Hash(h),
                ..
            }) => Ok(h),
            _ => Err(CommandResponse::wrong_type()),
        }
    }

    fn get_hash(&mut self, key: &Bytes) -> Result<Option<&HashValue>, CommandResponse> {
        self.purge_hash_fields_in(self.db(), key);
        match self.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::Hash(h) => Ok(Some(h)),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    pub fn hash_hset(&mut self, key: &Bytes, pairs: Vec<(Bytes, Bytes)>) -> CommandResponse {
        let hash = match self.get_or_create_hash(key) {
            Ok(h) => h,
            Err(e) => return e,
        };
        let mut new_fields = 0i64;
        for (field, value) in pairs {
            if hash.insert(field, value).is_none() {
                new_fields += 1;
            }
        }
        CommandResponse::integer(new_fields)
    }

    pub fn hash_hget(&mut self, key: &Bytes, field: &Bytes) -> CommandResponse {
        match self.get_hash(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::nil(),
            Ok(Some(h)) => match h.get(field) {
                Some(v) => CommandResponse::bulk(v.clone()),
                None => CommandResponse::nil(),
            },
        }
    }

    pub fn hash_hdel(&mut self, key: &Bytes, fields: &[Bytes]) -> CommandResponse {
        self.purge_hash_fields_in(self.db(), key);
        let entry = match self.get_mut(key) {
            None => return CommandResponse::integer(0),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::Hash(h) => {
                let mut removed = 0i64;
                for field in fields {
                    if h.swap_remove(field).is_some() {
                        removed += 1;
                    }
                }
                if h.is_empty() {
                    self.del(key);
                }
                CommandResponse::integer(removed)
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    pub fn hash_hgetall(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_hash(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(h)) => {
                let mut items = Vec::with_capacity(h.len() * 2);
                for (k, v) in h {
                    items.push(CommandResponse::bulk(k.clone()));
                    items.push(CommandResponse::bulk(v.clone()));
                }
                CommandResponse::array(items)
            }
        }
    }

    pub fn hash_hmget(&mut self, key: &Bytes, fields: &[Bytes]) -> CommandResponse {
        match self.get_hash(key) {
            Err(e) => e,
            Ok(None) => {
                let items = fields.iter().map(|_| CommandResponse::nil()).collect();
                CommandResponse::array(items)
            }
            Ok(Some(h)) => {
                let items = fields
                    .iter()
                    .map(|f| match h.get(f) {
                        Some(v) => CommandResponse::bulk(v.clone()),
                        None => CommandResponse::nil(),
                    })
                    .collect();
                CommandResponse::array(items)
            }
        }
    }

    pub fn hash_hincrby(&mut self, key: &Bytes, field: Bytes, delta: i64) -> CommandResponse {
        let hash = match self.get_or_create_hash(key) {
            Ok(h) => h,
            Err(e) => return e,
        };
        let current = match hash.get(&field) {
            None => 0i64,
            Some(v) => match std::str::from_utf8(v) {
                Ok(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return CommandResponse::error("ERR hash value is not an integer"),
                },
                Err(_) => return CommandResponse::error("ERR hash value is not an integer"),
            },
        };
        let Some(new_val) = current.checked_add(delta) else {
            return CommandResponse::error("ERR increment or decrement would overflow");
        };
        hash.insert(field, Bytes::from(new_val.to_string()));
        CommandResponse::integer(new_val)
    }

    pub fn hash_hincrbyfloat(&mut self, key: &Bytes, field: Bytes, delta: f64) -> CommandResponse {
        let hash = match self.get_or_create_hash(key) {
            Ok(h) => h,
            Err(e) => return e,
        };
        let current = match hash.get(&field) {
            None => 0.0f64,
            Some(v) => match std::str::from_utf8(v) {
                Ok(s) => match s.parse::<f64>() {
                    Ok(n) => n,
                    Err(_) => return CommandResponse::error("ERR hash value is not a valid float"),
                },
                Err(_) => return CommandResponse::error("ERR hash value is not a valid float"),
            },
        };
        let new_val = current + delta;
        if !new_val.is_finite() {
            return CommandResponse::error("ERR increment would produce NaN or Infinity");
        }
        let formatted = format!("{}", new_val);
        hash.insert(field, Bytes::from(formatted.clone()));
        CommandResponse::bulk(Bytes::from(formatted))
    }

    pub fn hash_hkeys(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_hash(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(h)) => {
                let items = h.keys().map(|k| CommandResponse::bulk(k.clone())).collect();
                CommandResponse::array(items)
            }
        }
    }

    pub fn hash_hvals(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_hash(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(h)) => {
                let items = h
                    .values()
                    .map(|v| CommandResponse::bulk(v.clone()))
                    .collect();
                CommandResponse::array(items)
            }
        }
    }

    pub fn hash_hlen(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_hash(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(h)) => CommandResponse::integer(h.len() as i64),
        }
    }

    pub fn hash_hexists(&mut self, key: &Bytes, field: &Bytes) -> CommandResponse {
        match self.get_hash(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(h)) => CommandResponse::integer(if h.contains_key(field) { 1 } else { 0 }),
        }
    }

    pub fn hash_hsetnx(&mut self, key: &Bytes, field: Bytes, value: Bytes) -> CommandResponse {
        let hash = match self.get_or_create_hash(key) {
            Ok(h) => h,
            Err(e) => return e,
        };
        CommandResponse::integer(hash.insert_if_absent(field, value) as i64)
    }

    /// `HEXPIRE`-family core: give each of `fields` the expiry `at` (unix
    /// ms) where `cond` allows, as Redis 7.4 does: per field `-2` (no such
    /// field or key), `0` (condition not met), `1` (set) or `2` (deleted:
    /// `at` is already past). For GT/LT a field without an expiry counts as
    /// an infinite one.
    pub fn hash_field_expire(
        &mut self,
        key: &Bytes,
        at: u64,
        cond: crate::store::ExpireCond,
        fields: &[Bytes],
    ) -> CommandResponse {
        self.purge_hash_fields_in(self.db(), key);
        let Some(idx) = self.index_of_live(key) else {
            return CommandResponse::array(
                fields
                    .iter()
                    .map(|_| CommandResponse::integer(-2))
                    .collect(),
            );
        };
        if !matches!(self.entry_value(idx), RedisValue::Hash(_)) {
            return CommandResponse::wrong_type();
        }
        self.capture_index(idx);
        let now = now_millis();
        let mut out = Vec::with_capacity(fields.len());
        let mut deleted = Vec::new();
        let mut set_any = false;
        {
            let RedisValue::Hash(h) = self.entry_value_mut(idx) else {
                unreachable!()
            };
            for f in fields {
                if !h.contains_key(f) {
                    out.push(CommandResponse::integer(-2));
                    continue;
                }
                let current = h.field_ttl(f);
                let refused = (cond.nx && current.is_some())
                    || (cond.xx && current.is_none())
                    || (cond.gt && current.is_none_or(|cur| at <= cur))
                    || (cond.lt && current.is_some_and(|cur| at >= cur));
                if refused {
                    out.push(CommandResponse::integer(0));
                } else if at <= now && !is_loading() {
                    h.swap_remove(f);
                    deleted.push(f.clone());
                    out.push(CommandResponse::integer(2));
                } else {
                    h.set_field_ttl(f, at);
                    set_any = true;
                    out.push(CommandResponse::integer(1));
                }
            }
        }
        if set_any {
            self.note_field_ttl(key);
        }
        let empty = matches!(self.entry_value(idx), RedisValue::Hash(h) if h.is_empty());
        if empty {
            self.del(key);
        }
        CommandResponse::array(out)
    }

    /// `HTTL`/`HPTTL`/`HEXPIRETIME`/`HPEXPIRETIME`: per field `-2` (no such
    /// field or key), `-1` (no expiry), else the time left (`absolute`:
    /// the deadline), in ms or, rounded up, seconds.
    pub fn hash_field_ttl(
        &mut self,
        key: &Bytes,
        fields: &[Bytes],
        millis: bool,
        absolute: bool,
    ) -> CommandResponse {
        let h = match self.get_hash(key) {
            Err(e) => return e,
            Ok(None) => {
                return CommandResponse::array(
                    fields
                        .iter()
                        .map(|_| CommandResponse::integer(-2))
                        .collect(),
                )
            }
            Ok(Some(h)) => h,
        };
        let now = now_millis();
        let out = fields
            .iter()
            .map(|f| {
                if !h.contains_key(f) {
                    return CommandResponse::integer(-2);
                }
                let Some(at) = h.field_ttl(f) else {
                    return CommandResponse::integer(-1);
                };
                let v = match (absolute, millis) {
                    (true, true) => at as i64,
                    (true, false) => (at / 1000) as i64,
                    (false, true) => at.saturating_sub(now) as i64,
                    (false, false) => (at.saturating_sub(now) as i64 + 999) / 1000,
                };
                CommandResponse::integer(v)
            })
            .collect();
        CommandResponse::array(out)
    }

    /// `HPERSIST`: per field `-2` (no such field or key), `-1` (it had no
    /// expiry) or `1` (expiry removed).
    pub fn hash_field_persist(&mut self, key: &Bytes, fields: &[Bytes]) -> CommandResponse {
        self.purge_hash_fields_in(self.db(), key);
        let Some(idx) = self.index_of_live(key) else {
            return CommandResponse::array(
                fields
                    .iter()
                    .map(|_| CommandResponse::integer(-2))
                    .collect(),
            );
        };
        if !matches!(self.entry_value(idx), RedisValue::Hash(_)) {
            return CommandResponse::wrong_type();
        }
        self.capture_index(idx);
        let RedisValue::Hash(h) = self.entry_value_mut(idx) else {
            unreachable!()
        };
        let out = fields
            .iter()
            .map(|f| {
                if !h.contains_key(f) {
                    CommandResponse::integer(-2)
                } else if h.persist_field(f) {
                    CommandResponse::integer(1)
                } else {
                    CommandResponse::integer(-1)
                }
            })
            .collect();
        CommandResponse::array(out)
    }

    /// `HGETDEL key FIELDS n field…` (Redis 8): each field's value (nil if
    /// absent), the fields removed — and the key, once empty. Returns the
    /// fields removed, for the log.
    pub fn hash_hgetdel(&mut self, key: &Bytes, fields: &[Bytes]) -> (CommandResponse, Vec<Bytes>) {
        let nils =
            || CommandResponse::array(fields.iter().map(|_| CommandResponse::nil()).collect());
        self.purge_hash_fields_in(self.db(), key);
        let Some(idx) = self.index_of_live(key) else {
            return (nils(), Vec::new());
        };
        if !matches!(self.entry_value(idx), RedisValue::Hash(_)) {
            return (CommandResponse::wrong_type(), Vec::new());
        }
        self.capture_index(idx);
        let mut deleted = Vec::new();
        let RedisValue::Hash(h) = self.entry_value_mut(idx) else {
            unreachable!()
        };
        let out = fields
            .iter()
            .map(|f| match h.swap_remove(f) {
                Some(v) => {
                    deleted.push(f.clone());
                    CommandResponse::bulk(v)
                }
                None => CommandResponse::nil(),
            })
            .collect();
        let empty = h.is_empty();
        if empty {
            self.del(key);
        }
        (CommandResponse::array(out), deleted)
    }

    /// `HGETEX key [EX|PX|EXAT|PXAT t | PERSIST] FIELDS n field…` (Redis 8):
    /// each field's value, then its expiry changed — `At` a deadline already
    /// past removes the fields. Returns what changed, for the log.
    pub fn hash_hgetex(
        &mut self,
        key: &Bytes,
        fields: &[Bytes],
        op: FieldExpiry,
    ) -> (CommandResponse, FieldChanges) {
        let mut changes = FieldChanges::default();
        let nils =
            || CommandResponse::array(fields.iter().map(|_| CommandResponse::nil()).collect());
        self.purge_hash_fields_in(self.db(), key);
        let Some(idx) = self.index_of_live(key) else {
            return (nils(), changes);
        };
        if !matches!(self.entry_value(idx), RedisValue::Hash(_)) {
            return (CommandResponse::wrong_type(), changes);
        }
        let values: Vec<CommandResponse> = {
            let RedisValue::Hash(h) = self.entry_value(idx) else {
                unreachable!()
            };
            fields
                .iter()
                .map(|f| match h.get(f) {
                    Some(v) => CommandResponse::bulk(v.clone()),
                    None => CommandResponse::nil(),
                })
                .collect()
        };
        if matches!(op, FieldExpiry::Keep) {
            return (CommandResponse::array(values), changes);
        }
        self.capture_index(idx);
        let now = now_millis();
        let mut ttl_set = false;
        {
            let RedisValue::Hash(h) = self.entry_value_mut(idx) else {
                unreachable!()
            };
            for f in fields {
                if !h.contains_key(f) {
                    continue;
                }
                match op {
                    FieldExpiry::Keep => {}
                    FieldExpiry::Persist => {
                        if h.persist_field(f) {
                            changes.persisted.push(f.clone());
                        }
                    }
                    FieldExpiry::At(at) if at <= now && !is_loading() => {
                        h.swap_remove(f);
                        changes.deleted.push(f.clone());
                    }
                    FieldExpiry::At(at) => {
                        h.set_field_ttl(f, at);
                        ttl_set = true;
                        changes
                            .expire_at
                            .get_or_insert((at, Vec::new()))
                            .1
                            .push(f.clone());
                    }
                }
            }
        }
        if ttl_set {
            self.note_field_ttl(key);
        }
        if matches!(self.entry_value(idx), RedisValue::Hash(h) if h.is_empty()) {
            self.del(key);
        }
        (CommandResponse::array(values), changes)
    }

    /// `HSETEX key [FNX|FXX] [EX|PX|EXAT|PXAT t | KEEPTTL] FIELDS n field
    /// value…` (Redis 8): `1` once every field is set, `0` when `FNX` (one
    /// already exists) or `FXX` (one does not) refuses all of them. Without
    /// an expiry option the fields lose theirs, as with `HSET`; `At` a
    /// deadline already past leaves them removed. Returns what changed.
    pub fn hash_hsetex(
        &mut self,
        key: &Bytes,
        pairs: Vec<(Bytes, Bytes)>,
        only: Option<bool>,
        ttl: SetExpiry,
    ) -> (CommandResponse, FieldChanges) {
        let mut changes = FieldChanges::default();
        let existing: Vec<bool> = match self.get_hash(key) {
            Err(e) => return (e, changes),
            Ok(None) => vec![false; pairs.len()],
            Ok(Some(h)) => pairs.iter().map(|(f, _)| h.contains_key(f)).collect(),
        };
        let refused = match only {
            Some(true) => existing.iter().any(|&e| e),
            Some(false) => existing.iter().any(|&e| !e),
            None => false,
        };
        if refused {
            return (CommandResponse::integer(0), changes);
        }
        let now = now_millis();
        let hash = match self.get_or_create_hash(key) {
            Ok(h) => h,
            Err(e) => return (e, changes),
        };
        let mut ttl_set = false;
        for (f, v) in pairs {
            let kept = match ttl {
                SetExpiry::Keep => hash.field_ttl(&f),
                _ => None,
            };
            // `insert` drops the field's expiry, as HSET does.
            hash.insert(f.clone(), v.clone());
            match ttl {
                SetExpiry::Discard => {}
                SetExpiry::Keep => {
                    if let Some(at) = kept {
                        hash.set_field_ttl(&f, at);
                        ttl_set = true;
                        changes.kept.push((f.clone(), at));
                    }
                }
                SetExpiry::At(at) if at <= now && !is_loading() => {
                    hash.swap_remove(&f);
                    changes.deleted.push(f.clone());
                }
                SetExpiry::At(at) => {
                    hash.set_field_ttl(&f, at);
                    ttl_set = true;
                    changes
                        .expire_at
                        .get_or_insert((at, Vec::new()))
                        .1
                        .push(f.clone());
                }
            }
            changes.set.push((f, v));
        }
        let empty = hash.is_empty();
        if ttl_set {
            self.note_field_ttl(key);
        }
        if empty {
            self.del(key);
        }
        (CommandResponse::integer(1), changes)
    }

    /// `HSCAN key cursor [MATCH pattern] [COUNT count]`: visits `count`
    /// fields by position, no copy of the hash.
    pub fn hash_hscan(
        &mut self,
        key: &Bytes,
        cursor: usize,
        pattern: Option<&str>,
        count: usize,
    ) -> CommandResponse {
        match self.get_hash(key) {
            Err(e) => e,
            Ok(None) => scan_reply(0, vec![]),
            Ok(Some(h)) => {
                let (window, next) = scan_window(h.len(), cursor, count);
                let mut items = Vec::with_capacity(window.len() * 2);
                for i in window.rev() {
                    let Some((field, value)) = h.get_index(i) else {
                        continue;
                    };
                    let matches = pattern.is_none_or(|p| {
                        crate::store::glob_match_pub(p, std::str::from_utf8(field).unwrap_or(""))
                    });
                    if matches {
                        items.push(CommandResponse::bulk(field.clone()));
                        items.push(CommandResponse::bulk(value.clone()));
                    }
                }
                scan_reply(next, items)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hset_hget() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hash");
        store.hash_hset(&key, vec![(Bytes::from("f1"), Bytes::from("v1"))]);
        match store.hash_hget(&key, &Bytes::from("f1")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v1")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_hset_multiple() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hash");
        let r = store.hash_hset(
            &key,
            vec![
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ],
        );
        assert!(matches!(r, CommandResponse::Integer(2)));
    }

    #[test]
    fn test_hdel() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hash");
        store.hash_hset(
            &key,
            vec![
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ],
        );
        let r = store.hash_hdel(&key, &[Bytes::from("f1")]);
        assert!(matches!(r, CommandResponse::Integer(1)));
        assert!(matches!(
            store.hash_hget(&key, &Bytes::from("f1")),
            CommandResponse::Nil
        ));
    }

    #[test]
    fn test_hgetall() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hash");
        store.hash_hset(
            &key,
            vec![
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ],
        );
        match store.hash_hgetall(&key) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 4), // 2 pairs
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_hmget() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hash");
        store.hash_hset(&key, vec![(Bytes::from("f1"), Bytes::from("v1"))]);
        match store.hash_hmget(&key, &[Bytes::from("f1"), Bytes::from("f2")]) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(
                    matches!(&items[0], CommandResponse::BulkString(v) if v == &Bytes::from("v1"))
                );
                assert!(matches!(&items[1], CommandResponse::Nil));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_hincrby() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hash");
        let r = store.hash_hincrby(&key, Bytes::from("f1"), 5);
        assert!(matches!(r, CommandResponse::Integer(5)));
        let r = store.hash_hincrby(&key, Bytes::from("f1"), 3);
        assert!(matches!(r, CommandResponse::Integer(8)));
    }

    #[test]
    fn test_hkeys_hvals_hlen() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hash");
        store.hash_hset(
            &key,
            vec![
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ],
        );
        assert!(matches!(store.hash_hlen(&key), CommandResponse::Integer(2)));
        match store.hash_hkeys(&key) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected array"),
        }
        match store.hash_hvals(&key) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_hexists() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hash");
        store.hash_hset(&key, vec![(Bytes::from("f1"), Bytes::from("v1"))]);
        assert!(matches!(
            store.hash_hexists(&key, &Bytes::from("f1")),
            CommandResponse::Integer(1)
        ));
        assert!(matches!(
            store.hash_hexists(&key, &Bytes::from("f2")),
            CommandResponse::Integer(0)
        ));
    }

    #[test]
    fn test_hsetnx() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hash");
        assert!(matches!(
            store.hash_hsetnx(&key, Bytes::from("f1"), Bytes::from("v1")),
            CommandResponse::Integer(1)
        ));
        assert!(matches!(
            store.hash_hsetnx(&key, Bytes::from("f1"), Bytes::from("v2")),
            CommandResponse::Integer(0)
        ));
        match store.hash_hget(&key, &Bytes::from("f1")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v1")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_auto_delete_empty_hash() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hash");
        store.hash_hset(&key, vec![(Bytes::from("f1"), Bytes::from("v1"))]);
        store.hash_hdel(&key, &[Bytes::from("f1")]);
        assert!(!store.exists(&key));
    }

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn ints(r: CommandResponse) -> Vec<i64> {
        match r {
            CommandResponse::Array(items) => items
                .into_iter()
                .map(|i| match i {
                    CommandResponse::Integer(n) => n,
                    other => panic!("{other:?}"),
                })
                .collect(),
            other => panic!("{other:?}"),
        }
    }

    fn hash_with(store: &mut ShardStore, key: &str, fields: &[&str]) {
        let pairs = fields.iter().map(|f| (b(f), b("v"))).collect();
        store.hash_hset(&b(key), pairs);
    }

    #[test]
    fn field_expiry_codes_follow_redis() {
        let mut store = ShardStore::new();
        hash_with(&mut store, "h", &["a", "b"]);
        let later = now_millis() + 100_000;
        let none = crate::store::ExpireCond::default();
        // a set, b set, c missing.
        assert_eq!(
            ints(store.hash_field_expire(&b("h"), later, none, &[b("a"), b("b"), b("c")])),
            vec![1, 1, -2]
        );
        // NX refuses fields that have one; XX refuses those without.
        let nx = crate::store::ExpireCond {
            nx: true,
            ..Default::default()
        };
        assert_eq!(
            ints(store.hash_field_expire(&b("h"), later, nx, &[b("a")])),
            vec![0]
        );
        // GT: only a later deadline; LT: only an earlier one.
        let gt = crate::store::ExpireCond {
            gt: true,
            ..Default::default()
        };
        let lt = crate::store::ExpireCond {
            lt: true,
            ..Default::default()
        };
        assert_eq!(
            ints(store.hash_field_expire(&b("h"), later - 1, gt, &[b("a")])),
            vec![0]
        );
        assert_eq!(
            ints(store.hash_field_expire(&b("h"), later - 1, lt, &[b("a")])),
            vec![1]
        );
        // A deadline already past deletes the field.
        assert_eq!(
            ints(store.hash_field_expire(&b("h"), 1, none, &[b("b")])),
            vec![2]
        );
        assert!(matches!(
            store.hash_hlen(&b("h")),
            CommandResponse::Integer(1)
        ));
        // Missing key: -2 per field.
        assert_eq!(
            ints(store.hash_field_expire(&b("nope"), later, none, &[b("x"), b("y")])),
            vec![-2, -2]
        );
    }

    #[test]
    fn ttl_persist_and_overwrite() {
        let mut store = ShardStore::new();
        hash_with(&mut store, "h", &["a", "b"]);
        let at = now_millis() + 50_000;
        store.hash_field_expire(&b("h"), at, Default::default(), &[b("a")]);
        let ttl = ints(store.hash_field_ttl(&b("h"), &[b("a"), b("b"), b("c")], false, false));
        assert!(ttl[0] > 40 && ttl[0] <= 50, "{ttl:?}");
        assert_eq!(&ttl[1..], &[-1, -2]);
        assert_eq!(
            ints(store.hash_field_ttl(&b("h"), &[b("a")], true, true)),
            vec![at as i64]
        );
        // Writing the field drops its expiry (Redis 7.4).
        store.hash_hset(&b("h"), vec![(b("a"), b("new"))]);
        assert_eq!(
            ints(store.hash_field_ttl(&b("h"), &[b("a")], false, false)),
            vec![-1]
        );
        store.hash_field_expire(&b("h"), at, Default::default(), &[b("b")]);
        assert_eq!(
            ints(store.hash_field_persist(&b("h"), &[b("b"), b("a"), b("z")])),
            vec![1, -1, -2]
        );
    }

    #[test]
    fn due_fields_vanish_on_access_and_are_reported() {
        let mut store = ShardStore::new();
        hash_with(&mut store, "h", &["a", "b"]);
        store.hash_field_expire(&b("h"), now_millis() + 30, Default::default(), &[b("a")]);
        std::thread::sleep(std::time::Duration::from_millis(60));
        assert!(matches!(
            store.hash_hget(&b("h"), &b("a")),
            CommandResponse::Nil
        ));
        assert!(matches!(
            store.hash_hlen(&b("h")),
            CommandResponse::Integer(1)
        ));
        assert_eq!(store.expired_fields, vec![(0, b("h"), vec![b("a")])]);
        // The last field going removes the key.
        store.expired_fields.clear();
        store.hash_field_expire(&b("h"), now_millis() + 30, Default::default(), &[b("b")]);
        std::thread::sleep(std::time::Duration::from_millis(60));
        assert!(matches!(store.hash_hgetall(&b("h")), CommandResponse::Array(v) if v.is_empty()));
        assert!(!store.exists(&b("h")));
    }

    #[test]
    fn the_active_cycle_expires_fields_nobody_reads() {
        let mut store = ShardStore::new();
        for i in 0..50 {
            let k = format!("h{i}");
            hash_with(&mut store, &k, &["a", "b"]);
            store.hash_field_expire(&b(&k), now_millis() + 20, Default::default(), &[b("a")]);
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        for _ in 0..20 {
            store.active_expire_cycle(std::time::Duration::from_millis(50));
        }
        let expired: usize = store.expired_fields.iter().map(|(_, _, f)| f.len()).sum();
        assert_eq!(expired, 50);
    }
}
