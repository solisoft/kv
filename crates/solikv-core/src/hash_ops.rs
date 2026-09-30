use bytes::Bytes;

use crate::set_ops::{scan_reply, scan_window};
use crate::store::ShardStore;
use crate::types::*;

impl ShardStore {
    fn get_or_create_hash(&mut self, key: &Bytes) -> Result<&mut HashValue, CommandResponse> {
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
        if let indexmap::map::Entry::Vacant(e) = hash.entry(field) {
            e.insert(value);
            CommandResponse::integer(1)
        } else {
            CommandResponse::integer(0)
        }
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
}
