use bytes::{Bytes, BytesMut};

use crate::store::ShardStore;
use crate::types::*;

impl ShardStore {
    // ---- GET ----
    pub fn string_get(&mut self, key: &Bytes) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::nil(),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => CommandResponse::bulk(v.clone()),
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- SET ----
    /// SET key value [EX seconds] [PX milliseconds] [NX|XX] [GET]
    pub fn string_set(
        &mut self,
        key: Bytes,
        value: Bytes,
        expire_ms: Option<u64>,
        nx: bool,
        xx: bool,
        get: bool,
    ) -> CommandResponse {
        let old = if get {
            match self.get(&key) {
                Some(entry) => match &entry.value {
                    RedisValue::String(v) => Some(CommandResponse::bulk(v.clone())),
                    _ => return CommandResponse::wrong_type(),
                },
                None => Some(CommandResponse::nil()),
            }
        } else {
            None
        };

        if nx && self.exists(&key) {
            return old.unwrap_or(CommandResponse::nil());
        }
        if xx && !self.exists(&key) {
            return old.unwrap_or(CommandResponse::nil());
        }

        self.set(key, RedisValue::String(value), expire_ms);

        old.unwrap_or(CommandResponse::ok())
    }

    // ---- SETNX ----
    pub fn string_setnx(&mut self, key: Bytes, value: Bytes) -> CommandResponse {
        if self.exists(&key) {
            CommandResponse::integer(0)
        } else {
            self.set(key, RedisValue::String(value), None);
            CommandResponse::integer(1)
        }
    }

    // ---- SETEX ----
    pub fn string_setex(&mut self, key: Bytes, seconds: u64, value: Bytes) -> CommandResponse {
        self.set(key, RedisValue::String(value), Some(seconds * 1000));
        CommandResponse::ok()
    }

    // ---- PSETEX ----
    pub fn string_psetex(&mut self, key: Bytes, millis: u64, value: Bytes) -> CommandResponse {
        self.set(key, RedisValue::String(value), Some(millis));
        CommandResponse::ok()
    }

    // ---- GETSET ----
    pub fn string_getset(&mut self, key: Bytes, value: Bytes) -> CommandResponse {
        let old = match self.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::String(v) => CommandResponse::bulk(v.clone()),
                _ => return CommandResponse::wrong_type(),
            },
            None => CommandResponse::nil(),
        };
        self.set(key, RedisValue::String(value), None);
        old
    }

    // ---- GETDEL ----
    pub fn string_getdel(&mut self, key: &Bytes) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::nil(),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => {
                    let result = CommandResponse::bulk(v.clone());
                    self.del(key);
                    result
                }
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- GETEX ----
    pub fn string_getex(&mut self, key: &Bytes, expire_ms: Option<u64>, persist: bool) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::nil(),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => {
                    let result = CommandResponse::bulk(v.clone());
                    if persist {
                        self.persist(key);
                    } else if let Some(ms) = expire_ms {
                        self.expire(key, ms);
                    }
                    result
                }
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- MGET ----
    pub fn string_mget(&mut self, keys: &[Bytes]) -> CommandResponse {
        let results: Vec<CommandResponse> = keys
            .iter()
            .map(|k| self.string_get(k))
            .collect();
        CommandResponse::array(results)
    }

    // ---- MSET ----
    pub fn string_mset(&mut self, pairs: Vec<(Bytes, Bytes)>) -> CommandResponse {
        for (key, value) in pairs {
            self.set(key, RedisValue::String(value), None);
        }
        CommandResponse::ok()
    }

    // ---- MSETNX ----
    pub fn string_msetnx(&mut self, pairs: Vec<(Bytes, Bytes)>) -> CommandResponse {
        // Only set if NONE of the keys exist
        for (key, _) in &pairs {
            if self.exists(key) {
                return CommandResponse::integer(0);
            }
        }
        for (key, value) in pairs {
            self.set(key, RedisValue::String(value), None);
        }
        CommandResponse::integer(1)
    }

    // ---- INCR ----
    pub fn string_incr(&mut self, key: &Bytes) -> CommandResponse {
        self.string_incrby(key, 1)
    }

    // ---- DECR ----
    pub fn string_decr(&mut self, key: &Bytes) -> CommandResponse {
        self.string_incrby(key, -1)
    }

    // ---- INCRBY ----
    pub fn string_incrby(&mut self, key: &Bytes, delta: i64) -> CommandResponse {
        let current = match self.get(key) {
            None => 0i64,
            Some(entry) => match &entry.value {
                RedisValue::String(v) => {
                    match std::str::from_utf8(v) {
                        Ok(s) => match s.parse::<i64>() {
                            Ok(n) => n,
                            Err(_) => return CommandResponse::error("ERR value is not an integer or out of range"),
                        },
                        Err(_) => return CommandResponse::error("ERR value is not an integer or out of range"),
                    }
                }
                _ => return CommandResponse::wrong_type(),
            },
        };

        match current.checked_add(delta) {
            Some(new_val) => {
                self.set(
                    key.clone(),
                    RedisValue::String(Bytes::from(new_val.to_string())),
                    None,
                );
                CommandResponse::integer(new_val)
            }
            None => CommandResponse::error("ERR increment or decrement would overflow"),
        }
    }

    // ---- DECRBY ----
    pub fn string_decrby(&mut self, key: &Bytes, delta: i64) -> CommandResponse {
        self.string_incrby(key, -delta)
    }

    // ---- INCRBYFLOAT ----
    pub fn string_incrbyfloat(&mut self, key: &Bytes, delta: f64) -> CommandResponse {
        let current = match self.get(key) {
            None => 0.0f64,
            Some(entry) => match &entry.value {
                RedisValue::String(v) => {
                    match std::str::from_utf8(v) {
                        Ok(s) => match s.parse::<f64>() {
                            Ok(n) => n,
                            Err(_) => return CommandResponse::error("ERR value is not a valid float"),
                        },
                        Err(_) => return CommandResponse::error("ERR value is not a valid float"),
                    }
                }
                _ => return CommandResponse::wrong_type(),
            },
        };

        let new_val = current + delta;
        let formatted = if new_val == new_val.trunc() {
            format!("{}", new_val as i64)
        } else {
            format!("{}", new_val)
        };
        self.set(
            key.clone(),
            RedisValue::String(Bytes::from(formatted.clone())),
            None,
        );
        CommandResponse::bulk(Bytes::from(formatted))
    }

    // ---- APPEND ----
    pub fn string_append(&mut self, key: &Bytes, value: &Bytes) -> CommandResponse {
        match self.get(key) {
            None => {
                self.set(key.clone(), RedisValue::String(value.clone()), None);
                CommandResponse::integer(value.len() as i64)
            }
            Some(entry) => match &entry.value {
                RedisValue::String(existing) => {
                    let mut new_val = BytesMut::with_capacity(existing.len() + value.len());
                    new_val.extend_from_slice(existing);
                    new_val.extend_from_slice(value);
                    let new_bytes = new_val.freeze();
                    let len = new_bytes.len() as i64;
                    self.set(key.clone(), RedisValue::String(new_bytes), None);
                    CommandResponse::integer(len)
                }
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- STRLEN ----
    pub fn string_strlen(&mut self, key: &Bytes) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::integer(0),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => CommandResponse::integer(v.len() as i64),
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- GETRANGE ----
    pub fn string_getrange(&mut self, key: &Bytes, start: i64, end: i64) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::bulk(Bytes::new()),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => {
                    let len = v.len() as i64;
                    if len == 0 {
                        return CommandResponse::bulk(Bytes::new());
                    }
                    let s = if start < 0 { (len + start).max(0) } else { start.min(len) };
                    let e = if end < 0 { (len + end).max(0) } else { end.min(len - 1) };
                    if s > e {
                        CommandResponse::bulk(Bytes::new())
                    } else {
                        CommandResponse::bulk(v.slice(s as usize..=e as usize))
                    }
                }
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- SETRANGE ----
    pub fn string_setrange(&mut self, key: &Bytes, offset: usize, value: &Bytes) -> CommandResponse {
        let current = match self.get(key) {
            None => Bytes::new(),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => v.clone(),
                _ => return CommandResponse::wrong_type(),
            },
        };

        let needed = offset + value.len();
        let mut buf = BytesMut::with_capacity(needed.max(current.len()));
        buf.extend_from_slice(&current);
        // Pad with zeros if needed
        while buf.len() < offset {
            buf.extend_from_slice(&[0]);
        }
        // Overwrite or extend
        if offset + value.len() > buf.len() {
            buf.truncate(offset);
            buf.extend_from_slice(value);
        } else {
            buf[offset..offset + value.len()].copy_from_slice(value);
        }

        let new_len = buf.len() as i64;
        self.set(key.clone(), RedisValue::String(buf.freeze()), None);
        CommandResponse::integer(new_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_nonexistent() {
        let mut store = ShardStore::new();
        assert!(matches!(store.string_get(&Bytes::from("x")), CommandResponse::Nil));
    }

    #[test]
    fn test_set_and_get() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("v"), None, false, false, false);
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v")),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_set_nx() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("v1"), None, true, false, false);
        // NX: should not overwrite
        store.string_set(Bytes::from("k"), Bytes::from("v2"), None, true, false, false);
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v1")),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_set_xx() {
        let mut store = ShardStore::new();
        // XX on nonexistent: should not set
        store.string_set(Bytes::from("k"), Bytes::from("v"), None, false, true, false);
        assert!(matches!(store.string_get(&Bytes::from("k")), CommandResponse::Nil));

        // Set first, then XX should update
        store.string_set(Bytes::from("k"), Bytes::from("v1"), None, false, false, false);
        store.string_set(Bytes::from("k"), Bytes::from("v2"), None, false, true, false);
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v2")),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_set_get_flag() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("old"), None, false, false, false);
        let resp = store.string_set(Bytes::from("k"), Bytes::from("new"), None, false, false, true);
        match resp {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("old")),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_set_with_expiry() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("v"), Some(5000), false, false, false);
        let ttl = store.pttl(&Bytes::from("k"));
        assert!(ttl > 0 && ttl <= 5000);
    }

    #[test]
    fn test_setnx() {
        let mut store = ShardStore::new();
        assert!(matches!(store.string_setnx(Bytes::from("k"), Bytes::from("v1")), CommandResponse::Integer(1)));
        assert!(matches!(store.string_setnx(Bytes::from("k"), Bytes::from("v2")), CommandResponse::Integer(0)));
    }

    #[test]
    fn test_setex() {
        let mut store = ShardStore::new();
        store.string_setex(Bytes::from("k"), 10, Bytes::from("v"));
        let ttl = store.pttl(&Bytes::from("k"));
        assert!(ttl > 9000 && ttl <= 10000);
    }

    #[test]
    fn test_getset() {
        let mut store = ShardStore::new();
        let r = store.string_getset(Bytes::from("k"), Bytes::from("v1"));
        assert!(matches!(r, CommandResponse::Nil));
        let r = store.string_getset(Bytes::from("k"), Bytes::from("v2"));
        match r {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v1")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_getdel() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("v"), None, false, false, false);
        let r = store.string_getdel(&Bytes::from("k"));
        match r {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v")),
            _ => panic!("expected bulk"),
        }
        assert!(matches!(store.string_get(&Bytes::from("k")), CommandResponse::Nil));
    }

    #[test]
    fn test_mget_mset() {
        let mut store = ShardStore::new();
        store.string_mset(vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
        ]);
        let r = store.string_mget(&[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
        match r {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(&items[0], CommandResponse::BulkString(v) if v == &Bytes::from("1")));
                assert!(matches!(&items[1], CommandResponse::BulkString(v) if v == &Bytes::from("2")));
                assert!(matches!(&items[2], CommandResponse::Nil));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_msetnx() {
        let mut store = ShardStore::new();
        let r = store.string_msetnx(vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
        ]);
        assert!(matches!(r, CommandResponse::Integer(1)));

        // Should fail because 'a' exists
        let r = store.string_msetnx(vec![
            (Bytes::from("a"), Bytes::from("3")),
            (Bytes::from("c"), Bytes::from("4")),
        ]);
        assert!(matches!(r, CommandResponse::Integer(0)));
        // 'c' should not exist
        assert!(matches!(store.string_get(&Bytes::from("c")), CommandResponse::Nil));
    }

    #[test]
    fn test_incr_decr() {
        let mut store = ShardStore::new();
        let r = store.string_incr(&Bytes::from("k"));
        assert!(matches!(r, CommandResponse::Integer(1)));
        let r = store.string_incr(&Bytes::from("k"));
        assert!(matches!(r, CommandResponse::Integer(2)));
        let r = store.string_decr(&Bytes::from("k"));
        assert!(matches!(r, CommandResponse::Integer(1)));
    }

    #[test]
    fn test_incrby() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("10"), None, false, false, false);
        let r = store.string_incrby(&Bytes::from("k"), 5);
        assert!(matches!(r, CommandResponse::Integer(15)));
    }

    #[test]
    fn test_decrby() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("10"), None, false, false, false);
        let r = store.string_decrby(&Bytes::from("k"), 3);
        assert!(matches!(r, CommandResponse::Integer(7)));
    }

    #[test]
    fn test_incr_non_integer() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("abc"), None, false, false, false);
        let r = store.string_incr(&Bytes::from("k"));
        assert!(matches!(r, CommandResponse::Error(_)));
    }

    #[test]
    fn test_incrbyfloat() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("10.5"), None, false, false, false);
        let r = store.string_incrbyfloat(&Bytes::from("k"), 0.5);
        match r {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("11")),
            other => panic!("Expected bulk, got {:?}", other),
        }
    }

    #[test]
    fn test_append() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("hello"), None, false, false, false);
        let r = store.string_append(&Bytes::from("k"), &Bytes::from(" world"));
        assert!(matches!(r, CommandResponse::Integer(11)));
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("hello world")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_append_nonexistent() {
        let mut store = ShardStore::new();
        let r = store.string_append(&Bytes::from("k"), &Bytes::from("hello"));
        assert!(matches!(r, CommandResponse::Integer(5)));
    }

    #[test]
    fn test_strlen() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("hello"), None, false, false, false);
        assert!(matches!(store.string_strlen(&Bytes::from("k")), CommandResponse::Integer(5)));
        assert!(matches!(store.string_strlen(&Bytes::from("x")), CommandResponse::Integer(0)));
    }

    #[test]
    fn test_getrange() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("hello world"), None, false, false, false);
        match store.string_getrange(&Bytes::from("k"), 0, 4) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("hello")),
            _ => panic!("expected bulk"),
        }
        // Negative indices
        match store.string_getrange(&Bytes::from("k"), -5, -1) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("world")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_setrange() {
        let mut store = ShardStore::new();
        store.string_set(Bytes::from("k"), Bytes::from("hello world"), None, false, false, false);
        let r = store.string_setrange(&Bytes::from("k"), 6, &Bytes::from("Redis"));
        assert!(matches!(r, CommandResponse::Integer(11)));
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("hello Redis")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_setrange_with_padding() {
        let mut store = ShardStore::new();
        let r = store.string_setrange(&Bytes::from("k"), 5, &Bytes::from("hi"));
        assert!(matches!(r, CommandResponse::Integer(7)));
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => {
                assert_eq!(v.len(), 7);
                assert_eq!(&v[5..], b"hi");
            }
            _ => panic!("expected bulk"),
        }
    }
}
