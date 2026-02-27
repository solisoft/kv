use bytes::Bytes;
use std::collections::HashMap;

use crate::expiry::ExpiryHeap;
use crate::types::*;

/// Per-shard key-value store. Owned by a single thread — no locking needed.
#[derive(Debug)]
pub struct ShardStore {
    data: HashMap<Bytes, KeyEntry>,
    expiry_heap: ExpiryHeap,
    /// Buffer of keys that were lazily expired during get/get_mut. Drained by ShardHandle.
    pub expired_buffer: Vec<Bytes>,
}

impl ShardStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expiry_heap: ExpiryHeap::new(),
            expired_buffer: Vec::new(),
        }
    }

    /// Get a reference to an entry, performing lazy expiry check.
    pub fn get(&mut self, key: &Bytes) -> Option<&KeyEntry> {
        // Lazy expiry: check on access
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired() {
                self.expired_buffer.push(key.clone());
                self.data.remove(key);
                return None;
            }
        }
        self.data.get(key)
    }

    /// Get a mutable reference to an entry, performing lazy expiry check.
    pub fn get_mut(&mut self, key: &Bytes) -> Option<&mut KeyEntry> {
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired() {
                self.expired_buffer.push(key.clone());
                self.data.remove(key);
                return None;
            }
        }
        self.data.get_mut(key)
    }

    /// Set a key-value pair with optional expiry in milliseconds.
    pub fn set(&mut self, key: Bytes, value: RedisValue, expire_ms: Option<u64>) {
        let entry = if let Some(ms) = expire_ms {
            let expires_at = now_millis() + ms;
            self.expiry_heap.push(key.clone(), expires_at);
            KeyEntry::with_expiry(value, expires_at)
        } else {
            KeyEntry::new(value)
        };
        self.data.insert(key, entry);
    }

    /// Set a key-value pair with an absolute expiry timestamp in milliseconds.
    pub fn set_with_absolute_expiry(&mut self, key: Bytes, value: RedisValue, expires_at: u64) {
        self.expiry_heap.push(key.clone(), expires_at);
        self.data.insert(key, KeyEntry::with_expiry(value, expires_at));
    }

    /// Insert a KeyEntry directly.
    pub fn insert_entry(&mut self, key: Bytes, entry: KeyEntry) {
        if let Some(exp) = entry.expires_at {
            self.expiry_heap.push(key.clone(), exp);
        }
        self.data.insert(key, entry);
    }

    /// Delete a key. Returns true if the key existed.
    pub fn del(&mut self, key: &Bytes) -> bool {
        self.data.remove(key).is_some()
    }

    /// Check if a key exists (non-expired).
    pub fn exists(&mut self, key: &Bytes) -> bool {
        self.get(key).is_some()
    }

    /// Set expiry on existing key. Returns true if key exists.
    pub fn expire(&mut self, key: &Bytes, expire_ms: u64) -> bool {
        if let Some(entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                self.data.remove(key);
                return false;
            }
            let expires_at = now_millis() + expire_ms;
            entry.expires_at = Some(expires_at);
            self.expiry_heap.push(key.clone(), expires_at);
            true
        } else {
            false
        }
    }

    /// Set absolute expiry on existing key. Returns true if key exists.
    pub fn expire_at(&mut self, key: &Bytes, expires_at_ms: u64) -> bool {
        if let Some(entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                self.data.remove(key);
                return false;
            }
            entry.expires_at = Some(expires_at_ms);
            self.expiry_heap.push(key.clone(), expires_at_ms);
            true
        } else {
            false
        }
    }

    /// Remove the expiry from a key. Returns true if key exists.
    pub fn persist(&mut self, key: &Bytes) -> bool {
        if let Some(entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                self.data.remove(key);
                return false;
            }
            if entry.expires_at.is_some() {
                entry.expires_at = None;
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Get TTL in milliseconds. Returns -2 if key doesn't exist, -1 if no expiry.
    pub fn pttl(&mut self, key: &Bytes) -> i64 {
        match self.get(key) {
            None => -2,
            Some(entry) => match entry.expires_at {
                None => -1,
                Some(exp) => {
                    let now = now_millis();
                    if exp <= now {
                        -2
                    } else {
                        (exp - now) as i64
                    }
                }
            },
        }
    }

    /// Get TTL in seconds.
    pub fn ttl(&mut self, key: &Bytes) -> i64 {
        let pttl = self.pttl(key);
        if pttl > 0 {
            ((pttl + 999) / 1000) as i64 // ceiling division
        } else {
            pttl
        }
    }

    /// Get the type of a key.
    pub fn key_type(&mut self, key: &Bytes) -> &'static str {
        match self.get(key) {
            None => "none",
            Some(entry) => entry.type_name(),
        }
    }

    /// Return the number of keys (not checking expiry for all).
    pub fn dbsize(&self) -> usize {
        self.data.len()
    }

    /// Get all keys matching a glob pattern.
    pub fn keys(&mut self, pattern: &str) -> Vec<Bytes> {
        let keys: Vec<Bytes> = self.data.keys().cloned().collect();
        keys.into_iter()
            .filter(|k| {
                let key_str = std::str::from_utf8(k).unwrap_or("");
                glob_match(pattern, key_str)
            })
            .filter(|k| self.get(k).is_some()) // filter expired
            .collect()
    }

    /// SCAN implementation: cursor-based iteration.
    pub fn scan(&mut self, cursor: usize, pattern: Option<&str>, count: usize) -> (usize, Vec<Bytes>) {
        let all_keys: Vec<Bytes> = self.data.keys().cloned().collect();
        let total = all_keys.len();
        if total == 0 {
            return (0, Vec::new());
        }

        let start = cursor;
        let mut result = Vec::new();
        let mut i = start;
        let mut checked = 0;

        while checked < count && i < total {
            let key = &all_keys[i];
            if self.get(key).is_some() {
                let matches = pattern
                    .map(|p| {
                        let key_str = std::str::from_utf8(key).unwrap_or("");
                        glob_match(p, key_str)
                    })
                    .unwrap_or(true);
                if matches {
                    result.push(key.clone());
                }
            }
            i += 1;
            checked += 1;
        }

        let next_cursor = if i >= total { 0 } else { i };
        (next_cursor, result)
    }

    /// Flush all keys.
    pub fn flush(&mut self) {
        self.data.clear();
        self.expiry_heap = ExpiryHeap::new();
    }

    /// Run active expiry: remove expired keys from the heap. Returns the expired key names.
    pub fn run_active_expiry(&mut self) -> Vec<Bytes> {
        let now = now_millis();
        let expired_keys = self.expiry_heap.drain_expired(now);
        let mut removed = Vec::new();
        for key in expired_keys {
            if let Some(entry) = self.data.get(&key) {
                if entry.is_expired() {
                    self.data.remove(&key);
                    removed.push(key);
                }
            }
        }
        removed
    }

    /// Rename a key. Returns error message if source doesn't exist.
    pub fn rename(&mut self, from: &Bytes, to: Bytes) -> Result<(), &'static str> {
        // Check if 'from' exists (with lazy expiry)
        if self.get(from).is_none() {
            return Err("ERR no such key");
        }
        let entry = self.data.remove(from).unwrap();
        self.data.insert(to, entry);
        Ok(())
    }

    /// Get random key.
    pub fn random_key(&self) -> Option<Bytes> {
        self.data.keys().next().cloned()
    }

    /// Get all data for persistence/snapshotting.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &KeyEntry)> {
        self.data.iter()
    }

    /// Get mutable reference to underlying data (for restore).
    pub fn data_mut(&mut self) -> &mut HashMap<Bytes, KeyEntry> {
        &mut self.data
    }
}

impl Default for ShardStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple glob matching for Redis KEYS/SCAN patterns.
pub fn glob_match_pub(pattern: &str, text: &str) -> bool {
    glob_match(pattern, text)
}

fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    glob_match_impl(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_impl(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("key1"), RedisValue::String(Bytes::from("val1")), None);
        let entry = store.get(&Bytes::from("key1")).unwrap();
        match &entry.value {
            RedisValue::String(v) => assert_eq!(v, &Bytes::from("val1")),
            _ => panic!("Expected string"),
        }
    }

    #[test]
    fn test_del() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("key1"), RedisValue::String(Bytes::from("val1")), None);
        assert!(store.del(&Bytes::from("key1")));
        assert!(!store.del(&Bytes::from("key1")));
        assert!(store.get(&Bytes::from("key1")).is_none());
    }

    #[test]
    fn test_exists() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("key1"), RedisValue::String(Bytes::from("val1")), None);
        assert!(store.exists(&Bytes::from("key1")));
        assert!(!store.exists(&Bytes::from("key2")));
    }

    #[test]
    fn test_expire_and_ttl() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("key1"), RedisValue::String(Bytes::from("val1")), Some(5000));
        let ttl = store.pttl(&Bytes::from("key1"));
        assert!(ttl > 0 && ttl <= 5000);
    }

    #[test]
    fn test_lazy_expiry() {
        let mut store = ShardStore::new();
        // Set with 0ms expiry (immediately expired)
        store.set(Bytes::from("key1"), RedisValue::String(Bytes::from("val1")), None);
        store.data.get_mut(&Bytes::from("key1")).unwrap().expires_at = Some(1); // epoch + 1ms = past
        assert!(store.get(&Bytes::from("key1")).is_none());
    }

    #[test]
    fn test_persist() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("key1"), RedisValue::String(Bytes::from("val1")), Some(5000));
        assert!(store.persist(&Bytes::from("key1")));
        assert_eq!(store.pttl(&Bytes::from("key1")), -1);
    }

    #[test]
    fn test_key_type() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("s"), RedisValue::String(Bytes::from("v")), None);
        assert_eq!(store.key_type(&Bytes::from("s")), "string");
        assert_eq!(store.key_type(&Bytes::from("x")), "none");
    }

    #[test]
    fn test_rename() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("a"), RedisValue::String(Bytes::from("v")), None);
        assert!(store.rename(&Bytes::from("a"), Bytes::from("b")).is_ok());
        assert!(store.get(&Bytes::from("a")).is_none());
        assert!(store.get(&Bytes::from("b")).is_some());
    }

    #[test]
    fn test_rename_nonexistent() {
        let mut store = ShardStore::new();
        assert!(store.rename(&Bytes::from("a"), Bytes::from("b")).is_err());
    }

    #[test]
    fn test_dbsize() {
        let mut store = ShardStore::new();
        assert_eq!(store.dbsize(), 0);
        store.set(Bytes::from("a"), RedisValue::String(Bytes::from("1")), None);
        store.set(Bytes::from("b"), RedisValue::String(Bytes::from("2")), None);
        assert_eq!(store.dbsize(), 2);
    }

    #[test]
    fn test_flush() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("a"), RedisValue::String(Bytes::from("1")), None);
        store.flush();
        assert_eq!(store.dbsize(), 0);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h*llo", "hllo"));
        assert!(glob_match("h*llo", "heeeello"));
        assert!(!glob_match("h?llo", "hllo"));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("user:*", "admin:123"));
    }

    #[test]
    fn test_keys_pattern() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("user:1"), RedisValue::String(Bytes::from("a")), None);
        store.set(Bytes::from("user:2"), RedisValue::String(Bytes::from("b")), None);
        store.set(Bytes::from("admin:1"), RedisValue::String(Bytes::from("c")), None);
        let keys = store.keys("user:*");
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_scan() {
        let mut store = ShardStore::new();
        for i in 0..20 {
            store.set(
                Bytes::from(format!("key:{}", i)),
                RedisValue::String(Bytes::from("v")),
                None,
            );
        }
        let (cursor, first_batch) = store.scan(0, None, 10);
        assert_eq!(first_batch.len(), 10);
        assert_ne!(cursor, 0);

        let (cursor2, second_batch) = store.scan(cursor, None, 10);
        assert_eq!(second_batch.len(), 10);
        assert_eq!(cursor2, 0);
    }
}
