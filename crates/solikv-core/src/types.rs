use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

/// Core Redis value types stored in the database.
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
    ZSet(ZSetValue),
}

/// Sorted set with dual index: score->member and member->score.
#[derive(Debug, Clone)]
pub struct ZSetValue {
    pub scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    pub members: HashMap<Bytes, OrderedFloat<f64>>,
}

impl ZSetValue {
    pub fn new() -> Self {
        Self {
            scores: BTreeMap::new(),
            members: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Insert or update a member. Returns true if the member is new.
    pub fn insert(&mut self, score: f64, member: Bytes) -> bool {
        let score = OrderedFloat(score);
        if let Some(&old_score) = self.members.get(&member) {
            if old_score == score {
                return false;
            }
            self.scores.remove(&(old_score, member.clone()));
        }
        let is_new = !self.members.contains_key(&member);
        self.members.insert(member.clone(), score);
        self.scores.insert((score, member), ());
        is_new
    }

    /// Remove a member. Returns true if it existed.
    pub fn remove(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.members.remove(member) {
            self.scores.remove(&(score, member.clone()));
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).map(|s| s.into_inner())
    }

    /// Increment a member's score. Returns the new score.
    pub fn incr(&mut self, member: Bytes, delta: f64) -> f64 {
        let old = self.members.get(&member).copied().unwrap_or(OrderedFloat(0.0));
        let new_score = old.into_inner() + delta;
        self.insert(new_score, member);
        new_score
    }
}

impl Default for ZSetValue {
    fn default() -> Self {
        Self::new()
    }
}

/// A key entry in the store, holding value + optional expiry.
#[derive(Debug, Clone)]
pub struct KeyEntry {
    pub value: RedisValue,
    pub expires_at: Option<u64>, // milliseconds since UNIX epoch
}

impl KeyEntry {
    pub fn new(value: RedisValue) -> Self {
        Self {
            value,
            expires_at: None,
        }
    }

    pub fn with_expiry(value: RedisValue, expires_at: u64) -> Self {
        Self {
            value,
            expires_at: Some(expires_at),
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(exp) = self.expires_at {
            now_millis() >= exp
        } else {
            false
        }
    }

    pub fn type_name(&self) -> &'static str {
        match &self.value {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Hash(_) => "hash",
            RedisValue::Set(_) => "set",
            RedisValue::ZSet(_) => "zset",
        }
    }
}

/// Current time in milliseconds since UNIX epoch.
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Current time in seconds since UNIX epoch.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Command response type for all operations.
#[derive(Debug, Clone)]
pub enum CommandResponse {
    Ok,
    Nil,
    Integer(i64),
    BulkString(Bytes),
    SimpleString(Bytes),
    Array(Vec<CommandResponse>),
    Error(String),
    Queued,
}

impl CommandResponse {
    pub fn ok() -> Self {
        CommandResponse::Ok
    }

    pub fn nil() -> Self {
        CommandResponse::Nil
    }

    pub fn integer(n: i64) -> Self {
        CommandResponse::Integer(n)
    }

    pub fn bulk(data: Bytes) -> Self {
        CommandResponse::BulkString(data)
    }

    pub fn bulk_string(s: &str) -> Self {
        CommandResponse::BulkString(Bytes::from(s.to_string()))
    }

    pub fn simple(s: &str) -> Self {
        CommandResponse::SimpleString(Bytes::from(s.to_string()))
    }

    pub fn error(msg: impl Into<String>) -> Self {
        CommandResponse::Error(msg.into())
    }

    pub fn array(items: Vec<CommandResponse>) -> Self {
        CommandResponse::Array(items)
    }

    pub fn wrong_type() -> Self {
        CommandResponse::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }

    pub fn wrong_arity(cmd: &str) -> Self {
        CommandResponse::Error(format!("ERR wrong number of arguments for '{}' command", cmd))
    }

    pub fn syntax_error() -> Self {
        CommandResponse::Error("ERR syntax error".to_string())
    }

    pub fn is_error(&self) -> bool {
        matches!(self, CommandResponse::Error(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_entry_no_expiry() {
        let entry = KeyEntry::new(RedisValue::String(Bytes::from("hello")));
        assert!(!entry.is_expired());
        assert_eq!(entry.type_name(), "string");
    }

    #[test]
    fn test_key_entry_expired() {
        let entry = KeyEntry::with_expiry(RedisValue::String(Bytes::from("hello")), 1);
        assert!(entry.is_expired());
    }

    #[test]
    fn test_key_entry_not_expired() {
        let entry = KeyEntry::with_expiry(
            RedisValue::String(Bytes::from("hello")),
            now_millis() + 100_000,
        );
        assert!(!entry.is_expired());
    }

    #[test]
    fn test_zset_value() {
        let mut zs = ZSetValue::new();
        assert!(zs.insert(1.0, Bytes::from("a")));
        assert!(zs.insert(2.0, Bytes::from("b")));
        assert!(!zs.insert(1.5, Bytes::from("a"))); // update
        assert_eq!(zs.len(), 2);
        assert_eq!(zs.score(&Bytes::from("a")), Some(1.5));
        assert!(zs.remove(&Bytes::from("a")));
        assert_eq!(zs.len(), 1);
    }

    #[test]
    fn test_type_names() {
        assert_eq!(KeyEntry::new(RedisValue::String(Bytes::from(""))).type_name(), "string");
        assert_eq!(KeyEntry::new(RedisValue::List(VecDeque::new())).type_name(), "list");
        assert_eq!(KeyEntry::new(RedisValue::Hash(HashMap::new())).type_name(), "hash");
        assert_eq!(KeyEntry::new(RedisValue::Set(HashSet::new())).type_name(), "set");
        assert_eq!(KeyEntry::new(RedisValue::ZSet(ZSetValue::new())).type_name(), "zset");
    }
}
