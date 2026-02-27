use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

/// Core Redis value types stored in the database.
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
    ZSet(ZSetValue),
    Stream(StreamValue),
    HyperLogLog(HyperLogLogValue),
    BloomFilter(BloomFilterValue),
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

/// Stream entry ID: millisecond timestamp + sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId { ms: u64::MAX, seq: u64::MAX };

    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    pub fn parse(s: &str) -> Option<StreamIdInput> {
        if s == "*" {
            return Some(StreamIdInput::Auto);
        }
        if s == "-" {
            return Some(StreamIdInput::Min);
        }
        if s == "+" {
            return Some(StreamIdInput::Max);
        }
        if let Some((ms_s, seq_s)) = s.split_once('-') {
            let ms = ms_s.parse::<u64>().ok()?;
            let seq = seq_s.parse::<u64>().ok()?;
            Some(StreamIdInput::Explicit(StreamId { ms, seq }))
        } else {
            let ms = s.parse::<u64>().ok()?;
            Some(StreamIdInput::Partial(ms))
        }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// Parsed stream ID input from user commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamIdInput {
    Auto,
    Min,
    Max,
    Explicit(StreamId),
    Partial(u64),
}

/// Trim strategy for streams.
#[derive(Debug, Clone, Copy)]
pub enum StreamTrim {
    MaxLen { exact: bool, threshold: usize },
    MinId { exact: bool, threshold: StreamId },
}

/// A pending entry in a consumer group.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub id: StreamId,
    pub consumer: Bytes,
    pub delivery_time: u64,
    pub delivery_count: u64,
}

/// A consumer within a consumer group.
#[derive(Debug, Clone)]
pub struct StreamConsumer {
    pub name: Bytes,
    pub seen_time: u64,
    pub pending: BTreeMap<StreamId, ()>,
}

/// A consumer group on a stream.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub pending: BTreeMap<StreamId, PendingEntry>,
    pub consumers: HashMap<Bytes, StreamConsumer>,
}

/// Redis Stream: append-only log of field-value entries.
#[derive(Debug, Clone)]
pub struct StreamValue {
    pub entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub last_id: StreamId,
    pub groups: HashMap<Bytes, ConsumerGroup>,
}

impl StreamValue {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::MIN,
            groups: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for StreamValue {
    fn default() -> Self {
        Self::new()
    }
}

/// HyperLogLog — 14-bit precision, 16384 registers.
#[derive(Debug, Clone)]
pub struct HyperLogLogValue {
    pub registers: Vec<u8>, // len = 16384, each stores max leading-zeros + 1
}

const HLL_P: u32 = 14;
const HLL_M: usize = 1 << HLL_P; // 16384

fn siphash_with_seed(data: &[u8], seed: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    data.hash(&mut hasher);
    hasher.finish()
}

impl HyperLogLogValue {
    pub fn new() -> Self {
        Self {
            registers: vec![0u8; HLL_M],
        }
    }

    pub fn add(&mut self, element: &[u8]) -> bool {
        let hash = siphash_with_seed(element, 0);
        let index = (hash & ((1 << HLL_P) - 1)) as usize;
        let remaining = hash >> HLL_P;
        // Count leading zeros of the remaining 50 bits, +1
        let rho = if remaining == 0 {
            (64 - HLL_P) as u8 + 1
        } else {
            (remaining.leading_zeros() - HLL_P + 1) as u8
        };
        if rho > self.registers[index] {
            self.registers[index] = rho;
            true
        } else {
            false
        }
    }

    pub fn count(&self) -> u64 {
        let m = HLL_M as f64;
        // alpha_m for m=16384
        let alpha_m = 0.7213 / (1.0 + 1.079 / m);

        let mut sum: f64 = 0.0;
        let mut zeros: u32 = 0;
        for &reg in &self.registers {
            sum += 2.0_f64.powi(-(reg as i32));
            if reg == 0 {
                zeros += 1;
            }
        }

        let estimate = alpha_m * m * m / sum;

        // Small range correction (linear counting)
        if estimate <= 2.5 * m && zeros > 0 {
            let lc = m * (m / zeros as f64).ln();
            return lc as u64;
        }

        // Large range correction (not needed for p=14 in practice, but included)
        let two32 = 4_294_967_296.0_f64; // 2^32
        if estimate > two32 / 30.0 {
            let corrected = -two32 * (1.0 - estimate / two32).ln();
            return corrected as u64;
        }

        estimate as u64
    }

    pub fn merge(&mut self, other: &HyperLogLogValue) {
        for i in 0..HLL_M {
            if other.registers[i] > self.registers[i] {
                self.registers[i] = other.registers[i];
            }
        }
    }
}

impl Default for HyperLogLogValue {
    fn default() -> Self {
        Self::new()
    }
}

/// Bloom Filter — double hashing, configurable capacity and error rate.
#[derive(Debug, Clone)]
pub struct BloomFilterValue {
    pub bits: Vec<u8>,     // bit array (packed bytes)
    pub num_bits: u64,     // total bits
    pub num_hashes: u32,   // number of hash functions
    pub num_items: u64,    // items inserted (for BF.INFO)
    pub capacity: u64,     // original requested capacity
    pub error_rate: f64,   // original requested error rate
}

impl BloomFilterValue {
    pub fn with_capacity(capacity: u64, error_rate: f64) -> Self {
        let num_bits = (-((capacity as f64) * error_rate.ln()) / (2.0_f64.ln().powi(2))).ceil() as u64;
        let num_bits = num_bits.max(8); // minimum 8 bits
        let num_hashes = ((num_bits as f64 / capacity as f64) * 2.0_f64.ln()).ceil() as u32;
        let num_hashes = num_hashes.max(1);
        let byte_len = ((num_bits + 7) / 8) as usize;
        Self {
            bits: vec![0u8; byte_len],
            num_bits,
            num_hashes,
            num_items: 0,
            capacity,
            error_rate,
        }
    }

    pub fn add(&mut self, element: &[u8]) -> bool {
        let h1 = siphash_with_seed(element, 0);
        let h2 = siphash_with_seed(element, 1);
        let mut any_new = false;
        for i in 0..self.num_hashes {
            let bit_index = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.num_bits;
            let byte_idx = (bit_index / 8) as usize;
            let bit_offset = (bit_index % 8) as u8;
            if self.bits[byte_idx] & (1 << bit_offset) == 0 {
                any_new = true;
                self.bits[byte_idx] |= 1 << bit_offset;
            }
        }
        if any_new {
            self.num_items += 1;
        }
        any_new
    }

    pub fn exists(&self, element: &[u8]) -> bool {
        let h1 = siphash_with_seed(element, 0);
        let h2 = siphash_with_seed(element, 1);
        for i in 0..self.num_hashes {
            let bit_index = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.num_bits;
            let byte_idx = (bit_index / 8) as usize;
            let bit_offset = (bit_index % 8) as u8;
            if self.bits[byte_idx] & (1 << bit_offset) == 0 {
                return false;
            }
        }
        true
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
            RedisValue::Stream(_) => "stream",
            RedisValue::HyperLogLog(_) => "string",
            RedisValue::BloomFilter(_) => "string",
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
        assert_eq!(KeyEntry::new(RedisValue::Stream(StreamValue::new())).type_name(), "stream");
        assert_eq!(KeyEntry::new(RedisValue::HyperLogLog(HyperLogLogValue::new())).type_name(), "string");
        assert_eq!(KeyEntry::new(RedisValue::BloomFilter(BloomFilterValue::with_capacity(100, 0.01))).type_name(), "string");
    }
}
