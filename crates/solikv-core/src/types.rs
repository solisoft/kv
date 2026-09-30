use bytes::Bytes;
use indexmap::{IndexMap, IndexSet};
use ordered_float::OrderedFloat;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::sorted_list::SortedList;

/// Hasher of the keyspace and of every collection value.
///
/// foldhash instead of the standard SipHash: several times faster on the
/// short keys a cache holds, and still seeded per map from a random
/// per-process value, so an attacker who does not know the seed cannot
/// precompute colliding keys.
pub type FastHasher = foldhash::fast::RandomState;

/// A hash's fields. Insertion-indexed so `HSCAN` walks it by position and
/// `HRANDFIELD` picks a field in O(1).
pub type HashValue = IndexMap<Bytes, Bytes, FastHasher>;

/// A set's members. Insertion-indexed for `SSCAN`, `SPOP` and
/// `SRANDMEMBER` (random member in O(1)).
pub type SetValue = IndexSet<Bytes, FastHasher>;

/// Core Redis value types stored in the database.
///
/// Everything but strings and lists is boxed: a plain string entry then
/// costs 40 bytes of value instead of the size of the largest variant (a
/// stream, 88 bytes), and strings are what most keys hold.
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(Box<HashValue>),
    Set(Box<SetValue>),
    ZSet(Box<ZSetValue>),
    Stream(Box<StreamValue>),
    HyperLogLog(Box<HyperLogLogValue>),
    BloomFilter(Box<BloomFilterValue>),
}

impl RedisValue {
    pub fn new_hash() -> Self {
        RedisValue::Hash(Box::default())
    }

    pub fn new_set() -> Self {
        RedisValue::Set(Box::default())
    }

    pub fn new_zset() -> Self {
        RedisValue::ZSet(Box::default())
    }

    pub fn hash(h: HashValue) -> Self {
        RedisValue::Hash(Box::new(h))
    }

    pub fn set(s: SetValue) -> Self {
        RedisValue::Set(Box::new(s))
    }

    pub fn zset(z: ZSetValue) -> Self {
        RedisValue::ZSet(Box::new(z))
    }

    pub fn stream(s: StreamValue) -> Self {
        RedisValue::Stream(Box::new(s))
    }

    pub fn hll(h: HyperLogLogValue) -> Self {
        RedisValue::HyperLogLog(Box::new(h))
    }

    pub fn bloom(b: BloomFilterValue) -> Self {
        RedisValue::BloomFilter(Box::new(b))
    }
}

/// Sorted set with dual index: an order-statistic list of `(score, member)`
/// (ranks and ranges in O(log n), see [`SortedList`]) and member → score.
#[derive(Debug, Clone, Default)]
pub struct ZSetValue {
    pub scores: SortedList<(OrderedFloat<f64>, Bytes)>,
    pub members: IndexMap<Bytes, OrderedFloat<f64>, FastHasher>,
}

impl ZSetValue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Insert or update a member. Returns true if the member is new.
    pub fn insert(&mut self, score: f64, member: Bytes) -> bool {
        self.upsert(member, |_| Some(score)).0.is_none()
    }

    /// One hash lookup for `ZADD`'s read-decide-write: `decide` gets the
    /// member's current score (if any) and returns the new one, or `None`
    /// to leave it. Returns `(previous score, whether it changed)`.
    pub fn upsert(
        &mut self,
        member: Bytes,
        decide: impl FnOnce(Option<f64>) -> Option<f64>,
    ) -> (Option<f64>, bool) {
        match self.members.entry(member) {
            indexmap::map::Entry::Occupied(mut e) => {
                let old = *e.get();
                match decide(Some(old.into_inner())) {
                    Some(new) if OrderedFloat(new) != old => {
                        let new = OrderedFloat(new);
                        *e.get_mut() = new;
                        let m = e.key().clone();
                        self.scores.remove(&(old, m.clone()));
                        self.scores.insert((new, m));
                        (Some(old.into_inner()), true)
                    }
                    _ => (Some(old.into_inner()), false),
                }
            }
            indexmap::map::Entry::Vacant(e) => match decide(None) {
                Some(new) => {
                    let new = OrderedFloat(new);
                    let m = e.key().clone();
                    e.insert(new);
                    self.scores.insert((new, m));
                    (None, true)
                }
                None => (None, false),
            },
        }
    }

    /// Remove a member. Returns true if it existed.
    pub fn remove(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.members.swap_remove(member) {
            self.scores.remove(&(score, member.clone()));
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).map(|s| s.into_inner())
    }

    /// Rank (0-based, ascending) of `member`.
    pub fn rank(&self, member: &Bytes) -> Option<usize> {
        let score = *self.members.get(member)?;
        self.scores.position(&(score, member.clone()))
    }

    /// Increment a member's score. Returns the new score.
    pub fn incr(&mut self, member: Bytes, delta: f64) -> f64 {
        let old = self
            .members
            .get(&member)
            .copied()
            .unwrap_or(OrderedFloat(0.0));
        let new_score = old.into_inner() + delta;
        self.insert(new_score, member);
        new_score
    }

    /// Remove and return the lowest (`max = false`) or highest member.
    pub fn pop(&mut self, max: bool) -> Option<(Bytes, f64)> {
        let (score, member) = if max {
            self.scores.pop_last()?
        } else {
            self.scores.pop_first()?
        };
        self.members.swap_remove(&member);
        Some((member, score.into_inner()))
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
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

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

/// Largest string value, same as Redis' `proto-max-bulk-len` (512 MB).
pub const MAX_STRING_LEN: usize = 512 * 1024 * 1024;

/// SETBIT/GETBIT offsets must be below this (2^32 bits = a 512 MB string).
pub const MAX_BIT_OFFSET: u64 = 1 << 32;

/// Largest bloom filter bit array (2^32 bits = 512 MB), whatever the
/// requested capacity and error rate work out to.
pub const MAX_BLOOM_BITS: u64 = 1 << 32;

/// Bloom Filter — double hashing, configurable capacity and error rate.
#[derive(Debug, Clone)]
pub struct BloomFilterValue {
    pub bits: Vec<u8>,   // bit array (packed bytes)
    pub num_bits: u64,   // total bits
    pub num_hashes: u32, // number of hash functions
    pub num_items: u64,  // items inserted (for BF.INFO)
    pub capacity: u64,   // original requested capacity
    pub error_rate: f64, // original requested error rate
}

impl BloomFilterValue {
    /// Bits a filter for `capacity` items at `error_rate` needs, or `None`
    /// when the parameters are invalid or it would exceed `MAX_BLOOM_BITS`.
    pub fn required_bits(capacity: u64, error_rate: f64) -> Option<u64> {
        if capacity == 0 || !(error_rate > 0.0 && error_rate < 1.0) {
            return None;
        }
        let bits = (-((capacity as f64) * error_rate.ln()) / (2.0_f64.ln().powi(2))).ceil();
        (bits.is_finite() && bits <= MAX_BLOOM_BITS as f64).then_some((bits as u64).max(8))
    }

    /// Callers pass parameters that [`Self::required_bits`] accepts; anything
    /// else is clamped rather than allocated as asked.
    pub fn with_capacity(capacity: u64, error_rate: f64) -> Self {
        let capacity = capacity.max(1);
        let num_bits = Self::required_bits(capacity, error_rate).unwrap_or(MAX_BLOOM_BITS);
        let num_bits = num_bits.max(8); // minimum 8 bits
        let num_hashes = ((num_bits as f64 / capacity as f64) * 2.0_f64.ln()).ceil() as u32;
        let num_hashes = num_hashes.max(1);
        let byte_len = num_bits.div_ceil(8) as usize;
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
    /// Last access (LRU clock) or access frequency (LFU), as Redis packs
    /// them in 24 bits; see [`access`]. Drives eviction, `OBJECT IDLETIME`
    /// and `OBJECT FREQ`.
    pub access: u32,
    /// Snapshot epoch this entry was last written out in (see
    /// `ShardStore::begin_snapshot`): lets a snapshot that runs in steps
    /// save each key exactly once, as it was when the snapshot began.
    pub snap: u32,
}

impl KeyEntry {
    pub fn new(value: RedisValue) -> Self {
        Self {
            value,
            expires_at: None,
            access: access::initial(),
            snap: 0,
        }
    }

    pub fn with_expiry(value: RedisValue, expires_at: u64) -> Self {
        Self {
            value,
            expires_at: Some(expires_at),
            access: access::initial(),
            snap: 0,
        }
    }

    /// Whether the TTL has passed. Never true while this thread is loading
    /// (see [`LoadingGuard`]).
    #[inline]
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(exp) => now_millis() >= exp && !is_loading(),
            None => false,
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

/// Access tracking for eviction: an LRU clock or an LFU counter per key,
/// packed into the entry's 24-bit `access` field the way Redis packs its
/// `robj.lru`.
pub mod access {
    use super::{now_millis, AtomicU8, Ordering};
    use std::cell::Cell;

    /// What `access` holds: set from `maxmemory-policy`.
    static MODE: AtomicU8 = AtomicU8::new(MODE_LRU);
    const MODE_LRU: u8 = 0;
    const MODE_LFU: u8 = 1;

    /// 24-bit clock mask.
    pub const CLOCK_MAX: u32 = (1 << 24) - 1;
    /// LFU: counter a new key starts with (`LFU_INIT_VAL`).
    pub const LFU_INIT: u32 = 5;
    /// LFU tunables, Redis' defaults (`lfu-log-factor`, `lfu-decay-time`).
    const LFU_LOG_FACTOR: f64 = 10.0;
    const LFU_DECAY_MINUTES: u32 = 1;

    /// Track frequency (true) or recency (false).
    pub fn set_lfu(lfu: bool) {
        MODE.store(if lfu { MODE_LFU } else { MODE_LRU }, Ordering::Relaxed);
    }

    #[inline]
    pub fn is_lfu() -> bool {
        MODE.load(Ordering::Relaxed) == MODE_LFU
    }

    /// LRU clock: seconds, 24 bits (wraps every ~194 days, like Redis').
    #[inline]
    pub fn lru_clock() -> u32 {
        ((now_millis() / 1000) as u32) & CLOCK_MAX
    }

    /// LFU time: minutes, 16 bits.
    #[inline]
    fn lfu_minutes() -> u32 {
        ((now_millis() / 60_000) as u32) & 0xFFFF
    }

    /// `access` for a key just created.
    #[inline]
    pub fn initial() -> u32 {
        if is_lfu() {
            (lfu_minutes() << 8) | LFU_INIT
        } else {
            lru_clock()
        }
    }

    /// `access` after the key was read or written.
    #[inline]
    pub fn touched(prev: u32) -> u32 {
        if is_lfu() {
            let counter = log_incr(decayed(prev));
            (lfu_minutes() << 8) | counter
        } else {
            lru_clock()
        }
    }

    /// Seconds since the last access (`OBJECT IDLETIME`, LRU ranking).
    pub fn idle_secs(access: u32) -> u64 {
        let now = lru_clock();
        let a = access & CLOCK_MAX;
        if now >= a {
            (now - a) as u64
        } else {
            (now + (CLOCK_MAX - a)) as u64
        }
    }

    /// The LFU counter, decayed for the time since the last access
    /// (`OBJECT FREQ`, LFU ranking).
    pub fn decayed(access: u32) -> u32 {
        let counter = access & 0xFF;
        let then = access >> 8;
        let now = lfu_minutes();
        let elapsed = if now >= then {
            now - then
        } else {
            0xFFFF - then + now
        };
        counter.saturating_sub(elapsed / LFU_DECAY_MINUTES)
    }

    thread_local! {
        static RNG: Cell<u64> = const { Cell::new(0x2545_F491_4F6C_DD1D) };
    }

    /// Redis' logarithmic counter increment: the higher the counter, the
    /// less likely an access raises it.
    fn log_incr(counter: u32) -> u32 {
        if counter >= 255 {
            return 255;
        }
        let r = RNG.with(|c| {
            let mut x = c.get();
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            c.set(x);
            (x >> 11) as f64 / (1u64 << 53) as f64
        });
        let base = counter.saturating_sub(LFU_INIT) as f64;
        let p = 1.0 / (base * LFU_LOG_FACTOR + 1.0);
        if r < p {
            counter + 1
        } else {
            counter
        }
    }
}

thread_local! {
    static LOADING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether this thread is replaying persisted data (see [`LoadingGuard`]).
#[inline]
pub fn is_loading() -> bool {
    LOADING.with(|l| l.get())
}

/// Marks this thread as loading persisted data until dropped.
///
/// While loading, no key counts as expired. The append-only log records every
/// expiry the live server acted on as an explicit `DEL`, at the point it
/// happened; replaying with the clock running as well would expire keys at
/// *replay* time instead, and a later logged command would then run against
/// a different state than it did live (`INCR` on a key that had not expired
/// yet, say, recreating it from 0). Redis loads the same way. Keys whose TTL
/// passed while the server was down expire normally once loading ends.
///
/// Thread-local rather than global so loading one dataset (a test, a replay
/// into a scratch store) never changes expiry for another thread.
pub struct LoadingGuard {
    prev: bool,
}

impl LoadingGuard {
    pub fn new() -> Self {
        let prev = LOADING.with(|l| l.replace(true));
        Self { prev }
    }
}

impl Default for LoadingGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for LoadingGuard {
    fn drop(&mut self) {
        let prev = self.prev;
        LOADING.with(|l| l.set(prev));
    }
}

/// Current time in milliseconds since UNIX epoch, from a clock a dedicated
/// thread refreshes every millisecond.
///
/// Every command that looks at a TTL asks for the time, often several times;
/// a cached value is one relaxed load instead of a `clock_gettime`. The
/// refresh thread keeps it within about a millisecond of the real clock, so
/// a key never outlives its TTL by more than that — the granularity Redis
/// works at too (it caches the time once per event-loop iteration). It never
/// goes backwards: a TTL set now and checked just after is measured against
/// the same clock. (A cache refreshed from the command path instead, every
/// 16th call, went arbitrarily stale on a quiet server.)
#[inline]
pub fn now_millis() -> u64 {
    let t = CLOCK.load(Ordering::Relaxed);
    if t != 0 {
        t
    } else {
        start_clock()
    }
}

static CLOCK: AtomicU64 = AtomicU64::new(0);
static CLOCK_THREAD: OnceLock<()> = OnceLock::new();

fn wall_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cold]
fn start_clock() -> u64 {
    CLOCK_THREAD.get_or_init(|| {
        let spawned = std::thread::Builder::new()
            .name("solikv-clock".into())
            .spawn(|| loop {
                std::thread::sleep(Duration::from_millis(1));
                // fetch_max: a wall clock stepped back must not make a TTL
                // already judged expired come back to life.
                CLOCK.fetch_max(wall_millis(), Ordering::Relaxed);
            });
        match spawned {
            // Only a running refresher may publish a cached time; without
            // one, 0 keeps every call on the real clock.
            Ok(_) => {
                CLOCK.fetch_max(wall_millis(), Ordering::Relaxed);
            }
            Err(e) => tracing::error!("clock thread: {e}; reading the time per call"),
        }
    });
    match CLOCK.load(Ordering::Relaxed) {
        0 => wall_millis(),
        t => t,
    }
}

/// Current time in seconds since UNIX epoch.
#[inline]
pub fn now_secs() -> u64 {
    now_millis() / 1000
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
        CommandResponse::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        )
    }

    pub fn wrong_arity(cmd: &str) -> Self {
        CommandResponse::Error(format!(
            "ERR wrong number of arguments for '{}' command",
            cmd
        ))
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
        assert_eq!(
            KeyEntry::new(RedisValue::String(Bytes::from(""))).type_name(),
            "string"
        );
        assert_eq!(
            KeyEntry::new(RedisValue::List(VecDeque::new())).type_name(),
            "list"
        );
        assert_eq!(KeyEntry::new(RedisValue::new_hash()).type_name(), "hash");
        assert_eq!(KeyEntry::new(RedisValue::new_set()).type_name(), "set");
        assert_eq!(KeyEntry::new(RedisValue::new_zset()).type_name(), "zset");
        assert_eq!(
            KeyEntry::new(RedisValue::stream(StreamValue::new())).type_name(),
            "stream"
        );
        assert_eq!(
            KeyEntry::new(RedisValue::hll(HyperLogLogValue::new())).type_name(),
            "string"
        );
        assert_eq!(
            KeyEntry::new(RedisValue::bloom(BloomFilterValue::with_capacity(
                100, 0.01
            )))
            .type_name(),
            "string"
        );
    }
}
