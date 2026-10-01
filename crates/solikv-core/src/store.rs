use bytes::Bytes;
use indexmap::IndexSet;
use std::time::{Duration, Instant};

use crate::incmap::{IncMap, IncSet};
use crate::types::*;

/// `EXPIRE … NX|XX|GT|LT`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExpireCond {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
}

/// Index slots each keyspace map migrates per active-expiry cycle, on top
/// of what inserts and removals migrate (see [`IncMap`]).
const REHASH_PER_CYCLE: usize = 4096;

/// Keys a single active-expiry sample looks at (Redis'
/// `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP`).
pub const EXPIRE_SAMPLE: usize = 20;

/// `maxmemory-policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    #[default]
    NoEviction,
    AllKeysLru,
    VolatileLru,
    AllKeysLfu,
    VolatileLfu,
    AllKeysRandom,
    VolatileRandom,
    VolatileTtl,
}

impl EvictionPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "noeviction" => Self::NoEviction,
            "allkeys-lru" => Self::AllKeysLru,
            "volatile-lru" => Self::VolatileLru,
            "allkeys-lfu" => Self::AllKeysLfu,
            "volatile-lfu" => Self::VolatileLfu,
            "allkeys-random" => Self::AllKeysRandom,
            "volatile-random" => Self::VolatileRandom,
            "volatile-ttl" => Self::VolatileTtl,
            _ => return None,
        })
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::VolatileLru => "volatile-lru",
            Self::AllKeysLfu => "allkeys-lfu",
            Self::VolatileLfu => "volatile-lfu",
            Self::AllKeysRandom => "allkeys-random",
            Self::VolatileRandom => "volatile-random",
            Self::VolatileTtl => "volatile-ttl",
        }
    }

    pub fn is_lfu(&self) -> bool {
        matches!(self, Self::AllKeysLfu | Self::VolatileLfu)
    }

    fn volatile_only(&self) -> bool {
        matches!(
            self,
            Self::VolatileLru | Self::VolatileLfu | Self::VolatileRandom | Self::VolatileTtl
        )
    }
}

/// Writes one entry of a snapshot. Supplied by the persistence layer (the
/// store knows nothing of the file format): appends `key`'s entry to `out`,
/// preceded by a database marker when `db` differs from `*cur_db`, which it
/// then updates. It may skip the entry (an expired key).
pub type SnapshotEncoder =
    fn(out: &mut Vec<u8>, cur_db: &mut u32, db: u32, key: &[u8], entry: &KeyEntry);

/// Serializes one large value a slice at a time (see "Large values" on
/// [`ShardStore`]). Supplied by the persistence layer, with the bytes that
/// precede the value's first element.
pub trait ValueWriter: Send {
    /// Append up to `budget` more elements of `value` — the same value, not
    /// changed since the writer began — to `out`. Returns how many it wrote,
    /// and whether the value is now complete.
    fn step(&mut self, out: &mut Vec<u8>, value: &RedisValue, budget: usize) -> (usize, bool);
}

/// Begins the sliced serialization of `key`'s entry, if it is large enough
/// to need one: everything up to the value's first element (preceded by a
/// database marker, always), and the writer for the rest. `None`: write it
/// whole with the [`SnapshotEncoder`] (small, or expired).
pub type LargeEncoder =
    fn(db: u32, key: &[u8], entry: &KeyEntry) -> Option<(Vec<u8>, Box<dyn ValueWriter>)>;

/// The encoders of a snapshot.
#[derive(Clone, Copy)]
pub struct SnapshotEncoders {
    pub whole: SnapshotEncoder,
    pub large: Option<LargeEncoder>,
}

/// Elements of a large value serialized per unit of a step's budget (a
/// unit is one whole entry).
const ELEMENTS_PER_UNIT: usize = 16;

/// A large entry being serialized across steps.
struct Pending {
    db: u32,
    key: Bytes,
    writer: Box<dyn ValueWriter>,
}

/// A snapshot being taken in steps (see [`ShardStore::begin_snapshot`]).
struct Snapshot {
    epoch: u32,
    encoder: SnapshotEncoder,
    large: Option<LargeEncoder>,
    /// Serialized entries not yet handed to the snapshot writer.
    out: Vec<u8>,
    /// Database the last entry in the output stream belonged to.
    cur_db: u32,
    /// Databases still to walk, each with its cursor (next position is
    /// `cursor - 1`, walking down).
    walk: Vec<(u32, usize)>,
    /// The large entry in progress, if any (one at a time: the walk waits
    /// for it). Its bytes go straight to `out`, step after step, so no copy
    /// of it builds up here.
    pending: Option<Pending>,
    /// Entries saved by commands while a large entry is in progress: they
    /// follow it into `out` once it is complete, so entries never
    /// interleave.
    side: Vec<u8>,
    /// Database of the last entry in `side` (`u32::MAX`: none yet).
    side_db: u32,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("epoch", &self.epoch)
            .field("walk", &self.walk)
            .field("pending", &self.pending.as_ref().map(|p| (p.db, &p.key)))
            .finish()
    }
}

impl Snapshot {
    /// Whether `key` of `db` is the large entry in progress.
    fn is_pending(&self, db: u32, key: &[u8]) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|p| p.db == db && p.key.as_ref() == key)
    }

    /// Finish the large entry in progress with the rest of `value`.
    fn finish_pending(&mut self, value: &RedisValue) {
        if let Some(mut p) = self.pending.take() {
            let (_, done) = p.writer.step(&mut self.out, value, usize::MAX);
            debug_assert!(done, "an unbounded step completes the value");
            self.pending_done(p.db);
        }
    }

    /// The large entry of `db` is complete in `out`: the entries saved
    /// meanwhile follow it.
    fn pending_done(&mut self, db: u32) {
        self.cur_db = db;
        if !self.side.is_empty() {
            self.out.extend_from_slice(&self.side);
            self.side.clear();
            self.cur_db = self.side_db;
        }
        self.side_db = u32::MAX;
    }

    /// Save one whole entry: to `out`, or to `side` while a large entry is
    /// being sliced into `out`.
    fn encode_whole(&mut self, db: u32, key: &[u8], e: &KeyEntry) {
        if self.pending.is_some() {
            (self.encoder)(&mut self.side, &mut self.side_db, db, key, e);
        } else {
            (self.encoder)(&mut self.out, &mut self.cur_db, db, key, e);
        }
    }
}

/// The keyspace: positional like `IndexMap`, but grows without a pause
/// (see [`IncMap`]).
type KeyMap = IncMap<KeyEntry>;

/// A distinct, non-zero xorshift seed per store.
fn seed() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let mut z = NEXT.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (z ^ (z >> 31)) | 1
}

/// One logical database's keys (`SELECT n`).
#[derive(Debug, Default)]
struct Keyspace {
    data: KeyMap,
    /// Keys that have a TTL — Redis' `expires` dict. Sampled by active
    /// expiry and by the `volatile-*` eviction policies.
    volatile: IncSet,
    /// Keys by cluster hash slot, kept only in cluster mode (database 0):
    /// `CLUSTER COUNTKEYSINSLOT` and `GETKEYSINSLOT`, which resharding calls
    /// once per batch of keys, would otherwise scan the whole keyspace.
    slots: Option<Box<SlotIndex>>,
    /// Keys whose hash has fields with an expiry (`HEXPIRE`): sampled by
    /// active expiry. May hold keys that no longer qualify; the cycle drops
    /// those when it meets them.
    hfe: IncSet,
}

/// See `Keyspace::slots`.
#[derive(Debug)]
struct SlotIndex {
    sets: Vec<Option<Box<IndexSet<Bytes, FastHasher>>>>,
}

impl SlotIndex {
    fn new() -> Self {
        Self {
            sets: (0..16384).map(|_| None).collect(),
        }
    }

    fn add(&mut self, key: &Bytes) {
        let s = crate::keyspace::hash_slot(key) as usize;
        self.sets[s]
            .get_or_insert_with(Default::default)
            .insert(key.clone());
    }

    fn remove(&mut self, key: &[u8]) {
        let s = crate::keyspace::hash_slot(key) as usize;
        if let Some(set) = &mut self.sets[s] {
            set.swap_remove(key);
            if set.is_empty() {
                self.sets[s] = None;
            }
        }
    }

    fn clear(&mut self) {
        for s in self.sets.iter_mut() {
            *s = None;
        }
    }
}

impl Keyspace {
    /// Insert or replace, keeping `volatile` in step. Returns the replaced
    /// entry.
    fn insert(&mut self, key: Bytes, entry: KeyEntry) -> Option<KeyEntry> {
        let has_ttl = entry.expires_at.is_some();
        let (idx, old) = self.data.insert_full(key, entry);
        if old.is_none() {
            if let Some(ix) = &mut self.slots {
                if let Some((k, _)) = self.data.get_index(idx) {
                    ix.add(k);
                }
            }
        }
        let had_ttl = old.as_ref().is_some_and(|o| o.expires_at.is_some());
        if has_ttl && !had_ttl {
            let k = self.data.get_index(idx).map(|(k, _)| k.clone());
            if let Some(k) = k {
                self.volatile.insert(k);
            }
        } else if had_ttl && !has_ttl {
            let k = self.data.get_index(idx).map(|(k, _)| k.clone());
            if let Some(k) = k {
                self.volatile.swap_remove(&k);
            }
        }
        // A hash arriving with field expiry (RENAME, COPY, MOVE, RESTORE, a
        // loaded snapshot): make sure active expiry samples it.
        if let Some((k, e)) = self.data.get_index(idx) {
            if matches!(&e.value, RedisValue::Hash(h) if h.has_field_ttls()) {
                let k = k.clone();
                self.hfe.insert(k);
            }
        }
        old
    }

    fn remove_index(&mut self, idx: usize) -> Option<(Bytes, KeyEntry)> {
        let (k, e) = self.data.swap_remove_index(idx)?;
        if let Some(ix) = &mut self.slots {
            ix.remove(&k);
        }
        if e.expires_at.is_some() {
            self.volatile.swap_remove(&k);
        }
        if matches!(&e.value, RedisValue::Hash(h) if h.has_field_ttls()) {
            self.hfe.swap_remove(&k);
        }
        Some((k, e))
    }

    fn set_expiry(&mut self, idx: usize, at: Option<u64>) {
        let Some((k, e)) = self.data.get_index_mut(idx) else {
            return;
        };
        let had = e.expires_at.is_some();
        e.expires_at = at;
        match (had, at.is_some()) {
            (false, true) => {
                let k = k.clone();
                self.volatile.insert(k);
            }
            (true, false) => {
                let k = k.clone();
                self.volatile.swap_remove(&k);
            }
            _ => {}
        }
    }

    /// See [`ShardStore::active_expire_cycle`].
    fn rehash(&mut self, budget: usize) {
        self.data.rehash(budget);
        self.volatile.rehash(budget);
        self.hfe.rehash(budget);
    }

    fn clear(&mut self) {
        self.data = KeyMap::default();
        self.volatile = IncSet::default();
        if let Some(ix) = &mut self.slots {
            ix.clear();
        }
    }
}

/// Per-shard key-value store. Owned by a single thread — no locking needed.
///
/// Keys live in an [`IncMap`], so SCAN/RANDOMKEY and eviction sampling
/// address them by position in O(1) without cloning the key set, and the
/// keyspace grows without an O(n) pause.
///
/// # Logical databases
///
/// Every shard holds its part of every database. The *selected* one lives in
/// `ks`, where every command method finds it; the others wait in `parked`.
/// [`select`](Self::select) swaps two keyspaces' headers — a few pointers, no
/// rehash — and is a no-op for the database already selected, so a server
/// whose clients never `SELECT` pays one compare per command.
///
/// # Snapshots in steps
///
/// A snapshot does not hold the shard for its whole serialization. It walks
/// the keys a bounded number at a time (see [`snapshot_step`]); in between,
/// commands run. Point-in-time consistency comes from the entries' `snap`
/// epoch: any change to a key the walk has not reached yet — a write, a
/// delete, an overwrite — first serializes the key as it was, and marks it
/// done. Keys created after the snapshot began are marked done from the
/// start. Each key present when it began is therefore written exactly once,
/// with the value it had then.
///
/// # Large values
///
/// One entry is the unit of that walk, so a single huge collection would
/// still hold the shard for its whole serialization. With a
/// [`LargeEncoder`] (see [`begin_snapshot_with`]), such an entry is instead
/// serialized a slice of elements per step, straight into the output that
/// each step hands to the snapshot writer, the walk pausing until it is
/// complete; entries that commands save meanwhile wait in a side buffer
/// and follow it, so entries never interleave. The value is not copied: it
/// only has to stay unchanged until it is done, and the same rule that
/// keeps every other key point-in-time ensures it — anything about to
/// change the entry (a write, a delete, an expiry, a rename) first finishes
/// its serialization as it still is. Reads touch nothing. A write to the
/// huge key while it is being sliced therefore pays for the rest of it, as
/// any first write to a key not yet reached pays for that key; every other
/// key of the shard is served between slices.
///
/// [`snapshot_step`]: Self::snapshot_step
/// [`begin_snapshot_with`]: Self::begin_snapshot_with
#[derive(Debug)]
pub struct ShardStore {
    ks: Keyspace,
    /// Which database `ks` is.
    db: u32,
    /// Number of the next record this shard logs (see
    /// `solikv_persist::AofWriter`): here, beside fields every command
    /// touches, because it is taken under the shard lock on every write.
    pub log_seq: u64,
    /// Every other database, by index; `parked[db]` is an empty placeholder
    /// for the selected one. Grows on demand to the highest database used.
    parked: Vec<Keyspace>,
    /// Keys expired lazily (during get/get_mut) since the owner last drained
    /// this, with the database each was in. Drained by `ShardHandle`, which
    /// logs and announces them.
    pub expired_buffer: Vec<(u32, Bytes)>,
    /// Hash fields expired since the owner last drained this: database,
    /// key, fields. Logged as `HDEL` and announced as `hexpired`.
    pub expired_fields: Vec<(u32, Bytes, Vec<Bytes>)>,
    snapshot: Option<Box<Snapshot>>,
    /// Epoch of the latest snapshot begun (entries' `snap` compare to it).
    snap_epoch: u32,
    rng: u64,
    /// Replica: an expired key reads as missing but stays until the
    /// master's `DEL` arrives, and the active cycle does not run — keys
    /// expire on the master's clock, not on this one (Redis ≥ 3.2).
    replica_expiry: bool,
    /// Until when (unix ms, 0 = not) a `CLIENT PAUSE` holds expiry, shared
    /// by the stores of one server: meanwhile keys expire as on a replica
    /// (read as missing, removed later), as Redis pauses expiry with writes
    /// so that a failover's replica catches up with an unchanging dataset.
    expiry_hold: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
}

impl ShardStore {
    pub fn new() -> Self {
        Self {
            ks: Keyspace::default(),
            db: 0,
            log_seq: 0,
            parked: Vec::new(),
            expired_buffer: Vec::new(),
            expired_fields: Vec::new(),
            snapshot: None,
            snap_epoch: 0,
            rng: seed(),
            replica_expiry: false,
            expiry_hold: None,
        }
    }

    /// See the `replica_expiry` field.
    pub fn set_replica_expiry(&mut self, on: bool) {
        self.replica_expiry = on;
    }

    /// See the `expiry_hold` field.
    pub fn set_expiry_hold(&mut self, hold: std::sync::Arc<std::sync::atomic::AtomicU64>) {
        self.expiry_hold = Some(hold);
    }

    /// Whether an expired key is kept for now (read as missing, not
    /// removed): on a replica, or while a pause holds expiry.
    #[inline]
    fn keeps_expired(&self) -> bool {
        self.replica_expiry
            || self.expiry_hold.as_ref().is_some_and(|h| {
                let until = h.load(std::sync::atomic::Ordering::Relaxed);
                until != 0 && now_millis() < until
            })
    }

    // ---- Cluster slots ----

    /// Keep database 0's keys indexed by hash slot (cluster mode).
    pub fn enable_slot_index(&mut self) {
        self.ensure_parked(0);
        let Some(ks) = self.keyspace_mut(0) else {
            return;
        };
        if ks.slots.is_some() {
            return;
        }
        let mut ix = SlotIndex::new();
        for k in ks.data.keys() {
            ix.add(k);
        }
        ks.slots = Some(Box::new(ix));
    }

    /// Keys of database 0 in `slot` (expired ones not yet reclaimed
    /// included, as Redis counts them).
    pub fn count_in_slot(&self, slot: u16) -> usize {
        let Some(ks) = self.keyspace(0) else { return 0 };
        match &ks.slots {
            Some(ix) => ix.sets[slot as usize % 16384]
                .as_ref()
                .map_or(0, |s| s.len()),
            None => ks
                .data
                .keys()
                .filter(|k| crate::keyspace::hash_slot(k) == slot)
                .count(),
        }
    }

    /// Up to `count` keys of database 0 in `slot`.
    pub fn keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes> {
        let Some(ks) = self.keyspace(0) else {
            return Vec::new();
        };
        match &ks.slots {
            Some(ix) => ix.sets[slot as usize % 16384]
                .as_ref()
                .map(|s| s.iter().take(count).cloned().collect())
                .unwrap_or_default(),
            None => ks
                .data
                .keys()
                .filter(|k| crate::keyspace::hash_slot(k) == slot)
                .take(count)
                .cloned()
                .collect(),
        }
    }

    #[inline]
    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    // ---- Logical databases ----

    /// The selected database.
    #[inline]
    pub fn db(&self) -> u32 {
        self.db
    }

    /// Make `db` the database every command method works on.
    #[inline]
    pub fn select(&mut self, db: u32) {
        if db != self.db {
            self.select_slow(db);
        }
    }

    #[cold]
    fn select_slow(&mut self, db: u32) {
        let cur = self.db as usize;
        let want = db as usize;
        self.ensure_parked(cur.max(want));
        // Park the selected keyspace in its placeholder slot, then take the
        // wanted one out of its slot (which becomes the new placeholder).
        std::mem::swap(&mut self.ks, &mut self.parked[cur]);
        std::mem::swap(&mut self.ks, &mut self.parked[want]);
        self.db = db;
    }

    fn ensure_parked(&mut self, idx: usize) {
        if self.parked.len() <= idx {
            self.parked.resize_with(idx + 1, Keyspace::default);
        }
    }

    /// Highest database index this shard has ever held keys in, plus one.
    pub fn db_span(&self) -> u32 {
        (self.parked.len() as u32).max(self.db + 1)
    }

    fn keyspace(&self, db: u32) -> Option<&Keyspace> {
        if db == self.db {
            Some(&self.ks)
        } else {
            self.parked.get(db as usize)
        }
    }

    fn keyspace_mut(&mut self, db: u32) -> Option<&mut Keyspace> {
        if db == self.db {
            Some(&mut self.ks)
        } else {
            self.parked.get_mut(db as usize)
        }
    }

    /// Keys in `db` (expired ones not yet reclaimed included, like `DBSIZE`).
    pub fn dbsize_of(&self, db: u32) -> usize {
        self.keyspace(db).map_or(0, |k| k.data.len())
    }

    /// Keys in `db` that carry a TTL (`INFO keyspace`'s `expires`).
    pub fn expires_of(&self, db: u32) -> usize {
        self.keyspace(db).map_or(0, |k| k.volatile.len())
    }

    /// Swap the contents of two databases (`SWAPDB`).
    pub fn swap_db(&mut self, a: u32, b: u32) {
        if a == b {
            return;
        }
        // A snapshot walks databases by number: finish both first.
        self.snapshot_capture_db(a);
        self.snapshot_capture_db(b);
        let cur = self.db as usize;
        let hi = a.max(b).max(self.db) as usize;
        self.ensure_parked(hi);
        // Park the selected database so all three live in `parked`, swap,
        // and select the same index again.
        std::mem::swap(&mut self.ks, &mut self.parked[cur]);
        self.parked.swap(a as usize, b as usize);
        std::mem::swap(&mut self.ks, &mut self.parked[cur]);
    }

    /// Move `key` from the selected database to `to` (`MOVE`). False when
    /// the key is missing or `to` already has it.
    pub fn move_key(&mut self, key: &Bytes, to: u32) -> bool {
        if to == self.db || self.get(key).is_none() {
            return false;
        }
        self.ensure_parked(to as usize);
        let target_expired = match self.parked[to as usize].data.get(key) {
            Some(existing) if !existing.is_expired() => return false,
            Some(_) => true,
            None => false,
        };
        if target_expired {
            if let Some(idx) = self.parked[to as usize].data.get_index_of(key) {
                self.capture_parked(to, idx);
                self.parked[to as usize].remove_index(idx);
            }
        }
        let Some(idx) = self.ks.data.get_index_of(key) else {
            return false;
        };
        self.capture(idx);
        let Some((k, entry)) = self.ks.remove_index(idx) else {
            return false;
        };
        let entry = self.fresh(entry);
        self.parked[to as usize].insert(k, entry);
        true
    }

    /// Every entry of every database, with the database it is in.
    pub fn iter_all(&self) -> impl Iterator<Item = (u32, &Bytes, &KeyEntry)> {
        let active = self.db;
        let parked = self
            .parked
            .iter()
            .enumerate()
            .filter(move |(i, _)| *i as u32 != active)
            .flat_map(|(i, ks)| ks.data.iter().map(move |(k, e)| (i as u32, k, e)));
        self.ks
            .data
            .iter()
            .map(move |(k, e)| (active, k, e))
            .chain(parked)
    }

    /// Insert an entry into database `db` (loading).
    pub fn insert_entry_in(&mut self, db: u32, key: Bytes, entry: KeyEntry) {
        self.select(db);
        self.insert_entry(key, entry);
    }

    #[inline]
    fn note_expired(&mut self, key: Bytes) {
        self.expired_buffer.push((self.db, key));
    }

    // ---- Snapshot hooks ----

    /// A new entry: marked as already saved when a snapshot is running (it
    /// did not exist when the snapshot began).
    #[inline]
    fn fresh(&self, mut entry: KeyEntry) -> KeyEntry {
        if self.snapshot.is_some() {
            entry.snap = self.snap_epoch;
        }
        entry
    }

    /// Before the entry at `idx` of the selected database changes or goes:
    /// if a running snapshot has not saved it yet, save it as it is now.
    #[inline]
    fn capture(&mut self, idx: usize) {
        if self.snapshot.is_some() {
            self.capture_slow(self.db, idx);
        }
    }

    fn capture_parked(&mut self, db: u32, idx: usize) {
        if self.snapshot.is_some() {
            self.capture_slow(db, idx);
        }
    }

    #[cold]
    fn capture_slow(&mut self, db: u32, idx: usize) {
        let epoch = self.snap_epoch;
        let Some(snap) = self.snapshot.as_deref_mut() else {
            return;
        };
        let ks = if db == self.db {
            &mut self.ks
        } else {
            match self.parked.get_mut(db as usize) {
                Some(k) => k,
                None => return,
            }
        };
        let Some((k, e)) = ks.data.get_index_mut(idx) else {
            return;
        };
        if e.snap != epoch {
            e.snap = epoch;
            if snap.is_pending(db, k) {
                // The large entry being sliced: finish it as it still is.
                snap.finish_pending(&e.value);
            } else {
                snap.encode_whole(db, k, e);
            }
        }
    }

    /// Save every entry of `db` a running snapshot has not saved yet
    /// (before a whole-database change: FLUSHDB, SWAPDB).
    fn snapshot_capture_db(&mut self, db: u32) {
        if self.snapshot.is_none() {
            return;
        }
        let n = self.keyspace(db).map_or(0, |k| k.data.len());
        for idx in 0..n {
            self.capture_slow(db, idx);
        }
    }

    /// Start a snapshot of every database of this shard, as of now. Take it
    /// with [`snapshot_step`](Self::snapshot_step) until that reports done.
    /// One at a time: returns false if one is already running.
    pub fn begin_snapshot(&mut self, encoder: SnapshotEncoder) -> bool {
        self.begin_snapshot_with(SnapshotEncoders {
            whole: encoder,
            large: None,
        })
    }

    /// [`begin_snapshot`](Self::begin_snapshot), with an encoder for large
    /// values: those are serialized a slice per step instead of whole in
    /// one, so one huge key does not hold the shard for its entire
    /// serialization (see "Large values" on [`ShardStore`]).
    pub fn begin_snapshot_with(&mut self, encoders: SnapshotEncoders) -> bool {
        if self.snapshot.is_some() {
            return false;
        }
        self.snap_epoch = self.snap_epoch.wrapping_add(1).max(1);
        let span = self.db_span();
        let mut walk = Vec::new();
        for db in (0..span).rev() {
            let n = self.dbsize_of(db);
            if n > 0 {
                walk.push((db, n));
            }
        }
        self.snapshot = Some(Box::new(Snapshot {
            epoch: self.snap_epoch,
            encoder: encoders.whole,
            large: encoders.large,
            out: Vec::new(),
            cur_db: 0,
            walk,
            pending: None,
            side: Vec::new(),
            side_db: u32::MAX,
        }));
        true
    }

    /// Serialize up to `budget` more entries of the running snapshot.
    /// Returns what was serialized since the last step (entries the walk
    /// reached, and entries commands saved before changing them), and
    /// whether the snapshot is complete — in which case it has ended.
    pub fn snapshot_step(&mut self, budget: usize) -> (Vec<u8>, bool) {
        let Some(mut snap) = self.snapshot.take() else {
            return (Vec::new(), true);
        };
        let mut left = budget.max(1);
        while left > 0 {
            // A large entry in progress goes first; the walk waits for it.
            if let Some(p) = snap.pending.as_mut() {
                let ks = if p.db == self.db {
                    &mut self.ks
                } else {
                    &mut self.parked[p.db as usize]
                };
                let Some(idx) = ks.data.get_index_of(&p.key) else {
                    // Every change to the entry finishes it first (see
                    // `capture_slow`), so it cannot be gone.
                    debug_assert!(false, "large snapshot entry vanished");
                    snap.pending = None;
                    continue;
                };
                let e = &mut ks.data[idx];
                let (used, done) = p.writer.step(
                    &mut snap.out,
                    &e.value,
                    left.saturating_mul(ELEMENTS_PER_UNIT),
                );
                left = left.saturating_sub(used.div_ceil(ELEMENTS_PER_UNIT).max(1));
                if !done {
                    break;
                }
                e.snap = snap.epoch;
                let db = p.db;
                snap.pending = None;
                snap.pending_done(db);
                continue;
            }
            let Some(&(db, cursor)) = snap.walk.last() else {
                break;
            };
            let ks = if db == self.db {
                &mut self.ks
            } else {
                match self.parked.get_mut(db as usize) {
                    Some(k) => k,
                    None => {
                        snap.walk.pop();
                        continue;
                    }
                }
            };
            // Walk down from the cursor: an entry only ever moves down (a
            // swap-remove fills a hole with the last entry), so nothing
            // present at the start can slip past the walk.
            let mut c = cursor.min(ks.data.len());
            while c > 0 && left > 0 {
                c -= 1;
                left -= 1;
                let Some((k, e)) = ks.data.get_index_mut(c) else {
                    continue;
                };
                if e.snap == snap.epoch || snap.is_pending(db, k) {
                    continue;
                }
                if let Some((head, writer)) = snap.large.and_then(|large| large(db, k, e)) {
                    // Sliced from the next iteration on, the walk paused.
                    snap.out.extend_from_slice(&head);
                    snap.cur_db = db;
                    snap.pending = Some(Pending {
                        db,
                        key: k.clone(),
                        writer,
                    });
                    break;
                }
                e.snap = snap.epoch;
                (snap.encoder)(&mut snap.out, &mut snap.cur_db, db, k, e);
            }
            if c == 0 {
                snap.walk.pop();
            } else if let Some(last) = snap.walk.last_mut() {
                last.1 = c;
            }
        }
        let out = std::mem::take(&mut snap.out);
        let done = snap.walk.is_empty() && snap.pending.is_none();
        if !done {
            self.snapshot = Some(snap);
        }
        (out, done)
    }

    /// Drop a running snapshot (its writer failed).
    pub fn abort_snapshot(&mut self) {
        self.snapshot = None;
    }

    pub fn snapshot_running(&self) -> bool {
        self.snapshot.is_some()
    }

    // ---- Lookups ----

    /// Get a reference to an entry, performing lazy expiry check.
    #[inline]
    pub fn get(&mut self, key: &Bytes) -> Option<&KeyEntry> {
        // Single index lookup instead of get + get after expiry.
        let idx = self.ks.data.get_index_of(key)?;
        if self.ks.data[idx].is_expired() {
            self.expire_index(idx);
            return None;
        }
        let e = &mut self.ks.data[idx];
        if access::touching() {
            e.access = access::touched(e.access);
        }
        Some(e)
    }

    /// Get a mutable reference to an entry, performing lazy expiry check.
    #[inline]
    pub fn get_mut(&mut self, key: &Bytes) -> Option<&mut KeyEntry> {
        let idx = self.ks.data.get_index_of(key)?;
        if self.ks.data[idx].is_expired() {
            self.expire_index(idx);
            return None;
        }
        self.capture(idx);
        let e = &mut self.ks.data[idx];
        if access::touching() {
            e.access = access::touched(e.access);
        }
        Some(e)
    }

    fn expire_index(&mut self, idx: usize) {
        if self.keeps_expired() {
            return;
        }
        self.capture(idx);
        if let Some((k, _)) = self.ks.remove_index(idx) {
            self.note_expired(k);
        }
    }

    /// Replace (or create) `key`'s entry.
    fn put(&mut self, key: Bytes, entry: KeyEntry) {
        if self.snapshot.is_some() {
            if let Some(idx) = self.ks.data.get_index_of(&key) {
                self.capture_slow(self.db, idx);
            }
        }
        let entry = self.fresh(entry);
        self.ks.insert(key, entry);
    }

    /// Set a key-value pair with optional expiry in milliseconds.
    pub fn set(&mut self, key: Bytes, value: RedisValue, expire_ms: Option<u64>) {
        let entry = match expire_ms {
            Some(ms) => KeyEntry::with_expiry(value, now_millis().saturating_add(ms)),
            None => KeyEntry::new(value),
        };
        self.put(key, entry);
    }

    /// Set a key-value pair with an absolute expiry timestamp in milliseconds.
    pub fn set_with_absolute_expiry(&mut self, key: Bytes, value: RedisValue, expires_at: u64) {
        self.put(key, KeyEntry::with_expiry(value, expires_at));
    }

    /// Replace `key`'s value, keeping its TTL if it has a live one (what
    /// every in-place modification — `APPEND`, `INCR`, `SETRANGE`, `SETBIT`
    /// … — must do; re-inserting with no expiry made them drop it).
    pub fn set_keep_ttl(&mut self, key: &Bytes, value: RedisValue) {
        if let Some(entry) = self.get_mut(key) {
            entry.value = value;
        } else {
            self.put(key.clone(), KeyEntry::new(value));
        }
    }

    /// Insert a KeyEntry directly.
    pub fn insert_entry(&mut self, key: Bytes, entry: KeyEntry) {
        self.put(key, entry);
    }

    /// Delete a key. Returns true if the key existed.
    pub fn del(&mut self, key: &Bytes) -> bool {
        self.take(key).is_some()
    }

    /// Remove `key`'s entry, live or not.
    fn take(&mut self, key: &[u8]) -> Option<(Bytes, KeyEntry)> {
        let idx = self.ks.data.get_index_of(key)?;
        self.capture(idx);
        self.ks.remove_index(idx)
    }

    /// Check if a key exists (non-expired).
    pub fn exists(&mut self, key: &Bytes) -> bool {
        self.get(key).is_some()
    }

    /// Give a live key the absolute deadline `at` (ms); false when missing.
    fn set_deadline(&mut self, key: &Bytes, at: Option<u64>) -> bool {
        let Some(idx) = self.ks.data.get_index_of(key) else {
            return false;
        };
        if self.ks.data[idx].is_expired() {
            self.expire_index(idx);
            return false;
        }
        self.capture(idx);
        self.ks.set_expiry(idx, at);
        true
    }

    /// Set expiry on existing key. Returns true if key exists.
    pub fn expire(&mut self, key: &Bytes, expire_ms: u64) -> bool {
        self.set_deadline(key, Some(now_millis().saturating_add(expire_ms)))
    }

    /// Set absolute expiry on existing key. Returns true if key exists.
    pub fn expire_at(&mut self, key: &Bytes, expires_at_ms: u64) -> bool {
        self.set_deadline(key, Some(expires_at_ms))
    }

    /// `EXPIRE`-family: give `key` the absolute deadline `at` (ms) if the
    /// condition allows. A deadline already passed deletes the key. For
    /// GT/LT a key without a TTL counts as an infinite one, as in Redis.
    /// True when the key existed and the condition held.
    pub fn expire_cond(&mut self, key: &Bytes, at: u64, c: ExpireCond) -> bool {
        let current = match self.get(key) {
            None => return false,
            Some(e) => e.expires_at,
        };
        if (c.nx && current.is_some()) || (c.xx && current.is_none()) {
            return false;
        }
        if c.gt && current.is_none_or(|cur| at <= cur) {
            return false;
        }
        if c.lt && current.is_some_and(|cur| at >= cur) {
            return false;
        }
        if at <= now_millis() {
            self.take(key);
        } else {
            self.expire_at(key, at);
        }
        true
    }

    /// Remove the expiry from a key. Returns true if key exists.
    pub fn persist(&mut self, key: &Bytes) -> bool {
        let Some(idx) = self.ks.data.get_index_of(key) else {
            return false;
        };
        if self.ks.data[idx].is_expired() {
            self.expire_index(idx);
            return false;
        }
        if self.ks.data[idx].expires_at.is_none() {
            return false;
        }
        self.capture(idx);
        self.ks.set_expiry(idx, None);
        true
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
            (pttl + 999) / 1000 // ceiling division
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
        self.ks.data.len()
    }

    /// Get all keys matching a glob pattern.
    pub fn keys(&mut self, pattern: &str) -> Vec<Bytes> {
        let mut out = Vec::new();
        for (k, e) in self.ks.data.iter() {
            if e.is_expired() {
                continue;
            }
            let key_str = std::str::from_utf8(k).unwrap_or("");
            if glob_match(pattern, key_str) {
                out.push(k.clone());
            }
        }
        out
    }

    /// SCAN implementation.
    ///
    /// The cursor is a position in the key map, walked from the end down:
    /// `0` starts at the last key, and the cursor returned is where the next
    /// call continues (`0` once the walk reached the first key). Removing a
    /// key moves the *last* key into its hole — a position the walk has
    /// already been through — so a key present for the whole iteration is
    /// always returned (possibly twice), the guarantee Redis' SCAN makes.
    /// A walk upwards, as this was, could miss it.
    pub fn scan(
        &mut self,
        cursor: usize,
        pattern: Option<&str>,
        count: usize,
    ) -> (usize, Vec<Bytes>) {
        self.scan_typed(cursor, pattern, None, count)
    }

    /// [`scan`](Self::scan) with `SCAN … TYPE t`: only keys of that type
    /// (`string`, `list`, …) are returned; the others still consume `count`.
    pub fn scan_typed(
        &mut self,
        cursor: usize,
        pattern: Option<&str>,
        type_filter: Option<&str>,
        count: usize,
    ) -> (usize, Vec<Bytes>) {
        let count = count.max(1);
        let len = self.ks.data.len();
        let mut c = if cursor == 0 { len } else { cursor.min(len) };
        let mut result = Vec::with_capacity(count.min(len));
        let mut deferred_expire: Vec<Bytes> = Vec::new();
        let mut examined = 0;
        while c > 0 && examined < count {
            c -= 1;
            examined += 1;
            let (k, entry) = self.ks.data.get_index(c).expect("c < len");
            if entry.is_expired() {
                if !self.keeps_expired() {
                    deferred_expire.push(k.clone());
                }
                continue;
            }
            if !type_filter.is_none_or(|t| t.eq_ignore_ascii_case(entry.type_name())) {
                continue;
            }
            let matches =
                pattern.is_none_or(|p| glob_match(p, std::str::from_utf8(k).unwrap_or("")));
            if matches {
                result.push(k.clone());
            }
        }
        // Expired keys go after the walk: removing them moves entries this
        // call has already passed, never ones still ahead of the cursor.
        for key in deferred_expire {
            if let Some((k, _)) = self.take(&key) {
                self.note_expired(k);
            }
        }
        (c, result)
    }

    /// Delete `key` if it is (still) there, reporting whether it was live.
    /// An expired key counts as absent but is reported through
    /// `expired_buffer`, like any other lazy expiry.
    pub fn remove_live(&mut self, key: &Bytes) -> Option<KeyEntry> {
        let (k, entry) = self.take(key)?;
        if entry.is_expired() {
            self.note_expired(k);
            return None;
        }
        Some(entry)
    }

    /// Empty the selected database (`FLUSHDB`).
    pub fn flush(&mut self) {
        self.snapshot_capture_db(self.db);
        self.ks.clear();
    }

    /// Empty every database (`FLUSHALL`).
    pub fn flush_all(&mut self) {
        for db in 0..self.db_span() {
            self.snapshot_capture_db(db);
        }
        self.ks.clear();
        self.parked.clear();
    }

    /// One active-expiry cycle, the way Redis runs `activeExpireCycle`: in
    /// each database, sample [`EXPIRE_SAMPLE`] keys that have a TTL, delete
    /// the expired ones, and sample again while more than a quarter of a
    /// sample had expired — until `budget` is spent. Returns the keys
    /// removed, with the database each was in.
    pub fn active_expire_cycle(&mut self, budget: Duration) -> Vec<(u32, Bytes)> {
        let mut removed = Vec::new();
        // Idle time also moves index growth along (inserts drive it too):
        // a lookup probes two tables until a migration ends.
        for db in 0..self.db_span() {
            if let Some(ks) = self.keyspace_mut(db) {
                ks.rehash(REHASH_PER_CYCLE);
            }
        }
        // Loading: nothing expires (see `LoadingGuard`); nor on a replica.
        if is_loading() || self.keeps_expired() {
            return removed;
        }
        let start = Instant::now();
        let span = self.db_span();
        'dbs: for db in 0..span {
            while let Some(ks) = self.keyspace(db) {
                let n = ks.volatile.len();
                if n == 0 {
                    break;
                }
                let now = now_millis();
                let sample = EXPIRE_SAMPLE.min(n);
                let mut expired_here = 0;
                for _ in 0..sample {
                    let r = self.next_rand() as usize;
                    let Some(ks) = self.keyspace(db) else { break };
                    if ks.volatile.is_empty() {
                        break;
                    }
                    let key = ks.volatile.get_index(r % ks.volatile.len()).cloned();
                    let Some(key) = key else { break };
                    let Some(idx) = ks.data.get_index_of(&key) else {
                        // Out of step (cannot happen); repair.
                        if let Some(ks) = self.keyspace_mut(db) {
                            ks.volatile.swap_remove(&key);
                        }
                        continue;
                    };
                    if ks.data[idx].expires_at.is_some_and(|at| at <= now) {
                        self.capture_parked(db, idx);
                        if let Some(ks) = self.keyspace_mut(db) {
                            if let Some((k, _)) = ks.remove_index(idx) {
                                removed.push((db, k));
                                expired_here += 1;
                            }
                        }
                    }
                }
                if start.elapsed() >= budget {
                    break 'dbs;
                }
                if expired_here * 4 <= sample {
                    break;
                }
            }
        }
        if start.elapsed() < budget {
            self.active_expire_fields(start, budget);
        }
        removed
    }

    // ---- Hash field expiry ----

    /// Position of `key` in the selected database, expiring it first if its
    /// TTL has passed (as `get` does).
    pub(crate) fn index_of_live(&mut self, key: &Bytes) -> Option<usize> {
        let idx = self.ks.data.get_index_of(key)?;
        if self.ks.data[idx].is_expired() {
            self.expire_index(idx);
            return None;
        }
        Some(idx)
    }

    pub(crate) fn entry_value(&self, idx: usize) -> &RedisValue {
        &self.ks.data[idx].value
    }

    /// Mutable value at `idx`: call [`capture_index`](Self::capture_index)
    /// first, so a running snapshot keeps the old version.
    pub(crate) fn entry_value_mut(&mut self, idx: usize) -> &mut RedisValue {
        &mut self.ks.data[idx].value
    }

    pub(crate) fn capture_index(&mut self, idx: usize) {
        self.capture(idx);
    }

    /// Note that `key`'s hash now has a field with an expiry, so active
    /// expiry samples it.
    pub(crate) fn note_field_ttl(&mut self, key: &Bytes) {
        self.ks.hfe.insert(key.clone());
    }

    /// Expire `key`'s due hash fields, in database `db`: on access, before
    /// a hash command reads or changes it, and from the active cycle. The
    /// fields go to `expired_fields` (logged as `HDEL`); a hash left empty
    /// is removed, as `HDEL` of its last field would. Nothing expires while
    /// loading or on a replica (the master's `HDEL`s do it).
    pub(crate) fn purge_hash_fields_in(&mut self, db: u32, key: &Bytes) {
        if is_loading() || self.keeps_expired() {
            return;
        }
        let now = now_millis();
        let Some(ks) = self.keyspace(db) else { return };
        let Some(idx) = ks.data.get_index_of(key) else {
            return;
        };
        if !matches!(&ks.data[idx].value, RedisValue::Hash(h) if h.needs_purge(now)) {
            return;
        }
        self.capture_parked(db, idx);
        let Some(ks) = self.keyspace_mut(db) else {
            return;
        };
        let RedisValue::Hash(h) = &mut ks.data[idx].value else {
            return;
        };
        let gone = h.purge_expired(now);
        let (empty, has_ttls) = (h.is_empty(), h.has_field_ttls());
        if !has_ttls {
            ks.hfe.swap_remove(key);
        }
        if empty {
            ks.remove_index(idx);
        }
        if !gone.is_empty() {
            self.expired_fields.push((db, key.clone(), gone));
        }
    }

    /// One pass of field expiry over the hashes with field TTLs in each
    /// database: like the key cycle, sample, and go on while a quarter or
    /// more of the sample had something due.
    fn active_expire_fields(&mut self, start: Instant, budget: Duration) {
        let span = self.db_span();
        for db in 0..span {
            while let Some(ks) = self.keyspace(db) {
                let n = ks.hfe.len();
                if n == 0 {
                    break;
                }
                let sample = EXPIRE_SAMPLE.min(n);
                let before = self.expired_fields.len();
                for _ in 0..sample {
                    let r = self.next_rand() as usize;
                    let Some(ks) = self.keyspace(db) else { break };
                    if ks.hfe.is_empty() {
                        break;
                    }
                    let Some(key) = ks.hfe.get_index(r % ks.hfe.len()).cloned() else {
                        break;
                    };
                    let live = ks.data.get(&key).is_some_and(
                        |e| matches!(&e.value, RedisValue::Hash(h) if h.has_field_ttls()),
                    );
                    if !live {
                        if let Some(ks) = self.keyspace_mut(db) {
                            ks.hfe.swap_remove(&key);
                        }
                        continue;
                    }
                    self.purge_hash_fields_in(db, &key);
                }
                let due = self.expired_fields.len() - before;
                if start.elapsed() >= budget || due * 4 <= sample {
                    break;
                }
            }
        }
    }

    /// Run active expiry: one cycle with a 1 ms budget (tests; the server
    /// runs [`active_expire_cycle`](Self::active_expire_cycle) on a timer).
    pub fn run_active_expiry(&mut self) -> Vec<(u32, Bytes)> {
        self.active_expire_cycle(Duration::from_millis(1))
    }

    // ---- Eviction ----

    /// Keys this shard can evict under `policy` (all keys, or those with a
    /// TTL), across its databases.
    pub fn evictable(&self, policy: EvictionPolicy) -> usize {
        let per = |k: &Keyspace| {
            if policy.volatile_only() {
                k.volatile.len()
            } else {
                k.data.len()
            }
        };
        per(&self.ks)
            + self
                .parked
                .iter()
                .enumerate()
                .filter(|(i, _)| *i as u32 != self.db)
                .map(|(_, k)| per(k))
                .sum::<usize>()
    }

    /// Evict one key chosen as Redis does: sample `samples` candidates and
    /// take the best by the policy (idlest, least frequently used, nearest
    /// to expiring, or any). Returns it with its database, or `None` when
    /// nothing is evictable.
    pub fn evict_one(&mut self, policy: EvictionPolicy, samples: usize) -> Option<(u32, Bytes)> {
        if policy == EvictionPolicy::NoEviction {
            return None;
        }
        let dbs: Vec<u32> = (0..self.db_span())
            .filter(|&db| {
                self.keyspace(db).is_some_and(|k| {
                    if policy.volatile_only() {
                        !k.volatile.is_empty()
                    } else {
                        !k.data.is_empty()
                    }
                })
            })
            .collect();
        if dbs.is_empty() {
            return None;
        }
        // (score: higher = better to evict, db, key)
        let mut best: Option<(u64, u32, Bytes)> = None;
        let samples = samples.max(1);
        for _ in 0..samples {
            let db = dbs[(self.next_rand() as usize) % dbs.len()];
            let r = self.next_rand() as usize;
            let ks = self.keyspace(db)?;
            let (key, entry) = if policy.volatile_only() {
                let k = ks.volatile.get_index(r % ks.volatile.len())?;
                match ks.data.get(k) {
                    Some(e) => (k, e),
                    None => continue,
                }
            } else {
                let (k, e) = ks.data.get_index(r % ks.data.len())?;
                (k, e)
            };
            let score = match policy {
                EvictionPolicy::AllKeysLru | EvictionPolicy::VolatileLru => {
                    access::idle_secs(entry.access)
                }
                EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu => {
                    255 - access::decayed(entry.access) as u64
                }
                EvictionPolicy::VolatileTtl => u64::MAX - entry.expires_at.unwrap_or(u64::MAX),
                _ => 0,
            };
            if best.as_ref().is_none_or(|(s, _, _)| score > *s) {
                best = Some((score, db, key.clone()));
            }
            if matches!(
                policy,
                EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom
            ) {
                break;
            }
        }
        let (_, db, key) = best?;
        let idx = self.keyspace(db)?.data.get_index_of(&key)?;
        self.capture_parked(db, idx);
        self.keyspace_mut(db)?.remove_index(idx);
        Some((db, key))
    }

    /// `TOUCH`: refresh a live key's LRU/LFU (whatever `CLIENT NO-TOUCH`
    /// says — `TOUCH` is the exception Redis makes). False when missing.
    pub fn touch_key(&mut self, key: &Bytes) -> bool {
        let Some(idx) = self.index_of_live(key) else {
            return false;
        };
        let e = &mut self.ks.data[idx];
        e.access = access::touched(e.access);
        true
    }

    /// `OBJECT IDLETIME` / `OBJECT FREQ` source: the key's access field.
    pub fn access_of(&mut self, key: &Bytes) -> Option<u32> {
        let idx = self.ks.data.get_index_of(key)?;
        if self.ks.data[idx].is_expired() {
            self.expire_index(idx);
            return None;
        }
        Some(self.ks.data[idx].access)
    }

    /// Rename a key. Returns error message if source doesn't exist.
    pub fn rename(&mut self, from: &Bytes, to: Bytes) -> Result<(), &'static str> {
        // Check if 'from' exists (with lazy expiry)
        if self.get(from).is_none() {
            return Err("ERR no such key");
        }
        let (_, entry) = self.take(from).expect("checked above");
        self.put(to, entry);
        Ok(())
    }

    /// Get a pseudo-random key in O(1) by position.
    pub fn random_key(&mut self) -> Option<Bytes> {
        let len = self.ks.data.len();
        if len == 0 {
            return None;
        }
        let idx = (self.next_rand() as usize) % len;
        self.ks.data.get_index(idx).map(|(k, _)| k.clone())
    }

    /// The live entry for `key`, without the lazy-expiry side effect (for
    /// read-only callers such as persistence, which hold `&self`).
    /// [`peek`](Self::peek) in database `db`, without selecting it.
    pub fn peek_in(&self, db: u32, key: &[u8]) -> Option<&KeyEntry> {
        self.keyspace(db)?.data.get(key).filter(|e| !e.is_expired())
    }

    pub fn peek(&self, key: &[u8]) -> Option<&KeyEntry> {
        self.ks.data.get(key).filter(|e| !e.is_expired())
    }

    /// Remove and return every entry of every database, with its database,
    /// leaving the store empty. Used to move a dataset replayed into a
    /// scratch store over to the shards that own it.
    pub fn drain_entries(&mut self) -> Vec<(u32, Bytes, KeyEntry)> {
        let db = self.db;
        let ks = std::mem::take(&mut self.ks);
        let mut out: Vec<(u32, Bytes, KeyEntry)> =
            ks.data.into_iter().map(|(k, e)| (db, k, e)).collect();
        for (i, ks) in std::mem::take(&mut self.parked).into_iter().enumerate() {
            out.extend(ks.data.into_iter().map(|(k, e)| (i as u32, k, e)));
        }
        out
    }

    /// Get all data for persistence/snapshotting.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &KeyEntry)> {
        self.ks.data.iter()
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

    fn s(v: &str) -> RedisValue {
        RedisValue::String(Bytes::from(v.to_string()))
    }

    fn b(v: &str) -> Bytes {
        Bytes::from(v.to_string())
    }

    #[test]
    fn test_set_get() {
        let mut store = ShardStore::new();
        store.set(b("key1"), s("val1"), None);
        let entry = store.get(&b("key1")).unwrap();
        match &entry.value {
            RedisValue::String(v) => assert_eq!(v, &b("val1")),
            _ => panic!("Expected string"),
        }
    }

    #[test]
    fn test_del() {
        let mut store = ShardStore::new();
        store.set(b("key1"), s("val1"), None);
        assert!(store.del(&b("key1")));
        assert!(!store.del(&b("key1")));
        assert!(store.get(&b("key1")).is_none());
    }

    #[test]
    fn test_exists() {
        let mut store = ShardStore::new();
        store.set(b("key1"), s("val1"), None);
        assert!(store.exists(&b("key1")));
        assert!(!store.exists(&b("key2")));
    }

    #[test]
    fn test_expire_and_ttl() {
        let mut store = ShardStore::new();
        store.set(b("key1"), s("val1"), Some(5000));
        let ttl = store.pttl(&b("key1"));
        assert!(ttl > 0 && ttl <= 5000);
        assert_eq!(store.expires_of(0), 1);
    }

    #[test]
    fn test_lazy_expiry() {
        let mut store = ShardStore::new();
        store.set_with_absolute_expiry(b("key1"), s("val1"), 1); // long past
        assert!(store.get(&b("key1")).is_none());
        assert_eq!(store.expired_buffer, vec![(0, b("key1"))]);
        assert_eq!(store.expires_of(0), 0);
    }

    #[test]
    fn test_persist() {
        let mut store = ShardStore::new();
        store.set(b("key1"), s("val1"), Some(5000));
        assert!(store.persist(&b("key1")));
        assert_eq!(store.pttl(&b("key1")), -1);
        assert_eq!(store.expires_of(0), 0);
    }

    #[test]
    fn volatile_index_follows_every_change() {
        let mut store = ShardStore::new();
        store.set(b("a"), s("1"), Some(10_000));
        store.set(b("b"), s("1"), None);
        assert_eq!(store.expires_of(0), 1);
        // Overwrite without a TTL drops it from the index.
        store.set(b("a"), s("2"), None);
        assert_eq!(store.expires_of(0), 0);
        assert!(store.expire(&b("b"), 10_000));
        assert_eq!(store.expires_of(0), 1);
        store.rename(&b("b"), b("c")).unwrap();
        assert_eq!(store.expires_of(0), 1);
        assert!(store.pttl(&b("c")) > 0);
        assert!(store.del(&b("c")));
        assert_eq!(store.expires_of(0), 0);
    }

    #[test]
    fn test_key_type() {
        let mut store = ShardStore::new();
        store.set(b("s"), s("v"), None);
        assert_eq!(store.key_type(&b("s")), "string");
        assert_eq!(store.key_type(&b("x")), "none");
    }

    #[test]
    fn test_rename() {
        let mut store = ShardStore::new();
        store.set(b("a"), s("v"), None);
        assert!(store.rename(&b("a"), b("b")).is_ok());
        assert!(store.get(&b("a")).is_none());
        assert!(store.get(&b("b")).is_some());
    }

    #[test]
    fn test_rename_nonexistent() {
        let mut store = ShardStore::new();
        assert!(store.rename(&b("a"), b("b")).is_err());
    }

    #[test]
    fn test_dbsize() {
        let mut store = ShardStore::new();
        assert_eq!(store.dbsize(), 0);
        store.set(b("a"), s("1"), None);
        store.set(b("b"), s("2"), None);
        assert_eq!(store.dbsize(), 2);
    }

    #[test]
    fn test_flush() {
        let mut store = ShardStore::new();
        store.set(b("a"), s("1"), Some(1000));
        store.flush();
        assert_eq!(store.dbsize(), 0);
        assert_eq!(store.expires_of(0), 0);
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
        store.set(b("user:1"), s("a"), None);
        store.set(b("user:2"), s("b"), None);
        store.set(b("admin:1"), s("c"), None);
        let keys = store.keys("user:*");
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_scan() {
        let mut store = ShardStore::new();
        for i in 0..20 {
            store.set(b(&format!("key:{}", i)), s("v"), None);
        }
        let mut all_keys = Vec::new();
        let mut cursor = 0;
        loop {
            let (next_cursor, batch) = store.scan(cursor, None, 10);
            all_keys.extend(batch);
            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(all_keys.len(), 20);
    }

    #[test]
    fn test_scan_does_not_require_full_clone_for_partial() {
        let mut store = ShardStore::new();
        for i in 0..100 {
            store.set(b(&format!("k{i}")), s("v"), None);
        }
        let (next, batch) = store.scan(0, None, 5);
        assert_eq!(batch.len(), 5);
        assert_eq!(next, 95);
    }

    /// Redis' guarantee: a key present from the first call to the last is
    /// returned, whatever else is deleted or added in between.
    #[test]
    fn scan_returns_every_key_present_throughout_despite_deletes() {
        let mut store = ShardStore::new();
        for i in 0..2000 {
            store.set(b(&format!("k{i}")), s("v"), None);
        }
        let stable: Vec<Bytes> = (0..2000)
            .filter(|i| i % 3 == 0)
            .map(|i| b(&format!("k{i}")))
            .collect();
        let mut seen = std::collections::HashSet::new();
        let mut cursor = 0;
        let mut round = 0;
        loop {
            let (next, batch) = store.scan(cursor, None, 37);
            seen.extend(batch);
            // Between calls: delete keys (never the stable ones), add some.
            for j in 0..25 {
                let victim = round * 25 + j;
                if victim % 3 != 0 {
                    store.del(&b(&format!("k{victim}")));
                }
                store.set(b(&format!("new{round}-{j}")), s("v"), None);
            }
            round += 1;
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        for k in &stable {
            assert!(seen.contains(k), "SCAN missed {:?}", k);
        }
    }

    #[test]
    fn test_random_key_o1() {
        let mut store = ShardStore::new();
        assert!(store.random_key().is_none());
        store.set(b("only"), s("v"), None);
        assert_eq!(store.random_key().unwrap(), b("only"));
    }

    #[test]
    fn active_expiry_reclaims_without_any_access() {
        let mut store = ShardStore::new();
        for i in 0..5000 {
            store.set_with_absolute_expiry(b(&format!("e{i}")), s("v"), 1);
        }
        for i in 0..100 {
            store.set(b(&format!("live{i}")), s("v"), Some(1_000_000));
        }
        let mut removed = 0;
        for _ in 0..200 {
            removed += store.active_expire_cycle(Duration::from_millis(50)).len();
            if store.dbsize() == 100 {
                break;
            }
        }
        assert_eq!(removed, 5000);
        assert_eq!(store.dbsize(), 100);
        assert_eq!(store.expires_of(0), 100);
    }

    #[test]
    fn eviction_prefers_by_policy() {
        let mut store = ShardStore::new();
        store.set(b("persistent"), s("v"), None);
        store.set(b("soon"), s("v"), Some(1_000));
        store.set(b("later"), s("v"), Some(1_000_000));
        // volatile-ttl never picks a key without a TTL, and prefers the one
        // expiring first when both are sampled.
        let mut picked_soon = 0;
        for _ in 0..50 {
            let mut st = ShardStore::new();
            st.set(b("persistent"), s("v"), None);
            st.set(b("soon"), s("v"), Some(1_000));
            st.set(b("later"), s("v"), Some(1_000_000));
            let (_, k) = st.evict_one(EvictionPolicy::VolatileTtl, 10).unwrap();
            assert_ne!(k, b("persistent"));
            if k == b("soon") {
                picked_soon += 1;
            }
        }
        assert!(picked_soon > 40);
        assert!(store.evict_one(EvictionPolicy::NoEviction, 5).is_none());
        // allkeys-random empties everything.
        while store.evict_one(EvictionPolicy::AllKeysRandom, 5).is_some() {}
        assert_eq!(store.dbsize(), 0);
        assert_eq!(store.expires_of(0), 0);
    }

    fn encode_for_test(out: &mut Vec<u8>, cur_db: &mut u32, db: u32, key: &[u8], e: &KeyEntry) {
        if db != *cur_db {
            out.extend_from_slice(format!("[db{db}]").as_bytes());
            *cur_db = db;
        }
        if let RedisValue::String(v) = &e.value {
            out.extend_from_slice(key);
            out.push(b'=');
            out.extend_from_slice(v);
            out.push(b';');
        }
    }

    fn parse_snapshot(bytes: &[u8]) -> std::collections::BTreeMap<String, String> {
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        let mut db = 0;
        let mut out = std::collections::BTreeMap::new();
        for part in text.split(';').filter(|p| !p.is_empty()) {
            let mut p = part;
            while let Some(rest) = p.strip_prefix("[db") {
                let end = rest.find(']').unwrap();
                db = rest[..end].parse::<u32>().unwrap();
                p = &rest[end + 1..];
            }
            let (k, v) = p.split_once('=').unwrap();
            assert!(
                out.insert(format!("{db}/{k}"), v.to_string()).is_none(),
                "{k} twice"
            );
        }
        out
    }

    /// A snapshot taken in small steps, with writes, deletes, overwrites
    /// and new keys between the steps, holds exactly the dataset as it was
    /// when it began.
    #[test]
    fn stepped_snapshot_is_point_in_time() {
        let mut store = ShardStore::new();
        let mut expect = std::collections::BTreeMap::new();
        for db in [0u32, 3] {
            store.select(db);
            for i in 0..3000 {
                store.set(b(&format!("k{i}")), s(&format!("v{i}")), None);
                expect.insert(format!("{db}/k{i}"), format!("v{i}"));
            }
        }
        store.select(0);
        assert!(store.begin_snapshot(encode_for_test));
        assert!(!store.begin_snapshot(encode_for_test));
        let mut out = Vec::new();
        let mut round = 0;
        loop {
            let (chunk, done) = store.snapshot_step(97);
            out.extend(chunk);
            if done {
                break;
            }
            // Churn between steps, in both databases.
            for db in [0u32, 3] {
                store.select(db);
                for j in 0..20 {
                    let i = (round * 53 + j * 7) % 3000;
                    match j % 4 {
                        0 => {
                            store.del(&b(&format!("k{i}")));
                        }
                        1 => store.set(b(&format!("k{i}")), s("changed"), None),
                        2 => store.set(b(&format!("fresh{round}-{j}")), s("x"), None),
                        _ => {
                            store.set_keep_ttl(&b(&format!("k{i}")), s("kept"));
                        }
                    }
                }
            }
            store.select(0);
            round += 1;
        }
        assert!(!store.snapshot_running());
        assert_eq!(parse_snapshot(&out), expect);
    }

    /// Test encoders: lists as `key=a,b,c;`, sliced from 10 elements.
    fn encode_list_for_test(
        out: &mut Vec<u8>,
        cur_db: &mut u32,
        db: u32,
        key: &[u8],
        e: &KeyEntry,
    ) {
        if let RedisValue::List(l) = &e.value {
            if db != *cur_db {
                out.extend_from_slice(format!("[db{db}]").as_bytes());
                *cur_db = db;
            }
            out.extend_from_slice(key);
            out.push(b'=');
            let items: Vec<&[u8]> = l.iter().map(|b| b.as_ref()).collect();
            out.extend_from_slice(&items.join(&b","[..]));
            out.push(b';');
        } else {
            encode_for_test(out, cur_db, db, key, e);
        }
    }

    struct ListSlices {
        pos: usize,
    }

    impl ValueWriter for ListSlices {
        fn step(&mut self, out: &mut Vec<u8>, value: &RedisValue, budget: usize) -> (usize, bool) {
            let RedisValue::List(l) = value else {
                panic!("not a list");
            };
            let end = l.len().min(self.pos.saturating_add(budget.max(1)));
            for (i, item) in l.range(self.pos..end).enumerate() {
                if self.pos + i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(item);
            }
            let n = end - self.pos;
            self.pos = end;
            if end == l.len() {
                out.push(b';');
            }
            (n, end == l.len())
        }
    }

    fn large_list_for_test(
        db: u32,
        key: &[u8],
        e: &KeyEntry,
    ) -> Option<(Vec<u8>, Box<dyn ValueWriter>)> {
        match &e.value {
            RedisValue::List(l) if l.len() >= 10 => {
                let mut head = format!("[db{db}]").into_bytes();
                head.extend_from_slice(key);
                head.push(b'=');
                Some((head, Box::new(ListSlices { pos: 0 })))
            }
            _ => None,
        }
    }

    const LIST_ENCODERS: SnapshotEncoders = SnapshotEncoders {
        whole: encode_list_for_test,
        large: Some(large_list_for_test),
    };

    fn big_list(n: usize) -> RedisValue {
        RedisValue::List((0..n).map(|i| b(&format!("e{i}"))).collect())
    }

    fn joined(n: usize) -> String {
        (0..n)
            .map(|i| format!("e{i}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// A huge list is serialized a slice per step (the shard is free in
    /// between, reads of it included), and the snapshot holds it as it was
    /// when the snapshot began — also when it is written to, or deleted,
    /// half-way through.
    #[test]
    fn large_values_are_sliced_and_stay_point_in_time() {
        // Alone in its shard: whole, it would be one step; sliced at 32
        // elements a step, it takes 1000 / 32.
        let mut store = ShardStore::new();
        store.set(b("big"), big_list(1000), None);
        assert!(store.begin_snapshot_with(LIST_ENCODERS));
        let mut out = Vec::new();
        let mut steps = 0;
        loop {
            let (chunk, done) = store.snapshot_step(2);
            assert!(
                chunk.len() < 1000,
                "one step wrote {} bytes: not sliced",
                chunk.len()
            );
            out.extend(chunk);
            steps += 1;
            if done {
                break;
            }
        }
        assert!(steps >= 1000 / 32, "not sliced: {steps} steps");
        assert_eq!(parse_snapshot(&out).get("0/big"), Some(&joined(1000)));

        for change in ["none", "write", "delete", "flush"] {
            let mut store = ShardStore::new();
            let mut expect = std::collections::BTreeMap::new();
            for i in 0..200 {
                store.set(b(&format!("k{i}")), s(&format!("v{i}")), None);
                expect.insert(format!("0/k{i}"), format!("v{i}"));
            }
            store.set(b("big"), big_list(1000), None);
            expect.insert("0/big".into(), joined(1000));
            assert!(store.begin_snapshot_with(LIST_ENCODERS));
            let mut out = Vec::new();
            let mut steps = 0;
            loop {
                // Budget 2: 32 elements a step.
                let (chunk, done) = store.snapshot_step(2);
                out.extend(chunk);
                steps += 1;
                if done {
                    break;
                }
                // Reads are free; other keys keep changing.
                assert!(store.get(&b("big")).is_some() || change != "none");
                store.set(b(&format!("k{}", steps % 200)), s("changed"), None);
                if steps == 10 {
                    match change {
                        "write" => {
                            if let Some(e) = store.get_mut(&b("big")) {
                                if let RedisValue::List(l) = &mut e.value {
                                    l.push_front(b("new"));
                                }
                            }
                        }
                        "delete" => {
                            store.del(&b("big"));
                        }
                        "flush" => store.flush_all(),
                        _ => {}
                    }
                }
            }
            assert_eq!(parse_snapshot(&out), expect, "{change}");
        }
    }

    #[test]
    fn snapshot_captures_flush_and_swapdb() {
        let mut store = ShardStore::new();
        store.set(b("a"), s("1"), None);
        store.select(1);
        store.set(b("b"), s("2"), None);
        store.select(0);
        assert!(store.begin_snapshot(encode_for_test));
        store.swap_db(0, 1);
        store.flush_all();
        let (out, done) = store.snapshot_step(1000);
        assert!(done);
        let got = parse_snapshot(&out);
        assert_eq!(got.get("0/a").map(String::as_str), Some("1"));
        assert_eq!(got.get("1/b").map(String::as_str), Some("2"));
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn value_sizes() {
        // Keep an eye on per-key overhead: a string entry is value + TTL +
        // access/snapshot words.
        assert!(std::mem::size_of::<RedisValue>() <= 40);
        assert!(std::mem::size_of::<KeyEntry>() <= 64);
    }
}
