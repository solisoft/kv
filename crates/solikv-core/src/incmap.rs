//! A keyed map that never stops the world to grow.
//!
//! [`IndexMap`](indexmap::IndexMap) grows like a `Vec`: past its capacity,
//! every entry is copied into a buffer twice the size and every index is
//! rehashed, in one step. For a keyspace that is one O(n) pause under the
//! shard lock — about 229 000 keys per shard pause for 40–170 ms — and since
//! keys spread evenly, every shard reaches its threshold within the same few
//! milliseconds. Redis avoids this with incremental rehashing; so does this
//! map:
//!
//! * Entries live in fixed-size chunks ([`Chunks`]): growing allocates one
//!   more chunk and copies nothing.
//! * The index is an open-addressing table of `(hash, position)` slots. When
//!   it fills, a table twice the size is allocated and the old one is
//!   migrated a few slots at a time, on each insert and removal (and on
//!   [`rehash`](IncMap::rehash), which idle callers can drive). Meanwhile a
//!   lookup probes the new table, then the old one; a key is in exactly one.
//!
//! Positions behave exactly like `IndexMap`'s: entries are numbered
//! `0..len`, an insert appends, and [`swap_remove_index`] fills the hole with
//! the last entry. SCAN and the stepped snapshot walk rely on that (an entry
//! only ever moves *down*).
//!
//! [`swap_remove_index`]: IncMap::swap_remove_index

use bytes::Bytes;
use std::hash::BuildHasher;

use crate::types::FastHasher;

const CHUNK_SHIFT: usize = 12;
const CHUNK: usize = 1 << CHUNK_SHIFT;
const CHUNK_MASK: usize = CHUNK - 1;

/// A vector stored in chunks of [`CHUNK`] elements, so that pushing never
/// moves what is already stored. The first chunk grows like a `Vec` (small
/// maps stay small); every later one is allocated at full size.
struct Chunks<T> {
    chunks: Vec<Vec<T>>,
    len: usize,
}

impl<T> Chunks<T> {
    const fn new() -> Self {
        Self {
            chunks: Vec::new(),
            len: 0,
        }
    }

    #[inline]
    fn get(&self, i: usize) -> Option<&T> {
        if i >= self.len {
            return None;
        }
        // SAFETY: elements `0..len` are stored in order, `CHUNK` per chunk,
        // so `i < len` puts chunk `i >> CHUNK_SHIFT` in `chunks` and offset
        // `i & CHUNK_MASK` within that chunk's length. This is on every
        // key lookup; the two bounds checks it skips showed in benchmarks.
        unsafe {
            Some(
                self.chunks
                    .get_unchecked(i >> CHUNK_SHIFT)
                    .get_unchecked(i & CHUNK_MASK),
            )
        }
    }

    #[inline]
    fn get_mut(&mut self, i: usize) -> Option<&mut T> {
        if i >= self.len {
            return None;
        }
        // SAFETY: as in `get`.
        unsafe {
            Some(
                self.chunks
                    .get_unchecked_mut(i >> CHUNK_SHIFT)
                    .get_unchecked_mut(i & CHUNK_MASK),
            )
        }
    }

    fn push(&mut self, value: T) {
        let c = self.len >> CHUNK_SHIFT;
        if c == self.chunks.len() {
            self.chunks.push(if c == 0 {
                Vec::new()
            } else {
                Vec::with_capacity(CHUNK)
            });
        }
        self.chunks[c].push(value);
        self.len += 1;
    }

    fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        let c = self.len >> CHUNK_SHIFT;
        let v = self.chunks[c].pop();
        // Keep one empty chunk past the end, so that a push/pop pair at a
        // chunk boundary does not allocate and free a chunk each time.
        if self.chunks.len() > c + 2 {
            self.chunks.truncate(c + 2);
        }
        v
    }

    /// `Vec::swap_remove`.
    fn swap_remove(&mut self, i: usize) -> T {
        let last = self.pop().expect("swap_remove on an empty Chunks");
        if i == self.len {
            last
        } else {
            std::mem::replace(self.get_mut(i).expect("i < len"), last)
        }
    }

    fn iter(&self) -> impl Iterator<Item = &T> {
        self.chunks.iter().flat_map(|c| c.iter())
    }

    fn into_flat(self) -> std::iter::Flatten<std::vec::IntoIter<Vec<T>>> {
        self.chunks.into_iter().flatten()
    }
}

/// Slot states. A full slot is `hash << 32 | (position + 1)`.
const EMPTY: u64 = 0;
/// Only in a table being migrated away from: a slot whose key moved to the
/// new table or was removed. Probes go past it.
const TOMB: u64 = u32::MAX as u64;

#[inline]
fn slot(h: u32, pos: usize) -> u64 {
    (h as u64) << 32 | (pos as u64 + 1)
}

#[inline]
fn slot_hash(s: u64) -> u32 {
    (s >> 32) as u32
}

#[inline]
fn slot_pos(s: u64) -> usize {
    (s as u32 - 1) as usize
}

/// A linear-probing table of slots. The table being written to (`cur`) is
/// kept below 3/4 full and deletes by backward shift, so it never holds a
/// [`TOMB`]; the table being migrated (`old`) only loses entries, as tombs.
struct Table {
    slots: Box<[u64]>,
    mask: usize,
    /// Full slots.
    used: usize,
}

impl Table {
    fn with_capacity(cap: usize) -> Self {
        let cap = cap.next_power_of_two().max(8);
        Self {
            // Zeroed: the allocator hands fresh pages over as they are, so a
            // big table costs its page faults as it fills, not up front.
            slots: vec![EMPTY; cap].into_boxed_slice(),
            mask: cap - 1,
            used: 0,
        }
    }

    #[inline]
    fn full(&self) -> bool {
        (self.used + 1) * 4 > self.slots.len() * 3
    }

    /// Place a slot whose key is known to be absent.
    #[inline]
    fn place(&mut self, s: u64) {
        let mut i = slot_hash(s) as usize & self.mask;
        while self.slots[i] != EMPTY {
            i = (i + 1) & self.mask;
        }
        self.slots[i] = s;
        self.used += 1;
    }

    /// Index of the slot pointing at position `pos` with hash `h`.
    #[inline]
    fn find_pos(&self, h: u32, pos: usize) -> Option<usize> {
        let want = slot(h, pos);
        let mut i = h as usize & self.mask;
        loop {
            let s = self.slots[i];
            if s == want {
                return Some(i);
            }
            if s == EMPTY {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Backward-shift deletion (table without tombs).
    fn remove_shift(&mut self, at: usize) {
        let mask = self.mask;
        let mut hole = at;
        let mut j = at;
        loop {
            j = (j + 1) & mask;
            let s = self.slots[j];
            if s == EMPTY {
                break;
            }
            let home = slot_hash(s) as usize & mask;
            // Move `s` into the hole unless its home lies cyclically in
            // (hole, j] — then it is already as close to home as it can be.
            let stays = if hole <= j {
                hole < home && home <= j
            } else {
                hole < home || home <= j
            };
            if !stays {
                self.slots[hole] = s;
                hole = j;
            }
        }
        self.slots[hole] = EMPTY;
        self.used -= 1;
    }
}

/// Slots migrated from the old table per insert or removal. Migration must
/// end before the new table fills: it starts when the new table is 3/8 full
/// and has `cap/2` slots to move, with `3·cap/8` inserts to go — at least
/// 1.33 slots per insert. Eight leaves room, and still bounds each call.
const MIGRATE_PER_OP: usize = 8;

/// See the module documentation.
pub struct IncMap<V> {
    hasher: FastHasher,
    entries: Chunks<(Bytes, V)>,
    cur: Table,
    old: Option<Table>,
    /// Next slot of `old` to migrate.
    cursor: usize,
}

impl<V> Default for IncMap<V> {
    fn default() -> Self {
        Self {
            hasher: FastHasher::default(),
            entries: Chunks::new(),
            cur: Table::with_capacity(8),
            old: None,
            cursor: 0,
        }
    }
}

impl<V> std::fmt::Debug for IncMap<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncMap")
            .field("len", &self.entries.len)
            .field("capacity", &self.cur.slots.len())
            .field("migrating", &self.old.is_some())
            .finish()
    }
}

impl<V> IncMap<V> {
    #[inline]
    fn hash(&self, key: &[u8]) -> u32 {
        let h = self.hasher.hash_one(key);
        (h ^ (h >> 32)) as u32
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.len == 0
    }

    /// Whether a migration to a bigger index is under way.
    pub fn is_rehashing(&self) -> bool {
        self.old.is_some()
    }

    #[inline]
    fn find_in(&self, t: &Table, h: u32, key: &[u8]) -> Option<(usize, usize)> {
        let mut i = h as usize & t.mask;
        loop {
            let s = t.slots[i];
            if s == EMPTY {
                return None;
            }
            if s != TOMB && slot_hash(s) == h {
                let pos = slot_pos(s);
                if let Some((k, _)) = self.entries.get(pos) {
                    if k.as_ref() == key {
                        return Some((i, pos));
                    }
                }
            }
            i = (i + 1) & t.mask;
        }
    }

    #[inline]
    fn find(&self, h: u32, key: &[u8]) -> Option<usize> {
        if let Some((_, pos)) = self.find_in(&self.cur, h, key) {
            return Some(pos);
        }
        let old = self.old.as_ref()?;
        self.find_in(old, h, key).map(|(_, pos)| pos)
    }

    #[inline]
    pub fn get_index_of(&self, key: &[u8]) -> Option<usize> {
        if self.entries.len == 0 {
            return None;
        }
        self.find(self.hash(key), key)
    }

    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get_index_of(key).is_some()
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let pos = self.get_index_of(key)?;
        self.entries.get(pos).map(|(_, v)| v)
    }

    #[inline]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        let pos = self.get_index_of(key)?;
        self.entries.get_mut(pos).map(|(_, v)| v)
    }

    #[inline]
    pub fn get_index(&self, pos: usize) -> Option<(&Bytes, &V)> {
        self.entries.get(pos).map(|(k, v)| (k, v))
    }

    #[inline]
    pub fn get_index_mut(&mut self, pos: usize) -> Option<(&Bytes, &mut V)> {
        self.entries.get_mut(pos).map(|(k, v)| (&*k, v))
    }

    /// Insert or replace. Returns the key's position and the value it
    /// replaced (the key itself is kept, as `IndexMap::insert_full` does).
    pub fn insert_full(&mut self, key: Bytes, value: V) -> (usize, Option<V>) {
        let h = self.hash(&key);
        if let Some(pos) = self.find(h, &key) {
            let (_, v) = self.entries.get_mut(pos).expect("found");
            return (pos, Some(std::mem::replace(v, value)));
        }
        if self.cur.full() {
            self.grow();
        }
        let pos = self.entries.len;
        // Positions are stored as `pos + 1` in 32 bits, `u32::MAX` meaning
        // a tomb.
        assert!(pos < (u32::MAX - 1) as usize, "IncMap: too many entries");
        self.entries.push((key, value));
        self.cur.place(slot(h, pos));
        self.migrate(MIGRATE_PER_OP);
        (pos, None)
    }

    /// Remove the entry at `pos`, moving the last entry into its place.
    pub fn swap_remove_index(&mut self, pos: usize) -> Option<(Bytes, V)> {
        let h = self.hash(&self.entries.get(pos)?.0);
        self.unlink(h, pos);
        let last = self.entries.len - 1;
        if pos != last {
            // The last entry moves to `pos`: repoint its slot.
            let lh = self.hash(&self.entries.get(last).expect("last < len").0);
            let moved = slot(lh, pos);
            if let Some(i) = self.cur.find_pos(lh, last) {
                self.cur.slots[i] = moved;
            } else if let Some(old) = &mut self.old {
                if let Some(i) = old.find_pos(lh, last) {
                    old.slots[i] = moved;
                }
            }
        }
        let removed = self.entries.swap_remove(pos);
        self.migrate(MIGRATE_PER_OP);
        Some(removed)
    }

    /// Remove `key`, moving the last entry into its place.
    pub fn swap_remove(&mut self, key: &[u8]) -> Option<V> {
        let pos = self.get_index_of(key)?;
        self.swap_remove_index(pos).map(|(_, v)| v)
    }

    /// Drop the slot of the entry at `pos` (hash `h`), wherever it is.
    fn unlink(&mut self, h: u32, pos: usize) {
        if let Some(i) = self.cur.find_pos(h, pos) {
            self.cur.remove_shift(i);
        } else if let Some(old) = &mut self.old {
            if let Some(i) = old.find_pos(h, pos) {
                old.slots[i] = TOMB;
                old.used -= 1;
            }
        }
    }

    /// Start migrating to a table twice the size. A migration still under
    /// way is finished first (it cannot be, at the rate inserts drive it —
    /// see [`MIGRATE_PER_OP`] — but this keeps the invariant cheap to see).
    fn grow(&mut self) {
        self.migrate(usize::MAX);
        let bigger = Table::with_capacity(self.cur.slots.len() * 2);
        let old = std::mem::replace(&mut self.cur, bigger);
        self.old = Some(old);
        self.cursor = 0;
    }

    /// Move up to `budget` slots of the old table to the new one. Returns
    /// whether a migration is still under way.
    fn migrate(&mut self, budget: usize) -> bool {
        let Some(old) = &mut self.old else {
            return false;
        };
        let end = old.slots.len();
        let stop = self.cursor.saturating_add(budget).min(end);
        while self.cursor < stop {
            let s = old.slots[self.cursor];
            if s != EMPTY && s != TOMB {
                self.cur.place(s);
                // A tomb, not empty: entries further along may probe past it.
                old.slots[self.cursor] = TOMB;
            }
            self.cursor += 1;
        }
        if self.cursor >= end {
            self.old = None;
            self.cursor = 0;
            return false;
        }
        true
    }

    /// Migrate up to `budget` index slots, for callers with idle time (the
    /// active-expiry cycle). Returns whether a migration is still under way.
    pub fn rehash(&mut self, budget: usize) -> bool {
        self.migrate(budget)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &V)> {
        self.entries.iter().map(|(k, v)| (k, v))
    }

    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.entries.iter().map(|(k, _)| k)
    }
}

/// By position, as `IndexMap` indexes: panics past `len`.
impl<V> std::ops::Index<usize> for IncMap<V> {
    type Output = V;

    #[inline]
    fn index(&self, pos: usize) -> &V {
        &self
            .entries
            .get(pos)
            .expect("IncMap: position out of bounds")
            .1
    }
}

impl<V> std::ops::IndexMut<usize> for IncMap<V> {
    #[inline]
    fn index_mut(&mut self, pos: usize) -> &mut V {
        &mut self
            .entries
            .get_mut(pos)
            .expect("IncMap: position out of bounds")
            .1
    }
}

impl<V> IntoIterator for IncMap<V> {
    type Item = (Bytes, V);
    type IntoIter = std::iter::Flatten<std::vec::IntoIter<Vec<(Bytes, V)>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_flat()
    }
}

/// A set of keys with [`IncMap`]'s growth: `IndexSet`'s positional API.
#[derive(Default, Debug)]
pub struct IncSet(IncMap<()>);

impl IncSet {
    /// Whether the key was newly added.
    #[inline]
    pub fn insert(&mut self, key: Bytes) -> bool {
        self.0.insert_full(key, ()).1.is_none()
    }

    #[inline]
    pub fn swap_remove(&mut self, key: &[u8]) -> bool {
        self.0.swap_remove(key).is_some()
    }

    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.0.contains_key(key)
    }

    #[inline]
    pub fn get_index(&self, pos: usize) -> Option<&Bytes> {
        self.0.get_index(pos).map(|(k, _)| k)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn rehash(&mut self, budget: usize) -> bool {
        self.0.rehash(budget)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.0.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    fn key(i: u64) -> Bytes {
        Bytes::from(format!("key:{i}"))
    }

    /// Random inserts, overwrites and swap-removes, compared step by step
    /// with `IndexMap`, which has the same positional semantics.
    #[test]
    fn matches_indexmap_under_random_operations() {
        let mut m: IncMap<u64> = IncMap::default();
        let mut model: IndexMap<Bytes, u64> = IndexMap::new();
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for step in 0..400_000u64 {
            let r = next();
            // Grow mostly, with bursts of removals so tables migrate while
            // entries leave both of them.
            let removing = (step / 50_000) % 2 == 1;
            match r % 10 {
                0..=5 if !removing => {
                    let k = key(next() % 300_000);
                    let a = m.insert_full(k.clone(), step);
                    let b = model.insert_full(k, step);
                    assert_eq!(a, b, "insert at step {step}");
                }
                0..=6 => {
                    if model.is_empty() {
                        continue;
                    }
                    let pos = (next() as usize) % model.len();
                    let a = m.swap_remove_index(pos);
                    let b = model.swap_remove_index(pos);
                    assert_eq!(a, b, "remove at step {step}");
                }
                _ => {
                    let k = key(next() % 300_000);
                    assert_eq!(m.get_index_of(&k), model.get_index_of(&k), "lookup {step}");
                    assert_eq!(m.get(&k), model.get(&k));
                }
            }
            assert_eq!(m.len(), model.len());
        }
        for (pos, (k, v)) in model.iter().enumerate() {
            assert_eq!(m.get_index(pos), Some((k, v)));
            assert_eq!(m.get_index_of(k), Some(pos));
        }
        let all: Vec<_> = m.iter().map(|(k, v)| (k.clone(), *v)).collect();
        let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), *v)).collect();
        assert_eq!(all, want);
    }

    /// No single insert pays for the whole growth: the work an insert does
    /// on the index stays bounded while the map grows past several
    /// thresholds.
    #[test]
    fn growth_is_incremental() {
        let mut m: IncMap<()> = IncMap::default();
        let mut saw_migration = false;
        for i in 0..1_000_000u64 {
            m.insert_full(key(i), ());
            saw_migration |= m.is_rehashing();
            // A migration started by growth always ends before the next.
            if m.cur.full() {
                assert!(!m.is_rehashing());
            }
        }
        assert!(saw_migration);
        for i in (0..1_000_000u64).step_by(997) {
            assert!(m.contains_key(&key(i)));
        }
        assert!(!m.contains_key(b"absent"));
    }

    #[test]
    fn rehash_finishes_a_migration_without_inserts() {
        let mut m: IncMap<()> = IncMap::default();
        let mut i = 0u64;
        while !m.is_rehashing() {
            m.insert_full(key(i), ());
            i += 1;
        }
        while m.rehash(16) {}
        assert!(!m.is_rehashing());
        for j in 0..i {
            assert!(m.contains_key(&key(j)));
        }
    }

    #[test]
    fn set_api() {
        let mut s = IncSet::default();
        assert!(s.insert(key(1)));
        assert!(!s.insert(key(1)));
        assert!(s.insert(key(2)));
        assert_eq!(s.len(), 2);
        assert!(s.swap_remove(&key(1)));
        assert!(!s.swap_remove(&key(1)));
        assert_eq!(s.get_index(0), Some(&key(2)));
        assert!(s.contains(&key(2)));
    }
}
