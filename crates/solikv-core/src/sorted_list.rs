//! An ordered sequence with O(log n) positional access: the score index of
//! a sorted set.
//!
//! Elements live in a list of sorted chunks of at most `2 * LOAD` elements
//! (the layout of Python's `sortedcontainers`), with a Fenwick tree over the
//! chunk lengths. Finding an element or a rank is a binary search over the
//! chunks' last elements, one inside a chunk, and a prefix sum; inserting or
//! removing shifts at most one chunk (a `memmove` of a few KB) and updates the
//! tree. `ZRANK`, `ZRANGE start stop`, `ZCOUNT` and every `LIMIT offset` are
//! therefore logarithmic instead of a walk from the first element, which is
//! what a `BTreeMap` without subtree counts gave.
//!
//! Chunk splits and merges rebuild the Fenwick tree (O(n / LOAD)); they happen
//! once per `LOAD` updates at most, so the rebuild is amortised to O(1).

const LOAD: usize = 256;

#[derive(Debug, Clone)]
pub struct SortedList<T> {
    chunks: Vec<Vec<T>>,
    /// Fenwick tree (1-based, stored 0-based) over `chunks[i].len()`.
    fen: Vec<usize>,
    len: usize,
}

impl<T: Ord> Default for SortedList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Ord> SortedList<T> {
    pub fn new() -> Self {
        Self {
            chunks: Vec::new(),
            fen: Vec::new(),
            len: 0,
        }
    }

    /// Build from elements already in ascending order (no duplicates).
    pub fn from_sorted(items: Vec<T>) -> Self {
        let len = items.len();
        let mut chunks = Vec::with_capacity(len.div_ceil(LOAD));
        let mut it = items.into_iter().peekable();
        while it.peek().is_some() {
            chunks.push(it.by_ref().take(LOAD).collect::<Vec<T>>());
        }
        let mut s = Self {
            chunks,
            fen: Vec::new(),
            len,
        };
        s.rebuild_index();
        s
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        self.chunks.clear();
        self.fen.clear();
        self.len = 0;
    }

    fn rebuild_index(&mut self) {
        let n = self.chunks.len();
        self.fen.clear();
        self.fen.extend(self.chunks.iter().map(|c| c.len()));
        for i in 0..n {
            let j = i | (i + 1);
            if j < n {
                self.fen[j] += self.fen[i];
            }
        }
    }

    #[inline]
    fn fen_add(&mut self, mut i: usize, delta: isize) {
        while i < self.fen.len() {
            self.fen[i] = (self.fen[i] as isize + delta) as usize;
            i |= i + 1;
        }
    }

    /// Elements in chunks `0..i`.
    #[inline]
    fn prefix(&self, i: usize) -> usize {
        let mut sum = 0;
        let mut i = i;
        while i > 0 {
            sum += self.fen[i - 1];
            i &= i - 1;
        }
        sum
    }

    /// `(chunk, offset)` of global position `pos` (`pos < len`).
    fn locate(&self, pos: usize) -> (usize, usize) {
        // Fenwick descent: the largest chunk index whose prefix is <= pos.
        let n = self.fen.len();
        let mut idx = 0usize;
        let mut rem = pos;
        let mut step = n.next_power_of_two();
        while step > 0 {
            let next = idx + step;
            if next <= n && self.fen[next - 1] <= rem {
                idx = next;
                rem -= self.fen[next - 1];
            }
            step >>= 1;
        }
        (idx, rem)
    }

    /// Index of the first chunk whose last element does not satisfy `pred`
    /// (`pred` must be true for a prefix of the sequence).
    fn chunk_partition(&self, pred: &impl Fn(&T) -> bool) -> usize {
        self.chunks.partition_point(|c| c.last().is_some_and(pred))
    }

    /// Number of leading elements for which `pred` holds; `pred` must be
    /// true for a prefix of the sequence and false afterwards.
    pub fn partition_point(&self, pred: impl Fn(&T) -> bool) -> usize {
        let ci = self.chunk_partition(&pred);
        if ci >= self.chunks.len() {
            return self.len;
        }
        self.prefix(ci) + self.chunks[ci].partition_point(|x| pred(x))
    }

    /// Position of `x` if present.
    pub fn position(&self, x: &T) -> Option<usize> {
        let ci = self.chunk_partition(&|y: &T| y < x);
        let chunk = self.chunks.get(ci)?;
        match chunk.binary_search(x) {
            Ok(off) => Some(self.prefix(ci) + off),
            Err(_) => None,
        }
    }

    /// Insert `x`; false (and nothing changes) if an equal element exists.
    pub fn insert(&mut self, x: T) -> bool {
        if self.chunks.is_empty() {
            self.chunks.push(vec![x]);
            self.fen.push(1);
            self.len = 1;
            return true;
        }
        let mut ci = self.chunk_partition(&|y: &T| y < &x);
        if ci == self.chunks.len() {
            ci -= 1; // after every element: append to the last chunk
        }
        let chunk = &mut self.chunks[ci];
        match chunk.binary_search(&x) {
            Ok(_) => return false,
            Err(off) => chunk.insert(off, x),
        }
        self.len += 1;
        if self.chunks[ci].len() > 2 * LOAD {
            let tail = self.chunks[ci].split_off(LOAD);
            self.chunks.insert(ci + 1, tail);
            self.rebuild_index();
        } else {
            self.fen_add(ci, 1);
        }
        true
    }

    /// Remove the element equal to `x`; whether it was there.
    pub fn remove(&mut self, x: &T) -> bool {
        let ci = self.chunk_partition(&|y: &T| y < x);
        let Some(chunk) = self.chunks.get_mut(ci) else {
            return false;
        };
        match chunk.binary_search(x) {
            Ok(off) => {
                chunk.remove(off);
                self.after_remove(ci);
                true
            }
            Err(_) => false,
        }
    }

    /// Remove and return the element at `pos`.
    pub fn remove_at(&mut self, pos: usize) -> Option<T> {
        if pos >= self.len {
            return None;
        }
        let (ci, off) = self.locate(pos);
        let x = self.chunks[ci].remove(off);
        self.after_remove(ci);
        Some(x)
    }

    fn after_remove(&mut self, ci: usize) {
        self.len -= 1;
        let n = self.chunks[ci].len();
        if n == 0 {
            self.chunks.remove(ci);
            self.rebuild_index();
        } else if n < LOAD / 4 && self.chunks.len() > 1 {
            // Merge a small chunk into a neighbour so chunks stay dense.
            let (a, b) = if ci + 1 < self.chunks.len() {
                (ci, ci + 1)
            } else {
                (ci - 1, ci)
            };
            let tail = self.chunks.remove(b);
            self.chunks[a].extend(tail);
            if self.chunks[a].len() > 2 * LOAD {
                let half = self.chunks[a].len() / 2;
                let t = self.chunks[a].split_off(half);
                self.chunks.insert(a + 1, t);
            }
            self.rebuild_index();
        } else {
            self.fen_add(ci, -1);
        }
    }

    pub fn get(&self, pos: usize) -> Option<&T> {
        if pos >= self.len {
            return None;
        }
        let (ci, off) = self.locate(pos);
        self.chunks[ci].get(off)
    }

    pub fn first(&self) -> Option<&T> {
        self.chunks.first().and_then(|c| c.first())
    }

    pub fn last(&self) -> Option<&T> {
        self.chunks.last().and_then(|c| c.last())
    }

    pub fn pop_first(&mut self) -> Option<T> {
        self.remove_at(0)
    }

    pub fn pop_last(&mut self) -> Option<T> {
        self.len.checked_sub(1).and_then(|p| self.remove_at(p))
    }

    /// Elements from position `start` onwards, ascending.
    pub fn iter_from(&self, start: usize) -> impl DoubleEndedIterator<Item = &T> + '_ {
        self.range(start, self.len)
    }

    /// Elements at positions `start..end`, ascending (reverse it for a
    /// descending walk: both ends are found in O(log n)).
    pub fn range(&self, start: usize, end: usize) -> impl DoubleEndedIterator<Item = &T> + '_ {
        let end = end.min(self.len);
        let start = start.min(end);
        let (head, mid, tail): (&[T], &[Vec<T>], &[T]) = if start == end {
            (&[], &[], &[])
        } else {
            let (cs, os) = self.locate(start);
            let (ce, oe) = self.locate(end - 1);
            if cs == ce {
                (&self.chunks[cs][os..=oe], &[], &[])
            } else {
                (
                    &self.chunks[cs][os..],
                    &self.chunks[cs + 1..ce],
                    &self.chunks[ce][..=oe],
                )
            }
        };
        head.iter().chain(mid.iter().flatten()).chain(tail.iter())
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> + '_ {
        self.chunks.iter().flatten()
    }
}

impl<T: Ord> FromIterator<T> for SortedList<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut v: Vec<T> = iter.into_iter().collect();
        v.sort();
        v.dedup();
        Self::from_sorted(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic xorshift, so the randomized test needs no dependency.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    #[test]
    fn matches_a_sorted_vec_under_random_operations() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut sl = SortedList::new();
        let mut model: Vec<u32> = Vec::new();
        for round in 0..60_000 {
            let x = (rng.next() % 5_000) as u32;
            match rng.next() % 6 {
                0..=2 => {
                    let expect = model.binary_search(&x).is_err();
                    if expect {
                        let p = model.binary_search(&x).unwrap_err();
                        model.insert(p, x);
                    }
                    assert_eq!(sl.insert(x), expect);
                }
                3 => {
                    let expect = model.binary_search(&x).ok();
                    if let Some(p) = expect {
                        model.remove(p);
                    }
                    assert_eq!(sl.remove(&x), expect.is_some());
                }
                4 if !model.is_empty() => {
                    let p = (rng.next() as usize) % model.len();
                    assert_eq!(sl.remove_at(p), Some(model.remove(p)));
                }
                _ => {
                    assert_eq!(sl.position(&x), model.binary_search(&x).ok());
                    assert_eq!(
                        sl.partition_point(|y| *y < x),
                        model.partition_point(|y| *y < x)
                    );
                }
            }
            assert_eq!(sl.len(), model.len());
            if round % 997 == 0 && !model.is_empty() {
                for p in [0, model.len() / 3, model.len() - 1] {
                    assert_eq!(sl.get(p), model.get(p));
                    let got: Vec<u32> = sl.range(p, p + 17).copied().collect();
                    let want: Vec<u32> = model[p..(p + 17).min(model.len())].to_vec();
                    assert_eq!(got, want);
                    let got_rev: Vec<u32> = sl.range(p, p + 5).rev().copied().collect();
                    let mut want_rev: Vec<u32> = model[p..(p + 5).min(model.len())].to_vec();
                    want_rev.reverse();
                    assert_eq!(got_rev, want_rev);
                }
                assert!(sl.iter().copied().eq(model.iter().copied()));
            }
        }
    }

    #[test]
    fn pops_and_bounds() {
        let mut sl: SortedList<i32> = (0..1000).rev().collect();
        assert_eq!(sl.first(), Some(&0));
        assert_eq!(sl.last(), Some(&999));
        assert_eq!(sl.pop_first(), Some(0));
        assert_eq!(sl.pop_last(), Some(999));
        assert_eq!(sl.len(), 998);
        assert_eq!(sl.get(0), Some(&1));
        assert_eq!(sl.get(998), None);
        assert_eq!(sl.range(990, 2000).count(), 8);
        assert_eq!(sl.iter_from(5000).count(), 0);
        while sl.pop_first().is_some() {}
        assert!(sl.is_empty());
        assert!(sl.insert(7));
        assert_eq!(sl.iter().copied().collect::<Vec<_>>(), vec![7]);
    }
}
