use bytes::Bytes;
use std::collections::BinaryHeap;

/// Max keys drained from the expiry heap in a single active-expiry tick.
/// Bounds lock-hold time when many keys expire at once.
pub const MAX_EXPIRE_PER_TICK: usize = 20;

/// Entry in the expiry heap.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExpiryEntry {
    pub expires_at: u64,
    pub key: Bytes,
}

impl Ord for ExpiryEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap: earliest expiry first
        other
            .expires_at
            .cmp(&self.expires_at)
            .then_with(|| other.key.cmp(&self.key))
    }
}

impl PartialOrd for ExpiryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Per-shard expiry heap for active key expiration.
#[derive(Debug)]
pub struct ExpiryHeap {
    heap: BinaryHeap<ExpiryEntry>,
}

impl ExpiryHeap {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, key: Bytes, expires_at: u64) {
        self.heap.push(ExpiryEntry { expires_at, key });
    }

    /// Remove and return expired entries as of `now_ms`, up to `limit` keys.
    /// Remaining expired entries stay on the heap for a later tick.
    pub fn drain_expired(&mut self, now_ms: u64, limit: usize) -> Vec<Bytes> {
        let mut expired = Vec::new();
        while expired.len() < limit {
            match self.heap.peek() {
                Some(entry) if entry.expires_at <= now_ms => {
                    let entry = self.heap.pop().unwrap();
                    expired.push(entry.key);
                }
                _ => break,
            }
        }
        expired
    }

    /// Peek at the next expiry time, if any.
    pub fn next_expiry(&self) -> Option<u64> {
        self.heap.peek().map(|e| e.expires_at)
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

impl Default for ExpiryHeap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expiry_heap_ordering() {
        let mut heap = ExpiryHeap::new();
        heap.push(Bytes::from("c"), 300);
        heap.push(Bytes::from("a"), 100);
        heap.push(Bytes::from("b"), 200);

        let expired = heap.drain_expired(150, MAX_EXPIRE_PER_TICK);
        assert_eq!(expired, vec![Bytes::from("a")]);

        let expired = heap.drain_expired(350, MAX_EXPIRE_PER_TICK);
        assert_eq!(expired.len(), 2);
    }

    #[test]
    fn test_expiry_heap_empty() {
        let mut heap = ExpiryHeap::new();
        assert!(heap.drain_expired(1000, MAX_EXPIRE_PER_TICK).is_empty());
        assert!(heap.next_expiry().is_none());
    }

    #[test]
    fn test_expiry_next() {
        let mut heap = ExpiryHeap::new();
        heap.push(Bytes::from("a"), 100);
        heap.push(Bytes::from("b"), 50);
        assert_eq!(heap.next_expiry(), Some(50));
    }

    #[test]
    fn test_expiry_drain_respects_limit() {
        let mut heap = ExpiryHeap::new();
        for i in 0..50 {
            heap.push(Bytes::from(format!("k{i}")), 100);
        }
        let expired = heap.drain_expired(200, 20);
        assert_eq!(expired.len(), 20);
        // Remaining expired entries still on the heap
        assert_eq!(heap.drain_expired(200, 100).len(), 30);
    }
}
