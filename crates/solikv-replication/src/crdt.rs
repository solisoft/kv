use bytes::Bytes;
use std::collections::{HashMap, HashSet};

/// Last-Writer-Wins Register for string values.
#[derive(Debug, Clone)]
pub struct LwwRegister {
    pub value: Bytes,
    pub timestamp: u64,
    pub node_id: String,
}

impl LwwRegister {
    pub fn new(value: Bytes, timestamp: u64, node_id: String) -> Self {
        Self { value, timestamp, node_id }
    }

    /// Merge with another LWW register. Higher timestamp wins.
    /// On tie, lexicographically higher node_id wins.
    pub fn merge(&mut self, other: &LwwRegister) -> bool {
        if other.timestamp > self.timestamp
            || (other.timestamp == self.timestamp && other.node_id > self.node_id)
        {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
            self.node_id = other.node_id.clone();
            true
        } else {
            false
        }
    }
}

/// Observed-Remove Set (OR-Set) for set values.
#[derive(Debug, Clone)]
pub struct OrSet {
    /// member -> set of (node_id, timestamp) tags
    elements: HashMap<Bytes, HashSet<(String, u64)>>,
    /// Tombstoned tags
    removed: HashSet<(String, u64)>,
}

impl OrSet {
    pub fn new() -> Self {
        Self {
            elements: HashMap::new(),
            removed: HashSet::new(),
        }
    }

    pub fn add(&mut self, member: Bytes, node_id: String, timestamp: u64) {
        let tags = self.elements.entry(member).or_default();
        tags.insert((node_id, timestamp));
    }

    pub fn remove(&mut self, member: &Bytes) {
        if let Some(tags) = self.elements.remove(member) {
            self.removed.extend(tags);
        }
    }

    pub fn merge(&mut self, other: &OrSet) {
        for (member, other_tags) in &other.elements {
            let tags = self.elements.entry(member.clone()).or_default();
            for tag in other_tags {
                if !self.removed.contains(tag) {
                    tags.insert(tag.clone());
                }
            }
        }
        self.removed.extend(other.removed.iter().cloned());
        // Clean up: remove tombstoned tags from elements
        for tags in self.elements.values_mut() {
            tags.retain(|t| !self.removed.contains(t));
        }
        self.elements.retain(|_, tags| !tags.is_empty());
    }

    pub fn members(&self) -> Vec<&Bytes> {
        self.elements.keys().collect()
    }

    pub fn contains(&self, member: &Bytes) -> bool {
        self.elements.contains_key(member)
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

impl Default for OrSet {
    fn default() -> Self {
        Self::new()
    }
}

/// PN-Counter for integer increment/decrement.
#[derive(Debug, Clone)]
pub struct PnCounter {
    increments: HashMap<String, i64>,
    decrements: HashMap<String, i64>,
}

impl PnCounter {
    pub fn new() -> Self {
        Self {
            increments: HashMap::new(),
            decrements: HashMap::new(),
        }
    }

    pub fn increment(&mut self, node_id: &str, amount: i64) {
        if amount >= 0 {
            *self.increments.entry(node_id.to_string()).or_insert(0) += amount;
        } else {
            *self.decrements.entry(node_id.to_string()).or_insert(0) += -amount;
        }
    }

    pub fn value(&self) -> i64 {
        let p: i64 = self.increments.values().sum();
        let n: i64 = self.decrements.values().sum();
        p - n
    }

    pub fn merge(&mut self, other: &PnCounter) {
        for (node, &val) in &other.increments {
            let entry = self.increments.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(val);
        }
        for (node, &val) in &other.decrements {
            let entry = self.decrements.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(val);
        }
    }
}

impl Default for PnCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lww_register_merge() {
        let mut r1 = LwwRegister::new(Bytes::from("a"), 1, "node1".to_string());
        let r2 = LwwRegister::new(Bytes::from("b"), 2, "node2".to_string());
        assert!(r1.merge(&r2));
        assert_eq!(r1.value, Bytes::from("b"));
    }

    #[test]
    fn test_lww_register_tie_break() {
        let mut r1 = LwwRegister::new(Bytes::from("a"), 1, "node1".to_string());
        let r2 = LwwRegister::new(Bytes::from("b"), 1, "node2".to_string());
        assert!(r1.merge(&r2));
        assert_eq!(r1.value, Bytes::from("b"));
    }

    #[test]
    fn test_or_set() {
        let mut s1 = OrSet::new();
        s1.add(Bytes::from("a"), "n1".to_string(), 1);
        s1.add(Bytes::from("b"), "n1".to_string(), 2);

        let mut s2 = OrSet::new();
        s2.add(Bytes::from("b"), "n2".to_string(), 3);
        s2.add(Bytes::from("c"), "n2".to_string(), 4);

        s1.merge(&s2);
        assert_eq!(s1.len(), 3); // a, b, c
    }

    #[test]
    fn test_or_set_remove() {
        let mut s1 = OrSet::new();
        s1.add(Bytes::from("a"), "n1".to_string(), 1);
        s1.remove(&Bytes::from("a"));
        assert!(s1.is_empty());

        // After merge, removed element stays removed
        let mut s2 = OrSet::new();
        s2.add(Bytes::from("a"), "n1".to_string(), 1);
        s2.merge(&s1);
        assert!(s2.is_empty());
    }

    #[test]
    fn test_pn_counter() {
        let mut c1 = PnCounter::new();
        c1.increment("n1", 5);
        c1.increment("n1", -2);
        assert_eq!(c1.value(), 3);

        let mut c2 = PnCounter::new();
        c2.increment("n2", 10);

        c1.merge(&c2);
        assert_eq!(c1.value(), 13);
    }

    #[test]
    fn test_pn_counter_convergence() {
        let mut c1 = PnCounter::new();
        c1.increment("n1", 5);

        let mut c2 = PnCounter::new();
        c2.increment("n2", 3);

        // Both merge with each other
        let c1_clone = c1.clone();
        let c2_clone = c2.clone();
        c1.merge(&c2_clone);
        c2.merge(&c1_clone);

        // Should converge to same value
        assert_eq!(c1.value(), c2.value());
        assert_eq!(c1.value(), 8);
    }
}
