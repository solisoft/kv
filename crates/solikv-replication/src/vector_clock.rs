use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Vector clock for causal ordering of events.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VectorClock {
    clocks: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    /// Increment the clock for a node.
    pub fn increment(&mut self, node_id: &str) -> u64 {
        let entry = self.clocks.entry(node_id.to_string()).or_insert(0);
        *entry += 1;
        *entry
    }

    /// Get the clock value for a node.
    pub fn get(&self, node_id: &str) -> u64 {
        self.clocks.get(node_id).copied().unwrap_or(0)
    }

    /// Merge with another vector clock (element-wise max).
    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &val) in &other.clocks {
            let entry = self.clocks.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(val);
        }
    }

    /// Check if this clock happened before the other.
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        let mut at_least_one_less = false;
        for (node, &val) in &other.clocks {
            let my_val = self.get(node);
            if my_val > val {
                return false;
            }
            if my_val < val {
                at_least_one_less = true;
            }
        }
        // Also check nodes that are in self but not in other
        for (node, &val) in &self.clocks {
            if other.get(node) < val {
                return false;
            }
        }
        at_least_one_less
    }

    /// Check if two clocks are concurrent (neither happened before the other).
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) && !other.happened_before(self) && self != other
    }
}

impl PartialEq for VectorClock {
    fn eq(&self, other: &Self) -> bool {
        if self.clocks.len() != other.clocks.len() {
            return false;
        }
        self.clocks.iter().all(|(k, v)| other.clocks.get(k) == Some(v))
    }
}

impl Eq for VectorClock {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment_and_get() {
        let mut vc = VectorClock::new();
        vc.increment("n1");
        vc.increment("n1");
        vc.increment("n2");
        assert_eq!(vc.get("n1"), 2);
        assert_eq!(vc.get("n2"), 1);
        assert_eq!(vc.get("n3"), 0);
    }

    #[test]
    fn test_happened_before() {
        let mut vc1 = VectorClock::new();
        vc1.increment("n1");

        let mut vc2 = vc1.clone();
        vc2.increment("n2");

        assert!(vc1.happened_before(&vc2));
        assert!(!vc2.happened_before(&vc1));
    }

    #[test]
    fn test_concurrent() {
        let mut vc1 = VectorClock::new();
        vc1.increment("n1");

        let mut vc2 = VectorClock::new();
        vc2.increment("n2");

        assert!(vc1.is_concurrent(&vc2));
    }

    #[test]
    fn test_merge() {
        let mut vc1 = VectorClock::new();
        vc1.increment("n1");
        vc1.increment("n1");

        let mut vc2 = VectorClock::new();
        vc2.increment("n2");
        vc2.increment("n2");
        vc2.increment("n2");

        vc1.merge(&vc2);
        assert_eq!(vc1.get("n1"), 2);
        assert_eq!(vc1.get("n2"), 3);
    }
}
