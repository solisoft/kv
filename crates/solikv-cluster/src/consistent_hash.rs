use crc16::*;

const NUM_SLOTS: u16 = 16384;

/// Redis-compatible consistent hash ring with 16384 slots.
#[derive(Debug, Clone)]
pub struct ConsistentHash {
    /// Maps slot -> node_id
    slots: Vec<Option<String>>,
    nodes: Vec<String>,
}

impl ConsistentHash {
    pub fn new() -> Self {
        Self {
            slots: vec![None; NUM_SLOTS as usize],
            nodes: Vec::new(),
        }
    }

    /// Add a node and assign it a range of slots.
    pub fn add_node(&mut self, node_id: &str, slot_ranges: &[(u16, u16)]) {
        self.nodes.push(node_id.to_string());
        for &(start, end) in slot_ranges {
            for slot in start..=end.min(NUM_SLOTS - 1) {
                self.slots[slot as usize] = Some(node_id.to_string());
            }
        }
    }

    /// Assign slots evenly among nodes.
    pub fn distribute_evenly(&mut self) {
        if self.nodes.is_empty() {
            return;
        }
        let per_node = NUM_SLOTS as usize / self.nodes.len();
        let mut slot = 0;
        for (i, node) in self.nodes.iter().enumerate() {
            let end = if i == self.nodes.len() - 1 {
                NUM_SLOTS as usize
            } else {
                slot + per_node
            };
            for s in slot..end {
                self.slots[s] = Some(node.clone());
            }
            slot = end;
        }
    }

    /// Get the node responsible for a key.
    pub fn get_node(&self, key: &[u8]) -> Option<&str> {
        let slot = key_slot(key);
        self.slots[slot as usize].as_deref()
    }

    /// Get the slot for a key.
    pub fn key_slot(key: &[u8]) -> u16 {
        key_slot(key)
    }

    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    pub fn num_slots() -> u16 {
        NUM_SLOTS
    }
}

impl Default for ConsistentHash {
    fn default() -> Self {
        Self::new()
    }
}

/// CRC16 CCITT for Redis slot computation.
fn key_slot(key: &[u8]) -> u16 {
    let effective = extract_hash_tag(key);
    State::<XMODEM>::calculate(effective) % NUM_SLOTS
}

fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 {
                return &key[start + 1..start + 1 + end];
            }
        }
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_range() {
        for i in 0..10000 {
            let key = format!("key:{}", i);
            let slot = key_slot(key.as_bytes());
            assert!(slot < NUM_SLOTS);
        }
    }

    #[test]
    fn test_hash_tag() {
        let s1 = key_slot(b"user:{123}:name");
        let s2 = key_slot(b"user:{123}:email");
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_distribute_evenly() {
        let mut ring = ConsistentHash::new();
        ring.nodes = vec![
            "node1".to_string(),
            "node2".to_string(),
            "node3".to_string(),
        ];
        ring.distribute_evenly();

        // All slots should be assigned
        assert!(ring.slots.iter().all(|s| s.is_some()));

        // Each node should have roughly equal slots
        let count1 = ring
            .slots
            .iter()
            .filter(|s| s.as_deref() == Some("node1"))
            .count();
        let count2 = ring
            .slots
            .iter()
            .filter(|s| s.as_deref() == Some("node2"))
            .count();
        let count3 = ring
            .slots
            .iter()
            .filter(|s| s.as_deref() == Some("node3"))
            .count();
        assert!(count1 > 5000);
        assert!(count2 > 5000);
        assert!(count3 > 5000);
        assert_eq!(count1 + count2 + count3, NUM_SLOTS as usize);
    }

    #[test]
    fn test_get_node() {
        let mut ring = ConsistentHash::new();
        ring.nodes = vec!["n1".to_string(), "n2".to_string()];
        ring.distribute_evenly();
        assert!(ring.get_node(b"somekey").is_some());
    }
}
