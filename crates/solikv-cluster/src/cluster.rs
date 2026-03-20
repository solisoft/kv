use crate::gossip::GossipState;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum ClusterState {
    Init,
    Handshake,
    Joined,
}

pub struct ClusterSlot {
    pub start: u16,
    pub end: u16,
    pub owner: Option<String>,
}

pub struct ClusterManager {
    state: Arc<RwLock<ClusterState>>,
    my_node_id: String,
    my_ip: String,
    my_port: u16,
    gossip: GossipState,
    slots: Arc<RwLock<Vec<ClusterSlot>>>,
    config: Arc<RwLock<ClusterConfig>>,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub require_full_coverage: bool,
    pub slot_coverage_threshold: f64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            require_full_coverage: true,
            slot_coverage_threshold: 0.95,
        }
    }
}

impl ClusterManager {
    pub fn new(node_id: String, ip: String, port: u16, gossip: GossipState) -> Self {
        let slots = (0..16384)
            .map(|i| ClusterSlot {
                start: i,
                end: i,
                owner: Some(node_id.clone()),
            })
            .collect();

        Self {
            state: Arc::new(RwLock::new(ClusterState::Init)),
            my_node_id: node_id,
            my_ip: ip,
            my_port: port,
            gossip,
            slots: Arc::new(RwLock::new(slots)),
            config: Arc::new(RwLock::new(ClusterConfig::default())),
        }
    }

    pub fn enable(&self) {
        self.config.write().enabled = true;
    }

    pub fn disable(&self) {
        self.config.write().enabled = false;
    }

    pub fn is_enabled(&self) -> bool {
        self.config.read().enabled
    }

    pub fn state(&self) -> ClusterState {
        self.state.read().clone()
    }

    pub fn set_state(&self, state: ClusterState) {
        *self.state.write() = state;
    }

    pub fn node_id(&self) -> &str {
        &self.my_node_id
    }

    pub fn meet(&self, ip: String, port: u16) {
        let node_id = format!("{}:{}", ip, port);
        self.gossip.add_node(node_id, ip.clone(), port);

        // Use a single write lock to avoid TOCTOU race
        let mut state = self.state.write();
        if *state == ClusterState::Init {
            *state = ClusterState::Handshake;
        }
    }

    pub fn add_slots(&self, start: u16, end: u16) -> Result<(), String> {
        if start > end {
            return Err("Invalid slot range".to_string());
        }
        if end >= 16384 {
            return Err("Slot out of range".to_string());
        }

        let mut slots = self.slots.write();
        for i in start..=end {
            slots[i as usize].owner = Some(self.my_node_id.clone());
        }

        tracing::info!("Added slots {}-{} to node {}", start, end, self.my_node_id);
        Ok(())
    }

    pub fn del_slots(&self, start: u16, end: u16) -> Result<(), String> {
        if start > end {
            return Err("Invalid slot range".to_string());
        }

        let mut slots = self.slots.write();
        for i in start..=end {
            if slots[i as usize].owner.as_ref() == Some(&self.my_node_id) {
                slots[i as usize].owner = None;
            }
        }

        tracing::info!(
            "Removed slots {}-{} from node {}",
            start,
            end,
            self.my_node_id
        );
        Ok(())
    }

    pub fn get_slot_owner(&self, slot: u16) -> Option<String> {
        self.slots.read()[slot as usize].owner.clone()
    }

    pub fn get_my_slots(&self) -> Vec<(u16, u16)> {
        let slots = self.slots.read();
        let mut ranges: Vec<(u16, u16)> = Vec::new();
        let mut start: Option<u16> = None;

        for (i, slot) in slots.iter().enumerate() {
            if slot.owner.as_ref() == Some(&self.my_node_id) {
                if start.is_none() {
                    start = Some(i as u16);
                }
            } else if let Some(s) = start {
                ranges.push((s, (i - 1) as u16));
                start = None;
            }
        }

        if let Some(s) = start {
            ranges.push((s, 16383));
        }

        ranges
    }

    pub fn key_slot(&self, key: &[u8]) -> u16 {
        crate::consistent_hash::ConsistentHash::key_slot(key)
    }

    pub fn get_slot_owner_for_key(&self, key: &[u8]) -> Option<String> {
        let slot = self.key_slot(key);
        self.get_slot_owner(slot)
    }

    pub fn is_my_slot(&self, key: &[u8]) -> bool {
        if !self.is_enabled() {
            return true;
        }

        match self.get_slot_owner_for_key(key) {
            Some(owner) => owner == self.my_node_id,
            None => false,
        }
    }

    pub fn cluster_info(&self) -> String {
        let state = match *self.state.read() {
            ClusterState::Init => "fail",
            ClusterState::Handshake => "fail",
            ClusterState::Joined => "ok",
        };

        let nodes = self.gossip.get_all_nodes();
        let my_slots = self.get_my_slots();
        let slot_count: u32 = my_slots.iter().map(|(s, e)| (e - s + 1) as u32).sum();

        format!(
            "cluster_state:{}\ncluster_slots_assigned:{}\ncluster_slots_ok:{}\ncluster_slots_fail:{}\ncluster_known_nodes:{}\ncluster_size:{}\ncluster_current_epoch:0\ncluster_my_epoch:0\ncluster_stats_messages_received:0\ncluster_stats_messages_sent:0\n",
            state,
            slot_count,
            slot_count,
            16384 - slot_count,
            nodes.len(),
            nodes.len().saturating_sub(1),
        )
    }

    pub fn cluster_nodes(&self) -> String {
        let nodes = self.gossip.get_all_nodes();
        let my_slots = self.get_my_slots();

        let mut output = String::new();

        for node in nodes {
            let is_myself = node.node_id == self.my_node_id;

            let flags = if is_myself {
                "myself,".to_string() + &self.slots_to_flags(&my_slots)
            } else {
                let alive = self
                    .gossip
                    .get_alive_nodes()
                    .iter()
                    .any(|n| n.node_id == node.node_id);
                if alive { "master" } else { "master,fail" }.to_string()
            };

            let master_id = node.master_id.clone().unwrap_or_else(|| "-".to_string());
            let ping = if node.ping_sent.is_some() {
                "0"
            } else {
                "9999"
            };
            let pong = "9999";
            let _link_status = &node.link_status;

            let slot_str = if is_myself {
                self.slots_to_string(&my_slots)
            } else {
                "-".to_string()
            };

            output.push_str(&format!(
                "{} {}@{} {} {} {} {} {}\n",
                node.node_id, node.ip, node.port, flags, master_id, ping, pong, slot_str,
            ));
        }

        output
    }

    pub fn cluster_slots(&self) -> Vec<Vec<String>> {
        let mut result: Vec<Vec<String>> = Vec::new();

        let my_slots = self.get_my_slots();
        if !my_slots.is_empty() {
            for (start, end) in my_slots {
                result.push(vec![
                    start.to_string(),
                    end.to_string(),
                    self.my_node_id.clone(),
                    format!("{}:{}", self.my_ip, self.my_port),
                ]);
            }
        }

        result.sort_by(|a, b| {
            a[0].parse::<u16>()
                .unwrap()
                .cmp(&b[0].parse::<u16>().unwrap())
        });
        result
    }

    fn slots_to_flags(&self, slots: &[(u16, u16)]) -> String {
        if slots.is_empty() {
            "slave".to_string()
        } else {
            "master".to_string()
        }
    }

    fn slots_to_string(&self, slots: &[(u16, u16)]) -> String {
        if slots.is_empty() {
            "-".to_string()
        } else {
            slots
                .iter()
                .map(|(s, e)| {
                    if s == e {
                        s.to_string()
                    } else {
                        format!("{}-{}", s, e)
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        }
    }

    pub fn export_state(&self) -> ClusterStateSnapshot {
        let my_slots = self.get_my_slots();
        let slots = self.slots.read();
        let mut slot_owners: Vec<(u16, u16, String)> = Vec::new();

        let mut start: Option<u16> = None;
        let mut last_owner: Option<String> = None;

        for (i, slot) in slots.iter().enumerate() {
            let _current_owner = slot.owner.clone().unwrap_or_default();
            match (start, &last_owner, &slot.owner) {
                (Some(_s), Some(lo), Some(co)) if lo == co => {
                    // Continue current range
                }
                (Some(s), Some(lo), _) => {
                    // End current range
                    slot_owners.push((s, (i - 1) as u16, lo.clone()));
                    start = None;
                    last_owner = None;
                }
                (None, _, Some(co)) => {
                    // Start new range
                    start = Some(i as u16);
                    last_owner = Some(co.clone());
                }
                _ => {}
            }
        }

        // Handle last range
        if let (Some(s), Some(lo)) = (start, last_owner) {
            slot_owners.push((s, 16383, lo));
        }

        // Get known cluster nodes (excluding self)
        let known_nodes = self.gossip.get_known_nodes();

        ClusterStateSnapshot {
            node_id: self.my_node_id.clone(),
            ip: self.my_ip.clone(),
            port: self.my_port,
            my_slots,
            slot_owners,
            known_nodes,
        }
    }

    pub fn import_state(&self, snapshot: &ClusterStateSnapshot) {
        // Restore slot ownership for this node
        for (start, end, owner) in &snapshot.slot_owners {
            if owner == &self.my_node_id {
                let _ = self.add_slots(*start, *end);
            }
        }

        // Reconnect to known cluster nodes
        for (ip, port) in &snapshot.known_nodes {
            self.meet(ip.clone(), *port);
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClusterStateSnapshot {
    pub node_id: String,
    pub ip: String,
    pub port: u16,
    pub my_slots: Vec<(u16, u16)>,
    pub slot_owners: Vec<(u16, u16, String)>,
    pub known_nodes: Vec<(String, u16)>,
}
