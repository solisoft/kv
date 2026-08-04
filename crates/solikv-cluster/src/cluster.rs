use crate::gossip::{GossipMessage, GossipState};
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
        // The constructor claims every slot, which is correct for one node and
        // wrong the moment there are several. See `release_all_slots`.
        self.release_all_slots();
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

    /// Applies one authenticated gossip frame.
    ///
    /// The dispatch the server drains into. Kept on the manager rather than in
    /// the server loop so that "what a frame does to cluster state" is one
    /// place, and so it can be exercised without a socket.
    ///
    /// A `Ping` produces a `Pong` for the caller to send back; everything else
    /// is applied and returns nothing.
    pub fn handle_gossip(&self, message: GossipMessage) -> Option<GossipMessage> {
        match message {
            GossipMessage::Ping {
                node_id,
                ip,
                port,
                ping_id,
            } => Some(self.gossip.handle_ping(&node_id, ip, port, ping_id)),
            GossipMessage::Pong {
                node_id,
                ip,
                port,
                ping_id,
            } => {
                self.gossip.handle_pong(&node_id, &ip, port, ping_id);
                None
            }
            GossipMessage::Meet { node_id, ip, port } => {
                self.gossip.handle_meet(node_id, ip, port);
                None
            }
            GossipMessage::Update {
                node_id,
                ip,
                port,
                flags,
                master_id,
                slots,
            } => {
                // Ownership first, then membership. A peer that claims slots is
                // telling us which keys are not ours to answer, and recording
                // that late means a window where this node answers for them.
                self.record_peer_slots(&node_id, &slots);
                self.gossip
                    .handle_update(node_id, ip, port, flags, master_id);
                None
            }
        }
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

    /// Records what a peer says it owns.
    ///
    /// A claim is taken at face value, because gossip has no arbiter: two nodes
    /// claiming one slot is a misconfiguration, and the honest response is that
    /// the last claim heard wins and both are visible in `CLUSTER NODES`. Silently
    /// preferring one would hide the overlap, which is the thing an operator has
    /// to see.
    ///
    /// A peer's claim never takes a slot away from *this* node's own claim —
    /// releasing ours is a local decision, and letting a remote message do it
    /// would let one misconfigured node stop this one from serving.
    pub fn record_peer_slots(&self, node_id: &str, ranges: &[(u16, u16)]) {
        if node_id == self.my_node_id {
            return;
        }
        let mut slots = self.slots.write();
        for (start, end) in ranges {
            if start > end {
                continue;
            }
            for i in *start..=*end {
                let slot = &mut slots[i as usize];
                if slot.owner.as_deref() == Some(self.my_node_id.as_str()) {
                    continue;
                }
                slot.owner = Some(node_id.to_string());
            }
        }
    }

    /// The ranges this node claims, coalesced — the form that goes on the wire.
    ///
    /// Delegates to [`ClusterManager::get_my_slots`] rather than repeating the
    /// coalescing. Two implementations of "which slots are mine" is how a node
    /// comes to announce one set and serve another.
    pub fn my_slot_ranges(&self) -> Vec<(u16, u16)> {
        self.get_my_slots()
    }

    /// Gives up every slot this node claims.
    ///
    /// Called when cluster mode is enabled, and that is the fix for the bug this
    /// module had: the constructor claims all 16384 so a single node serves every
    /// key, which is right until the node is one of several. Without releasing,
    /// every member believes it owns everything, `is_my_slot` is always true, and
    /// the `MOVED` machinery below never fires.
    ///
    /// A cluster with nothing assigned then serves nothing, which is Redis's
    /// behaviour and the only honest one: answering for a slot nobody assigned
    /// means answering from a node that may not hold the data.
    pub fn release_all_slots(&self) {
        let mut slots = self.slots.write();
        let mut released = 0usize;
        for slot in slots.iter_mut() {
            if slot.owner.as_deref() == Some(self.my_node_id.as_str()) {
                slot.owner = None;
                released += 1;
            }
        }
        if released > 0 {
            tracing::info!(
                released,
                "cluster mode: released this node's claim on every slot. Until slots are \
                 assigned, keys are answered with CLUSTERDOWN rather than from a node that \
                 may not hold them"
            );
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
        self.slots_of(&self.my_node_id)
    }

    /// The ranges a given node owns, coalesced.
    ///
    /// Written once and used for every node, including this one. It was
    /// `get_my_slots` only, and `CLUSTER NODES` printed `-` for every peer as a
    /// literal — so ownership could be known and still invisible. That matters
    /// more than a cosmetic gap: `CLUSTER NODES` is what a Redis-cluster client
    /// reads to build its slot map, so a peer whose ranges print as `-` is a peer
    /// the client will never route to.
    pub fn slots_of(&self, node_id: &str) -> Vec<(u16, u16)> {
        let slots = self.slots.read();
        let mut ranges: Vec<(u16, u16)> = Vec::new();
        let mut start: Option<u16> = None;

        for (i, slot) in slots.iter().enumerate() {
            if slot.owner.as_deref() == Some(node_id) {
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

    /// Where the owner of a slot can be reached, for a `MOVED` redirect.
    ///
    /// Resolved through gossip rather than stored in the slot table. Ownership is
    /// keyed by node id because that is the stable identity; an address recorded
    /// alongside it would go stale the moment a node moved, and the redirect
    /// would then send clients to a machine that is no longer there — while the
    /// ownership itself was still correct.
    ///
    /// `None` means the slot has an owner this node cannot locate: it knows who,
    /// not where. That is a different answer from an unassigned slot and the
    /// caller says so.
    pub fn get_slot_owner_address(&self, slot: u16) -> Option<(String, u16)> {
        let owner = self.get_slot_owner(slot)?;
        if owner == self.my_node_id {
            return Some((self.my_ip.clone(), self.my_port));
        }
        self.gossip
            .get_all_nodes()
            .into_iter()
            .find(|n| n.node_id == owner)
            .map(|n| (n.ip, n.port))
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
                // `fail` is about liveness, and it is the one an operator acts
                // on; a live peer that serves no slot is a different situation
                // and shows up in the ranges at the end of the line.
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

            // Every node's real ranges, not a literal dash for the peers. A
            // client builds its slot map from this line.
            let their_slots = if is_myself {
                my_slots.clone()
            } else {
                self.slots_of(&node.node_id)
            };
            let slot_str = self.slots_to_string(&their_slots);

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

#[cfg(test)]
mod slot_ownership_tests {
    use super::*;
    use crate::gossip::GossipState;

    fn manager(id: &str) -> ClusterManager {
        let gossip = GossipState::new(id.to_string(), "127.0.0.1".to_string(), 6379);
        ClusterManager::new(id.to_string(), "127.0.0.1".to_string(), 6379, gossip)
    }

    #[test]
    fn a_single_node_owns_everything_until_cluster_mode() {
        // The constructor's claim is right for one node: without it a standalone
        // server answers nothing, because every key belongs to a slot nobody owns.
        let m = manager("solo");
        assert_eq!(m.get_my_slots(), vec![(0, 16383)]);
        assert!(m.is_my_slot(b"anything"));
    }

    #[test]
    fn enabling_cluster_mode_releases_the_blanket_claim() {
        // The bug this module had. Every member kept claiming 0-16383 after
        // meeting its peers, so `is_my_slot` was always true and the MOVED
        // machinery never fired: members agreed and sharding did not.
        let m = manager("a");
        m.enable();
        assert!(
            m.get_my_slots().is_empty(),
            "a node in cluster mode still claims every slot"
        );
        assert!(
            !m.is_my_slot(b"key"),
            "an unassigned slot was answered locally"
        );
    }

    #[test]
    fn a_peer_claim_is_recorded_so_a_key_can_be_redirected() {
        let m = manager("a");
        m.enable();
        m.record_peer_slots("b", &[(0, 8191)]);
        assert_eq!(m.get_slot_owner(0).as_deref(), Some("b"));
        assert_eq!(m.get_slot_owner(8191).as_deref(), Some("b"));
        // Outside the claimed range nobody owns it, which is a different answer
        // from "b owns it" and has to stay one.
        assert!(m.get_slot_owner(8192).is_none());
    }

    #[test]
    fn a_peer_cannot_take_a_slot_this_node_claims() {
        // Releasing our own claim is a local decision. If a remote frame could
        // do it, one misconfigured node would stop this one from serving — and
        // the operator would look at the wrong machine.
        let m = manager("a");
        m.enable();
        m.add_slots(0, 100).unwrap();
        m.record_peer_slots("b", &[(0, 200)]);

        assert_eq!(m.get_slot_owner(0).as_deref(), Some("a"), "ours was taken");
        assert_eq!(m.get_slot_owner(100).as_deref(), Some("a"));
        // Beyond ours, the peer's claim stands.
        assert_eq!(m.get_slot_owner(101).as_deref(), Some("b"));
    }

    #[test]
    fn our_own_update_arriving_back_is_ignored() {
        // Gossip is a mesh; a frame can come back. Recording our own claim as a
        // peer's would be harmless here and is refused anyway, because the next
        // reader of this function should not have to work out why it is safe.
        let m = manager("a");
        m.enable();
        m.add_slots(0, 10).unwrap();
        m.record_peer_slots("a", &[(500, 600)]);
        assert!(m.get_slot_owner(500).is_none());
    }

    #[test]
    fn a_backwards_peer_range_is_skipped_not_iterated() {
        let m = manager("a");
        m.enable();
        m.record_peer_slots("b", &[(200, 100), (300, 301)]);
        assert!(m.get_slot_owner(150).is_none());
        assert_eq!(m.get_slot_owner(300).as_deref(), Some("b"));
    }

    #[test]
    fn what_goes_on_the_wire_is_what_this_node_serves() {
        // One definition of "my slots". Two would let a node announce one set and
        // answer for another, and the disagreement is invisible from either side.
        let m = manager("a");
        m.enable();
        m.add_slots(0, 99).unwrap();
        m.add_slots(200, 299).unwrap();
        assert_eq!(m.my_slot_ranges(), m.get_my_slots());
        assert_eq!(m.my_slot_ranges(), vec![(0, 99), (200, 299)]);
    }

    #[test]
    fn releasing_is_idempotent() {
        let m = manager("a");
        m.enable();
        m.release_all_slots();
        assert!(m.get_my_slots().is_empty());
    }

    #[test]
    fn a_slot_this_node_owns_resolves_to_its_own_address() {
        let m = manager("a");
        m.enable();
        m.add_slots(100, 200).unwrap();
        assert_eq!(
            m.get_slot_owner_address(150),
            Some(("127.0.0.1".to_string(), 6379))
        );
    }

    #[test]
    fn an_unassigned_slot_has_no_address() {
        // Distinguished from "owned but unlocatable", because the two send an
        // operator to different places: nobody assigned it, versus the owner is
        // known and unreachable.
        let m = manager("a");
        m.enable();
        assert!(m.get_slot_owner_address(1234).is_none());
        assert!(m.get_slot_owner(1234).is_none());
    }

    #[test]
    fn a_peer_owned_slot_has_an_owner_even_before_its_address_is_known() {
        // The state that produced an honest refusal instead of a redirect: this
        // node knows *who* owns the slot and not *where*. Both facts have to be
        // separately observable, because a `MOVED` needs the second and a
        // diagnosis needs the first.
        let m = manager("a");
        m.enable();
        m.record_peer_slots("b", &[(9000, 9100)]);
        assert_eq!(m.get_slot_owner(9050).as_deref(), Some("b"));
        assert!(
            m.get_slot_owner_address(9050).is_none(),
            "an address was invented for a peer gossip has never seen"
        );
    }

    #[test]
    fn ownership_is_keyed_by_node_id_not_by_address() {
        // The reason the address is resolved at redirect time. If it were stored
        // beside the claim, a node that moved would keep being advertised at its
        // old address while its ownership stayed perfectly correct — and the
        // redirect would send clients to a machine that is no longer there.
        let m = manager("a");
        m.enable();
        m.record_peer_slots("b", &[(0, 10)]);
        assert_eq!(m.get_slot_owner(5).as_deref(), Some("b"));
        // Nothing in the slot table mentions an address.
        assert!(!m.get_slot_owner(5).unwrap().contains(':'));
    }
}
