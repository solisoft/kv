use serde::{Deserialize, Serialize};

/// Represents a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: String,
    pub addr: String,
    pub port: u16,
    pub slots: Vec<(u16, u16)>,
    pub role: NodeRole,
    pub flags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeRole {
    Master,
    Replica,
}

impl ClusterNode {
    pub fn new(id: String, addr: String, port: u16) -> Self {
        Self {
            id,
            addr,
            port,
            slots: Vec::new(),
            role: NodeRole::Master,
            flags: vec!["myself".to_string()],
        }
    }
}
