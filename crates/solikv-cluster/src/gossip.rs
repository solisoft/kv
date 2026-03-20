use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

pub const CLUSTER_BUS_PORT_OFFSET: u16 = 10000;

#[derive(Debug, Clone, PartialEq)]
pub enum NodeFlag {
    Master,
    Slave,
    Myself,
    Fail,
    Handshake,
    NoFail,
}

impl NodeFlag {
    pub fn as_str(&self) -> &str {
        match self {
            NodeFlag::Master => "master",
            NodeFlag::Slave => "slave",
            NodeFlag::Myself => "myself",
            NodeFlag::Fail => "fail",
            NodeFlag::Handshake => "handshake",
            NodeFlag::NoFail => "nofail",
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ClusterNodeInfo {
    pub node_id: String,
    pub ip: String,
    pub port: u16,
    pub flags: Vec<NodeFlag>,
    pub master_id: Option<String>,
    pub ping_sent: Option<u64>,
    pub pong_received: u64,
    pub link_status: String,
}

impl ClusterNodeInfo {
    pub fn new_myself(node_id: String, ip: String, port: u16) -> Self {
        Self {
            node_id,
            ip,
            port,
            flags: vec![NodeFlag::Master, NodeFlag::Myself],
            master_id: None,
            ping_sent: None,
            pong_received: 0,
            link_status: "connected".to_string(),
        }
    }

    pub fn from_gossip(node_id: String, ip: String, port: u16) -> Self {
        Self {
            node_id,
            ip,
            port,
            flags: vec![NodeFlag::Handshake],
            master_id: None,
            ping_sent: None,
            pong_received: 0,
            link_status: "connected".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum GossipMessage {
    Ping {
        node_id: String,
        ip: String,
        port: u16,
        ping_id: u64,
    },
    Pong {
        node_id: String,
        ip: String,
        port: u16,
        ping_id: u64,
    },
    Meet {
        node_id: String,
        ip: String,
        port: u16,
    },
    Update {
        node_id: String,
        ip: String,
        port: u16,
        flags: Vec<String>,
        master_id: Option<String>,
    },
}

impl GossipMessage {
    pub fn encode(&self) -> Vec<u8> {
        let msg = match self {
            GossipMessage::Ping {
                node_id,
                ip,
                port,
                ping_id,
            } => {
                format!("PING {} {} {} {}\n", node_id, ip, port, ping_id)
            }
            GossipMessage::Pong {
                node_id,
                ip,
                port,
                ping_id,
            } => {
                format!("PONG {} {} {} {}\n", node_id, ip, port, ping_id)
            }
            GossipMessage::Meet { node_id, ip, port } => {
                format!("MEET {} {} {}\n", node_id, ip, port)
            }
            GossipMessage::Update {
                node_id,
                ip,
                port,
                flags,
                master_id,
            } => {
                let flags_str = flags.join(",");
                match master_id {
                    Some(mid) => {
                        format!("UPDATE {} {} {} {} {}\n", node_id, ip, port, flags_str, mid)
                    }
                    None => format!("UPDATE {} {} {} {}\n", node_id, ip, port, flags_str),
                }
            }
        };
        msg.into_bytes()
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        let msg = String::from_utf8(data.to_vec()).ok()?;
        let parts: Vec<&str> = msg.split_whitespace().collect();

        if parts.is_empty() {
            return None;
        }

        match parts[0] {
            "PING" if parts.len() >= 5 => Some(GossipMessage::Ping {
                node_id: parts[1].to_string(),
                ip: parts[2].to_string(),
                port: parts[3].parse().ok()?,
                ping_id: parts[4].parse().ok()?,
            }),
            "PONG" if parts.len() >= 5 => Some(GossipMessage::Pong {
                node_id: parts[1].to_string(),
                ip: parts[2].to_string(),
                port: parts[3].parse().ok()?,
                ping_id: parts[4].parse().ok()?,
            }),
            "MEET" if parts.len() >= 4 => Some(GossipMessage::Meet {
                node_id: parts[1].to_string(),
                ip: parts[2].to_string(),
                port: parts[3].parse().ok()?,
            }),
            "UPDATE" if parts.len() >= 5 => Some(GossipMessage::Update {
                node_id: parts[1].to_string(),
                ip: parts[2].to_string(),
                port: parts[3].parse().ok()?,
                flags: parts[4].split(',').map(|s| s.to_string()).collect(),
                master_id: parts.get(5).map(|s| s.to_string()),
            }),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct GossipState {
    myself: Arc<RwLock<ClusterNodeInfo>>,
    nodes: Arc<RwLock<HashMap<String, ClusterNodeInfo>>>,
    config: Arc<RwLock<GossipConfig>>,
}

#[derive(Debug, Clone)]
pub struct GossipConfig {
    pub node_timeout_ms: u64,
    pub ping_interval_ms: u64,
    pub gossip_interval_ms: u64,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            node_timeout_ms: 15000,
            ping_interval_ms: 1000,
            gossip_interval_ms: 1000,
        }
    }
}

impl GossipState {
    pub fn new(node_id: String, ip: String, port: u16) -> Self {
        let myself = ClusterNodeInfo::new_myself(node_id.clone(), ip, port);

        Self {
            myself: Arc::new(RwLock::new(myself)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(GossipConfig::default())),
        }
    }

    pub fn myself_id(&self) -> String {
        self.myself.read().node_id.clone()
    }

    pub fn add_node(&self, node_id: String, ip: String, port: u16) {
        let mut nodes = self.nodes.write();
        nodes
            .entry(node_id.clone())
            .or_insert_with(|| ClusterNodeInfo::from_gossip(node_id, ip, port));
    }

    pub fn remove_node(&self, node_id: &str) {
        self.nodes.write().remove(node_id);
    }

    pub fn get_all_nodes(&self) -> Vec<ClusterNodeInfo> {
        let mut result: Vec<ClusterNodeInfo> = vec![self.myself.read().clone()];
        result.extend(self.nodes.read().values().cloned().collect::<Vec<_>>());
        result
    }

    pub fn get_known_nodes(&self) -> Vec<(String, u16)> {
        let myself_id = self.myself.read().node_id.clone();
        self.nodes
            .read()
            .values()
            .filter(|n| n.node_id != myself_id)
            .map(|n| (n.ip.clone(), n.port))
            .collect()
    }

    pub fn get_alive_nodes(&self) -> Vec<ClusterNodeInfo> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let timeout = self.config.read().node_timeout_ms;

        self.nodes
            .read()
            .values()
            .filter(|n| now.saturating_sub(n.pong_received) < timeout)
            .cloned()
            .collect()
    }

    pub fn handle_ping(
        &self,
        _sender_id: &str,
        _ip: String,
        _port: u16,
        ping_id: u64,
    ) -> GossipMessage {
        let _now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let myself = self.myself.read();

        GossipMessage::Pong {
            node_id: myself.node_id.clone(),
            ip: myself.ip.clone(),
            port: myself.port,
            ping_id,
        }
    }

    pub fn handle_pong(&self, node_id: &str, ping_id: u64) {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(node_id) {
            node.pong_received = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            if let Some(ping) = node.ping_sent {
                if ping == ping_id {
                    node.link_status = "connected".to_string();
                }
            }
        }
    }

    pub fn handle_meet(&self, node_id: String, ip: String, port: u16) {
        self.add_node(node_id, ip, port);
    }

    pub fn handle_update(
        &self,
        node_id: String,
        ip: String,
        port: u16,
        flags: Vec<String>,
        master_id: Option<String>,
    ) {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(&node_id) {
            node.ip = ip;
            node.port = port;
            node.flags = flags
                .iter()
                .map(|s| match s.as_str() {
                    "master" => NodeFlag::Master,
                    "slave" => NodeFlag::Slave,
                    "fail" => NodeFlag::Fail,
                    "handshake" => NodeFlag::Handshake,
                    _ => NodeFlag::NoFail,
                })
                .collect();
            node.master_id = master_id;
        }
    }
}

pub async fn start_gossip_server(
    port: u16,
    state: GossipState,
    msg_tx: mpsc::UnboundedSender<GossipMessage>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(
        "Cluster gossip server listening on {} (localhost only)",
        addr
    );

    let state_clone = state.clone();

    tokio::spawn(async move {
        let mut buf = [0u8; 1024];

        loop {
            match listener.accept().await {
                Ok((mut stream, peer_addr)) => {
                    // Only accept connections from loopback or known cluster peers
                    if !peer_addr.ip().is_loopback() {
                        let known = state_clone
                            .get_all_nodes()
                            .iter()
                            .any(|n| n.ip == peer_addr.ip().to_string());
                        if !known {
                            tracing::warn!(
                                "Gossip: rejecting connection from unknown peer {}",
                                peer_addr
                            );
                            continue;
                        }
                    }

                    let tx = msg_tx.clone();

                    tokio::spawn(async move {
                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) => break,
                                Ok(n) => {
                                    if let Some(msg) = GossipMessage::decode(&buf[..n]) {
                                        let _ = tx.send(msg);
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("Gossip accept error: {}", e);
                }
            }
        }
    });

    Ok(())
}

pub fn generate_node_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let rand: u64 = rand_simple();
    format!("{:x}-{:x}", ts, rand)
}

pub fn generate_stable_node_id(ip: &str, port: u16) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{}:{}", ip, port).hash(&mut hasher);
    let hash = hasher.finish();
    format!("{:016x}-{:016x}", hash, port)
}

fn rand_simple() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish()
}
