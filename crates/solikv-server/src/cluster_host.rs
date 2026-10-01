//! What the cluster asks of this server (`solikv_cluster::ClusterHost`).

use bytes::Bytes;
use std::sync::Arc;

use solikv_core::CommandResponse;
use solikv_engine::replication::ReplController;
use solikv_engine::CommandEngine;

use crate::replication::Replication;

pub struct ServerClusterHost {
    engine: Arc<CommandEngine>,
    replication: Arc<Replication>,
}

impl ServerClusterHost {
    pub fn new(engine: Arc<CommandEngine>, replication: Arc<Replication>) -> Self {
        Self {
            engine,
            replication,
        }
    }
}

impl solikv_cluster::ClusterHost for ServerClusterHost {
    fn repl_offset(&self) -> u64 {
        self.engine.repl().offset()
    }

    fn replicate(&self, ip: &str, port: u16) {
        if let Err(e) = self.replication.replicaof(ip.to_string(), port) {
            tracing::error!("cluster: cannot replicate {ip}:{port}: {e}");
        }
    }

    fn promote(&self) {
        if let Err(e) = self.replication.no_one() {
            tracing::error!("cluster: promotion failed: {e}");
        }
    }

    fn pause_writes(&self, ms: u64) {
        self.engine.pause_writes(ms);
    }

    fn deliver_publish(&self, channel: &[u8], message: &[u8], shard: bool) {
        let ch = Bytes::copy_from_slice(channel);
        let msg = Bytes::copy_from_slice(message);
        if shard {
            self.engine.pubsub.publish_shard(&ch, msg);
        } else {
            self.engine.pubsub.publish(ch, msg);
        }
    }

    fn dbsize(&self) -> u64 {
        match self.engine.execute_in_db(0, "DBSIZE", &[]) {
            CommandResponse::Integer(n) => n.max(0) as u64,
            _ => 0,
        }
    }

    fn count_keys_in_slot(&self, slot: u16) -> u64 {
        self.engine.count_keys_in_slot(slot)
    }

    fn keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes> {
        self.engine.keys_in_slot(slot, count)
    }
}
