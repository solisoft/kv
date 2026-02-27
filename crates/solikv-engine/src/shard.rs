use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use solikv_core::ShardStore;

use solikv_core::CommandResponse;

/// Handle to a shard's store, protected by a mutex.
#[derive(Clone)]
pub struct ShardHandle {
    store: Arc<Mutex<ShardStore>>,
    tick: Arc<AtomicU64>,
}

impl ShardHandle {
    pub fn execute<F>(&self, f: F) -> CommandResponse
    where
        F: FnOnce(&mut ShardStore) -> CommandResponse,
    {
        let result = {
            let mut store = self.store.lock();
            f(&mut store)
        };

        // Periodic active expiry every 100 ops
        let tick = self.tick.fetch_add(1, Ordering::Relaxed);
        if tick % 100 == 0 {
            self.store.lock().run_active_expiry();
        }

        result
    }

    /// Read-only access to the store (used by RDB save).
    pub fn with_store<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&ShardStore) -> R,
    {
        let store = self.store.lock();
        f(&store)
    }
}

/// Manages multiple shards.
pub struct ShardManager {
    shards: Vec<ShardHandle>,
    num_shards: usize,
}

impl ShardManager {
    pub fn new(num_shards: usize) -> Self {
        let mut shards = Vec::with_capacity(num_shards);

        for _ in 0..num_shards {
            shards.push(ShardHandle {
                store: Arc::new(Mutex::new(ShardStore::new())),
                tick: Arc::new(AtomicU64::new(0)),
            });
        }

        ShardManager { shards, num_shards }
    }

    /// Get the shard for a key using consistent hashing.
    pub fn shard_for_key(&self, key: &bytes::Bytes) -> &ShardHandle {
        let slot = solikv_core::keyspace::hash_slot(key) as usize;
        &self.shards[slot % self.num_shards]
    }

    /// Get a shard by index (for multi-shard operations).
    pub fn shard(&self, idx: usize) -> &ShardHandle {
        &self.shards[idx % self.num_shards]
    }

    /// Get all shards.
    pub fn all_shards(&self) -> &[ShardHandle] {
        &self.shards
    }

    pub fn num_shards(&self) -> usize {
        self.num_shards
    }
}
