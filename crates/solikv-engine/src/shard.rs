use bytes::Bytes;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use solikv_core::ShardStore;
use solikv_pubsub::PubSubBroker;

use solikv_core::CommandResponse;

use crate::dispatch::{NOTIFY_EXPIRED, NOTIFY_KEYEVENT, NOTIFY_KEYSPACE};

/// Handle to a shard's store, protected by a mutex.
#[derive(Clone)]
pub struct ShardHandle {
    store: Arc<Mutex<ShardStore>>,
    tick: Arc<AtomicU64>,
    pubsub: Option<Arc<PubSubBroker>>,
    notify_flags: Option<Arc<AtomicU16>>,
}

impl ShardHandle {
    pub fn execute<F>(&self, f: F) -> CommandResponse
    where
        F: FnOnce(&mut ShardStore) -> CommandResponse,
    {
        let result = {
            let mut store = self.store.lock();
            let result = f(&mut store);

            // Drain lazy-expired keys from the buffer
            let lazy_expired = std::mem::take(&mut store.expired_buffer);
            drop(store);

            // Emit expired notifications for lazily expired keys
            self.emit_expired_notifications(&lazy_expired);

            result
        };

        // Periodic active expiry every 100 ops
        let tick = self.tick.fetch_add(1, Ordering::Relaxed);
        if tick.is_multiple_of(100) {
            let active_expired = self.store.lock().run_active_expiry();
            self.emit_expired_notifications(&active_expired);
        }

        result
    }

    /// Emit "expired" keyspace notifications for a set of expired keys.
    fn emit_expired_notifications(&self, keys: &[Bytes]) {
        if keys.is_empty() {
            return;
        }
        let (Some(pubsub), Some(nf)) = (&self.pubsub, &self.notify_flags) else {
            return;
        };
        let flags = nf.load(Ordering::Relaxed);
        if flags == 0 || flags & NOTIFY_EXPIRED == 0 {
            return;
        }
        for key in keys {
            let key_str = std::str::from_utf8(key).unwrap_or("<binary>");
            if flags & NOTIFY_KEYSPACE != 0 {
                let channel = Bytes::from(format!("__keyspace@0__:{}", key_str));
                pubsub.publish(channel, Bytes::from("expired"));
            }
            if flags & NOTIFY_KEYEVENT != 0 {
                let channel = Bytes::from("__keyevent@0__:expired");
                pubsub.publish(channel, key.clone());
            }
        }
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
                pubsub: None,
                notify_flags: None,
            });
        }

        ShardManager { shards, num_shards }
    }

    /// Create a new ShardManager with notification context.
    pub fn with_notifications(
        num_shards: usize,
        pubsub: Arc<PubSubBroker>,
        notify_flags: Arc<AtomicU16>,
    ) -> Self {
        let mut shards = Vec::with_capacity(num_shards);

        for _ in 0..num_shards {
            shards.push(ShardHandle {
                store: Arc::new(Mutex::new(ShardStore::new())),
                tick: Arc::new(AtomicU64::new(0)),
                pubsub: Some(pubsub.clone()),
                notify_flags: Some(notify_flags.clone()),
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
