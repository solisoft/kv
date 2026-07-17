use bytes::Bytes;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use solikv_core::ShardStore;
use solikv_pubsub::PubSubBroker;

use solikv_core::CommandResponse;

use crate::dispatch::{NOTIFY_EXPIRED, NOTIFY_KEYEVENT, NOTIFY_KEYSPACE};

/// Unlocked store for **solo mode** (single Tokio worker — Redis-shaped).
///
/// # Safety invariant
/// Callers must only touch the store from the solo runtime's single worker
/// thread (`--solo` ⇒ `worker_threads(1)`). Concurrent access from another
/// OS thread is undefined behaviour.
struct SoloStore {
    inner: UnsafeCell<ShardStore>,
}

// SAFETY: solo mode guarantees single-threaded access.
unsafe impl Send for SoloStore {}
unsafe impl Sync for SoloStore {}

impl SoloStore {
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(ShardStore::new()),
        }
    }

    #[inline]
    fn with_mut<R>(&self, f: impl FnOnce(&mut ShardStore) -> R) -> R {
        // SAFETY: solo invariant — only one thread mutates the store.
        f(unsafe { &mut *self.inner.get() })
    }

    #[inline]
    fn with_ref<R>(&self, f: impl FnOnce(&ShardStore) -> R) -> R {
        f(unsafe { &*self.inner.get() })
    }
}

enum StoreBackend {
    /// Multi-threaded / multi-shard path (mutex per shard).
    Locked {
        store: Arc<Mutex<ShardStore>>,
        tick: Arc<AtomicU64>,
    },
    /// Redis-shaped single-thread path: no mutex on the hot path.
    Solo {
        store: Arc<SoloStore>,
        tick: AtomicU64,
    },
}

/// Handle to a shard's store.
#[derive(Clone)]
pub struct ShardHandle {
    backend: Arc<StoreBackend>,
    pubsub: Option<Arc<PubSubBroker>>,
    notify_flags: Option<Arc<AtomicU16>>,
}

impl ShardHandle {
    fn new_locked(
        store: Arc<Mutex<ShardStore>>,
        pubsub: Option<Arc<PubSubBroker>>,
        notify_flags: Option<Arc<AtomicU16>>,
    ) -> Self {
        Self {
            backend: Arc::new(StoreBackend::Locked {
                store,
                tick: Arc::new(AtomicU64::new(0)),
            }),
            pubsub,
            notify_flags,
        }
    }

    fn new_solo(
        store: Arc<SoloStore>,
        pubsub: Option<Arc<PubSubBroker>>,
        notify_flags: Option<Arc<AtomicU16>>,
    ) -> Self {
        Self {
            backend: Arc::new(StoreBackend::Solo {
                store,
                tick: AtomicU64::new(0),
            }),
            pubsub,
            notify_flags,
        }
    }

    #[inline]
    pub fn execute<F>(&self, f: F) -> CommandResponse
    where
        F: FnOnce(&mut ShardStore) -> CommandResponse,
    {
        let (result, lazy_expired, active_expired) = match self.backend.as_ref() {
            StoreBackend::Locked { store, tick } => {
                let mut guard = store.lock();
                let result = f(&mut guard);
                let lazy_expired = std::mem::take(&mut guard.expired_buffer);
                let t = tick.fetch_add(1, Ordering::Relaxed);
                let active_expired = if t.is_multiple_of(100) {
                    guard.run_active_expiry()
                } else {
                    Vec::new()
                };
                (result, lazy_expired, active_expired)
            }
            StoreBackend::Solo { store, tick } => store.with_mut(|s| {
                let result = f(s);
                let lazy_expired = std::mem::take(&mut s.expired_buffer);
                // Relaxed atomic is enough; solo is single-threaded so this is
                // essentially a plain load/store without cache-line contention.
                let t = tick.fetch_add(1, Ordering::Relaxed);
                let active_expired = if t.is_multiple_of(100) {
                    s.run_active_expiry()
                } else {
                    Vec::new()
                };
                (result, lazy_expired, active_expired)
            }),
        };

        self.emit_expired_notifications(&lazy_expired);
        if !active_expired.is_empty() {
            self.emit_expired_notifications(&active_expired);
        }
        result
    }

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

    pub fn with_store<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&ShardStore) -> R,
    {
        match self.backend.as_ref() {
            StoreBackend::Locked { store, .. } => {
                let guard = store.lock();
                f(&guard)
            }
            StoreBackend::Solo { store, .. } => store.with_ref(f),
        }
    }

    pub fn is_solo(&self) -> bool {
        matches!(self.backend.as_ref(), StoreBackend::Solo { .. })
    }
}

/// Manages one or more shards.
pub struct ShardManager {
    shards: Vec<ShardHandle>,
    num_shards: usize,
    solo: bool,
}

impl ShardManager {
    pub fn new(num_shards: usize) -> Self {
        Self::with_notifications_inner(num_shards, None, None, false)
    }

    pub fn with_notifications(
        num_shards: usize,
        pubsub: Arc<PubSubBroker>,
        notify_flags: Arc<AtomicU16>,
    ) -> Self {
        Self::with_notifications_inner(num_shards, Some(pubsub), Some(notify_flags), false)
    }

    /// Redis-shaped single-shard manager: no mutex on the command hot path.
    ///
    /// Must be paired with a single-worker Tokio runtime (`worker_threads(1)`).
    pub fn solo(pubsub: Arc<PubSubBroker>, notify_flags: Arc<AtomicU16>) -> Self {
        Self::with_notifications_inner(1, Some(pubsub), Some(notify_flags), true)
    }

    /// Solo without notifications (unit tests).
    pub fn solo_plain() -> Self {
        Self::with_notifications_inner(1, None, None, true)
    }

    fn with_notifications_inner(
        num_shards: usize,
        pubsub: Option<Arc<PubSubBroker>>,
        notify_flags: Option<Arc<AtomicU16>>,
        solo: bool,
    ) -> Self {
        let num_shards = if solo { 1 } else { num_shards.max(1) };
        let mut shards = Vec::with_capacity(num_shards);

        if solo {
            shards.push(ShardHandle::new_solo(
                Arc::new(SoloStore::new()),
                pubsub,
                notify_flags,
            ));
        } else {
            for _ in 0..num_shards {
                shards.push(ShardHandle::new_locked(
                    Arc::new(Mutex::new(ShardStore::new())),
                    pubsub.clone(),
                    notify_flags.clone(),
                ));
            }
        }

        ShardManager {
            shards,
            num_shards,
            solo,
        }
    }

    pub fn is_solo(&self) -> bool {
        self.solo
    }

    #[inline]
    pub fn shard_for_key(&self, key: &bytes::Bytes) -> &ShardHandle {
        self.shard_for_key_bytes(key.as_ref())
    }

    #[inline]
    pub fn shard_index_for_key(&self, key: &[u8]) -> usize {
        if self.solo || self.num_shards == 1 {
            return 0;
        }
        (fast_shard_hash(key) as usize) % self.num_shards
    }

    #[inline]
    pub fn shard_for_key_bytes(&self, key: &[u8]) -> &ShardHandle {
        &self.shards[self.shard_index_for_key(key)]
    }

    pub fn shard(&self, idx: usize) -> &ShardHandle {
        &self.shards[idx % self.num_shards]
    }

    pub fn all_shards(&self) -> &[ShardHandle] {
        &self.shards
    }

    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    pub fn scan_all(&self) -> Vec<(bytes::Bytes, String, Option<u64>)> {
        let mut results = Vec::new();
        for shard in &self.shards {
            let shard_data = shard.with_store(|store| {
                store
                    .iter()
                    .map(|(key, entry)| {
                        let r#type = match &entry.value {
                            solikv_core::types::RedisValue::String(_) => "string",
                            solikv_core::types::RedisValue::List(_) => "list",
                            solikv_core::types::RedisValue::Hash(_) => "hash",
                            solikv_core::types::RedisValue::Set(_) => "set",
                            solikv_core::types::RedisValue::ZSet(_) => "zset",
                            solikv_core::types::RedisValue::HyperLogLog(_) => "hll",
                            solikv_core::types::RedisValue::BloomFilter(_) => "bloom",
                            solikv_core::types::RedisValue::Stream(_) => "stream",
                        };
                        (key.clone(), r#type.to_string(), entry.expires_at)
                    })
                    .collect::<Vec<_>>()
            });
            results.extend(shard_data);
        }
        results
    }
}

/// FNV-1a 64-bit — cheap, well-distributed for short Redis keys.
#[inline]
fn fast_shard_hash(key: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut h = FNV_OFFSET;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}
