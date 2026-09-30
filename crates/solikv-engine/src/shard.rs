use bytes::Bytes;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use solikv_core::ShardStore;
use solikv_pubsub::PubSubBroker;

use solikv_core::CommandResponse;
use solikv_persist::AofWriter;

use crate::aof_log::Effects;
use crate::dispatch::{NOTIFY_EXPIRED, NOTIFY_KEYEVENT, NOTIFY_KEYSPACE};

/// Reply to every command once the server has started shutting down.
pub const SHUTTING_DOWN: &str = "ERR server is shutting down";

/// State every shard handle shares.
#[derive(Default)]
struct Shared {
    /// The append-only log, once startup has loaded the dataset. Set once;
    /// commands replayed before it is set are, correctly, not logged again.
    aof: OnceLock<AofWriter>,
    /// Shutdown barrier: no command touches a store once this is set (see
    /// `ShardManager::freeze`).
    frozen: AtomicBool,
}

/// Unlocked store for **solo mode** (single Tokio worker — Redis-shaped).
///
/// # Safety invariant
/// Callers must only touch the store from the solo runtime's single worker
/// thread (`--solo` ⇒ `worker_threads(1)`). Concurrent access from another
/// OS thread is undefined behaviour.
struct SoloStore {
    inner: UnsafeCell<ShardStore>,
    /// Debug builds check the invariant instead of trusting it: entering the
    /// store while another entry is live is exactly the data race the
    /// invariant forbids, and it is how `block_in_place` went unnoticed.
    #[cfg(debug_assertions)]
    busy: std::sync::atomic::AtomicBool,
}

// SAFETY: solo mode guarantees single-threaded access.
unsafe impl Send for SoloStore {}
unsafe impl Sync for SoloStore {}

impl SoloStore {
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(ShardStore::new()),
            #[cfg(debug_assertions)]
            busy: std::sync::atomic::AtomicBool::new(false),
        }
    }

    #[inline]
    fn enter(&self) -> SoloEntry<'_> {
        #[cfg(debug_assertions)]
        assert!(
            !self.busy.swap(true, Ordering::Acquire),
            "solo store entered concurrently: two threads (or a re-entrant call) \
             touched the unlocked store"
        );
        SoloEntry { _store: self }
    }

    #[inline]
    fn with_mut<R>(&self, f: impl FnOnce(&mut ShardStore) -> R) -> R {
        let _entry = self.enter();
        // SAFETY: solo invariant — only one thread mutates the store.
        f(unsafe { &mut *self.inner.get() })
    }

    #[inline]
    fn with_ref<R>(&self, f: impl FnOnce(&ShardStore) -> R) -> R {
        let _entry = self.enter();
        // SAFETY: as above.
        f(unsafe { &*self.inner.get() })
    }
}

/// Marks the solo store as in use for the length of one closure.
struct SoloEntry<'a> {
    _store: &'a SoloStore,
}

impl Drop for SoloEntry<'_> {
    #[inline]
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        self._store.busy.store(false, Ordering::Release);
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
    index: u32,
    shared: Arc<Shared>,
}

impl ShardHandle {
    fn new(
        backend: StoreBackend,
        pubsub: Option<Arc<PubSubBroker>>,
        notify_flags: Option<Arc<AtomicU16>>,
        index: u32,
        shared: Arc<Shared>,
    ) -> Self {
        Self {
            backend: Arc::new(backend),
            pubsub,
            notify_flags,
            index,
            shared,
        }
    }

    /// Run `f` with the store, locked (or, solo, on the only thread).
    #[inline]
    fn locked<T>(&self, f: impl FnOnce(&mut ShardStore, &AtomicU64) -> T) -> T {
        match self.backend.as_ref() {
            StoreBackend::Locked { store, tick } => {
                let mut guard = store.lock();
                f(&mut guard, tick)
            }
            StoreBackend::Solo { store, tick } => store.with_mut(|s| f(s, tick)),
        }
    }

    /// Run a command against the store.
    ///
    /// With the append-only log on, the command (see `aof_log`) and any key
    /// expired along the way are logged before the lock is released, so this
    /// shard's log holds its changes in the order they happened.
    #[inline]
    pub fn execute<F>(&self, f: F) -> CommandResponse
    where
        F: FnOnce(&mut ShardStore) -> CommandResponse,
    {
        self.run(|store, _| f(store), false)
    }

    /// Run a command that logs its own effects on this shard (multi-key and
    /// cross-shard writes; see `aof_log`), instead of being logged as itself.
    #[inline]
    pub fn execute_with_effects<F>(&self, f: F) -> CommandResponse
    where
        F: FnOnce(&mut ShardStore, &mut Effects) -> CommandResponse,
    {
        self.run(f, true)
    }

    #[inline]
    fn run<F>(&self, f: F, explicit: bool) -> CommandResponse
    where
        F: FnOnce(&mut ShardStore, &mut Effects) -> CommandResponse,
    {
        let aof = self.shared.aof.get();
        let (result, lazy_expired, active_expired) = self.locked(|store, tick| {
            // Checked under the lock: once `freeze` has taken and released
            // this lock, nothing changes the store again.
            if self.shared.frozen.load(Ordering::Relaxed) {
                return (
                    CommandResponse::error(SHUTTING_DOWN),
                    Vec::new(),
                    Vec::new(),
                );
            }
            let mut effects = Effects::new(aof.is_some());
            let result = f(store, &mut effects);
            let lazy_expired = std::mem::take(&mut store.expired_buffer);
            // Relaxed atomic is enough; solo is single-threaded so this is
            // essentially a plain load/store without cache-line contention.
            let t = tick.fetch_add(1, Ordering::Relaxed);
            let active_expired = if t.is_multiple_of(100) {
                store.run_active_expiry()
            } else {
                Vec::new()
            };
            if let Some(w) = aof {
                crate::aof_log::log_under_lock(
                    w,
                    self.index,
                    &result,
                    &lazy_expired,
                    explicit.then_some(&mut effects),
                    &active_expired,
                );
            }
            (result, lazy_expired, active_expired)
        });

        self.emit_expired_notifications(&lazy_expired);
        if !active_expired.is_empty() {
            self.emit_expired_notifications(&active_expired);
        }
        result
    }

    /// Append a record to this shard's log under its lock (transaction
    /// markers).
    pub(crate) fn log_marker(&self, record: &[u8]) {
        if let Some(w) = self.shared.aof.get() {
            self.with_store(|_| w.log_raw(self.index, record));
        }
    }

    /// This shard's position, which is also its log's number.
    pub fn index(&self) -> u32 {
        self.index
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

    /// Mutable access that is **not logged**: only for loading a dataset
    /// before the append-only log is attached, or scratch stores that have
    /// none. Anything else must go through [`execute`](Self::execute).
    pub fn with_store_mut_unlogged<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut ShardStore) -> R,
    {
        self.locked(|store, _| f(store))
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
    shared: Arc<Shared>,
}

impl ShardManager {
    /// Names the key → shard function below. Persisted with the append-only
    /// log, whose per-shard files are only valid for a server that routes
    /// keys the same way: change it whenever `shard_index_for_key` changes.
    pub const HASH_SCHEME: &'static str = "fnv1a-64";

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
        let shared = Arc::new(Shared::default());
        let mut shards = Vec::with_capacity(num_shards);

        for i in 0..num_shards {
            let backend = if solo {
                StoreBackend::Solo {
                    store: Arc::new(SoloStore::new()),
                    tick: AtomicU64::new(0),
                }
            } else {
                StoreBackend::Locked {
                    store: Arc::new(Mutex::new(ShardStore::new())),
                    tick: Arc::new(AtomicU64::new(0)),
                }
            };
            shards.push(ShardHandle::new(
                backend,
                pubsub.clone(),
                notify_flags.clone(),
                i as u32,
                shared.clone(),
            ));
        }

        ShardManager {
            shards,
            num_shards,
            solo,
            shared,
        }
    }

    /// Start logging every change to `writer`. Once only: returns false if a
    /// writer was already attached.
    pub fn attach_aof(&self, writer: AofWriter) -> bool {
        self.shared.aof.set(writer).is_ok()
    }

    pub fn aof(&self) -> Option<&AofWriter> {
        self.shared.aof.get()
    }

    /// Stop every store from changing: set the flag each command checks under
    /// its shard's lock, then take and release each lock. When this returns,
    /// every change made so far has also been handed to the log writer (same
    /// lock hold), and none can follow. Used by shutdown.
    pub fn freeze(&self) {
        self.shared.frozen.store(true, Ordering::SeqCst);
        for shard in &self.shards {
            shard.with_store(|_| ());
        }
    }

    /// Undo [`freeze`](Self::freeze) (a `SHUTDOWN` whose final save failed).
    pub fn unfreeze(&self) {
        self.shared.frozen.store(false, Ordering::SeqCst);
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
