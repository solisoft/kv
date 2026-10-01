use bytes::Bytes;
use std::cell::{Cell, RefCell, UnsafeCell};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::{Mutex, MutexGuard};
use solikv_core::ShardStore;
use solikv_pubsub::PubSubBroker;

use solikv_core::CommandResponse;
use solikv_persist::AofWriter;

use crate::aof_log::Effects;
use crate::dispatch::{NOTIFY_EXPIRED, NOTIFY_HASH, NOTIFY_KEYEVENT, NOTIFY_KEYSPACE};
use crate::watch::WatchRegistry;

/// Reply to every command once the server has started shutting down.
pub const SHUTTING_DOWN: &str = "ERR server is shutting down";

/// State every shard handle shares.
#[derive(Default)]
struct Shared {
    /// Until when (unix ms) a `CLIENT PAUSE` holds expiry; every store of
    /// this manager reads it (see `ShardStore::set_expiry_hold`).
    expiry_hold: Arc<std::sync::atomic::AtomicU64>,
    /// The append-only log, once startup has loaded the dataset. Set once;
    /// commands replayed before it is set are, correctly, not logged again.
    aof: OnceLock<AofWriter>,
    /// Shutdown barrier: no command touches a store once this is set (see
    /// `ShardManager::freeze`).
    frozen: AtomicBool,
    /// Keys clients `WATCH` (marked by writes under the shard lock).
    watch: WatchRegistry,
    /// `INFO stats` counters.
    expired_keys: std::sync::atomic::AtomicU64,
    evicted_keys: std::sync::atomic::AtomicU64,
    /// Told about keys the server removes itself (expiry, eviction): client
    /// caches that hold them must be invalidated.
    removed_hook: OnceLock<RemovedHook>,
    /// `DEBUG SET-ACTIVE-EXPIRE 0` stops the active expiry cycle.
    active_expire_off: AtomicBool,
    /// Active expiry cycles per second (`hz`; 0 = unset, meaning 10).
    hz: std::sync::atomic::AtomicU32,
    /// The replication stream (see `crate::replication`).
    repl: Arc<crate::replication::ReplFeed>,
}

/// See `Shared::removed_hook`.
pub type RemovedHook = Arc<dyn Fn(&[(u32, Bytes)]) + Send + Sync>;

// ---------------------------------------------------------------------------
// The database a command runs in
// ---------------------------------------------------------------------------

thread_local! {
    /// Database of the command this thread is executing (`SELECT`). Set by
    /// the engine for the length of one command (scripts' `redis.call`
    /// inherit it); every shard visit selects it in its store.
    static CURRENT_DB: Cell<u32> = const { Cell::new(0) };
}

/// The database the running command works in.
#[inline]
pub fn current_db() -> u32 {
    CURRENT_DB.with(|c| c.get())
}

/// Switch the running command's database until the enclosing [`DbGuard`]
/// restores it (a script's `SELECT`).
pub(crate) fn set_current_db(db: u32) {
    CURRENT_DB.with(|c| c.set(db));
}

/// Runs the commands issued while it lives in database `db`; restores the
/// previous one on drop.
pub struct DbGuard {
    prev: u32,
}

impl DbGuard {
    #[inline]
    pub fn enter(db: u32) -> Self {
        Self {
            prev: CURRENT_DB.with(|c| c.replace(db)),
        }
    }
}

impl Drop for DbGuard {
    #[inline]
    fn drop(&mut self) {
        let prev = self.prev;
        CURRENT_DB.with(|c| c.set(prev));
    }
}

// ---------------------------------------------------------------------------
// Holding several shards at once
// ---------------------------------------------------------------------------

/// Shards this thread holds locked for an atomic multi-shard operation
/// (`ShardManager::lock_shards`): commands run meanwhile use the held store
/// instead of locking it again, which with a non-reentrant mutex would
/// deadlock — and would let another client in between two of them.
#[derive(Default)]
struct Held {
    /// Identity of the `Shared` the stores belong to (0 = none held).
    owner: usize,
    stores: Vec<*mut ShardStore>,
}

thread_local! {
    static HELD_OWNER: Cell<usize> = const { Cell::new(0) };
    static HELD: RefCell<Held> = RefCell::new(Held::default());
}

/// The store of shard `idx`, if this thread holds it.
#[inline]
fn held_store(shared: &Arc<Shared>, idx: u32) -> Option<*mut ShardStore> {
    let owner = HELD_OWNER.with(|o| o.get());
    if owner == 0 || owner != Arc::as_ptr(shared) as usize {
        return None;
    }
    HELD.with(|h| {
        h.borrow()
            .stores
            .get(idx as usize)
            .copied()
            .filter(|p| !p.is_null())
    })
}

/// How long a waiter spins for a shard lock before parking.
const SPIN_FOR: std::time::Duration = std::time::Duration::from_micros(50);

/// Take a shard's lock, letting one waiter at a time spin for it.
///
/// parking_lot's mutex spins only while no thread is parked on it. Under a
/// hot key every waiter soon is, and from then on each hand-over is a futex
/// wake: the lock sits free until the woken thread is scheduled again —
/// measured at a quarter of the time with six clients pipelining to one key.
/// A thread spinning here instead takes the lock the moment it is released.
/// One spinner is enough to bridge each hand-over; the others park, so a
/// queue of waiters costs one core, not one per waiter. parking_lot still
/// hands the lock to a parked thread now and then (its eventual fairness),
/// so the spinner cannot starve them.
#[inline]
fn lock_store<'a>(
    store: &'a Mutex<ShardStore>,
    spinning: &AtomicBool,
) -> MutexGuard<'a, ShardStore> {
    if let Some(guard) = store.try_lock() {
        return guard;
    }
    lock_store_slow(store, spinning)
}

#[cold]
fn lock_store_slow<'a>(
    store: &'a Mutex<ShardStore>,
    spinning: &AtomicBool,
) -> MutexGuard<'a, ShardStore> {
    if !spinning.load(Ordering::Relaxed) && !spinning.swap(true, Ordering::Acquire) {
        let start = std::time::Instant::now();
        let mut spins = 0u32;
        let got = loop {
            if !store.is_locked() {
                if let Some(guard) = store.try_lock() {
                    break Some(guard);
                }
            }
            std::hint::spin_loop();
            spins = spins.wrapping_add(1);
            if spins.is_multiple_of(256) && start.elapsed() >= SPIN_FOR {
                break None;
            }
        };
        spinning.store(false, Ordering::Release);
        if let Some(guard) = got {
            return guard;
        }
    }
    store.lock()
}

/// Shards locked by [`ShardManager::lock_shards`]; released on drop.
pub struct HeldShards<'a> {
    guards: Vec<(u32, MutexGuard<'a, ShardStore>)>,
    /// Whether this guard made this thread an owner (outermost guard).
    outermost: bool,
    /// The replication stream this hold opened a block in: its records are
    /// sent as one `MULTI … EXEC` when the hold ends, still under the locks.
    repl: Option<Arc<crate::replication::ReplFeed>>,
}

impl Drop for HeldShards<'_> {
    fn drop(&mut self) {
        if let Some(feed) = self.repl.take() {
            feed.end_tx();
        }
        HELD.with(|h| {
            let mut h = h.borrow_mut();
            for (i, _) in &self.guards {
                if let Some(p) = h.stores.get_mut(*i as usize) {
                    *p = std::ptr::null_mut();
                }
            }
            if self.outermost {
                h.owner = 0;
                h.stores.clear();
            }
        });
        if self.outermost {
            HELD_OWNER.with(|o| o.set(0));
        }
        // The guards unlock as the Vec drops, after the pointers are gone.
        self.guards.clear();
    }
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
    fn new(store: ShardStore) -> Self {
        Self {
            inner: UnsafeCell::new(store),
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
        /// Set while a thread spins for the lock (see [`lock_store`]).
        spinning: AtomicBool,
    },
    /// Redis-shaped single-thread path: no mutex on the hot path.
    Solo { store: Arc<SoloStore> },
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
    fn locked<T>(&self, f: impl FnOnce(&mut ShardStore, ()) -> T) -> T {
        match self.backend.as_ref() {
            StoreBackend::Locked { store, spinning } => {
                if let Some(p) = held_store(&self.shared, self.index) {
                    // SAFETY: this thread holds the shard's lock (a guard in
                    // a live `HeldShards`), and nothing else uses the store
                    // through that guard while the pointer is registered.
                    return f(unsafe { &mut *p }, ());
                }
                let mut guard = lock_store(store, spinning);
                f(&mut guard, ())
            }
            StoreBackend::Solo { store } => store.with_mut(|s| f(s, ())),
        }
    }

    /// The replication stream, when writes must be recorded in it, with
    /// whether this shard is part of this thread's multi-shard hold (its
    /// records then join the hold's block).
    #[inline]
    fn repl_sink(&self) -> Option<crate::aof_log::ReplSink<'_>> {
        let feed = &self.shared.repl;
        if !feed.producing() {
            return None;
        }
        Some(crate::aof_log::ReplSink {
            feed,
            held: held_store(&self.shared, self.index).is_some(),
        })
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
        let aof = self.aof_on();
        let repl = self.repl_sink();
        let db = current_db();
        let (result, lazy_expired, expired_fields) = self.locked(|store, _| {
            // Checked under the lock: once `freeze` has taken and released
            // this lock, nothing changes the store again.
            if self.shared.frozen.load(Ordering::Relaxed) {
                return (
                    CommandResponse::error(SHUTTING_DOWN),
                    Vec::new(),
                    Vec::new(),
                );
            }
            store.select(db);
            let mut effects = Effects::new(aof.is_some() || repl.is_some());
            let result = f(store, &mut effects);
            let lazy_expired = std::mem::take(&mut store.expired_buffer);
            let expired_fields = if store.expired_fields.is_empty() {
                Vec::new()
            } else {
                std::mem::take(&mut store.expired_fields)
            };
            if aof.is_some() || repl.is_some() {
                crate::aof_log::log_under_lock(
                    aof,
                    repl,
                    self.index,
                    &mut store.log_seq,
                    db,
                    &result,
                    &lazy_expired,
                    &expired_fields,
                    explicit.then_some(&mut effects),
                    &[],
                );
            }
            // Still under the lock: see `watch` for why that matters.
            let watch = &self.shared.watch;
            if watch.is_active() {
                if !result.is_error() {
                    crate::watch::mark_running(watch);
                }
                for (d, k) in lazy_expired.iter() {
                    watch.touch(*d, k);
                }
                for (d, k, _) in expired_fields.iter() {
                    watch.touch(*d, k);
                }
            }
            (result, lazy_expired, expired_fields)
        });
        self.emit_field_expirations(&expired_fields);

        if !lazy_expired.is_empty() {
            self.shared
                .expired_keys
                .fetch_add(lazy_expired.len() as u64, Ordering::Relaxed);
            self.emit_expired_notifications(&lazy_expired);
        }
        result
    }

    /// Remove keys under the lock with `remove` (active expiry, eviction),
    /// logging each as a `DEL` and marking watchers, as a command's own
    /// expiries are. Returns the keys removed with their databases.
    fn remove_keys(
        &self,
        remove: impl FnOnce(&mut ShardStore) -> Vec<(u32, Bytes)>,
    ) -> Vec<(u32, Bytes)> {
        let aof = self.aof_on();
        let repl = self.repl_sink();
        let (removed, fields) = self.locked(|store, _| {
            if self.shared.frozen.load(Ordering::Relaxed) {
                return (Vec::new(), Vec::new());
            }
            let mut removed = std::mem::take(&mut store.expired_buffer);
            removed.extend(remove(store));
            let fields = std::mem::take(&mut store.expired_fields);
            if removed.is_empty() && fields.is_empty() {
                return (removed, fields);
            }
            if aof.is_some() || repl.is_some() {
                let db = store.db();
                crate::aof_log::log_under_lock(
                    aof,
                    repl,
                    self.index,
                    &mut store.log_seq,
                    db,
                    &CommandResponse::Ok,
                    &[],
                    &fields,
                    None,
                    &removed,
                );
            }
            let watch = &self.shared.watch;
            if watch.is_active() {
                for (d, k) in removed.iter() {
                    watch.touch(*d, k);
                }
                for (d, k, _) in fields.iter() {
                    watch.touch(*d, k);
                }
            }
            (removed, fields)
        });
        self.emit_field_expirations(&fields);
        removed
    }

    /// One active-expiry cycle on this shard (Redis' `activeExpireCycle`,
    /// see `ShardStore::active_expire_cycle`), run by the server's timer so
    /// keys expire whether or not anything touches them. Logged and
    /// announced like lazy expiry.
    pub fn active_expire_cycle(&self, budget: std::time::Duration) -> usize {
        let removed = self.remove_keys(|store| store.active_expire_cycle(budget));
        if !removed.is_empty() {
            self.shared
                .expired_keys
                .fetch_add(removed.len() as u64, Ordering::Relaxed);
            self.emit_expired_notifications(&removed);
        }
        removed.len()
    }

    /// Evict one key under `policy` (see `ShardStore::evict_one`); logged
    /// as a `DEL` and announced as `evicted`.
    pub fn evict_one(
        &self,
        policy: solikv_core::store::EvictionPolicy,
        samples: usize,
    ) -> Option<(u32, Bytes)> {
        let removed =
            self.remove_keys(|store| store.evict_one(policy, samples).into_iter().collect());
        let evicted = removed.last().cloned();
        self.emit_notifications(&removed, crate::dispatch::NOTIFY_EVICTED, "evicted");
        evicted
    }

    /// Keys evictable under `policy` on this shard.
    pub fn evictable(&self, policy: solikv_core::store::EvictionPolicy) -> usize {
        self.with_store(|s| s.evictable(policy))
    }

    /// Append a record to this shard's log under its lock (transaction
    /// markers).
    pub(crate) fn log_marker(&self, record: &[u8]) {
        if let Some(w) = self.shared.aof.get() {
            self.locked(|store, _| {
                if w.shard_on(self.index) {
                    w.log_raw(self.index, &mut store.log_seq, record)
                }
            });
        }
    }

    /// The log writer, when this shard's changes are logged (see
    /// `AofWriter::shard_on`). Read under the shard's lock.
    #[inline]
    fn aof_on(&self) -> Option<&AofWriter> {
        self.shared.aof.get().filter(|w| w.shard_on(self.index))
    }

    /// This shard's position, which is also its log's number.
    pub fn index(&self) -> u32 {
        self.index
    }

    /// `hexpired` events for hash fields that expired (Redis 7.4 sends one
    /// per key, whatever the number of fields).
    fn emit_field_expirations(&self, fields: &[(u32, Bytes, Vec<Bytes>)]) {
        if fields.is_empty() {
            return;
        }
        let keys: Vec<(u32, Bytes)> = fields.iter().map(|(d, k, _)| (*d, k.clone())).collect();
        self.emit_notifications(&keys, NOTIFY_HASH, "hexpired");
    }

    fn emit_expired_notifications(&self, keys: &[(u32, Bytes)]) {
        self.emit_notifications(keys, NOTIFY_EXPIRED, "expired");
    }

    fn emit_notifications(&self, keys: &[(u32, Bytes)], class: u16, event: &'static str) {
        if keys.is_empty() {
            return;
        }
        if let Some(hook) = self.shared.removed_hook.get() {
            hook(keys);
        }
        let (Some(pubsub), Some(nf)) = (&self.pubsub, &self.notify_flags) else {
            return;
        };
        let flags = nf.load(Ordering::Relaxed);
        if flags == 0 || flags & class == 0 {
            return;
        }
        for (db, key) in keys {
            if flags & NOTIFY_KEYSPACE != 0 {
                let key_str = String::from_utf8_lossy(key);
                let channel = Bytes::from(format!("__keyspace@{}__:{}", db, key_str));
                pubsub.publish(channel, Bytes::from_static(event.as_bytes()));
            }
            if flags & NOTIFY_KEYEVENT != 0 {
                let channel = Bytes::from(format!("__keyevent@{}__:{}", db, event));
                pubsub.publish(channel, key.clone());
            }
        }
    }

    pub fn with_store<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&ShardStore) -> R,
    {
        match self.backend.as_ref() {
            StoreBackend::Locked { store, spinning } => {
                if let Some(p) = held_store(&self.shared, self.index) {
                    // SAFETY: see `locked`.
                    return f(unsafe { &*p });
                }
                let guard = lock_store(store, spinning);
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
    ///
    /// A key goes to the shard of its Redis Cluster slot (CRC16 of the key,
    /// or of its `{hash tag}`, mod 16384), slot mod shard count. Keys that
    /// share a tag therefore share a shard, as they share a slot — clients
    /// use tags to keep multi-key commands on one node, and here that also
    /// keeps them on one lock.
    pub const HASH_SCHEME: &'static str = "crc16-slot-mod";

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
            let mut store = ShardStore::new();
            store.set_expiry_hold(shared.expiry_hold.clone());
            let backend = if solo {
                StoreBackend::Solo {
                    store: Arc::new(SoloStore::new(store)),
                }
            } else {
                StoreBackend::Locked {
                    store: Arc::new(Mutex::new(store)),
                    spinning: AtomicBool::new(false),
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
        if self.shared.aof.get().is_some() {
            return false;
        }
        // Each shard's numbering continues from its counter. Nothing is
        // numbered until the writer is attached, so the counters cannot move
        // between here and the `set`.
        for (i, shard) in self.shards.iter().enumerate() {
            shard.with_store(|s| writer.start_shard(i as u32, s.log_seq));
        }
        self.shared.aof.set(writer).is_ok()
    }

    /// The log writer, attached or not logging (`CONFIG SET appendonly no`
    /// keeps it, every shard off).
    pub fn aof(&self) -> Option<&AofWriter> {
        self.shared.aof.get()
    }

    /// The log writer while the log is on.
    pub fn aof_on(&self) -> Option<&AofWriter> {
        self.shared.aof.get().filter(|w| w.any_on())
    }

    /// The replication stream.
    pub fn repl(&self) -> &Arc<crate::replication::ReplFeed> {
        &self.shared.repl
    }

    /// Whether writes produce effect records (for the log or replicas).
    #[inline]
    pub fn logging(&self) -> bool {
        self.shared.aof.get().is_some_and(|w| w.any_on()) || self.shared.repl.producing()
    }

    /// Replica expiry semantics on every store: an expired key reads as
    /// missing and is not deleted (the master's `DEL` removes it), and the
    /// active cycle does not run. Off again on promotion.
    pub fn set_replica_expiry(&self, on: bool) {
        for shard in &self.shards {
            shard.with_store_mut_unlogged(|s| s.set_replica_expiry(on));
        }
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
        solikv_core::keyspace::hash_slot(key) as usize % self.num_shards
    }

    /// Keys expired so far (`INFO stats`).
    /// See `Shared::removed_hook` (set once).
    pub fn set_removed_hook(&self, hook: RemovedHook) {
        let _ = self.shared.removed_hook.set(hook);
    }

    /// Active expiry cycles per second (`hz`).
    pub fn hz(&self) -> u32 {
        match self.shared.hz.load(Ordering::Relaxed) {
            0 => 10,
            n => n,
        }
    }

    pub fn set_hz(&self, hz: u32) {
        self.shared.hz.store(hz.clamp(1, 500), Ordering::Relaxed);
    }

    pub fn set_active_expire(&self, on: bool) {
        self.shared.active_expire_off.store(!on, Ordering::Relaxed);
    }

    pub fn active_expire_enabled(&self) -> bool {
        !self.shared.active_expire_off.load(Ordering::Relaxed)
    }

    /// Hold expiry until `until` (unix ms; 0 releases): `CLIENT PAUSE`.
    pub fn hold_expiry_until(&self, until: u64) {
        self.shared.expiry_hold.store(until, Ordering::Release);
    }

    pub fn expired_keys(&self) -> u64 {
        self.shared.expired_keys.load(Ordering::Relaxed)
    }

    /// Keys evicted so far (`INFO stats`).
    pub fn evicted_keys(&self) -> u64 {
        self.shared.evicted_keys.load(Ordering::Relaxed)
    }

    pub(crate) fn note_evicted(&self, n: u64) {
        self.shared.evicted_keys.fetch_add(n, Ordering::Relaxed);
    }

    /// The watch registry (`WATCH`).
    pub fn watch(&self) -> &WatchRegistry {
        &self.shared.watch
    }

    /// Lock the shards at `indices` (any order, duplicates allowed) until
    /// the returned guard drops. Meanwhile, commands this thread runs against
    /// those shards use the held locks: an operation spanning several shards
    /// runs as one step no other client can see half of.
    ///
    /// Locks are always taken in ascending shard order, and a thread already
    /// holding shards only adds the missing ones — so two lockers cannot
    /// deadlock as long as every multi-lock goes through here. Solo mode has
    /// nothing to lock: its single thread runs one command at a time.
    pub fn lock_shards(&self, indices: impl IntoIterator<Item = usize>) -> HeldShards<'_> {
        let mut idx: Vec<usize> = indices
            .into_iter()
            .filter(|&i| i < self.num_shards)
            .collect();
        if self.solo {
            idx.clear();
        }
        idx.sort_unstable();
        idx.dedup();

        let me = Arc::as_ptr(&self.shared) as usize;
        let owner = HELD_OWNER.with(|o| o.get());
        let outermost = owner == 0;
        if !outermost && owner != me {
            // Another manager's shards are held (a scratch engine inside a
            // replay): unrelated locks, never nested in practice.
            tracing::error!("lock_shards: this thread already holds another manager's shards");
        }
        let mut guards = Vec::with_capacity(idx.len());
        for i in idx {
            if !outermost && held_store(&self.shared, i as u32).is_some() {
                continue;
            }
            let StoreBackend::Locked { store, spinning } = self.shards[i].backend.as_ref() else {
                continue;
            };
            guards.push((i as u32, lock_store(store, spinning)));
        }
        HELD.with(|h| {
            let mut h = h.borrow_mut();
            if outermost {
                h.owner = me;
                h.stores.clear();
                h.stores.resize(self.num_shards, std::ptr::null_mut());
            }
            for (i, g) in guards.iter_mut() {
                h.stores[*i as usize] = &mut **g as *mut ShardStore;
            }
        });
        if outermost {
            HELD_OWNER.with(|o| o.set(me));
        }
        let repl = (outermost && self.shared.repl.producing()).then(|| {
            self.shared.repl.begin_tx();
            self.shared.repl.clone()
        });
        HeldShards {
            guards,
            outermost,
            repl,
        }
    }

    /// Shard `idx`'s store while `held` holds it (shared borrow, for
    /// operations that read several shards at once). `None` if not held.
    ///
    /// # Safety
    /// While the returned reference lives, the caller must not run anything
    /// against shard `idx` (`execute`, `with_store_mut_unlogged`, …): those
    /// reach the same store through the held lock, mutably.
    pub unsafe fn held_store<'a>(
        &'a self,
        held: &'a HeldShards<'_>,
        idx: usize,
    ) -> Option<&'a ShardStore> {
        let _ = held;
        // SAFETY: registered by a live `HeldShards` on this thread, whose
        // guard keeps the lock for at least `'a`; exclusivity is the
        // caller's contract above.
        held_store(&self.shared, idx as u32).map(|p| unsafe { &*p })
    }

    /// [`lock_shards`](Self::lock_shards) for the shards owning `keys`.
    pub fn lock_keys<'k>(&self, keys: impl IntoIterator<Item = &'k [u8]>) -> HeldShards<'_> {
        let idx: Vec<usize> = keys
            .into_iter()
            .map(|k| self.shard_index_for_key(k))
            .collect();
        self.lock_shards(idx)
    }

    /// [`lock_shards`](Self::lock_shards) for every shard.
    pub fn lock_all(&self) -> HeldShards<'_> {
        self.lock_shards(0..self.num_shards)
    }

    /// The shard holding the keys of cluster slot `slot` (keys go to the
    /// shard of their slot, see `HASH_SCHEME`).
    pub fn shard_for_slot(&self, slot: u16) -> &ShardHandle {
        if self.solo || self.num_shards == 1 {
            return &self.shards[0];
        }
        &self.shards[slot as usize % self.num_shards]
    }

    #[inline]
    pub fn shard_for_key_bytes(&self, key: &[u8]) -> &ShardHandle {
        &self.shards[self.shard_index_for_key(key)]
    }

    pub fn shard(&self, idx: usize) -> &ShardHandle {
        &self.shards[idx % self.num_shards]
    }

    /// Whether this thread holds any of this manager's shards
    /// ([`lock_shards`](Self::lock_shards)): it must then lock no other one,
    /// or it would take locks out of the ascending order that keeps two
    /// holders from waiting on each other.
    pub fn holding(&self) -> bool {
        HELD_OWNER.with(|o| o.get()) == Arc::as_ptr(&self.shared) as usize
    }

    /// Whether this thread holds shard `idx`.
    pub fn is_held(&self, idx: usize) -> bool {
        held_store(&self.shared, idx as u32).is_some()
    }

    pub fn all_shards(&self) -> &[ShardHandle] {
        &self.shards
    }

    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Every key of database 0 (the cluster dump; clusters have only 0).
    pub fn scan_all(&self) -> Vec<(bytes::Bytes, String, Option<u64>)> {
        let mut results = Vec::new();
        for shard in &self.shards {
            let shard_data = shard.with_store(|store| {
                store
                    .iter_all()
                    .filter(|(db, _, _)| *db == 0)
                    .map(|(_, k, e)| (k, e))
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

/// Run active expiry on every shard `hz` times a second (Redis' `hz`), on
/// the current Tokio runtime — in solo mode that is the only thread allowed
/// to touch the store. Each shard gets at most 1 ms of its lock per cycle.
pub fn spawn_active_expiry(shards: Arc<ShardManager>, hz: u32) -> tokio::task::JoinHandle<()> {
    shards.set_hz(hz);
    tokio::spawn(async move {
        let budget = std::time::Duration::from_millis(1);
        loop {
            // `hz` is read every cycle: `CONFIG SET hz` takes effect at once.
            let period = std::time::Duration::from_millis(1000 / shards.hz() as u64);
            tokio::time::sleep(period).await;
            if !shards.active_expire_enabled() {
                continue;
            }
            // Like Redis' slow cycle, spend up to 25% of the period on expiry
            // while shards keep finding expired keys, in 1 ms lock holds.
            // A single fixed pass per tick gave each shard 1 ms: enough with
            // 16 shards, but one shard (`--solo`) reclaimed 10 ms worth of
            // keys per second and fell behind any real expiry rate.
            let deadline = std::time::Instant::now() + period / 4;
            loop {
                let mut removed = 0;
                for shard in shards.all_shards() {
                    removed += shard.active_expire_cycle(budget);
                    // Let the connections this worker also serves run
                    // between shards.
                    tokio::task::yield_now().await;
                }
                if removed == 0 || std::time::Instant::now() >= deadline {
                    break;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_sharing_a_hash_tag_share_a_shard() {
        let m = ShardManager::new(16);
        let a = m.shard_index_for_key(b"{user:1}:name");
        let b = m.shard_index_for_key(b"{user:1}:email");
        assert_eq!(a, b);
        assert_eq!(
            m.shard_index_for_key(b"foo"),
            solikv_core::keyspace::hash_slot(b"foo") as usize % 16
        );
    }

    #[test]
    fn held_shards_are_reentrant_and_released() {
        let m = ShardManager::new(4);
        {
            let _outer = m.lock_shards([2, 0, 2]);
            // A command against a held shard does not deadlock.
            let r = m
                .shard(0)
                .execute(|s| CommandResponse::integer(s.dbsize() as i64));
            assert!(matches!(r, CommandResponse::Integer(0)));
            // Nested: adds shard 3, keeps 0 and 2.
            {
                let _inner = m.lock_shards([0, 3]);
                m.shard(3).execute(|_| CommandResponse::ok());
            }
            m.shard(2).execute(|_| CommandResponse::ok());
        }
        // Every lock is free again: another thread can take them.
        let m = Arc::new(m);
        let m2 = m.clone();
        std::thread::spawn(move || {
            let _all = m2.lock_all();
        })
        .join()
        .unwrap();
    }

    #[test]
    fn commands_run_in_the_current_database() {
        let m = ShardManager::new(1);
        {
            let _db = DbGuard::enter(5);
            m.shard(0).execute(|s| {
                s.set(
                    Bytes::from("k"),
                    solikv_core::RedisValue::String(Bytes::from("v")),
                    None,
                );
                CommandResponse::ok()
            });
        }
        let n0 = m
            .shard(0)
            .execute(|s| CommandResponse::integer(s.dbsize() as i64));
        assert!(matches!(n0, CommandResponse::Integer(0)));
        let _db = DbGuard::enter(5);
        let n5 = m
            .shard(0)
            .execute(|s| CommandResponse::integer(s.dbsize() as i64));
        assert!(matches!(n5, CommandResponse::Integer(1)));
    }
}
