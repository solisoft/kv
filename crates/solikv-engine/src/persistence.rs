//! Loading, snapshots, log rewrites and the shutdown sequence.
//!
//! File formats live in `solikv_persist`; this module decides when they are
//! written and read, because doing it right needs the shards: a snapshot is
//! taken one shard at a time under that shard's lock, and a log rewrite
//! switches each shard's log to the next generation inside the same lock
//! hold (see `solikv_persist::aof` for why).
//!
//! # Startup
//!
//! With `appendonly` on, only the log is loaded — `dump.rdb` may be older
//! and loading both applied every logged write twice. Without a manifest, a
//! pre-manifest `appendonly.aof` is loaded instead (Redis prefers the AOF
//! the same way), and failing that the snapshot. Whatever was loaded, unless
//! it came from a manifest this server can keep appending to, is rewritten
//! into a fresh log generation before the first client connects.
//!
//! Each old shard's base section and logs are replayed into a scratch
//! single-shard store — so a per-shard `FLUSHALL` record empties that
//! shard's keys and no others — then every key is moved to the shard that
//! owns it now. The shard count and hashing can therefore change between
//! runs.
//!
//! # Where captures run
//!
//! Serializing a shard happens under its lock, which on the solo runtime
//! means on its only worker thread; everything else (file I/O, fsync,
//! manifest) runs on a background thread. On the locked runtime the whole
//! save runs off the event loop and holds each shard only while that shard
//! is serialized.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use solikv_core::{KeyEntry, LoadingGuard};
use solikv_persist::aof::{self, Manifest, AOF_DIRNAME, LEGACY_AOF_NAME};
use solikv_persist::rdb::{self, SnapshotFile};
use solikv_persist::{AofWriter, FsyncPolicy, PersistStats};
use solikv_pubsub::PubSubBroker;

use crate::aof_log::PUT_RECORD;
use crate::shard::ShardManager;
use crate::CommandEngine;

/// Persistence settings, fixed at startup.
#[derive(Debug, Clone)]
pub struct PersistConfig {
    pub dir: PathBuf,
    /// Snapshot file is `{dir}/{dbfilename}.rdb`.
    pub dbfilename: String,
    pub appendonly: bool,
    pub fsync: FsyncPolicy,
    /// Rewrite the log once it has grown by this percentage since the last
    /// rewrite (0 = never automatically).
    pub auto_rewrite_percentage: u64,
    /// ... and is at least this many bytes.
    pub auto_rewrite_min_size: u64,
    /// Seconds between periodic snapshots (0 = none). Also decides whether
    /// shutdown takes a final snapshot, as Redis' `save` points do.
    pub save_interval: u64,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data"),
            dbfilename: "dump".to_string(),
            appendonly: false,
            fsync: FsyncPolicy::Everysec,
            auto_rewrite_percentage: 100,
            auto_rewrite_min_size: 64 * 1024 * 1024,
            save_interval: 300,
        }
    }
}

/// What startup loaded.
#[derive(Debug, Default)]
pub struct LoadReport {
    pub keys: u64,
    pub source: &'static str,
    pub log_records: u64,
}

pub struct Persistence {
    cfg: PersistConfig,
    pub stats: Arc<PersistStats>,
    /// The log's manifest while the log is active.
    manifest: Mutex<Option<Manifest>>,
    /// Must be rewritten before the log can be appended to (set by `load`).
    needs_rewrite: AtomicBool,
    /// A `.aof` from before manifests, to retire once rewritten.
    legacy_aof: Mutex<Option<PathBuf>>,
}

impl Persistence {
    pub fn new(cfg: PersistConfig) -> Self {
        let stats = Arc::new(PersistStats::new());
        stats.aof_enabled.store(cfg.appendonly, Ordering::Relaxed);
        Self {
            cfg,
            stats,
            manifest: Mutex::new(None),
            needs_rewrite: AtomicBool::new(false),
            legacy_aof: Mutex::new(None),
        }
    }

    /// Data arrived from outside the log (an import): rewrite the log at
    /// startup so it holds it.
    pub fn mark_needs_rewrite(&self) {
        self.needs_rewrite.store(true, Ordering::Relaxed);
    }

    pub fn config(&self) -> &PersistConfig {
        &self.cfg
    }

    pub fn aof_dir(&self) -> PathBuf {
        self.cfg.dir.join(AOF_DIRNAME)
    }

    pub fn rdb_path(&self) -> PathBuf {
        rdb::rdb_path(&self.cfg.dir, &self.cfg.dbfilename)
    }

    // -----------------------------------------------------------------------
    // Startup
    // -----------------------------------------------------------------------

    /// Load the dataset into `shards` (see the module doc for the order).
    /// Must run before the log is attached and before clients connect.
    pub fn load(&self, shards: &ShardManager) -> io::Result<LoadReport> {
        self.stats.loading.store(true, Ordering::Relaxed);
        let _loading = LoadingGuard::new();
        let result = self.load_inner(shards);
        self.stats.loading.store(false, Ordering::Relaxed);
        result
    }

    fn load_inner(&self, shards: &ShardManager) -> io::Result<LoadReport> {
        let mut report = LoadReport::default();
        if self.cfg.appendonly {
            let aof_dir = self.aof_dir();
            if let Some(m) = Manifest::load(&aof_dir)? {
                let (keys, records) = self.load_manifest(&aof_dir, &m, shards)?;
                report.keys = keys;
                report.log_records = records;
                report.source = "aof";
                let same_layout = m.shards as usize == shards.num_shards()
                    && (m.shards == 1 || m.hash == ShardManager::HASH_SCHEME);
                if !same_layout {
                    tracing::info!(
                        "Append-only log was written by {} shards ({}); rewriting it for {} ({})",
                        m.shards,
                        m.hash,
                        shards.num_shards(),
                        ShardManager::HASH_SCHEME
                    );
                    self.needs_rewrite.store(true, Ordering::Relaxed);
                }
                *self.manifest.lock() = Some(m);
                return Ok(report);
            }
            let legacy = self.cfg.dir.join(LEGACY_AOF_NAME);
            if legacy.exists() {
                tracing::info!(
                    "Loading {} (single-file log from an earlier version) — the snapshot \
                     next to it is not loaded, as the log already holds every write",
                    legacy.display()
                );
                let mut scratch = Scratch::new();
                let outcome = aof::replay_log(&legacy, |args| scratch.apply(args))?;
                report.log_records = outcome.commands;
                report.keys = scratch.move_into(shards);
                report.source = "legacy-aof";
                *self.legacy_aof.lock() = Some(legacy);
                self.needs_rewrite.store(true, Ordering::Relaxed);
                return Ok(report);
            }
            // First start with the log on: whatever the snapshot holds
            // becomes the log's base.
            self.needs_rewrite.store(true, Ordering::Relaxed);
        }
        let mut keys = 0u64;
        let mut batch = Router::new(shards);
        let files = rdb::load_rdb(&self.cfg.dir, &self.cfg.dbfilename, |k, e| {
            keys += 1;
            batch.push(k, e);
        })?;
        batch.flush();
        report.keys = keys;
        report.source = if files.is_empty() { "none" } else { "rdb" };
        Ok(report)
    }

    /// Replay every shard of a manifest; returns (keys, records).
    fn load_manifest(
        &self,
        aof_dir: &Path,
        m: &Manifest,
        shards: &ShardManager,
    ) -> io::Result<(u64, u64)> {
        let mut scratch: Vec<Scratch> = (0..m.shards).map(|_| Scratch::new()).collect();
        if let Some(b) = m.base {
            rdb::load_snapshot_file(&aof::base_path(aof_dir, b), |section, k, e| {
                let s = scratch.get_mut(section as usize).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "base section {} beyond the manifest's {} shards",
                            section, m.shards
                        ),
                    )
                })?;
                s.insert(k, e);
                Ok(())
            })?;
        }
        let mut records = 0u64;
        for (s, store) in scratch.iter_mut().enumerate() {
            for &g in &m.incrs {
                let path = aof::incr_path(aof_dir, g, s as u32);
                if !path.exists() {
                    continue;
                }
                let outcome = aof::replay_log(&path, |args| store.apply(args))?;
                records += outcome.commands;
                if let Some(len) = outcome.truncated_at {
                    // New records would land after the partial one and turn a
                    // recoverable tail into corruption mid-file.
                    aof::truncate_log(&path, len)?;
                }
            }
        }
        let mut keys = 0u64;
        for store in scratch {
            keys += store.move_into(shards);
        }
        Ok((keys, records))
    }

    /// After `load`: make sure the log can be appended to (rewriting it if
    /// the load requires), then start the writer and attach it to `shards`.
    pub fn start_aof(&self, shards: &ShardManager) -> io::Result<()> {
        if !self.cfg.appendonly {
            return Ok(());
        }
        let aof_dir = self.aof_dir();
        let n = shards.num_shards() as u32;
        let current = self.manifest.lock().clone();
        let gen = match current {
            Some(m) if !self.needs_rewrite.load(Ordering::Relaxed) => m.current_incr(),
            old => {
                // Nothing appends yet: a plain synchronous rewrite.
                let g = old.as_ref().map_or(1, max_gen_plus_one);
                aof::create_incr_files(&aof_dir, g, n)?;
                let mut file = SnapshotFile::create(&aof::base_path(&aof_dir, g))?;
                for i in 0..shards.num_shards() {
                    let body = capture_shard(shards, i, None)?;
                    file.write_section(i as u32, &body)?;
                }
                file.commit()?;
                let m = Manifest {
                    shards: n,
                    hash: ShardManager::HASH_SCHEME.to_string(),
                    base: Some(g),
                    incrs: vec![g],
                };
                m.store(&aof_dir)?;
                *self.manifest.lock() = Some(m);
                self.needs_rewrite.store(false, Ordering::Relaxed);
                if let Some(legacy) = self.legacy_aof.lock().take() {
                    let retired = legacy.with_extension("aof.migrated");
                    std::fs::rename(&legacy, &retired)?;
                    tracing::info!(
                        "Rewrote {} into {}; the old file is kept as {}",
                        legacy.display(),
                        aof_dir.display(),
                        retired.display()
                    );
                }
                g
            }
        };
        let m = self.manifest.lock().clone().expect("manifest set above");
        remove_unreferenced(&aof_dir, &m);
        let size = m.disk_size(&aof_dir);
        self.stats.aof_base_size.store(size, Ordering::Relaxed);
        self.stats.aof_current_size.store(size, Ordering::Relaxed);
        let writer = aof::spawn_aof_writer(&aof_dir, n, gen, self.cfg.fsync, self.stats.clone())?
            .single_threaded(shards.is_solo());
        shards.attach_aof(writer);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Snapshots
    // -----------------------------------------------------------------------

    /// `SAVE`: snapshot now, on this thread.
    pub fn save(&self, shards: &ShardManager, dirty: u64) -> io::Result<()> {
        let mut file = SnapshotFile::create(&self.rdb_path())?;
        for i in 0..shards.num_shards() {
            let body = capture_shard(shards, i, None)?;
            file.write_section(i as u32, &body)?;
        }
        file.commit()?;
        self.after_save(dirty);
        Ok(())
    }

    fn after_save(&self, dirty: u64) {
        let s = &self.stats;
        s.rdb_last_save_time.store(unix_now(), Ordering::Relaxed);
        s.rdb_dirty_at_last_save.store(dirty, Ordering::Relaxed);
        s.rdb_saves.fetch_add(1, Ordering::Relaxed);
        // Superseded per-shard files from before the single snapshot file:
        // left behind, they would be loaded if the new file ever went missing.
        for old in rdb::legacy_shard_files(&self.cfg.dir, &self.cfg.dbfilename) {
            if std::fs::remove_file(&old).is_ok() {
                tracing::info!("Removed superseded snapshot {}", old.display());
            }
        }
    }

    /// `BGSAVE`: snapshot in the background. Err when one is already running.
    pub fn bgsave(
        self: &Arc<Self>,
        shards: &Arc<ShardManager>,
        dirty: u64,
    ) -> Result<(), &'static str> {
        if self
            .stats
            .rdb_bgsave_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Err("ERR Background save already in progress");
        }
        let this = self.clone();
        // Solo: the store may only be read on this thread.
        let captured = if shards.is_solo() {
            match capture_shard(shards, 0, None) {
                Ok(body) => Some(vec![(0u32, body)]),
                Err(e) => {
                    this.finish_bgsave(Err(e), dirty);
                    return Ok(());
                }
            }
        } else {
            None
        };
        let shards = shards.clone();
        let spawned = std::thread::Builder::new()
            .name("solikv-bgsave".into())
            .spawn(move || {
                let res = (|| -> io::Result<()> {
                    let mut file = SnapshotFile::create(&this.rdb_path())?;
                    match captured {
                        Some(sections) => {
                            for (i, body) in sections {
                                file.write_section(i, &body)?;
                            }
                        }
                        None => {
                            for i in 0..shards.num_shards() {
                                let body = capture_shard(&shards, i, None)?;
                                file.write_section(i as u32, &body)?;
                            }
                        }
                    }
                    file.commit()?;
                    Ok(())
                })();
                this.finish_bgsave(res, dirty);
            });
        if spawned.is_err() {
            self.stats
                .rdb_bgsave_in_progress
                .store(false, Ordering::Release);
            return Err("ERR could not start the background save thread");
        }
        Ok(())
    }

    fn finish_bgsave(&self, res: io::Result<()>, dirty: u64) {
        match res {
            Ok(()) => {
                self.after_save(dirty);
                self.stats.rdb_last_bgsave_ok.store(true, Ordering::Relaxed);
                tracing::info!("Background snapshot saved to {}", self.rdb_path().display());
            }
            Err(e) => {
                self.stats
                    .rdb_last_bgsave_ok
                    .store(false, Ordering::Relaxed);
                tracing::error!(
                    "Background snapshot to {} failed: {}",
                    self.rdb_path().display(),
                    e
                );
            }
        }
        self.stats
            .rdb_bgsave_in_progress
            .store(false, Ordering::Release);
    }

    // -----------------------------------------------------------------------
    // Log rewrite
    // -----------------------------------------------------------------------

    /// `BGREWRITEAOF`: compact the log in the background.
    pub fn bgrewrite(self: &Arc<Self>, shards: &Arc<ShardManager>) -> Result<(), String> {
        let Some(writer) = shards.aof().cloned() else {
            return Err("ERR append-only file is disabled (--appendonly false)".into());
        };
        if self
            .stats
            .aof_rewrite_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Err("ERR Background append only file rewriting already in progress".into());
        }
        let aof_dir = self.aof_dir();
        // List the next generation before any record can go to it.
        let prepared = (|| -> io::Result<(Manifest, u64)> {
            let old = self
                .manifest
                .lock()
                .clone()
                .ok_or_else(|| io::Error::other("log has no manifest"))?;
            let g = max_gen_plus_one(&old);
            aof::create_incr_files(&aof_dir, g, old.shards)?;
            let mut mid = old.clone();
            mid.incrs.push(g);
            mid.store(&aof_dir)?;
            *self.manifest.lock() = Some(mid);
            Ok((old, g))
        })();
        let (old, g) = match prepared {
            Ok(v) => v,
            Err(e) => {
                self.finish_rewrite(Err(e));
                return Err("ERR could not start the append-only file rewrite".into());
            }
        };

        let captured = if shards.is_solo() {
            match capture_shard(shards, 0, Some((&writer, g))) {
                Ok(body) => Some(vec![(0u32, body)]),
                Err(e) => {
                    self.finish_rewrite(Err(e));
                    return Ok(());
                }
            }
        } else {
            None
        };
        let this = self.clone();
        let shards = shards.clone();
        let spawned = std::thread::Builder::new()
            .name("solikv-aofrw".into())
            .spawn(move || {
                let res = (|| -> io::Result<()> {
                    let mut file = SnapshotFile::create(&aof::base_path(&aof_dir, g))?;
                    match captured {
                        Some(sections) => {
                            for (i, body) in sections {
                                file.write_section(i, &body)?;
                            }
                        }
                        None => {
                            for i in 0..shards.num_shards() {
                                let body = capture_shard(&shards, i, Some((&writer, g)))?;
                                file.write_section(i as u32, &body)?;
                            }
                        }
                    }
                    file.commit()?;
                    // Every shard has rotated; make the old generation's tail
                    // durable before the manifest stops naming it... and the
                    // new base before it starts to.
                    writer.sync()?;
                    let new = Manifest {
                        shards: old.shards,
                        hash: old.hash.clone(),
                        base: Some(g),
                        incrs: vec![g],
                    };
                    new.store(&aof_dir)?;
                    *this.manifest.lock() = Some(new.clone());
                    remove_unreferenced(&aof_dir, &new);
                    let size = new.disk_size(&aof_dir);
                    this.stats.aof_base_size.store(size, Ordering::Relaxed);
                    this.stats.aof_current_size.store(size, Ordering::Relaxed);
                    Ok(())
                })();
                this.finish_rewrite(res);
            });
        if spawned.is_err() {
            self.finish_rewrite(Err(io::Error::other("could not spawn")));
            return Err("ERR could not start the append-only file rewrite".into());
        }
        Ok(())
    }

    fn finish_rewrite(&self, res: io::Result<()>) {
        match res {
            Ok(()) => {
                self.stats.aof_rewrites.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .aof_last_rewrite_ok
                    .store(true, Ordering::Relaxed);
                tracing::info!("Append-only file rewritten");
            }
            Err(e) => {
                // The manifest still names every generation written so far,
                // so nothing is lost; the next rewrite starts over.
                self.stats
                    .aof_last_rewrite_ok
                    .store(false, Ordering::Relaxed);
                tracing::error!("Append-only file rewrite failed: {}", e);
            }
        }
        self.stats
            .aof_rewrite_in_progress
            .store(false, Ordering::Release);
    }

    /// Whether the log has grown enough to rewrite on its own.
    pub fn should_auto_rewrite(&self) -> bool {
        let pct = self.cfg.auto_rewrite_percentage;
        if pct == 0 || !self.cfg.appendonly {
            return false;
        }
        let s = &self.stats;
        if s.aof_rewrite_in_progress.load(Ordering::Relaxed) {
            return false;
        }
        let current = s.aof_current_size.load(Ordering::Relaxed);
        let base = s.aof_base_size.load(Ordering::Relaxed).max(1);
        current >= self.cfg.auto_rewrite_min_size
            && current.saturating_sub(base).saturating_mul(100) / base >= pct
    }

    // -----------------------------------------------------------------------
    // Shutdown
    // -----------------------------------------------------------------------

    /// Stop every store from changing, make the log durable, and take a final
    /// snapshot when `save` asks for one (`None`: when periodic snapshots are
    /// configured, as Redis does for its `save` points). On error the stores
    /// are unfrozen and the server can keep running.
    pub fn shutdown(
        &self,
        shards: &ShardManager,
        dirty: u64,
        save: Option<bool>,
    ) -> io::Result<()> {
        shards.freeze();
        let res = (|| -> io::Result<()> {
            if let Some(w) = shards.aof() {
                w.sync()?;
            }
            if save.unwrap_or(self.cfg.save_interval > 0) {
                self.save(shards, dirty)?;
            }
            Ok(())
        })();
        if res.is_err() {
            shards.unfreeze();
        }
        res
    }
}

impl CommandEngine {
    /// Load persisted data into this engine's shards and start the log
    /// (startup; see [`Persistence::load`]).
    pub fn load_persisted(&self) -> io::Result<LoadReport> {
        let p = self.persistence();
        let report = p.load(&self.shards)?;
        p.start_aof(&self.shards)?;
        Ok(report)
    }
}

/// Serialize shard `i` under its lock; with `rotate`, switch its log to the
/// given generation in the same lock hold.
fn capture_shard(
    shards: &ShardManager,
    i: usize,
    rotate: Option<(&AofWriter, u64)>,
) -> io::Result<Vec<u8>> {
    shards.shard(i).with_store(|store| {
        let mut body = Vec::new();
        rdb::serialize_store(store, &mut body)?;
        if let Some((w, g)) = rotate {
            w.rotate(i as u32, g);
        }
        Ok(body)
    })
}

fn max_gen_plus_one(m: &Manifest) -> u64 {
    m.incrs.iter().copied().chain(m.base).max().unwrap_or(0) + 1
}

/// Delete base/incremental files the manifest does not name: leftovers of a
/// rewrite that crashed, or of the generation a rewrite replaced.
fn remove_unreferenced(aof_dir: &Path, m: &Manifest) {
    let keep: HashSet<PathBuf> = m.files(aof_dir).into_iter().collect();
    let Ok(entries) = std::fs::read_dir(aof_dir) else {
        return;
    };
    for e in entries.flatten() {
        let path = e.path();
        let name = e.file_name().to_string_lossy().to_string();
        let ours = name.starts_with("appendonly.")
            && (name.ends_with(".aof") || name.ends_with(".base.rdb") || name.ends_with(".tmp"))
            && name != "appendonly.manifest";
        if ours && !keep.contains(&path) {
            let _ = std::fs::remove_file(&path);
        }
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// One old shard's data, replayed in isolation.
struct Scratch {
    engine: Arc<CommandEngine>,
}

impl Scratch {
    fn new() -> Self {
        let engine = Arc::new(CommandEngine::new(
            Arc::new(ShardManager::new(1)),
            Arc::new(PubSubBroker::new()),
        ));
        engine.init_self_ref(Arc::downgrade(&engine));
        Self { engine }
    }

    fn insert(&mut self, key: Bytes, entry: KeyEntry) {
        self.engine
            .shards
            .shard(0)
            .with_store_mut_unlogged(|s| s.insert_entry(key, entry));
    }

    fn apply(&mut self, args: Vec<Bytes>) {
        if args[0].as_ref() == PUT_RECORD.as_bytes() {
            if args.len() == 3 {
                match rdb::decode_entry(&args[2]) {
                    Ok(entry) => self.insert(args[1].clone(), entry),
                    Err(e) => tracing::error!("Skipping undecodable {} record: {}", PUT_RECORD, e),
                }
            }
            return;
        }
        let name = String::from_utf8_lossy(&args[0]).to_uppercase();
        let resp = self.engine.execute(&name, &args[1..]);
        if resp.is_error() {
            tracing::warn!("Replayed {} failed: {:?}", name, resp);
        }
    }

    /// Move every key to the shard that owns it in `shards`.
    fn move_into(self, shards: &ShardManager) -> u64 {
        let entries = self
            .engine
            .shards
            .shard(0)
            .with_store_mut_unlogged(|s| s.drain_entries());
        let n = entries.len() as u64;
        let mut router = Router::new(shards);
        for (k, e) in entries {
            router.push(k, e);
        }
        router.flush();
        n
    }
}

/// Batches loaded keys per destination shard, so each shard is locked once
/// per batch rather than once per key.
struct Router<'a> {
    shards: &'a ShardManager,
    pending: Vec<Vec<(Bytes, KeyEntry)>>,
}

impl<'a> Router<'a> {
    const BATCH: usize = 4096;

    fn new(shards: &'a ShardManager) -> Self {
        Self {
            shards,
            pending: (0..shards.num_shards()).map(|_| Vec::new()).collect(),
        }
    }

    fn push(&mut self, key: Bytes, entry: KeyEntry) {
        let i = self.shards.shard_index_for_key(&key);
        self.pending[i].push((key, entry));
        if self.pending[i].len() >= Self::BATCH {
            self.flush_one(i);
        }
    }

    fn flush_one(&mut self, i: usize) {
        let batch = std::mem::take(&mut self.pending[i]);
        self.shards.shard(i).with_store_mut_unlogged(|s| {
            for (k, e) in batch {
                s.insert_entry(k, e);
            }
        });
    }

    fn flush(&mut self) {
        for i in 0..self.pending.len() {
            if !self.pending[i].is_empty() {
                self.flush_one(i);
            }
        }
    }
}

/// Route keys that were bucketed some other way (e.g. a Redis RDB import,
/// which buckets by cluster slot) to the shards that own them.
pub fn insert_routed(
    shards: &ShardManager,
    entries: impl IntoIterator<Item = (Bytes, KeyEntry)>,
) -> u64 {
    let mut router = Router::new(shards);
    let mut n = 0;
    for (k, e) in entries {
        router.push(k, e);
        n += 1;
    }
    router.flush();
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use solikv_core::CommandResponse;

    /// A directory removed on drop.
    struct TmpDir(PathBuf);

    impl TmpDir {
        fn new(tag: &str) -> Self {
            static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let p = std::env::temp_dir().join(format!(
                "solikv-persist-{}-{}-{}",
                tag,
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&p);
            Self(p)
        }
    }

    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn cfg(dir: &Path, appendonly: bool) -> PersistConfig {
        PersistConfig {
            dir: dir.to_path_buf(),
            dbfilename: "dump".into(),
            appendonly,
            fsync: FsyncPolicy::Everysec,
            auto_rewrite_percentage: 0,
            auto_rewrite_min_size: 0,
            save_interval: 300,
        }
    }

    /// An engine over `shards`, loaded from `dir`.
    fn start(dir: &Path, shards: ShardManager, appendonly: bool) -> Arc<CommandEngine> {
        let engine = Arc::new(
            CommandEngine::new(Arc::new(shards), Arc::new(PubSubBroker::new()))
                .with_persistence(Arc::new(Persistence::new(cfg(dir, appendonly)))),
        );
        engine.init_self_ref(Arc::downgrade(&engine));
        engine.load_persisted().expect("load");
        engine
    }

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn run(e: &CommandEngine, cmd: &[&str]) -> CommandResponse {
        let args: Vec<Bytes> = cmd[1..].iter().map(|s| b(s)).collect();
        e.execute(&cmd[0].to_uppercase(), &args)
    }

    fn get(e: &CommandEngine, key: &str) -> Option<String> {
        match run(e, &["GET", key]) {
            CommandResponse::BulkString(v) => Some(String::from_utf8_lossy(&v).to_string()),
            CommandResponse::Nil => None,
            other => panic!("GET {key}: {other:?}"),
        }
    }

    fn int(r: CommandResponse) -> i64 {
        match r {
            CommandResponse::Integer(n) => n,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn lrange(e: &CommandEngine, key: &str) -> Vec<String> {
        match run(e, &["LRANGE", key, "0", "-1"]) {
            CommandResponse::Array(items) => items
                .into_iter()
                .map(|i| match i {
                    CommandResponse::BulkString(v) => String::from_utf8_lossy(&v).to_string(),
                    other => panic!("{other:?}"),
                })
                .collect(),
            other => panic!("{other:?}"),
        }
    }

    fn stop(e: &CommandEngine) {
        e.shutdown(Some(false)).expect("shutdown");
    }

    fn wait_rewrite(e: &CommandEngine) {
        let s = &e.persistence().stats;
        for _ in 0..500 {
            if !s.aof_rewrite_in_progress.load(Ordering::Acquire) {
                assert!(
                    s.aof_last_rewrite_ok.load(Ordering::Relaxed),
                    "rewrite failed"
                );
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("rewrite did not finish");
    }

    /// The bug this module exists for: a snapshot plus the whole log replayed
    /// on top applied INCR and RPUSH twice.
    #[test]
    fn a_snapshot_and_the_log_never_apply_a_write_twice() {
        let dir = TmpDir::new("twice");
        let e = start(&dir.0, ShardManager::new(4), true);
        for _ in 0..5 {
            run(&e, &["INCR", "counter"]);
        }
        run(&e, &["RPUSH", "list", "a", "b"]);
        assert!(!run(&e, &["SAVE"]).is_error());
        run(&e, &["INCR", "counter"]);
        run(&e, &["RPUSH", "list", "c"]);
        assert!(!run(&e, &["BGREWRITEAOF"]).is_error());
        wait_rewrite(&e);
        run(&e, &["INCR", "counter"]);
        stop(&e);

        let e = start(&dir.0, ShardManager::new(4), true);
        assert_eq!(get(&e, "counter").as_deref(), Some("7"));
        assert_eq!(lrange(&e, "list"), vec!["a", "b", "c"]);
        stop(&e);

        // And again, from the log the second run wrote to.
        let e = start(&dir.0, ShardManager::new(4), true);
        assert_eq!(get(&e, "counter").as_deref(), Some("7"));
        assert_eq!(lrange(&e, "list"), vec!["a", "b", "c"]);
    }

    /// Writers keep going while the log is rewritten and a snapshot is taken:
    /// every write lands exactly once.
    #[test]
    fn concurrent_writes_during_a_rewrite_are_neither_lost_nor_doubled() {
        let dir = TmpDir::new("concurrent");
        let e = start(&dir.0, ShardManager::new(8), true);
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let e = e.clone();
                std::thread::spawn(move || {
                    for i in 0..2000 {
                        run(&e, &["INCR", &format!("c{}", i % 37)]);
                        run(&e, &["RPUSH", &format!("l{}", t), &i.to_string()]);
                        if i % 97 == 0 {
                            run(&e, &["MSET", &format!("m{i}"), "x", &format!("n{i}"), "y"]);
                            run(&e, &["DEL", &format!("m{i}")]);
                        }
                    }
                })
            })
            .collect();
        for _ in 0..3 {
            let _ = run(&e, &["BGREWRITEAOF"]);
            let _ = run(&e, &["BGSAVE"]);
            wait_rewrite(&e);
        }
        for t in threads {
            t.join().unwrap();
        }
        stop(&e);

        let e = start(&dir.0, ShardManager::new(8), true);
        let total: i64 = (0..37)
            .map(|k| get(&e, &format!("c{k}")).unwrap().parse::<i64>().unwrap())
            .sum();
        assert_eq!(total, 4 * 2000);
        for t in 0..4 {
            let l = lrange(&e, &format!("l{t}"));
            assert_eq!(l.len(), 2000);
            assert!(l.iter().enumerate().all(|(i, v)| *v == i.to_string()));
        }
        assert_eq!(get(&e, "m0"), None);
        assert_eq!(get(&e, "n0").as_deref(), Some("y"));
    }

    #[test]
    fn the_shard_count_can_change_between_runs() {
        let dir = TmpDir::new("reshard");
        let e = start(&dir.0, ShardManager::new(4), true);
        for i in 0..200 {
            run(&e, &["SET", &format!("k{i}"), &i.to_string()]);
        }
        // A per-shard FLUSHALL record must only empty that shard's keys.
        run(&e, &["FLUSHALL"]);
        for i in 0..200 {
            run(&e, &["SET", &format!("k{i}"), &(i * 2).to_string()]);
        }
        stop(&e);

        for shards in [
            ShardManager::new(3),
            ShardManager::solo_plain(),
            ShardManager::new(5),
        ] {
            let e = start(&dir.0, shards, true);
            for i in 0..200 {
                assert_eq!(get(&e, &format!("k{i}")), Some((i * 2).to_string()));
            }
            assert_eq!(int(run(&e, &["DBSIZE"])), 200);
            stop(&e);
        }
    }

    /// `EXPIRE k 10` logged verbatim restarted the TTL on every restart.
    #[test]
    fn ttls_replay_as_absolute_times() {
        let dir = TmpDir::new("ttl");
        let e = start(&dir.0, ShardManager::new(2), true);
        run(&e, &["SET", "a", "1", "PX", "1500"]);
        run(&e, &["SET", "b", "1"]);
        run(&e, &["PEXPIRE", "b", "1500"]);
        run(&e, &["SETEX", "c", "100", "1"]);
        std::thread::sleep(std::time::Duration::from_millis(700));
        stop(&e);

        let e = start(&dir.0, ShardManager::new(2), true);
        for k in ["a", "b"] {
            let pttl = int(run(&e, &["PTTL", k]));
            assert!(pttl > 0 && pttl <= 900, "{k}: pttl {pttl} was reset");
        }
        let ttl = int(run(&e, &["TTL", "c"]));
        assert!(ttl > 90 && ttl <= 100, "c: ttl {ttl}");
    }

    /// A key expired live and then recreated: replay must see the expiry at
    /// the same point, not at replay time.
    #[test]
    fn expiries_replay_where_they_happened() {
        let dir = TmpDir::new("expiry");
        let e = start(&dir.0, ShardManager::new(2), true);
        run(&e, &["SET", "k", "5", "PX", "50"]);
        std::thread::sleep(std::time::Duration::from_millis(120));
        assert_eq!(int(run(&e, &["INCR", "k"])), 1);
        run(&e, &["SET", "gone", "x", "PX", "60000"]);
        run(&e, &["PERSIST", "gone"]);
        stop(&e);

        let e = start(&dir.0, ShardManager::new(2), true);
        assert_eq!(get(&e, "k").as_deref(), Some("1"));
        assert_eq!(int(run(&e, &["PTTL", "k"])), -1);
        assert_eq!(int(run(&e, &["PTTL", "gone"])), -1);
    }

    #[test]
    fn spop_xadd_and_cross_shard_stores_replay_their_results() {
        let dir = TmpDir::new("effects");
        let e = start(&dir.0, ShardManager::new(4), true);
        run(&e, &["SADD", "s", "a", "b", "c", "d"]);
        let popped = run(&e, &["SPOP", "s"]);
        let id = run(&e, &["XADD", "st", "*", "f", "v"]);
        run(&e, &["PFADD", "h1", "x", "y"]);
        run(&e, &["PFADD", "h2", "z"]);
        run(&e, &["PFMERGE", "hm", "h1", "h2"]);
        run(&e, &["SET", "s1", "\x0f"]);
        run(&e, &["SET", "s2", "\x33"]);
        run(&e, &["BITOP", "OR", "bo", "s1", "s2"]);
        run(&e, &["ZADD", "z1", "1", "a", "2", "b"]);
        run(&e, &["ZUNIONSTORE", "zu", "1", "z1"]);
        run(&e, &["XGROUP", "CREATE", "st", "g", "0"]);
        run(&e, &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "st", ">"]);
        stop(&e);

        let e = start(&dir.0, ShardManager::new(4), true);
        let CommandResponse::BulkString(m) = popped else {
            panic!("{popped:?}")
        };
        assert_eq!(
            int(run(
                &e,
                &["SISMEMBER", "s", std::str::from_utf8(&m).unwrap()]
            )),
            0
        );
        assert_eq!(int(run(&e, &["SCARD", "s"])), 3);
        let CommandResponse::BulkString(id) = id else {
            panic!("{id:?}")
        };
        match run(&e, &["XRANGE", "st", "-", "+"]) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    CommandResponse::Array(entry) => {
                        assert!(matches!(&entry[0], CommandResponse::BulkString(i) if *i == id))
                    }
                    other => panic!("{other:?}"),
                }
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(int(run(&e, &["PFCOUNT", "hm"])), 3);
        assert_eq!(get(&e, "bo").as_deref(), Some("\x3f"));
        assert_eq!(int(run(&e, &["ZCARD", "zu"])), 2);
        // The group delivered the entry: it is pending for consumer c.
        match run(&e, &["XPENDING", "st", "g"]) {
            CommandResponse::Array(summary) => {
                assert!(
                    matches!(summary[0], CommandResponse::Integer(1)),
                    "{summary:?}"
                )
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_transaction_is_bracketed_in_the_log() {
        let dir = TmpDir::new("tx");
        let e = start(&dir.0, ShardManager::new(1), true);
        {
            let _tx = e.aof_transaction();
            run(&e, &["SET", "a", "1"]);
            run(&e, &["SET", "b", "2"]);
        }
        stop(&e);
        let log = std::fs::read(aof::incr_path(&dir.0.join(AOF_DIRNAME), 1, 0)).unwrap();
        let text = String::from_utf8_lossy(&log);
        let multi = text.find("MULTI").expect("MULTI marker");
        let exec = text.find("EXEC").expect("EXEC marker");
        assert!(multi < text.find("$1\r\na\r\n").unwrap());
        assert!(exec > text.find("$1\r\nb\r\n").unwrap());
    }

    #[test]
    fn a_log_from_an_earlier_version_loads_and_is_migrated() {
        let dir = TmpDir::new("legacy");
        // A snapshot next to the old log holds the same writes: loading both
        // was the old double-apply. Only the log counts.
        {
            let e = start(&dir.0, ShardManager::new(2), false);
            run(&e, &["RPUSH", "l", "x"]);
            assert!(!run(&e, &["SAVE"]).is_error());
        }
        {
            let mut old = solikv_persist::AofPersistence::new(
                &dir.0.join(LEGACY_AOF_NAME),
                FsyncPolicy::Always,
            )
            .unwrap();
            old.append(&[b("RPUSH"), b("l"), b("x")]).unwrap();
            old.append(&[b("SET"), b("a"), b("1")]).unwrap();
            old.append(&[b("INCR"), b("a")]).unwrap();
            old.append(&[b("MSET"), b("x"), b("1"), b("y"), b("2")])
                .unwrap();
        }
        let e = start(&dir.0, ShardManager::new(3), true);
        assert_eq!(lrange(&e, "l"), vec!["x"]);
        assert_eq!(get(&e, "a").as_deref(), Some("2"));
        assert_eq!(get(&e, "y").as_deref(), Some("2"));
        assert!(!dir.0.join(LEGACY_AOF_NAME).exists());
        assert!(dir.0.join("appendonly.aof.migrated").exists());
        run(&e, &["INCR", "a"]);
        stop(&e);
        let e = start(&dir.0, ShardManager::new(3), true);
        assert_eq!(get(&e, "a").as_deref(), Some("3"));
        assert_eq!(lrange(&e, "l"), vec!["x"]);
    }

    #[test]
    fn without_the_log_the_snapshot_is_loaded_whatever_wrote_it() {
        let dir = TmpDir::new("rdb");
        let e = start(&dir.0, ShardManager::new(4), false);
        for i in 0..50 {
            run(&e, &["SET", &format!("k{i}"), "v"]);
        }
        run(&e, &["XADD", "st", "1-1", "f", "v"]);
        assert!(!run(&e, &["SAVE"]).is_error());
        let e = start(&dir.0, ShardManager::new(2), false);
        assert_eq!(int(run(&e, &["DBSIZE"])), 51);
        assert_eq!(int(run(&e, &["XLEN", "st"])), 1);

        // Enabling the log later starts it from the snapshot.
        let e = start(&dir.0, ShardManager::new(2), true);
        run(&e, &["SET", "k0", "changed"]);
        stop(&e);
        let e = start(&dir.0, ShardManager::new(2), true);
        assert_eq!(int(run(&e, &["DBSIZE"])), 51);
        assert_eq!(get(&e, "k0").as_deref(), Some("changed"));
    }

    #[test]
    fn a_log_cut_mid_record_is_truncated_and_appended_to_cleanly() {
        let dir = TmpDir::new("torn");
        let e = start(&dir.0, ShardManager::new(1), true);
        run(&e, &["SET", "a", "1"]);
        stop(&e);
        let incr = aof::incr_path(&dir.0.join(AOF_DIRNAME), 1, 0);
        let mut bytes = std::fs::read(&incr).unwrap();
        bytes.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nb");
        std::fs::write(&incr, &bytes).unwrap();

        let e = start(&dir.0, ShardManager::new(1), true);
        assert_eq!(get(&e, "a").as_deref(), Some("1"));
        run(&e, &["SET", "c", "3"]);
        stop(&e);
        let e = start(&dir.0, ShardManager::new(1), true);
        assert_eq!(get(&e, "c").as_deref(), Some("3"));
        assert_eq!(get(&e, "b"), None);
    }

    #[test]
    fn shutdown_freezes_the_stores() {
        let dir = TmpDir::new("freeze");
        let e = start(&dir.0, ShardManager::new(2), true);
        run(&e, &["SET", "a", "1"]);
        e.shutdown(None).unwrap();
        assert!(run(&e, &["SET", "a", "2"]).is_error());
        assert!(dir.0.join("dump.rdb").exists(), "final snapshot");
        let e = start(&dir.0, ShardManager::new(2), true);
        assert_eq!(get(&e, "a").as_deref(), Some("1"));
    }

    #[test]
    fn bgsave_reports_a_save_in_progress_and_lastsave_moves() {
        let dir = TmpDir::new("bgsave");
        let e = start(&dir.0, ShardManager::new(2), false);
        run(&e, &["SET", "a", "1"]);
        let before = int(run(&e, &["LASTSAVE"]));
        assert!(matches!(
            run(&e, &["BGSAVE"]),
            CommandResponse::SimpleString(ref s) if s == "Background saving started"
        ));
        for _ in 0..500 {
            if !e
                .persistence()
                .stats
                .rdb_bgsave_in_progress
                .load(Ordering::Acquire)
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(dir.0.join("dump.rdb").exists());
        assert!(int(run(&e, &["LASTSAVE"])) >= before);
        assert!(run(&e, &["BGREWRITEAOF"]).is_error(), "no log to rewrite");
    }
}
