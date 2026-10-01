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
    /// Redis' `save` points: snapshot once `changes` writes have happened
    /// within `seconds` of the last snapshot, for any `(seconds, changes)`.
    /// Empty: no periodic snapshot, and none at shutdown.
    pub save: Vec<(u64, u64)>,
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
            save: vec![(300, 1)],
        }
    }
}

impl PersistConfig {
    /// Parse a `save` value (`"3600 1 300 100"`, `""` for none).
    pub fn parse_save(v: &str) -> Result<Vec<(u64, u64)>, String> {
        let nums: Vec<&str> = v.split_whitespace().collect();
        if !nums.len().is_multiple_of(2) {
            return Err("Invalid save parameters".into());
        }
        nums.chunks(2)
            .map(|p| match (p[0].parse::<u64>(), p[1].parse::<u64>()) {
                (Ok(secs), Ok(changes)) => Ok((secs, changes)),
                _ => Err("Invalid save parameters".into()),
            })
            .collect()
    }

    /// The `save` value as `CONFIG GET` reports it.
    pub fn save_string(&self) -> String {
        self.save
            .iter()
            .map(|(s, c)| format!("{} {}", s, c))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Whether a save point is met: `elapsed` seconds since the last
    /// snapshot, `changes` writes since then.
    pub fn save_due(&self, elapsed: u64, changes: u64) -> bool {
        changes > 0
            && self
                .save
                .iter()
                .any(|&(secs, n)| elapsed >= secs && changes >= n)
    }
}

/// What startup loaded.
#[derive(Debug, Default)]
pub struct LoadReport {
    pub keys: u64,
    pub source: &'static str,
    pub log_records: u64,
    /// The `FUNCTION` libraries the snapshot or log held (`FUNCTION DUMP`'s
    /// payload): `None` when neither said anything about them — data from
    /// before they were persisted there (see `legacy_functions_path`).
    pub functions: Option<Vec<u8>>,
}

/// Where libraries were kept before snapshots and the log held them. Read
/// once to migrate; removed once a snapshot or a log rewrite holds them.
pub fn legacy_functions_path(dir: &Path) -> PathBuf {
    dir.join("functions.lib")
}

/// The libraries at a snapshot's cut: read while shard 0's lock is held,
/// at the instant its snapshot begins (and, for a log rewrite, its log
/// switches generation) — see `CommandEngine::commit_functions`, which
/// changes them under the same lock.
struct FunctionsCut {
    registry: Option<Arc<crate::lua::FunctionRegistry>>,
    taken: parking_lot::Mutex<Option<Vec<u8>>>,
}

impl FunctionsCut {
    fn take_now(&self) {
        if let Some(r) = &self.registry {
            let dump = if r.is_empty() { Vec::new() } else { r.dump() };
            *self.taken.lock() = Some(dump);
        }
    }

    /// The libraries taken at the cut (none taken: none to write).
    fn result(&self) -> Vec<u8> {
        self.taken.lock().take().unwrap_or_default()
    }
}

pub struct Persistence {
    /// Settings; `appendfsync`, `save` and the rewrite thresholds change at
    /// runtime (`CONFIG SET`).
    cfg: parking_lot::RwLock<PersistConfig>,
    pub stats: Arc<PersistStats>,
    /// The log's manifest while the log is active.
    manifest: Mutex<Option<Manifest>>,
    /// Must be rewritten before the log can be appended to (set by `load`).
    needs_rewrite: AtomicBool,
    /// A `.aof` from before manifests, to retire once rewritten.
    legacy_aof: Mutex<Option<PathBuf>>,
    /// Held while a snapshot is taken (SAVE, BGSAVE, a log rewrite): a
    /// store runs one stepped snapshot at a time, so a second waits.
    snapshotting: Mutex<()>,
    /// The engine's `FUNCTION` libraries, written into every snapshot.
    functions: parking_lot::RwLock<Option<Arc<crate::lua::FunctionRegistry>>>,
}

impl Persistence {
    pub fn new(cfg: PersistConfig) -> Self {
        let stats = Arc::new(PersistStats::new());
        stats.aof_enabled.store(cfg.appendonly, Ordering::Relaxed);
        Self {
            cfg: parking_lot::RwLock::new(cfg),
            stats,
            manifest: Mutex::new(None),
            needs_rewrite: AtomicBool::new(false),
            legacy_aof: Mutex::new(None),
            snapshotting: Mutex::new(()),
            functions: parking_lot::RwLock::new(None),
        }
    }

    /// The libraries snapshots carry (set by the engine that owns this).
    pub fn set_functions(&self, registry: Arc<crate::lua::FunctionRegistry>) {
        *self.functions.write() = Some(registry);
    }

    fn functions_cut(&self) -> Arc<FunctionsCut> {
        Arc::new(FunctionsCut {
            registry: self.functions.read().clone(),
            taken: parking_lot::Mutex::new(None),
        })
    }

    /// Data arrived from outside the log (an import): rewrite the log at
    /// startup so it holds it.
    pub fn mark_needs_rewrite(&self) {
        self.needs_rewrite.store(true, Ordering::Relaxed);
    }

    /// The settings now.
    pub fn config(&self) -> PersistConfig {
        self.cfg.read().clone()
    }

    /// `CONFIG SET appendfsync`: the policy and, with the log on, its writer.
    pub fn set_fsync(&self, policy: FsyncPolicy, shards: &ShardManager) {
        self.cfg.write().fsync = policy;
        if let Some(w) = shards.aof() {
            w.set_policy(policy);
        }
    }

    /// `CONFIG SET save`.
    pub fn set_save(&self, points: Vec<(u64, u64)>) {
        self.cfg.write().save = points;
    }

    /// `CONFIG SET auto-aof-rewrite-percentage` / `-min-size`.
    pub fn set_auto_rewrite(&self, percentage: Option<u64>, min_size: Option<u64>) {
        let mut c = self.cfg.write();
        if let Some(p) = percentage {
            c.auto_rewrite_percentage = p;
        }
        if let Some(m) = min_size {
            c.auto_rewrite_min_size = m;
        }
    }

    pub fn aof_dir(&self) -> PathBuf {
        self.cfg.read().dir.join(AOF_DIRNAME)
    }

    pub fn rdb_path(&self) -> PathBuf {
        let cfg = self.config();
        rdb::rdb_path(&cfg.dir, &cfg.dbfilename)
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
        if self.cfg.read().appendonly {
            let aof_dir = self.aof_dir();
            if let Some(m) = Manifest::load(&aof_dir)? {
                let (keys, records, functions) = self.load_manifest(&aof_dir, &m, shards)?;
                report.keys = keys;
                report.log_records = records;
                report.functions = functions;
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
            let legacy = self.cfg.read().dir.join(LEGACY_AOF_NAME);
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
        let cfg = self.config();
        let mut functions = None;
        let files = rdb::load_rdb_with_functions(
            &cfg.dir,
            &cfg.dbfilename,
            |db, k, e| {
                keys += 1;
                batch.push(db, k, e);
            },
            |dump| {
                functions = Some(dump.to_vec());
                Ok(())
            },
        )?;
        batch.flush();
        report.functions = functions;
        report.keys = keys;
        report.source = if files.is_empty() { "none" } else { "rdb" };
        Ok(report)
    }

    /// Replay every shard of a manifest; returns (keys, records, the
    /// libraries if the base or the log said anything about them).
    fn load_manifest(
        &self,
        aof_dir: &Path,
        m: &Manifest,
        shards: &ShardManager,
    ) -> io::Result<(u64, u64, Option<Vec<u8>>)> {
        let mut scratch: Vec<Scratch> = (0..m.shards).map(|_| Scratch::new()).collect();
        if let Some(b) = m.base {
            let mut base_functions = None;
            rdb::load_snapshot_file_with_functions(
                &aof::base_path(aof_dir, b),
                |section, db, k, e| {
                    let s = scratch.get_mut(section as usize).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "base section {} beyond the manifest's {} shards",
                                section, m.shards
                            ),
                        )
                    })?;
                    s.insert(db, k, e);
                    Ok(())
                },
                |dump| {
                    base_functions = Some(dump);
                    Ok(())
                },
            )?;
            // The libraries are shard 0's: its log holds the changes since.
            if let (Some(dump), Some(s0)) = (base_functions, scratch.first_mut()) {
                s0.restore_functions(&dump)?;
            }
        }
        let mut records = 0u64;
        for (s, store) in scratch.iter_mut().enumerate() {
            for &g in &m.incrs {
                let path = aof::incr_path(aof_dir, g, s as u32);
                if !path.exists() {
                    continue;
                }
                store.next_file();
                let outcome = aof::replay_log(&path, |args| store.apply(args))?;
                records += outcome.commands;
                if let Some(len) = outcome.truncated_at {
                    // New records would land after the partial one and turn a
                    // recoverable tail into corruption mid-file.
                    aof::truncate_log(&path, len)?;
                }
            }
        }
        let functions = scratch.first().and_then(Scratch::functions);
        let mut keys = 0u64;
        for store in scratch {
            keys += store.move_into(shards);
        }
        Ok((keys, records, functions))
    }

    /// After `load`: make sure the log can be appended to (rewriting it if
    /// the load requires), then start the writer and attach it to `shards`.
    pub fn start_aof(&self, shards: &ShardManager) -> io::Result<()> {
        if !self.cfg.read().appendonly {
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
                let cut = self.functions_cut();
                for i in 0..shards.num_shards() {
                    let body = capture_shard(shards, i, None, Some(&cut))?;
                    file.write_section(i as u32, &body)?;
                }
                file.write_functions(&cut.result())?;
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
        let writer =
            aof::spawn_aof_writer(&aof_dir, n, gen, self.cfg.read().fsync, self.stats.clone())?
                .single_threaded(shards.is_solo());
        shards.attach_aof(writer);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Snapshots
    // -----------------------------------------------------------------------

    /// `SAVE`: snapshot now, on this thread.
    ///
    /// The caller waits for the whole save, as Redis' does; the other
    /// clients do not, in multi-shard mode: each shard is serialized a step
    /// at a time, its lock released in between (see `capture_shard_stepped`).
    /// A solo store can only be stepped from its own thread, which this
    /// command is occupying — there `SAVE` blocks the server, like Redis'.
    pub fn save(&self, shards: &ShardManager, dirty: u64) -> io::Result<()> {
        let _one_at_a_time = self.snapshotting.lock();
        let mut file = SnapshotFile::create(&self.rdb_path())?;
        let cut = self.functions_cut();
        for i in 0..shards.num_shards() {
            let body = if shards.is_solo() {
                capture_shard(shards, i, None, Some(&cut))?
            } else {
                capture_shard_stepped(shards, i, None, Some(&cut))?
            };
            file.write_section(i as u32, &body)?;
        }
        file.write_functions(&cut.result())?;
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
        let cfg = self.config();
        remove_legacy_functions(&cfg.dir);
        for old in rdb::legacy_shard_files(&cfg.dir, &cfg.dbfilename) {
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
        let cut = self.functions_cut();
        // Solo: the store may only be read on its own thread.
        let captured = if shards.is_solo() {
            match solo_capture(shards, None, cut.clone()) {
                Ok(c) => Some(c),
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
                    let _one_at_a_time = this.snapshotting.lock();
                    let mut file = SnapshotFile::create(&this.rdb_path())?;
                    match captured {
                        Some(solo) => file.write_section(0, &solo.body()?)?,
                        None => {
                            for i in 0..shards.num_shards() {
                                let body = capture_shard_stepped(&shards, i, None, Some(&cut))?;
                                file.write_section(i as u32, &body)?;
                            }
                        }
                    }
                    file.write_functions(&cut.result())?;
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
        let Some(writer) = shards.aof_on().cloned() else {
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
        let prepared = (|| -> io::Result<u64> {
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
            Ok(g)
        })();
        let g = match prepared {
            Ok(v) => v,
            Err(e) => {
                self.finish_rewrite(Err(e));
                return Err("ERR could not start the append-only file rewrite".into());
            }
        };
        self.run_rewrite(shards, writer, g, false)
    }

    /// `CONFIG SET appendonly yes`: start a log from the dataset as it is.
    ///
    /// The writes made while the log was off are in no log, so nothing an
    /// old manifest names can be appended to: the log starts over, at a
    /// generation no file on disk has used, from a rewrite. The manifest is
    /// removed first and written again only once that rewrite's base is on
    /// disk — a crash in between restarts from the snapshot, as Redis does.
    /// Each shard is logged from its cut in the rewrite on
    /// (`begin_capture`), so its base and its log meet exactly.
    pub fn enable_aof(self: &Arc<Self>, shards: &Arc<ShardManager>) -> Result<(), String> {
        if shards.aof_on().is_some() {
            self.cfg.write().appendonly = true;
            return Ok(());
        }
        if self
            .stats
            .aof_rewrite_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Err("ERR Background append only file rewriting already in progress".into());
        }
        let aof_dir = self.aof_dir();
        let n = shards.num_shards() as u32;
        let prepared = (|| -> io::Result<(AofWriter, u64)> {
            let previous = self.manifest.lock().take();
            match std::fs::remove_file(aof::manifest_path(&aof_dir)) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
            let g = previous
                .as_ref()
                .map_or(1, max_gen_plus_one)
                .max(max_gen_on_disk(&aof_dir) + 1);
            aof::create_incr_files(&aof_dir, g, n)?;
            let writer = match shards.aof() {
                Some(w) => w.clone(),
                None => {
                    let w = aof::spawn_aof_writer(
                        &aof_dir,
                        n,
                        g,
                        self.cfg.read().fsync,
                        self.stats.clone(),
                    )?
                    .single_threaded(shards.is_solo());
                    // Attached with every shard off; each comes on at its cut.
                    for i in 0..n {
                        w.set_shard_on(i, false);
                    }
                    shards.attach_aof(w.clone());
                    w
                }
            };
            Ok((writer, g))
        })();
        let (writer, g) = match prepared {
            Ok(v) => v,
            Err(e) => {
                self.finish_rewrite(Err(e));
                return Err("ERR could not start the append-only file".into());
            }
        };
        self.cfg.write().appendonly = true;
        self.stats.aof_enabled.store(true, Ordering::Relaxed);
        self.run_rewrite(shards, writer, g, true)
    }

    /// `CONFIG SET appendonly no`: stop logging. Each shard stops between
    /// two of its commands; what was logged is made durable. The log is
    /// kept, as Redis keeps it: restarting with the log on loads it as it
    /// was when it stopped.
    pub fn disable_aof(&self, shards: &ShardManager) -> Result<(), String> {
        if self.stats.aof_rewrite_in_progress.load(Ordering::Acquire) {
            return Err(
                "ERR Background append only file rewriting in progress, try again when it ends"
                    .into(),
            );
        }
        if let Some(w) = shards.aof() {
            for i in 0..shards.num_shards() {
                shards
                    .shard(i)
                    .with_store_mut_unlogged(|_| w.set_shard_on(i as u32, false));
            }
            w.sync().map_err(|e| format!("ERR {}", e))?;
        }
        self.cfg.write().appendonly = false;
        self.stats.aof_enabled.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// The rewrite into generation `g` (listed already, unless `fresh`: a
    /// log being turned on, whose manifest is written here at the end).
    fn run_rewrite(
        self: &Arc<Self>,
        shards: &Arc<ShardManager>,
        writer: AofWriter,
        g: u64,
        fresh: bool,
    ) -> Result<(), String> {
        let aof_dir = self.aof_dir();
        let n = shards.num_shards() as u32;
        let failed = {
            let this = self.clone();
            let shards = shards.clone();
            let writer = writer.clone();
            move |e: io::Error| {
                if fresh {
                    // No log after all: every shard off again.
                    for i in 0..shards.num_shards() {
                        shards
                            .shard(i)
                            .with_store_mut_unlogged(|_| writer.set_shard_on(i as u32, false));
                    }
                    this.cfg.write().appendonly = false;
                    this.stats.aof_enabled.store(false, Ordering::Relaxed);
                }
                this.finish_rewrite(Err(e));
            }
        };

        let cut = self.functions_cut();
        let captured = if shards.is_solo() {
            match solo_capture(shards, Some((&writer, g)), cut.clone()) {
                Ok(c) => Some(c),
                Err(e) => {
                    failed(e);
                    return Ok(());
                }
            }
        } else {
            None
        };
        let this = self.clone();
        let shards = shards.clone();
        let failed_in_thread = failed.clone();
        let spawned = std::thread::Builder::new()
            .name("solikv-aofrw".into())
            .spawn(move || {
                let res = (|| -> io::Result<()> {
                    let _one_at_a_time = this.snapshotting.lock();
                    let mut file = SnapshotFile::create(&aof::base_path(&aof_dir, g))?;
                    match captured {
                        Some(solo) => file.write_section(0, &solo.body()?)?,
                        None => {
                            for i in 0..shards.num_shards() {
                                let body = capture_shard_stepped(
                                    &shards,
                                    i,
                                    Some((&writer, g)),
                                    Some(&cut),
                                )?;
                                file.write_section(i as u32, &body)?;
                            }
                        }
                    }
                    file.write_functions(&cut.result())?;
                    file.commit()?;
                    // Every shard has rotated; make the old generation's tail
                    // durable before the manifest stops naming it... and the
                    // new base before it starts to.
                    writer.sync()?;
                    let new = Manifest {
                        shards: n,
                        hash: ShardManager::HASH_SCHEME.to_string(),
                        base: Some(g),
                        incrs: vec![g],
                    };
                    new.store(&aof_dir)?;
                    *this.manifest.lock() = Some(new.clone());
                    remove_unreferenced(&aof_dir, &new);
                    remove_legacy_functions(&this.config().dir);
                    let size = new.disk_size(&aof_dir);
                    this.stats.aof_base_size.store(size, Ordering::Relaxed);
                    this.stats.aof_current_size.store(size, Ordering::Relaxed);
                    Ok(())
                })();
                match res {
                    Ok(()) => this.finish_rewrite(Ok(())),
                    Err(e) => failed_in_thread(e),
                }
            });
        if spawned.is_err() {
            failed(io::Error::other("could not spawn"));
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
        let pct = self.cfg.read().auto_rewrite_percentage;
        if pct == 0 || !self.cfg.read().appendonly {
            return false;
        }
        let s = &self.stats;
        if s.aof_rewrite_in_progress.load(Ordering::Relaxed) {
            return false;
        }
        let current = s.aof_current_size.load(Ordering::Relaxed);
        let base = s.aof_base_size.load(Ordering::Relaxed).max(1);
        current >= self.cfg.read().auto_rewrite_min_size
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
            if save.unwrap_or(!self.cfg.read().save.is_empty()) {
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
        // Libraries first: a rewrite `start_aof` makes must hold them.
        let invalid = |e: String| io::Error::new(io::ErrorKind::InvalidData, e);
        let migrated = match &report.functions {
            Some(dump) => {
                let n = self.restore_functions_dump(dump).map_err(invalid)?;
                if n > 0 {
                    tracing::info!("Loaded {} function libraries", n);
                }
                false
            }
            None => match self.load_legacy_functions()? {
                Some(n) => {
                    tracing::info!("Loaded {} function libraries from functions.lib", n);
                    true
                }
                None => false,
            },
        };
        p.start_aof(&self.shards)?;
        if migrated {
            if let Some(w) = self.shards.aof() {
                // Into the log, so that the log alone restores them; then
                // the old file can go. (Without the log it goes after the
                // next snapshot, which holds them.)
                let dump = Bytes::from(self.functions_dump());
                let args = [
                    Bytes::from_static(b"RESTORE"),
                    dump,
                    Bytes::from_static(b"REPLACE"),
                ];
                self.commit_functions_logged(&args)?;
                w.sync()?;
                remove_legacy_functions(&p.config().dir);
            }
        }
        Ok(report)
    }
}

/// Serialize shard `i` under its lock; with `rotate`, switch its log to the
/// given generation in the same lock hold.
fn capture_shard(
    shards: &ShardManager,
    i: usize,
    rotate: Option<(&AofWriter, u64)>,
    cut: Option<&FunctionsCut>,
) -> io::Result<Vec<u8>> {
    shards.shard(i).with_store_mut_unlogged(|store| {
        let mut body = Vec::new();
        rdb::serialize_store(store, &mut body)?;
        if let Some((w, g)) = rotate {
            w.rotate(i as u32, &mut store.log_seq, g);
            // From here on its changes go to generation `g`, after this cut
            // — and are logged, if the log was just turned on.
            w.set_shard_on(i as u32, true);
        }
        if i == 0 {
            if let Some(cut) = cut {
                cut.take_now();
            }
        }
        Ok(body)
    })
}

/// Begin a point-in-time snapshot of every shard at one instant, and run
/// `at_instant` inside that instant (replication registers its replica
/// there, so the snapshot and the stream that follows it meet exactly).
/// Each shard's snapshot is then serialized with [`snapshot_step`], a step
/// at a time, while commands keep running (see `capture_shard_stepped`).
///
/// False — and nothing begun — when a shard already has a snapshot running
/// (a `BGSAVE`); the caller retries.
pub fn begin_snapshot_everywhere(shards: &ShardManager, at_instant: impl FnOnce()) -> bool {
    let _held = shards.lock_all();
    let mut begun = Vec::new();
    for i in 0..shards.num_shards() {
        let ok = shards
            .shard(i)
            .with_store_mut_unlogged(|s| s.begin_snapshot(rdb::snapshot_encoder));
        if !ok {
            for j in begun {
                shards
                    .shard(j)
                    .with_store_mut_unlogged(|s: &mut solikv_core::ShardStore| s.abort_snapshot());
            }
            return false;
        }
        begun.push(i);
    }
    at_instant();
    true
}

/// One step of shard `i`'s snapshot begun by [`begin_snapshot_everywhere`]:
/// the bytes serialized, and whether the shard is done. In solo mode, call
/// it on the runtime's thread only.
pub fn snapshot_step(shards: &ShardManager, i: usize) -> (Vec<u8>, bool) {
    shards
        .shard(i)
        .with_store_mut_unlogged(|s| s.snapshot_step(SNAPSHOT_STEP))
}

/// Entries serialized per lock hold by a stepped snapshot: small enough
/// that commands queued on the shard wait well under a millisecond.
const SNAPSHOT_STEP: usize = 1024;

/// [`capture_shard`] without holding the shard for the whole
/// serialization: the snapshot begins (and, with `rotate`, the log switches
/// generation) in one lock hold, then entries are serialized
/// `SNAPSHOT_STEP` at a time, the lock released in between. The store keeps
/// the result point-in-time (see `ShardStore::begin_snapshot`): a key
/// changed before the walk reaches it is saved as it was first. Multi-shard
/// mode only — a solo store may not be touched from this thread.
fn capture_shard_stepped(
    shards: &ShardManager,
    i: usize,
    rotate: Option<(&AofWriter, u64)>,
    cut: Option<&FunctionsCut>,
) -> io::Result<Vec<u8>> {
    let shard = shards.shard(i);
    let mut attempts = 0u32;
    loop {
        let began = shard.with_store_mut_unlogged(|store| begin_capture(store, i, rotate, cut));
        if began {
            break;
        }
        // A replica's full resynchronization holds the snapshot slot; it
        // ends.
        attempts += 1;
        if attempts > BEGIN_ATTEMPTS {
            return Err(io::Error::other(
                "a snapshot of this shard is already running",
            ));
        }
        std::thread::sleep(BEGIN_RETRY);
    }
    let mut body = Vec::new();
    loop {
        let (chunk, done) = shard.with_store_mut_unlogged(|s| s.snapshot_step(SNAPSHOT_STEP));
        body.extend_from_slice(&chunk);
        if done {
            return Ok(body);
        }
        // Let the commands that queued on the lock in.
        std::thread::yield_now();
    }
}

/// Begin shard `i`'s stepped snapshot, switching its log to the next
/// generation in the same lock hold with `rotate`. False when one is
/// already running.
fn begin_capture(
    store: &mut solikv_core::ShardStore,
    i: usize,
    rotate: Option<(&AofWriter, u64)>,
    cut: Option<&FunctionsCut>,
) -> bool {
    if !store.begin_snapshot(rdb::snapshot_encoder) {
        return false;
    }
    if let Some((w, g)) = rotate {
        w.rotate(i as u32, &mut store.log_seq, g);
        // From here on its changes go to generation `g`, after this cut —
        // and are logged, if the log was just turned on.
        w.set_shard_on(i as u32, true);
    }
    // Shard 0's cut is the libraries' (see `FunctionsCut`).
    if i == 0 {
        if let Some(cut) = cut {
            cut.take_now();
        }
    }
    true
}

/// How often, and how long, a snapshot waits for another to free the
/// shard's snapshot slot (one at a time per shard).
const BEGIN_RETRY: std::time::Duration = std::time::Duration::from_millis(20);
const BEGIN_ATTEMPTS: u32 = 3000;

/// The solo shard's snapshot for a background save or log rewrite.
enum SoloCapture {
    /// Taken whole, before returning: no runtime to step it on.
    Whole(Vec<u8>),
    /// Being taken a step at a time on the runtime's worker; the steps
    /// arrive here, `None` once complete.
    Steps(std::sync::mpsc::Receiver<io::Result<Option<Vec<u8>>>>),
}

impl SoloCapture {
    /// The serialized shard (waits for the steps, on the writer thread).
    fn body(self) -> io::Result<Vec<u8>> {
        match self {
            SoloCapture::Whole(body) => Ok(body),
            SoloCapture::Steps(rx) => {
                let mut body = Vec::new();
                loop {
                    match rx.recv() {
                        Ok(Ok(Some(chunk))) => body.extend_from_slice(&chunk),
                        Ok(Ok(None)) => return Ok(body),
                        Ok(Err(e)) => return Err(e),
                        Err(_) => return Err(io::Error::other("the snapshot was abandoned")),
                    }
                }
            }
        }
    }
}

/// Start the solo shard's snapshot for `BGSAVE` / `BGREWRITEAOF`.
///
/// The solo store belongs to the runtime's only worker, so its snapshot
/// cannot be stepped from the writer thread as a locked shard's is. It is
/// stepped by a task on that worker instead, yielding to the connections
/// between steps: the server keeps answering while a large dataset is
/// saved, rather than stalling for the whole serialization. The store keeps
/// the result point-in-time (`ShardStore::begin_snapshot`). Outside such a
/// runtime (no runtime, or one with several workers, where a task could run
/// on another thread), the shard is captured whole, here.
fn solo_capture(
    shards: &Arc<ShardManager>,
    rotate: Option<(&AofWriter, u64)>,
    cut: Arc<FunctionsCut>,
) -> io::Result<SoloCapture> {
    let single_worker = tokio::runtime::Handle::try_current()
        .ok()
        .filter(|h| h.metrics().num_workers() == 1);
    let Some(handle) = single_worker else {
        return capture_shard(shards, 0, rotate, Some(&cut)).map(SoloCapture::Whole);
    };
    // Begun here, inside the command: the snapshot is of this instant, as a
    // fork would be, whatever runs between now and the first step.
    let mut began = shards
        .shard(0)
        .with_store_mut_unlogged(|store| begin_capture(store, 0, rotate, Some(&cut)));
    let (tx, rx) = std::sync::mpsc::channel();
    let shards = shards.clone();
    let rotate = rotate.map(|(w, g)| (w.clone(), g));
    handle.spawn(async move {
        let shard = shards.shard(0);
        let mut attempts = 0u32;
        // Another snapshot (a replica's full sync) holds the slot: wait.
        while !began {
            let rotate = rotate.as_ref().map(|(w, g)| (w, *g));
            began =
                shard.with_store_mut_unlogged(|store| begin_capture(store, 0, rotate, Some(&cut)));
            if began {
                break;
            }
            attempts += 1;
            if attempts > BEGIN_ATTEMPTS {
                let _ = tx.send(Err(io::Error::other(
                    "a snapshot of this shard is already running",
                )));
                return;
            }
            tokio::time::sleep(BEGIN_RETRY).await;
        }
        loop {
            let (chunk, done) = shard.with_store_mut_unlogged(|s| s.snapshot_step(SNAPSHOT_STEP));
            if tx.send(Ok(Some(chunk))).is_err() {
                // The writer gave up: free the snapshot slot.
                shard.with_store_mut_unlogged(|s| s.abort_snapshot());
                return;
            }
            if done {
                let _ = tx.send(Ok(None));
                return;
            }
            tokio::task::yield_now().await;
        }
    });
    Ok(SoloCapture::Steps(rx))
}

/// A snapshot or log rewrite now holds the libraries: the file an earlier
/// version kept them in is superseded.
fn remove_legacy_functions(dir: &Path) {
    let path = legacy_functions_path(dir);
    if std::fs::remove_file(&path).is_ok() {
        tracing::info!(
            "Removed {} (the libraries are in the snapshot now)",
            path.display()
        );
    }
}

/// The highest generation any `appendonly.<gen>.…` file in `aof_dir` has
/// (0 for none): a new log must not append to a file left from an old one.
fn max_gen_on_disk(aof_dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(aof_dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.strip_prefix("appendonly.")?
                .split('.')
                .next()?
                .parse::<u64>()
                .ok()
        })
        .max()
        .unwrap_or(0)
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
    /// Database the log being replayed is in (its `SELECT` records). Every
    /// file starts in database 0.
    db: u32,
    /// The base or a `FUNCTION` record said what the libraries are.
    functions_seen: bool,
}

impl Scratch {
    fn new() -> Self {
        let engine = Arc::new(CommandEngine::new(
            Arc::new(ShardManager::new(1)),
            Arc::new(PubSubBroker::new()),
        ));
        engine.init_self_ref(Arc::downgrade(&engine));
        Self {
            engine,
            db: 0,
            functions_seen: false,
        }
    }

    /// The libraries of a base snapshot.
    fn restore_functions(&mut self, dump: &[u8]) -> io::Result<()> {
        self.engine
            .restore_functions_dump(dump)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.functions_seen = true;
        Ok(())
    }

    /// The libraries after replay, if the base or the log held any record
    /// of them.
    fn functions(&self) -> Option<Vec<u8>> {
        self.functions_seen.then(|| self.engine.functions_dump())
    }

    fn insert(&mut self, db: u32, key: Bytes, entry: KeyEntry) {
        self.engine
            .shards
            .shard(0)
            .with_store_mut_unlogged(|s| s.insert_entry_in(db, key, entry));
    }

    fn apply(&mut self, args: Vec<Bytes>) {
        if args[0].as_ref() == PUT_RECORD.as_bytes() {
            if args.len() == 3 {
                match rdb::decode_entry(&args[2]) {
                    Ok(entry) => self.insert(self.db, args[1].clone(), entry),
                    Err(e) => tracing::error!("Skipping undecodable {} record: {}", PUT_RECORD, e),
                }
            }
            return;
        }
        let name = String::from_utf8_lossy(&args[0]).to_uppercase();
        if name == "SELECT" {
            match args
                .get(1)
                .and_then(|a| std::str::from_utf8(a).ok()?.parse::<u32>().ok())
            {
                Some(db) => self.db = db,
                None => tracing::warn!("Replayed SELECT without a valid database"),
            }
            return;
        }
        if name == "FUNCTION" {
            self.functions_seen = true;
        }
        let resp = self.engine.execute_in_db(self.db, &name, &args[1..]);
        if resp.is_error() {
            tracing::warn!("Replayed {} failed: {:?}", name, resp);
        }
    }

    /// Next file: logs start in database 0.
    fn next_file(&mut self) {
        self.db = 0;
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
        for (db, k, e) in entries {
            router.push(db, k, e);
        }
        router.flush();
        n
    }
}

/// Batches loaded keys per destination shard, so each shard is locked once
/// per batch rather than once per key.
struct Router<'a> {
    shards: &'a ShardManager,
    pending: Vec<Vec<(u32, Bytes, KeyEntry)>>,
}

impl<'a> Router<'a> {
    const BATCH: usize = 4096;

    fn new(shards: &'a ShardManager) -> Self {
        Self {
            shards,
            pending: (0..shards.num_shards()).map(|_| Vec::new()).collect(),
        }
    }

    fn push(&mut self, db: u32, key: Bytes, entry: KeyEntry) {
        let i = self.shards.shard_index_for_key(&key);
        self.pending[i].push((db, key, entry));
        if self.pending[i].len() >= Self::BATCH {
            self.flush_one(i);
        }
    }

    fn flush_one(&mut self, i: usize) {
        let batch = std::mem::take(&mut self.pending[i]);
        self.shards.shard(i).with_store_mut_unlogged(|s| {
            for (db, k, e) in batch {
                s.insert_entry_in(db, k, e);
            }
            s.select(0);
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
    entries: impl IntoIterator<Item = (u32, Bytes, KeyEntry)>,
) -> u64 {
    let mut router = Router::new(shards);
    let mut n = 0;
    for (db, k, e) in entries {
        router.push(db, k, e);
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
            save: vec![(300, 1)],
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

    fn in_db(e: &CommandEngine, db: u32, cmd: &[&str]) -> CommandResponse {
        let args: Vec<Bytes> = cmd[1..].iter().map(|s| b(s)).collect();
        e.execute_in_db(db, &cmd[0].to_uppercase(), &args)
    }

    fn get_in(e: &CommandEngine, db: u32, key: &str) -> Option<String> {
        match in_db(e, db, &["GET", key]) {
            CommandResponse::BulkString(v) => Some(String::from_utf8_lossy(&v).to_string()),
            CommandResponse::Nil => None,
            other => panic!("GET {key}: {other:?}"),
        }
    }

    /// Keys in several databases come back in their databases, from the log
    /// (SELECT records), from its rewrite (database markers in the base) and
    /// from a snapshot; MOVE, SWAPDB and FLUSHDB replay per database.
    #[test]
    fn databases_survive_the_log_its_rewrite_and_snapshots() {
        for appendonly in [true, false] {
            let dir = TmpDir::new(if appendonly { "dbs-aof" } else { "dbs-rdb" });
            let e = start(&dir.0, ShardManager::new(4), appendonly);
            in_db(&e, 0, &["SET", "k", "zero"]);
            in_db(&e, 3, &["SET", "k", "three"]);
            in_db(&e, 3, &["RPUSH", "l", "a", "b"]);
            in_db(&e, 7, &["SET", "gone", "x"]);
            in_db(&e, 7, &["FLUSHDB"]);
            in_db(&e, 3, &["SET", "mv", "m"]);
            assert_eq!(int(in_db(&e, 3, &["MOVE", "mv", "4"])), 1);
            if appendonly {
                assert!(!run(&e, &["BGREWRITEAOF"]).is_error());
                wait_rewrite(&e);
            }
            in_db(&e, 5, &["SET", "five", "5"]);
            in_db(&e, 0, &["SWAPDB", "5", "6"]);
            in_db(&e, 3, &["INCR", "n"]);
            if !appendonly {
                assert!(!run(&e, &["SAVE"]).is_error());
            }
            stop(&e);

            // A different shard count, too.
            let e = start(&dir.0, ShardManager::new(3), appendonly);
            assert_eq!(get_in(&e, 0, "k").as_deref(), Some("zero"));
            assert_eq!(get_in(&e, 3, "k").as_deref(), Some("three"));
            assert_eq!(get_in(&e, 3, "n").as_deref(), Some("1"));
            assert_eq!(get_in(&e, 4, "mv").as_deref(), Some("m"));
            assert_eq!(get_in(&e, 3, "mv"), None);
            assert_eq!(get_in(&e, 7, "gone"), None);
            assert_eq!(get_in(&e, 5, "five"), None);
            assert_eq!(get_in(&e, 6, "five").as_deref(), Some("5"));
            assert_eq!(int(in_db(&e, 3, &["LLEN", "l"])), 2);
            stop(&e);
        }
    }

    /// RENAME, SMOVE and the STORE commands across shards are logged as
    /// their effect on each shard and replay to the same state.
    #[test]
    fn cross_shard_writes_replay_exactly() {
        let dir = TmpDir::new("xshard");
        let m = ShardManager::new(4);
        let a = "key:a";
        let other = (0..)
            .map(|i| format!("key:b{i}"))
            .find(|k| m.shard_index_for_key(k.as_bytes()) != m.shard_index_for_key(a.as_bytes()))
            .unwrap();
        let e = start(&dir.0, m, true);
        run(&e, &["SET", a, "v", "EX", "1000"]);
        assert!(!run(&e, &["RENAME", a, &other]).is_error());
        run(&e, &["SADD", "s1", "x", "y"]);
        run(&e, &["SADD", "s2", "y"]);
        assert_eq!(int(run(&e, &["SINTERSTORE", "si", "s1", "s2"])), 1);
        assert_eq!(int(run(&e, &["SMOVE", "s1", "s3", "x"])), 1);
        run(&e, &["ZADD", "z1", "1", "a"]);
        run(&e, &["ZADD", "z2", "2", "a"]);
        assert_eq!(int(run(&e, &["ZUNIONSTORE", "zu", "2", "z1", "z2"])), 1);
        stop(&e);

        let e = start(&dir.0, ShardManager::new(4), true);
        assert_eq!(get(&e, a), None);
        assert_eq!(get(&e, &other).as_deref(), Some("v"));
        assert!(int(run(&e, &["TTL", &other])) > 900);
        assert_eq!(int(run(&e, &["SCARD", "si"])), 1);
        assert_eq!(int(run(&e, &["SISMEMBER", "s3", "x"])), 1);
        assert_eq!(int(run(&e, &["SISMEMBER", "s1", "x"])), 0);
        match run(&e, &["ZSCORE", "zu", "a"]) {
            CommandResponse::BulkString(v) => assert_eq!(v, "3"),
            other => panic!("{other:?}"),
        }
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
    /// Big enough that a snapshot takes many steps, with the lock released
    /// between them while writers change keys the walk has and has not
    /// reached: after a restart from the rewritten log (and a BGSAVE file),
    /// the dataset is exactly what the live server held.
    #[test]
    fn stepped_snapshots_under_writes_restart_to_the_same_state() {
        let dir = TmpDir::new("stepped");
        let e = start(&dir.0, ShardManager::new(4), true);
        for chunk in 0..40 {
            let mut cmd = vec!["MSET".to_string()];
            for i in chunk * 1000..(chunk + 1) * 1000 {
                cmd.push(format!("k{i}"));
                cmd.push(format!("v{i}"));
            }
            let refs: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
            run(&e, &refs);
        }
        let writers: Vec<_> = (0..3)
            .map(|t| {
                let e = e.clone();
                std::thread::spawn(move || {
                    for i in 0..6000 {
                        let k = format!("k{}", (i * 7 + t * 13_331) % 40_000);
                        match i % 4 {
                            0 => run(&e, &["SET", &k, &format!("w{t}-{i}")]),
                            1 => run(&e, &["DEL", &k]),
                            2 => run(&e, &["APPEND", &k, "+"]),
                            _ => run(&e, &["SET", &format!("new{t}-{i}"), "n"]),
                        };
                    }
                })
            })
            .collect();
        for _ in 0..4 {
            let _ = run(&e, &["BGREWRITEAOF"]);
            wait_rewrite(&e);
            let _ = run(&e, &["BGSAVE"]);
        }
        for w in writers {
            w.join().unwrap();
        }
        let _ = run(&e, &["BGREWRITEAOF"]);
        wait_rewrite(&e);
        let live: Vec<Option<String>> = (0..40_000).map(|i| get(&e, &format!("k{i}"))).collect();
        let dbsize = int(run(&e, &["DBSIZE"]));
        stop(&e);

        let e = start(&dir.0, ShardManager::new(4), true);
        assert_eq!(int(run(&e, &["DBSIZE"])), dbsize);
        for (i, v) in live.iter().enumerate() {
            assert_eq!(&get(&e, &format!("k{i}")), v, "k{i}");
        }
    }

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

    #[test]
    fn save_points_parse_and_decide_like_redis() {
        assert_eq!(
            PersistConfig::parse_save("3600 1 300 100").unwrap(),
            vec![(3600, 1), (300, 100)]
        );
        assert_eq!(PersistConfig::parse_save("").unwrap(), vec![]);
        assert!(PersistConfig::parse_save("3600").is_err());
        assert!(PersistConfig::parse_save("x 1").is_err());
        let cfg = PersistConfig {
            save: vec![(3600, 1), (300, 100)],
            ..PersistConfig::default()
        };
        assert!(!cfg.save_due(10, 1_000)); // neither point reached yet
        assert!(cfg.save_due(300, 100));
        assert!(!cfg.save_due(300, 99));
        assert!(cfg.save_due(3600, 1));
        assert!(!cfg.save_due(7200, 0)); // nothing changed
        assert_eq!(cfg.save_string(), "3600 1 300 100");
    }

    #[test]
    fn config_set_changes_persistence_at_runtime() {
        let dir = TmpDir::new("config-set");
        let e = start(&dir.0, ShardManager::new(2), true);
        let get = |e: &CommandEngine, p: &str| match run(e, &["CONFIG", "GET", p]) {
            CommandResponse::Array(v) => match &v[1] {
                CommandResponse::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        };
        assert!(matches!(
            run(&e, &["CONFIG", "SET", "save", "60 5 10 1000"]),
            CommandResponse::Ok
        ));
        assert_eq!(get(&e, "save"), "60 5 10 1000");
        assert_eq!(e.persistence().config().save, vec![(60, 5), (10, 1000)]);
        assert!(matches!(
            run(&e, &["CONFIG", "SET", "appendfsync", "always"]),
            CommandResponse::Ok
        ));
        assert_eq!(get(&e, "appendfsync"), "always");
        assert!(matches!(
            run(&e, &["CONFIG", "SET", "auto-aof-rewrite-min-size", "1mb"]),
            CommandResponse::Ok
        ));
        assert_eq!(e.persistence().config().auto_rewrite_min_size, 1024 * 1024);
        // Writes still reach the log under the new policy.
        run(&e, &["SET", "k", "v"]);
        e.shards.aof().unwrap().sync().unwrap();
        assert!(matches!(
            run(&e, &["CONFIG", "SET", "appendfsync", "sometimes"]),
            CommandResponse::Error(_)
        ));
        assert!(matches!(
            run(&e, &["CONFIG", "SET", "save", "60"]),
            CommandResponse::Error(_)
        ));
        assert_eq!(get(&e, "save"), "60 5 10 1000");
        e.shutdown(Some(false)).unwrap();
        let e = start(&dir.0, ShardManager::new(2), true);
        assert_eq!(get(&e, "save"), "300 1");
        assert_eq!(super::tests::get(&e, "k").as_deref(), Some("v"));
    }
}
