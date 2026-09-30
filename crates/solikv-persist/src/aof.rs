//! The append-only log.
//!
//! # Layout
//!
//! ```text
//! <dir>/appendonlydir/
//!     appendonly.manifest            which files make up the log, in order
//!     appendonly.<gen>.base.rdb      snapshot the log starts from (optional)
//!     appendonly.<gen>.incr.<s>.aof  commands for shard <s>, RESP-encoded
//! ```
//!
//! # One log per shard, so a snapshot and its log always meet exactly
//!
//! Shards run in parallel, each behind its own lock, and a snapshot visits
//! them one at a time. A single log for the whole server cannot be split at
//! "the moment of the snapshot": there is no such moment. A command that ran
//! after shard 0 was saved but before shard 5 was would be in the snapshot for
//! one and not the other, and replaying the log over it applied it twice —
//! harmless for `SET`, wrong for `INCR` and `RPUSH`. That is what the previous
//! single `appendonly.aof`, replayed over the periodic RDB, did on every
//! restart.
//!
//! So each shard has its own log, and every record is appended while that
//! shard's lock is still held, in the order the shard executed it. A rewrite
//! saves a shard and switches that shard's log to the next generation under
//! one lock hold (`AofWriter::rotate`): everything the snapshot contains is in
//! the old generation, everything it lacks in the new one. Recovery replays,
//! per shard, its base section and then its logs, and nothing is applied
//! twice or skipped. Records are *effects on that shard*: a command touching
//! several shards is logged as one record per shard, and a result computed
//! from another shard's data is logged as the value it produced (the engine
//! owns that decision; see `solikv_engine::aof_log`).
//!
//! The manifest makes the switch crash-safe. A rewrite first lists the new
//! generation next to the old one, so a crash mid-rewrite replays base +
//! old logs + new logs; only once the new base is on disk does the manifest
//! drop the old files.
//!
//! # Replay
//!
//! Streaming: records are handed out one at a time, never collected. A tail
//! cut off mid-record (crash while writing) is dropped and reported so the
//! caller can truncate it away before appending again; anything else that
//! does not parse is corruption and stops startup. Records between `MULTI`
//! and `EXEC` markers are applied together or, if the log ends before the
//! `EXEC`, not at all.
//!
//! A pre-manifest `appendonly.aof` (one log for everything) still replays;
//! the server rewrites it into this layout right after loading it.

use bytes::Bytes;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::stats::PersistStats;

const MAX_AOF_BULK_LEN: usize = 512 * 1024 * 1024;

/// Most arguments one AOF command may declare (same bound as the RESP decoder).
const MAX_AOF_ARGS: usize = 1024 * 1024;

/// Directory holding the manifest, base and incremental files.
pub const AOF_DIRNAME: &str = "appendonlydir";

/// The pre-manifest single log, in the data directory itself.
pub const LEGACY_AOF_NAME: &str = "appendonly.aof";

const MANIFEST_NAME: &str = "appendonly.manifest";

// Commands rejected during AOF replay. The on-disk AOF is trusted only as
// far as filesystem permissions go; an attacker who can write to the data
// directory shouldn't be able to make us run scripting, server-control, or
// data-migration commands at startup.
//
// Each entry is a single command name (no subcommand prefixes) — replay only
// inspects args[0], so e.g. "SCRIPT LOAD" is matched as "SCRIPT".
//
// Scripts never need replaying: the commands a script runs are logged one by
// one as it runs them.
const AOF_BLOCKED_COMMANDS: &[&str] = &[
    "EVAL",
    "EVALSHA",
    "SCRIPT",
    "DEBUG",
    "SHUTDOWN",
    "BGREWRITEAOF",
    "BGSAVE",
    "SAVE",
    "SLAVEOF",
    "REPLICAOF",
    "CLUSTER",
    "MIGRATE",
    "RESTORE",
    "MODULE",
];

/// AOF fsync policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    Everysec,
    No,
}

// ---------------------------------------------------------------------------
// Files and manifest
// ---------------------------------------------------------------------------

pub fn base_path(aof_dir: &Path, gen: u64) -> PathBuf {
    aof_dir.join(format!("appendonly.{}.base.rdb", gen))
}

pub fn incr_path(aof_dir: &Path, gen: u64, shard: u32) -> PathBuf {
    aof_dir.join(format!("appendonly.{}.incr.{}.aof", gen, shard))
}

pub fn manifest_path(aof_dir: &Path) -> PathBuf {
    aof_dir.join(MANIFEST_NAME)
}

/// Which files make up the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// Shard count the per-shard logs were written with.
    pub shards: u32,
    /// Key → shard function they were written with (see
    /// `ShardManager::HASH_SCHEME`). A log can only be appended to by a
    /// server that routes keys the same way.
    pub hash: String,
    /// Generation of the base snapshot, if any.
    pub base: Option<u64>,
    /// Generations of incremental logs, oldest first.
    pub incrs: Vec<u64>,
}

impl Manifest {
    pub fn to_text(&self) -> String {
        let mut s = String::from("# SoliKV append-only manifest. Do not edit.\nformat 1\n");
        s.push_str(&format!("shards {}\nhash {}\n", self.shards, self.hash));
        if let Some(b) = self.base {
            s.push_str(&format!("base {}\n", b));
        }
        for g in &self.incrs {
            s.push_str(&format!("incr {}\n", g));
        }
        s
    }

    pub fn parse(text: &str) -> io::Result<Self> {
        let bad = |what: &str| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("AOF manifest: {}", what),
            )
        };
        let mut shards = None;
        let mut hash = None;
        let mut base = None;
        let mut incrs = Vec::new();
        let mut format_ok = false;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (k, v) = line.split_once(' ').ok_or_else(|| bad("malformed line"))?;
            let num = || v.parse::<u64>().map_err(|_| bad("bad number"));
            match k {
                "format" => format_ok = v == "1",
                "shards" => shards = Some(num()? as u32),
                "hash" => hash = Some(v.to_string()),
                "base" => base = Some(num()?),
                "incr" => incrs.push(num()?),
                _ => return Err(bad("unknown key")),
            }
        }
        if !format_ok {
            return Err(bad("unsupported format"));
        }
        let shards = shards
            .filter(|&n| n > 0)
            .ok_or_else(|| bad("no shard count"))?;
        if incrs.is_empty() {
            return Err(bad("no incremental log"));
        }
        Ok(Self {
            shards,
            hash: hash.ok_or_else(|| bad("no hash"))?,
            base,
            incrs,
        })
    }

    pub fn load(aof_dir: &Path) -> io::Result<Option<Self>> {
        match std::fs::read_to_string(manifest_path(aof_dir)) {
            Ok(text) => Self::parse(&text).map(Some),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Replace the manifest atomically.
    pub fn store(&self, aof_dir: &Path) -> io::Result<()> {
        crate::fsutil::write_atomic(&manifest_path(aof_dir), self.to_text())
    }

    /// The generation incremental records currently go to.
    pub fn current_incr(&self) -> u64 {
        *self.incrs.last().expect("manifest has an incremental log")
    }

    /// Every file this manifest names.
    pub fn files(&self, aof_dir: &Path) -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = self
            .base
            .map(|b| base_path(aof_dir, b))
            .into_iter()
            .collect();
        for &g in &self.incrs {
            for s in 0..self.shards {
                v.push(incr_path(aof_dir, g, s));
            }
        }
        v
    }

    /// Bytes on disk across every file named.
    pub fn disk_size(&self, aof_dir: &Path) -> u64 {
        self.files(aof_dir)
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
            .sum()
    }
}

/// Create (empty) the incremental files of generation `gen`, so a manifest
/// naming them never points at a file that does not exist.
pub fn create_incr_files(aof_dir: &Path, gen: u64, shards: u32) -> io::Result<()> {
    crate::fsutil::create_private_dir_all(aof_dir)?;
    for s in 0..shards {
        let f = crate::fsutil::open_private_append(&incr_path(aof_dir, gen, s))?;
        f.sync_all()?;
    }
    crate::fsutil::sync_parent_dir(&manifest_path(aof_dir))
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Append a RESP array to `buf`, without going through `fmt`.
pub fn encode_into(buf: &mut Vec<u8>, name: &[u8], args: &[&[u8]]) {
    buf.reserve(16 + name.len() + args.iter().map(|a| a.len() + 16).sum::<usize>());
    push_header(buf, b'*', 1 + args.len());
    push_bulk(buf, name);
    for arg in args {
        push_bulk(buf, arg);
    }
}

/// Serialize a command as a RESP array.
pub fn encode_command(name: &[u8], args: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_into(&mut buf, name, args);
    buf
}

#[inline]
fn push_header(buf: &mut Vec<u8>, kind: u8, n: usize) {
    let mut digits = [0u8; 20];
    let mut i = digits.len();
    let mut n = n;
    loop {
        i -= 1;
        digits[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    buf.push(kind);
    buf.extend_from_slice(&digits[i..]);
    buf.extend_from_slice(b"\r\n");
}

#[inline]
fn push_bulk(buf: &mut Vec<u8>, data: &[u8]) {
    push_header(buf, b'$', data.len());
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

/// Most bytes a shard may have waiting for the writer before producers wait
/// for it (backpressure: a stalled disk must not turn into unbounded memory).
const MAX_PENDING: usize = 64 * 1024 * 1024;

/// Records a shard produced since the writer last came by.
#[derive(Default)]
struct Pending {
    data: Vec<u8>,
    /// `(offset, generation)`: from `offset` on, records go to `generation`.
    rotations: Vec<(usize, u64)>,
    /// Database the shard's log is in at the end of `data`: replay starts
    /// every file in database 0, and a `SELECT` is written whenever a record
    /// belongs to another one (see [`AofWriter::append_db`]).
    db: u32,
}

struct Inner {
    shards: Vec<parking_lot::Mutex<Pending>>,
    policy: FsyncPolicy,
    /// `sync()` requests waiting for the next fsync.
    syncs: parking_lot::Mutex<Vec<std::sync::mpsc::SyncSender<io::Result<()>>>>,
    thread: std::sync::OnceLock<std::thread::Thread>,
    /// Set when the last handle is dropped.
    closed: std::sync::atomic::AtomicBool,
}

/// Handle to the append-only log writer.
///
/// Producers append records to their shard's in-memory buffer while holding
/// that shard's lock — a memcpy under an uncontended mutex, so the order in
/// the buffer is the order the shard executed them. A dedicated thread swaps
/// the buffers out every millisecond (right away under `appendfsync always`),
/// writes them to the per-shard files and fsyncs by policy.
///
/// This used to be a channel. A send is cheap until the receiver is parked,
/// and then every send is a futex wake: under the shard lock, that syscall
/// serialized a hot key's clients behind it (pipelined LPUSH on one key fell
/// from 1.65 M to 0.66 M ops/s).
///
/// The writer is an OS thread, not a Tokio task: on the solo runtime a task
/// would share the single worker with the producers, and backpressure could
/// only be relieved by handing the worker to another thread.
#[derive(Clone)]
pub struct AofWriter {
    inner: Arc<Inner>,
    /// Keeps `closed` accurate: the writer exits once every handle is gone.
    _handle: Arc<HandleGuard>,
    /// Set for the solo runtime: wait for the writer on this thread instead
    /// of `block_in_place`, which would let another thread run the store.
    single_threaded: bool,
}

struct HandleGuard(Arc<Inner>);

impl Drop for HandleGuard {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
        if let Some(t) = self.0.thread.get() {
            t.unpark();
        }
    }
}

impl AofWriter {
    /// Declare that producers run on the solo (single-worker) runtime.
    pub fn single_threaded(mut self, yes: bool) -> Self {
        self.single_threaded = yes;
        self
    }

    /// Shard count this writer has logs for.
    pub fn shards(&self) -> u32 {
        self.inner.shards.len() as u32
    }

    /// Log a command for `shard`.
    pub fn log(&self, shard: u32, name: &str, args: &[Bytes]) {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_ref()).collect();
        self.append(shard, |buf| encode_into(buf, name.as_bytes(), &args));
    }

    /// Log a record already encoded with [`encode_command`].
    pub fn log_raw(&self, shard: u32, record: &[u8]) {
        self.append(shard, |buf| buf.extend_from_slice(record));
    }

    /// Encode a record straight into `shard`'s buffer. Call with the shard's
    /// lock held.
    pub fn append(&self, shard: u32, write: impl FnOnce(&mut Vec<u8>)) {
        self.append_inner(shard, None, write)
    }

    fn append_inner(&self, shard: u32, db: Option<u32>, write: impl FnOnce(&mut Vec<u8>)) {
        let Some(slot) = self.inner.shards.get(shard as usize) else {
            tracing::error!("AOF record for unknown shard {}", shard);
            return;
        };
        let mut p = slot.lock();
        if p.data.len() >= MAX_PENDING {
            drop(p);
            self.wait_for_room(slot);
            p = slot.lock();
        }
        if let Some(db) = db {
            if p.db != db {
                let n = db.to_string();
                encode_into(&mut p.data, b"SELECT", &[n.as_bytes()]);
                p.db = db;
            }
        }
        write(&mut p.data);
        drop(p);
        if self.inner.policy == FsyncPolicy::Always {
            if let Some(t) = self.inner.thread.get() {
                t.unpark();
            }
        }
    }

    /// [`append`](Self::append) for a record that applies to database `db`:
    /// preceded by `SELECT db` when the shard's log is in another one, as
    /// Redis writes its AOF. Call with the shard's lock held.
    pub fn append_db(&self, shard: u32, db: u32, write: impl FnOnce(&mut Vec<u8>)) {
        self.append_inner(shard, Some(db), write)
    }

    /// Backpressure: the disk is behind; wait for the writer to catch up.
    /// Never drops writes, so durability is preserved under load.
    fn wait_for_room(&self, slot: &parking_lot::Mutex<Pending>) {
        let wait = || {
            while slot.lock().data.len() >= MAX_PENDING {
                if let Some(t) = self.inner.thread.get() {
                    t.unpark();
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        };
        // On the multi-thread runtime, hand the worker's other tasks to
        // another thread while we wait; on the solo runtime, wait right here
        // — the store belongs to this thread alone.
        let multi = !self.single_threaded
            && matches!(
                tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()),
                Ok(tokio::runtime::RuntimeFlavor::MultiThread)
            );
        if multi {
            tokio::task::block_in_place(wait)
        } else {
            wait()
        }
    }

    /// Switch `shard`'s log to generation `gen`. Call while holding the
    /// shard's lock, right after saving it.
    pub fn rotate(&self, shard: u32, gen: u64) {
        if let Some(slot) = self.inner.shards.get(shard as usize) {
            let mut p = slot.lock();
            let at = p.data.len();
            p.rotations.push((at, gen));
            // The new file replays from database 0.
            p.db = 0;
        }
    }

    /// Wait until everything logged so far is written and fsynced.
    pub fn sync(&self) -> io::Result<()> {
        let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
        self.inner.syncs.lock().push(ack_tx);
        if let Some(t) = self.inner.thread.get() {
            t.unpark();
        }
        ack_rx
            .recv()
            .map_err(|_| io::Error::other("AOF writer is gone"))?
    }
}

struct ShardLog {
    w: BufWriter<std::fs::File>,
    dirty: bool,
}

impl ShardLog {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            w: BufWriter::with_capacity(64 * 1024, crate::fsutil::open_private_append(path)?),
            dirty: false,
        })
    }

    fn flush(&mut self, fsync: bool) -> io::Result<()> {
        self.w.flush()?;
        if fsync && self.dirty {
            self.w.get_ref().sync_data()?;
            self.dirty = false;
        }
        Ok(())
    }
}

/// Spawn the writer thread, appending to generation `gen`'s per-shard files
/// in `aof_dir`. When every handle is dropped it flushes, fsyncs and exits.
pub fn spawn_aof_writer(
    aof_dir: &Path,
    shards: u32,
    gen: u64,
    policy: FsyncPolicy,
    stats: Arc<PersistStats>,
) -> io::Result<AofWriter> {
    crate::fsutil::create_private_dir_all(aof_dir)?;
    let mut logs = Vec::with_capacity(shards as usize);
    for s in 0..shards {
        logs.push(ShardLog::open(&incr_path(aof_dir, gen, s))?);
    }
    let dir = aof_dir.to_path_buf();
    let inner = Arc::new(Inner {
        // Appending may continue a file that ended in any database: the first
        // record of each shard always states its own.
        shards: (0..shards)
            .map(|_| {
                parking_lot::Mutex::new(Pending {
                    db: u32::MAX,
                    ..Pending::default()
                })
            })
            .collect(),
        policy,
        syncs: parking_lot::Mutex::new(Vec::new()),
        thread: std::sync::OnceLock::new(),
        closed: std::sync::atomic::AtomicBool::new(false),
    });

    let thread_inner = inner.clone();
    let handle = std::thread::Builder::new()
        .name("solikv-aof".into())
        .spawn(move || writer_loop(thread_inner, logs, dir, stats))?;
    let _ = inner.thread.set(handle.thread().clone());

    Ok(AofWriter {
        _handle: Arc::new(HandleGuard(inner.clone())),
        inner,
        single_threaded: false,
    })
}

fn writer_loop(inner: Arc<Inner>, mut logs: Vec<ShardLog>, dir: PathBuf, stats: Arc<PersistStats>) {
    use std::time::{Duration, Instant};

    let mut last_sync = Instant::now();
    let mut last_error_log: Option<Instant> = None;
    let mut report = |res: io::Result<()>, what: &str| {
        stats
            .aof_last_write_ok
            .store(res.is_ok(), Ordering::Relaxed);
        if let Err(e) = res {
            // Once a second at most: a full disk fails every write.
            if last_error_log.is_none_or(|t| t.elapsed() >= Duration::from_secs(1)) {
                tracing::error!("AOF {} failed: {}", what, e);
                last_error_log = Some(Instant::now());
            }
        }
    };
    let flush_all = |logs: &mut Vec<ShardLog>, fsync: bool| -> io::Result<()> {
        let mut result = Ok(());
        for log in logs.iter_mut() {
            if let Err(e) = log.flush(fsync) {
                result = Err(e);
            }
        }
        result
    };
    // Swapped in for each shard's buffer, so capacity is reused both ways.
    let mut spare = Pending::default();

    loop {
        let closing = inner.closed.load(Ordering::Acquire);
        // Sync requests seen now are answered after this pass is on disk.
        let syncs = std::mem::take(&mut *inner.syncs.lock());

        let mut wrote = false;
        for (s, slot) in inner.shards.iter().enumerate() {
            {
                let mut p = slot.lock();
                if p.data.is_empty() && p.rotations.is_empty() {
                    continue;
                }
                // The buffers only: `db` describes the log, not the batch.
                std::mem::swap(&mut p.data, &mut spare.data);
                std::mem::swap(&mut p.rotations, &mut spare.rotations);
            }
            let log = &mut logs[s];
            let mut from = 0;
            for &(at, g) in &spare.rotations {
                if at > from {
                    report(log.w.write_all(&spare.data[from..at]), "write");
                    log.dirty = true;
                }
                from = at;
                report(log.flush(true), "fsync");
                match ShardLog::open(&incr_path(&dir, g, s as u32)) {
                    Ok(new) => *log = new,
                    Err(e) => report(Err(e), "rotate"),
                }
            }
            if spare.data.len() > from {
                report(log.w.write_all(&spare.data[from..]), "write");
                log.dirty = true;
            }
            stats
                .aof_current_size
                .fetch_add(spare.data.len() as u64, Ordering::Relaxed);
            spare.data.clear();
            spare.rotations.clear();
            wrote = true;
        }

        let fsync_now = !syncs.is_empty()
            || closing
            || (wrote && inner.policy == FsyncPolicy::Always)
            || (last_sync.elapsed() >= Duration::from_secs(1)
                && inner.policy == FsyncPolicy::Everysec);
        if fsync_now || last_sync.elapsed() >= Duration::from_secs(1) {
            let res = flush_all(&mut logs, fsync_now);
            for ack in syncs {
                let _ = ack.send(match &res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
                });
            }
            report(res, "fsync");
            last_sync = Instant::now();
        }
        if closing {
            break;
        }
        match inner.policy {
            // Woken by each append; the timeout keeps the once-a-second flush.
            FsyncPolicy::Always => std::thread::park_timeout(Duration::from_millis(100)),
            // Poll: an append never has to wake this thread (see `AofWriter`).
            _ => std::thread::park_timeout(Duration::from_millis(1)),
        }
    }
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

/// What replaying one log found.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplayOutcome {
    /// Records handed to the caller.
    pub commands: u64,
    /// When the log ended part-way through a record or a transaction: the
    /// length of the complete prefix. Truncate the file to this before
    /// appending to it again, or the next record lands after garbage.
    pub truncated_at: Option<u64>,
}

/// Replay a RESP command log, handing each record to `apply` in order.
///
/// A file that ends part-way through a command — the crash-while-writing
/// case — loads everything before it and logs a warning, like Redis'
/// default `aof-load-truncated yes`; so does one that ends inside a
/// `MULTI` … `EXEC` block, whose records are then not applied. Anything else
/// that does not parse is corruption and is an `InvalidData` error: replaying
/// past it would resynchronise on arbitrary bytes and load commands nobody
/// wrote.
///
/// Nothing is allocated from a length in the file before the bytes are
/// actually there, so a forged header cannot make startup allocate
/// gigabytes.
pub fn replay_log(path: &Path, mut apply: impl FnMut(Vec<Bytes>)) -> io::Result<ReplayOutcome> {
    let file = std::fs::File::open(path)?;
    let mut reader = CountingReader {
        inner: BufReader::with_capacity(1 << 20, file),
        pos: 0,
    };
    let mut outcome = ReplayOutcome::default();
    let mut entry = 0usize;
    // Records of an open MULTI block, and where the block started.
    let mut tx: Option<(u64, Vec<Vec<Bytes>>)> = None;

    loop {
        let start = reader.pos;
        let args = match read_record(&mut reader, entry)? {
            Record::Complete(args) => args,
            Record::Eof => break,
            Record::Truncated => {
                tracing::warn!(
                    "AOF {:?} ends part-way through command #{}; loading the complete \
                     commands before it",
                    path,
                    entry + 1,
                );
                outcome.truncated_at = Some(start);
                break;
            }
        };
        entry += 1;

        let name = String::from_utf8_lossy(&args[0]).to_uppercase();
        match name.as_str() {
            "MULTI" if args.len() == 1 => {
                if tx.is_some() {
                    return Err(corrupt(entry, "MULTI inside MULTI"));
                }
                tx = Some((start, Vec::new()));
                continue;
            }
            "EXEC" if args.len() == 1 => {
                let (_, records) = tx
                    .take()
                    .ok_or_else(|| corrupt(entry, "EXEC without MULTI"))?;
                for r in records {
                    outcome.commands += 1;
                    apply(r);
                }
                continue;
            }
            _ => {}
        }
        if AOF_BLOCKED_COMMANDS.contains(&name.as_str()) {
            tracing::warn!("Skipping blocked command in AOF replay: {}", name);
            continue;
        }
        match &mut tx {
            Some((_, records)) => records.push(args),
            None => {
                outcome.commands += 1;
                apply(args);
            }
        }
    }

    if let Some((start, records)) = tx {
        tracing::warn!(
            "AOF {:?} ends inside a transaction; its {} commands are not applied",
            path,
            records.len()
        );
        outcome.truncated_at = Some(outcome.truncated_at.map_or(start, |t| t.min(start)));
    }
    Ok(outcome)
}

enum Record {
    Complete(Vec<Bytes>),
    Truncated,
    Eof,
}

fn corrupt(entry: usize, what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("AOF corrupt at command #{}: {}", entry + 1, what),
    )
}

fn read_record<R: BufRead>(reader: &mut R, entry: usize) -> io::Result<Record> {
    let line = match read_line_crlf(reader)? {
        Line::Complete(l) => l,
        Line::Eof => return Ok(Record::Eof),
        Line::Truncated => return Ok(Record::Truncated),
    };
    if !line.starts_with(b"*") {
        return Err(corrupt(entry, "expected '*'"));
    }
    let count: usize = std::str::from_utf8(&line[1..])
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&c| c > 0 && c <= MAX_AOF_ARGS)
        .ok_or_else(|| corrupt(entry, "bad argument count"))?;

    let mut args = Vec::with_capacity(count.min(64));
    for _ in 0..count {
        let len_line = match read_line_crlf(reader)? {
            Line::Complete(l) => l,
            Line::Eof | Line::Truncated => return Ok(Record::Truncated),
        };
        if !len_line.starts_with(b"$") {
            return Err(corrupt(entry, "expected '$'"));
        }
        let len: usize = std::str::from_utf8(&len_line[1..])
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&l| l <= MAX_AOF_BULK_LEN)
            .ok_or_else(|| corrupt(entry, "bad or over-large bulk length"))?;

        // `take` + `read_to_end` grows with the bytes that really exist,
        // instead of `vec![0; len]` up front.
        let mut buf = Vec::with_capacity(len.min(1 << 16));
        (&mut *reader).take(len as u64).read_to_end(&mut buf)?;
        let mut crlf = [0u8; 2];
        if buf.len() < len || reader.read_exact(&mut crlf).is_err() {
            return Ok(Record::Truncated);
        }
        if &crlf != b"\r\n" {
            return Err(corrupt(entry, "bulk not terminated by CRLF"));
        }
        args.push(Bytes::from(buf));
    }
    Ok(Record::Complete(args))
}

enum Line {
    Complete(Vec<u8>),
    Truncated,
    Eof,
}

/// Read a line terminated by \r\n, returning content without the terminator.
///
/// Only `*<n>` / `$<n>` headers are read this way, so a line longer than a few
/// dozen bytes is corruption — refused rather than buffered without limit.
fn read_line_crlf<R: BufRead>(reader: &mut R) -> io::Result<Line> {
    const MAX_HEADER_LINE: usize = 64;
    let mut buf = Vec::new();
    let n = (&mut *reader)
        .take(MAX_HEADER_LINE as u64 + 2)
        .read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Ok(Line::Eof);
    }
    if buf.last() != Some(&b'\n') {
        if buf.len() > MAX_HEADER_LINE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "AOF header line too long",
            ));
        }
        return Ok(Line::Truncated);
    }
    buf.pop();
    if buf.pop() != Some(b'\r') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "AOF header line not terminated by CRLF",
        ));
    }
    Ok(Line::Complete(buf))
}

struct CountingReader<R> {
    inner: R,
    pos: u64,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl<R: BufRead> BufRead for CountingReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        self.pos += amt as u64;
        self.inner.consume(amt)
    }
}

/// Cut a log back to its complete prefix (see [`ReplayOutcome::truncated_at`]).
pub fn truncate_log(path: &Path, len: u64) -> io::Result<()> {
    let f = std::fs::OpenOptions::new().write(true).open(path)?;
    f.set_len(len)?;
    f.sync_all()
}

// ---------------------------------------------------------------------------
// Synchronous appender (tests, tools)
// ---------------------------------------------------------------------------

/// Synchronous AOF writing and whole-file replay, for tests and tools.
pub struct AofPersistence {
    writer: std::fs::File,
    policy: FsyncPolicy,
}

impl AofPersistence {
    pub fn new(path: &Path, policy: FsyncPolicy) -> io::Result<Self> {
        Ok(Self {
            writer: crate::fsutil::open_private_append(path)?,
            policy,
        })
    }

    /// Append a command to the AOF.
    pub fn append(&mut self, args: &[Bytes]) -> io::Result<()> {
        let (name, rest) = args
            .split_first()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty command"))?;
        let rest: Vec<&[u8]> = rest.iter().map(|a| a.as_ref()).collect();
        self.writer.write_all(&encode_command(name, &rest))?;
        if self.policy == FsyncPolicy::Always {
            self.writer.sync_data()?;
        }
        Ok(())
    }

    /// Replay a log into memory. Tests only — the server streams with
    /// [`replay_log`].
    pub fn replay(path: &Path) -> io::Result<Vec<Vec<Bytes>>> {
        let mut commands = Vec::new();
        replay_log(path, |c| commands.push(c))?;
        Ok(commands)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn test_aof_append_format() {
        let buf = encode_command(b"SET", &[b"key", b"value"]);
        let expected = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        assert_eq!(std::str::from_utf8(&buf).unwrap(), expected);
    }

    #[test]
    fn test_aof_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");
        {
            let mut aof = AofPersistence::new(&path, FsyncPolicy::Always).unwrap();
            aof.append(&[b("SET"), b("key1"), b("val1")]).unwrap();
            aof.append(&[b("SET"), b("key2"), b("val2")]).unwrap();
        }
        let commands = AofPersistence::replay(&path).unwrap();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0][0], b("SET"));
        assert_eq!(commands[0][1], b("key1"));
    }

    #[test]
    fn test_aof_replay_binary_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_binary.aof");
        {
            let mut aof = AofPersistence::new(&path, FsyncPolicy::Always).unwrap();
            let binary_val = Bytes::from(b"hello\r\nworld".as_slice());
            aof.append(&[b("SET"), b("key"), binary_val]).unwrap();
        }
        let commands = AofPersistence::replay(&path).unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0][2], Bytes::from(b"hello\r\nworld".as_slice()));
    }

    // SEC-007: blocked commands must be skipped during replay.
    #[test]
    fn test_aof_replay_skips_blocked_commands() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blocked.aof");
        {
            let mut aof = AofPersistence::new(&path, FsyncPolicy::Always).unwrap();
            aof.append(&[b("SET"), b("ok"), b("1")]).unwrap();
            aof.append(&[b("EVAL"), b("return 1"), b("0")]).unwrap();
            aof.append(&[b("FLUSHALL")]).unwrap();
            aof.append(&[b("SHUTDOWN")]).unwrap();
            aof.append(&[b("CLUSTER"), b("RESET")]).unwrap();
            aof.append(&[b("SET"), b("ok2"), b("2")]).unwrap();
        }
        let commands = AofPersistence::replay(&path).unwrap();
        let names: Vec<&[u8]> = commands.iter().map(|c| c[0].as_ref()).collect();
        assert!(!names.contains(&b"EVAL".as_ref()));
        assert!(!names.contains(&b"SHUTDOWN".as_ref()));
        assert!(!names.contains(&b"CLUSTER".as_ref()));
        assert!(names.contains(&b"SET".as_ref()));
        assert!(names.contains(&b"FLUSHALL".as_ref()));
    }

    // SEC-006: a forged AOF file declaring an over-large bulk length must
    // abort replay rather than allocating gigabytes.
    #[test]
    fn test_aof_replay_rejects_oversized_bulk_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized.aof");
        let huge = MAX_AOF_BULK_LEN + 1;
        std::fs::write(&path, format!("*2\r\n$3\r\nSET\r\n${}\r\n", huge)).unwrap();
        // An over-large declared length is corruption, not a truncated tail.
        let err = AofPersistence::replay(&path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_aof_replay_huge_count_does_not_preallocate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("count.aof");
        std::fs::write(&path, "*18446744073709551615\r\n").unwrap();
        assert!(AofPersistence::replay(&path).is_err());

        // Within bounds, but the file ends: a truncated tail, loaded as empty.
        std::fs::write(&path, "*1000000\r\n$3\r\nSET\r\n").unwrap();
        assert!(AofPersistence::replay(&path).unwrap().is_empty());
    }

    #[test]
    fn test_aof_replay_truncated_tail_keeps_complete_commands() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tail.aof");
        let good = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";
        // A declared 100-byte bulk with 3 bytes present: crash mid-write.
        std::fs::write(
            &path,
            format!("{good}*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$100\r\nabc"),
        )
        .unwrap();
        let mut seen = Vec::new();
        let outcome = replay_log(&path, |c| seen.push(c)).unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0][1], b("a"));
        assert_eq!(outcome.truncated_at, Some(good.len() as u64));

        // Truncating to the reported length leaves a clean log.
        truncate_log(&path, outcome.truncated_at.unwrap()).unwrap();
        assert_eq!(
            replay_log(&path, |_| {}).unwrap(),
            ReplayOutcome {
                commands: 1,
                truncated_at: None
            }
        );
    }

    #[test]
    fn test_aof_replay_garbage_in_the_middle_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mid.aof");
        std::fs::write(
            &path,
            "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\ngarbage\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n",
        )
        .unwrap();
        assert!(AofPersistence::replay(&path).is_err());
    }

    #[test]
    fn an_unfinished_transaction_is_not_applied() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tx.aof");
        let mut body = Vec::new();
        body.extend(encode_command(b"MULTI", &[]));
        body.extend(encode_command(b"SET", &[b"a", b"1"]));
        body.extend(encode_command(b"EXEC", &[]));
        let complete = body.len() as u64;
        body.extend(encode_command(b"MULTI", &[]));
        body.extend(encode_command(b"SET", &[b"b", b"2"]));
        std::fs::write(&path, &body).unwrap();

        let mut seen = Vec::new();
        let outcome = replay_log(&path, |c| seen.push(c)).unwrap();
        assert_eq!(seen, vec![vec![b("SET"), b("a"), b("1")]]);
        assert_eq!(outcome.truncated_at, Some(complete));
    }

    #[test]
    fn manifest_round_trips_and_rejects_nonsense() {
        let m = Manifest {
            shards: 4,
            hash: "fnv1a".into(),
            base: Some(3),
            incrs: vec![3, 4],
        };
        assert_eq!(Manifest::parse(&m.to_text()).unwrap(), m);
        assert!(Manifest::parse("format 1\nshards 0\nhash x\nincr 1\n").is_err());
        assert!(Manifest::parse("format 2\nshards 1\nhash x\nincr 1\n").is_err());
        assert!(Manifest::parse("format 1\nshards 1\nhash x\n").is_err());
    }

    #[test]
    fn the_writer_rotates_each_shard_independently() {
        let dir = tempfile::tempdir().unwrap();
        let aof = dir.path().join(AOF_DIRNAME);
        create_incr_files(&aof, 1, 2).unwrap();
        let stats = Arc::new(PersistStats::new());
        let w = spawn_aof_writer(&aof, 2, 1, FsyncPolicy::Everysec, stats).unwrap();
        w.log(0, "SET", &[b("a"), b("1")]);
        w.log(1, "SET", &[b("b"), b("1")]);
        w.rotate(0, 2);
        w.log(0, "SET", &[b("a"), b("2")]);
        w.log(1, "SET", &[b("b"), b("2")]);
        w.sync().unwrap();

        let read = |g, s| {
            let mut v = Vec::new();
            replay_log(&incr_path(&aof, g, s), |c| v.push(c[2].clone())).unwrap();
            v
        };
        assert_eq!(read(1, 0), vec![b("1")]);
        assert_eq!(read(2, 0), vec![b("2")]);
        assert_eq!(read(1, 1), vec![b("1"), b("2")]);
    }
}
