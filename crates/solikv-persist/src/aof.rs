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

impl FsyncPolicy {
    fn to_u8(self) -> u8 {
        match self {
            FsyncPolicy::Always => 0,
            FsyncPolicy::Everysec => 1,
            FsyncPolicy::No => 2,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => FsyncPolicy::Always,
            1 => FsyncPolicy::Everysec,
            _ => FsyncPolicy::No,
        }
    }
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

/// Most bytes one thread may have waiting for the writer before it waits
/// for it (backpressure: a stalled disk must not turn into unbounded memory).
const MAX_PENDING: usize = 64 * 1024 * 1024;

/// What a sequenced entry in a thread's buffer is.
const KIND_RECORD: u8 = 0;
const KIND_ROTATE: u8 = 1;

/// `db` of a record that states no database (transaction markers).
const NO_DB: u32 = u32::MAX;

/// Entry header in a thread's buffer: shard u32, seq u64, db u32, kind u8,
/// payload length u32 — then the payload (an encoded record, or the new
/// generation for a rotation).
const ENTRY_HEADER: usize = 4 + 8 + 4 + 1 + 4;

/// One producer thread's entries since the writer last came by. Only that
/// thread appends and only the writer drains, so its lock is uncontended and
/// its memory stays in the producing core's cache — unlike a buffer per
/// shard, which every core serving that shard would write in turn.
#[derive(Default)]
struct ThreadBuf {
    data: parking_lot::Mutex<Vec<u8>>,
}

struct Inner {
    shards: u32,
    /// Threads registered by their first append since the writer last
    /// looked; it moves them into its own list.
    threads: parking_lot::Mutex<Vec<Arc<ThreadBuf>>>,
    /// Per shard, the sequence number its first record will carry (see
    /// [`AofWriter::start_shard`]).
    starts: Vec<std::sync::atomic::AtomicU64>,
    /// Per shard, whether its changes are logged (`CONFIG SET appendonly`
    /// turns shards on one at a time, each at its rewrite's cut; see
    /// [`AofWriter::shard_on`]).
    on: Vec<std::sync::atomic::AtomicBool>,
    /// `appendfsync`, changeable at runtime (`CONFIG SET`).
    policy: std::sync::atomic::AtomicU8,
    /// `sync()` requests waiting for the next fsync.
    syncs: parking_lot::Mutex<Vec<std::sync::mpsc::SyncSender<io::Result<()>>>>,
    thread: std::sync::OnceLock<std::thread::Thread>,
    /// Set when the last handle is dropped.
    closed: std::sync::atomic::AtomicBool,
}

impl Inner {
    #[inline]
    fn policy(&self) -> FsyncPolicy {
        FsyncPolicy::from_u8(self.policy.load(Ordering::Relaxed))
    }
}

thread_local! {
    /// This thread's buffer in each live writer (tests run several writers
    /// in one process; the server has one).
    static LOCAL: std::cell::RefCell<Vec<(std::sync::Weak<Inner>, Arc<ThreadBuf>)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Handle to the append-only log writer.
///
/// # Ordering without a shared buffer
///
/// A shard's records must reach its log in the order the shard executed
/// them, so they are produced while its lock is held. They used to be
/// appended there to one buffer per shard: under a hot key, every core
/// serving it then wrote that buffer in turn, and each record cost the lock
/// holder a second mutex and the buffer's cache lines fetched from the
/// previous holder's core — inside the critical section everyone queues on
/// (pipelined single-key writes ran ~35% slower with the log on than off).
///
/// Now the lock holder only takes the next number from the shard's own
/// counter (`seq`, kept in the store it already has in cache) and copies the
/// record into *its thread's* buffer. A dedicated thread drains every
/// thread's buffer each millisecond (right away under `appendfsync always`),
/// puts each shard's entries back in sequence order and writes them to the
/// per-shard files, fsyncing by policy. An entry whose predecessor sits in a
/// buffer drained later in the same pass waits for the next pass; since a
/// number is only taken with the record appended in the same lock hold,
/// everything appended before a pass begins is complete by its end, which is
/// what [`sync`](Self::sync) relies on.
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

    /// Change `appendfsync`; the writer applies it from its next pass.
    pub fn set_policy(&self, policy: FsyncPolicy) {
        self.inner.policy.store(policy.to_u8(), Ordering::Relaxed);
        if let Some(t) = self.inner.thread.get() {
            t.unpark();
        }
    }

    /// Shard count this writer has logs for.
    pub fn shards(&self) -> u32 {
        self.inner.shards
    }

    /// Declare the sequence number `shard`'s first record will carry: the
    /// value of its counter when the writer is attached. Call before any
    /// record for that shard is appended.
    pub fn start_shard(&self, shard: u32, seq: u64) {
        if let Some(s) = self.inner.starts.get(shard as usize) {
            s.store(seq, Ordering::Release);
        }
    }

    /// Whether `shard`'s changes are logged. Read under the shard's lock by
    /// every write, and changed only under it ([`set_shard_on`]), so a shard
    /// turns on or off between two of its commands, never inside one.
    ///
    /// [`set_shard_on`]: Self::set_shard_on
    #[inline]
    pub fn shard_on(&self, shard: u32) -> bool {
        self.inner
            .on
            .get(shard as usize)
            .is_some_and(|o| o.load(Ordering::Relaxed))
    }

    /// See [`shard_on`](Self::shard_on). Call with the shard's lock held.
    pub fn set_shard_on(&self, shard: u32, on: bool) {
        if let Some(o) = self.inner.on.get(shard as usize) {
            o.store(on, Ordering::Relaxed);
        }
    }

    /// Whether any shard is logged: the log is on.
    pub fn any_on(&self) -> bool {
        self.inner.on.iter().any(|o| o.load(Ordering::Relaxed))
    }

    /// Log a command for `shard`. `seq` is the shard's counter; call with the
    /// shard's lock held, as for every append.
    pub fn log(&self, shard: u32, seq: &mut u64, name: &str, args: &[Bytes]) {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_ref()).collect();
        self.append(shard, seq, |buf| encode_into(buf, name.as_bytes(), &args));
    }

    /// Log a record already encoded with [`encode_command`].
    pub fn log_raw(&self, shard: u32, seq: &mut u64, record: &[u8]) {
        self.append(shard, seq, |buf| buf.extend_from_slice(record));
    }

    /// Encode a record for `shard`, numbered from its counter `seq`. Call
    /// with the shard's lock held: the lock is what makes the numbers follow
    /// execution order.
    pub fn append(&self, shard: u32, seq: &mut u64, write: impl FnOnce(&mut Vec<u8>)) {
        self.push(shard, seq, NO_DB, KIND_RECORD, write)
    }

    /// [`append`](Self::append) for a record that applies to database `db`:
    /// the writer precedes it with `SELECT db` when the shard's log is in
    /// another one, as Redis writes its AOF.
    pub fn append_db(&self, shard: u32, seq: &mut u64, db: u32, write: impl FnOnce(&mut Vec<u8>)) {
        self.push(shard, seq, db, KIND_RECORD, write)
    }

    /// Switch `shard`'s log to generation `gen`. Call while holding the
    /// shard's lock, right after saving it.
    pub fn rotate(&self, shard: u32, seq: &mut u64, gen: u64) {
        self.push(shard, seq, NO_DB, KIND_ROTATE, |buf| {
            buf.extend_from_slice(&gen.to_le_bytes())
        })
    }

    #[inline]
    fn push(&self, shard: u32, seq: &mut u64, db: u32, kind: u8, write: impl FnOnce(&mut Vec<u8>)) {
        if shard >= self.inner.shards {
            tracing::error!("AOF record for unknown shard {}", shard);
            return;
        }
        self.with_local_buf(|tb| {
            let mut data = tb.data.lock();
            if data.len() >= MAX_PENDING {
                drop(data);
                self.wait_for_room(tb);
                data = tb.data.lock();
            }
            let n = *seq;
            *seq += 1;
            let at = data.len();
            let mut header = [0u8; ENTRY_HEADER];
            header[0..4].copy_from_slice(&shard.to_le_bytes());
            header[4..12].copy_from_slice(&n.to_le_bytes());
            header[12..16].copy_from_slice(&db.to_le_bytes());
            header[16] = kind;
            data.extend_from_slice(&header);
            write(&mut data);
            let len = (data.len() - at - ENTRY_HEADER) as u32;
            data[at + ENTRY_HEADER - 4..at + ENTRY_HEADER].copy_from_slice(&len.to_le_bytes());
        });
        if self.inner.policy() == FsyncPolicy::Always {
            if let Some(t) = self.inner.thread.get() {
                t.unpark();
            }
        }
    }

    /// Run `f` on this thread's buffer in this writer, registered on first
    /// use. Borrowed, not cloned: an `Arc` clone per record would be two
    /// atomic operations under the shard lock for nothing.
    #[inline]
    fn with_local_buf<R>(&self, f: impl FnOnce(&ThreadBuf) -> R) -> R {
        LOCAL.with(|l| {
            let me = Arc::as_ptr(&self.inner);
            {
                let l = l.borrow();
                if let Some((_, tb)) = l.iter().find(|(w, _)| std::ptr::eq(w.as_ptr(), me)) {
                    return f(tb);
                }
            }
            let tb = Arc::new(ThreadBuf::default());
            self.inner.threads.lock().push(tb.clone());
            let mut lm = l.borrow_mut();
            lm.retain(|(w, _)| w.strong_count() > 0);
            lm.push((Arc::downgrade(&self.inner), tb.clone()));
            drop(lm);
            f(&tb)
        })
    }

    /// Backpressure: the disk is behind; wait for the writer to catch up.
    /// Never drops writes, so durability is preserved under load.
    fn wait_for_room(&self, tb: &ThreadBuf) {
        let wait = || {
            while tb.data.lock().len() >= MAX_PENDING {
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
/// Each shard's numbering starts at 0 unless [`AofWriter::start_shard`] says
/// otherwise.
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
        shards,
        threads: parking_lot::Mutex::new(Vec::new()),
        starts: (0..shards)
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect(),
        on: (0..shards)
            .map(|_| std::sync::atomic::AtomicBool::new(true))
            .collect(),
        policy: std::sync::atomic::AtomicU8::new(policy.to_u8()),
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

/// One sequenced entry, as the writer orders them.
struct Entry {
    seq: u64,
    db: u32,
    kind: u8,
    /// Where the payload is: in this pass's drained buffer `buf` at
    /// `off..off+len`, or (`buf == usize::MAX`) in `held[off]`.
    buf: usize,
    off: usize,
    len: usize,
}

/// A shard's position in its sequence, and what it is waiting on.
struct ShardOrder {
    /// The number the next entry written must carry; `None` until the
    /// first entry, when it is read from `starts`.
    next: Option<u64>,
    /// Database the log is in (`NO_DB` until the first record states one:
    /// appending may continue a file that ended in any database).
    db: u32,
    /// Entries that arrived ahead of a predecessor, with their payloads.
    held: Vec<(u64, u32, u8, Vec<u8>)>,
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
    let mut order: Vec<ShardOrder> = (0..inner.shards)
        .map(|_| ShardOrder {
            next: None,
            db: NO_DB,
            held: Vec::new(),
        })
        .collect();
    // Each thread's second buffer: swapped with the one it fills, drained,
    // and swapped back to the same thread — a buffer never moves to another
    // core, whose cache would then pay for every line it writes.
    let mut threads: Vec<(Arc<ThreadBuf>, Vec<u8>)> = Vec::new();
    // Indexes into `threads` of the buffers this pass drained.
    let mut drained: Vec<usize> = Vec::new();
    let mut per_shard: Vec<Vec<Entry>> = (0..inner.shards).map(|_| Vec::new()).collect();
    let mut select = Vec::new();

    loop {
        let closing = inner.closed.load(Ordering::Acquire);
        // Sync requests seen now are answered after this pass is on disk.
        let syncs = std::mem::take(&mut *inner.syncs.lock());

        // Drain every thread's buffer; forget threads that have exited.
        threads.extend(
            std::mem::take(&mut *inner.threads.lock())
                .into_iter()
                .map(|tb| (tb, Vec::new())),
        );
        let mut t = 0;
        while t < threads.len() {
            let (tb, back) = &mut threads[t];
            // Read before draining: once only this list holds the buffer, its
            // thread has exited and appends nothing more, so this drain is
            // its last. (The fence pairs with the release in the thread's
            // `Arc` drop, after its last append.)
            let alive = Arc::strong_count(tb) > 1;
            std::sync::atomic::fence(Ordering::Acquire);
            {
                let mut data = tb.data.lock();
                if !data.is_empty() {
                    std::mem::swap(&mut *data, back);
                }
            }
            if !back.is_empty() {
                drained.push(t);
            }
            if !alive && back.is_empty() {
                threads.swap_remove(t);
                continue;
            }
            t += 1;
        }
        for &t in &drained {
            let buf = &threads[t].1;
            let mut at = 0;
            while at + ENTRY_HEADER <= buf.len() {
                let shard = u32::from_le_bytes(buf[at..at + 4].try_into().unwrap()) as usize;
                let seq = u64::from_le_bytes(buf[at + 4..at + 12].try_into().unwrap());
                let db = u32::from_le_bytes(buf[at + 12..at + 16].try_into().unwrap());
                let kind = buf[at + 16];
                let len = u32::from_le_bytes(buf[at + 17..at + 21].try_into().unwrap()) as usize;
                per_shard[shard].push(Entry {
                    seq,
                    db,
                    kind,
                    buf: t,
                    off: at + ENTRY_HEADER,
                    len,
                });
                at += ENTRY_HEADER + len;
            }
        }

        let mut wrote = false;
        for (s, entries) in per_shard.iter_mut().enumerate() {
            let ord = &mut order[s];
            if entries.is_empty() && ord.held.is_empty() {
                continue;
            }
            for (i, (seq, db, kind, _)) in ord.held.iter().enumerate() {
                entries.push(Entry {
                    seq: *seq,
                    db: *db,
                    kind: *kind,
                    buf: usize::MAX,
                    off: i,
                    len: 0,
                });
            }
            // Each thread's entries are already in order: mostly a merge.
            entries.sort_unstable_by_key(|e| e.seq);
            let mut next = ord
                .next
                .unwrap_or_else(|| inner.starts[s].load(Ordering::Acquire));
            let log = &mut logs[s];
            let mut written = 0u64;
            let mut stop = entries.len();
            for (i, e) in entries.iter().enumerate() {
                if e.seq != next {
                    // A predecessor is in a buffer this pass drained before
                    // it was appended: wait for the next pass.
                    stop = i;
                    break;
                }
                next += 1;
                let payload: &[u8] = if e.buf == usize::MAX {
                    &ord.held[e.off].3
                } else {
                    &threads[e.buf].1[e.off..e.off + e.len]
                };
                match e.kind {
                    KIND_ROTATE => {
                        let gen = u64::from_le_bytes(payload[..8].try_into().unwrap());
                        report(log.flush(true), "fsync");
                        match ShardLog::open(&incr_path(&dir, gen, s as u32)) {
                            Ok(new) => *log = new,
                            Err(err) => report(Err(err), "rotate"),
                        }
                        // The new file replays from database 0.
                        ord.db = 0;
                    }
                    _ => {
                        if e.db != NO_DB && e.db != ord.db {
                            select.clear();
                            let n = e.db.to_string();
                            encode_into(&mut select, b"SELECT", &[n.as_bytes()]);
                            report(log.w.write_all(&select), "write");
                            written += select.len() as u64;
                            ord.db = e.db;
                        }
                        report(log.w.write_all(payload), "write");
                        written += payload.len() as u64;
                        log.dirty = true;
                    }
                }
            }
            ord.next = Some(next);
            let mut held = Vec::new();
            for e in &entries[stop..] {
                let payload = if e.buf == usize::MAX {
                    std::mem::take(&mut ord.held[e.off].3)
                } else {
                    threads[e.buf].1[e.off..e.off + e.len].to_vec()
                };
                held.push((e.seq, e.db, e.kind, payload));
            }
            if closing && !held.is_empty() {
                // Every producer is gone, so no predecessor can still come:
                // a gap here is a bug. Keep the records rather than lose them.
                tracing::error!(
                    "AOF shard {}: {} record(s) after a missing sequence number {}; \
                     writing them anyway",
                    s,
                    held.len(),
                    next
                );
                for (_, _, kind, payload) in held.drain(..) {
                    if kind == KIND_RECORD {
                        report(log.w.write_all(&payload), "write");
                        written += payload.len() as u64;
                        log.dirty = true;
                    }
                }
            }
            ord.held = held;
            entries.clear();
            if written > 0 {
                stats.aof_current_size.fetch_add(written, Ordering::Relaxed);
                wrote = true;
            }
        }
        for t in drained.drain(..) {
            let back = &mut threads[t].1;
            back.clear();
            // Not a spike's worth of memory kept for good.
            if back.capacity() > 4 * 1024 * 1024 {
                *back = Vec::new();
            }
        }

        let fsync_now = !syncs.is_empty()
            || closing
            || (wrote && inner.policy() == FsyncPolicy::Always)
            || (last_sync.elapsed() >= Duration::from_secs(1)
                && inner.policy() == FsyncPolicy::Everysec);
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
        } else if wrote {
            // Hand this pass's records to the kernel now, fsync or not: a
            // process that dies (SIGKILL, the OOM killer, a panic that
            // aborts) then loses nothing it acknowledged — only a machine
            // crash can cost the last second under `everysec`, as with
            // Redis. Kept in this buffer until the once-a-second flush, they
            // were lost with the process.
            for log in logs.iter_mut() {
                if !log.w.buffer().is_empty() {
                    report(log.w.flush(), "write");
                }
            }
        }
        if closing {
            break;
        }
        match inner.policy() {
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
        let (mut s0, mut s1) = (0, 0);
        w.log(0, &mut s0, "SET", &[b("a"), b("1")]);
        w.log(1, &mut s1, "SET", &[b("b"), b("1")]);
        w.rotate(0, &mut s0, 2);
        w.log(0, &mut s0, "SET", &[b("a"), b("2")]);
        w.log(1, &mut s1, "SET", &[b("b"), b("2")]);
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

    /// Many threads take turns on one shard, as a hot key's clients do: the
    /// log must hold their records in the order the shard numbered them,
    /// though each thread's records sit in its own buffer.
    #[test]
    fn records_from_many_threads_land_in_shard_order() {
        let dir = tempfile::tempdir().unwrap();
        let aof = dir.path().join(AOF_DIRNAME);
        create_incr_files(&aof, 1, 1).unwrap();
        let stats = Arc::new(PersistStats::new());
        let w = spawn_aof_writer(&aof, 1, 1, FsyncPolicy::Everysec, stats).unwrap();
        // Start where a store that already logged 7 records would.
        w.start_shard(0, 7);
        let shard = Arc::new(parking_lot::Mutex::new(7u64));
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let (w, shard) = (w.clone(), shard.clone());
                std::thread::spawn(move || {
                    for i in 0..2000 {
                        let mut seq = shard.lock();
                        let v = format!("{}", *seq);
                        w.log(0, &mut seq, "SET", &[b("k"), Bytes::from(v)]);
                        if i % 500 == 0 && t % 2 == 0 {
                            drop(seq);
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        w.sync().unwrap();
        let mut got = Vec::new();
        replay_log(&incr_path(&aof, 1, 0), |c| got.push(c[2].clone())).unwrap();
        let want: Vec<Bytes> = (7..7 + 16000)
            .map(|n: u64| Bytes::from(n.to_string()))
            .collect();
        assert_eq!(got.len(), want.len());
        assert!(got == want, "records out of shard order");
    }

    /// A record for another database is preceded by `SELECT`, once.
    #[test]
    fn the_writer_selects_the_database_of_each_record() {
        let dir = tempfile::tempdir().unwrap();
        let aof = dir.path().join(AOF_DIRNAME);
        create_incr_files(&aof, 1, 1).unwrap();
        let stats = Arc::new(PersistStats::new());
        let w = spawn_aof_writer(&aof, 1, 1, FsyncPolicy::Everysec, stats).unwrap();
        let mut seq = 0;
        for db in [0u32, 0, 3, 3, 0] {
            w.append_db(0, &mut seq, db, |buf| {
                encode_into(buf, b"SET", &[b"k", b"v"])
            });
        }
        w.sync().unwrap();
        let mut got = Vec::new();
        replay_log(&incr_path(&aof, 1, 0), |c| {
            got.push(format!(
                "{} {}",
                String::from_utf8_lossy(&c[0]),
                String::from_utf8_lossy(&c[1])
            ))
        })
        .unwrap();
        assert_eq!(
            got,
            vec!["SELECT 0", "SET k", "SET k", "SELECT 3", "SET k", "SET k", "SELECT 0", "SET k"]
        );
    }
}
