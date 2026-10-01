//! Master/replica replication over the Redis protocol.
//!
//! The stream itself — what is sent, in which order, with which offsets —
//! is `solikv_engine::replication`. This module is the network around it:
//!
//! - [`serve_replica`]: a client of this node sent `PSYNC` (or `SYNC`). It
//!   gets `+CONTINUE` and the backlog from its offset, or `+FULLRESYNC`, a
//!   snapshot taken at the instant it was registered with the stream, and
//!   then the stream; it acknowledges with `REPLCONF ACK <offset>`.
//! - [`Replication`]: this node follows a master (`REPLICAOF host port`).
//!   It runs the handshake (`AUTH`, `REPLCONF`, `PSYNC`), loads a full
//!   synchronization or continues from its offset, applies the stream, and
//!   passes the same bytes on to its own replicas. A dropped link
//!   reconnects with backoff and resumes with `PSYNC <replid> <offset+1>`.
//!
//! The snapshot is SoliKV's own format (sections per shard). A replica of a
//! **Redis** master is accepted too: a payload starting with `REDIS` is read
//! with the Redis RDB importer, every database into database 0 (the
//! importer's limitation), and Redis' command stream is applied as any
//! client's commands would be.

use bytes::{Bytes, BytesMut};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use solikv_core::{CommandResponse, LoadingGuard};
use solikv_engine::replication::{
    ApplyingGuard, Psync, ReplController, ReplicaStatus, Sink, StreamParser,
};
use solikv_engine::CommandEngine;

/// How often a replica acknowledges, and a master pings its replicas.
const ACK_EVERY: Duration = Duration::from_secs(1);
pub const PING_EVERY: Duration = Duration::from_secs(10);
/// No byte from the master for this long: the link is considered dead
/// (`repl-timeout`).
const REPL_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Master side
// ---------------------------------------------------------------------------

/// What a would-be replica announced before `PSYNC`.
#[derive(Debug, Default, Clone)]
pub struct Handshake {
    pub listening_port: u16,
    pub ip_address: Option<String>,
}

/// Unregisters a replica from the stream when its connection ends.
struct Registered {
    engine: Arc<CommandEngine>,
    sink: Arc<Sink>,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.engine.repl().unregister(&self.sink);
    }
}

/// Serve a replica on `socket`, which sent `PSYNC <replid> <offset>`
/// (`psync`) or `SYNC` (`None`). `pending` holds whatever it sent after.
/// Returns when the replica goes away or falls too far behind.
pub async fn serve_replica<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    mut pending: BytesMut,
    engine: Arc<CommandEngine>,
    peer: std::net::SocketAddr,
    hs: Handshake,
    psync: Option<(String, i64)>,
) -> std::io::Result<()> {
    let feed = engine.repl().clone();
    let sink = Arc::new(Sink {
        id: feed.next_sink_id(),
        notify: Arc::new(tokio::sync::Notify::new()),
        ack: Arc::new(AtomicU64::new(0)),
        addr: hs
            .ip_address
            .clone()
            .unwrap_or_else(|| peer.ip().to_string()),
        listening_port: hs.listening_port,
        online: Arc::new(AtomicBool::new(false)),
    });

    let continued = match &psync {
        Some((id, off)) => match feed.try_continue(id, *off, &sink) {
            Psync::Continue { replid } => Some(replid),
            Psync::Full => None,
        },
        None => None,
    };
    let _registered;
    match continued {
        Some(replid) => {
            _registered = Registered {
                engine: engine.clone(),
                sink: sink.clone(),
            };
            tracing::info!(%peer, "replica continues from its offset (partial resynchronization)");
            socket
                .write_all(format!("+CONTINUE {}\r\n", replid).as_bytes())
                .await?;
        }
        None => {
            let (replid, offset, payload) = full_snapshot(&engine, &sink).await?;
            _registered = Registered {
                engine: engine.clone(),
                sink: sink.clone(),
            };
            tracing::info!(
                %peer,
                bytes = payload.len(),
                "replica needs a full resynchronization; sending a snapshot"
            );
            if psync.is_some() {
                socket
                    .write_all(format!("+FULLRESYNC {} {}\r\n", replid, offset).as_bytes())
                    .await?;
            }
            socket
                .write_all(format!("${}\r\n", payload.len()).as_bytes())
                .await?;
            socket.write_all(&payload).await?;
            drop(payload);
            sink.online.store(true, Ordering::Relaxed);
        }
    }
    socket.flush().await?;

    let mut parser = StreamParser::new();
    loop {
        match feed.take(&sink) {
            Some(bytes) if !bytes.is_empty() => {
                socket.write_all(&bytes).await?;
                continue;
            }
            Some(_) => {}
            None => {
                tracing::warn!(%peer, "replica fell too far behind; dropping the link");
                return Ok(());
            }
        }
        tokio::select! {
            _ = sink.notify.notified() => {}
            r = socket.read_buf(&mut pending) => {
                if r? == 0 {
                    return Ok(());
                }
                parser.push(&pending);
                pending.clear();
                while let Some((args, _)) = parser
                    .next_command()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
                {
                    // `REPLCONF ACK <offset> [FACK <aofoffset>]`
                    if args.len() >= 3
                        && args[0].eq_ignore_ascii_case(b"REPLCONF")
                        && args[1].eq_ignore_ascii_case(b"ACK")
                    {
                        if let Some(off) = std::str::from_utf8(&args[2])
                            .ok()
                            .and_then(|s| s.parse::<u64>().ok())
                        {
                            sink.ack.fetch_max(off, Ordering::Relaxed);
                            feed.acked.notify_waiters();
                        }
                    }
                }
            }
        }
    }
}

/// Take the snapshot a full resynchronization sends: every shard at one
/// instant, the replica registered with the stream at that same instant.
async fn full_snapshot(
    engine: &Arc<CommandEngine>,
    sink: &Arc<Sink>,
) -> std::io::Result<(String, u64, Vec<u8>)> {
    let shards = engine.shards.clone();
    let feed = engine.repl().clone();
    let mut registered = None;
    let mut attempts = 0u32;
    loop {
        let began = solikv_engine::persistence::begin_snapshot_everywhere(&shards, || {
            registered = Some(feed.register_full(sink));
            // Libraries are not part of the data: they follow the snapshot
            // as a command, replacing whatever the replica had.
            if let CommandResponse::BulkString(dump) =
                engine.execute("FUNCTION", &[Bytes::from_static(b"DUMP")])
            {
                feed.command(b"FUNCTION", &[b"RESTORE", &dump, b"FLUSH"]);
            }
        });
        if began {
            break;
        }
        // A background save holds the shards' snapshot slot; it ends.
        attempts += 1;
        if attempts > 3000 {
            return Err(std::io::Error::other(
                "could not start a snapshot for the replica: one is always running",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (replid, offset) = registered.expect("registered inside the snapshot instant");

    let n = shards.num_shards();
    let bodies: Vec<Vec<u8>> = if shards.is_solo() {
        // The solo store belongs to this thread: step it here, letting the
        // other connections run in between.
        let mut body = Vec::new();
        loop {
            let (chunk, done) = solikv_engine::persistence::snapshot_step(&shards, 0);
            body.extend_from_slice(&chunk);
            if done {
                break;
            }
            tokio::task::yield_now().await;
        }
        vec![body]
    } else {
        let shards = shards.clone();
        tokio::task::spawn_blocking(move || {
            (0..n)
                .map(|i| {
                    let mut body = Vec::new();
                    loop {
                        let (chunk, done) = solikv_engine::persistence::snapshot_step(&shards, i);
                        body.extend_from_slice(&chunk);
                        if done {
                            return body;
                        }
                        std::thread::yield_now();
                    }
                })
                .collect()
        })
        .await
        .map_err(std::io::Error::other)?
    };
    let mut w = solikv_persist::rdb::SnapshotWriter::new(Vec::new())?;
    for (i, body) in bodies.iter().enumerate() {
        w.write_section(i as u32, body)?;
    }
    let (payload, _) = w.finish()?;
    Ok((replid, offset, payload))
}

/// Ping the replicas every [`PING_EVERY`] (part of the stream, so a replica
/// can tell a quiet master from a dead link).
pub fn spawn_master_pinger(engine: Arc<CommandEngine>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PING_EVERY);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            engine.repl().ping();
        }
    });
}

/// `WAIT numreplicas timeout`: block until that many replicas acknowledged
/// everything written so far, or the timeout (ms, 0 = forever) passes.
pub async fn wait(engine: &CommandEngine, numreplicas: usize, timeout_ms: u64) -> usize {
    let feed = engine.repl();
    let target = feed.offset();
    let n = feed.acked_at_least(target);
    if n >= numreplicas || feed.connected() == 0 {
        return n;
    }
    feed.getack();
    let deadline =
        (timeout_ms > 0).then(|| tokio::time::Instant::now() + Duration::from_millis(timeout_ms));
    loop {
        let notified = feed.acked.notified();
        let n = feed.acked_at_least(target);
        if n >= numreplicas {
            return n;
        }
        match deadline {
            Some(d) => {
                if tokio::time::timeout_at(d, notified).await.is_err() {
                    return feed.acked_at_least(target);
                }
            }
            None => notified.await,
        }
    }
}

// ---------------------------------------------------------------------------
// Replica side
// ---------------------------------------------------------------------------

/// How this node authenticates to its master, and what it announces.
#[derive(Debug, Clone, Default)]
pub struct ReplConfig {
    pub masterauth: Option<String>,
    pub masteruser: Option<String>,
    /// This node's RESP port (`REPLCONF listening-port`).
    pub listening_port: u16,
    /// `replica-announce-ip`.
    pub announce_ip: Option<String>,
    /// Where the snapshot is staged while it is received.
    pub dir: PathBuf,
}

#[derive(Debug, Default)]
struct LinkState {
    master: Option<(String, u16)>,
    state: &'static str,
    last_io: Option<Instant>,
    sync_in_progress: bool,
}

/// This node's replication link (see the module doc).
pub struct Replication {
    engine: Arc<CommandEngine>,
    cfg: parking_lot::RwLock<ReplConfig>,
    link: parking_lot::Mutex<LinkState>,
    /// Bumped on every role change: a link task whose generation is stale
    /// stops at its next step.
    generation: AtomicU64,
    runtime: tokio::runtime::Handle,
    /// Told when a link to a master completes a synchronization (cluster
    /// failover waits on it).
    pub synced: tokio::sync::Notify,
    me: std::sync::Weak<Self>,
}

impl Replication {
    /// Create the controller and install it in the engine. Call inside the
    /// runtime.
    pub fn install(engine: Arc<CommandEngine>, cfg: ReplConfig) -> Arc<Self> {
        let me = Arc::new_cyclic(|me| Self {
            engine: engine.clone(),
            cfg: parking_lot::RwLock::new(cfg),
            link: parking_lot::Mutex::new(LinkState::default()),
            generation: AtomicU64::new(0),
            runtime: tokio::runtime::Handle::current(),
            synced: tokio::sync::Notify::new(),
            me: me.clone(),
        });
        engine.set_repl_controller(me.clone());
        me
    }

    pub fn set_masterauth(&self, user: Option<String>, pass: Option<String>) {
        let mut c = self.cfg.write();
        c.masteruser = user;
        c.masterauth = pass;
    }

    /// The master this node follows, if any.
    pub fn master(&self) -> Option<(String, u16)> {
        self.link.lock().master.clone()
    }

    pub fn is_connected(&self) -> bool {
        self.link.lock().state == "connected"
    }

    fn set_state(&self, gen: u64, state: &'static str) {
        if self.generation.load(Ordering::Acquire) == gen {
            self.link.lock().state = state;
        }
    }

    fn touch(&self) {
        self.link.lock().last_io = Some(Instant::now());
    }

    async fn run_link(self: Arc<Self>, gen: u64, host: String, port: u16) {
        let mut backoff = Duration::from_millis(100);
        while self.generation.load(Ordering::Acquire) == gen {
            self.set_state(gen, "connecting");
            match self.sync_once(gen, &host, port).await {
                Ok(()) => backoff = Duration::from_millis(100),
                Err(e) => {
                    tracing::warn!(master = %format!("{host}:{port}"), error = %e, "replication link down");
                }
            }
            {
                let mut l = self.link.lock();
                if self.generation.load(Ordering::Acquire) == gen {
                    l.state = "connect";
                    l.sync_in_progress = false;
                }
            }
            if self.generation.load(Ordering::Acquire) != gen {
                return;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(2));
        }
    }

    /// One connection to the master: handshake, synchronization, stream.
    async fn sync_once(&self, gen: u64, host: &str, port: u16) -> Result<(), String> {
        let stale = || self.generation.load(Ordering::Acquire) != gen;
        let stream = tokio::time::timeout(
            CONNECT_TIMEOUT,
            tokio::net::TcpStream::connect((host, port)),
        )
        .await
        .map_err(|_| "connect timed out".to_string())?
        .map_err(|e| format!("connect: {e}"))?;
        let _ = stream.set_nodelay(true);
        let mut link = Link::new(stream);
        let cfg = self.cfg.read().clone();

        // PING (an `-NOAUTH` answer is fine: AUTH comes next).
        link.send(&[b"PING"]).await?;
        let r = link.line().await?;
        if r.starts_with('-') && !r.starts_with("-NOAUTH") && !r.starts_with("-NOPERM") {
            return Err(format!("master answered PING with {r}"));
        }
        if let Some(pass) = &cfg.masterauth {
            match &cfg.masteruser {
                Some(user) => {
                    link.send(&[b"AUTH", user.as_bytes(), pass.as_bytes()])
                        .await?
                }
                None => link.send(&[b"AUTH", pass.as_bytes()]).await?,
            }
            let r = link.line().await?;
            if !r.starts_with("+OK") {
                return Err(format!("master refused AUTH: {r}"));
            }
        }
        let port_s = cfg.listening_port.to_string();
        link.send(&[b"REPLCONF", b"listening-port", port_s.as_bytes()])
            .await?;
        expect_ok(&mut link, "REPLCONF listening-port").await?;
        if let Some(ip) = &cfg.announce_ip {
            link.send(&[b"REPLCONF", b"ip-address", ip.as_bytes()])
                .await?;
            expect_ok(&mut link, "REPLCONF ip-address").await?;
        }
        link.send(&[b"REPLCONF", b"capa", b"eof", b"capa", b"psync2"])
            .await?;
        expect_ok(&mut link, "REPLCONF capa").await?;

        let feed = self.engine.repl().clone();
        let (replid, _, offset, _) = feed.ids();
        let resume = feed.is_active();
        let off_s = (offset + 1).to_string();
        if resume {
            link.send(&[b"PSYNC", replid.as_bytes(), off_s.as_bytes()])
                .await?;
        } else {
            link.send(&[b"PSYNC", b"?", b"-1"]).await?;
        }
        if stale() {
            return Ok(());
        }
        let r = link.line().await?;
        if let Some(rest) = r.strip_prefix("+FULLRESYNC ") {
            let mut it = rest.split_whitespace();
            let id = it.next().ok_or("FULLRESYNC without an id")?.to_string();
            let off: u64 = it
                .next()
                .and_then(|s| s.parse().ok())
                .ok_or("FULLRESYNC without an offset")?;
            self.link.lock().sync_in_progress = true;
            self.set_state(gen, "sync");
            let path = self.receive_payload(&mut link, &cfg).await?;
            if stale() {
                let _ = std::fs::remove_file(&path);
                return Ok(());
            }
            let loaded = self.load_payload(&path);
            let _ = std::fs::remove_file(&path);
            let keys = loaded?;
            feed.reset_to(&id, off);
            self.link.lock().sync_in_progress = false;
            tracing::info!(keys, master = %format!("{host}:{port}"), "full resynchronization loaded");
        } else if let Some(rest) = r.strip_prefix("+CONTINUE") {
            let id = rest.trim();
            if !id.is_empty() {
                feed.switch_replid(id);
            }
            tracing::info!(master = %format!("{host}:{port}"), "partial resynchronization accepted");
        } else {
            return Err(format!("master refused PSYNC: {r}"));
        }
        if stale() {
            return Ok(());
        }
        self.set_state(gen, "connected");
        self.touch();
        self.synced.notify_waiters();

        // The stream.
        let mut parser = StreamParser::new();
        parser.push(&link.take_buffered());
        let mut applier = Applier::new(self.engine.clone());
        let mut ack = tokio::time::interval(ACK_EVERY);
        ack.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut buf = vec![0u8; 64 * 1024];
        let mut last_io = Instant::now();
        loop {
            let mut ack_now = false;
            while let Some((args, raw)) = parser.next_command()? {
                if applier.apply(args) {
                    ack_now = true;
                }
                feed.proxy(&raw);
            }
            if ack_now {
                send_ack(&mut link, feed.offset()).await?;
            }
            if stale() {
                return Ok(());
            }
            tokio::select! {
                r = link.stream.read(&mut buf) => {
                    let n = r.map_err(|e| format!("read: {e}"))?;
                    if n == 0 {
                        return Err("master closed the link".into());
                    }
                    parser.push(&buf[..n]);
                    last_io = Instant::now();
                    self.touch();
                }
                _ = ack.tick() => {
                    if last_io.elapsed() > REPL_TIMEOUT {
                        return Err("no data from the master within the replication timeout".into());
                    }
                    send_ack(&mut link, feed.offset()).await?;
                }
            }
        }
    }

    /// Read the snapshot (`$<len>` or `$EOF:<mark>`) into a file.
    async fn receive_payload(&self, link: &mut Link, cfg: &ReplConfig) -> Result<PathBuf, String> {
        // Redis sends bare newlines while it prepares the snapshot.
        let header = loop {
            let l = link.line_timeout(REPL_TIMEOUT).await?;
            if !l.is_empty() {
                break l;
            }
        };
        let header = header
            .strip_prefix('$')
            .ok_or_else(|| format!("expected a snapshot, got {header}"))?
            .to_string();
        let _ = solikv_persist::fsutil::create_private_dir_all(&cfg.dir);
        let path = cfg.dir.join(format!(
            "temp-repl-{}-{}.rdb",
            std::process::id(),
            self.generation.load(Ordering::Relaxed)
        ));
        let mut file = tokio::fs::File::create(&path)
            .await
            .map_err(|e| format!("cannot stage the snapshot at {}: {e}", path.display()))?;
        if let Some(mark) = header.strip_prefix("EOF:") {
            // Diskless: read until the 40-byte mark.
            let mark = mark.as_bytes().to_vec();
            let mut tail: Vec<u8> = Vec::new();
            loop {
                let chunk = link.read_some(REPL_TIMEOUT).await?;
                tail.extend_from_slice(&chunk);
                if tail.len() >= mark.len() && tail.ends_with(&mark) {
                    let body = tail.len() - mark.len();
                    file.write_all(&tail[..body])
                        .await
                        .map_err(|e| e.to_string())?;
                    break;
                }
                if tail.len() > 2 * mark.len() {
                    let keep = tail.len() - mark.len();
                    file.write_all(&tail[..keep])
                        .await
                        .map_err(|e| e.to_string())?;
                    tail.drain(..keep);
                }
            }
        } else {
            let mut left: usize = header
                .trim()
                .parse()
                .map_err(|_| format!("bad snapshot length {header}"))?;
            while left > 0 {
                let chunk = link.read_up_to(left, REPL_TIMEOUT).await?;
                left -= chunk.len();
                file.write_all(&chunk).await.map_err(|e| e.to_string())?;
            }
        }
        file.flush().await.map_err(|e| e.to_string())?;
        Ok(path)
    }

    /// Replace the dataset with the snapshot at `path`. Logged like any
    /// write: the local append-only log gets a `FLUSHALL` and every key.
    fn load_payload(&self, path: &std::path::Path) -> Result<u64, String> {
        let _applying = ApplyingGuard::new();
        let _loading = LoadingGuard::new();
        let mut magic = [0u8; 5];
        {
            use std::io::Read;
            let mut f = std::fs::File::open(path).map_err(|e| e.to_string())?;
            f.read_exact(&mut magic)
                .map_err(|e| format!("snapshot: {e}"))?;
        }
        // Read fully before touching the dataset: a damaged snapshot must
        // not leave the replica half-flushed.
        let mut entries: Vec<(u32, Bytes, solikv_core::types::KeyEntry)> = Vec::new();
        if &magic == b"REDIS" {
            let (buckets, _) = solikv_persist::redis_rdb::import_redis_rdb(path, 1)
                .map_err(|e| format!("Redis snapshot: {e}"))?;
            for (k, e) in buckets.into_iter().flatten() {
                entries.push((0, k, e));
            }
        } else {
            solikv_persist::rdb::load_snapshot_file(path, |_, db, k, e| {
                entries.push((db, k, e));
                Ok(())
            })
            .map_err(|e| format!("snapshot: {e}"))?;
        }
        self.engine.execute_in_db(0, "FLUSHALL", &[]);
        let n = entries.len() as u64;
        for (db, k, e) in entries {
            self.engine.put_entry(db, k, e);
        }
        Ok(n)
    }
}

impl ReplController for Replication {
    fn replicaof(&self, host: String, port: u16) -> Result<(), String> {
        {
            let l = self.link.lock();
            if l.master.as_ref() == Some(&(host.clone(), port)) {
                return Ok(());
            }
        }
        let gen = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let feed = self.engine.repl();
        feed.become_replica();
        // Its own replicas follow a new history now.
        feed.drop_sinks();
        self.engine.shards.set_replica_expiry(true);
        {
            let mut l = self.link.lock();
            l.master = Some((host.clone(), port));
            l.state = "connect";
            l.last_io = None;
            l.sync_in_progress = false;
        }
        tracing::info!(master = %format!("{host}:{port}"), "following a master");
        if let Some(me) = self.me.upgrade() {
            self.runtime.spawn(me.run_link(gen, host, port));
        }
        Ok(())
    }

    fn no_one(&self) -> Result<(), String> {
        let was = self.link.lock().master.take();
        if was.is_none() {
            return Ok(());
        }
        self.generation.fetch_add(1, Ordering::AcqRel);
        {
            let mut l = self.link.lock();
            l.state = "";
            l.sync_in_progress = false;
        }
        self.engine.shards.set_replica_expiry(false);
        self.engine.repl().promote();
        tracing::info!("promoted to master");
        Ok(())
    }

    fn status(&self) -> Option<ReplicaStatus> {
        let l = self.link.lock();
        let (host, port) = l.master.clone()?;
        Some(ReplicaStatus {
            master_host: host,
            master_port: port,
            state: if l.state.is_empty() {
                "connect"
            } else {
                l.state
            },
            last_io_secs: l.last_io.map_or(-1, |t| t.elapsed().as_secs() as i64),
            sync_in_progress: l.sync_in_progress,
        })
    }
}

async fn expect_ok(link: &mut Link, what: &str) -> Result<(), String> {
    let r = link.line().await?;
    if r.starts_with('+') {
        Ok(())
    } else {
        Err(format!("master refused {what}: {r}"))
    }
}

async fn send_ack(link: &mut Link, offset: u64) -> Result<(), String> {
    let off = offset.to_string();
    link.send(&[b"REPLCONF", b"ACK", off.as_bytes()]).await
}

/// Applies the master's stream to the engine.
struct Applier {
    engine: Arc<CommandEngine>,
    db: u32,
    multi: Option<Vec<Vec<Bytes>>>,
}

impl Applier {
    fn new(engine: Arc<CommandEngine>) -> Self {
        Self {
            engine,
            db: 0,
            multi: None,
        }
    }

    /// Apply one command; true when the master asked for an ack.
    fn apply(&mut self, args: Vec<Bytes>) -> bool {
        if args.is_empty() {
            return false;
        }
        let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        match name.as_str() {
            "PING" => false,
            "REPLCONF" => args
                .get(1)
                .is_some_and(|a| a.eq_ignore_ascii_case(b"GETACK")),
            "MULTI" => {
                self.multi = Some(Vec::new());
                false
            }
            "EXEC" => {
                let Some(queued) = self.multi.take() else {
                    return false;
                };
                let _applying = ApplyingGuard::new();
                let _loading = LoadingGuard::new();
                let engine = self.engine.clone();
                let _held = engine.shards.lock_all();
                let _tx = engine.aof_transaction();
                for cmd in queued {
                    self.exec_one(cmd);
                }
                false
            }
            _ => {
                if let Some(q) = self.multi.as_mut() {
                    q.push(args);
                } else {
                    let _applying = ApplyingGuard::new();
                    let _loading = LoadingGuard::new();
                    self.exec_one(args);
                }
                false
            }
        }
    }

    fn exec_one(&mut self, args: Vec<Bytes>) {
        let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        match name.as_str() {
            "SELECT" => {
                if let Some(db) = args
                    .get(1)
                    .and_then(|a| std::str::from_utf8(a).ok()?.parse::<u32>().ok())
                {
                    self.db = db;
                }
            }
            solikv_engine::aof_log::PUT_RECORD => {
                if args.len() == 3 {
                    match solikv_persist::rdb::decode_entry(&args[2]) {
                        Ok(entry) => self.engine.put_entry(self.db, args[1].clone(), entry),
                        Err(e) => tracing::error!("replication: undecodable #PUT: {e}"),
                    }
                }
            }
            "PUBLISH" if args.len() == 3 => {
                self.engine.pubsub.publish(args[1].clone(), args[2].clone());
            }
            _ => {
                let resp = self.engine.execute_in_db(self.db, &name, &args[1..]);
                if let CommandResponse::Error(e) = &resp {
                    tracing::warn!("replication: {} from the master failed here: {}", name, e);
                }
            }
        }
    }
}

/// The replica's connection to its master, with a read buffer.
struct Link {
    stream: tokio::net::TcpStream,
    buf: BytesMut,
}

impl Link {
    fn new(stream: tokio::net::TcpStream) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(64 * 1024),
        }
    }

    async fn send(&mut self, args: &[&[u8]]) -> Result<(), String> {
        let rec = solikv_persist::aof::encode_command(args[0], &args[1..]);
        self.stream
            .write_all(&rec)
            .await
            .map_err(|e| format!("write: {e}"))
    }

    async fn fill(&mut self, timeout: Duration) -> Result<(), String> {
        let n = tokio::time::timeout(timeout, self.stream.read_buf(&mut self.buf))
            .await
            .map_err(|_| "master did not answer in time".to_string())?
            .map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            return Err("master closed the link".into());
        }
        Ok(())
    }

    async fn line(&mut self) -> Result<String, String> {
        self.line_timeout(Duration::from_secs(30)).await
    }

    /// One `\n`-terminated line, without its line ending.
    async fn line_timeout(&mut self, timeout: Duration) -> Result<String, String> {
        loop {
            if let Some(nl) = self.buf.iter().position(|&b| b == b'\n') {
                let line = self.buf.split_to(nl + 1);
                return Ok(String::from_utf8_lossy(&line).trim_end().to_string());
            }
            if self.buf.len() > 64 * 1024 {
                return Err("master sent an overlong line".into());
            }
            self.fill(timeout).await?;
        }
    }

    async fn read_some(&mut self, timeout: Duration) -> Result<Vec<u8>, String> {
        if self.buf.is_empty() {
            self.fill(timeout).await?;
        }
        Ok(self.buf.split().to_vec())
    }

    async fn read_up_to(&mut self, max: usize, timeout: Duration) -> Result<Vec<u8>, String> {
        if self.buf.is_empty() {
            self.fill(timeout).await?;
        }
        let n = max.min(self.buf.len());
        Ok(self.buf.split_to(n).to_vec())
    }

    fn take_buffered(&mut self) -> Vec<u8> {
        self.buf.split().to_vec()
    }
}
