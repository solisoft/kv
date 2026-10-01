use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use solikv_engine::CommandEngine;

trait AsyncSocket: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncSocket for T {}
use solikv_pubsub::{PubSubBroker, PubSubMessage};
use solikv_resp::codec::{decode_frame, encode_frame, RespFrame};
use solikv_resp::connection::ClientConnection;
use solikv_resp::parser::ParsedCommand;
use solikv_resp::{CommandDecoder, Resolved};

use solikv_cluster::ClusterManager;
use solikv_core::CommandResponse;

/// One open connection, counted against `maxclients` while it lives.
struct LiveConn(Arc<std::sync::atomic::AtomicUsize>);

impl LiveConn {
    fn new(live: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        live.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self(live)
    }
}

impl Drop for LiveConn {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

const MAX_AUTH_FAILURES: u32 = 10;
const AUTH_COOLDOWN_SECS: u64 = 30;

struct AuthFailureTracker {
    failures: RwLock<std::collections::HashMap<String, (u32, std::time::Instant)>>,
}

impl Default for AuthFailureTracker {
    fn default() -> Self {
        Self {
            failures: RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl AuthFailureTracker {
    fn is_blocked(&self, ip: &str) -> bool {
        let failures = self.failures.read().unwrap();
        if let Some((count, first_failure)) = failures.get(ip) {
            if *count >= MAX_AUTH_FAILURES {
                let elapsed = first_failure.elapsed().as_secs();
                return elapsed < AUTH_COOLDOWN_SECS;
            }
        }
        false
    }

    fn record_failure(&self, ip: &str) {
        let mut failures = self.failures.write().unwrap();
        let now = std::time::Instant::now();
        // Keyed by peer IP, so it grows with every address that ever failed.
        // Forget the ones whose cooldown is long over.
        if failures.len() >= AUTH_TRACKER_PRUNE_AT {
            failures.retain(|_, (_, last)| last.elapsed().as_secs() < AUTH_COOLDOWN_SECS);
        }
        let entry = failures.entry(ip.to_string()).or_insert((0, now));
        entry.0 += 1;
        entry.1 = now;
    }

    fn record_success(&self, ip: &str) {
        if !self.failures.read().unwrap().contains_key(ip) {
            return;
        }
        let mut failures = self.failures.write().unwrap();
        failures.remove(ip);
    }
}

/// Prune the failure map once it holds this many addresses.
const AUTH_TRACKER_PRUNE_AT: usize = 4096;

/// Most bytes buffered for a connection that has not authenticated: enough
/// for `AUTH` with a password up to `MAX_UNAUTH_BULK_LEN`, and nothing like
/// the 256 MB that used to be allowed before a single credential was checked.
pub const MAX_UNAUTH_READ_BUF: usize = 128 * 1024;

/// Per-connection limits.
#[derive(Debug, Clone)]
pub struct ConnLimits {
    /// Longest a TLS handshake may take before the socket is dropped.
    pub handshake_timeout: std::time::Duration,
    /// A connection that must authenticate has this long to do it, from
    /// accept. `None` disables. Holds the connection slots (10 000) against
    /// peers that connect and say nothing.
    pub auth_timeout: Option<std::time::Duration>,
    /// Close an authenticated client idle this long (Redis `timeout`).
    /// `None` = never, the Redis default. Not applied in pub/sub mode.
    pub idle_timeout: Option<std::time::Duration>,
    /// Most bytes buffered for one authenticated client's queries (Redis
    /// `client-query-buffer-limit`).
    pub query_buffer_limit: usize,
}

impl Default for ConnLimits {
    fn default() -> Self {
        Self {
            handshake_timeout: std::time::Duration::from_secs(10),
            auth_timeout: Some(std::time::Duration::from_secs(10)),
            idle_timeout: None,
            query_buffer_limit: 1024 * 1024 * 1024,
        }
    }
}

pub async fn run(
    addr: &str,
    engine: Arc<CommandEngine>,
    pubsub: Arc<PubSubBroker>,
    password: Option<Arc<String>>,
    cluster: Option<Arc<ClusterManager>>,
    tls_config: Option<ServerConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    run_with_limits(
        addr,
        engine,
        pubsub,
        password,
        cluster,
        tls_config,
        ConnLimits::default(),
    )
    .await
}

pub async fn run_with_limits(
    addr: &str,
    engine: Arc<CommandEngine>,
    pubsub: Arc<PubSubBroker>,
    password: Option<Arc<String>>,
    cluster: Option<Arc<ClusterManager>>,
    tls_config: Option<ServerConfig>,
    limits: ConnLimits,
) -> Result<(), Box<dyn std::error::Error>> {
    let limits = Arc::new(limits);
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("RESP server listening on {}", addr);
    let tls_acceptor = tls_config.map(|c| TlsAcceptor::from(Arc::new(c)));
    // `requirepass` is the default user's password.
    if let Some(p) = &password {
        engine.acl().set_requirepass(Some(p.as_str()));
    }

    let live = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let auth_tracker = Arc::new(AuthFailureTracker::default());

    loop {
        let (socket, peer_addr) = listener.accept().await?;

        // `maxclients` (CONFIG SET-able), answered as Redis does.
        if live.load(std::sync::atomic::Ordering::Relaxed) >= engine.maxclients() {
            tracing::warn!(
                "Connection limit reached ({}), rejecting {}",
                engine.maxclients(),
                peer_addr
            );
            engine
                .stats()
                .rejected_connections
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::spawn(async move {
                let mut socket = socket;
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    socket.write_all(b"-ERR max number of clients reached\r\n"),
                )
                .await;
            });
            continue;
        }
        let permit = LiveConn::new(live.clone());

        // TCP_NODELAY: disable Nagle's algorithm for lower latency. Set on
        // the underlying TcpStream before any TLS wrap.
        let _ = socket.set_nodelay(true);
        let local_addr = socket
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_default();

        let engine = engine.clone();
        let pubsub = pubsub.clone();
        let password = password.clone();
        let cluster = cluster.clone();
        let tls_acceptor = tls_acceptor.clone();
        let auth_tracker = auth_tracker.clone();
        let limits = limits.clone();

        tokio::spawn(async move {
            let accepted_at = tokio::time::Instant::now();
            let mut socket: Box<dyn AsyncSocket> = match tls_acceptor {
                // Bounded: a peer that opens a socket and never finishes the
                // handshake otherwise holds a connection slot forever.
                Some(acceptor) => {
                    match tokio::time::timeout(limits.handshake_timeout, acceptor.accept(socket))
                        .await
                    {
                        Ok(Ok(stream)) => Box::new(stream),
                        Ok(Err(e)) => {
                            tracing::debug!("TLS accept error from {}: {}", peer_addr, e);
                            drop(permit);
                            return;
                        }
                        Err(_) => {
                            tracing::debug!("TLS handshake from {} timed out", peer_addr);
                            drop(permit);
                            return;
                        }
                    }
                }
                None => Box::new(socket),
            };
            if let Err(e) = handle_connection(
                &mut socket,
                engine,
                pubsub,
                peer_addr,
                local_addr,
                password,
                cluster,
                auth_tracker,
                &limits,
                accepted_at,
            )
            .await
            {
                tracing::debug!("Connection error from {}: {}", peer_addr, e);
            }
            drop(permit);
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: S,
    engine: Arc<CommandEngine>,
    pubsub: Arc<PubSubBroker>,
    peer_addr: std::net::SocketAddr,
    local_addr: String,
    password: Option<Arc<String>>,
    cluster: Option<Arc<ClusterManager>>,
    auth_tracker: Arc<AuthFailureTracker>,
    limits: &ConnLimits,
    accepted_at: tokio::time::Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let auth_deadline = limits.auth_timeout.map(|t| accepted_at + t);

    let mut read_buf = BytesMut::with_capacity(65536);
    let mut write_buf = BytesMut::with_capacity(65536);
    let mut conn = ClientConnection::new(peer_addr, !engine.acl().default_auto_auth());
    // Database, MULTI queue, watches, CLIENT metadata: engine-side state.
    let mut session = engine.new_session(peer_addr.to_string(), local_addr);
    let info = session.info.clone();
    // Out-of-band messages for this connection (tracking invalidations).
    let (push_tx, mut push_rx) = mpsc::unbounded_channel::<solikv_engine::tracking::Push>();
    info.set_push(push_tx);

    let mut decoder = CommandDecoder::new();
    // What a replica announces (`REPLCONF`) before it asks to sync.
    let mut repl_hs = crate::replication::Handshake::default();
    // Cluster mode: `ASKING` (for the next command only) and `READONLY`.
    let mut asking = false;
    let mut readonly_conn = false;
    loop {
        // ── Phase 1: Decode every complete command in the buffer ──
        // Tighter limits apply until the connection has AUTHed: a
        // not-yet-authenticated peer cannot declare arrays larger than
        // MAX_UNAUTH_ARRAY_LEN or bulks larger than MAX_UNAUTH_BULK_LEN
        // (SEC-013). The decoder resumes where the last read left off, and
        // read-only commands borrow their arguments from `read_buf`.
        let mut commands = Vec::new();
        loop {
            match decoder.next(&mut read_buf, conn.authenticated, resolve_command) {
                Ok(Some(cmd)) => commands.push(cmd),
                Ok(None) => break, // need more data
                Err(e) => {
                    encode_frame(&RespFrame::error(format!("ERR {e}")), &mut write_buf);
                    socket.write_all(&write_buf).await?;
                    return Ok(());
                }
            }
        }

        // If no complete command, read more data from the socket
        if commands.is_empty() {
            let cap = if conn.authenticated {
                limits.query_buffer_limit
            } else {
                MAX_UNAUTH_READ_BUF
            };
            if read_buf.len() > cap {
                encode_frame(
                    &RespFrame::error("ERR Protocol error: query buffer limit exceeded"),
                    &mut write_buf,
                );
                let _ = socket.write_all(&write_buf).await;
                return Err("read buffer exceeded maximum size".into());
            }
            if info.is_killed() {
                return Ok(());
            }
            prepare_read_buf(&mut read_buf, decoder.missing().min(cap));
            let idle_timeout = limits.idle_timeout.or_else(|| {
                let t = engine.idle_timeout();
                (t > 0).then(|| std::time::Duration::from_secs(t))
            });
            let read = async {
                // `CLIENT KILL` from another connection ends this one even
                // while it waits for input; a push is written as it comes.
                tokio::select! {
                    r = socket.read_buf(&mut read_buf) => Waited::Read(r),
                    _ = info.killed() => Waited::Killed,
                    p = push_rx.recv() => Waited::Push(p),
                }
            };
            let n = match (conn.authenticated, auth_deadline, idle_timeout) {
                // Not yet authenticated: the whole connection has until the
                // deadline, not each read — a byte every few seconds must not
                // keep a slot forever.
                (false, Some(deadline), _) => match tokio::time::timeout_at(deadline, read).await {
                    Ok(n) => n,
                    Err(_) => {
                        tracing::debug!("{} did not authenticate in time", peer_addr);
                        return Ok(());
                    }
                },
                (true, _, Some(idle)) => match tokio::time::timeout(idle, read).await {
                    Ok(n) => n,
                    Err(_) => return Ok(()),
                },
                _ => read.await,
            };
            let n = match n {
                Waited::Read(r) => Some(r),
                Waited::Killed => None,
                Waited::Push(Some(p)) => {
                    encode_push(&p, session.resp, false, &mut write_buf);
                    while let Ok(p) = push_rx.try_recv() {
                        encode_push(&p, session.resp, false, &mut write_buf);
                    }
                    if !write_buf.is_empty() {
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                    continue;
                }
                Waited::Push(None) => continue,
            };
            let Some(n) = n else {
                return Ok(()); // killed
            };
            let n = n?;
            if n == 0 {
                return Ok(()); // EOF
            }
            continue;
        }

        // ── Phase 3: Execute commands ──
        // Fast path: all commands are pure engine commands (no MULTI/EXEC/SUBSCRIBE/AUTH)
        // This is the common case for pipelined benchmarks
        if let Some(last) = commands.last() {
            info.touch(&last.name, commands.len());
        }
        let fast_path = conn.authenticated
            && !asking
            && !session.in_multi()
            && commands
                .iter()
                .all(|cmd| !needs_connection_state(&cmd.name));

        if fast_path {
            // Pre-reserve write buffer for batch responses
            write_buf.reserve(commands.len() * 16);
            for cmd in commands {
                // Cluster mode: MOVED, ASK, CROSSSLOT, CLUSTERDOWN.
                if let Some(redirect) = cluster_redirect(
                    &cluster,
                    &engine,
                    &cmd.name,
                    &cmd.args,
                    false,
                    readonly_conn,
                ) {
                    encode_frame(&redirect, &mut write_buf);
                    continue;
                }
                if engine.writes_paused() {
                    wait_while_paused(&engine, &cmd.name).await;
                }
                if engine.script_running() {
                    if !write_buf.is_empty() {
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                    if let Some(busy) = engine.script_gate(&cmd.name, &cmd.args).await {
                        encode_response(&busy, &mut write_buf);
                        continue;
                    }
                }
                let resp = if engine.is_blocking_form(&cmd.name, &cmd.args) {
                    if let Some(denied) = engine.acl_gate(&mut session, &cmd.name, &cmd.args) {
                        encode_response(&denied, &mut write_buf);
                        continue;
                    }
                    match run_blocking(
                        &mut socket,
                        &mut read_buf,
                        &mut write_buf,
                        &engine,
                        &mut session,
                        &info,
                        &cmd.name,
                        &cmd.args,
                        limits.query_buffer_limit,
                    )
                    .await?
                    {
                        Some(r) => r,
                        None => return Ok(()),
                    }
                } else {
                    engine.execute_session(&mut session, &cmd.name, &cmd.args)
                };
                if session.reply_allowed() {
                    encode_for(session.resp, &cmd.name, &cmd.args, resp, &mut write_buf);
                }
                drop(cmd);
                // A long pipeline's replies go out as they accumulate
                // instead of all being held until the batch ends.
                if write_buf.len() >= FLUSH_AT {
                    socket.write_all(&write_buf).await?;
                    write_buf.clear();
                }
            }
        } else {
            // Slow path: sequential processing with connection state
            for cmd in commands {
                // AUTH [username] password
                if cmd.name == "AUTH" {
                    let ip = peer_addr.ip().to_string();
                    if auth_tracker.is_blocked(&ip) {
                        tracing::warn!("AUTH blocked for {} (too many failures)", ip);
                        encode_frame(
                            &RespFrame::error("ERR too many failures, please wait"),
                            &mut write_buf,
                        );
                        socket.write_all(&write_buf).await?;
                        return Ok(());
                    }
                    let frame = match acl_auth(&engine, &cmd.args) {
                        Ok(user) => {
                            conn.authenticated = true;
                            session.set_user(user, engine.acl().version());
                            auth_tracker.record_success(&ip);
                            RespFrame::ok()
                        }
                        Err(AuthError::Arity) => {
                            RespFrame::error("ERR wrong number of arguments for 'auth' command")
                        }
                        Err(AuthError::NoPassword) => RespFrame::error(
                            "ERR AUTH <password> called without any password configured for \
                             the default user. Are you sure your configuration is correct?",
                        ),
                        Err(AuthError::Wrong) => {
                            auth_tracker.record_failure(&ip);
                            tracing::warn!("Failed AUTH attempt from {}", ip);
                            let who = if cmd.args.len() == 2 {
                                String::from_utf8_lossy(&cmd.args[0]).to_string()
                            } else {
                                "default".to_string()
                            };
                            engine.acl().log_auth_failure(&who, info.describe());
                            RespFrame::error(WRONGPASS)
                        }
                    };
                    encode_frame(&frame, &mut write_buf);
                    continue;
                }

                // HELLO [protover [AUTH user pass] [SETNAME name]]
                if cmd.name == "HELLO" {
                    let ip = peer_addr.ip().to_string();
                    if auth_tracker.is_blocked(&ip) {
                        encode_frame(
                            &RespFrame::error("ERR too many failures, please wait"),
                            &mut write_buf,
                        );
                        socket.write_all(&write_buf).await?;
                        return Ok(());
                    }
                    let reply = hello(
                        &cmd.args,
                        &password,
                        &mut conn.authenticated,
                        &auth_tracker,
                        &ip,
                        &engine,
                        &mut session,
                        cluster.is_some(),
                    );
                    if session.resp >= 3 {
                        encode_response3(&reply, &mut write_buf);
                    } else {
                        encode_response(&reply, &mut write_buf);
                    }
                    continue;
                }

                // Allow QUIT and PING without auth
                if !conn.authenticated && !matches!(cmd.name.as_str(), "QUIT" | "PING") {
                    encode_frame(
                        &RespFrame::error("NOAUTH Authentication required."),
                        &mut write_buf,
                    );
                    continue;
                }

                // Replication: the handshake a replica runs, then the link.
                if !session.in_multi()
                    && matches!(cmd.name.as_str(), "REPLCONF" | "PSYNC" | "SYNC" | "WAIT")
                {
                    if let Some(denied) = engine.acl_gate(&mut session, &cmd.name, &cmd.args) {
                        encode_response(&denied, &mut write_buf);
                        continue;
                    }
                    match cmd.name.as_str() {
                        "REPLCONF" => {
                            if let Some(reply) = replconf(&cmd.args, &mut repl_hs) {
                                encode_frame(&reply, &mut write_buf);
                            }
                        }
                        "WAIT" => {
                            let parsed = (cmd.args.len() == 2)
                                .then(|| {
                                    let n = std::str::from_utf8(&cmd.args[0])
                                        .ok()?
                                        .parse::<i64>()
                                        .ok()?;
                                    let t = std::str::from_utf8(&cmd.args[1])
                                        .ok()?
                                        .parse::<i64>()
                                        .ok()?;
                                    Some((n, t))
                                })
                                .flatten();
                            let frame = match parsed {
                                None if cmd.args.len() != 2 => RespFrame::error(
                                    "ERR wrong number of arguments for 'wait' command",
                                ),
                                Some((n, t)) if n >= 0 && t >= 0 => {
                                    if !write_buf.is_empty() {
                                        socket.write_all(&write_buf).await?;
                                        write_buf.clear();
                                    }
                                    let got =
                                        crate::replication::wait(&engine, n as usize, t as u64)
                                            .await;
                                    RespFrame::Integer(got as i64)
                                }
                                Some((_, t)) if t < 0 => {
                                    RespFrame::error("ERR timeout is negative")
                                }
                                _ => {
                                    RespFrame::error("ERR value is out of range, must be positive")
                                }
                            };
                            encode_frame(&frame, &mut write_buf);
                        }
                        _ => {
                            // PSYNC replid offset | SYNC: this connection
                            // becomes the replica's link.
                            let psync = if cmd.name == "PSYNC" {
                                if cmd.args.len() != 2 {
                                    encode_frame(
                                        &RespFrame::error(
                                            "ERR wrong number of arguments for 'psync' command",
                                        ),
                                        &mut write_buf,
                                    );
                                    continue;
                                }
                                let id = String::from_utf8_lossy(&cmd.args[0]).to_string();
                                let off = std::str::from_utf8(&cmd.args[1])
                                    .ok()
                                    .and_then(|s| s.parse::<i64>().ok())
                                    .unwrap_or(-1);
                                Some((id, off))
                            } else {
                                None
                            };
                            if !write_buf.is_empty() {
                                socket.write_all(&write_buf).await?;
                                write_buf.clear();
                            }
                            let pending = read_buf.split();
                            crate::replication::serve_replica(
                                &mut socket,
                                pending,
                                engine.clone(),
                                peer_addr,
                                repl_hs.clone(),
                                psync,
                            )
                            .await?;
                            return Ok(());
                        }
                    }
                    continue;
                }

                if !session.in_multi()
                    && matches!(
                        cmd.name.as_str(),
                        "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" | "MONITOR"
                    )
                {
                    if let Some(denied) = engine.acl_gate(&mut session, &cmd.name, &cmd.args) {
                        encode_response(&denied, &mut write_buf);
                        continue;
                    }
                }
                match cmd.name.as_str() {
                    "SSUBSCRIBE" if !session.in_multi() => {
                        // Shard channels live in the slot of their name.
                        if let Some(redirect) = cluster_redirect(
                            &cluster,
                            &engine,
                            &cmd.name,
                            &cmd.args,
                            false,
                            readonly_conn,
                        ) {
                            encode_frame(&redirect, &mut write_buf);
                            continue;
                        }
                        for channel in &cmd.args {
                            let key = solikv_pubsub::broker::shard_channel(channel);
                            if !conn.ssubscriptions.contains(&key) {
                                conn.ssubscriptions.push(key);
                            }
                            encode_frame_p(
                                session.resp,
                                &RespFrame::Array(vec![
                                    RespFrame::BulkString(Bytes::from("ssubscribe")),
                                    RespFrame::BulkString(channel.clone()),
                                    RespFrame::Integer(conn.ssubscriptions.len() as i64),
                                ]),
                                &mut write_buf,
                            );
                        }
                        if !write_buf.is_empty() {
                            socket.write_all(&write_buf).await?;
                            write_buf.clear();
                        }
                        return handle_pubsub_mode(
                            &mut socket,
                            &pubsub,
                            &engine,
                            &mut conn,
                            &mut read_buf,
                            &mut session,
                            &info,
                            &mut push_rx,
                        )
                        .await;
                    }
                    "SUNSUBSCRIBE" if !session.in_multi() => {
                        encode_frame_p(
                            session.resp,
                            &RespFrame::Array(vec![
                                RespFrame::BulkString(Bytes::from("sunsubscribe")),
                                RespFrame::Null,
                                RespFrame::Integer(0),
                            ]),
                            &mut write_buf,
                        );
                        continue;
                    }
                    "MONITOR" if !session.in_multi() => {
                        encode_frame(&RespFrame::ok(), &mut write_buf);
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                        return monitor_loop(&mut socket, &engine, &info, &mut read_buf).await;
                    }
                    "SUBSCRIBE" if !session.in_multi() => {
                        // Enter pubsub mode — send confirmations, then enter delivery loop
                        for channel in &cmd.args {
                            conn.subscriptions.push(channel.clone());
                            let count = conn.subscriptions.len() + conn.psubscriptions.len();
                            encode_frame_p(
                                session.resp,
                                &RespFrame::Array(vec![
                                    RespFrame::BulkString(Bytes::from("subscribe")),
                                    RespFrame::BulkString(channel.clone()),
                                    RespFrame::Integer(count as i64),
                                ]),
                                &mut write_buf,
                            );
                        }

                        // Flush confirmations, then enter pubsub loop
                        if !write_buf.is_empty() {
                            socket.write_all(&write_buf).await?;
                            write_buf.clear();
                        }

                        return handle_pubsub_mode(
                            &mut socket,
                            &pubsub,
                            &engine,
                            &mut conn,
                            &mut read_buf,
                            &mut session,
                            &info,
                            &mut push_rx,
                        )
                        .await;
                    }
                    "PSUBSCRIBE" if !session.in_multi() => {
                        for pattern_bytes in &cmd.args {
                            let pattern =
                                std::str::from_utf8(pattern_bytes).unwrap_or("").to_string();
                            conn.psubscriptions.push(pattern.clone());
                            let count = conn.subscriptions.len() + conn.psubscriptions.len();
                            encode_frame_p(
                                session.resp,
                                &RespFrame::Array(vec![
                                    RespFrame::BulkString(Bytes::from("psubscribe")),
                                    RespFrame::BulkString(pattern_bytes.clone()),
                                    RespFrame::Integer(count as i64),
                                ]),
                                &mut write_buf,
                            );
                        }

                        if !write_buf.is_empty() {
                            socket.write_all(&write_buf).await?;
                            write_buf.clear();
                        }

                        return handle_pubsub_mode(
                            &mut socket,
                            &pubsub,
                            &engine,
                            &mut conn,
                            &mut read_buf,
                            &mut session,
                            &info,
                            &mut push_rx,
                        )
                        .await;
                    }
                    "UNSUBSCRIBE" if !session.in_multi() => {
                        if cmd.args.is_empty() {
                            conn.subscriptions.clear();
                        } else {
                            for ch in &cmd.args {
                                conn.subscriptions.retain(|s| s != ch);
                            }
                        }
                        encode_frame_p(
                            session.resp,
                            &RespFrame::Array(vec![
                                RespFrame::BulkString(Bytes::from("unsubscribe")),
                                RespFrame::Null,
                                RespFrame::Integer(
                                    (conn.subscriptions.len() + conn.psubscriptions.len()) as i64,
                                ),
                            ]),
                            &mut write_buf,
                        );
                        continue;
                    }
                    "PUNSUBSCRIBE" if !session.in_multi() => {
                        if cmd.args.is_empty() {
                            conn.psubscriptions.clear();
                        } else {
                            for pat_bytes in &cmd.args {
                                let pat = std::str::from_utf8(pat_bytes).unwrap_or("").to_string();
                                conn.psubscriptions.retain(|s| s != &pat);
                            }
                        }
                        encode_frame_p(
                            session.resp,
                            &RespFrame::Array(vec![
                                RespFrame::BulkString(Bytes::from("punsubscribe")),
                                RespFrame::Null,
                                RespFrame::Integer(
                                    (conn.subscriptions.len() + conn.psubscriptions.len()) as i64,
                                ),
                            ]),
                            &mut write_buf,
                        );
                        continue;
                    }
                    "QUIT" => {
                        encode_frame(&RespFrame::ok(), &mut write_buf);
                        socket.write_all(&write_buf).await?;
                        return Ok(());
                    }
                    "CLUSTER" => {
                        if let Some(denied) = engine.acl_gate(&mut session, &cmd.name, &cmd.args) {
                            encode_response(&denied, &mut write_buf);
                            continue;
                        }
                        match &cluster {
                            Some(c) => {
                                let reply = c.command(&cmd.args);
                                if session.resp >= 3 {
                                    encode_response3(&reply, &mut write_buf);
                                } else {
                                    encode_response(&reply, &mut write_buf);
                                }
                            }
                            None => encode_frame(
                                &RespFrame::error("ERR This instance has cluster support disabled"),
                                &mut write_buf,
                            ),
                        }
                        continue;
                    }
                    "ASKING" | "READONLY" | "READWRITE" => {
                        if cluster.is_none() {
                            encode_frame(
                                &RespFrame::error("ERR This instance has cluster support disabled"),
                                &mut write_buf,
                            );
                            continue;
                        }
                        match cmd.name.as_str() {
                            "ASKING" => asking = true,
                            "READONLY" => readonly_conn = true,
                            _ => readonly_conn = false,
                        }
                        encode_frame(&RespFrame::ok(), &mut write_buf);
                        continue;
                    }
                    _ => {}
                }

                // Cluster mode has database 0 only, as in Redis.
                if cmd.name == "SELECT"
                    && cluster.is_some()
                    && cmd.args.first().is_some_and(|a| a.as_ref() != b"0")
                {
                    encode_frame(
                        &RespFrame::error("ERR SELECT is not allowed in cluster mode"),
                        &mut write_buf,
                    );
                    continue;
                }
                // Queued inside MULTI (the engine queues), or run now.
                let asked = std::mem::take(&mut asking) || cmd.name == "RESTORE-ASKING";
                if !session.in_multi() {
                    if let Some(redirect) = cluster_redirect(
                        &cluster,
                        &engine,
                        &cmd.name,
                        &cmd.args,
                        asked,
                        readonly_conn,
                    ) {
                        encode_frame(&redirect, &mut write_buf);
                        continue;
                    }
                    if engine.writes_paused() {
                        if !write_buf.is_empty() {
                            socket.write_all(&write_buf).await?;
                            write_buf.clear();
                        }
                        wait_while_paused(&engine, &cmd.name).await;
                    }
                }
                // `MIGRATE` talks to another server: run it here, without
                // holding anything while it waits.
                if cmd.name == "MIGRATE" && !session.in_multi() {
                    if let Some(denied) = engine.acl_gate(&mut session, &cmd.name, &cmd.args) {
                        encode_response(&denied, &mut write_buf);
                        continue;
                    }
                    if !write_buf.is_empty() {
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                    let reply = migrate(&engine, session.db, &cmd.args).await;
                    encode_frame(&reply, &mut write_buf);
                    continue;
                }
                let cmd = if cmd.name == "RESTORE-ASKING" {
                    ParsedCommand {
                        name: solikv_resp::parser::Name::from_static("RESTORE"),
                        ..cmd
                    }
                } else {
                    cmd
                };
                if engine.script_running() {
                    if !write_buf.is_empty() {
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                    if let Some(busy) = engine.script_gate(&cmd.name, &cmd.args).await {
                        encode_response(&busy, &mut write_buf);
                        continue;
                    }
                }
                if !session.in_multi() && engine.is_blocking_form(&cmd.name, &cmd.args) {
                    if let Some(denied) = engine.acl_gate(&mut session, &cmd.name, &cmd.args) {
                        encode_response(&denied, &mut write_buf);
                        continue;
                    }
                }
                let response =
                    if !session.in_multi() && engine.is_blocking_form(&cmd.name, &cmd.args) {
                        match run_blocking(
                            &mut socket,
                            &mut read_buf,
                            &mut write_buf,
                            &engine,
                            &mut session,
                            &info,
                            &cmd.name,
                            &cmd.args,
                            limits.query_buffer_limit,
                        )
                        .await?
                        {
                            Some(r) => r,
                            None => return Ok(()),
                        }
                    } else {
                        engine.execute_session(&mut session, &cmd.name, &cmd.args)
                    };
                if cmd.name == "RESET" {
                    // Back to the default user: with a password, that is
                    // unauthenticated.
                    conn.authenticated = engine.acl().default_auto_auth();
                    session.set_user(engine.acl().default_user(), engine.acl().version());
                }
                if session.reply_allowed() {
                    encode_for(session.resp, &cmd.name, &cmd.args, response, &mut write_buf);
                }
                if write_buf.len() >= FLUSH_AT {
                    socket.write_all(&write_buf).await?;
                    write_buf.clear();
                }
            }
        }

        // ── Phase 4: Single flush for all responses ──
        while let Ok(p) = push_rx.try_recv() {
            encode_push(&p, session.resp, false, &mut write_buf);
        }
        if !write_buf.is_empty() {
            socket.write_all(&write_buf).await?;
            write_buf.clear();
        }
        shrink_after_spike(&mut write_buf);
        if !decoder.in_progress() {
            shrink_after_spike(&mut read_buf);
        }
    }
}

/// What a connection waiting for input got.
enum Waited {
    Read(std::io::Result<usize>),
    Killed,
    Push(Option<solikv_engine::tracking::Push>),
}

/// A tracking invalidation: a RESP3 `invalidate` push, or — for a RESP2
/// client in pub/sub mode (a `REDIRECT` target) — a message on
/// `__redis__:invalidate`. A RESP2 client outside pub/sub mode cannot take
/// out-of-band data; it gets nothing.
fn encode_push(p: &solikv_engine::tracking::Push, proto: u8, pubsub: bool, dst: &mut BytesMut) {
    let solikv_engine::tracking::Push::Invalidate(keys) = p;
    let keys_frame = |dst: &mut BytesMut| match keys {
        None => dst.extend_from_slice(if proto >= 3 { b"_\r\n" } else { b"$-1\r\n" }),
        Some(keys) => {
            array_header(b'*', keys.len(), dst);
            for k in keys {
                encode_bulk(k, dst);
            }
        }
    };
    if proto >= 3 {
        dst.extend_from_slice(b">2\r\n");
        encode_bulk(b"invalidate", dst);
        keys_frame(dst);
    } else if pubsub {
        dst.extend_from_slice(b"*3\r\n");
        encode_bulk(b"message", dst);
        encode_bulk(b"__redis__:invalidate", dst);
        keys_frame(dst);
    }
}

/// `MONITOR`: every command the server runs, until the client leaves.
async fn monitor_loop<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    engine: &Arc<CommandEngine>,
    info: &Arc<solikv_engine::session::ClientInfo>,
    read_buf: &mut BytesMut,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut feed = engine.stats().subscribe_monitor();
    loop {
        tokio::select! {
            line = feed.rx.recv() => match line {
                Ok(line) => socket.write_all(&line).await?,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => return Ok(()),
            },
            n = socket.read_buf(read_buf) => {
                if n? == 0 {
                    return Ok(());
                }
                // Only QUIT/RESET mean anything here.
                while let Ok(Some((frame, consumed))) = decode_frame(read_buf) {
                    read_buf.advance(consumed);
                    if let Ok(cmd) = ParsedCommand::from_frame(frame) {
                        if cmd.name == "QUIT" || cmd.name == "RESET" {
                            let _ = socket.write_all(b"+OK\r\n").await;
                            return Ok(());
                        }
                    }
                }
                if read_buf.len() > MAX_PUBSUB_READ_BUF {
                    return Ok(());
                }
            }
            _ = info.killed() => return Ok(()),
        }
    }
}

/// `AUTH [username] password` against the ACL users.
fn acl_auth(
    engine: &CommandEngine,
    args: &[Bytes],
) -> Result<Arc<solikv_engine::acl::AclUser>, AuthError> {
    let (user, pass) = match args {
        [pass] => (None, pass),
        [user, pass] => (Some(user.as_ref()), pass),
        _ => return Err(AuthError::Arity),
    };
    engine.acl().authenticate(user, pass).map_err(|e| match e {
        solikv_engine::acl::AuthFailure::NoPassword => AuthError::NoPassword,
        solikv_engine::acl::AuthFailure::Wrong => AuthError::Wrong,
    })
}

/// Replies are written out once this many bytes are pending.
const FLUSH_AT: usize = 1024 * 1024;

/// Run a command that may block (`BLPOP`, `XREAD … BLOCK`, …). While the
/// client waits, replies already produced go out, and the socket is still
/// read: bytes the client pipelines behind the blocking command are kept for
/// later, and a client that hangs up (or is `CLIENT KILL`ed) stops waiting
/// and is unregistered. `Ok(None)`: the connection is done.
#[allow(clippy::too_many_arguments)]
async fn run_blocking<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    engine: &CommandEngine,
    session: &mut solikv_engine::Session,
    info: &solikv_engine::session::ClientInfo,
    name: &str,
    args: &[Bytes],
    query_buffer_limit: usize,
) -> std::io::Result<Option<CommandResponse>> {
    let blocked = match engine.block_start(session, name, args) {
        solikv_engine::BlockStart::Done(r) => return Ok(Some(r)),
        solikv_engine::BlockStart::Wait(b) => b,
    };
    if !write_buf.is_empty() {
        socket.write_all(write_buf).await?;
        write_buf.clear();
    }
    loop {
        tokio::select! {
            r = blocked.wait() => return Ok(Some(r)),
            n = socket.read_buf(read_buf) => {
                let n = n?;
                if n == 0 || read_buf.len() > query_buffer_limit {
                    return Ok(None);
                }
            }
            _ = info.killed() => return Ok(None),
        }
    }
}

/// Buffers start at, and shrink back to, this size.
const BUF_SIZE: usize = 64 * 1024;

/// Name → static spelling, and whether the arguments may borrow the read
/// buffer: only for read-only commands, whose arguments are gone once they
/// ran (a stored key or value must not pin the buffer it arrived in).
fn resolve_command(name: &[u8]) -> Option<Resolved> {
    solikv_engine::commands::lookup_ci(name).map(|s| Resolved {
        name: s.name,
        zero_copy: s.has(solikv_engine::commands::F_READONLY),
    })
}

/// Commands the connection loop handles itself (the slow path).
fn needs_connection_state(name: &str) -> bool {
    matches!(
        name,
        "MULTI"
            | "SUBSCRIBE"
            | "UNSUBSCRIBE"
            | "PSUBSCRIBE"
            | "PUNSUBSCRIBE"
            | "QUIT"
            | "AUTH"
            | "HELLO"
            | "CLUSTER"
            | "SELECT"
            | "RESET"
            | "MONITOR"
            | "SSUBSCRIBE"
            | "SUNSUBSCRIBE"
            | "REPLCONF"
            | "PSYNC"
            | "SYNC"
            | "WAIT"
            | "ASKING"
            | "READONLY"
            | "READWRITE"
            | "MIGRATE"
            | "RESTORE-ASKING"
    )
}

/// Make room before a read: at least what the command being assembled
/// still needs, and otherwise grow geometrically. (`read_buf`'s own growth
/// could be as small as 64 bytes a read, a copy of the whole buffer each
/// time a big command came in.)
fn prepare_read_buf(buf: &mut BytesMut, missing: usize) {
    let spare = buf.capacity() - buf.len();
    let want = missing.max(if spare < 16 * 1024 {
        BUF_SIZE.max(buf.len())
    } else {
        0
    });
    if want > spare {
        buf.reserve(want);
    }
}

/// Give back the memory a burst (a huge command or reply) left behind.
fn shrink_after_spike(buf: &mut BytesMut) {
    if buf.is_empty() && buf.capacity() > 16 * BUF_SIZE {
        *buf = BytesMut::with_capacity(BUF_SIZE);
    }
}

/// Encode a reply straight into the output buffer (no intermediate frame
/// tree), in RESP2. RESP3-typed replies are flattened the way Redis shows
/// them to a RESP2 client.
pub fn encode_response(resp: &CommandResponse, dst: &mut BytesMut) {
    match resp {
        CommandResponse::Ok => dst.extend_from_slice(b"+OK\r\n"),
        CommandResponse::Nil => dst.extend_from_slice(b"$-1\r\n"),
        CommandResponse::NilArray => dst.extend_from_slice(b"*-1\r\n"),
        CommandResponse::Integer(n) => {
            dst.extend_from_slice(b":");
            write_decimal(dst, *n);
            dst.extend_from_slice(b"\r\n");
        }
        CommandResponse::BulkString(b) => encode_bulk(b, dst),
        CommandResponse::SimpleString(s) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(s);
            dst.extend_from_slice(b"\r\n");
        }
        CommandResponse::Error(msg) => encode_error(msg, dst),
        CommandResponse::Array(items)
        | CommandResponse::Set(items)
        | CommandResponse::Push(items) => {
            array_header(b'*', items.len(), dst);
            for item in items {
                encode_response(item, dst);
            }
        }
        CommandResponse::Queued => dst.extend_from_slice(b"+QUEUED\r\n"),
        CommandResponse::Double(d) => encode_bulk(solikv_core::format_double(*d).as_bytes(), dst),
        CommandResponse::Bool(b) => dst.extend_from_slice(if *b { b":1\r\n" } else { b":0\r\n" }),
        CommandResponse::Map(pairs) | CommandResponse::Pairs(pairs) => {
            array_header(b'*', pairs.len() * 2, dst);
            for (k, v) in pairs {
                encode_response(k, dst);
                encode_response(v, dst);
            }
        }
        CommandResponse::Verbatim(_, b) => encode_bulk(b, dst),
    }
}

/// The same reply in RESP3 (`HELLO 3`): nulls are `_`, and maps, sets,
/// doubles, booleans and pushes have their own types.
pub fn encode_response3(resp: &CommandResponse, dst: &mut BytesMut) {
    match resp {
        CommandResponse::Nil | CommandResponse::NilArray => dst.extend_from_slice(b"_\r\n"),
        CommandResponse::Array(items) => {
            array_header(b'*', items.len(), dst);
            for item in items {
                encode_response3(item, dst);
            }
        }
        CommandResponse::Set(items) => {
            array_header(b'~', items.len(), dst);
            for item in items {
                encode_response3(item, dst);
            }
        }
        CommandResponse::Push(items) => {
            array_header(b'>', items.len(), dst);
            for item in items {
                encode_response3(item, dst);
            }
        }
        CommandResponse::Double(d) => {
            dst.extend_from_slice(b",");
            dst.extend_from_slice(solikv_core::format_double(*d).as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        CommandResponse::Bool(b) => dst.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" }),
        CommandResponse::Map(pairs) => {
            array_header(b'%', pairs.len(), dst);
            for (k, v) in pairs {
                encode_response3(k, dst);
                encode_response3(v, dst);
            }
        }
        CommandResponse::Pairs(pairs) => {
            array_header(b'*', pairs.len(), dst);
            for (a, b) in pairs {
                dst.extend_from_slice(b"*2\r\n");
                encode_response3(a, dst);
                encode_response3(b, dst);
            }
        }
        CommandResponse::Verbatim(fmt, b) => {
            dst.extend_from_slice(b"=");
            write_decimal(dst, (b.len() + 4) as i64);
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(fmt.as_bytes());
            dst.extend_from_slice(b":");
            dst.extend_from_slice(b);
            dst.extend_from_slice(b"\r\n");
        }
        other => encode_response(other, dst),
    }
}

/// Encode `resp`, the reply to `name args…`, for a client speaking `proto`.
#[inline]
pub fn encode_for(
    proto: u8,
    name: &str,
    args: &[Bytes],
    resp: CommandResponse,
    dst: &mut BytesMut,
) {
    if proto >= 3 {
        encode_response3(&solikv_engine::resp3::shape(name, args, resp), dst);
    } else {
        encode_response(&resp, dst);
    }
}

fn array_header(kind: u8, n: usize, dst: &mut BytesMut) {
    dst.extend_from_slice(&[kind]);
    write_decimal(dst, n as i64);
    dst.extend_from_slice(b"\r\n");
}

fn encode_bulk(b: &[u8], dst: &mut BytesMut) {
    dst.reserve(b.len() + 16);
    dst.extend_from_slice(b"$");
    write_decimal(dst, b.len() as i64);
    dst.extend_from_slice(b"\r\n");
    dst.extend_from_slice(b);
    dst.extend_from_slice(b"\r\n");
}

fn encode_error(msg: &str, dst: &mut BytesMut) {
    dst.extend_from_slice(b"-");
    dst.extend_from_slice(msg.as_bytes());
    dst.extend_from_slice(b"\r\n");
}

fn write_decimal(dst: &mut BytesMut, n: i64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let neg = n < 0;
    let mut v = n.unsigned_abs();
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    if neg {
        dst.extend_from_slice(b"-");
    }
    dst.extend_from_slice(&buf[i..]);
}

/// Most bytes buffered from a client in pub/sub mode, where only short
/// (P)SUBSCRIBE / PING / QUIT commands are valid.
const MAX_PUBSUB_READ_BUF: usize = 1024 * 1024;

/// Pub/sub delivery loop. Entered when client issues SUBSCRIBE or PSUBSCRIBE.
/// Uses a fan-in mpsc channel: each subscription spawns a forwarding task from
/// broadcast::Receiver -> mpsc::UnboundedSender. The main loop select!s between
/// the fan-in receiver and new frames from the socket.
#[allow(clippy::too_many_arguments)]
async fn handle_pubsub_mode<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    pubsub: &Arc<PubSubBroker>,
    engine: &Arc<CommandEngine>,
    conn: &mut ClientConnection,
    read_buf: &mut BytesMut,
    session: &mut solikv_engine::Session,
    info: &Arc<solikv_engine::session::ClientInfo>,
    push_rx: &mut mpsc::UnboundedReceiver<solikv_engine::tracking::Push>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let proto = session.resp;
    // `CLIENT LIST` shows the subscription counts; zeroed when this returns.
    struct SubCounts<'a>(&'a solikv_engine::session::ClientInfo);
    impl Drop for SubCounts<'_> {
        fn drop(&mut self) {
            self.0.subs.store(0, std::sync::atomic::Ordering::Relaxed);
            self.0.psubs.store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let _counts = SubCounts(info);
    let publish_counts = |conn: &ClientConnection| {
        info.subs.store(
            conn.subscriptions.len() as u32,
            std::sync::atomic::Ordering::Relaxed,
        );
        info.psubs.store(
            conn.psubscriptions.len() as u32,
            std::sync::atomic::Ordering::Relaxed,
        );
    };
    publish_counts(conn);
    let (fan_tx, mut fan_rx) = mpsc::unbounded_channel::<PubSubMessage>();
    let mut write_buf = BytesMut::with_capacity(65536);
    // Output-buffer accounting shared with the forwarding tasks (Redis'
    // `client-output-buffer-limit pubsub 32mb 8mb 60`).
    let outbuf = Arc::new(OutBuf::default());
    let mut soft_since: Option<tokio::time::Instant> = None;

    // Forwarding tasks: channel_name -> JoinHandle
    let mut channel_tasks: HashMap<Bytes, JoinHandle<()>> = HashMap::new();
    let mut pattern_tasks: HashMap<String, JoinHandle<()>> = HashMap::new();

    // Set up forwarding tasks for initial subscriptions
    for ch in &conn.subscriptions {
        let rx = pubsub.subscribe(ch.clone());
        let tx = fan_tx.clone();
        let handle = tokio::spawn(forward_broadcast(rx, tx, outbuf.clone()));
        channel_tasks.insert(ch.clone(), handle);
    }
    for pat in &conn.psubscriptions {
        let rx = pubsub.psubscribe(pat.clone());
        let tx = fan_tx.clone();
        let handle = tokio::spawn(forward_broadcast(rx, tx, outbuf.clone()));
        pattern_tasks.insert(pat.clone(), handle);
    }
    for ch in &conn.ssubscriptions {
        let rx = pubsub.subscribe(ch.clone());
        let tx = fan_tx.clone();
        let handle = tokio::spawn(forward_broadcast(rx, tx, outbuf.clone()));
        channel_tasks.insert(ch.clone(), handle);
    }

    loop {
        tokio::select! {
            // Ended by `CLIENT KILL`.
            _ = info.killed() => {
                for (_, h) in channel_tasks.drain() { h.abort(); }
                for (_, h) in pattern_tasks.drain() { h.abort(); }
                return Ok(());
            }
            // A forwarder found this client too slow (output buffer over
            // the hard limit, or messages already lost): disconnect it, as
            // Redis does, rather than buffer without bound or drop silently.
            _ = outbuf.overflow.notified() => {
                tracing::warn!("{}: pub/sub client closed for overcoming the output buffer limit", info.addr);
                for (_, h) in channel_tasks.drain() { h.abort(); }
                for (_, h) in pattern_tasks.drain() { h.abort(); }
                return Ok(());
            }
            // Branch 1: incoming message from any subscription
            msg = fan_rx.recv() => {
                let Some(msg) = msg else { break; };
                // Everything already queued goes out in the same write.
                let mut drained = msg.size();
                encode_pubsub_message(&msg, proto, &mut write_buf);
                while write_buf.len() < BUF_SIZE {
                    match fan_rx.try_recv() {
                        Ok(m) => {
                            drained += m.size();
                            encode_pubsub_message(&m, proto, &mut write_buf);
                        }
                        Err(_) => break,
                    }
                }
                // A subscriber that stopped reading blocks this write; the
                // forwarders then hit the hard limit, and that must still
                // close the connection.
                tokio::select! {
                    r = socket.write_all(&write_buf) => r?,
                    _ = outbuf.overflow.notified() => {
                        tracing::warn!("{}: pub/sub client closed for overcoming the output buffer limit", info.addr);
                        for (_, h) in channel_tasks.drain() { h.abort(); }
                        for (_, h) in pattern_tasks.drain() { h.abort(); }
                        return Ok(());
                    }
                }
                write_buf.clear();
                let left = outbuf.pending.fetch_sub(drained, std::sync::atomic::Ordering::AcqRel) - drained;
                if left > PUBSUB_SOFT_LIMIT {
                    let since = *soft_since.get_or_insert_with(tokio::time::Instant::now);
                    if since.elapsed() >= PUBSUB_SOFT_SECONDS {
                        tracing::warn!("{}: pub/sub client closed for staying over the soft output buffer limit", info.addr);
                        for (_, h) in channel_tasks.drain() { h.abort(); }
                        for (_, h) in pattern_tasks.drain() { h.abort(); }
                        return Ok(());
                    }
                } else {
                    soft_since = None;
                }
                shrink_after_spike(&mut write_buf);
            }
            // Tracking invalidations (this client may be a REDIRECT target).
            p = push_rx.recv() => {
                if let Some(p) = p {
                    encode_push(&p, proto, true, &mut write_buf);
                    while let Ok(p) = push_rx.try_recv() {
                        encode_push(&p, proto, true, &mut write_buf);
                    }
                    socket.write_all(&write_buf).await?;
                    write_buf.clear();
                }
            }
            // Branch 2: new data from the client socket
            n = socket.read_buf(read_buf) => {
                let n = n?;
                // Frames are decoded below as they complete, so a full buffer
                // here is one frame larger than the limit.
                if read_buf.len() > MAX_PUBSUB_READ_BUF {
                    return Err("pub/sub read buffer exceeded maximum size".into());
                }
                if n == 0 {
                    // Client disconnected — abort all forwarding tasks
                    for (_, h) in channel_tasks.drain() { h.abort(); }
                    for (_, h) in pattern_tasks.drain() { h.abort(); }
                    return Ok(());
                }

                // Decode all complete frames
                loop {
                    match decode_frame(read_buf) {
                        Ok(Some((frame, consumed))) => {
                            read_buf.advance(consumed);
                            let cmd = match ParsedCommand::from_frame(frame) {
                                Ok(cmd) => cmd,
                                Err(e) => {
                                    encode_frame(&RespFrame::error(e), &mut write_buf);
                                    continue;
                                }
                            };

                            match cmd.name.as_str() {
                                "SUBSCRIBE" => {
                                    if let Some(denied) = engine.acl_gate(session, "SUBSCRIBE", &cmd.args) {
                                        encode_response(&denied, &mut write_buf);
                                        continue;
                                    }
                                    for channel in &cmd.args {
                                        if !conn.subscriptions.contains(channel) {
                                            conn.subscriptions.push(channel.clone());
                                            let rx = pubsub.subscribe(channel.clone());
                                            let tx = fan_tx.clone();
                                            let handle = tokio::spawn(forward_broadcast(rx, tx, outbuf.clone()));
                                            channel_tasks.insert(channel.clone(), handle);
                                        }
                                        let count = conn.subscriptions.len() + conn.psubscriptions.len();
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("subscribe")),
                                                RespFrame::BulkString(channel.clone()),
                                                RespFrame::Integer(count as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                }
                                "UNSUBSCRIBE" => {
                                    let channels_to_remove = if cmd.args.is_empty() {
                                        std::mem::take(&mut conn.subscriptions)
                                    } else {
                                        let mut removed = Vec::new();
                                        for ch in &cmd.args {
                                            if conn.subscriptions.contains(ch) {
                                                conn.subscriptions.retain(|s| s != ch);
                                                removed.push(ch.clone());
                                            }
                                        }
                                        removed
                                    };
                                    for ch in &channels_to_remove {
                                        if let Some(h) = channel_tasks.remove(ch) {
                                            h.abort();
                                        }
                                    }
                                    let count = conn.subscriptions.len() + conn.psubscriptions.len();
                                    for ch in &channels_to_remove {
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("unsubscribe")),
                                                RespFrame::BulkString(ch.clone()),
                                                RespFrame::Integer(count as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                    if channels_to_remove.is_empty() {
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("unsubscribe")),
                                                RespFrame::Null,
                                                RespFrame::Integer(count as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                    // Exit pubsub mode if no subscriptions remain
                                    if !conn.is_subscribed() {
                                        if !write_buf.is_empty() {
                                            socket.write_all(&write_buf).await?;
                                            write_buf.clear();
                                        }
                                        return Ok(());
                                    }
                                }
                                "SSUBSCRIBE" => {
                                    if let Some(denied) = engine.acl_gate(session, "SSUBSCRIBE", &cmd.args) {
                                        encode_response(&denied, &mut write_buf);
                                        continue;
                                    }
                                    for channel in &cmd.args {
                                        let key = solikv_pubsub::broker::shard_channel(channel);
                                        if !conn.ssubscriptions.contains(&key) {
                                            conn.ssubscriptions.push(key.clone());
                                            let rx = pubsub.subscribe(key.clone());
                                            let tx = fan_tx.clone();
                                            let handle = tokio::spawn(forward_broadcast(rx, tx, outbuf.clone()));
                                            channel_tasks.insert(key, handle);
                                        }
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("ssubscribe")),
                                                RespFrame::BulkString(channel.clone()),
                                                RespFrame::Integer(conn.ssubscriptions.len() as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                }
                                "SUNSUBSCRIBE" => {
                                    let removed: Vec<Bytes> = if cmd.args.is_empty() {
                                        std::mem::take(&mut conn.ssubscriptions)
                                    } else {
                                        let mut removed = Vec::new();
                                        for ch in &cmd.args {
                                            let key = solikv_pubsub::broker::shard_channel(ch);
                                            if conn.ssubscriptions.contains(&key) {
                                                conn.ssubscriptions.retain(|s| s != &key);
                                                removed.push(key);
                                            }
                                        }
                                        removed
                                    };
                                    for key in &removed {
                                        if let Some(h) = channel_tasks.remove(key) {
                                            h.abort();
                                        }
                                    }
                                    let n = conn.ssubscriptions.len() as i64;
                                    if removed.is_empty() {
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("sunsubscribe")),
                                                RespFrame::Null,
                                                RespFrame::Integer(n),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                    for key in &removed {
                                        let (name, _) = solikv_pubsub::broker::split_shard(key);
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("sunsubscribe")),
                                                RespFrame::BulkString(name),
                                                RespFrame::Integer(n),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                    if !conn.is_subscribed() {
                                        if !write_buf.is_empty() {
                                            socket.write_all(&write_buf).await?;
                                            write_buf.clear();
                                        }
                                        return Ok(());
                                    }
                                }
                                "PSUBSCRIBE" => {
                                    if let Some(denied) = engine.acl_gate(session, "PSUBSCRIBE", &cmd.args) {
                                        encode_response(&denied, &mut write_buf);
                                        continue;
                                    }
                                    for pat_bytes in &cmd.args {
                                        let pat = std::str::from_utf8(pat_bytes).unwrap_or("").to_string();
                                        if !conn.psubscriptions.contains(&pat) {
                                            conn.psubscriptions.push(pat.clone());
                                            let rx = pubsub.psubscribe(pat.clone());
                                            let tx = fan_tx.clone();
                                            let handle = tokio::spawn(forward_broadcast(rx, tx, outbuf.clone()));
                                            pattern_tasks.insert(pat.clone(), handle);
                                        }
                                        let count = conn.subscriptions.len() + conn.psubscriptions.len();
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("psubscribe")),
                                                RespFrame::BulkString(pat_bytes.clone()),
                                                RespFrame::Integer(count as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                }
                                "PUNSUBSCRIBE" => {
                                    let pats_to_remove = if cmd.args.is_empty() {
                                        std::mem::take(&mut conn.psubscriptions)
                                    } else {
                                        let mut removed = Vec::new();
                                        for pat_bytes in &cmd.args {
                                            let pat = std::str::from_utf8(pat_bytes).unwrap_or("").to_string();
                                            if conn.psubscriptions.contains(&pat) {
                                                conn.psubscriptions.retain(|s| s != &pat);
                                                removed.push(pat);
                                            }
                                        }
                                        removed
                                    };
                                    for pat in &pats_to_remove {
                                        if let Some(h) = pattern_tasks.remove(pat) {
                                            h.abort();
                                        }
                                    }
                                    let count = conn.subscriptions.len() + conn.psubscriptions.len();
                                    for pat in &pats_to_remove {
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("punsubscribe")),
                                                RespFrame::BulkString(Bytes::from(pat.clone())),
                                                RespFrame::Integer(count as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                    if pats_to_remove.is_empty() {
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("punsubscribe")),
                                                RespFrame::Null,
                                                RespFrame::Integer(count as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                    if !conn.is_subscribed() {
                                        if !write_buf.is_empty() {
                                            socket.write_all(&write_buf).await?;
                                            write_buf.clear();
                                        }
                                        return Ok(());
                                    }
                                }
                                "PING" if proto >= 3 => {
                                    let r = engine.execute_session(session, "PING", &cmd.args);
                                    encode_response3(&r, &mut write_buf);
                                }
                                "PING" => {
                                    if cmd.args.is_empty() {
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("pong")),
                                                RespFrame::BulkString(Bytes::from("")),
                                            ]),
                                            &mut write_buf,
                                        );
                                    } else {
                                        encode_frame_p(
                                            proto,
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("pong")),
                                                RespFrame::BulkString(cmd.args[0].clone()),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                }
                                "RESET" | "QUIT" => {
                                    // Exit pubsub mode entirely
                                    for (_, h) in channel_tasks.drain() { h.abort(); }
                                    for (_, h) in pattern_tasks.drain() { h.abort(); }
                                    conn.subscriptions.clear();
                                    conn.psubscriptions.clear();
                                    conn.ssubscriptions.clear();
                                    if cmd.name == "QUIT" {
                                        encode_frame(&RespFrame::ok(), &mut write_buf);
                                        if !write_buf.is_empty() {
                                            socket.write_all(&write_buf).await?;
                                        }
                                        return Ok(());
                                    }
                                    encode_frame(&RespFrame::SimpleString(Bytes::from("RESET")), &mut write_buf);
                                    if !write_buf.is_empty() {
                                        socket.write_all(&write_buf).await?;
                                        write_buf.clear();
                                    }
                                    return Ok(());
                                }
                                // RESP3 lets a subscribed client run any command.
                                _ if proto >= 3 => {
                                    let r = engine.execute_session(session, &cmd.name, &cmd.args);
                                    if session.reply_allowed() {
                                        encode_for(proto, &cmd.name, &cmd.args, r, &mut write_buf);
                                    }
                                }
                                _ => {
                                    encode_frame(
                                        &RespFrame::error(format!(
                                            "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                                            cmd.name.to_lowercase()
                                        )),
                                        &mut write_buf,
                                    );
                                }
                            }
                        }
                        Ok(None) => break, // need more data
                        Err(e) => {
                            encode_frame(&RespFrame::error(e), &mut write_buf);
                            break;
                        }
                    }
                }
                publish_counts(conn);

                if !write_buf.is_empty() {
                    socket.write_all(&write_buf).await?;
                    write_buf.clear();
                }
            }
        }
    }

    // Clean up
    for (_, h) in channel_tasks {
        h.abort();
    }
    for (_, h) in pattern_tasks {
        h.abort();
    }
    Ok(())
}

/// Forwarding task: reads from a broadcast receiver and sends to the fan-in mpsc channel.
/// Redis' `client-output-buffer-limit pubsub` defaults: a subscriber
/// with more than 32 MB queued, or more than 8 MB for 60 s, is disconnected.
const PUBSUB_HARD_LIMIT: usize = 32 * 1024 * 1024;
const PUBSUB_SOFT_LIMIT: usize = 8 * 1024 * 1024;
const PUBSUB_SOFT_SECONDS: std::time::Duration = std::time::Duration::from_secs(60);

/// What a subscriber has queued and not yet written.
#[derive(Default)]
struct OutBuf {
    pending: std::sync::atomic::AtomicUsize,
    overflow: tokio::sync::Notify,
}

async fn forward_broadcast(
    mut rx: tokio::sync::broadcast::Receiver<PubSubMessage>,
    tx: mpsc::UnboundedSender<PubSubMessage>,
    outbuf: Arc<OutBuf>,
) {
    loop {
        match rx.recv().await {
            Ok(msg) => {
                let size = msg.size();
                let queued = outbuf
                    .pending
                    .fetch_add(size, std::sync::atomic::Ordering::AcqRel)
                    + size;
                if queued > PUBSUB_HARD_LIMIT {
                    outbuf.overflow.notify_one();
                    return;
                }
                if tx.send(msg).is_err() {
                    return; // fan-in channel closed
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                // Messages were lost for this subscriber: it could not keep
                // up. Redis never drops a message; it disconnects the client.
                tracing::debug!("pubsub subscriber lagged by {} messages", n);
                outbuf.overflow.notify_one();
                return;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return; // channel closed
            }
        }
    }
}

/// A frame as a RESP3 push when the client speaks RESP3 (pub/sub replies
/// are pushes there), otherwise as is.
fn encode_frame_p(proto: u8, frame: &RespFrame, dst: &mut BytesMut) {
    match frame {
        RespFrame::Array(items) if proto >= 3 => {
            array_header(b'>', items.len(), dst);
            for item in items {
                match item {
                    RespFrame::Null => dst.extend_from_slice(b"_\r\n"),
                    other => encode_frame(other, dst),
                }
            }
        }
        other => encode_frame(other, dst),
    }
}

/// `message` / `pmessage` push, encoded directly.
fn encode_pubsub_message(msg: &PubSubMessage, proto: u8, dst: &mut BytesMut) {
    fn bulk(dst: &mut BytesMut, b: &[u8]) {
        dst.extend_from_slice(b"$");
        write_decimal(dst, b.len() as i64);
        dst.extend_from_slice(b"\r\n");
        dst.extend_from_slice(b);
        dst.extend_from_slice(b"\r\n");
    }
    match &msg.pattern {
        Some(p) => {
            dst.extend_from_slice(if proto >= 3 { b">4\r\n" } else { b"*4\r\n" });
            bulk(dst, b"pmessage");
            bulk(dst, p);
        }
        None => {
            dst.extend_from_slice(if proto >= 3 { b">3\r\n" } else { b"*3\r\n" });
            let (channel, shard) = solikv_pubsub::broker::split_shard(&msg.channel);
            bulk(dst, if shard { b"smessage" } else { b"message" });
            bulk(dst, &channel);
            bulk(dst, &msg.message);
            return;
        }
    }
    bulk(dst, &msg.channel);
    bulk(dst, &msg.message);
}

/// Cluster mode: where a command's keys are served, as a redirection to
/// answer instead of running it — or `None` to run it here.
///
/// Redis Cluster's rules: every key of a command must hash to one slot
/// (`-CROSSSLOT`); a slot served elsewhere is `-MOVED` (a `READONLY` client
/// of a replica may read its master's slots); a slot migrating away is
/// `-ASK` for keys no longer here, `-TRYAGAIN` when a multi-key command
/// finds some and not others; an importing slot is served after `ASKING`.
fn cluster_redirect(
    cluster: &Option<Arc<ClusterManager>>,
    engine: &CommandEngine,
    name: &str,
    args: &[bytes::Bytes],
    asking: bool,
    readonly: bool,
) -> Option<RespFrame> {
    let cluster = cluster.as_ref()?;
    let spec = solikv_engine::commands::lookup(name)?;
    let keys: smallkeys::Keys<'_> = match spec.keys {
        solikv_engine::commands::Keys::None => return None,
        solikv_engine::commands::Keys::Range {
            first,
            last,
            step: _,
        } if first == last && first > 0 => {
            smallkeys::Keys::One(args.get(first as usize - 1)?.as_ref())
        }
        _ => smallkeys::Keys::Many(
            spec.key_positions(args)
                .into_iter()
                .filter_map(|i| args.get(i).map(|k| k.as_ref()))
                .collect(),
        ),
    };
    let first = keys.first()?;
    let slot = cluster.key_slot(first);
    if let smallkeys::Keys::Many(v) = &keys {
        if v.iter().any(|k| cluster.key_slot(k) != slot) {
            return Some(RespFrame::Error(
                "CROSSSLOT Keys in request don't hash to the same slot".into(),
            ));
        }
    }
    let moved = |cluster: &ClusterManager| -> RespFrame {
        match cluster.slot_owner_addr(slot) {
            Some((ip, port)) => RespFrame::Error(format!("MOVED {} {}:{}", slot, ip, port)),
            None => RespFrame::Error(format!("CLUSTERDOWN Hash slot {} is not served", slot)),
        }
    };
    let present =
        |keys: &smallkeys::Keys<'_>| keys.iter().filter(|k| engine.key_exists(0, k)).count();
    match cluster.slot_route(slot) {
        solikv_cluster::SlotRoute::Local => None,
        solikv_cluster::SlotRoute::Down => {
            Some(RespFrame::Error("CLUSTERDOWN The cluster is down".into()))
        }
        solikv_cluster::SlotRoute::Moved => {
            if readonly && !spec.is_write() && cluster.master_serves(slot) {
                return None;
            }
            Some(moved(cluster))
        }
        solikv_cluster::SlotRoute::Special => {
            if cluster.slot_owner(slot).as_deref() == Some(cluster.node_id().as_str()) {
                // Migrating away: what is still here is served here.
                let (ip, port) = cluster.migrating_to(slot)?;
                let here = present(&keys);
                if here == keys.len() {
                    None
                } else if here == 0 {
                    Some(RespFrame::Error(format!("ASK {} {}:{}", slot, ip, port)))
                } else {
                    Some(RespFrame::Error(
                        "TRYAGAIN Multiple keys request during rehashing of slot".into(),
                    ))
                }
            } else if asking && cluster.is_importing(slot) {
                if keys.len() > 1 && present(&keys) != keys.len() {
                    return Some(RespFrame::Error(
                        "TRYAGAIN Multiple keys request during rehashing of slot".into(),
                    ));
                }
                None
            } else if readonly && !spec.is_write() && cluster.master_serves(slot) {
                None
            } else {
                Some(moved(cluster))
            }
        }
    }
}

/// The keys of one command, without a heap allocation for the common case.
mod smallkeys {
    pub enum Keys<'a> {
        One(&'a [u8]),
        Many(Vec<&'a [u8]>),
    }

    impl<'a> Keys<'a> {
        pub fn first(&self) -> Option<&'a [u8]> {
            match self {
                Keys::One(k) => Some(k),
                Keys::Many(v) => v.first().copied(),
            }
        }

        pub fn len(&self) -> usize {
            match self {
                Keys::One(_) => 1,
                Keys::Many(v) => v.len(),
            }
        }

        pub fn iter(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
            let (one, many): (Option<&'a [u8]>, &[&'a [u8]]) = match self {
                Keys::One(k) => (Some(k), &[]),
                Keys::Many(v) => (None, v.as_slice()),
            };
            one.into_iter().chain(many.iter().copied())
        }
    }
}

/// A write arriving while a manual failover pauses clients waits for the
/// pause to end (reads go on).
async fn wait_while_paused(engine: &CommandEngine, name: &str) {
    let write = solikv_engine::commands::lookup(name).is_some_and(|s| s.is_write());
    if !write {
        return;
    }
    while engine.writes_paused() {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// `MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE]
/// [AUTH password | AUTH2 username password] [KEYS key [key ...]]`: move keys
/// to another server with `DUMP` payloads and `RESTORE-ASKING` (which a
/// Redis or SoliKV target accepts while it imports the slot), then delete
/// them here unless `COPY`.
async fn migrate(engine: &Arc<CommandEngine>, db: u32, args: &[Bytes]) -> RespFrame {
    if args.len() < 5 {
        return RespFrame::error("ERR wrong number of arguments for 'migrate' command");
    }
    let host = String::from_utf8_lossy(&args[0]).to_string();
    let Some(port) = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
    else {
        return RespFrame::error("ERR value is not an integer or out of range");
    };
    let Some(dest_db) = std::str::from_utf8(&args[3])
        .ok()
        .and_then(|p| p.parse::<u32>().ok())
    else {
        return RespFrame::error("ERR value is not an integer or out of range");
    };
    let Some(timeout) = std::str::from_utf8(&args[4])
        .ok()
        .and_then(|p| p.parse::<i64>().ok())
    else {
        return RespFrame::error("ERR value is not an integer or out of range");
    };
    let timeout =
        std::time::Duration::from_millis(if timeout <= 0 { 1000 } else { timeout as u64 });
    let (mut copy, mut replace) = (false, false);
    let mut auth: Option<(Option<Bytes>, Bytes)> = None;
    let mut keys: Vec<Bytes> = Vec::new();
    let mut i = 5;
    while i < args.len() {
        let a = &args[i];
        if a.eq_ignore_ascii_case(b"COPY") {
            copy = true;
        } else if a.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
        } else if a.eq_ignore_ascii_case(b"AUTH") && i + 1 < args.len() {
            auth = Some((None, args[i + 1].clone()));
            i += 1;
        } else if a.eq_ignore_ascii_case(b"AUTH2") && i + 2 < args.len() {
            auth = Some((Some(args[i + 1].clone()), args[i + 2].clone()));
            i += 2;
        } else if a.eq_ignore_ascii_case(b"KEYS") {
            if !args[2].is_empty() {
                return RespFrame::error(
                    "ERR When using MIGRATE KEYS option, the key argument must be set to the empty string",
                );
            }
            keys.extend(args[i + 1..].iter().cloned());
            break;
        } else {
            return RespFrame::error("ERR syntax error");
        }
        i += 1;
    }
    if !args[2].is_empty() {
        keys.push(args[2].clone());
    }
    // What to send: the keys that exist, with their TTLs.
    let mut items: Vec<(Bytes, i64, Bytes)> = Vec::new();
    for k in &keys {
        let dump = engine.execute_in_db(db, "DUMP", std::slice::from_ref(k));
        let CommandResponse::BulkString(payload) = dump else {
            continue;
        };
        let ttl = match engine.execute_in_db(db, "PTTL", std::slice::from_ref(k)) {
            CommandResponse::Integer(t) if t > 0 => t,
            _ => 0,
        };
        items.push((k.clone(), ttl, payload));
    }
    if items.is_empty() {
        return RespFrame::SimpleString(Bytes::from_static(b"NOKEY"));
    }
    let io = async {
        let mut sock = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
        let _ = sock.set_nodelay(true);
        let mut out: Vec<u8> = Vec::new();
        let mut expected = 0usize;
        if let Some((user, pass)) = &auth {
            match user {
                Some(u) => out.extend(solikv_persist::aof::encode_command(b"AUTH", &[u, pass])),
                None => out.extend(solikv_persist::aof::encode_command(b"AUTH", &[pass])),
            }
            expected += 1;
        }
        let dbs = dest_db.to_string();
        out.extend(solikv_persist::aof::encode_command(
            b"SELECT",
            &[dbs.as_bytes()],
        ));
        expected += 1;
        for (k, ttl, payload) in &items {
            let t = ttl.to_string();
            let mut a: Vec<&[u8]> = vec![k, t.as_bytes(), payload];
            if replace {
                a.push(b"REPLACE");
            }
            out.extend(solikv_persist::aof::encode_command(b"RESTORE-ASKING", &a));
            expected += 1;
        }
        sock.write_all(&out).await?;
        // Replies: simple lines (+OK / -ERR ...), one per command.
        let mut replies = Vec::with_capacity(expected);
        let mut buf = BytesMut::with_capacity(4096);
        while replies.len() < expected {
            if let Some(nl) = buf.iter().position(|&b| b == b'\n') {
                let line = buf.split_to(nl + 1);
                replies.push(String::from_utf8_lossy(&line).trim_end().to_string());
                continue;
            }
            if sock.read_buf(&mut buf).await? == 0 {
                return Err(std::io::Error::other("target closed the connection"));
            }
        }
        Ok::<Vec<String>, std::io::Error>(replies)
    };
    let replies = match tokio::time::timeout(timeout, io).await {
        Err(_) => return RespFrame::error("IOERR error or timeout reading to target instance"),
        Ok(Err(e)) => {
            return RespFrame::error(format!(
                "IOERR error or timeout connecting to the client: {e}"
            ))
        }
        Ok(Ok(r)) => r,
    };
    // AUTH and SELECT first; then one reply per key.
    let skip = replies.len() - items.len();
    for r in &replies[..skip] {
        if let Some(e) = r.strip_prefix('-') {
            return RespFrame::error(format!("ERR Target instance replied with error: {e}"));
        }
    }
    let mut first_err = None;
    for ((k, _, _), r) in items.iter().zip(&replies[skip..]) {
        if let Some(e) = r.strip_prefix('-') {
            first_err.get_or_insert_with(|| e.to_string());
            continue;
        }
        if !copy {
            engine.execute_in_db(db, "DEL", std::slice::from_ref(k));
        }
    }
    match first_err {
        Some(e) => RespFrame::error(format!("ERR Target instance replied with error: {e}")),
        None => RespFrame::ok(),
    }
}

/// `REPLCONF <option> <value> ...` from a would-be replica. `None`: no reply
/// (`ACK`, which a replica sends on its link, not as a command).
fn replconf(args: &[Bytes], hs: &mut crate::replication::Handshake) -> Option<RespFrame> {
    if !args.len().is_multiple_of(2) || args.is_empty() {
        return Some(RespFrame::error("ERR syntax error"));
    }
    for pair in args.chunks(2) {
        let opt = String::from_utf8_lossy(&pair[0]).to_ascii_lowercase();
        let val = String::from_utf8_lossy(&pair[1]).to_string();
        match opt.as_str() {
            "listening-port" => match val.parse::<u16>() {
                Ok(p) => hs.listening_port = p,
                Err(_) => {
                    return Some(RespFrame::error(
                        "ERR value is not an integer or out of range",
                    ))
                }
            },
            "ip-address" => hs.ip_address = Some(val),
            "capa" | "rdb-only" | "rdb-filter-only" => {}
            "ack" | "getack" | "fack" => return None,
            _ => {
                return Some(RespFrame::error(format!(
                    "ERR Unrecognized REPLCONF option: {}",
                    opt
                )))
            }
        }
    }
    Some(RespFrame::ok())
}

/// Convert our CommandResponse to a RESP frame for the wire.
/// Redis 6+'s reply to a failed AUTH.
const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";

enum AuthError {
    Arity,
    NoPassword,
    Wrong,
}

/// `HELLO [protover [AUTH username password] [SETNAME clientname]]`:
/// switch the connection to RESP2 or RESP3, optionally authenticating and
/// naming it, and describe the server.
#[allow(clippy::too_many_arguments)]
fn hello(
    args: &[Bytes],
    _password: &Option<Arc<String>>,
    authenticated: &mut bool,
    tracker: &AuthFailureTracker,
    ip: &str,
    engine: &CommandEngine,
    session: &mut solikv_engine::Session,
    cluster: bool,
) -> CommandResponse {
    let mut i = 0;
    let mut proto = session.resp;
    if let Some(v) = args.first() {
        match std::str::from_utf8(v)
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
        {
            Some(p @ (2 | 3)) => proto = p as u8,
            Some(_) => return CommandResponse::error("NOPROTO unsupported protocol version"),
            None => {
                return CommandResponse::error(
                    "ERR Protocol version is not an integer or out of range",
                )
            }
        }
        i = 1;
    }
    let mut setname: Option<Bytes> = None;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "AUTH" if i + 2 < args.len() => {
                match acl_auth(engine, &args[i + 1..i + 3]) {
                    Ok(user) => {
                        *authenticated = true;
                        session.set_user(user, engine.acl().version());
                        tracker.record_success(ip);
                    }
                    Err(AuthError::Wrong) => {
                        tracker.record_failure(ip);
                        tracing::warn!("Failed AUTH attempt from {}", ip);
                        return CommandResponse::error(WRONGPASS);
                    }
                    Err(_) => return CommandResponse::error(WRONGPASS),
                }
                i += 3;
            }
            "SETNAME" if i + 1 < args.len() => {
                setname = Some(args[i + 1].clone());
                i += 2;
            }
            _ => {
                return CommandResponse::error(format!(
                    "ERR Syntax error in HELLO option '{}'",
                    opt.to_lowercase()
                ))
            }
        }
    }
    if !*authenticated {
        return CommandResponse::error(
            "NOAUTH HELLO must be called with the client already authenticated, otherwise the \
             HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and \
             select the RESP protocol version at the same time",
        );
    }
    if let Some(name) = setname {
        let r = engine.execute_session(session, "CLIENT", &[Bytes::from_static(b"SETNAME"), name]);
        if r.is_error() {
            return r;
        }
    }
    session.set_resp(proto);
    let bulk = |s: &str| CommandResponse::bulk(Bytes::from(s.to_string()));
    CommandResponse::Map(vec![
        (bulk("server"), bulk("redis")),
        (bulk("version"), bulk("7.2.0")),
        (bulk("proto"), CommandResponse::Integer(proto as i64)),
        (bulk("id"), CommandResponse::Integer(session.info.id as i64)),
        (
            bulk("mode"),
            bulk(if cluster { "cluster" } else { "standalone" }),
        ),
        (bulk("role"), bulk("master")),
        (bulk("modules"), CommandResponse::array(vec![])),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_auth_failure_map_is_pruned() {
        let tracker = AuthFailureTracker::default();
        let Some(long_ago) = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(AUTH_COOLDOWN_SECS + 5))
        else {
            return;
        };
        {
            let mut map = tracker.failures.write().unwrap();
            for i in 0..AUTH_TRACKER_PRUNE_AT {
                map.insert(format!("10.1.{}.{}", i / 256, i % 256), (1, long_ago));
            }
        }
        tracker.record_failure("192.0.2.1");
        assert_eq!(tracker.failures.read().unwrap().len(), 1);
    }
}
