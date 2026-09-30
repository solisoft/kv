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
use solikv_resp::codec::{decode_frame, decode_frame_with_auth, encode_frame, RespFrame};
use solikv_resp::connection::ClientConnection;
use solikv_resp::parser::ParsedCommand;

use solikv_cluster::ClusterManager;
use solikv_core::CommandResponse;

/// Maximum number of concurrent client connections.
const MAX_CONNECTIONS: usize = 10_000;

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

    let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    let auth_tracker = Arc::new(AuthFailureTracker::default());

    loop {
        let (socket, peer_addr) = listener.accept().await?;

        let permit = match conn_semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                tracing::warn!(
                    "Connection limit reached ({}), rejecting {}",
                    MAX_CONNECTIONS,
                    peer_addr
                );
                drop(socket);
                continue;
            }
        };

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
    let mut conn = ClientConnection::new(peer_addr, password.is_some());
    // Database, MULTI queue, watches, CLIENT metadata: engine-side state.
    let mut session = engine.new_session(peer_addr.to_string(), local_addr);
    let info = session.info.clone();

    loop {
        // ── Phase 1: Decode all complete frames from buffer ──
        // Apply tighter pre-auth limits until the connection has AUTHed:
        // a not-yet-authenticated peer cannot force the server to allocate
        // arrays larger than MAX_UNAUTH_ARRAY_LEN or bulks larger than
        // MAX_UNAUTH_BULK_LEN. (SEC-013)
        let mut frames = Vec::new();
        loop {
            match decode_frame_with_auth(&read_buf, conn.authenticated) {
                Ok(Some((frame, consumed))) => {
                    read_buf.advance(consumed);
                    frames.push(frame);
                }
                Ok(None) => break, // need more data
                Err(e) => {
                    encode_frame(&RespFrame::error(e), &mut write_buf);
                    socket.write_all(&write_buf).await?;
                    return Ok(());
                }
            }
        }

        // If no complete frames, read more data from socket
        if frames.is_empty() {
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
            let read = async {
                // `CLIENT KILL` from another connection ends this one even
                // while it waits for input.
                tokio::select! {
                    r = socket.read_buf(&mut read_buf) => Some(r),
                    _ = info.killed() => None,
                }
            };
            let n = match (conn.authenticated, auth_deadline, limits.idle_timeout) {
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
            let Some(n) = n else {
                return Ok(()); // killed
            };
            let n = n?;
            if n == 0 {
                return Ok(()); // EOF
            }
            continue;
        }

        // ── Phase 2: Parse all frames into commands ──
        let mut commands = Vec::with_capacity(frames.len());
        for frame in frames {
            match ParsedCommand::from_frame(frame) {
                Ok(cmd) => commands.push(cmd),
                Err(e) => {
                    encode_frame(&RespFrame::error(e), &mut write_buf);
                    socket.write_all(&write_buf).await?;
                    write_buf.clear();
                    return Ok(());
                }
            }
        }

        // ── Phase 3: Execute commands ──
        // Fast path: all commands are pure engine commands (no MULTI/EXEC/SUBSCRIBE/AUTH)
        // This is the common case for pipelined benchmarks
        if let Some(last) = commands.last() {
            info.touch(&last.name, commands.len());
        }
        let fast_path = conn.authenticated
            && !session.in_multi()
            && commands.iter().all(|cmd| {
                !matches!(
                    cmd.name.as_str(),
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
                )
            });

        if fast_path {
            // Pre-reserve write buffer for batch responses
            write_buf.reserve(commands.len() * 16);
            for cmd in commands {
                // Check for MOVED redirect in cluster mode
                if let Some(moved_frame) = check_cluster_moved(&cluster, &cmd.name, &cmd.args) {
                    encode_frame(&moved_frame, &mut write_buf);
                    continue;
                }
                let resp = engine.execute_session(&mut session, &cmd.name, &cmd.args);
                if session.reply_allowed() {
                    encode_frame(&command_response_to_frame(resp), &mut write_buf);
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
                    let frame = match check_auth(&password, &cmd.args) {
                        Ok(()) => {
                            conn.authenticated = true;
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
                    let frame = hello(
                        &cmd.args,
                        &password,
                        &mut conn.authenticated,
                        &auth_tracker,
                        &ip,
                        &engine,
                        &mut session,
                        cluster.is_some(),
                    );
                    encode_frame(&frame, &mut write_buf);
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

                match cmd.name.as_str() {
                    "SUBSCRIBE" if !session.in_multi() => {
                        // Enter pubsub mode — send confirmations, then enter delivery loop
                        for channel in &cmd.args {
                            conn.subscriptions.push(channel.clone());
                            let count = conn.subscriptions.len() + conn.psubscriptions.len();
                            encode_frame(
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
                            &password,
                            &info,
                        )
                        .await;
                    }
                    "PSUBSCRIBE" if !session.in_multi() => {
                        for pattern_bytes in &cmd.args {
                            let pattern =
                                std::str::from_utf8(pattern_bytes).unwrap_or("").to_string();
                            conn.psubscriptions.push(pattern.clone());
                            let count = conn.subscriptions.len() + conn.psubscriptions.len();
                            encode_frame(
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
                            &password,
                            &info,
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
                        encode_frame(
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
                        encode_frame(
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
                        if let Some(ref cluster_mgr) = cluster {
                            if cmd.args.is_empty() {
                                encode_frame(
                                    &RespFrame::error(
                                        "ERR wrong number of arguments for 'cluster' command",
                                    ),
                                    &mut write_buf,
                                );
                                continue;
                            }
                            let sub_cmd = std::str::from_utf8(&cmd.args[0])
                                .unwrap_or("")
                                .to_uppercase();
                            match sub_cmd.as_str() {
                                "INFO" => {
                                    let info = cluster_mgr.cluster_info();
                                    encode_frame(
                                        &RespFrame::BulkString(Bytes::from(info)),
                                        &mut write_buf,
                                    );
                                }
                                "NODES" => {
                                    let nodes = cluster_mgr.cluster_nodes();
                                    encode_frame(
                                        &RespFrame::BulkString(Bytes::from(nodes)),
                                        &mut write_buf,
                                    );
                                }
                                "SLOTS" => {
                                    let slots = cluster_mgr.cluster_slots();
                                    let mut result = Vec::new();
                                    for slot_info in slots {
                                        let mut slot_array = Vec::new();
                                        for part in slot_info {
                                            slot_array
                                                .push(RespFrame::BulkString(Bytes::from(part)));
                                        }
                                        result.push(RespFrame::Array(slot_array));
                                    }
                                    encode_frame(&RespFrame::Array(result), &mut write_buf);
                                }
                                "MEET" => {
                                    if cmd.args.len() != 3 {
                                        encode_frame(
                                            &RespFrame::error("ERR CLUSTER MEET ip port"),
                                            &mut write_buf,
                                        );
                                        continue;
                                    }
                                    let ip =
                                        std::str::from_utf8(&cmd.args[1]).unwrap_or("127.0.0.1");
                                    let port = std::str::from_utf8(&cmd.args[2])
                                        .unwrap_or("7000")
                                        .parse::<u16>()
                                        .unwrap_or(7000);
                                    cluster_mgr.meet(ip.to_string(), port);
                                    encode_frame(
                                        &RespFrame::SimpleString(Bytes::from("OK")),
                                        &mut write_buf,
                                    );
                                }
                                "ADDSLOTS" => {
                                    if cmd.args.len() < 2 {
                                        encode_frame(
                                            &RespFrame::error(
                                                "ERR CLUSTER ADDSLOTS slot [slot ...]",
                                            ),
                                            &mut write_buf,
                                        );
                                        continue;
                                    }
                                    // Parsed, not `unwrap_or(0)`. That default
                                    // made `CLUSTER ADDSLOTS abc` claim slot 0 —
                                    // a silent wrong assignment, and slot 0 is
                                    // where every empty-string key lands.
                                    let mut failed = false;
                                    for slot_arg in &cmd.args[1..] {
                                        let slot_str = std::str::from_utf8(slot_arg).unwrap_or("");
                                        let parsed = match slot_str.split_once('-') {
                                            Some((start, end)) => {
                                                match (start.parse::<u16>(), end.parse::<u16>()) {
                                                    (Ok(s), Ok(e)) => Ok((s, e)),
                                                    _ => Err(()),
                                                }
                                            }
                                            None => match slot_str.parse::<u16>() {
                                                Ok(slot) => Ok((slot, slot)),
                                                Err(_) => Err(()),
                                            },
                                        };
                                        let Ok((start, end)) = parsed else {
                                            encode_frame(
                                                &RespFrame::error(format!(
                                                    "ERR {:?} is not a slot or a slot range",
                                                    slot_str
                                                )),
                                                &mut write_buf,
                                            );
                                            failed = true;
                                            break;
                                        };
                                        if let Err(e) = cluster_mgr.add_slots(start, end) {
                                            encode_frame(&RespFrame::error(e), &mut write_buf);
                                            failed = true;
                                            break;
                                        }
                                    }
                                    if failed {
                                        continue;
                                    }
                                    if write_buf.is_empty()
                                        || !matches!(write_buf.last(), Some(b'*'))
                                    {
                                        encode_frame(
                                            &RespFrame::SimpleString(Bytes::from("OK")),
                                            &mut write_buf,
                                        );
                                    }
                                }
                                "DELSLOTS" => {
                                    if cmd.args.len() < 2 {
                                        encode_frame(
                                            &RespFrame::error(
                                                "ERR CLUSTER DELSLOTS slot [slot ...]",
                                            ),
                                            &mut write_buf,
                                        );
                                        continue;
                                    }
                                    // Parsed like ADDSLOTS. `unwrap_or(0)` made
                                    // `DELSLOTS abc` release slot 0, and an
                                    // out-of-range slot indexed past the table.
                                    let mut failed = false;
                                    for slot_arg in &cmd.args[1..] {
                                        let slot_str = std::str::from_utf8(slot_arg).unwrap_or("");
                                        let Some((start, end)) = parse_slot_range(slot_str) else {
                                            encode_frame(
                                                &RespFrame::error(format!(
                                                    "ERR {:?} is not a slot or a slot range",
                                                    slot_str
                                                )),
                                                &mut write_buf,
                                            );
                                            failed = true;
                                            break;
                                        };
                                        if let Err(e) = cluster_mgr.del_slots(start, end) {
                                            encode_frame(&RespFrame::error(e), &mut write_buf);
                                            failed = true;
                                            break;
                                        }
                                    }
                                    if !failed {
                                        encode_frame(
                                            &RespFrame::SimpleString(Bytes::from("OK")),
                                            &mut write_buf,
                                        );
                                    }
                                }
                                "KEYSLOT" => {
                                    if cmd.args.len() != 2 {
                                        encode_frame(
                                            &RespFrame::error("ERR CLUSTER KEYSLOT key"),
                                            &mut write_buf,
                                        );
                                        continue;
                                    }
                                    let slot = cluster_mgr.key_slot(&cmd.args[1]);
                                    encode_frame(&RespFrame::Integer(slot as i64), &mut write_buf);
                                }
                                _ => {
                                    encode_frame(
                                        &RespFrame::error(format!(
                                            "ERR Unknown cluster subcommand '{}'",
                                            sub_cmd
                                        )),
                                        &mut write_buf,
                                    );
                                }
                            }
                        } else {
                            encode_frame(
                                &RespFrame::error("ERR This instance has cluster support disabled"),
                                &mut write_buf,
                            );
                        }
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
                if !session.in_multi() {
                    if let Some(moved_frame) = check_cluster_moved(&cluster, &cmd.name, &cmd.args) {
                        encode_frame(&moved_frame, &mut write_buf);
                        continue;
                    }
                }
                let response = engine.execute_session(&mut session, &cmd.name, &cmd.args);
                if cmd.name == "RESET" {
                    // Back to the default user: with a password, that is
                    // unauthenticated.
                    conn.authenticated = password.is_none();
                }
                if session.reply_allowed() {
                    encode_frame(&command_response_to_frame(response), &mut write_buf);
                }
            }
        }

        // ── Phase 4: Single flush for all responses ──
        if !write_buf.is_empty() {
            socket.write_all(&write_buf).await?;
            write_buf.clear();
        }
    }
}

/// Most bytes buffered from a client in pub/sub mode, where only short
/// (P)SUBSCRIBE / PING / QUIT commands are valid.
const MAX_PUBSUB_READ_BUF: usize = 1024 * 1024;

/// Pub/sub delivery loop. Entered when client issues SUBSCRIBE or PSUBSCRIBE.
/// Uses a fan-in mpsc channel: each subscription spawns a forwarding task from
/// broadcast::Receiver -> mpsc::UnboundedSender. The main loop select!s between
/// the fan-in receiver and new frames from the socket.
async fn handle_pubsub_mode<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    pubsub: &Arc<PubSubBroker>,
    _engine: &Arc<CommandEngine>,
    conn: &mut ClientConnection,
    read_buf: &mut BytesMut,
    _password: &Option<Arc<String>>,
    info: &Arc<solikv_engine::session::ClientInfo>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    // Forwarding tasks: channel_name -> JoinHandle
    let mut channel_tasks: HashMap<Bytes, JoinHandle<()>> = HashMap::new();
    let mut pattern_tasks: HashMap<String, JoinHandle<()>> = HashMap::new();

    // Set up forwarding tasks for initial subscriptions
    for ch in &conn.subscriptions {
        let rx = pubsub.subscribe(ch.clone());
        let tx = fan_tx.clone();
        let handle = tokio::spawn(forward_broadcast(rx, tx));
        channel_tasks.insert(ch.clone(), handle);
    }
    for pat in &conn.psubscriptions {
        let rx = pubsub.psubscribe(pat.clone());
        let tx = fan_tx.clone();
        let handle = tokio::spawn(forward_broadcast(rx, tx));
        pattern_tasks.insert(pat.clone(), handle);
    }

    loop {
        tokio::select! {
            // Ended by `CLIENT KILL`.
            _ = info.killed() => {
                for (_, h) in channel_tasks.drain() { h.abort(); }
                for (_, h) in pattern_tasks.drain() { h.abort(); }
                return Ok(());
            }
            // Branch 1: incoming message from any subscription
            msg = fan_rx.recv() => {
                let Some(msg) = msg else { break; };
                if let Some(ref pattern) = msg.pattern {
                    // pmessage
                    encode_frame(
                        &RespFrame::Array(vec![
                            RespFrame::BulkString(Bytes::from("pmessage")),
                            RespFrame::BulkString(Bytes::from(pattern.clone())),
                            RespFrame::BulkString(msg.channel),
                            RespFrame::BulkString(msg.message),
                        ]),
                        &mut write_buf,
                    );
                } else {
                    // message
                    encode_frame(
                        &RespFrame::Array(vec![
                            RespFrame::BulkString(Bytes::from("message")),
                            RespFrame::BulkString(msg.channel),
                            RespFrame::BulkString(msg.message),
                        ]),
                        &mut write_buf,
                    );
                }
                if !write_buf.is_empty() {
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
                                    for channel in &cmd.args {
                                        if !conn.subscriptions.contains(channel) {
                                            conn.subscriptions.push(channel.clone());
                                            let rx = pubsub.subscribe(channel.clone());
                                            let tx = fan_tx.clone();
                                            let handle = tokio::spawn(forward_broadcast(rx, tx));
                                            channel_tasks.insert(channel.clone(), handle);
                                        }
                                        let count = conn.subscriptions.len() + conn.psubscriptions.len();
                                        encode_frame(
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
                                        encode_frame(
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("unsubscribe")),
                                                RespFrame::BulkString(ch.clone()),
                                                RespFrame::Integer(count as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                    if channels_to_remove.is_empty() {
                                        encode_frame(
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("unsubscribe")),
                                                RespFrame::Null,
                                                RespFrame::Integer(count as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                    // Exit pubsub mode if no subscriptions remain
                                    if count == 0 {
                                        if !write_buf.is_empty() {
                                            socket.write_all(&write_buf).await?;
                                            write_buf.clear();
                                        }
                                        return Ok(());
                                    }
                                }
                                "PSUBSCRIBE" => {
                                    for pat_bytes in &cmd.args {
                                        let pat = std::str::from_utf8(pat_bytes).unwrap_or("").to_string();
                                        if !conn.psubscriptions.contains(&pat) {
                                            conn.psubscriptions.push(pat.clone());
                                            let rx = pubsub.psubscribe(pat.clone());
                                            let tx = fan_tx.clone();
                                            let handle = tokio::spawn(forward_broadcast(rx, tx));
                                            pattern_tasks.insert(pat.clone(), handle);
                                        }
                                        let count = conn.subscriptions.len() + conn.psubscriptions.len();
                                        encode_frame(
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
                                        encode_frame(
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("punsubscribe")),
                                                RespFrame::BulkString(Bytes::from(pat.clone())),
                                                RespFrame::Integer(count as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                    if pats_to_remove.is_empty() {
                                        encode_frame(
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("punsubscribe")),
                                                RespFrame::Null,
                                                RespFrame::Integer(count as i64),
                                            ]),
                                            &mut write_buf,
                                        );
                                    }
                                    if count == 0 {
                                        if !write_buf.is_empty() {
                                            socket.write_all(&write_buf).await?;
                                            write_buf.clear();
                                        }
                                        return Ok(());
                                    }
                                }
                                "PING" => {
                                    if cmd.args.is_empty() {
                                        encode_frame(
                                            &RespFrame::Array(vec![
                                                RespFrame::BulkString(Bytes::from("pong")),
                                                RespFrame::BulkString(Bytes::from("")),
                                            ]),
                                            &mut write_buf,
                                        );
                                    } else {
                                        encode_frame(
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
async fn forward_broadcast(
    mut rx: tokio::sync::broadcast::Receiver<PubSubMessage>,
    tx: mpsc::UnboundedSender<PubSubMessage>,
) {
    loop {
        match rx.recv().await {
            Ok(msg) => {
                if tx.send(msg).is_err() {
                    return; // fan-in channel closed
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::debug!("pubsub subscriber lagged by {} messages", n);
                // Continue receiving
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return; // channel closed
            }
        }
    }
}

fn get_command_key<'a>(name: &'a str, args: &'a [bytes::Bytes]) -> Option<&'a [u8]> {
    let name_upper = name.to_uppercase();
    match name_upper.as_str() {
        "GET" | "SET" | "DEL" | "EXISTS" | "EXPIRE" | "EXPIREAT" | "TTL" | "PTTL" | "PERSIST"
        | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "APPEND" | "STRLEN"
        | "GETSET" | "SETEX" | "PSETEX" | "SETNX" | "SETXX" | "GETEX" | "HDEL" | "HEXISTS"
        | "HGET" | "HGETALL" | "HINCRBY" | "HINCRBYFLOAT" | "HMGET" | "HMSET" | "HSET"
        | "HSETNX" | "HVALS" | "HKEYS" | "HLEN" | "HSCAN" | "LGET" | "LPUSH" | "LPOP" | "RPUSH"
        | "RPOP" | "LLEN" | "LRANGE" | "LSET" | "LREM" | "LTRIM" | "LINDEX" | "SADD" | "SCARD"
        | "SDIFF" | "SINTER" | "SISMEMBER" | "SMEMBERS" | "SPOP" | "SRANDMEMBER" | "SUNION"
        | "ZADD" | "ZCARD" | "ZCOUNT" | "ZRANGE" | "ZRANGEBYSCORE" | "ZRANK" | "ZREVRANGE"
        | "ZREVRANGEBYSCORE" | "ZREVRANK" | "ZSCORE" | "ZINCRBY" | "ZREM" | "ZREMRANGEBYSCORE"
        | "ZREMRANGEBYRANK" | "ZSCAN" | "MGET" | "MSET" | "MSETNX" | "GETBIT" | "SETBIT"
        | "BITCOUNT" | "BITPOS" | "BITOP" | "BITFIELD" | "PFADD" | "PFCOUNT" | "PFMERGE" => {
            if args.is_empty() {
                None
            } else {
                Some(&args[0])
            }
        }
        _ => None,
    }
}

/// What to say when a slot has an owner whose address cannot be read.
///
/// A different fault from an unassigned slot, and it gets a different sentence:
/// somebody claimed this slot under an identifier that carries no address, so the
/// cluster knows who owns it and cannot say where. Redirecting to a guessed
/// address would send the client somewhere nobody chose.
fn unroutable_owner(slot: u16, owner: &str) -> String {
    format!(
        "CLUSTERDOWN Hash slot {} is claimed by {:?}, which is not an address this cluster can \
         redirect to",
        slot, owner
    )
}

fn check_cluster_moved(
    cluster: &Option<Arc<ClusterManager>>,
    cmd_name: &str,
    args: &[bytes::Bytes],
) -> Option<RespFrame> {
    let cluster = cluster.as_ref()?;
    if !cluster.is_enabled() {
        return None;
    }

    let key = get_command_key(cmd_name, args)?;
    let slot = cluster.key_slot(key);

    if !cluster.is_my_slot(key) {
        // The owner's *address*, resolved through gossip. Ownership is recorded
        // by node id — the stable identity — so the address is looked up at
        // redirect time rather than stored beside the claim, where it would go
        // stale the moment a node moved.
        if let Some((ip, port)) = cluster.get_slot_owner_address(slot) {
            return Some(RespFrame::Error(format!("MOVED {} {}:{}", slot, ip, port)));
        }

        // Two different failures, two different sentences. Both used to be
        // `MOVED <slot> 127.0.0.1:7000` — a port nothing in this cluster is
        // required to be on. A client follows a MOVED, so it would connect
        // there, fail, and report a connection error against an address the
        // operator never configured.
        return Some(RespFrame::Error(
            match cluster.get_slot_owner_for_key(key) {
                Some(owner) => unroutable_owner(slot, &owner),
                None => format!(
                    "CLUSTERDOWN Hash slot {} is not served: no node claims it",
                    slot
                ),
            },
        ));
    }

    None
}

/// `N` or `N-M`, each below 16384.
fn parse_slot_range(text: &str) -> Option<(u16, u16)> {
    let (start, end) = match text.split_once('-') {
        Some((start, end)) => (start.parse::<u16>().ok()?, end.parse::<u16>().ok()?),
        None => {
            let slot = text.parse::<u16>().ok()?;
            (slot, slot)
        }
    };
    (start <= end && end < 16384).then_some((start, end))
}

/// Constant-time byte comparison to prevent timing side-channel attacks on password checks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Convert our CommandResponse to a RESP frame for the wire.
/// Redis 6+'s reply to a failed AUTH.
const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";

enum AuthError {
    Arity,
    NoPassword,
    Wrong,
}

/// `AUTH password` or `AUTH username password`. There is one user,
/// `default` (ACLs are not implemented): any other name is refused like a
/// wrong password, so URLs of the form `redis://default:pw@host` work.
fn check_auth(password: &Option<Arc<String>>, args: &[Bytes]) -> Result<(), AuthError> {
    let (user, pass) = match args {
        [pass] => (None, pass),
        [user, pass] => (Some(user), pass),
        _ => return Err(AuthError::Arity),
    };
    if user.is_some_and(|u| u.as_ref() != b"default") {
        return Err(AuthError::Wrong);
    }
    match password {
        // No password: `AUTH pw` is an error; `AUTH default <anything>`
        // succeeds, as for Redis' passwordless default user.
        None if user.is_none() => Err(AuthError::NoPassword),
        None => Ok(()),
        Some(expected) if constant_time_eq(pass, expected.as_bytes()) => Ok(()),
        Some(_) => Err(AuthError::Wrong),
    }
}

/// `HELLO [protover [AUTH username password] [SETNAME clientname]]`.
/// Only RESP2 is spoken: `HELLO 3` gets `NOPROTO`, which clients take as
/// "fall back to RESP2".
#[allow(clippy::too_many_arguments)]
fn hello(
    args: &[Bytes],
    password: &Option<Arc<String>>,
    authenticated: &mut bool,
    tracker: &AuthFailureTracker,
    ip: &str,
    engine: &CommandEngine,
    session: &mut solikv_engine::Session,
    cluster: bool,
) -> RespFrame {
    let mut i = 0;
    if let Some(v) = args.first() {
        match std::str::from_utf8(v)
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
        {
            Some(2) => {}
            Some(_) => return RespFrame::error("NOPROTO unsupported protocol version"),
            None => {
                return RespFrame::error("ERR Protocol version is not an integer or out of range")
            }
        }
        i = 1;
    }
    let mut setname: Option<Bytes> = None;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "AUTH" if i + 2 < args.len() => {
                match check_auth(password, &args[i + 1..i + 3]) {
                    Ok(()) => {
                        *authenticated = true;
                        tracker.record_success(ip);
                    }
                    Err(AuthError::Wrong) => {
                        tracker.record_failure(ip);
                        tracing::warn!("Failed AUTH attempt from {}", ip);
                        return RespFrame::error(WRONGPASS);
                    }
                    Err(_) => return RespFrame::error(WRONGPASS),
                }
                i += 3;
            }
            "SETNAME" if i + 1 < args.len() => {
                setname = Some(args[i + 1].clone());
                i += 2;
            }
            _ => {
                return RespFrame::error(format!(
                    "ERR Syntax error in HELLO option '{}'",
                    opt.to_lowercase()
                ))
            }
        }
    }
    if !*authenticated {
        return RespFrame::error(
            "NOAUTH HELLO must be called with the client already authenticated, otherwise the \
             HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and \
             select the RESP protocol version at the same time",
        );
    }
    if let Some(name) = setname {
        let r = engine.execute_session(session, "CLIENT", &[Bytes::from_static(b"SETNAME"), name]);
        if let CommandResponse::Error(e) = r {
            return RespFrame::Error(e);
        }
    }
    let bulk = |s: &str| RespFrame::BulkString(Bytes::from(s.to_string()));
    RespFrame::Array(vec![
        bulk("server"),
        bulk("redis"),
        bulk("version"),
        bulk("7.2.0"),
        bulk("proto"),
        RespFrame::Integer(2),
        bulk("id"),
        RespFrame::Integer(session.info.id as i64),
        bulk("mode"),
        bulk(if cluster { "cluster" } else { "standalone" }),
        bulk("role"),
        bulk("master"),
        bulk("modules"),
        RespFrame::Array(vec![]),
    ])
}

fn command_response_to_frame(resp: CommandResponse) -> RespFrame {
    match resp {
        CommandResponse::Ok => RespFrame::ok(),
        CommandResponse::Nil => RespFrame::Null,
        CommandResponse::Integer(n) => RespFrame::Integer(n),
        CommandResponse::BulkString(b) => RespFrame::BulkString(b),
        CommandResponse::SimpleString(s) => RespFrame::SimpleString(s),
        CommandResponse::Array(items) => {
            RespFrame::Array(items.into_iter().map(command_response_to_frame).collect())
        }
        CommandResponse::Error(msg) => RespFrame::Error(msg),
        CommandResponse::Queued => RespFrame::SimpleString(Bytes::from("QUEUED")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_ranges_are_parsed_and_bounded() {
        assert_eq!(parse_slot_range("5"), Some((5, 5)));
        assert_eq!(parse_slot_range("0-16383"), Some((0, 16383)));
        assert_eq!(parse_slot_range("16384"), None);
        assert_eq!(parse_slot_range("20000"), None);
        assert_eq!(parse_slot_range("10-5"), None);
        assert_eq!(parse_slot_range("abc"), None);
        assert_eq!(parse_slot_range("1-x"), None);
    }

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
