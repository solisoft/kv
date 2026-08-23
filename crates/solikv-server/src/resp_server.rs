use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;
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

use crate::auth::{constant_time_eq, AuthFailureTracker};

/// Maximum number of concurrent client connections.
const MAX_CONNECTIONS: usize = 10_000;

pub async fn run(
    addr: &str,
    engine: Arc<CommandEngine>,
    pubsub: Arc<PubSubBroker>,
    password: Option<Arc<String>>,
    cluster: Option<Arc<ClusterManager>>,
    tls_config: Option<ServerConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
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

        let engine = engine.clone();
        let pubsub = pubsub.clone();
        let password = password.clone();
        let cluster = cluster.clone();
        let tls_acceptor = tls_acceptor.clone();
        let auth_tracker = auth_tracker.clone();

        tokio::spawn(async move {
            let mut socket: Box<dyn AsyncSocket> = match tls_acceptor {
                Some(acceptor) => match acceptor.accept(socket).await {
                    Ok(stream) => Box::new(stream),
                    Err(e) => {
                        tracing::debug!("TLS accept error from {}: {}", peer_addr, e);
                        drop(permit);
                        return;
                    }
                },
                None => Box::new(socket),
            };
            if let Err(e) = handle_connection(
                &mut socket,
                engine,
                pubsub,
                peer_addr,
                password,
                cluster,
                auth_tracker,
            )
            .await
            {
                tracing::debug!("Connection error from {}: {}", peer_addr, e);
            }
            drop(permit);
        });
    }
}

async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: S,
    engine: Arc<CommandEngine>,
    pubsub: Arc<PubSubBroker>,
    peer_addr: std::net::SocketAddr,
    password: Option<Arc<String>>,
    cluster: Option<Arc<ClusterManager>>,
    auth_tracker: Arc<AuthFailureTracker>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// Maximum read buffer size per connection (256 MB).
    const MAX_READ_BUF: usize = 256 * 1024 * 1024;

    let mut read_buf = BytesMut::with_capacity(65536);
    let mut write_buf = BytesMut::with_capacity(65536);
    let mut conn = ClientConnection::new(peer_addr, password.is_some());

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
            if read_buf.len() > MAX_READ_BUF {
                return Err("read buffer exceeded maximum size".into());
            }
            let n = socket.read_buf(&mut read_buf).await?;
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
        let fast_path = conn.authenticated
            && !conn.in_transaction
            && commands.iter().all(|cmd| {
                !matches!(
                    cmd.name.as_str(),
                    "MULTI"
                        | "EXEC"
                        | "DISCARD"
                        | "SUBSCRIBE"
                        | "UNSUBSCRIBE"
                        | "PSUBSCRIBE"
                        | "PUNSUBSCRIBE"
                        | "QUIT"
                        | "AUTH"
                        | "CLUSTER"
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
                let resp = engine.execute(&cmd.name, &cmd.args);
                encode_frame(&command_response_to_frame(resp), &mut write_buf);
            }
        } else {
            // Slow path: sequential processing with connection state
            for cmd in commands {
                // Handle AUTH command
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
                    match &password {
                        None => {
                            encode_frame(
                                &RespFrame::error("ERR Client sent AUTH, but no password is set"),
                                &mut write_buf,
                            );
                        }
                        Some(pass) => {
                            if cmd.args.len() != 1 {
                                encode_frame(
                                    &RespFrame::error(
                                        "ERR wrong number of arguments for 'auth' command",
                                    ),
                                    &mut write_buf,
                                );
                            } else if constant_time_eq(&cmd.args[0], pass.as_bytes()) {
                                conn.authenticated = true;
                                auth_tracker.record_success(&ip);
                                encode_frame(&RespFrame::ok(), &mut write_buf);
                            } else {
                                auth_tracker.record_failure(&ip);
                                tracing::warn!("Failed AUTH attempt from {}", ip);
                                encode_frame(
                                    &RespFrame::error("ERR invalid password"),
                                    &mut write_buf,
                                );
                            }
                        }
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

                match cmd.name.as_str() {
                    "SUBSCRIBE" => {
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
                        )
                        .await;
                    }
                    "PSUBSCRIBE" => {
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
                        )
                        .await;
                    }
                    "UNSUBSCRIBE" => {
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
                    "PUNSUBSCRIBE" => {
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
                    "MULTI" => {
                        conn.in_transaction = true;
                        conn.tx_queue.clear();
                        encode_frame(&RespFrame::ok(), &mut write_buf);
                        continue;
                    }
                    "EXEC" => {
                        if !conn.in_transaction {
                            encode_frame(
                                &RespFrame::error("ERR EXEC without MULTI"),
                                &mut write_buf,
                            );
                            continue;
                        }
                        let queue = std::mem::take(&mut conn.tx_queue);
                        conn.in_transaction = false;

                        let mut results = Vec::new();
                        for args in queue {
                            let name = std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
                            let cmd_args: Vec<Bytes> = args[1..].to_vec();
                            let resp = engine.execute(&name, &cmd_args);
                            results.push(command_response_to_frame(resp));
                        }
                        encode_frame(&RespFrame::Array(results), &mut write_buf);
                        continue;
                    }
                    "DISCARD" => {
                        if !conn.in_transaction {
                            encode_frame(
                                &RespFrame::error("ERR DISCARD without MULTI"),
                                &mut write_buf,
                            );
                        } else {
                            conn.reset_transaction();
                            encode_frame(&RespFrame::ok(), &mut write_buf);
                        }
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
                                    for slot_arg in &cmd.args[1..] {
                                        let slot_str = std::str::from_utf8(slot_arg).unwrap_or("");
                                        if let Some((start, end)) = slot_str.split_once('-') {
                                            let start: u16 = start.parse().unwrap_or(0);
                                            let end: u16 = end.parse().unwrap_or(0);
                                            if let Err(e) = cluster_mgr.add_slots(start, end) {
                                                encode_frame(&RespFrame::error(e), &mut write_buf);
                                                break;
                                            }
                                        } else {
                                            let slot: u16 = slot_str.parse().unwrap_or(0);
                                            if let Err(e) = cluster_mgr.add_slots(slot, slot) {
                                                encode_frame(&RespFrame::error(e), &mut write_buf);
                                                break;
                                            }
                                        }
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
                                    for slot_arg in &cmd.args[1..] {
                                        let slot_str = std::str::from_utf8(slot_arg).unwrap_or("");
                                        if let Some((start, end)) = slot_str.split_once('-') {
                                            let start: u16 = start.parse().unwrap_or(0);
                                            let end: u16 = end.parse().unwrap_or(0);
                                            if let Err(e) = cluster_mgr.del_slots(start, end) {
                                                encode_frame(&RespFrame::error(e), &mut write_buf);
                                                break;
                                            }
                                        } else {
                                            let slot: u16 = slot_str.parse().unwrap_or(0);
                                            if let Err(e) = cluster_mgr.del_slots(slot, slot) {
                                                encode_frame(&RespFrame::error(e), &mut write_buf);
                                                break;
                                            }
                                        }
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

                // Queue in transaction or execute
                if conn.in_transaction {
                    let mut full_args = vec![Bytes::from(cmd.name.clone())];
                    full_args.extend(cmd.args.clone());
                    conn.tx_queue.push(full_args);
                    encode_frame(
                        &RespFrame::SimpleString(Bytes::from("QUEUED")),
                        &mut write_buf,
                    );
                } else {
                    // Check for MOVED redirect in cluster mode
                    if let Some(moved_frame) = check_cluster_moved(&cluster, &cmd.name, &cmd.args) {
                        encode_frame(&moved_frame, &mut write_buf);
                    } else {
                        let response = engine.execute(&cmd.name, &cmd.args);
                        encode_frame(&command_response_to_frame(response), &mut write_buf);
                    }
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
                                        conn.subscriptions.drain(..).collect::<Vec<_>>()
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
                                        conn.psubscriptions.drain(..).collect::<Vec<_>>()
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
        let owner = match cluster.get_slot_owner_for_key(key) {
            Some(o) => o,
            None => {
                return Some(RespFrame::Error(format!("MOVED {} 127.0.0.1:7000", slot)));
            }
        };

        let owner_info: Vec<&str> = owner.split('@').collect();
        let (ip, port) = if owner_info.len() >= 2 {
            let addr: Vec<&str> = owner_info[1].split(':').collect();
            if addr.len() >= 2 {
                (addr[0], addr[1])
            } else {
                return Some(RespFrame::Error(format!("MOVED {} 127.0.0.1:7000", slot)));
            }
        } else {
            let addr: Vec<&str> = owner.split(':').collect();
            if addr.len() >= 2 {
                (addr[0], addr[1])
            } else {
                return Some(RespFrame::Error(format!("MOVED {} 127.0.0.1:7000", slot)));
            }
        };

        return Some(RespFrame::Error(format!("MOVED {} {}:{}", slot, ip, port)));
    }

    None
}

/// Convert our CommandResponse to a RESP frame for the wire.
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
