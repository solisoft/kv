use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use solikv_engine::CommandEngine;
use solikv_pubsub::{PubSubBroker, PubSubMessage};
use solikv_resp::codec::{decode_frame, encode_frame, RespFrame};
use solikv_resp::connection::ClientConnection;
use solikv_resp::parser::ParsedCommand;

use solikv_core::CommandResponse;

pub async fn run(
    addr: &str,
    engine: Arc<CommandEngine>,
    pubsub: Arc<PubSubBroker>,
    password: Option<Arc<String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("RESP server listening on {}", addr);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        // TCP_NODELAY: disable Nagle's algorithm for lower latency
        let _ = socket.set_nodelay(true);
        let engine = engine.clone();
        let pubsub = pubsub.clone();
        let password = password.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, engine, pubsub, peer_addr, password).await {
                tracing::debug!("Connection error from {}: {}", peer_addr, e);
            }
        });
    }
}

async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    engine: Arc<CommandEngine>,
    pubsub: Arc<PubSubBroker>,
    peer_addr: std::net::SocketAddr,
    password: Option<Arc<String>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut read_buf = BytesMut::with_capacity(65536);
    let mut write_buf = BytesMut::with_capacity(65536);
    let mut conn = ClientConnection::new(peer_addr, password.is_some());

    loop {
        // ── Phase 1: Decode all complete frames from buffer ──
        let mut frames = Vec::new();
        loop {
            match decode_frame(&read_buf) {
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
                )
            });

        if fast_path {
            // Pre-reserve write buffer for batch responses
            write_buf.reserve(commands.len() * 16);
            for cmd in commands {
                let resp = engine.execute(&cmd.name, &cmd.args);
                encode_frame(&command_response_to_frame(resp), &mut write_buf);
            }
        } else {
            // Slow path: sequential processing with connection state
            for cmd in commands {
                // Handle AUTH command
                if cmd.name == "AUTH" {
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
                                    &RespFrame::error("ERR wrong number of arguments for 'auth' command"),
                                    &mut write_buf,
                                );
                            } else if cmd.args[0] == pass.as_bytes() {
                                conn.authenticated = true;
                                encode_frame(&RespFrame::ok(), &mut write_buf);
                            } else {
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
                            let count =
                                conn.subscriptions.len() + conn.psubscriptions.len();
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
                            let count =
                                conn.subscriptions.len() + conn.psubscriptions.len();
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
                                let pat =
                                    std::str::from_utf8(pat_bytes).unwrap_or("").to_string();
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
                            let name =
                                std::str::from_utf8(&args[0]).unwrap_or("").to_uppercase();
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
                    let response = engine.execute(&cmd.name, &cmd.args);
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

/// Pub/sub delivery loop. Entered when client issues SUBSCRIBE or PSUBSCRIBE.
/// Uses a fan-in mpsc channel: each subscription spawns a forwarding task from
/// broadcast::Receiver -> mpsc::UnboundedSender. The main loop select!s between
/// the fan-in receiver and new frames from the socket.
async fn handle_pubsub_mode(
    socket: &mut tokio::net::TcpStream,
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
    for (_, h) in channel_tasks { h.abort(); }
    for (_, h) in pattern_tasks { h.abort(); }
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
