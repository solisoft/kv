use bytes::{Buf, Bytes, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use solikv_engine::CommandEngine;
use solikv_pubsub::PubSubBroker;
use solikv_resp::codec::{decode_frame, encode_frame, RespFrame};
use solikv_resp::connection::ClientConnection;
use solikv_resp::parser::ParsedCommand;

use solikv_core::CommandResponse;

pub async fn run(
    addr: &str,
    engine: Arc<CommandEngine>,
    pubsub: Arc<PubSubBroker>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("RESP server listening on {}", addr);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        // TCP_NODELAY: disable Nagle's algorithm for lower latency
        let _ = socket.set_nodelay(true);
        let engine = engine.clone();
        let pubsub = pubsub.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, engine, pubsub, peer_addr).await {
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut read_buf = BytesMut::with_capacity(65536);
    let mut write_buf = BytesMut::with_capacity(65536);
    let mut conn = ClientConnection::new(peer_addr);

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
        // Fast path: all commands are pure engine commands (no MULTI/EXEC/SUBSCRIBE)
        // This is the common case for pipelined benchmarks
        let fast_path = !conn.in_transaction
            && commands.iter().all(|cmd| {
                !matches!(
                    cmd.name.as_str(),
                    "MULTI" | "EXEC" | "DISCARD" | "SUBSCRIBE" | "UNSUBSCRIBE" | "QUIT"
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
                match cmd.name.as_str() {
                    "SUBSCRIBE" => {
                        for channel in &cmd.args {
                            conn.subscriptions.push(channel.clone());
                            let _rx = pubsub.subscribe(channel.clone());
                            let count = conn.subscriptions.len();
                            encode_frame(
                                &RespFrame::Array(vec![
                                    RespFrame::BulkString(Bytes::from("subscribe")),
                                    RespFrame::BulkString(channel.clone()),
                                    RespFrame::Integer(count as i64),
                                ]),
                                &mut write_buf,
                            );
                        }
                        continue;
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
                                RespFrame::Integer(conn.subscriptions.len() as i64),
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
