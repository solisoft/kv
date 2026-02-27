//! Integration tests that spin up a SoliKV server and test via Redis protocol.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

async fn start_test_server(port: u16) -> tokio::task::JoinHandle<()> {
    let shards = Arc::new(solikv_engine::ShardManager::new(4));
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let engine = Arc::new(solikv_engine::CommandEngine::new(shards, pubsub.clone()));

    let addr = format!("127.0.0.1:{}", port);
    let handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
        loop {
            let (mut socket, _peer_addr) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => break,
            };
            let engine = engine.clone();
            tokio::spawn(async move {
                use bytes::{Buf, Bytes, BytesMut};
                use solikv_resp::codec::{decode_frame, encode_frame, RespFrame};
                use solikv_resp::parser::ParsedCommand;
                use solikv_core::CommandResponse;
                use tokio::io::{AsyncReadExt, AsyncWriteExt};

                fn to_frame(r: CommandResponse) -> RespFrame {
                    match r {
                        CommandResponse::Ok => RespFrame::ok(),
                        CommandResponse::Nil => RespFrame::Null,
                        CommandResponse::Integer(n) => RespFrame::Integer(n),
                        CommandResponse::BulkString(b) => RespFrame::BulkString(b),
                        CommandResponse::SimpleString(s) => RespFrame::SimpleString(s),
                        CommandResponse::Array(items) => RespFrame::Array(items.into_iter().map(to_frame).collect()),
                        CommandResponse::Error(msg) => RespFrame::Error(msg),
                        CommandResponse::Queued => RespFrame::SimpleString(Bytes::from("QUEUED")),
                    }
                }

                let mut read_buf = BytesMut::with_capacity(65536);
                let mut write_buf = BytesMut::with_capacity(65536);

                loop {
                    // Decode all complete frames
                    let mut frames = Vec::new();
                    loop {
                        match decode_frame(&read_buf) {
                            Ok(Some((frame, consumed))) => {
                                read_buf.advance(consumed);
                                frames.push(frame);
                            }
                            Ok(None) => break,
                            Err(_) => return,
                        }
                    }

                    if frames.is_empty() {
                        let n = match socket.read_buf(&mut read_buf).await {
                            Ok(n) => n,
                            Err(_) => return,
                        };
                        if n == 0 { return; }
                        continue;
                    }

                    for frame in frames {
                        let cmd = match ParsedCommand::from_frame(frame) {
                            Ok(cmd) => cmd,
                            Err(e) => {
                                encode_frame(&RespFrame::error(e), &mut write_buf);
                                continue;
                            }
                        };

                        if cmd.name == "QUIT" {
                            encode_frame(&RespFrame::ok(), &mut write_buf);
                            let _ = socket.write_all(&write_buf).await;
                            return;
                        }

                        let response = engine.execute(&cmd.name, &cmd.args);
                        encode_frame(&to_frame(response), &mut write_buf);
                    }

                    if !write_buf.is_empty() {
                        if socket.write_all(&write_buf).await.is_err() {
                            return;
                        }
                        write_buf.clear();
                    }
                }
            });
        }
    });

    sleep(Duration::from_millis(100)).await;
    handle
}

use std::sync::atomic::{AtomicU16, Ordering};
static PORT_COUNTER: AtomicU16 = AtomicU16::new(16379);
fn next_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[tokio::test]
async fn test_ping() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: String = redis::cmd("PING").query_async(&mut con).await.unwrap();
    assert_eq!(result, "PONG");
}

#[tokio::test]
async fn test_set_get() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET").arg("mykey").arg("myvalue").query_async(&mut con).await.unwrap();
    let result: String = redis::cmd("GET").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, "myvalue");
}

#[tokio::test]
async fn test_set_with_expiry() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET")
        .arg("mykey").arg("myvalue").arg("EX").arg("10")
        .query_async(&mut con).await.unwrap();

    let result: String = redis::cmd("GET").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, "myvalue");

    let ttl: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);
}

#[tokio::test]
async fn test_incr_decr() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: i64 = redis::cmd("INCR").arg("counter").query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);
    let result: i64 = redis::cmd("INCR").arg("counter").query_async(&mut con).await.unwrap();
    assert_eq!(result, 2);
    let result: i64 = redis::cmd("DECR").arg("counter").query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);
}

#[tokio::test]
async fn test_del_exists() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET").arg("key1").arg("val1").query_async(&mut con).await.unwrap();
    let exists: i64 = redis::cmd("EXISTS").arg("key1").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 1);

    let deleted: i64 = redis::cmd("DEL").arg("key1").query_async(&mut con).await.unwrap();
    assert_eq!(deleted, 1);

    let exists: i64 = redis::cmd("EXISTS").arg("key1").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn test_list_operations() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("RPUSH").arg("mylist").arg("a").arg("b").arg("c")
        .query_async(&mut con).await.unwrap();
    let len: i64 = redis::cmd("LLEN").arg("mylist").query_async(&mut con).await.unwrap();
    assert_eq!(len, 3);

    let range: Vec<String> = redis::cmd("LRANGE").arg("mylist").arg(0).arg(-1)
        .query_async(&mut con).await.unwrap();
    assert_eq!(range, vec!["a", "b", "c"]);

    let popped: String = redis::cmd("LPOP").arg("mylist").query_async(&mut con).await.unwrap();
    assert_eq!(popped, "a");
}

#[tokio::test]
async fn test_hash_operations() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("HSET").arg("myhash").arg("field1").arg("value1").arg("field2").arg("value2")
        .query_async(&mut con).await.unwrap();

    let val: String = redis::cmd("HGET").arg("myhash").arg("field1")
        .query_async(&mut con).await.unwrap();
    assert_eq!(val, "value1");

    let len: i64 = redis::cmd("HLEN").arg("myhash").query_async(&mut con).await.unwrap();
    assert_eq!(len, 2);
}

#[tokio::test]
async fn test_set_operations() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let added: i64 = redis::cmd("SADD").arg("myset").arg("a").arg("b").arg("c").arg("a")
        .query_async(&mut con).await.unwrap();
    assert_eq!(added, 3);

    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 3);

    let is_member: i64 = redis::cmd("SISMEMBER").arg("myset").arg("a")
        .query_async(&mut con).await.unwrap();
    assert_eq!(is_member, 1);
}

#[tokio::test]
async fn test_sorted_set_operations() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("ZADD").arg("myzset")
        .arg("1").arg("a")
        .arg("2").arg("b")
        .arg("3").arg("c")
        .query_async(&mut con).await.unwrap();

    let card: i64 = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 3);

    let range: Vec<String> = redis::cmd("ZRANGE").arg("myzset").arg(0).arg(-1)
        .query_async(&mut con).await.unwrap();
    assert_eq!(range, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn test_publish() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let count: i64 = redis::cmd("PUBLISH").arg("channel1").arg("hello")
        .query_async(&mut con).await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_dbsize() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET").arg("k1").arg("v1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("k2").arg("v2").query_async(&mut con).await.unwrap();

    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert!(size >= 2);
}

#[tokio::test]
async fn test_type_command() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET").arg("mystr").arg("hello").query_async(&mut con).await.unwrap();
    let _: i64 = redis::cmd("RPUSH").arg("mylist").arg("a").query_async(&mut con).await.unwrap();

    let t: String = redis::cmd("TYPE").arg("mystr").query_async(&mut con).await.unwrap();
    assert_eq!(t, "string");

    let t: String = redis::cmd("TYPE").arg("mylist").query_async(&mut con).await.unwrap();
    assert_eq!(t, "list");
}

#[tokio::test]
async fn test_append_strlen() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET").arg("mykey").arg("hello").query_async(&mut con).await.unwrap();
    let len: i64 = redis::cmd("APPEND").arg("mykey").arg(" world").query_async(&mut con).await.unwrap();
    assert_eq!(len, 11);

    let strlen: i64 = redis::cmd("STRLEN").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(strlen, 11);

    let val: String = redis::cmd("GET").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(val, "hello world");
}

#[tokio::test]
async fn test_echo() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: String = redis::cmd("ECHO").arg("hello").query_async(&mut con).await.unwrap();
    assert_eq!(result, "hello");
}

#[tokio::test]
async fn test_info() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let info: String = redis::cmd("INFO").query_async(&mut con).await.unwrap();
    assert!(info.contains("solikv_version"));
}

#[tokio::test]
async fn test_getrange() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET").arg("mykey").arg("hello world").query_async(&mut con).await.unwrap();

    let result: String = redis::cmd("GETRANGE").arg("mykey").arg(0).arg(4)
        .query_async(&mut con).await.unwrap();
    assert_eq!(result, "hello");

    let result: String = redis::cmd("GETRANGE").arg("mykey").arg(-5).arg(-1)
        .query_async(&mut con).await.unwrap();
    assert_eq!(result, "world");
}

#[tokio::test]
async fn test_setnx_setex() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: i64 = redis::cmd("SETNX").arg("mykey").arg("v1").query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);
    let result: i64 = redis::cmd("SETNX").arg("mykey").arg("v2").query_async(&mut con).await.unwrap();
    assert_eq!(result, 0);

    let val: String = redis::cmd("GET").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(val, "v1");
}

#[tokio::test]
async fn test_incrby_decrby() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET").arg("num").arg("10").query_async(&mut con).await.unwrap();
    let result: i64 = redis::cmd("INCRBY").arg("num").arg(5).query_async(&mut con).await.unwrap();
    assert_eq!(result, 15);
    let result: i64 = redis::cmd("DECRBY").arg("num").arg(3).query_async(&mut con).await.unwrap();
    assert_eq!(result, 12);
}

#[tokio::test]
async fn test_expire_persist() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET").arg("mykey").arg("hello").query_async(&mut con).await.unwrap();
    let result: i64 = redis::cmd("EXPIRE").arg("mykey").arg(10).query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);

    let ttl: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);

    let result: i64 = redis::cmd("PERSIST").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);

    let ttl: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(ttl, -1);
}

#[tokio::test]
async fn test_flushdb() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET").arg("k1").arg("v1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("k2").arg("v2").query_async(&mut con).await.unwrap();

    let _: () = redis::cmd("FLUSHDB").query_async(&mut con).await.unwrap();
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 0);
}
