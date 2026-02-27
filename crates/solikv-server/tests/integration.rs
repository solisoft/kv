//! Integration tests that spin up a SoliKV server and test via Redis protocol.

use std::sync::atomic::AtomicU16 as StdAtomicU16;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

async fn start_test_server(port: u16) -> tokio::task::JoinHandle<()> {
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(solikv_engine::ShardManager::new(4));
    let engine = Arc::new(solikv_engine::CommandEngine::new(shards, pubsub.clone()));
    engine.init_self_ref(Arc::downgrade(&engine));

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

// ---- Scripting tests ----

#[tokio::test]
async fn test_eval_return_integer() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: i64 = redis::cmd("EVAL")
        .arg("return 42")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 42);
}

#[tokio::test]
async fn test_eval_return_string() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: String = redis::cmd("EVAL")
        .arg("return 'hello'")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "hello");
}

#[tokio::test]
async fn test_eval_keys_argv() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: String = redis::cmd("EVAL")
        .arg("return KEYS[1]")
        .arg(1)
        .arg("mykey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "mykey");

    let result: String = redis::cmd("EVAL")
        .arg("return ARGV[1]")
        .arg(0)
        .arg("myarg")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "myarg");
}

#[tokio::test]
async fn test_eval_redis_call() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // SET and GET via Lua
    let _: () = redis::cmd("EVAL")
        .arg("redis.call('SET', KEYS[1], ARGV[1])")
        .arg(1)
        .arg("luakey")
        .arg("luaval")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: String = redis::cmd("GET")
        .arg("luakey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "luaval");
}

#[tokio::test]
async fn test_eval_table_return() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: Vec<i64> = redis::cmd("EVAL")
        .arg("return {1, 2, 3}")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_script_load_and_evalsha() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // SCRIPT LOAD
    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg("return 'hello'")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(sha.len(), 40);

    // EVALSHA
    let result: String = redis::cmd("EVALSHA")
        .arg(&sha)
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "hello");
}

#[tokio::test]
async fn test_script_exists_and_flush() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg("return 1")
        .query_async(&mut con)
        .await
        .unwrap();

    // SCRIPT EXISTS — cached sha should return 1, nonexistent should return 0
    let exists: Vec<i64> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(&sha)
        .arg("0000000000000000000000000000000000000000")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(exists, vec![1, 0]);

    // SCRIPT FLUSH
    let _: () = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut con)
        .await
        .unwrap();

    // After flush, sha no longer exists
    let exists: Vec<i64> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(&sha)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(exists, vec![0]);
}

#[tokio::test]
async fn test_evalsha_noscript_error() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: redis::RedisResult<String> = redis::cmd("EVALSHA")
        .arg("0000000000000000000000000000000000000000")
        .arg(0)
        .query_async(&mut con)
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("NOSCRIPT") || err_str.contains("No matching script"),
        "unexpected error: {}",
        err_str
    );
}

// ---- Stream tests ----

#[tokio::test]
async fn test_xadd_xlen() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let id1: String = redis::cmd("XADD").arg("mystream").arg("*").arg("name").arg("Sara").arg("age").arg("30")
        .query_async(&mut con).await.unwrap();
    assert!(id1.contains('-'));

    let id2: String = redis::cmd("XADD").arg("mystream").arg("*").arg("name").arg("John").arg("age").arg("25")
        .query_async(&mut con).await.unwrap();
    assert!(id2.contains('-'));

    let len: i64 = redis::cmd("XLEN").arg("mystream").query_async(&mut con).await.unwrap();
    assert_eq!(len, 2);
}

#[tokio::test]
async fn test_xrange() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: String = redis::cmd("XADD").arg("s").arg("1000-0").arg("k").arg("1")
        .query_async(&mut con).await.unwrap();
    let _: String = redis::cmd("XADD").arg("s").arg("2000-0").arg("k").arg("2")
        .query_async(&mut con).await.unwrap();
    let _: String = redis::cmd("XADD").arg("s").arg("3000-0").arg("k").arg("3")
        .query_async(&mut con).await.unwrap();

    // Full range
    let result: Vec<redis::Value> = redis::cmd("XRANGE").arg("s").arg("-").arg("+")
        .query_async(&mut con).await.unwrap();
    assert_eq!(result.len(), 3);

    // With COUNT
    let result: Vec<redis::Value> = redis::cmd("XRANGE").arg("s").arg("-").arg("+").arg("COUNT").arg("2")
        .query_async(&mut con).await.unwrap();
    assert_eq!(result.len(), 2);
}

#[tokio::test]
async fn test_xread() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: String = redis::cmd("XADD").arg("s").arg("1000-0").arg("k").arg("1")
        .query_async(&mut con).await.unwrap();
    let _: String = redis::cmd("XADD").arg("s").arg("2000-0").arg("k").arg("2")
        .query_async(&mut con).await.unwrap();

    // XREAD returns array of [key, entries]
    let result: Vec<redis::Value> = redis::cmd("XREAD").arg("COUNT").arg("10").arg("STREAMS").arg("s").arg("0-0")
        .query_async(&mut con).await.unwrap();
    assert_eq!(result.len(), 1); // one stream
}

#[tokio::test]
async fn test_xdel_xtrim() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    for i in 1..=5 {
        let id = format!("{}-0", i * 1000);
        let _: String = redis::cmd("XADD").arg("s").arg(&id).arg("k").arg(i.to_string())
            .query_async(&mut con).await.unwrap();
    }

    // XDEL
    let deleted: i64 = redis::cmd("XDEL").arg("s").arg("1000-0")
        .query_async(&mut con).await.unwrap();
    assert_eq!(deleted, 1);

    let len: i64 = redis::cmd("XLEN").arg("s").query_async(&mut con).await.unwrap();
    assert_eq!(len, 4);

    // XTRIM MAXLEN
    let trimmed: i64 = redis::cmd("XTRIM").arg("s").arg("MAXLEN").arg("2")
        .query_async(&mut con).await.unwrap();
    assert_eq!(trimmed, 2);

    let len: i64 = redis::cmd("XLEN").arg("s").query_async(&mut con).await.unwrap();
    assert_eq!(len, 2);
}

#[tokio::test]
async fn test_consumer_groups() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // Add entries
    let _: String = redis::cmd("XADD").arg("s").arg("1000-0").arg("k").arg("1")
        .query_async(&mut con).await.unwrap();
    let _: String = redis::cmd("XADD").arg("s").arg("2000-0").arg("k").arg("2")
        .query_async(&mut con).await.unwrap();

    // Create group
    let _: () = redis::cmd("XGROUP").arg("CREATE").arg("s").arg("grp").arg("0")
        .query_async(&mut con).await.unwrap();

    // Read with consumer
    let result: Vec<redis::Value> = redis::cmd("XREADGROUP")
        .arg("GROUP").arg("grp").arg("consumer1")
        .arg("COUNT").arg("10")
        .arg("STREAMS").arg("s").arg(">")
        .query_async(&mut con).await.unwrap();
    assert_eq!(result.len(), 1); // one stream

    // ACK first entry
    let acked: i64 = redis::cmd("XACK").arg("s").arg("grp").arg("1000-0")
        .query_async(&mut con).await.unwrap();
    assert_eq!(acked, 1);
}

#[tokio::test]
async fn test_stream_type() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: String = redis::cmd("XADD").arg("mystream").arg("*").arg("k").arg("v")
        .query_async(&mut con).await.unwrap();

    let t: String = redis::cmd("TYPE").arg("mystream").query_async(&mut con).await.unwrap();
    assert_eq!(t, "stream");
}

// ---- HyperLogLog tests ----

#[tokio::test]
async fn test_pfadd_pfcount() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let changed: i64 = redis::cmd("PFADD").arg("visitors").arg("alice").arg("bob").arg("charlie").arg("alice")
        .query_async(&mut con).await.unwrap();
    assert_eq!(changed, 1);

    let count: i64 = redis::cmd("PFCOUNT").arg("visitors")
        .query_async(&mut con).await.unwrap();
    assert_eq!(count, 3);

    // Adding existing elements should return 0
    let changed: i64 = redis::cmd("PFADD").arg("visitors").arg("alice")
        .query_async(&mut con).await.unwrap();
    assert_eq!(changed, 0);
}

#[tokio::test]
async fn test_pfmerge() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("PFADD").arg("hll1").arg("a").arg("b").arg("c")
        .query_async(&mut con).await.unwrap();
    let _: i64 = redis::cmd("PFADD").arg("hll2").arg("c").arg("d")
        .query_async(&mut con).await.unwrap();

    let _: () = redis::cmd("PFMERGE").arg("merged").arg("hll1").arg("hll2")
        .query_async(&mut con).await.unwrap();

    let count: i64 = redis::cmd("PFCOUNT").arg("merged")
        .query_async(&mut con).await.unwrap();
    assert_eq!(count, 4);
}

// ---- Bloom Filter tests ----

#[tokio::test]
async fn test_bf_add_exists() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let added: i64 = redis::cmd("BF.ADD").arg("myfilter").arg("hello")
        .query_async(&mut con).await.unwrap();
    assert_eq!(added, 1);

    let exists: i64 = redis::cmd("BF.EXISTS").arg("myfilter").arg("hello")
        .query_async(&mut con).await.unwrap();
    assert_eq!(exists, 1);

    let exists: i64 = redis::cmd("BF.EXISTS").arg("myfilter").arg("world")
        .query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn test_bf_reserve_custom() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("BF.RESERVE").arg("myfilter").arg("0.001").arg("10000")
        .query_async(&mut con).await.unwrap();

    let added: i64 = redis::cmd("BF.ADD").arg("myfilter").arg("test")
        .query_async(&mut con).await.unwrap();
    assert_eq!(added, 1);

    let exists: i64 = redis::cmd("BF.EXISTS").arg("myfilter").arg("test")
        .query_async(&mut con).await.unwrap();
    assert_eq!(exists, 1);

    // BF.INFO should return array
    let info: Vec<redis::Value> = redis::cmd("BF.INFO").arg("myfilter")
        .query_async(&mut con).await.unwrap();
    assert!(!info.is_empty());
}

// ---- Pub/Sub + Keyspace notification tests ----
// These require the real resp_server (with pubsub delivery loop), not the minimal test server.

use bytes::{Buf, BytesMut};
use solikv_resp::codec::{decode_frame, encode_frame, RespFrame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Start a full resp_server that supports SUBSCRIBE/PSUBSCRIBE.
async fn start_full_server(port: u16) -> tokio::task::JoinHandle<()> {
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let notify_flags = Arc::new(StdAtomicU16::new(0));
    let shards = Arc::new(solikv_engine::ShardManager::with_notifications(
        4,
        pubsub.clone(),
        notify_flags.clone(),
    ));
    let engine = Arc::new(
        solikv_engine::CommandEngine::new(shards, pubsub.clone())
            .with_notify_flags(notify_flags),
    );
    engine.init_self_ref(Arc::downgrade(&engine));

    let addr = format!("127.0.0.1:{}", port);
    let handle = tokio::spawn(async move {
        solikv_server::resp_server::run(&addr, engine, pubsub, None)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(100)).await;
    handle
}

/// Send a raw RESP command over a TCP stream and return the decoded response frames.
async fn send_raw_command(stream: &mut TcpStream, args: &[&str]) -> Vec<RespFrame> {
    let mut buf = BytesMut::new();
    // Build RESP array manually
    let frame = RespFrame::Array(
        args.iter()
            .map(|a| RespFrame::BulkString(bytes::Bytes::from(a.to_string())))
            .collect(),
    );
    encode_frame(&frame, &mut buf);
    stream.write_all(&buf).await.unwrap();

    // Read response
    let mut read_buf = BytesMut::with_capacity(4096);
    let mut responses = Vec::new();

    // Give a brief moment, then read available data
    sleep(Duration::from_millis(50)).await;
    let _ = tokio::time::timeout(Duration::from_millis(200), stream.read_buf(&mut read_buf))
        .await;

    loop {
        match decode_frame(&read_buf) {
            Ok(Some((frame, consumed))) => {
                read_buf.advance(consumed);
                responses.push(frame);
            }
            _ => break,
        }
    }
    responses
}

/// Read frames from a stream with a timeout.
async fn read_frames(stream: &mut TcpStream, timeout_ms: u64) -> Vec<RespFrame> {
    let mut read_buf = BytesMut::with_capacity(4096);
    let mut responses = Vec::new();

    let _ = tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        stream.read_buf(&mut read_buf),
    )
    .await;

    loop {
        match decode_frame(&read_buf) {
            Ok(Some((frame, consumed))) => {
                read_buf.advance(consumed);
                responses.push(frame);
            }
            _ => break,
        }
    }
    responses
}

fn frame_to_strings(frame: &RespFrame) -> Vec<String> {
    match frame {
        RespFrame::Array(items) => items.iter().map(|f| match f {
            RespFrame::BulkString(b) => String::from_utf8_lossy(b).to_string(),
            RespFrame::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
            RespFrame::Integer(n) => n.to_string(),
            RespFrame::Null => "(nil)".to_string(),
            _ => format!("{:?}", f),
        }).collect(),
        _ => vec![format!("{:?}", frame)],
    }
}

#[tokio::test]
async fn test_subscribe_message_delivery() {
    let port = next_port();
    let _server = start_full_server(port).await;

    // Subscriber connection
    let mut sub = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    // Publisher connection
    let mut pub_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    // Subscribe to "test-ch"
    let responses = send_raw_command(&mut sub, &["SUBSCRIBE", "test-ch"]).await;
    assert!(!responses.is_empty());
    let sub_ack = frame_to_strings(&responses[0]);
    assert_eq!(sub_ack[0], "subscribe");
    assert_eq!(sub_ack[1], "test-ch");
    assert_eq!(sub_ack[2], "1");

    // Publish a message
    let _pub_responses = send_raw_command(&mut pub_conn, &["PUBLISH", "test-ch", "hello-world"]).await;

    // Read the delivered message
    let messages = read_frames(&mut sub, 500).await;
    assert!(!messages.is_empty(), "Expected to receive a message");

    let msg = frame_to_strings(&messages[0]);
    assert_eq!(msg[0], "message");
    assert_eq!(msg[1], "test-ch");
    assert_eq!(msg[2], "hello-world");
}

#[tokio::test]
async fn test_psubscribe_pattern_delivery() {
    let port = next_port();
    let _server = start_full_server(port).await;

    let mut sub = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let mut pub_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    // PSUBSCRIBE to "test-*"
    let responses = send_raw_command(&mut sub, &["PSUBSCRIBE", "test-*"]).await;
    assert!(!responses.is_empty());
    let sub_ack = frame_to_strings(&responses[0]);
    assert_eq!(sub_ack[0], "psubscribe");
    assert_eq!(sub_ack[1], "test-*");

    // Publish to "test-foo"
    let _pub_responses = send_raw_command(&mut pub_conn, &["PUBLISH", "test-foo", "bar"]).await;

    // Should receive pmessage
    let messages = read_frames(&mut sub, 500).await;
    assert!(!messages.is_empty(), "Expected to receive a pmessage");

    let msg = frame_to_strings(&messages[0]);
    assert_eq!(msg[0], "pmessage");
    assert_eq!(msg[1], "test-*");  // pattern
    assert_eq!(msg[2], "test-foo"); // actual channel
    assert_eq!(msg[3], "bar");     // message
}

#[tokio::test]
async fn test_keyspace_notification_on_set() {
    let port = next_port();
    let _server = start_full_server(port).await;

    let mut sub = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let mut cmd_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    // Enable all keyspace notifications via CONFIG SET
    let _ = send_raw_command(&mut cmd_conn, &["CONFIG", "SET", "notify-keyspace-events", "KEA"]).await;

    // Subscribe to keyevent channel for SET events
    let responses = send_raw_command(&mut sub, &["PSUBSCRIBE", "__keyevent@0__:*"]).await;
    assert!(!responses.is_empty());

    // Execute SET
    let _ = send_raw_command(&mut cmd_conn, &["SET", "nk-mykey", "myvalue"]).await;

    // Should receive keyevent notification for "set"
    let messages = read_frames(&mut sub, 500).await;
    assert!(!messages.is_empty(), "Expected to receive a keyevent notification");

    let msg = frame_to_strings(&messages[0]);
    assert_eq!(msg[0], "pmessage");
    assert_eq!(msg[1], "__keyevent@0__:*");
    assert_eq!(msg[2], "__keyevent@0__:set");
    assert_eq!(msg[3], "nk-mykey");
}

#[tokio::test]
async fn test_keyspace_notification_on_del() {
    let port = next_port();
    let _server = start_full_server(port).await;

    let mut sub = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let mut cmd_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    // Enable notifications
    let _ = send_raw_command(&mut cmd_conn, &["CONFIG", "SET", "notify-keyspace-events", "KEg"]).await;

    // Create a key
    let _ = send_raw_command(&mut cmd_conn, &["SET", "delkey", "val"]).await;

    // Subscribe to keyevent for DEL
    let responses = send_raw_command(&mut sub, &["SUBSCRIBE", "__keyevent@0__:del"]).await;
    assert!(!responses.is_empty());

    // Delete the key
    let _ = send_raw_command(&mut cmd_conn, &["DEL", "delkey"]).await;

    // Should receive del notification
    let messages = read_frames(&mut sub, 500).await;
    assert!(!messages.is_empty(), "Expected del notification");

    let msg = frame_to_strings(&messages[0]);
    assert_eq!(msg[0], "message");
    assert_eq!(msg[1], "__keyevent@0__:del");
    assert_eq!(msg[2], "delkey");
}

#[tokio::test]
async fn test_keyspace_notification_disabled_by_default() {
    let port = next_port();
    let _server = start_full_server(port).await;

    let mut sub = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let mut cmd_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    // Subscribe without enabling notifications
    let _responses = send_raw_command(&mut sub, &["PSUBSCRIBE", "__keyevent@0__:*"]).await;

    // Execute SET
    let _ = send_raw_command(&mut cmd_conn, &["SET", "nonotify", "val"]).await;

    // Should NOT receive any notification
    let messages = read_frames(&mut sub, 300).await;
    assert!(messages.is_empty(), "Expected no notifications when disabled");
}

#[tokio::test]
async fn test_config_get_notify_keyspace_events() {
    let port = next_port();
    let _server = start_full_server(port).await;

    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    // Default: empty
    let responses = send_raw_command(&mut conn, &["CONFIG", "GET", "notify-keyspace-events"]).await;
    assert!(!responses.is_empty());
    let vals = frame_to_strings(&responses[0]);
    assert_eq!(vals[0], "notify-keyspace-events");
    assert_eq!(vals[1], "");

    // Set to KEA and verify
    let _ = send_raw_command(&mut conn, &["CONFIG", "SET", "notify-keyspace-events", "KEA"]).await;
    let responses = send_raw_command(&mut conn, &["CONFIG", "GET", "notify-keyspace-events"]).await;
    let vals = frame_to_strings(&responses[0]);
    assert_eq!(vals[0], "notify-keyspace-events");
    // Should contain K, E, and various type flags
    assert!(vals[1].contains('K'));
    assert!(vals[1].contains('E'));
}

// ---- Bitmap tests ----

#[tokio::test]
async fn test_setbit_getbit() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // SETBIT on bit 7 (last bit of first byte) should return old value 0
    let old: i64 = redis::cmd("SETBIT").arg("bm").arg(7).arg(1)
        .query_async(&mut con).await.unwrap();
    assert_eq!(old, 0);

    // GETBIT should return 1
    let bit: i64 = redis::cmd("GETBIT").arg("bm").arg(7)
        .query_async(&mut con).await.unwrap();
    assert_eq!(bit, 1);

    // GETBIT on unset bit should return 0
    let bit: i64 = redis::cmd("GETBIT").arg("bm").arg(0)
        .query_async(&mut con).await.unwrap();
    assert_eq!(bit, 0);

    // The underlying value should be "\x01" (bit 7 = 1 in big-endian)
    let val: Vec<u8> = redis::cmd("GET").arg("bm")
        .query_async(&mut con).await.unwrap();
    assert_eq!(val, vec![0x01]);
}

#[tokio::test]
async fn test_bitcount() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // Set several bits
    let _: i64 = redis::cmd("SETBIT").arg("bm").arg(0).arg(1).query_async(&mut con).await.unwrap();
    let _: i64 = redis::cmd("SETBIT").arg("bm").arg(1).arg(1).query_async(&mut con).await.unwrap();
    let _: i64 = redis::cmd("SETBIT").arg("bm").arg(7).arg(1).query_async(&mut con).await.unwrap();
    let _: i64 = redis::cmd("SETBIT").arg("bm").arg(8).arg(1).query_async(&mut con).await.unwrap();

    // Total: 4 bits set
    let count: i64 = redis::cmd("BITCOUNT").arg("bm")
        .query_async(&mut con).await.unwrap();
    assert_eq!(count, 4);

    // Count only first byte (byte 0): bits 0, 1, 7 = 3
    let count: i64 = redis::cmd("BITCOUNT").arg("bm").arg(0).arg(0)
        .query_async(&mut con).await.unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_bitop() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // Set k1 = 0xFF, k2 = 0x0F
    let _: () = redis::cmd("SET").arg("k1").arg(vec![0xFFu8]).query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("k2").arg(vec![0x0Fu8]).query_async(&mut con).await.unwrap();

    // AND
    let len: i64 = redis::cmd("BITOP").arg("AND").arg("and_dest").arg("k1").arg("k2")
        .query_async(&mut con).await.unwrap();
    assert_eq!(len, 1);
    let val: Vec<u8> = redis::cmd("GET").arg("and_dest").query_async(&mut con).await.unwrap();
    assert_eq!(val, vec![0x0F]);

    // OR
    let len: i64 = redis::cmd("BITOP").arg("OR").arg("or_dest").arg("k1").arg("k2")
        .query_async(&mut con).await.unwrap();
    assert_eq!(len, 1);
    let val: Vec<u8> = redis::cmd("GET").arg("or_dest").query_async(&mut con).await.unwrap();
    assert_eq!(val, vec![0xFF]);

    // XOR
    let len: i64 = redis::cmd("BITOP").arg("XOR").arg("xor_dest").arg("k1").arg("k2")
        .query_async(&mut con).await.unwrap();
    assert_eq!(len, 1);
    let val: Vec<u8> = redis::cmd("GET").arg("xor_dest").query_async(&mut con).await.unwrap();
    assert_eq!(val, vec![0xF0]);

    // NOT
    let len: i64 = redis::cmd("BITOP").arg("NOT").arg("not_dest").arg("k2")
        .query_async(&mut con).await.unwrap();
    assert_eq!(len, 1);
    let val: Vec<u8> = redis::cmd("GET").arg("not_dest").query_async(&mut con).await.unwrap();
    assert_eq!(val, vec![0xF0]);
}

// ---- Geospatial tests ----

#[tokio::test]
async fn test_geoadd_geopos() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let added: i64 = redis::cmd("GEOADD")
        .arg("mygeo")
        .arg("13.361389").arg("38.115556").arg("Palermo")
        .arg("15.087269").arg("37.502669").arg("Catania")
        .query_async(&mut con).await.unwrap();
    assert_eq!(added, 2);

    // GEOPOS returns nested arrays [[lon, lat], [lon, lat]]
    let pos: Vec<Vec<String>> = redis::cmd("GEOPOS")
        .arg("mygeo").arg("Palermo")
        .query_async(&mut con).await.unwrap();
    assert_eq!(pos.len(), 1);
    let lon: f64 = pos[0][0].parse().unwrap();
    let lat: f64 = pos[0][1].parse().unwrap();
    assert!((lon - 13.361389).abs() < 0.001, "lon: {}", lon);
    assert!((lat - 38.115556).abs() < 0.001, "lat: {}", lat);
}

#[tokio::test]
async fn test_geodist() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("GEOADD")
        .arg("mygeo")
        .arg("13.361389").arg("38.115556").arg("Palermo")
        .arg("15.087269").arg("37.502669").arg("Catania")
        .query_async(&mut con).await.unwrap();

    // Default unit (meters)
    let dist: String = redis::cmd("GEODIST")
        .arg("mygeo").arg("Palermo").arg("Catania")
        .query_async(&mut con).await.unwrap();
    let dist_m: f64 = dist.parse().unwrap();
    assert!(dist_m > 160000.0 && dist_m < 170000.0, "dist_m: {}", dist_m);

    // Kilometers
    let dist: String = redis::cmd("GEODIST")
        .arg("mygeo").arg("Palermo").arg("Catania").arg("km")
        .query_async(&mut con).await.unwrap();
    let dist_km: f64 = dist.parse().unwrap();
    assert!((dist_km - 166.274).abs() < 1.0, "dist_km: {}", dist_km);
}

#[tokio::test]
async fn test_geosearch_radius() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("GEOADD")
        .arg("mygeo")
        .arg("13.361389").arg("38.115556").arg("Palermo")
        .arg("15.087269").arg("37.502669").arg("Catania")
        .arg("2.349014").arg("48.864716").arg("Paris")
        .query_async(&mut con).await.unwrap();

    // Search within 200km of (15, 37) — should find Catania and Palermo, not Paris
    let results: Vec<String> = redis::cmd("GEOSEARCH")
        .arg("mygeo")
        .arg("FROMLONLAT").arg("15").arg("37")
        .arg("BYRADIUS").arg("200").arg("km")
        .arg("ASC")
        .query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], "Catania");
    assert_eq!(results[1], "Palermo");
}

#[tokio::test]
async fn test_geohash() {
    let port = next_port();
    let _server = start_test_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("GEOADD")
        .arg("mygeo")
        .arg("13.361389").arg("38.115556").arg("Palermo")
        .query_async(&mut con).await.unwrap();

    let hashes: Vec<String> = redis::cmd("GEOHASH")
        .arg("mygeo").arg("Palermo")
        .query_async(&mut con).await.unwrap();
    assert_eq!(hashes.len(), 1);
    assert_eq!(hashes[0].len(), 11);
    // Palermo geohash should start with "sqc8b49"
    assert!(hashes[0].starts_with("sqc8b49"), "hash: {}", hashes[0]);
}
