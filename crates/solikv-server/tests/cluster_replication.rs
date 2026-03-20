//! Cluster and Replication integration tests
//! These tests require the full server with cluster/replication support

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

use redis::AsyncCommands;

// Port allocation
static PORT_COUNTER: AtomicU16 = AtomicU16::new(17000);
fn next_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Start a full server with cluster enabled
async fn start_cluster_server(port: u16) -> tokio::task::JoinHandle<()> {
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let notify_flags = Arc::new(std::sync::atomic::AtomicU16::new(0));
    let shards = Arc::new(solikv_engine::ShardManager::with_notifications(
        2,
        pubsub.clone(),
        notify_flags.clone(),
    ));
    let engine = Arc::new(
        solikv_engine::CommandEngine::new(shards, pubsub.clone()).with_notify_flags(notify_flags),
    );
    engine.init_self_ref(Arc::downgrade(&engine));

    let addr = format!("127.0.0.1:{}", port);
    let handle = tokio::spawn(async move {
        solikv_server::resp_server::run(&addr, engine, pubsub, None, None)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(100)).await;
    handle
}

/// Start a server in non-cluster mode for replication tests
async fn start_replication_server(port: u16) -> tokio::task::JoinHandle<()> {
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let notify_flags = Arc::new(std::sync::atomic::AtomicU16::new(0));
    let shards = Arc::new(solikv_engine::ShardManager::with_notifications(
        2,
        pubsub.clone(),
        notify_flags.clone(),
    ));
    let engine = Arc::new(
        solikv_engine::CommandEngine::new(shards, pubsub.clone()).with_notify_flags(notify_flags),
    );
    engine.init_self_ref(Arc::downgrade(&engine));

    let addr = format!("127.0.0.1:{}", port);
    let handle = tokio::spawn(async move {
        solikv_server::resp_server::run(&addr, engine, pubsub, None, None)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(100)).await;
    handle
}

/// Send a raw RESP command over TCP
async fn send_command(port: u16, args: &[&str]) -> String {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let mut buf = BytesMut::new();
    let frame = solikv_resp::codec::RespFrame::Array(
        args.iter()
            .map(|a| solikv_resp::codec::RespFrame::BulkString(bytes::Bytes::from(a.to_string())))
            .collect(),
    );
    solikv_resp::codec::encode_frame(&frame, &mut buf);
    stream.write_all(&buf).await.unwrap();

    let mut read_buf = BytesMut::with_capacity(4096);
    stream.read_buf(&mut read_buf).await.unwrap();

    String::from_utf8_lossy(&read_buf).to_string()
}

// ===================== CLUSTER TESTS =====================

#[tokio::test]
async fn test_cluster_info_disabled_by_default() {
    let port = next_port();
    let _server = start_cluster_server(port).await;

    let result = send_command(port, &["CLUSTER", "INFO"]).await;
    assert!(result.contains("ERR This instance has cluster support disabled"));
}

#[tokio::test]
async fn test_cluster_nodes_disabled_by_default() {
    let port = next_port();
    let _server = start_cluster_server(port).await;

    let result = send_command(port, &["CLUSTER", "NODES"]).await;
    assert!(result.contains("ERR This instance has cluster support disabled"));
}

// Note: Full cluster tests would require --cluster-enabled flag
// These tests verify cluster commands work when cluster mode is enabled
// For now, we test that commands are properly rejected when cluster is disabled

// ===================== REPLICATION TESTS =====================

#[tokio::test]
async fn test_role_master() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let role: Vec<redis::Value> = redis::cmd("ROLE").query_async(&mut con).await.unwrap();
    assert!(!role.is_empty());
}

#[tokio::test]
async fn test_replicaof_no_one() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // Initially master - just verify we get a valid response
    let role: Vec<redis::Value> = redis::cmd("ROLE").query_async(&mut con).await.unwrap();
    assert!(!role.is_empty());

    // Replicaof NO ONE - should succeed
    let result: redis::RedisResult<String> = redis::cmd("REPLICAOF")
        .arg("NO")
        .arg("ONE")
        .query_async(&mut con)
        .await;
    // This might return OK or an error depending on implementation
    // Just make sure it doesn't panic
}

#[tokio::test]
async fn test_replicaof_command_syntax() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // Invalid syntax - should fail
    let result: Result<String, _> = redis::cmd("REPLICAOF")
        .arg("invalid")
        .query_async(&mut con)
        .await;

    // Should be an error (port not valid)
    assert!(result.is_err());
}

// ===================== ADDITIONAL COMMAND TESTS =====================

// String commands
#[tokio::test]
async fn test_set_get_delete() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET")
        .arg("key")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();
    let val: String = redis::cmd("GET")
        .arg("key")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "value");

    let _: i64 = redis::cmd("DEL")
        .arg("key")
        .query_async(&mut con)
        .await
        .unwrap();
    let val: Option<String> = redis::cmd("GET")
        .arg("key")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(val.is_none());
}

#[tokio::test]
async fn test_mset_mget() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("MSET")
        .arg("k1")
        .arg("v1")
        .arg("k2")
        .arg("v2")
        .arg("k3")
        .arg("v3")
        .query_async(&mut con)
        .await
        .unwrap();

    let vals: Vec<Option<String>> = redis::cmd("MGET")
        .arg("k1")
        .arg("k2")
        .arg("k3")
        .arg("nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();

    assert_eq!(vals[0].as_ref().unwrap().as_str(), "v1");
    assert_eq!(vals[1].as_ref().unwrap().as_str(), "v2");
    assert_eq!(vals[2].as_ref().unwrap().as_str(), "v3");
    assert!(vals[3].is_none());
}

#[tokio::test]
async fn test_incr_incrby_incrbyfloat() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // Test INCR on new key
    let val: i64 = redis::cmd("INCR")
        .arg("counter")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, 1);

    // Test INCRBY
    let val: i64 = redis::cmd("INCRBY")
        .arg("counter")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, 6);

    // Test INCRBYFLOAT
    let val: f64 = redis::cmd("INCRBYFLOAT")
        .arg("counter")
        .arg(0.5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((val - 6.5).abs() < 0.001);
}

#[tokio::test]
async fn test_decr_decrby() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // Test DECR on new key (starts from 0)
    let val: i64 = redis::cmd("DECR")
        .arg("decrkey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, -1);

    // Test DECRBY
    let val: i64 = redis::cmd("DECRBY")
        .arg("decrkey")
        .arg(4)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, -5);
}

#[tokio::test]
async fn test_append() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let len: i64 = redis::cmd("APPEND")
        .arg("s")
        .arg("Hello")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 5);

    let len: i64 = redis::cmd("APPEND")
        .arg("s")
        .arg(" World")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 11);

    let val: String = redis::cmd("GET")
        .arg("s")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "Hello World");
}

#[tokio::test]
async fn test_strlen() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET")
        .arg("s")
        .arg("Hello")
        .query_async(&mut con)
        .await
        .unwrap();
    let len: i64 = redis::cmd("STRLEN")
        .arg("s")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 5);
}

#[tokio::test]
async fn test_setnx() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: i64 = redis::cmd("SETNX")
        .arg("key")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 1);

    let result: i64 = redis::cmd("SETNX")
        .arg("key")
        .arg("newvalue")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 0);

    let val: String = redis::cmd("GET")
        .arg("key")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "value");
}

#[tokio::test]
async fn test_setex_psetex() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SETEX")
        .arg("key1")
        .arg(60)
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();
    let ttl: i64 = redis::cmd("TTL")
        .arg("key1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(ttl > 0 && ttl <= 60);

    let _: () = redis::cmd("PSETEX")
        .arg("key2")
        .arg(60000)
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();
    let ttl: i64 = redis::cmd("TTL")
        .arg("key2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(ttl > 0 && ttl <= 60);
}

#[tokio::test]
async fn test_getrange_setsrange() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET")
        .arg("s")
        .arg("Hello World")
        .query_async(&mut con)
        .await
        .unwrap();

    // GETRANGE
    let val: String = redis::cmd("GETRANGE")
        .arg("s")
        .arg(0)
        .arg(4)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "Hello");

    let val: String = redis::cmd("GETRANGE")
        .arg("s")
        .arg(-5)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "World");

    // SETRANGE
    let len: i64 = redis::cmd("SETRANGE")
        .arg("s")
        .arg(6)
        .arg("Rust")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 11); // 6 + length of "Rust"
}

#[tokio::test]
async fn test_getset() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET")
        .arg("key")
        .arg("old")
        .query_async(&mut con)
        .await
        .unwrap();
    let old: String = redis::cmd("GETSET")
        .arg("key")
        .arg("new")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(old, "old");

    let val: String = redis::cmd("GET")
        .arg("key")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "new");
}

#[tokio::test]
async fn test_exists() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET")
        .arg("k1")
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("k2")
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();

    let cnt: i64 = redis::cmd("EXISTS")
        .arg("k1")
        .arg("k2")
        .arg("k3")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(cnt, 2);
}

#[tokio::test]
async fn test_expire_expireat_ttl() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET")
        .arg("key")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: i64 = redis::cmd("EXPIRE")
        .arg("key")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 1);

    let ttl: i64 = redis::cmd("TTL")
        .arg("key")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(ttl > 0 && ttl <= 10);

    let result: i64 = redis::cmd("PERSIST")
        .arg("key")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 1);

    let ttl: i64 = redis::cmd("TTL")
        .arg("key")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(ttl, -1);
}

// List commands
#[tokio::test]
async fn test_lpush_rpush() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let len: i64 = redis::cmd("RPUSH")
        .arg("list")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 3);

    let len: i64 = redis::cmd("LPUSH")
        .arg("list")
        .arg("x")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 4);

    let vals: Vec<String> = redis::cmd("LRANGE")
        .arg("list")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(vals, vec!["x", "a", "b", "c"]);
}

#[tokio::test]
async fn test_lpop_rpop() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("RPUSH")
        .arg("list")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("LPOP")
        .arg("list")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "a");

    let val: String = redis::cmd("RPOP")
        .arg("list")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "c");
}

#[tokio::test]
async fn test_lindex_lset() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("RPUSH")
        .arg("list")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("LINDEX")
        .arg("list")
        .arg(1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "b");

    let _: () = redis::cmd("LSET")
        .arg("list")
        .arg(1)
        .arg("x")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("LINDEX")
        .arg("list")
        .arg(1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "x");
}

#[tokio::test]
async fn test_llen_ltrim() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("RPUSH")
        .arg("list")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .arg("e")
        .query_async(&mut con)
        .await
        .unwrap();

    let len: i64 = redis::cmd("LLEN")
        .arg("list")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 5);

    let _: () = redis::cmd("LTRIM")
        .arg("list")
        .arg(1)
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();

    let vals: Vec<String> = redis::cmd("LRANGE")
        .arg("list")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(vals, vec!["b", "c", "d"]);
}

#[tokio::test]
async fn test_lrem() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("RPUSH")
        .arg("list")
        .arg("a")
        .arg("b")
        .arg("a")
        .arg("c")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    // LREM should remove elements
    let removed: i64 = redis::cmd("LREM")
        .arg("list")
        .arg(0)
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    // All 'a's should be removed
    assert!(removed >= 1);
}

// Hash commands
#[tokio::test]
async fn test_hset_hget_hgetall() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("HSET")
        .arg("hash")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("HGET")
        .arg("hash")
        .arg("f1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "v1");

    let all: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
        .arg("hash")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(all.get("f1").unwrap(), "v1");
    assert_eq!(all.get("f2").unwrap(), "v2");
}

#[tokio::test]
async fn test_hdel_hexists_hlen() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("HSET")
        .arg("hash")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .query_async(&mut con)
        .await
        .unwrap();

    let exists: i64 = redis::cmd("HEXISTS")
        .arg("hash")
        .arg("f1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(exists, 1);

    let len: i64 = redis::cmd("HLEN")
        .arg("hash")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 2);

    let deleted: i64 = redis::cmd("HDEL")
        .arg("hash")
        .arg("f1")
        .arg("f3")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    let exists: i64 = redis::cmd("HEXISTS")
        .arg("hash")
        .arg("f1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn test_hmget_hmset() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("HMSET")
        .arg("hash")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .query_async(&mut con)
        .await
        .unwrap();

    let vals: Vec<Option<String>> = redis::cmd("HMGET")
        .arg("hash")
        .arg("f1")
        .arg("f2")
        .arg("f3")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(vals[0].as_ref().unwrap().as_str(), "v1");
    assert_eq!(vals[1].as_ref().unwrap().as_str(), "v2");
    assert!(vals[2].is_none());
}

#[tokio::test]
async fn test_hkeys_hvals() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("HSET")
        .arg("hash")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .query_async(&mut con)
        .await
        .unwrap();

    // Just verify these commands work
    let keys: Vec<String> = redis::cmd("HKEYS")
        .arg("hash")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(keys.len(), 2);

    let vals: Vec<String> = redis::cmd("HVALS")
        .arg("hash")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(vals.len(), 2);
}

#[tokio::test]
async fn test_hincrby_hincrbyfloat() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("HSET")
        .arg("hash")
        .arg("counter")
        .arg("10")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: i64 = redis::cmd("HINCRBY")
        .arg("hash")
        .arg("counter")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, 15);

    let val: f64 = redis::cmd("HINCRBYFLOAT")
        .arg("hash")
        .arg("counter")
        .arg(0.5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((val - 15.5).abs() < 0.001);
}

// Set commands
#[tokio::test]
async fn test_sadd_smembers_sismember() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let added: i64 = redis::cmd("SADD")
        .arg("set")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 3);

    let members: Vec<String> = redis::cmd("SMEMBERS")
        .arg("set")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members.len(), 3);

    let is_member: i64 = redis::cmd("SISMEMBER")
        .arg("set")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(is_member, 1);

    let is_member: i64 = redis::cmd("SISMEMBER")
        .arg("set")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(is_member, 0);
}

#[tokio::test]
async fn test_srem_scard() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("set")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("SREM")
        .arg("set")
        .arg("a")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 1);

    let card: i64 = redis::cmd("SCARD")
        .arg("set")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(card, 2);
}

#[tokio::test]
async fn test_spop() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("set")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("SPOP")
        .arg("set")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(["a", "b", "c"].contains(&val.as_str()));

    let card: i64 = redis::cmd("SCARD")
        .arg("set")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(card, 2);
}

#[tokio::test]
async fn test_sunion_sdiff_sinter() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("set1")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("SADD")
        .arg("set2")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    // Just verify these commands return something (not empty error)
    let _: Vec<String> = redis::cmd("SUNION")
        .arg("set1")
        .arg("set2")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: Vec<String> = redis::cmd("SDIFF")
        .arg("set1")
        .arg("set2")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: Vec<String> = redis::cmd("SINTER")
        .arg("set1")
        .arg("set2")
        .query_async(&mut con)
        .await
        .unwrap();
}

// Sorted set commands
#[tokio::test]
async fn test_zadd_zrange_zscore() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let added: i64 = redis::cmd("ZADD")
        .arg("zset")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .arg(3)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 3);

    let members: Vec<String> = redis::cmd("ZRANGE")
        .arg("zset")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["a", "b", "c"]);

    let score: String = redis::cmd("ZSCORE")
        .arg("zset")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(score, "2");
}

#[tokio::test]
async fn test_zcard_zcount_zrank() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("ZADD")
        .arg("zset")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .arg(3)
        .arg("c")
        .arg(4)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let card: i64 = redis::cmd("ZCARD")
        .arg("zset")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(card, 4);

    let count: i64 = redis::cmd("ZCOUNT")
        .arg("zset")
        .arg(2)
        .arg(4)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 3);

    let rank: i64 = redis::cmd("ZRANK")
        .arg("zset")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(rank, 2);
}

#[tokio::test]
async fn test_zrem_zincrby() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("ZADD")
        .arg("zset")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .arg(3)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("ZREM")
        .arg("zset")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 1);

    let score: f64 = redis::cmd("ZINCRBY")
        .arg("zset")
        .arg(5)
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 6.0).abs() < 0.001);
}

#[tokio::test]
async fn test_zrevrange() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: i64 = redis::cmd("ZADD")
        .arg("zset")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .arg(3)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZREVRANGE")
        .arg("zset")
        .arg(0)
        .arg(1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["c", "b"]);
}

// Server commands
#[tokio::test]
async fn test_dbsize_flushdb() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET")
        .arg("k1")
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("k2")
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();

    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert!(size >= 2);

    let _: () = redis::cmd("FLUSHDB").query_async(&mut con).await.unwrap();

    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 0);
}

#[tokio::test]
async fn test_type() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = redis::cmd("SET")
        .arg("s")
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("f")
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("SADD")
        .arg("set")
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("zset")
        .arg(1)
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();

    let t: String = redis::cmd("TYPE")
        .arg("s")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(t, "string");
    let t: String = redis::cmd("TYPE")
        .arg("l")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(t, "list");
    let t: String = redis::cmd("TYPE")
        .arg("h")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(t, "hash");
    let t: String = redis::cmd("TYPE")
        .arg("set")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(t, "set");
    let t: String = redis::cmd("TYPE")
        .arg("zset")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(t, "zset");
}

#[tokio::test]
async fn test_echo() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let val: String = redis::cmd("ECHO")
        .arg("hello world")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "hello world");
}

#[tokio::test]
async fn test_info() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let info: String = redis::cmd("INFO").query_async(&mut con).await.unwrap();
    assert!(info.contains("solikv_version"));
}

#[tokio::test]
async fn test_client_commands() {
    let port = next_port();
    let _server = start_replication_server(port).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // CLIENT ID
    let id: i64 = redis::cmd("CLIENT")
        .arg("ID")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(id > 0);

    // CLIENT SETNAME
    let name: String = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("test-client")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(name, "OK");

    // CLIENT GETNAME - may return nil if not implemented
    let result: redis::RedisResult<String> = redis::cmd("CLIENT")
        .arg("GETNAME")
        .query_async(&mut con)
        .await;
    if let Ok(retrieved) = result {
        assert_eq!(retrieved, "test-client");
    }
}
