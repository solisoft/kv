//! Pipelined commands for one shard run under one hold of its lock
//! (`CommandEngine::batch_shard`). These tests check that a batch changes
//! nothing a client can see: replies in order, blocked clients woken,
//! `WATCH` still aborting, and no deadlock between batches and the
//! commands that lock several shards.

use redis::aio::MultiplexedConnection;
use redis::{cmd, Value};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

use solikv_server::resp_server::ConnLimits;

const SHARDS: usize = 8;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(23500);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

async fn start_with(shards: Arc<solikv_engine::ShardManager>) -> u16 {
    let port = next_port();
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let engine = Arc::new(solikv_engine::CommandEngine::new(shards, pubsub.clone()));
    engine.init_self_ref(Arc::downgrade(&engine));
    engine.set_tcp_port(port);
    let addr = format!("127.0.0.1:{}", port);
    tokio::spawn(async move {
        solikv_server::resp_server::run_with_limits(
            &addr,
            engine,
            pubsub,
            None,
            None,
            None,
            ConnLimits::default(),
        )
        .await
        .unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    port
}

async fn start() -> u16 {
    start_with(Arc::new(solikv_engine::ShardManager::new(SHARDS))).await
}

async fn conn(port: u16) -> MultiplexedConnection {
    redis::Client::open(format!("redis://127.0.0.1:{}/", port))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// A key named `{prefix}:N` on a different shard from `key`.
fn other_shard_key(key: &str, prefix: &str) -> String {
    let m = solikv_engine::ShardManager::new(SHARDS);
    let s = m.shard_index_for_key(key.as_bytes());
    (0..)
        .map(|i| format!("{}:{}", prefix, i))
        .find(|k| m.shard_index_for_key(k.as_bytes()) != s)
        .unwrap()
}

/// Replies come back in command order whether a command ran in a batch,
/// between two batches, or broke one (another shard, a keyless command,
/// an error). Raw RESP over one socket: the whole pipeline arrives in one
/// read, and an error reply inside it is checked like any other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replies_keep_pipeline_order_across_batches() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let port = start().await;
    let other = other_shard_key("k", "other");
    let cmds: Vec<Vec<&str>> = vec![
        vec!["SET", "k", "1"],
        vec!["INCR", "k"],
        vec!["INCRBY", "k", "10"],
        vec!["GET", "k"],
        vec!["SET", &other, "x"],
        vec!["INCR", "k"],
        vec!["PING"],
        vec!["LPUSH", "k", "a"],
        vec!["APPEND", "{k}s", "ab"],
        vec!["APPEND", "{k}s", "cd"],
        vec!["GET", "{k}s"],
        vec!["GET", &other],
    ];
    let mut req = Vec::new();
    for c in &cmds {
        req.extend_from_slice(format!("*{}\r\n", c.len()).as_bytes());
        for a in c {
            req.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
        }
    }
    let want = "+OK\r\n:2\r\n:12\r\n$2\r\n12\r\n+OK\r\n:13\r\n+PONG\r\n\
                -WRONGTYPE Operation against a key holding the wrong kind of value\r\n\
                :2\r\n:4\r\n$4\r\nabcd\r\n$1\r\nx\r\n";
    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    sock.write_all(&req).await.unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    while got.len() < want.len() {
        let n = timeout(Duration::from_secs(5), sock.read(&mut buf))
            .await
            .expect("replies did not arrive")
            .unwrap();
        assert!(n > 0, "connection closed");
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(String::from_utf8_lossy(&got), want);
}

/// A client blocked on a key is served once a batch that pushed to it ends,
/// and sees the first element pushed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_wakes_a_blocked_client() {
    let port = start().await;
    let mut blocked = conn(port).await;
    let waiter = tokio::spawn(async move {
        let r: Option<(String, String)> = cmd("BLPOP")
            .arg("q")
            .arg(5)
            .query_async(&mut blocked)
            .await
            .unwrap();
        r
    });
    sleep(Duration::from_millis(200)).await;
    let mut c = conn(port).await;
    let mut p = redis::pipe();
    for i in 0..20 {
        p.cmd("RPUSH").arg("q").arg(i);
    }
    let _: Vec<i64> = p.query_async(&mut c).await.unwrap();
    let got = timeout(Duration::from_secs(5), waiter)
        .await
        .expect("blocked client not woken")
        .unwrap();
    assert_eq!(got, Some(("q".to_string(), "0".to_string())));
    let len: i64 = cmd("LLEN").arg("q").query_async(&mut c).await.unwrap();
    assert_eq!(len, 19);
}

/// A write made inside a batch aborts another client's transaction on the
/// key it watched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batched_write_aborts_a_watching_transaction() {
    let port = start().await;
    let mut watcher = conn(port).await;
    let mut writer = conn(port).await;
    let _: () = cmd("SET")
        .arg("w")
        .arg(0)
        .query_async(&mut watcher)
        .await
        .unwrap();
    let _: () = cmd("WATCH")
        .arg("w")
        .query_async(&mut watcher)
        .await
        .unwrap();
    let mut p = redis::pipe();
    for _ in 0..10 {
        p.cmd("INCR").arg("w");
    }
    let _: Vec<i64> = p.query_async(&mut writer).await.unwrap();
    let mut tx = redis::pipe();
    tx.atomic().cmd("SET").arg("w").arg("mine");
    let r: Option<Vec<Value>> = tx.query_async(&mut watcher).await.unwrap();
    assert_eq!(r, None, "EXEC must abort: the watched key changed");
    let v: i64 = cmd("GET").arg("w").query_async(&mut writer).await.unwrap();
    assert_eq!(v, 10);
}

/// Connections batching on two shards while others run commands that lock
/// both (cross-shard `MSET`, `RENAME`, `EXEC`): everything completes, and
/// every increment is counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn batches_and_multi_shard_commands_do_not_deadlock() {
    let port = start().await;
    let a = "ctr:a".to_string();
    let b = other_shard_key(&a, "ctr:b");
    let mut tasks = Vec::new();
    for t in 0..8 {
        let key = if t % 2 == 0 { a.clone() } else { b.clone() };
        let mut c = conn(port).await;
        tasks.push(tokio::spawn(async move {
            for _ in 0..50 {
                let mut p = redis::pipe();
                for _ in 0..40 {
                    p.cmd("INCR").arg(&key);
                }
                let _: Vec<i64> = p.query_async(&mut c).await.unwrap();
            }
        }));
    }
    for t in 0..4 {
        let (a, b) = (a.clone(), b.clone());
        let mut c = conn(port).await;
        tasks.push(tokio::spawn(async move {
            for i in 0..200 {
                let x = format!("x{}:{}", t, i);
                let y = other_shard_key(&x, "y");
                let _: () = cmd("MSET")
                    .arg(&x)
                    .arg(1)
                    .arg(&y)
                    .arg(2)
                    .query_async(&mut c)
                    .await
                    .unwrap();
                let _: () = cmd("RENAME")
                    .arg(&x)
                    .arg(format!("{}:r", y))
                    .query_async(&mut c)
                    .await
                    .unwrap();
                let mut tx = redis::pipe();
                tx.atomic().cmd("GET").arg(&a).cmd("GET").arg(&b);
                let _: Value = tx.query_async(&mut c).await.unwrap();
            }
        }));
    }
    timeout(Duration::from_secs(60), async {
        for t in tasks {
            t.await.unwrap();
        }
    })
    .await
    .expect("deadlock: batches and multi-shard commands did not finish");
    let mut c = conn(port).await;
    let va: i64 = cmd("GET").arg(&a).query_async(&mut c).await.unwrap();
    let vb: i64 = cmd("GET").arg(&b).query_async(&mut c).await.unwrap();
    assert_eq!(va + vb, 8 * 50 * 40);
    assert_eq!(va, 4 * 50 * 40);
}

/// What batches wrote reaches the append-only log in execution order: a
/// restart rebuilds exactly the state the batches left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_writes_replay_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = solikv_engine::persistence::PersistConfig {
        dir: dir.path().to_path_buf(),
        appendonly: true,
        auto_rewrite_percentage: 0,
        save_interval: 0,
        ..Default::default()
    };
    let engine_for = |cfg: &solikv_engine::persistence::PersistConfig,
                      pubsub: Arc<solikv_pubsub::PubSubBroker>| {
        let shards = Arc::new(solikv_engine::ShardManager::new(SHARDS));
        let engine = Arc::new(
            solikv_engine::CommandEngine::new(shards, pubsub).with_persistence(Arc::new(
                solikv_engine::persistence::Persistence::new(cfg.clone()),
            )),
        );
        engine.init_self_ref(Arc::downgrade(&engine));
        engine.load_persisted().expect("load");
        engine
    };
    let expected: Vec<String>;
    {
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let engine = engine_for(&cfg, pubsub.clone());
        let port = next_port();
        let addr = format!("127.0.0.1:{}", port);
        let e2 = engine.clone();
        let server = tokio::spawn(async move {
            solikv_server::resp_server::run_with_limits(
                &addr,
                e2,
                pubsub,
                None,
                None,
                None,
                ConnLimits::default(),
            )
            .await
            .unwrap();
        });
        sleep(Duration::from_millis(100)).await;
        let mut tasks = Vec::new();
        for t in 0..4 {
            let mut c = conn(port).await;
            tasks.push(tokio::spawn(async move {
                for i in 0..25 {
                    let mut p = redis::pipe();
                    for j in 0..16 {
                        p.cmd("RPUSH").arg("log").arg(format!("{t}-{i}-{j}"));
                    }
                    p.cmd("INCR").arg("{log}n");
                    let _: Vec<i64> = p.query_async(&mut c).await.unwrap();
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let mut c = conn(port).await;
        expected = cmd("LRANGE")
            .arg("log")
            .arg(0)
            .arg(-1)
            .query_async(&mut c)
            .await
            .unwrap();
        engine.shutdown(Some(false)).unwrap();
        server.abort();
    }
    let engine = engine_for(&cfg, Arc::new(solikv_pubsub::PubSubBroker::new()));
    let r = engine.execute(
        "LRANGE",
        &[
            bytes::Bytes::from("log"),
            bytes::Bytes::from("0"),
            bytes::Bytes::from("-1"),
        ],
    );
    let got: Vec<String> = match r {
        solikv_core::CommandResponse::Array(items) => items
            .into_iter()
            .map(|i| match i {
                solikv_core::CommandResponse::BulkString(b) => {
                    String::from_utf8(b.to_vec()).unwrap()
                }
                other => panic!("unexpected {:?}", other),
            })
            .collect(),
        other => panic!("unexpected {:?}", other),
    };
    assert_eq!(got.len(), 4 * 25 * 16);
    assert_eq!(got, expected);
}
