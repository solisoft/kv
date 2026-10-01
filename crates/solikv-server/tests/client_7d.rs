//! `CLIENT NO-TOUCH` and `CLIENT PAUSE` as Redis has them: a NO-TOUCH
//! client's commands leave the keys' LRU/LFU alone (except `TOUCH`), and a
//! pause holds expiry — keys past their TTL read as missing but stay, and
//! the active cycle waits for the pause to end.

use redis::aio::MultiplexedConnection;
use redis::cmd;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use solikv_server::resp_server::ConnLimits;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(24900);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

/// A server with active expiry running, as `main` starts it.
async fn start() -> u16 {
    let port = next_port();
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(solikv_engine::ShardManager::new(4));
    solikv_engine::shard::spawn_active_expiry(shards.clone(), 10);
    let engine = Arc::new(solikv_engine::CommandEngine::new(shards, pubsub.clone()));
    engine.init_self_ref(Arc::downgrade(&engine));
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

async fn conn(port: u16) -> MultiplexedConnection {
    redis::Client::open(format!("redis://127.0.0.1:{}/", port))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn idle(c: &mut MultiplexedConnection, key: &str) -> i64 {
    cmd("OBJECT")
        .arg("IDLETIME")
        .arg(key)
        .query_async(c)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_no_touch_client_leaves_lru_alone_except_with_touch() {
    let port = start().await;
    let mut plain = conn(port).await;
    let mut quiet = conn(port).await;
    let _: () = cmd("CLIENT")
        .arg("NO-TOUCH")
        .arg("ON")
        .query_async(&mut quiet)
        .await
        .unwrap();
    let _: () = cmd("SET")
        .arg("k")
        .arg("v")
        .query_async(&mut plain)
        .await
        .unwrap();
    sleep(Duration::from_millis(2100)).await;

    // Reads and writes by the NO-TOUCH client: the key stays idle.
    let _: String = cmd("GET").arg("k").query_async(&mut quiet).await.unwrap();
    let _: i64 = cmd("APPEND")
        .arg("k")
        .arg("w")
        .query_async(&mut quiet)
        .await
        .unwrap();
    assert!(idle(&mut plain, "k").await >= 2);
    // A plain client's read refreshes it.
    let _: String = cmd("GET").arg("k").query_async(&mut plain).await.unwrap();
    assert_eq!(idle(&mut plain, "k").await, 0);

    sleep(Duration::from_millis(2100)).await;
    assert!(idle(&mut plain, "k").await >= 2);
    // TOUCH is the exception: it touches, NO-TOUCH or not.
    let n: i64 = cmd("TOUCH")
        .arg("k")
        .arg("missing")
        .query_async(&mut quiet)
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(idle(&mut plain, "k").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pause_holds_expiry_until_it_ends() {
    let port = start().await;
    let mut c = conn(port).await;
    let _: () = cmd("SET")
        .arg("k")
        .arg("v")
        .arg("PX")
        .arg(150)
        .query_async(&mut c)
        .await
        .unwrap();
    let _: () = cmd("CLIENT")
        .arg("PAUSE")
        .arg(5000)
        .arg("WRITE")
        .query_async(&mut c)
        .await
        .unwrap();
    sleep(Duration::from_millis(600)).await;
    // Past its TTL: reads as missing, but neither the read nor the active
    // cycle (several have run) removed it.
    let v: Option<String> = cmd("GET").arg("k").query_async(&mut c).await.unwrap();
    assert_eq!(v, None);
    let n: i64 = cmd("DBSIZE").query_async(&mut c).await.unwrap();
    assert_eq!(n, 1, "expired during a pause");

    let _: () = cmd("CLIENT")
        .arg("UNPAUSE")
        .query_async(&mut c)
        .await
        .unwrap();
    let mut n = 1;
    for _ in 0..40 {
        n = cmd("DBSIZE").query_async(&mut c).await.unwrap();
        if n == 0 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(n, 0, "the key was not expired after the pause");
}
