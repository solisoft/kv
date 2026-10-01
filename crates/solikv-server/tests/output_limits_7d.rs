//! `client-output-buffer-limit` for normal clients (and the pubsub class,
//! now configurable): a client whose replies wait past the hard limit, or
//! past the soft limit for longer than allowed, is disconnected and its
//! buffers freed; others are untouched.

use redis::aio::MultiplexedConnection;
use redis::cmd;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, timeout};

use solikv_server::resp_server::ConnLimits;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(24800);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

async fn start() -> u16 {
    let port = next_port();
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(solikv_engine::ShardManager::new(4));
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

async fn config_set(c: &mut MultiplexedConnection, v: &str) {
    let _: () = cmd("CONFIG")
        .arg("SET")
        .arg("client-output-buffer-limit")
        .arg(v)
        .query_async(c)
        .await
        .unwrap();
}

/// A list whose `LRANGE 0 -1` is about 10 MB.
async fn big_list(c: &mut MultiplexedConnection) {
    let v = "x".repeat(100);
    for _ in 0..100 {
        let mut p = cmd("RPUSH");
        p.arg("big");
        for _ in 0..1000 {
            p.arg(&v);
        }
        let _: i64 = p.query_async(c).await.unwrap();
    }
}

fn resp(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
    }
    out
}

/// Send `args` on a fresh socket, wait `idle` without reading, then read
/// until the server closes or `want` bytes arrived. Returns the bytes read.
async fn ask(port: u16, args: &[&str], idle: Duration, want: usize) -> usize {
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    s.write_all(&resp(args)).await.unwrap();
    sleep(idle).await;
    let mut got = 0;
    let mut buf = vec![0u8; 1 << 16];
    loop {
        match timeout(Duration::from_secs(10), s.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => return got,
            Ok(Ok(n)) => {
                got += n;
                if got >= want {
                    return got;
                }
            }
            Err(_) => panic!("neither the reply nor a disconnection after {got} bytes"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_limits_are_configurable_as_in_redis() {
    let port = start().await;
    let mut c = conn(port).await;
    let get = |c: &mut MultiplexedConnection| {
        let mut c = c.clone();
        async move {
            let v: Vec<String> = cmd("CONFIG")
                .arg("GET")
                .arg("client-output-buffer-limit")
                .query_async(&mut c)
                .await
                .unwrap();
            v[1].clone()
        }
    };
    assert_eq!(
        get(&mut c).await,
        "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"
    );
    config_set(&mut c, "normal 1mb 512kb 10 pubsub 0 0 0").await;
    assert_eq!(
        get(&mut c).await,
        "normal 1048576 524288 10 slave 268435456 67108864 60 pubsub 0 0 0"
    );
    let err: redis::RedisResult<()> = cmd("CONFIG")
        .arg("SET")
        .arg("client-output-buffer-limit")
        .arg("normal 1mb 0")
        .query_async(&mut c)
        .await;
    assert!(err.is_err());
    let err: redis::RedisResult<()> = cmd("CONFIG")
        .arg("SET")
        .arg("client-output-buffer-limit")
        .arg("everyone 1 2 3")
        .query_async(&mut c)
        .await;
    assert!(err.is_err());
}

/// Over the hard limit: disconnected before the reply is sent, and gone
/// from `CLIENT LIST`; a client whose reply fits is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_over_the_hard_limit_closes_the_client() {
    let port = start().await;
    let mut c = conn(port).await;
    big_list(&mut c).await;
    // No limit (the default): the whole reply arrives.
    let full = ask(
        port,
        &["LRANGE", "big", "0", "-1"],
        Duration::ZERO,
        10_000_000,
    )
    .await;
    assert!(full >= 10_000_000, "got {full} bytes");

    config_set(&mut c, "normal 1mb 0 0").await;
    let got = ask(
        port,
        &["LRANGE", "big", "0", "-1"],
        Duration::ZERO,
        10_000_000,
    )
    .await;
    assert!(
        got < 1_000_000,
        "{got} bytes arrived past a 1 MB hard limit"
    );
    let got = ask(port, &["KEYS", "*"], Duration::ZERO, 1).await;
    assert!(got > 0, "a small reply is not cut");
    for i in 0..20_000 {
        let _: () = cmd("SET")
            .arg(format!("key-with-a-long-name-{i:08}"))
            .arg(1)
            .query_async(&mut c)
            .await
            .unwrap();
    }
    config_set(&mut c, "normal 64kb 0 0").await;
    let got = ask(port, &["KEYS", "*"], Duration::ZERO, 2_000_000).await;
    assert!(
        got < 65_536,
        "{got} bytes of KEYS * past a 64 KB hard limit"
    );
    // The others are untouched, and the cut clients are gone.
    let n: i64 = cmd("LLEN").arg("big").query_async(&mut c).await.unwrap();
    assert_eq!(n, 100_000);
    let list: String = cmd("CLIENT").arg("LIST").query_async(&mut c).await.unwrap();
    assert_eq!(list.lines().count(), 1, "{list}");
}

/// Over the soft limit: tolerated for its seconds — a client that keeps
/// reading gets everything — then disconnected if it does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_over_the_soft_limit_too_long_is_closed() {
    let port = start().await;
    let mut c = conn(port).await;
    big_list(&mut c).await;
    config_set(&mut c, "normal 0 1mb 1").await;
    // Reads at once: the reply drains within the second.
    let got = ask(
        port,
        &["LRANGE", "big", "0", "-1"],
        Duration::ZERO,
        10_000_000,
    )
    .await;
    assert!(got >= 10_000_000, "a reading client got only {got} bytes");
    // Does not read for 3 s: more than 1 MB stays queued past the second.
    let got = ask(
        port,
        &["LRANGE", "big", "0", "-1"],
        Duration::from_secs(3),
        10_000_000,
    )
    .await;
    assert!(got < 10_000_000, "a stalled client was kept ({got} bytes)");
}

/// The pubsub class is configurable too, for subscribers already
/// connected: one that stops reading is cut at the new hard limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pubsub_limit_follows_config_set() {
    let port = start().await;
    let mut c = conn(port).await;
    let mut sub = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    sub.write_all(&resp(&["SUBSCRIBE", "ch"])).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let _ = timeout(Duration::from_secs(5), sub.read(&mut buf))
        .await
        .unwrap();
    config_set(&mut c, "pubsub 2mb 0 0").await;
    // The subscriber reads nothing from here on.
    let msg = "m".repeat(64 * 1024);
    for _ in 0..400 {
        let _: i64 = cmd("PUBLISH")
            .arg("ch")
            .arg(&msg)
            .query_async(&mut c)
            .await
            .unwrap();
    }
    let mut subs = 1;
    for _ in 0..100 {
        let n: Vec<(String, i64)> = cmd("PUBSUB")
            .arg("NUMSUB")
            .arg("ch")
            .query_async(&mut c)
            .await
            .unwrap();
        subs = n[0].1;
        if subs == 0 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(subs, 0, "a subscriber 25 MB behind was kept");
}
