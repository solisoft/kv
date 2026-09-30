//! The performance work, checked for behaviour through a real server: a big
//! command arriving in small pieces, keys expiring on an idle server, and a
//! pub/sub subscriber that stops reading. (Eviction has its own test binary,
//! `eviction.rs`: it needs the process' memory counter to itself.)

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

use solikv_server::resp_server::ConnLimits;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(19500);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

async fn start_with(
    configure: impl FnOnce(solikv_engine::CommandEngine) -> solikv_engine::CommandEngine,
) -> (u16, Arc<solikv_engine::CommandEngine>) {
    let port = next_port();
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(solikv_engine::ShardManager::new(4));
    let engine = Arc::new(configure(solikv_engine::CommandEngine::new(
        shards.clone(),
        pubsub.clone(),
    )));
    engine.init_self_ref(Arc::downgrade(&engine));
    solikv_engine::shard::spawn_active_expiry(shards, 50);
    let addr = format!("127.0.0.1:{}", port);
    let e = engine.clone();
    tokio::spawn(async move {
        solikv_server::resp_server::run_with_limits(
            &addr,
            e,
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
    (port, engine)
}

fn cmd(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Send one command and read one reply line (enough for integer/status).
async fn ask(s: &mut TcpStream, args: &[&[u8]]) -> String {
    s.write_all(&cmd(args)).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
        .await
        .expect("reply")
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[tokio::test]
async fn a_big_command_in_small_pieces() {
    let (port, _) = start_with(|e| e).await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let values: Vec<Vec<u8>> = (0..50_000)
        .map(|i| format!("value-{i}").into_bytes())
        .collect();
    let mut args: Vec<&[u8]> = vec![b"RPUSH", b"biglist"];
    args.extend(values.iter().map(|v| v.as_slice()));
    let frame = cmd(&args);
    for piece in frame.chunks(1500) {
        s.write_all(piece).await.unwrap();
    }
    let reply = {
        let mut buf = vec![0u8; 64];
        let n = s.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    };
    assert_eq!(reply, ":50000\r\n");
    assert_eq!(ask(&mut s, &[b"LLEN", b"biglist"]).await, ":50000\r\n");
    assert_eq!(
        ask(&mut s, &[b"LINDEX", b"biglist", b"-1"]).await,
        "$11\r\nvalue-49999\r\n"
    );
}

#[tokio::test]
async fn keys_expire_on_an_idle_server() {
    let (port, _) = start_with(|e| e).await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut batch = Vec::new();
    for i in 0..20_000 {
        let k = format!("e{i}");
        batch.extend(cmd(&[b"SET", k.as_bytes(), b"v", b"PX", b"50"]));
    }
    s.write_all(&batch).await.unwrap();
    // Drain the 20k +OK replies.
    let mut got = 0;
    let mut buf = vec![0u8; 1 << 16];
    while got < 20_000 * 5 {
        got += s.read(&mut buf).await.unwrap();
    }
    assert_eq!(ask(&mut s, &[b"DBSIZE"]).await, ":20000\r\n");
    // Nobody touches the keys: the timer alone must reclaim them.
    let mut left = String::new();
    for _ in 0..40 {
        sleep(Duration::from_millis(100)).await;
        left = ask(&mut s, &[b"DBSIZE"]).await;
        if left == ":0\r\n" {
            break;
        }
    }
    assert_eq!(left, ":0\r\n");
    let info = ask(&mut s, &[b"INFO", b"stats"]).await;
    assert!(info.contains("expired_keys:20000"), "{info}");
}

#[tokio::test]
async fn a_subscriber_that_stops_reading_is_disconnected() {
    let (port, _) = start_with(|e| e).await;
    let mut sub = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    // A small receive buffer, so the server side backs up quickly.
    sub.write_all(&cmd(&[b"SUBSCRIBE", b"news"])).await.unwrap();
    let mut buf = vec![0u8; 256];
    let _ = sub.read(&mut buf).await.unwrap();

    let mut publisher = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let payload = vec![b'p'; 64 * 1024];
    // 64 KB × 1000 = 64 MB, twice the hard limit, never read by `sub`.
    let mut delivered_to_none = false;
    for _ in 0..1000 {
        let r = ask(&mut publisher, &[b"PUBLISH", b"news", &payload]).await;
        if r == ":0\r\n" {
            delivered_to_none = true;
            break;
        }
    }
    assert!(
        delivered_to_none,
        "the stalled subscriber was never dropped"
    );
    // The server still answers.
    assert_eq!(ask(&mut publisher, &[b"PING"]).await, "+PONG\r\n");
    // And the subscriber's connection is closed (EOF after what was sent).
    let mut total = 0usize;
    let closed = tokio::time::timeout(Duration::from_secs(10), async {
        let mut b = vec![0u8; 1 << 20];
        loop {
            match sub.read(&mut b).await {
                Ok(0) | Err(_) => return true,
                Ok(n) => total += n,
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        closed,
        "subscriber connection not closed ({total} bytes read)"
    );
}
