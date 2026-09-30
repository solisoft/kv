//! `maxmemory` through a real server. Alone in its binary: the memory
//! counter is process-wide, and other tests allocating in parallel would
//! move it.

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

use solikv_core::memory::Counting;
use solikv_server::resp_server::ConnLimits;

/// Counted, so `maxmemory` has a number to compare against in this binary.
#[global_allocator]
static ALLOC: Counting<std::alloc::System> = Counting(std::alloc::System);

static PORT_COUNTER: AtomicU16 = AtomicU16::new(19800);

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
async fn maxmemory_evicts_or_refuses() {
    solikv_core::memory::enable();
    let base = solikv_core::memory::used() as u64;
    let limit = base + 8 * 1024 * 1024;
    let (port, _) = start_with(move |e| {
        e.with_maxmemory(limit, solikv_core::store::EvictionPolicy::AllKeysLru, 5)
    })
    .await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let value = vec![b'x'; 10_000];
    for i in 0..3000 {
        let k = format!("k{i}");
        let r = ask(&mut s, &[b"SET", k.as_bytes(), &value]).await;
        assert_eq!(r, "+OK\r\n", "SET {k}");
    }
    // 30 MB written into an 8 MB budget: most keys were evicted.
    let n: i64 = ask(&mut s, &[b"DBSIZE"])
        .await
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap();
    assert!(n > 0 && n < 2000, "{n} keys left");
    let info = ask(&mut s, &[b"INFO", b"stats"]).await;
    assert!(!info.contains("evicted_keys:0\r\n"), "{info}");

    // noeviction: writes fail with OOM once over, reads still work.
    assert_eq!(
        ask(
            &mut s,
            &[b"CONFIG", b"SET", b"maxmemory-policy", b"noeviction"]
        )
        .await,
        "+OK\r\n"
    );
    let mut refused = false;
    for i in 0..3000 {
        let k = format!("n{i}");
        let r = ask(&mut s, &[b"SET", k.as_bytes(), &value]).await;
        if r.starts_with("-OOM") {
            refused = true;
            break;
        }
    }
    assert!(refused, "never refused under noeviction");
    assert!(ask(&mut s, &[b"GET", b"nosuchkey"])
        .await
        .starts_with("$-1"));
    // OBJECT IDLETIME is real now.
    assert!(ask(&mut s, &[b"OBJECT", b"IDLETIME", b"k2999"])
        .await
        .starts_with(':'));
}
