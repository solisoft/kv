//! Smaller Redis behaviours: `SORT … BY nosort` on a set where the order
//! must be deterministic, `RESTORE … IDLETIME|FREQ`, `DEBUG RELOAD`, and
//! the `CONFIG SET` persistence parameters over the wire.

use redis::aio::MultiplexedConnection;
use redis::{cmd, RedisResult};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use solikv_server::resp_server::ConnLimits;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(25500);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

async fn start(dir: &std::path::Path) -> (u16, Arc<solikv_engine::CommandEngine>) {
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(solikv_engine::ShardManager::new(4));
    let cfg = solikv_engine::persistence::PersistConfig {
        dir: dir.to_path_buf(),
        appendonly: true,
        auto_rewrite_percentage: 0,
        ..Default::default()
    };
    let engine = Arc::new(
        solikv_engine::CommandEngine::new(shards, pubsub.clone())
            .with_persistence(Arc::new(solikv_engine::persistence::Persistence::new(cfg))),
    );
    engine.init_self_ref(Arc::downgrade(&engine));
    engine.load_persisted().unwrap();
    engine.set_debug_mode("yes").unwrap();
    let port = next_port();
    engine.set_tcp_port(port);
    let addr = format!("127.0.0.1:{}", port);
    let e2 = engine.clone();
    tokio::spawn(async move {
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
    (port, engine)
}

async fn conn(port: u16) -> MultiplexedConnection {
    redis::Client::open(format!("redis://127.0.0.1:{}/", port))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn q<T: redis::FromRedisValue>(
    c: &mut MultiplexedConnection,
    args: &[&str],
) -> RedisResult<T> {
    let mut command = cmd(args[0]);
    for a in &args[1..] {
        command.arg(*a);
    }
    command.query_async(c).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sort_nosort_on_a_set_is_deterministic_where_it_must_be() {
    let dir = tempfile::tempdir().unwrap();
    let (port, _e) = start(dir.path()).await;
    let mut c = conn(port).await;
    let members = ["pear", "apple", "fig", "kiwi", "banana", "cherry"];
    let mut sadd = vec!["SADD", "{s}set"];
    sadd.extend(members);
    let _: i64 = q(&mut c, &sadd).await.unwrap();
    let mut sorted: Vec<String> = members.iter().map(|m| m.to_string()).collect();
    sorted.sort();
    // STORE: sorted lexicographically, like Redis.
    let n: i64 = q(
        &mut c,
        &["SORT", "{s}set", "BY", "nosort", "STORE", "{s}out"],
    )
    .await
    .unwrap();
    assert_eq!(n, 6);
    let stored: Vec<String> = q(&mut c, &["LRANGE", "{s}out", "0", "-1"]).await.unwrap();
    assert_eq!(stored, sorted);
    // Inside a script too, and DESC still applies.
    let from_script: Vec<String> = q(
        &mut c,
        &[
            "EVAL",
            "return redis.call('SORT', KEYS[1], 'BY', 'nosort', 'DESC')",
            "1",
            "{s}set",
        ],
    )
    .await
    .unwrap();
    let mut desc = sorted.clone();
    desc.reverse();
    assert_eq!(from_script, desc);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_idletime_and_freq_reach_object() {
    let dir = tempfile::tempdir().unwrap();
    let (port, _e) = start(dir.path()).await;
    let mut c = conn(port).await;
    let _: () = q(&mut c, &["SET", "src", "v"]).await.unwrap();
    let payload: Vec<u8> = cmd("DUMP").arg("src").query_async(&mut c).await.unwrap();
    let _: () = cmd("RESTORE")
        .arg("idle")
        .arg(0)
        .arg(&payload)
        .arg("IDLETIME")
        .arg(5000)
        .query_async(&mut c)
        .await
        .unwrap();
    let idle: i64 = q(&mut c, &["OBJECT", "IDLETIME", "idle"]).await.unwrap();
    assert!((5000..5005).contains(&idle), "idle {idle}");
    let _: () = q(
        &mut c,
        &["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"],
    )
    .await
    .unwrap();
    let _: () = cmd("RESTORE")
        .arg("hot")
        .arg(0)
        .arg(&payload)
        .arg("FREQ")
        .arg(200)
        .query_async(&mut c)
        .await
        .unwrap();
    let freq: i64 = q(&mut c, &["OBJECT", "FREQ", "hot"]).await.unwrap();
    assert_eq!(freq, 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn debug_reload_keeps_the_dataset() {
    let dir = tempfile::tempdir().unwrap();
    let (port, _e) = start(dir.path()).await;
    let mut c = conn(port).await;
    let _: () = q(&mut c, &["SET", "a", "1", "EX", "1000"]).await.unwrap();
    let _: i64 = q(&mut c, &["RPUSH", "l", "x", "y"]).await.unwrap();
    let _: i64 = q(&mut c, &["HSET", "h", "f", "v", "g", "w"]).await.unwrap();
    let _: Vec<i64> = q(&mut c, &["HEXPIRE", "h", "1000", "FIELDS", "1", "f"])
        .await
        .unwrap();
    let _: () = q(&mut c, &["SELECT", "2"]).await.unwrap();
    let _: () = q(&mut c, &["SET", "in2", "z"]).await.unwrap();
    let _: () = q(&mut c, &["SELECT", "0"]).await.unwrap();
    let _: () = q(&mut c, &["DEBUG", "RELOAD"]).await.unwrap();
    let v: String = q(&mut c, &["GET", "a"]).await.unwrap();
    assert_eq!(v, "1");
    let ttl: i64 = q(&mut c, &["TTL", "a"]).await.unwrap();
    assert!(ttl > 990, "{ttl}");
    let l: Vec<String> = q(&mut c, &["LRANGE", "l", "0", "-1"]).await.unwrap();
    assert_eq!(l, vec!["x", "y"]);
    let t: Vec<i64> = q(&mut c, &["HTTL", "h", "FIELDS", "2", "f", "g"])
        .await
        .unwrap();
    assert!(t[0] > 990 && t[1] == -1, "{t:?}");
    let _: () = q(&mut c, &["SELECT", "2"]).await.unwrap();
    let v: String = q(&mut c, &["GET", "in2"]).await.unwrap();
    assert_eq!(v, "z");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistence_parameters_change_at_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let (port, engine) = start(dir.path()).await;
    let mut c = conn(port).await;
    let _: () = q(
        &mut c,
        &[
            "CONFIG",
            "SET",
            "save",
            "900 1 60 100",
            "appendfsync",
            "always",
        ],
    )
    .await
    .unwrap();
    let got: Vec<String> = q(&mut c, &["CONFIG", "GET", "save"]).await.unwrap();
    assert_eq!(got, vec!["save", "900 1 60 100"]);
    let got: Vec<String> = q(&mut c, &["CONFIG", "GET", "appendfsync"]).await.unwrap();
    assert_eq!(got, vec!["appendfsync", "always"]);
    assert_eq!(
        engine.persistence().config().save,
        vec![(900, 1), (60, 100)]
    );
    // Atomic: a bad value anywhere sets nothing.
    let e = q::<()>(
        &mut c,
        &["CONFIG", "SET", "save", "", "appendfsync", "bogus"],
    )
    .await;
    assert!(e.is_err());
    assert_eq!(
        engine.persistence().config().save,
        vec![(900, 1), (60, 100)]
    );
    let _: () = q(&mut c, &["CONFIG", "SET", "save", ""]).await.unwrap();
    assert!(engine.persistence().config().save.is_empty());
}
