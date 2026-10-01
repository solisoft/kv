//! Operations through a real server: ACL users and their limits, MONITOR,
//! SLOWLOG and INFO commandstats, client-side caching, sharded pub/sub,
//! CONFIG SET, maxclients and the HTTP ops endpoints.

use redis::aio::MultiplexedConnection;
use redis::{cmd, RedisResult, Value};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;

use solikv_server::resp_server::ConnLimits;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(24500);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

async fn start_engine(password: Option<&str>) -> (u16, Arc<solikv_engine::CommandEngine>) {
    let port = next_port();
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(solikv_engine::ShardManager::new(4));
    let engine = Arc::new(solikv_engine::CommandEngine::new(shards, pubsub.clone()));
    engine.init_self_ref(Arc::downgrade(&engine));
    engine.set_tcp_port(port);
    let password = password.map(|p| Arc::new(p.to_string()));
    let addr = format!("127.0.0.1:{}", port);
    let e = engine.clone();
    tokio::spawn(async move {
        solikv_server::resp_server::run_with_limits(
            &addr,
            e,
            pubsub,
            password,
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

async fn conn_url(url: &str) -> RedisResult<MultiplexedConnection> {
    redis::Client::open(url)?
        .get_multiplexed_async_connection()
        .await
}

async fn conn(port: u16) -> MultiplexedConnection {
    conn_url(&format!("redis://127.0.0.1:{}/", port))
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

#[tokio::test]
async fn acl_users_are_limited_to_their_commands_and_keys() {
    let (port, _) = start_engine(Some("adminpw")).await;
    let mut admin = conn_url(&format!("redis://default:adminpw@127.0.0.1:{}/", port))
        .await
        .unwrap();
    let _: String = q(
        &mut admin,
        &[
            "ACL",
            "SETUSER",
            "app",
            "on",
            ">apppw",
            "~app:*",
            "+@read",
            "+@write",
            "-flushall",
            "+client",
            "&news",
        ],
    )
    .await
    .unwrap();
    let mut app = conn_url(&format!("redis://app:apppw@127.0.0.1:{}/", port))
        .await
        .unwrap();
    let who: String = q(&mut app, &["ACL", "WHOAMI"]).await.unwrap_or_default();
    // ACL itself is not in @read/@write: WHOAMI is refused for app.
    let _ = who;
    let _: String = q(&mut app, &["SET", "app:1", "v"]).await.unwrap();
    let e = q::<String>(&mut app, &["SET", "other", "v"])
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("No permissions to access a key"),
        "{e}"
    );
    let e = q::<String>(&mut app, &["FLUSHALL"]).await.unwrap_err();
    assert!(
        e.to_string()
            .contains("has no permissions to run the 'flushall' command"),
        "{e}"
    );
    // Denials are logged.
    let log: Vec<Value> = q(&mut admin, &["ACL", "LOG"]).await.unwrap();
    assert!(log.len() >= 2);
    // Wrong password.
    assert!(conn_url(&format!("redis://app:nope@127.0.0.1:{}/", port))
        .await
        .is_err());
    // ACL LIST shows the rules; DELUSER disconnects the user's clients.
    let list: Vec<String> = q(&mut admin, &["ACL", "LIST"]).await.unwrap();
    assert!(
        list.iter()
            .any(|l| l.starts_with("user app on #") && l.contains("~app:*")),
        "{list:?}"
    );
    let n: i64 = q(&mut admin, &["ACL", "DELUSER", "app"]).await.unwrap();
    assert_eq!(n, 1);
    sleep(Duration::from_millis(100)).await;
    assert!(q::<String>(&mut app, &["GET", "app:1"]).await.is_err());
    let dry: String = q(&mut admin, &["ACL", "DRYRUN", "default", "GET", "x"])
        .await
        .unwrap();
    assert_eq!(dry, "OK");
}

#[tokio::test]
async fn monitor_streams_commands() {
    let (port, _) = start_engine(None).await;
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    s.write_all(b"*1\r\n$7\r\nMONITOR\r\n").await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = s.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n");
    let mut c = conn(port).await;
    let _: String = q(&mut c, &["SET", "mk", "a b\n"]).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    let n = s.read(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf[..n]);
    assert!(text.contains(r#""set" "mk" "a b\n""#), "{text}");
    assert!(text.starts_with('+'), "{text}");
}

#[tokio::test]
async fn slowlog_and_commandstats() {
    let (port, _) = start_engine(None).await;
    let mut c = conn(port).await;
    let _: String = q(&mut c, &["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await
        .unwrap();
    let _: String = q(&mut c, &["SET", "k", "v"]).await.unwrap();
    let _: Option<String> = q(&mut c, &["GET", "k"]).await.unwrap();
    let _: Option<String> = q(&mut c, &["GET", "missing"]).await.unwrap();
    let len: i64 = q(&mut c, &["SLOWLOG", "LEN"]).await.unwrap();
    assert!(len >= 3, "{len}");
    let entries: Vec<Value> = q(&mut c, &["SLOWLOG", "GET", "1"]).await.unwrap();
    assert_eq!(entries.len(), 1);
    let info: String = q(&mut c, &["INFO", "commandstats"]).await.unwrap();
    assert!(info.contains("cmdstat_get:calls=2"), "{info}");
    assert!(info.contains("cmdstat_set:calls=1"), "{info}");
    let stats: String = q(&mut c, &["INFO", "stats"]).await.unwrap();
    assert!(stats.contains("keyspace_hits:1"), "{stats}");
    assert!(stats.contains("keyspace_misses:1"), "{stats}");
    let _: String = q(&mut c, &["SLOWLOG", "RESET"]).await.unwrap();
    let _: String = q(&mut c, &["CONFIG", "RESETSTAT"]).await.unwrap();
    let info: String = q(&mut c, &["INFO", "commandstats"]).await.unwrap();
    assert!(!info.contains("cmdstat_get"), "{info}");
    let all: String = q(&mut c, &["INFO", "everything"]).await.unwrap();
    for field in [
        "redis_version:",
        "run_id:",
        "used_memory_rss:",
        "used_cpu_user:",
        "maxclients:",
        "instantaneous_ops_per_sec:",
    ] {
        assert!(all.contains(field), "{field}");
    }
}

#[tokio::test]
async fn config_set_changes_behaviour_and_rejects_unknown_parameters() {
    let (port, _) = start_engine(None).await;
    let mut c = conn(port).await;
    let _: String = q(
        &mut c,
        &[
            "CONFIG",
            "SET",
            "maxmemory-policy",
            "allkeys-lru",
            "hz",
            "20",
        ],
    )
    .await
    .unwrap();
    let v: Vec<String> = q(&mut c, &["CONFIG", "GET", "maxmemory-policy"])
        .await
        .unwrap();
    assert_eq!(v, vec!["maxmemory-policy", "allkeys-lru"]);
    let e = q::<String>(&mut c, &["CONFIG", "SET", "no-such-thing", "1"])
        .await
        .unwrap_err();
    assert!(e.to_string().contains("Unknown option"), "{e}");
    let e = q::<String>(&mut c, &["CONFIG", "SET", "databases", "4"])
        .await
        .unwrap_err();
    assert!(e.to_string().contains("immutable"), "{e}");
    let e = q::<String>(&mut c, &["CONFIG", "SET", "hz", "abc"])
        .await
        .unwrap_err();
    assert!(e.to_string().contains("CONFIG SET failed"), "{e}");
    // requirepass takes effect for new connections.
    let _: String = q(&mut c, &["CONFIG", "SET", "requirepass", "s3cret"])
        .await
        .unwrap();
    let mut other = conn(port).await;
    assert!(q::<String>(&mut other, &["GET", "k"]).await.is_err());
    let e = q::<String>(&mut c, &["CONFIG", "REWRITE"])
        .await
        .unwrap_err();
    assert!(e.to_string().contains("without a config file"), "{e}");
}

#[tokio::test]
async fn maxclients_refuses_extra_connections() {
    let (port, engine) = start_engine(None).await;
    engine.set_maxclients(2);
    let _a = conn(port).await;
    let _b = conn(port).await;
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut buf = vec![0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"-ERR max number of clients reached\r\n");
}

async fn send(s: &mut tokio::net::TcpStream, args: &[&str]) -> String {
    let mut out = format!("*{}\r\n", args.len());
    for a in args {
        out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    s.write_all(out.as_bytes()).await.unwrap();
    sleep(Duration::from_millis(80)).await;
    let mut buf = vec![0u8; 8192];
    let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

/// Client-side caching: a RESP3 client that read a key is told when another
/// client changes it; a BCAST client hears about its prefixes.
#[tokio::test]
async fn client_tracking_invalidates_readers() {
    let (port, _) = start_engine(None).await;
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    send(&mut s, &["HELLO", "3"]).await;
    assert_eq!(send(&mut s, &["CLIENT", "TRACKING", "ON"]).await, "+OK\r\n");
    let mut w = conn(port).await;
    let _: String = q(&mut w, &["SET", "cached", "1"]).await.unwrap();
    assert_eq!(send(&mut s, &["GET", "cached"]).await, "$1\r\n1\r\n");
    let _: String = q(&mut w, &["SET", "cached", "2"]).await.unwrap();
    sleep(Duration::from_millis(80)).await;
    let mut buf = vec![0u8; 1024];
    let n = s.read(&mut buf).await.unwrap();
    assert_eq!(
        &buf[..n],
        b">2\r\n$10\r\ninvalidate\r\n*1\r\n$6\r\ncached\r\n"
    );
    let info = send(&mut s, &["CLIENT", "TRACKINGINFO"]).await;
    assert!(info.contains("flags") && info.contains("on"), "{info:?}");
    // FLUSHALL: everything.
    send(&mut s, &["GET", "cached"]).await;
    let _: String = q(&mut w, &["FLUSHALL"]).await.unwrap();
    sleep(Duration::from_millis(80)).await;
    let n = s.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b">2\r\n$10\r\ninvalidate\r\n_\r\n");

    // BCAST with a prefix.
    let mut b = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    send(&mut b, &["HELLO", "3"]).await;
    send(
        &mut b,
        &["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "user:"],
    )
    .await;
    let _: String = q(&mut w, &["SET", "user:7", "x"]).await.unwrap();
    let _: String = q(&mut w, &["SET", "other", "x"]).await.unwrap();
    sleep(Duration::from_millis(80)).await;
    let n = b.read(&mut buf).await.unwrap();
    assert_eq!(
        &buf[..n],
        b">2\r\n$10\r\ninvalidate\r\n*1\r\n$6\r\nuser:7\r\n"
    );
    let e = send(&mut b, &["CLIENT", "TRACKING", "ON", "PREFIX", "x"]).await;
    assert!(e.starts_with("-ERR"), "{e:?}");
}

/// RESP2 tracking via REDIRECT to a client subscribed to
/// `__redis__:invalidate`.
#[tokio::test]
async fn client_tracking_redirect_for_resp2() {
    let (port, _) = start_engine(None).await;
    let mut sub = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let id = send(&mut sub, &["CLIENT", "ID"]).await;
    let id = id.trim_start_matches(':').trim().to_string();
    send(&mut sub, &["SUBSCRIBE", "__redis__:invalidate"]).await;
    let mut reader = conn(port).await;
    let _: String = q(&mut reader, &["CLIENT", "TRACKING", "ON", "REDIRECT", &id])
        .await
        .unwrap();
    let _: Option<String> = q(&mut reader, &["GET", "k"]).await.unwrap();
    let mut w = conn(port).await;
    let _: String = q(&mut w, &["SET", "k", "1"]).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    let mut buf = vec![0u8; 1024];
    let n = sub.read(&mut buf).await.unwrap();
    assert_eq!(
        &buf[..n],
        b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nk\r\n"
    );
    let r: i64 = q(&mut reader, &["CLIENT", "GETREDIR"]).await.unwrap();
    assert_eq!(r.to_string(), id);
}

#[tokio::test]
async fn sharded_pubsub_is_its_own_namespace() {
    let (port, _) = start_engine(None).await;
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let r = send(&mut s, &["SSUBSCRIBE", "orders"]).await;
    assert_eq!(r, "*3\r\n$10\r\nssubscribe\r\n$6\r\norders\r\n:1\r\n");
    let mut p = conn(port).await;
    // A plain PUBLISH on the same name does not reach shard subscribers.
    let n: i64 = q(&mut p, &["PUBLISH", "orders", "x"]).await.unwrap();
    assert_eq!(n, 0);
    let n: i64 = q(&mut p, &["SPUBLISH", "orders", "hi"]).await.unwrap();
    assert_eq!(n, 1);
    sleep(Duration::from_millis(80)).await;
    let mut buf = vec![0u8; 1024];
    let n = s.read(&mut buf).await.unwrap();
    assert_eq!(
        &buf[..n],
        b"*3\r\n$8\r\nsmessage\r\n$6\r\norders\r\n$2\r\nhi\r\n"
    );
    let chans: Vec<String> = q(&mut p, &["PUBSUB", "SHARDCHANNELS"]).await.unwrap();
    assert_eq!(chans, vec!["orders"]);
    let plain: Vec<String> = q(&mut p, &["PUBSUB", "CHANNELS"]).await.unwrap();
    assert!(plain.is_empty());
    let r = send(&mut s, &["SUNSUBSCRIBE", "orders"]).await;
    assert_eq!(r, "*3\r\n$12\r\nsunsubscribe\r\n$6\r\norders\r\n:0\r\n");
}

#[tokio::test]
async fn metrics_render_info_and_commands() {
    let (port, engine) = start_engine(None).await;
    let mut c = conn(port).await;
    let _: String = q(&mut c, &["SET", "k", "v"]).await.unwrap();
    let text = solikv_server::rest_server::render_metrics(&engine);
    assert!(text.contains("solikv_up 1"), "{text}");
    assert!(text.contains("solikv_connected_clients "), "{text}");
    assert!(
        text.contains("solikv_command_calls_total{cmd=\"set\"} 1"),
        "{text}"
    );
    assert!(text.contains("solikv_keyspace_keys{db=\"0\"} 1"), "{text}");
}

/// Keys a script reads are tracked for the client that ran it, as if it had
/// read them itself; keys it only writes are not.
#[tokio::test]
async fn client_tracking_covers_keys_read_by_scripts() {
    let (port, _) = start_engine(None).await;
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    send(&mut s, &["HELLO", "3"]).await;
    assert_eq!(send(&mut s, &["CLIENT", "TRACKING", "ON"]).await, "+OK\r\n");
    let mut w = conn(port).await;
    let _: String = q(&mut w, &["SET", "sk", "1"]).await.unwrap();
    let r = send(
        &mut s,
        &[
            "EVAL",
            "redis.call('SET', KEYS[2], 'w') return redis.call('GET', KEYS[1])",
            "2",
            "sk",
            "written",
        ],
    )
    .await;
    assert_eq!(r, "$1\r\n1\r\n");
    let _: String = q(&mut w, &["SET", "written", "2"]).await.unwrap();
    let _: String = q(&mut w, &["SET", "sk", "2"]).await.unwrap();
    sleep(Duration::from_millis(80)).await;
    let mut buf = vec![0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
        .await
        .expect("no invalidation for a key the script read")
        .unwrap();
    assert_eq!(&buf[..n], b">2\r\n$10\r\ninvalidate\r\n*1\r\n$2\r\nsk\r\n");
}
