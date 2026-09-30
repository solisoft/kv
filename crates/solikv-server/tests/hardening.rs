//! Connection-level hardening, exercised through a real socket: what a peer
//! can do before AUTH, how long it may hold a connection, and the REST
//! listener's Host and auth-failure handling.

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

use solikv_server::resp_server::ConnLimits;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(18000);

/// The next port nothing else is listening on.
fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn engine() -> (
    Arc<solikv_engine::CommandEngine>,
    Arc<solikv_pubsub::PubSubBroker>,
) {
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(solikv_engine::ShardManager::new(2));
    let engine = Arc::new(solikv_engine::CommandEngine::new(shards, pubsub.clone()));
    engine.init_self_ref(Arc::downgrade(&engine));
    (engine, pubsub)
}

async fn start(password: Option<&str>, limits: ConnLimits) -> u16 {
    let port = next_port();
    let (engine, pubsub) = engine();
    let password = password.map(|p| Arc::new(p.to_string()));
    let addr = format!("127.0.0.1:{}", port);
    tokio::spawn(async move {
        solikv_server::resp_server::run_with_limits(
            &addr, engine, pubsub, password, None, None, limits,
        )
        .await
        .unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    port
}

/// Everything the server sends until it closes the socket (or `wait` passes).
async fn read_until_close(stream: &mut TcpStream, wait: Duration) -> (Vec<u8>, bool) {
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        match tokio::time::timeout_at(deadline, stream.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => return (out, true),
            Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
            Err(_) => return (out, false),
        }
    }
}

async fn ping_works(port: u16, password: Option<&str>) -> bool {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    if let Some(pw) = password {
        let auth = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", pw.len(), pw);
        s.write_all(auth.as_bytes()).await.unwrap();
    }
    s.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    sleep(Duration::from_millis(100)).await;
    let mut buf = [0u8; 256];
    let n = s.read(&mut buf).await.unwrap_or(0);
    String::from_utf8_lossy(&buf[..n]).contains("PONG")
}

/// `*1\r\n` repeated used to recurse once per level and overflow the stack,
/// aborting the whole process — before AUTH, with protected mode on.
#[tokio::test]
async fn deep_nesting_is_refused_and_the_server_survives() {
    for password in [Some("secret"), None] {
        let port = start(password, ConnLimits::default()).await;
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let payload = b"*1\r\n".repeat(100_000);
        let _ = s.write_all(&payload).await;
        let (reply, closed) = read_until_close(&mut s, Duration::from_secs(2)).await;
        assert!(closed, "connection left open");
        assert!(
            String::from_utf8_lossy(&reply).contains("nesting"),
            "{:?}",
            String::from_utf8_lossy(&reply)
        );
        assert!(ping_works(port, password).await, "server did not survive");
    }
}

/// A line with no CRLF is refused at the inline bound, not buffered to 256 MB.
#[tokio::test]
async fn an_endless_line_is_refused_before_auth() {
    let port = start(Some("secret"), ConnLimits::default()).await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let _ = s.write_all(&vec![b'A'; 200 * 1024]).await;
    let (reply, closed) = read_until_close(&mut s, Duration::from_secs(2)).await;
    assert!(closed);
    assert!(String::from_utf8_lossy(&reply).contains("too big inline"));
}

/// Before AUTH, a peer may buffer a little more than one AUTH command.
#[tokio::test]
async fn the_pre_auth_buffer_is_small() {
    let port = start(Some("secret"), ConnLimits::default()).await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    // A well-formed but incomplete 60 KB bulk, then more frames behind it:
    // the buffer passes the pre-auth cap long before 256 MB.
    let mut payload = b"*2\r\n$4\r\nAUTH\r\n$60000\r\n".to_vec();
    payload.extend(vec![b'x'; 59_000]);
    let _ = s.write_all(&payload).await;
    let _ = s.write_all(&vec![b'y'; 200 * 1024]).await;
    let (_, closed) = read_until_close(&mut s, Duration::from_secs(2)).await;
    assert!(closed, "pre-auth buffer grew past its cap");
}

#[tokio::test]
async fn an_unauthenticated_connection_is_closed_at_the_deadline() {
    let limits = ConnLimits {
        auth_timeout: Some(Duration::from_millis(300)),
        ..Default::default()
    };
    let port = start(Some("secret"), limits).await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    // A byte now and then must not keep the slot: the deadline is for the
    // whole connection, not per read.
    for _ in 0..3 {
        let _ = s.write_all(b"P").await;
        sleep(Duration::from_millis(150)).await;
    }
    let (_, closed) = read_until_close(&mut s, Duration::from_secs(1)).await;
    assert!(closed, "an unauthenticated trickle kept the connection");
    // An authenticated client is not subject to it.
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n")
        .await
        .unwrap();
    sleep(Duration::from_millis(600)).await;
    s.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let (reply, _) = read_until_close(&mut s, Duration::from_millis(300)).await;
    assert!(String::from_utf8_lossy(&reply).contains("PONG"));
}

#[tokio::test]
async fn the_idle_timeout_closes_quiet_clients_when_set() {
    let limits = ConnLimits {
        idle_timeout: Some(Duration::from_millis(300)),
        ..Default::default()
    };
    let port = start(None, limits).await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let (reply, closed) = read_until_close(&mut s, Duration::from_secs(2)).await;
    assert!(String::from_utf8_lossy(&reply).contains("PONG"));
    assert!(closed, "idle client not closed");
}

#[tokio::test]
async fn the_query_buffer_limit_applies_after_auth() {
    let limits = ConnLimits {
        query_buffer_limit: 1024 * 1024,
        ..Default::default()
    };
    let port = start(None, limits).await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut payload = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4000000\r\n".to_vec();
    payload.extend(vec![b'x'; 2 * 1024 * 1024]);
    let _ = s.write_all(&payload).await;
    let (reply, closed) = read_until_close(&mut s, Duration::from_secs(2)).await;
    assert!(closed);
    assert!(String::from_utf8_lossy(&reply).contains("query buffer limit"));
}

// ---------------------------------------------------------------- REST

async fn start_rest(config: solikv_server::rest_server::RestConfig) -> u16 {
    let port = next_port();
    let (engine, _) = engine();
    let addr = format!("127.0.0.1:{}", port);
    tokio::spawn(async move {
        solikv_server::rest_server::run_with(&addr, engine, config)
            .await
            .unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    port
}

/// DNS rebinding: a page on evil.example resolves its name to 127.0.0.1 and
/// reads the loopback REST API. The Host header still says evil.example.
#[tokio::test]
async fn rest_refuses_a_foreign_host_on_loopback() {
    let port = start_rest(solikv_server::rest_server::RestConfig {
        allowed_hosts: Some(
            solikv_server::rest_server::LOOPBACK_HOSTS
                .iter()
                .map(|h| h.to_string())
                .collect(),
        ),
        handshake_timeout: Duration::from_secs(5),
        ..Default::default()
    })
    .await;
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/dbsize", port);
    let foreign = client
        .get(&url)
        .header("Host", format!("evil.example:{}", port))
        .send()
        .await
        .unwrap();
    assert_eq!(foreign.status(), reqwest::StatusCode::MISDIRECTED_REQUEST);
    let local = client
        .get(&url)
        .header("Host", format!("localhost:{}", port))
        .send()
        .await
        .unwrap();
    assert_eq!(local.status(), reqwest::StatusCode::OK);
}

/// A request with no credentials is not a failed guess: counting it locked
/// out everyone behind a shared address after ten anonymous probes.
#[tokio::test]
async fn rest_missing_credentials_do_not_count_as_failures() {
    let port = start_rest(solikv_server::rest_server::RestConfig {
        token: Some(Arc::new("tok".to_string())),
        handshake_timeout: Duration::from_secs(5),
        ..Default::default()
    })
    .await;
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/dbsize", port);
    for _ in 0..15 {
        let r = client.get(&url).send().await.unwrap();
        assert_eq!(r.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    let ok = client.get(&url).bearer_auth("tok").send().await.unwrap();
    assert_eq!(ok.status(), reqwest::StatusCode::OK);

    // Wrong tokens still count.
    for _ in 0..10 {
        let _ = client.get(&url).bearer_auth("nope").send().await.unwrap();
    }
    let blocked = client.get(&url).bearer_auth("tok").send().await.unwrap();
    assert_eq!(blocked.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
}
