//! Phase 7b between real server processes: REST under ACL users and its
//! connection cap, client types for `CLIENT LIST`/`KILL`, `CLIENT PAUSE`,
//! `min-replicas-to-write`, and `MIGRATE`'s cached connections.

mod common;

use common::{next_port, wait_until, Client, Server, TmpDir};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

/// Fails the test instead of hanging it.
fn watchdog(secs: u64) {
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(secs));
        eprintln!("watchdog: test exceeded {secs} s");
        std::process::exit(101);
    });
}

/// One HTTP/1.1 request; the status code and body.
fn http(port: u16, method: &str, path: &str, auth: Option<&str>, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(a) = auth {
        req.push_str(&format!("Authorization: {a}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body);
    s.write_all(req.as_bytes()).unwrap();
    let mut out = String::new();
    let _ = s.read_to_string(&mut out);
    let status = out
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = out.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn basic(user: &str, pass: &str) -> String {
    use base64::Engine;
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
    )
}

fn authed(srv: &Server, pass: &str) -> Client {
    let mut c = srv.client();
    c.ok(&["AUTH", pass]);
    c
}

#[test]
fn rest_routes_run_under_the_acl_user_of_the_request() {
    watchdog(120);
    let dir = TmpDir::new("rest-acl");
    let rest = next_port();
    let srv = Server::start(
        &dir.0,
        &["--requirepass", "rootpw", "--rest-port", &rest.to_string()],
    );
    let mut c = authed(&srv, "rootpw");
    c.ok(&[
        "ACL",
        "SETUSER",
        "reader",
        "on",
        ">readerpw",
        "~public:*",
        "+get",
        "+set",
    ]);
    wait_until(Duration::from_secs(10), || {
        http(rest, "GET", "/health", None, "").0 == 200
    });

    // No credentials, a wrong token, a wrong password.
    assert_eq!(http(rest, "GET", "/kv/public:a", None, "").0, 401);
    assert_eq!(
        http(rest, "GET", "/kv/public:a", Some("Bearer nope"), "").0,
        401
    );
    assert_eq!(
        http(
            rest,
            "GET",
            "/kv/public:a",
            Some(&basic("reader", "bad")),
            ""
        )
        .0,
        401
    );

    // The token acts as `default`: everything.
    let (st, _) = http(
        rest,
        "PUT",
        "/kv/secret",
        Some("Bearer rootpw"),
        r#"{"value":"s3"}"#,
    );
    assert_eq!(st, 200);
    // `default` by its own credentials too.
    assert_eq!(
        http(
            rest,
            "GET",
            "/kv/secret",
            Some(&basic("default", "rootpw")),
            ""
        )
        .0,
        200
    );

    // `reader`: its keys and commands, nothing else.
    let reader = basic("reader", "readerpw");
    assert_eq!(
        http(
            rest,
            "PUT",
            "/kv/public:a",
            Some(&reader),
            r#"{"value":"1"}"#
        )
        .0,
        200
    );
    let (st, body) = http(rest, "GET", "/kv/public:a", Some(&reader), "");
    assert_eq!(st, 200, "{body}");
    let (st, body) = http(rest, "GET", "/kv/secret", Some(&reader), "");
    assert_eq!(st, 403, "{body}");
    assert!(body.contains("NOPERM"), "{body}");
    let (st, body) = http(rest, "DELETE", "/kv/public:a", Some(&reader), "");
    assert_eq!(st, 403, "DEL is not among reader's commands: {body}");
    // The denial is in the ACL log, like a RESP one.
    let log = c.cmd(&["ACL", "LOG"]);
    assert!(log.contains("reader"), "{log}");
}

#[test]
fn rest_connections_past_the_cap_get_503() {
    watchdog(120);
    let dir = TmpDir::new("rest-cap");
    let rest = next_port();
    let _srv = Server::start(
        &dir.0,
        &[
            "--rest-port",
            &rest.to_string(),
            "--rest-max-connections",
            "2",
        ],
    );
    wait_until(Duration::from_secs(10), || {
        http(rest, "GET", "/health", None, "").0 == 200
    });
    // Two idle keep-alive connections hold both slots.
    let hold: Vec<TcpStream> = (0..2)
        .map(|_| TcpStream::connect(("127.0.0.1", rest)).unwrap())
        .collect();
    std::thread::sleep(Duration::from_millis(300));
    let (st, body) = http(rest, "GET", "/health", None, "");
    assert_eq!(st, 503, "{body}");
    drop(hold);
    wait_until(Duration::from_secs(10), || {
        http(rest, "GET", "/health", None, "").0 == 200
    });
}

fn link_up(c: &mut Client) -> bool {
    c.info("replication").contains("master_link_status:up")
}

#[test]
fn client_types_list_and_kill_replication_links() {
    watchdog(180);
    let mdir = TmpDir::new("ctype-m");
    let rdir = TmpDir::new("ctype-r");
    let master = Server::start(&mdir.0, &[]);
    let replica = Server::start(
        &rdir.0,
        &["--replicaof", &format!("127.0.0.1 {}", master.port)],
    );
    let mut r = replica.client();
    wait_until(Duration::from_secs(30), || link_up(&mut r));
    let mut m = master.client();

    let replicas = m.bulk(&["CLIENT", "LIST", "TYPE", "replica"]);
    assert_eq!(
        replicas.lines().filter(|l| !l.is_empty()).count(),
        1,
        "{replicas}"
    );
    assert!(replicas.contains("flags=S"), "{replicas}");
    let normal = m.bulk(&["CLIENT", "LIST", "TYPE", "normal"]);
    assert!(!normal.contains("flags=S"), "{normal}");
    assert!(m
        .cmd(&["CLIENT", "LIST", "TYPE", "bogus"])
        .starts_with("-ERR Unknown client type"));

    let masters = r.bulk(&["CLIENT", "LIST", "TYPE", "master"]);
    assert!(masters.contains("flags=M"), "{masters}");

    // Killing the replica's link on the master: it comes back.
    assert_eq!(m.int(&["CLIENT", "KILL", "TYPE", "replica"]), 1);
    m.ok(&["SET", "after-kill", "1"]);
    wait_until(Duration::from_secs(30), || {
        r.get("after-kill").as_deref() == Some("1")
    });
    // Killing the master link on the replica: it reconnects too.
    assert_eq!(r.int(&["CLIENT", "KILL", "TYPE", "master"]), 1);
    m.ok(&["SET", "after-kill-2", "1"]);
    wait_until(Duration::from_secs(30), || {
        link_up(&mut r) && r.get("after-kill-2").as_deref() == Some("1")
    });
    // NO-EVICT shows as a flag.
    m.ok(&["CLIENT", "NO-EVICT", "on"]);
    let me = m.bulk(&["CLIENT", "INFO"]);
    assert!(me.contains(" flags=e "), "{me}");
}

#[test]
fn client_pause_holds_writes_or_everything() {
    watchdog(120);
    let dir = TmpDir::new("pause");
    let srv = Server::start(&dir.0, &[]);
    let mut a = srv.client();
    let mut b = srv.client();
    b.ok(&["SET", "k", "v"]);

    // WRITE: reads go on, writes wait for the end.
    a.ok(&["CLIENT", "PAUSE", "800", "WRITE"]);
    let t = Instant::now();
    assert_eq!(b.get("k").as_deref(), Some("v"));
    assert!(t.elapsed() < Duration::from_millis(400), "a read was held");
    let t = Instant::now();
    b.ok(&["SET", "k", "w"]);
    assert!(
        t.elapsed() >= Duration::from_millis(500),
        "a write went through a WRITE pause after {:?}",
        t.elapsed()
    );

    // ALL: reads wait too, and UNPAUSE ends it early.
    a.ok(&["CLIENT", "PAUSE", "20000", "ALL"]);
    let port = srv.port;
    let reader = std::thread::spawn(move || {
        let mut c = Client::connect(port);
        let t = Instant::now();
        let v = c.get("k");
        (v, t.elapsed())
    });
    std::thread::sleep(Duration::from_millis(500));
    a.ok(&["CLIENT", "UNPAUSE"]);
    let (v, waited) = reader.join().unwrap();
    assert_eq!(v.as_deref(), Some("w"));
    assert!(
        waited >= Duration::from_millis(400) && waited < Duration::from_secs(10),
        "{waited:?}"
    );
    assert!(a
        .cmd(&["CLIENT", "PAUSE", "-1"])
        .starts_with("-ERR timeout is not an integer"));
}

#[test]
fn min_replicas_to_write_refuses_writes_without_good_replicas() {
    watchdog(180);
    let mdir = TmpDir::new("minrepl-m");
    let rdir = TmpDir::new("minrepl-r");
    let master = Server::start(
        &mdir.0,
        &[
            "--min-replicas-to-write",
            "1",
            "--min-replicas-max-lag",
            "2",
        ],
    );
    let mut m = master.client();
    let r = m.cmd(&["SET", "k", "v"]);
    assert!(r.starts_with("-NOREPLICAS"), "{r}");
    // Reads are fine.
    assert!(m.get("k").is_none());

    let mut replica = Server::start(
        &rdir.0,
        &["--replicaof", &format!("127.0.0.1 {}", master.port)],
    );
    wait_until(Duration::from_secs(30), || {
        m.cmd(&["SET", "k", "v"]) == "+OK\r\n"
    });
    assert!(m.info("replication").contains("min_slaves_good_slaves:1"));

    // The replica dies: once its lag passes 2 s, writes are refused again.
    replica.kill();
    wait_until(Duration::from_secs(30), || {
        m.cmd(&["SET", "k", "v2"]).starts_with("-NOREPLICAS")
    });
    // Runtime configuration turns it off.
    m.ok(&["CONFIG", "SET", "min-replicas-to-write", "0"]);
    m.ok(&["SET", "k", "v3"]);
}

fn connections_received(c: &mut Client) -> u64 {
    c.info("stats")
        .lines()
        .find_map(|l| l.strip_prefix("total_connections_received:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap()
}

#[test]
fn migrate_reuses_its_connection_to_a_target() {
    watchdog(120);
    let adir = TmpDir::new("mig-a");
    let bdir = TmpDir::new("mig-b");
    let a = Server::start(&adir.0, &[]);
    let b = Server::start(&bdir.0, &[]);
    let mut ca = a.client();
    let mut cb = b.client();
    for i in 0..3 {
        ca.ok(&["SET", &format!("m{i}"), &format!("v{i}")]);
    }
    let bport = b.port.to_string();
    ca.ok(&["MIGRATE", "127.0.0.1", &bport, "m0", "0", "5000"]);
    let after_first = connections_received(&mut cb);
    ca.ok(&["MIGRATE", "127.0.0.1", &bport, "m1", "0", "5000"]);
    ca.ok(&["MIGRATE", "127.0.0.1", &bport, "m2", "0", "5000", "COPY"]);
    assert_eq!(
        connections_received(&mut cb),
        after_first,
        "MIGRATE opened a new connection instead of reusing its cached one"
    );
    for i in 0..3 {
        assert_eq!(cb.get(&format!("m{i}")), Some(format!("v{i}")));
    }
    assert!(ca.get("m1").is_none(), "MIGRATE without COPY keeps the key");
    assert_eq!(ca.get("m2").as_deref(), Some("v2"), "COPY deleted the key");

    // The target drops the cached link: the next MIGRATE retries on a new one.
    cb.int(&["CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes"]);
    ca.ok(&["SET", "m3", "v3"]);
    ca.ok(&["MIGRATE", "127.0.0.1", &bport, "m3", "0", "5000"]);
    assert_eq!(cb.get("m3").as_deref(), Some("v3"));
}

#[test]
fn monitor_and_slowlog_do_not_show_secrets() {
    watchdog(60);
    let dir = TmpDir::new("redact");
    let srv = Server::start(&dir.0, &[]);
    let mut mon = TcpStream::connect(("127.0.0.1", srv.port)).unwrap();
    mon.set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    mon.write_all(b"*1\r\n$7\r\nMONITOR\r\n").unwrap();
    let mut c = srv.client();
    c.ok(&["CONFIG", "SET", "slowlog-log-slower-than", "0"]);
    c.ok(&[
        "ACL",
        "SETUSER",
        "u",
        "on",
        ">hunter2-secret",
        "~*",
        "+@all",
    ]);
    let hello = c.cmd(&["HELLO", "2", "AUTH", "u", "hunter2-secret"]);
    assert!(hello.starts_with('*'), "{hello}");
    c.ok(&["SET", "visible", "1"]);
    std::thread::sleep(Duration::from_millis(300));
    let mut seen = Vec::new();
    let mut buf = [0u8; 8192];
    while let Ok(n) = mon.read(&mut buf) {
        if n == 0 {
            break;
        }
        seen.extend_from_slice(&buf[..n]);
    }
    let seen = String::from_utf8_lossy(&seen);
    assert!(seen.contains(r#""set" "visible" "1""#), "{seen}");
    assert!(
        !seen.contains("hunter2"),
        "MONITOR leaked a password: {seen}"
    );
    assert!(
        !seen.contains(r#""acl""#),
        "MONITOR showed an admin command: {seen}"
    );
    let slow = c.cmd(&["SLOWLOG", "GET", "-1"]);
    assert!(
        !slow.contains("hunter2"),
        "SLOWLOG leaked a password: {slow}"
    );
    assert!(slow.contains("(redacted)"), "{slow}");
}
