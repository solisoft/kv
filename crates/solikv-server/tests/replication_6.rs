//! Master/replica replication between real server processes: full and
//! partial resynchronization, the stream's ordering and atomicity, `WAIT`,
//! read-only replicas, expiry, promotion.

mod common;

use common::{wait_until, Server, TmpDir};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn replica_of(dir: &TmpDir, master: &Server, extra: &[&str]) -> Server {
    let spec = format!("127.0.0.1 {}", master.port);
    let mut args = vec!["--replicaof", spec.as_str()];
    args.extend_from_slice(extra);
    Server::start(&dir.0, &args)
}

fn wait_synced(replica: &Server) {
    wait_until(Duration::from_secs(30), || {
        replica
            .client()
            .info("replication")
            .contains("master_link_status:up")
    });
}

#[test]
fn a_replica_loads_the_dataset_then_follows_the_stream() {
    let (dm, dr) = (TmpDir::new("repl-m"), TmpDir::new("repl-r"));
    let master = Server::start(&dm.0, &["--shards", "4"]);
    let mut m = master.client();
    m.ok(&["SET", "a", "1"]);
    m.cmd(&["RPUSH", "l", "x", "y"]);
    m.cmd(&["HSET", "h", "f", "v"]);
    m.cmd(&["SET", "ttl", "v", "EX", "1000"]);
    m.ok(&["SELECT", "3"]);
    m.ok(&["SET", "in3", "yes"]);
    m.ok(&["SELECT", "0"]);
    m.cmd(&["INCR", "n"]);

    let replica = replica_of(&dr, &master, &["--shards", "2"]);
    wait_synced(&replica);
    let mut r = replica.client();
    wait_until(Duration::from_secs(10), || {
        r.get("n").as_deref() == Some("1")
    });
    assert_eq!(r.get("a").as_deref(), Some("1"));
    assert_eq!(
        r.cmd(&["LRANGE", "l", "0", "-1"]),
        "*2\r\n$1\r\nx\r\n$1\r\ny\r\n"
    );
    assert_eq!(r.cmd(&["HGET", "h", "f"]), "$1\r\nv\r\n");
    let ttl: i64 = r.int(&["TTL", "ttl"]);
    assert!(ttl > 900 && ttl <= 1000, "ttl {ttl}");
    r.ok(&["SELECT", "3"]);
    assert_eq!(r.get("in3").as_deref(), Some("yes"));
    r.ok(&["SELECT", "0"]);

    // The stream: writes after the sync, non-idempotent ones included.
    for _ in 0..10 {
        m.cmd(&["INCR", "n"]);
    }
    m.cmd(&["RPUSH", "l", "z"]);
    m.cmd(&["DEL", "a"]);
    // The DEL was written last: once it is applied, so is everything before.
    wait_until(Duration::from_secs(10), || r.get("a").is_none());
    assert_eq!(r.get("n").as_deref(), Some("11"));
    assert_eq!(r.int(&["LLEN", "l"]), 3);

    // Read-only replica.
    assert!(r.cmd(&["SET", "x", "1"]).starts_with("-READONLY"));
    // Roles.
    assert!(m.info("replication").contains("connected_slaves:1"));
    assert!(r.cmd(&["ROLE"]).contains("slave"));
}

#[test]
fn multi_shard_writes_and_whole_keyspace_commands_replicate_once() {
    let (dm, dr) = (TmpDir::new("repl-ms-m"), TmpDir::new("repl-ms-r"));
    let master = Server::start(&dm.0, &["--shards", "8"]);
    let replica = replica_of(&dr, &master, &["--shards", "3"]);
    wait_synced(&replica);
    let mut m = master.client();
    let mut r = replica.client();

    let mut mset = vec!["MSET"];
    let keys: Vec<String> = (0..50).map(|i| format!("k{i}")).collect();
    for k in &keys {
        mset.push(k);
        mset.push("v");
    }
    m.ok(&mset);
    m.ok(&["MULTI"]);
    m.cmd(&["INCR", "t1"]);
    m.cmd(&["INCR", "t2"]);
    m.cmd(&["INCR", "t3"]);
    m.cmd(&["EXEC"]);
    m.ok(&["SET", "swapme", "1"]);
    // SWAPDB is logged once per shard locally; twice on a replica would
    // swap back.
    m.ok(&["SWAPDB", "0", "1"]);
    wait_until(Duration::from_secs(10), || {
        r.ok(&["SELECT", "1"]);
        let v = r.get("swapme");
        r.ok(&["SELECT", "0"]);
        v.as_deref() == Some("1")
    });
    assert_eq!(r.get("swapme"), None);
    m.ok(&["SWAPDB", "0", "1"]);
    wait_until(Duration::from_secs(10), || {
        r.get("swapme").as_deref() == Some("1")
    });
    for k in &keys {
        assert_eq!(r.get(k).as_deref(), Some("v"), "{k}");
    }
    assert_eq!(r.get("t3").as_deref(), Some("1"));
    m.ok(&["FLUSHALL"]);
    wait_until(Duration::from_secs(10), || r.int(&["DBSIZE"]) == 0);
}

#[test]
fn wait_counts_acknowledging_replicas() {
    let (dm, dr) = (TmpDir::new("repl-w-m"), TmpDir::new("repl-w-r"));
    let master = Server::start(&dm.0, &[]);
    let mut m = master.client();
    assert_eq!(m.int(&["WAIT", "1", "100"]), 0, "no replica yet");
    let replica = replica_of(&dr, &master, &[]);
    wait_synced(&replica);
    m.ok(&["SET", "w", "1"]);
    assert_eq!(m.int(&["WAIT", "1", "5000"]), 1);
    let t = std::time::Instant::now();
    assert_eq!(m.int(&["WAIT", "2", "300"]), 1);
    assert!(
        t.elapsed() >= Duration::from_millis(250),
        "WAIT did not wait"
    );
}

#[test]
fn expired_keys_disappear_on_the_replica_with_the_master() {
    let (dm, dr) = (TmpDir::new("repl-e-m"), TmpDir::new("repl-e-r"));
    let master = Server::start(&dm.0, &[]);
    let replica = replica_of(&dr, &master, &[]);
    wait_synced(&replica);
    let mut m = master.client();
    let mut r = replica.client();
    m.ok(&["SET", "gone", "v", "PX", "300"]);
    wait_until(Duration::from_secs(5), || r.get("gone").is_some());
    std::thread::sleep(Duration::from_millis(400));
    // Read as missing on the replica as soon as the TTL passed…
    assert_eq!(r.get("gone"), None);
    // …and removed once the master's DEL arrives.
    wait_until(Duration::from_secs(5), || r.int(&["DBSIZE"]) == 0);
}

/// A TCP proxy whose connections the test can cut.
struct Proxy {
    port: u16,
    cut: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<TcpStream>>>,
}

impl Proxy {
    fn start(target: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let cut = Arc::new(AtomicBool::new(false));
        let conns: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
        let (c2, k2) = (conns.clone(), cut.clone());
        std::thread::spawn(move || {
            for inbound in listener.incoming() {
                let Ok(inbound) = inbound else { continue };
                if k2.load(Ordering::SeqCst) {
                    continue;
                }
                let Ok(outbound) = TcpStream::connect(("127.0.0.1", target)) else {
                    continue;
                };
                c2.lock().unwrap().push(inbound.try_clone().unwrap());
                c2.lock().unwrap().push(outbound.try_clone().unwrap());
                let pipe = |mut a: TcpStream, mut b: TcpStream| {
                    std::thread::spawn(move || {
                        let mut buf = [0u8; 16384];
                        while let Ok(n) = a.read(&mut buf) {
                            if n == 0 || b.write_all(&buf[..n]).is_err() {
                                break;
                            }
                        }
                        let _ = b.shutdown(std::net::Shutdown::Both);
                    });
                };
                pipe(inbound.try_clone().unwrap(), outbound.try_clone().unwrap());
                pipe(outbound, inbound);
            }
        });
        Self { port, cut, conns }
    }

    /// Cut every connection; refuse new ones while `hold`.
    fn cut(&self, hold: bool) {
        self.cut.store(hold, Ordering::SeqCst);
        for c in self.conns.lock().unwrap().drain(..) {
            let _ = c.shutdown(std::net::Shutdown::Both);
        }
    }
}

#[test]
fn a_dropped_link_resumes_with_a_partial_resynchronization() {
    let (dm, dr) = (TmpDir::new("repl-p-m"), TmpDir::new("repl-p-r"));
    let master = Server::start(&dm.0, &[]);
    let proxy = Proxy::start(master.port);
    let spec = format!("127.0.0.1 {}", proxy.port);
    let replica = Server::start(&dr.0, &["--replicaof", &spec]);
    wait_synced(&replica);
    let mut m = master.client();
    let mut r = replica.client();
    m.cmd(&["INCR", "c"]);
    wait_until(Duration::from_secs(10), || {
        r.get("c").as_deref() == Some("1")
    });
    let full_before = stat(&m.info("replication"), "sync_full");

    proxy.cut(true);
    for _ in 0..20 {
        m.cmd(&["INCR", "c"]);
    }
    std::thread::sleep(Duration::from_millis(300));
    proxy.cut(false);
    wait_until(Duration::from_secs(20), || {
        r.get("c").as_deref() == Some("21")
    });
    let info = m.info("replication");
    assert_eq!(
        stat(&info, "sync_full"),
        full_before,
        "resynchronized everything:\n{info}"
    );
    assert!(stat(&info, "sync_partial_ok") >= 1, "{info}");
}

fn stat(info: &str, name: &str) -> u64 {
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{name}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

#[test]
fn a_promoted_replica_keeps_the_data_and_takes_writes() {
    let (dm, dr) = (TmpDir::new("repl-pr-m"), TmpDir::new("repl-pr-r"));
    let master = Server::start(&dm.0, &[]);
    let replica = replica_of(&dr, &master, &[]);
    wait_synced(&replica);
    let mut m = master.client();
    let mut r = replica.client();
    m.ok(&["SET", "k", "v"]);
    wait_until(Duration::from_secs(10), || r.get("k").is_some());
    r.ok(&["REPLICAOF", "NO", "ONE"]);
    assert!(r.cmd(&["ROLE"]).contains("master"));
    r.ok(&["SET", "k2", "v2"]);
    assert_eq!(r.get("k").as_deref(), Some("v"));
    let info = r.info("replication");
    assert!(info.contains("role:master"));
    assert!(!info.contains("master_replid2:0000000000000000000000000000000000000000"));
}
