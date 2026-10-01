//! Cluster mode between real server processes: forming, routing a Redis
//! cluster client, redirections, resharding with MIGRATE, failover.

mod common;

use common::{wait_until, Client, Server, TmpDir};
use std::time::Duration;

const SECRET: &str = "a-cluster-password-of-at-least-32-bytes!!";

struct Node {
    srv: Server,
    _dir: TmpDir,
}

impl Node {
    fn start(tag: &str) -> Self {
        let dir = TmpDir::new(tag);
        let srv = Server::start(
            &dir.0,
            &[
                "--cluster-enabled",
                "--cluster-password",
                SECRET,
                "--cluster-node-timeout",
                "1500",
                "--shards",
                "2",
            ],
        );
        Self { srv, _dir: dir }
    }

    fn c(&self) -> Client {
        self.srv.client()
    }

    fn id(&self) -> String {
        self.c().bulk(&["CLUSTER", "MYID"]).trim_end().to_string()
    }

    fn port(&self) -> u16 {
        self.srv.port
    }
}

fn cluster_ok(n: &Node, known: usize) -> bool {
    let info = n.c().bulk(&["CLUSTER", "INFO"]);
    info.contains("cluster_state:ok") && info.contains(&format!("cluster_known_nodes:{known}\r"))
}

/// Three masters, slots split as `redis-cli --cluster create` would.
fn three_masters() -> Vec<Node> {
    let nodes: Vec<Node> = (0..3).map(|i| Node::start(&format!("c{i}"))).collect();
    for n in &nodes[1..] {
        nodes[0]
            .c()
            .ok(&["CLUSTER", "MEET", "127.0.0.1", &n.port().to_string()]);
    }
    let ranges = [("0", "5460"), ("5461", "10922"), ("10923", "16383")];
    for (n, (a, b)) in nodes.iter().zip(ranges) {
        n.c().ok(&["CLUSTER", "ADDSLOTSRANGE", a, b]);
    }
    for n in &nodes {
        wait_until(Duration::from_secs(30), || cluster_ok(n, 3));
    }
    nodes
}

fn slot_of(c: &mut Client, key: &str) -> u16 {
    c.int(&["CLUSTER", "KEYSLOT", key]) as u16
}

fn owner_index(slot: u16) -> usize {
    match slot {
        0..=5460 => 0,
        5461..=10922 => 1,
        _ => 2,
    }
}

#[test]
fn three_masters_form_and_a_cluster_client_routes_through_them() {
    let nodes = three_masters();
    let urls: Vec<String> = nodes
        .iter()
        .map(|n| format!("redis://127.0.0.1:{}/", n.port()))
        .collect();
    let client = redis::cluster::ClusterClient::new(urls).unwrap();
    let mut con = client.get_connection().unwrap();
    for i in 0..200 {
        let _: () = redis::cmd("SET")
            .arg(format!("key:{i}"))
            .arg(i)
            .query(&mut con)
            .unwrap();
    }
    for i in 0..200 {
        let v: i64 = redis::cmd("GET")
            .arg(format!("key:{i}"))
            .query(&mut con)
            .unwrap();
        assert_eq!(v, i);
    }
    // Spread over every master, each holding only its slots' keys.
    for n in &nodes {
        let dbsize = n.c().int(&["DBSIZE"]);
        assert!(dbsize > 20, "a master got {dbsize} of 200 keys");
    }

    // CLUSTER SLOTS: Redis' shape, every node's ranges.
    let slots = nodes[1].c().cmd(&["CLUSTER", "SLOTS"]);
    assert!(slots.starts_with("*3\r\n"), "{slots}");
    for n in &nodes {
        assert!(slots.contains(&format!(":{}\r\n", n.port())), "{slots}");
    }
    let shards = nodes[2].c().cmd(&["CLUSTER", "SHARDS"]);
    assert!(shards.starts_with("*3\r\n"), "{shards}");
    let lines = nodes[0].c().bulk(&["CLUSTER", "NODES"]);
    assert_eq!(
        lines.lines().filter(|l| !l.is_empty()).count(),
        3,
        "{lines}"
    );
    assert!(lines.contains("myself,master"));
}

#[test]
fn redirections_moved_crossslot_and_ask_during_a_migration() {
    let nodes = three_masters();
    let mut c0 = nodes[0].c();

    // A key owned by node 1, asked of node 0.
    let key = (0..)
        .map(|i| format!("k{i}"))
        .find(|k| owner_index(slot_of(&mut c0, k)) == 1)
        .unwrap();
    let slot = slot_of(&mut c0, &key);
    let r = c0.cmd(&["SET", &key, "v"]);
    assert_eq!(
        r,
        format!("-MOVED {} 127.0.0.1:{}\r\n", slot, nodes[1].port())
    );
    let r = c0.cmd(&["MSET", "{a}1", "x", "{b}2", "y"]);
    assert!(r.starts_with("-CROSSSLOT"), "{r}");

    // Migrate that slot from node 1 to node 2, with keys in it.
    let mut c1 = nodes[1].c();
    let mut c2 = nodes[2].c();
    let tag = format!("{{{key}}}");
    for i in 0..5 {
        c1.ok(&["SET", &format!("{tag}:{i}"), &i.to_string()]);
    }
    c1.ok(&["SET", &key, "v"]);
    let (id1, id2) = (nodes[1].id(), nodes[2].id());
    let s = slot.to_string();
    c2.ok(&["CLUSTER", "SETSLOT", &s, "IMPORTING", &id1]);
    c1.ok(&["CLUSTER", "SETSLOT", &s, "MIGRATING", &id2]);
    assert_eq!(c1.int(&["CLUSTER", "COUNTKEYSINSLOT", &s]), 6);
    // Move one key: the source answers ASK for it, the target serves it
    // after ASKING, not without.
    let moved_key = format!("{tag}:0");
    let r = c1.cmd(&[
        "MIGRATE",
        "127.0.0.1",
        &nodes[2].port().to_string(),
        &moved_key,
        "0",
        "5000",
    ]);
    assert_eq!(r, "+OK\r\n");
    let r = c1.cmd(&["GET", &moved_key]);
    assert_eq!(
        r,
        format!("-ASK {} 127.0.0.1:{}\r\n", slot, nodes[2].port())
    );
    assert!(c2.cmd(&["GET", &moved_key]).starts_with("-MOVED"));
    c2.ok(&["ASKING"]);
    assert_eq!(c2.cmd(&["GET", &moved_key]), "$1\r\n0\r\n");
    // A multi-key command split between the two: TRYAGAIN.
    let r = c1.cmd(&["MGET", &moved_key, &format!("{tag}:1")]);
    assert!(r.starts_with("-TRYAGAIN"), "{r}");
    // The rest, as redis-cli --cluster reshard does it.
    let keys = c1.cmd(&["CLUSTER", "GETKEYSINSLOT", &s, "100"]);
    let names: Vec<String> = keys
        .split("\r\n")
        .filter(|l| !l.is_empty() && !l.starts_with('*') && !l.starts_with('$'))
        .map(String::from)
        .collect();
    assert_eq!(names.len(), 5, "{keys}");
    let mut args = vec![
        "MIGRATE".to_string(),
        "127.0.0.1".into(),
        nodes[2].port().to_string(),
        String::new(),
        "0".into(),
        "5000".into(),
        "KEYS".into(),
    ];
    args.extend(names);
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    assert_eq!(c1.cmd(&refs), "+OK\r\n");
    assert_eq!(c1.int(&["CLUSTER", "COUNTKEYSINSLOT", &s]), 0);
    c2.ok(&["CLUSTER", "SETSLOT", &s, "NODE", &id2]);
    c1.ok(&["CLUSTER", "SETSLOT", &s, "NODE", &id2]);
    // Everyone learns the new owner (its epoch went up).
    wait_until(Duration::from_secs(15), || {
        c0.cmd(&["GET", &key]) == format!("-MOVED {} 127.0.0.1:{}\r\n", slot, nodes[2].port())
    });
    assert_eq!(c2.get(&key).as_deref(), Some("v"));
    assert_eq!(c2.get(&format!("{tag}:4")).as_deref(), Some("4"));
}

#[test]
fn a_replica_takes_over_when_its_master_fails() {
    let mut nodes = three_masters();
    let replica = Node::start("c-rep");
    replica
        .c()
        .ok(&["CLUSTER", "MEET", "127.0.0.1", &nodes[0].port().to_string()]);
    wait_until(Duration::from_secs(20), || {
        replica
            .c()
            .bulk(&["CLUSTER", "INFO"])
            .contains("cluster_known_nodes:4\r")
    });
    let master_id = nodes[0].id();
    replica.c().ok(&["CLUSTER", "REPLICATE", &master_id]);
    wait_until(Duration::from_secs(20), || {
        replica
            .c()
            .info("replication")
            .contains("master_link_status:up")
    });
    // Data on master 0, replicated.
    let mut c0 = nodes[0].c();
    let key = (0..)
        .map(|i| format!("f{i}"))
        .find(|k| owner_index(slot_of(&mut c0, k)) == 0)
        .unwrap();
    c0.ok(&["SET", &key, "kept"]);
    wait_until(Duration::from_secs(20), || cluster_ok_any(&replica));
    let mut cr = replica.c();
    cr.ok(&["READONLY"]);
    wait_until(Duration::from_secs(10), || {
        cr.cmd(&["GET", &key]) == "$4\r\nkept\r\n"
    });
    // The others see it as a replica of master 0.
    wait_until(Duration::from_secs(10), || {
        nodes[1]
            .c()
            .bulk(&["CLUSTER", "NODES"])
            .lines()
            .any(|l| l.contains(" slave ") && l.contains(&master_id))
    });

    nodes[0].srv.kill();
    // FAIL by majority, election, promotion: the replica serves the slots.
    wait_until(Duration::from_secs(40), || {
        replica
            .c()
            .bulk(&["CLUSTER", "NODES"])
            .lines()
            .any(|l| l.contains("myself,master") && l.contains("0-5460"))
    });
    wait_until(Duration::from_secs(20), || cluster_ok_any(&nodes[1]));
    let mut c1 = nodes[1].c();
    let moved = c1.cmd(&["GET", &key]);
    assert_eq!(
        moved,
        format!(
            "-MOVED {} 127.0.0.1:{}\r\n",
            slot_of(&mut c1, &key),
            replica.port()
        )
    );
    wait_until(Duration::from_secs(20), || cluster_ok_any(&replica));
    let mut cr = replica.c();
    assert_eq!(cr.get(&key).as_deref(), Some("kept"));
    cr.ok(&["SET", &key, "after"]);
    assert!(cr.info("replication").contains("role:master"));
    drop(nodes);
}

fn cluster_ok_any(n: &Node) -> bool {
    n.c()
        .bulk(&["CLUSTER", "INFO"])
        .contains("cluster_state:ok")
}

#[test]
fn a_manual_failover_swaps_master_and_replica() {
    let nodes = three_masters();
    let replica = Node::start("c-mf");
    replica
        .c()
        .ok(&["CLUSTER", "MEET", "127.0.0.1", &nodes[1].port().to_string()]);
    wait_until(Duration::from_secs(20), || {
        replica
            .c()
            .bulk(&["CLUSTER", "INFO"])
            .contains("cluster_known_nodes:4\r")
    });
    let master_id = nodes[1].id();
    replica.c().ok(&["CLUSTER", "REPLICATE", &master_id]);
    wait_until(Duration::from_secs(20), || {
        replica
            .c()
            .info("replication")
            .contains("master_link_status:up")
    });
    let mut c1 = nodes[1].c();
    let key = (0..)
        .map(|i| format!("m{i}"))
        .find(|k| owner_index(slot_of(&mut c1, k)) == 1)
        .unwrap();
    c1.ok(&["SET", &key, "before"]);

    replica.c().ok(&["CLUSTER", "FAILOVER"]);
    wait_until(Duration::from_secs(30), || {
        replica.c().info("replication").contains("role:master")
            && nodes[1].c().info("replication").contains("role:slave")
    });
    wait_until(Duration::from_secs(20), || cluster_ok_any(&replica));
    let mut cr = replica.c();
    assert_eq!(cr.get(&key).as_deref(), Some("before"));
    cr.ok(&["SET", &key, "after"]);
    // The old master follows the new one.
    wait_until(Duration::from_secs(20), || {
        let mut c = nodes[1].c();
        c.ok(&["READONLY"]);
        c.cmd(&["GET", &key]) == "$5\r\nafter\r\n"
    });
}

#[test]
fn publish_reaches_subscribers_on_every_node() {
    let nodes = three_masters();
    let mut sub = nodes[2].c();
    assert!(sub.cmd(&["SUBSCRIBE", "news"]).contains("subscribe"));
    // Relayed over the bus: retry until the link is up.
    let mut got = String::new();
    for _ in 0..50 {
        nodes[0].c().int(&["PUBLISH", "news", "hello"]);
        sub_read_timeout(&mut sub, &mut got);
        if got.contains("hello") {
            break;
        }
    }
    assert!(got.contains("hello"), "{got}");
}

fn sub_read_timeout(c: &mut Client, got: &mut String) {
    // A short read: a message, or nothing yet.
    if let Some(r) = c.try_reply(Duration::from_millis(200)) {
        got.push_str(&r);
    }
}

#[test]
fn cluster_state_survives_a_restart() {
    let dir = TmpDir::new("c-restart");
    let args = [
        "--cluster-enabled",
        "--cluster-password",
        SECRET,
        "--cluster-node-timeout",
        "1500",
    ];
    let port = common::next_port();
    let srv = Server::start_on(port, &dir.0, &args);
    let id = srv.client().bulk(&["CLUSTER", "MYID"]);
    srv.client().ok(&["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]);
    srv.client().ok(&["SET", "x", "1"]);
    let mut srv = srv;
    srv.terminate();
    drop(srv);
    let srv = Server::start_on(port, &dir.0, &args);
    let mut c = srv.client();
    assert_eq!(c.bulk(&["CLUSTER", "MYID"]), id);
    wait_until(Duration::from_secs(10), || {
        c.bulk(&["CLUSTER", "INFO"]).contains("cluster_state:ok")
    });
    let slot = c.int(&["CLUSTER", "KEYSLOT", "x"]).to_string();
    assert_eq!(c.int(&["CLUSTER", "COUNTKEYSINSLOT", &slot]), 1);
    assert_eq!(c.get("x").as_deref(), Some("1"));
}
