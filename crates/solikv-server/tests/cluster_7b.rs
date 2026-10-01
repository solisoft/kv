//! Replica migration between real cluster nodes: a master left without a
//! working replica gets one from a master that has more than the barrier.

mod common;

use common::{wait_until, Client, Server, TmpDir};
use std::time::Duration;

const SECRET: &str = "a-cluster-bus-secret-of-at-least-32-bytes!";

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
                "--cluster-bus-secret",
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
}

/// The id of the master `node` replicates, as `CLUSTER NODES` on `node`
/// says ("-" for a master).
fn master_of(node: &Node) -> String {
    let nodes = node.c().bulk(&["CLUSTER", "NODES"]);
    let line = nodes
        .lines()
        .find(|l| l.contains("myself"))
        .unwrap_or_default()
        .to_string();
    line.split_whitespace().nth(3).unwrap_or("").to_string()
}

#[test]
fn an_orphaned_master_gets_a_replica_from_a_master_with_two() {
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(150));
        eprintln!("watchdog: test exceeded 150 s");
        std::process::exit(101);
    });
    let a = Node::start("mig-a");
    let b = Node::start("mig-b");
    let r1 = Node::start("mig-r1");
    let r2 = Node::start("mig-r2");
    let r3 = Node::start("mig-r3");
    for n in [&b, &r1, &r2, &r3] {
        a.c()
            .ok(&["CLUSTER", "MEET", "127.0.0.1", &n.srv.port.to_string()]);
    }
    a.c().ok(&["CLUSTER", "ADDSLOTSRANGE", "0", "8191"]);
    b.c().ok(&["CLUSTER", "ADDSLOTSRANGE", "8192", "16383"]);
    for n in [&a, &b, &r1, &r2, &r3] {
        wait_until(Duration::from_secs(30), || {
            n.c()
                .bulk(&["CLUSTER", "INFO"])
                .contains("cluster_known_nodes:5\r")
        });
    }
    let (aid, bid) = (a.id(), b.id());
    r1.c().ok(&["CLUSTER", "REPLICATE", &aid]);
    r2.c().ok(&["CLUSTER", "REPLICATE", &aid]);
    r3.c().ok(&["CLUSTER", "REPLICATE", &bid]);
    for r in [&r1, &r2, &r3] {
        wait_until(Duration::from_secs(30), || {
            r.c().info("replication").contains("master_link_status:up")
                && r.c()
                    .bulk(&["CLUSTER", "INFO"])
                    .contains("cluster_state:ok")
        });
    }
    // Every node must have seen b with its replica before it goes.
    std::thread::sleep(Duration::from_secs(2));

    // b's only replica dies: b is orphaned, and the replica of a with the
    // smallest id moves to it.
    let mut r3 = r3;
    r3.srv.kill();
    let mover = if r1.id() < r2.id() { &r1 } else { &r2 };
    let stayer = if r1.id() < r2.id() { &r2 } else { &r1 };
    wait_until(Duration::from_secs(60), || master_of(mover) == bid);
    wait_until(Duration::from_secs(30), || {
        let info = mover.c().info("replication");
        info.contains(&format!("master_port:{}", b.srv.port))
            && info.contains("master_link_status:up")
    });
    // The other stays with a: a keeps its barrier of one.
    assert_eq!(master_of(stayer), aid);
    // The others learn of the move.
    let mover_id = mover.id();
    wait_until(Duration::from_secs(30), || {
        a.c()
            .bulk(&["CLUSTER", "NODES"])
            .lines()
            .any(|l| l.starts_with(&mover_id) && l.split_whitespace().nth(3) == Some(bid.as_str()))
    });
}

#[test]
fn replica_migration_can_be_turned_off() {
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(120));
        eprintln!("watchdog: test exceeded 120 s");
        std::process::exit(101);
    });
    let a = Node::start("nomig-a");
    let b = Node::start("nomig-b");
    let start_replica = |tag: &str| {
        let dir = TmpDir::new(tag);
        let srv = Server::start(
            &dir.0,
            &[
                "--cluster-enabled",
                "--cluster-bus-secret",
                SECRET,
                "--cluster-node-timeout",
                "1500",
                "--cluster-allow-replica-migration",
                "no",
            ],
        );
        Node { srv, _dir: dir }
    };
    let r1 = start_replica("nomig-r1");
    let r2 = start_replica("nomig-r2");
    let r3 = Node::start("nomig-r3");
    for n in [&b, &r1, &r2, &r3] {
        a.c()
            .ok(&["CLUSTER", "MEET", "127.0.0.1", &n.srv.port.to_string()]);
    }
    a.c().ok(&["CLUSTER", "ADDSLOTSRANGE", "0", "8191"]);
    b.c().ok(&["CLUSTER", "ADDSLOTSRANGE", "8192", "16383"]);
    for n in [&a, &b, &r1, &r2, &r3] {
        wait_until(Duration::from_secs(30), || {
            n.c()
                .bulk(&["CLUSTER", "INFO"])
                .contains("cluster_known_nodes:5\r")
        });
    }
    let (aid, bid) = (a.id(), b.id());
    r1.c().ok(&["CLUSTER", "REPLICATE", &aid]);
    r2.c().ok(&["CLUSTER", "REPLICATE", &aid]);
    r3.c().ok(&["CLUSTER", "REPLICATE", &bid]);
    for r in [&r1, &r2, &r3] {
        wait_until(Duration::from_secs(30), || {
            r.c().info("replication").contains("master_link_status:up")
        });
    }
    std::thread::sleep(Duration::from_secs(2));
    let mut r3 = r3;
    r3.srv.kill();
    // Well past node timeout + the migration delay: nobody moved.
    std::thread::sleep(Duration::from_secs(12));
    let diag = || {
        format!(
            "a={aid} b={bid} r1={} r2={}\nr2 sees:\n{}\nr2 log:\n{}",
            r1.id(),
            r2.id(),
            r2.c().bulk(&["CLUSTER", "NODES"]),
            std::fs::read_to_string(r2._dir.0.join(format!("stderr-{}.log", r2.srv.port)))
                .unwrap_or_default()
        )
    };
    assert_eq!(master_of(&r1), aid, "{}", diag());
    assert_eq!(master_of(&r2), aid, "{}", diag());
}
