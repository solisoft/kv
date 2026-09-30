//! Redis 7 commands added for parity, through the real binary on a
//! multi-shard server with the append-only log on: blocking commands (wake
//! order, timeouts, disconnects, transactions, chains across shards, what the
//! log replays), and the list, sorted-set, set, hash, key, string, geo and
//! stream commands that were missing.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static PORT_COUNTER: AtomicU16 = AtomicU16::new(23400);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

struct TmpDir(PathBuf);

impl TmpDir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "solikv-5a-{}-{}-{}",
            tag,
            std::process::id(),
            next_port()
        ));
        let _ = std::fs::remove_dir_all(&p);
        Self(p)
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Server {
    child: Child,
    port: u16,
    _dir: Option<TmpDir>,
}

impl Server {
    /// A fresh 8-shard server with the log on.
    fn new(tag: &str) -> Self {
        let dir = TmpDir::new(tag);
        let mut s = Self::start(&dir.0, &["--shards", "8"]);
        s._dir = Some(dir);
        s
    }

    fn start(dir: &Path, extra: &[&str]) -> Self {
        let port = next_port();
        let child = Command::new(env!("CARGO_BIN_EXE_solikv"))
            .args(["--port", &port.to_string(), "--rest-port", "0"])
            .args(["--bind", "127.0.0.1", "--log-level", "warn"])
            .arg("--dir")
            .arg(dir)
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn solikv");
        let deadline = Instant::now() + Duration::from_secs(20);
        while !Self::answers(port) {
            assert!(Instant::now() < deadline, "server did not start");
            thread::sleep(Duration::from_millis(20));
        }
        Self {
            child,
            port,
            _dir: None,
        }
    }

    fn answers(port: u16) -> bool {
        let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) else {
            return false;
        };
        let _ = s.set_read_timeout(Some(Duration::from_millis(500)));
        if s.write_all(b"*1\r\n$4\r\nPING\r\n").is_err() {
            return false;
        }
        let mut buf = [0u8; 7];
        s.read_exact(&mut buf).is_ok() && &buf == b"+PONG\r\n"
    }

    fn client(&self) -> Client {
        let s = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        Client {
            r: BufReader::new(s.try_clone().unwrap()),
            w: s,
        }
    }

    fn sigterm(mut self) {
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(self.child.id() as i32),
            nix::sys::signal::Signal::SIGTERM,
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if self.child.try_wait().unwrap().is_some() {
                return;
            }
            assert!(Instant::now() < deadline, "server did not exit");
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Client {
    r: BufReader<TcpStream>,
    w: TcpStream,
}

impl Client {
    fn send(&mut self, args: &[&str]) {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        self.w.write_all(out.as_bytes()).unwrap();
    }

    /// Send a command; return the reply as text: `:5`, `$v`, `+OK`,
    /// `-ERR …`, `nil` (null bulk), `nilarr` (null array), arrays as
    /// `[a,b]`.
    fn cmd(&mut self, args: &[&str]) -> String {
        self.send(args);
        self.reply()
    }

    fn reply(&mut self) -> String {
        let mut line = String::new();
        self.r.read_line(&mut line).unwrap();
        let line = line.trim_end().to_string();
        match line.as_bytes().first() {
            Some(b'$') => {
                let n: i64 = line[1..].parse().unwrap();
                if n < 0 {
                    return "nil".into();
                }
                let mut buf = vec![0u8; n as usize + 2];
                self.r.read_exact(&mut buf).unwrap();
                format!("${}", String::from_utf8_lossy(&buf[..n as usize]))
            }
            Some(b'*') => {
                let n: i64 = line[1..].parse().unwrap();
                if n < 0 {
                    return "nilarr".into();
                }
                let items: Vec<String> = (0..n).map(|_| self.reply()).collect();
                format!("[{}]", items.join(","))
            }
            _ => line,
        }
    }
}

/// Keys on different shards of an 8-shard server (slot mod 8).
fn keys_on_different_shards(prefix: &str) -> (String, String) {
    let m = solikv_engine::ShardManager::new(8);
    let a = format!("{prefix}:a");
    let ia = m.shard_index_for_key(a.as_bytes());
    for i in 0.. {
        let b = format!("{prefix}:b{i}");
        if m.shard_index_for_key(b.as_bytes()) != ia {
            return (a, b);
        }
    }
    unreachable!()
}

/// Wait until `n` clients are blocked (INFO clients).
fn wait_blocked(c: &mut Client, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let info = c.cmd(&["INFO", "clients"]);
        if info.contains(&format!("blocked_clients:{n}\r")) {
            return;
        }
        assert!(Instant::now() < deadline, "never saw {n} blocked: {info}");
        thread::sleep(Duration::from_millis(10));
    }
}

// ---------------------------------------------------------------------------
// Blocking
// ---------------------------------------------------------------------------

#[test]
fn blpop_wakes_blocked_clients_in_the_order_they_blocked() {
    let s = Server::new("fifo");
    let mut c = s.client();
    let mut waiters = Vec::new();
    for i in 0..3 {
        let mut w = s.client();
        w.send(&["BLPOP", "q", "0"]);
        wait_blocked(&mut c, i + 1);
        waiters.push(w);
    }
    assert_eq!(c.cmd(&["RPUSH", "q", "a", "b", "c"]), ":3");
    let got: Vec<String> = waiters.iter_mut().map(|w| w.reply()).collect();
    assert_eq!(got, vec!["[$q,$a]", "[$q,$b]", "[$q,$c]"]);
    assert_eq!(c.cmd(&["EXISTS", "q"]), ":0");
    wait_blocked(&mut c, 0);
}

#[test]
fn blocking_timeouts_and_immediate_answers() {
    let s = Server::new("timeout");
    let mut c = s.client();
    let t = Instant::now();
    assert_eq!(c.cmd(&["BLPOP", "none", "0.2"]), "nilarr");
    assert!(t.elapsed() >= Duration::from_millis(190));
    assert_eq!(
        c.cmd(&["BLMOVE", "none", "x", "LEFT", "LEFT", "0.05"]),
        "nil"
    );
    assert_eq!(c.cmd(&["BZPOPMIN", "none", "0.05"]), "nilarr");
    assert!(c
        .cmd(&["BLPOP", "none", "-1"])
        .starts_with("-ERR timeout is negative"));
    assert!(c
        .cmd(&["BLPOP", "none", "abc"])
        .starts_with("-ERR timeout is not a float"));
    // Data already there: served at once, from the first non-empty key.
    let (a, b) = keys_on_different_shards("imm");
    c.cmd(&["RPUSH", &b, "x"]);
    assert_eq!(c.cmd(&["BRPOP", &a, &b, "1"]), format!("[${b},$x]"));
    c.cmd(&["SET", "str", "v"]);
    assert!(c.cmd(&["BLPOP", "str", "1"]).starts_with("-WRONGTYPE"));
    // The connection still works after a timeout.
    assert_eq!(c.cmd(&["PING"]), "+PONG");
}

#[test]
fn blocked_keys_across_shards_and_chained_moves() {
    let s = Server::new("chain");
    let mut c = s.client();
    let (src, dst) = keys_on_different_shards("chain");
    let mut mover = s.client();
    mover.send(&["BLMOVE", &src, &dst, "RIGHT", "LEFT", "0"]);
    wait_blocked(&mut c, 1);
    let mut popper = s.client();
    popper.send(&["BLPOP", "other", &dst, "0"]);
    wait_blocked(&mut c, 2);
    c.cmd(&["LPUSH", &src, "v"]);
    assert_eq!(mover.reply(), "$v");
    assert_eq!(popper.reply(), format!("[${dst},$v]"));
    assert_eq!(c.cmd(&["EXISTS", &src, &dst]), ":0");
}

#[test]
fn transactions_do_not_block_and_serve_after_exec() {
    let s = Server::new("multi");
    let mut c = s.client();
    // Inside MULTI a blocking command answers at once.
    c.cmd(&["MULTI"]);
    c.cmd(&["BLPOP", "empty", "0"]);
    assert_eq!(c.cmd(&["EXEC"]), "[nilarr]");

    let mut w = s.client();
    w.send(&["BLPOP", "tx", "0"]);
    wait_blocked(&mut c, 1);
    c.cmd(&["MULTI"]);
    c.cmd(&["RPUSH", "tx", "a"]);
    c.cmd(&["LLEN", "tx"]);
    // Nobody is served in the middle of the transaction.
    assert_eq!(c.cmd(&["EXEC"]), "[:1,:1]");
    assert_eq!(w.reply(), "[$tx,$a]");
}

#[test]
fn disconnected_and_unblocked_clients_are_forgotten() {
    let s = Server::new("gone");
    let mut c = s.client();
    {
        let mut w = s.client();
        w.send(&["BLPOP", "g", "0"]);
        wait_blocked(&mut c, 1);
    }
    wait_blocked(&mut c, 0);
    c.cmd(&["RPUSH", "g", "kept"]);
    assert_eq!(c.cmd(&["LLEN", "g"]), ":1");

    let mut w = s.client();
    let id = w.cmd(&["CLIENT", "ID"]);
    w.send(&["BLPOP", "u", "0"]);
    wait_blocked(&mut c, 1);
    assert_eq!(c.cmd(&["CLIENT", "UNBLOCK", &id[1..]]), ":1");
    assert_eq!(w.reply(), "nilarr");
    w.send(&["BLPOP", "u", "0"]);
    wait_blocked(&mut c, 1);
    assert_eq!(c.cmd(&["CLIENT", "UNBLOCK", &id[1..], "ERROR"]), ":1");
    assert!(w.reply().starts_with("-UNBLOCKED"));
    assert_eq!(c.cmd(&["CLIENT", "UNBLOCK", "999999"]), ":0");
}

#[test]
fn sorted_set_and_multi_pop_blocking() {
    let s = Server::new("bz");
    let mut c = s.client();
    let mut w = s.client();
    w.send(&["BZPOPMAX", "z1", "z2", "0"]);
    wait_blocked(&mut c, 1);
    c.cmd(&["ZADD", "z2", "1", "a", "2", "b"]);
    assert_eq!(w.reply(), "[$z2,$b,$2]");

    w.send(&["BZMPOP", "0", "2", "z3", "z2", "MIN", "COUNT", "5"]);
    assert_eq!(w.reply(), "[$z2,[[$a,$1]]]");

    w.send(&["BLMPOP", "0", "1", "l", "RIGHT", "COUNT", "2"]);
    wait_blocked(&mut c, 1);
    c.cmd(&["RPUSH", "l", "1", "2", "3"]);
    assert_eq!(w.reply(), "[$l,[$3,$2]]");
}

#[test]
fn xread_and_xreadgroup_block_for_new_entries() {
    let s = Server::new("xread");
    let mut c = s.client();
    c.cmd(&["XADD", "st", "1-1", "f", "old"]);
    let mut w = s.client();
    w.send(&["XREAD", "BLOCK", "0", "STREAMS", "st", "$"]);
    wait_blocked(&mut c, 1);
    c.cmd(&["XADD", "st", "2-1", "f", "new"]);
    assert_eq!(w.reply(), "[[$st,[[$2-1,[$f,$new]]]]]");
    assert_eq!(
        w.cmd(&["XREAD", "BLOCK", "50", "STREAMS", "st", "$"]),
        "nilarr"
    );
    assert_eq!(w.cmd(&["XREAD", "STREAMS", "st", "9-9"]), "nilarr");

    c.cmd(&["XGROUP", "CREATE", "st", "g", "$"]);
    w.send(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "alice",
        "BLOCK",
        "0",
        "STREAMS",
        "st",
        ">",
    ]);
    wait_blocked(&mut c, 1);
    c.cmd(&["XADD", "st", "3-1", "f", "x"]);
    assert_eq!(w.reply(), "[[$st,[[$3-1,[$f,$x]]]]]");
    assert!(c.cmd(&["XPENDING", "st", "g"]).starts_with("[:1,$3-1,$3-1"));
    // Unknown group: an error at once, not a block.
    assert!(w
        .cmd(&[
            "XREADGROUP",
            "GROUP",
            "nope",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "st",
            ">"
        ])
        .starts_with("-NOGROUP"));
}

#[test]
fn served_pops_replay_from_the_log() {
    let dir = TmpDir::new("replay");
    let s = Server::start(&dir.0, &["--shards", "8"]);
    let (src, dst) = keys_on_different_shards("rep");
    {
        let mut c = s.client();
        let mut w = s.client();
        w.send(&["BLMOVE", &src, &dst, "LEFT", "RIGHT", "0"]);
        wait_blocked(&mut c, 1);
        c.cmd(&["RPUSH", &src, "a", "b"]);
        assert_eq!(w.reply(), "$a");
        let mut p = s.client();
        p.send(&["BLPOP", "bl", "0"]);
        wait_blocked(&mut c, 1);
        c.cmd(&["RPUSH", "bl", "x", "y"]);
        assert_eq!(p.reply(), "[$bl,$x]");
        assert_eq!(
            c.cmd(&["LMPOP", "2", "none", &src, "LEFT"]),
            format!("[${src},[$b]]")
        );
    }
    s.sigterm();
    let s = Server::start(&dir.0, &["--shards", "8"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["LRANGE", &dst, "0", "-1"]), "[$a]");
    assert_eq!(c.cmd(&["LRANGE", "bl", "0", "-1"]), "[$y]");
    assert_eq!(c.cmd(&["EXISTS", &src]), ":0");
}

// ---------------------------------------------------------------------------
// Lists
// ---------------------------------------------------------------------------

#[test]
fn list_family() {
    let s = Server::new("lists");
    let mut c = s.client();
    let (a, b) = keys_on_different_shards("lm");
    c.cmd(&["RPUSH", &a, "1", "2", "3"]);
    assert_eq!(c.cmd(&["LMOVE", &a, &b, "LEFT", "RIGHT"]), "$1");
    assert_eq!(c.cmd(&["RPOPLPUSH", &a, &b]), "$3");
    assert_eq!(c.cmd(&["LRANGE", &b, "0", "-1"]), "[$3,$1]");
    assert_eq!(c.cmd(&["LMOVE", "missing", &b, "LEFT", "LEFT"]), "nil");
    c.cmd(&["SET", "s", "x"]);
    assert!(c
        .cmd(&["LMOVE", &a, "s", "LEFT", "LEFT"])
        .starts_with("-WRONGTYPE"));
    assert_eq!(c.cmd(&["LLEN", &a]), ":1");
    assert!(c
        .cmd(&["LMOVE", &a, &b, "UP", "LEFT"])
        .starts_with("-ERR syntax"));
    // Same list: a rotation.
    c.cmd(&["RPUSH", "rot", "a", "b", "c"]);
    assert_eq!(c.cmd(&["LMOVE", "rot", "rot", "LEFT", "RIGHT"]), "$a");
    assert_eq!(c.cmd(&["LRANGE", "rot", "0", "-1"]), "[$b,$c,$a]");

    assert_eq!(c.cmd(&["LPUSHX", "nolist", "a"]), ":0");
    assert_eq!(c.cmd(&["EXISTS", "nolist"]), ":0");
    assert_eq!(c.cmd(&["RPUSHX", "rot", "d", "e"]), ":5");
    assert_eq!(c.cmd(&["LPUSHX", "rot", "z"]), ":6");

    c.cmd(&["RPUSH", "p", "a", "b", "c", "1", "2", "3", "c", "c"]);
    assert_eq!(c.cmd(&["LPOS", "p", "c"]), ":2");
    assert_eq!(c.cmd(&["LPOS", "p", "c", "RANK", "2"]), ":6");
    assert_eq!(c.cmd(&["LPOS", "p", "c", "RANK", "-1"]), ":7");
    assert_eq!(c.cmd(&["LPOS", "p", "c", "COUNT", "2"]), "[:2,:6]");
    assert_eq!(c.cmd(&["LPOS", "p", "c", "COUNT", "0"]), "[:2,:6,:7]");
    assert_eq!(
        c.cmd(&["LPOS", "p", "c", "COUNT", "0", "MAXLEN", "3"]),
        "[:2]"
    );
    assert_eq!(c.cmd(&["LPOS", "p", "x"]), "nil");
    assert_eq!(c.cmd(&["LPOS", "p", "x", "COUNT", "1"]), "[]");
    assert!(c
        .cmd(&["LPOS", "p", "c", "RANK", "0"])
        .starts_with("-ERR RANK can't be zero"));

    // Pops with a count answer arrays, even for one element.
    assert_eq!(c.cmd(&["LPOP", "p", "1"]), "[$a]");
    assert_eq!(c.cmd(&["RPOP", "p", "2"]), "[$c,$c]");
    assert_eq!(c.cmd(&["LPOP", "p", "0"]), "[]");
    assert_eq!(c.cmd(&["LPOP", "nope", "2"]), "nilarr");
    assert!(c
        .cmd(&["LPOP", "p", "-1"])
        .starts_with("-ERR value is out of range"));

    assert_eq!(
        c.cmd(&["LMPOP", "2", "nope", "p", "RIGHT", "COUNT", "2"]),
        "[$p,[$3,$2]]"
    );
    assert_eq!(c.cmd(&["LMPOP", "1", "nope", "LEFT"]), "nilarr");
    assert!(c.cmd(&["LMPOP", "0", "LEFT"]).starts_with("-ERR numkeys"));
    assert!(c
        .cmd(&["LMPOP", "1", "p", "LEFT", "COUNT", "0"])
        .starts_with("-ERR count"));

    c.cmd(&["ZADD", "zm", "1", "a", "2", "b", "3", "c"]);
    assert_eq!(
        c.cmd(&["ZMPOP", "1", "zm", "MAX", "COUNT", "2"]),
        "[$zm,[[$c,$3],[$b,$2]]]"
    );
    assert_eq!(c.cmd(&["ZMPOP", "1", "none", "MIN"]), "nilarr");
}

// ---------------------------------------------------------------------------
// Sorted sets, sets, hashes
// ---------------------------------------------------------------------------

#[test]
fn sorted_set_algebra_across_shards() {
    let s = Server::new("zalg");
    let mut c = s.client();
    let (a, b) = keys_on_different_shards("za");
    c.cmd(&["ZADD", &a, "1", "x", "2", "y", "3", "z"]);
    c.cmd(&["ZADD", &b, "10", "y", "20", "w"]);
    assert_eq!(c.cmd(&["ZUNION", "2", &a, &b]), "[$x,$z,$y,$w]");
    assert_eq!(
        c.cmd(&["ZUNION", "2", &a, &b, "WITHSCORES"]),
        "[$x,$1,$z,$3,$y,$12,$w,$20]"
    );
    assert_eq!(
        c.cmd(&[
            "ZINTER",
            "2",
            &a,
            &b,
            "WEIGHTS",
            "2",
            "1",
            "AGGREGATE",
            "MAX",
            "WITHSCORES"
        ]),
        "[$y,$10]"
    );
    assert_eq!(
        c.cmd(&["ZDIFF", "2", &a, &b, "WITHSCORES"]),
        "[$x,$1,$z,$3]"
    );
    assert_eq!(c.cmd(&["ZDIFFSTORE", "dz", "2", &a, &b]), ":2");
    assert_eq!(c.cmd(&["ZRANGE", "dz", "0", "-1"]), "[$x,$z]");
    assert_eq!(c.cmd(&["ZINTERCARD", "2", &a, &b]), ":1");
    assert_eq!(c.cmd(&["ZINTERCARD", "1", &a, "LIMIT", "2"]), ":2");
    assert!(c.cmd(&["ZINTERCARD", "0", &a]).starts_with("-ERR numkeys"));
    assert!(c
        .cmd(&["ZUNION", "0", &a])
        .starts_with("-ERR at least 1 input key"));
    assert!(c
        .cmd(&["ZDIFF", "1", &a, "WEIGHTS", "1"])
        .starts_with("-ERR syntax"));
    // Plain sets count as scored 1.
    c.cmd(&["SADD", "plain", "x", "q"]);
    assert_eq!(
        c.cmd(&["ZUNION", "2", &a, "plain", "WITHSCORES"]),
        "[$q,$1,$x,$2,$y,$2,$z,$3]"
    );

    assert_eq!(
        c.cmd(&[
            "ZRANGESTORE",
            &b,
            &a,
            "1",
            "+inf",
            "BYSCORE",
            "LIMIT",
            "1",
            "5"
        ]),
        ":2"
    );
    assert_eq!(
        c.cmd(&["ZRANGE", &b, "0", "-1", "WITHSCORES"]),
        "[$y,$2,$z,$3]"
    );
    assert_eq!(c.cmd(&["ZRANGESTORE", "zr", &a, "0", "0", "REV"]), ":1");
    assert_eq!(c.cmd(&["ZRANGE", "zr", "0", "-1"]), "[$z]");
    assert_eq!(c.cmd(&["ZRANGESTORE", "zr", "nothing", "0", "-1"]), ":0");
    assert_eq!(c.cmd(&["EXISTS", "zr"]), ":0");
    assert!(c
        .cmd(&["ZRANGESTORE", "zr", &a, "0", "1", "WITHSCORES"])
        .starts_with("-ERR syntax"));

    assert_eq!(c.cmd(&["ZMSCORE", &a, "x", "nope", "z"]), "[$1,nil,$3]");
    assert_eq!(c.cmd(&["ZMSCORE", "none", "x"]), "[nil]");
    let one = c.cmd(&["ZRANDMEMBER", &a]);
    assert!(["$x", "$y", "$z"].contains(&one.as_str()), "{one}");
    assert_eq!(c.cmd(&["ZRANDMEMBER", &a, "5"]).matches('$').count(), 3);
    assert_eq!(c.cmd(&["ZRANDMEMBER", &a, "-5"]).matches('$').count(), 5);
    assert_eq!(
        c.cmd(&["ZRANDMEMBER", &a, "2", "WITHSCORES"])
            .matches('$')
            .count(),
        4
    );
    assert_eq!(c.cmd(&["ZRANDMEMBER", "none"]), "nil");
    assert_eq!(c.cmd(&["ZRANDMEMBER", "none", "3"]), "[]");
}

#[test]
fn set_and_hash_additions() {
    let s = Server::new("sethash");
    let mut c = s.client();
    let (a, b) = keys_on_different_shards("si");
    c.cmd(&["SADD", &a, "1", "2", "3", "4"]);
    c.cmd(&["SADD", &b, "2", "3", "4", "5"]);
    assert_eq!(c.cmd(&["SMISMEMBER", &a, "1", "9", "3"]), "[:1,:0,:1]");
    assert_eq!(c.cmd(&["SMISMEMBER", "none", "1"]), "[:0]");
    assert_eq!(c.cmd(&["SINTERCARD", "2", &a, &b]), ":3");
    assert_eq!(c.cmd(&["SINTERCARD", "2", &a, &b, "LIMIT", "2"]), ":2");
    assert_eq!(c.cmd(&["SINTERCARD", "2", &a, "none"]), ":0");
    assert!(c
        .cmd(&["SINTERCARD", "2", &a, &b, "LIMIT", "-1"])
        .starts_with("-ERR LIMIT"));
    c.cmd(&["SET", "str", "v"]);
    assert!(c
        .cmd(&["SINTERCARD", "2", &a, "str"])
        .starts_with("-WRONGTYPE"));

    c.cmd(&["HSET", "h", "f", "hello", "g", ""]);
    assert_eq!(c.cmd(&["HSTRLEN", "h", "f"]), ":5");
    assert_eq!(c.cmd(&["HSTRLEN", "h", "nope"]), ":0");
    assert_eq!(c.cmd(&["HSTRLEN", "none", "f"]), ":0");
    let f = c.cmd(&["HRANDFIELD", "h"]);
    assert!(f == "$f" || f == "$g", "{f}");
    assert_eq!(c.cmd(&["HRANDFIELD", "h", "10"]).matches('$').count(), 2);
    assert_eq!(
        c.cmd(&["HRANDFIELD", "h", "-3", "WITHVALUES"])
            .matches('$')
            .count(),
        6
    );
    assert_eq!(c.cmd(&["HRANDFIELD", "none", "2"]), "[]");
    assert!(c
        .cmd(&["HRANDFIELD", "h", "1", "WITHSCORES"])
        .starts_with("-ERR syntax"));
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

#[test]
fn consumer_group_recovery_commands() {
    let s = Server::new("xclaim");
    let mut c = s.client();
    for i in 1..=4 {
        c.cmd(&["XADD", "st", &format!("{i}-0"), "n", &i.to_string()]);
    }
    c.cmd(&["XGROUP", "CREATE", "st", "g", "0"]);
    c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "alice",
        "COUNT",
        "3",
        "STREAMS",
        "st",
        ">",
    ]);
    thread::sleep(Duration::from_millis(30));

    // Too recent for a 10 s min-idle: nothing claimed.
    assert_eq!(c.cmd(&["XCLAIM", "st", "g", "bob", "10000", "1-0"]), "[]");
    assert_eq!(
        c.cmd(&["XCLAIM", "st", "g", "bob", "10", "1-0"]),
        "[[$1-0,[$n,$1]]]"
    );
    // JUSTID: ids only, delivery count untouched.
    assert_eq!(
        c.cmd(&["XCLAIM", "st", "g", "bob", "0", "2-0", "JUSTID"]),
        "[$2-0]"
    );
    let pel = c.cmd(&["XPENDING", "st", "g", "-", "+", "10"]);
    assert!(pel.contains("[$1-0,$bob,"), "{pel}");
    // 2-0 was claimed with JUSTID: still delivered once.
    assert!(pel.contains(",:1],[$3-0,$alice,"), "{pel}");
    // FORCE takes an entry nobody had; RETRYCOUNT and IDLE set its state.
    assert_eq!(
        c.cmd(&[
            "XCLAIM",
            "st",
            "g",
            "bob",
            "0",
            "4-0",
            "FORCE",
            "JUSTID",
            "RETRYCOUNT",
            "7",
            "IDLE",
            "5000"
        ]),
        "[$4-0]"
    );
    let four = c.cmd(&["XPENDING", "st", "g", "4-0", "4-0", "1"]);
    assert!(
        four.starts_with("[[$4-0,$bob,:") && four.ends_with(",:7]]"),
        "{four}"
    );
    let idle: i64 = four.split(",:").nth(1).unwrap().parse().unwrap();
    assert!(idle >= 5000, "{four}");
    assert!(c
        .cmd(&["XCLAIM", "st", "g", "bob", "x", "1-0"])
        .starts_with("-ERR Invalid min-idle-time"));
    assert!(c
        .cmd(&["XCLAIM", "st", "nope", "bob", "0", "1-0"])
        .starts_with("-NOGROUP"));

    // XPENDING filters: IDLE and consumer.
    assert_eq!(
        c.cmd(&["XPENDING", "st", "g", "IDLE", "4000", "-", "+", "10"])
            .matches("$bob")
            .count(),
        1
    );
    assert_eq!(
        c.cmd(&["XPENDING", "st", "g", "-", "+", "10", "alice"])
            .matches("$alice")
            .count(),
        1
    );

    // XAUTOCLAIM: an entry deleted from the stream leaves the PEL.
    c.cmd(&["XDEL", "st", "3-0"]);
    let r = c.cmd(&["XAUTOCLAIM", "st", "g", "carol", "0", "0-0", "COUNT", "10"]);
    assert!(r.starts_with("[$0-0,[[$1-0,"), "{r}");
    assert!(r.ends_with(",[$3-0]]"), "{r}");
    let r = c.cmd(&[
        "XAUTOCLAIM",
        "st",
        "g",
        "carol",
        "0",
        "0-0",
        "COUNT",
        "1",
        "JUSTID",
    ]);
    assert_eq!(r, "[$2-0,[$1-0],[]]");
    assert!(c
        .cmd(&["XAUTOCLAIM", "st", "g", "carol", "0", "0", "COUNT", "0"])
        .starts_with("-ERR COUNT must be > 0"));
}

#[test]
fn stream_metadata_and_introspection() {
    let s = Server::new("xinfo");
    let mut c = s.client();
    assert!(c
        .cmd(&["XADD", "fresh", "0-0", "f", "v"])
        .contains("greater than 0-0"));
    assert_eq!(c.cmd(&["EXISTS", "fresh"]), ":0");

    for i in 1..=5 {
        c.cmd(&["XADD", "st", &format!("{i}-1"), "f", "v"]);
    }
    c.cmd(&["XDEL", "st", "2-1"]);
    let info = c.cmd(&["XINFO", "STREAM", "st"]);
    assert!(info.contains("$entries-added,:5"), "{info}");
    assert!(info.contains("$max-deleted-entry-id,$2-1"), "{info}");
    assert!(info.contains("$last-generated-id,$5-1"), "{info}");

    c.cmd(&["XGROUP", "CREATE", "st", "g1", "$"]);
    c.cmd(&["XGROUP", "CREATE", "st", "g0", "0", "ENTRIESREAD", "0"]);
    assert_eq!(
        c.cmd(&["XGROUP", "CREATECONSUMER", "st", "g0", "zed"]),
        ":1"
    );
    assert_eq!(
        c.cmd(&["XGROUP", "CREATECONSUMER", "st", "g0", "zed"]),
        ":0"
    );
    c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g0",
        "amy",
        "COUNT",
        "2",
        "STREAMS",
        "st",
        ">",
    ]);
    let groups = c.cmd(&["XINFO", "GROUPS", "st"]);
    assert!(
        groups.starts_with("[[$name,$g0,$consumers,:2,$pending,:2,$last-delivered-id,$3-1,"),
        "{groups}"
    );
    assert!(groups.contains("[$name,$g1,$consumers,:0,$pending,:0,$last-delivered-id,$5-1,$entries-read,:5,$lag,:0]"), "{groups}");
    let cons = c.cmd(&["XINFO", "CONSUMERS", "st", "g0"]);
    assert!(
        cons.starts_with("[[$name,$amy,$pending,:2,$idle,:"),
        "{cons}"
    );
    assert!(cons.contains("[$name,$zed,$pending,:0,$idle,:"), "{cons}");
    assert!(cons.ends_with("$inactive,:-1]]"), "{cons}");
    let full = c.cmd(&["XINFO", "STREAM", "st", "FULL", "COUNT", "1"]);
    assert!(full.contains("$entries,[[$1-1,[$f,$v]]]"), "{full}");
    assert!(full.contains("$pel-count,:2"), "{full}");

    assert_eq!(
        c.cmd(&["XGROUP", "SETID", "st", "g0", "0", "ENTRIESREAD", "0"]),
        "+OK"
    );
    assert!(c
        .cmd(&["XINFO", "GROUPS", "st"])
        .contains("$last-delivered-id,$0-0,$entries-read,:0"));

    // XSETID and its checks.
    assert!(c
        .cmd(&["XSETID", "st", "1-0"])
        .contains("smaller than the target stream top item"));
    assert!(c
        .cmd(&["XSETID", "st", "9-0", "ENTRIESADDED", "1"])
        .contains("smaller than the target stream length"));
    assert!(c
        .cmd(&["XSETID", "st", "9-0", "MAXDELETEDID", "10-0"])
        .contains("max_deleted_entry_id"));
    assert_eq!(c.cmd(&["XSETID", "st", "9-0", "ENTRIESADDED", "50"]), "+OK");
    assert!(c.cmd(&["XADD", "st", "8-0", "f", "v"]).starts_with("-ERR"));
    assert!(c
        .cmd(&["XINFO", "STREAM", "st"])
        .contains("$entries-added,:50"));
    assert!(c
        .cmd(&["XSETID", "none", "1-0"])
        .starts_with("-ERR no such key"));

    // Trimming options.
    assert!(c
        .cmd(&["XADD", "st", "MAXLEN", "2", "LIMIT", "10", "*", "f", "v"])
        .contains("LIMIT cannot be used without"));
    assert!(c
        .cmd(&["XADD", "st", "MAXLEN", "~", "2", "LIMIT", "10", "*", "f", "v"])
        .starts_with('$'));
    assert!(c
        .cmd(&["XTRIM", "st", "MAXLEN", "-1"])
        .contains("MAXLEN argument must be >= 0"));
    assert!(c
        .cmd(&["XTRIM", "st", "MAXLEN", "1", "junk"])
        .starts_with("-ERR syntax"));
}

#[test]
fn consumer_group_state_replays_with_its_delivery_times() {
    let dir = TmpDir::new("xreplay");
    let s = Server::start(&dir.0, &["--shards", "8"]);
    {
        let mut c = s.client();
        c.cmd(&["XADD", "st", "1-0", "f", "a"]);
        c.cmd(&["XADD", "st", "2-0", "f", "b"]);
        c.cmd(&["XGROUP", "CREATE", "st", "g", "0"]);
        c.cmd(&["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "st", ">"]);
        c.cmd(&["XCLAIM", "st", "g", "bob", "0", "2-0"]);
    }
    thread::sleep(Duration::from_millis(400));
    s.sigterm();
    let s = Server::start(&dir.0, &["--shards", "8"]);
    let mut c = s.client();
    let pel = c.cmd(&["XPENDING", "st", "g", "-", "+", "10"]);
    // Owners and counts as they were; idle times kept counting.
    assert!(pel.starts_with("[[$1-0,$alice,:"), "{pel}");
    assert!(pel.contains(",:1],[$2-0,$bob,:"), "{pel}");
    assert!(pel.ends_with(",:2]]"), "{pel}");
    let idle: i64 = pel.split(",:").nth(1).unwrap().parse().unwrap();
    assert!(idle >= 400, "idle reset by replay: {pel}");
    // The group's position survived: nothing new to read.
    assert_eq!(
        c.cmd(&["XREADGROUP", "GROUP", "g", "carol", "STREAMS", "st", ">"]),
        "nilarr"
    );
}
