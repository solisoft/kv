//! Restarts of the real binary: what survives SIGTERM, SHUTDOWN, a changed
//! shard count, and a snapshot next to the append-only log.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

static PORT_COUNTER: AtomicU16 = AtomicU16::new(19400);

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
            "solikv-it-{}-{}-{}",
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
}

impl Server {
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
        // A connect alone can succeed before the server listens (a TCP
        // self-connect when the port is in the host's ephemeral range): wait
        // for the server itself to answer.
        while !Self::answers(port) {
            assert!(Instant::now() < deadline, "server did not start");
            std::thread::sleep(Duration::from_millis(20));
        }
        Self { child, port }
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

    fn signal(&self, sig: nix::sys::signal::Signal) {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(self.child.id() as i32), sig).unwrap();
    }

    fn wait_exit(mut self) -> std::process::ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "server did not exit");
            std::thread::sleep(Duration::from_millis(20));
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
    /// Send a command; return the reply as text (`:5`, `$v`, `+OK`, `-ERR`,
    /// arrays as `*` + comma-joined items, nil as `nil`).
    fn cmd(&mut self, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        self.w.write_all(out.as_bytes()).unwrap();
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
                let items: Vec<String> = (0..n.max(0)).map(|_| self.reply()).collect();
                format!("*{}", items.join(","))
            }
            _ => line,
        }
    }
}

/// The restart bug: the periodic snapshot plus the whole log replayed on top
/// applied INCR and RPUSH twice. Also: SIGTERM saves, and a different shard
/// count on restart still finds every key.
#[test]
fn restarts_keep_exact_values_across_sigterm_and_resharding() {
    let dir = TmpDir::new("restart");
    let s = Server::start(&dir.0, &["--shards", "4"]);
    let mut c = s.client();
    for _ in 0..3 {
        c.cmd(&["INCR", "counter"]);
    }
    c.cmd(&["RPUSH", "list", "a", "b"]);
    assert_eq!(c.cmd(&["SAVE"]), "+OK");
    c.cmd(&["INCR", "counter"]);
    for i in 0..100 {
        c.cmd(&["SET", &format!("k{i}"), &i.to_string()]);
    }
    s.signal(nix::sys::signal::Signal::SIGTERM);
    assert!(s.wait_exit().success());

    let s = Server::start(&dir.0, &["--shards", "3"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["GET", "counter"]), "$4");
    assert_eq!(c.cmd(&["LRANGE", "list", "0", "-1"]), "*$a,$b");
    for i in 0..100 {
        assert_eq!(c.cmd(&["GET", &format!("k{i}")]), format!("${i}"));
    }
    assert_eq!(c.cmd(&["DBSIZE"]), ":102");
}

/// Only Ctrl-C used to save; `docker stop` and systemd send SIGTERM.
#[test]
fn sigterm_takes_the_final_snapshot_without_a_log() {
    let dir = TmpDir::new("sigterm");
    let s = Server::start(&dir.0, &["--appendonly", "false"]);
    let mut c = s.client();
    c.cmd(&["SET", "a", "1"]);
    s.signal(nix::sys::signal::Signal::SIGTERM);
    assert!(s.wait_exit().success());
    assert!(dir.0.join("dump.rdb").exists());

    let s = Server::start(&dir.0, &["--appendonly", "false"]);
    assert_eq!(s.client().cmd(&["GET", "a"]), "$1");
}

#[test]
fn sighup_is_ignored() {
    let dir = TmpDir::new("sighup");
    let s = Server::start(&dir.0, &[]);
    s.signal(nix::sys::signal::Signal::SIGHUP);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(s.client().cmd(&["PING"]), "+PONG");
}

#[test]
fn shutdown_nosave_exits_and_the_log_still_has_everything() {
    let dir = TmpDir::new("shutdown");
    let s = Server::start(&dir.0, &[]);
    let mut c = s.client();
    c.cmd(&["SET", "a", "1"]);
    c.cmd(&["INCR", "a"]);
    assert_eq!(c.cmd(&["SHUTDOWN", "NOSAVE"]), "+OK");
    assert!(s.wait_exit().success());
    assert!(!dir.0.join("dump.rdb").exists(), "NOSAVE took a snapshot");

    let s = Server::start(&dir.0, &[]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["GET", "a"]), "$2");
    assert_eq!(
        c.cmd(&["BGREWRITEAOF"]),
        "+Background append only file rewriting started"
    );
    let info = c.cmd(&["INFO"]);
    assert!(info.contains("aof_enabled:1"), "{info}");
}

const LIB_A: &str = "#!lua name=liba\nredis.register_function('fa', function(keys, args) return 'a:' .. args[1] end)";
const LIB_B: &str = "#!lua name=libb\nredis.register_function('fb', function(keys, args) return 'b:' .. args[1] end)";

impl Client {
    /// A bulk reply's raw bytes (`FUNCTION DUMP` is binary).
    fn bulk_raw(&mut self, args: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        self.w.write_all(out.as_bytes()).unwrap();
        let mut line = String::new();
        self.r.read_line(&mut line).unwrap();
        let n: usize = line.trim_end()[1..].parse().expect("a bulk reply");
        let mut buf = vec![0u8; n + 2];
        self.r.read_exact(&mut buf).unwrap();
        buf.truncate(n);
        buf
    }

    fn wait_rewrite_done(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let info = self.cmd(&["INFO", "persistence"]);
            if info.contains("aof_rewrite_in_progress:0") {
                return;
            }
            assert!(Instant::now() < deadline, "rewrite did not finish: {info}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// SIGKILL, after giving the log writer (which collects records every
/// millisecond) time to hand the last ones to the kernel.
fn kill_hard(s: Server) {
    std::thread::sleep(Duration::from_millis(200));
    s.signal(nix::sys::signal::Signal::SIGKILL);
    let _ = s.wait_exit();
}

/// Libraries live in the log (`FUNCTION` records in shard 0's) and in its
/// rewritten base — not in a side file — and come back after a crash.
#[test]
fn functions_survive_restarts_through_the_log_and_its_rewrite() {
    let dir = TmpDir::new("fn-aof");
    let s = Server::start(&dir.0, &["--shards", "4"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["FUNCTION", "LOAD", LIB_A]), "$liba");
    kill_hard(s);
    assert!(!dir.0.join("functions.lib").exists());

    let s = Server::start(&dir.0, &["--shards", "4"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["FCALL", "fa", "0", "x"]), "$a:x");
    // Into the base, then a change after it: both must come back.
    assert_eq!(c.cmd(&["FUNCTION", "LOAD", LIB_B]), "$libb");
    assert_eq!(
        c.cmd(&["BGREWRITEAOF"]),
        "+Background append only file rewriting started"
    );
    c.wait_rewrite_done();
    assert_eq!(c.cmd(&["FUNCTION", "DELETE", "liba"]), "+OK");
    kill_hard(s);

    // A different shard count: the libraries are not any shard's data.
    let s = Server::start(&dir.0, &["--shards", "3"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["FCALL", "fb", "0", "y"]), "$b:y");
    assert!(c.cmd(&["FCALL", "fa", "0", "y"]).starts_with("-ERR"));
    // FLUSH is a change too: nothing comes back after it.
    assert_eq!(c.cmd(&["FUNCTION", "FLUSH"]), "+OK");
    kill_hard(s);
    let s = Server::start(&dir.0, &["--shards", "3"]);
    assert_eq!(s.client().cmd(&["FUNCTION", "LIST"]), "*");
}

/// Without the log, `dump.rdb` carries the libraries.
#[test]
fn functions_survive_in_the_snapshot_alone() {
    let dir = TmpDir::new("fn-rdb");
    let s = Server::start(&dir.0, &["--appendonly", "false"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["FUNCTION", "LOAD", LIB_A]), "$liba");
    assert_eq!(c.cmd(&["SET", "k", "v"]), "+OK");
    assert_eq!(c.cmd(&["SAVE"]), "+OK");
    kill_hard(s);
    assert!(!dir.0.join("functions.lib").exists());

    let s = Server::start(&dir.0, &["--appendonly", "false"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["FCALL", "fa", "0", "z"]), "$a:z");
    assert_eq!(c.cmd(&["GET", "k"]), "$v");
}

/// `functions.lib`, where an earlier version kept the libraries: loaded once,
/// written into the log, then removed — the log alone restores them after.
#[test]
fn functions_lib_from_an_earlier_version_migrates_into_the_log() {
    let src = TmpDir::new("fn-src");
    let s = Server::start(&src.0, &["--appendonly", "false"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["FUNCTION", "LOAD", LIB_A]), "$liba");
    let dump = c.bulk_raw(&["FUNCTION", "DUMP"]);
    drop(s);

    let dir = TmpDir::new("fn-legacy");
    std::fs::create_dir_all(&dir.0).unwrap();
    std::fs::write(dir.0.join("functions.lib"), &dump).unwrap();
    let s = Server::start(&dir.0, &[]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["FCALL", "fa", "0", "m"]), "$a:m");
    assert!(
        !dir.0.join("functions.lib").exists(),
        "functions.lib kept after its libraries went into the log"
    );
    kill_hard(s);

    let s = Server::start(&dir.0, &[]);
    assert_eq!(s.client().cmd(&["FCALL", "fa", "0", "n"]), "$a:n");
}

/// `appendfsync everysec` may lose the last second to a machine crash, not
/// to the process dying: records reach the kernel within a writer pass
/// (they used to wait in a user-space buffer for the once-a-second flush).
#[test]
fn acknowledged_writes_survive_a_killed_process() {
    let dir = TmpDir::new("kill9");
    let s = Server::start(&dir.0, &["--shards", "4"]);
    let mut c = s.client();
    for i in 0..50 {
        assert_eq!(c.cmd(&["SET", &format!("k{i}"), "v"]), "+OK");
    }
    assert_eq!(c.cmd(&["INCRBY", "n", "7"]), ":7");
    kill_hard(s);

    let s = Server::start(&dir.0, &["--shards", "4"]);
    let mut c = s.client();
    assert_eq!(c.cmd(&["DBSIZE"]), ":51");
    assert_eq!(c.cmd(&["GET", "n"]), "$7");
}

/// `appendfsync always`: a reply leaves only after its record is written
/// and fsynced (group commit), so a process killed the instant a reply
/// arrives — no grace period for the writer — has every acknowledged write
/// in its log. Several rounds on one directory: each restart replays what
/// the rounds before it acknowledged.
#[test]
fn appendfsync_always_holds_replies_until_the_write_is_durable() {
    let dir = TmpDir::new("always");
    let args = ["--shards", "4", "--appendfsync", "always"];
    let mut expected = 0i64;
    for round in 0..5 {
        let s = Server::start(&dir.0, &args);
        let mut c = s.client();
        assert_eq!(
            c.cmd(&["GET", "n"]),
            if expected == 0 {
                "nil".to_string()
            } else {
                format!("${expected}")
            }
        );
        // Pipelined: every reply in one batch shares one fsync.
        let mut batch = String::new();
        for i in 0..20 {
            for cmd in [
                vec!["INCR".to_string(), "n".into()],
                vec!["SET".into(), format!("r{round}:{i}"), "v".into()],
            ] {
                batch.push_str(&format!("*{}\r\n", cmd.len()));
                for a in &cmd {
                    batch.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
                }
            }
        }
        c.w.write_all(batch.as_bytes()).unwrap();
        for i in 0..20 {
            expected += 1;
            assert_eq!(c.reply(), format!(":{expected}"), "round {round}, INCR {i}");
            assert_eq!(c.reply(), "+OK");
        }
        // The last reply is in: kill now, without the writer's grace.
        s.signal(nix::sys::signal::Signal::SIGKILL);
        let _ = s.wait_exit();
    }
    let s = Server::start(&dir.0, &args);
    let mut c = s.client();
    assert_eq!(c.cmd(&["GET", "n"]), format!("${expected}"));
    assert_eq!(c.cmd(&["DBSIZE"]), ":101");
}

/// `CONFIG SET appendonly yes` starts a log from the dataset as it is (a
/// rewrite: what was written before is in its base, what follows in its
/// log); `no` stops it, keeping what it had, as Redis does.
#[test]
fn config_set_appendonly_turns_the_log_on_and_off() {
    let dir = TmpDir::new("aof-toggle");
    let off = ["--appendonly", "false", "--save", ""];
    let on = ["--appendonly", "true", "--save", ""];
    let s = Server::start(&dir.0, &off);
    let mut c = s.client();
    c.cmd(&["SET", "a", "1"]);
    assert_eq!(c.cmd(&["CONFIG", "SET", "appendonly", "yes"]), "+OK");
    c.wait_rewrite_done();
    assert!(c.cmd(&["INFO", "persistence"]).contains("aof_enabled:1"));
    assert_eq!(c.cmd(&["CONFIG", "GET", "appendonly"]), "*$appendonly,$yes");
    c.cmd(&["SET", "b", "2"]);
    for _ in 0..3 {
        c.cmd(&["INCR", "n"]);
    }
    kill_hard(s);

    let s = Server::start(&dir.0, &on);
    let mut c = s.client();
    assert_eq!(c.cmd(&["GET", "a"]), "$1", "written before: in the base");
    assert_eq!(c.cmd(&["GET", "b"]), "$2", "written after: in the log");
    assert_eq!(c.cmd(&["GET", "n"]), "$3");
    // Off: nothing more is logged; what was stays.
    assert_eq!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]), "+OK");
    assert!(c.cmd(&["INFO", "persistence"]).contains("aof_enabled:0"));
    c.cmd(&["SET", "c", "3"]);
    kill_hard(s);

    let s = Server::start(&dir.0, &on);
    let mut c = s.client();
    assert_eq!(c.cmd(&["GET", "c"]), "nil", "written with the log off");
    assert_eq!(c.cmd(&["GET", "b"]), "$2");
    // Off, a write, on again: the new log starts from the dataset as it is.
    assert_eq!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]), "+OK");
    c.cmd(&["SET", "e", "5"]);
    assert_eq!(c.cmd(&["CONFIG", "SET", "appendonly", "yes"]), "+OK");
    c.wait_rewrite_done();
    c.cmd(&["SET", "d", "4"]);
    kill_hard(s);

    let s = Server::start(&dir.0, &on);
    let mut c = s.client();
    for (k, v) in [
        ("a", "$1"),
        ("b", "$2"),
        ("n", "$3"),
        ("e", "$5"),
        ("d", "$4"),
    ] {
        assert_eq!(c.cmd(&["GET", k]), v, "{k}");
    }
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "appendonly", "maybe"]).get(..4),
        Some("-ERR")
    );
}
