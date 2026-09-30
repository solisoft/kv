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
