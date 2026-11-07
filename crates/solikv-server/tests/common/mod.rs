//! Real server processes for the multi-process tests (replication, cluster).
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

static PORT_COUNTER: AtomicU16 = AtomicU16::new(0);

/// A free port whose cluster bus port (+10000) is free too.
pub fn next_port() -> u16 {
    let _ = PORT_COUNTER.compare_exchange(
        0,
        20000 + (std::process::id() % 40) as u16 * 100,
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if port > 29_000 {
            PORT_COUNTER.store(20_000, Ordering::SeqCst);
            continue;
        }
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
            && std::net::TcpListener::bind(("127.0.0.1", port + 10000)).is_ok()
        {
            return port;
        }
    }
}

pub struct TmpDir(pub PathBuf);

impl TmpDir {
    pub fn new(tag: &str) -> Self {
        static N: AtomicU16 = AtomicU16::new(0);
        let p = std::env::temp_dir().join(format!(
            "solikv-it6-{}-{}-{}",
            tag,
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub struct Server {
    pub child: Child,
    pub port: u16,
    /// The server's stderr, shown when a test fails (its directory goes
    /// with the test).
    pub log: Option<PathBuf>,
}

impl Server {
    pub fn start(dir: &Path, extra: &[&str]) -> Self {
        Self::start_on(next_port(), dir, extra)
    }

    pub fn start_on(port: u16, dir: &Path, extra: &[&str]) -> Self {
        // The server logs to stdout (tracing's default writer): both
        // streams go to one file, or the log would always be empty.
        let log_path = dir.join(format!("stderr-{port}.log"));
        let log = std::fs::File::create(&log_path).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_solikv"))
            .args(["--port", &port.to_string(), "--rest-port", "0"])
            .args(["--bind", "127.0.0.1", "--log-level", "info"])
            .arg("--dir")
            .arg(dir)
            .args(extra)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn solikv");
        let deadline = Instant::now() + Duration::from_secs(20);
        while !Self::answers(port) {
            assert!(Instant::now() < deadline, "server did not start");
            std::thread::sleep(Duration::from_millis(20));
        }
        Self {
            child,
            port,
            log: Some(log_path),
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

    pub fn client(&self) -> Client {
        Client::connect(self.port)
    }

    /// SIGTERM, and wait for the exit (a graceful shutdown).
    pub fn terminate(&mut self) {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(self.child.id() as i32),
            nix::sys::signal::Signal::SIGTERM,
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        while self.child.try_wait().ok().flatten().is_none() {
            assert!(Instant::now() < deadline, "server did not exit");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // A failing test: what the server logged is often the only account
        // of why (a CI runner's temp directory is gone afterwards).
        if std::thread::panicking() {
            if let Some(text) = self
                .log
                .as_ref()
                .and_then(|p| std::fs::read_to_string(p).ok())
            {
                let lines: Vec<&str> = text.lines().collect();
                eprintln!("--- server :{} log, last 60 lines ---", self.port);
                for l in &lines[lines.len().saturating_sub(60)..] {
                    eprintln!("{l}");
                }
            }
        }
    }
}

pub struct Client {
    r: BufReader<TcpStream>,
    w: TcpStream,
}

impl Client {
    pub fn connect(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
        Self {
            r: BufReader::new(s.try_clone().unwrap()),
            w: s,
        }
    }

    /// Send a command, return its reply as raw RESP text.
    pub fn cmd(&mut self, args: &[&str]) -> String {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        self.w.write_all(out.as_bytes()).unwrap();
        self.read_reply()
    }

    pub fn read_reply(&mut self) -> String {
        let mut line = String::new();
        self.r.read_line(&mut line).unwrap();
        match line.as_bytes().first() {
            Some(b'$') => {
                let n: i64 = line[1..].trim().parse().unwrap();
                if n >= 0 {
                    let mut body = vec![0u8; n as usize + 2];
                    self.r.read_exact(&mut body).unwrap();
                    line.push_str(&String::from_utf8_lossy(&body));
                }
                line
            }
            Some(b'*') | Some(b'%') | Some(b'~') => {
                let n: i64 = line[1..].trim().parse().unwrap();
                let n = if line.starts_with('%') { n * 2 } else { n };
                for _ in 0..n.max(0) {
                    let item = self.read_reply();
                    line.push_str(&item);
                }
                line
            }
            _ => line,
        }
    }

    /// The next reply if one starts arriving within `timeout`.
    pub fn try_reply(&mut self, timeout: Duration) -> Option<String> {
        if !self.r.buffer().is_empty() {
            return Some(self.read_reply());
        }
        self.w.set_read_timeout(Some(timeout)).unwrap();
        let ready = self.r.fill_buf().map(|b| !b.is_empty()).unwrap_or(false);
        self.w
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        ready.then(|| self.read_reply())
    }

    pub fn ok(&mut self, args: &[&str]) {
        let r = self.cmd(args);
        assert_eq!(r, "+OK\r\n", "{:?}", args);
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        let r = self.cmd(&["GET", key]);
        if r.starts_with("$-1") {
            return None;
        }
        assert!(r.starts_with('$'), "GET {key}: {r}");
        Some(
            r.split_once("\r\n")
                .unwrap()
                .1
                .trim_end_matches("\r\n")
                .to_string(),
        )
    }

    pub fn int(&mut self, args: &[&str]) -> i64 {
        let r = self.cmd(args);
        r.strip_prefix(':')
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| panic!("{:?}: {}", args, r))
    }

    /// A bulk reply's text.
    pub fn bulk(&mut self, args: &[&str]) -> String {
        let r = self.cmd(args);
        assert!(r.starts_with('$'), "{:?}: {}", args, r);
        r.split_once("\r\n").unwrap().1.to_string()
    }

    pub fn info(&mut self, section: &str) -> String {
        self.bulk(&["INFO", section])
    }
}

/// Poll `f` until it holds, or panic after `timeout`.
pub fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    loop {
        if f() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "condition not reached in {:?}",
            timeout
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
