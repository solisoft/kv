//! TLS on every channel, between real server processes: RESP and REST with
//! and without client certificates, the replication link, the cluster bus.

mod common;

use common::{next_port, Server, TmpDir};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ---------------------------------------------------------------------------
// Certificates
// ---------------------------------------------------------------------------

/// A CA, and one certificate it signed (for 127.0.0.1 and localhost, usable
/// as server and client certificate).
struct Pki {
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
    ca_der: Vec<u8>,
    cert_pem: String,
    key_pem: String,
}

fn pki(dir: &Path, tag: &str) -> Pki {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        SanType,
    };
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, format!("solikv test CA {tag}"));
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params
        .subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    params
        .distinguished_name
        .push(DnType::CommonName, format!("solikv node {tag}"));
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &ca_cert, &ca_key).unwrap();

    let write = |name: &str, body: &str| {
        let p = dir.join(format!("{tag}-{name}"));
        std::fs::write(&p, body).unwrap();
        p
    };
    Pki {
        ca: write("ca.pem", &ca_cert.pem()),
        cert: write("cert.pem", &cert.pem()),
        key: write("key.pem", &key.serialize_pem()),
        ca_der: ca_cert.der().to_vec(),
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
    }
}

impl Pki {
    /// The server flags for this PKI.
    fn flags(&self) -> Vec<String> {
        vec![
            "--tls-cert".into(),
            self.cert.display().to_string(),
            "--tls-key".into(),
            self.key.display().to_string(),
            "--tls-ca-cert".into(),
            self.ca.display().to_string(),
        ]
    }

    /// A client trusting this CA, presenting this PKI's certificate or none.
    fn client(&self, with_cert: bool) -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.ca_der.clone().into()).unwrap();
        let b = rustls::ClientConfig::builder().with_root_certificates(roots);
        Arc::new(if with_cert {
            let certs = pem::parse_many(self.cert_pem.as_bytes())
                .unwrap()
                .into_iter()
                .map(|p| p.contents().to_vec().into())
                .collect();
            let key = pem::parse(self.key_pem.as_bytes()).unwrap();
            let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key.contents().to_vec().into());
            b.with_client_auth_cert(certs, key).unwrap()
        } else {
            b.with_no_client_auth()
        })
    }
}

// ---------------------------------------------------------------------------
// A TLS RESP client
// ---------------------------------------------------------------------------

type Tls = tokio_rustls::client::TlsStream<tokio::net::TcpStream>;

async fn tls_connect(port: u16, cfg: Arc<rustls::ClientConfig>) -> std::io::Result<Tls> {
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    let name = rustls::pki_types::ServerName::IpAddress(
        "127.0.0.1".parse::<std::net::IpAddr>().unwrap().into(),
    );
    tokio_rustls::TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
}

/// One complete RESP reply at the start of `buf`, as text, and its length.
fn parse_reply(buf: &[u8]) -> Option<(String, usize)> {
    let nl = buf.windows(2).position(|w| w == b"\r\n")?;
    let line = String::from_utf8_lossy(&buf[..nl]).to_string();
    let head = nl + 2;
    match buf.first()? {
        b'$' => {
            let n: i64 = line[1..].parse().ok()?;
            if n < 0 {
                return Some((line + "\r\n", head));
            }
            let end = head + n as usize + 2;
            (buf.len() >= end).then(|| {
                (
                    format!("{line}\r\n{}", String::from_utf8_lossy(&buf[head..end])),
                    end,
                )
            })
        }
        b'*' => {
            let n: i64 = line[1..].parse().ok()?;
            let mut out = line + "\r\n";
            let mut at = head;
            for _ in 0..n.max(0) {
                let (item, used) = parse_reply(&buf[at..])?;
                out.push_str(&item);
                at += used;
            }
            Some((out, at))
        }
        _ => Some((line + "\r\n", head)),
    }
}

async fn cmd(s: &mut Tls, args: &[&str]) -> std::io::Result<String> {
    let mut out = format!("*{}\r\n", args.len());
    for a in args {
        out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    s.write_all(out.as_bytes()).await?;
    let mut buf = Vec::new();
    loop {
        if let Some((reply, _)) = parse_reply(&buf) {
            return Ok(reply);
        }
        let mut chunk = [0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(10), s.read(&mut chunk))
            .await
            .map_err(|_| std::io::Error::other("no reply"))??;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "closed",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// One command on a fresh TLS connection.
async fn one(port: u16, cfg: Arc<rustls::ClientConfig>, args: &[&str]) -> std::io::Result<String> {
    let mut s = tls_connect(port, cfg).await?;
    cmd(&mut s, args).await
}

/// A server process whose RESP port speaks TLS; ready once a TLS `PING`
/// (with `client`) answers.
fn start_tls(dir: &Path, port: u16, args: &[String], client: Arc<rustls::ClientConfig>) -> Server {
    let log = std::fs::File::create(dir.join(format!("stderr-{port}.log"))).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_solikv"))
        .args(["--port", &port.to_string(), "--rest-port", "0"])
        .args(["--bind", "127.0.0.1", "--log-level", "info"])
        .arg("--dir")
        .arg(dir)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn solikv");
    let srv = Server {
        child,
        port,
        log: None,
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let ok = rt.block_on(one(port, client.clone(), &["PING"]));
        if matches!(ok.as_deref(), Ok("+PONG\r\n")) {
            return srv;
        }
        assert!(
            Instant::now() < deadline,
            "TLS server did not start: {:?}\n{}",
            ok,
            std::fs::read_to_string(dir.join(format!("stderr-{port}.log"))).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn strs(v: &[String]) -> Vec<String> {
    v.to_vec()
}

async fn wait_for(timeout: Duration, mut f: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f().await {
        assert!(
            Instant::now() < deadline,
            "condition not reached in {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Fails the test instead of hanging it.
fn watchdog(secs: u64) {
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(secs));
        eprintln!("watchdog: test exceeded {secs} s");
        std::process::exit(101);
    });
}

// ---------------------------------------------------------------------------
// RESP
// ---------------------------------------------------------------------------

#[test]
fn resp_over_tls_without_client_certificates() {
    watchdog(120);
    let dir = TmpDir::new("tls-plain");
    let p = pki(&dir.0, "a");
    let port = next_port();
    // A certificate, no CA: encryption, no client certificates.
    let args = strs(&[
        "--tls-cert".into(),
        p.cert.display().to_string(),
        "--tls-key".into(),
        p.key.display().to_string(),
    ]);
    let _srv = start_tls(&dir.0, port, &args, p.client(false));
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut s = tls_connect(port, p.client(false)).await.unwrap();
        assert_eq!(cmd(&mut s, &["SET", "k", "v"]).await.unwrap(), "+OK\r\n");
        assert_eq!(cmd(&mut s, &["GET", "k"]).await.unwrap(), "$1\r\nv\r\n");
        // Cleartext RESP on the TLS port gets nothing back.
        let mut plain = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        plain.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut buf = [0u8; 64];
        let r = tokio::time::timeout(Duration::from_secs(3), plain.read(&mut buf)).await;
        assert!(
            !matches!(r, Ok(Ok(n)) if n > 0 && buf[..n].starts_with(b"+PONG")),
            "cleartext PING answered on the TLS port"
        );
    });
}

#[test]
fn client_certificates_are_required_once_a_ca_is_set() {
    watchdog(120);
    let dir = TmpDir::new("tls-mtls");
    let p = pki(&dir.0, "a");
    let other = pki(&dir.0, "other");
    let port = next_port();
    let _srv = start_tls(&dir.0, port, &p.flags(), p.client(true));
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // With a certificate the CA signed.
        assert_eq!(
            one(port, p.client(true), &["PING"]).await.unwrap(),
            "+PONG\r\n"
        );
        // Without one: the handshake (or, in TLS 1.3, the first read) fails.
        let r = one(port, p.client(false), &["PING"]).await;
        assert!(r.is_err(), "no client certificate, yet: {r:?}");
        // With one from another CA (trusting the server's CA, so only the
        // client side is wrong).
        let mut roots = rustls::RootCertStore::empty();
        roots.add(p.ca_der.clone().into()).unwrap();
        let certs = pem::parse_many(other.cert_pem.as_bytes())
            .unwrap()
            .into_iter()
            .map(|c| c.contents().to_vec().into())
            .collect();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            pem::parse(other.key_pem.as_bytes())
                .unwrap()
                .contents()
                .to_vec()
                .into(),
        );
        let foreign = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(certs, key)
                .unwrap(),
        );
        let r = one(port, foreign, &["PING"]).await;
        assert!(r.is_err(), "a foreign CA's certificate was accepted: {r:?}");
    });
}

#[test]
fn optional_client_certificates() {
    watchdog(120);
    let dir = TmpDir::new("tls-optional");
    let p = pki(&dir.0, "a");
    let port = next_port();
    let mut args = p.flags();
    args.extend(["--tls-auth-clients".to_string(), "optional".to_string()]);
    let _srv = start_tls(&dir.0, port, &args, p.client(false));
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        assert_eq!(
            one(port, p.client(false), &["PING"]).await.unwrap(),
            "+PONG\r\n"
        );
        assert_eq!(
            one(port, p.client(true), &["PING"]).await.unwrap(),
            "+PONG\r\n"
        );
    });
}

#[test]
fn tls_options_without_a_certificate_refuse_to_start() {
    watchdog(60);
    let dir = TmpDir::new("tls-refuse");
    let p = pki(&dir.0, "a");
    for args in [
        vec!["--tls-replication", "yes"],
        vec!["--tls-ca-cert", p.ca.to_str().unwrap()],
        // TLS replication needs a CA to check the master against.
        vec![
            "--tls-cert",
            p.cert.to_str().unwrap(),
            "--tls-key",
            p.key.to_str().unwrap(),
            "--tls-replication",
            "yes",
        ],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_solikv"))
            .args(["--port", &next_port().to_string(), "--rest-port", "0"])
            .args(["--bind", "127.0.0.1", "--dir"])
            .arg(&dir.0)
            .args(&args)
            .output()
            .unwrap();
        assert!(!out.status.success(), "{args:?} started");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("TLS"),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

// ---------------------------------------------------------------------------
// REST
// ---------------------------------------------------------------------------

#[test]
fn rest_requires_the_client_certificate_too() {
    watchdog(120);
    let dir = TmpDir::new("tls-rest");
    let p = pki(&dir.0, "a");
    let port = next_port();
    let rest = next_port();
    let mut args = p.flags();
    args.extend(["--rest-port".to_string(), rest.to_string()]);
    let _srv = start_tls(&dir.0, port, &args, p.client(true));
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let get = |cfg: Arc<rustls::ClientConfig>| async move {
            let mut s = tls_connect(rest, cfg).await?;
            s.write_all(
                b"GET /kv/missing HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await?;
            let mut out = Vec::new();
            tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut out))
                .await
                .map_err(|_| std::io::Error::other("timeout"))??;
            Ok::<String, std::io::Error>(String::from_utf8_lossy(&out).to_string())
        };
        let ok = get(p.client(true)).await.unwrap();
        assert!(ok.starts_with("HTTP/1.1 404"), "{ok}");
        let refused = get(p.client(false)).await;
        assert!(
            !matches!(&refused, Ok(r) if r.starts_with("HTTP/1.1")),
            "REST answered a client without a certificate: {refused:?}"
        );
    });
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

#[test]
fn a_replica_follows_its_master_over_tls() {
    watchdog(180);
    let dir = TmpDir::new("tls-repl");
    let p = pki(&dir.0, "a");
    let mdir = TmpDir::new("tls-repl-m");
    let rdir = TmpDir::new("tls-repl-r");
    let mport = next_port();
    let rport = next_port();
    let mut margs = p.flags();
    margs.extend(["--requirepass".to_string(), "masterpw".to_string()]);
    let _master = start_tls(&mdir.0, mport, &margs, p.client(true));
    let mut rargs = p.flags();
    rargs.extend([
        "--tls-replication".to_string(),
        "yes".to_string(),
        "--masterauth".to_string(),
        "masterpw".to_string(),
        "--replicaof".to_string(),
        format!("127.0.0.1 {mport}"),
    ]);
    let _replica = start_tls(&rdir.0, rport, &rargs, p.client(true));
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut m = tls_connect(mport, p.client(true)).await.unwrap();
        assert_eq!(cmd(&mut m, &["AUTH", "masterpw"]).await.unwrap(), "+OK\r\n");
        assert_eq!(
            cmd(&mut m, &["SET", "over", "tls"]).await.unwrap(),
            "+OK\r\n"
        );
        let c = p.client(true);
        wait_for(Duration::from_secs(30), async || {
            matches!(
                one(rport, c.clone(), &["GET", "over"]).await.as_deref(),
                Ok("$3\r\ntls\r\n")
            )
        })
        .await;
        let info = one(rport, p.client(true), &["INFO", "replication"])
            .await
            .unwrap();
        assert!(info.contains("master_link_status:up"), "{info}");
        // The stream follows, not only the snapshot.
        assert_eq!(cmd(&mut m, &["INCR", "n"]).await.unwrap(), ":1\r\n");
        wait_for(Duration::from_secs(30), async || {
            matches!(
                one(rport, c.clone(), &["GET", "n"]).await.as_deref(),
                Ok("$1\r\n1\r\n")
            )
        })
        .await;
    });
}

#[test]
fn a_replica_without_tls_replication_cannot_follow_a_tls_master() {
    watchdog(120);
    let dir = TmpDir::new("tls-repl-no");
    let p = pki(&dir.0, "a");
    let mdir = TmpDir::new("tls-repl-no-m");
    let rdir = TmpDir::new("tls-repl-no-r");
    let mport = next_port();
    let _master = start_tls(&mdir.0, mport, &p.flags(), p.client(true));
    let mut m = Server::start(&rdir.0, &["--replicaof", &format!("127.0.0.1 {mport}")]);
    std::thread::sleep(Duration::from_secs(3));
    let info = m.client().info("replication");
    assert!(info.contains("master_link_status:down"), "{info}");
    m.kill();
}

// ---------------------------------------------------------------------------
// The cluster bus
// ---------------------------------------------------------------------------

const BUS_SECRET: &str = "a-cluster-bus-secret-of-at-least-32-bytes!";

fn cluster_args(p: &Pki) -> Vec<String> {
    let mut a = p.flags();
    a.extend(
        [
            "--cluster-enabled",
            "--cluster-bus-secret",
            BUS_SECRET,
            "--tls-cluster",
            "yes",
            "--cluster-node-timeout",
            "1500",
            "--shards",
            "2",
        ]
        .map(String::from),
    );
    a
}

async fn known_nodes(port: u16, cfg: Arc<rustls::ClientConfig>) -> usize {
    let info = one(port, cfg, &["CLUSTER", "INFO"])
        .await
        .unwrap_or_default();
    info.lines()
        .find_map(|l| l.strip_prefix("cluster_known_nodes:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

#[test]
fn the_cluster_bus_forms_over_tls_and_refuses_a_foreign_ca() {
    watchdog(180);
    let dir = TmpDir::new("tls-bus");
    let p = pki(&dir.0, "a");
    let foreign = pki(&dir.0, "foreign");
    let dirs: Vec<TmpDir> = (0..3)
        .map(|i| TmpDir::new(&format!("tls-bus{i}")))
        .collect();
    let ports: Vec<u16> = (0..3).map(|_| next_port()).collect();
    let _a = start_tls(&dirs[0].0, ports[0], &cluster_args(&p), p.client(true));
    let _b = start_tls(&dirs[1].0, ports[1], &cluster_args(&p), p.client(true));
    // Same bus secret, certificate from another CA.
    let _c = start_tls(
        &dirs[2].0,
        ports[2],
        &cluster_args(&foreign),
        foreign.client(true),
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let c = p.client(true);
        assert_eq!(
            one(
                ports[0],
                c.clone(),
                &["CLUSTER", "MEET", "127.0.0.1", &ports[1].to_string()]
            )
            .await
            .unwrap(),
            "+OK\r\n"
        );
        wait_for(Duration::from_secs(30), async || {
            known_nodes(ports[0], c.clone()).await == 2
                && known_nodes(ports[1], c.clone()).await == 2
        })
        .await;
        let nodes = one(ports[1], c.clone(), &["CLUSTER", "NODES"])
            .await
            .unwrap();
        assert!(
            nodes.contains(&format!("127.0.0.1:{}", ports[0])),
            "{nodes}"
        );

        // The foreign node is told to meet ours: neither side's TLS accepts
        // the other, so it never learns of them.
        let _ = one(
            ports[2],
            foreign.client(true),
            &["CLUSTER", "MEET", "127.0.0.1", &ports[0].to_string()],
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(known_nodes(ports[2], foreign.client(true)).await, 1);
        let nodes = one(ports[0], c.clone(), &["CLUSTER", "NODES"])
            .await
            .unwrap();
        assert!(
            !nodes
                .lines()
                .any(|l| l.contains(&format!(":{}@", ports[2])) && !l.contains("handshake")),
            "the foreign node joined: {nodes}"
        );
    });
}

// ---------------------------------------------------------------------------
// Cluster dump / restore
// ---------------------------------------------------------------------------

/// `--cluster-dump`/`--cluster-restore` run as a one-shot process.
fn dump_tool(args: &[String]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_solikv"))
        .args(args)
        .output()
        .expect("run solikv dump tool")
}

#[test]
fn cluster_dump_and_restore_over_tls() {
    watchdog(180);
    let dir = TmpDir::new("tls-dump");
    let p = pki(&dir.0, "a");
    let foreign = pki(&dir.0, "foreign");
    let node_dir = TmpDir::new("tls-dump-node");
    let port = next_port();
    let _n = start_tls(&node_dir.0, port, &cluster_args(&p), p.client(true));
    let c = p.client(true);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        assert_eq!(
            one(port, c.clone(), &["CLUSTER", "ADDSLOTSRANGE", "0", "16383"])
                .await
                .unwrap(),
            "+OK\r\n"
        );
        wait_for(Duration::from_secs(20), async || {
            one(port, c.clone(), &["SET", "dump:s", "hello"])
                .await
                .map(|r| r == "+OK\r\n")
                .unwrap_or(false)
        })
        .await;
        one(port, c.clone(), &["RPUSH", "dump:l", "a", "b"])
            .await
            .unwrap();
        one(port, c.clone(), &["HSET", "dump:h", "f", "v"])
            .await
            .unwrap();
        one(port, c.clone(), &["XADD", "dump:x", "*", "f", "v"])
            .await
            .unwrap();
        one(
            port,
            c.clone(),
            &["SET", "dump:t", "a\r\nb", "PX", "600000"],
        )
        .await
        .unwrap();
    });

    let connect = format!("127.0.0.1:{port}");
    let out_file = dir.0.join("cluster.jsonl");
    let base = |pki: &Pki, file: &Path, verb: &str| {
        let mut a = vec![
            verb.to_string(),
            file.display().to_string(),
            "--cluster-connect".into(),
            connect.clone(),
        ];
        a.extend(pki.flags());
        a
    };

    // In clear against a TLS port: refused (the node never answers RESP).
    let clear = dump_tool(&[
        "--cluster-dump".into(),
        dir.0.join("clear.jsonl").display().to_string(),
        "--cluster-connect".into(),
        connect.clone(),
    ]);
    assert!(
        !clear.status.success(),
        "a clear dump of a TLS node succeeded"
    );

    // A certificate from another CA: the handshake fails both ways.
    let wrong = dump_tool(&base(
        &foreign,
        &dir.0.join("wrong.jsonl"),
        "--cluster-dump",
    ));
    assert!(!wrong.status.success(), "a foreign-CA dump succeeded");

    // The node's own TLS material: the dump works.
    let ok = dump_tool(&base(&p, &out_file, "--cluster-dump"));
    assert!(
        ok.status.success(),
        "dump failed: {}{}",
        String::from_utf8_lossy(&ok.stdout),
        String::from_utf8_lossy(&ok.stderr)
    );
    let dumped = std::fs::read_to_string(&out_file).unwrap();
    for k in ["dump:s", "dump:l", "dump:h", "dump:x", "dump:t"] {
        assert!(dumped.contains(k), "{k} missing from the dump: {dumped}");
    }

    // Restore it into the emptied node, over TLS.
    rt.block_on(async {
        one(port, c.clone(), &["FLUSHALL"]).await.unwrap();
    });
    let restored = dump_tool(&base(&p, &out_file, "--cluster-restore"));
    assert!(
        restored.status.success(),
        "restore failed: {}{}",
        String::from_utf8_lossy(&restored.stdout),
        String::from_utf8_lossy(&restored.stderr)
    );
    rt.block_on(async {
        assert_eq!(
            one(port, c.clone(), &["GET", "dump:s"]).await.unwrap(),
            "$5\r\nhello\r\n"
        );
        assert_eq!(
            one(port, c.clone(), &["LRANGE", "dump:l", "0", "-1"])
                .await
                .unwrap(),
            "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
        );
        assert_eq!(
            one(port, c.clone(), &["HGET", "dump:h", "f"])
                .await
                .unwrap(),
            "$1\r\nv\r\n"
        );
        assert_eq!(
            one(port, c.clone(), &["XLEN", "dump:x"]).await.unwrap(),
            ":1\r\n"
        );
        // A value holding CRLF comes back whole, with its TTL.
        assert_eq!(
            one(port, c.clone(), &["GET", "dump:t"]).await.unwrap(),
            "$4\r\na\r\nb\r\n"
        );
        let pttl = one(port, c.clone(), &["PTTL", "dump:t"]).await.unwrap();
        let ms: i64 = pttl.trim().trim_start_matches(':').parse().unwrap();
        assert!(ms > 500_000 && ms <= 600_000, "TTL lost: {pttl}");
    });
}

// ---------------------------------------------------------------------------
// --tls-auth-clients-user CN
// ---------------------------------------------------------------------------

#[test]
fn a_client_certificate_names_its_acl_user() {
    watchdog(120);
    let dir = TmpDir::new("tls-cn");
    let p = pki(&dir.0, "a");
    let port = next_port();
    let mut args = p.flags();
    args.extend(["--requirepass", "secret", "--tls-auth-clients-user", "CN"].map(String::from));
    let _srv = start_tls(&dir.0, port, &args, p.client(true));
    // The certificate's subject CN ("solikv node a"; its issuer's CN is the
    // CA's, so this also shows the subject is the one read).
    let user = "solikv node a";
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // No such user yet: the connection starts unauthenticated.
        let r = one(port, p.client(true), &["GET", "k"]).await.unwrap();
        assert!(r.starts_with("-NOAUTH"), "{r}");

        let mut admin = tls_connect(port, p.client(true)).await.unwrap();
        assert_eq!(
            cmd(&mut admin, &["AUTH", "secret"]).await.unwrap(),
            "+OK\r\n"
        );
        assert_eq!(
            cmd(
                &mut admin,
                &["ACL", "SETUSER", user, "on", "nopass", "+@all", "~*"]
            )
            .await
            .unwrap(),
            "+OK\r\n"
        );
        // Now the certificate alone authenticates, as that user.
        let mut s = tls_connect(port, p.client(true)).await.unwrap();
        assert_eq!(
            cmd(&mut s, &["ACL", "WHOAMI"]).await.unwrap(),
            format!("${}\r\n{user}\r\n", user.len())
        );
        assert_eq!(cmd(&mut s, &["SET", "k", "v"]).await.unwrap(), "+OK\r\n");

        // A user turned off authenticates no one.
        assert_eq!(
            cmd(&mut admin, &["ACL", "SETUSER", user, "off"])
                .await
                .unwrap(),
            "+OK\r\n"
        );
        let r = one(port, p.client(true), &["GET", "k"]).await.unwrap();
        assert!(r.starts_with("-NOAUTH"), "{r}");
    });

    // Without client certificates there is nothing to name a user with.
    let no_ca = Command::new(env!("CARGO_BIN_EXE_solikv"))
        .args(["--port", &next_port().to_string(), "--rest-port", "0"])
        .args(["--bind", "127.0.0.1", "--dir"])
        .arg(&dir.0)
        .args([
            "--tls-cert",
            p.cert.to_str().unwrap(),
            "--tls-key",
            p.key.to_str().unwrap(),
            "--tls-auth-clients-user",
            "CN",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!no_ca.success(), "CN without client certificates started");
}
