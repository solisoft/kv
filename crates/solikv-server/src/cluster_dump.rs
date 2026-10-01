//! Cluster dump/restore functionality for SoliKV
//! Provides tools to dump all cluster data to a file and restore it

use solikv_cluster::ConsistentHash;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// The TLS client configuration dump/restore connect with, when the nodes'
/// RESP ports speak TLS (the same `--tls-*` flags as the nodes).
pub type DumpTls = Arc<rustls::ClientConfig>;

/// A connection to one node: plain TCP, or TLS over it. Dump and restore are
/// one-shot blocking tools, so rustls is driven synchronously.
pub enum NodeStream {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Read for NodeStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            NodeStream::Plain(s) => s.read(buf),
            NodeStream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for NodeStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            NodeStream::Plain(s) => s.write(buf),
            NodeStream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            NodeStream::Plain(s) => s.flush(),
            NodeStream::Tls(s) => s.flush(),
        }
    }
}

const DUMP_MAGIC: &[u8] = b"SLKVCLST";
/// 2: each key carries its `DUMP` payload (`dump`), restored with
/// `RESTORE`; its TTL is `PTTL`'s milliseconds (`None`: no TTL). Version-1
/// files (readable values only, TTL in whole seconds × 1000, 0 for none)
/// still restore, for the types they recorded.
const DUMP_VERSION: u16 = 2;

#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub node_id: String,
    pub ip: String,
    pub port: u16,
    pub slots: Vec<(u16, u16)>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DumpHeader {
    pub version: u16,
    pub nodes: Vec<DumpNodeInfo>,
    pub created_at: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DumpNodeInfo {
    pub node_id: String,
    pub ip: String,
    pub port: u16,
    pub slots: Vec<(u16, u16)>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DumpKey {
    pub key: String,
    pub key_bytes: Vec<u8>,
    pub r#type: String,
    pub ttl_ms: Option<i64>,
    pub value: serde_json::Value,
    /// The key's `DUMP` payload, base64 (version ≥ 2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dump: Option<String>,
}

/// The masters in a `CLUSTER NODES` reply (Redis' format:
/// `id ip:port@cport[,hostname] flags master ping pong epoch link slot…`).
/// Replicas are left out: their data is their master's.
pub fn parse_cluster_nodes(output: &str) -> Vec<ClusterNode> {
    let mut nodes = Vec::new();
    for line in output.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            continue;
        }
        let flags = parts[2];
        if !flags.split(',').any(|f| f == "master") || flags.contains("handshake") {
            continue;
        }
        let hostport = parts[1].split(['@', ',']).next().unwrap_or("");
        let Some((ip, port)) = hostport.rsplit_once(':') else {
            continue;
        };
        let Ok(port) = port.parse::<u16>() else {
            continue;
        };
        let mut slots = Vec::new();
        for range in &parts[8.min(parts.len())..] {
            // `[slot->-node]` / `[slot-<-node]`: migration markers, not ranges.
            if range.starts_with('[') {
                continue;
            }
            match range.split_once('-') {
                Some((a, b)) => {
                    if let (Ok(a), Ok(b)) = (a.parse::<u16>(), b.parse::<u16>()) {
                        slots.push((a, b));
                    }
                }
                None => {
                    if let Ok(a) = range.parse::<u16>() {
                        slots.push((a, a));
                    }
                }
            }
        }
        nodes.push(ClusterNode {
            node_id: parts[0].to_string(),
            ip: ip.to_string(),
            port,
            slots,
        });
    }
    nodes
}

/// One RESP2 reply.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Status(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

impl Reply {
    fn into_bulk(self) -> Option<Vec<u8>> {
        match self {
            Reply::Bulk(b) => b,
            Reply::Status(s) => Some(s.into_bytes()),
            _ => None,
        }
    }

    fn into_array(self) -> Vec<Reply> {
        match self {
            Reply::Array(Some(a)) => a,
            _ => Vec::new(),
        }
    }

    fn as_text(&self) -> String {
        match self {
            Reply::Status(s) | Reply::Error(s) => s.clone(),
            Reply::Int(n) => n.to_string(),
            Reply::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            Reply::Bulk(None) | Reply::Array(None) => String::new(),
            Reply::Array(Some(a)) => a.iter().map(Reply::as_text).collect::<Vec<_>>().join(" "),
        }
    }

    /// The reply, or an error if the node answered with one.
    fn ok(self) -> Result<Reply, std::io::Error> {
        match self {
            Reply::Error(e) => Err(std::io::Error::other(e)),
            r => Ok(r),
        }
    }
}

/// Read exactly one complete reply. Bulk strings are read by length, so
/// values holding `\r\n` or arbitrary bytes come back whole, however the
/// transport (TCP segments, TLS records) splits them.
pub fn read_reply<R: BufRead>(r: &mut R) -> Result<Reply, std::io::Error> {
    read_reply_depth(r, 0)
}

fn read_reply_depth<R: BufRead>(r: &mut R, depth: usize) -> Result<Reply, std::io::Error> {
    let bad = |m: String| std::io::Error::new(std::io::ErrorKind::InvalidData, m);
    if depth > 16 {
        return Err(bad("reply nested too deeply".into()));
    }
    let mut line = Vec::new();
    if r.read_until(b'\n', &mut line)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "connection closed",
        ));
    }
    if !line.ends_with(b"\r\n") {
        return Err(bad("truncated reply line".into()));
    }
    line.truncate(line.len() - 2);
    let (kind, rest) = line
        .split_first()
        .ok_or_else(|| bad("empty reply line".into()))?;
    let text = String::from_utf8_lossy(rest).into_owned();
    let num = || {
        text.parse::<i64>()
            .map_err(|_| bad(format!("bad length {text:?}")))
    };
    Ok(match kind {
        b'+' => Reply::Status(text),
        b'-' => Reply::Error(text),
        b':' => Reply::Int(num()?),
        b'$' => {
            let n = num()?;
            if n < 0 {
                Reply::Bulk(None)
            } else {
                let mut buf = vec![0u8; n as usize + 2];
                r.read_exact(&mut buf)?;
                buf.truncate(n as usize);
                Reply::Bulk(Some(buf))
            }
        }
        b'*' => {
            let n = num()?;
            if n < 0 {
                Reply::Array(None)
            } else {
                let mut items = Vec::with_capacity((n as usize).min(1024));
                for _ in 0..n {
                    items.push(read_reply_depth(r, depth + 1)?);
                }
                Reply::Array(Some(items))
            }
        }
        _ => return Err(bad(format!("unexpected reply type {:?}", *kind as char))),
    })
}

/// A connection to one node.
pub struct NodeConn {
    io: BufReader<NodeStream>,
}

impl NodeConn {
    /// Send one command and read its reply.
    pub fn cmd(&mut self, args: &[&[u8]]) -> Result<Reply, std::io::Error> {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        let w = self.io.get_mut();
        w.write_all(&out)?;
        w.flush()?;
        read_reply(&mut self.io)
    }
}

/// Connect to one node — over TLS when `tls` is given, checking the node's
/// certificate against the CA and presenting ours — authenticating with
/// `AUTH [user] password` when a password is given.
pub fn connect_node(
    ip: &str,
    port: u16,
    user: Option<&str>,
    password: Option<&str>,
    tls: Option<&DumpTls>,
) -> Result<NodeConn, std::io::Error> {
    let addr = (ip.trim_start_matches('[').trim_end_matches(']'), port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no address for {ip}:{port}"),
            )
        })?;
    let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
    tcp.set_read_timeout(Some(Duration::from_secs(30)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(30)))?;
    let stream = match tls {
        None => NodeStream::Plain(tcp),
        Some(cfg) => {
            let conn = rustls::ClientConnection::new(cfg.clone(), crate::tls::server_name(ip))
                .map_err(std::io::Error::other)?;
            let mut s = rustls::StreamOwned::new(conn, tcp);
            // Finish the handshake here, so a refused certificate is
            // reported as a connection failure, not as a garbled reply.
            while s.conn.is_handshaking() {
                s.conn.complete_io(&mut s.sock)?;
            }
            NodeStream::Tls(Box::new(s))
        }
    };
    let mut conn = NodeConn {
        io: BufReader::with_capacity(64 * 1024, stream),
    };
    if let Some(pwd) = password {
        let reply = match user {
            Some(u) => conn.cmd(&[b"AUTH", u.as_bytes(), pwd.as_bytes()])?,
            None => conn.cmd(&[b"AUTH", pwd.as_bytes()])?,
        };
        if reply != Reply::Status("OK".into()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("authentication failed: {}", reply.as_text()),
            ));
        }
    }
    Ok(conn)
}

fn text(b: &[u8]) -> serde_json::Value {
    serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
}

/// A readable rendering of a value, for people and tools reading the dump;
/// restore uses the `DUMP` payload beside it.
fn readable_value(
    conn: &mut NodeConn,
    key: &[u8],
    r#type: &str,
) -> Result<serde_json::Value, std::io::Error> {
    use serde_json::Value;
    Ok(match r#type {
        "string" => conn
            .cmd(&[b"GET", key])?
            .ok()?
            .into_bulk()
            .map_or(Value::Null, |b| text(&b)),
        "list" => Value::Array(
            conn.cmd(&[b"LRANGE", key, b"0", b"-1"])?
                .ok()?
                .into_array()
                .into_iter()
                .filter_map(Reply::into_bulk)
                .map(|b| text(&b))
                .collect(),
        ),
        "set" => Value::Array(
            conn.cmd(&[b"SMEMBERS", key])?
                .ok()?
                .into_array()
                .into_iter()
                .filter_map(Reply::into_bulk)
                .map(|b| text(&b))
                .collect(),
        ),
        "hash" => {
            let items = conn.cmd(&[b"HGETALL", key])?.ok()?.into_array();
            let mut map = serde_json::Map::new();
            let mut it = items.into_iter().filter_map(Reply::into_bulk);
            while let (Some(f), Some(v)) = (it.next(), it.next()) {
                map.insert(String::from_utf8_lossy(&f).into_owned(), text(&v));
            }
            Value::Object(map)
        }
        "zset" => {
            let items = conn
                .cmd(&[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"])?
                .ok()?
                .into_array();
            let mut out = Vec::new();
            let mut it = items.into_iter().filter_map(Reply::into_bulk);
            while let (Some(m), Some(s)) = (it.next(), it.next()) {
                let score = String::from_utf8_lossy(&s).parse::<f64>().unwrap_or(0.0);
                out.push(serde_json::json!({ "member": text(&m), "score": score }));
            }
            Value::Array(out)
        }
        _ => Value::Null,
    })
}

/// Every key of one node (SCAN, so the node keeps serving), with its type,
/// remaining TTL in milliseconds, readable value and `DUMP` payload.
pub fn scan_all_keys(conn: &mut NodeConn) -> Result<Vec<DumpKey>, std::io::Error> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut out = Vec::new();
    let mut cursor = b"0".to_vec();
    loop {
        let mut page = conn
            .cmd(&[b"SCAN", &cursor, b"COUNT", b"1000"])?
            .ok()?
            .into_array();
        if page.len() != 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "malformed SCAN reply",
            ));
        }
        let keys = page.pop().unwrap().into_array();
        cursor = page.pop().unwrap().into_bulk().unwrap_or_default();
        for key in keys.into_iter().filter_map(Reply::into_bulk) {
            let r#type = conn.cmd(&[b"TYPE", &key])?.ok()?.as_text();
            if r#type == "none" {
                continue; // deleted or expired since SCAN listed it
            }
            let ttl_ms = match conn.cmd(&[b"PTTL", &key])?.ok()? {
                Reply::Int(ms) if ms > 0 => Some(ms),
                Reply::Int(-2) => continue,
                _ => None,
            };
            let dump = conn
                .cmd(&[b"DUMP", &key])?
                .ok()?
                .into_bulk()
                .map(|p| b64.encode(p));
            if dump.is_none() {
                continue; // gone between TYPE and DUMP
            }
            let value = readable_value(conn, &key, &r#type)?;
            out.push(DumpKey {
                key: String::from_utf8_lossy(&key).into_owned(),
                key_bytes: key,
                r#type,
                ttl_ms,
                value,
                dump,
            });
        }
        if cursor == b"0" {
            break;
        }
    }
    Ok(out)
}

/// Split `host:port` (IPv6 hosts in brackets), defaulting to port 6379.
fn split_addr(addr: &str) -> (String, u16) {
    match addr.rsplit_once(':') {
        Some((ip, port)) if !ip.is_empty() => (ip.to_string(), port.parse().unwrap_or(6379)),
        _ => ("127.0.0.1".to_string(), addr.parse().unwrap_or(6379)),
    }
}

pub fn dump_cluster(
    output_path: &Path,
    format: &str,
    connect_addr: &str,
    user: Option<&str>,
    password: Option<&str>,
    per_node_password: Option<&str>,
    tls: Option<&DumpTls>,
) -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    let (seed_ip, seed_port) = split_addr(connect_addr);

    tracing::info!("Connecting to seed node {}:{}", seed_ip, seed_port);
    let mut conn = connect_node(&seed_ip, seed_port, user, password, tls)?;
    let nodes_reply = conn.cmd(&[b"CLUSTER", b"NODES"])?.ok()?.as_text();
    let nodes = parse_cluster_nodes(&nodes_reply);
    if nodes.is_empty() {
        // A node not in cluster mode, or an empty reply: an empty dump
        // would read as an empty cluster.
        return Err(format!(
            "seed node {seed_ip}:{seed_port} listed no masters (reply: {:?})",
            nodes_reply.chars().take(120).collect::<String>()
        )
        .into());
    }
    tracing::info!("Found {} nodes in cluster", nodes.len());

    let mut all_data: Vec<DumpKey> = Vec::new();
    let mut failed: Vec<String> = Vec::new();
    for node in &nodes {
        let node_password = if node.ip == seed_ip && node.port == seed_port {
            password
        } else {
            per_node_password
        };
        tracing::info!(
            "Dumping node {}:{} (id: {})",
            node.ip,
            node.port,
            node.node_id
        );
        let got = connect_node(&node.ip, node.port, user, node_password, tls)
            .and_then(|mut c| scan_all_keys(&mut c));
        match got {
            Ok(keys) => all_data.extend(keys),
            Err(e) => failed.push(format!("{}:{}: {e}", node.ip, node.port)),
        }
    }
    // A dump missing a node's keys must not pass for the whole cluster.
    if !failed.is_empty() {
        return Err(format!(
            "{} of {} nodes could not be dumped, no file written: {}",
            failed.len(),
            nodes.len(),
            failed.join("; ")
        )
        .into());
    }

    let header = DumpHeader {
        version: DUMP_VERSION,
        nodes: nodes
            .iter()
            .map(|n| DumpNodeInfo {
                node_id: n.node_id.clone(),
                ip: n.ip.clone(),
                port: n.port,
                slots: n.slots.clone(),
            })
            .collect(),
        created_at: chrono::Utc::now().to_rfc3339(),
    };

    // Every key and value of the cluster: 0600, and no following a symlink
    // planted at the output path.
    let file = solikv_persist::fsutil::create_private(output_path)?;
    let mut writer = std::io::BufWriter::new(file);
    if format == "jsonl" {
        writeln!(writer, "{}", serde_json::to_string(&header)?)?;
        for key in &all_data {
            writeln!(writer, "{}", serde_json::to_string(key)?)?;
        }
    } else {
        // Binary: magic, version, then the same records as JSONL, each
        // length-prefixed (u32 LE) — the header first.
        writer.write_all(DUMP_MAGIC)?;
        writer.write_all(&DUMP_VERSION.to_le_bytes())?;
        let mut record = |bytes: Vec<u8>| -> std::io::Result<()> {
            writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
            writer.write_all(&bytes)
        };
        record(serde_json::to_vec(&header)?)?;
        for key in &all_data {
            record(serde_json::to_vec(key)?)?;
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;

    tracing::info!(
        "Cluster dump complete: {} keys from {} nodes",
        all_data.len(),
        nodes.len()
    );
    Ok((all_data.len(), nodes.len()))
}

/// The header and keys of a dump file, JSONL or binary.
fn read_dump_file(
    path: &Path,
) -> Result<(DumpHeader, Vec<DumpKey>), Box<dyn std::error::Error + Send + Sync>> {
    let content = std::fs::read(path)?;
    if content.starts_with(DUMP_MAGIC) {
        let mut at = DUMP_MAGIC.len() + 2;
        let version = u16::from_le_bytes(
            content
                .get(DUMP_MAGIC.len()..at)
                .ok_or("truncated binary dump")?
                .try_into()?,
        );
        if version < 2 {
            return Err(
                "binary dumps written before version 2 hold no restorable data; \
                        dump again (jsonl or binary)"
                    .into(),
            );
        }
        let mut records = Vec::new();
        while at < content.len() {
            let len = u32::from_le_bytes(
                content
                    .get(at..at + 4)
                    .ok_or("truncated binary dump")?
                    .try_into()?,
            ) as usize;
            at += 4;
            records.push(content.get(at..at + len).ok_or("truncated binary dump")?);
            at += len;
        }
        let (first, rest) = records.split_first().ok_or("empty dump file")?;
        let header: DumpHeader = serde_json::from_slice(first)?;
        let keys = rest
            .iter()
            .map(|r| serde_json::from_slice::<DumpKey>(r))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok((header, keys));
    }
    let content = String::from_utf8(content)?;
    let mut lines = content.lines().filter(|l| !l.trim().is_empty());
    let header: DumpHeader = serde_json::from_str(lines.next().ok_or("empty dump file")?)?;
    let keys = lines
        .map(serde_json::from_str::<DumpKey>)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((header, keys))
}

/// The commands that recreate one dumped key: `RESTORE` from its payload
/// (version ≥ 2), or, for a version-1 dump that has none, the type's write
/// commands from the readable value (strings, lists, sets, hashes, sorted
/// sets — the types version 1 recorded).
fn restore_commands(k: &DumpKey) -> Result<Vec<Vec<Vec<u8>>>, String> {
    use base64::Engine;
    let key = if k.key_bytes.is_empty() {
        k.key.as_bytes().to_vec()
    } else {
        k.key_bytes.clone()
    };
    let ttl = k.ttl_ms.filter(|&t| t > 0);
    if let Some(p) = &k.dump {
        let payload = base64::engine::general_purpose::STANDARD
            .decode(p)
            .map_err(|e| format!("bad payload: {e}"))?;
        return Ok(vec![vec![
            b"RESTORE".to_vec(),
            key,
            ttl.unwrap_or(0).to_string().into_bytes(),
            payload,
            b"REPLACE".to_vec(),
        ]]);
    }
    let s = |v: &serde_json::Value| v.as_str().map(|s| s.as_bytes().to_vec());
    let mut cmds = vec![vec![b"DEL".to_vec(), key.clone()]];
    let mut write = |name: &[u8], items: Vec<Vec<u8>>| {
        if !items.is_empty() {
            let mut c = vec![name.to_vec(), key.clone()];
            c.extend(items);
            cmds.push(c);
        }
    };
    match (k.r#type.as_str(), &k.value) {
        ("string", v) => write(b"SET", vec![s(v).ok_or("string value is not a string")?]),
        ("list", serde_json::Value::Array(a)) => write(b"RPUSH", a.iter().filter_map(s).collect()),
        ("set", serde_json::Value::Array(a)) => write(b"SADD", a.iter().filter_map(s).collect()),
        ("hash", serde_json::Value::Object(m)) => write(
            b"HSET",
            m.iter()
                .flat_map(|(f, v)| [Some(f.as_bytes().to_vec()), s(v)])
                .flatten()
                .collect(),
        ),
        ("zset", serde_json::Value::Array(a)) => write(
            b"ZADD",
            a.iter()
                .flat_map(|e| {
                    [
                        e.get("score")
                            .and_then(|x| x.as_f64())
                            .map(|x| x.to_string().into_bytes()),
                        e.get("member").and_then(s),
                    ]
                })
                .flatten()
                .collect(),
        ),
        (t, _) => return Err(format!("cannot restore a version-1 {t} without a payload")),
    }
    if let Some(ms) = ttl {
        cmds.push(vec![b"PEXPIRE".to_vec(), key, ms.to_string().into_bytes()]);
    }
    Ok(cmds)
}

pub fn restore_cluster(
    input_path: &Path,
    connect_addr: &str,
    user: Option<&str>,
    password: Option<&str>,
    per_node_password: Option<&str>,
    tls: Option<&DumpTls>,
) -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    let (header, keys) = read_dump_file(input_path)?;
    tracing::info!(
        "Dump version: {}, nodes: {}, keys: {}",
        header.version,
        header.nodes.len(),
        keys.len()
    );

    let (seed_ip, seed_port) = split_addr(connect_addr);
    let mut seed = connect_node(&seed_ip, seed_port, user, password, tls)?;
    let nodes_reply = seed.cmd(&[b"CLUSTER", b"NODES"])?.ok()?.as_text();
    let mut owner: Vec<Option<(String, u16)>> = vec![None; 16384];
    for node in parse_cluster_nodes(&nodes_reply) {
        for &(start, end) in &node.slots {
            for slot in start..=end.min(16383) {
                owner[slot as usize] = Some((node.ip.clone(), node.port));
            }
        }
    }

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    // A few workers, each keeping one connection per node it writes to.
    let queue = Mutex::new(keys.into_iter());
    let success = AtomicUsize::new(0);
    let errors = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                let mut conns: HashMap<(String, u16), NodeConn> = HashMap::new();
                loop {
                    let Some(k) = queue.lock().unwrap().next() else {
                        break;
                    };
                    let key = if k.key_bytes.is_empty() {
                        k.key.as_bytes()
                    } else {
                        &k.key_bytes[..]
                    };
                    let slot = ConsistentHash::key_slot(key) as usize;
                    let result = (|| -> Result<(), String> {
                        let target = owner[slot]
                            .clone()
                            .ok_or_else(|| format!("no node owns slot {slot}"))?;
                        let cmds = restore_commands(&k)?;
                        if !conns.contains_key(&target) {
                            let pw = if target.0 == seed_ip && target.1 == seed_port {
                                password
                            } else {
                                per_node_password
                            };
                            let c = connect_node(&target.0, target.1, user, pw, tls)
                                .map_err(|e| e.to_string())?;
                            conns.insert(target.clone(), c);
                        }
                        let conn = conns.get_mut(&target).expect("inserted above");
                        let mut broken = None;
                        for c in &cmds {
                            let args: Vec<&[u8]> = c.iter().map(Vec::as_slice).collect();
                            match conn.cmd(&args) {
                                Ok(r) => {
                                    r.ok().map_err(|e| e.to_string())?;
                                }
                                Err(e) => {
                                    broken = Some(e.to_string());
                                    break;
                                }
                            }
                        }
                        if let Some(e) = broken {
                            // The link is in an unknown state: reconnect next time.
                            conns.remove(&target);
                            return Err(e);
                        }
                        Ok(())
                    })();
                    match result {
                        Ok(()) => success.fetch_add(1, Ordering::Relaxed),
                        Err(e) => {
                            tracing::warn!("restore of {:?} failed: {e}", k.key);
                            errors.fetch_add(1, Ordering::Relaxed)
                        }
                    };
                }
            });
        }
    });

    let (ok, failed) = (success.into_inner(), errors.into_inner());
    tracing::info!("Restore complete: {} success, {} errors", ok, failed);
    Ok((ok, failed))
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "07c37dfeb235213a872192d90877d0cd55635b91";

    #[test]
    fn cluster_nodes_lines_are_read_in_redis_format() {
        let output = format!(
            "{A} 127.0.0.1:30004@31004,host-a myself,master - 0 1426238317239 4 connected 0-5460 7000 [93->-{A}]\n\
             e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 slave {A} 0 1426238316232 4 connected\n\
             292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master,fail - 0 1426238318243 3 disconnected 10923-16383\n"
        );
        let nodes = parse_cluster_nodes(&output);
        assert_eq!(nodes.len(), 2, "the replica is left out");
        assert_eq!(nodes[0].node_id, A);
        assert_eq!(nodes[0].ip, "127.0.0.1");
        assert_eq!(nodes[0].port, 30004, "the data port, not the bus port");
        assert_eq!(nodes[0].slots, vec![(0, 5460), (7000, 7000)]);
        assert_eq!(nodes[1].slots, vec![(10923, 16383)]);
    }

    #[test]
    fn cluster_nodes_ignores_short_and_empty_lines() {
        let output = format!("\n\nfoo bar\n{A} 10.0.0.1:7000@17000 master - 0 0 1 connected\n");
        let nodes = parse_cluster_nodes(&output);
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].slots.is_empty());
    }

    #[test]
    fn test_consistent_hash_slot_calculation() {
        let slot1 = ConsistentHash::key_slot(b"hello");
        let slot2 = ConsistentHash::key_slot(b"world");
        let slot3 = ConsistentHash::key_slot(b"hello");

        assert_eq!(slot1, slot3);
        assert_ne!(slot1, slot2);
        assert!(slot1 < 16384);
        assert!(slot2 < 16384);
    }

    #[test]
    fn test_slot_calculation_known_keys() {
        let slot1 = ConsistentHash::key_slot(b"key:123");
        let slot2 = ConsistentHash::key_slot(b"key:456");
        assert!(slot1 < 16384);
        assert!(slot2 < 16384);
        assert_ne!(slot1, slot2);
    }

    #[test]
    fn test_dump_header_serialization() {
        let header = DumpHeader {
            version: 1,
            nodes: vec![DumpNodeInfo {
                node_id: "test-node".to_string(),
                ip: "127.0.0.1".to_string(),
                port: 6379,
                slots: vec![(0, 16383)],
            }],
            created_at: "2026-03-20T12:00:00Z".to_string(),
        };

        let json = serde_json::to_string(&header).unwrap();
        let parsed: DumpHeader = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.version, 1);
        assert_eq!(parsed.nodes[0].port, 6379);
    }

    #[test]
    fn test_dump_key_serialization() {
        let key = DumpKey {
            key: "testkey".to_string(),
            key_bytes: vec![],
            r#type: "string".to_string(),
            ttl_ms: Some(3600000),
            value: serde_json::Value::String("testvalue".to_string()),
            dump: None,
        };

        let json = serde_json::to_string(&key).unwrap();
        let parsed: DumpKey = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.key, "testkey");
        assert_eq!(parsed.ttl_ms, Some(3600000));
    }

    /// A reader handing out one byte per read, like a transport that splits
    /// every reply at the worst places.
    struct Trickle(Vec<u8>, usize);
    impl Read for Trickle {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.1 >= self.0.len() || buf.is_empty() {
                return Ok(0);
            }
            buf[0] = self.0[self.1];
            self.1 += 1;
            Ok(1)
        }
    }

    #[test]
    fn replies_are_read_whole_however_they_are_split() {
        let wire = b"*3\r\n$1\r\na\r\n$4\r\nx\r\ny\r\n$-1\r\n:42\r\n-ERR no\r\n+OK\r\n";
        let mut r = BufReader::with_capacity(1, Trickle(wire.to_vec(), 0));
        assert_eq!(
            read_reply(&mut r).unwrap(),
            Reply::Array(Some(vec![
                Reply::Bulk(Some(b"a".to_vec())),
                Reply::Bulk(Some(b"x\r\ny".to_vec())),
                Reply::Bulk(None),
            ]))
        );
        assert_eq!(read_reply(&mut r).unwrap(), Reply::Int(42));
        assert_eq!(read_reply(&mut r).unwrap(), Reply::Error("ERR no".into()));
        assert_eq!(read_reply(&mut r).unwrap(), Reply::Status("OK".into()));
        assert!(read_reply(&mut r).is_err(), "EOF is an error, not a reply");
    }

    #[test]
    fn a_truncated_bulk_is_an_error() {
        let mut r = BufReader::new(&b"$10\r\nabc"[..]);
        assert!(read_reply(&mut r).is_err());
    }

    fn v1_key(t: &str, value: serde_json::Value, ttl: Option<i64>) -> DumpKey {
        DumpKey {
            key: "k".into(),
            key_bytes: vec![],
            r#type: t.into(),
            ttl_ms: ttl,
            value,
            dump: None,
        }
    }

    fn strs(cmds: &[Vec<Vec<u8>>]) -> Vec<Vec<String>> {
        cmds.iter()
            .map(|c| {
                c.iter()
                    .map(|a| String::from_utf8_lossy(a).into_owned())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn version_1_dumps_restore_by_type_not_as_json_strings() {
        let list =
            restore_commands(&v1_key("list", serde_json::json!(["a", "b"]), Some(0))).unwrap();
        assert_eq!(
            strs(&list),
            [vec!["DEL", "k"], vec!["RPUSH", "k", "a", "b"]]
        );
        let hash =
            restore_commands(&v1_key("hash", serde_json::json!({"f": "v"}), Some(5000))).unwrap();
        assert_eq!(
            strs(&hash),
            [
                vec!["DEL", "k"],
                vec!["HSET", "k", "f", "v"],
                vec!["PEXPIRE", "k", "5000"]
            ]
        );
        let zset = restore_commands(&v1_key(
            "zset",
            serde_json::json!([{"member": "m", "score": 1.5}]),
            None,
        ))
        .unwrap();
        assert_eq!(
            strs(&zset),
            [vec!["DEL", "k"], vec!["ZADD", "k", "1.5", "m"]]
        );
        assert!(restore_commands(&v1_key("stream", serde_json::Value::Null, None)).is_err());
    }

    #[test]
    fn version_2_dumps_restore_from_the_payload() {
        use base64::Engine;
        let mut k = v1_key("list", serde_json::json!(["a"]), Some(1234));
        k.key_bytes = b"k\x00bin".to_vec();
        k.dump = Some(base64::engine::general_purpose::STANDARD.encode(b"\x0e\x01payload"));
        let cmds = restore_commands(&k).unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0][0], b"RESTORE");
        assert_eq!(cmds[0][1], b"k\x00bin", "the exact key bytes");
        assert_eq!(cmds[0][2], b"1234");
        assert_eq!(cmds[0][3], b"\x0e\x01payload");
        assert_eq!(cmds[0][4], b"REPLACE");
    }

    #[test]
    fn binary_dumps_read_back() {
        let dir = std::env::temp_dir().join(format!("solikv-dump-bin-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("d.bin");
        let header = DumpHeader {
            version: DUMP_VERSION,
            nodes: vec![],
            created_at: "now".into(),
        };
        let key = v1_key("string", serde_json::json!("v"), None);
        let mut bytes = DUMP_MAGIC.to_vec();
        bytes.extend_from_slice(&DUMP_VERSION.to_le_bytes());
        for rec in [
            serde_json::to_vec(&header).unwrap(),
            serde_json::to_vec(&key).unwrap(),
        ] {
            bytes.extend_from_slice(&(rec.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&rec);
        }
        std::fs::write(&path, &bytes).unwrap();
        let (h, keys) = read_dump_file(&path).unwrap();
        assert_eq!(h.version, DUMP_VERSION);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].value, serde_json::json!("v"));

        // A version-1 binary file held no restorable records.
        let mut old = DUMP_MAGIC.to_vec();
        old.extend_from_slice(&1u16.to_le_bytes());
        std::fs::write(&path, &old).unwrap();
        assert!(read_dump_file(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
