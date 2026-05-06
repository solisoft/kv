//! Cluster dump/restore functionality for SoliKV
//! Provides tools to dump all cluster data to a file and restore it

use solikv_cluster::ConsistentHash;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const DUMP_MAGIC: &[u8] = b"SLKVCLST";
const DUMP_VERSION: u16 = 1;

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
}

pub fn parse_cluster_nodes(output: &str) -> Vec<ClusterNode> {
    let mut nodes = Vec::new();

    for line in output.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue;
        }

        let node_id = parts[0].to_string();
        let addr = parts[1];

        // Parse ip:port from format ip@port
        let (ip, port) = if let Some((ip, port_str)) = addr.split_once('@') {
            if let Some((_, port)) = port_str.rsplit_once(':') {
                (ip.to_string(), port.parse().unwrap_or(6379))
            } else {
                (ip.to_string(), port_str.parse().unwrap_or(6379))
            }
        } else if let Some((ip, port)) = addr.rsplit_once(':') {
            (ip.to_string(), port.parse().unwrap_or(6379))
        } else {
            ("127.0.0.1".to_string(), 6379)
        };

        // Parse slots
        let mut slots = Vec::new();
        if let Some(slot_part) = parts.get(3) {
            for range in slot_part.split(',') {
                if range == "-" {
                    slots.push((0, 16383));
                    continue;
                }
                if let Some((start, end)) = range.split_once('-') {
                    if let (Ok(s), Ok(e)) = (start.parse::<u16>(), end.parse::<u16>()) {
                        slots.push((s, e));
                    }
                } else if let Ok(s) = range.parse::<u16>() {
                    slots.push((s, s));
                }
            }
        }

        nodes.push(ClusterNode {
            node_id,
            ip,
            port,
            slots,
        });
    }

    nodes
}

fn read_full_response(stream: &mut TcpStream) -> Result<Vec<u8>, std::io::Error> {
    let mut response = Vec::new();
    let mut buf = [0u8; 4096];

    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            break;
        }
        response.extend_from_slice(&buf[..n]);

        if !response.is_empty() {
            let first = response[0];

            if first == b'+' || first == b'-' || first == b':' {
                if response.ends_with(b"\r\n") {
                    break;
                }
            } else if first == b'$' {
                if response.len() >= 5 && response.starts_with(b"$-1\r\n") {
                    break;
                }
                if response.ends_with(b"\r\n") {
                    if let Some(pos) = response[1..].iter().position(|&b| b == b'\r') {
                        let len_str = String::from_utf8_lossy(&response[1..1 + pos]);
                        if let Ok(len) = len_str.trim().parse::<usize>() {
                            let expected = 1 + pos + 2 + len + 2;
                            if response.len() >= expected {
                                break;
                            }
                        }
                    }
                }
            } else if first == b'*' && response.ends_with(b"\r\n") && response.ends_with(b"\r\n") {
                break;
            }
        }
    }

    Ok(response)
}

pub fn send_resp_command(stream: &mut TcpStream, args: &[&str]) -> Result<String, std::io::Error> {
    let mut cmd = format!("*{}\r\n", args.len());
    for arg in args {
        cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }

    stream.write_all(cmd.as_bytes())?;
    stream.flush()?;

    let response = read_full_response(stream)?;

    Ok(String::from_utf8_lossy(&response).to_string())
}

pub fn connect_node(
    ip: &str,
    port: u16,
    password: Option<&str>,
) -> Result<TcpStream, std::io::Error> {
    let addr = format!("{}:{}", ip, port);
    let mut stream = TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(5))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;

    if let Some(pwd) = password {
        let resp = send_resp_command(&mut stream, &["AUTH", pwd])?;
        if !resp.starts_with("+OK") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Authentication failed",
            ));
        }
    }

    Ok(stream)
}

#[allow(clippy::type_complexity)]
pub fn scan_all_keys(
    stream: &mut TcpStream,
) -> Result<Vec<(String, String, Option<i64>, serde_json::Value)>, std::io::Error> {
    let mut all_keys = Vec::new();

    // Use KEYS * to get all keys (simpler for single-node dump)
    let resp = send_resp_command(stream, &["KEYS", "*"])?;
    let keys = parse_keys_response(&resp)?;

    for key in keys {
        let key_str = String::from_utf8_lossy(&key).to_string();

        // Get key type
        let type_resp = send_resp_command(stream, &["TYPE", &key_str])?;
        let r#type = parse_type_response(&type_resp);

        // Get TTL
        let ttl_resp = send_resp_command(stream, &["TTL", &key_str])?;
        let ttl = parse_ttl_response(&ttl_resp);

        // Get value based on type
        let value = match r#type.as_str() {
            "string" => {
                let val_resp = send_resp_command(stream, &["GET", &key_str])?;
                parse_string_value(&val_resp)
            }
            "list" => {
                let val_resp = send_resp_command(stream, &["LRANGE", &key_str, "0", "-1"])?;
                parse_list_value(&val_resp)
            }
            "hash" => {
                let val_resp = send_resp_command(stream, &["HGETALL", &key_str])?;
                parse_hash_value(&val_resp)
            }
            "set" => {
                let val_resp = send_resp_command(stream, &["SMEMBERS", &key_str])?;
                parse_set_value(&val_resp)
            }
            "zset" => {
                let val_resp =
                    send_resp_command(stream, &["ZRANGE", &key_str, "0", "-1", "WITHSCORES"])?;
                parse_zset_value(&val_resp)
            }
            _ => serde_json::Value::Null,
        };

        all_keys.push((key_str, r#type, ttl, value));
    }

    Ok(all_keys)
}

fn parse_keys_response(resp: &str) -> Result<Vec<Vec<u8>>, std::io::Error> {
    let lines: Vec<&str> = resp.lines().collect();
    if lines.is_empty() || !lines[0].starts_with('*') {
        return Ok(Vec::new());
    }

    let count: usize = lines[0].trim_start_matches('*').parse().unwrap_or(0);
    let mut keys = Vec::new();

    let mut i = 2;
    while i < lines.len() && keys.len() < count {
        if lines[i].starts_with('$') {
            if i + 1 < lines.len() {
                keys.push(lines[i + 1].as_bytes().to_vec());
                i += 2;
            } else {
                break;
            }
        } else {
            i += 1;
        }
    }

    Ok(keys)
}

#[allow(dead_code)]
fn parse_scan_response(resp: &str) -> Result<(String, Vec<Vec<u8>>), std::io::Error> {
    let lines: Vec<&str> = resp.lines().collect();
    if lines.len() < 4 || !lines[0].starts_with('*') {
        return Ok((String::new(), Vec::new()));
    }

    let cursor = lines[2].trim_start_matches('$').to_string();

    let mut keys = Vec::new();
    let mut i = 4;
    while i < lines.len() - 1 {
        if lines[i].starts_with('$') {
            let _len: usize = lines[i].trim_start_matches('$').parse().unwrap_or(0);
            if i + 1 < lines.len() {
                keys.push(lines[i + 1].as_bytes().to_vec());
                i += 2;
            } else {
                break;
            }
        } else {
            i += 1;
        }
    }

    Ok((cursor, keys))
}

fn parse_type_response(resp: &str) -> String {
    let resp = resp.trim();
    if let Some(stripped) = resp.strip_prefix('+') {
        return stripped.trim().to_string();
    }
    "none".to_string()
}

fn parse_ttl_response(resp: &str) -> Option<i64> {
    let resp = resp.trim();
    if let Some(stripped) = resp.strip_prefix(':') {
        if let Ok(ttl) = stripped.trim().parse::<i64>() {
            if ttl > 0 {
                return Some(ttl * 1000);
            }
            return Some(0);
        }
    }
    None
}

fn parse_string_value(resp: &str) -> serde_json::Value {
    let resp = resp.trim();
    if resp.starts_with('$') {
        if let Some(crlf_pos) = resp.find("\r\n") {
            let len_str = &resp[1..crlf_pos];
            if let Ok(len) = len_str.parse::<usize>() {
                let value_start = crlf_pos + 2;
                let value_end = value_start + len;
                if resp.len() >= value_end {
                    return serde_json::Value::String(resp[value_start..value_end].to_string());
                }
            }
        }
    }
    serde_json::Value::Null
}

fn parse_list_value(resp: &str) -> serde_json::Value {
    let lines: Vec<&str> = resp.lines().collect();
    if lines.is_empty() || !lines[0].starts_with('*') {
        return serde_json::Value::Array(Vec::new());
    }

    let count: usize = lines[0].trim_start_matches('*').parse().unwrap_or(0);
    let mut items = Vec::new();

    let mut i = 2;
    while i < lines.len() && items.len() < count {
        if lines[i].starts_with('$') {
            if i + 1 < lines.len() {
                items.push(serde_json::Value::String(lines[i + 1].to_string()));
                i += 2;
            } else {
                break;
            }
        } else {
            i += 1;
        }
    }

    serde_json::Value::Array(items)
}

fn parse_hash_value(resp: &str) -> serde_json::Value {
    let lines: Vec<&str> = resp.lines().collect();
    if lines.is_empty() || !lines[0].starts_with('*') {
        return serde_json::Value::Object(serde_json::Map::new());
    }

    let count: usize = lines[0].trim_start_matches('*').parse().unwrap_or(0);
    let mut map = serde_json::Map::new();

    let mut i = 2;
    while i + 1 < lines.len() && map.len() < count / 2 {
        if lines[i].starts_with('$') {
            if i + 1 < lines.len() {
                let key = lines[i + 1].to_string();
                i += 2;
                if i < lines.len() && lines[i].starts_with('$') && i + 1 < lines.len() {
                    let value = lines[i + 1].to_string();
                    map.insert(key, serde_json::Value::String(value));
                    i += 2;
                }
            }
        } else {
            i += 1;
        }
    }

    serde_json::Value::Object(map)
}

fn parse_set_value(resp: &str) -> serde_json::Value {
    let lines: Vec<&str> = resp.lines().collect();
    if lines.is_empty() || !lines[0].starts_with('*') {
        return serde_json::Value::Array(Vec::new());
    }

    let count: usize = lines[0].trim_start_matches('*').parse().unwrap_or(0);
    let mut items = Vec::new();

    let mut i = 2;
    while i < lines.len() && items.len() < count {
        if lines[i].starts_with('$') {
            if i + 1 < lines.len() {
                items.push(serde_json::Value::String(lines[i + 1].to_string()));
                i += 2;
            } else {
                break;
            }
        } else {
            i += 1;
        }
    }

    serde_json::Value::Array(items)
}

fn parse_zset_value(resp: &str) -> serde_json::Value {
    let lines: Vec<&str> = resp.lines().collect();
    if lines.is_empty() || !lines[0].starts_with('*') {
        return serde_json::Value::Array(Vec::new());
    }

    let items: Vec<&str> = lines
        .iter()
        .filter(|l| !l.is_empty() && !l.starts_with('*') && !l.starts_with('$'))
        .cloned()
        .collect();

    let mut result = Vec::new();
    let mut i = 0;
    while i < items.len() - 1 {
        result.push(serde_json::json!({
            "member": items[i],
            "score": items[i + 1].parse::<f64>().unwrap_or(0.0)
        }));
        i += 2;
    }

    serde_json::Value::Array(result)
}

pub fn dump_cluster(
    output_path: &Path,
    format: &str,
    connect_addr: &str,
    password: Option<&str>,
    per_node_password: Option<&str>,
) -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    let (seed_ip, seed_port) = if let Some((ip, port)) = connect_addr.rsplit_once(':') {
        (ip.to_string(), port.parse().unwrap_or(6379))
    } else {
        ("127.0.0.1".to_string(), 6379)
    };

    tracing::info!("Connecting to seed node {}:{}", seed_ip, seed_port);
    let mut stream = connect_node(&seed_ip, seed_port, password)?;

    let nodes_resp = send_resp_command(&mut stream, &["CLUSTER", "NODES"])?;
    let nodes = parse_cluster_nodes(&nodes_resp);

    tracing::info!("Found {} nodes in cluster", nodes.len());

    let mut all_data: Vec<DumpKey> = Vec::new();
    let mut success_nodes = 0usize;
    let mut failed_nodes = 0usize;

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

        match connect_node(&node.ip, node.port, node_password) {
            Ok(mut node_stream) => match scan_all_keys(&mut node_stream) {
                Ok(keys) => {
                    for (key, r#type, ttl, value) in keys {
                        all_data.push(DumpKey {
                            key,
                            key_bytes: Vec::new(),
                            r#type,
                            ttl_ms: ttl,
                            value,
                        });
                    }
                    success_nodes += 1;
                }
                Err(e) => {
                    eprintln!("Failed to scan keys: {}", e);
                    failed_nodes += 1;
                }
            },
            Err(e) => {
                eprintln!("Failed to connect: {}", e);
                failed_nodes += 1;
            }
        }
    }

    // Prepare node info for dump
    let node_info: Vec<DumpNodeInfo> = nodes
        .iter()
        .map(|n| DumpNodeInfo {
            node_id: n.node_id.clone(),
            ip: n.ip.clone(),
            port: n.port,
            slots: n.slots.clone(),
        })
        .collect();

    // Write dump file
    let header = DumpHeader {
        version: DUMP_VERSION,
        nodes: node_info,
        created_at: chrono::Utc::now().to_rfc3339(),
    };

    if format == "jsonl" {
        let file = std::fs::File::create(output_path)?;
        let mut writer = std::io::BufWriter::new(file);

        // Write header as first line
        writeln!(writer, "{}", serde_json::to_string(&header)?)?;

        // Write each key
        for key in &all_data {
            writeln!(writer, "{}", serde_json::to_string(key)?)?;
        }
    } else {
        // Binary format
        let file = std::fs::File::create(output_path)?;
        let mut writer = std::io::BufWriter::new(file);

        // Magic + version
        writer.write_all(DUMP_MAGIC)?;
        writer.write_all(&DUMP_VERSION.to_le_bytes())?;

        // Nodes
        let num_nodes = (nodes.len() as u16).to_le_bytes();
        writer.write_all(&num_nodes)?;

        for node in &nodes {
            let node_id_bytes = node.node_id.as_bytes();
            writer.write_all(&(node_id_bytes.len() as u16).to_le_bytes())?;
            writer.write_all(node_id_bytes)?;

            let ip_bytes = node.ip.as_bytes();
            writer.write_all(&(ip_bytes.len() as u16).to_le_bytes())?;
            writer.write_all(ip_bytes)?;
            writer.write_all(&node.port.to_le_bytes())?;

            let num_slots = (node.slots.len() as u32).to_le_bytes();
            writer.write_all(&num_slots)?;
            for (s, e) in &node.slots {
                writer.write_all(&s.to_le_bytes())?;
                writer.write_all(&e.to_le_bytes())?;
            }
        }

        // Keys
        let num_keys = (all_data.len() as u64).to_le_bytes();
        writer.write_all(&num_keys)?;

        for key in &all_data {
            let key_bytes = key.key.as_bytes();
            writer.write_all(&(key_bytes.len() as u16).to_le_bytes())?;
            writer.write_all(key_bytes)?;

            let type_byte: u8 = match key.r#type.as_str() {
                "string" => 0,
                "list" => 1,
                "hash" => 2,
                "set" => 3,
                "zset" => 4,
                _ => 255,
            };
            writer.write_all(&[type_byte])?;

            let ttl = key.ttl_ms.unwrap_or(-1);
            writer.write_all(&ttl.to_le_bytes())?;

            // Serialize value as JSON for now
            let value_json = serde_json::to_string(&key.value).unwrap_or_default();
            let value_bytes = value_json.as_bytes();
            writer.write_all(&(value_bytes.len() as u32).to_le_bytes())?;
            writer.write_all(value_bytes)?;
        }
    }

    tracing::info!(
        "Cluster dump complete: {} keys from {} nodes",
        all_data.len(),
        success_nodes
    );
    Ok((all_data.len(), success_nodes))
}

pub fn restore_cluster(
    input_path: &Path,
    connect_addr: &str,
    password: Option<&str>,
    per_node_password: Option<&str>,
) -> Result<(usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    // Read dump file
    let content = std::fs::read_to_string(input_path)?;
    let lines: Vec<&str> = content.lines().collect();

    if lines.is_empty() {
        return Err("Empty dump file".into());
    }

    // Parse header
    let header: DumpHeader = serde_json::from_str(lines[0])?;
    tracing::info!(
        "Dump version: {}, nodes: {}",
        header.version,
        header.nodes.len()
    );

    // Build slot -> node mapping
    let mut slot_to_node: HashMap<u16, (&str, u16)> = HashMap::new();
    for node in &header.nodes {
        for (start, end) in &node.slots {
            for slot in *start..=*end {
                slot_to_node.insert(slot, (&node.ip as &str, node.port));
            }
        }
    }

    // Parse keys
    let mut keys_to_restore: Vec<(String, String, Option<i64>, serde_json::Value)> = Vec::new();
    for line in &lines[1..] {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(dump_key) = serde_json::from_str::<DumpKey>(line) {
            keys_to_restore.push((
                dump_key.key,
                dump_key.r#type,
                dump_key.ttl_ms,
                dump_key.value,
            ));
        }
    }

    tracing::info!("Restoring {} keys", keys_to_restore.len());

    let (seed_ip, seed_port) = if let Some((ip, port)) = connect_addr.rsplit_once(':') {
        (ip.to_string(), port.parse().unwrap_or(6379))
    } else {
        (
            "127.0.0.1".to_string(),
            connect_addr.parse().unwrap_or(6379),
        )
    };

    let mut stream = connect_node(&seed_ip, seed_port, password)?;

    // Get current cluster nodes
    let nodes_resp = send_resp_command(&mut stream, &["CLUSTER", "NODES"])?;
    let current_nodes = parse_cluster_nodes(&nodes_resp);

    // Build slot -> current node mapping (with owned strings)
    let mut current_slot_to_node: HashMap<u16, (String, u16)> = HashMap::new();
    for node in &current_nodes {
        for (start, end) in &node.slots {
            for slot in *start..=*end {
                current_slot_to_node.insert(slot, (node.ip.clone(), node.port));
            }
        }
    }

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    let semaphore = Arc::new(AtomicUsize::new(4));
    let mut handles = Vec::new();
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    for (key, r#type, ttl, value) in keys_to_restore {
        while semaphore.load(Ordering::Relaxed) == 0 {
            thread::yield_now();
        }
        semaphore.fetch_sub(1, Ordering::Relaxed);

        let password = password.map(|s| s.to_string());
        let slot_to_node = current_slot_to_node.clone();
        let sem = semaphore.clone();
        let success = success_count.clone();
        let errors = error_count.clone();
        let seed_ip = seed_ip.clone();
        let per_node_password = per_node_password.map(|s| s.to_string());

        let handle = thread::spawn(move || {
            // Calculate slot
            let slot = ConsistentHash::key_slot(key.as_bytes());

            // Find target node
            let target = slot_to_node.get(&slot).cloned();

            let result = if let Some((target_ip, target_port)) = target {
                let node_password = if target_ip == seed_ip && target_port == seed_port {
                    password.as_deref()
                } else {
                    per_node_password.as_deref()
                };
                match connect_node(&target_ip, target_port, node_password) {
                    Ok(mut stream) => {
                        // Restore based on type
                        let result = match r#type.as_str() {
                            "string" => {
                                if let Some(s) = value.as_str() {
                                    send_resp_command(&mut stream, &["SET", &key, s])
                                } else {
                                    Err(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "Invalid string value",
                                    ))
                                }
                            }
                            _ => {
                                let json_str = serde_json::to_string(&value).unwrap_or_default();
                                send_resp_command(&mut stream, &["SET", &key, &json_str])
                            }
                        };

                        if result.is_ok() {
                            if let Some(ttl_s) = ttl {
                                if ttl_s > 0 {
                                    let ttl_s = (ttl_s / 1000) as u64;
                                    let _ = send_resp_command(
                                        &mut stream,
                                        &["EXPIRE", &key, &ttl_s.to_string()],
                                    );
                                }
                            }
                        }
                        result
                    }
                    Err(e) => Err(e),
                }
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("No node found for slot {}", slot),
                ))
            };

            sem.fetch_add(1, Ordering::Relaxed);

            match result {
                Ok(_) => success.fetch_add(1, Ordering::Relaxed),
                Err(_) => errors.fetch_add(1, Ordering::Relaxed),
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().ok();
    }

    let final_success = success_count.load(Ordering::Relaxed);
    let final_errors = error_count.load(Ordering::Relaxed);

    tracing::info!(
        "Restore complete: {} success, {} errors",
        final_success,
        final_errors
    );
    Ok((final_success, final_errors))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cluster_nodes_single_master() {
        let output = "abc123 127.0.0.1:6379@16379 myself,master - 0 0 0-16383";
        let nodes = parse_cluster_nodes(output);

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_id, "abc123");
        assert_eq!(nodes[0].ip.contains("127.0.0.1"), true);
        assert!(nodes[0].port > 0);
    }

    #[test]
    fn test_parse_cluster_nodes_with_at_sign() {
        let output =
            "f85ba0f344c07497-0000000000003ffb 127.0.0.1@16379 myself,master - 9999 9999 0-16383";
        let nodes = parse_cluster_nodes(output);

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].ip, "127.0.0.1");
        assert_eq!(nodes[0].port, 16379);
    }

    #[test]
    fn test_parse_cluster_nodes_multiple_slots() {
        let output = "abc123 127.0.0.1:6379@16379 myself,master - 0 0 0-5460,5461-10922";
        let nodes = parse_cluster_nodes(output);

        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].slots.len() >= 1);
    }

    #[test]
    fn test_parse_cluster_nodes_single_slot() {
        let output = "abc123 127.0.0.1:6379@16379 master - 0 0 500";
        let nodes = parse_cluster_nodes(output);

        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].slots.len() >= 1);
    }

    #[test]
    fn test_parse_cluster_nodes_multiple_nodes() {
        let output = "node1 127.0.0.1:6379@16379 master - 0 0 0-5460\nnode2 127.0.0.1:6380@16380 master - 0 0 5461-10922\nnode3 127.0.0.1:6381@16381 master - 0 0 10923-16383";
        let nodes = parse_cluster_nodes(output);

        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn test_parse_cluster_nodes_ignores_empty_lines() {
        let output = "abc123 127.0.0.1:6379@16379 myself,master - 0 0 0-16383\n\nnode2 127.0.0.1:6380@16380 master - 0 0 5461-16383";
        let nodes = parse_cluster_nodes(output);

        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_parse_type_response_simple_string() {
        let resp = "+string\r\n";
        let result = parse_type_response(resp);
        assert_eq!(result, "string");
    }

    #[test]
    fn test_parse_type_response_list() {
        let resp = "+list\r\n";
        let result = parse_type_response(resp);
        assert_eq!(result, "list");
    }

    #[test]
    fn test_parse_type_response_hash() {
        let resp = "+hash\r\n";
        let result = parse_type_response(resp);
        assert_eq!(result, "hash");
    }

    #[test]
    fn test_parse_type_response_with_whitespace() {
        let resp = "+string  \r\n";
        let result = parse_type_response(resp);
        assert_eq!(result, "string");
    }

    #[test]
    fn test_parse_type_response_invalid() {
        let resp = "invalid\r\n";
        let result = parse_type_response(resp);
        assert_eq!(result, "none");
    }

    #[test]
    fn test_parse_ttl_response_no_expiry() {
        let resp = ":-1\r\n";
        let result = parse_ttl_response(resp);
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_parse_ttl_response_has_expiry() {
        let resp = ":3600\r\n";
        let result = parse_ttl_response(resp);
        assert_eq!(result, Some(3600000));
    }

    #[test]
    fn test_parse_ttl_response_no_key() {
        let resp = ":-2\r\n";
        let result = parse_ttl_response(resp);
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_parse_string_value_simple() {
        let resp = "$9\r\ntestvalue\r\n";
        let result = parse_string_value(resp);
        assert_eq!(result, serde_json::Value::String("testvalue".to_string()));
    }

    #[test]
    fn test_parse_string_value_empty() {
        let resp = "$0\r\n\r\n";
        let result = parse_string_value(resp);
        assert_eq!(result, serde_json::Value::Null);
    }

    #[test]
    fn test_parse_string_value_with_newlines() {
        let resp = "$11\r\nhello\r\nworl\r\n";
        let result = parse_string_value(resp);
        assert_eq!(
            result,
            serde_json::Value::String("hello\r\nworl".to_string())
        );
    }

    #[test]
    fn test_parse_string_value_null() {
        let resp = "+OK\r\n";
        let result = parse_string_value(resp);
        assert_eq!(result, serde_json::Value::Null);
    }

    #[test]
    fn test_parse_list_value() {
        let resp = "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
        let result = parse_list_value(resp);
        let arr = result.as_array().unwrap();
        assert!(arr.len() >= 1);
    }

    #[test]
    fn test_parse_list_value_empty() {
        let resp = "*0\r\n";
        let result = parse_list_value(resp);
        assert_eq!(result, serde_json::Value::Array(vec![]));
    }

    #[test]
    fn test_parse_set_value() {
        let resp = "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n";
        let result = parse_set_value(resp);
        let arr = result.as_array().unwrap();
        assert!(arr.len() >= 1);
    }

    #[test]
    fn test_parse_hash_value() {
        let resp = "*4\r\n$5\r\nfield1\r\n$6\r\nvalue1\r\n$5\r\nfield2\r\n$6\r\nvalue2\r\n";
        let result = parse_hash_value(resp);
        match result {
            serde_json::Value::Object(map) => {
                assert!(map.len() >= 1);
            }
            _ => panic!("Expected object"),
        }
    }

    #[test]
    fn test_parse_zset_value() {
        let resp = "*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n";
        let result = parse_zset_value(resp);
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);
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
        };

        let json = serde_json::to_string(&key).unwrap();
        let parsed: DumpKey = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.key, "testkey");
        assert_eq!(parsed.ttl_ms, Some(3600000));
    }
}
