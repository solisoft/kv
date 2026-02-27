//! Redis RDB file importer for SoliKV.
//!
//! Parses standard Redis `dump.rdb` files and converts all supported data types
//! into SoliKV's `RedisValue` types, distributing keys to shard buckets.

pub mod intset;
pub mod listpack;
pub mod lzf;
pub mod parser;
pub mod ziplist;

use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::Path;

use solikv_core::keyspace::hash_slot;
use solikv_core::types::*;

use parser::RdbReader;

// Redis RDB opcodes
const OP_AUX: u8 = 0xFA;
const OP_RESIZEDB: u8 = 0xFB;
const OP_EXPIRETIME_MS: u8 = 0xFC;
const OP_EXPIRETIME: u8 = 0xFD;
const OP_SELECTDB: u8 = 0xFE;
const OP_EOF: u8 = 0xFF;
const OP_IDLE: u8 = 0xF8;
const OP_FREQ: u8 = 0xF9;

// Redis value type codes
const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
const TYPE_ZSET: u8 = 3;
const TYPE_HASH: u8 = 4;
const TYPE_ZSET_2: u8 = 5;
const TYPE_MODULE_OLD: u8 = 6;
const TYPE_MODULE_2: u8 = 7;
const TYPE_HASH_ZIPMAP: u8 = 9;
const TYPE_LIST_ZIPLIST: u8 = 10;
const TYPE_SET_INTSET: u8 = 11;
const TYPE_ZSET_ZIPLIST: u8 = 12;
const TYPE_HASH_ZIPLIST: u8 = 13;
const TYPE_LIST_QUICKLIST: u8 = 14;
const TYPE_STREAM_LISTPACKS: u8 = 15;
const TYPE_HASH_LISTPACK: u8 = 16;
const TYPE_ZSET_LISTPACK: u8 = 17;
const TYPE_LIST_QUICKLIST_2: u8 = 18;
const TYPE_STREAM_LISTPACKS_2: u8 = 19;
const TYPE_SET_LISTPACK: u8 = 20;
const TYPE_STREAM_LISTPACKS_3: u8 = 21;
const TYPE_HASH_FIELD_EXPIRY_LISTPACK: u8 = 22;
const TYPE_HASH_FIELD_EXPIRY_HT: u8 = 23;
const TYPE_HASH_FIELD_EXPIRY_LISTPACK_EX: u8 = 24;
const TYPE_HASH_FIELD_EXPIRY_HT_EX: u8 = 25;
const TYPE_STREAM_LISTPACKS_4: u8 = 26;

/// Statistics from a Redis RDB import.
#[derive(Debug, Default)]
pub struct ImportStats {
    pub rdb_version: u32,
    pub keys_imported: usize,
    pub keys_skipped: usize,
    pub keys_expired: usize,
    pub databases_seen: usize,
}

impl std::fmt::Display for ImportStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RDB v{}: {} keys imported, {} skipped (unsupported type), {} expired, {} database(s)",
            self.rdb_version, self.keys_imported, self.keys_skipped, self.keys_expired, self.databases_seen
        )
    }
}

/// Import a Redis RDB file, returning key-entry pairs bucketed by shard index.
///
/// Keys from all Redis databases are flattened into a single namespace.
/// Already-expired keys are filtered out.
pub fn import_redis_rdb(
    path: &Path,
    num_shards: usize,
) -> io::Result<(Vec<Vec<(Bytes, KeyEntry)>>, ImportStats)> {
    let file = std::fs::File::open(path)?;
    let mut reader = RdbReader::new(file);

    let version = reader.read_header()?;
    tracing::info!("Parsing Redis RDB version {version}");

    let mut stats = ImportStats {
        rdb_version: version,
        ..Default::default()
    };

    let mut shards: Vec<Vec<(Bytes, KeyEntry)>> = (0..num_shards).map(|_| Vec::new()).collect();
    let mut current_expiry_ms: Option<u64> = None;
    let mut current_db: i64 = -1;
    let now = now_millis();

    loop {
        let opcode = reader.read_u8()?;

        match opcode {
            OP_EOF => {
                // Optionally skip 8-byte CRC64 checksum (RDB version >= 5)
                if version >= 5 {
                    let _ = reader.skip(8);
                }
                break;
            }
            OP_AUX => {
                let key = reader.read_string()?;
                let val = reader.read_string()?;
                tracing::debug!(
                    "RDB AUX: {}={}",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&val)
                );
            }
            OP_SELECTDB => {
                let db = reader.read_length()?;
                if current_db != -1 && db as i64 != current_db {
                    tracing::info!("RDB: switching to database {db} (all databases flattened into single namespace)");
                }
                current_db = db as i64;
                stats.databases_seen += 1;
            }
            OP_RESIZEDB => {
                let _db_size = reader.read_length()?;
                let _expires_size = reader.read_length()?;
            }
            OP_EXPIRETIME_MS => {
                let mut buf = [0u8; 8];
                reader.read_exact(&mut buf)?;
                current_expiry_ms = Some(u64::from_le_bytes(buf));
            }
            OP_EXPIRETIME => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                let secs = u32::from_le_bytes(buf) as u64;
                current_expiry_ms = Some(secs * 1000);
            }
            OP_IDLE => {
                // LRU idle time — just skip the length-encoded value
                let _ = reader.read_length()?;
            }
            OP_FREQ => {
                // LFU frequency — single byte
                let _ = reader.read_u8()?;
            }
            type_code => {
                // This is a value type code — read key then value
                let key = reader.read_string()?;
                let expiry = current_expiry_ms.take();

                // Filter already-expired keys
                if let Some(exp_ms) = expiry {
                    if exp_ms <= now {
                        stats.keys_expired += 1;
                        skip_value(&mut reader, type_code)?;
                        continue;
                    }
                }

                match read_value(&mut reader, type_code) {
                    Ok(Some(value)) => {
                        let entry = match expiry {
                            Some(exp) => KeyEntry::with_expiry(value, exp),
                            None => KeyEntry::new(value),
                        };

                        let shard_idx = hash_slot(&key) as usize % num_shards;
                        shards[shard_idx].push((key, entry));
                        stats.keys_imported += 1;
                    }
                    Ok(None) => {
                        // Unsupported type, already skipped in read_value
                        stats.keys_skipped += 1;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to read key '{}' (type {type_code}) at offset {}: {e}",
                            String::from_utf8_lossy(&key),
                            reader.position()
                        );
                        return Err(e);
                    }
                }
            }
        }
    }

    Ok((shards, stats))
}

/// Read a Redis value by type code. Returns `Ok(None)` for unsupported types (with a warning logged).
fn read_value<R: io::Read>(reader: &mut RdbReader<R>, type_code: u8) -> io::Result<Option<RedisValue>> {
    match type_code {
        TYPE_STRING => {
            let val = reader.read_string()?;
            Ok(Some(RedisValue::String(val)))
        }
        TYPE_LIST => {
            let len = reader.read_length()?;
            let mut list = VecDeque::with_capacity(len);
            for _ in 0..len {
                list.push_back(reader.read_string()?);
            }
            Ok(Some(RedisValue::List(list)))
        }
        TYPE_SET => {
            let len = reader.read_length()?;
            let mut set = HashSet::with_capacity(len);
            for _ in 0..len {
                set.insert(reader.read_string()?);
            }
            Ok(Some(RedisValue::Set(set)))
        }
        TYPE_ZSET => {
            // Scores as ASCII strings
            let len = reader.read_length()?;
            let mut zset = ZSetValue::new();
            for _ in 0..len {
                let member = reader.read_string()?;
                let score = reader.read_string_f64()?;
                zset.insert(score, member);
            }
            Ok(Some(RedisValue::ZSet(zset)))
        }
        TYPE_HASH => {
            let len = reader.read_length()?;
            let mut hash = HashMap::with_capacity(len);
            for _ in 0..len {
                let field = reader.read_string()?;
                let value = reader.read_string()?;
                hash.insert(field, value);
            }
            Ok(Some(RedisValue::Hash(hash)))
        }
        TYPE_ZSET_2 => {
            // Scores as binary f64
            let len = reader.read_length()?;
            let mut zset = ZSetValue::new();
            for _ in 0..len {
                let member = reader.read_string()?;
                let score = reader.read_binary_f64()?;
                zset.insert(score, member);
            }
            Ok(Some(RedisValue::ZSet(zset)))
        }
        TYPE_LIST_ZIPLIST => {
            let blob = reader.read_string_raw()?;
            let entries = ziplist::decode_ziplist(&blob)?;
            Ok(Some(RedisValue::List(VecDeque::from(entries))))
        }
        TYPE_SET_INTSET => {
            let blob = reader.read_string_raw()?;
            let members = intset::decode_intset(&blob)?;
            let set: HashSet<Bytes> = members.into_iter().collect();
            Ok(Some(RedisValue::Set(set)))
        }
        TYPE_ZSET_ZIPLIST => {
            let blob = reader.read_string_raw()?;
            let entries = ziplist::decode_ziplist(&blob)?;
            if entries.len() % 2 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ZSET_ZIPLIST: odd number of entries",
                ));
            }
            let mut zset = ZSetValue::new();
            for pair in entries.chunks_exact(2) {
                let member = pair[0].clone();
                let score = parse_score_bytes(&pair[1])?;
                zset.insert(score, member);
            }
            Ok(Some(RedisValue::ZSet(zset)))
        }
        TYPE_HASH_ZIPLIST => {
            let blob = reader.read_string_raw()?;
            let entries = ziplist::decode_ziplist(&blob)?;
            if entries.len() % 2 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HASH_ZIPLIST: odd number of entries",
                ));
            }
            let mut hash = HashMap::with_capacity(entries.len() / 2);
            for pair in entries.chunks_exact(2) {
                hash.insert(pair[0].clone(), pair[1].clone());
            }
            Ok(Some(RedisValue::Hash(hash)))
        }
        TYPE_LIST_QUICKLIST => {
            let count = reader.read_length()?;
            let mut list = VecDeque::new();
            for _ in 0..count {
                let blob = reader.read_string_raw()?;
                let entries = ziplist::decode_ziplist(&blob)?;
                for entry in entries {
                    list.push_back(entry);
                }
            }
            Ok(Some(RedisValue::List(list)))
        }
        TYPE_HASH_LISTPACK => {
            let blob = reader.read_string_raw()?;
            let entries = listpack::decode_listpack(&blob)?;
            if entries.len() % 2 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HASH_LISTPACK: odd number of entries",
                ));
            }
            let mut hash = HashMap::with_capacity(entries.len() / 2);
            for pair in entries.chunks_exact(2) {
                hash.insert(pair[0].clone(), pair[1].clone());
            }
            Ok(Some(RedisValue::Hash(hash)))
        }
        TYPE_ZSET_LISTPACK => {
            let blob = reader.read_string_raw()?;
            let entries = listpack::decode_listpack(&blob)?;
            if entries.len() % 2 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ZSET_LISTPACK: odd number of entries",
                ));
            }
            let mut zset = ZSetValue::new();
            for pair in entries.chunks_exact(2) {
                let member = pair[0].clone();
                let score = parse_score_bytes(&pair[1])?;
                zset.insert(score, member);
            }
            Ok(Some(RedisValue::ZSet(zset)))
        }
        TYPE_LIST_QUICKLIST_2 => {
            let count = reader.read_length()?;
            let mut list = VecDeque::new();
            for _ in 0..count {
                let container = reader.read_length()?;
                let blob = reader.read_string_raw()?;
                match container {
                    1 => {
                        // PLAIN: raw string element
                        list.push_back(Bytes::from(blob));
                    }
                    2 => {
                        // PACKED: listpack encoded
                        let entries = listpack::decode_listpack(&blob)?;
                        for entry in entries {
                            list.push_back(entry);
                        }
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("LIST_QUICKLIST_2: unknown container type {container}"),
                        ));
                    }
                }
            }
            Ok(Some(RedisValue::List(list)))
        }
        TYPE_SET_LISTPACK => {
            let blob = reader.read_string_raw()?;
            let entries = listpack::decode_listpack(&blob)?;
            let set: HashSet<Bytes> = entries.into_iter().collect();
            Ok(Some(RedisValue::Set(set)))
        }
        // Unsupported types — skip and warn
        TYPE_MODULE_OLD | TYPE_MODULE_2 | TYPE_HASH_ZIPMAP
        | TYPE_STREAM_LISTPACKS | TYPE_STREAM_LISTPACKS_2
        | TYPE_STREAM_LISTPACKS_3 | TYPE_STREAM_LISTPACKS_4
        | TYPE_HASH_FIELD_EXPIRY_LISTPACK | TYPE_HASH_FIELD_EXPIRY_HT
        | TYPE_HASH_FIELD_EXPIRY_LISTPACK_EX | TYPE_HASH_FIELD_EXPIRY_HT_EX => {
            tracing::warn!(
                "Skipping unsupported Redis type {} ({})",
                type_code,
                type_name(type_code)
            );
            skip_value(reader, type_code)?;
            Ok(None)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown Redis value type code: {type_code}"),
        )),
    }
}

/// Skip over the value data for a given type code without converting it.
fn skip_value<R: io::Read>(reader: &mut RdbReader<R>, type_code: u8) -> io::Result<()> {
    match type_code {
        TYPE_STRING => {
            reader.read_string_raw()?;
        }
        TYPE_LIST => {
            let len = reader.read_length()?;
            for _ in 0..len {
                reader.read_string_raw()?;
            }
        }
        TYPE_SET => {
            let len = reader.read_length()?;
            for _ in 0..len {
                reader.read_string_raw()?;
            }
        }
        TYPE_ZSET => {
            let len = reader.read_length()?;
            for _ in 0..len {
                reader.read_string_raw()?;
                reader.read_string_f64()?;
            }
        }
        TYPE_HASH => {
            let len = reader.read_length()?;
            for _ in 0..len {
                reader.read_string_raw()?;
                reader.read_string_raw()?;
            }
        }
        TYPE_ZSET_2 => {
            let len = reader.read_length()?;
            for _ in 0..len {
                reader.read_string_raw()?;
                reader.read_binary_f64()?;
            }
        }
        // Single-blob types
        TYPE_LIST_ZIPLIST | TYPE_SET_INTSET | TYPE_ZSET_ZIPLIST | TYPE_HASH_ZIPLIST
        | TYPE_HASH_LISTPACK | TYPE_ZSET_LISTPACK | TYPE_SET_LISTPACK | TYPE_HASH_ZIPMAP => {
            reader.read_string_raw()?;
        }
        TYPE_LIST_QUICKLIST => {
            let count = reader.read_length()?;
            for _ in 0..count {
                reader.read_string_raw()?;
            }
        }
        TYPE_LIST_QUICKLIST_2 => {
            let count = reader.read_length()?;
            for _ in 0..count {
                let _container = reader.read_length()?;
                reader.read_string_raw()?;
            }
        }
        // Streams — skip the complex structure
        TYPE_STREAM_LISTPACKS | TYPE_STREAM_LISTPACKS_2
        | TYPE_STREAM_LISTPACKS_3 | TYPE_STREAM_LISTPACKS_4 => {
            skip_stream(reader, type_code)?;
        }
        // Module — cannot reliably skip MODULE_OLD, can skip MODULE_2 via opcode loop
        TYPE_MODULE_2 => {
            skip_module_2(reader)?;
        }
        TYPE_MODULE_OLD => {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "MODULE_OLD (type 6) cannot be skipped — the data format is unknown",
            ));
        }
        // Hash with field expiry
        TYPE_HASH_FIELD_EXPIRY_LISTPACK | TYPE_HASH_FIELD_EXPIRY_LISTPACK_EX => {
            // Single listpack blob + minimum-expiry (u64 LE)
            reader.read_string_raw()?;
            reader.skip(8)?;
        }
        TYPE_HASH_FIELD_EXPIRY_HT | TYPE_HASH_FIELD_EXPIRY_HT_EX => {
            // length + (field string + value string + expiry_ms u64)* + minimum-expiry (u64 LE)
            let len = reader.read_length()?;
            for _ in 0..len {
                reader.read_string_raw()?; // field
                reader.read_string_raw()?; // value
                reader.skip(8)?; // field expiry timestamp
            }
            reader.skip(8)?; // minimum expiry
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("cannot skip unknown type {type_code}"),
            ));
        }
    }
    Ok(())
}

/// Skip a Redis stream structure.
fn skip_stream<R: io::Read>(reader: &mut RdbReader<R>, type_code: u8) -> io::Result<()> {
    // Listpack entries: count + (master_key string + listpack string) * count
    let listpacks_count = reader.read_length()?;
    for _ in 0..listpacks_count {
        reader.read_string_raw()?; // master key
        reader.read_string_raw()?; // listpack
    }

    // Stream metadata
    let _items = reader.read_length()?; // total items
    let _last_id_ms = reader.read_length()?; // last entry ID ms
    let _last_id_seq = reader.read_length()?; // last entry ID seq

    if type_code >= TYPE_STREAM_LISTPACKS_2 {
        // first entry ID
        let _first_id_ms = reader.read_length()?;
        let _first_id_seq = reader.read_length()?;
        // max deleted entry ID
        let _max_del_ms = reader.read_length()?;
        let _max_del_seq = reader.read_length()?;
        // entries_added
        let _entries_added = reader.read_length()?;
    }

    // Consumer groups
    let cgroups = reader.read_length()?;
    for _ in 0..cgroups {
        reader.read_string_raw()?; // group name
        let _last_id_ms = reader.read_length()?;
        let _last_id_seq = reader.read_length()?;

        if type_code >= TYPE_STREAM_LISTPACKS_2 {
            let _entries_read = reader.read_length()?;
        }

        // Group PEL (pending entries list)
        let pel_count = reader.read_length()?;
        for _ in 0..pel_count {
            reader.skip(16)?; // 128-bit stream ID
            reader.skip(8)?; // delivery time
            let _delivery_count = reader.read_length()?;
        }

        // Consumers
        let consumers_count = reader.read_length()?;
        for _ in 0..consumers_count {
            reader.read_string_raw()?; // consumer name
            reader.skip(8)?; // seen-time

            if type_code >= TYPE_STREAM_LISTPACKS_3 {
                reader.skip(8)?; // active-time
            }

            // Consumer PEL (references into group PEL)
            let consumer_pel = reader.read_length()?;
            for _ in 0..consumer_pel {
                reader.skip(16)?; // 128-bit stream ID
            }
        }
    }

    Ok(())
}

/// Skip a MODULE_2 value using its opcode-based format.
fn skip_module_2<R: io::Read>(reader: &mut RdbReader<R>) -> io::Result<()> {
    let _module_id = reader.read_length()?;

    loop {
        let opcode = reader.read_length()?;
        match opcode {
            0 => break, // EOF
            1 | 2 => {
                // Signed/unsigned int
                reader.read_length()?;
            }
            3 | 4 => {
                // Float / Double
                reader.skip(8)?;
            }
            5 => {
                // String
                reader.read_string_raw()?;
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("MODULE_2: unknown opcode {opcode}"),
                ));
            }
        }
    }
    Ok(())
}

/// Parse a score from bytes (decimal ASCII string).
fn parse_score_bytes(b: &Bytes) -> io::Result<f64> {
    let s = std::str::from_utf8(b)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "score is not valid UTF-8"))?;
    s.parse::<f64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid score value: '{s}'"),
        )
    })
}

/// Human-readable name for a Redis type code.
fn type_name(code: u8) -> &'static str {
    match code {
        0 => "STRING",
        1 => "LIST",
        2 => "SET",
        3 => "ZSET",
        4 => "HASH",
        5 => "ZSET_2",
        6 => "MODULE_OLD",
        7 => "MODULE_2",
        9 => "HASH_ZIPMAP",
        10 => "LIST_ZIPLIST",
        11 => "SET_INTSET",
        12 => "ZSET_ZIPLIST",
        13 => "HASH_ZIPLIST",
        14 => "LIST_QUICKLIST",
        15 => "STREAM_LISTPACKS",
        16 => "HASH_LISTPACK",
        17 => "ZSET_LISTPACK",
        18 => "LIST_QUICKLIST_2",
        19 => "STREAM_LISTPACKS_2",
        20 => "SET_LISTPACK",
        21 => "STREAM_LISTPACKS_3",
        22 => "HASH_FIELD_EXPIRY_LISTPACK",
        23 => "HASH_FIELD_EXPIRY_HT",
        24 => "HASH_FIELD_EXPIRY_LISTPACK_EX",
        25 => "HASH_FIELD_EXPIRY_HT_EX",
        26 => "STREAM_LISTPACKS_4",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal Redis RDB file in memory with the given entries.
    fn build_rdb(entries: &[RdbEntry]) -> Vec<u8> {
        let mut buf = Vec::new();

        // Header: "REDIS0009"
        buf.extend_from_slice(b"REDIS0009");

        // SELECTDB 0
        buf.push(OP_SELECTDB);
        buf.push(0); // db 0 as 6-bit length

        for entry in entries {
            // Optional expiry
            if let Some(exp_ms) = entry.expiry_ms {
                buf.push(OP_EXPIRETIME_MS);
                buf.extend_from_slice(&exp_ms.to_le_bytes());
            }

            // Type + key + value
            buf.push(entry.type_code);
            write_rdb_string(&mut buf, &entry.key);

            match entry.type_code {
                TYPE_STRING => {
                    write_rdb_string(&mut buf, entry.string_val.as_deref().unwrap());
                }
                TYPE_LIST => {
                    let items = entry.list_val.as_ref().unwrap();
                    write_rdb_length(&mut buf, items.len());
                    for item in items {
                        write_rdb_string(&mut buf, item);
                    }
                }
                TYPE_SET => {
                    let items = entry.list_val.as_ref().unwrap();
                    write_rdb_length(&mut buf, items.len());
                    for item in items {
                        write_rdb_string(&mut buf, item);
                    }
                }
                TYPE_HASH => {
                    let pairs = entry.hash_val.as_ref().unwrap();
                    write_rdb_length(&mut buf, pairs.len());
                    for (f, v) in pairs {
                        write_rdb_string(&mut buf, f);
                        write_rdb_string(&mut buf, v);
                    }
                }
                TYPE_ZSET_2 => {
                    let members = entry.zset_val.as_ref().unwrap();
                    write_rdb_length(&mut buf, members.len());
                    for (score, member) in members {
                        write_rdb_string(&mut buf, member);
                        buf.extend_from_slice(&score.to_le_bytes());
                    }
                }
                _ => {}
            }
        }

        // EOF + 8 bytes CRC (zeroed for test)
        buf.push(OP_EOF);
        buf.extend_from_slice(&[0u8; 8]);

        buf
    }

    struct RdbEntry<'a> {
        type_code: u8,
        key: &'a [u8],
        expiry_ms: Option<u64>,
        string_val: Option<&'a [u8]>,
        list_val: Option<Vec<&'a [u8]>>,
        hash_val: Option<Vec<(&'a [u8], &'a [u8])>>,
        zset_val: Option<Vec<(f64, &'a [u8])>>,
    }

    fn write_rdb_string(buf: &mut Vec<u8>, data: &[u8]) {
        write_rdb_length(buf, data.len());
        buf.extend_from_slice(data);
    }

    fn write_rdb_length(buf: &mut Vec<u8>, len: usize) {
        if len < 64 {
            buf.push(len as u8); // 00xxxxxx
        } else if len < 16384 {
            buf.push(0x40 | ((len >> 8) as u8)); // 01xxxxxx
            buf.push((len & 0xFF) as u8);
        } else {
            buf.push(0x80); // 10_000000 = 32-bit BE
            buf.extend_from_slice(&(len as u32).to_be_bytes());
        }
    }

    #[test]
    fn test_import_string() {
        let rdb = build_rdb(&[RdbEntry {
            type_code: TYPE_STRING,
            key: b"hello",
            expiry_ms: None,
            string_val: Some(b"world"),
            list_val: None,
            hash_val: None,
            zset_val: None,
        }]);

        let mut reader = RdbReader::new(Cursor::new(rdb));
        let version = reader.read_header().unwrap();
        assert_eq!(version, 9);
    }

    #[test]
    fn test_import_full() {
        let rdb = build_rdb(&[
            RdbEntry {
                type_code: TYPE_STRING,
                key: b"key1",
                expiry_ms: None,
                string_val: Some(b"val1"),
                list_val: None,
                hash_val: None,
                zset_val: None,
            },
            RdbEntry {
                type_code: TYPE_LIST,
                key: b"mylist",
                expiry_ms: None,
                string_val: None,
                list_val: Some(vec![b"a", b"b", b"c"]),
                hash_val: None,
                zset_val: None,
            },
            RdbEntry {
                type_code: TYPE_SET,
                key: b"myset",
                expiry_ms: None,
                string_val: None,
                list_val: Some(vec![b"x", b"y"]),
                hash_val: None,
                zset_val: None,
            },
            RdbEntry {
                type_code: TYPE_HASH,
                key: b"myhash",
                expiry_ms: None,
                string_val: None,
                list_val: None,
                hash_val: Some(vec![(b"f1", b"v1"), (b"f2", b"v2")]),
                zset_val: None,
            },
            RdbEntry {
                type_code: TYPE_ZSET_2,
                key: b"myzset",
                expiry_ms: None,
                string_val: None,
                list_val: None,
                hash_val: None,
                zset_val: Some(vec![(1.0, b"a"), (2.5, b"b")]),
            },
        ]);

        // Write to temp file
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.rdb");
        std::fs::write(&path, &rdb).unwrap();

        let (shards, stats) = import_redis_rdb(&path, 4).unwrap();
        assert_eq!(stats.rdb_version, 9);
        assert_eq!(stats.keys_imported, 5);
        assert_eq!(stats.keys_skipped, 0);
        assert_eq!(stats.keys_expired, 0);

        // Collect all keys
        let all_keys: Vec<&Bytes> = shards.iter().flat_map(|s| s.iter().map(|(k, _)| k)).collect();
        assert_eq!(all_keys.len(), 5);
    }

    #[test]
    fn test_import_expired_key_filtered() {
        let rdb = build_rdb(&[
            RdbEntry {
                type_code: TYPE_STRING,
                key: b"alive",
                expiry_ms: None,
                string_val: Some(b"yes"),
                list_val: None,
                hash_val: None,
                zset_val: None,
            },
            RdbEntry {
                type_code: TYPE_STRING,
                key: b"dead",
                expiry_ms: Some(1), // expired (epoch + 1ms)
                string_val: Some(b"no"),
                list_val: None,
                hash_val: None,
                zset_val: None,
            },
        ]);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.rdb");
        std::fs::write(&path, &rdb).unwrap();

        let (shards, stats) = import_redis_rdb(&path, 2).unwrap();
        assert_eq!(stats.keys_imported, 1);
        assert_eq!(stats.keys_expired, 1);

        let all_keys: Vec<&Bytes> = shards.iter().flat_map(|s| s.iter().map(|(k, _)| k)).collect();
        assert_eq!(all_keys.len(), 1);
        assert_eq!(all_keys[0], &Bytes::from("alive"));
    }

    #[test]
    fn test_import_with_future_expiry() {
        let future_ms = now_millis() + 3_600_000; // 1 hour from now
        let rdb = build_rdb(&[RdbEntry {
            type_code: TYPE_STRING,
            key: b"ttlkey",
            expiry_ms: Some(future_ms),
            string_val: Some(b"val"),
            list_val: None,
            hash_val: None,
            zset_val: None,
        }]);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.rdb");
        std::fs::write(&path, &rdb).unwrap();

        let (shards, stats) = import_redis_rdb(&path, 1).unwrap();
        assert_eq!(stats.keys_imported, 1);
        assert_eq!(stats.keys_expired, 0);

        let entry = &shards[0][0];
        assert_eq!(entry.0, Bytes::from("ttlkey"));
        assert_eq!(entry.1.expires_at, Some(future_ms));
    }

    #[test]
    fn test_import_shard_distribution() {
        // Create many keys and verify they distribute across shards
        let mut entries = Vec::new();
        for i in 0..100 {
            entries.push(RdbEntry {
                type_code: TYPE_STRING,
                key: format!("key:{i}").as_bytes().to_vec().leak(),
                expiry_ms: None,
                string_val: Some(b"v"),
                list_val: None,
                hash_val: None,
                zset_val: None,
            });
        }

        let rdb = build_rdb(&entries);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.rdb");
        std::fs::write(&path, &rdb).unwrap();

        let (shards, stats) = import_redis_rdb(&path, 4).unwrap();
        assert_eq!(stats.keys_imported, 100);

        // Each shard should have some keys (statistical guarantee with 100 keys / 4 shards)
        for (i, shard) in shards.iter().enumerate() {
            assert!(!shard.is_empty(), "shard {i} should have keys");
        }
    }
}
