use bytes::Bytes;
use solikv_core::types::*;
use solikv_core::ShardStore;
use std::collections::VecDeque;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const RDB_MAGIC: &[u8] = b"SOLIKV";
const RDB_VERSION: u8 = 1;

// Type markers
const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_HASH: u8 = 2;
const TYPE_SET: u8 = 3;
const TYPE_ZSET: u8 = 4;
const ENTRY_END: u8 = 0xFF;

pub struct RdbPersistence;

impl RdbPersistence {
    /// Save a shard store to a writer in RDB format.
    pub fn save<W: Write>(store: &ShardStore, writer: &mut W) -> io::Result<()> {
        // Header
        writer.write_all(RDB_MAGIC)?;
        writer.write_all(&[RDB_VERSION])?;

        for (key, entry) in store.iter() {
            if entry.is_expired() {
                continue;
            }

            // Write expiry if present
            if let Some(exp) = entry.expires_at {
                writer.write_all(&[0xFC])?; // expiry marker
                writer.write_all(&exp.to_le_bytes())?;
            }

            // Write value type + key + value
            match &entry.value {
                RedisValue::String(v) => {
                    writer.write_all(&[TYPE_STRING])?;
                    write_bytes(writer, key)?;
                    write_bytes(writer, v)?;
                }
                RedisValue::List(list) => {
                    writer.write_all(&[TYPE_LIST])?;
                    write_bytes(writer, key)?;
                    write_len(writer, list.len())?;
                    for item in list {
                        write_bytes(writer, item)?;
                    }
                }
                RedisValue::Hash(hash) => {
                    writer.write_all(&[TYPE_HASH])?;
                    write_bytes(writer, key)?;
                    write_len(writer, hash.len())?;
                    for (field, value) in hash {
                        write_bytes(writer, field)?;
                        write_bytes(writer, value)?;
                    }
                }
                RedisValue::Set(set) => {
                    writer.write_all(&[TYPE_SET])?;
                    write_bytes(writer, key)?;
                    write_len(writer, set.len())?;
                    for member in set {
                        write_bytes(writer, member)?;
                    }
                }
                RedisValue::ZSet(zset) => {
                    writer.write_all(&[TYPE_ZSET])?;
                    write_bytes(writer, key)?;
                    write_len(writer, zset.len())?;
                    for ((score, member), _) in &zset.scores {
                        write_bytes(writer, member)?;
                        writer.write_all(&score.into_inner().to_le_bytes())?;
                    }
                }
            }
        }

        writer.write_all(&[ENTRY_END])?;
        Ok(())
    }

    /// Load a shard store from a reader.
    pub fn load<R: Read>(reader: &mut R, store: &mut ShardStore) -> io::Result<()> {
        let mut magic = [0u8; 6];
        reader.read_exact(&mut magic)?;
        if &magic != RDB_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid RDB magic"));
        }
        let mut version = [0u8; 1];
        reader.read_exact(&mut version)?;
        if version[0] != RDB_VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "unsupported RDB version"));
        }

        let mut current_expiry: Option<u64> = None;

        loop {
            let mut type_byte = [0u8; 1];
            if reader.read_exact(&mut type_byte).is_err() {
                break;
            }

            match type_byte[0] {
                ENTRY_END => break,
                0xFC => {
                    // Expiry in milliseconds
                    let mut buf = [0u8; 8];
                    reader.read_exact(&mut buf)?;
                    current_expiry = Some(u64::from_le_bytes(buf));
                    continue;
                }
                TYPE_STRING => {
                    let key = read_bytes(reader)?;
                    let value = read_bytes(reader)?;
                    let entry = make_entry(RedisValue::String(value), current_expiry.take());
                    store.insert_entry(key, entry);
                }
                TYPE_LIST => {
                    let key = read_bytes(reader)?;
                    let len = read_len(reader)?;
                    let mut list = VecDeque::with_capacity(len);
                    for _ in 0..len {
                        list.push_back(read_bytes(reader)?);
                    }
                    let entry = make_entry(RedisValue::List(list), current_expiry.take());
                    store.insert_entry(key, entry);
                }
                TYPE_HASH => {
                    let key = read_bytes(reader)?;
                    let len = read_len(reader)?;
                    let mut hash = std::collections::HashMap::with_capacity(len);
                    for _ in 0..len {
                        let field = read_bytes(reader)?;
                        let value = read_bytes(reader)?;
                        hash.insert(field, value);
                    }
                    let entry = make_entry(RedisValue::Hash(hash), current_expiry.take());
                    store.insert_entry(key, entry);
                }
                TYPE_SET => {
                    let key = read_bytes(reader)?;
                    let len = read_len(reader)?;
                    let mut set = std::collections::HashSet::with_capacity(len);
                    for _ in 0..len {
                        set.insert(read_bytes(reader)?);
                    }
                    let entry = make_entry(RedisValue::Set(set), current_expiry.take());
                    store.insert_entry(key, entry);
                }
                TYPE_ZSET => {
                    let key = read_bytes(reader)?;
                    let len = read_len(reader)?;
                    let mut zset = ZSetValue::new();
                    for _ in 0..len {
                        let member = read_bytes(reader)?;
                        let mut score_buf = [0u8; 8];
                        reader.read_exact(&mut score_buf)?;
                        let score = f64::from_le_bytes(score_buf);
                        zset.insert(score, member);
                    }
                    let entry = make_entry(RedisValue::ZSet(zset), current_expiry.take());
                    store.insert_entry(key, entry);
                }
                b => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown type byte: {}", b),
                    ));
                }
            }
        }

        Ok(())
    }
}

fn make_entry(value: RedisValue, expiry: Option<u64>) -> KeyEntry {
    match expiry {
        Some(exp) => KeyEntry::with_expiry(value, exp),
        None => KeyEntry::new(value),
    }
}

fn write_bytes<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    write_len(writer, data.len())?;
    writer.write_all(data)
}

fn write_len<W: Write>(writer: &mut W, len: usize) -> io::Result<()> {
    writer.write_all(&(len as u32).to_le_bytes())
}

fn read_bytes<R: Read>(reader: &mut R) -> io::Result<Bytes> {
    let len = read_len(reader)?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(Bytes::from(buf))
}

fn read_len<R: Read>(reader: &mut R) -> io::Result<usize> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf) as usize)
}

/// Build the RDB file path for a specific shard: `{dir}/{basename}-{shard_idx}.rdb`
pub fn rdb_path_for_shard(dir: &Path, basename: &str, shard_idx: usize) -> PathBuf {
    dir.join(format!("{}-{}.rdb", basename, shard_idx))
}

/// Save all shards to individual RDB files using atomic write-rename.
/// `with_store_fn` provides read-only access to each shard's store.
pub fn save_all_shards<F>(
    dir: &Path,
    basename: &str,
    num_shards: usize,
    with_store_fn: F,
) -> io::Result<()>
where
    F: Fn(usize, &dyn Fn(&ShardStore) -> io::Result<()>) -> io::Result<()>,
{
    std::fs::create_dir_all(dir)?;
    for idx in 0..num_shards {
        let final_path = rdb_path_for_shard(dir, basename, idx);
        let tmp_path = dir.join(format!("{}-{}.rdb.tmp", basename, idx));

        with_store_fn(idx, &|store: &ShardStore| {
            let file = std::fs::File::create(&tmp_path)?;
            let mut writer = BufWriter::new(file);
            RdbPersistence::save(store, &mut writer)?;
            writer.flush()?;
            writer.into_inner().map_err(|e| e.into_error())?.sync_all()?;
            Ok(())
        })?;

        std::fs::rename(&tmp_path, &final_path)?;
    }
    Ok(())
}

/// Load all shards from RDB files.
/// `with_store_mut_fn` provides mutable access to each shard's store.
pub fn load_all_shards<F>(
    dir: &Path,
    basename: &str,
    num_shards: usize,
    with_store_mut_fn: F,
) -> io::Result<usize>
where
    F: Fn(usize, &dyn Fn(&mut ShardStore) -> io::Result<()>) -> io::Result<()>,
{
    let mut loaded = 0;
    for idx in 0..num_shards {
        let path = rdb_path_for_shard(dir, basename, idx);
        if !path.exists() {
            continue;
        }
        with_store_mut_fn(idx, &|store: &mut ShardStore| {
            let mut file = std::fs::File::open(&path)?;
            RdbPersistence::load(&mut file, store)?;
            Ok(())
        })?;
        loaded += 1;
    }
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_rdb_roundtrip_string() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("key1"), RedisValue::String(Bytes::from("value1")), None);
        store.set(Bytes::from("key2"), RedisValue::String(Bytes::from("value2")), Some(60000));

        let mut buf = Vec::new();
        RdbPersistence::save(&store, &mut buf).unwrap();

        let mut loaded = ShardStore::new();
        let mut cursor = Cursor::new(&buf);
        RdbPersistence::load(&mut cursor, &mut loaded).unwrap();

        assert_eq!(loaded.dbsize(), 2);
    }

    #[test]
    fn test_rdb_roundtrip_list() {
        let mut store = ShardStore::new();
        store.list_rpush(&Bytes::from("mylist"), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);

        let mut buf = Vec::new();
        RdbPersistence::save(&store, &mut buf).unwrap();

        let mut loaded = ShardStore::new();
        RdbPersistence::load(&mut Cursor::new(&buf), &mut loaded).unwrap();
        assert_eq!(loaded.dbsize(), 1);
    }

    #[test]
    fn test_rdb_roundtrip_hash() {
        let mut store = ShardStore::new();
        store.hash_hset(&Bytes::from("myhash"), vec![
            (Bytes::from("f1"), Bytes::from("v1")),
            (Bytes::from("f2"), Bytes::from("v2")),
        ]);

        let mut buf = Vec::new();
        RdbPersistence::save(&store, &mut buf).unwrap();

        let mut loaded = ShardStore::new();
        RdbPersistence::load(&mut Cursor::new(&buf), &mut loaded).unwrap();
        assert_eq!(loaded.dbsize(), 1);
    }

    #[test]
    fn test_rdb_roundtrip_set() {
        let mut store = ShardStore::new();
        store.set_sadd(&Bytes::from("myset"), vec![Bytes::from("a"), Bytes::from("b")]);

        let mut buf = Vec::new();
        RdbPersistence::save(&store, &mut buf).unwrap();

        let mut loaded = ShardStore::new();
        RdbPersistence::load(&mut Cursor::new(&buf), &mut loaded).unwrap();
        assert_eq!(loaded.dbsize(), 1);
    }

    #[test]
    fn test_rdb_roundtrip_zset() {
        let mut store = ShardStore::new();
        store.zset_zadd(&Bytes::from("myzset"), vec![
            (1.0, Bytes::from("a")),
            (2.5, Bytes::from("b")),
        ], false, false, false, false, false);

        let mut buf = Vec::new();
        RdbPersistence::save(&store, &mut buf).unwrap();

        let mut loaded = ShardStore::new();
        RdbPersistence::load(&mut Cursor::new(&buf), &mut loaded).unwrap();
        assert_eq!(loaded.dbsize(), 1);
    }
}
