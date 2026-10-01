//! SoliKV snapshot format.
//!
//! A snapshot is one file holding every shard, each in its own *section*:
//!
//! ```text
//! "SOLIKV" | version (u8 = 4)
//! ( 0xFE | section index (u32 LE) | ( [ 0xFB | database (u32 LE) ] entry* )* | 0xFF )*
//! 0xFD | CRC-32 (u32 LE) of every byte before it
//! entry = [ 0xFC | expires-at ms (u64 LE) ] type (u8) | key | value
//! ```
//!
//! Entries belong to database 0 until a `0xFB` marker names another one
//! (`SELECT n`); version 3 files, written before logical databases, have no
//! markers and load entirely into database 0.
//!
//! Sections keep the shard a key was saved from. `dump.rdb` does not need
//! that — loading re-hashes every key to the shard that owns it under the
//! current configuration — but the append-only log's base does: its
//! per-shard logs continue exactly where their own section stopped (see
//! `crate::aof`).
//!
//! Version 2 files (`{name}-{shard}.rdb`, one per shard, no sections and no
//! checksum) are still read; they load as a single section.

use bytes::Bytes;
use solikv_core::types::*;
use solikv_core::ShardStore;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const RDB_MAGIC: &[u8] = b"SOLIKV";
/// Per-shard files without sections or checksum.
const RDB_VERSION_LEGACY: u8 = 2;
/// Sectioned, checksummed, with streams; no database markers.
const RDB_VERSION_V3: u8 = 3;
/// Version 3 plus database markers.
const RDB_VERSION: u8 = 4;

const MAX_VALUE_LEN: usize = 512 * 1024 * 1024;
/// Bounds a declared element count. Nothing is allocated from it (capacity is
/// capped separately), so it only has to reject absurd values early.
const MAX_COLLECTION_LEN: usize = u32::MAX as usize;

fn sanitize_len(len: usize) -> io::Result<usize> {
    if len > MAX_VALUE_LEN {
        return Err(invalid("RDB value length exceeds maximum"));
    }
    Ok(len)
}

fn sanitize_collection_len(len: usize) -> io::Result<usize> {
    if len > MAX_COLLECTION_LEN {
        return Err(invalid("RDB collection length exceeds maximum"));
    }
    Ok(len)
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

// Type markers
const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_HASH: u8 = 2;
const TYPE_SET: u8 = 3;
const TYPE_ZSET: u8 = 4;
const TYPE_HLL: u8 = 5;
const TYPE_BLOOM: u8 = 6;
const TYPE_STREAM: u8 = 7;
/// A stream followed by the Redis 7 metadata ([`write_stream_meta`]).
const TYPE_STREAM2: u8 = 8;
/// A hash with field expiry (`HEXPIRE`): `TYPE_HASH`'s body, then the
/// number of fields with an expiry and, for each, the field and its
/// deadline (unix ms, u64 LE). Hashes without any stay `TYPE_HASH`.
const TYPE_HASH_TTL: u8 = 9;

// Framing
const SELECT_DB: u8 = 0xFB;
const EXPIRY: u8 = 0xFC;
const CHECKSUM: u8 = 0xFD;
const SECTION: u8 = 0xFE;
const ENTRY_END: u8 = 0xFF;

/// Most entries preallocated for a collection before its elements are read.
const PREALLOC_CAP: usize = 4096;

pub struct RdbPersistence;

impl RdbPersistence {
    /// Write a single-section snapshot of one store (tests, tools).
    pub fn save<W: Write>(store: &ShardStore, writer: &mut W) -> io::Result<()> {
        let mut body = Vec::new();
        serialize_store(store, &mut body)?;
        let mut w = SnapshotWriter::new(writer)?;
        w.write_section(0, &body)?;
        w.finish()?;
        Ok(())
    }

    /// Load every section of a snapshot into one store.
    pub fn load<R: Read>(reader: &mut R, store: &mut ShardStore) -> io::Result<()> {
        read_snapshot(reader, |_, db, key, entry| {
            store.insert_entry_in(db, key, entry);
            Ok(())
        })?;
        store.select(0);
        Ok(())
    }
}

/// Serialize every live entry of `store`, all databases (section body).
pub fn serialize_store(store: &ShardStore, out: &mut Vec<u8>) -> io::Result<()> {
    // `iter_all` yields each database's entries together.
    let mut current = 0u32;
    for (db, key, entry) in store.iter_all() {
        if entry.is_expired() {
            continue;
        }
        if db != current {
            out.push(SELECT_DB);
            out.extend_from_slice(&db.to_le_bytes());
            current = db;
        }
        write_entry(out, key, entry)?;
    }
    Ok(())
}

/// `solikv_core::store::SnapshotEncoder` for this format: `key`'s entry,
/// preceded by a database marker when the stream is in another database.
/// Expired entries are left out, as `serialize_store` does.
pub fn snapshot_encoder(
    out: &mut Vec<u8>,
    cur_db: &mut u32,
    db: u32,
    key: &[u8],
    entry: &KeyEntry,
) {
    if entry.is_expired() {
        return;
    }
    if db != *cur_db {
        out.push(SELECT_DB);
        out.extend_from_slice(&db.to_le_bytes());
        *cur_db = db;
    }
    // Writing to a Vec cannot fail.
    let _ = write_entry(out, key, entry);
}

/// One key: optional expiry, type, key, value.
pub fn write_entry<W: Write>(w: &mut W, key: &[u8], entry: &KeyEntry) -> io::Result<()> {
    if let Some(exp) = entry.expires_at {
        w.write_all(&[EXPIRY])?;
        w.write_all(&exp.to_le_bytes())?;
    }
    w.write_all(&[type_byte(&entry.value)])?;
    write_bytes(w, key)?;
    write_value(w, &entry.value)
}

/// A value on its own (expiry, type, body — no key), as the append-only log
/// carries it for effects it cannot express as a command.
pub fn encode_entry(entry: &KeyEntry) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(exp) = entry.expires_at {
        out.push(EXPIRY);
        out.extend_from_slice(&exp.to_le_bytes());
    }
    out.push(type_byte(&entry.value));
    // Writing into a Vec cannot fail.
    let _ = write_value(&mut out, &entry.value);
    out
}

/// Inverse of [`encode_entry`].
pub fn decode_entry(bytes: &[u8]) -> io::Result<KeyEntry> {
    let mut r = bytes;
    let mut t = [0u8; 1];
    r.read_exact(&mut t)?;
    let mut expiry = None;
    if t[0] == EXPIRY {
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf)?;
        expiry = Some(u64::from_le_bytes(buf));
        r.read_exact(&mut t)?;
    }
    let value = read_value(&mut r, t[0])?;
    if !r.is_empty() {
        return Err(invalid("trailing bytes after encoded value"));
    }
    Ok(make_entry(value, expiry))
}

fn type_byte(value: &RedisValue) -> u8 {
    match value {
        RedisValue::String(_) => TYPE_STRING,
        RedisValue::List(_) => TYPE_LIST,
        RedisValue::Hash(h) if h.has_field_ttls() => TYPE_HASH_TTL,
        RedisValue::Hash(_) => TYPE_HASH,
        RedisValue::Set(_) => TYPE_SET,
        RedisValue::ZSet(_) => TYPE_ZSET,
        RedisValue::HyperLogLog(_) => TYPE_HLL,
        RedisValue::BloomFilter(_) => TYPE_BLOOM,
        RedisValue::Stream(_) => TYPE_STREAM2,
    }
}

fn write_value<W: Write>(w: &mut W, value: &RedisValue) -> io::Result<()> {
    match value {
        RedisValue::String(v) => write_bytes(w, v),
        RedisValue::List(list) => {
            write_len(w, list.len())?;
            for item in list {
                write_bytes(w, item)?;
            }
            Ok(())
        }
        RedisValue::Hash(hash) => {
            write_len(w, hash.len())?;
            for (field, value) in hash.iter() {
                write_bytes(w, field)?;
                write_bytes(w, value)?;
            }
            if hash.has_field_ttls() {
                let ttls: Vec<_> = hash.field_ttls().collect();
                write_len(w, ttls.len())?;
                for (field, at) in ttls {
                    write_bytes(w, field)?;
                    w.write_all(&at.to_le_bytes())?;
                }
            }
            Ok(())
        }
        RedisValue::Set(set) => {
            write_len(w, set.len())?;
            for member in set.iter() {
                write_bytes(w, member)?;
            }
            Ok(())
        }
        RedisValue::ZSet(zset) => {
            write_len(w, zset.len())?;
            for (score, member) in zset.scores.iter() {
                write_bytes(w, member)?;
                w.write_all(&score.into_inner().to_le_bytes())?;
            }
            Ok(())
        }
        RedisValue::HyperLogLog(hll) => write_bytes(w, &hll.registers),
        RedisValue::BloomFilter(bf) => {
            w.write_all(&bf.num_bits.to_le_bytes())?;
            w.write_all(&bf.num_hashes.to_le_bytes())?;
            w.write_all(&bf.num_items.to_le_bytes())?;
            w.write_all(&bf.capacity.to_le_bytes())?;
            w.write_all(&bf.error_rate.to_le_bytes())?;
            write_bytes(w, &bf.bits)
        }
        RedisValue::Stream(stream) => {
            write_stream(w, stream)?;
            write_stream_meta(w, stream)
        }
    }
}

fn write_id<W: Write>(w: &mut W, id: &StreamId) -> io::Result<()> {
    w.write_all(&id.ms.to_le_bytes())?;
    w.write_all(&id.seq.to_le_bytes())
}

fn write_stream<W: Write>(w: &mut W, s: &StreamValue) -> io::Result<()> {
    write_id(w, &s.last_id)?;
    write_len(w, s.entries.len())?;
    for (id, fields) in &s.entries {
        write_id(w, id)?;
        write_len(w, fields.len())?;
        for (f, v) in fields {
            write_bytes(w, f)?;
            write_bytes(w, v)?;
        }
    }
    write_len(w, s.groups.len())?;
    for group in s.groups.values() {
        write_bytes(w, &group.name)?;
        write_id(w, &group.last_delivered_id)?;
        write_len(w, group.pending.len())?;
        for pe in group.pending.values() {
            write_id(w, &pe.id)?;
            write_bytes(w, &pe.consumer)?;
            w.write_all(&pe.delivery_time.to_le_bytes())?;
            w.write_all(&pe.delivery_count.to_le_bytes())?;
        }
        write_len(w, group.consumers.len())?;
        for c in group.consumers.values() {
            write_bytes(w, &c.name)?;
            w.write_all(&c.seen_time.to_le_bytes())?;
            write_len(w, c.pending.len())?;
            for id in c.pending.keys() {
                write_id(w, id)?;
            }
        }
    }
    Ok(())
}

/// What `write_stream` predates: entries added, the highest deleted ID,
/// each group's entries-read and each consumer's active time — by name, as
/// the groups are read back into hash maps.
fn write_stream_meta<W: Write>(w: &mut W, s: &StreamValue) -> io::Result<()> {
    w.write_all(&s.entries_added.to_le_bytes())?;
    write_id(w, &s.max_deleted_id)?;
    write_len(w, s.groups.len())?;
    for g in s.groups.values() {
        write_bytes(w, &g.name)?;
        w.write_all(&g.entries_read.unwrap_or(u64::MAX).to_le_bytes())?;
        write_len(w, g.consumers.len())?;
        for c in g.consumers.values() {
            write_bytes(w, &c.name)?;
            w.write_all(&c.active_time.to_le_bytes())?;
        }
    }
    Ok(())
}

fn read_stream_meta<R: Read>(r: &mut R, s: &mut StreamValue) -> io::Result<()> {
    s.entries_added = read_u64(r)?;
    s.max_deleted_id = read_id(r)?;
    let ngroups = sanitize_collection_len(read_len(r)?)?;
    for _ in 0..ngroups {
        let name = read_bytes(r)?;
        let entries_read = read_u64(r)?;
        let nconsumers = sanitize_collection_len(read_len(r)?)?;
        let mut active = Vec::with_capacity(nconsumers.min(PREALLOC_CAP));
        for _ in 0..nconsumers {
            let cname = read_bytes(r)?;
            active.push((cname, read_u64(r)?));
        }
        if let Some(g) = s.groups.get_mut(&name) {
            g.entries_read = (entries_read != u64::MAX).then_some(entries_read);
            for (cname, t) in active {
                if let Some(c) = g.consumers.get_mut(&cname) {
                    c.active_time = t;
                }
            }
        }
    }
    Ok(())
}

fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_id<R: Read>(r: &mut R) -> io::Result<StreamId> {
    let ms = read_u64(r)?;
    let seq = read_u64(r)?;
    Ok(StreamId::new(ms, seq))
}

fn read_stream<R: Read>(r: &mut R) -> io::Result<StreamValue> {
    let mut s = StreamValue::new();
    s.last_id = read_id(r)?;
    let n = sanitize_collection_len(read_len(r)?)?;
    for _ in 0..n {
        let id = read_id(r)?;
        let nf = sanitize_collection_len(read_len(r)?)?;
        let mut fields = Vec::with_capacity(nf.min(PREALLOC_CAP));
        for _ in 0..nf {
            let f = read_bytes(r)?;
            let v = read_bytes(r)?;
            fields.push((f, v));
        }
        s.entries.insert(id, fields);
    }
    let ngroups = sanitize_collection_len(read_len(r)?)?;
    for _ in 0..ngroups {
        let name = read_bytes(r)?;
        let last_delivered_id = read_id(r)?;
        let npending = sanitize_collection_len(read_len(r)?)?;
        let mut pending = BTreeMap::new();
        for _ in 0..npending {
            let id = read_id(r)?;
            let consumer = read_bytes(r)?;
            let delivery_time = read_u64(r)?;
            let delivery_count = read_u64(r)?;
            pending.insert(
                id,
                PendingEntry {
                    id,
                    consumer,
                    delivery_time,
                    delivery_count,
                },
            );
        }
        let nconsumers = sanitize_collection_len(read_len(r)?)?;
        let mut consumers = HashMap::with_capacity(nconsumers.min(PREALLOC_CAP));
        for _ in 0..nconsumers {
            let cname = read_bytes(r)?;
            let seen_time = read_u64(r)?;
            let nids = sanitize_collection_len(read_len(r)?)?;
            let mut cp = BTreeMap::new();
            for _ in 0..nids {
                cp.insert(read_id(r)?, ());
            }
            let mut c = StreamConsumer::new(cname.clone(), seen_time);
            c.pending = cp;
            consumers.insert(cname, c);
        }
        let mut g = ConsumerGroup::new(name.clone(), last_delivered_id, None);
        g.pending = pending;
        g.consumers = consumers;
        s.groups.insert(name, g);
    }
    // Written before this metadata existed: estimate what can be.
    s.entries_added = s.entries.len() as u64;
    Ok(s)
}

fn read_value<R: Read>(r: &mut R, type_byte: u8) -> io::Result<RedisValue> {
    Ok(match type_byte {
        TYPE_STRING => RedisValue::String(read_bytes(r)?),
        TYPE_LIST => {
            let len = sanitize_collection_len(read_len(r)?)?;
            // Grow on push: a hostile header must not size the allocation.
            let mut list = VecDeque::with_capacity(len.min(PREALLOC_CAP));
            for _ in 0..len {
                list.push_back(read_bytes(r)?);
            }
            RedisValue::List(list)
        }
        TYPE_HASH | TYPE_HASH_TTL => {
            let len = sanitize_collection_len(read_len(r)?)?;
            let mut hash = HashValue::with_capacity(len.min(PREALLOC_CAP));
            for _ in 0..len {
                let field = read_bytes(r)?;
                let value = read_bytes(r)?;
                hash.insert(field, value);
            }
            if type_byte == TYPE_HASH_TTL {
                let n = sanitize_collection_len(read_len(r)?)?;
                for _ in 0..n {
                    let field = read_bytes(r)?;
                    let at = read_u64(r)?;
                    if !hash.set_field_ttl(&field, at) {
                        return Err(invalid("expiry for a field the hash does not have"));
                    }
                }
            }
            RedisValue::hash(hash)
        }
        TYPE_SET => {
            let len = sanitize_collection_len(read_len(r)?)?;
            let mut set =
                SetValue::with_capacity_and_hasher(len.min(PREALLOC_CAP), Default::default());
            for _ in 0..len {
                set.insert(read_bytes(r)?);
            }
            RedisValue::set(set)
        }
        TYPE_ZSET => {
            let len = sanitize_collection_len(read_len(r)?)?;
            let mut zset = ZSetValue::new();
            for _ in 0..len {
                let member = read_bytes(r)?;
                let mut score_buf = [0u8; 8];
                r.read_exact(&mut score_buf)?;
                zset.insert(f64::from_le_bytes(score_buf), member);
            }
            RedisValue::zset(zset)
        }
        TYPE_HLL => {
            let registers = read_bytes(r)?;
            if registers.len() != 16384 {
                return Err(invalid("invalid HLL register size"));
            }
            RedisValue::hll(HyperLogLogValue {
                registers: registers.to_vec(),
            })
        }
        TYPE_BLOOM => {
            let num_bits = read_u64(r)?;
            let mut buf4 = [0u8; 4];
            r.read_exact(&mut buf4)?;
            let num_hashes = u32::from_le_bytes(buf4);
            let num_items = read_u64(r)?;
            let capacity = read_u64(r)?;
            let error_rate = f64::from_le_bytes(read_u64(r)?.to_le_bytes());
            let bits = read_bytes(r)?;
            RedisValue::bloom(BloomFilterValue {
                bits: bits.to_vec(),
                num_bits,
                num_hashes,
                num_items,
                capacity,
                error_rate,
            })
        }
        TYPE_STREAM => RedisValue::stream(read_stream(r)?),
        TYPE_STREAM2 => {
            let mut st = read_stream(r)?;
            read_stream_meta(r, &mut st)?;
            RedisValue::stream(st)
        }
        b => return Err(invalid(format!("unknown type byte: {}", b))),
    })
}

/// Called for each key of a snapshot: `(section, database, key, entry)`.
pub type OnEntry<'a> = dyn FnMut(u32, u32, Bytes, KeyEntry) -> io::Result<()> + 'a;

/// Read one section's entries up to its end marker.
fn read_entries<R: Read>(
    r: &mut R,
    section: u32,
    on_entry: &mut OnEntry<'_>,
    legacy: bool,
    with_dbs: bool,
) -> io::Result<u64> {
    let mut count = 0u64;
    let mut current_expiry: Option<u64> = None;
    let mut db = 0u32;
    loop {
        let mut t = [0u8; 1];
        match r.read_exact(&mut t) {
            Ok(()) => {}
            // Version 2 files could end without the end marker.
            Err(e) if legacy && e.kind() == io::ErrorKind::UnexpectedEof => return Ok(count),
            Err(e) => return Err(e),
        }
        match t[0] {
            ENTRY_END => return Ok(count),
            EXPIRY => {
                current_expiry = Some(read_u64(r)?);
            }
            SELECT_DB if with_dbs => {
                let mut buf = [0u8; 4];
                r.read_exact(&mut buf)?;
                db = u32::from_le_bytes(buf);
            }
            TYPE_STREAM if legacy => return Err(invalid("unknown type byte: 7")),
            ty => {
                let key = read_bytes(r)?;
                let value = read_value(r, ty)?;
                on_entry(section, db, key, make_entry(value, current_expiry.take()))?;
                count += 1;
            }
        }
    }
}

/// What a snapshot held.
#[derive(Debug, Default, Clone, Copy)]
pub struct SnapshotInfo {
    pub version: u8,
    pub sections: u32,
    pub keys: u64,
}

/// Stream a snapshot, calling `on_entry(section, database, key, entry)` for
/// each key.
///
/// The checksum is verified at the end, so a caller that must not act on a
/// damaged file loads into scratch storage first; the startup path does.
pub fn read_snapshot<R: Read>(
    reader: R,
    mut on_entry: impl FnMut(u32, u32, Bytes, KeyEntry) -> io::Result<()>,
) -> io::Result<SnapshotInfo> {
    let mut r = CrcReader::new(reader);
    let mut magic = [0u8; 6];
    r.read_exact(&mut magic)?;
    if magic != RDB_MAGIC {
        return Err(invalid("invalid RDB magic"));
    }
    let mut version = [0u8; 1];
    r.read_exact(&mut version)?;
    let mut info = SnapshotInfo {
        version: version[0],
        ..Default::default()
    };
    match version[0] {
        RDB_VERSION_LEGACY => {
            info.sections = 1;
            info.keys = read_entries(&mut r, 0, &mut on_entry, true, false)?;
            Ok(info)
        }
        v @ (RDB_VERSION_V3 | RDB_VERSION) => loop {
            let mut t = [0u8; 1];
            r.read_exact(&mut t)?;
            match t[0] {
                SECTION => {
                    let mut idx = [0u8; 4];
                    r.read_exact(&mut idx)?;
                    let idx = u32::from_le_bytes(idx);
                    info.keys += read_entries(&mut r, idx, &mut on_entry, false, v == RDB_VERSION)?;
                    info.sections += 1;
                }
                CHECKSUM => {
                    let expected = r.crc();
                    let mut buf = [0u8; 4];
                    r.inner.read_exact(&mut buf)?;
                    if u32::from_le_bytes(buf) != expected {
                        return Err(invalid("RDB checksum mismatch"));
                    }
                    return Ok(info);
                }
                b => return Err(invalid(format!("unexpected RDB marker {:#x}", b))),
            }
        },
        _ => Err(invalid("unsupported RDB version")),
    }
}

/// Load a snapshot file from disk.
pub fn load_snapshot_file(
    path: &Path,
    on_entry: impl FnMut(u32, u32, Bytes, KeyEntry) -> io::Result<()>,
) -> io::Result<SnapshotInfo> {
    let file = std::fs::File::open(path)?;
    read_snapshot(BufReader::with_capacity(1 << 20, file), on_entry)
}

/// Streams a snapshot out: header, then sections as they are captured, then
/// the checksum.
pub struct SnapshotWriter<W: Write> {
    w: CrcWriter<W>,
}

impl<W: Write> SnapshotWriter<W> {
    pub fn new(inner: W) -> io::Result<Self> {
        let mut w = CrcWriter::new(inner);
        w.write_all(RDB_MAGIC)?;
        w.write_all(&[RDB_VERSION])?;
        Ok(Self { w })
    }

    /// Append one section whose body came from [`serialize_store`].
    pub fn write_section(&mut self, idx: u32, body: &[u8]) -> io::Result<()> {
        self.w.write_all(&[SECTION])?;
        self.w.write_all(&idx.to_le_bytes())?;
        self.w.write_all(body)?;
        self.w.write_all(&[ENTRY_END])
    }

    /// Write the checksum and hand back the writer, with the byte count.
    pub fn finish(mut self) -> io::Result<(W, u64)> {
        self.w.write_all(&[CHECKSUM])?;
        let crc = self.w.crc;
        self.w.inner.write_all(&crc.to_le_bytes())?;
        let len = self.w.len + 4;
        Ok((self.w.inner, len))
    }
}

/// A snapshot written to `<path>.tmp` and renamed over `path` once it is on
/// disk, so a crash leaves either the old file or the new one.
pub struct SnapshotFile {
    writer: Option<SnapshotWriter<BufWriter<std::fs::File>>>,
    tmp: PathBuf,
    path: PathBuf,
}

impl SnapshotFile {
    pub fn create(path: &Path) -> io::Result<Self> {
        if let Some(dir) = path.parent() {
            crate::fsutil::create_private_dir_all(dir)?;
        }
        let tmp = crate::fsutil::tmp_path(path);
        let file = crate::fsutil::create_private(&tmp)?;
        Ok(Self {
            writer: Some(SnapshotWriter::new(BufWriter::with_capacity(
                1 << 20,
                file,
            ))?),
            tmp,
            path: path.to_path_buf(),
        })
    }

    pub fn write_section(&mut self, idx: u32, body: &[u8]) -> io::Result<()> {
        self.writer
            .as_mut()
            .expect("snapshot already committed")
            .write_section(idx, body)
    }

    /// fsync, rename into place, fsync the directory. Returns the file size.
    pub fn commit(mut self) -> io::Result<u64> {
        let writer = self.writer.take().expect("snapshot already committed");
        let (buf, len) = writer.finish()?;
        let file = buf.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&self.tmp, &self.path)?;
        crate::fsutil::sync_parent_dir(&self.path)?;
        Ok(len)
    }
}

impl Drop for SnapshotFile {
    fn drop(&mut self) {
        // An abandoned snapshot (error part-way) must not leave its temp
        // file; after a commit the temp path no longer exists.
        let _ = std::fs::remove_file(&self.tmp);
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
    let len = u32::try_from(len).map_err(|_| invalid("length does not fit the RDB format"))?;
    writer.write_all(&len.to_le_bytes())
}

fn read_bytes<R: Read>(reader: &mut R) -> io::Result<Bytes> {
    let len = sanitize_len(read_len(reader)?)?;
    // `take` + `read_to_end` grows with the bytes that exist, so a declared
    // length larger than the file cannot size the allocation.
    let mut buf = Vec::with_capacity(len.min(1 << 16));
    reader.take(len as u64).read_to_end(&mut buf)?;
    if buf.len() != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "RDB value truncated",
        ));
    }
    Ok(Bytes::from(buf))
}

fn read_len<R: Read>(reader: &mut R) -> io::Result<usize> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf) as usize)
}

/// The snapshot file for `--dbfilename`: `{dir}/{basename}.rdb`.
pub fn rdb_path(dir: &Path, basename: &str) -> PathBuf {
    dir.join(format!("{}.rdb", basename))
}

/// A version-2 per-shard file: `{dir}/{basename}-{shard_idx}.rdb`.
pub fn rdb_path_for_shard(dir: &Path, basename: &str, shard_idx: usize) -> PathBuf {
    dir.join(format!("{}-{}.rdb", basename, shard_idx))
}

/// The version-2 per-shard files present in `dir`, in shard order, whatever
/// shard count wrote them.
pub fn legacy_shard_files(dir: &Path, basename: &str) -> Vec<PathBuf> {
    let prefix = format!("{}-", basename);
    let mut found: Vec<(usize, PathBuf)> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            let idx = name
                .strip_prefix(&prefix)?
                .strip_suffix(".rdb")?
                .parse::<usize>()
                .ok()?;
            Some((idx, e.path()))
        })
        .collect();
    found.sort();
    found.into_iter().map(|(_, p)| p).collect()
}

/// Load `{basename}.rdb`, or failing that the version-2 per-shard files.
/// Every key goes to `on_entry`; the caller routes it. Returns the files read.
pub fn load_rdb(
    dir: &Path,
    basename: &str,
    mut on_entry: impl FnMut(u32, Bytes, KeyEntry),
) -> io::Result<Vec<PathBuf>> {
    let single = rdb_path(dir, basename);
    if single.exists() {
        load_snapshot_file(&single, |_, db, k, e| {
            on_entry(db, k, e);
            Ok(())
        })?;
        return Ok(vec![single]);
    }
    let legacy = legacy_shard_files(dir, basename);
    for path in &legacy {
        load_snapshot_file(path, |_, db, k, e| {
            on_entry(db, k, e);
            Ok(())
        })?;
    }
    Ok(legacy)
}

// ---------------------------------------------------------------------------
// CRC-32 (IEEE), slicing-by-8
// ---------------------------------------------------------------------------

const fn crc_tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        t[0][i] = c;
        i += 1;
    }
    let mut i = 0;
    while i < 256 {
        let mut s = 1;
        while s < 8 {
            t[s][i] = (t[s - 1][i] >> 8) ^ t[0][(t[s - 1][i] & 0xFF) as usize];
            s += 1;
        }
        i += 1;
    }
    t
}

static CRC_TABLES: [[u32; 256]; 8] = crc_tables();

/// Continue a CRC-32 over `data` (start with 0).
pub fn crc32_update(crc: u32, data: &[u8]) -> u32 {
    let t = &CRC_TABLES;
    let mut c = !crc;
    let (chunks, rest) = data.as_chunks::<8>();
    for ch in chunks {
        let lo = c ^ u32::from_le_bytes([ch[0], ch[1], ch[2], ch[3]]);
        let hi = u32::from_le_bytes([ch[4], ch[5], ch[6], ch[7]]);
        c = t[7][(lo & 0xFF) as usize]
            ^ t[6][((lo >> 8) & 0xFF) as usize]
            ^ t[5][((lo >> 16) & 0xFF) as usize]
            ^ t[4][(lo >> 24) as usize]
            ^ t[3][(hi & 0xFF) as usize]
            ^ t[2][((hi >> 8) & 0xFF) as usize]
            ^ t[1][((hi >> 16) & 0xFF) as usize]
            ^ t[0][(hi >> 24) as usize];
    }
    for &b in rest {
        c = t[0][((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}

struct CrcWriter<W: Write> {
    inner: W,
    crc: u32,
    len: u64,
}

impl<W: Write> CrcWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            crc: 0,
            len: 0,
        }
    }
}

impl<W: Write> Write for CrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.crc = crc32_update(self.crc, &buf[..n]);
        self.len += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct CrcReader<R: Read> {
    inner: R,
    crc: u32,
}

impl<R: Read> CrcReader<R> {
    fn new(inner: R) -> Self {
        Self { inner, crc: 0 }
    }
    fn crc(&self) -> u32 {
        self.crc
    }
}

impl<R: Read> Read for CrcReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.crc = crc32_update(self.crc, &buf[..n]);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(store: &ShardStore) -> ShardStore {
        let mut buf = Vec::new();
        RdbPersistence::save(store, &mut buf).unwrap();
        let mut loaded = ShardStore::new();
        RdbPersistence::load(&mut Cursor::new(&buf), &mut loaded).unwrap();
        loaded
    }

    #[test]
    fn test_rdb_roundtrip_string() {
        let mut store = ShardStore::new();
        store.set(
            Bytes::from("key1"),
            RedisValue::String(Bytes::from("value1")),
            None,
        );
        store.set(
            Bytes::from("key2"),
            RedisValue::String(Bytes::from("value2")),
            Some(60000),
        );
        let loaded = roundtrip(&store);
        assert_eq!(loaded.dbsize(), 2);
        assert!(loaded.peek(b"key2").unwrap().expires_at.is_some());
    }

    #[test]
    fn test_rdb_roundtrip_list() {
        let mut store = ShardStore::new();
        store.list_rpush(
            &Bytes::from("mylist"),
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        );
        assert_eq!(roundtrip(&store).dbsize(), 1);
    }

    #[test]
    fn test_rdb_roundtrip_hash() {
        let mut store = ShardStore::new();
        store.hash_hset(
            &Bytes::from("myhash"),
            vec![
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ],
        );
        assert_eq!(roundtrip(&store).dbsize(), 1);
    }

    #[test]
    fn test_rdb_roundtrip_set() {
        let mut store = ShardStore::new();
        store.set_sadd(
            &Bytes::from("myset"),
            vec![Bytes::from("a"), Bytes::from("b")],
        );
        assert_eq!(roundtrip(&store).dbsize(), 1);
    }

    #[test]
    fn test_rdb_roundtrip_zset() {
        let mut store = ShardStore::new();
        store.zset_zadd(
            &Bytes::from("myzset"),
            vec![(1.0, Bytes::from("a")), (2.5, Bytes::from("b"))],
            false,
            false,
            false,
            false,
            false,
        );
        assert_eq!(roundtrip(&store).dbsize(), 1);
    }

    #[test]
    fn test_rdb_roundtrip_hll() {
        let mut store = ShardStore::new();
        store.pfadd(
            &Bytes::from("myhll"),
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        );
        let mut loaded = roundtrip(&store);
        assert_eq!(loaded.dbsize(), 1);
        let count = match loaded.pfcount(&[Bytes::from("myhll")]) {
            CommandResponse::Integer(c) => c,
            _ => panic!("expected integer"),
        };
        assert_eq!(count, 3);
    }

    #[test]
    fn test_rdb_roundtrip_bloom() {
        let mut store = ShardStore::new();
        store.bf_reserve(&Bytes::from("mybf"), 0.01, 1000);
        store.bf_add(&Bytes::from("mybf"), &Bytes::from("hello"));
        store.bf_add(&Bytes::from("mybf"), &Bytes::from("world"));
        let mut loaded = roundtrip(&store);
        assert_eq!(loaded.dbsize(), 1);
        assert!(matches!(
            loaded.bf_exists(&Bytes::from("mybf"), &Bytes::from("hello")),
            CommandResponse::Integer(1)
        ));
        assert!(matches!(
            loaded.bf_exists(&Bytes::from("mybf"), &Bytes::from("nothere")),
            CommandResponse::Integer(0)
        ));
    }

    /// Streams used to be skipped ("persisted via AOF") — with a compacted
    /// log that would lose them.
    #[test]
    fn test_rdb_roundtrip_stream_with_groups() {
        let mut s = StreamValue::new();
        s.last_id = StreamId::new(5, 1);
        s.entries.insert(
            StreamId::new(5, 0),
            vec![(Bytes::from("f"), Bytes::from("v"))],
        );
        s.entries.insert(
            StreamId::new(5, 1),
            vec![(Bytes::from("g"), Bytes::from("w"))],
        );
        let mut pending = BTreeMap::new();
        pending.insert(
            StreamId::new(5, 0),
            PendingEntry {
                id: StreamId::new(5, 0),
                consumer: Bytes::from("c1"),
                delivery_time: 42,
                delivery_count: 2,
            },
        );
        let mut cp = BTreeMap::new();
        cp.insert(StreamId::new(5, 0), ());
        let mut consumers = HashMap::new();
        let mut c1 = StreamConsumer::new(Bytes::from("c1"), 41);
        c1.pending = cp;
        c1.active_time = 40;
        consumers.insert(Bytes::from("c1"), c1);
        let mut g1 = ConsumerGroup::new(Bytes::from("g1"), StreamId::new(5, 0), Some(1));
        g1.pending = pending;
        g1.consumers = consumers;
        s.groups.insert(Bytes::from("g1"), g1);
        s.entries_added = 7;
        s.max_deleted_id = StreamId::new(4, 9);
        let mut store = ShardStore::new();
        store.insert_entry(Bytes::from("st"), KeyEntry::new(RedisValue::stream(s)));
        let loaded = roundtrip(&store);
        match &loaded.peek(b"st").unwrap().value {
            RedisValue::Stream(s) => {
                assert_eq!(s.last_id, StreamId::new(5, 1));
                assert_eq!(s.entries.len(), 2);
                let g = &s.groups[&Bytes::from("g1")];
                assert_eq!(g.pending[&StreamId::new(5, 0)].delivery_count, 2);
                assert_eq!(g.consumers[&Bytes::from("c1")].pending.len(), 1);
                assert_eq!(g.consumers[&Bytes::from("c1")].active_time, 40);
                assert_eq!(g.entries_read, Some(1));
                assert_eq!(s.entries_added, 7);
                assert_eq!(s.max_deleted_id, StreamId::new(4, 9));
            }
            _ => panic!("expected a stream"),
        }
    }

    #[test]
    fn sections_keep_their_index_and_a_flipped_byte_is_caught() {
        let mut a = ShardStore::new();
        a.set(Bytes::from("a"), RedisValue::String(Bytes::from("1")), None);
        let mut b = ShardStore::new();
        b.set(Bytes::from("b"), RedisValue::String(Bytes::from("2")), None);
        let mut out = Vec::new();
        let mut w = SnapshotWriter::new(&mut out).unwrap();
        for (i, s) in [(0u32, &a), (7u32, &b)] {
            let mut body = Vec::new();
            serialize_store(s, &mut body).unwrap();
            w.write_section(i, &body).unwrap();
        }
        w.finish().unwrap();

        let mut seen = Vec::new();
        let info = read_snapshot(Cursor::new(&out), |sec, _, k, _| {
            seen.push((sec, k));
            Ok(())
        })
        .unwrap();
        assert_eq!(info.sections, 2);
        assert_eq!(seen, vec![(0, Bytes::from("a")), (7, Bytes::from("b"))]);

        let mut bad = out.clone();
        let n = bad.len();
        bad[n - 8] ^= 0x40;
        assert!(read_snapshot(Cursor::new(&bad), |_, _, _, _| Ok(())).is_err());
    }

    #[test]
    fn databases_round_trip_and_a_version_3_file_loads_into_db_0() {
        let mut store = ShardStore::new();
        store.set(Bytes::from("a"), RedisValue::String(Bytes::from("0")), None);
        store.select(3);
        store.set(Bytes::from("a"), RedisValue::String(Bytes::from("3")), None);
        store.set(Bytes::from("b"), RedisValue::String(Bytes::from("3")), None);
        store.select(1);
        store.set(Bytes::from("c"), RedisValue::String(Bytes::from("1")), None);

        let mut buf = Vec::new();
        RdbPersistence::save(&store, &mut buf).unwrap();
        let mut seen = Vec::new();
        read_snapshot(Cursor::new(&buf), |_, db, k, _| {
            seen.push((db, k));
            Ok(())
        })
        .unwrap();
        seen.sort();
        assert_eq!(
            seen,
            vec![
                (0, Bytes::from("a")),
                (1, Bytes::from("c")),
                (3, Bytes::from("a")),
                (3, Bytes::from("b")),
            ]
        );
        let mut loaded = ShardStore::new();
        RdbPersistence::load(&mut Cursor::new(&buf), &mut loaded).unwrap();
        assert_eq!(loaded.dbsize_of(0), 1);
        assert_eq!(loaded.dbsize_of(1), 1);
        assert_eq!(loaded.dbsize_of(3), 2);

        // A version 3 body is the same bytes without markers.
        let mut v3 = Vec::new();
        let mut w = SnapshotWriter::new(&mut v3).unwrap();
        let mut body = Vec::new();
        write_entry(
            &mut body,
            b"k",
            &KeyEntry::new(RedisValue::String(Bytes::from("v"))),
        )
        .unwrap();
        w.write_section(0, &body).unwrap();
        w.finish().unwrap();
        v3[RDB_MAGIC.len()] = RDB_VERSION_V3;
        // The version byte is covered by the checksum: recompute it.
        let n = v3.len();
        let crc = crc32_update(0, &v3[..n - 4]);
        v3[n - 4..].copy_from_slice(&crc.to_le_bytes());
        let mut old = ShardStore::new();
        RdbPersistence::load(&mut Cursor::new(&v3), &mut old).unwrap();
        assert_eq!(old.dbsize_of(0), 1);
    }

    #[test]
    fn a_version_2_file_still_loads() {
        // magic, version 2, one string entry, end marker.
        let mut v2 = Vec::new();
        v2.extend_from_slice(RDB_MAGIC);
        v2.push(RDB_VERSION_LEGACY);
        v2.push(TYPE_STRING);
        write_bytes(&mut v2, b"k").unwrap();
        write_bytes(&mut v2, b"v").unwrap();
        v2.push(ENTRY_END);
        let mut store = ShardStore::new();
        RdbPersistence::load(&mut Cursor::new(&v2), &mut store).unwrap();
        assert_eq!(store.dbsize(), 1);
    }

    #[test]
    fn encoded_entries_round_trip() {
        let entry = KeyEntry::with_expiry(RedisValue::String(Bytes::from("x")), 99);
        let back = decode_entry(&encode_entry(&entry)).unwrap();
        assert_eq!(back.expires_at, Some(99));
        assert!(matches!(back.value, RedisValue::String(ref s) if s == "x"));
    }

    #[test]
    fn crc32_matches_the_reference() {
        assert_eq!(crc32_update(0, b"123456789"), 0xCBF4_3926);
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 7) as u8).collect();
        let split = crc32_update(crc32_update(0, &data[..333]), &data[333..]);
        assert_eq!(split, crc32_update(0, &data));
    }

    // SEC-006: read_bytes must reject value lengths above MAX_VALUE_LEN
    // instead of allocating a multi-GB buffer.
    #[test]
    fn test_rdb_read_bytes_rejects_oversized_value() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&((MAX_VALUE_LEN + 1) as u32).to_le_bytes());
        let err = read_bytes(&mut Cursor::new(&buf)).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_rdb_read_bytes_accepts_at_limit() {
        let mut buf = Vec::new();
        write_len(&mut buf, 4).unwrap();
        buf.extend_from_slice(b"test");
        let bytes = read_bytes(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(bytes.as_ref(), b"test");
    }

    #[test]
    fn a_declared_length_beyond_the_file_is_an_error_not_an_allocation() {
        let mut buf = Vec::new();
        write_len(&mut buf, 400_000_000).unwrap();
        buf.extend_from_slice(b"short");
        assert!(read_bytes(&mut Cursor::new(&buf)).is_err());
    }

    #[test]
    fn test_rdb_sanitize_collection_len_errors_on_overflow() {
        assert_eq!(sanitize_collection_len(10).unwrap(), 10);
        assert_eq!(
            sanitize_collection_len(MAX_COLLECTION_LEN).unwrap(),
            MAX_COLLECTION_LEN
        );
        let err = sanitize_collection_len(usize::MAX).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn snapshot_files_are_atomic_and_legacy_files_are_found_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = rdb_path(dir.path(), "dump");
        let mut store = ShardStore::new();
        store.set(Bytes::from("k"), RedisValue::String(Bytes::from("v")), None);
        let mut body = Vec::new();
        serialize_store(&store, &mut body).unwrap();
        let mut f = SnapshotFile::create(&path).unwrap();
        f.write_section(0, &body).unwrap();
        f.commit().unwrap();
        assert!(path.exists());
        assert!(!crate::fsutil::tmp_path(&path).exists());

        for i in [10usize, 2, 0] {
            std::fs::write(rdb_path_for_shard(dir.path(), "old", i), b"x").unwrap();
        }
        let found = legacy_shard_files(dir.path(), "old");
        let names: Vec<_> = found
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["old-0.rdb", "old-2.rdb", "old-10.rdb"]);
    }

    #[test]
    fn hash_field_expiry_survives_encoding() {
        let mut h = solikv_core::types::HashValue::default();
        h.insert(Bytes::from("a"), Bytes::from("1"));
        h.insert(Bytes::from("b"), Bytes::from("2"));
        h.set_field_ttl(&Bytes::from("a"), 4_102_444_800_000);
        let e = solikv_core::types::KeyEntry::new(RedisValue::hash(h));
        let back = decode_entry(&encode_entry(&e)).unwrap();
        let RedisValue::Hash(h) = back.value else {
            panic!()
        };
        assert_eq!(h.len(), 2);
        assert_eq!(h.field_ttl(b"a"), Some(4_102_444_800_000));
        assert_eq!(h.field_ttl(b"b"), None);
        // And DUMP/RESTORE keeps it too (SoliKV's own payload).
        let mut h2 = solikv_core::types::HashValue::default();
        h2.insert(Bytes::from("x"), Bytes::from("y"));
        h2.set_field_ttl(&Bytes::from("x"), 4_102_444_800_000);
        let p = crate::dump::dump(&RedisValue::hash(h2));
        let RedisValue::Hash(h2) = crate::dump::restore(&p).unwrap() else {
            panic!()
        };
        assert_eq!(h2.field_ttl(b"x"), Some(4_102_444_800_000));
    }
}
