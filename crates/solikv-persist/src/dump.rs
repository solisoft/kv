//! `DUMP`/`RESTORE` payloads in Redis' format: an RDB-encoded value, the
//! RDB version it was written with, and a CRC64 of both — so a value can be
//! moved between SoliKV and Redis (`MIGRATE`-style tooling, `redis-cli
//! --pipe` scripts, client libraries' copy helpers).
//!
//! Strings, lists, sets, hashes and sorted sets are written in encodings
//! every Redis since 2.x loads, and anything a Redis 7.x `DUMP` produces for
//! them is read back (listpacks, quicklists, intsets, ziplists, via the RDB
//! importer). Streams, HyperLogLogs and bloom filters, whose layouts differ
//! from Redis', use a private type byte: they round-trip between SoliKV
//! servers, and Redis refuses them as it refuses any unknown type.

use bytes::Bytes;

use solikv_core::types::{KeyEntry, RedisValue};

use crate::redis_rdb::parser::RdbReader;

const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
const TYPE_HASH: u8 = 4;
const TYPE_ZSET_2: u8 = 5;
/// SoliKV's own encoding (`crate::rdb::encode_entry`) of other values.
const TYPE_SOLIKV: u8 = 0xF0;

/// RDB version written in the footer: old enough for every Redis 6/7 to
/// accept, as the encodings above are.
const DUMP_RDB_VERSION: u16 = 9;
/// Newest RDB version accepted (Redis 7.4 writes 12).
const MAX_RDB_VERSION: u16 = 12;

/// Why a payload was refused (Redis' two errors).
#[derive(Debug, PartialEq, Eq)]
pub enum RestoreError {
    /// Version too new or checksum mismatch.
    VersionOrChecksum,
    /// The value itself does not parse.
    BadData,
}

fn crc64_table() -> &'static [u64; 256] {
    static T: std::sync::OnceLock<[u64; 256]> = std::sync::OnceLock::new();
    T.get_or_init(|| {
        // Jones polynomial, reflected (what Redis' crc64.c computes).
        const POLY: u64 = 0x95ac_9329_ac4b_c9b5;
        let mut t = [0u64; 256];
        for (i, e) in t.iter_mut().enumerate() {
            let mut c = i as u64;
            for _ in 0..8 {
                c = if c & 1 == 1 { (c >> 1) ^ POLY } else { c >> 1 };
            }
            *e = c;
        }
        t
    })
}

/// Redis' CRC64 (Jones, reflected, init 0, no final xor).
pub fn crc64(mut crc: u64, data: &[u8]) -> u64 {
    let t = crc64_table();
    for &b in data {
        crc = t[((crc ^ b as u64) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

fn write_len(out: &mut Vec<u8>, len: usize) {
    if len < 64 {
        out.push(len as u8);
    } else if len < 16384 {
        out.push(0x40 | (len >> 8) as u8);
        out.push(len as u8);
    } else if len <= u32::MAX as usize {
        out.push(0x80);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.push(0x81);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }
}

fn write_str(out: &mut Vec<u8>, s: &[u8]) {
    write_len(out, s.len());
    out.extend_from_slice(s);
}

/// The `DUMP` payload of a value.
pub fn dump(value: &RedisValue) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        RedisValue::String(s) => {
            out.push(TYPE_STRING);
            write_str(&mut out, s);
        }
        RedisValue::List(l) => {
            out.push(TYPE_LIST);
            write_len(&mut out, l.len());
            for e in l {
                write_str(&mut out, e);
            }
        }
        RedisValue::Set(s) => {
            out.push(TYPE_SET);
            write_len(&mut out, s.len());
            for e in s.iter() {
                write_str(&mut out, e);
            }
        }
        // A hash with field expiry goes out in SoliKV's own encoding (below),
        // which keeps the expiries: a payload Redis would misread is worse
        // than one it refuses.
        RedisValue::Hash(h) if !h.has_field_ttls() => {
            out.push(TYPE_HASH);
            write_len(&mut out, h.len());
            for (f, v) in h.iter() {
                write_str(&mut out, f);
                write_str(&mut out, v);
            }
        }
        RedisValue::ZSet(z) => {
            out.push(TYPE_ZSET_2);
            write_len(&mut out, z.len());
            for (score, member) in z.scores.iter() {
                write_str(&mut out, member);
                out.extend_from_slice(&score.into_inner().to_le_bytes());
            }
        }
        other => {
            out.push(TYPE_SOLIKV);
            out.extend_from_slice(&crate::rdb::encode_entry(&KeyEntry::new(other.clone())));
        }
    }
    out.extend_from_slice(&DUMP_RDB_VERSION.to_le_bytes());
    let crc = crc64(0, &out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// The value in a `RESTORE` payload.
pub fn restore(payload: &[u8]) -> Result<RedisValue, RestoreError> {
    if payload.len() < 11 {
        return Err(RestoreError::VersionOrChecksum);
    }
    let (body, crc) = payload.split_at(payload.len() - 8);
    let version = u16::from_le_bytes([body[body.len() - 2], body[body.len() - 1]]);
    if version > MAX_RDB_VERSION {
        return Err(RestoreError::VersionOrChecksum);
    }
    let crc = u64::from_le_bytes(crc.try_into().expect("8 bytes"));
    // Redis skips the check when the checksum is 0 (written by a server
    // with checksums off).
    if crc != 0 && crc64(0, body) != crc {
        return Err(RestoreError::VersionOrChecksum);
    }
    let value = &body[..body.len() - 2];
    let (&ty, data) = value.split_first().ok_or(RestoreError::BadData)?;
    if ty == TYPE_SOLIKV {
        return crate::rdb::decode_entry(data)
            .map(|e| e.value)
            .map_err(|_| RestoreError::BadData);
    }
    let mut r = RdbReader::new(data);
    match crate::redis_rdb::read_value(&mut r, ty) {
        Ok(Some(v)) if r.position() as usize == data.len() => Ok(v),
        _ => Err(RestoreError::BadData),
    }
}

/// `DUMP` of a string value, for tests and tools.
pub fn dump_string(s: &[u8]) -> Vec<u8> {
    dump(&RedisValue::String(Bytes::copy_from_slice(s)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_matches_redis() {
        // The check value from Redis' crc64.c test.
        assert_eq!(crc64(0, b"123456789"), 0xe9c6_d914_c4b8_d9ca);
    }

    #[test]
    fn a_redis_dump_restores() {
        // `SET mykey 10` then `DUMP mykey`, from Redis' documentation (RDB
        // 10, the value int-encoded).
        let redis = b"\x00\xc0\n\n\x00n\x9fWE\x0e\xaec\xbb";
        match restore(redis) {
            Ok(RedisValue::String(s)) => assert_eq!(&s[..], b"10"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn values_round_trip_and_damage_is_caught() {
        let mut z = solikv_core::types::ZSetValue::new();
        z.insert(1.5, Bytes::from("a"));
        z.insert(-2.0, Bytes::from("b"));
        let p = dump(&RedisValue::zset(z));
        match restore(&p) {
            Ok(RedisValue::ZSet(z)) => {
                assert_eq!(z.score(&Bytes::from("a")), Some(1.5));
                assert_eq!(z.score(&Bytes::from("b")), Some(-2.0));
            }
            other => panic!("{other:?}"),
        }
        let mut bad = dump_string(b"hello");
        bad[3] ^= 1;
        assert_eq!(restore(&bad).unwrap_err(), RestoreError::VersionOrChecksum);
        let big = dump_string(&vec![b'x'; 70_000]);
        assert!(matches!(restore(&big), Ok(RedisValue::String(s)) if s.len() == 70_000));
    }
}
