use bytes::{Bytes, BytesMut};

use crate::store::ShardStore;
use crate::types::*;

/// Options of `SET key value [NX|XX] [GET] [EX|PX|EXAT|PXAT|KEEPTTL]`,
/// already validated.
#[derive(Debug, Clone, Copy, Default)]
pub struct SetOptions {
    /// Absolute expiry in ms (any of EX/PX/EXAT/PXAT, resolved by the caller).
    pub expire_at: Option<u64>,
    pub keep_ttl: bool,
    pub nx: bool,
    pub xx: bool,
    pub get: bool,
}

impl ShardStore {
    // ---- GET ----
    #[inline]
    pub fn string_get(&mut self, key: &Bytes) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::nil(),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => CommandResponse::bulk(v.clone()),
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- SET ----
    /// SET key value [EX seconds] [PX milliseconds] [NX|XX] [GET]
    pub fn string_set(
        &mut self,
        key: Bytes,
        value: Bytes,
        expire_ms: Option<u64>,
        nx: bool,
        xx: bool,
        get: bool,
    ) -> CommandResponse {
        // Fast path: plain SET (redis-benchmark default) — no NX/XX/GET, no extra lookups.
        if !nx && !xx && !get {
            self.set(key, RedisValue::String(value), expire_ms);
            return CommandResponse::ok();
        }

        let old = if get {
            match self.get(&key) {
                Some(entry) => match &entry.value {
                    RedisValue::String(v) => Some(CommandResponse::bulk(v.clone())),
                    _ => return CommandResponse::wrong_type(),
                },
                None => Some(CommandResponse::nil()),
            }
        } else {
            None
        };

        if nx && self.exists(&key) {
            return old.unwrap_or(CommandResponse::nil());
        }
        if xx && !self.exists(&key) {
            return old.unwrap_or(CommandResponse::nil());
        }

        self.set(key, RedisValue::String(value), expire_ms);

        old.unwrap_or(CommandResponse::ok())
    }

    /// SET with every option (see [`SetOptions`]).
    pub fn string_set_opts(&mut self, key: Bytes, value: Bytes, o: SetOptions) -> CommandResponse {
        let old = if o.get {
            match self.get(&key) {
                Some(entry) => match &entry.value {
                    RedisValue::String(v) => Some(CommandResponse::bulk(v.clone())),
                    _ => return CommandResponse::wrong_type(),
                },
                None => Some(CommandResponse::nil()),
            }
        } else {
            None
        };
        if o.nx || o.xx {
            let exists = match &old {
                Some(prev) => !matches!(prev, CommandResponse::Nil),
                None => self.exists(&key),
            };
            if (o.nx && exists) || (o.xx && !exists) {
                return old.unwrap_or(CommandResponse::nil());
            }
        }
        match (o.expire_at, o.keep_ttl) {
            (Some(at), _) => self.set_with_absolute_expiry(key, RedisValue::String(value), at),
            (None, true) => self.set_keep_ttl(&key, RedisValue::String(value)),
            (None, false) => self.set(key, RedisValue::String(value), None),
        }
        old.unwrap_or(CommandResponse::ok())
    }

    // ---- SETNX ----
    pub fn string_setnx(&mut self, key: Bytes, value: Bytes) -> CommandResponse {
        if self.exists(&key) {
            CommandResponse::integer(0)
        } else {
            self.set(key, RedisValue::String(value), None);
            CommandResponse::integer(1)
        }
    }

    // ---- SETEX ----
    pub fn string_setex(&mut self, key: Bytes, seconds: u64, value: Bytes) -> CommandResponse {
        self.set(
            key,
            RedisValue::String(value),
            Some(seconds.saturating_mul(1000)),
        );
        CommandResponse::ok()
    }

    // ---- PSETEX ----
    pub fn string_psetex(&mut self, key: Bytes, millis: u64, value: Bytes) -> CommandResponse {
        self.set(key, RedisValue::String(value), Some(millis));
        CommandResponse::ok()
    }

    // ---- GETSET ----
    pub fn string_getset(&mut self, key: Bytes, value: Bytes) -> CommandResponse {
        let old = match self.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::String(v) => CommandResponse::bulk(v.clone()),
                _ => return CommandResponse::wrong_type(),
            },
            None => CommandResponse::nil(),
        };
        self.set(key, RedisValue::String(value), None);
        old
    }

    // ---- GETDEL ----
    pub fn string_getdel(&mut self, key: &Bytes) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::nil(),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => {
                    let result = CommandResponse::bulk(v.clone());
                    self.del(key);
                    result
                }
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- GETEX ----
    pub fn string_getex(
        &mut self,
        key: &Bytes,
        expire_ms: Option<u64>,
        persist: bool,
    ) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::nil(),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => {
                    let result = CommandResponse::bulk(v.clone());
                    if persist {
                        self.persist(key);
                    } else if let Some(ms) = expire_ms {
                        self.expire(key, ms);
                    }
                    result
                }
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    /// GETEX with an absolute expiry (`expire_at`, ms) or `persist`. An
    /// expiry already in the past deletes the key, as in Redis.
    pub fn string_getex_at(
        &mut self,
        key: &Bytes,
        expire_at: Option<u64>,
        persist: bool,
    ) -> CommandResponse {
        let result = match self.get(key) {
            None => return CommandResponse::nil(),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => CommandResponse::bulk(v.clone()),
                _ => return CommandResponse::wrong_type(),
            },
        };
        if persist {
            self.persist(key);
        } else if let Some(at) = expire_at {
            if at <= now_millis() {
                self.del(key);
            } else {
                self.expire_at(key, at);
            }
        }
        result
    }

    // ---- MGET ----
    pub fn string_mget(&mut self, keys: &[Bytes]) -> CommandResponse {
        let results: Vec<CommandResponse> = keys.iter().map(|k| self.string_get(k)).collect();
        CommandResponse::array(results)
    }

    // ---- MSET ----
    pub fn string_mset(&mut self, pairs: Vec<(Bytes, Bytes)>) -> CommandResponse {
        for (key, value) in pairs {
            self.set(key, RedisValue::String(value), None);
        }
        CommandResponse::ok()
    }

    // ---- MSETNX ----
    pub fn string_msetnx(&mut self, pairs: Vec<(Bytes, Bytes)>) -> CommandResponse {
        // Only set if NONE of the keys exist
        for (key, _) in &pairs {
            if self.exists(key) {
                return CommandResponse::integer(0);
            }
        }
        for (key, value) in pairs {
            self.set(key, RedisValue::String(value), None);
        }
        CommandResponse::integer(1)
    }

    // ---- INCR ----
    pub fn string_incr(&mut self, key: &Bytes) -> CommandResponse {
        self.string_incrby(key, 1)
    }

    // ---- DECR ----
    pub fn string_decr(&mut self, key: &Bytes) -> CommandResponse {
        self.string_incrby(key, -1)
    }

    // ---- INCRBY ----
    pub fn string_incrby(&mut self, key: &Bytes, delta: i64) -> CommandResponse {
        let current = match self.get(key) {
            None => 0i64,
            Some(entry) => match &entry.value {
                RedisValue::String(v) => match std::str::from_utf8(v) {
                    Ok(s) => match s.parse::<i64>() {
                        Ok(n) => n,
                        Err(_) => {
                            return CommandResponse::error(
                                "ERR value is not an integer or out of range",
                            )
                        }
                    },
                    Err(_) => {
                        return CommandResponse::error(
                            "ERR value is not an integer or out of range",
                        )
                    }
                },
                _ => return CommandResponse::wrong_type(),
            },
        };

        match current.checked_add(delta) {
            Some(new_val) => {
                self.set_keep_ttl(key, RedisValue::String(Bytes::from(new_val.to_string())));
                CommandResponse::integer(new_val)
            }
            None => CommandResponse::error("ERR increment or decrement would overflow"),
        }
    }

    // ---- DECRBY ----
    pub fn string_decrby(&mut self, key: &Bytes, delta: i64) -> CommandResponse {
        self.string_incrby(key, -delta)
    }

    // ---- INCRBYFLOAT ----
    pub fn string_incrbyfloat(&mut self, key: &Bytes, delta: f64) -> CommandResponse {
        let current = match self.get(key) {
            None => 0.0f64,
            Some(entry) => match &entry.value {
                RedisValue::String(v) => match std::str::from_utf8(v) {
                    Ok(s) => match s.parse::<f64>() {
                        Ok(n) => n,
                        Err(_) => return CommandResponse::error("ERR value is not a valid float"),
                    },
                    Err(_) => return CommandResponse::error("ERR value is not a valid float"),
                },
                _ => return CommandResponse::wrong_type(),
            },
        };

        let new_val = current + delta;
        if !new_val.is_finite() {
            return CommandResponse::error("ERR increment would produce NaN or Infinity");
        }
        let formatted = if new_val == new_val.trunc() && new_val.abs() < 1e17 {
            format!("{}", new_val as i64)
        } else {
            format!("{}", new_val)
        };
        self.set_keep_ttl(key, RedisValue::String(Bytes::from(formatted.clone())));
        CommandResponse::bulk(Bytes::from(formatted))
    }

    /// Run `f` on the string at `key` as a growable buffer, in place: the
    /// value is taken out of the entry, turned back into a `BytesMut`
    /// without copying when nothing else shares it, and put back — so
    /// `APPEND` and `SETBIT` grow a value in amortised O(1) instead of
    /// copying all of it on every call. `create` makes a missing key an
    /// empty string first. `Ok(None)`: missing and not created. TTL kept.
    fn with_string_buf<R>(
        &mut self,
        key: &Bytes,
        create: bool,
        f: impl FnOnce(&mut BytesMut) -> R,
    ) -> Result<Option<R>, CommandResponse> {
        match self.get_mut(key) {
            Some(entry) => match &mut entry.value {
                RedisValue::String(v) => {
                    let taken = std::mem::take(v);
                    let mut buf = taken
                        .try_into_mut()
                        .unwrap_or_else(|shared| BytesMut::from(shared.as_ref()));
                    let r = f(&mut buf);
                    *v = buf.freeze();
                    Ok(Some(r))
                }
                _ => Err(CommandResponse::wrong_type()),
            },
            None if create => {
                let mut buf = BytesMut::new();
                let r = f(&mut buf);
                self.set(key.clone(), RedisValue::String(buf.freeze()), None);
                Ok(Some(r))
            }
            None => Ok(None),
        }
    }

    /// Length of the string at `key` (0 when missing), or WRONGTYPE.
    fn string_len_of(&mut self, key: &Bytes) -> Result<usize, CommandResponse> {
        match self.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => Ok(v.len()),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    // ---- APPEND ----
    pub fn string_append(&mut self, key: &Bytes, value: &Bytes) -> CommandResponse {
        let len = match self.string_len_of(key) {
            Ok(n) => n,
            Err(e) => return e,
        };
        if len.saturating_add(value.len()) > MAX_STRING_LEN {
            return CommandResponse::error(
                "ERR string exceeds maximum allowed size (proto-max-bulk-len)",
            );
        }
        if len == 0 && !self.exists(key) {
            // A new key: store the argument itself, no buffer.
            self.set(key.clone(), RedisValue::String(value.clone()), None);
            return CommandResponse::integer(value.len() as i64);
        }
        match self.with_string_buf(key, true, |buf| {
            buf.extend_from_slice(value);
            buf.len()
        }) {
            Ok(n) => CommandResponse::integer(n.unwrap_or(0) as i64),
            Err(e) => e,
        }
    }

    // ---- STRLEN ----
    pub fn string_strlen(&mut self, key: &Bytes) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::integer(0),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => CommandResponse::integer(v.len() as i64),
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- GETRANGE ----
    pub fn string_getrange(&mut self, key: &Bytes, start: i64, end: i64) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::bulk(Bytes::new()),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => {
                    let len = v.len() as i64;
                    if len == 0 {
                        return CommandResponse::bulk(Bytes::new());
                    }
                    let s = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    };
                    let e = if end < 0 {
                        (len + end).max(0)
                    } else {
                        end.min(len - 1)
                    };
                    if s > e {
                        CommandResponse::bulk(Bytes::new())
                    } else {
                        CommandResponse::bulk(v.slice(s as usize..=e as usize))
                    }
                }
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- SETRANGE ----
    pub fn string_setrange(
        &mut self,
        key: &Bytes,
        offset: usize,
        value: &Bytes,
    ) -> CommandResponse {
        let len = match self.string_len_of(key) {
            Ok(n) => n,
            Err(e) => return e,
        };
        // Redis: an empty value changes nothing (and creates no key).
        if value.is_empty() {
            return CommandResponse::integer(len as i64);
        }

        // Checked, and capped like Redis' proto-max-bulk-len: `offset` comes
        // from the client, and `usize::MAX + len` used to wrap to a small
        // `needed` and then zero-pad one byte at a time towards 2^64 while
        // holding the shard lock.
        let needed = match offset.checked_add(value.len()) {
            Some(n) if n <= MAX_STRING_LEN => n,
            _ => {
                return CommandResponse::error(
                    "ERR string exceeds maximum allowed size (proto-max-bulk-len)",
                )
            }
        };
        match self.with_string_buf(key, true, |buf| {
            if buf.len() < needed {
                buf.resize(needed, 0);
            }
            buf[offset..needed].copy_from_slice(value);
            buf.len()
        }) {
            Ok(n) => CommandResponse::integer(n.unwrap_or(0) as i64),
            Err(e) => e,
        }
    }

    // ---- SETBIT ----
    pub fn string_setbit(&mut self, key: &Bytes, offset: u64, value: u8) -> CommandResponse {
        if offset >= MAX_BIT_OFFSET {
            return CommandResponse::error("ERR bit offset is not an integer or out of range");
        }
        let byte_offset = (offset / 8) as usize;
        let bit_pos = 7 - (offset % 8) as u8;
        match self.with_string_buf(key, true, |buf| {
            // Auto-grow with zero bytes if needed
            if byte_offset >= buf.len() {
                buf.resize(byte_offset + 1, 0);
            }
            let old_bit = (buf[byte_offset] >> bit_pos) & 1;
            if value != 0 {
                buf[byte_offset] |= 1 << bit_pos;
            } else {
                buf[byte_offset] &= !(1 << bit_pos);
            }
            old_bit
        }) {
            Ok(bit) => CommandResponse::integer(bit.unwrap_or(0) as i64),
            Err(e) => e,
        }
    }

    // ---- GETBIT ----
    pub fn string_getbit(&mut self, key: &Bytes, offset: u64) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::integer(0),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => {
                    let byte_offset = (offset / 8) as usize;
                    if byte_offset >= v.len() {
                        return CommandResponse::integer(0);
                    }
                    let bit_pos = 7 - (offset % 8) as u8;
                    let bit = (v[byte_offset] >> bit_pos) & 1;
                    CommandResponse::integer(bit as i64)
                }
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- BITCOUNT ----
    pub fn string_bitcount(
        &mut self,
        key: &Bytes,
        start: Option<i64>,
        end: Option<i64>,
    ) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::integer(0),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => {
                    let len = v.len() as i64;
                    if len == 0 {
                        return CommandResponse::integer(0);
                    }
                    let (s, e) = match (start, end) {
                        (Some(s), Some(e)) => {
                            let s = if s < 0 { (len + s).max(0) } else { s.min(len) };
                            let e = if e < 0 {
                                (len + e).max(0)
                            } else {
                                e.min(len - 1)
                            };
                            (s as usize, e as usize)
                        }
                        _ => (0, (len - 1) as usize),
                    };
                    if s > e {
                        return CommandResponse::integer(0);
                    }
                    let count: u32 = v[s..=e].iter().map(|b| b.count_ones()).sum();
                    CommandResponse::integer(count as i64)
                }
                _ => CommandResponse::wrong_type(),
            },
        }
    }

    // ---- BITOP ----
    /// Takes pre-fetched source byte arrays (dispatch handles cross-shard fetching).
    pub fn string_bitop(&mut self, dest: Bytes, op: &str, sources: &[Vec<u8>]) -> CommandResponse {
        if sources.is_empty() {
            self.set(dest, RedisValue::String(Bytes::new()), None);
            return CommandResponse::integer(0);
        }

        let max_len = sources.iter().map(|s| s.len()).max().unwrap_or(0);

        let result: Vec<u8> = match op {
            "NOT" => {
                let src = &sources[0];
                (0..max_len)
                    .map(|i| !src.get(i).copied().unwrap_or(0))
                    .collect()
            }
            _ => (0..max_len)
                .map(|i| {
                    let mut val = sources[0].get(i).copied().unwrap_or(0);
                    for src in &sources[1..] {
                        let b = src.get(i).copied().unwrap_or(0);
                        match op {
                            "AND" => val &= b,
                            "OR" => val |= b,
                            "XOR" => val ^= b,
                            _ => {}
                        }
                    }
                    val
                })
                .collect(),
        };

        let len = result.len() as i64;
        self.set(dest, RedisValue::String(Bytes::from(result)), None);
        CommandResponse::integer(len)
    }

    // ---- GET_RAW (helper for cross-shard BITOP) ----
    pub fn string_get_raw(&mut self, key: &Bytes) -> CommandResponse {
        match self.get(key) {
            None => CommandResponse::nil(),
            Some(entry) => match &entry.value {
                RedisValue::String(v) => CommandResponse::bulk(v.clone()),
                _ => CommandResponse::wrong_type(),
            },
        }
    }
}

/// Overflow handling of `BITFIELD` (`OVERFLOW WRAP|SAT|FAIL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOverflow {
    Wrap,
    Sat,
    Fail,
}

/// An integer field of a `BITFIELD` command: `i1`..`i64` or `u1`..`u63`.
#[derive(Debug, Clone, Copy)]
pub struct BitType {
    pub signed: bool,
    pub bits: u32,
}

/// One `BITFIELD` sub-command; offsets are in bits.
#[derive(Debug, Clone, Copy)]
pub enum BitfieldOp {
    Get(BitType, u64),
    Set(BitType, u64, i64, BitOverflow),
    Incr(BitType, u64, i64, BitOverflow),
}

impl BitType {
    fn range(&self) -> (i128, i128) {
        if self.signed {
            (-(1i128 << (self.bits - 1)), (1i128 << (self.bits - 1)) - 1)
        } else {
            (0, (1i128 << self.bits) - 1)
        }
    }

    /// `v` as a field of this type: wrapped, saturated, or `None` (FAIL).
    fn fit(&self, v: i128, of: BitOverflow) -> Option<i64> {
        let (lo, hi) = self.range();
        if (lo..=hi).contains(&v) {
            return Some(v as i64);
        }
        match of {
            BitOverflow::Fail => None,
            BitOverflow::Sat => Some(if v < lo { lo } else { hi } as i64),
            BitOverflow::Wrap => {
                let m = 1i128 << self.bits;
                let mut w = v.rem_euclid(m);
                if self.signed && w > hi {
                    w -= m;
                }
                Some(w as i64)
            }
        }
    }

    fn decode(&self, raw: u64) -> i64 {
        if self.signed && self.bits < 64 && raw >> (self.bits - 1) & 1 == 1 {
            (raw | (!0u64 << self.bits)) as i64
        } else {
            raw as i64
        }
    }
}

fn read_bits(buf: &[u8], offset: u64, bits: u32) -> u64 {
    let mut v = 0u64;
    for i in 0..bits as u64 {
        let pos = offset + i;
        let byte = (pos / 8) as usize;
        let bit = buf.get(byte).map_or(0, |b| (b >> (7 - pos % 8)) & 1);
        v = (v << 1) | bit as u64;
    }
    v
}

fn write_bits(buf: &mut [u8], offset: u64, bits: u32, value: u64) {
    for i in 0..bits as u64 {
        let pos = offset + i;
        let byte = (pos / 8) as usize;
        let mask = 1u8 << (7 - pos % 8);
        if (value >> (bits as u64 - 1 - i)) & 1 == 1 {
            buf[byte] |= mask;
        } else {
            buf[byte] &= !mask;
        }
    }
}

/// Byte range `[start, end]` of a `len`-long string from Redis-style
/// indexes (negative from the end), or `None` when empty.
fn norm_range(start: i64, end: i64, len: i64) -> Option<(i64, i64)> {
    let s = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };
    let e = if end < 0 {
        (len + end).max(-1)
    } else {
        end.min(len - 1)
    };
    (s <= e && s < len).then_some((s, e))
}

impl ShardStore {
    /// `BITFIELD key [GET|SET|INCRBY …] [OVERFLOW …]…`. Write operations
    /// create the key, grown with zeros to the highest field written.
    pub fn string_bitfield(&mut self, key: &Bytes, ops: &[BitfieldOp]) -> CommandResponse {
        let highest_write = ops
            .iter()
            .filter_map(|op| match op {
                BitfieldOp::Set(t, o, ..) | BitfieldOp::Incr(t, o, ..) => Some(o + t.bits as u64),
                BitfieldOp::Get(..) => None,
            })
            .max();
        let run = |buf: &mut [u8], write: bool| -> Vec<CommandResponse> {
            let mut out = Vec::with_capacity(ops.len());
            for op in ops {
                match *op {
                    BitfieldOp::Get(t, o) => out.push(CommandResponse::integer(
                        t.decode(read_bits(buf, o, t.bits)),
                    )),
                    BitfieldOp::Set(t, o, v, of) if write => {
                        let old = t.decode(read_bits(buf, o, t.bits));
                        if let Some(nv) = t.fit(v as i128, of) {
                            write_bits(buf, o, t.bits, nv as u64);
                            out.push(CommandResponse::integer(old));
                        } else {
                            out.push(CommandResponse::Nil);
                        }
                    }
                    BitfieldOp::Incr(t, o, by, of) if write => {
                        let old = t.decode(read_bits(buf, o, t.bits));
                        match t.fit(old as i128 + by as i128, of) {
                            Some(nv) => {
                                write_bits(buf, o, t.bits, nv as u64);
                                out.push(CommandResponse::integer(nv));
                            }
                            None => out.push(CommandResponse::Nil),
                        }
                    }
                    _ => out.push(CommandResponse::Nil),
                }
            }
            out
        };
        match highest_write {
            None => {
                // Reads only: no copy of the value.
                let v = match self.get(key) {
                    None => Bytes::new(),
                    Some(e) => match &e.value {
                        RedisValue::String(v) => v.clone(),
                        _ => return CommandResponse::wrong_type(),
                    },
                };
                CommandResponse::array(
                    ops.iter()
                        .map(|op| match *op {
                            BitfieldOp::Get(t, o) => {
                                CommandResponse::integer(t.decode(read_bits(&v, o, t.bits)))
                            }
                            _ => CommandResponse::Nil,
                        })
                        .collect(),
                )
            }
            Some(bits) => {
                let need = bits.div_ceil(8) as usize;
                match self.with_string_buf(key, true, |b| {
                    if b.len() < need {
                        b.resize(need, 0);
                    }
                    run(&mut b[..], true)
                }) {
                    Ok(out) => CommandResponse::array(out.unwrap_or_default()),
                    Err(e) => e,
                }
            }
        }
    }

    /// `BITPOS key bit [start [end [BYTE|BIT]]]`.
    pub fn string_bitpos(
        &mut self,
        key: &Bytes,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        bitmode: bool,
    ) -> CommandResponse {
        let v = match self.get(key) {
            None => return CommandResponse::integer(if bit == 1 { -1 } else { 0 }),
            Some(e) => match &e.value {
                RedisValue::String(v) => v.clone(),
                _ => return CommandResponse::wrong_type(),
            },
        };
        let unit = if bitmode { 8 } else { 1 };
        let total = v.len() as i64 * unit;
        let end_given = end.is_some();
        let Some((s, e)) = norm_range(start.unwrap_or(0), end.unwrap_or(-1), total) else {
            return CommandResponse::integer(-1);
        };
        let (first, last) = if bitmode { (s, e) } else { (s * 8, e * 8 + 7) };
        let mut pos = first;
        while pos <= last {
            // Whole bytes that cannot match are skipped.
            if pos % 8 == 0 && pos + 7 <= last {
                let b = v[(pos / 8) as usize];
                if (bit == 1 && b == 0) || (bit == 0 && b == 0xff) {
                    pos += 8;
                    continue;
                }
            }
            let b = (v[(pos / 8) as usize] >> (7 - pos % 8)) & 1;
            if b == bit {
                return CommandResponse::integer(pos);
            }
            pos += 1;
        }
        if bit == 0 && !end_given {
            // Looking for a clear bit past the end: the string is padded
            // with zeros on the right.
            return CommandResponse::integer(last + 1);
        }
        CommandResponse::integer(-1)
    }

    /// `BITCOUNT key [start end [BYTE|BIT]]`.
    pub fn string_bitcount_mode(
        &mut self,
        key: &Bytes,
        range: Option<(i64, i64)>,
        bitmode: bool,
    ) -> CommandResponse {
        if !bitmode {
            return self.string_bitcount(key, range.map(|r| r.0), range.map(|r| r.1));
        }
        let v = match self.get(key) {
            None => return CommandResponse::integer(0),
            Some(e) => match &e.value {
                RedisValue::String(v) => v.clone(),
                _ => return CommandResponse::wrong_type(),
            },
        };
        let (s, e) = range.unwrap_or((0, -1));
        let Some((s, e)) = norm_range(s, e, v.len() as i64 * 8) else {
            return CommandResponse::integer(0);
        };
        let n = (s..=e)
            .filter(|&p| (v[(p / 8) as usize] >> (7 - p % 8)) & 1 == 1)
            .count();
        CommandResponse::integer(n as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_nonexistent() {
        let mut store = ShardStore::new();
        assert!(matches!(
            store.string_get(&Bytes::from("x")),
            CommandResponse::Nil
        ));
    }

    #[test]
    fn test_set_and_get() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("v"),
            None,
            false,
            false,
            false,
        );
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v")),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_set_nx() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("v1"),
            None,
            true,
            false,
            false,
        );
        // NX: should not overwrite
        store.string_set(
            Bytes::from("k"),
            Bytes::from("v2"),
            None,
            true,
            false,
            false,
        );
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v1")),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_set_xx() {
        let mut store = ShardStore::new();
        // XX on nonexistent: should not set
        store.string_set(Bytes::from("k"), Bytes::from("v"), None, false, true, false);
        assert!(matches!(
            store.string_get(&Bytes::from("k")),
            CommandResponse::Nil
        ));

        // Set first, then XX should update
        store.string_set(
            Bytes::from("k"),
            Bytes::from("v1"),
            None,
            false,
            false,
            false,
        );
        store.string_set(
            Bytes::from("k"),
            Bytes::from("v2"),
            None,
            false,
            true,
            false,
        );
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v2")),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_set_get_flag() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("old"),
            None,
            false,
            false,
            false,
        );
        let resp = store.string_set(
            Bytes::from("k"),
            Bytes::from("new"),
            None,
            false,
            false,
            true,
        );
        match resp {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("old")),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_set_with_expiry() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("v"),
            Some(5000),
            false,
            false,
            false,
        );
        let ttl = store.pttl(&Bytes::from("k"));
        assert!(ttl > 0 && ttl <= 5000);
    }

    #[test]
    fn test_setnx() {
        let mut store = ShardStore::new();
        assert!(matches!(
            store.string_setnx(Bytes::from("k"), Bytes::from("v1")),
            CommandResponse::Integer(1)
        ));
        assert!(matches!(
            store.string_setnx(Bytes::from("k"), Bytes::from("v2")),
            CommandResponse::Integer(0)
        ));
    }

    #[test]
    fn test_setex() {
        let mut store = ShardStore::new();
        store.string_setex(Bytes::from("k"), 10, Bytes::from("v"));
        let ttl = store.pttl(&Bytes::from("k"));
        assert!(ttl > 9000 && ttl <= 10000);
    }

    #[test]
    fn test_getset() {
        let mut store = ShardStore::new();
        let r = store.string_getset(Bytes::from("k"), Bytes::from("v1"));
        assert!(matches!(r, CommandResponse::Nil));
        let r = store.string_getset(Bytes::from("k"), Bytes::from("v2"));
        match r {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v1")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_getdel() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("v"),
            None,
            false,
            false,
            false,
        );
        let r = store.string_getdel(&Bytes::from("k"));
        match r {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("v")),
            _ => panic!("expected bulk"),
        }
        assert!(matches!(
            store.string_get(&Bytes::from("k")),
            CommandResponse::Nil
        ));
    }

    #[test]
    fn test_mget_mset() {
        let mut store = ShardStore::new();
        store.string_mset(vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
        ]);
        let r = store.string_mget(&[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
        match r {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 3);
                assert!(
                    matches!(&items[0], CommandResponse::BulkString(v) if v == &Bytes::from("1"))
                );
                assert!(
                    matches!(&items[1], CommandResponse::BulkString(v) if v == &Bytes::from("2"))
                );
                assert!(matches!(&items[2], CommandResponse::Nil));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_msetnx() {
        let mut store = ShardStore::new();
        let r = store.string_msetnx(vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("2")),
        ]);
        assert!(matches!(r, CommandResponse::Integer(1)));

        // Should fail because 'a' exists
        let r = store.string_msetnx(vec![
            (Bytes::from("a"), Bytes::from("3")),
            (Bytes::from("c"), Bytes::from("4")),
        ]);
        assert!(matches!(r, CommandResponse::Integer(0)));
        // 'c' should not exist
        assert!(matches!(
            store.string_get(&Bytes::from("c")),
            CommandResponse::Nil
        ));
    }

    #[test]
    fn test_incr_decr() {
        let mut store = ShardStore::new();
        let r = store.string_incr(&Bytes::from("k"));
        assert!(matches!(r, CommandResponse::Integer(1)));
        let r = store.string_incr(&Bytes::from("k"));
        assert!(matches!(r, CommandResponse::Integer(2)));
        let r = store.string_decr(&Bytes::from("k"));
        assert!(matches!(r, CommandResponse::Integer(1)));
    }

    #[test]
    fn test_incrby() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("10"),
            None,
            false,
            false,
            false,
        );
        let r = store.string_incrby(&Bytes::from("k"), 5);
        assert!(matches!(r, CommandResponse::Integer(15)));
    }

    #[test]
    fn test_decrby() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("10"),
            None,
            false,
            false,
            false,
        );
        let r = store.string_decrby(&Bytes::from("k"), 3);
        assert!(matches!(r, CommandResponse::Integer(7)));
    }

    #[test]
    fn test_incr_non_integer() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("abc"),
            None,
            false,
            false,
            false,
        );
        let r = store.string_incr(&Bytes::from("k"));
        assert!(matches!(r, CommandResponse::Error(_)));
    }

    #[test]
    fn test_incrbyfloat() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("10.5"),
            None,
            false,
            false,
            false,
        );
        let r = store.string_incrbyfloat(&Bytes::from("k"), 0.5);
        match r {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("11")),
            other => panic!("Expected bulk, got {:?}", other),
        }
    }

    #[test]
    fn test_append() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("hello"),
            None,
            false,
            false,
            false,
        );
        let r = store.string_append(&Bytes::from("k"), &Bytes::from(" world"));
        assert!(matches!(r, CommandResponse::Integer(11)));
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("hello world")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_append_nonexistent() {
        let mut store = ShardStore::new();
        let r = store.string_append(&Bytes::from("k"), &Bytes::from("hello"));
        assert!(matches!(r, CommandResponse::Integer(5)));
    }

    #[test]
    fn test_strlen() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("hello"),
            None,
            false,
            false,
            false,
        );
        assert!(matches!(
            store.string_strlen(&Bytes::from("k")),
            CommandResponse::Integer(5)
        ));
        assert!(matches!(
            store.string_strlen(&Bytes::from("x")),
            CommandResponse::Integer(0)
        ));
    }

    #[test]
    fn test_getrange() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("hello world"),
            None,
            false,
            false,
            false,
        );
        match store.string_getrange(&Bytes::from("k"), 0, 4) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("hello")),
            _ => panic!("expected bulk"),
        }
        // Negative indices
        match store.string_getrange(&Bytes::from("k"), -5, -1) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("world")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_setrange() {
        let mut store = ShardStore::new();
        store.string_set(
            Bytes::from("k"),
            Bytes::from("hello world"),
            None,
            false,
            false,
            false,
        );
        let r = store.string_setrange(&Bytes::from("k"), 6, &Bytes::from("Redis"));
        assert!(matches!(r, CommandResponse::Integer(11)));
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("hello Redis")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_setrange_with_padding() {
        let mut store = ShardStore::new();
        let r = store.string_setrange(&Bytes::from("k"), 5, &Bytes::from("hi"));
        assert!(matches!(r, CommandResponse::Integer(7)));
        match store.string_get(&Bytes::from("k")) {
            CommandResponse::BulkString(v) => {
                assert_eq!(v.len(), 7);
                assert_eq!(&v[5..], b"hi");
            }
            _ => panic!("expected bulk"),
        }
    }

    // ---- Bitmap tests ----

    #[test]
    fn test_setbit_getbit() {
        let mut store = ShardStore::new();
        // Set bit 7 (last bit of first byte) to 1
        let old = store.string_setbit(&Bytes::from("bm"), 7, 1);
        assert!(matches!(old, CommandResponse::Integer(0)));
        let bit = store.string_getbit(&Bytes::from("bm"), 7);
        assert!(matches!(bit, CommandResponse::Integer(1)));
        // Bit 0 should still be 0
        let bit = store.string_getbit(&Bytes::from("bm"), 0);
        assert!(matches!(bit, CommandResponse::Integer(0)));
    }

    #[test]
    fn test_setbit_auto_grow() {
        let mut store = ShardStore::new();
        // Set bit at offset 24 (byte 3) on non-existent key
        let old = store.string_setbit(&Bytes::from("bm"), 24, 1);
        assert!(matches!(old, CommandResponse::Integer(0)));
        // Underlying string should be 4 bytes
        match store.string_get(&Bytes::from("bm")) {
            CommandResponse::BulkString(v) => assert_eq!(v.len(), 4),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_getbit_missing_key() {
        let mut store = ShardStore::new();
        let bit = store.string_getbit(&Bytes::from("nokey"), 100);
        assert!(matches!(bit, CommandResponse::Integer(0)));
    }

    #[test]
    fn test_bitcount() {
        let mut store = ShardStore::new();
        // 0xFF = 8 bits set
        store.string_set(
            Bytes::from("bm"),
            Bytes::from(vec![0xFF]),
            None,
            false,
            false,
            false,
        );
        let count = store.string_bitcount(&Bytes::from("bm"), None, None);
        assert!(matches!(count, CommandResponse::Integer(8)));
    }

    #[test]
    fn test_bitcount_range() {
        let mut store = ShardStore::new();
        // Two bytes: 0xFF (8 bits), 0x0F (4 bits)
        store.string_set(
            Bytes::from("bm"),
            Bytes::from(vec![0xFF, 0x0F]),
            None,
            false,
            false,
            false,
        );
        // Only count byte 1
        let count = store.string_bitcount(&Bytes::from("bm"), Some(1), Some(1));
        assert!(matches!(count, CommandResponse::Integer(4)));
        // Negative index: last byte
        let count = store.string_bitcount(&Bytes::from("bm"), Some(-1), Some(-1));
        assert!(matches!(count, CommandResponse::Integer(4)));
    }

    #[test]
    fn test_bitop_and() {
        let mut store = ShardStore::new();
        let result = store.string_bitop(
            Bytes::from("dest"),
            "AND",
            &[vec![0xFF, 0x0F], vec![0x0F, 0xFF]],
        );
        assert!(matches!(result, CommandResponse::Integer(2)));
        match store.string_get(&Bytes::from("dest")) {
            CommandResponse::BulkString(v) => assert_eq!(v.as_ref(), &[0x0F, 0x0F]),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_bitop_or() {
        let mut store = ShardStore::new();
        let result = store.string_bitop(Bytes::from("dest"), "OR", &[vec![0xF0], vec![0x0F]]);
        assert!(matches!(result, CommandResponse::Integer(1)));
        match store.string_get(&Bytes::from("dest")) {
            CommandResponse::BulkString(v) => assert_eq!(v.as_ref(), &[0xFF]),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_bitop_xor() {
        let mut store = ShardStore::new();
        let result = store.string_bitop(Bytes::from("dest"), "XOR", &[vec![0xFF], vec![0xFF]]);
        assert!(matches!(result, CommandResponse::Integer(1)));
        match store.string_get(&Bytes::from("dest")) {
            CommandResponse::BulkString(v) => assert_eq!(v.as_ref(), &[0x00]),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_bitop_not() {
        let mut store = ShardStore::new();
        let result = store.string_bitop(Bytes::from("dest"), "NOT", &[vec![0xF0]]);
        assert!(matches!(result, CommandResponse::Integer(1)));
        match store.string_get(&Bytes::from("dest")) {
            CommandResponse::BulkString(v) => assert_eq!(v.as_ref(), &[0x0F]),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_setbit_wrongtype() {
        let mut store = ShardStore::new();
        // Create a list key
        store.list_lpush(&Bytes::from("mylist"), vec![Bytes::from("a")]);
        let r = store.string_setbit(&Bytes::from("mylist"), 0, 1);
        assert!(matches!(r, CommandResponse::Error(_)));
    }
}
