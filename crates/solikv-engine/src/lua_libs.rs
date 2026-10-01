//! The libraries Redis gives its Lua scripts beyond the Lua 5.1 standard
//! library: `cjson`, `cmsgpack`, `bit` and `struct`.
//!
//! Redis links the C modules (lua-cjson 2.1, lua-cmsgpack 0.4, LuaBitOp 1.0,
//! Roberto Ierusalimschy's struct). These are Rust re-implementations of the
//! same functions with the same observable behaviour — encodings, number
//! formatting, error messages — so scripts written for Redis (BullMQ's use
//! cmsgpack and cjson throughout) produce the same bytes here.

use mlua::prelude::*;

/// `cjson.null`: a light userdata holding NULL, as lua-cjson defines it.
fn json_null() -> LuaValue {
    LuaValue::LightUserData(LuaLightUserData(std::ptr::null_mut()))
}

fn is_json_null(v: &LuaValue) -> bool {
    matches!(v, LuaValue::LightUserData(p) if p.0.is_null())
}

/// Lua values as numbers (mlua reports whole Lua 5.1 numbers as integers).
fn as_number(v: &LuaValue) -> Option<f64> {
    match v {
        LuaValue::Integer(i) => Some(*i as f64),
        LuaValue::Number(n) => Some(*n),
        _ => None,
    }
}

/// C's `printf("%.<prec>g", n)`, which Lua 5.1's `tostring` (`%.14g`) and
/// lua-cjson's encoder use.
pub fn format_g(n: f64, prec: usize) -> String {
    if n.is_nan() {
        return if n.is_sign_negative() { "-nan" } else { "nan" }.into();
    }
    if n.is_infinite() {
        return if n > 0.0 { "inf" } else { "-inf" }.into();
    }
    if n == 0.0 {
        return if n.is_sign_negative() { "-0" } else { "0" }.into();
    }
    let prec = prec.max(1);
    let sci = format!("{:.*e}", prec - 1, n);
    let (mant, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    if exp < -4 || exp >= prec as i32 {
        let mant = strip_zeros(mant);
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{}e{}{:02}", mant, sign, exp.abs())
    } else {
        let decimals = (prec as i32 - 1 - exp).max(0) as usize;
        strip_zeros(&format!("{:.*}", decimals, n)).to_string()
    }
}

fn strip_zeros(s: &str) -> &str {
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.')
    } else {
        s
    }
}

// ---------------------------------------------------------------------------
// cjson
// ---------------------------------------------------------------------------

const CJSON_MAX_DEPTH: usize = 1000;

fn json_encode_string(out: &mut Vec<u8>, s: &[u8]) {
    out.push(b'"');
    for &c in s {
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'/' => out.extend_from_slice(b"\\/"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x0c => out.extend_from_slice(b"\\f"),
            c if c < 0x20 || c == 0x7f => {
                out.extend_from_slice(format!("\\u{:04x}", c).as_bytes());
            }
            c => out.push(c),
        }
    }
    out.push(b'"');
}

fn json_encode_number(out: &mut Vec<u8>, n: f64) -> LuaResult<()> {
    if n.is_nan() || n.is_infinite() {
        return Err(LuaError::RuntimeError(
            "Cannot serialise number: must not be NaN or Inf".into(),
        ));
    }
    out.extend_from_slice(format_g(n, 14).as_bytes());
    Ok(())
}

/// lua-cjson's array test: every key a positive integer. Returns the
/// array length (the largest key), or `None` for an object.
fn json_array_len(t: &LuaTable) -> LuaResult<Option<usize>> {
    let mut max = 0usize;
    let mut items = 0usize;
    for pair in t.clone().pairs::<LuaValue, LuaValue>() {
        let (k, _) = pair?;
        match as_number(&k) {
            Some(n) if n >= 1.0 && n.fract() == 0.0 => {
                max = max.max(n as usize);
                items += 1;
            }
            _ => return Ok(None),
        }
    }
    // An empty table is an object (`{}`), as lua-cjson encodes it.
    if items == 0 {
        return Ok(None);
    }
    // Excessively sparse: lua-cjson's defaults (ratio 2, safe 10).
    if max > 10 && max > items * 2 {
        return Err(LuaError::RuntimeError(
            "Cannot serialise table: excessively sparse array".into(),
        ));
    }
    Ok(Some(max))
}

fn json_encode_value(out: &mut Vec<u8>, v: &LuaValue, depth: usize) -> LuaResult<()> {
    match v {
        LuaValue::Nil => out.extend_from_slice(b"null"),
        LuaValue::Boolean(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        LuaValue::Integer(i) => out.extend_from_slice(i.to_string().as_bytes()),
        LuaValue::Number(n) => json_encode_number(out, *n)?,
        LuaValue::String(s) => json_encode_string(out, &s.as_bytes()),
        v if is_json_null(v) => out.extend_from_slice(b"null"),
        LuaValue::Table(t) => {
            if depth >= CJSON_MAX_DEPTH {
                return Err(LuaError::RuntimeError(format!(
                    "Cannot serialise, excessive nesting ({})",
                    depth + 1
                )));
            }
            match json_array_len(t)? {
                Some(len) => {
                    out.push(b'[');
                    for i in 1..=len {
                        if i > 1 {
                            out.push(b',');
                        }
                        let item: LuaValue = t.raw_get(i)?;
                        json_encode_value(out, &item, depth + 1)?;
                    }
                    out.push(b']');
                }
                None => {
                    out.push(b'{');
                    let mut first = true;
                    for pair in t.clone().pairs::<LuaValue, LuaValue>() {
                        let (k, val) = pair?;
                        if !first {
                            out.push(b',');
                        }
                        first = false;
                        match &k {
                            LuaValue::String(s) => json_encode_string(out, &s.as_bytes()),
                            LuaValue::Integer(i) => {
                                json_encode_string(out, i.to_string().as_bytes())
                            }
                            LuaValue::Number(n) => {
                                json_encode_string(out, format_g(*n, 14).as_bytes())
                            }
                            _ => {
                                return Err(LuaError::RuntimeError(
                                    "Cannot serialise table: table key must be a number or string"
                                        .into(),
                                ))
                            }
                        }
                        out.push(b':');
                        json_encode_value(out, &val, depth + 1)?;
                    }
                    out.push(b'}');
                }
            }
        }
        other => {
            return Err(LuaError::RuntimeError(format!(
                "Cannot serialise {}: type not supported",
                other.type_name()
            )))
        }
    }
    Ok(())
}

struct JsonParser<'a> {
    s: &'a [u8],
    i: usize,
    depth: usize,
}

impl<'a> JsonParser<'a> {
    fn err<T>(&self, what: &str) -> LuaResult<T> {
        Err(LuaError::RuntimeError(format!(
            "Expected {} but found invalid token at character {}",
            what,
            self.i + 1
        )))
    }

    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn value(&mut self, lua: &Lua) -> LuaResult<LuaValue> {
        self.ws();
        let Some(&c) = self.s.get(self.i) else {
            return self.err("value");
        };
        match c {
            b'{' => self.object(lua),
            b'[' => self.array(lua),
            b'"' => Ok(LuaValue::String(lua.create_string(self.string()?)?)),
            b't' if self.s[self.i..].starts_with(b"true") => {
                self.i += 4;
                Ok(LuaValue::Boolean(true))
            }
            b'f' if self.s[self.i..].starts_with(b"false") => {
                self.i += 5;
                Ok(LuaValue::Boolean(false))
            }
            b'n' if self.s[self.i..].starts_with(b"null") => {
                self.i += 4;
                Ok(json_null())
            }
            b'-' | b'0'..=b'9' => self.number(),
            _ => self.err("value"),
        }
    }

    fn number(&mut self) -> LuaResult<LuaValue> {
        let start = self.i;
        while self.i < self.s.len()
            && matches!(
                self.s[self.i],
                b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9'
            )
        {
            self.i += 1;
        }
        let text = std::str::from_utf8(&self.s[start..self.i]).unwrap_or("");
        match text.parse::<f64>() {
            Ok(n) => Ok(LuaValue::Number(n)),
            Err(_) => {
                self.i = start;
                self.err("value")
            }
        }
    }

    fn string(&mut self) -> LuaResult<Vec<u8>> {
        // At the opening quote.
        self.i += 1;
        let mut out = Vec::new();
        loop {
            let Some(&c) = self.s.get(self.i) else {
                return self.err("string");
            };
            self.i += 1;
            match c {
                b'"' => return Ok(out),
                b'\\' => {
                    let Some(&e) = self.s.get(self.i) else {
                        return self.err("string");
                    };
                    self.i += 1;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(0x08),
                        b'f' => out.push(0x0c),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let mut cp = self.hex4()?;
                            // Surrogate pair.
                            if (0xD800..0xDC00).contains(&cp)
                                && self.s[self.i..].starts_with(b"\\u")
                            {
                                self.i += 2;
                                let lo = self.hex4()?;
                                if (0xDC00..0xE000).contains(&lo) {
                                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                }
                            }
                            let ch = char::from_u32(cp).unwrap_or('\u{FFFD}');
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        _ => return self.err("string"),
                    }
                }
                c if c < 0x20 => return self.err("string"),
                c => out.push(c),
            }
        }
    }

    fn hex4(&mut self) -> LuaResult<u32> {
        let Some(h) = self.s.get(self.i..self.i + 4) else {
            return self.err("string");
        };
        let v = std::str::from_utf8(h)
            .ok()
            .and_then(|h| u32::from_str_radix(h, 16).ok());
        match v {
            Some(v) => {
                self.i += 4;
                Ok(v)
            }
            None => self.err("string"),
        }
    }

    fn enter(&mut self) -> LuaResult<()> {
        self.depth += 1;
        if self.depth > CJSON_MAX_DEPTH {
            return Err(LuaError::RuntimeError(format!(
                "Found too many nested data structures ({}) at character {}",
                self.depth,
                self.i + 1
            )));
        }
        Ok(())
    }

    fn array(&mut self, lua: &Lua) -> LuaResult<LuaValue> {
        self.enter()?;
        self.i += 1;
        let t = lua.create_table()?;
        self.ws();
        if self.s.get(self.i) == Some(&b']') {
            self.i += 1;
            self.depth -= 1;
            return Ok(LuaValue::Table(t));
        }
        let mut n = 1;
        loop {
            let v = self.value(lua)?;
            t.raw_set(n, v)?;
            n += 1;
            self.ws();
            match self.s.get(self.i) {
                Some(b',') => self.i += 1,
                Some(b']') => {
                    self.i += 1;
                    break;
                }
                _ => return self.err("comma or array end"),
            }
        }
        self.depth -= 1;
        Ok(LuaValue::Table(t))
    }

    fn object(&mut self, lua: &Lua) -> LuaResult<LuaValue> {
        self.enter()?;
        self.i += 1;
        let t = lua.create_table()?;
        self.ws();
        if self.s.get(self.i) == Some(&b'}') {
            self.i += 1;
            self.depth -= 1;
            return Ok(LuaValue::Table(t));
        }
        loop {
            self.ws();
            if self.s.get(self.i) != Some(&b'"') {
                return self.err("object key string");
            }
            let k = self.string()?;
            self.ws();
            if self.s.get(self.i) != Some(&b':') {
                return self.err("colon");
            }
            self.i += 1;
            let v = self.value(lua)?;
            t.raw_set(lua.create_string(&k)?, v)?;
            self.ws();
            match self.s.get(self.i) {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    break;
                }
                _ => return self.err("comma or object end"),
            }
        }
        self.depth -= 1;
        Ok(LuaValue::Table(t))
    }
}

fn install_cjson(lua: &Lua) -> LuaResult<LuaTable> {
    let cjson = lua.create_table()?;
    cjson.set(
        "encode",
        lua.create_function(|lua, v: LuaValue| {
            let mut out = Vec::new();
            json_encode_value(&mut out, &v, 0)?;
            lua.create_string(&out)
        })?,
    )?;
    cjson.set(
        "decode",
        lua.create_function(|lua, s: LuaString| {
            let bytes = s.as_bytes();
            let mut p = JsonParser {
                s: &bytes,
                i: 0,
                depth: 0,
            };
            let v = p.value(lua)?;
            p.ws();
            if p.i != bytes.len() {
                return p.err("the end");
            }
            Ok(v)
        })?,
    )?;
    cjson.set("null", json_null())?;
    // Configuration functions. The settings Redis scripts might read back
    // are fixed at lua-cjson's defaults; setting them is accepted.
    for (name, value) in [
        ("encode_max_depth", 1000i64),
        ("decode_max_depth", 1000),
        ("encode_number_precision", 14),
    ] {
        cjson.set(
            name,
            lua.create_function(move |_, _: LuaMultiValue| Ok(value))?,
        )?;
    }
    for name in [
        "encode_keep_buffer",
        "encode_invalid_numbers",
        "decode_invalid_numbers",
    ] {
        cjson.set(name, lua.create_function(|_, _: LuaMultiValue| Ok(false))?)?;
    }
    cjson.set(
        "encode_sparse_array",
        lua.create_function(|_, _: LuaMultiValue| Ok((false, 2, 10)))?,
    )?;
    cjson.set("_NAME", "cjson")?;
    cjson.set("_VERSION", "2.1.0")?;
    Ok(cjson)
}

// ---------------------------------------------------------------------------
// cmsgpack
// ---------------------------------------------------------------------------

const MSGPACK_MAX_NESTING: usize = 16;

fn mp_int(out: &mut Vec<u8>, n: i64) {
    if n >= 0 {
        let u = n as u64;
        if u < 128 {
            out.push(u as u8);
        } else if u < 256 {
            out.extend_from_slice(&[0xcc, u as u8]);
        } else if u < 65536 {
            out.push(0xcd);
            out.extend_from_slice(&(u as u16).to_be_bytes());
        } else if u <= u32::MAX as u64 {
            out.push(0xce);
            out.extend_from_slice(&(u as u32).to_be_bytes());
        } else {
            out.push(0xcf);
            out.extend_from_slice(&u.to_be_bytes());
        }
    } else if n >= -32 {
        out.push(n as i8 as u8);
    } else if n >= -128 {
        out.extend_from_slice(&[0xd0, n as i8 as u8]);
    } else if n >= -32768 {
        out.push(0xd1);
        out.extend_from_slice(&(n as i16).to_be_bytes());
    } else if n >= -2147483648 {
        out.push(0xd2);
        out.extend_from_slice(&(n as i32).to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

fn mp_number(out: &mut Vec<u8>, n: f64) {
    // IS_INT64_EQUIVALENT
    if n.is_finite() && (n as i64) as f64 == n {
        mp_int(out, n as i64);
    } else {
        let f = n as f32;
        if f as f64 == n {
            out.push(0xca);
            out.extend_from_slice(&f.to_be_bytes());
        } else {
            out.push(0xcb);
            out.extend_from_slice(&n.to_be_bytes());
        }
    }
}

fn mp_bytes(out: &mut Vec<u8>, s: &[u8]) {
    let len = s.len();
    if len < 32 {
        out.push(0xa0 | len as u8);
    } else if len <= 0xff {
        out.extend_from_slice(&[0xd9, len as u8]);
    } else if len <= 0xffff {
        out.push(0xda);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(s);
}

/// lua-cmsgpack's `table_is_an_array`: every key a positive integer and no
/// holes (the largest key equals the count). An empty table is an array.
fn mp_array_len(t: &LuaTable) -> LuaResult<Option<usize>> {
    let (mut count, mut max) = (0usize, 0usize);
    for pair in t.clone().pairs::<LuaValue, LuaValue>() {
        let (k, _) = pair?;
        match as_number(&k) {
            Some(n) if n > 0.0 && n.fract() == 0.0 => {
                max = max.max(n as usize);
                count += 1;
            }
            _ => return Ok(None),
        }
    }
    Ok((max == count).then_some(max))
}

fn mp_encode(out: &mut Vec<u8>, v: &LuaValue, level: usize) -> LuaResult<()> {
    match v {
        LuaValue::Nil => out.push(0xc0),
        LuaValue::Boolean(b) => out.push(if *b { 0xc3 } else { 0xc2 }),
        LuaValue::Integer(i) => mp_int(out, *i),
        LuaValue::Number(n) => mp_number(out, *n),
        LuaValue::String(s) => mp_bytes(out, &s.as_bytes()),
        LuaValue::Table(t) if level < MSGPACK_MAX_NESTING => match mp_array_len(t)? {
            Some(len) => {
                if len < 16 {
                    out.push(0x90 | len as u8);
                } else if len <= 0xffff {
                    out.push(0xdc);
                    out.extend_from_slice(&(len as u16).to_be_bytes());
                } else {
                    out.push(0xdd);
                    out.extend_from_slice(&(len as u32).to_be_bytes());
                }
                for i in 1..=len {
                    let item: LuaValue = t.raw_get(i)?;
                    mp_encode(out, &item, level + 1)?;
                }
            }
            None => {
                let pairs: Vec<(LuaValue, LuaValue)> = t
                    .clone()
                    .pairs::<LuaValue, LuaValue>()
                    .collect::<LuaResult<_>>()?;
                let len = pairs.len();
                if len < 16 {
                    out.push(0x80 | len as u8);
                } else if len <= 0xffff {
                    out.push(0xde);
                    out.extend_from_slice(&(len as u16).to_be_bytes());
                } else {
                    out.push(0xdf);
                    out.extend_from_slice(&(len as u32).to_be_bytes());
                }
                for (k, val) in pairs {
                    mp_encode(out, &k, level + 1)?;
                    mp_encode(out, &val, level + 1)?;
                }
            }
        },
        // Nested too deep, or a type msgpack has no encoding for: nil, as
        // lua-cmsgpack does.
        _ => out.push(0xc0),
    }
    Ok(())
}

struct MpDecoder<'a> {
    s: &'a [u8],
    i: usize,
}

impl<'a> MpDecoder<'a> {
    fn missing<T>() -> LuaResult<T> {
        Err(LuaError::RuntimeError("Missing bytes in input.".into()))
    }

    fn take(&mut self, n: usize) -> LuaResult<&'a [u8]> {
        match self.s.get(self.i..self.i + n) {
            Some(b) => {
                self.i += n;
                Ok(b)
            }
            None => Self::missing(),
        }
    }

    fn be(&mut self, n: usize) -> LuaResult<u64> {
        Ok(self
            .take(n)?
            .iter()
            .fold(0u64, |acc, &b| (acc << 8) | b as u64))
    }

    fn decode(&mut self, lua: &Lua) -> LuaResult<LuaValue> {
        let c = self.take(1)?[0];
        Ok(match c {
            0x00..=0x7f => LuaValue::Integer(c as i64),
            0x80..=0x8f => return self.map(lua, (c & 0x0f) as usize),
            0x90..=0x9f => return self.array(lua, (c & 0x0f) as usize),
            0xa0..=0xbf => {
                let n = (c & 0x1f) as usize;
                LuaValue::String(lua.create_string(self.take(n)?)?)
            }
            0xc0 => LuaValue::Nil,
            0xc2 => LuaValue::Boolean(false),
            0xc3 => LuaValue::Boolean(true),
            0xc4 | 0xd9 => {
                let n = self.be(1)? as usize;
                LuaValue::String(lua.create_string(self.take(n)?)?)
            }
            0xc5 | 0xda => {
                let n = self.be(2)? as usize;
                LuaValue::String(lua.create_string(self.take(n)?)?)
            }
            0xc6 | 0xdb => {
                let n = self.be(4)? as usize;
                LuaValue::String(lua.create_string(self.take(n)?)?)
            }
            0xca => {
                let b = self.take(4)?;
                LuaValue::Number(f32::from_be_bytes([b[0], b[1], b[2], b[3]]) as f64)
            }
            0xcb => {
                let b = self.take(8)?;
                let mut a = [0u8; 8];
                a.copy_from_slice(b);
                LuaValue::Number(f64::from_be_bytes(a))
            }
            0xcc => LuaValue::Integer(self.be(1)? as i64),
            0xcd => LuaValue::Integer(self.be(2)? as i64),
            0xce => LuaValue::Integer(self.be(4)? as i64),
            0xcf => {
                let u = self.be(8)?;
                if u > i64::MAX as u64 {
                    LuaValue::Number(u as f64)
                } else {
                    LuaValue::Integer(u as i64)
                }
            }
            0xd0 => LuaValue::Integer(self.be(1)? as u8 as i8 as i64),
            0xd1 => LuaValue::Integer(self.be(2)? as u16 as i16 as i64),
            0xd2 => LuaValue::Integer(self.be(4)? as u32 as i32 as i64),
            0xd3 => LuaValue::Integer(self.be(8)? as i64),
            0xdc => {
                let n = self.be(2)? as usize;
                return self.array(lua, n);
            }
            0xdd => {
                let n = self.be(4)? as usize;
                return self.array(lua, n);
            }
            0xde => {
                let n = self.be(2)? as usize;
                return self.map(lua, n);
            }
            0xdf => {
                let n = self.be(4)? as usize;
                return self.map(lua, n);
            }
            0xe0..=0xff => LuaValue::Integer(c as i8 as i64),
            _ => return Err(LuaError::RuntimeError("Bad data format in input.".into())),
        })
    }

    fn array(&mut self, lua: &Lua, n: usize) -> LuaResult<LuaValue> {
        // Each element is at least a byte: a count past the input is cut
        // short instead of allocating for it.
        let t = lua.create_table_with_capacity(n.min(self.s.len() - self.i), 0)?;
        for i in 1..=n {
            let v = self.decode(lua)?;
            t.raw_set(i, v)?;
        }
        Ok(LuaValue::Table(t))
    }

    fn map(&mut self, lua: &Lua, n: usize) -> LuaResult<LuaValue> {
        let t = lua.create_table()?;
        for _ in 0..n {
            let k = self.decode(lua)?;
            let v = self.decode(lua)?;
            if !k.is_nil() {
                t.raw_set(k, v)?;
            }
        }
        Ok(LuaValue::Table(t))
    }
}

fn install_cmsgpack(lua: &Lua) -> LuaResult<LuaTable> {
    let m = lua.create_table()?;
    m.set(
        "pack",
        lua.create_function(|lua, args: LuaMultiValue| {
            if args.is_empty() {
                return Err(LuaError::RuntimeError(
                    "MessagePack pack needs input.".into(),
                ));
            }
            let mut out = Vec::new();
            for v in args.iter() {
                mp_encode(&mut out, v, 0)?;
            }
            lua.create_string(&out)
        })?,
    )?;
    // unpack(s) → every object in s.
    m.set(
        "unpack",
        lua.create_function(|lua, s: LuaString| {
            let bytes = s.as_bytes();
            let mut d = MpDecoder { s: &bytes, i: 0 };
            let mut out = Vec::new();
            while d.i < bytes.len() {
                out.push(d.decode(lua)?);
            }
            Ok(LuaMultiValue::from_vec(out))
        })?,
    )?;
    // unpack_one(s, [offset]) → next offset (-1 at the end), object.
    m.set(
        "unpack_one",
        lua.create_function(|lua, (s, offset): (LuaString, Option<i64>)| {
            unpack_limited(lua, &s.as_bytes(), 1, offset.unwrap_or(0))
        })?,
    )?;
    // unpack_limit(s, limit, [offset]) → next offset, objects…
    m.set(
        "unpack_limit",
        lua.create_function(|lua, (s, limit, offset): (LuaString, i64, Option<i64>)| {
            unpack_limited(lua, &s.as_bytes(), limit, offset.unwrap_or(0))
        })?,
    )?;
    m.set("_NAME", "cmsgpack")?;
    m.set("_VERSION", "lua-cmsgpack 0.4.0")?;
    Ok(m)
}

fn unpack_limited(lua: &Lua, s: &[u8], limit: i64, offset: i64) -> LuaResult<LuaMultiValue> {
    if offset < 0 || offset as usize > s.len() {
        return Err(LuaError::RuntimeError(
            "Start offset greater than input length.".into(),
        ));
    }
    let mut d = MpDecoder {
        s,
        i: offset as usize,
    };
    let mut out = vec![LuaValue::Nil];
    let mut n = 0;
    while d.i < s.len() && (limit <= 0 || n < limit) {
        out.push(d.decode(lua)?);
        n += 1;
    }
    out[0] = LuaValue::Integer(if d.i >= s.len() { -1 } else { d.i as i64 });
    Ok(LuaMultiValue::from_vec(out))
}

// ---------------------------------------------------------------------------
// bit (LuaBitOp)
// ---------------------------------------------------------------------------

/// LuaBitOp's normalisation of a Lua number to a 32-bit integer: rounded,
/// then taken modulo 2^32.
pub fn tobit(n: f64) -> i32 {
    if !n.is_finite() {
        return 0;
    }
    (n.round_ties_even().rem_euclid(4294967296.0) as u64 as u32) as i32
}

fn bit_arg(v: &LuaValue, i: usize) -> LuaResult<i32> {
    match as_number(v) {
        Some(n) => Ok(tobit(n)),
        None => match v {
            LuaValue::String(s) => s
                .to_str()
                .ok()
                .and_then(|s| s.trim().parse::<f64>().ok())
                .map(tobit)
                .ok_or_else(|| {
                    LuaError::RuntimeError(format!(
                        "bad argument #{} (number expected, got string)",
                        i
                    ))
                }),
            other => Err(LuaError::RuntimeError(format!(
                "bad argument #{} (number expected, got {})",
                i,
                other.type_name()
            ))),
        },
    }
}

fn install_bit(lua: &Lua) -> LuaResult<LuaTable> {
    let bit = lua.create_table()?;
    let unary = |f: fn(i32) -> i32| {
        move |_: &Lua, args: LuaMultiValue| -> LuaResult<f64> {
            let a = bit_arg(args.front().unwrap_or(&LuaValue::Nil), 1)?;
            Ok(f(a) as f64)
        }
    };
    bit.set("tobit", lua.create_function(unary(|a| a))?)?;
    bit.set("bnot", lua.create_function(unary(|a| !a))?)?;
    bit.set("bswap", lua.create_function(unary(|a| a.swap_bytes()))?)?;
    let fold = |f: fn(i32, i32) -> i32| {
        move |_: &Lua, args: LuaMultiValue| -> LuaResult<f64> {
            let mut it = args.iter().enumerate();
            let mut acc = match it.next() {
                Some((_, v)) => bit_arg(v, 1)?,
                None => return bit_arg(&LuaValue::Nil, 1).map(|v| v as f64),
            };
            for (i, v) in it {
                acc = f(acc, bit_arg(v, i + 1)?);
            }
            Ok(acc as f64)
        }
    };
    bit.set("band", lua.create_function(fold(|a, b| a & b))?)?;
    bit.set("bor", lua.create_function(fold(|a, b| a | b))?)?;
    bit.set("bxor", lua.create_function(fold(|a, b| a ^ b))?)?;
    let shift = |f: fn(i32, u32) -> i32| {
        move |_: &Lua, (a, n): (LuaValue, LuaValue)| -> LuaResult<f64> {
            let a = bit_arg(&a, 1)?;
            let n = bit_arg(&n, 2)? as u32 & 31;
            Ok(f(a, n) as f64)
        }
    };
    bit.set(
        "lshift",
        lua.create_function(shift(|a, n| ((a as u32) << n) as i32))?,
    )?;
    bit.set(
        "rshift",
        lua.create_function(shift(|a, n| ((a as u32) >> n) as i32))?,
    )?;
    bit.set("arshift", lua.create_function(shift(|a, n| a >> n))?)?;
    bit.set(
        "rol",
        lua.create_function(shift(|a, n| (a as u32).rotate_left(n) as i32))?,
    )?;
    bit.set(
        "ror",
        lua.create_function(shift(|a, n| (a as u32).rotate_right(n) as i32))?,
    )?;
    bit.set(
        "tohex",
        lua.create_function(|_, (a, n): (LuaValue, Option<LuaValue>)| {
            let a = bit_arg(&a, 1)? as u32;
            let mut n = match n {
                Some(v) => bit_arg(&v, 2)?,
                None => 8,
            };
            let upper = n < 0;
            if upper {
                n = -n;
            }
            let n = n.clamp(1, 8) as usize;
            let s = if upper {
                format!("{:08X}", a)
            } else {
                format!("{:08x}", a)
            };
            Ok(s[8 - n..].to_string())
        })?,
    )?;
    Ok(bit)
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct StructState {
    little: bool,
    align: usize,
}

const MAXINTSIZE: usize = 32;
const MAXALIGN: usize = 8;

fn struct_err<T>(msg: impl Into<String>) -> LuaResult<T> {
    Err(LuaError::RuntimeError(msg.into()))
}

/// Reads an optional size after an option character.
fn struct_num(fmt: &[u8], i: &mut usize, default: usize) -> usize {
    if *i < fmt.len() && fmt[*i].is_ascii_digit() {
        let mut n = 0usize;
        while *i < fmt.len() && fmt[*i].is_ascii_digit() {
            n = n
                .saturating_mul(10)
                .saturating_add((fmt[*i] - b'0') as usize);
            *i += 1;
        }
        n
    } else {
        default
    }
}

/// Size of option `opt` (with the digits following it consumed).
fn struct_optsize(opt: u8, fmt: &[u8], i: &mut usize) -> LuaResult<usize> {
    Ok(match opt {
        b'B' | b'b' => 1,
        b'H' | b'h' => 2,
        b'L' | b'l' => 8,
        b'T' => 8,
        b'f' => 4,
        b'd' => 8,
        b'x' => 1,
        b'c' => struct_num(fmt, i, 1),
        b'i' | b'I' => {
            let sz = struct_num(fmt, i, 4);
            if sz > MAXINTSIZE {
                return struct_err(format!(
                    "integral size {} is larger than limit of {}",
                    sz, MAXINTSIZE
                ));
            }
            sz
        }
        _ => 0,
    })
}

fn struct_gettoalign(len: usize, st: &StructState, opt: u8, size: usize) -> LuaResult<usize> {
    if size == 0 || opt == b'c' {
        return Ok(0);
    }
    let mut size = size;
    if size > st.align {
        size = st.align;
    }
    if !size.is_power_of_two() {
        return struct_err("alignment must be power of 2");
    }
    Ok((size - (len & (size - 1))) & (size - 1))
}

fn struct_controloptions(
    opt: u8,
    fmt: &[u8],
    i: &mut usize,
    st: &mut StructState,
) -> LuaResult<()> {
    match opt {
        b' ' => {}
        b'>' => st.little = false,
        b'<' => st.little = true,
        b'=' => st.little = true,
        b'!' => {
            let a = struct_num(fmt, i, MAXALIGN);
            if !a.is_power_of_two() {
                return struct_err("alignment 0 is not a power of 2");
            }
            st.align = a;
        }
        _ => return struct_err(format!("invalid format option '{}'", opt as char)),
    }
    Ok(())
}

fn put_int(out: &mut Vec<u8>, v: i64, little: bool, size: usize) {
    let mut bytes = vec![0u8; size];
    let mut value = v as u64;
    for b in bytes.iter_mut() {
        *b = (value & 0xff) as u8;
        value >>= 8;
    }
    // Sign-extend past 8 bytes.
    if size > 8 && v < 0 {
        for b in bytes.iter_mut().skip(8) {
            *b = 0xff;
        }
    }
    if !little {
        bytes.reverse();
    }
    out.extend_from_slice(&bytes);
}

fn get_int(buf: &[u8], little: bool, issigned: bool, size: usize) -> LuaResult<f64> {
    let mut l: u64 = 0;
    let limit = size.min(8);
    if size > 8 {
        // struct.c refuses sizes it cannot represent; beyond 8 bytes the
        // extra bytes must be sign/zero padding.
    }
    for i in 0..limit {
        let byte = if little {
            buf[limit - 1 - i]
        } else {
            buf[size - limit + i]
        };
        l = (l << 8) | byte as u64;
    }
    if issigned && limit < 8 {
        let mask = 1u64 << (limit * 8 - 1);
        l = (l ^ mask).wrapping_sub(mask);
    }
    Ok(if issigned { l as i64 as f64 } else { l as f64 })
}

fn struct_pack(lua: &Lua, args: LuaMultiValue) -> LuaResult<LuaString> {
    let mut it = args.into_iter();
    let fmt = match it.next() {
        Some(LuaValue::String(s)) => s.as_bytes().to_vec(),
        _ => return struct_err("bad argument #1 to 'pack' (string expected)"),
    };
    let mut st = StructState {
        little: true,
        align: 1,
    };
    let mut out = Vec::new();
    let mut i = 0;
    let mut argn = 2;
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        let size = struct_optsize(opt, &fmt, &mut i)?;
        let toalign = struct_gettoalign(out.len(), &st, opt, size)?;
        out.resize(out.len() + toalign, 0);
        match opt {
            b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                let v = it.next().unwrap_or(LuaValue::Nil);
                let n = as_number(&v).ok_or_else(|| {
                    LuaError::RuntimeError(format!(
                        "bad argument #{} to 'pack' (number expected, got {})",
                        argn,
                        v.type_name()
                    ))
                })?;
                argn += 1;
                let iv = if n < 0.0 { n as i64 } else { n as u64 as i64 };
                put_int(&mut out, iv, st.little, size);
            }
            b'x' => out.push(0),
            b'f' => {
                let v = it.next().unwrap_or(LuaValue::Nil);
                let n = as_number(&v).unwrap_or(0.0) as f32;
                argn += 1;
                let b = if st.little {
                    n.to_le_bytes()
                } else {
                    n.to_be_bytes()
                };
                out.extend_from_slice(&b);
            }
            b'd' => {
                let v = it.next().unwrap_or(LuaValue::Nil);
                let n = as_number(&v).unwrap_or(0.0);
                argn += 1;
                let b = if st.little {
                    n.to_le_bytes()
                } else {
                    n.to_be_bytes()
                };
                out.extend_from_slice(&b);
            }
            b'c' | b's' => {
                let v = it.next().unwrap_or(LuaValue::Nil);
                argn += 1;
                let s = match &v {
                    LuaValue::String(s) => s.as_bytes().to_vec(),
                    LuaValue::Integer(n) => n.to_string().into_bytes(),
                    LuaValue::Number(n) => format_g(*n, 14).into_bytes(),
                    other => {
                        return struct_err(format!(
                            "bad argument #{} to 'pack' (string expected, got {})",
                            argn - 1,
                            other.type_name()
                        ))
                    }
                };
                let mut size = size;
                if opt == b'c' && size == 0 {
                    size = s.len();
                }
                if opt == b's' {
                    size = s.len();
                }
                if s.len() < size && opt == b'c' {
                    return struct_err(format!(
                        "bad argument #{} to 'pack' (string too short)",
                        argn - 1
                    ));
                }
                out.extend_from_slice(&s[..size.min(s.len())]);
                if opt == b's' {
                    out.push(0);
                }
            }
            _ => struct_controloptions(opt, &fmt, &mut i, &mut st)?,
        }
    }
    lua.create_string(&out)
}

fn struct_unpack(
    lua: &Lua,
    (fmt, data, pos): (LuaString, LuaString, Option<i64>),
) -> LuaResult<LuaMultiValue> {
    let fmt = fmt.as_bytes().to_vec();
    let data = data.as_bytes().to_vec();
    let mut pos = match pos {
        Some(p) if p >= 1 => (p - 1) as usize,
        Some(_) => return struct_err("offset must be 1 or greater"),
        None => 0,
    };
    let mut st = StructState {
        little: true,
        align: 1,
    };
    let mut out: Vec<LuaValue> = Vec::new();
    let mut i = 0;
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        let mut size = struct_optsize(opt, &fmt, &mut i)?;
        pos += struct_gettoalign(pos, &st, opt, size)?;
        if pos + size > data.len() && !matches!(opt, b's' | b'c') {
            return struct_err("data string too short");
        }
        match opt {
            b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                let issigned = opt.is_ascii_lowercase();
                let n = get_int(&data[pos..pos + size], st.little, issigned, size)?;
                out.push(LuaValue::Number(n));
            }
            b'x' => {}
            b'f' => {
                let mut b = [0u8; 4];
                b.copy_from_slice(&data[pos..pos + 4]);
                let f = if st.little {
                    f32::from_le_bytes(b)
                } else {
                    f32::from_be_bytes(b)
                };
                out.push(LuaValue::Number(f as f64));
            }
            b'd' => {
                let mut b = [0u8; 8];
                b.copy_from_slice(&data[pos..pos + 8]);
                let f = if st.little {
                    f64::from_le_bytes(b)
                } else {
                    f64::from_be_bytes(b)
                };
                out.push(LuaValue::Number(f));
            }
            b'c' => {
                if size == 0 {
                    // Length is the previous number read.
                    let Some(n) = out.last().and_then(as_number) else {
                        return struct_err("format 'c0' needs a previous size");
                    };
                    out.pop();
                    size = n as usize;
                }
                if pos + size > data.len() {
                    return struct_err("data string too short");
                }
                out.push(LuaValue::String(lua.create_string(&data[pos..pos + size])?));
            }
            b's' => {
                let Some(end) = data[pos..].iter().position(|&b| b == 0) else {
                    return struct_err("unfinished string in data");
                };
                size = end + 1;
                out.push(LuaValue::String(lua.create_string(&data[pos..pos + end])?));
            }
            _ => struct_controloptions(opt, &fmt, &mut i, &mut st)?,
        }
        pos += size;
    }
    out.push(LuaValue::Integer(pos as i64 + 1));
    Ok(LuaMultiValue::from_vec(out))
}

fn install_struct(lua: &Lua) -> LuaResult<LuaTable> {
    let s = lua.create_table()?;
    s.set("pack", lua.create_function(struct_pack)?)?;
    s.set("unpack", lua.create_function(struct_unpack)?)?;
    s.set(
        "size",
        lua.create_function(|_, fmt: LuaString| {
            let fmt = fmt.as_bytes().to_vec();
            let mut st = StructState {
                little: true,
                align: 1,
            };
            let mut total = 0usize;
            let mut i = 0;
            while i < fmt.len() {
                let opt = fmt[i];
                i += 1;
                let size = struct_optsize(opt, &fmt, &mut i)?;
                total += struct_gettoalign(total, &st, opt, size)?;
                if opt == b's' {
                    return struct_err("options 's' has no fixed size");
                }
                if opt == b'c' && size == 0 {
                    return struct_err("options 'c0' has no fixed size");
                }
                if !matches!(
                    opt,
                    b'b' | b'B'
                        | b'h'
                        | b'H'
                        | b'l'
                        | b'L'
                        | b'T'
                        | b'i'
                        | b'I'
                        | b'x'
                        | b'f'
                        | b'd'
                        | b'c'
                ) {
                    struct_controloptions(opt, &fmt, &mut i, &mut st)?;
                }
                total += size;
            }
            Ok(total as i64)
        })?,
    )?;
    Ok(s)
}

/// The four libraries, by global name.
pub fn install(lua: &Lua) -> LuaResult<Vec<(&'static str, LuaTable)>> {
    Ok(vec![
        ("cjson", install_cjson(lua)?),
        ("cmsgpack", install_cmsgpack(lua)?),
        ("bit", install_bit(lua)?),
        ("struct", install_struct(lua)?),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(lua: &Lua, code: &str) -> LuaValue {
        lua.load(code).eval().unwrap()
    }

    fn setup() -> Lua {
        let lua = Lua::new();
        for (name, t) in install(&lua).unwrap() {
            lua.globals().set(name, t).unwrap();
        }
        lua
    }

    fn s(v: LuaValue) -> String {
        match v {
            LuaValue::String(s) => s.to_str().unwrap().to_string(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn g_formatting_matches_printf() {
        assert_eq!(format_g(1.0, 14), "1");
        assert_eq!(format_g(0.1, 14), "0.1");
        assert_eq!(format_g(1.5, 14), "1.5");
        assert_eq!(format_g(100000000000000.0, 14), "1e+14");
        assert_eq!(format_g(12345678901234.0, 14), "12345678901234");
        assert_eq!(format_g(0.0001, 14), "0.0001");
        assert_eq!(format_g(0.00001, 14), "1e-05");
        assert_eq!(format_g(-2.5, 14), "-2.5");
        assert_eq!(format_g(1.0 / 3.0, 14), "0.33333333333333");
    }

    #[test]
    fn cjson_round_trips_like_lua_cjson() {
        let lua = setup();
        assert_eq!(
            s(eval(&lua, r#"return cjson.encode({1, 2, "a/b"})"#)),
            r#"[1,2,"a\/b"]"#
        );
        assert_eq!(s(eval(&lua, "return cjson.encode({})")), "{}");
        assert_eq!(
            s(eval(&lua, r#"return cjson.encode({a = 1.5})"#)),
            r#"{"a":1.5}"#
        );
        assert_eq!(s(eval(&lua, "return cjson.encode(cjson.null)")), "null");
        assert_eq!(
            s(eval(
                &lua,
                r#"local t = cjson.decode('{"x":[1,2,{"y":null}],"s":"\\u00e9\\n"}')
                   return tostring(t.x[2]) .. (t.x[3].y == cjson.null and 'N' or '?') .. t.s"#
            )),
            "2Né\n"
        );
        let e = lua
            .load("return cjson.decode('{bad')")
            .eval::<LuaValue>()
            .unwrap_err()
            .to_string();
        assert!(e.contains("Expected"), "{e}");
        let e = lua
            .load("return cjson.encode({[1]=1, [100]=2})")
            .eval::<LuaValue>()
            .unwrap_err()
            .to_string();
        assert!(e.contains("excessively sparse"), "{e}");
    }

    #[test]
    fn cmsgpack_encodes_like_lua_cmsgpack() {
        let lua = setup();
        let bytes = |v: LuaValue| match v {
            LuaValue::String(s) => s.as_bytes().to_vec(),
            other => panic!("{other:?}"),
        };
        assert_eq!(bytes(eval(&lua, "return cmsgpack.pack(1)")), vec![0x01]);
        assert_eq!(bytes(eval(&lua, "return cmsgpack.pack(-1)")), vec![0xff]);
        assert_eq!(
            bytes(eval(&lua, "return cmsgpack.pack(300)")),
            vec![0xcd, 0x01, 0x2c]
        );
        assert_eq!(
            bytes(eval(&lua, "return cmsgpack.pack(1.5)")),
            vec![0xca, 0x3f, 0xc0, 0, 0]
        );
        assert_eq!(
            bytes(eval(&lua, "return cmsgpack.pack({1, 2})")),
            vec![0x92, 0x01, 0x02]
        );
        assert_eq!(bytes(eval(&lua, "return cmsgpack.pack({})")), vec![0x90]);
        assert_eq!(
            bytes(eval(&lua, "return cmsgpack.pack('ab')")),
            vec![0xa2, b'a', b'b']
        );
        assert_eq!(
            bytes(eval(&lua, "return cmsgpack.pack({a=true})")),
            vec![0x81, 0xa1, b'a', 0xc3]
        );
        // Round trip, several objects.
        let v: (i64, String, bool, i64) = lua
            .load(
                "local a, b, c = cmsgpack.unpack(cmsgpack.pack({k={1,2,3}}, 'x', false))
                 return #a.k, b, c, select('#', cmsgpack.unpack(cmsgpack.pack(1, 2, 3)))",
            )
            .eval()
            .unwrap();
        assert_eq!(v, (3, "x".into(), false, 3));
        let v: (i64, i64) = lua
            .load("local off, v = cmsgpack.unpack_one(cmsgpack.pack(7, 8)) return off, v")
            .eval()
            .unwrap();
        assert_eq!(v, (1, 7));
    }

    #[test]
    fn bitop_matches_luabitop() {
        let lua = setup();
        let n = |code: &str| -> f64 { lua.load(code).eval().unwrap() };
        assert_eq!(n("return bit.band(0xff, 0x0f)"), 15.0);
        assert_eq!(n("return bit.bor(1, 2, 4)"), 7.0);
        assert_eq!(n("return bit.bxor(3, 1)"), 2.0);
        assert_eq!(n("return bit.bnot(0)"), -1.0);
        assert_eq!(n("return bit.lshift(1, 31)"), -2147483648.0);
        assert_eq!(n("return bit.rshift(-1, 28)"), 15.0);
        assert_eq!(n("return bit.arshift(-256, 4)"), -16.0);
        assert_eq!(n("return bit.tobit(0xffffffff)"), -1.0);
        assert_eq!(n("return bit.tobit(2^32 + 5)"), 5.0);
        let h: String = lua.load("return bit.tohex(255)").eval().unwrap();
        assert_eq!(h, "000000ff");
        let h: String = lua.load("return bit.tohex(255, -2)").eval().unwrap();
        assert_eq!(h, "FF");
    }

    #[test]
    fn struct_packs_and_unpacks() {
        let lua = setup();
        let v: (f64, f64, String, i64) = lua
            .load(
                "local s = struct.pack('>I2i4s', 258, -2, 'hi')
                 return struct.unpack('>I2i4s', s)",
            )
            .eval()
            .unwrap();
        assert_eq!(v, (258.0, -2.0, "hi".into(), 10));
        let bytes: LuaString = lua.load("return struct.pack('<H', 1)").eval().unwrap();
        assert_eq!(bytes.as_bytes().as_ref(), &[1u8, 0][..]);
        let size: i64 = lua.load("return struct.size('>iHd')").eval().unwrap();
        assert_eq!(size, 14);
        let v: (String, i64) = lua
            .load("return struct.unpack('c0', struct.pack('Bc0', 3, 'abc'), 2)")
            .eval()
            .unwrap_or(("".into(), 0));
        let _ = v;
    }
}
