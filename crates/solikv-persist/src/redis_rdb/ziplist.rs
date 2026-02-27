use bytes::Bytes;
use std::io;

/// Decode a Redis ziplist blob into a list of byte strings.
///
/// Ziplist format:
///   zlbytes(u32 LE) + zltail(u32 LE) + zllen(u16 LE) + entries... + 0xFF
///
/// Each entry:
///   prevlen (1 or 5 bytes) + encoding + data
///
/// String encodings:
///   00xxxxxx           -> 6-bit length string
///   01xxxxxx yyyyyyyy  -> 14-bit length string
///   10000000 + 4 bytes -> 32-bit BE length string
///
/// Integer encodings:
///   0xC0 -> i16 LE
///   0xD0 -> i32 LE
///   0xE0 -> i64 LE
///   0xF0 -> i24 LE (3 bytes)
///   0xFE -> i8
///   0xF1..=0xFD -> inline integer 0..12 (value = (byte - 0xF1))
pub fn decode_ziplist(data: &[u8]) -> io::Result<Vec<Bytes>> {
    if data.len() < 11 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist too short"));
    }

    // Header
    let _zlbytes = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let _zltail = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    let zllen = u16::from_le_bytes([data[8], data[9]]) as usize;

    let mut pos = 10; // after header
    let mut entries = Vec::with_capacity(zllen);

    loop {
        if pos >= data.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: unexpected end"));
        }

        // End marker
        if data[pos] == 0xFF {
            break;
        }

        // prevlen: 1 byte if < 254, else 5 bytes (0xFE + u32 LE)
        if data[pos] < 254 {
            pos += 1;
        } else {
            pos += 5;
        }

        if pos >= data.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: truncated entry"));
        }

        let enc = data[pos];

        match enc >> 6 {
            // String encodings
            0b00 => {
                // 6-bit length
                let len = (enc & 0x3F) as usize;
                pos += 1;
                if pos + len > data.len() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: string overflows"));
                }
                entries.push(Bytes::copy_from_slice(&data[pos..pos + len]));
                pos += len;
            }
            0b01 => {
                // 14-bit length
                if pos + 1 >= data.len() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: truncated 14-bit len"));
                }
                let len = (((enc & 0x3F) as usize) << 8) | (data[pos + 1] as usize);
                pos += 2;
                if pos + len > data.len() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: string overflows"));
                }
                entries.push(Bytes::copy_from_slice(&data[pos..pos + len]));
                pos += len;
            }
            0b10 => {
                // 32-bit BE length
                if pos + 5 > data.len() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: truncated 32-bit len"));
                }
                let len = u32::from_be_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]]) as usize;
                pos += 5;
                if pos + len > data.len() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: string overflows"));
                }
                entries.push(Bytes::copy_from_slice(&data[pos..pos + len]));
                pos += len;
            }
            0b11 => {
                // Integer encodings
                match enc {
                    0xC0 => {
                        // i16 LE
                        pos += 1;
                        if pos + 2 > data.len() {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: truncated i16"));
                        }
                        let val = i16::from_le_bytes([data[pos], data[pos + 1]]);
                        entries.push(Bytes::from(val.to_string()));
                        pos += 2;
                    }
                    0xD0 => {
                        // i32 LE
                        pos += 1;
                        if pos + 4 > data.len() {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: truncated i32"));
                        }
                        let val = i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                        entries.push(Bytes::from(val.to_string()));
                        pos += 4;
                    }
                    0xE0 => {
                        // i64 LE
                        pos += 1;
                        if pos + 8 > data.len() {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: truncated i64"));
                        }
                        let val = i64::from_le_bytes([
                            data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
                            data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
                        ]);
                        entries.push(Bytes::from(val.to_string()));
                        pos += 8;
                    }
                    0xF0 => {
                        // i24 LE (3 bytes, sign-extended)
                        pos += 1;
                        if pos + 3 > data.len() {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: truncated i24"));
                        }
                        let val = (data[pos] as i32)
                            | ((data[pos + 1] as i32) << 8)
                            | ((data[pos + 2] as i32) << 16);
                        // Sign-extend from 24 bits
                        let val = if val & 0x800000 != 0 {
                            val | !0xFFFFFF
                        } else {
                            val
                        };
                        entries.push(Bytes::from(val.to_string()));
                        pos += 3;
                    }
                    0xFE => {
                        // i8
                        pos += 1;
                        if pos >= data.len() {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "ziplist: truncated i8"));
                        }
                        let val = data[pos] as i8;
                        entries.push(Bytes::from(val.to_string()));
                        pos += 1;
                    }
                    b @ 0xF1..=0xFD => {
                        // Inline integer 0..12
                        let val = (b as i64) - 0xF1;
                        entries.push(Bytes::from(val.to_string()));
                        pos += 1;
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("ziplist: unknown integer encoding 0x{enc:02X}"),
                        ));
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal ziplist with string entries.
    fn build_ziplist(entries: &[&[u8]]) -> Vec<u8> {
        let mut body = Vec::new();
        let mut prev_len: usize = 0;

        for entry in entries {
            // prevlen
            if prev_len < 254 {
                body.push(prev_len as u8);
            } else {
                body.push(0xFE);
                body.extend_from_slice(&(prev_len as u32).to_le_bytes());
            }

            let entry_start = body.len();

            // encoding: 6-bit string length (entries must be < 64 bytes)
            assert!(entry.len() < 64);
            body.push(entry.len() as u8); // 00xxxxxx
            body.extend_from_slice(entry);

            prev_len = body.len() - entry_start;
        }

        body.push(0xFF); // end marker

        let total_len = 10 + body.len(); // header + body
        let tail_offset = if entries.is_empty() { 10 } else { total_len - 1 }; // points to last entry or end

        let mut data = Vec::new();
        data.extend_from_slice(&(total_len as u32).to_le_bytes()); // zlbytes
        data.extend_from_slice(&(tail_offset as u32).to_le_bytes()); // zltail
        data.extend_from_slice(&(entries.len() as u16).to_le_bytes()); // zllen
        data.extend(body);

        data
    }

    #[test]
    fn test_ziplist_strings() {
        let zl = build_ziplist(&[b"hello", b"world"]);
        let result = decode_ziplist(&zl).unwrap();
        assert_eq!(result, vec![Bytes::from("hello"), Bytes::from("world")]);
    }

    #[test]
    fn test_ziplist_empty() {
        let zl = build_ziplist(&[]);
        let result = decode_ziplist(&zl).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_ziplist_integer_i16() {
        // Build ziplist with a single i16 entry: value 42
        let mut data = Vec::new();
        let body_start = 10;

        // Entry: prevlen=0, encoding=0xC0, value=42 as i16 LE
        let mut body = Vec::new();
        body.push(0); // prevlen
        body.push(0xC0); // i16 encoding
        body.extend_from_slice(&42i16.to_le_bytes());
        body.push(0xFF); // end

        let total_len = body_start + body.len();
        data.extend_from_slice(&(total_len as u32).to_le_bytes());
        data.extend_from_slice(&(body_start as u32).to_le_bytes());
        data.extend_from_slice(&1u16.to_le_bytes());
        data.extend(body);

        let result = decode_ziplist(&data).unwrap();
        assert_eq!(result, vec![Bytes::from("42")]);
    }

    #[test]
    fn test_ziplist_inline_int() {
        // Build ziplist with inline integers 0 (0xF1) and 12 (0xFD)
        let mut body = Vec::new();
        body.push(0); // prevlen
        body.push(0xF1); // inline 0
        body.push(1); // prevlen of previous entry (1 byte for prevlen + 1 byte for encoding)
        body.push(0xFD); // inline 12
        body.push(0xFF); // end

        let total_len = 10 + body.len();
        let mut data = Vec::new();
        data.extend_from_slice(&(total_len as u32).to_le_bytes());
        data.extend_from_slice(&10u32.to_le_bytes());
        data.extend_from_slice(&2u16.to_le_bytes());
        data.extend(body);

        let result = decode_ziplist(&data).unwrap();
        assert_eq!(result, vec![Bytes::from("0"), Bytes::from("12")]);
    }
}
