use bytes::Bytes;
use std::io;

/// Decode a Redis listpack blob into a list of byte strings.
///
/// Listpack format:
///   tot-bytes(u32 LE) + num-elements(u16 LE) + entries... + 0xFF
///
/// Each entry: encoding byte(s) + data + back-length varint
///
/// Encoding types:
///   0xxxxxxx                -> 7-bit unsigned int (0..127)
///   10xxxxxx + N bytes      -> string, 6-bit length (0..63)
///   110xxxxx + 1 byte       -> 13-bit signed int
///   1110xxxx + 1 byte       -> string, 12-bit length (0..4095)
///   11110000 + 4 bytes      -> string, 32-bit length
///   11110001 + 2 bytes      -> i16 LE
///   11110010 + 3 bytes      -> i24 LE
///   11110011 + 4 bytes      -> i32 LE
///   11110100 + 8 bytes      -> i64 LE
pub fn decode_listpack(data: &[u8]) -> io::Result<Vec<Bytes>> {
    if data.len() < 7 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack too short"));
    }

    let _tot_bytes = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let num_elements = u16::from_le_bytes([data[4], data[5]]) as usize;

    let mut pos = 6; // after header
    let mut entries = Vec::with_capacity(num_elements);

    loop {
        if pos >= data.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: unexpected end"));
        }

        // End marker
        if data[pos] == 0xFF {
            break;
        }

        let enc = data[pos];

        if enc & 0x80 == 0 {
            // 0xxxxxxx: 7-bit unsigned int
            let val = (enc & 0x7F) as u64;
            entries.push(Bytes::from(val.to_string()));
            pos += 1;
        } else if enc & 0xC0 == 0x80 {
            // 10xxxxxx: string with 6-bit length
            let len = (enc & 0x3F) as usize;
            pos += 1;
            if pos + len > data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: string overflows"));
            }
            entries.push(Bytes::copy_from_slice(&data[pos..pos + len]));
            pos += len;
        } else if enc & 0xE0 == 0xC0 {
            // 110xxxxx + 1 byte: 13-bit signed int
            if pos + 1 >= data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: truncated 13-bit int"));
            }
            let raw = ((enc as u16 & 0x1F) << 8) | (data[pos + 1] as u16);
            // Sign-extend from 13 bits
            let val = if raw & 0x1000 != 0 {
                (raw as i16) | !0x1FFF
            } else {
                raw as i16
            };
            entries.push(Bytes::from(val.to_string()));
            pos += 2;
        } else if enc & 0xF0 == 0xE0 {
            // 1110xxxx + 1 byte: string with 12-bit length
            if pos + 1 >= data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: truncated 12-bit len"));
            }
            let len = (((enc & 0x0F) as usize) << 8) | (data[pos + 1] as usize);
            pos += 2;
            if pos + len > data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: string overflows"));
            }
            entries.push(Bytes::copy_from_slice(&data[pos..pos + len]));
            pos += len;
        } else {
            match enc {
                0xF0 => {
                    // 32-bit length string
                    if pos + 5 > data.len() {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: truncated 32-bit len"));
                    }
                    let len = u32::from_le_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]]) as usize;
                    pos += 5;
                    if pos + len > data.len() {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: string overflows"));
                    }
                    entries.push(Bytes::copy_from_slice(&data[pos..pos + len]));
                    pos += len;
                }
                0xF1 => {
                    // i16 LE
                    if pos + 3 > data.len() {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: truncated i16"));
                    }
                    let val = i16::from_le_bytes([data[pos + 1], data[pos + 2]]);
                    entries.push(Bytes::from(val.to_string()));
                    pos += 3;
                }
                0xF2 => {
                    // i24 LE (3 bytes, sign-extended)
                    if pos + 4 > data.len() {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: truncated i24"));
                    }
                    let raw = (data[pos + 1] as i32)
                        | ((data[pos + 2] as i32) << 8)
                        | ((data[pos + 3] as i32) << 16);
                    let val = if raw & 0x800000 != 0 {
                        raw | !0xFFFFFF
                    } else {
                        raw
                    };
                    entries.push(Bytes::from(val.to_string()));
                    pos += 4;
                }
                0xF3 => {
                    // i32 LE
                    if pos + 5 > data.len() {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: truncated i32"));
                    }
                    let val = i32::from_le_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]]);
                    entries.push(Bytes::from(val.to_string()));
                    pos += 5;
                }
                0xF4 => {
                    // i64 LE
                    if pos + 9 > data.len() {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: truncated i64"));
                    }
                    let val = i64::from_le_bytes([
                        data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4],
                        data[pos + 5], data[pos + 6], data[pos + 7], data[pos + 8],
                    ]);
                    entries.push(Bytes::from(val.to_string()));
                    pos += 9;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("listpack: unknown encoding 0x{enc:02X}"),
                    ));
                }
            }
        }

        // Skip back-length varint: read bytes until high bit is 0
        loop {
            if pos >= data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "listpack: truncated back-length"));
            }
            let b = data[pos];
            pos += 1;
            if b & 0x80 == 0 {
                break;
            }
        }
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a simple listpack from entries. Each entry is either a small uint or a short string.
    fn build_listpack(items: &[ListpackItem]) -> Vec<u8> {
        let mut body = Vec::new();

        for item in items {
            let entry_start = body.len();

            match item {
                ListpackItem::Uint(v) => {
                    assert!(*v < 128);
                    body.push(*v as u8); // 0xxxxxxx encoding
                }
                ListpackItem::Str(s) => {
                    assert!(s.len() < 64);
                    body.push(0x80 | s.len() as u8); // 10xxxxxx encoding
                    body.extend_from_slice(s);
                }
            }

            // Back-length: entry size (encoding + data, excluding back-length itself)
            let entry_len = body.len() - entry_start;
            encode_backlen(&mut body, entry_len);
        }

        body.push(0xFF); // end marker

        let total = 6 + body.len();
        let mut data = Vec::new();
        data.extend_from_slice(&(total as u32).to_le_bytes()); // tot-bytes
        data.extend_from_slice(&(items.len() as u16).to_le_bytes()); // num-elements
        data.extend(body);

        data
    }

    enum ListpackItem<'a> {
        Uint(u64),
        Str(&'a [u8]),
    }

    fn encode_backlen(buf: &mut Vec<u8>, mut len: usize) {
        if len <= 127 {
            buf.push(len as u8);
            return;
        }
        loop {
            let byte = (len & 0x7F) as u8;
            len >>= 7;
            if len > 0 {
                buf.push(byte | 0x80);
            } else {
                buf.push(byte);
                break;
            }
        }
    }

    #[test]
    fn test_listpack_uints() {
        let lp = build_listpack(&[ListpackItem::Uint(0), ListpackItem::Uint(42), ListpackItem::Uint(127)]);
        let result = decode_listpack(&lp).unwrap();
        assert_eq!(result, vec![Bytes::from("0"), Bytes::from("42"), Bytes::from("127")]);
    }

    #[test]
    fn test_listpack_strings() {
        let lp = build_listpack(&[ListpackItem::Str(b"hello"), ListpackItem::Str(b"world")]);
        let result = decode_listpack(&lp).unwrap();
        assert_eq!(result, vec![Bytes::from("hello"), Bytes::from("world")]);
    }

    #[test]
    fn test_listpack_mixed() {
        let lp = build_listpack(&[ListpackItem::Str(b"name"), ListpackItem::Uint(99)]);
        let result = decode_listpack(&lp).unwrap();
        assert_eq!(result, vec![Bytes::from("name"), Bytes::from("99")]);
    }

    #[test]
    fn test_listpack_empty() {
        let lp = build_listpack(&[]);
        let result = decode_listpack(&lp).unwrap();
        assert!(result.is_empty());
    }
}
