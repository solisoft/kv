use bytes::Bytes;
use std::io;

/// Decode a Redis intset blob into a list of decimal ASCII byte strings.
///
/// Format: encoding(u32 LE) + length(u32 LE) + sorted integers (LE).
/// encoding: 2 = i16, 4 = i32, 8 = i64.
pub fn decode_intset(data: &[u8]) -> io::Result<Vec<Bytes>> {
    if data.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "intset too short"));
    }

    let encoding = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let length = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let elem_size = encoding as usize;

    if elem_size != 2 && elem_size != 4 && elem_size != 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("intset: invalid encoding {encoding}"),
        ));
    }

    let expected = 8 + length * elem_size;
    if data.len() < expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("intset: expected {expected} bytes, got {}", data.len()),
        ));
    }

    let mut result = Vec::with_capacity(length);
    let mut offset = 8;

    for _ in 0..length {
        let val: i64 = match elem_size {
            2 => i16::from_le_bytes([data[offset], data[offset + 1]]) as i64,
            4 => i32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as i64,
            8 => i64::from_le_bytes([
                data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
            ]),
            _ => unreachable!(),
        };
        result.push(Bytes::from(val.to_string()));
        offset += elem_size;
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intset_i16() {
        // encoding=2, length=3, values: 1, 2, 3
        let mut data = Vec::new();
        data.extend_from_slice(&2u32.to_le_bytes()); // encoding
        data.extend_from_slice(&3u32.to_le_bytes()); // length
        data.extend_from_slice(&1i16.to_le_bytes());
        data.extend_from_slice(&2i16.to_le_bytes());
        data.extend_from_slice(&3i16.to_le_bytes());

        let result = decode_intset(&data).unwrap();
        assert_eq!(result, vec![Bytes::from("1"), Bytes::from("2"), Bytes::from("3")]);
    }

    #[test]
    fn test_intset_i32() {
        let mut data = Vec::new();
        data.extend_from_slice(&4u32.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&(-100i32).to_le_bytes());
        data.extend_from_slice(&100_000i32.to_le_bytes());

        let result = decode_intset(&data).unwrap();
        assert_eq!(result, vec![Bytes::from("-100"), Bytes::from("100000")]);
    }

    #[test]
    fn test_intset_i64() {
        let mut data = Vec::new();
        data.extend_from_slice(&8u32.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&i64::MAX.to_le_bytes());

        let result = decode_intset(&data).unwrap();
        assert_eq!(result, vec![Bytes::from(i64::MAX.to_string())]);
    }

    #[test]
    fn test_intset_empty() {
        let mut data = Vec::new();
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&0u32.to_le_bytes());

        let result = decode_intset(&data).unwrap();
        assert!(result.is_empty());
    }
}
