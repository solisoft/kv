use bytes::Bytes;
use std::io::{self, BufReader, Read};

use super::lzf;

/// Result of `read_length_encoding`: either a plain length or a special encoding marker.
pub enum LengthResult {
    /// A plain length value.
    Length(usize),
    /// Special encoding: INT8 (next 1 byte).
    Int8,
    /// Special encoding: INT16 (next 2 bytes LE).
    Int16,
    /// Special encoding: INT32 (next 4 bytes LE).
    Int32,
    /// Special encoding: LZF compressed string.
    Lzf,
}

/// Low-level reader for the Redis RDB binary format.
pub struct RdbReader<R: Read> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read> RdbReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            pos: 0,
        }
    }

    /// Current byte position in the stream.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Read exactly `n` bytes.
    pub fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(buf)?;
        self.pos += buf.len() as u64;
        Ok(())
    }

    /// Read a single byte.
    pub fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// Validate the "REDIS" magic header and parse the 4-digit ASCII version number.
    pub fn read_header(&mut self) -> io::Result<u32> {
        let mut magic = [0u8; 5];
        self.read_exact(&mut magic)?;
        if &magic != b"REDIS" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid Redis RDB magic: expected 'REDIS', got '{}'",
                    String::from_utf8_lossy(&magic)
                ),
            ));
        }

        let mut ver_buf = [0u8; 4];
        self.read_exact(&mut ver_buf)?;
        let ver_str = std::str::from_utf8(&ver_buf).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid RDB version encoding")
        })?;
        let version: u32 = ver_str.parse().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid RDB version number: '{ver_str}'"),
            )
        })?;

        Ok(version)
    }

    /// Read a length-encoded value (2-bit prefix encoding).
    ///
    /// Returns either a plain length or a special encoding marker.
    pub fn read_length_encoding(&mut self) -> io::Result<LengthResult> {
        let first = self.read_u8()?;
        let enc_type = first >> 6;

        match enc_type {
            0b00 => {
                // 6-bit length
                Ok(LengthResult::Length((first & 0x3F) as usize))
            }
            0b01 => {
                // 14-bit length
                let second = self.read_u8()?;
                let len = (((first & 0x3F) as usize) << 8) | (second as usize);
                Ok(LengthResult::Length(len))
            }
            0b10 => {
                // 32-bit or 64-bit length (discriminated by remaining 6 bits)
                let disc = first & 0x3F;
                match disc {
                    0 => {
                        // 32-bit BE
                        let mut buf = [0u8; 4];
                        self.read_exact(&mut buf)?;
                        Ok(LengthResult::Length(u32::from_be_bytes(buf) as usize))
                    }
                    1 => {
                        // 64-bit BE
                        let mut buf = [0u8; 8];
                        self.read_exact(&mut buf)?;
                        Ok(LengthResult::Length(u64::from_be_bytes(buf) as usize))
                    }
                    _ => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown length encoding discriminator: {disc}"),
                    )),
                }
            }
            0b11 => {
                // Special encoding
                let special = first & 0x3F;
                match special {
                    0 => Ok(LengthResult::Int8),
                    1 => Ok(LengthResult::Int16),
                    2 => Ok(LengthResult::Int32),
                    3 => Ok(LengthResult::Lzf),
                    _ => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown special encoding: {special}"),
                    )),
                }
            }
            _ => unreachable!(),
        }
    }

    /// Read a non-special length value. Errors if a special encoding is encountered.
    pub fn read_length(&mut self) -> io::Result<usize> {
        match self.read_length_encoding()? {
            LengthResult::Length(len) => Ok(len),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected plain length, got special encoding",
            )),
        }
    }

    /// Read a length-prefixed string (handles raw bytes, integer encodings, and LZF).
    pub fn read_string(&mut self) -> io::Result<Bytes> {
        match self.read_length_encoding()? {
            LengthResult::Length(len) => {
                let mut buf = vec![0u8; len];
                self.read_exact(&mut buf)?;
                Ok(Bytes::from(buf))
            }
            LengthResult::Int8 => {
                let val = self.read_u8()? as i8;
                Ok(Bytes::from(val.to_string()))
            }
            LengthResult::Int16 => {
                let mut buf = [0u8; 2];
                self.read_exact(&mut buf)?;
                let val = i16::from_le_bytes(buf);
                Ok(Bytes::from(val.to_string()))
            }
            LengthResult::Int32 => {
                let mut buf = [0u8; 4];
                self.read_exact(&mut buf)?;
                let val = i32::from_le_bytes(buf);
                Ok(Bytes::from(val.to_string()))
            }
            LengthResult::Lzf => {
                let compressed_len = self.read_length()?;
                let uncompressed_len = self.read_length()?;
                let mut compressed = vec![0u8; compressed_len];
                self.read_exact(&mut compressed)?;
                let decompressed = lzf::decompress(&compressed, uncompressed_len)?;
                Ok(Bytes::from(decompressed))
            }
        }
    }

    /// Read raw string bytes (not converting to Bytes, for use in sub-decoders).
    pub fn read_string_raw(&mut self) -> io::Result<Vec<u8>> {
        match self.read_length_encoding()? {
            LengthResult::Length(len) => {
                let mut buf = vec![0u8; len];
                self.read_exact(&mut buf)?;
                Ok(buf)
            }
            LengthResult::Int8 => {
                let val = self.read_u8()? as i8;
                Ok(val.to_string().into_bytes())
            }
            LengthResult::Int16 => {
                let mut buf = [0u8; 2];
                self.read_exact(&mut buf)?;
                let val = i16::from_le_bytes(buf);
                Ok(val.to_string().into_bytes())
            }
            LengthResult::Int32 => {
                let mut buf = [0u8; 4];
                self.read_exact(&mut buf)?;
                let val = i32::from_le_bytes(buf);
                Ok(val.to_string().into_bytes())
            }
            LengthResult::Lzf => {
                let compressed_len = self.read_length()?;
                let uncompressed_len = self.read_length()?;
                let mut compressed = vec![0u8; compressed_len];
                self.read_exact(&mut compressed)?;
                lzf::decompress(&compressed, uncompressed_len)
            }
        }
    }

    /// Read a ZSET v1 score: length byte with special values 253=NaN, 254=+inf, 255=-inf,
    /// otherwise `length` bytes of ASCII score string.
    pub fn read_string_f64(&mut self) -> io::Result<f64> {
        let len = self.read_u8()?;
        match len {
            253 => Ok(f64::NAN),
            254 => Ok(f64::INFINITY),
            255 => Ok(f64::NEG_INFINITY),
            n => {
                let mut buf = vec![0u8; n as usize];
                self.read_exact(&mut buf)?;
                let s = std::str::from_utf8(&buf).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid score string encoding")
                })?;
                s.parse::<f64>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid score value: '{s}'"),
                    )
                })
            }
        }
    }

    /// Read an 8-byte LE IEEE 754 double (used by ZSET_2).
    pub fn read_binary_f64(&mut self) -> io::Result<f64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }

    /// Skip `n` bytes.
    pub fn skip(&mut self, n: usize) -> io::Result<()> {
        let mut remaining = n;
        let mut buf = [0u8; 4096];
        while remaining > 0 {
            let to_read = remaining.min(buf.len());
            self.read_exact(&mut buf[..to_read])?;
            remaining -= to_read;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_header() {
        let data = b"REDIS0009";
        let mut reader = RdbReader::new(Cursor::new(data.as_slice()));
        let version = reader.read_header().unwrap();
        assert_eq!(version, 9);
        assert_eq!(reader.position(), 9);
    }

    #[test]
    fn test_read_header_invalid_magic() {
        let data = b"NOTRD0009";
        let mut reader = RdbReader::new(Cursor::new(data.as_slice()));
        assert!(reader.read_header().is_err());
    }

    #[test]
    fn test_length_6bit() {
        // 00xxxxxx: 6-bit length = 10
        let data = [0b00_001010];
        let mut reader = RdbReader::new(Cursor::new(data.as_slice()));
        assert_eq!(reader.read_length().unwrap(), 10);
    }

    #[test]
    fn test_length_14bit() {
        // 01xxxxxx yyyyyyyy: 14-bit length = 300
        // 300 = 0b000100101100 -> high 6 bits = 0b000001, low 8 bits = 0b00101100
        let data = [0b01_000001, 0b00101100];
        let mut reader = RdbReader::new(Cursor::new(data.as_slice()));
        assert_eq!(reader.read_length().unwrap(), 300);
    }

    #[test]
    fn test_length_32bit() {
        // 10_000000 + 4 bytes BE
        let mut data = vec![0b10_000000];
        data.extend_from_slice(&1000u32.to_be_bytes());
        let mut reader = RdbReader::new(Cursor::new(data));
        assert_eq!(reader.read_length().unwrap(), 1000);
    }

    #[test]
    fn test_read_string_raw_bytes() {
        // 6-bit length=5 + "hello"
        let mut data = vec![5u8]; // 00_000101
        data.extend_from_slice(b"hello");
        let mut reader = RdbReader::new(Cursor::new(data));
        let s = reader.read_string().unwrap();
        assert_eq!(s, Bytes::from("hello"));
    }

    #[test]
    fn test_read_string_int8() {
        // Special encoding: 11_000000 (INT8) + value 42
        let data = [0b11_000000, 42];
        let mut reader = RdbReader::new(Cursor::new(data.as_slice()));
        let s = reader.read_string().unwrap();
        assert_eq!(s, Bytes::from("42"));
    }

    #[test]
    fn test_read_string_int16() {
        // Special encoding: 11_000001 (INT16) + value -1 as LE
        let mut data = vec![0b11_000001];
        data.extend_from_slice(&(-1i16).to_le_bytes());
        let mut reader = RdbReader::new(Cursor::new(data));
        let s = reader.read_string().unwrap();
        assert_eq!(s, Bytes::from("-1"));
    }

    #[test]
    fn test_read_string_int32() {
        // Special encoding: 11_000010 (INT32) + value 100000 as LE
        let mut data = vec![0b11_000010];
        data.extend_from_slice(&100000i32.to_le_bytes());
        let mut reader = RdbReader::new(Cursor::new(data));
        let s = reader.read_string().unwrap();
        assert_eq!(s, Bytes::from("100000"));
    }

    #[test]
    fn test_read_string_f64_normal() {
        // Length 3 + "1.5"
        let data = [3, b'1', b'.', b'5'];
        let mut reader = RdbReader::new(Cursor::new(data.as_slice()));
        let val = reader.read_string_f64().unwrap();
        assert!((val - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_read_string_f64_inf() {
        let data = [254];
        let mut reader = RdbReader::new(Cursor::new(data.as_slice()));
        let val = reader.read_string_f64().unwrap();
        assert!(val.is_infinite() && val > 0.0);
    }

    #[test]
    fn test_read_string_f64_neg_inf() {
        let data = [255];
        let mut reader = RdbReader::new(Cursor::new(data.as_slice()));
        let val = reader.read_string_f64().unwrap();
        assert!(val.is_infinite() && val < 0.0);
    }

    #[test]
    fn test_read_string_f64_nan() {
        let data = [253];
        let mut reader = RdbReader::new(Cursor::new(data.as_slice()));
        let val = reader.read_string_f64().unwrap();
        assert!(val.is_nan());
    }

    #[test]
    fn test_read_binary_f64() {
        let val: f64 = 3.14;
        let data = val.to_le_bytes();
        let mut reader = RdbReader::new(Cursor::new(data.as_slice()));
        let result = reader.read_binary_f64().unwrap();
        assert!((result - 3.14).abs() < f64::EPSILON);
    }
}
