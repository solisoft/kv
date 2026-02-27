use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// A RESP frame that can be sent/received over the wire.
#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    SimpleString(Bytes),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<RespFrame>),
    Null,
}

impl RespFrame {
    pub fn ok() -> Self {
        RespFrame::SimpleString(Bytes::from_static(b"OK"))
    }

    pub fn pong() -> Self {
        RespFrame::SimpleString(Bytes::from_static(b"PONG"))
    }

    pub fn null() -> Self {
        RespFrame::Null
    }

    pub fn error(msg: impl Into<String>) -> Self {
        RespFrame::Error(msg.into())
    }

    pub fn integer(n: i64) -> Self {
        RespFrame::Integer(n)
    }

    pub fn bulk(data: Bytes) -> Self {
        RespFrame::BulkString(data)
    }

    pub fn bulk_string(s: &str) -> Self {
        RespFrame::BulkString(Bytes::from(s.to_string()))
    }

    pub fn array(items: Vec<RespFrame>) -> Self {
        RespFrame::Array(items)
    }
}

/// RESP2/RESP3 codec for Tokio.
#[derive(Debug, Default)]
pub struct RespCodec;

impl RespCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Decoder for RespCodec {
    type Item = RespFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match decode_frame(src) {
            Ok(Some((frame, consumed))) => {
                src.advance(consumed);
                Ok(Some(frame))
            }
            Ok(None) => Ok(None), // need more data
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        }
    }
}

impl Encoder<RespFrame> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode_frame(&item, dst);
        Ok(())
    }
}

/// Encode a RESP frame into bytes.
pub fn encode_frame(frame: &RespFrame, dst: &mut BytesMut) {
    match frame {
        RespFrame::SimpleString(s) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(s);
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::Error(msg) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(msg.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::Integer(n) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(n.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::BulkString(data) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(data.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(data);
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::Null => {
            dst.extend_from_slice(b"$-1\r\n");
        }
        RespFrame::Array(items) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(items.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for item in items {
                encode_frame(item, dst);
            }
        }
    }
}

/// Try to decode a frame from the buffer. Returns (frame, bytes_consumed) or None if incomplete.
pub fn decode_frame(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    if src.is_empty() {
        return Ok(None);
    }

    match src[0] {
        b'+' => decode_simple_string(src),
        b'-' => decode_error(src),
        b':' => decode_integer(src),
        b'$' => decode_bulk_string(src),
        b'*' => decode_array(src),
        _ => {
            // Try inline command (plain text terminated by \r\n)
            decode_inline(src)
        }
    }
}

fn find_crlf(src: &[u8]) -> Option<usize> {
    for i in 0..src.len().saturating_sub(1) {
        if src[i] == b'\r' && src[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

fn decode_simple_string(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    match find_crlf(&src[1..]) {
        None => Ok(None),
        Some(pos) => {
            let s = Bytes::copy_from_slice(&src[1..1 + pos]);
            Ok(Some((RespFrame::SimpleString(s), 1 + pos + 2)))
        }
    }
}

fn decode_error(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    match find_crlf(&src[1..]) {
        None => Ok(None),
        Some(pos) => {
            let s = String::from_utf8_lossy(&src[1..1 + pos]).to_string();
            Ok(Some((RespFrame::Error(s), 1 + pos + 2)))
        }
    }
}

fn decode_integer(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    match find_crlf(&src[1..]) {
        None => Ok(None),
        Some(pos) => {
            let s = std::str::from_utf8(&src[1..1 + pos])
                .map_err(|e| format!("invalid utf8 in integer: {}", e))?;
            let n: i64 = s.parse()
                .map_err(|e| format!("invalid integer: {}", e))?;
            Ok(Some((RespFrame::Integer(n), 1 + pos + 2)))
        }
    }
}

fn decode_bulk_string(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    match find_crlf(&src[1..]) {
        None => Ok(None),
        Some(pos) => {
            let len_str = std::str::from_utf8(&src[1..1 + pos])
                .map_err(|e| format!("invalid utf8 in bulk length: {}", e))?;
            let len: i64 = len_str.parse()
                .map_err(|e| format!("invalid bulk length: {}", e))?;

            if len == -1 {
                return Ok(Some((RespFrame::Null, 1 + pos + 2)));
            }

            let len = len as usize;
            let data_start = 1 + pos + 2;
            let total_needed = data_start + len + 2;

            if src.len() < total_needed {
                return Ok(None); // need more data
            }

            let data = Bytes::copy_from_slice(&src[data_start..data_start + len]);
            Ok(Some((RespFrame::BulkString(data), total_needed)))
        }
    }
}

fn decode_array(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    match find_crlf(&src[1..]) {
        None => Ok(None),
        Some(pos) => {
            let count_str = std::str::from_utf8(&src[1..1 + pos])
                .map_err(|e| format!("invalid utf8 in array count: {}", e))?;
            let count: i64 = count_str.parse()
                .map_err(|e| format!("invalid array count: {}", e))?;

            if count == -1 {
                return Ok(Some((RespFrame::Null, 1 + pos + 2)));
            }

            let count = count as usize;
            let mut offset = 1 + pos + 2;
            let mut items = Vec::with_capacity(count);

            for _ in 0..count {
                match decode_frame(&src[offset..])? {
                    Some((frame, consumed)) => {
                        items.push(frame);
                        offset += consumed;
                    }
                    None => return Ok(None), // need more data
                }
            }

            Ok(Some((RespFrame::Array(items), offset)))
        }
    }
}

fn decode_inline(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    match find_crlf(src) {
        None => Ok(None),
        Some(pos) => {
            let line = std::str::from_utf8(&src[..pos])
                .map_err(|e| format!("invalid utf8 in inline: {}", e))?;
            let parts: Vec<&str> = line.split_whitespace().collect();
            let items: Vec<RespFrame> = parts
                .into_iter()
                .map(|p| RespFrame::BulkString(Bytes::from(p.to_string())))
                .collect();
            Ok(Some((RespFrame::Array(items), pos + 2)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple_string() {
        let mut buf = BytesMut::new();
        encode_frame(&RespFrame::SimpleString(Bytes::from("OK")), &mut buf);
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let mut buf = BytesMut::new();
        encode_frame(&RespFrame::Error("ERR unknown".to_string()), &mut buf);
        assert_eq!(&buf[..], b"-ERR unknown\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let mut buf = BytesMut::new();
        encode_frame(&RespFrame::Integer(42), &mut buf);
        assert_eq!(&buf[..], b":42\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let mut buf = BytesMut::new();
        encode_frame(&RespFrame::BulkString(Bytes::from("hello")), &mut buf);
        assert_eq!(&buf[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_null() {
        let mut buf = BytesMut::new();
        encode_frame(&RespFrame::Null, &mut buf);
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let mut buf = BytesMut::new();
        encode_frame(
            &RespFrame::Array(vec![
                RespFrame::BulkString(Bytes::from("SET")),
                RespFrame::BulkString(Bytes::from("key")),
                RespFrame::BulkString(Bytes::from("value")),
            ]),
            &mut buf,
        );
        assert_eq!(
            &buf[..],
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        );
    }

    #[test]
    fn test_decode_simple_string() {
        let data = b"+OK\r\n";
        let (frame, consumed) = decode_frame(data).unwrap().unwrap();
        assert_eq!(frame, RespFrame::SimpleString(Bytes::from("OK")));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_decode_error() {
        let data = b"-ERR unknown\r\n";
        let (frame, consumed) = decode_frame(data).unwrap().unwrap();
        assert_eq!(frame, RespFrame::Error("ERR unknown".to_string()));
        assert_eq!(consumed, 14);
    }

    #[test]
    fn test_decode_integer() {
        let data = b":42\r\n";
        let (frame, consumed) = decode_frame(data).unwrap().unwrap();
        assert_eq!(frame, RespFrame::Integer(42));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_decode_negative_integer() {
        let data = b":-1\r\n";
        let (frame, _) = decode_frame(data).unwrap().unwrap();
        assert_eq!(frame, RespFrame::Integer(-1));
    }

    #[test]
    fn test_decode_bulk_string() {
        let data = b"$5\r\nhello\r\n";
        let (frame, consumed) = decode_frame(data).unwrap().unwrap();
        assert_eq!(frame, RespFrame::BulkString(Bytes::from("hello")));
        assert_eq!(consumed, 11);
    }

    #[test]
    fn test_decode_null_bulk() {
        let data = b"$-1\r\n";
        let (frame, _) = decode_frame(data).unwrap().unwrap();
        assert_eq!(frame, RespFrame::Null);
    }

    #[test]
    fn test_decode_array() {
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let (frame, _) = decode_frame(data).unwrap().unwrap();
        match frame {
            RespFrame::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespFrame::BulkString(Bytes::from("GET")));
                assert_eq!(items[1], RespFrame::BulkString(Bytes::from("key")));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_decode_incomplete() {
        let data = b"$5\r\nhel";
        assert!(decode_frame(data).unwrap().is_none());
    }

    #[test]
    fn test_decode_inline() {
        let data = b"PING\r\n";
        let (frame, _) = decode_frame(data).unwrap().unwrap();
        match frame {
            RespFrame::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], RespFrame::BulkString(Bytes::from("PING")));
            }
            _ => panic!("expected array from inline"),
        }
    }

    #[test]
    fn test_decode_inline_with_args() {
        let data = b"SET key value\r\n";
        let (frame, _) = decode_frame(data).unwrap().unwrap();
        match frame {
            RespFrame::Array(items) => {
                assert_eq!(items.len(), 3);
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_roundtrip() {
        let original = RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("SET")),
            RespFrame::BulkString(Bytes::from("mykey")),
            RespFrame::BulkString(Bytes::from("myvalue")),
        ]);
        let mut buf = BytesMut::new();
        encode_frame(&original, &mut buf);
        let (decoded, _) = decode_frame(&buf).unwrap().unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_codec_decoder() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&b"+OK\r\n"[..]);
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, RespFrame::SimpleString(Bytes::from("OK")));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_codec_encoder() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(RespFrame::ok(), &mut buf).unwrap();
        assert_eq!(&buf[..], b"+OK\r\n");
    }
}
