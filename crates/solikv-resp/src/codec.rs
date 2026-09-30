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
            write_i64(dst, *n);
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::BulkString(data) => {
            dst.extend_from_slice(b"$");
            write_usize(dst, data.len());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(data);
            dst.extend_from_slice(b"\r\n");
        }
        RespFrame::Null => {
            dst.extend_from_slice(b"$-1\r\n");
        }
        RespFrame::Array(items) => {
            dst.extend_from_slice(b"*");
            write_usize(dst, items.len());
            dst.extend_from_slice(b"\r\n");
            for item in items {
                encode_frame(item, dst);
            }
        }
    }
}

/// Format `n` as decimal into `dst` without allocating a `String`.
#[inline]
fn write_i64(dst: &mut BytesMut, n: i64) {
    // itoa-style stack buffer
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let neg = n < 0;
    let mut v = if neg {
        // careful with i64::MIN
        (n as i128).unsigned_abs()
    } else {
        n as u128
    };
    if v == 0 {
        dst.extend_from_slice(b"0");
        return;
    }
    while v > 0 {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    dst.extend_from_slice(&buf[i..]);
}

#[inline]
fn write_usize(dst: &mut BytesMut, n: usize) {
    write_i64(dst, n as i64);
}

/// Maximum size of a single bulk string (512 MB, same as Redis default).
pub const MAX_BULK_STRING_LEN: usize = 512 * 1024 * 1024;

/// Maximum number of elements in a single array.
pub const MAX_ARRAY_LEN: usize = 1024 * 1024;

/// Maximum pre-auth array elements (before user is authenticated).
pub const MAX_UNAUTH_ARRAY_LEN: usize = 32;

/// Maximum pre-auth bulk string length (before user is authenticated).
pub const MAX_UNAUTH_BULK_LEN: usize = 64 * 1024;

/// Maximum array nesting depth once authenticated. Commands are flat arrays,
/// but the same decoder reads server replies (SCAN, CLUSTER SLOTS…), which
/// nest a couple of levels. Each level is one stack frame of recursion, so
/// this bound is what keeps `*1\r\n*1\r\n…` from overflowing the stack.
pub const MAX_NESTING_DEPTH: usize = 32;

/// Maximum nesting depth before AUTH: a command is a flat array, so an
/// unauthenticated peer gets no nesting at all.
pub const MAX_UNAUTH_NESTING_DEPTH: usize = 1;

/// Longest line accepted before its CRLF: an inline command, a simple
/// string/error/integer, or a `$`/`*` length header. Same bound as Redis'
/// `PROTO_INLINE_MAX_SIZE`. Without it a peer that never sends `\r\n` makes
/// the server buffer — and rescan — an ever-growing line.
pub const MAX_INLINE_LEN: usize = 64 * 1024;

/// Try to decode a frame from an authenticated connection.
/// Returns (frame, bytes_consumed) or None if incomplete.
pub fn decode_frame(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    decode_frame_with_auth(src, true)
}

/// Try to decode a frame, applying tighter limits when the connection is
/// not yet authenticated. An unauthenticated peer cannot declare arrays
/// larger than `MAX_UNAUTH_ARRAY_LEN` or bulk strings larger than
/// `MAX_UNAUTH_BULK_LEN`. This bounds the memory an attacker can force the
/// server to allocate before completing AUTH.
pub fn decode_frame_with_auth(
    src: &[u8],
    authenticated: bool,
) -> Result<Option<(RespFrame, usize)>, String> {
    decode_at_depth(src, authenticated, 0)
}

fn decode_at_depth(
    src: &[u8],
    authenticated: bool,
    depth: usize,
) -> Result<Option<(RespFrame, usize)>, String> {
    if src.is_empty() {
        return Ok(None);
    }

    match src[0] {
        b'+' => decode_simple_string(src),
        b'-' => decode_error(src),
        b':' => decode_integer(src),
        b'$' => decode_bulk_string(src, authenticated),
        b'*' => decode_array(src, authenticated, depth),
        _ => {
            // Try inline command (plain text terminated by \r\n)
            decode_inline(src, authenticated)
        }
    }
}

/// Position of the first `\r\n` in `src`.
fn find_crlf(src: &[u8]) -> Option<usize> {
    let mut start = 0;
    while let Some(i) = memchr::memchr(b'\r', &src[start..]) {
        let pos = start + i;
        match src.get(pos + 1) {
            Some(b'\n') => return Some(pos),
            Some(_) => start = pos + 1,
            None => return None,
        }
    }
    None
}

/// `find_crlf`, bounded: a line longer than `MAX_INLINE_LEN` is a protocol
/// error whether or not its CRLF has arrived yet.
fn find_line_end(src: &[u8]) -> Result<Option<usize>, String> {
    // Never scan past the bound: a huge buffer with no CRLF costs at most
    // MAX_INLINE_LEN bytes of search per decode attempt.
    let window = &src[..src.len().min(MAX_INLINE_LEN + 2)];
    match find_crlf(window) {
        Some(pos) if pos > MAX_INLINE_LEN => Err("Protocol error: too big inline request".into()),
        Some(pos) => Ok(Some(pos)),
        None if src.len() > MAX_INLINE_LEN => Err("Protocol error: too big inline request".into()),
        None => Ok(None),
    }
}

fn decode_simple_string(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    match find_line_end(&src[1..])? {
        None => Ok(None),
        Some(pos) => {
            let s = Bytes::copy_from_slice(&src[1..1 + pos]);
            Ok(Some((RespFrame::SimpleString(s), 1 + pos + 2)))
        }
    }
}

fn decode_error(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    match find_line_end(&src[1..])? {
        None => Ok(None),
        Some(pos) => {
            let s = String::from_utf8_lossy(&src[1..1 + pos]).to_string();
            Ok(Some((RespFrame::Error(s), 1 + pos + 2)))
        }
    }
}

fn decode_integer(src: &[u8]) -> Result<Option<(RespFrame, usize)>, String> {
    match find_line_end(&src[1..])? {
        None => Ok(None),
        Some(pos) => {
            let s = std::str::from_utf8(&src[1..1 + pos])
                .map_err(|e| format!("invalid utf8 in integer: {}", e))?;
            let n: i64 = s.parse().map_err(|e| format!("invalid integer: {}", e))?;
            Ok(Some((RespFrame::Integer(n), 1 + pos + 2)))
        }
    }
}

fn decode_bulk_string(
    src: &[u8],
    authenticated: bool,
) -> Result<Option<(RespFrame, usize)>, String> {
    match find_line_end(&src[1..])? {
        None => Ok(None),
        Some(pos) => {
            let len_str = std::str::from_utf8(&src[1..1 + pos])
                .map_err(|e| format!("invalid utf8 in bulk length: {}", e))?;
            let len: i64 = len_str
                .parse()
                .map_err(|e| format!("invalid bulk length: {}", e))?;

            if len == -1 {
                return Ok(Some((RespFrame::Null, 1 + pos + 2)));
            }

            if len < 0 {
                return Err(format!("invalid bulk length: {}", len));
            }

            let len = len as usize;
            let max = if authenticated {
                MAX_BULK_STRING_LEN
            } else {
                MAX_UNAUTH_BULK_LEN
            };
            if len > max {
                return Err(format!(
                    "bulk string length {} exceeds maximum {}",
                    len, max
                ));
            }

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

fn decode_array(
    src: &[u8],
    authenticated: bool,
    depth: usize,
) -> Result<Option<(RespFrame, usize)>, String> {
    let max_depth = if authenticated {
        MAX_NESTING_DEPTH
    } else {
        MAX_UNAUTH_NESTING_DEPTH
    };
    if depth >= max_depth {
        return Err(format!(
            "Protocol error: array nesting exceeds maximum depth {}",
            max_depth
        ));
    }
    match find_line_end(&src[1..])? {
        None => Ok(None),
        Some(pos) => {
            let count_str = std::str::from_utf8(&src[1..1 + pos])
                .map_err(|e| format!("invalid utf8 in array count: {}", e))?;
            let count: i64 = count_str
                .parse()
                .map_err(|e| format!("invalid array count: {}", e))?;

            if count == -1 {
                return Ok(Some((RespFrame::Null, 1 + pos + 2)));
            }

            if count < 0 {
                return Err(format!("invalid array count: {}", count));
            }

            let count = count as usize;
            let max = if authenticated {
                MAX_ARRAY_LEN
            } else {
                MAX_UNAUTH_ARRAY_LEN
            };
            if count > max {
                return Err(format!("array count {} exceeds maximum {}", count, max));
            }

            let mut offset = 1 + pos + 2;
            let mut items = Vec::new();

            for _ in 0..count {
                match decode_at_depth(&src[offset..], authenticated, depth + 1)? {
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

fn decode_inline(src: &[u8], authenticated: bool) -> Result<Option<(RespFrame, usize)>, String> {
    match find_line_end(src)? {
        None => Ok(None),
        Some(pos) => {
            let line = std::str::from_utf8(&src[..pos])
                .map_err(|e| format!("invalid utf8 in inline: {}", e))?;
            let parts: Vec<&str> = line.split_whitespace().collect();
            if !authenticated && parts.len() > MAX_UNAUTH_ARRAY_LEN {
                return Err(format!(
                    "array count {} exceeds maximum {}",
                    parts.len(),
                    MAX_UNAUTH_ARRAY_LEN
                ));
            }
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
        assert_eq!(&buf[..], b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
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

    // SEC-013: an `*N\r\n` header with a huge N must NOT trigger a
    // `Vec::with_capacity(N)` allocation. We verify this indirectly by
    // sending a partial array — the decoder should return Ok(None)
    // (incomplete) without panicking or hanging on a giant alloc.
    #[test]
    fn test_decode_array_huge_count_does_not_preallocate() {
        let header = format!("*{}\r\n", MAX_ARRAY_LEN);
        let result = decode_frame(header.as_bytes()).unwrap();
        // Incomplete: header parses, but no elements present. The decoder
        // must NOT have eagerly allocated MAX_ARRAY_LEN slots.
        assert!(result.is_none(), "expected incomplete, got {:?}", result);
    }

    #[test]
    fn test_decode_array_above_max_rejects() {
        let header = format!("*{}\r\n", MAX_ARRAY_LEN + 1);
        let err = decode_frame(header.as_bytes()).unwrap_err();
        assert!(
            err.to_lowercase().contains("array"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_decode_bulk_above_max_rejects() {
        let header = format!("${}\r\n", MAX_BULK_STRING_LEN + 1);
        let err = decode_frame(header.as_bytes()).unwrap_err();
        assert!(
            err.to_lowercase().contains("bulk"),
            "unexpected error: {}",
            err
        );
    }

    // SEC-013: unauthenticated connections cannot declare arrays beyond the
    // tight pre-auth cap, even though the same payload is fine post-auth.
    #[test]
    fn test_decode_array_pre_auth_cap_rejects() {
        let header = format!("*{}\r\n", MAX_UNAUTH_ARRAY_LEN + 1);
        let err = decode_frame_with_auth(header.as_bytes(), false).unwrap_err();
        assert!(err.contains("array"), "unexpected error: {}", err);

        // Same payload with authenticated=true must NOT trip the limit.
        let res = decode_frame_with_auth(header.as_bytes(), true).unwrap();
        assert!(
            res.is_none(),
            "post-auth: expected incomplete, got {:?}",
            res
        );
    }

    #[test]
    fn test_decode_bulk_pre_auth_cap_rejects() {
        let header = format!("${}\r\n", MAX_UNAUTH_BULK_LEN + 1);
        let err = decode_frame_with_auth(header.as_bytes(), false).unwrap_err();
        assert!(
            err.to_lowercase().contains("bulk"),
            "unexpected error: {}",
            err
        );

        // Same payload with authenticated=true is below MAX_BULK_STRING_LEN
        // and should be accepted (returns Ok(None) — needs more data).
        let res = decode_frame_with_auth(header.as_bytes(), true).unwrap();
        assert!(
            res.is_none(),
            "post-auth: expected incomplete, got {:?}",
            res
        );
    }

    // Bulk inside an array also gets the pre-auth cap (recursion).
    #[test]
    fn test_decode_array_with_oversized_bulk_pre_auth_rejects() {
        // *2\r\n$<huge>\r\n... — the bulk inside the array must trip the cap.
        let mut buf = Vec::new();
        buf.extend_from_slice(b"*2\r\n");
        buf.extend_from_slice(format!("${}\r\n", MAX_UNAUTH_BULK_LEN + 1).as_bytes());
        let err = decode_frame_with_auth(&buf, false).unwrap_err();
        assert!(
            err.to_lowercase().contains("bulk"),
            "unexpected error: {}",
            err
        );
    }
    // A deeply nested frame (`*1\r\n` repeated) used to recurse once per
    // level and overflow the stack before AUTH. It must now be rejected
    // quickly, both pre- and post-auth, without recursing that deep.
    #[test]
    fn test_decode_deep_nesting_rejected() {
        let buf = b"*1\r\n".repeat(100_000);
        let err = decode_frame_with_auth(&buf, true).unwrap_err();
        assert!(err.contains("nesting"), "unexpected error: {}", err);
        let err = decode_frame_with_auth(&buf, false).unwrap_err();
        assert!(err.contains("nesting"), "unexpected error: {}", err);
    }

    #[test]
    fn test_decode_nesting_pre_auth_flat_only() {
        // A flat command array is fine before AUTH…
        let flat = b"*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n";
        assert!(decode_frame_with_auth(flat, false).unwrap().is_some());
        // …an array inside an array is not.
        let nested = b"*1\r\n*1\r\n$4\r\nPING\r\n";
        assert!(decode_frame_with_auth(nested, false).is_err());
        // Post-auth, modest nesting (server replies) still decodes.
        assert!(decode_frame_with_auth(nested, true).unwrap().is_some());
    }

    #[test]
    fn test_decode_nesting_at_limit_ok() {
        let mut buf = b"*1\r\n".repeat(MAX_NESTING_DEPTH);
        buf.extend_from_slice(b":1\r\n");
        assert!(decode_frame(&buf).unwrap().is_some());
        let mut buf = b"*1\r\n".repeat(MAX_NESTING_DEPTH + 1);
        buf.extend_from_slice(b":1\r\n");
        assert!(decode_frame(&buf).is_err());
    }

    // A line that never ends must be refused once it passes the inline
    // bound, instead of being buffered until the 256 MB read cap.
    #[test]
    fn test_decode_unterminated_inline_rejected() {
        let buf = vec![b'A'; MAX_INLINE_LEN + 10];
        let err = decode_frame_with_auth(&buf, false).unwrap_err();
        assert!(err.contains("too big inline"), "unexpected error: {}", err);
        let mut plus = vec![b'+'];
        plus.extend(vec![b'x'; MAX_INLINE_LEN + 10]);
        assert!(decode_frame(&plus).is_err());
        // Just under the bound is merely incomplete.
        let buf = vec![b'A'; MAX_INLINE_LEN - 1];
        assert!(decode_frame(&buf).unwrap().is_none());
    }

    #[test]
    fn test_decode_inline_pre_auth_arg_cap() {
        let mut line = "X ".repeat(MAX_UNAUTH_ARRAY_LEN + 1).into_bytes();
        line.extend_from_slice(b"\r\n");
        assert!(decode_frame_with_auth(&line, false).is_err());
        assert!(decode_frame_with_auth(&line, true).unwrap().is_some());
    }

    #[test]
    fn test_find_crlf_lone_cr() {
        assert_eq!(find_crlf(b"ab\rc\r\n"), Some(4));
        assert_eq!(find_crlf(b"ab\r"), None);
        assert_eq!(find_crlf(b"\r\n"), Some(0));
    }
}
