//! Incremental decoder for client commands.
//!
//! A command is a multibulk (`*N` then `N` bulk strings) or an inline line.
//! The frame-at-a-time decoder in `codec` rebuilt the whole array from the
//! start of the buffer on every read and threw the work away when the frame
//! was still incomplete, so a 1 MB `RPUSH` arriving in 64 KB reads was parsed
//! — and every argument copied — about sixteen times over. This one keeps
//! its place: elements already found are remembered as offsets into the
//! buffer, and the next call resumes at the first missing one. Each byte is
//! looked at once.
//!
//! Arguments of read-only commands are zero-copy slices of the read buffer
//! (a `GET key` allocates nothing but its argument vector); they are dropped
//! when the command has run, so the buffer is reused. Arguments of every
//! other command are copied into their own allocations: a key or value kept
//! in the store must not pin the read buffer it arrived in.

use bytes::{Buf, Bytes, BytesMut};

use crate::codec::{
    MAX_ARRAY_LEN, MAX_BULK_STRING_LEN, MAX_INLINE_LEN, MAX_UNAUTH_ARRAY_LEN, MAX_UNAUTH_BULK_LEN,
};
use crate::parser::{Name, ParsedCommand};

/// What the caller knows about a command name: its canonical (upper-case,
/// static) spelling, and whether its arguments may borrow the read buffer.
pub struct Resolved {
    pub name: &'static str,
    pub zero_copy: bool,
}

/// See the module docs.
#[derive(Debug, Default)]
pub struct CommandDecoder {
    /// Element count of the multibulk being assembled (`None` until its
    /// header is parsed).
    argc: Option<usize>,
    /// Offset of the next element header.
    pos: usize,
    /// `(start, end)` of every element payload found so far.
    spans: Vec<(usize, usize)>,
    /// Bytes still missing for the element being read, when known.
    missing: usize,
}

/// A decimal length (`-1`, `42`), strictly: digits with an optional sign.
fn parse_len(s: &[u8]) -> Option<i64> {
    let (neg, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, s),
    };
    if digits.is_empty() || digits.len() > 18 {
        return None;
    }
    let mut n: i64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n * 10 + (c - b'0') as i64;
    }
    Some(if neg { -n } else { n })
}

/// End of the line starting at `from` (position of its `\r`), bounded by
/// `MAX_INLINE_LEN`.
fn line_end(buf: &[u8], from: usize) -> Result<Option<usize>, String> {
    let window_end = buf.len().min(from + MAX_INLINE_LEN + 2);
    let window = &buf[from..window_end];
    let mut start = 0;
    while let Some(i) = memchr::memchr(b'\r', &window[start..]) {
        let p = start + i;
        match window.get(p + 1) {
            Some(b'\n') => {
                if p > MAX_INLINE_LEN {
                    return Err("Protocol error: too big inline request".into());
                }
                return Ok(Some(from + p));
            }
            Some(_) => start = p + 1,
            None => break,
        }
    }
    if buf.len() - from > MAX_INLINE_LEN {
        return Err("Protocol error: too big inline request".into());
    }
    Ok(None)
}

/// A copy of `data` in `Bytes`' shared representation.
///
/// `copy_from_slice` returns a Vec-backed `Bytes` whose first clone
/// allocates the shared header it then switches to — and the engine clones
/// a write's arguments (the key it stores, the values it keeps) while it
/// holds the shard lock, where a pipeline of writes to one hot key queues.
/// Cloning once here, on the connection's own thread, moves that allocation
/// out of the lock: the engine's clones are then a reference-count bump.
fn owned_copy(data: &[u8]) -> Bytes {
    let b = Bytes::copy_from_slice(data);
    drop(b.clone());
    b
}

impl CommandDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes the command being assembled still needs, when a bulk length
    /// says so (reserve at least this much before reading).
    pub fn missing(&self) -> usize {
        self.missing
    }

    /// Whether part of a command has been parsed and more is awaited.
    pub fn in_progress(&self) -> bool {
        self.argc.is_some()
    }

    fn reset(&mut self) {
        self.argc = None;
        self.pos = 0;
        self.spans.clear();
        self.missing = 0;
        if self.spans.capacity() > 4096 {
            self.spans = Vec::new();
        }
    }

    /// The next complete command at the head of `buf`, consumed from it;
    /// `Ok(None)` when more input is needed. `resolve` looks the name up.
    /// Errors are protocol errors: the connection should be closed.
    pub fn next(
        &mut self,
        buf: &mut BytesMut,
        authenticated: bool,
        resolve: impl Fn(&[u8]) -> Option<Resolved>,
    ) -> Result<Option<ParsedCommand>, String> {
        loop {
            if self.argc.is_none() {
                if buf.is_empty() {
                    return Ok(None);
                }
                if buf[0] != b'*' {
                    match self.inline(buf, authenticated, &resolve)? {
                        Inline::Command(c) => return Ok(Some(c)),
                        Inline::NeedMore => return Ok(None),
                        Inline::Blank => continue,
                    }
                }
                let Some(eol) = line_end(buf, 1)? else {
                    return Ok(None);
                };
                let n = parse_len(&buf[1..eol])
                    .ok_or_else(|| "Protocol error: invalid multibulk length".to_string())?;
                let max = if authenticated {
                    MAX_ARRAY_LEN
                } else {
                    MAX_UNAUTH_ARRAY_LEN
                } as i64;
                if n > max {
                    return Err("Protocol error: invalid multibulk length".into());
                }
                if n <= 0 {
                    // `*0` / `*-1`: an empty command, skipped (Redis too).
                    buf.advance(eol + 2);
                    continue;
                }
                self.argc = Some(n as usize);
                self.pos = eol + 2;
                self.spans.reserve((n as usize).min(1024));
            }
            let argc = self.argc.unwrap_or(0);
            let max_bulk = if authenticated {
                MAX_BULK_STRING_LEN
            } else {
                MAX_UNAUTH_BULK_LEN
            };
            while self.spans.len() < argc {
                let p = self.pos;
                let Some(&tag) = buf.get(p) else {
                    self.missing = 0;
                    return Ok(None);
                };
                if tag == b'*' {
                    // Commands are flat: nothing recurses, so depth costs
                    // nothing, but say what was wrong.
                    return Err("Protocol error: array nesting is not allowed in a command \
                         (expected '$', got '*')"
                        .into());
                }
                if tag != b'$' {
                    return Err(format!(
                        "Protocol error: expected '$', got '{}'",
                        tag as char
                    ));
                }
                let Some(eol) = line_end(buf, p + 1)? else {
                    self.missing = 0;
                    return Ok(None);
                };
                let len = parse_len(&buf[p + 1..eol])
                    .filter(|&l| l >= 0)
                    .ok_or_else(|| "Protocol error: invalid bulk length".to_string())?
                    as usize;
                if len > max_bulk {
                    return Err("Protocol error: invalid bulk length".into());
                }
                let start = eol + 2;
                let end = start + len;
                if buf.len() < end + 2 {
                    self.missing = end + 2 - buf.len();
                    return Ok(None);
                }
                if &buf[end..end + 2] != b"\r\n" {
                    return Err("Protocol error: bulk string not terminated by CRLF".into());
                }
                self.spans.push((start, end));
                self.pos = end + 2;
            }
            let total = self.pos;
            let (ns, ne) = self.spans[0];
            let resolved = resolve(&buf[ns..ne]);
            let name = match &resolved {
                Some(r) => Name::from_static(r.name),
                None => Name::from_raw(&buf[ns..ne]),
            };
            let args: Vec<Bytes> = if resolved.as_ref().is_some_and(|r| r.zero_copy) {
                let frame = buf.split_to(total).freeze();
                self.spans[1..]
                    .iter()
                    .map(|&(s, e)| frame.slice(s..e))
                    .collect()
            } else {
                let args = self.spans[1..]
                    .iter()
                    .map(|&(s, e)| owned_copy(&buf[s..e]))
                    .collect();
                buf.advance(total);
                args
            };
            self.reset();
            return Ok(Some(ParsedCommand { name, args }));
        }
    }

    /// An inline command: one line of space-separated words.
    fn inline(
        &mut self,
        buf: &mut BytesMut,
        authenticated: bool,
        resolve: &impl Fn(&[u8]) -> Option<Resolved>,
    ) -> Result<Inline, String> {
        let Some(eol) = line_end(buf, 0)? else {
            return Ok(Inline::NeedMore);
        };
        let line = buf.split_to(eol + 2);
        let mut words = line[..eol]
            .split(|c| c.is_ascii_whitespace())
            .filter(|w| !w.is_empty());
        let Some(first) = words.next() else {
            // An empty line: nothing to run.
            return Ok(Inline::Blank);
        };
        let args: Vec<Bytes> = words.map(owned_copy).collect();
        if !authenticated && args.len() + 1 > MAX_UNAUTH_ARRAY_LEN {
            return Err(format!(
                "array count {} exceeds maximum {}",
                args.len() + 1,
                MAX_UNAUTH_ARRAY_LEN
            ));
        }
        let name = match resolve(first) {
            Some(r) => Name::from_static(r.name),
            None => Name::from_raw(first),
        };
        Ok(Inline::Command(ParsedCommand { name, args }))
    }
}

enum Inline {
    Command(ParsedCommand),
    NeedMore,
    Blank,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(name: &[u8]) -> Option<Resolved> {
        match name.to_ascii_uppercase().as_slice() {
            b"GET" => Some(Resolved {
                name: "GET",
                zero_copy: true,
            }),
            b"SET" => Some(Resolved {
                name: "SET",
                zero_copy: false,
            }),
            b"RPUSH" => Some(Resolved {
                name: "RPUSH",
                zero_copy: false,
            }),
            _ => None,
        }
    }

    fn feed(dec: &mut CommandDecoder, buf: &mut BytesMut, bytes: &[u8]) -> Vec<ParsedCommand> {
        buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        while let Some(c) = dec.next(buf, true, resolve).unwrap() {
            out.push(c);
        }
        out
    }

    #[test]
    fn pipelined_and_mixed_case() {
        let mut dec = CommandDecoder::new();
        let mut buf = BytesMut::new();
        let cmds = feed(
            &mut dec,
            &mut buf,
            b"*2\r\n$3\r\nget\r\n$1\r\nk\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nvv\r\nPING\r\n",
        );
        assert_eq!(cmds.len(), 3);
        assert_eq!(cmds[0].name, "GET");
        assert_eq!(cmds[0].args, vec![Bytes::from("k")]);
        assert_eq!(cmds[1].name, "SET");
        assert_eq!(cmds[1].args, vec![Bytes::from("k"), Bytes::from("vv")]);
        assert_eq!(cmds[2].name, "PING");
        assert!(cmds[2].args.is_empty());
        assert!(buf.is_empty());
    }

    /// A large command split at every possible byte boundary decodes the
    /// same, and never before it is complete.
    #[test]
    fn fragmented_input_at_every_boundary() {
        let mut frame = b"*4\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n".to_vec();
        frame.extend_from_slice(b"$10\r\n0123456789\r\n$0\r\n\r\n");
        frame.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$4\r\nlist\r\n");
        for cut in 0..frame.len() {
            let mut dec = CommandDecoder::new();
            let mut buf = BytesMut::new();
            let mut got = feed(&mut dec, &mut buf, &frame[..cut]);
            got.extend(feed(&mut dec, &mut buf, &frame[cut..]));
            assert_eq!(got.len(), 2, "cut at {cut}");
            assert_eq!(got[0].name, "RPUSH");
            assert_eq!(
                got[0].args,
                vec![Bytes::from("list"), Bytes::from("0123456789"), Bytes::new()]
            );
            assert_eq!(got[1].name, "GET");
        }
    }

    /// Byte by byte: each element is parsed once (the work is linear).
    #[test]
    fn byte_at_a_time_big_command() {
        let n = 2000;
        let mut frame = format!("*{}\r\n$5\r\nRPUSH\r\n$1\r\nk\r\n", n + 2).into_bytes();
        for i in 0..n {
            let v = format!("v{i}");
            frame.extend_from_slice(format!("${}\r\n{}\r\n", v.len(), v).as_bytes());
        }
        let mut dec = CommandDecoder::new();
        let mut buf = BytesMut::new();
        let mut got = Vec::new();
        for b in &frame {
            got.extend(feed(&mut dec, &mut buf, std::slice::from_ref(b)));
        }
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].args.len(), n + 1);
        assert_eq!(got[0].args[n], Bytes::from(format!("v{}", n - 1)));
    }

    #[test]
    fn unknown_names_keep_their_spelling_upper_cased() {
        let mut dec = CommandDecoder::new();
        let mut buf = BytesMut::new();
        let cmds = feed(&mut dec, &mut buf, b"*1\r\n$7\r\nfoobar1\r\n");
        assert_eq!(cmds[0].name, "FOOBAR1");
    }

    #[test]
    fn protocol_errors() {
        for bad in [
            &b"*1\r\n:5\r\n"[..],
            b"*x\r\n",
            b"*1\r\n$-5\r\n",
            b"*1\r\n$3\r\nabcde\r\n",
        ] {
            let mut dec = CommandDecoder::new();
            let mut buf = BytesMut::from(bad);
            assert!(dec.next(&mut buf, true, resolve).is_err(), "{bad:?}");
        }
        // Before AUTH: small arrays and bulks only.
        let mut dec = CommandDecoder::new();
        let mut buf = BytesMut::from(&b"*100\r\n"[..]);
        assert!(dec.next(&mut buf, false, resolve).is_err());
        let mut buf = BytesMut::from(&b"*1\r\n$100000\r\n"[..]);
        let mut dec = CommandDecoder::new();
        assert!(dec.next(&mut buf, false, resolve).is_err());
        // A line that never ends is refused past the inline limit.
        let mut buf = BytesMut::from(&vec![b'a'; MAX_INLINE_LEN + 10][..]);
        let mut dec = CommandDecoder::new();
        assert!(dec.next(&mut buf, true, resolve).is_err());
    }

    #[test]
    fn empty_multibulk_is_skipped() {
        let mut dec = CommandDecoder::new();
        let mut buf = BytesMut::new();
        let cmds = feed(&mut dec, &mut buf, b"*0\r\n\r\n*1\r\n$4\r\nPING\r\n");
        assert_eq!(cmds.len(), 1);
    }

    #[test]
    fn zero_copy_args_share_the_buffer_and_release_it() {
        let mut dec = CommandDecoder::new();
        let mut buf = BytesMut::with_capacity(1024);
        buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nabc\r\n");
        let cmd = dec.next(&mut buf, true, resolve).unwrap().unwrap();
        assert_eq!(cmd.args[0], Bytes::from("abc"));
        drop(cmd);
        // Unique again: reserving reuses the allocation.
        buf.reserve(512);
        assert!(buf.capacity() >= 512);
    }
}
