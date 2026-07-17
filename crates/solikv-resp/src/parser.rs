use bytes::Bytes;

use crate::codec::RespFrame;

/// A parsed Redis command with name and arguments.
#[derive(Debug, Clone)]
pub struct ParsedCommand {
    pub name: String,     // uppercase command name
    pub args: Vec<Bytes>, // arguments (raw bytes)
}

impl ParsedCommand {
    /// Parse a RESP frame into a command.
    pub fn from_frame(frame: RespFrame) -> Result<Self, String> {
        match frame {
            RespFrame::Array(mut items) if !items.is_empty() => {
                // Convert frames → Bytes in place without an intermediate full copy.
                let mut args: Vec<Bytes> = Vec::with_capacity(items.len());
                for item in items.drain(..) {
                    match item {
                        RespFrame::BulkString(b) => args.push(b),
                        RespFrame::SimpleString(b) => args.push(b),
                        RespFrame::Integer(n) => args.push(Bytes::from(n.to_string())),
                        _ => return Err("ERR Protocol error: expected string in array".to_string()),
                    }
                }

                // Split command name from args without skip(1).collect() realloc.
                let cmd_bytes = args.remove(0);
                let name = ascii_uppercase_owned(&cmd_bytes)?;

                Ok(ParsedCommand { name, args })
            }
            _ => Err("ERR Protocol error: expected array".to_string()),
        }
    }

    pub fn arg_str(&self, idx: usize) -> Option<&str> {
        self.args.get(idx).and_then(|b| std::str::from_utf8(b).ok())
    }

    pub fn arg_bytes(&self, idx: usize) -> Option<&Bytes> {
        self.args.get(idx)
    }

    pub fn arg_i64(&self, idx: usize) -> Option<i64> {
        self.arg_str(idx).and_then(|s| s.parse().ok())
    }

    pub fn arg_u64(&self, idx: usize) -> Option<u64> {
        self.arg_str(idx).and_then(|s| s.parse().ok())
    }

    pub fn arg_f64(&self, idx: usize) -> Option<f64> {
        self.arg_str(idx).and_then(|s| s.parse().ok())
    }

    pub fn arg_usize(&self, idx: usize) -> Option<usize> {
        self.arg_str(idx).and_then(|s| s.parse().ok())
    }
}

/// Uppercase an ASCII command name with a single allocation.
/// Fast-path: already-uppercase names are copied as-is (no byte-by-byte transform).
#[inline]
fn ascii_uppercase_owned(bytes: &[u8]) -> Result<String, String> {
    // Validate UTF-8 (command names are always ASCII in practice).
    if !bytes.is_ascii() {
        // Fall back for non-ASCII (rare)
        let s = std::str::from_utf8(bytes).map_err(|_| "ERR invalid command name".to_string())?;
        return Ok(s.to_uppercase());
    }
    // Check if already all uppercase / digits / symbols — common for clients that
    // send "GET"/"SET" in upper case.
    if bytes.iter().all(|&b| !b.is_ascii_lowercase()) {
        // SAFETY: we verified is_ascii
        return Ok(unsafe { String::from_utf8_unchecked(bytes.to_vec()) });
    }
    let mut out = Vec::with_capacity(bytes.len());
    out.extend(bytes.iter().map(|b| b.to_ascii_uppercase()));
    // SAFETY: ASCII uppercase stays valid UTF-8
    Ok(unsafe { String::from_utf8_unchecked(out) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_set_command() {
        let frame = RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("SET")),
            RespFrame::BulkString(Bytes::from("key")),
            RespFrame::BulkString(Bytes::from("value")),
        ]);
        let cmd = ParsedCommand::from_frame(frame).unwrap();
        assert_eq!(cmd.name, "SET");
        assert_eq!(cmd.args.len(), 2);
        assert_eq!(cmd.args[0], Bytes::from("key"));
        assert_eq!(cmd.args[1], Bytes::from("value"));
    }

    #[test]
    fn test_parse_lowercase_command() {
        let frame = RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("get")),
            RespFrame::BulkString(Bytes::from("k")),
        ]);
        let cmd = ParsedCommand::from_frame(frame).unwrap();
        assert_eq!(cmd.name, "GET");
    }
}
