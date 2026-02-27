use bytes::Bytes;

use crate::codec::RespFrame;

/// A parsed Redis command with name and arguments.
#[derive(Debug, Clone)]
pub struct ParsedCommand {
    pub name: String,      // uppercase command name
    pub args: Vec<Bytes>,  // arguments (raw bytes)
}

impl ParsedCommand {
    /// Parse a RESP frame into a command.
    pub fn from_frame(frame: RespFrame) -> Result<Self, String> {
        match frame {
            RespFrame::Array(items) if !items.is_empty() => {
                let mut args = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        RespFrame::BulkString(b) => args.push(b),
                        RespFrame::SimpleString(b) => args.push(b),
                        RespFrame::Integer(n) => args.push(Bytes::from(n.to_string())),
                        _ => return Err("ERR Protocol error: expected string in array".to_string()),
                    }
                }

                let name = std::str::from_utf8(&args[0])
                    .map_err(|_| "ERR invalid command name".to_string())?
                    .to_uppercase();

                Ok(ParsedCommand {
                    name,
                    args: args.into_iter().skip(1).collect(),
                })
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
    fn test_parse_case_insensitive() {
        let frame = RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("get")),
            RespFrame::BulkString(Bytes::from("key")),
        ]);
        let cmd = ParsedCommand::from_frame(frame).unwrap();
        assert_eq!(cmd.name, "GET");
    }

    #[test]
    fn test_parse_ping() {
        let frame = RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("PING")),
        ]);
        let cmd = ParsedCommand::from_frame(frame).unwrap();
        assert_eq!(cmd.name, "PING");
        assert!(cmd.args.is_empty());
    }

    #[test]
    fn test_arg_helpers() {
        let frame = RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("SETEX")),
            RespFrame::BulkString(Bytes::from("key")),
            RespFrame::BulkString(Bytes::from("60")),
            RespFrame::BulkString(Bytes::from("value")),
        ]);
        let cmd = ParsedCommand::from_frame(frame).unwrap();
        assert_eq!(cmd.arg_str(0), Some("key"));
        assert_eq!(cmd.arg_u64(1), Some(60));
    }

    #[test]
    fn test_parse_empty_array() {
        let frame = RespFrame::Array(vec![]);
        assert!(ParsedCommand::from_frame(frame).is_err());
    }
}
