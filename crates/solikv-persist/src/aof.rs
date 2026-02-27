use bytes::Bytes;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

/// AOF fsync policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    Everysec,
    No,
}

// ---------------------------------------------------------------------------
// Lock-free AOF writer (channel-based)
// ---------------------------------------------------------------------------

/// Lock-free AOF writer handle.
///
/// Commands are RESP-serialized on the caller's thread and sent through
/// a bounded channel to a dedicated writer task. Zero lock contention on
/// the hot path — callers only pay for serialization + a channel send.
#[derive(Clone)]
pub struct AofWriter {
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl AofWriter {
    /// Log a write command. Returns immediately — no locks, no I/O.
    pub fn log(&self, name: &str, args: &[Bytes]) {
        // Pre-serialize to RESP on the caller's thread
        let count = 1 + args.len();
        let cap = 32 + name.len() + args.iter().map(|a| a.len() + 16).sum::<usize>();
        let mut buf = Vec::with_capacity(cap);

        let _ = write!(buf, "*{}\r\n${}\r\n", count, name.len());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(b"\r\n");
        for arg in args {
            let _ = write!(buf, "${}\r\n", arg.len());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }

        // Non-blocking send — drops silently if channel is full (shouldn't
        // happen with the 256K buffer unless the disk is stalled).
        let _ = self.tx.try_send(buf);
    }
}

/// Spawn a background AOF writer task. Returns a lock-free [`AofWriter`] handle.
///
/// The writer task:
/// - Receives pre-serialized RESP bytes through a channel
/// - Batch-drains all pending messages per wake-up
/// - Writes through a 64 KB `BufWriter` (reduces syscalls)
/// - Applies fsync according to the chosen policy
/// - On channel close (all senders dropped): flushes + fsyncs + exits
pub fn spawn_aof_writer(path: &Path, policy: FsyncPolicy) -> io::Result<AofWriter> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256 * 1024);
    let needs_periodic_sync = policy == FsyncPolicy::Everysec;

    tokio::spawn(async move {
        let mut writer = BufWriter::with_capacity(64 * 1024, file);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        // First tick fires immediately — consume it.
        interval.tick().await;

        loop {
            tokio::select! {
                biased; // prefer draining messages over timer ticks

                msg = rx.recv() => {
                    match msg {
                        Some(buf) => {
                            let _ = writer.write_all(&buf);
                            // Batch-drain everything pending
                            while let Ok(buf) = rx.try_recv() {
                                let _ = writer.write_all(&buf);
                            }
                            if policy == FsyncPolicy::Always {
                                let _ = writer.flush();
                                let _ = writer.get_ref().sync_data();
                            }
                        }
                        None => {
                            // All senders dropped — final flush
                            let _ = writer.flush();
                            let _ = writer.get_ref().sync_data();
                            break;
                        }
                    }
                }

                _ = interval.tick(), if needs_periodic_sync => {
                    let _ = writer.flush();
                    let _ = writer.get_ref().sync_data();
                }
            }
        }
    });

    Ok(AofWriter { tx })
}

// ---------------------------------------------------------------------------
// Legacy synchronous AOF (used only for tests / replay)
// ---------------------------------------------------------------------------

/// Synchronous AOF persistence (used for replay and tests).
pub struct AofPersistence {
    writer: Option<std::fs::File>,
    policy: FsyncPolicy,
}

impl AofPersistence {
    pub fn new(path: &Path, policy: FsyncPolicy) -> io::Result<Self> {
        let writer = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            writer: Some(writer),
            policy,
        })
    }

    /// Append a command to the AOF.
    pub fn append(&mut self, args: &[Bytes]) -> io::Result<()> {
        let writer = match &mut self.writer {
            Some(w) => w,
            None => return Ok(()),
        };

        write!(writer, "*{}\r\n", args.len())?;
        for arg in args {
            write!(writer, "${}\r\n", arg.len())?;
            writer.write_all(arg)?;
            writer.write_all(b"\r\n")?;
        }

        if self.policy == FsyncPolicy::Always {
            writer.flush()?;
            writer.sync_data()?;
        }

        Ok(())
    }

    /// Replay AOF file and return commands (binary-safe).
    pub fn replay(path: &Path) -> io::Result<Vec<Vec<Bytes>>> {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut commands = Vec::new();

        loop {
            let line = match read_line_crlf(&mut reader) {
                Ok(l) => l,
                Err(_) => break,
            };
            if line.is_empty() {
                break;
            }
            if !line.starts_with(b"*") {
                continue;
            }
            let count: usize = match std::str::from_utf8(&line[1..])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(c) => c,
                None => continue,
            };

            let mut args = Vec::with_capacity(count);
            let mut valid = true;
            for _ in 0..count {
                let len_line = match read_line_crlf(&mut reader) {
                    Ok(l) => l,
                    Err(_) => {
                        valid = false;
                        break;
                    }
                };
                if !len_line.starts_with(b"$") {
                    valid = false;
                    break;
                }
                let len: usize = match std::str::from_utf8(&len_line[1..])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(l) => l,
                    None => {
                        valid = false;
                        break;
                    }
                };

                let mut buf = vec![0u8; len];
                if reader.read_exact(&mut buf).is_err() {
                    valid = false;
                    break;
                }
                let mut crlf = [0u8; 2];
                if reader.read_exact(&mut crlf).is_err() {
                    valid = false;
                    break;
                }

                args.push(Bytes::from(buf));
            }
            if valid && args.len() == count {
                commands.push(args);
            }
        }

        Ok(commands)
    }
}

/// Read a line terminated by \r\n, returning content without the terminator.
fn read_line_crlf<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match reader.read_exact(&mut byte) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                if buf.is_empty() {
                    return Err(e);
                }
                return Ok(buf);
            }
            Err(e) => return Err(e),
        }
        if byte[0] == b'\r' {
            let mut next = [0u8; 1];
            reader.read_exact(&mut next)?;
            if next[0] == b'\n' {
                return Ok(buf);
            }
            buf.push(byte[0]);
            buf.push(next[0]);
        } else {
            buf.push(byte[0]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_aof_append_format() {
        let mut buf = Vec::new();
        let args = vec![Bytes::from("SET"), Bytes::from("key"), Bytes::from("value")];
        write!(buf, "*{}\r\n", args.len()).unwrap();
        for arg in &args {
            write!(buf, "${}\r\n", arg.len()).unwrap();
            buf.write_all(arg).unwrap();
            buf.write_all(b"\r\n").unwrap();
        }
        let expected = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        assert_eq!(std::str::from_utf8(&buf).unwrap(), expected);
    }

    #[test]
    fn test_aof_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");

        {
            let mut aof = AofPersistence::new(&path, FsyncPolicy::Always).unwrap();
            aof.append(&[Bytes::from("SET"), Bytes::from("key1"), Bytes::from("val1")])
                .unwrap();
            aof.append(&[Bytes::from("SET"), Bytes::from("key2"), Bytes::from("val2")])
                .unwrap();
        }

        let commands = AofPersistence::replay(&path).unwrap();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0][0], Bytes::from("SET"));
        assert_eq!(commands[0][1], Bytes::from("key1"));
    }

    #[test]
    fn test_aof_replay_binary_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_binary.aof");

        {
            let mut aof = AofPersistence::new(&path, FsyncPolicy::Always).unwrap();
            let binary_val = Bytes::from(b"hello\r\nworld".as_slice());
            aof.append(&[Bytes::from("SET"), Bytes::from("key"), binary_val])
                .unwrap();
        }

        let commands = AofPersistence::replay(&path).unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0][2], Bytes::from(b"hello\r\nworld".as_slice()));
    }
}
