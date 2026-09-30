use bytes::Bytes;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

const MAX_AOF_BULK_LEN: usize = 512 * 1024 * 1024;

/// Most arguments one AOF command may declare (same bound as the RESP decoder).
const MAX_AOF_ARGS: usize = 1024 * 1024;

// Commands rejected during AOF replay. The on-disk AOF is trusted only as
// far as filesystem permissions go; an attacker who can write to the data
// directory shouldn't be able to make us run scripting, server-control, or
// data-migration commands at startup.
//
// Each entry is a single command name (no subcommand prefixes) — replay only
// inspects args[0], so e.g. "SCRIPT LOAD" is matched as "SCRIPT".
const AOF_BLOCKED_COMMANDS: &[&str] = &[
    "EVAL",
    "EVALSHA",
    "SCRIPT",
    "DEBUG",
    "SHUTDOWN",
    "BGREWRITEAOF",
    "BGSAVE",
    "SLAVEOF",
    "REPLICAOF",
    "CLUSTER",
    "MIGRATE",
    "RESTORE",
    "MODULE",
];

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
/// a bounded channel to a dedicated writer thread. Zero lock contention on
/// the hot path — callers only pay for serialization + a channel send.
///
/// The writer is an OS thread, not a Tokio task: on the solo runtime a task
/// would share the single worker with the producers, so a full channel could
/// only be drained by handing the worker to another thread — which is what
/// `block_in_place` did, and it let two threads into the unlocked solo store.
#[derive(Clone)]
pub struct AofWriter {
    tx: std::sync::mpsc::SyncSender<Vec<u8>>,
    /// Set for the solo runtime: wait for channel space on this thread instead
    /// of `block_in_place`, which would let another thread run the store.
    single_threaded: bool,
}

impl AofWriter {
    /// Declare that producers run on the solo (single-worker) runtime.
    pub fn single_threaded(mut self, yes: bool) -> Self {
        self.single_threaded = yes;
        self
    }

    /// Log a write command. Serializes on the caller's thread, then enqueues.
    ///
    /// Never drops writes: if the channel is full we apply backpressure by
    /// blocking until the writer drains (so durability is preserved under load).
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

        match self.tx.try_send(buf) {
            Ok(()) => {}
            Err(std::sync::mpsc::TrySendError::Full(buf)) => {
                // Backpressure: wait for the writer thread to catch up. The
                // writer is its own OS thread, so a blocking send always
                // makes progress. On the multi-thread runtime, hand the
                // worker's other tasks to another thread while we wait; on
                // the solo runtime, block right here — the store belongs to
                // this thread alone.
                let multi = !self.single_threaded
                    && matches!(
                        tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()),
                        Ok(tokio::runtime::RuntimeFlavor::MultiThread)
                    );
                let sent = if multi {
                    tokio::task::block_in_place(|| self.tx.send(buf))
                } else {
                    self.tx.send(buf)
                };
                if sent.is_err() {
                    tracing::error!("AOF writer is gone — write not persisted");
                }
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                tracing::error!("AOF writer is gone — write not persisted");
            }
        }
    }
}

/// Spawn the AOF writer thread. Returns a lock-free [`AofWriter`] handle.
///
/// The writer thread:
/// - Receives pre-serialized RESP bytes through a channel
/// - Batch-drains all pending messages per wake-up
/// - Writes through a 64 KB `BufWriter` (reduces syscalls)
/// - Applies fsync according to the chosen policy
/// - On channel close (all senders dropped): flushes + fsyncs + exits
pub fn spawn_aof_writer(path: &Path, policy: FsyncPolicy) -> io::Result<AofWriter> {
    let file = crate::fsutil::open_private_append(path)?;

    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(256 * 1024);

    std::thread::Builder::new()
        .name("solikv-aof".into())
        .spawn(move || {
            use std::sync::mpsc::RecvTimeoutError;
            use std::time::{Duration, Instant};

            let mut writer = BufWriter::with_capacity(64 * 1024, file);
            let mut last_sync = Instant::now();
            loop {
                let wait = Duration::from_secs(1).saturating_sub(last_sync.elapsed());
                match rx.recv_timeout(wait) {
                    Ok(buf) => {
                        let _ = writer.write_all(&buf);
                        // Batch-drain everything pending
                        while let Ok(buf) = rx.try_recv() {
                            let _ = writer.write_all(&buf);
                        }
                        if policy == FsyncPolicy::Always {
                            let _ = writer.flush();
                            let _ = writer.get_ref().sync_data();
                            last_sync = Instant::now();
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        // All senders dropped — final flush
                        let _ = writer.flush();
                        let _ = writer.get_ref().sync_data();
                        break;
                    }
                }
                if last_sync.elapsed() >= Duration::from_secs(1) {
                    let _ = writer.flush();
                    if policy == FsyncPolicy::Everysec {
                        let _ = writer.get_ref().sync_data();
                    }
                    last_sync = Instant::now();
                }
            }
        })?;

    Ok(AofWriter {
        tx,
        single_threaded: false,
    })
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
        let writer = crate::fsutil::open_private_append(path)?;
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
    ///
    /// A file that ends part-way through a command — the crash-while-writing
    /// case — loads everything before it and logs a warning, like Redis'
    /// default `aof-load-truncated yes`. Anything else that does not parse is
    /// corruption and is an `InvalidData` error: replaying past it would
    /// resynchronise on arbitrary bytes and load commands nobody wrote.
    ///
    /// Nothing is allocated from a length in the file before the bytes are
    /// actually there, so a forged header cannot make startup allocate
    /// gigabytes.
    pub fn replay(path: &Path) -> io::Result<Vec<Vec<Bytes>>> {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut commands = Vec::new();
        let mut entry = 0usize;

        let corrupt = |entry: usize, what: &str| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("AOF corrupt at command #{}: {}", entry + 1, what),
            )
        };

        loop {
            let line = match read_line_crlf(&mut reader) {
                Ok(line) => line,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };
            if line.is_empty() {
                continue;
            }
            if !line.starts_with(b"*") {
                return Err(corrupt(entry, "expected '*'"));
            }
            let count: usize = std::str::from_utf8(&line[1..])
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|&c| c > 0 && c <= MAX_AOF_ARGS)
                .ok_or_else(|| corrupt(entry, "bad argument count"))?;

            let mut args = Vec::with_capacity(count.min(64));
            let mut truncated = false;
            for _ in 0..count {
                let len_line = match read_line_crlf(&mut reader) {
                    Ok(l) => l,
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        truncated = true;
                        break;
                    }
                    Err(e) => return Err(e),
                };
                if !len_line.starts_with(b"$") {
                    return Err(corrupt(entry, "expected '$'"));
                }
                let len: usize = std::str::from_utf8(&len_line[1..])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .filter(|&l| l <= MAX_AOF_BULK_LEN)
                    .ok_or_else(|| corrupt(entry, "bad or over-large bulk length"))?;

                // `take` + `read_to_end` grows with the bytes that really
                // exist, instead of `vec![0; len]` up front.
                let mut buf = Vec::new();
                (&mut reader).take(len as u64).read_to_end(&mut buf)?;
                let mut crlf = [0u8; 2];
                if buf.len() < len || reader.read_exact(&mut crlf).is_err() {
                    truncated = true;
                    break;
                }
                if &crlf != b"\r\n" {
                    return Err(corrupt(entry, "bulk not terminated by CRLF"));
                }
                args.push(Bytes::from(buf));
            }
            if truncated {
                tracing::warn!(
                    "AOF {:?} ends part-way through command #{}; loading the {} complete \
                     commands before it",
                    path,
                    entry + 1,
                    commands.len()
                );
                break;
            }
            entry += 1;

            if let Some(cmd) = args.first() {
                let cmd_upper = std::str::from_utf8(cmd)
                    .map(|s| s.to_uppercase())
                    .unwrap_or_default();
                if AOF_BLOCKED_COMMANDS.contains(&cmd_upper.as_str()) {
                    tracing::warn!("Skipping blocked command in AOF replay: {}", cmd_upper);
                    continue;
                }
            }
            commands.push(args);
        }

        Ok(commands)
    }
}

/// Read a line terminated by \r\n, returning content without the terminator.
///
/// Only `*<n>` / `$<n>` headers are read this way, so a line longer than a few
/// dozen bytes is corruption — refused rather than buffered without limit.
fn read_line_crlf<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    const MAX_HEADER_LINE: usize = 64;
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if buf.len() > MAX_HEADER_LINE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "AOF header line too long",
            ));
        }
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

    // SEC-007: blocked commands must be skipped during replay.
    #[test]
    fn test_aof_replay_skips_blocked_commands() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blocked.aof");

        {
            let mut aof = AofPersistence::new(&path, FsyncPolicy::Always).unwrap();
            aof.append(&[Bytes::from("SET"), Bytes::from("ok"), Bytes::from("1")])
                .unwrap();
            aof.append(&[
                Bytes::from("EVAL"),
                Bytes::from("return 1"),
                Bytes::from("0"),
            ])
            .unwrap();
            aof.append(&[Bytes::from("FLUSHALL")]).unwrap();
            aof.append(&[Bytes::from("SHUTDOWN")]).unwrap();
            aof.append(&[Bytes::from("CLUSTER"), Bytes::from("RESET")])
                .unwrap();
            aof.append(&[Bytes::from("SET"), Bytes::from("ok2"), Bytes::from("2")])
                .unwrap();
        }

        let commands = AofPersistence::replay(&path).unwrap();
        // EVAL, SHUTDOWN, CLUSTER skipped (blocked); FLUSHALL is not on the
        // AOF blocklist but still allowed today — see AOF_BLOCKED_COMMANDS.
        let names: Vec<&[u8]> = commands.iter().map(|c| c[0].as_ref()).collect();
        assert!(!names.contains(&b"EVAL".as_ref()));
        assert!(!names.contains(&b"SHUTDOWN".as_ref()));
        assert!(!names.contains(&b"CLUSTER".as_ref()));
        assert!(names.contains(&b"SET".as_ref()));
    }

    // SEC-006: a forged AOF file declaring an over-large bulk length must
    // abort replay rather than allocating gigabytes.
    #[test]
    fn test_aof_replay_rejects_oversized_bulk_length() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized.aof");

        // *2\r\n$3\r\nSET\r\n$<huge>\r\n... — replay should bail out without OOM.
        let huge = MAX_AOF_BULK_LEN + 1;
        let mut f = std::fs::File::create(&path).unwrap();
        write!(f, "*2\r\n$3\r\nSET\r\n${}\r\n", huge).unwrap();
        // Don't even bother writing the body: the length check fires first.
        drop(f);

        // An over-large declared length is corruption, not a truncated tail:
        // replay refuses rather than loading around it.
        let err = AofPersistence::replay(&path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_aof_replay_huge_count_does_not_preallocate() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("count.aof");
        let mut f = std::fs::File::create(&path).unwrap();
        write!(f, "*18446744073709551615\r\n").unwrap();
        drop(f);
        assert!(AofPersistence::replay(&path).is_err());

        let mut f = std::fs::File::create(&path).unwrap();
        // Within bounds, but the file ends: a truncated tail, loaded as empty.
        write!(f, "*1000000\r\n$3\r\nSET\r\n").unwrap();
        drop(f);
        assert!(AofPersistence::replay(&path).unwrap().is_empty());
    }

    #[test]
    fn test_aof_replay_truncated_tail_keeps_complete_commands() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tail.aof");
        let mut f = std::fs::File::create(&path).unwrap();
        write!(f, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n").unwrap();
        // A declared 100-byte bulk with 3 bytes present: crash mid-write.
        write!(f, "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$100\r\nabc").unwrap();
        drop(f);
        let commands = AofPersistence::replay(&path).unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0][1], Bytes::from("a"));
    }

    #[test]
    fn test_aof_replay_garbage_in_the_middle_is_an_error() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mid.aof");
        let mut f = std::fs::File::create(&path).unwrap();
        write!(f, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n").unwrap();
        write!(f, "garbage\r\n").unwrap();
        write!(f, "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n").unwrap();
        drop(f);
        assert!(AofPersistence::replay(&path).is_err());
    }
}
