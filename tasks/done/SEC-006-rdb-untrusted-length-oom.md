# SEC-006: RDB loader allocates `Vec`/`HashMap` from untrusted u32 length headers — OOM / panic on malicious file

- **Severity:** Medium
- **Status:** Todo
- **Location:** `crates/solikv-persist/src/rdb.rs:152-194`, `rdb.rs:266-277`

**Issue:** `read_len` reads a raw `u32` from the file and returns it as `usize`. The loader then immediately uses it for unbounded allocations:

- `let mut buf = vec![0u8; len];` (`read_bytes`, line 268) — up to ~4 GB allocation per call.
- `VecDeque::with_capacity(len)` for lists.
- `HashMap::with_capacity(len)` for hashes — capacity is multiplied by slot size, so a 4 G capacity is many tens of GB of header allocation.
- `HashSet::with_capacity(len)` for sets.

A crafted `dump-N.rdb` placed in `args.dir` (or supplied via `--import-redis-rdb`) makes the loader OOM-kill the server during start-up, before any client even connects. The same pattern in AOF replay (`crates/solikv-persist/src/aof.rs:204` — `let mut buf = vec![0u8; len];`) has identical behaviour.

**Fix:** Sanity-check `len` against the remaining file size (`file.metadata()?.len()` minus current offset) before each allocation, and cap with a hard limit (e.g. `MAX_VALUE_LEN = 512 MB`, `MAX_COLLECTION_LEN = 16 M`). For collections, prefer `with_capacity(min(len, REASONABLE_CAP))` and grow on push.

**Threat model:** assumes an attacker who can write into `args.dir` (compromised host, multi-tenant volume, restored backup) — not remotely reachable, but degrades operator confidence in restart-from-disk recovery.
