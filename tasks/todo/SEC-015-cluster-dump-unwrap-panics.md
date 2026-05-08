# SEC-015 — `cluster_dump` panics on malformed CLI input via `addr.parse().unwrap()` and similar

**Severity:** Low

## Location

- `crates/solikv-server/src/cluster_dump.rs:163` — `TcpStream::connect_timeout(&addr.parse().unwrap(), ...)`
- `crates/solikv-server/src/cluster_dump.rs:267-271` — `a[0].parse::<u16>().unwrap().cmp(...)`
- `crates/solikv-cluster/src/gossip.rs:248-251, 287-290` — `SystemTime::...duration_since(UNIX_EPOCH).unwrap()`
- `crates/solikv-cluster/src/cluster.rs:268-270` — same `.unwrap()` on slot parse during `cluster_slots()` sort

## Issue

Several call sites unwrap parser results that originate from operator- or
peer-supplied input. They will not corrupt data, but they crash the server (or
the standalone dump/restore tool) on malformed input:

- `cluster_dump --cluster-connect "not a host"` ⇒ panic in
  `addr.parse().unwrap()`.
- A `CLUSTER NODES` response with a non-numeric slot field ⇒ panic in the sort
  closure.
- A system clock that goes backwards before UNIX epoch (very unlikely on
  servers, but possible on misconfigured VMs / containers booting with bad
  RTC) ⇒ every gossip pong-handler tick panics.

This pattern is also a future-availability risk: any new code path that
forwards untrusted strings into one of these unwrap sites becomes a remote
DoS.

## Fix

Replace `.unwrap()` on parser results with explicit error handling:

```rust
let socket: SocketAddr = addr.parse()
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput,
                                format!("bad address {addr:?}: {e}")))?;
```

For the slot sort, use `unwrap_or(0)` (or filter out unparseable slots before
sorting). For SystemTime, use `unwrap_or_default()` — a zero offset is a
sane fallback for a clock that briefly reports pre-epoch.

Add `#[deny(clippy::unwrap_used)]` at crate level on
`solikv-server` and `solikv-cluster` to keep this from regressing
(allow on tests only).

## Verification

- Unit: invalid `--cluster-connect` returns an `Err`, not a panic.
- `cargo clippy --quiet -- -D warnings` clean (with the new deny).
