# SEC-013: RESP decoder pre-allocates `Vec::with_capacity(N)` from untrusted array length, before AUTH

- **Severity:** Medium
- **Status:** Todo
- **Location:** `crates/solikv-resp/src/codec.rs:130-269`, `crates/solikv-server/src/resp_server.rs:67-110`

**Issue:** `decode_array` accepts `*N\r\n` with `N` up to `MAX_ARRAY_LEN = 1_048_576` and immediately runs `Vec::with_capacity(count)` over `RespFrame` slots (~32 B each ⇒ ~32 MB per single declared array). The decoder runs *before* the AUTH check in `resp_server::handle_connection` — an unauthenticated attacker can open `MAX_CONNECTIONS = 10_000` sockets and send `*1048576\r\n` on each, forcing the server to immediately allocate ~320 GB of `Vec` capacity. The connection-level read-buffer cap (`MAX_READ_BUF = 256 MB`) does not bound this, since it counts only buffered bytes, not the decoder's working allocations. Same applies to `MAX_BULK_STRING_LEN = 512 MB` declared in `$<len>\r\n` headers — the bulk path waits for the bytes before allocating, but a slow drip (Slowloris-style) keeps thousands of half-filled bulk buffers alive.

**Fix:**
- Reject any `*N`/`$L` frame from a not-yet-authenticated peer where `N`/`L` exceeds a small pre-auth limit (e.g. 32 elements / 64 KB).
- Replace `Vec::with_capacity(count)` with grow-on-push, or cap to `min(count, 4096)` and reallocate as needed.
- Track per-connection memory footprint (declared bulk + array sizes still pending) and apply the existing `MAX_READ_BUF` semantics to it.
