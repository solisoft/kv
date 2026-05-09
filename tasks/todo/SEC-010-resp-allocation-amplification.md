# SEC-010 — RESP decoder pre-allocates from untrusted lengths, enabling per-connection memory amplification

**Severity:** Medium

## Location

- `crates/solikv-resp/src/codec.rs:227-269` (`decode_array`, `Vec::with_capacity(count)` on line 254)
- `crates/solikv-resp/src/codec.rs:188-225` (`decode_bulk_string`, `Bytes::copy_from_slice` of length-prefixed buffer)
- `crates/solikv-resp/src/codec.rs:131` (`MAX_ARRAY_LEN = 1_048_576`)

## Issue

Two amplification vectors:

1. **Array preallocation.** `decode_array` accepts any `count ≤ MAX_ARRAY_LEN`
   (1 048 576) and immediately calls `Vec::with_capacity(count)`. Each
   `RespFrame` is at least 24 B on 64-bit (enum tag + `Bytes` overhead). At
   the cap, a single `*1048576\r\n` line followed by no further data forces
   the server to reserve ≥ 24 MiB before any data has even arrived.
   Multiplied by `MAX_CONNECTIONS = 10 000` this is 240 GiB of reservation
   from idle attackers.

2. **Bulk-string copy on speculative length.** `decode_bulk_string` accepts
   any length up to `MAX_BULK_STRING_LEN = 512 MiB`. The framed read buffer is
   capped at 256 MiB (`resp_server.rs:76`), so 512 MiB inputs cannot complete
   — but `Bytes::copy_from_slice(&src[..len])` still runs once `total_needed`
   is satisfied, which means up to a 256 MiB heap allocation per legitimate
   in-flight request.

In aggregate, an authenticated attacker can drive memory pressure several
orders of magnitude above the actual data bandwidth they consume.

## Fix

1. Replace `Vec::with_capacity(count)` in `decode_array` with `Vec::new()` and
   let it grow on `push`. The amortized cost is the same; the worst-case
   allocation is bounded by what the attacker actually sends.
2. Lower the per-frame caps to something realistic. Suggested:
   - `MAX_BULK_STRING_LEN = 64 MiB` (128 KiB is more typical for K/V workloads
     but 64 MiB matches Redis client buffer limits).
   - `MAX_ARRAY_LEN = 1 048 576` is fine to keep as an upper bound, but
     additionally cap *aggregate* allocation across all frames in a single
     `BytesMut` — i.e. add a per-connection memory budget.
3. Optionally make the limits configurable via CLI (`--proto-max-bulk-len`,
   `--client-output-buffer-limit`).

## Verification

- Unit: feeding `*1048576\r\n` plus a minimal payload no longer triggers a
  multi-MB allocation immediately.
- Existing parser tests still pass.
