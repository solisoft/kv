# SEC-002 — RESP parser recurses without depth limit, allowing single-packet stack overflow / crash

**Severity:** High

## Location

- `crates/solikv-resp/src/codec.rs:227-269` (`decode_array`, recursive call to `decode_frame`)
- `crates/solikv-resp/src/codec.rs:131` (`MAX_ARRAY_LEN = 1_048_576`)

## Issue

`decode_array` calls `decode_frame(&src[offset..])` for every element, and
`decode_frame` dispatches back to `decode_array` for nested arrays
(`*` prefix). There is no recursion-depth limit. An attacker connected to the
RESP port can send a single buffer containing `*1\r\n*1\r\n*1\r\n…` repeated a
few thousand times. Each level adds a stack frame, and the recursion eventually
overflows the (default 2 MB) thread stack — the Tokio worker dies with SIGSEGV
and the server's connection accept loop on that worker is gone.

This is *pre-auth* (the parser runs before AUTH is checked) and unauthenticated
remote callers can crash the server with a single TCP packet. `MAX_ARRAY_LEN`
limits each level's element count but does not bound nesting depth.

## Fix

1. Add a `decode_frame_with_depth(src, depth)` helper. Bump `depth` on each
   recursive call from `decode_array`. Reject (return `Err`) when `depth`
   exceeds a hard cap (suggested: 32, matching Redis client/server typical
   nesting use).
2. Change the public `decode_frame(src)` to call
   `decode_frame_with_depth(src, 0)`.
3. Add a unit test that constructs an N-deep nested array (e.g. 10 000) and
   asserts the parser returns an error rather than panicking / overflowing.

## Verification

- Unit: deep-nested input returns `Err`, not a stack overflow.
- Existing parser tests still pass.
- `cargo clippy --quiet -- -D warnings` clean.
