# SEC-014 — Gossip server uses a fixed 1024-byte read buffer with no framing, silently truncating long messages

**Severity:** Low

## Location

- `crates/solikv-cluster/src/gossip.rs:346` (`let mut buf = [0u8; 1024];`)
- `crates/solikv-cluster/src/gossip.rs:368-379` (`stream.read(&mut buf)` then `decode(&buf[..n])`)

## Issue

The gossip listener reads a single TCP `read()` worth of bytes (≤ 1024) and
hands them to `GossipMessage::decode`. This has two consequences:

1. **Truncation when packets are larger than 1024 B.** A future
   `UPDATE <id> <ip> <port> <comma-separated flags> <master>` with a long
   flags list, or a node ID longer than expected, simply gets cut. `decode`
   then returns `None` and the message is silently dropped — there is no
   reassembly across reads.

2. **Boundary fragility.** Two messages arriving back-to-back may be coalesced
   by TCP into a single read; `decode` parses only the first line and
   discards the second.

With a reasonable cluster, the impact today is "occasional missed gossip,
re-sent next tick". But once `GossipMessage` carries authentication tags
(see SEC-009), TLS, or cluster-state diffs, this fragility becomes a real
problem.

## Fix

1. Switch to a length-prefixed framing (e.g. 2-byte BE length + payload) and
   keep a per-connection accumulator buffer. Use `tokio_util::codec::Framed`
   with a small custom codec, or hand-roll it.
2. Cap maximum frame size at e.g. 16 KiB to bound memory.
3. Make the per-connection buffer grow to that cap rather than being a
   stack-allocated 1024-byte array shared across the loop iteration.

## Verification

- Unit: a synthesized 2 KB UPDATE message round-trips (encode → split into
  two TCP reads → decode) successfully.
- Manual: introduce a long node-id and watch tracing logs to confirm no
  truncation warnings.
