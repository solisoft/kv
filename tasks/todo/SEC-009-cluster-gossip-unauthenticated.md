# SEC-009 — Cluster gossip messages are unauthenticated, allowing topology spoofing on any reachable bus port

**Severity:** Medium

## Location

- `crates/solikv-cluster/src/gossip.rs:102-178` (`GossipMessage::encode` / `decode` — plain space-separated text)
- `crates/solikv-cluster/src/gossip.rs:300-328` (`handle_meet`, `handle_update` — overwrite node info with no verification)
- `crates/solikv-cluster/src/gossip.rs:331-389` (`start_gossip_server` — no shared secret, no signature)

## Issue

Gossip messages have zero integrity protection: any peer that connects to the
cluster bus port can send `MEET <id> <ip> <port>` and `UPDATE <id> <ip> <port>
<flags> <master>` packets, and the receiver applies them blindly via
`handle_update`. Specifically `handle_update` (line 304) overwrites the
`ip` / `port` fields of an existing node — meaning a malicious peer can
redirect cluster traffic for any node ID it can guess or learn.

Mitigations *currently in place*:

- Listener binds to `127.0.0.1` only (`gossip.rs:336`).
- Non-loopback peers are rejected unless their IP is already in the known-nodes
  list (`gossip.rs:352-364`).

These help when the cluster runs on a single host, but the moment the operator
exposes the bus port (multi-host cluster, container with mapped port,
mis-configured firewall), there is no second line of defense — no shared
cluster secret, no HMAC, no TLS. Compare with Redis cluster's `cluster-auth`
shared key.

## Fix

1. Add `--cluster-secret <SECRET>` (also via env / file per SEC-005). When
   set, every gossip message is wrapped with an HMAC-SHA256 over the message
   bytes; receivers reject messages whose MAC does not verify or that lack a
   MAC.
2. While in there, switch the wire format to a length-prefixed framed format
   so the 1024-byte read buffer (`gossip.rs:346`) does not silently truncate
   long UPDATE messages — see SEC-014.
3. Document that `--cluster-secret` is **required** when running across more
   than one host. Refuse to start with `cluster_enabled` + non-loopback bind +
   no secret (mirroring SEC-007).
4. Validate `ip` and `port` in `handle_update` / `handle_meet`: parse as a
   real `SocketAddr`, refuse loopback↔public swaps mid-session, refuse port
   0 / >65535.

## Verification

- New unit: signed message accepted; unsigned/tampered rejected.
- New unit: `handle_update` with malformed IP refuses to overwrite.
