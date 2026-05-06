# SEC-016: Cluster bus and replication paths have no auth/TLS — dead today, footgun when wired

- **Severity:** Low
- **Status:** Todo
- **Location:** `crates/solikv-cluster/src/gossip.rs:331-390`, `crates/solikv-server/src/replication.rs:113-195`

**Issue:** Neither `start_gossip_server` nor `connect_to_master` is actually called from `main.rs` today (verified with grep) — the modules ship as scaffolding. When they get wired up they are pre-positioned to ship without authentication or transport security:

- **Gossip server.** Binds `127.0.0.1:port` (good), but the only filter for non-loopback peers is `state.get_all_nodes().iter().any(|n| n.ip == peer_addr.ip().to_string())` — IP-based, no shared secret, no PSK, no signature on `MEET`/`PING`/`PONG`/`UPDATE` messages. `handle_update` happily rewrites a known node's `ip`/`port`/`flags`/`master_id` based on whatever the wire said. On a multi-tenant host any local process binds to a high port and gossips itself in.
- **Replication.** `connect_to_master` is `TcpStream::connect` with no AUTH handshake and no TLS. `replicate_command` writes `SET`/`DEL`/etc. unsigned. Pairs with SEC-002 (no TLS) — replication password and replicated values transit plaintext.

Because the code is unused, there is no exploit *today* — but the next "wire up replication" PR will inherit these gaps unless the trust model is fixed first.

**Fix:** Before exposing either module, add a shared `cluster-bus-secret` (or per-pair PSK) that signs every gossip frame with HMAC-SHA-256, verify on receive, and drop unverifiable messages. Add TLS support to the cluster bus and to replica→master connections. Reject `UPDATE` messages whose `node_id` differs from the sender's verified identity.
