# SEC-002: No TLS on RESP or REST — AUTH password and data plaintext on the wire

- **Severity:** High
- **Status:** Todo
- **Location:** `crates/solikv-server/src/resp_server.rs:28`, `crates/solikv-server/src/rest_server.rs:84-86`, `crates/solikv-server/Cargo.toml`

**Issue:** Both servers use raw `TcpListener::bind` with no TLS support whatsoever (no `rustls`, no `tokio-rustls`, no `axum-server` TLS feature). The RESP `AUTH` password and the REST `Authorization: Bearer …` token are sent in clear text alongside every request, as is the entire keyspace. A passive observer on any segment between client and server gets the master credential and full data visibility; an active MITM gets full takeover.

**Fix:** Add a TLS option (e.g. `rustls` + `tokio-rustls`) gated by `--tls-cert`/`--tls-key`/`--tls-client-ca` flags for both ports. Document in README that the cleartext mode is loopback-only.
