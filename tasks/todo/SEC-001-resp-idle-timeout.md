# SEC-001 — RESP server has no read/idle timeout, enabling connection exhaustion DoS

**Severity:** High

## Location

- `crates/solikv-server/src/resp_server.rs:67-110` (`handle_connection`)
- `crates/solikv-server/src/resp_server.rs:19` (`MAX_CONNECTIONS = 10_000`)

## Issue

`handle_connection` calls `socket.read_buf(&mut read_buf).await` (line 105) with no
read timeout. A connection that sends nothing (or a partial RESP frame) sits forever
in `read_buf` and holds one of the 10,000 semaphore permits.

A single attacker opening 10,000 idle TCP connections (no bytes ever sent) drains
the connection pool and locks every legitimate client out. The pre-pubsub-mode read
loop, the pubsub-mode `socket.read_buf` (line 611), and the `auth_middleware`
fast-fail path all share this property: no per-stage deadline.

There is also no `tcp_keepalive` configuration, so half-open TCP connections are
never reaped.

## Fix

1. Wrap the per-iteration `socket.read_buf(...)` calls in
   `tokio::time::timeout(...)` with a configurable idle deadline (suggested
   default: 60 s, matching Redis `timeout 0` override).
2. On timeout, write a RESP error and close the connection.
3. Apply the same timeout in `handle_pubsub_mode` (`resp_server.rs:611`).
4. Optionally enable TCP keepalive on the accepted socket via
   `socket2::SockRef::from(&stream).set_tcp_keepalive(...)`.

## Verification

- Unit: open a TCP connection, send 0 bytes, assert it is closed after the
  configured timeout.
- Manual: `nc -q -1 127.0.0.1 6379 &` ×10 000, then verify a fresh `redis-cli`
  client can still connect within `timeout * 2`.
