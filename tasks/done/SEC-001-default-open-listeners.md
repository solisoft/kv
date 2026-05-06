# SEC-001: RESP and REST listeners default-open on 0.0.0.0 with no auth

- **Severity:** Critical
- **Status:** Todo
- **Location:** `crates/solikv-server/src/main.rs:38-39`, `crates/solikv-server/src/main.rs:65-67`, `crates/solikv-server/src/resp_server.rs:21-65`, `crates/solikv-server/src/rest_server.rs:90-122`

**Issue:** The CLI defaults `--bind 0.0.0.0` and `--requirepass` is `Option<String>` with no default. When the user just runs `solikv`, both the RESP server (port 6379) and the REST API (port 5020) bind to every interface and accept commands without authentication — `auth_middleware` short-circuits to `next.run(req).await` when `state.password` is `None`, and `resp_server` takes the fast path that skips the `conn.authenticated` check entirely. Anything reachable on the network gets full read/write to the keyspace, including `FLUSHALL`, `KEYS *`, `EVAL`, `CONFIG SET`, and `CLUSTER MEET`.

**Fix:** Adopt Redis-style "protected mode": when `requirepass` is unset, refuse to bind to a non-loopback address unless `--protected-mode no` (or equivalent) is passed. Log a startup warning when running with no password. Consider defaulting `--bind` to `127.0.0.1`.
