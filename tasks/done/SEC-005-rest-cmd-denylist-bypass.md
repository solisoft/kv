# SEC-005: REST `/cmd` denylist misses dangerous commands (KEYS, RENAME, RESET, FAILOVER…)

- **Severity:** Medium
- **Status:** Todo
- **Location:** `crates/solikv-server/src/rest_server.rs:445-483`

**Issue:** `REST_BLOCKED_COMMANDS` is a hand-curated denylist intended to block destructive ops on the `POST /cmd` endpoint. It lists `FLUSHDB`, `FLUSHALL`, `SHUTDOWN`, `EVAL`, etc. — but omits several commands that the dispatcher does honour:

- `KEYS *` — full O(n) keyspace scan, blocking DoS, plus information disclosure of every key in the store.
- `SCAN`/`HSCAN`/`ZSCAN` with attacker-chosen `COUNT` — same DoS shape.
- `RENAME`/`RENAMENX` — silent data overwrite.
- `RESET`, `CLIENT KILL` (when wired), `LATENCY`, `MEMORY`, `WAIT`, `FAILOVER` — server-state changes.
- `BF.RESERVE` and other untyped allocators — unbounded allocation.

Denylists also drift: any new command added in `dispatch.rs` is silently REST-reachable until someone remembers to update this constant.

**Fix:** Invert to an allowlist of REST-safe commands, or route every `/cmd` request through the same authorization model as RESP and require an explicit `--enable-rest-cmd-passthrough` flag for arbitrary commands. At minimum, add `KEYS`, `SCAN`/`HSCAN`/`ZSCAN`, `RENAME`, `RENAMENX`, `RESET`, `LATENCY`, `MEMORY`, `WAIT`, `FAILOVER`, `CLIENT` to the blocked list.
