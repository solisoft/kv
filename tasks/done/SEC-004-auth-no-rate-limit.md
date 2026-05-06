# SEC-004: AUTH has no rate limit, lockout, or attempt logging

- **Severity:** Medium
- **Status:** Todo
- **Location:** `crates/solikv-server/src/resp_server.rs:163-191`, `crates/solikv-server/src/rest_server.rs:90-122`

**Issue:** Wrong AUTH attempts return `-ERR invalid password` and the connection stays open — there is no per-IP backoff, no global throttle, no failed-attempt logging. With `MAX_CONNECTIONS = 10_000` and `constant_time_eq` running on a 64-byte buffer, an unauthenticated attacker can pipeline tens of thousands of `AUTH` guesses per connection and open thousands of connections in parallel. The REST middleware has the same shape — every request can carry a different bearer guess. There is no audit log of failed attempts, so brute-force activity is invisible to the operator.

**Fix:** Track failed AUTH count per peer IP; after N failures (e.g. 10) close the socket and add a temporary block. Log each failed AUTH at `tracing::warn` with the peer address. Recommend (or enforce) a minimum `requirepass` length.
