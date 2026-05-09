# SEC-013 — No rate limiting on RESP `AUTH` or REST Bearer attempts; brute-forceable passwords

**Severity:** Medium

## Location

- `crates/solikv-server/src/resp_server.rs:163-191` (RESP `AUTH` handler — no failure counter, no delay)
- `crates/solikv-server/src/rest_server.rs:90-122` (REST `auth_middleware` — no failure counter, no delay)
- `crates/solikv-server/src/resp_server.rs:19` (`MAX_CONNECTIONS = 10_000`)

## Issue

Both authentication paths use a constant-time string comparison (good) but
neither imposes a delay on failure, locks out a misbehaving peer IP, nor caps
the number of attempts per connection. A client can pipeline thousands of
`AUTH <guess>` per TCP connection (RESP processes commands in batch) and open
up to 10 000 simultaneous connections.

With a typical 8-character ASCII-printable password (~52 bits of entropy)
this is still computationally infeasible offline, but with a *weak* password
(dictionary word, defaulted secret, leaked from another system) it is brute-
forceable in minutes from a LAN attacker.

The REST side is even worse: each request goes through `auth_middleware`
independently with no per-IP throttling.

## Fix

1. After N (suggested: 5) failed `AUTH` attempts on a single connection,
   close it and emit a structured warning log.
2. Add a global token-bucket per peer-IP for failed AUTH (suggested: 30
   failures/minute, then 1-second delay before the response). Use a
   `dashmap::DashMap<IpAddr, AttemptState>` cleaned up periodically to bound
   memory.
3. Apply the same logic to the REST middleware: after the per-IP failure
   threshold, return 429 Too Many Requests with `Retry-After`.

## Verification

- Unit: 6 wrong AUTHs on the same connection ⇒ connection closed after 5th.
- Unit: 31 wrong AUTHs from one IP ⇒ subsequent requests delayed/rejected.
