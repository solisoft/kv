# Changelog

All notable changes to SoliKV are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Version numbers track the `solikv-server` crate.

## [Unreleased]

Security hardening pass (SEC-001 … SEC-016) plus the review fixes that came out
of it. See [SECURITY.md](SECURITY.md) for the full threat model and the current
list of known limitations.

### Breaking

- **`--bind` now defaults to `127.0.0.1`** instead of `0.0.0.0`. A server that
  relied on the old default to accept remote traffic must pass `--bind 0.0.0.0`
  explicitly, and will then also need a password (see protected mode below).
  The published Docker image sets `--bind 0.0.0.0 --dir /data` as its default
  `CMD`, so container deployments keep working, but still need a password.
- **An empty password is refused at startup.** An empty `--requirepass`,
  `--requirepass-file`, or `SOLIKV_REQUIREPASS` previously satisfied protected
  mode while accepting `AUTH ""` and a bare `Authorization: Bearer ` header.
  Unset the variable to run without authentication, or set a real secret.
- **Half-specified TLS flags are refused at startup.** `--tls-cert` without
  `--tls-key` (or `--tls-client-ca` with no server certificate) used to log
  "TLS disabled" and serve cleartext; it now exits with an error.
- **`REPLICAOF` / `SLAVEOF` return an error** instead of replying `OK` without
  doing anything. Replication stays disabled until the cluster bus has HMAC
  authentication and TLS (SEC-016). There is no `--replicaof` flag.

### Added

- **TLS on the REST API** (SEC-002). `--tls-cert` / `--tls-key` now enable TLS
  on both the RESP and REST ports; previously only RESP was covered and the REST
  port needed a reverse proxy.
- **mTLS** (SEC-002). `--tls-client-ca PATH` requires client certificates on
  both ports. The flag existed before but was reserved and ignored.
- **Protected mode** (SEC-001). Binding to a non-loopback address with no
  password is refused; `--protected-mode no` opts out. Note that TLS does not
  satisfy protected mode — it provides encryption, not authentication.
- **Connection limits on REST**: concurrent connections are capped at 10 000
  (matching RESP), and a TLS handshake that does not complete within 10 seconds
  is dropped, so an unauthenticated peer cannot pin connection tasks.
- **Debug-build aliasing detector for solo mode**: `--solo` keeps its store in
  an unlocked `UnsafeCell`, and debug builds now panic if two accesses overlap.
  Compiled out of release builds entirely.

### Fixed

- **`AUTH` and `Bearer` tokens could be accepted when wrong.** The constant-time
  comparison cast a `usize` length difference to `u8`, so any difference that was
  a multiple of 256 truncated to zero and was ignored — `Bearer <password><256
  bytes>` authenticated. The comparison now folds the length in at full width,
  and its timing still depends only on the length of the configured secret.
- **A transient `accept()` error took down the REST server, and with it the
  process.** Wiring TLS into REST replaced `axum::serve` with a hand-rolled
  accept loop that propagated errors; `EMFILE` under fd exhaustion or a
  connection aborted mid-handshake was enough. Per-connection errors are now
  skipped and resource exhaustion is logged and retried.
- **Undefined behaviour in solo mode.** `--solo` runs one Tokio worker, but
  `KEYS`, `FLUSHDB`/`FLUSHALL`, and `SAVE`/`BGSAVE` used `block_in_place`, which
  hands the worker's queue to a replacement OS thread — that thread then aliased
  `&mut ShardStore` while the blocking command held it. These commands now run
  inline in solo mode.
- **`SAVE` / `BGSAVE` ignored `--dir` and `--dbfilename`**, always writing
  `data/dump-*.rdb`. They now use the configured directory and basename, so a
  manual save no longer diverges from the snapshots written at startup and on
  the background timer.
- **A full AUTH failure table silently disabled rate limiting.** Once 10 000 IPs
  were tracked, failures from any new IP went unrecorded, so seeding the table
  (a routed IPv6 /64 is enough) turned the limiter off for everyone. The table
  now evicts its least valuable entries — not currently serving a cooldown, then
  least recently active — so a flood cannot lift an active block.
- **The AUTH failure table was swept on the read path**, which REST hits on every
  request: above 5 000 entries each request took a write lock and scanned the
  whole map, turning the limiter into a global mutex on the request path exactly
  during the flood it was meant to bound. Eviction now happens only when
  recording a failure.
- **AOF replay trusted declared lengths for allocation.** A ~20-byte header could
  reserve 512 MB, and an array count was trusted for `Vec::with_capacity`.
  Declared lengths are now capped against the file's actual size, array counts
  are capped at 16 M, and argument vectors grow on push.
- **The sample REST auth middleware accepted any non-empty API key.**
  `www/app/middleware/auth.sl` now requires a configured secret and rejects
  every request until one is set.
- **`--tls-key` could fail on a valid key file.** The parser took the first PEM
  block with a plausible tag, so a leading block it could not decode (an OpenSSH
  key, say) masked a usable PKCS#8 key further down. It now takes the first block
  that actually parses. OpenSSH-format keys are not supported and now say so.

### Security

- **Lua scripts can no longer reach server-control commands.** The `redis.call`
  denylist gained `FLUSHALL`, `FLUSHDB`, `CONFIG`, `SAVE`, `BGSAVE`, `SHUTDOWN`,
  `DEBUG`, `SLAVEOF`, `REPLICAOF`, `CLUSTER`, `MODULE`, `MIGRATE`, `RESTORE`,
  and `BGREWRITEAOF`.
- **AUTH rate limiting** (SEC-004) is now shared between the RESP and REST paths
  rather than duplicated, and its memory is bounded at 10 000 tracked IPs.

### Documentation

- `SECURITY.md`: documents the empty-password and TLS-flag validation, the
  password-comparison guarantee, connection limits, the AOF replay caps, and the
  tracker's eviction policy. Records the remaining solo-mode AOF backpressure
  hazard under known limitations.
- `README.md` and `--bind`'s help text no longer suggest that TLS satisfies
  protected mode — a non-loopback bind requires a password either way.
- Docs site: corrected the `--bind` default; documented `--solo`,
  `--requirepass-file`, `--protected-mode`, and the three TLS flags; documented
  REST TLS and the 429 rate-limit response; removed the `--replicaof` flag and
  the `REPLICAOF` examples, which described behaviour that does not exist; noted
  that `BGSAVE` is currently an alias for `SAVE`.
- `tests/chaos_cluster_test.sh` asserts that `REPLICAOF` is refused and the role
  stays `master`, instead of suppressing stderr and testing nothing.

## [0.4.2]

Baseline for this changelog. Earlier history is in the git log.
