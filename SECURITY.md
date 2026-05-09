# Security

This document describes SoliKV's security model and the hardening shipped in
the SEC-001…SEC-016 series.

## Reporting vulnerabilities

Please report security issues privately to the maintainers rather than via
public GitHub issues. Include reproduction steps and affected versions.

## Threat model

SoliKV is a Redis-protocol-compatible server. Two trust boundaries matter:

1. **Network → server**: any party who can open a TCP connection to the
   RESP port (6379) or the REST port (5020).
2. **Local user → daemon**: any local UNIX user who shares the host (e.g.
   multi-tenant deployments, shared CI runners).

Out of scope: an attacker with root on the same host, or with write access
to the data directory after server startup.

## Hardening summary

### Network exposure

- **Protected mode (SEC-001)**: starting with `--bind` set to a non-loopback
  address while `--requirepass` is unset is refused by default. Operators must
  either set a password or pass `--protected-mode no` to opt out.
- **TLS for RESP (SEC-002)**: pass `--tls-cert PATH --tls-key PATH` to enable
  TLS termination on the RESP port. REST TLS is not yet wired (planned
  follow-up). The `--tls-client-ca` flag is reserved for mTLS.
- **AUTH rate limiting (SEC-004)**: both the RESP server and the REST
  middleware track failed AUTH attempts per peer IP and block the IP for
  30 s after 10 failures, logging every failed attempt at `warn`. The REST
  side uses `into_make_service_with_connect_info::<SocketAddr>()` so the
  middleware sees the real peer address.

### Command surface

- **REST denylist (SEC-005)**: `POST /cmd` rejects destructive or DoS-prone
  commands (`KEYS`, `SCAN`/`HSCAN`/`ZSCAN`/`SSCAN`, `RENAME`, `RENAMENX`,
  `RESET`, `LATENCY`, `MEMORY`, `WAIT`, `FAILOVER`, `CLIENT`, `INFO`,
  `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `SHUTDOWN`, `EVAL`, `CONFIG`, …).

### Persistence

- **RDB length sanitization (SEC-006)**: `read_bytes` rejects any chunk longer
  than 512 MB (`MAX_VALUE_LEN`); collection counts are clamped to 16 M
  (`MAX_COLLECTION_LEN`) before `with_capacity`.
- **AOF length sanitization (SEC-006)**: bulk lengths above 512 MB
  (`MAX_AOF_BULK_LEN`) abort the replay.
- **AOF replay denylist (SEC-007)**: `EVAL`, `EVALSHA`, `SCRIPT`, `DEBUG`,
  `SHUTDOWN`, `BGREWRITEAOF`, `BGSAVE`, `SLAVEOF`/`REPLICAOF`, `CLUSTER`,
  `MIGRATE`, `RESTORE`, `MODULE` are skipped (with a `warn`) on replay.
  The data directory must still be on a filesystem only the SoliKV UID can
  write to — replay is not authenticated.

### Operational

- **PID file (SEC-003)**: moved from `/tmp/solikv.pid` to
  `<data-dir>/solikv.pid`. The "kill old daemon" path resolves
  `/proc/<pid>/exe` with `fs::read_link`, canonicalizes both that and
  `current_exe()`, and only signals the PID if the canonical paths match.
  Signalling uses `nix::sys::signal::kill` directly, not a shell-out.
- **Cluster dump password (SEC-008)**: `solikv --cluster-dump` /
  `--cluster-restore` only sends `--cluster-password` to the seed node by
  default. To AUTH against discovered nodes, pass
  `--cluster-password-seed-only=false`.
- **Password sources (SEC-009)**: `--requirepass`/`--cluster-password` may be
  supplied via `--requirepass-file PATH` / `--cluster-password-file PATH`
  (preferred) or the `SOLIKV_REQUIREPASS` / `SOLIKV_CLUSTER_PASSWORD` env
  vars. Password files are opened with `O_NOFOLLOW` and **rejected** if the
  mode is looser than `0600` (any group/world bits set ⇒ startup fails).
  CLI flags remain for compatibility but log a `warn` because
  `/proc/<pid>/cmdline` exposes them to other local users.
- **Installer (SEC-011)**: `install.sh` downloads `SHA256SUMS` alongside the
  release tarball, greps out exactly the line for the tarball it just
  downloaded, and verifies that single line with `sha256sum -c --strict`
  (or `shasum -a 256`). Missing-tarball, missing-checksum-line, and tag
  fetch failures all abort with a non-zero exit. `tar --no-same-owner` is
  used during extraction.

### Implementation hardening

- **EVAL/EVALSHA arithmetic (SEC-012)**: `numkeys + 2` uses `checked_add`;
  overflow returns `ERR Number of keys can't be greater than number of args`
  instead of panicking the connection. Same fix applied to `ZUNIONSTORE` and
  `ZINTERSTORE`.
- **RESP decoder allocation (SEC-013)**: `decode_array` no longer calls
  `Vec::with_capacity(N)` from an untrusted `*N\r\n` header — capacity grows
  on push. The decoder additionally takes an `authenticated` flag; an
  unauthenticated peer cannot declare arrays larger than
  `MAX_UNAUTH_ARRAY_LEN` (32) or bulk strings larger than
  `MAX_UNAUTH_BULK_LEN` (64 KiB). The cap applies recursively, so a bulk
  embedded inside an array is also bound. The RESP server passes the
  current `conn.authenticated` state into every decode call.
- **FFI memory safety (SEC-010)**: `static mut KV_STORE` replaced with
  `OnceLock<Mutex<HashMap>>`. Every `solikv_*` extern returns `-1` on null
  pointers and validates lengths via separate `validate_key`/`validate_value`
  helpers, so keys are bounded by `MAX_KEY_LEN` (4 KiB) and values by
  `MAX_VALUE_LEN` (64 KiB) without conflating the two. The crate is now in
  the workspace and covered by `cargo test`.
- **Lua VM hygiene (SEC-014)**: when a pooled VM is created, the post-setup
  global key set is snapshotted. On reuse, every key on `_G` that is not in
  the snapshot is wiped, then `sandbox_lua` and `setup_redis_module` are
  re-applied — so a previous script cannot leak `_G.foo = 'secret'` to the
  next script that lands on this thread, nor permanently replace
  `redis.call`. Mutations to the *contents* of pre-existing tables
  (e.g. clearing `string.upper`) are still possible; full isolation requires
  per-script `_ENV`, which we hold for a follow-up if cross-tenant scoping
  ever lands.
- **Build artifact hygiene (SEC-015)**: `.gitignore` now excludes `*.rmeta`,
  `*.rcgu.o`, `*.rcgu`, `*.o`, `*.rlib`, `*.dylib`, `*.so`, `*.dll`, `*.pdb`,
  `*.pdb.lock` to prevent accidentally committing rustc artifacts.
- **Cluster bus / replication (SEC-016)**: `gossip.rs` and `replication.rs`
  carry `SECURITY NOTE (SEC-016)` blocks describing the auth/TLS/HMAC work
  required before the modules are wired into `main.rs`.

## Known limitations and follow-ups

- **REST TLS is not implemented** (SEC-002 follow-up): only the RESP port
  has TLS today. Front the REST port with a TLS-terminating reverse proxy
  if it is exposed beyond loopback.
- **Cluster bus / replication TLS + HMAC** (SEC-016): scaffolding only; do
  not wire these modules to a non-loopback listener until the work
  described in the in-file `SECURITY NOTE (SEC-016)` is done.
- **Lua nested-table isolation** (SEC-014 partial): top-level `_G` keys and
  the `redis.*` module are restored between scripts, but mutations to the
  contents of inherited tables (`string`, `math`, `table`, …) are not.
  Adequate for the single-tenant model; revisit if per-tenant scopes land.
- **Argv password leakage**: `--requirepass <pw>` and `--cluster-password
  <pw>` remain visible in `/proc/<pid>/cmdline` until the process exits.
  Prefer the file or env-var form.

## Operating recommendations

- Run with `--requirepass-file` or `SOLIKV_REQUIREPASS` and a 0600 file.
- Bind to loopback (`--bind 127.0.0.1`) unless TLS is configured.
- For multi-host deployments: front the RESP port with a TLS terminator
  (or use `--tls-cert/--tls-key`) and require `requirepass`.
- Keep the data directory on a filesystem writable only by the SoliKV UID
  (AOF replay trusts the file).
- Do not use the cluster bus or replication modules until SEC-016 is closed.
