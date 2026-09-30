# Security

This document describes SoliKV's security model and the hardening shipped in
the SEC-001…SEC-016 series and the 2026-09 audit follow-up.

## Reporting vulnerabilities

Please report security issues privately to the maintainers rather than via
public GitHub issues. Include reproduction steps and affected versions.

## Threat model

SoliKV is a Redis-protocol-compatible server. Two trust boundaries matter:

1. **Network → server**: any party who can open a TCP connection to the
   RESP port (6379), the REST port (5020) or, in cluster mode, the cluster
   bus (RESP port + 10000).
2. **Local user → daemon**: any local UNIX user who shares the host (e.g.
   multi-tenant deployments, shared CI runners).

Out of scope: an attacker with root on the same host, or with write access
to the data directory after server startup.

## Hardening summary

### Network exposure

- **Protected mode (SEC-001)**: starting with `--bind` set to a non-loopback
  address while `--requirepass` is unset is refused by default. Operators must
  either set a password or pass `--protected-mode no` to opt out.
- **TLS (SEC-002)**: `--tls-cert PATH --tls-key PATH` enables TLS on both the
  RESP and the REST port. Handshakes are bounded by `--tls-handshake-timeout`
  (default 10 s). `--tls-client-ca` (mTLS) is not implemented, and the server
  refuses to start with it rather than silently skip client verification.
- **REST credentials**: the REST Bearer token defaults to the RESP password;
  `--rest-token-file` / `SOLIKV_REST_TOKEN` / `--rest-token` set a separate
  one, so a leaked REST token is not also the RESP password. `--rest-port 0`
  disables the REST API.
- **DNS rebinding**: on a loopback bind the REST API only answers requests
  whose `Host` is `localhost`, `127.0.0.1` or `::1` (plus
  `--rest-allowed-hosts`), so a web page that rebinds its own name to
  127.0.0.1 cannot read or write keys. On other binds the check runs when
  `--rest-allowed-hosts` is set.
- **AUTH rate limiting (SEC-004)**: both the RESP server and the REST
  middleware track failed AUTH attempts per peer IP and block the IP for
  30 s after 10 failures, logging every failed attempt at `warn`. The REST
  side uses `into_make_service_with_connect_info::<SocketAddr>()` so the
  middleware sees the real peer address. A REST request with *no*
  credentials is refused but not counted — behind a reverse proxy, counting
  it let anonymous probes lock everyone out. Both failure maps are pruned of
  expired entries, so they do not grow with every address that ever failed.
- **Pre-auth limits**: before AUTH a connection may not nest arrays at all,
  may buffer at most 128 KiB, and must authenticate within `--auth-timeout`
  seconds of connecting (default 10; the deadline covers the whole
  connection, so a byte-at-a-time trickle does not extend it). Inline
  commands and RESP header lines are capped at 64 KiB before their CRLF.
  After AUTH, nesting is capped at 32 levels (each level is a stack frame —
  `*1\r\n` repeated used to overflow the stack and abort the process) and
  the query buffer at `--client-query-buffer-limit` (default 1 GiB, as in
  Redis). `--timeout` closes idle authenticated clients (default 0 = never).

### Command surface

- **REST denylist (SEC-005)**: `POST /cmd` rejects destructive or DoS-prone
  commands (`KEYS`, `SCAN`/`HSCAN`/`ZSCAN`/`SSCAN`, `RENAME`, `RENAMENX`,
  `RESET`, `LATENCY`, `MEMORY`, `WAIT`, `FAILOVER`, `CLIENT`, `INFO`,
  `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `SHUTDOWN`, `EVAL`, `CONFIG`, …).

### Persistence

- **RDB length sanitization (SEC-006)**: `read_bytes` rejects any chunk longer
  than 512 MB (`MAX_VALUE_LEN`); collection counts are clamped to 16 M
  (`MAX_COLLECTION_LEN`) before `with_capacity`. The Redis `dump.rdb`
  importer (`--import-redis-rdb`) reads strings with `take` + `read_to_end`
  instead of `vec![0; len]`, refuses LZF strings claiming more than the
  compressed bytes could expand to, and caps collection pre-allocation at
  4096 slots — a crafted file used to abort startup.
- **AOF replay (SEC-006)**: nothing is allocated from a length in the file
  before the bytes are there. A file that ends part-way through a command
  (a crash mid-write) loads everything before it with a `warn`, like Redis'
  `aof-load-truncated yes`. Anything else that does not parse — a bad header,
  a bulk above 512 MB, a missing CRLF — is corruption, and the server
  **refuses to start** rather than serve a partial dataset and append after
  the damage.
- **File permissions**: the data directory is created `0700` and every
  persistence file (RDB, AOF, `cluster.json`, cluster dumps, pid file,
  daemon log) `0600`, opened with `O_NOFOLLOW`. They used to follow the
  umask (typically world-readable), handing the dataset to any local user.
- **AOF replay denylist (SEC-007)**: `EVAL`, `EVALSHA`, `SCRIPT`, `DEBUG`,
  `SHUTDOWN`, `BGREWRITEAOF`, `BGSAVE`, `SLAVEOF`/`REPLICAOF`, `CLUSTER`,
  `MIGRATE`, `RESTORE`, `MODULE` are skipped (with a `warn`) on replay.
  The data directory must still be on a filesystem only the SoliKV UID can
  write to — replay is not authenticated.

### Operational

- **Daemon log**: `--daemon` logs to `<data-dir>/solikv.log` (or
  `--logfile`), not a fixed `/tmp/solikv.log` another user could pre-create
  as a symlink.
- **Container**: the Docker image runs as UID 10001, not root; a bind-mounted
  data directory must be writable by that UID.
- **PID file (SEC-003)**: moved from `/tmp/solikv.pid` to
  `<data-dir>/solikv.pid`. The "kill old daemon" path resolves
  `/proc/<pid>/exe` with `fs::read_link`, canonicalizes both that and
  `current_exe()`, and only signals the PID if the canonical paths match.
  Signalling uses `nix::sys::signal::kill` directly, not a shell-out.
- **Cluster dump password (SEC-008)**: `solikv --cluster-dump` /
  `--cluster-restore` only sends `--cluster-password` to the seed node by
  default. To AUTH against discovered nodes, pass
  `--cluster-password-seed-only=false` (this form was previously rejected by
  the argument parser).
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
- **Per-command resource bounds**: `SETRANGE` offsets are checked and capped
  at 512 MB (an offset near 2^64 used to wrap and zero-pad forever under the
  shard lock); `SETBIT`/`GETBIT` offsets must be below 2^32; `BF.RESERVE`
  validates its error rate and capacity and refuses filters above 512 MB;
  pop/scan `COUNT`s no longer size an allocation. TTL arithmetic is checked:
  an `EX`/`EXPIRE` whose milliseconds overflow returns `invalid expire time`
  instead of wrapping to a tiny TTL.
- **Lua limits**: each scripting VM is capped at `--lua-memory-limit` bytes
  (default 64 MB) — the instruction hook cannot interrupt a C function such
  as `string.rep` — and scripts are loaded in text mode only, never as
  precompiled bytecode.
- **Solo mode thread safety**: `--solo` keeps its store unlocked on the
  promise that one thread touches it. `KEYS`, `FLUSH*`, `SAVE` and AOF
  backpressure used `block_in_place`, which hands the worker to a second
  thread — a data race. They now run inline in solo mode, the AOF writer is
  its own OS thread, the server (including the shutdown save) runs on the
  runtime worker, and debug builds assert the store is never entered twice.
- **Cluster bus (SEC-016)**: wired and authenticated. Every frame carries an
  HMAC-SHA-256 over length-prefixed fields (timestamp, nonce, receiver,
  payload) with a shared secret of at least 32 bytes; cluster mode refuses to
  start without one. Frames name their receiving node, so a capture cannot be
  replayed at other nodes; only `PING`/`MEET` — the handshake, sent before
  the peer's id is known — may be unaddressed, and they carry no slot
  ownership. One nonce memory spans all bus connections and keeps each nonce
  until its timestamp leaves the ±5 min window (the previous per-connection
  memory never saw a repeat, because senders open a connection per frame).
  Frames are read with a hard 8 KiB line limit, at most 256 bus connections
  are served at once, reads time out after 10 s, and rejected frames are
  logged at most once a second. Slot numbers ≥ 16384 are refused everywhere
  instead of indexing past the slot table.

## Known limitations and follow-ups

- **The cluster bus is not encrypted**: frames are authenticated and
  replay-protected, but topology, node ids and addresses travel in clear.
  Run the bus on a private network or a VPN. A shared secret proves
  membership, not identity: any member can sign as any node.
- **Replication is not implemented**; the `solikv-replication` crate is not
  wired into the server.
- **mTLS** (`--tls-client-ca`) is not implemented.
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
- For multi-host deployments: use `--tls-cert/--tls-key` (covers RESP and
  REST) and require `requirepass`; give REST its own token, or disable it
  with `--rest-port 0` if unused.
- Keep the data directory on a filesystem writable only by the SoliKV UID
  (AOF replay trusts the file).
- In cluster mode, keep the bus port (RESP port + 10000) on a private
  network: it is authenticated, not encrypted.
