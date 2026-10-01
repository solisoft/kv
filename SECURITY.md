# Security

This document describes SoliKV's security model and the hardening shipped in
the SEC-001…SEC-016 series and the 2026-09 audit follow-up (phases 1–7).

## Reporting vulnerabilities

Please report security issues privately to the maintainers rather than via
public GitHub issues. Include reproduction steps and affected versions.

## Threat model

SoliKV is a Redis-protocol-compatible server. Two trust boundaries matter:

1. **Network → server**: any party who can open a TCP connection to the
   RESP port (6379), the REST port (5020) or, in cluster mode, the cluster
   bus (RESP port + 10000 unless `--cluster-port`), and any party on the
   path of the links the server opens itself: to its master (replication),
   to `MIGRATE` targets, and to other cluster nodes.
2. **Local user → daemon**: any local UNIX user who shares the host (e.g.
   multi-tenant deployments, shared CI runners).

Out of scope: an attacker with root on the same host, or with write access
to the data directory after server startup. A replica trusts its master
completely (it applies the master's stream as the master's own writes, as
Redis does), and cluster nodes trust each other: compromising one member
compromises the cluster.

## Channels at a glance

| Channel | Authentication | Encryption |
|---|---|---|
| RESP (client → server) | `requirepass` / ACL users (`AUTH`, `HELLO … AUTH`); optional client certificates | TLS with `--tls-cert`/`--tls-key` |
| REST (client → server) | Bearer token (`--rest-token`, else `requirepass`) acting as `--rest-token-user`, or `Basic` credentials of any ACL user; optional client certificates | TLS with the same certificate |
| Replication (replica → master) | `--masterauth` (+ `--masteruser` for an ACL user) | TLS with `--tls-replication yes` |
| `MIGRATE` (server → target) | `AUTH` / `AUTH2` arguments | TLS with `--tls-replication yes` |
| Cluster bus (node ↔ node) | HMAC-SHA-256 envelope keyed by `--cluster-bus-secret`, receiver-bound, replay-protected | TLS with `--tls-cluster yes` (mutual, CA-checked) |
| `--cluster-dump` / `--cluster-restore` (tool → nodes) | `--cluster-password` (+ `--cluster-user`) | none (plain TCP) |

## Hardening summary

### Network exposure

- **Protected mode (SEC-001)**: starting with `--bind` set to a non-loopback
  address while the `default` user needs no password is refused. The check
  runs after `--aclfile` is loaded, so a password given to `default` there
  counts. Operators must set a password or pass `--protected-mode no`.
- **TLS (SEC-002)**: `--tls-cert PATH --tls-key PATH` (PKCS#8, PKCS#1 or SEC1
  keys) enables TLS on the RESP and REST ports; handshakes are bounded by
  `--tls-handshake-timeout` (default 10 s). The same certificate is this
  node's client certificate on the links it opens.
  - **Client certificates (mTLS)**: `--tls-ca-cert PATH` (also accepted as
    `--tls-client-ca`, and `tls-ca-cert-file` in a config file) checks client
    certificates on RESP and REST per `--tls-auth-clients yes|optional|no`
    — `yes` by default once a CA is set, as in Redis. A certificate proves
    membership of the CA, not an ACL identity: clients still `AUTH`.
  - **Replication and `MIGRATE`**: `--tls-replication yes` makes a replica
    dial its master over TLS and check the master's certificate against the
    CA; `MIGRATE` uses the same setting, as in Redis.
  - **Cluster bus**: `--tls-cluster yes` runs every bus connection over TLS
    both ways; the listening node requires a certificate signed by the CA
    from the dialling node, and the dialling node checks the listener's.
    The HMAC envelope stays underneath.
  - **Peer names are not checked**: peers are verified against the CA, not
    against the address dialled (Redis does the same — nodes are reached by
    IP and change roles in failovers). Anything holding a certificate from
    the CA counts as a member: use a private CA for the cluster.
  - Misconfiguration refuses to start: TLS options without a certificate,
    `--tls-auth-clients` without a CA, or outbound TLS without a CA.
- **REST credentials**: the Bearer token defaults to the RESP password;
  `--rest-token-file` / `SOLIKV_REST_TOKEN` / `--rest-token` set a separate
  one, so a leaked REST token is not also the RESP password. The token acts
  as the ACL user `--rest-token-user` (default `default`); other users
  authenticate with `Authorization: Basic`. **Every REST route is checked
  against the request's ACL user** exactly as the command it runs would be
  over RESP (a denial is a `403` with Redis' `NOPERM` message and is
  recorded in `ACL LOG`). A server with no token and a password-less
  `default` user is open over REST as over RESP. `--rest-port 0` disables
  the REST API; `--rest-max-connections` (default 1024) answers `503` past
  the cap, counting TLS handshakes in flight.
- **DNS rebinding**: on a loopback bind the REST API only answers requests
  whose `Host` is `localhost`, `127.0.0.1` or `::1` (plus
  `--rest-allowed-hosts`), so a web page that rebinds its own name to
  127.0.0.1 cannot read or write keys. On other binds the check runs when
  `--rest-allowed-hosts` is set.
- **AUTH rate limiting (SEC-004)**: both the RESP server and the REST
  middleware track failed AUTH attempts per peer IP and block the IP for
  30 s after 10 failures, logging every failed attempt at `warn`. A REST
  request with *no* credentials is refused but not counted — behind a
  reverse proxy, counting it let anonymous probes lock everyone out. Both
  failure maps are pruned of expired entries.
- **Pre-auth limits**: before AUTH a connection may not nest arrays at all,
  may buffer at most 128 KiB, and must authenticate within `--auth-timeout`
  seconds of connecting (default 10; the deadline covers the whole
  connection, so a byte-at-a-time trickle does not extend it). Inline
  commands and RESP header lines are capped at 64 KiB before their CRLF.
  After AUTH, nesting is capped at 32 levels and the query buffer at
  `--client-query-buffer-limit` (default 1 GiB, as in Redis). `--timeout`
  closes idle authenticated clients (default 0 = never); `--maxclients`
  (default 10 000) refuses connections past it with Redis' error.

### Access control

- **ACL**: users with SHA-256-hashed passwords, command rules by name,
  category and `command|subcommand`, key patterns (`~`, `%R~`, `%W~`) and
  channel patterns (`&`), as in Redis 7, including **selectors**
  (`(~key* +cmd …)`): a command is allowed when the user's own permissions
  or any one selector allow all of it. `--aclfile` loads users at startup;
  `ACL SAVE` writes the file `0600`. `requirepass` is the `default` user's
  password. Checks cost one flag test for unrestricted users.
- **Secrets stay out of the logs**: `MONITOR` does not show administrative
  commands (`ACL`, `CONFIG`, `DEBUG`, …) or `AUTH`, and `MONITOR` and
  `SLOWLOG` show `(redacted)` for the passwords of `HELLO … AUTH`,
  `MIGRATE … AUTH|AUTH2`, `ACL SETUSER` password rules and `CONFIG SET
  requirepass|masterauth`, as Redis does.

### Command surface

- **REST denylist (SEC-005)**: `POST /cmd` rejects destructive or DoS-prone
  commands (`KEYS`, `SCAN`/`HSCAN`/`ZSCAN`/`SSCAN`, `RENAME`, `RENAMENX`,
  `RESET`, `LATENCY`, `MEMORY`, `WAIT`, `FAILOVER`, `CLIENT`, `INFO`,
  `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `SHUTDOWN`, `EVAL`, `CONFIG`, …) whatever
  the user's ACL allows; `/info` and `/dbsize` have their own read-only
  routes behind the same authentication.
- **`DEBUG`** is refused unless `--enable-debug-command local|yes`.
- **`CONFIG SET`** accepts only the parameters it can change safely at
  runtime; `dir`, `dbfilename` and the like are not settable, so Redis'
  "write a file anywhere" trick does not apply.

### Replication

- A replica authenticates with `--masterauth` / `--masteruser`; a replica of
  a master that requires a password but has no `--masterauth` says so
  rather than failing later. Without `--tls-replication`, the password and
  the whole dataset cross the network in cleartext.
- `min-replicas-to-write` / `min-replicas-max-lag` make a master refuse
  writes (`NOREPLICAS`) unless enough replicas acknowledged recently.
- `CLIENT KILL TYPE replica|master` closes replication links (they
  reconnect); `CLIENT PAUSE … WRITE|ALL` never holds replication links.

### Cluster bus (SEC-016)

- Every frame carries an HMAC-SHA-256 over length-prefixed fields
  (timestamp, nonce, receiver, payload). The key is
  `--cluster-bus-secret-file` / `SOLIKV_CLUSTER_BUS_SECRET` /
  `--cluster-bus-secret` (at least 32 bytes); cluster mode refuses to start
  without one. It is separate from `--cluster-password`, which only
  authenticates `--cluster-dump` / `--cluster-restore` to nodes; using
  `--cluster-password` as the bus key still works, with a deprecation
  warning, so one credential handed to an operator tool is not the key to
  the topology.
- Frames name their receiving node, so a capture cannot be replayed at other
  nodes; only `PING`/`MEET` — the handshake, sent before the peer's id is
  known — may be unaddressed, and they carry no slot ownership. One nonce
  memory spans all bus connections and keeps each nonce until its timestamp
  leaves the ±5 min window.
- Frames are read with a hard 1 MiB line limit, at most 256 bus connections
  are served at once, idle connections are dropped after 30 s, and rejected
  frames are logged at most once a second. Slot numbers ≥ 16384 are refused
  everywhere.
- Without `--tls-cluster`, a non-loopback bus logs a warning at startup: it
  is authenticated but cleartext.

### Persistence

- **RDB length sanitization (SEC-006)**: `read_bytes` rejects any chunk longer
  than 512 MB (`MAX_VALUE_LEN`) and grows with the bytes actually present;
  collections preallocate at most 4096 slots whatever count the file
  declares. Snapshots end with a CRC-32 that is checked on load. The Redis
  `dump.rdb` importer (`--import-redis-rdb`) reads strings with `take` +
  `read_to_end` instead of `vec![0; len]`, refuses LZF strings claiming more
  than the compressed bytes could expand to, and caps collection
  pre-allocation at 4096 slots.
- **AOF replay (SEC-006)**: nothing is allocated from a length in the file
  before the bytes are there. A file that ends part-way through a command
  (a crash mid-write) loads everything before it with a `warn`, like Redis'
  `aof-load-truncated yes`. Anything else that does not parse is corruption,
  and the server **refuses to start** rather than serve a partial dataset
  and append after the damage. A trailing `MULTI` block without its `EXEC`
  is not applied.
- **File permissions**: the data directory is created `0700` and every
  file the server writes (snapshot, AOF, `cluster.json`, cluster dumps,
  `functions.lib`, the ACL file, pid file, daemon log) `0600`, opened with
  `O_NOFOLLOW` where it is created.
- **AOF replay denylist (SEC-007)**: `EVAL`, `EVALSHA`, `SCRIPT`, `DEBUG`,
  `SHUTDOWN`, `BGREWRITEAOF`, `BGSAVE`, `SAVE`, `SLAVEOF`/`REPLICAOF`,
  `CLUSTER`, `MIGRATE`, `RESTORE`, `MODULE` are skipped (with a `warn`) on
  replay. Scripts are never replayed: their effects are logged as they run.
  The data directory must still be on a filesystem only the SoliKV UID can
  write to — replay is not authenticated.

### Operational

- **Daemon log**: `--daemon` logs to `<data-dir>/solikv.log` (or
  `--logfile`), not a fixed `/tmp/solikv.log` another user could pre-create
  as a symlink.
- **Container**: the Docker image runs as UID 10001, not root; a bind-mounted
  data directory must be writable by that UID.
- **PID file (SEC-003)**: `<data-dir>/solikv.pid`. The "kill old daemon"
  path resolves `/proc/<pid>/exe`, canonicalizes both that and
  `current_exe()`, and only signals the PID if the canonical paths match.
- **Cluster dump password (SEC-008)**: `--cluster-dump` / `--cluster-restore`
  only send `--cluster-password` to the seed node by default; pass
  `--cluster-password-seed-only=false` to AUTH against discovered nodes.
  `--cluster-user` authenticates as an ACL user.
- **Secret sources (SEC-009)**: `--requirepass-file`, `--cluster-password-file`,
  `--cluster-bus-secret-file` and `--rest-token-file` (preferred), or the
  `SOLIKV_REQUIREPASS` / `SOLIKV_CLUSTER_PASSWORD` /
  `SOLIKV_CLUSTER_BUS_SECRET` / `SOLIKV_REST_TOKEN` environment variables.
  Secret files are opened with `O_NOFOLLOW` and **rejected** if their mode
  is looser than `0600`. The CLI forms remain for compatibility but log a
  `warn` because `/proc/<pid>/cmdline` exposes them to other local users.
- **Installer (SEC-011)**: `install.sh` verifies the tarball against the
  release's `SHA256SUMS` line for it, and aborts on any missing piece.
  The checksum comes from the same origin as the tarball: it proves
  integrity, not authenticity.

### Implementation hardening

- **EVAL/EVALSHA arithmetic (SEC-012)**: `numkeys + 2` uses `checked_add`;
  same for `ZUNIONSTORE`/`ZINTERSTORE`.
- **RESP decoder (SEC-013)**: no allocation from an untrusted `*N` header;
  an unauthenticated peer cannot declare arrays larger than 32 elements or
  bulk strings larger than 64 KiB; nesting is bounded (see above).
- **FFI memory safety (SEC-010)**: no `static mut`; every `solikv_*` extern
  validates pointers and lengths.
- **Lua (SEC-014)**: scripts run on Lua 5.1 with Redis 7's isolation:
  globals and libraries (`string`, `table`, `math`, `redis`, `cjson`, …) are
  read-only, and nothing a script leaves behind survives it. Each VM is
  capped at `--lua-memory-limit` bytes (default 64 MB), scripts are loaded
  in text mode only, and `SCRIPT KILL` / `FUNCTION KILL` can stop a
  read-only script past `--busy-reply-threshold`.
- **Build artifact hygiene (SEC-015)**: `.gitignore` excludes rustc
  artifacts.
- **Per-command resource bounds**: `SETRANGE` offsets are capped at 512 MB;
  `SETBIT`/`GETBIT` offsets must be below 2^32; `BF.RESERVE` refuses filters
  above 512 MB; pop/scan `COUNT`s no longer size an allocation; TTL
  arithmetic is checked.
- **Solo mode thread safety**: `--solo` keeps its store unlocked on the
  promise that one thread touches it; nothing hands its worker to a second
  thread (`block_in_place` runs inline in solo mode), and debug builds
  assert the store is never entered twice.

## Known limitations

- **No Sentinel**: failover exists in cluster mode only; a standalone
  master/replica pair has no automatic failover.
- **Bus membership is shared**: the bus secret (and, with `--tls-cluster`,
  the CA) proves membership, not identity — any member can sign as any
  node. A compromised node can rewrite the topology.
- **Client certificates do not map to ACL users** (Redis 8's
  `tls-auth-clients-user` is not implemented): a certificate gets a client
  to the `AUTH` prompt, not past it.
- **`--cluster-dump` / `--cluster-restore` speak plain TCP**: they cannot
  reach nodes that only accept TLS.
- **`CLIENT NO-TOUCH`** is recorded but does not yet keep a client's reads
  from refreshing LRU/LFU.
- **Argv password leakage**: secrets given as CLI flags remain visible in
  `/proc/<pid>/cmdline` until the process exits. Prefer the file or
  environment form.

## Operating recommendations

- Run with `--requirepass-file` (or ACL users in `--aclfile`) and 0600 files.
- Bind to loopback (`--bind 127.0.0.1`) unless TLS is configured.
- For multi-host deployments: `--tls-cert/--tls-key` plus `--tls-ca-cert`
  (client certificates on RESP and REST), `--tls-replication yes` for
  replicas, and give REST its own token — or disable it with
  `--rest-port 0` if unused.
- In cluster mode, set `--cluster-bus-secret-file` (separate from any client
  password) and `--tls-cluster yes` unless the bus runs on a private
  network; use a CA dedicated to the cluster.
- Keep the data directory on a filesystem writable only by the SoliKV UID
  (AOF replay trusts the file).
