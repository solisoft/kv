# SoliKV

An in-memory key-value server written in Rust that speaks the Redis protocol.
It runs as many shards as the machine has cores, persists with RDB snapshots
and an append-only log, replicates, forms a Redis Cluster, and also exposes a
JSON REST API.

## Compatibility at a glance

SoliKV implements the Redis 7.x command surface that clients and frameworks
rely on; it is not a byte-for-byte Redis clone. Known differences are listed
under [Differences from Redis](#differences-from-redis).

- **Protocol**: RESP2 and RESP3 (`HELLO 3`), inline commands, pipelining,
  `AUTH user password`, `CLIENT` (`ID`, `LIST`, `KILL`, `PAUSE`, `TRACKING`
  for client-side caching, `NO-EVICT`, `NO-TOUCH` …).
- **Commands**: 253 in the command table (`COMMAND COUNT`): strings and
  bitmaps (`BITFIELD`, `BITPOS`, `LCS`), lists including the blocking ones
  (`BLPOP`, `BLMOVE`, `BLMPOP` …), hashes including field expiry (`HEXPIRE`
  family, Redis 8's `HGETDEL`/`HGETEX`/`HSETEX`), sets, sorted sets
  (unified `ZRANGE`, `BZPOPMIN`, `ZMPOP` …), streams with consumer groups
  (`XREADGROUP BLOCK`, `XAUTOCLAIM`, `XCLAIM` …), geo, HyperLogLog, Bloom
  filters (`BF.*`), pub/sub (pattern and sharded), transactions with `WATCH`,
  `DUMP`/`RESTORE` in Redis' payload format, `SORT`, `COPY`, `MIGRATE`.
- **Scripting**: Lua 5.1 like Redis, with `cjson`, `cmsgpack`, `bit` and
  `struct`; `EVAL`/`EVALSHA` (+ `_RO`), `SCRIPT KILL`, `#!lua flags=`,
  `FUNCTION`/`FCALL` (+ `_RO`). Globals and libraries are read-only, as in
  Redis 7.
- **Logical databases**: 16 by default (`--databases`), `SELECT`, `MOVE`,
  `SWAPDB`.
- **Memory**: `maxmemory` with the eight Redis eviction policies, active
  expiry (`hz`), keyspace notifications.
- **Security**: ACL users with command/category/key/channel rules and
  selectors, TLS on every channel (RESP, REST, replication, cluster bus),
  client certificates (optionally naming the ACL user, as Redis 8's
  `tls-auth-clients-user`), protected mode. See [SECURITY.md](SECURITY.md).
- **Persistence**: Redis 7-style append-only log directory with a manifest,
  checksummed RDB snapshots, save points, `BGREWRITEAOF`, `SHUTDOWN`, import
  of Redis `dump.rdb` files.
- **Replication**: `REPLICAOF`, `PSYNC` with full and partial resync, `WAIT`,
  `ROLE`, read-only replicas, `min-replicas-to-write`.
- **Cluster**: the Redis Cluster protocol — 16384 slots, `MOVED`/`ASK`,
  `CROSSSLOT`, `CLUSTER SLOTS`/`SHARDS`/`NODES` in Redis 7's format,
  resharding with `redis-cli --cluster reshard`, automatic and manual
  failover, replica migration.
- **Operations**: redis.conf-style config files, `CONFIG GET/SET/REWRITE`,
  `INFO` (all sections, `commandstats`, `errorstats`), `SLOWLOG`, `LATENCY`,
  `MONITOR`, `MEMORY`, Prometheus `/metrics`, `/health` and `/ready`.

## Getting started

### Install

```bash
# Install to ~/.local/bin
curl -sSL https://raw.githubusercontent.com/solisoft/kv/main/install.sh | sh

# Or system-wide (requires sudo)
curl -sSL https://raw.githubusercontent.com/solisoft/kv/main/install.sh | sh -s -- --system
```

### Build from source

```bash
cargo build --release          # binary: target/release/solikv
```

### Run

The default bind address is `0.0.0.0`, and **protected mode refuses to start
on a non-loopback address without a password**. Either bind to loopback or
set a password:

```bash
# Local development
solikv --bind 127.0.0.1

# Reachable from the network: set a password (file mode 0600 is enforced)
solikv --requirepass-file /etc/solikv/password --dir /var/lib/solikv

# Or with a config file in redis.conf syntax (also accepted as first argument)
solikv /etc/solikv/solikv.conf
```

```bash
redis-cli -p 6379 PING
```

### Docker

The image runs as the unprivileged user UID 10001; a bind-mounted data
directory must be writable by that UID. Inside a container the server binds
to `0.0.0.0`, so give it a password:

```bash
mkdir -p data && sudo chown 10001 data
docker run -p 6379:6379 -p 5020:5020 -v ./data:/data \
  ghcr.io/solisoft/kv:latest --dir /data --requirepass "$SOLIKV_PASSWORD"

# Or build locally
docker build -t solikv .
```

## Usage

### Redis clients

Any Redis client works: `redis-cli`, redis-py, ioredis, go-redis, Jedis,
Lettuce, and their cluster modes.

```bash
redis-cli -p 6379
SET greeting "hello" EX 60
GET greeting
```

### REST API

The REST API listens on `--rest-port` (5020; `0` disables it). Replies are
the JSON form of the command's reply: a bare string, number or array, `null`
with HTTP 404 for a missing value, `{"result":"OK"}` for OK and
`{"error": "…"}` with HTTP 400 (403 for `NOPERM` or a refused command).

| Method | Route | Command |
|---|---|---|
| `GET` / `PUT` / `DELETE` | `/kv/{key}` | `GET` / `SET` (`{"value": "…", "ex": 60}` or `"px"`) / `DEL` |
| `GET` | `/list/{key}?start=0&stop=-1` | `LRANGE` |
| `POST` | `/list/{key}/lpush`, `/rpush` | `LPUSH` / `RPUSH` (`{"values": [...]}`) |
| `POST` | `/list/{key}/lpop`, `/rpop` | `LPOP` / `RPOP` |
| `GET` / `PUT` | `/hash/{key}` | `HGETALL` / `HSET` (`{"fields": {...}}`) |
| `GET` / `DELETE` | `/hash/{key}/{field}` | `HGET` / `HDEL` |
| `GET` / `PUT` | `/set/{key}` | `SMEMBERS` / `SADD` (`{"members": [...]}`) |
| `DELETE` | `/set/{key}/{member}` | `SREM` |
| `GET` / `PUT` | `/zset/{key}` | `ZRANGE … WITHSCORES` / `ZADD` (`{"members": [{"score": 1, "member": "a"}]}`) |
| `POST` / `GET` / `POST` | `/pfadd/{key}`, `/pfcount/{key}`, `/pfmerge/{dest}` | HyperLogLog |
| `POST` / `GET` | `/bitmap/{key}/setbit`, `/bitmap/{key}/getbit/{offset}`, `/bitmap/{key}/bitcount` | Bitmaps |
| `POST` / `GET` | `/bf/reserve/{key}`, `/bf/add/{key}`, `/bf/exists/{key}/{item}`, `/bf/info/{key}` | Bloom filters |
| `POST` / `GET` | `/geo/{key}/add`, `/geo/{key}/pos/{member}`, `/geo/{key}/dist/{m1}/{m2}`, `/geo/{key}/search` | Geo |
| `POST` | `/cmd` | Any command: `{"command": "HSET", "args": ["user:1", "name", "Alice"]}` |
| `GET` | `/info`, `/dbsize` | `INFO`, `DBSIZE` |
| `GET` | `/health`, `/ready` | Liveness; readiness (503 while a script runs past `--busy-reply-threshold`). Never authenticated |
| `GET` | `/metrics` | Prometheus text format (token required when one is set, see `--metrics-auth`) |

```bash
curl -X PUT localhost:5020/kv/session:abc -H 'Content-Type: application/json' \
  -d '{"value":"token123","ex":3600}'          # {"result":"OK"}
curl localhost:5020/kv/session:abc              # "token123"
```

Authentication: `Authorization: Bearer <token>` (`--rest-token`, else the
`requirepass` password) acts as `--rest-token-user`; `Authorization: Basic`
authenticates as any ACL user, and every route is checked against that
user's ACL. `/cmd` refuses administrative and keyspace-wide commands
(`FLUSHALL`, `CONFIG`, `KEYS`, `SCAN`, `EVAL`, `CLUSTER`, …). On a loopback
bind the API only answers `Host: localhost`/`127.0.0.1`/`::1` (plus
`--rest-allowed-hosts`), which stops DNS-rebinding attacks from a browser.

## Configuration

Every option is a command-line flag. A config file in redis.conf syntax
(`name value…` per line, `#` comments, quotes, `include other.conf`) can be
given with `--config PATH` or as the first argument; each directive becomes
the flag of the same name, and flags on the command line override the file.
Redis directives with no meaning here are accepted with a warning so a stock
`redis.conf` loads; unknown directives are an error. `CONFIG SET` changes the
runtime-mutable ones — `maxmemory`, `maxmemory-policy`, `maxmemory-samples`,
`timeout`, `maxclients`, `slowlog-*`, `latency-monitor-threshold`,
`busy-reply-threshold`/`lua-time-limit`, `tracking-table-max-keys`, `hz`,
`requirepass`, `save`, `appendonly`, `appendfsync`, `auto-aof-rewrite-*`,
`min-replicas-*`, `client-output-buffer-limit`, `notify-keyspace-events` —
and `CONFIG REWRITE` writes them back to the file.

#### Network and limits

| Flag | Default | Description |
|---|---|---|
| `--port` | `6379` | Redis protocol port |
| `--rest-port` | `5020` | REST API port (0 = REST API disabled) |
| `--bind` | `0.0.0.0` | Bind address |
| `--protected-mode` | `yes` | Enable protected mode (reject connections if binding to non-loopback with no password) |
| `--maxclients` | `10000` | Most clients connected at once (Redis `maxclients`) |
| `--timeout` | `0` | Close an authenticated client idle for this many seconds (0 = never, like Redis' `timeout 0`) |
| `--auth-timeout` | `10` | When a password is set, a client must authenticate within this many seconds of connecting (0 = no limit) |
| `--client-query-buffer-limit` | `1073741824` | Most bytes buffered for one client's pending queries (Redis `client-query-buffer-limit`) |
| `--client-output-buffer-limit` | — | Output buffer limits per client class, as Redis takes them: "<normal\|replica\|pubsub> <hard> <soft> <soft-seconds> ...". A client over the hard limit, or over the soft one for that long, is disconnected; 0 disables a limit. Defaults: normal 0 0 0, replica 256mb 64mb 60, pubsub 32mb 8mb 60 |
| `--tls-handshake-timeout` | `10` | Longest a TLS handshake may take, in seconds |

#### Authentication and ACL

| Flag | Default | Description |
|---|---|---|
| `--requirepass` | — | Require password for client authentication (RESP AUTH + REST Bearer token) |
| `--requirepass-file` | — | Read password from a file (mode 0600 enforced). Takes precedence over --requirepass. |
| `--aclfile` | — | ACL users file (`ACL LOAD` / `ACL SAVE`); loaded at startup if present |
| `--rest-token` | — | Bearer token for the REST API. Defaults to the RESP password. A separate token keeps a leaked REST credential from also being the RESP password. |
| `--rest-token-file` | — | Read the REST token from a file (mode 0600 enforced) |
| `--rest-token-user` | `default` | The ACL user a valid REST Bearer token acts as. Other users authenticate with `Authorization: Basic` (user:password). |
| `--rest-max-connections` | `1024` | Most concurrent REST connections (0 = no limit) |
| `--rest-allowed-hosts` | — | Extra Host header names the REST API accepts (comma-separated). On a loopback bind, localhost/127.0.0.1/::1 are always accepted and any other Host is refused, which stops DNS rebinding from a browser. On other binds the check only runs when this is set. |
| `--metrics-auth` | — | Require the REST token for /metrics (yes/no; default: yes when a token or password is set) |

#### TLS

| Flag | Default | Description |
|---|---|---|
| `--tls-cert` | — | TLS certificate file. With --tls-key, enables TLS on the RESP and REST ports; it is also this node's client certificate on the links it opens (--tls-replication, --tls-cluster). |
| `--tls-key` | — | TLS private key file (PKCS#8, PKCS#1 or SEC1 PEM) |
| `--tls-ca-cert` (alias `--tls-client-ca`) | — | CA certificate(s) (PEM). Client certificates on RESP/REST are checked against it (see --tls-auth-clients), and so are the peers of replication links and the cluster bus. |
| `--tls-auth-clients` | — | Client certificates on RESP and REST: yes (required), optional, or no. Default: yes when --tls-ca-cert is set, as in Redis. |
| `--tls-auth-clients-user` | `off` | Authenticate a RESP client as the ACL user named by its certificate's subject CN, when that user exists and is on (Redis 8 `tls-auth-clients-user`); otherwise the connection starts as any other. Needs client certificates (--tls-ca-cert). CN or off. |
| `--tls-replication` | `no` | Replication links (and MIGRATE) use TLS: a replica connects to its master's TLS port and checks its certificate against --tls-ca-cert. |
| `--tls-cluster` | `no` | The cluster bus uses TLS, both ways, with a certificate signed by --tls-ca-cert required of every peer. The bus keeps its HMAC envelope. |

#### Engine

| Flag | Default | Description |
|---|---|---|
| `--shards` | `0` | Number of shards (0 = number of CPU cores). Ignored when --solo is set. |
| `--solo` | off | Solo mode: Redis-shaped single-threaded engine (1 shard, no mutex). Best for single-core / classic redis-benchmark fights. Uses one Tokio worker. |
| `--databases` | `16` | Number of logical databases (SELECT 0 … n-1). Cluster mode uses database 0 only. |
| `--hz` | `10` | Active expiry cycles per second (Redis `hz`) |
| `--maxmemory` | `0` | Memory limit (bytes; k/kb/m/mb/g/gb suffixes allowed; 0 = none). Past it, writes evict keys by --maxmemory-policy or are refused |
| `--maxmemory-policy` | `noeviction` | noeviction, allkeys-lru, volatile-lru, allkeys-lfu, volatile-lfu, allkeys-random, volatile-random or volatile-ttl |
| `--maxmemory-samples` | `5` | Keys sampled per eviction (more = closer to true LRU/LFU, slower) |
| `--notify-keyspace-events` | `""` | Keyspace notification flags (e.g. "KEA" for all events, "" to disable) |
| `--lua-memory-limit` | `67108864` | Most memory one Lua scripting VM may use, in bytes |
| `--busy-reply-threshold` | `5000` | Milliseconds a script may run before other clients get -BUSY and SCRIPT KILL applies (Redis `busy-reply-threshold` / `lua-time-limit`) |
| `--tracking-table-max-keys` | `1000000` | Keys remembered for client-side caching (0 = unbounded) |
| `--enable-debug-command` | `no` | DEBUG command: no, local (loopback clients only) or yes |

#### Persistence

| Flag | Default | Description |
|---|---|---|
| `--dir` | `data` | Data directory for persistence files |
| `--dbfilename` | `dump` | Snapshot base filename: the snapshot is {dir}/{name}.rdb (per-shard {name}-{shard}.rdb files from earlier versions are still loaded) |
| `--appendonly` | `true` | Enable append-only file persistence. When on, startup loads the log (in {dir}/appendonlydir) and not the snapshot, as Redis does. |
| `--appendfsync` | `everysec` | AOF fsync policy: always, everysec, no |
| `--save` | — | Redis `save` points, overriding --save-interval: "3600 1 300 100" snapshots once 1 change happened within 3600 s, or 100 within 300 s; "" disables periodic snapshots (and the one at shutdown) |
| `--save-interval` | `300` | Seconds between background snapshots when the data changed (0 = no periodic snapshots and no snapshot at shutdown) |
| `--auto-aof-rewrite-percentage` | `100` | Rewrite the append-only log once it has grown by this percentage since the last rewrite (0 = only on BGREWRITEAOF) |
| `--auto-aof-rewrite-min-size` | `64mb` | ... and is at least this large (bytes; k/kb/m/mb/g/gb suffixes allowed) |
| `--import-redis-rdb` | — | Import a Redis RDB file (dump.rdb) at startup |

#### Replication

| Flag | Default | Description |
|---|---|---|
| `--replicaof` | — | Follow a master at startup: "host port" or "host:port" (Redis `replicaof`). `REPLICAOF` changes it at runtime. |
| `--masterauth` | — | Password to authenticate to the master with (`masterauth`). |
| `--masteruser` | — | ACL user to authenticate to the master as (`masteruser`). |
| `--repl-backlog-size` | `1mb` | Replication backlog: how much of the stream a reconnecting replica can resume from without a full resynchronization. |
| `--replica-read-only` | `yes` | Refuse writes from clients while this node is a replica. |
| `--replica-announce-ip` | — | Address a replica announces to its master (`replica-announce-ip`), when the one the master sees is not the one to reach it on. |
| `--min-replicas-to-write` (alias `--min-slaves-to-write`) | `0` | A master refuses writes (NOREPLICAS) unless at least this many replicas acknowledged within --min-replicas-max-lag (0 = off) |
| `--min-replicas-max-lag` (alias `--min-slaves-max-lag`) | `10` | Seconds since its last acknowledgement within which a replica counts for --min-replicas-to-write |

#### Cluster

| Flag | Default | Description |
|---|---|---|
| `--cluster-enabled` | off | Enable cluster mode (Redis Cluster compatible) |
| `--cluster-port` | `0` | Cluster bus port (0: the data port + 10000). |
| `--cluster-announce-ip` | — | Address this node announces to the cluster (and to clients, in MOVED and CLUSTER SLOTS), when the bind address is not it. |
| `--cluster-node-timeout` | `15000` | Milliseconds a node may be unreachable before it is suspected (`cluster-node-timeout`). |
| `--cluster-require-full-coverage` | `yes` | Refuse every key while some slot is unassigned or its owner failed (`cluster-require-full-coverage`). |
| `--cluster-allow-replica-migration` | `yes` | A replica may move to a master left without working replicas (`cluster-allow-replica-migration`). |
| `--cluster-migration-barrier` | `1` | Working replicas a master keeps before one of them may migrate (`cluster-migration-barrier`). |
| `--cluster-bus-secret` | — | The cluster bus key (HMAC-SHA256, at least 32 bytes): every frame between nodes is signed with it. Separate from any client password, so a credential handed to an operator tool is not the key to the topology. Prefer --cluster-bus-secret-file or SOLIKV_CLUSTER_BUS_SECRET. |
| `--cluster-bus-secret-file` | — | Read the cluster bus key from a file (mode 0600 enforced) |

#### Cluster dump / restore tool

| Flag | Default | Description |
|---|---|---|
| `--cluster-dump` | — | Cluster dump: export all cluster data to file |
| `--cluster-dump-format` | `jsonl` | Cluster dump format: binary or jsonl |
| `--cluster-restore` | — | Cluster restore: import data from dump file |
| `--cluster-connect` | — | Cluster connect: initial node for dump/restore (host:port) |
| `--cluster-password` | — | Password `--cluster-dump`/`--cluster-restore` authenticate to the nodes with (`AUTH`). Before --cluster-bus-secret existed it was also the bus key; it still is, with a warning, when no bus secret is set. |
| `--cluster-password-file` | — | Read the dump/restore password from a file (mode 0600 enforced) |
| `--cluster-user` | — | ACL user dump/restore authenticate as (`AUTH user password`) |
| `--cluster-password-seed-only` | `true` | For cluster dump/restore: send password only to seed node, not to discovered nodes. `--cluster-password-seed-only=false` sends it to every node. |

#### Logging, diagnostics, process

| Flag | Default | Description |
|---|---|---|
| `--config` | — | Configuration file (redis.conf syntax). Also accepted as the first argument (`solikv /etc/solikv.conf`). Flags on the command line override the file. |
| `--log-level` | `info` | Log level |
| `--log-format` | `text` | Log output: text or json |
| `--logfile` | — | Daemon mode log file (default: <dir>/solikv.log) |
| `--daemon` / `-d` | off | Run as daemon in background (kills old instance if running) |
| `--slowlog-log-slower-than` | `10000` | Commands slower than this many microseconds go to the slow log (negative disables, 0 logs every command) |
| `--slowlog-max-len` | `128` | Slow log entries kept |
| `--latency-monitor-threshold` | `0` | Latency monitor threshold in milliseconds (0 = off) |

## Persistence

```text
<dir>/
  dump.rdb                       snapshot: every shard and database, CRC-32 checked
  appendonlydir/
    appendonly.manifest          which files make up the log, in order
    appendonly.<gen>.base.rdb    snapshot the log starts from
    appendonly.<gen>.incr.<s>.aof  RESP records for shard <s>
```

- **Append-only log** (on by default, `appendfsync everysec`): one log per
  shard, each record appended while that shard's lock is held, so a log
  replays in exactly the order its shard executed. Records are the *effect*
  on the shard: a command spanning shards writes one record per shard,
  relative TTLs are logged as absolute deadlines, expirations and evictions
  as `DEL`, and transactions and scripts are bracketed `MULTI`/`EXEC` (an
  unfinished block at the end of the log is not applied). A writer thread
  hands records to the kernel about every millisecond, so a process killed
  with SIGKILL loses at most that last pass, and a machine crash up to about
  a second under `everysec`. Under `always` each write is fsynced right
  after it runs, but the reply is not held until the fsync completes (Redis
  holds it).
- **Rewrites** (`BGREWRITEAOF`, automatic once the log has grown by
  `--auto-aof-rewrite-percentage`) snapshot each shard and switch it to a new
  log generation under one lock hold, so a snapshot and its log meet exactly
  and nothing is replayed twice. The manifest makes this crash-safe.
- **Snapshots**: `dump.rdb`, written in the background (`BGSAVE`, save
  points, shutdown) to a temporary file, fsynced, renamed, and the
  directory fsynced. Snapshots are
  taken in steps, so other clients keep being served while a shard is
  written; collections of 8192 elements or more are written a slice at a
  time (streams whole). `FUNCTION` libraries are stored in snapshots and the log.
- **Startup** loads the log when `appendonly` is on (as Redis does), the
  snapshot otherwise. Every key is re-hashed to the shard that owns it now,
  so `--shards` can change between runs.
- **Shutdown**: SIGTERM, SIGINT and `SHUTDOWN [SAVE|NOSAVE]` stop writes,
  fsync the log and take a final snapshot when save points are configured.
- **Importing from Redis**: `--import-redis-rdb dump.rdb` reads Redis RDB
  files, Redis 7.4's (RDB version 12) included — strings, lists, sets, sorted sets and hashes in
  every encoding (ziplist, listpack, intset, quicklist, LZF), hashes with
  field expiry (Redis 7.4), keeping each key's database. Streams and module
  types are skipped with a warning.

## Replication

```bash
solikv --port 6380 --replicaof "10.0.0.1 6379" --masterauth "$MASTER_PASSWORD"   # or at runtime:
redis-cli -p 6380 REPLICAOF 10.0.0.1 6379
redis-cli -p 6379 WAIT 1 1000
```

A replica performs a full resynchronization from a snapshot, then follows
the master's stream; after a disconnect it resumes from the backlog
(`--repl-backlog-size`) with `PSYNC` when it can. Writes that span several
shards reach replicas as one `MULTI`/`EXEC` block. Replicas are read-only by
default and do not expire keys on their own clock (the master sends the
`DEL`). `--tls-replication yes` encrypts the link. Standalone pairs have no
automatic failover (no Sentinel); failover exists in cluster mode.

## Cluster

Cluster mode needs a bus secret of at least 32 bytes on every node
(`--cluster-bus-secret-file`); the bus listens on the data port + 10000.

```bash
for p in 7000 7001 7002 7003 7004 7005; do
  solikv --port $p --bind 127.0.0.1 --rest-port 0 --dir data/$p \
         --cluster-enabled --cluster-bus-secret-file bus.secret &
done
redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
          127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 --cluster-replicas 1
redis-cli --cluster reshard 127.0.0.1:7000      # move slots, keys follow (MIGRATE)
redis-cli -c -p 7000 SET user:42 alice          # -c follows MOVED/ASK
```

Keys are routed by CRC16 slot with `{hash tags}`; multi-key commands on keys
in different slots return `CROSSSLOT`. Failure detection is gossip-based
(PFAIL, then FAIL by a majority of slot-owning masters), after which a
replica is elected and takes over the slots. `CLUSTER FAILOVER [FORCE|TAKEOVER]`
does it by hand. Every bus frame is signed with the bus secret, bound to its
receiver and replay-protected; `--tls-cluster yes` encrypts it too.

`solikv --cluster-connect host:port --cluster-dump FILE` exports every key
of a running cluster (each key's `DUMP` payload and TTL), and
`--cluster-restore FILE` loads it into another one with `RESTORE … REPLACE`.
Given the nodes' `--tls-cert`/`--tls-key`/`--tls-ca-cert`, the tool connects
over TLS. Files written by 0.4.x still restore.

Within one node, keys are also spread over `--shards` shards (one per core
by default) by the same slot function, each behind its own lock; commands
spanning shards lock them all in a fixed order, so multi-key commands,
`MULTI`/`EXEC` and scripts stay atomic.

## Differences from Redis

- No Sentinel; standalone master/replica pairs fail over by hand
  (`REPLICAOF NO ONE`).
- No modules (`MODULE` is an unknown command); Bloom filters are built in
  (`BF.*`).
- `DUMP` payloads use Redis' format and restore in both directions for
  strings, lists, sets, hashes (including field expiry, RDB version 12) and
  sorted sets; streams, HyperLogLogs and Bloom filters use a SoliKV-specific
  payload that only SoliKV restores.
- `--import-redis-rdb` skips streams.
- On a multi-shard server, `SCAN` cursors carry the shard index in their
  high bits and exceed 32 bits. Clients keep them as strings or 64-bit
  integers and work, but `redis-cli --cluster import` parses them as an
  `int` and stops after the first shard: import from a server started with
  `--shards 1`.
- `appendfsync always` does not hold a reply until its write is fsynced, so a
  machine crash can lose writes acknowledged in the last few milliseconds.
- The `INFO` field `redis_version` reports `7.2.0` for client feature
  detection (`solikv_version` is meant to be SoliKV's own, but currently
  reports the engine crate's `0.1.0`).

## Upgrading from 0.4.x

- **Data files are migrated on first start.** A single `appendonly.aof` is
  replayed, rewritten into `appendonlydir/` and kept as
  `appendonly.aof.migrated`; per-shard `dump-<n>.rdb` files are still read
  (any shard count) and the next snapshot is the single `dump.rdb`.
  `--dbfilename` is now the base name of `<name>.rdb`.
- **With `appendonly` on, startup loads only the log**, not the snapshot
  (previously the log was replayed on top of the snapshot, which applied
  `INCR`/`RPUSH` twice).
- **Logical databases are real**: `SELECT n` used to share one keyspace.
  Existing data loads into database 0.
- **Cluster key slots changed**: 0.4.x computed CRC16 with a wrong table, so
  `CLUSTER KEYSLOT` disagreed with Redis for most keys, and a 0.4.x cluster's
  keys sit on nodes chosen by the wrong slot. The bus protocol changed too
  (receiver-bound frames, node ids, epochs), so 0.4.x and 0.5 nodes do not
  form one cluster. (0.4.x's own `--cluster-dump` lost data, so do not
  migrate with it.) For each old node, start a *copy* of its data directory
  with 0.5 as a standalone server with one shard, then copy its keys into a
  new 0.5 cluster:

  ```bash
  solikv --dir copy-of-node-a --port 7400 --shards 1 --bind 127.0.0.1 --rest-port 0
  redis-cli --cluster import 127.0.0.1:7000 --cluster-from 127.0.0.1:7400 --cluster-copy
  ```

  (`--shards 1` matters: see the `SCAN` cursor note under *Differences from
  Redis*.)
- **Cluster bus key**: use `--cluster-bus-secret[-file]`; `--cluster-password`
  still works as the bus key with a deprecation warning.
- **Lua is 5.1** (was 5.4), like Redis: scripts using 5.4-only syntax
  (`//`, `goto`, `utf8`, integer subtypes) must be adapted.
- **Yes/no flags take a value** (`--replica-read-only no`); `--tls-client-ca`
  is an alias of `--tls-ca-cert`, and client certificates are then required
  unless `--tls-auth-clients optional|no`.
- **Docker** runs as UID 10001: `chown 10001` a bind-mounted data directory.
- The daemon log moved from `/tmp/solikv.log` to `<dir>/solikv.log`.
- Pub/sub subscribers that fall 32 MB behind (or 8 MB for 60 s) are
  disconnected, as in Redis; tune with `--client-output-buffer-limit`.

See [CHANGELOG.md](CHANGELOG.md) for everything that changed.

## Architecture

| Crate | Role |
|---|---|
| `solikv-core` | Data types, the sharded store, expiry, eviction |
| `solikv-engine` | Command table and dispatch, sessions, transactions, scripting, ACL, replication stream, persistence orchestration |
| `solikv-resp` | RESP2/RESP3 codec |
| `solikv-persist` | Snapshot format, append-only log, Redis RDB importer, `DUMP` payloads |
| `solikv-pubsub` | Publish/subscribe broker |
| `solikv-cluster` | Cluster state, gossip bus, failover |
| `solikv-server` | The `solikv` binary: RESP, REST, TLS, replication links, config files |
| `solikv-ffi` | A small C-ABI key-value store for embedding |

## Benchmarks

See [`benches/RESULTS.md`](benches/RESULTS.md). Reproduce with
`./scripts/bench_compare.sh`.

## License

MIT License - see [LICENSE](LICENSE) file.
