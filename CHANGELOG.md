# Changelog

## 0.5.0 — unreleased

The follow-up to the September 2026 audit (security, performance, missing
features). Proposed version: 0.5.0 — the on-disk layout, the cluster bus and
several defaults changed; see *Breaking changes* and the README's *Upgrading
from 0.4.x*. The workspace `Cargo.toml` versions are not bumped yet.

### Security

- **Pre-authentication crash fixed**: the RESP decoder recursed once per
  nesting level without a limit, so ~40 KB of `*1\r\n` overflowed the stack
  and aborted the process before `AUTH`. Nesting is refused before
  authentication and capped at 32 levels after; lines without CRLF are
  refused past 64 KiB; unauthenticated connections get a 128 KiB buffer and
  an `--auth-timeout`; TLS handshakes time out.
- **Allocation bounds**: `SETRANGE` (offset wrap-around and an endless
  zero-fill loop), `SETBIT`/`GETBIT`, `BF.RESERVE`, pop and scan `COUNT`s,
  TTL arithmetic, Redis RDB import lengths and AOF replay counts no longer
  size allocations from untrusted numbers.
- **`--solo` data race**: `KEYS`, `FLUSH*` and `SAVE` called
  `block_in_place` on the one-worker runtime, which let a second thread into
  the unlocked store (undefined behaviour). They now run inline, and the
  shutdown save runs on the worker.
- **Cluster bus**: frames are bound to their receiving node, length-prefixed
  MAC input, nonces validated, one replay memory for the whole bus (it was
  per connection, while every frame used a new connection), length-limited
  reader, connection cap, timeouts, rate-limited warnings; slot numbers
  ≥ 16384 are refused instead of panicking. The bus key is now its own
  secret (`--cluster-bus-secret[-file]`), and `--tls-cluster yes` encrypts
  the bus with mutual TLS.
- **TLS everywhere**: REST TLS with the RESP certificate, client certificates
  (`--tls-ca-cert`, `--tls-auth-clients`), TLS replication links and
  `MIGRATE` (`--tls-replication`), `--cluster-dump`/`--cluster-restore`
  over TLS. Peers are checked against the CA. `--tls-auth-clients-user CN`
  (Redis 8's `tls-auth-clients-user`) logs a RESP client in as the enabled
  ACL user its verified certificate's subject CN names.
- **ACL**: users with SHA-256 passwords, command/category/subcommand rules,
  key (`~`, `%R~`, `%W~`) and channel (`&`) patterns, selectors, `ACL LOG`,
  `--aclfile`. Scripts, `SUBSCRIBE`, blocking commands and every REST route
  are checked.
- **REST**: Host allowlist on loopback binds (DNS rebinding), separate
  `--rest-token`, `Basic` auth for ACL users, connection cap, a request
  without credentials no longer counts as a failed attempt; the auth-failure
  maps are pruned.
- **Secrets**: `MONITOR` and `SLOWLOG` showed passwords from
  `ACL SETUSER >pw`, `CONFIG SET masterauth` and `HELLO AUTH`; they are
  redacted as in Redis.
- **Files**: data directory 0700, data/pid/log files 0600 opened with
  `O_NOFOLLOW`; the daemon log moved from a fixed `/tmp` path to
  `<dir>/solikv.log`.
- **Lua**: 64 MB memory limit per VM, text chunks only, read-only globals and
  libraries.
- **Docker** image runs as UID 10001.
- **Dependencies**: rustls, rustls-webpki, h2, crossbeam-epoch, anyhow and
  rand updated, clearing 7 RustSec advisories and 2 unsoundness warnings.
- **Flags**: yes/no options were value-less switches, so
  `--replica-read-only no` turned the option *on*; they now take their
  value. `--cluster-password-seed-only=false` parses.

### Correctness and data safety

- **Restarts reproduce the stopped state exactly.** The single
  `appendonly.aof` was replayed on top of the RDB snapshot and never
  truncated, applying `INCR`, `RPUSH`, `APPEND`, `XADD` … twice. The log is
  now one file per shard in `appendonlydir/` with a manifest, records are
  appended under the shard lock in execution order, and a rewrite switches
  each shard's log at the instant its snapshot is taken.
- **TTLs** are logged as absolute deadlines and no longer restart on replay;
  expirations and evictions are logged as `DEL`.
- **SIGTERM** (docker stop, systemd) now saves: graceful shutdown on SIGTERM
  and SIGINT, `SHUTDOWN [SAVE|NOSAVE]`.
- **SIGKILL** no longer loses up to a second of acknowledged writes under
  `everysec`: records are handed to the kernel on every writer pass (about
  every millisecond) instead of at the once-a-second fsync.
- **Shard count independence**: snapshots are a single checksummed
  `dump.rdb`; loading re-hashes every key, so `--shards` can change.
- **Multi-key commands across shards**: `RENAME`, `SMOVE`, `S*STORE`,
  `Z*STORE`, `BITOP`, `PFMERGE`, `GEOSEARCHSTORE`, `MSET`/`MSETNX` worked
  inside the first key's shard only and could write keys where no lookup
  found them. Shards are now chosen by Redis Cluster slot (`{hash tags}`
  honoured) and spanning commands lock every shard involved.
- **CRC16 table**: 34 wrong entries made `CLUSTER KEYSLOT` disagree with
  Redis for most keys.
- **Transactions and scripts are atomic**; `WATCH`/`UNWATCH`, `EXECABORT`,
  nested `MULTI` refused without clearing the queue.
- **Logical databases** are real (`SELECT` returned OK on one shared
  keyspace, so `FLUSHDB` wiped everything).
- **No more fake OKs**: `REPLICAOF`, `CLIENT TRACKING`, `CONFIG SET` of
  unknown parameters and `COMMAND` either work or return an error.
- **Command options**: `SET KEEPTTL/EXAT/PXAT/GET`, `EXPIRE NX/XX/GT/LT`,
  `GETEX EXAT/PXAT/PERSIST`, `ZADD INCR`, unified `ZRANGE`
  (`BYSCORE/BYLEX/REV/LIMIT`), exclusive `(` bounds, `SCAN TYPE`,
  `ZSCAN MATCH`, `PUBSUB NUMSUB/CHANNELS/NUMPAT`, `XREADGROUP NOACK`;
  `APPEND`/`SETRANGE`/`SETBIT`/`INCR*` keep the key's TTL.
- `SCAN` no longer skips keys that exist for the whole iteration when others
  are deleted. On a multi-shard server its cursors are
  `position × shards + shard`, so they stay below the key count instead of
  carrying the shard in the high bits (which made them exceed 32 bits and
  stopped `redis-cli --cluster import` after the first shard).
- **`appendfsync always` holds replies until their writes are fsynced**
  (group commit, as Redis does before replying): a reply used to leave
  before its record even reached the file. Replies produced meanwhile on
  every connection, and REST responses, share one fsync; an fsync error
  under `always` stops the server, as in Redis.
- `INFO`'s `solikv_version` reports the release: every crate now takes the
  workspace version (it reported the engine crate's `0.1.0`).
- Expired keys are reclaimed by a timer (`--hz`), not only when traffic hits
  the shard: an idle server kept almost all expired keys resident.
- **Cluster dump/restore lost data**: replies split across reads were cut
  short, the first element of every array was dropped, values containing
  CRLF were truncated, TTLs were lost, collections were restored as strings
  holding their JSON, and failures exited 0. The tool now reads whole RESP
  replies, lists keys with `SCAN`, dumps each key's `DUMP` payload and TTL
  in milliseconds (file format version 2; the binary format restores too),
  restores with `RESTORE … REPLACE`, and fails loudly on any missed node or
  error. Version 1 files still restore.
- Smaller fixes found along the way: `INCRBYFLOAT` formatting, `GEOHASH`'s
  11th character, `XGROUP` events, `XADD` with a bad ID leaving an empty
  stream, `GEOSEARCHSTORE` across shards, `--import-redis-rdb` routing,
  `cluster_dump` reading the bus port as the data port, the cluster state
  export dropping a slot where two owners' ranges met.

### Performance

Measured on a 16-thread Ryzen 7 255 with fresh servers per run; the full
before/after comparison is in `benches/RESULTS.md`.

- Data structures: order-statistic sorted sets (`ZRANK`/`ZRANGE` by index in
  O(log N)), in-place string mutation (`APPEND`, `SETRANGE`, `SETBIT`), lazy
  `HSCAN`/`ZSCAN`/`SSCAN`, `SINTER` from the smallest set without cloning,
  O(1) random `SPOP`, boxed large value variants (a string entry is 64 bytes,
  down from about 112).
- Command path: a command table resolved once per command, allocation-free
  option parsing, replies encoded straight into the write buffer, RESP
  decoding without re-parsing incomplete frames, a cached clock.
- Contention: consecutive pipelined commands on one shard run under one lock
  hold, overhead moved out of the lock, waiters spin briefly instead of
  parking; per-thread counters instead of shared atomics.
- Snapshots are taken in steps, so `BGSAVE`, `SAVE` and `--solo` snapshots
  no longer stall other clients for the whole serialization. Lists, sets,
  hashes and sorted sets of 8192 elements or more are serialized a slice per
  step, so one huge key no longer holds its shard for a whole `BGSAVE`,
  `BGREWRITEAOF` or replica full sync (streams are still written whole).
- The keyspace grows incrementally (`IncMap`): growing past ~229k keys per
  shard copied and rehashed the whole map under the shard lock, pausing
  every shard at once.
- Note: the message of commit `2fc09ab` quotes baseline `ZADD` figures
  (0.33–0.53M ops/s) taken from servers reused between runs; the
  fresh-server figures are in commits `66e9673` (phase 7d) and `e77b042`.

### New commands and features

- **Blocking commands**: `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BLMPOP`,
  `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`; `XREAD`/`XREADGROUP BLOCK` block;
  `CLIENT UNBLOCK`.
- **Lists**: `LMOVE`, `RPOPLPUSH`, `LPOS`, `LPUSHX`, `RPUSHX`, `LMPOP`.
- **Sorted sets**: `ZREVRANGEBYSCORE`, `*BYLEX` family, `ZREMRANGEBY*`,
  `ZMSCORE`, `ZRANDMEMBER`, `ZRANGESTORE`, `ZUNION`, `ZINTER`, `ZDIFF[STORE]`,
  `ZINTERCARD`, `ZMPOP`.
- **Sets and hashes**: `SSCAN`, `SMISMEMBER`, `SINTERCARD`, `S*STORE`,
  `HSTRLEN`, `HRANDFIELD`; hash field expiry (`HEXPIRE`, `HPEXPIRE`,
  `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`,
  `HPERSIST`) and Redis 8's `HGETDEL`, `HGETEX`, `HSETEX`.
- **Streams**: `XCLAIM`, `XAUTOCLAIM`, `XSETID`, `XINFO GROUPS/CONSUMERS/
  STREAM FULL`, `XGROUP SETID/CREATECONSUMER`, `XPENDING IDLE`, `XADD`/`XTRIM`
  `LIMIT` with Redis' approximate `~` trimming.
- **Keys, strings, geo**: `DUMP`/`RESTORE` in Redis' format, `COPY`, `SORT`,
  `SORT_RO`, `TIME`, `LCS`, `BITPOS`, `BITFIELD`, `BITFIELD_RO`,
  `BITCOUNT … BIT`, `GEORADIUS*`, `EXPIRETIME`, `PEXPIRETIME`.
- **Scripting**: Lua 5.1 with `cjson`, `cmsgpack`, `bit`, `struct`;
  `EVAL_RO`/`EVALSHA_RO`, `SCRIPT KILL`, `-BUSY`, `#!lua flags=`,
  `FUNCTION`/`FCALL`/`FCALL_RO` (libraries stored in snapshots and the log).
- **Protocol**: RESP3 (`HELLO 3`, push frames, typed replies including inside
  `EXEC`), `AUTH user password`, client-side caching (`CLIENT TRACKING` in all
  modes), sharded pub/sub (`SSUBSCRIBE`, `SPUBLISH`).
- **Memory**: `--maxmemory` with the eight Redis eviction policies, `OBJECT
  IDLETIME/FREQ`, `CLIENT NO-EVICT`, `CLIENT NO-TOUCH`.
- **Replication**: `REPLICAOF`, `PSYNC` full and partial resync, backlog,
  `WAIT`, `ROLE`, `INFO replication`, read-only replicas,
  `min-replicas-to-write`.
- **Cluster**: Redis Cluster protocol — node ids, config epochs, `CLUSTER
  SLOTS/SHARDS/NODES` in Redis 7's format, `SETSLOT`, `COUNTKEYSINSLOT`,
  `GETKEYSINSLOT`, `MOVED`, `ASK`/`ASKING`, `CROSSSLOT`, `TRYAGAIN`,
  `READONLY`, `MIGRATE` (with connection caching), resharding with
  `redis-cli --cluster reshard`, failure detection and replica election,
  `CLUSTER FAILOVER [FORCE|TAKEOVER]`, replica migration, `PUBLISH` across
  nodes.
- **Persistence**: `BGREWRITEAOF` and automatic rewrites, Redis save points
  (`--save`), `LASTSAVE`, `CONFIG SET appendonly/appendfsync/save`,
  `DEBUG RELOAD`; Redis RDB import keeps databases and reads hashes with
  field expiry.
- **Operations**: redis.conf config files (`--config`, `include`),
  `CONFIG GET/SET/REWRITE`, full `INFO` (including `commandstats`,
  `errorstats`), `SLOWLOG`, `LATENCY`, `MONITOR`, `MEMORY`, `DEBUG` (off by
  default), `CLIENT LIST/KILL/PAUSE` with types, `maxclients`,
  `client-output-buffer-limit` for every class, `--log-format json`, REST
  `/health`, `/ready` and Prometheus `/metrics`.

### Breaking changes

- On-disk layout: `appendonlydir/` replaces `appendonly.aof` (migrated on
  first start and kept as `appendonly.aof.migrated`); `dump.rdb` replaces
  `dump-<n>.rdb` (still read); `--dbfilename` is the base name of
  `<name>.rdb`. With `appendonly` on, startup loads the log only.
- Keys are placed in shards by cluster slot; existing data is re-hashed on
  load. A 0.4.x *cluster* placed keys by the wrong CRC16 and speaks the old
  bus protocol: load a copy of each old node's data directory into a 0.5
  standalone server and copy it into a new cluster with
  `redis-cli --cluster import … --cluster-copy` (0.4.x's own
  `--cluster-dump` lost data).
- Lua 5.4 → 5.1.
- `SELECT n` selects a separate database; old data is in database 0.
- `--cluster-password` as the bus key is deprecated in favour of
  `--cluster-bus-secret`.
- `--tls-client-ca` is an alias of `--tls-ca-cert` and now enforces client
  certificates by default (`--tls-auth-clients`).
- Yes/no flags take a value.
- The Docker image runs as UID 10001.
- The daemon log is `<dir>/solikv.log`.
- Commands with nested arrays are refused; slow pub/sub subscribers are
  disconnected (32 MB hard, 8 MB for 60 s); the `redis` table in Lua is
  read-only.
- `solikv-replication` (unused) was removed from the workspace.

### Known limitations

- No Sentinel.
- A very large stream is still serialized in one step during snapshots and
  rewrites.

## 0.4.3

Cluster mode that forms, shards and is reachable.
