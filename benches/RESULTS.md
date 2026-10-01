# SoliKV benchmark results

## Audit fixes: before and after (2026-10-01)

**Before:** `main` at 6645a90 (0.4.3). **After:** `claude/audit-fixes` at 7109ba1.
**Machine:** AMD Ryzen 7 255 (16 threads), 27 GiB RAM, Linux 7.2; client and server on the same host over loopback.
**Builds:** both with `rbuild --faithful kv` (`lto = "fat"`, `codegen-units = 1`, mimalloc).
**Server flags (both):** `--bind 127.0.0.1 --appendonly true --appendfsync everysec`, default 16 shards, data directory on tmpfs.
**Method:** three rounds alternating before and after, a fresh server for every suite, median shown. `redis-benchmark` 7.4.2 built from source; a Go load generator for the targeted workloads.

### Targeted workloads

| Workload | Detail | Before | After | Speedup |
|---|---|---|---|---|
| Growing a bitmap with SETBIT | 100k SETBITs at rising offsets, 10 MB at the end | 40.96 s | 105.3 ms | **×389** |
| ZRANGE by index, deep | 10 members from rank 100k–190k of 200k | 3.2K/s | 405.5K/s | **×126** |
| Building a string with APPEND | 20k appends of 512 B, 10 MB at the end | 2.89 s | 23.0 ms | **×126** |
| A full ZSCAN | sorted set of 50k members, COUNT 100 | 2.59 s | 20.7 ms | **×125** |
| SINTER, large with small | 100k-member set with a 100-member set | 0.9K/s | 75.5K/s | **×81** |
| ZRANK | random members of a 200k-member sorted set | 7.9K/s | 583.0K/s | **×74** |
| A full HSCAN | hash of 50k fields, COUNT 100 | 698.4 ms | 18.4 ms | **×38** |
| Emptying a set with SPOP | 100k members, one SPOP at a time | 275.6 ms | 113.6 ms | **×2.4** |
| One RPUSH of 100k elements | a single 1.3 MB command | 15.4 ms | 12.0 ms | **×1.3** |
| One MSET of 10k pairs | a single 10k-pair command | 4.25 ms | 3.31 ms | **×1.3** |
| DEL of 1000 keys | keys spread over all shards | 234 µs | 211 µs | **×1.1** |

### One hot key — `redis-benchmark --threads 6 -c 96 -P 32 -n 20000000`, AOF on

| Command | Before | After | Change | Before, runs | After, runs |
|---|---|---|---|---|---|
| SET | 2.96M/s | 3.81M/s | +29% | 3.08 · 2.96 · 2.22 | 3.81 · 3.81 · 3.81 |
| INCR | 1.40M/s | 3.48M/s | +148% | 1.40 · 1.40 · 1.38 | 3.48 · 3.48 · 3.33 |
| HSET | 2.05M/s | 3.33M/s | +63% | 2.05 · 2.05 · 2.05 | 3.33 · 3.33 · 3.33 |
| ZADD | 2.35M/s | 3.20M/s | +36% | 2.86 · 2.35 · 2.35 | 3.20 · 3.20 · 3.20 |
| LPUSH | 3.33M/s | 2.50M/s | −25% | 2.76 · 3.33 · 3.33 | 2.58 · 2.50 · 2.28 |

### Latency seen by other clients while saving

| Measurement | Setup | Before | After |
|---|---|---|---|
| BGSAVE, worst GET | 1M keys over 16 shards, 16 clients | 14.1 ms | 3.27 ms |
| BGSAVE, GET p99.9 | 1M keys over 16 shards, 16 clients | 2.43 ms | 795 µs |
| SAVE, worst GET | other clients while SAVE runs | 200.0 ms | 74.2 ms |
| SAVE, GET p99.9 | other clients while SAVE runs | 2.46 ms | 664 µs |
| BGSAVE in --solo mode, worst GET | 1M keys, single-threaded engine | 1.21 ms | 1.51 ms |
| BGSAVE in --solo mode, GET p99.9 | 1M keys, single-threaded engine | 181 µs | 275 µs |
| Refill after FLUSHALL, worst SET | 1M keys flushed, then 16 clients write | 220.9 ms | 88.1 ms |
| Refill after FLUSHALL, SET p99.9 | 1M keys flushed, then 16 clients write | 8.66 ms | 4.16 ms |
| BGSAVE of a 2M-element list, worst | GET/LPUSH on other keys of that shard | 15.7 ms | 1.99 ms |
| BGSAVE of a 2M-element list, p99.9 | GET/LPUSH on other keys of that shard | 66 µs | 217 µs |

Expired keys still resident 3 s after 200k keys with a 200 ms TTL expired, idle server: **190,201 → 0**.

### Standard commands — 50 clients, 200k ops

| Command | Before | After | Change |
|---|---|---|---|
| PING INLINE | 122.8K | 122.2K | −0.4% |
| PING MBULK | 122.3K | 121.1K | −1.0% |
| SET | 122.9K | 121.1K | −1.5% |
| GET | 122.5K | 122.4K | −0.1% |
| INCR | 122.1K | 122.3K | +0.2% |
| LPUSH | 121.7K | 122.0K | +0.2% |
| RPUSH | 122.1K | 121.1K | −0.8% |
| LPOP | 123.3K | 122.3K | −0.8% |
| RPOP | 122.2K | 121.5K | −0.6% |
| SADD | 124.7K | 121.1K | −2.8% |
| HSET | 122.3K | 121.7K | −0.5% |
| ZADD | 122.5K | 122.9K | +0.4% |
| MSET | 121.2K | 122.6K | +1.2% |

### Pipelined — P=16, 50 clients, 2M ops

| Command | Before | After | Change |
|---|---|---|---|
| SET | 1.78M | 1.78M | +0.0% |
| GET | 1.81M | 1.78M | −1.7% |
| INCR | 1.70M | 1.76M | +3.8% |
| LPUSH | 1.75M | 1.77M | +1.0% |
| SADD | 1.80M | 1.78M | −1.2% |
| HSET | 1.76M | 1.79M | +1.8% |
| ZADD | 1.79M | 1.77M | −1.1% |

### Reading these numbers

- **Targeted workloads** are where data structures changed: in-place strings, order-statistic sorted sets, cursor-based scans that no longer clone the collection, set operations that iterate the smallest input.
- **Standard and pipelined `redis-benchmark`** saturate the single benchmark client before either server, so those tables show that nothing regressed, not server capacity. Differences of ±3% are noise.
- **Hot-key throughput** is reported by `redis-benchmark` in steps of its 0.25 s timer, about 4% at 20M operations, which is why several runs repeat the same value.
- **Hot-key LPUSH is the one regression (−25%).** The new server compacts its AOF automatically (`--auto-aof-rewrite-percentage 100`). Each rewrite must save the single 20M-element list, and writers to that key wait for it. With `--auto-aof-rewrite-percentage 0` the same test measured 3.08M/s against 2.58–2.67M/s for the old server in the same session. The old server never compacted its AOF; that is also why it applied writes twice on restart.
- **`--solo` BGSAVE rows** come from a separate run (three alternating rounds, fresh servers, scenario alone). The old server's low figures there came from a data race: `block_in_place` handed the only worker to a second thread while both touched the unsynchronized store. Fixed in the audit's first phase.
- **SAVE, worst GET** is a single sample per run and is inflated for both servers by the scenarios that run before it on the same server. Run alone (three alternating rounds, fresh servers) it measured 2.6–2.8 ms after against 4.2–6.0 ms before, with GET p99.9 at 0.81–0.85 ms against 2.50–2.72 ms.
- The old server had no working BGREWRITEAOF, so rewrite latency with a huge key is not compared. On the new server, the worst GET/LPUSH on other keys of that shard during a rewrite of a 2M-element list was 0.9 ms (17.6 ms in an earlier, noisier run).
- The 2026-07-16 table below reported pipelined SET at +57% over Redis 7.0. That comparison was not like-for-like: the old server replied before its AOF bytes were written (they were only queued), while Redis writes its AOF buffer before replying.
- Cluster mode, replication and TLS throughput were not measured.

To reproduce: `python3 benches/compare.py BEFORE AFTER results/` then `python3 benches/report.py results/`. See [README.md](README.md) for requirements.

---

## Historical: 0.4.1 vs Redis (2026-07-16)

**Date:** 2026-07-16  
**Machine:** AMD Ryzen 7 255 (16 threads), 27 GiB RAM, Linux x86_64  
**Tooling:** `redis-benchmark` (Redis 7.0.15 package), `oha`  
**Builds:** `cargo build --release` (SoliKV 0.4.1) vs Redis 7.0.15  

### Configuration

| Server | Port | Persistence |
|--------|------|-------------|
| SoliKV | 6390 (RESP), 5030 (REST) | AOF on, `appendfsync everysec` |
| Redis  | 6381 | AOF on, `appendfsync everysec`, RDB save disabled |

Both bound to `127.0.0.1`. Warmup: 50k SET+GET before measurement.

### Standard commands — 200k ops, 50 clients

| Command | SoliKV+AOF | Redis 7.0+AOF | Delta |
|---------|------------|---------------|-------|
| PING | 109K req/s | 108K req/s | +0.6% |
| SET | 106K req/s | 108K req/s | −1.5% |
| GET | 108K req/s | 108K req/s | +0.8% |
| INCR | 108K req/s | 108K req/s | tied |
| LPUSH | 108K req/s | 108K req/s | −0.5% |
| RPUSH | 108K req/s | 108K req/s | −0.6% |
| LPOP | 108K req/s | 109K req/s | −0.5% |
| RPOP | 109K req/s | 108K req/s | +0.4% |
| SADD | 108K req/s | 108K req/s | −0.6% |
| HSET | 107K req/s | 108K req/s | −1.0% |
| ZADD | 108K req/s | 108K req/s | +0.2% |
| MSET (10 keys) | 108K req/s | 105K req/s | **+2.3%** |

Exact rps (from `redis-benchmark -q`):

```
SoliKV: PING 108637  SET 106045  GET 108460  INCR 107759
        LPUSH 107933 RPUSH 107701 LPOP 108284 RPOP 108578
        SADD 107817  HSET 107066  ZADD 108108  MSET 107585

Redis:  PING 107991  SET 107701  GET 107643  INCR 107759
        LPUSH 108460 RPUSH 108342 LPOP 108873 RPOP 108167
        SADD 108460  HSET 108167  ZADD 107875  MSET 105208
```

### Pipeline — P=16, 500k ops, 50 clients

| Command | SoliKV+AOF | Redis 7.0+AOF | Delta |
|---------|------------|---------------|-------|
| SET | **1.60M** req/s | 1.02M req/s | **+57%** |
| GET | 1.63M req/s | 1.62M req/s | +1.0% |

```
SoliKV: SET 1602564  GET 1633987
Redis:  SET 1020408  GET 1618123
```

### Sustained load — 1M SET, 50 clients

| Metric | SoliKV+AOF | Redis 7.0+AOF |
|--------|------------|---------------|
| Throughput | 108K req/s | 108K req/s |
| p50 latency | 0.239 ms | 0.239 ms |
| p99 latency | 0.447 ms | 0.447 ms |
| max latency (200k sample) | 8.3 ms | 0.66 ms |

### REST API — `oha`, 10s, keep-alive

| Operation | Connections | Throughput |
|-----------|-------------|------------|
| GET `/kv/:key` | 100 | **485K** req/s |
| PUT `/kv/:key` | 100 | **449K** req/s |
| PUT `/kv/:key` | 200 | **489K** req/s |

### How to reproduce

```bash
# Build
cargo build --release -p solikv-server

# SoliKV (AOF everysec)
./target/release/solikv \
  --bind 127.0.0.1 --port 6390 --rest-port 5030 \
  --dir /tmp/solikv-bench --appendonly true --appendfsync everysec \
  --protected-mode no

# Redis (AOF everysec, no RDB)
redis-server --port 6381 --bind 127.0.0.1 --dir /tmp/redis-bench \
  --appendonly yes --appendfsync everysec --save "" --protected-mode no --daemonize yes

# RESP suite
./scripts/bench_compare.sh 50 200000 6390 6381

# REST
env -u NO_COLOR oha -z 10s -c 100 --no-tui http://127.0.0.1:5030/kv/benchkey
```

### vs Redis 8.6.2 (2026-07-16, same host)

SoliKV release vs `redis:8-alpine` (**host network**, AOF everysec both sides, RDB disabled).  
50 clients; standard n=200k; pipeline P=16 n=500k.

#### Standard commands

| Command | SoliKV | Redis 8.6.2 | Delta |
|---------|--------|-------------|-------|
| PING | 109K | 107K | +1.8% |
| SET | 108K | 109K | −1.4% |
| GET | 108K | 108K | +0.5% |
| INCR | 109K | 110K | −0.7% |
| LPUSH | 107K | 109K | −2.6% |
| RPUSH | 106K | 110K | −3.1% |
| LPOP | 107K | 109K | −2.1% |
| RPOP | 107K | 110K | −2.4% |
| SADD | 107K | 108K | −1.3% |
| HSET | 107K | 110K | −2.6% |
| ZADD | 106K | 108K | −1.8% |
| MSET (10 keys) | 105K | 106K | −0.6% |

Exact rps:

```
SoliKV:  PING 108519 SET 107759 GET 108225 INCR 108814
         LPUSH 106667 RPUSH 106157 LPOP 106781 RPOP 107181
         SADD 106724 HSET 106895 ZADD 106440 MSET 105430

Redis8:  PING 106610 SET 109290 GET 107643 INCR 109589
         LPUSH 109469 RPUSH 109529 LPOP 109111 RPOP 109769
         SADD 108167 HSET 109769 ZADD 108342 MSET 106101
```

#### Pipeline (P=16)

| Command | SoliKV | Redis 8.6.2 | Delta |
|---------|--------|-------------|-------|
| SET | **1.56M** | 0.87M | **+80%** |
| GET | 1.59M | 1.55M | +2.9% |

```
SoliKV: SET 1562500  GET 1592357
Redis8: SET  869565  GET 1547988
```

#### Sustained 1M SET

| | SoliKV | Redis 8.6.2 |
|--|--------|-------------|
| Throughput | 108K req/s | 110K req/s (−1.5%) |

#### Takeaways vs Redis 8 (multi-shard default)

- **Single-command path:** Redis 8 is slightly ahead on most writes (~1–3%); still within noise for many ops.
- **Pipeline SET:** SoliKV remains far ahead (~**+80%**); Redis 8’s pipelined write path did not close that gap vs Redis 7 on this machine.
- **Pipeline GET:** essentially tied (+3%).
- Docker port-publish (bridge NAT) understates Redis 8; use `--network host` for fair loopback benches.

### vs Redis 8.6.2 — **solo mode** (`--solo`)

Solo mode is Redis-shaped: **1 shard, no mutex, 1 Tokio worker**. Best match for classic `redis-benchmark` (single-core fight).

Config: SoliKV `--solo` + AOF everysec; Redis 8 host-net + AOF everysec. 50 clients, 200k ops (standard), P=16 500k (pipeline). Date 2026-07-17.

| Workload | SoliKV `--solo` | Redis 8.6.2 | Delta |
|----------|-----------------|-------------|-------|
| PING | **103K** | 98K | **+5%** |
| SET | **103K** | 102K | **+0.8%** |
| GET | 102K | 104K | −1.7% |
| INCR | 103K | 112K | −9% |
| Pipeline SET (P=16) | **1.45M** | 0.93M | **+56%** |
| Pipeline GET (P=16) | 1.60M | 1.61M | ~tie |

Without AOF (cache mode), solo SET/GET matched Redis 8 exactly at ~105K on this host.

```bash
./target/release/solikv --solo --bind 127.0.0.1 --port 6390 \
  --appendonly true --appendfsync everysec --protected-mode no --dir /tmp/solo
```

### Notes

- Standard single-command throughput is network/client bound for both servers (~108K with 50 clients on this host); differences of ±2–3% are often noise.
- Pipeline SET is where SoliKV’s batch fast-path shows up (large win vs Redis 7 and Redis 8).
- SoliKV max latency can spike higher on long SET runs (AOF/expiry under the shard mutex); p50 is competitive.
- Re-run after hardware or version changes; do not treat absolute req/s as portable across machines.
