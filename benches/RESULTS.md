# SoliKV benchmark results

**Date:** 2026-07-16  
**Machine:** AMD Ryzen 7 255 (16 threads), 27 GiB RAM, Linux x86_64  
**Tooling:** `redis-benchmark` (Redis 7.0.15 package), `oha`  
**Builds:** `cargo build --release` (SoliKV 0.4.1) vs Redis 7.0.15  

## Configuration

| Server | Port | Persistence |
|--------|------|-------------|
| SoliKV | 6390 (RESP), 5030 (REST) | AOF on, `appendfsync everysec` |
| Redis  | 6381 | AOF on, `appendfsync everysec`, RDB save disabled |

Both bound to `127.0.0.1`. Warmup: 50k SET+GET before measurement.

## Standard commands — 200k ops, 50 clients

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

## Pipeline — P=16, 500k ops, 50 clients

| Command | SoliKV+AOF | Redis 7.0+AOF | Delta |
|---------|------------|---------------|-------|
| SET | **1.60M** req/s | 1.02M req/s | **+57%** |
| GET | 1.63M req/s | 1.62M req/s | +1.0% |

```
SoliKV: SET 1602564  GET 1633987
Redis:  SET 1020408  GET 1618123
```

## Sustained load — 1M SET, 50 clients

| Metric | SoliKV+AOF | Redis 7.0+AOF |
|--------|------------|---------------|
| Throughput | 108K req/s | 108K req/s |
| p50 latency | 0.239 ms | 0.239 ms |
| p99 latency | 0.447 ms | 0.447 ms |
| max latency (200k sample) | 8.3 ms | 0.66 ms |

## REST API — `oha`, 10s, keep-alive

| Operation | Connections | Throughput |
|-----------|-------------|------------|
| GET `/kv/:key` | 100 | **485K** req/s |
| PUT `/kv/:key` | 100 | **449K** req/s |
| PUT `/kv/:key` | 200 | **489K** req/s |

## How to reproduce

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

## vs Redis 8.6.2 (2026-07-16, same host)

SoliKV release vs `redis:8-alpine` (**host network**, AOF everysec both sides, RDB disabled).  
50 clients; standard n=200k; pipeline P=16 n=500k.

### Standard commands

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

### Pipeline (P=16)

| Command | SoliKV | Redis 8.6.2 | Delta |
|---------|--------|-------------|-------|
| SET | **1.56M** | 0.87M | **+80%** |
| GET | 1.59M | 1.55M | +2.9% |

```
SoliKV: SET 1562500  GET 1592357
Redis8: SET  869565  GET 1547988
```

### Sustained 1M SET

| | SoliKV | Redis 8.6.2 |
|--|--------|-------------|
| Throughput | 108K req/s | 110K req/s (−1.5%) |

### Takeaways vs Redis 8 (multi-shard default)

- **Single-command path:** Redis 8 is slightly ahead on most writes (~1–3%); still within noise for many ops.
- **Pipeline SET:** SoliKV remains far ahead (~**+80%**); Redis 8’s pipelined write path did not close that gap vs Redis 7 on this machine.
- **Pipeline GET:** essentially tied (+3%).
- Docker port-publish (bridge NAT) understates Redis 8; use `--network host` for fair loopback benches.

## vs Redis 8.6.2 — **solo mode** (`--solo`)

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

## Notes

- Standard single-command throughput is network/client bound for both servers (~108K with 50 clients on this host); differences of ±2–3% are often noise.
- Pipeline SET is where SoliKV’s batch fast-path shows up (large win vs Redis 7 and Redis 8).
- SoliKV max latency can spike higher on long SET runs (AOF/expiry under the shard mutex); p50 is competitive.
- Re-run after hardware or version changes; do not treat absolute req/s as portable across machines.
