# Benchmarks

Tools behind [RESULTS.md](RESULTS.md). They compare two SoliKV binaries on one machine,
alternating them round by round, each suite on a fresh server with its own data directory.

| File | What it does |
|---|---|
| `loadgen/` | Go load generator for workloads `redis-benchmark` cannot express: huge commands, growing strings and bitmaps, sorted-set rank queries, full scans, latency during SAVE/BGSAVE, refill after FLUSHALL, idle expiry. Prints one JSON line per measurement. |
| `harness.py` | Starts and stops a server, runs `redis-benchmark` or the load generator against it. |
| `compare.py` | The full before/after run: suites, one hot key, `--solo` BGSAVE, a huge key. About 40 minutes with 3 rounds. |
| `report.py` | Prints the Markdown tables of `RESULTS.md` from a `compare.py` results directory. |
| `versus.py` | Several servers side by side (SoliKV, SoliKV `--solo`, Redis 7, Redis 8, KiviDB…), alternating round by round: suites, one hot key, a huge key. |
| `report_versus.py` | One column per server, best value of each row in bold. |

## Requirements

- `redis-benchmark` 7.x on `PATH`, or `REDIS_BENCHMARK=/path/to/redis-benchmark`. It ships with Redis; building it from the Redis sources (`make redis-benchmark`) is enough.
- Go 1.22+ to build the load generator, Python 3.8+.
- Both binaries built the same way. For measurements use `rbuild --faithful kv` (fat LTO, one codegen unit), not the default ThinLTO build.

## Run

```bash
cd benches
(cd loadgen && go build -o loadgen .)

# build and keep the "before" binary, then the "after" one
rbuild --faithful --no-link kv && cp ../target/remote/release/solikv /tmp/solikv-before
# ...check out the other revision...
rbuild --faithful --no-link kv && cp ../target/remote/release/solikv /tmp/solikv-after

python3 compare.py /tmp/solikv-before /tmp/solikv-after results/ --rounds 3
python3 report.py results/
```

Against Redis, build `redis-server` from the Redis sources (`make redis-server`) and give each server a name:

```bash
python3 versus.py results-vs/ \
  "SoliKV=solikv:/tmp/solikv-after:--save ''" \
  "SoliKV --solo=solikv:/tmp/solikv-after:--solo --save ''" \
  "Redis 7.4=redis:/opt/redis-7.4.11/src/redis-server" \
  "Redis 8.10=redis:/opt/redis-8.10.2/src/redis-server"
python3 report_versus.py results-vs/
```

Redis runs with `--appendonly yes --appendfsync everysec --save ""` and its other defaults
(`io-threads 1`); `--save ''` gives SoliKV the same no-snapshot setting.

KiviDB is a third kind, `kividb`, started with `--aof`. It has no command-line flag for the
fsync policy or save points, so the harness checks with `CONFIG GET` that they are `everysec`
and none, and refuses to run if a KiviDB version changes those defaults:

```bash
python3 versus.py results-kividb/ \
  "SoliKV=solikv:/tmp/solikv-after:--save ''" \
  "KiviDB 1.2=kividb:/opt/kividb/kividb" \
  "Redis 8.10=redis:/opt/redis-8.10.3/src/redis-server"
```

On a Linux machine with enough cores, keep servers and clients apart with `taskset` CPU lists:
`BENCH_SERVER_CPUS=0-7,16-23 BENCH_CLIENT_CPUS=8-15,24-31` put them on the two CCDs of a
Ryzen 9 9950X. Without these variables nothing is pinned.

The server under test listens on `127.0.0.1:6390` (REST on 6391); change it with `BENCH_PORT`.
Data directories go to the system temp directory unless `BENCH_TMP` is set. With AOF on, the
huge-key and hot-key runs write several GB, so point `BENCH_TMP` somewhere with room if `/tmp` is a small tmpfs.

A single scenario against a running server:

```bash
./loadgen/loadgen -addr 127.0.0.1:6390 -only zset_rank,append_growth
```

Opt-in scenarios: `save_latency_solo` (the server must run with `--solo`) and `bigkey_rewrite`
(`BIGKEY_N` elements, default 5M).

## Keep the machine quiet

Client and server share the CPU, so anything else running skews the result. A desktop
screensaver that started mid-run once took two cores while only one binary was being measured;
check the load before and after a run, and repeat a round that overlapped other work.
