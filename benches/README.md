# Benchmarks

Tools behind [RESULTS.md](RESULTS.md). They compare two SoliKV binaries on one machine,
alternating them round by round, each suite on a fresh server with its own data directory.

| File | What it does |
|---|---|
| `loadgen/` | Go load generator for workloads `redis-benchmark` cannot express: huge commands, growing strings and bitmaps, sorted-set rank queries, full scans, latency during SAVE/BGSAVE, refill after FLUSHALL, idle expiry. Prints one JSON line per measurement. |
| `harness.py` | Starts and stops a server, runs `redis-benchmark` or the load generator against it. |
| `compare.py` | The full before/after run: suites, one hot key, `--solo` BGSAVE, a huge key. About 40 minutes with 3 rounds. |
| `report.py` | Prints the Markdown tables of `RESULTS.md` from a `compare.py` results directory. |

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
