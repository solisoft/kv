#!/usr/bin/env python3
"""Benchmark several servers side by side, alternating them round by round.

usage: versus.py OUT_DIR TARGET... [--rounds 3] [--bigkey-n 2000000]

A TARGET is NAME=KIND:PATH[:EXTRA FLAGS], KIND being solikv, redis or kividb:

  versus.py results/ \\
    "SoliKV=solikv:/tmp/solikv:--save ''" \\
    "SoliKV --solo=solikv:/tmp/solikv:--solo --save ''" \\
    "Redis 7.4=redis:/opt/redis-7/src/redis-server" \\
    "Redis 8.10=redis:/opt/redis-8/src/redis-server"

Every server runs with AOF on, appendfsync everysec, on a fresh data
directory per suite. Redis gets `--save ""` (no RDB save points) from the
harness; give SoliKV `--save ''` to match. Writes per-round suites, hot-key
and huge-key results to OUT_DIR; `report_versus.py OUT_DIR` prints the tables.
"""
import argparse, datetime, json, os, platform, shlex, subprocess, sys

import harness

HOT_COMMANDS = ["set", "get", "incr", "lpush", "hset", "zadd"]


def parse_target(spec):
    name, _, rest = spec.partition("=")
    kind, _, rest = rest.partition(":")
    path, _, extra = rest.partition(":")
    if not name or kind not in ("solikv", "redis", "kividb") or not path:
        sys.exit(f"bad target {spec!r}: want NAME=solikv|redis|kividb:PATH[:FLAGS]")
    return name, harness.target(path, kind, shlex.split(extra))


def version(t):
    r = subprocess.run([t["path"], "--version"], capture_output=True, text=True)
    return (r.stdout or r.stderr).strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("targets", nargs="+")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--bigkey-n", type=int, default=2_000_000)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    targets = dict(parse_target(s) for s in a.targets)

    def log(msg):
        print(msg, file=sys.stderr, flush=True)

    def save(name, data):
        with open(os.path.join(a.out, name), "w") as f:
            json.dump(data, f, indent=1)

    cpu = ""
    try:
        cpu = next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
    except (OSError, StopIteration):
        pass
    save("meta.json", {
        "date": datetime.date.today().isoformat(), "host": platform.platform(), "cpu": cpu,
        "threads": os.cpu_count(), "rounds": a.rounds, "bigkey_n": a.bigkey_n,
        "targets": {n: {**t, "version": version(t)} for n, t in targets.items()},
    })

    suites = {n: [] for n in targets}
    for r in range(1, a.rounds + 1):
        for n, t in targets.items():
            log(f"suite {n} round {r}/{a.rounds}")
            suites[n].append(harness.suite(t))
            save("suites.json", suites)

    hot = {n: {c.upper(): [] for c in HOT_COMMANDS} for n in targets}
    for r in range(1, a.rounds + 1):
        for c in HOT_COMMANDS:
            for n, t in targets.items():
                log(f"hot key {c} {n} round {r}/{a.rounds}")
                hot[n][c.upper()].append(harness.hot_key(t, c))
        save("hotkey.json", hot)

    big = {}
    for n, t in targets.items():
        log(f"huge key {n}")
        p, d = harness.start_target(t)
        try:
            big[n] = harness.loadgen("bigkey_rewrite", env={"BIGKEY_N": str(a.bigkey_n)})
        finally:
            harness.stop(p, d)
        save("bigkey.json", big)
    log(f"done: {a.out}")


if __name__ == "__main__":
    main()
