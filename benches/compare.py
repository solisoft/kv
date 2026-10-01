#!/usr/bin/env python3
"""Compare two SoliKV binaries, alternating them round by round.

usage: compare.py BEFORE_BINARY AFTER_BINARY OUT_DIR [--rounds 3] [--bigkey-n 2000000]

Writes into OUT_DIR:
  before-N.json, after-N.json  one suite per round (redis-benchmark + load generator)
  hotkey.json                  ops/s per command and round, one hot key
  solo.json                    BGSAVE latency on a --solo server, per round
  bigkey.json                  latency on other keys while a huge list is saved
  meta.json                    binaries, versions, host, date

Then `report.py OUT_DIR` prints the Markdown tables used in RESULTS.md.
Build both binaries the same way (e.g. `rbuild --faithful kv`) and keep the
machine otherwise idle: one run takes about 40 minutes.
"""
import argparse, datetime, json, os, platform, subprocess, sys

import harness

HOT_COMMANDS = ["set", "lpush", "hset", "zadd", "incr"]


def save(out, name, data):
    with open(os.path.join(out, name), "w") as f:
        json.dump(data, f, indent=1)


def version(binary):
    return subprocess.run([binary, "--version"], capture_output=True, text=True).stdout.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("before")
    ap.add_argument("after")
    ap.add_argument("out")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--bigkey-n", type=int, default=2_000_000)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    sides = {"before": os.path.abspath(a.before), "after": os.path.abspath(a.after)}

    cpu = ""
    try:
        cpu = next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
    except (OSError, StopIteration):
        pass
    save(a.out, "meta.json", {
        "date": datetime.date.today().isoformat(), "host": platform.platform(), "cpu": cpu,
        "threads": os.cpu_count(), "rounds": a.rounds, "bigkey_n": a.bigkey_n,
        "binaries": {s: {"path": b, "version": version(b)} for s, b in sides.items()},
    })

    def log(msg):
        print(msg, file=sys.stderr, flush=True)

    for r in range(1, a.rounds + 1):
        for side, binary in sides.items():
            log(f"suite {side} round {r}/{a.rounds}")
            save(a.out, f"{side}-{r}.json", harness.suite(binary))

    hot = {side: {c.upper(): [] for c in HOT_COMMANDS} for side in sides}
    for r in range(1, a.rounds + 1):
        for c in HOT_COMMANDS:
            for side, binary in sides.items():
                log(f"hot key {c} {side} round {r}/{a.rounds}")
                hot[side][c.upper()].append(harness.hot_key(binary, c))
    save(a.out, "hotkey.json", hot)

    solo = {side: [] for side in sides}
    for r in range(1, a.rounds + 1):
        for side, binary in sides.items():
            log(f"solo BGSAVE {side} round {r}/{a.rounds}")
            p, d = harness.start(binary, extra=["--solo"])
            try:
                solo[side].append(harness.loadgen("save_latency_solo"))
            finally:
                harness.stop(p, d)
    save(a.out, "solo.json", solo)

    big = {}
    for side, binary in sides.items():
        log(f"huge key {side}")
        p, d = harness.start(binary)
        try:
            big[side] = harness.loadgen("bigkey_rewrite", env={"BIGKEY_N": str(a.bigkey_n)})
        finally:
            harness.stop(p, d)
    save(a.out, "bigkey.json", big)
    log(f"done: {a.out}")


if __name__ == "__main__":
    main()
