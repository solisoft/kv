#!/usr/bin/env python3
"""Print the before/after Markdown tables for a compare.py results directory.

usage: report.py OUT_DIR

Every figure is the median over rounds; per-round values are shown where
the spread matters (hot key).
"""
import glob, json, os, sys
from statistics import median

R = sys.argv[1]


def load(name):
    with open(os.path.join(R, name)) as f:
        return json.load(f)


def rounds(side):
    return [load(os.path.basename(p)) for p in sorted(glob.glob(os.path.join(R, f"{side}-*.json")))]


B, A = rounds("before"), rounds("after")


def samples(runs, section, key, field=None):
    vals = []
    for r in runs:
        v = r[section].get(key)
        if v is not None:
            vals.append(v[field] if field else v)
    return vals


def dur(ms):
    if ms >= 1000:
        return f"{ms / 1000:.2f} s"
    if ms >= 10:
        return f"{ms:.1f} ms"
    if ms >= 1:
        return f"{ms:.2f} ms"
    return f"{ms * 1000:.0f} µs"


def rate(v):
    return f"{v / 1e6:.2f}M/s" if v >= 1e6 else f"{v / 1e3:.1f}K/s"


def pct(before, after):
    d = 100 * (after / before - 1)
    return f"{'+' if d >= 0 else '−'}{abs(d):.1f}%"


# name, label, detail, kind, scale to ms (durations) or 1 (throughput)
SCENARIOS = [
    ("setbit_growth", "Growing a bitmap with SETBIT", "100k SETBITs at rising offsets, 10 MB at the end", "duration", 1),
    ("append_growth", "Building a string with APPEND", "20k appends of 512 B, 10 MB at the end", "duration", 1),
    ("zscan_full", "A full ZSCAN", "sorted set of 50k members, COUNT 100", "duration", 1),
    ("hscan_full", "A full HSCAN", "hash of 50k fields, COUNT 100", "duration", 1),
    ("zset_zrank", "ZRANK", "random members of a 200k-member sorted set", "throughput", 1),
    ("zset_zrange_offset", "ZRANGE by index, deep", "10 members from rank 100k–190k of 200k", "throughput", 1),
    ("sinter", "SINTER, large with small", "100k-member set with a 100-member set", "throughput", 1),
    ("spop_drain", "Emptying a set with SPOP", "100k members, one SPOP at a time", "duration", 1),
    ("bigframe_mset", "One MSET of 10k pairs", "a single 10k-pair command", "duration", 1),
    ("bigframe_rpush", "One RPUSH of 100k elements", "a single 1.3 MB command", "duration", 1),
    ("multi_del", "DEL of 1000 keys", "keys spread over all shards", "duration", 0.001),
]

print("### Targeted workloads\n")
print("| Workload | Detail | Before | After | Speedup |\n|---|---|---|---|---|")
rows = []
for name, label, detail, kind, scale in SCENARIOS:
    b, a = samples(B, "loadgen", name), samples(A, "loadgen", name)
    if not b or not a:
        continue
    b, a = median(b) * scale, median(a) * scale
    x = a / b if kind == "throughput" else b / a
    fmt = rate if kind == "throughput" else dur
    rows.append((x, f"| {label} | {detail} | {fmt(b)} | {fmt(a)} | **×{x:.0f}** |" if x >= 10
                 else f"| {label} | {detail} | {fmt(b)} | {fmt(a)} | **×{x:.1f}** |"))
for _, line in sorted(rows, key=lambda r: -r[0]):
    print(line)

hot = load("hotkey.json")
print("\n### One hot key — `redis-benchmark --threads 6 -c 96 -P 32 -n 20000000`, AOF on\n")
print("| Command | Before | After | Change | Before, runs | After, runs |\n|---|---|---|---|---|---|")
for cmd in ["SET", "INCR", "HSET", "ZADD", "LPUSH"]:
    bs, as_ = hot["before"][cmd], hot["after"][cmd]
    b, a = median(bs), median(as_)
    d = 100 * (a / b - 1)
    print(f"| {cmd} | {b / 1e6:.2f}M/s | {a / 1e6:.2f}M/s | {'+' if d >= 0 else '−'}{abs(d):.0f}% | "
          f"{' · '.join(f'{v / 1e6:.2f}' for v in bs)} | {' · '.join(f'{v / 1e6:.2f}' for v in as_)} |")

solo, big = load("solo.json"), load("bigkey.json")
n = load("meta.json").get("bigkey_n", 2_000_000)
big_label = f"{n / 1e6:g}M" if n >= 1_000_000 else f"{n / 1e3:g}k"
LATENCY = [
    ("save_latency_max", "BGSAVE, worst GET", "1M keys over 16 shards, 16 clients"),
    ("save_latency_p999", "BGSAVE, GET p99.9", "1M keys over 16 shards, 16 clients"),
    ("save_cmd_latency_max", "SAVE, worst GET", "other clients while SAVE runs"),
    ("save_cmd_latency_p999", "SAVE, GET p99.9", "other clients while SAVE runs"),
    ("save_latency_solo_max", "BGSAVE in --solo mode, worst GET", "1M keys, single-threaded engine"),
    ("save_latency_solo_p999", "BGSAVE in --solo mode, GET p99.9", "1M keys, single-threaded engine"),
    ("flush_refill_max", "Refill after FLUSHALL, worst SET", "1M keys flushed, then 16 clients write"),
    ("flush_refill_p999", "Refill after FLUSHALL, SET p99.9", "1M keys flushed, then 16 clients write"),
    ("bigkey_bgsave_max", f"BGSAVE of a {big_label}-element list, worst", "GET/LPUSH on other keys of that shard"),
    ("bigkey_bgsave_p999", f"BGSAVE of a {big_label}-element list, p99.9", "GET/LPUSH on other keys of that shard"),
]
print("\n### Latency seen by other clients while saving\n")
print("| Measurement | Setup | Before | After |\n|---|---|---|---|")
for name, label, detail in LATENCY:
    if name.startswith("save_latency_solo"):
        b = [r[name] for r in solo["before"] if name in r]
        a = [r[name] for r in solo["after"] if name in r]
    elif name.startswith("bigkey"):
        b = [big["before"][name]] if name in big.get("before", {}) else []
        a = [big["after"][name]] if name in big.get("after", {}) else []
    else:
        b, a = samples(B, "loadgen", name), samples(A, "loadgen", name)
    if b and a:
        print(f"| {label} | {detail} | {dur(median(b))} | {dur(median(a))} |")

b, a = samples(B, "loadgen", "expiry_idle_leftover"), samples(A, "loadgen", "expiry_idle_leftover")
if b and a:
    print(f"\nExpired keys still resident 3 s after 200k keys with a 200 ms TTL expired, "
          f"idle server: **{median(b):,.0f} → {median(a):,.0f}**.")

ORDER = ["PING_INLINE", "PING_MBULK", "SET", "GET", "INCR", "LPUSH", "RPUSH", "LPOP", "RPOP",
         "SADD", "HSET", "ZADD", "MSET"]
for section, title, fmt in (("std", "### Standard commands — 50 clients, 200k ops", lambda v: f"{v / 1e3:.1f}K"),
                            ("pipe", "### Pipelined — P=16, 50 clients, 2M ops", lambda v: f"{v / 1e6:.2f}M")):
    print(f"\n{title}\n")
    print("| Command | Before | After | Change |\n|---|---|---|---|")
    for cmd in ORDER:
        b, a = samples(B, section, cmd, "rps"), samples(A, section, cmd, "rps")
        if b and a:
            b, a = median(b), median(a)
            print(f"| {cmd.replace('_', ' ')} | {fmt(b)} | {fmt(a)} | {pct(b, a)} |")
