#!/usr/bin/env python3
"""Print Markdown tables for a versus.py results directory: one column per
server, medians over rounds, the best value of each row in bold.

usage: report_versus.py OUT_DIR
"""
import json, os, re, shlex, sys
from statistics import median

R = sys.argv[1]


def load(name):
    with open(os.path.join(R, name)) as f:
        return json.load(f)


meta, suites, hot, big = load("meta.json"), load("suites.json"), load("hotkey.json"), load("bigkey.json")
names = list(meta["targets"])


def dur(ms):
    if ms >= 1000:
        return f"{ms / 1000:.2f} s"
    if ms >= 10:
        return f"{ms:.1f} ms"
    if ms >= 1:
        return f"{ms:.2f} ms"
    return f"{ms * 1000:.0f} µs"


def rate(v):
    if v >= 1e6:
        return f"{v / 1e6:.2f}M/s"
    return f"{v / 1e3:.1f}K/s" if v >= 1e3 else f"{v:.0f}/s"


def row(label, values, fmt, higher_is_better):
    """values: list of medians or None, in `names` order."""
    present = [v for v in values if v is not None]
    best = (max if higher_is_better else min)(present) if present else None
    cells = []
    for v in values:
        if v is None:
            cells.append("—")
        else:
            s = fmt(v)
            cells.append(f"**{s}**" if v == best and len(present) > 1 else s)
    return f"| {label} | " + " | ".join(cells) + " |"


def header(first):
    return f"| {first} | " + " | ".join(names) + " |\n|---|" + "---|" * len(names)


def lg(name, scale=1):
    out = []
    for n in names:
        vals = [s["loadgen"][name] * scale for s in suites[n] if name in s["loadgen"]]
        out.append(median(vals) if vals else None)
    return out


def bench(section, cmd):
    out = []
    for n in names:
        vals = [s[section][cmd]["rps"] for s in suites[n] if cmd in s[section]]
        out.append(median(vals) if vals else None)
    return out


print("### Versions\n")
print("| Server | Version | Flags |\n|---|---|---|")
for n, t in meta["targets"].items():
    flags = f"`{shlex.join(t['extra'])}`" if t["extra"] else "—"
    m = re.search(r"v=(\S+)", t["version"])
    version = f"Redis {m.group(1)}" if m else t["version"]
    print(f"| {n} | {version} | {flags} |")

ORDER = ["PING_INLINE", "PING_MBULK", "SET", "GET", "INCR", "LPUSH", "RPUSH", "LPOP", "RPOP",
         "SADD", "HSET", "ZADD", "MSET"]
for section, title in (("std", "### Standard commands — 50 clients, 200k ops (req/s)"),
                       ("pipe", "### Pipelined — P=16, 50 clients, 2M ops (req/s)")):
    print(f"\n{title}\n")
    print(header("Command"))
    for cmd in ORDER:
        vals = bench(section, cmd)
        if any(v is not None for v in vals):
            print(row(cmd.replace("_", " "), vals, rate, True))

print("\n### One hot key — `redis-benchmark --threads 6 -c 96 -P 32 -n 20000000` (req/s)\n")
print(header("Command"))
for cmd in ["SET", "GET", "INCR", "LPUSH", "HSET", "ZADD"]:
    vals = [median(hot[n][cmd]) if hot.get(n, {}).get(cmd) else None for n in names]
    print(row(cmd, vals, rate, True))

SCENARIOS = [
    ("setbit_growth", "SETBIT growing a 10 MB bitmap (100k calls)", "duration", 1),
    ("append_growth", "APPEND building a 10 MB string (20k calls)", "duration", 1),
    ("zset_zrank", "ZRANK, 200k-member sorted set", "throughput", 1),
    ("zset_zrange_offset", "ZRANGE 10 members deep in a 200k sorted set", "throughput", 1),
    ("zscan_full", "Full ZSCAN, 50k members", "duration", 1),
    ("hscan_full", "Full HSCAN, 50k fields", "duration", 1),
    ("sinter", "SINTER 100k-member set with 100-member set", "throughput", 1),
    ("spop_drain", "SPOP until a 100k set is empty", "duration", 1),
    ("bigframe_mset", "One MSET of 10k pairs", "duration", 1),
    ("bigframe_rpush", "One RPUSH of 100k elements", "duration", 1),
    ("multi_del", "One DEL of 1000 keys", "duration", 0.001),
]
print("\n### Targeted workloads\n")
print(header("Workload"))
for name, label, kind, scale in SCENARIOS:
    vals = lg(name, scale)
    if any(v is not None for v in vals):
        print(row(label, vals, rate if kind == "throughput" else dur, kind == "throughput"))

LATENCY = [
    ("save_latency_p999", "GET p99.9 during BGSAVE (1M keys, 16 clients)"),
    ("save_latency_max", "GET worst during BGSAVE"),
    ("save_cmd_latency_p999", "GET p99.9 while SAVE runs"),
    ("save_cmd_latency_max", "GET worst while SAVE runs"),
    ("flush_refill_p999", "SET p99.9 refilling after FLUSHALL of 1M keys"),
    ("flush_refill_max", "SET worst refilling after FLUSHALL"),
]
print("\n### Latency seen by other clients\n")
print(header("Measurement"))
for name, label in LATENCY:
    vals = lg(name)
    if any(v is not None for v in vals):
        print(row(label, vals, dur, False))
n_big = meta.get("bigkey_n", 2_000_000)
big_label = f"{n_big / 1e6:g}M" if n_big >= 1_000_000 else f"{n_big / 1e3:g}k"
for key, label in (("bigkey_bgsave_max", f"Other keys, worst, BGSAVE with a {big_label}-element list"),
                   ("bigkey_bgrewriteaof_max", f"Other keys, worst, BGREWRITEAOF with a {big_label}-element list")):
    vals = [big.get(n, {}).get(key) for n in names]
    if any(v is not None for v in vals):
        print(row(label, vals, dur, False))

vals = lg("expiry_idle_leftover")
print("\n### Housekeeping\n")
print(header("Measurement"))
print(row("Expired keys still counted 3 s after 200k keys expired, idle server", vals,
          lambda v: f"{v:,.0f}", False))
