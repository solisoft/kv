"""Shared helpers for the SoliKV benchmarks: start a throwaway server, run
redis-benchmark or the load generator against it, and stop it again.

Every server gets a fresh data directory that is removed on stop, so runs
never see each other's AOF or snapshots.

Environment:
  REDIS_BENCHMARK  path to redis-benchmark (default: found on PATH)
  LOADGEN          path to the load generator (default: benches/loadgen/loadgen)
  BENCH_PORT       RESP port for the server under test (default 6390)
  BENCH_TMP        parent directory for data directories (default: system temp)
  BENCH_SERVER_CPUS / BENCH_CLIENT_CPUS
                   CPU lists for `taskset -c` (Linux), e.g. "0-7,16-23" and
                   "8-15,24-31" to keep servers and clients on separate CCDs
"""
import json, os, shutil, socket, subprocess, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
REDIS_BENCHMARK = os.environ.get("REDIS_BENCHMARK") or shutil.which("redis-benchmark") or "redis-benchmark"
LOADGEN = os.environ.get("LOADGEN") or os.path.join(HERE, "loadgen", "loadgen")
PORT = int(os.environ.get("BENCH_PORT", "6390"))
REST_PORT = PORT + 1


def _pin(cmd, var):
    cpus = os.environ.get(var)
    return ["taskset", "-c", cpus, *cmd] if cpus else list(cmd)


def server_cmd(cmd):
    return _pin(cmd, "BENCH_SERVER_CPUS")


def client_cmd(cmd):
    return _pin(cmd, "BENCH_CLIENT_CPUS")


def _wait_port(port, timeout=15):
    t = time.time()
    while time.time() - t < timeout:
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"server did not start on port {port}")


def start(binary, extra=(), aof="true"):
    """Start `binary` on a fresh data directory; return (process, data_dir)."""
    d = tempfile.mkdtemp(prefix="solikv-bench-", dir=os.environ.get("BENCH_TMP"))
    p = subprocess.Popen(server_cmd(
        [binary, "--port", str(PORT), "--rest-port", str(REST_PORT), "--bind", "127.0.0.1",
         "--dir", d, "--appendonly", aof, "--appendfsync", "everysec",
         "--log-level", "warn", *extra]),
        stdout=subprocess.DEVNULL, stderr=open(os.path.join(d, "stderr.log"), "w"))
    try:
        _wait_port(PORT)
    except RuntimeError:
        stop(p, d)
        raise
    return p, d


def start_redis(binary, extra=()):
    """Start redis-server with the settings the SoliKV runs use: AOF everysec,
    no RDB save points, default automatic AOF rewrite, everything else default."""
    d = tempfile.mkdtemp(prefix="redis-bench-", dir=os.environ.get("BENCH_TMP"))
    p = subprocess.Popen(server_cmd(
        [binary, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", d,
         "--appendonly", "yes", "--appendfsync", "everysec", "--save", "",
         "--protected-mode", "no", "--daemonize", "no",
         "--logfile", os.path.join(d, "redis.log"), *extra]),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        _wait_port(PORT)
    except RuntimeError:
        stop(p, d)
        raise
    return p, d


def config_get(name):
    """CONFIG GET `name` on the server under test, over a bare RESP socket."""
    with socket.create_connection(("127.0.0.1", PORT), 5) as s:
        cmd = f"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n${len(name)}\r\n{name}\r\n"
        s.sendall(cmd.encode())
        buf = b""
        while True:  # reply: *2 $len name $len value (RESP2) or %1 (RESP3)
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
            lines = buf.split(b"\r\n")
            if len(lines) >= 6:
                return lines[4].decode()


def start_kividb(binary, extra=()):
    """Start KiviDB with the settings the other servers use: AOF on, fsync
    everysec, no RDB save points. Its CLI has --aof but no flag for the fsync
    policy or save points, so their defaults are checked rather than set."""
    d = tempfile.mkdtemp(prefix="kividb-bench-", dir=os.environ.get("BENCH_TMP"))
    p = subprocess.Popen(server_cmd(
        [binary, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", d, "--aof",
         "--protected-mode", "no", "--update-check", "no", "--loglevel", "warning",
         "--logfile", os.path.join(d, "kividb.log"), *extra]),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        _wait_port(PORT)
        want = {"appendonly": "yes", "appendfsync": "everysec", "save": ""}
        got = {k: config_get(k) for k in want}
        if got != want:
            raise RuntimeError(f"KiviDB settings differ from the other servers: {got}, want {want}")
    except RuntimeError:
        stop(p, d)
        raise
    return p, d


def target(path, kind="solikv", extra=()):
    """A server to benchmark: kind is "solikv", "redis" or "kividb"."""
    return {"path": os.path.abspath(path), "kind": kind, "extra": list(extra)}


def start_target(t):
    if isinstance(t, str):
        t = target(t)
    if t["kind"] == "redis":
        return start_redis(t["path"], t["extra"])
    if t["kind"] == "kividb":
        return start_kividb(t["path"], t["extra"])
    return start(t["path"], t["extra"])


def stop(p, d):
    p.terminate()
    try:
        p.wait(10)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    shutil.rmtree(d, ignore_errors=True)


def redis_bench(args):
    """Run redis-benchmark --csv; return {COMMAND: {rps, p50, p99, max}}."""
    out = subprocess.run(client_cmd([REDIS_BENCHMARK, "-p", str(PORT), "--csv", *args]),
                         capture_output=True, text=True, timeout=1200).stdout
    res = {}
    for line in out.splitlines()[1:]:
        cols = [c.strip('"') for c in line.split('","')]
        if len(cols) >= 8:
            res[cols[0].split(" ")[0]] = {"rps": float(cols[1]), "p50": float(cols[3]),
                                          "p99": float(cols[6]), "max": float(cols[7])}
    return res


def loadgen(only=None, env=None):
    """Run the load generator; return {scenario: value}."""
    cmd = [LOADGEN, "-addr", f"127.0.0.1:{PORT}"]
    if only:
        cmd += ["-only", only]
    cmd = client_cmd(cmd)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600,
                       env={**os.environ, **(env or {})})
    if r.returncode != 0:
        raise RuntimeError(f"loadgen failed:\n{r.stderr[-2000:]}")
    out = {}
    for line in r.stdout.splitlines():
        if line.startswith("{"):
            e = json.loads(line)
            out[e["name"]] = e["value"]
    return out


def suite(t):
    """One full pass: standard and pipelined redis-benchmark on one server,
    then every default load-generator scenario on another. `t` is a binary
    path (SoliKV) or a target()."""
    p, d = start_target(t)
    try:
        std = redis_bench(["-t", "ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,zadd,mset",
                           "-n", "200000", "-c", "50"])
        pipe = redis_bench(["-t", "set,get,incr,lpush,sadd,hset,zadd",
                            "-n", "2000000", "-c", "50", "-P", "16"])
    finally:
        stop(p, d)
    p, d = start_target(t)
    try:
        lg = loadgen()
    finally:
        stop(p, d)
    return {"std": std, "pipe": pipe, "loadgen": lg}


def hot_key(t, command, ops=20_000_000):
    """Six benchmark threads, 96 connections, pipeline 32, all on one key.
    Returns ops/s. A fresh server per call so AOF files never pile up."""
    p, d = start_target(t)
    try:
        r = redis_bench(["-t", command, "-n", str(ops), "-c", "96", "-P", "32", "--threads", "6"])
    finally:
        stop(p, d)
    return r[command.upper()]["rps"]
