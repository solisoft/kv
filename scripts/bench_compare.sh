#!/usr/bin/env bash
# Compare Redis vs SoliKV RESP performance (AOF-enabled deployments recommended).
#
# Usage:
#   ./scripts/bench_compare.sh [CLIENTS] [REQUESTS] [SOLIKV_PORT] [REDIS_PORT]
#
# Defaults: 50 clients, 200000 requests, SoliKV=6390, Redis=6381
set -euo pipefail

CLIENTS=${1:-50}
REQUESTS=${2:-200000}
SOLI_PORT=${3:-6390}
REDIS_PORT=${4:-6381}
HOST=${BENCH_HOST:-127.0.0.1}

CMDS="ping_inline,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,zadd,mset"

echo "============================================"
echo " Redis vs SoliKV Benchmark Comparison"
echo "============================================"
echo "Host=$HOST  clients=$CLIENTS  n=$REQUESTS"
echo "SoliKV port=$SOLI_PORT  Redis port=$REDIS_PORT"
echo ""

run_bench() {
  local name=$1
  local port=$2
  echo "--- $name (port $port) ---"
  if ! redis-cli -h "$HOST" -p "$port" PING >/dev/null 2>&1; then
    echo "ERROR: cannot PING $HOST:$port — is the server running?"
    return 1
  fi
  redis-benchmark -h "$HOST" -p "$port" -c "$CLIENTS" -n "$REQUESTS" -t "$CMDS" -q
  echo ""
}

run_bench "SoliKV" "$SOLI_PORT"
run_bench "Redis"  "$REDIS_PORT"

echo "--- Pipeline comparison (P=16, n=$((REQUESTS > 100000 ? REQUESTS : 500000))) ---"
PIPE_N=$REQUESTS
if (( PIPE_N < 500000 )); then PIPE_N=500000; fi
echo "SoliKV:"
redis-benchmark -h "$HOST" -p "$SOLI_PORT" -c "$CLIENTS" -n "$PIPE_N" -P 16 -t set,get -q
echo ""
echo "Redis:"
redis-benchmark -h "$HOST" -p "$REDIS_PORT" -c "$CLIENTS" -n "$PIPE_N" -P 16 -t set,get -q
echo ""
echo "Done. See benches/RESULTS.md for the last published numbers."
