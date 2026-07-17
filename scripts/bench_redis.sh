#!/usr/bin/env bash
# Run redis-benchmark against a SoliKV (or Redis) server.
# Usage: ./scripts/bench_redis.sh [PORT] [CLIENTS] [REQUESTS]
set -euo pipefail

PORT=${1:-6379}
CLIENTS=${2:-50}
REQUESTS=${3:-200000}
HOST=${BENCH_HOST:-127.0.0.1}

echo "=== SoliKV / Redis Benchmark ==="
echo "Host: $HOST  Port: $PORT  Clients: $CLIENTS  Requests: $REQUESTS"
echo ""

run() {
  local label=$1
  shift
  echo "--- $label ---"
  redis-benchmark -h "$HOST" -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" "$@" -q
  echo ""
}

run "PING"  -t ping_inline
run "SET"   -t set
run "GET"   -t get
run "INCR"  -t incr
run "LPUSH" -t lpush
run "RPUSH" -t rpush
run "LPOP"  -t lpop
run "RPOP"  -t rpop
run "SADD"  -t sadd
run "HSET"  -t hset
run "ZADD"  -t zadd
run "MSET (10 keys)" -t mset

echo "--- Pipeline SET+GET (P=16) ---"
redis-benchmark -h "$HOST" -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -P 16 -t set,get -q
echo ""
