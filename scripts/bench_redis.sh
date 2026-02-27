#!/bin/bash
# Run redis-benchmark against a server
# Usage: ./bench_redis.sh [PORT] [CLIENTS] [REQUESTS]

PORT=${1:-6379}
CLIENTS=${2:-50}
REQUESTS=${3:-100000}

echo "=== SoliKV Benchmark ==="
echo "Port: $PORT, Clients: $CLIENTS, Requests: $REQUESTS"
echo ""

echo "--- SET ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t set -q

echo "--- GET ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t get -q

echo "--- INCR ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t incr -q

echo "--- LPUSH ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t lpush -q

echo "--- RPUSH ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t rpush -q

echo "--- SADD ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t sadd -q

echo "--- ZADD ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t zadd -q

echo "--- HSET ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t hset -q

echo "--- PING ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t ping_inline -q

echo "--- MSET (10 keys) ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -t mset -q

echo ""
echo "--- Pipeline SET+GET (16 pipeline) ---"
redis-benchmark -p "$PORT" -c "$CLIENTS" -n "$REQUESTS" -P 16 -t set,get -q
