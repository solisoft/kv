#!/bin/bash
# Compare Redis vs SoliKV performance
# Assumes Redis is on 6379 and SoliKV on 6380

CLIENTS=${1:-50}
REQUESTS=${2:-100000}

echo "============================================"
echo " Redis vs SoliKV Benchmark Comparison"
echo "============================================"
echo ""

run_bench() {
    local name=$1
    local port=$2
    echo "--- $name (port $port) ---"
    redis-benchmark -p "$port" -c "$CLIENTS" -n "$REQUESTS" -t set,get,incr,lpush,rpush,sadd,zadd,hset -q
    echo ""
}

echo "Running with $CLIENTS clients, $REQUESTS requests each"
echo ""

run_bench "Redis" 6379
run_bench "SoliKV" 6380

echo "--- Pipeline comparison (P=16) ---"
echo "Redis:"
redis-benchmark -p 6379 -c "$CLIENTS" -n "$REQUESTS" -P 16 -t set,get -q
echo ""
echo "SoliKV:"
redis-benchmark -p 6380 -c "$CLIENTS" -n "$REQUESTS" -P 16 -t set,get -q
