#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

BINARY="$PROJECT_ROOT/target/release/solikv"
DATA_DIR="/tmp/solikv_chaos_test"
TEST_RESULTS="$PROJECT_ROOT/test_results_$(date +%s).txt"

REDIS_CLI="${REDIS_CLI:-redis-cli}"

NUM_KEYS="${NUM_KEYS:-1000}"
SHUTDOWN_WAIT=1
STARTUP_WAIT=2

find_free_port() {
    local port=$((RANDOM % 63000 + 2000))
    local max_attempts=1000
    local attempts=0
    while [ $attempts -lt $max_attempts ]; do
        if ! nc -z 127.0.0.1 $port 2>/dev/null; then
            echo $port
            return 0
        fi
        port=$((port + 1))
        if [ $port -gt 65535 ]; then
            port=2000
        fi
        attempts=$((attempts + 1))
    done
    echo "ERROR: Could not find free port" >&2
    return 1
}

colors() {
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
}
colors

log_info() { echo -e "${BLUE}[INFO]${NC} $1" >&2; }
log_success() { echo -e "${GREEN}[OK]${NC} $1" >&2; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1" >&2; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

cleanup() {
    log_info "Cleaning up..."
    for pid in $(pgrep -f "solikv" 2>/dev/null || true); do
        kill $pid 2>/dev/null || true
    done
    sleep 1
    for pid in $(pgrep -f "solikv" 2>/dev/null || true); do
        kill -9 $pid 2>/dev/null || true
    done
    sleep 1
}

trap cleanup EXIT
trap cleanup INT
trap cleanup TERM

get_node_for_key() {
    local key="$1"
    local num_nodes="$2"
    # Simple hash: extract number from key and use modulo
    local key_num
    key_num=$(echo "$key" | grep -oE '[0-9]+$' | head -1)
    if [ -z "$key_num" ]; then
        key_num=0
    fi
    echo $((key_num % num_nodes))
}

wait_for_server() {
    local port="$1"
    local max_attempts=20
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if $REDIS_CLI -p $port PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
        attempt=$((attempt + 1))
    done
    return 1
}

start_node() {
    local node_id="$1"
    local port
    port=$(find_free_port)
    local rest_port
    rest_port=$(find_free_port)
    local dir="$DATA_DIR/node_$port"
    
    mkdir -p "$dir"
    
    $BINARY --port $port --rest-port $rest_port --dir "$dir" --shards 1 --appendonly false --log-level error >/dev/null 2>&1 &
    local pid=$!
    
    if ! wait_for_server $port; then
        log_error "Node on port $port failed to start"
        return 1
    fi
    
    log_success "Started node $node_id on port $port (PID: $pid)"
    printf "%d|%d" "$port" "$pid"
}

stop_node() {
    local port="$1"
    local graceful="${2:-false}"
    
    if [ "$graceful" = "true" ]; then
        $REDIS_CLI -p $port SHUTDOWN NOSAVE >/dev/null 2>&1 || true
    else
        local pid
        pid=$(lsof -t -i:$port 2>/dev/null || true)
        if [ -n "$pid" ]; then
            kill -9 $pid 2>/dev/null || true
        fi
    fi
    
    sleep $SHUTDOWN_WAIT
    log_info "Stopped node on port $port"
}

kill_node_random() {
    local ports=("$@")
    local idx=$((RANDOM % ${#ports[@]}))
    local port="${ports[$idx]}"
    echo "$port"
}

write_test_keys() {
    local keys_file="$1"
    shift
    local ports=("$@")
    local num_nodes=${#ports[@]}
    
    > "$keys_file"
    
    for i in $(seq 1 $NUM_KEYS); do
        local key="key:$i"
        local value="value_$i"
        local node_idx
        node_idx=$(get_node_for_key "$key" $num_nodes)
        local port="${ports[$node_idx]}"
        
        if $REDIS_CLI -p $port SET "$key" "$value" >/dev/null 2>&1; then
            echo "$key|$value|$port" >> "$keys_file"
        fi
    done
    
    log_info "Written $NUM_KEYS keys across $num_nodes nodes"
}

verify_keys() {
    local keys_file="$1"
    local num_nodes="$2"
    shift 2
    local check_alive_ports=("$@")
    
    local found=0
    local lost=0
    
    while IFS='|' read -r key value expected_port; do
        local found_port=""
        for ap in "${check_alive_ports[@]}"; do
            if [ "$ap" -eq "$expected_port" ]; then
                found_port=$ap
                break
            fi
        done
        
        if [ -z "$found_port" ]; then
            lost=$((lost + 1))
            continue
        fi
        
        local result
        result=$($REDIS_CLI -p $found_port GET "$key" 2>/dev/null || echo "")
        if [ "$result" = "$value" ]; then
            found=$((found + 1))
        else
            lost=$((lost + 1))
        fi
    done < "$keys_file"
    
    echo "$found|$lost"
}

get_alive_ports() {
    local ports=("$@")
    local alive=()
    
    for port in "${ports[@]}"; do
        if $REDIS_CLI -p $port PING >/dev/null 2>&1; then
            alive+=("$port")
        fi
    done
    
    echo "${alive[@]}"
}

run_replication_test() {
    cleanup
    sleep 1
    
    log_info "Starting master node"
    local master_result
    master_result=$(start_node "master")
    local master_port=$(echo "$master_result" | cut -d'|' -f1)
    
    sleep 1
    
    log_info "Starting replica node"
    local replica_result
    replica_result=$(start_node "replica")
    local replica_port=$(echo "$replica_result" | cut -d'|' -f1)
    
    log_info "Master: $master_port, Replica: $replica_port"
    sleep 1
    
    # Check master role
    local master_role
    master_role=$($REDIS_CLI -p $master_port ROLE | head -1)
    log_info "Master role: $master_role"
    
    # REPLICAOF is refused until the cluster bus has HMAC/TLS (SEC-016), so this
    # asserts the refusal rather than pretending to build a replication topology.
    # Restore the failover assertions below once replication is actually wired.
    log_info "Checking that REPLICAOF is refused..."
    local replicaof_out
    replicaof_out=$($REDIS_CLI -p $replica_port REPLICAOF 127.0.0.1 $master_port 2>&1)
    log_info "REPLICAOF replied: $replicaof_out"
    
    local refused=0
    case "$replicaof_out" in
        *"not available"*) refused=1 ;;
    esac
    
    sleep 1
    
    # The replica must stay a master: a silent "OK" that changes nothing is the
    # regression this guards against.
    local replica_role
    replica_role=$($REDIS_CLI -p $replica_port ROLE | head -1)
    log_info "Replica role after REPLICAOF: $replica_role"
    
    if [ "$refused" -eq 1 ] && [ "$replica_role" = "master" ]; then
        echo "repl|refused|2|0|1|0|100" >> "$TEST_RESULTS"
        log_success "Replication test: REPLICAOF correctly refused, role unchanged"
    else
        echo "repl|refused|2|0|0|1|0" >> "$TEST_RESULTS"
        log_error "Replication test: expected REPLICAOF to be refused and role to stay master"
    fi
    
    stop_node $master_port
    cleanup
    sleep 1
}

run_chaos_test() {
    local num_nodes="$1"
    local chaos_type="$2"
    
    log_info "=== Testing $num_nodes nodes - $chaos_type ==="
    
    cleanup
    sleep 1
    
    local keys_file="$DATA_DIR/keys_${num_nodes}_${chaos_type}.txt"
    rm -f "$keys_file"
    
    log_info "Starting $num_nodes nodes..."
    local node_ports=()
    for i in $(seq 1 $num_nodes); do
        local result
        result=$(start_node "node_$i")
        if [ -n "$result" ]; then
            local port=$(echo "$result" | cut -d'|' -f1)
            node_ports+=("$port")
        else
            log_warn "Failed to start node $i"
        fi
    done
    
    if [ ${#node_ports[@]} -ne $num_nodes ]; then
        log_error "Failed to start all nodes"
        cleanup
        return 1
    fi
    
    log_info "Ports: ${node_ports[*]}"
    sleep 1
    
    log_info "Writing test keys..."
    write_test_keys "$keys_file" "${node_ports[@]}"
    
    local alive_ports
    alive_ports=($(get_alive_ports "${node_ports[@]}"))
    local initial_count=${#alive_ports[@]}
    log_info "Initial alive nodes: $initial_count"
    
    case "$chaos_type" in
        "kill_one")
            local killed_port
            killed_port=$(kill_node_random "${alive_ports[@]}")
            log_info "Killing node on port $killed_port"
            stop_node $killed_port
            ;;
        "kill_half")
            local kill_count=$((num_nodes / 2))
            log_info "Killing $kill_count nodes..."
            for i in $(seq 1 $kill_count); do
                killed_port=$(kill_node_random "${alive_ports[@]}")
                alive_ports=("${alive_ports[@]/$killed_port}")
                stop_node $killed_port
            done
            ;;
        "kill_all")
            log_info "Killing all nodes..."
            for port in "${node_ports[@]}"; do
                stop_node $port
            done
            ;;
        "sequential_kill")
            log_info "Sequential kill test..."
            local remaining=$num_nodes
            while [ $remaining -gt 1 ]; do
                alive_ports=($(get_alive_ports "${node_ports[@]}"))
                if [ ${#alive_ports[@]} -eq 0 ]; then break; fi
                
                killed_port=$(kill_node_random "${alive_ports[@]}")
                log_info "Killing node on port $killed_port (remaining: $((remaining - 1)))"
                stop_node $killed_port
                
                sleep 0.5
                remaining=$((remaining - 1))
            done
            ;;
        *)
            log_error "Unknown chaos type: $chaos_type"
            return
            ;;
    esac
    
    sleep 1
    
    alive_ports=($(get_alive_ports "${node_ports[@]}"))
    local final_count=${#alive_ports[@]}
    log_info "Alive nodes after chaos: $final_count"
    
    local result
    result=$(verify_keys "$keys_file" $num_nodes "${alive_ports[@]}")
    local found=$(echo "$result" | cut -d'|' -f1)
    local lost=$(echo "$result" | cut -d'|' -f2)
    
    local total=$((found + lost))
    local availability=0
    if [ $total -gt 0 ]; then
        availability=$((found * 100 / total))
    fi
    
    echo "$num_nodes|$chaos_type|${initial_count}|${final_count}|$found|$lost|$availability" >> "$TEST_RESULTS"
    
    log_success "Results: $found/$total keys available ($availability%)"
    log_info "Lost keys: $lost"
    
    cleanup
    sleep 1
}

build_solikv() {
    log_info "Building SoliKV..."
    if [ ! -f "$BINARY" ]; then
        cargo build --release 2>&1 | tail -5
    fi
    
    if [ ! -f "$BINARY" ]; then
        log_error "Build failed!"
        exit 1
    fi
    
    log_success "Build complete"
}

print_summary() {
    log_info "========== TEST SUMMARY =========="
    echo ""
    printf "| %-8s | %-15s | %-6s | %-6s | %-6s | %-6s | %-12s |\n" "Nodes" "Chaos" "Initial" "Final" "Found" "Lost" "Availability"
    echo "|---------|-----------------|--------|--------|--------|--------|------------|"
    
    while IFS='|' read -r nodes chaos initial final found lost avail; do
        printf "| %-8s | %-15s | %-6s | %-6s | %-6s | %-6s | %-11s%% |\n" "$nodes" "$chaos" "$initial" "$final" "$found" "$lost" "$avail"
    done < "$TEST_RESULTS"
    
    echo ""
    log_info "Results saved to: $TEST_RESULTS"
}

main() {
    echo "SoliKV Chaos Cluster Test"
    echo "=========================="
    
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    > "$TEST_RESULTS"
    
    build_solkv
    
    # Quick test: just 3 nodes with kill_one
    run_chaos_test 3 "kill_one"
    
    # Test replication command
    log_info "=== Testing REPLICAOF command ==="
    run_replication_test
    
    print_summary
    
    log_success "All tests completed!"
}

main "$@"
